//! SQLite-backed aggregate provider request and connection admission.

use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use cap_fs_ext::{FollowSymlinks, MetadataExt as _, OpenOptionsFollowExt as _};
use cap_std::ambient_authority;
use cap_std::fs::{Dir, OpenOptions};
use market_squawk_domain::{DigestAlgorithm, EvidenceDigest, Timestamp};
use market_squawk_sources::{
    AuthorizationMode, BudgetUnavailableReason, BudgetWindowSemantics, ProviderBudgetPolicy,
    ProviderRateCollisionKind, ProviderRateDecision, ProviderRateDeclaration, ProviderRateGroupId,
    ProviderRatePermitId, ProviderRateRegistration, ProviderRateRunId, ProviderRateStore,
    ProviderRateStoreError, RetryAfter,
};
use rusqlite::{
    Connection, OpenFlags, OptionalExtension as _, Transaction, TransactionBehavior, params,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use uuid::Uuid;

const PROVIDER_RATE_APPLICATION_ID: i64 = 0x4d53_5152;
const PROVIDER_RATE_SCHEMA_VERSION: i64 = 1;
const MAXIMUM_RETAINED_RUNS: i64 = 1_024;
const MAXIMUM_AUTHORIZATION_SUBJECTS: i64 = 4_096;
const MAXIMUM_GROUPS: i64 = 4_096;
const MAXIMUM_DECLARATIONS: i64 = 4_096;
const MAXIMUM_COLLISION_KEYS: usize = 64;
const COLLISION_KEY_BYTES: usize = 33;
const BUSY_TIMEOUT: Duration = Duration::from_millis(750);
const OWNER_LOCK_FILE: &str = "provider-rate-authority.owner.lock";
const PROVIDER_RATE_LOGICAL_CHECKPOINT_SCHEMA: &str =
    "market-squawk.provider-rate-logical-checkpoint";
const PROVIDER_RATE_LOGICAL_CHECKPOINT_VERSION: u16 = 1;
const MAXIMUM_LOGICAL_CHECKPOINT_BYTES: usize = 64 * 1024 * 1024;

const SCHEMA: &str = r#"
CREATE TABLE provider_rate_runs (
    run_id BLOB PRIMARY KEY CHECK(length(run_id) = 16),
    status TEXT NOT NULL CHECK(status IN ('active', 'abandoned')),
    started_at_ns INTEGER NOT NULL,
    last_seen_at_ns INTEGER NOT NULL,
    ended_at_ns INTEGER,
    CHECK(last_seen_at_ns >= started_at_ns),
    CHECK(ended_at_ns IS NULL OR ended_at_ns >= started_at_ns)
) STRICT;

CREATE TABLE provider_rate_groups (
    group_id BLOB PRIMARY KEY CHECK(length(group_id) = 16),
    policy_digest BLOB NOT NULL CHECK(length(policy_digest) = 32),
    policy_json BLOB NOT NULL,
    state_json BLOB NOT NULL,
    state_digest BLOB NOT NULL CHECK(length(state_digest) = 32),
    state_version INTEGER NOT NULL CHECK(state_version >= 1),
    updated_at_ns INTEGER NOT NULL
) STRICT;

CREATE TABLE provider_rate_declarations (
    declaration_digest BLOB PRIMARY KEY CHECK(length(declaration_digest) = 32),
    group_id BLOB NOT NULL REFERENCES provider_rate_groups(group_id) ON DELETE RESTRICT,
    policy_digest BLOB NOT NULL CHECK(length(policy_digest) = 32),
    collision_keys BLOB NOT NULL CHECK(length(collision_keys) BETWEEN 33 AND 2112),
    row_digest BLOB NOT NULL CHECK(length(row_digest) = 32),
    created_at_ns INTEGER NOT NULL
) STRICT;

CREATE TABLE provider_rate_permits (
    permit_id BLOB PRIMARY KEY CHECK(length(permit_id) = 16),
    run_id BLOB NOT NULL REFERENCES provider_rate_runs(run_id) ON DELETE CASCADE,
    group_id BLOB NOT NULL REFERENCES provider_rate_groups(group_id) ON DELETE RESTRICT,
    acquired_at_ns INTEGER NOT NULL
) STRICT;

CREATE TABLE provider_authorization_subjects (
    authorization_mode INTEGER NOT NULL CHECK(authorization_mode IN (1, 2)),
    evidence_algorithm INTEGER NOT NULL CHECK(evidence_algorithm IN (1, 2)),
    evidence_digest BLOB NOT NULL CHECK(length(evidence_digest) = 32),
    subject TEXT NOT NULL CHECK(length(CAST(subject AS BLOB)) BETWEEN 1 AND 512),
    row_digest BLOB NOT NULL CHECK(length(row_digest) = 32),
    created_at_ns INTEGER NOT NULL,
    PRIMARY KEY(authorization_mode, evidence_algorithm, evidence_digest)
) STRICT;

CREATE INDEX provider_rate_declarations_group
    ON provider_rate_declarations(group_id);
CREATE INDEX provider_rate_permits_group
    ON provider_rate_permits(group_id);
"#;

/// Product-owned SQLite implementation of [`ProviderRateStore`].
#[derive(Clone, Debug)]
pub struct SqliteProviderRateStore {
    path: PathBuf,
    owner: Arc<ProviderRateOwnerLease>,
}

struct ProviderRateOwnerLease {
    _file: ProviderRateOwnerLock,
    run_id: Mutex<Option<ProviderRateRunId>>,
}

struct ProviderRateOwnerLock(File);

impl Drop for ProviderRateOwnerLock {
    fn drop(&mut self) {
        let _ignored = self.0.unlock();
    }
}

impl std::fmt::Debug for ProviderRateOwnerLease {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ProviderRateOwnerLease")
            .field("file", &"[EXCLUSIVE OWNER LOCK]")
            .finish_non_exhaustive()
    }
}

/// Non-cloneable owner lease over one exact logical provider-rate checkpoint.
///
/// It retains SQLite's writer transaction until post-materialization revalidation succeeds or the
/// lease is dropped. The logical payload contains rate policy state and authorization evidence,
/// never a live process run, permit, connection, or filesystem handle.
pub struct RetainedProviderRateCheckpoint {
    connection: Option<Connection>,
    checkpoint: ProviderRateLogicalCheckpoint,
}

impl RetainedProviderRateCheckpoint {
    /// Returns the canonical bounded logical export for the retained authority revision.
    #[must_use]
    pub fn bytes(&self) -> &[u8] {
        &self.checkpoint.bytes
    }

    /// Returns the domain-separated durable authority identity of the logical export.
    #[must_use]
    pub const fn authority_revision_sha256(&self) -> [u8; 32] {
        self.checkpoint.authority_revision_sha256
    }

    /// Returns the SHA-256 digest of the exact emitted logical bytes.
    #[must_use]
    pub const fn content_sha256(&self) -> [u8; 32] {
        self.checkpoint.content_sha256
    }

    /// Revalidates the emitted receipt and commits the retained writer transaction.
    ///
    /// Consuming the lease prevents a second materialization/revalidation cycle from reusing an
    /// authority snapshot after its writer fence has been released.
    pub fn revalidate_emitted(
        mut self,
        byte_length: u64,
        content_sha256: [u8; 32],
    ) -> Result<(), ProviderRateStoreError> {
        if usize::try_from(byte_length).ok() != Some(self.checkpoint.bytes.len())
            || content_sha256 != self.checkpoint.content_sha256
        {
            return Err(ProviderRateStoreError::Corrupt);
        }
        let connection = self
            .connection
            .as_ref()
            .ok_or(ProviderRateStoreError::Corrupt)?;
        let current = checkpoint_from_connection(connection)?;
        if current.bytes != self.checkpoint.bytes
            || current.authority_revision_sha256 != self.checkpoint.authority_revision_sha256
            || current.content_sha256 != self.checkpoint.content_sha256
        {
            return Err(ProviderRateStoreError::Corrupt);
        }
        let connection = self
            .connection
            .take()
            .ok_or(ProviderRateStoreError::Corrupt)?;
        connection.execute_batch("COMMIT").map_err(map_sql)
    }
}

impl std::fmt::Debug for RetainedProviderRateCheckpoint {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RetainedProviderRateCheckpoint")
            .field("byte_length", &self.checkpoint.bytes.len())
            .field("authority_revision_sha256", &"[SHA-256]")
            .field("content_sha256", &"[SHA-256]")
            .finish()
    }
}

impl Drop for RetainedProviderRateCheckpoint {
    fn drop(&mut self) {
        if let Some(connection) = self.connection.take() {
            let _ignored = connection.execute_batch("ROLLBACK");
        }
    }
}

struct ProviderRateLogicalCheckpoint {
    bytes: Vec<u8>,
    authority_revision_sha256: [u8; 32],
    content_sha256: [u8; 32],
    envelope: ProviderRateLogicalCheckpointEnvelope,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ProviderRateLogicalCheckpointEnvelope {
    schema: String,
    schema_version: u16,
    sqlite_application_id: i64,
    sqlite_user_version: i64,
    sqlite_schema_sha256: [u8; 32],
    capacities: ProviderRateCheckpointCapacities,
    groups: Vec<ProviderRateCheckpointGroup>,
    declarations: Vec<ProviderRateCheckpointDeclaration>,
    authorization_subjects: Vec<ProviderRateCheckpointAuthorizationSubject>,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ProviderRateCheckpointCapacities {
    maximum_groups: i64,
    maximum_declarations: i64,
    maximum_authorization_subjects: i64,
    maximum_collision_keys: usize,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ProviderRateCheckpointGroup {
    group_id: [u8; 16],
    policy_digest: [u8; 32],
    policy_json: Vec<u8>,
    state_json: Vec<u8>,
    state_digest: [u8; 32],
    state_version: i64,
    updated_at_ns: i64,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ProviderRateCheckpointDeclaration {
    declaration_digest: [u8; 32],
    group_id: [u8; 16],
    policy_digest: [u8; 32],
    collision_keys: Vec<u8>,
    row_digest: [u8; 32],
    created_at_ns: i64,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ProviderRateCheckpointAuthorizationSubject {
    authorization_mode: i64,
    evidence_algorithm: i64,
    evidence_digest: [u8; 32],
    subject: String,
    row_digest: [u8; 32],
    created_at_ns: i64,
}

impl SqliteProviderRateStore {
    /// Creates or opens one hardened provider-rate database at a controlled local path.
    ///
    /// # Errors
    ///
    /// Rejects symlinks, foreign SQLite files, unsupported schema versions, unsafe journal
    /// configuration, and corrupt state.
    pub fn try_open(path: impl Into<PathBuf>) -> Result<Self, ProviderRateStoreError> {
        let path = prepare_path(path.into())?;
        let owner = Arc::new(acquire_owner_lease(&path)?);
        let flags = OpenFlags::SQLITE_OPEN_READ_WRITE
            | OpenFlags::SQLITE_OPEN_CREATE
            | OpenFlags::SQLITE_OPEN_NO_MUTEX
            | OpenFlags::SQLITE_OPEN_NOFOLLOW;
        let connection = Connection::open_with_flags(&path, flags)
            .map_err(|_| ProviderRateStoreError::Unavailable)?;
        harden_connection(&connection)?;
        initialize_schema(&connection)?;
        harden_file_permissions(&path)?;
        verify_connection_configuration(&connection)?;
        verify_database_integrity(&connection)?;
        Ok(Self { path, owner })
    }

    /// Retains one bounded logical export while holding SQLite's real writer fence.
    ///
    /// The returned lease owns an `IMMEDIATE` transaction. It deliberately excludes process-era
    /// runs and permits, so every restored store must establish fresh process authority through
    /// [`ProviderRateStore::start_run`]. No store writer can commit between this retention,
    /// materialization, and [`RetainedProviderRateCheckpoint::revalidate_emitted`].
    pub fn retain_logical_checkpoint(
        &self,
    ) -> Result<RetainedProviderRateCheckpoint, ProviderRateStoreError> {
        let connection = self.connection()?;
        connection
            .execute_batch("BEGIN IMMEDIATE")
            .map_err(map_sql)?;
        let checkpoint = match checkpoint_from_connection(&connection) {
            Ok(checkpoint) => checkpoint,
            Err(error) => {
                let _ignored = connection.execute_batch("ROLLBACK");
                return Err(error);
            }
        };
        Ok(RetainedProviderRateCheckpoint {
            connection: Some(connection),
            checkpoint,
        })
    }

    /// Restores an exact logical checkpoint only into a database root that has never contained a
    /// provider-rate database or one of its SQLite/owner sidecars.
    ///
    /// The checkpoint is decoded and completely validated before a target is opened. Its process
    /// run and permit tables remain empty; callers must reopen normal authority through
    /// [`ProviderRateAuthority`](market_squawk_sources::ProviderRateAuthority), which calls
    /// [`ProviderRateStore::start_run`].
    pub fn restore_logical_fresh(
        path: impl Into<PathBuf>,
        bytes: &[u8],
        expected_authority_revision_sha256: [u8; 32],
    ) -> Result<Self, ProviderRateStoreError> {
        let checkpoint = decode_checkpoint(bytes)?;
        if checkpoint.authority_revision_sha256 != expected_authority_revision_sha256 {
            return Err(ProviderRateStoreError::Corrupt);
        }
        let path = prepare_fresh_restore_path(path.into())?;
        let store = Self::try_open(path)?;
        let result = restore_checkpoint(&store, &checkpoint);
        if result.is_err() {
            return Err(ProviderRateStoreError::Corrupt);
        }
        Ok(store)
    }

    fn connection(&self) -> Result<Connection, ProviderRateStoreError> {
        let path = prepare_path(self.path.clone())?;
        let flags = OpenFlags::SQLITE_OPEN_READ_WRITE
            | OpenFlags::SQLITE_OPEN_NO_MUTEX
            | OpenFlags::SQLITE_OPEN_NOFOLLOW;
        let connection = Connection::open_with_flags(path, flags)
            .map_err(|_| ProviderRateStoreError::Unavailable)?;
        harden_connection(&connection)?;
        verify_connection_configuration(&connection)?;
        Ok(connection)
    }
}

impl ProviderRateStore for SqliteProviderRateStore {
    fn start_run(&self, now: Timestamp) -> Result<ProviderRateRunId, ProviderRateStoreError> {
        let mut owned_run = self
            .owner
            .run_id
            .lock()
            .map_err(|_| ProviderRateStoreError::Corrupt)?;
        if let Some(run_id) = *owned_run {
            return Ok(run_id);
        }
        let mut connection = self.connection()?;
        let transaction = immediate(&mut connection)?;
        validate_global_clock(&transaction, now)?;
        transaction
            .execute(
                "DELETE FROM provider_rate_permits
                 WHERE run_id IN (
                    SELECT run_id FROM provider_rate_runs WHERE status = 'active'
                 )",
                [],
            )
            .map_err(map_sql)?;
        transaction
            .execute(
                "UPDATE provider_rate_runs
                 SET status = 'abandoned', ended_at_ns = ?1, last_seen_at_ns = ?1
                 WHERE status = 'active'",
                [now.unix_nanos()],
            )
            .map_err(map_sql)?;
        transaction
            .execute(
                "DELETE FROM provider_rate_runs
                 WHERE status = 'abandoned'
                   AND run_id NOT IN (
                       SELECT run_id
                       FROM provider_rate_runs
                       WHERE status = 'abandoned'
                       ORDER BY last_seen_at_ns DESC, run_id DESC
                       LIMIT ?1
                   )",
                [MAXIMUM_RETAINED_RUNS - 1],
            )
            .map_err(map_sql)?;
        let run_id = ProviderRateRunId::from_bytes(*Uuid::new_v4().as_bytes());
        transaction
            .execute(
                "INSERT INTO provider_rate_runs(
                    run_id, status, started_at_ns, last_seen_at_ns, ended_at_ns
                 ) VALUES (?1, 'active', ?2, ?2, NULL)",
                params![run_id.bytes(), now.unix_nanos()],
            )
            .map_err(map_sql)?;
        transaction.commit().map_err(map_sql)?;
        *owned_run = Some(run_id);
        Ok(run_id)
    }

    fn register(
        &self,
        run_id: ProviderRateRunId,
        declaration: &ProviderRateDeclaration,
        now: Timestamp,
    ) -> Result<ProviderRateRegistration, ProviderRateStoreError> {
        declaration
            .validate()
            .map_err(|_| ProviderRateStoreError::Corrupt)?;
        let policy_digest = sha256_bytes(declaration.policy_digest())?;
        let declaration_digest = sha256_bytes(declaration.declaration_digest())?;
        let collision_keys = encode_collision_keys(declaration)?;
        let policy_json = serde_json::to_vec(declaration.policy())
            .map_err(|_| ProviderRateStoreError::Corrupt)?;

        let mut connection = self.connection()?;
        let transaction = immediate(&mut connection)?;
        validate_run(&transaction, run_id, now)?;
        if let Some(existing) = existing_declaration(&transaction, declaration_digest)? {
            if existing.policy_digest != policy_digest || existing.collision_keys != collision_keys
            {
                return Err(ProviderRateStoreError::Conflict);
            }
            validate_group_policy(&transaction, existing.group_id, policy_digest)?;
            transaction.commit().map_err(map_sql)?;
            return Ok(ProviderRateRegistration::new(
                ProviderRateGroupId::from_bytes(existing.group_id),
                declaration.policy_digest(),
                declaration.declaration_digest(),
            ));
        }
        enforce_capacity(&transaction)?;
        let matches = matching_groups(&transaction, &collision_keys)?;
        let group_id = match matches.as_slice() {
            [] => {
                let group_id = *Uuid::new_v4().as_bytes();
                let state = RateState::new(declaration.policy(), now)?;
                insert_group(
                    &transaction,
                    group_id,
                    policy_digest,
                    &policy_json,
                    state,
                    now,
                )?;
                group_id
            }
            [group_id] => {
                validate_group_policy(&transaction, *group_id, policy_digest)?;
                *group_id
            }
            _ => return Err(ProviderRateStoreError::Conflict),
        };
        let row_digest =
            declaration_row_digest(declaration_digest, group_id, policy_digest, &collision_keys);
        transaction
            .execute(
                "INSERT INTO provider_rate_declarations(
                    declaration_digest, group_id, policy_digest, collision_keys, row_digest,
                    created_at_ns
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                params![
                    declaration_digest,
                    group_id,
                    policy_digest,
                    collision_keys,
                    row_digest,
                    now.unix_nanos()
                ],
            )
            .map_err(map_sql)?;
        transaction.commit().map_err(map_sql)?;
        Ok(ProviderRateRegistration::new(
            ProviderRateGroupId::from_bytes(group_id),
            declaration.policy_digest(),
            declaration.declaration_digest(),
        ))
    }

    fn try_acquire(
        &self,
        run_id: ProviderRateRunId,
        registration: ProviderRateRegistration,
        now: Timestamp,
    ) -> Result<ProviderRateDecision, ProviderRateStoreError> {
        let mut connection = self.connection()?;
        let transaction = immediate(&mut connection)?;
        validate_run(&transaction, run_id, now)?;
        let mut group = load_group(&transaction, registration)?;
        group.state.advance(group.policy.as_ref(), now)?;
        if group.state.disabled {
            return Ok(ProviderRateDecision::Unavailable(
                BudgetUnavailableReason::Disabled,
            ));
        }
        if let Some(deadline) = group.state.blocked_until(group.policy.as_ref(), now)? {
            persist_group(&transaction, &mut group, now)?;
            transaction.commit().map_err(map_sql)?;
            return Ok(ProviderRateDecision::WaitUntil(deadline));
        }
        let in_flight: i64 = transaction
            .query_row(
                "SELECT COUNT(*) FROM provider_rate_permits WHERE group_id = ?1",
                [registration.group_id().bytes()],
                |row| row.get(0),
            )
            .map_err(map_sql)?;
        if in_flight < 0 || in_flight > i64::from(group.policy.max_concurrent()) {
            return Err(ProviderRateStoreError::Corrupt);
        }
        if in_flight == i64::from(group.policy.max_concurrent()) {
            return Ok(ProviderRateDecision::Unavailable(
                BudgetUnavailableReason::ConcurrencyExhausted,
            ));
        }
        group.state.admit(group.policy.as_ref(), now)?;
        let permit_id = ProviderRatePermitId::from_bytes(*Uuid::new_v4().as_bytes());
        transaction
            .execute(
                "INSERT INTO provider_rate_permits(permit_id, run_id, group_id, acquired_at_ns)
                 VALUES (?1, ?2, ?3, ?4)",
                params![
                    permit_id.bytes(),
                    run_id.bytes(),
                    registration.group_id().bytes(),
                    now.unix_nanos()
                ],
            )
            .map_err(map_sql)?;
        persist_group(&transaction, &mut group, now)?;
        transaction.commit().map_err(map_sql)?;
        Ok(ProviderRateDecision::Ready(permit_id))
    }

    fn release(
        &self,
        run_id: ProviderRateRunId,
        registration: ProviderRateRegistration,
        permit_id: ProviderRatePermitId,
    ) -> Result<(), ProviderRateStoreError> {
        let mut connection = self.connection()?;
        let transaction = immediate(&mut connection)?;
        let deleted = transaction
            .execute(
                "DELETE FROM provider_rate_permits
                 WHERE permit_id = ?1 AND run_id = ?2 AND group_id = ?3",
                params![
                    permit_id.bytes(),
                    run_id.bytes(),
                    registration.group_id().bytes()
                ],
            )
            .map_err(map_sql)?;
        if deleted > 1 {
            return Err(ProviderRateStoreError::Corrupt);
        }
        transaction.commit().map_err(map_sql)
    }

    fn apply_retry_after(
        &self,
        run_id: ProviderRateRunId,
        registration: ProviderRateRegistration,
        now: Timestamp,
        retry_after: RetryAfter,
    ) -> Result<ProviderRateDecision, ProviderRateStoreError> {
        self.mutate_group(run_id, registration, now, |group| {
            let delay = match retry_after {
                RetryAfter::Delay(delay) => delay.get(),
                RetryAfter::AtWallClock(deadline) => deadline
                    .unix_nanos()
                    .checked_sub(now.unix_nanos())
                    .ok_or(ProviderRateStoreError::Clock)?
                    .max(0)
                    .unsigned_abs(),
            };
            if delay > group.policy.backoff().maximum_nanos() {
                group.state.disabled = true;
                return Ok(ProviderRateDecision::Unavailable(
                    BudgetUnavailableReason::RetryAfterExceedsPolicy,
                ));
            }
            let deadline = checked_timestamp_add(now, delay)?;
            group.state.cooldown_until_ns = Some(
                group
                    .state
                    .cooldown_until_ns
                    .map_or(deadline.unix_nanos(), |current| {
                        current.max(deadline.unix_nanos())
                    }),
            );
            Ok(ProviderRateDecision::WaitUntil(Timestamp::from_unix_nanos(
                group
                    .state
                    .cooldown_until_ns
                    .ok_or(ProviderRateStoreError::Corrupt)?,
            )))
        })
    }

    fn apply_refusal(
        &self,
        run_id: ProviderRateRunId,
        registration: ProviderRateRegistration,
        now: Timestamp,
        jitter_sample_basis_points: u16,
    ) -> Result<ProviderRateDecision, ProviderRateStoreError> {
        self.mutate_group(run_id, registration, now, |group| {
            let delay = group
                .policy
                .backoff()
                .delay_nanos(group.state.consecutive_refusals, jitter_sample_basis_points);
            group.state.consecutive_refusals = group
                .state
                .consecutive_refusals
                .checked_add(1)
                .ok_or(ProviderRateStoreError::Corrupt)?;
            let deadline = checked_timestamp_add(now, delay)?;
            group.state.cooldown_until_ns = Some(
                group
                    .state
                    .cooldown_until_ns
                    .map_or(deadline.unix_nanos(), |current| {
                        current.max(deadline.unix_nanos())
                    }),
            );
            Ok(ProviderRateDecision::WaitUntil(Timestamp::from_unix_nanos(
                group
                    .state
                    .cooldown_until_ns
                    .ok_or(ProviderRateStoreError::Corrupt)?,
            )))
        })
    }

    fn record_success(
        &self,
        run_id: ProviderRateRunId,
        registration: ProviderRateRegistration,
        now: Timestamp,
    ) -> Result<(), ProviderRateStoreError> {
        self.mutate_group(run_id, registration, now, |group| {
            group.state.consecutive_refusals = 0;
            Ok(())
        })
    }

    fn bind_authorization_subject(
        &self,
        run_id: ProviderRateRunId,
        mode: AuthorizationMode,
        evidence: EvidenceDigest,
        subject: &market_squawk_domain::SourceIdentifier,
        now: Timestamp,
    ) -> Result<(), ProviderRateStoreError> {
        let mode_id = authorization_mode_id(mode)?;
        let algorithm_id = digest_algorithm_id(evidence.algorithm());
        let evidence_bytes = evidence.bytes();
        let expected_digest =
            authorization_subject_row_digest(mode_id, algorithm_id, evidence_bytes, subject);
        let mut connection = self.connection()?;
        let transaction = immediate(&mut connection)?;
        validate_run(&transaction, run_id, now)?;
        let existing: Option<(String, Vec<u8>)> = transaction
            .query_row(
                "SELECT subject, row_digest
                 FROM provider_authorization_subjects
                 WHERE authorization_mode = ?1
                   AND evidence_algorithm = ?2
                   AND evidence_digest = ?3",
                params![mode_id, algorithm_id, evidence_bytes],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()
            .map_err(map_sql)?;
        if let Some((existing_subject, row_digest)) = existing {
            let existing_subject =
                market_squawk_domain::SourceIdentifier::try_from(existing_subject)
                    .map_err(|_| ProviderRateStoreError::Corrupt)?;
            let row_digest: [u8; 32] = fixed_bytes(row_digest)?;
            let actual_digest = authorization_subject_row_digest(
                mode_id,
                algorithm_id,
                evidence_bytes,
                &existing_subject,
            );
            if row_digest != actual_digest {
                return Err(ProviderRateStoreError::Corrupt);
            }
            if &existing_subject != subject {
                return Err(ProviderRateStoreError::Conflict);
            }
            transaction.commit().map_err(map_sql)?;
            return Ok(());
        }
        let count: i64 = transaction
            .query_row(
                "SELECT COUNT(*) FROM provider_authorization_subjects",
                [],
                |row| row.get(0),
            )
            .map_err(map_sql)?;
        if !(0..MAXIMUM_AUTHORIZATION_SUBJECTS).contains(&count) {
            return Err(ProviderRateStoreError::Capacity);
        }
        transaction
            .execute(
                "INSERT INTO provider_authorization_subjects(
                    authorization_mode, evidence_algorithm, evidence_digest, subject, row_digest,
                    created_at_ns
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                params![
                    mode_id,
                    algorithm_id,
                    evidence_bytes,
                    subject.as_str(),
                    expected_digest,
                    now.unix_nanos()
                ],
            )
            .map_err(map_sql)?;
        transaction.commit().map_err(map_sql)
    }

    fn resolve_authorization_subject(
        &self,
        mode: AuthorizationMode,
        evidence: EvidenceDigest,
    ) -> Result<Option<market_squawk_domain::SourceIdentifier>, ProviderRateStoreError> {
        let mode_id = authorization_mode_id(mode)?;
        let algorithm_id = digest_algorithm_id(evidence.algorithm());
        let evidence_bytes = evidence.bytes();
        let connection = self.connection()?;
        let row: Option<(String, Vec<u8>)> = connection
            .query_row(
                "SELECT subject, row_digest
                 FROM provider_authorization_subjects
                 WHERE authorization_mode = ?1
                   AND evidence_algorithm = ?2
                   AND evidence_digest = ?3",
                params![mode_id, algorithm_id, evidence_bytes],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()
            .map_err(map_sql)?;
        row.map(|(subject, row_digest)| {
            let subject = market_squawk_domain::SourceIdentifier::try_from(subject)
                .map_err(|_| ProviderRateStoreError::Corrupt)?;
            let row_digest: [u8; 32] = fixed_bytes(row_digest)?;
            let actual =
                authorization_subject_row_digest(mode_id, algorithm_id, evidence_bytes, &subject);
            if row_digest != actual {
                return Err(ProviderRateStoreError::Corrupt);
            }
            Ok(subject)
        })
        .transpose()
    }
}

fn acquire_owner_lease(path: &Path) -> Result<ProviderRateOwnerLease, ProviderRateStoreError> {
    let parent = path.parent().ok_or(ProviderRateStoreError::Unavailable)?;
    let directory = Dir::open_ambient_dir(parent, ambient_authority())
        .map_err(|_| ProviderRateStoreError::Unavailable)?;
    reject_unsafe_owner_entry(&directory)?;
    let mut options = OpenOptions::new();
    options.read(true).write(true).create(true);
    options.follow(FollowSymlinks::No);
    #[cfg(unix)]
    {
        use cap_std::fs::OpenOptionsExt as _;

        options.mode(0o600);
    }
    let file = directory
        .open_with(OWNER_LOCK_FILE, &options)
        .map_err(|_| ProviderRateStoreError::Unavailable)?;
    let metadata = file
        .metadata()
        .map_err(|_| ProviderRateStoreError::Unavailable)?;
    let named = directory
        .symlink_metadata(OWNER_LOCK_FILE)
        .map_err(|_| ProviderRateStoreError::Unavailable)?;
    if !safe_owner_metadata(&metadata)
        || !safe_owner_metadata(&named)
        || (metadata.dev(), metadata.ino()) != (named.dev(), named.ino())
    {
        return Err(ProviderRateStoreError::Unavailable);
    }
    let identity_file = file
        .try_clone()
        .map_err(|_| ProviderRateStoreError::Unavailable)?;
    let file = file.into_std();
    match file.try_lock() {
        Ok(()) => {}
        Err(std::fs::TryLockError::WouldBlock) => {
            return Err(ProviderRateStoreError::AlreadyOwned);
        }
        Err(std::fs::TryLockError::Error(_)) => {
            return Err(ProviderRateStoreError::Unavailable);
        }
    }
    let file = ProviderRateOwnerLock(file);
    let locked = identity_file
        .metadata()
        .map_err(|_| ProviderRateStoreError::Unavailable)?;
    let named = directory
        .symlink_metadata(OWNER_LOCK_FILE)
        .map_err(|_| ProviderRateStoreError::Unavailable)?;
    if !safe_owner_metadata(&locked)
        || !safe_owner_metadata(&named)
        || (locked.dev(), locked.ino()) != (named.dev(), named.ino())
    {
        return Err(ProviderRateStoreError::Unavailable);
    }
    harden_file_permissions(&parent.join(OWNER_LOCK_FILE))?;
    Ok(ProviderRateOwnerLease {
        _file: file,
        run_id: Mutex::new(None),
    })
}

fn reject_unsafe_owner_entry(directory: &Dir) -> Result<(), ProviderRateStoreError> {
    match directory.symlink_metadata(OWNER_LOCK_FILE) {
        Ok(metadata) if safe_owner_metadata(&metadata) => Ok(()),
        Ok(_) => Err(ProviderRateStoreError::Unavailable),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(_) => Err(ProviderRateStoreError::Unavailable),
    }
}

fn safe_owner_metadata(metadata: &cap_std::fs::Metadata) -> bool {
    if !metadata.is_file() || metadata.nlink() != 1 {
        return false;
    }
    #[cfg(windows)]
    {
        use cap_std::fs::MetadataExt as _;

        const FILE_ATTRIBUTE_REPARSE_POINT: u32 = 0x0000_0400;
        return metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT == 0;
    }
    #[cfg(not(windows))]
    true
}

impl SqliteProviderRateStore {
    fn mutate_group<T>(
        &self,
        run_id: ProviderRateRunId,
        registration: ProviderRateRegistration,
        now: Timestamp,
        mutation: impl FnOnce(&mut LoadedGroup) -> Result<T, ProviderRateStoreError>,
    ) -> Result<T, ProviderRateStoreError> {
        let mut connection = self.connection()?;
        let transaction = immediate(&mut connection)?;
        validate_run(&transaction, run_id, now)?;
        let mut group = load_group(&transaction, registration)?;
        group.state.advance(group.policy.as_ref(), now)?;
        let decision = mutation(&mut group)?;
        persist_group(&transaction, &mut group, now)?;
        transaction.commit().map_err(map_sql)?;
        Ok(decision)
    }
}

#[derive(Debug)]
struct ExistingDeclaration {
    group_id: [u8; 16],
    policy_digest: [u8; 32],
    collision_keys: Vec<u8>,
}

#[derive(Debug)]
struct LoadedGroup {
    group_id: [u8; 16],
    policy_digest: [u8; 32],
    policy: Box<ProviderBudgetPolicy>,
    state: RateState,
    version: i64,
}

type ProviderRateGroupRow = (Vec<u8>, Vec<u8>, Vec<u8>, Vec<u8>, i64);

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct RateState {
    windows: Vec<RateWindowState>,
    cooldown_until_ns: Option<i64>,
    consecutive_refusals: u32,
    disabled: bool,
    last_observed_ns: i64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct RateWindowState {
    started_at_ns: i64,
    admitted: u32,
    sliding_release_ns: Vec<i64>,
}

impl RateState {
    fn new(policy: &ProviderBudgetPolicy, now: Timestamp) -> Result<Self, ProviderRateStoreError> {
        let mut windows = Vec::new();
        windows
            .try_reserve_exact(policy.window_count())
            .map_err(|_| ProviderRateStoreError::Capacity)?;
        for _ in 0..policy.window_count() {
            windows.push(RateWindowState {
                started_at_ns: now.unix_nanos(),
                admitted: 0,
                sliding_release_ns: Vec::new(),
            });
        }
        Ok(Self {
            windows,
            cooldown_until_ns: None,
            consecutive_refusals: 0,
            disabled: false,
            last_observed_ns: now.unix_nanos(),
        })
    }

    fn advance(
        &mut self,
        policy: &ProviderBudgetPolicy,
        now: Timestamp,
    ) -> Result<(), ProviderRateStoreError> {
        if now.unix_nanos() < self.last_observed_ns || self.windows.len() != policy.window_count() {
            return Err(ProviderRateStoreError::Clock);
        }
        for (index, state) in self.windows.iter_mut().enumerate() {
            let window = policy
                .window(index)
                .ok_or(ProviderRateStoreError::Corrupt)?;
            match window.semantics() {
                BudgetWindowSemantics::Tumbling => {
                    if !state.sliding_release_ns.is_empty()
                        || state.admitted > window.requests_per_window()
                    {
                        return Err(ProviderRateStoreError::Corrupt);
                    }
                    let ends_at = state
                        .started_at_ns
                        .checked_add(
                            i64::try_from(window.window_nanos())
                                .map_err(|_| ProviderRateStoreError::Corrupt)?,
                        )
                        .ok_or(ProviderRateStoreError::Corrupt)?;
                    if now.unix_nanos() >= ends_at {
                        state.started_at_ns = now.unix_nanos();
                        state.admitted = 0;
                    }
                }
                BudgetWindowSemantics::Sliding => {
                    if state.admitted != 0
                        || state
                            .sliding_release_ns
                            .windows(2)
                            .any(|pair| pair[0] > pair[1])
                    {
                        return Err(ProviderRateStoreError::Corrupt);
                    }
                    state
                        .sliding_release_ns
                        .retain(|release| *release > now.unix_nanos());
                    if state.sliding_release_ns.len()
                        > usize::try_from(window.requests_per_window())
                            .map_err(|_| ProviderRateStoreError::Corrupt)?
                    {
                        return Err(ProviderRateStoreError::Corrupt);
                    }
                    state.started_at_ns = now.unix_nanos();
                }
            }
        }
        if self
            .cooldown_until_ns
            .is_some_and(|deadline| deadline <= now.unix_nanos())
        {
            self.cooldown_until_ns = None;
        }
        self.last_observed_ns = now.unix_nanos();
        Ok(())
    }

    fn blocked_until(
        &self,
        policy: &ProviderBudgetPolicy,
        now: Timestamp,
    ) -> Result<Option<Timestamp>, ProviderRateStoreError> {
        let mut blocker = self
            .cooldown_until_ns
            .filter(|deadline| *deadline > now.unix_nanos());
        for (index, state) in self.windows.iter().enumerate() {
            let window = policy
                .window(index)
                .ok_or(ProviderRateStoreError::Corrupt)?;
            let deadline = match window.semantics() {
                BudgetWindowSemantics::Tumbling
                    if state.admitted == window.requests_per_window() =>
                {
                    Some(
                        state
                            .started_at_ns
                            .checked_add(
                                i64::try_from(window.window_nanos())
                                    .map_err(|_| ProviderRateStoreError::Corrupt)?,
                            )
                            .ok_or(ProviderRateStoreError::Corrupt)?,
                    )
                }
                BudgetWindowSemantics::Sliding
                    if state.sliding_release_ns.len()
                        == usize::try_from(window.requests_per_window())
                            .map_err(|_| ProviderRateStoreError::Corrupt)? =>
                {
                    state.sliding_release_ns.first().copied()
                }
                BudgetWindowSemantics::Tumbling | BudgetWindowSemantics::Sliding => None,
            };
            if let Some(deadline) = deadline {
                blocker = Some(blocker.map_or(deadline, |current| current.max(deadline)));
            }
        }
        Ok(blocker.map(Timestamp::from_unix_nanos))
    }

    fn admit(
        &mut self,
        policy: &ProviderBudgetPolicy,
        now: Timestamp,
    ) -> Result<(), ProviderRateStoreError> {
        if self.blocked_until(policy, now)?.is_some() {
            return Err(ProviderRateStoreError::Corrupt);
        }
        for (index, state) in self.windows.iter_mut().enumerate() {
            let window = policy
                .window(index)
                .ok_or(ProviderRateStoreError::Corrupt)?;
            match window.semantics() {
                BudgetWindowSemantics::Tumbling => {
                    state.admitted = state
                        .admitted
                        .checked_add(1)
                        .ok_or(ProviderRateStoreError::Corrupt)?;
                }
                BudgetWindowSemantics::Sliding => {
                    let release = checked_timestamp_add(now, window.window_nanos())?;
                    state.sliding_release_ns.push(release.unix_nanos());
                }
            }
        }
        Ok(())
    }
}

fn prepare_path(path: PathBuf) -> Result<PathBuf, ProviderRateStoreError> {
    let parent = path.parent().ok_or(ProviderRateStoreError::Unavailable)?;
    let parent = parent
        .canonicalize()
        .map_err(|_| ProviderRateStoreError::Unavailable)?;
    let file_name = path
        .file_name()
        .ok_or(ProviderRateStoreError::Unavailable)?;
    let prepared = parent.join(file_name);
    match fs::symlink_metadata(&prepared) {
        Ok(metadata) if metadata.is_file() => {}
        Ok(_) => return Err(ProviderRateStoreError::Unavailable),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(_) => return Err(ProviderRateStoreError::Unavailable),
    }
    Ok(prepared)
}

fn prepare_fresh_restore_path(path: PathBuf) -> Result<PathBuf, ProviderRateStoreError> {
    let prepared = prepare_path(path)?;
    let parent = prepared
        .parent()
        .ok_or(ProviderRateStoreError::Unavailable)?;
    let file_name = prepared
        .file_name()
        .ok_or(ProviderRateStoreError::Unavailable)?;
    let mut wal_name = file_name.to_os_string();
    wal_name.push("-wal");
    let mut shm_name = file_name.to_os_string();
    shm_name.push("-shm");
    for entry in [
        prepared.clone(),
        parent.join(wal_name),
        parent.join(shm_name),
        parent.join(OWNER_LOCK_FILE),
    ] {
        match fs::symlink_metadata(entry) {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Ok(_) | Err(_) => return Err(ProviderRateStoreError::Conflict),
        }
    }
    Ok(prepared)
}

fn checkpoint_from_connection(
    connection: &Connection,
) -> Result<ProviderRateLogicalCheckpoint, ProviderRateStoreError> {
    verify_connection_configuration(connection)?;
    verify_exact_schema(connection)?;
    verify_database_integrity(connection)?;
    verify_foreign_keys(connection)?;
    let envelope = ProviderRateLogicalCheckpointEnvelope {
        schema: PROVIDER_RATE_LOGICAL_CHECKPOINT_SCHEMA.to_owned(),
        schema_version: PROVIDER_RATE_LOGICAL_CHECKPOINT_VERSION,
        sqlite_application_id: PROVIDER_RATE_APPLICATION_ID,
        sqlite_user_version: PROVIDER_RATE_SCHEMA_VERSION,
        sqlite_schema_sha256: provider_rate_schema_sha256()?,
        capacities: ProviderRateCheckpointCapacities {
            maximum_groups: MAXIMUM_GROUPS,
            maximum_declarations: MAXIMUM_DECLARATIONS,
            maximum_authorization_subjects: MAXIMUM_AUTHORIZATION_SUBJECTS,
            maximum_collision_keys: MAXIMUM_COLLISION_KEYS,
        },
        groups: checkpoint_groups(connection)?,
        declarations: checkpoint_declarations(connection)?,
        authorization_subjects: checkpoint_authorization_subjects(connection)?,
    };
    checkpoint_from_envelope(envelope)
}

fn checkpoint_from_envelope(
    envelope: ProviderRateLogicalCheckpointEnvelope,
) -> Result<ProviderRateLogicalCheckpoint, ProviderRateStoreError> {
    validate_checkpoint_envelope(&envelope)?;
    let bytes = serde_json::to_vec(&envelope).map_err(|_| ProviderRateStoreError::Corrupt)?;
    if bytes.is_empty() || bytes.len() > MAXIMUM_LOGICAL_CHECKPOINT_BYTES {
        return Err(ProviderRateStoreError::Capacity);
    }
    let content_sha256 = Sha256::digest(&bytes).into();
    let mut authority = Sha256::new();
    authority.update(b"market-squawk/provider-rate-logical-checkpoint-authority/v1\0");
    authority.update(content_sha256);
    Ok(ProviderRateLogicalCheckpoint {
        bytes,
        authority_revision_sha256: authority.finalize().into(),
        content_sha256,
        envelope,
    })
}

fn decode_checkpoint(
    bytes: &[u8],
) -> Result<ProviderRateLogicalCheckpoint, ProviderRateStoreError> {
    if bytes.is_empty() || bytes.len() > MAXIMUM_LOGICAL_CHECKPOINT_BYTES {
        return Err(ProviderRateStoreError::Capacity);
    }
    let envelope = serde_json::from_slice(bytes).map_err(|_| ProviderRateStoreError::Corrupt)?;
    let checkpoint = checkpoint_from_envelope(envelope)?;
    if checkpoint.bytes != bytes {
        return Err(ProviderRateStoreError::Corrupt);
    }
    Ok(checkpoint)
}

fn checkpoint_groups(
    connection: &Connection,
) -> Result<Vec<ProviderRateCheckpointGroup>, ProviderRateStoreError> {
    let count = bounded_table_count(connection, "provider_rate_groups", MAXIMUM_GROUPS)?;
    let capacity = usize::try_from(count).map_err(|_| ProviderRateStoreError::Capacity)?;
    let mut groups = Vec::new();
    groups
        .try_reserve_exact(capacity)
        .map_err(|_| ProviderRateStoreError::Capacity)?;
    let mut statement = connection
        .prepare(
            "SELECT group_id, policy_digest, policy_json, state_json, state_digest, \
             state_version, updated_at_ns FROM provider_rate_groups ORDER BY group_id",
        )
        .map_err(map_sql)?;
    let mut rows = statement.query([]).map_err(map_sql)?;
    while let Some(row) = rows.next().map_err(map_sql)? {
        groups.push(ProviderRateCheckpointGroup {
            group_id: fixed_bytes(row.get(0).map_err(map_sql)?)?,
            policy_digest: fixed_bytes(row.get(1).map_err(map_sql)?)?,
            policy_json: row.get(2).map_err(map_sql)?,
            state_json: row.get(3).map_err(map_sql)?,
            state_digest: fixed_bytes(row.get(4).map_err(map_sql)?)?,
            state_version: row.get(5).map_err(map_sql)?,
            updated_at_ns: row.get(6).map_err(map_sql)?,
        });
    }
    if groups.len() != capacity {
        return Err(ProviderRateStoreError::Corrupt);
    }
    Ok(groups)
}

fn checkpoint_declarations(
    connection: &Connection,
) -> Result<Vec<ProviderRateCheckpointDeclaration>, ProviderRateStoreError> {
    let count = bounded_table_count(
        connection,
        "provider_rate_declarations",
        MAXIMUM_DECLARATIONS,
    )?;
    let capacity = usize::try_from(count).map_err(|_| ProviderRateStoreError::Capacity)?;
    let mut declarations = Vec::new();
    declarations
        .try_reserve_exact(capacity)
        .map_err(|_| ProviderRateStoreError::Capacity)?;
    let mut statement = connection
        .prepare(
            "SELECT declaration_digest, group_id, policy_digest, collision_keys, row_digest, \
             created_at_ns FROM provider_rate_declarations ORDER BY declaration_digest",
        )
        .map_err(map_sql)?;
    let mut rows = statement.query([]).map_err(map_sql)?;
    while let Some(row) = rows.next().map_err(map_sql)? {
        declarations.push(ProviderRateCheckpointDeclaration {
            declaration_digest: fixed_bytes(row.get(0).map_err(map_sql)?)?,
            group_id: fixed_bytes(row.get(1).map_err(map_sql)?)?,
            policy_digest: fixed_bytes(row.get(2).map_err(map_sql)?)?,
            collision_keys: row.get(3).map_err(map_sql)?,
            row_digest: fixed_bytes(row.get(4).map_err(map_sql)?)?,
            created_at_ns: row.get(5).map_err(map_sql)?,
        });
    }
    if declarations.len() != capacity {
        return Err(ProviderRateStoreError::Corrupt);
    }
    Ok(declarations)
}

fn checkpoint_authorization_subjects(
    connection: &Connection,
) -> Result<Vec<ProviderRateCheckpointAuthorizationSubject>, ProviderRateStoreError> {
    let count = bounded_table_count(
        connection,
        "provider_authorization_subjects",
        MAXIMUM_AUTHORIZATION_SUBJECTS,
    )?;
    let capacity = usize::try_from(count).map_err(|_| ProviderRateStoreError::Capacity)?;
    let mut subjects = Vec::new();
    subjects
        .try_reserve_exact(capacity)
        .map_err(|_| ProviderRateStoreError::Capacity)?;
    let mut statement = connection
        .prepare(
            "SELECT authorization_mode, evidence_algorithm, evidence_digest, subject, row_digest, \
             created_at_ns FROM provider_authorization_subjects \
             ORDER BY authorization_mode, evidence_algorithm, evidence_digest",
        )
        .map_err(map_sql)?;
    let mut rows = statement.query([]).map_err(map_sql)?;
    while let Some(row) = rows.next().map_err(map_sql)? {
        subjects.push(ProviderRateCheckpointAuthorizationSubject {
            authorization_mode: row.get(0).map_err(map_sql)?,
            evidence_algorithm: row.get(1).map_err(map_sql)?,
            evidence_digest: fixed_bytes(row.get(2).map_err(map_sql)?)?,
            subject: row.get(3).map_err(map_sql)?,
            row_digest: fixed_bytes(row.get(4).map_err(map_sql)?)?,
            created_at_ns: row.get(5).map_err(map_sql)?,
        });
    }
    if subjects.len() != capacity {
        return Err(ProviderRateStoreError::Corrupt);
    }
    Ok(subjects)
}

fn bounded_table_count(
    connection: &Connection,
    table: &str,
    maximum: i64,
) -> Result<i64, ProviderRateStoreError> {
    let count: i64 = connection
        .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
            row.get(0)
        })
        .map_err(map_sql)?;
    if !(0..=maximum).contains(&count) {
        return Err(ProviderRateStoreError::Capacity);
    }
    Ok(count)
}

fn validate_checkpoint_envelope(
    checkpoint: &ProviderRateLogicalCheckpointEnvelope,
) -> Result<(), ProviderRateStoreError> {
    if checkpoint.schema != PROVIDER_RATE_LOGICAL_CHECKPOINT_SCHEMA
        || checkpoint.schema_version != PROVIDER_RATE_LOGICAL_CHECKPOINT_VERSION
        || checkpoint.sqlite_application_id != PROVIDER_RATE_APPLICATION_ID
        || checkpoint.sqlite_user_version != PROVIDER_RATE_SCHEMA_VERSION
        || checkpoint.sqlite_schema_sha256 != provider_rate_schema_sha256()?
        || checkpoint.capacities.maximum_groups != MAXIMUM_GROUPS
        || checkpoint.capacities.maximum_declarations != MAXIMUM_DECLARATIONS
        || checkpoint.capacities.maximum_authorization_subjects != MAXIMUM_AUTHORIZATION_SUBJECTS
        || checkpoint.capacities.maximum_collision_keys != MAXIMUM_COLLISION_KEYS
        || i64::try_from(checkpoint.groups.len()).map_or(true, |count| count > MAXIMUM_GROUPS)
        || i64::try_from(checkpoint.declarations.len())
            .map_or(true, |count| count > MAXIMUM_DECLARATIONS)
        || i64::try_from(checkpoint.authorization_subjects.len())
            .map_or(true, |count| count > MAXIMUM_AUTHORIZATION_SUBJECTS)
    {
        return Err(ProviderRateStoreError::Corrupt);
    }
    let mut groups = std::collections::BTreeMap::new();
    let mut previous_group = None;
    for group in &checkpoint.groups {
        if group.group_id == [0; 16]
            || group.state_version < 1
            || group.policy_json.is_empty()
            || group.state_json.is_empty()
            || group.policy_json.len() > MAXIMUM_LOGICAL_CHECKPOINT_BYTES
            || group.state_json.len() > MAXIMUM_LOGICAL_CHECKPOINT_BYTES
            || previous_group.is_some_and(|previous| previous >= group.group_id)
        {
            return Err(ProviderRateStoreError::Corrupt);
        }
        let policy: ProviderBudgetPolicy = serde_json::from_slice(&group.policy_json)
            .map_err(|_| ProviderRateStoreError::Corrupt)?;
        if serde_json::to_vec(&policy).map_err(|_| ProviderRateStoreError::Corrupt)?
            != group.policy_json
            || sha256_bytes(
                ProviderRateDeclaration::policy_digest_for(&policy)
                    .map_err(|_| ProviderRateStoreError::Corrupt)?,
            )? != group.policy_digest
        {
            return Err(ProviderRateStoreError::Corrupt);
        }
        let state: RateState = serde_json::from_slice(&group.state_json)
            .map_err(|_| ProviderRateStoreError::Corrupt)?;
        let last_observed_ns = state.last_observed_ns;
        if serde_json::to_vec(&state).map_err(|_| ProviderRateStoreError::Corrupt)?
            != group.state_json
            || group.updated_at_ns < last_observed_ns
            || state
                .windows
                .iter()
                .any(|window| window.started_at_ns > last_observed_ns)
        {
            return Err(ProviderRateStoreError::Corrupt);
        }
        let mut state_at_last_observation = state;
        state_at_last_observation
            .advance(&policy, Timestamp::from_unix_nanos(last_observed_ns))
            .map_err(|_| ProviderRateStoreError::Corrupt)?;
        if state_digest(
            group.group_id,
            group.policy_digest,
            group.state_version,
            &group.state_json,
        ) != group.state_digest
        {
            return Err(ProviderRateStoreError::Corrupt);
        }
        if groups.insert(group.group_id, group.policy_digest).is_some() {
            return Err(ProviderRateStoreError::Corrupt);
        }
        previous_group = Some(group.group_id);
    }

    let mut previous_declaration = None;
    let mut declarations_per_group = std::collections::BTreeMap::new();
    for declaration in &checkpoint.declarations {
        if declaration.declaration_digest == [0; 32]
            || declaration.group_id == [0; 16]
            || declaration.collision_keys.len() > MAXIMUM_COLLISION_KEYS * COLLISION_KEY_BYTES
            || previous_declaration
                .is_some_and(|previous| previous >= declaration.declaration_digest)
            || decode_collision_keys(&declaration.collision_keys).is_err()
            || groups.get(&declaration.group_id) != Some(&declaration.policy_digest)
            || declaration_row_digest(
                declaration.declaration_digest,
                declaration.group_id,
                declaration.policy_digest,
                &declaration.collision_keys,
            ) != declaration.row_digest
        {
            return Err(ProviderRateStoreError::Corrupt);
        }
        let declaration_count = declarations_per_group
            .entry(declaration.group_id)
            .or_insert(0_u64);
        *declaration_count = declaration_count
            .checked_add(1)
            .ok_or(ProviderRateStoreError::Corrupt)?;
        previous_declaration = Some(declaration.declaration_digest);
    }
    if groups
        .keys()
        .any(|group_id| !declarations_per_group.contains_key(group_id))
    {
        return Err(ProviderRateStoreError::Corrupt);
    }

    let mut previous_subject = None;
    for subject in &checkpoint.authorization_subjects {
        let key = (
            subject.authorization_mode,
            subject.evidence_algorithm,
            subject.evidence_digest,
        );
        if !matches!(subject.authorization_mode, 1 | 2)
            || !matches!(subject.evidence_algorithm, 1 | 2)
            || subject.subject.is_empty()
            || subject.subject.len() > 512
            || previous_subject.is_some_and(|previous| previous >= key)
        {
            return Err(ProviderRateStoreError::Corrupt);
        }
        let source = market_squawk_domain::SourceIdentifier::try_from(subject.subject.clone())
            .map_err(|_| ProviderRateStoreError::Corrupt)?;
        if authorization_subject_row_digest(
            subject.authorization_mode,
            subject.evidence_algorithm,
            subject.evidence_digest,
            &source,
        ) != subject.row_digest
        {
            return Err(ProviderRateStoreError::Corrupt);
        }
        previous_subject = Some(key);
    }
    Ok(())
}

fn restore_checkpoint(
    store: &SqliteProviderRateStore,
    checkpoint: &ProviderRateLogicalCheckpoint,
) -> Result<(), ProviderRateStoreError> {
    let mut connection = store.connection()?;
    verify_exact_schema(&connection)?;
    verify_database_integrity(&connection)?;
    verify_foreign_keys(&connection)?;
    let transaction = immediate(&mut connection)?;
    for table in [
        "provider_rate_runs",
        "provider_rate_groups",
        "provider_rate_declarations",
        "provider_rate_permits",
        "provider_authorization_subjects",
    ] {
        let count: i64 = transaction
            .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
                row.get(0)
            })
            .map_err(map_sql)?;
        if count != 0 {
            return Err(ProviderRateStoreError::Conflict);
        }
    }
    for group in &checkpoint.envelope.groups {
        transaction
            .execute(
                "INSERT INTO provider_rate_groups(
                    group_id, policy_digest, policy_json, state_json, state_digest,
                    state_version, updated_at_ns
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                params![
                    group.group_id,
                    group.policy_digest,
                    group.policy_json,
                    group.state_json,
                    group.state_digest,
                    group.state_version,
                    group.updated_at_ns,
                ],
            )
            .map_err(map_sql)?;
    }
    for declaration in &checkpoint.envelope.declarations {
        transaction
            .execute(
                "INSERT INTO provider_rate_declarations(
                    declaration_digest, group_id, policy_digest, collision_keys, row_digest,
                    created_at_ns
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                params![
                    declaration.declaration_digest,
                    declaration.group_id,
                    declaration.policy_digest,
                    declaration.collision_keys,
                    declaration.row_digest,
                    declaration.created_at_ns,
                ],
            )
            .map_err(map_sql)?;
    }
    for subject in &checkpoint.envelope.authorization_subjects {
        transaction
            .execute(
                "INSERT INTO provider_authorization_subjects(
                    authorization_mode, evidence_algorithm, evidence_digest, subject, row_digest,
                    created_at_ns
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                params![
                    subject.authorization_mode,
                    subject.evidence_algorithm,
                    subject.evidence_digest,
                    subject.subject,
                    subject.row_digest,
                    subject.created_at_ns,
                ],
            )
            .map_err(map_sql)?;
    }
    verify_foreign_keys(&transaction)?;
    transaction.commit().map_err(map_sql)?;
    verify_connection_configuration(&connection)?;
    verify_exact_schema(&connection)?;
    verify_database_integrity(&connection)?;
    verify_foreign_keys(&connection)
}

fn provider_rate_schema_sha256() -> Result<[u8; 32], ProviderRateStoreError> {
    let connection = Connection::open_in_memory().map_err(map_sql)?;
    connection.execute_batch(SCHEMA).map_err(map_sql)?;
    schema_sha256(&connection)
}

fn verify_exact_schema(connection: &Connection) -> Result<(), ProviderRateStoreError> {
    if schema_sha256(connection)? != provider_rate_schema_sha256()? {
        return Err(ProviderRateStoreError::Corrupt);
    }
    Ok(())
}

fn schema_sha256(connection: &Connection) -> Result<[u8; 32], ProviderRateStoreError> {
    let mut statement = connection
        .prepare(
            "SELECT type, name, tbl_name, COALESCE(sql, '') FROM sqlite_schema \
             WHERE name NOT LIKE 'sqlite_%' \
             ORDER BY type COLLATE BINARY, name COLLATE BINARY, tbl_name COLLATE BINARY, \
             COALESCE(sql, '') COLLATE BINARY",
        )
        .map_err(map_sql)?;
    let mut rows = statement.query([]).map_err(map_sql)?;
    let mut digest = Sha256::new();
    digest.update(b"market-squawk/provider-rate-sqlite-schema/v1\0");
    let mut count = 0_u64;
    while let Some(row) = rows.next().map_err(map_sql)? {
        for index in 0..4 {
            let value: String = row.get(index).map_err(map_sql)?;
            digest.update(
                u64::try_from(value.len())
                    .map_err(|_| ProviderRateStoreError::Corrupt)?
                    .to_be_bytes(),
            );
            digest.update(value.as_bytes());
        }
        count = count
            .checked_add(1)
            .ok_or(ProviderRateStoreError::Corrupt)?;
    }
    digest.update(count.to_be_bytes());
    Ok(digest.finalize().into())
}

fn harden_connection(connection: &Connection) -> Result<(), ProviderRateStoreError> {
    connection.busy_timeout(BUSY_TIMEOUT).map_err(map_sql)?;
    connection
        .pragma_update(None, "trusted_schema", "OFF")
        .map_err(map_sql)?;
    connection
        .pragma_update(None, "foreign_keys", "ON")
        .map_err(map_sql)?;
    connection
        .pragma_update(None, "synchronous", "FULL")
        .map_err(map_sql)?;
    connection
        .pragma_update(None, "wal_autocheckpoint", 1_000_i64)
        .map_err(map_sql)
}

fn initialize_schema(connection: &Connection) -> Result<(), ProviderRateStoreError> {
    let application_id: i64 = connection
        .query_row("PRAGMA application_id", [], |row| row.get(0))
        .map_err(map_sql)?;
    let user_version: i64 = connection
        .query_row("PRAGMA user_version", [], |row| row.get(0))
        .map_err(map_sql)?;
    if application_id == 0 && user_version == 0 {
        let object_count: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%'",
                [],
                |row| row.get(0),
            )
            .map_err(map_sql)?;
        if object_count != 0 {
            return Err(ProviderRateStoreError::Corrupt);
        }
        connection.execute_batch(SCHEMA).map_err(map_sql)?;
        connection
            .pragma_update(None, "application_id", PROVIDER_RATE_APPLICATION_ID)
            .map_err(map_sql)?;
        connection
            .pragma_update(None, "user_version", PROVIDER_RATE_SCHEMA_VERSION)
            .map_err(map_sql)?;
    } else if application_id != PROVIDER_RATE_APPLICATION_ID
        || user_version != PROVIDER_RATE_SCHEMA_VERSION
    {
        return Err(ProviderRateStoreError::Corrupt);
    }
    let journal_mode: String = connection
        .query_row("PRAGMA journal_mode=WAL", [], |row| row.get(0))
        .map_err(map_sql)?;
    if !journal_mode.eq_ignore_ascii_case("wal") {
        return Err(ProviderRateStoreError::Unavailable);
    }
    Ok(())
}

fn verify_connection_configuration(connection: &Connection) -> Result<(), ProviderRateStoreError> {
    let application_id: i64 = connection
        .query_row("PRAGMA application_id", [], |row| row.get(0))
        .map_err(map_sql)?;
    let user_version: i64 = connection
        .query_row("PRAGMA user_version", [], |row| row.get(0))
        .map_err(map_sql)?;
    let journal_mode: String = connection
        .query_row("PRAGMA journal_mode", [], |row| row.get(0))
        .map_err(map_sql)?;
    if application_id != PROVIDER_RATE_APPLICATION_ID
        || user_version != PROVIDER_RATE_SCHEMA_VERSION
        || !journal_mode.eq_ignore_ascii_case("wal")
    {
        return Err(ProviderRateStoreError::Corrupt);
    }
    Ok(())
}

fn verify_database_integrity(connection: &Connection) -> Result<(), ProviderRateStoreError> {
    let integrity: String = connection
        .query_row("PRAGMA integrity_check(1)", [], |row| row.get(0))
        .map_err(map_sql)?;
    if integrity != "ok" {
        return Err(ProviderRateStoreError::Corrupt);
    }
    Ok(())
}

fn verify_foreign_keys(connection: &Connection) -> Result<(), ProviderRateStoreError> {
    let mut statement = connection
        .prepare("PRAGMA foreign_key_check")
        .map_err(map_sql)?;
    let mut rows = statement.query([]).map_err(map_sql)?;
    if rows.next().map_err(map_sql)?.is_some() {
        return Err(ProviderRateStoreError::Corrupt);
    }
    Ok(())
}

#[cfg(unix)]
fn harden_file_permissions(path: &Path) -> Result<(), ProviderRateStoreError> {
    use std::os::unix::fs::PermissionsExt as _;

    fs::set_permissions(path, fs::Permissions::from_mode(0o600))
        .map_err(|_| ProviderRateStoreError::Unavailable)
}

#[cfg(not(unix))]
fn harden_file_permissions(_path: &Path) -> Result<(), ProviderRateStoreError> {
    Ok(())
}

fn immediate(connection: &mut Connection) -> Result<Transaction<'_>, ProviderRateStoreError> {
    connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(map_sql)
}

fn validate_global_clock(
    transaction: &Transaction<'_>,
    now: Timestamp,
) -> Result<(), ProviderRateStoreError> {
    let high_water: Option<i64> = transaction
        .query_row(
            "SELECT MAX(last_seen_at_ns) FROM provider_rate_runs",
            [],
            |row| row.get(0),
        )
        .map_err(map_sql)?;
    if high_water.is_some_and(|high_water| now.unix_nanos() < high_water) {
        return Err(ProviderRateStoreError::Clock);
    }
    Ok(())
}

fn validate_run(
    transaction: &Transaction<'_>,
    run_id: ProviderRateRunId,
    now: Timestamp,
) -> Result<(), ProviderRateStoreError> {
    validate_global_clock(transaction, now)?;
    let status: Option<String> = transaction
        .query_row(
            "SELECT status FROM provider_rate_runs WHERE run_id = ?1",
            [run_id.bytes()],
            |row| row.get(0),
        )
        .optional()
        .map_err(map_sql)?;
    if status.as_deref() != Some("active") {
        return Err(ProviderRateStoreError::Unavailable);
    }
    transaction
        .execute(
            "UPDATE provider_rate_runs SET last_seen_at_ns = ?2 WHERE run_id = ?1",
            params![run_id.bytes(), now.unix_nanos()],
        )
        .map_err(map_sql)?;
    Ok(())
}

fn enforce_capacity(transaction: &Transaction<'_>) -> Result<(), ProviderRateStoreError> {
    let (groups, declarations): (i64, i64) = transaction
        .query_row(
            "SELECT
                (SELECT COUNT(*) FROM provider_rate_groups),
                (SELECT COUNT(*) FROM provider_rate_declarations)",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .map_err(map_sql)?;
    if groups < 0
        || declarations < 0
        || groups >= MAXIMUM_GROUPS
        || declarations >= MAXIMUM_DECLARATIONS
    {
        return Err(ProviderRateStoreError::Capacity);
    }
    Ok(())
}

fn encode_collision_keys(
    declaration: &ProviderRateDeclaration,
) -> Result<Vec<u8>, ProviderRateStoreError> {
    let identities = declaration.collision_identities();
    if identities.is_empty() || identities.len() > MAXIMUM_COLLISION_KEYS {
        return Err(ProviderRateStoreError::Capacity);
    }
    let mut encoded = Vec::new();
    encoded
        .try_reserve_exact(
            identities
                .len()
                .checked_mul(COLLISION_KEY_BYTES)
                .ok_or(ProviderRateStoreError::Capacity)?,
        )
        .map_err(|_| ProviderRateStoreError::Capacity)?;
    for identity in identities {
        encoded.push(match identity.kind() {
            ProviderRateCollisionKind::PublicNetworkAuthority => 1,
            ProviderRateCollisionKind::AuthorizationSubject => 2,
        });
        encoded.extend_from_slice(&sha256_bytes(identity.digest())?);
    }
    Ok(encoded)
}

fn decode_collision_keys(encoded: &[u8]) -> Result<Vec<[u8; 33]>, ProviderRateStoreError> {
    if encoded.is_empty()
        || !encoded.len().is_multiple_of(COLLISION_KEY_BYTES)
        || encoded.len() / COLLISION_KEY_BYTES > MAXIMUM_COLLISION_KEYS
    {
        return Err(ProviderRateStoreError::Corrupt);
    }
    let decoded = encoded
        .chunks_exact(COLLISION_KEY_BYTES)
        .map(|chunk| {
            let key: [u8; 33] = chunk
                .try_into()
                .map_err(|_| ProviderRateStoreError::Corrupt)?;
            if !matches!(key[0], 1 | 2) {
                return Err(ProviderRateStoreError::Corrupt);
            }
            Ok(key)
        })
        .collect::<Result<Vec<_>, _>>()?;
    if decoded.windows(2).any(|pair| pair[0] >= pair[1]) {
        return Err(ProviderRateStoreError::Corrupt);
    }
    Ok(decoded)
}

fn existing_declaration(
    transaction: &Transaction<'_>,
    declaration_digest: [u8; 32],
) -> Result<Option<ExistingDeclaration>, ProviderRateStoreError> {
    transaction
        .query_row(
            "SELECT group_id, policy_digest, collision_keys, row_digest
             FROM provider_rate_declarations WHERE declaration_digest = ?1",
            [declaration_digest],
            |row| {
                Ok((
                    row.get::<_, Vec<u8>>(0)?,
                    row.get::<_, Vec<u8>>(1)?,
                    row.get::<_, Vec<u8>>(2)?,
                    row.get::<_, Vec<u8>>(3)?,
                ))
            },
        )
        .optional()
        .map_err(map_sql)?
        .map(|(group_id, policy_digest, collision_keys, row_digest)| {
            parse_declaration_row(
                declaration_digest,
                group_id,
                policy_digest,
                collision_keys,
                row_digest,
            )
        })
        .transpose()
}

fn matching_groups(
    transaction: &Transaction<'_>,
    candidate: &[u8],
) -> Result<Vec<[u8; 16]>, ProviderRateStoreError> {
    let candidate = decode_collision_keys(candidate)?;
    let mut statement = transaction
        .prepare(
            "SELECT declaration_digest, group_id, policy_digest, collision_keys, row_digest
             FROM provider_rate_declarations",
        )
        .map_err(map_sql)?;
    let rows = statement
        .query_map([], |row| {
            Ok((
                row.get::<_, Vec<u8>>(0)?,
                row.get::<_, Vec<u8>>(1)?,
                row.get::<_, Vec<u8>>(2)?,
                row.get::<_, Vec<u8>>(3)?,
                row.get::<_, Vec<u8>>(4)?,
            ))
        })
        .map_err(map_sql)?;
    let mut groups = Vec::new();
    for row in rows {
        let (declaration_digest, group_id, policy_digest, collision_keys, row_digest) =
            row.map_err(map_sql)?;
        let declaration = parse_declaration_row(
            fixed_bytes(declaration_digest)?,
            group_id,
            policy_digest,
            collision_keys,
            row_digest,
        )?;
        let collision_keys = decode_collision_keys(&declaration.collision_keys)?;
        if candidate
            .iter()
            .any(|identity| collision_keys.binary_search(identity).is_ok())
            && !groups.contains(&declaration.group_id)
        {
            groups.push(declaration.group_id);
        }
    }
    Ok(groups)
}

fn parse_declaration_row(
    declaration_digest: [u8; 32],
    group_id: Vec<u8>,
    policy_digest: Vec<u8>,
    collision_keys: Vec<u8>,
    row_digest: Vec<u8>,
) -> Result<ExistingDeclaration, ProviderRateStoreError> {
    let group_id = fixed_bytes(group_id)?;
    let policy_digest = fixed_bytes(policy_digest)?;
    let row_digest = fixed_bytes(row_digest)?;
    let _canonical_keys = decode_collision_keys(&collision_keys)?;
    if row_digest
        != declaration_row_digest(declaration_digest, group_id, policy_digest, &collision_keys)
    {
        return Err(ProviderRateStoreError::Corrupt);
    }
    Ok(ExistingDeclaration {
        group_id,
        policy_digest,
        collision_keys,
    })
}

fn validate_group_policy(
    transaction: &Transaction<'_>,
    group_id: [u8; 16],
    expected: [u8; 32],
) -> Result<(), ProviderRateStoreError> {
    let policy: Option<Vec<u8>> = transaction
        .query_row(
            "SELECT policy_digest FROM provider_rate_groups WHERE group_id = ?1",
            [group_id],
            |row| row.get(0),
        )
        .optional()
        .map_err(map_sql)?;
    if policy.as_deref() != Some(expected.as_slice()) {
        return Err(ProviderRateStoreError::Conflict);
    }
    Ok(())
}

fn insert_group(
    transaction: &Transaction<'_>,
    group_id: [u8; 16],
    policy_digest: [u8; 32],
    policy_json: &[u8],
    state: RateState,
    now: Timestamp,
) -> Result<(), ProviderRateStoreError> {
    let state_json = serde_json::to_vec(&state).map_err(|_| ProviderRateStoreError::Corrupt)?;
    let version = 1_i64;
    let state_digest = state_digest(group_id, policy_digest, version, &state_json);
    transaction
        .execute(
            "INSERT INTO provider_rate_groups(
                group_id, policy_digest, policy_json, state_json, state_digest,
                state_version, updated_at_ns
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![
                group_id,
                policy_digest,
                policy_json,
                state_json,
                state_digest,
                version,
                now.unix_nanos()
            ],
        )
        .map_err(map_sql)?;
    Ok(())
}

fn load_group(
    transaction: &Transaction<'_>,
    registration: ProviderRateRegistration,
) -> Result<LoadedGroup, ProviderRateStoreError> {
    let row: Option<ProviderRateGroupRow> = transaction
        .query_row(
            "SELECT policy_digest, policy_json, state_json, state_digest, state_version
             FROM provider_rate_groups WHERE group_id = ?1",
            [registration.group_id().bytes()],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                ))
            },
        )
        .optional()
        .map_err(map_sql)?;
    let (policy_digest, policy_json, state_json, state_digest_bytes, version) =
        row.ok_or(ProviderRateStoreError::Corrupt)?;
    let policy_digest: [u8; 32] = fixed_bytes(policy_digest)?;
    if policy_digest != sha256_bytes(registration.policy_digest())? || version < 1 {
        return Err(ProviderRateStoreError::Conflict);
    }
    let group_id = registration.group_id().bytes();
    let expected_state_digest = state_digest(group_id, policy_digest, version, &state_json);
    if state_digest_bytes.as_slice() != expected_state_digest.as_slice() {
        return Err(ProviderRateStoreError::Corrupt);
    }
    let declaration_digest = sha256_bytes(registration.declaration_digest())?;
    let declaration = existing_declaration(transaction, declaration_digest)?
        .ok_or(ProviderRateStoreError::Conflict)?;
    if declaration.group_id != group_id || declaration.policy_digest != policy_digest {
        return Err(ProviderRateStoreError::Conflict);
    }
    let policy: ProviderBudgetPolicy =
        serde_json::from_slice(&policy_json).map_err(|_| ProviderRateStoreError::Corrupt)?;
    let actual_policy_digest = ProviderRateDeclaration::policy_digest_for(&policy)
        .map_err(|_| ProviderRateStoreError::Corrupt)?;
    if sha256_bytes(actual_policy_digest)? != policy_digest {
        return Err(ProviderRateStoreError::Corrupt);
    }
    Ok(LoadedGroup {
        group_id,
        policy_digest,
        policy: Box::new(policy),
        state: serde_json::from_slice(&state_json).map_err(|_| ProviderRateStoreError::Corrupt)?,
        version,
    })
}

fn persist_group(
    transaction: &Transaction<'_>,
    group: &mut LoadedGroup,
    now: Timestamp,
) -> Result<(), ProviderRateStoreError> {
    let next_version = group
        .version
        .checked_add(1)
        .ok_or(ProviderRateStoreError::Corrupt)?;
    let state_json =
        serde_json::to_vec(&group.state).map_err(|_| ProviderRateStoreError::Corrupt)?;
    let digest = state_digest(
        group.group_id,
        group.policy_digest,
        next_version,
        &state_json,
    );
    let updated = transaction
        .execute(
            "UPDATE provider_rate_groups
             SET state_json = ?1, state_digest = ?2, state_version = ?3, updated_at_ns = ?4
             WHERE group_id = ?5 AND state_version = ?6",
            params![
                state_json,
                digest,
                next_version,
                now.unix_nanos(),
                group.group_id,
                group.version
            ],
        )
        .map_err(map_sql)?;
    if updated != 1 {
        return Err(ProviderRateStoreError::Conflict);
    }
    group.version = next_version;
    Ok(())
}

fn declaration_row_digest(
    declaration_digest: [u8; 32],
    group_id: [u8; 16],
    policy_digest: [u8; 32],
    collision_keys: &[u8],
) -> [u8; 32] {
    let mut digest = Sha256::new();
    digest.update(b"market-squawk/provider-rate-declaration-row/v1\0");
    digest.update(declaration_digest);
    digest.update(group_id);
    digest.update(policy_digest);
    digest.update((collision_keys.len() as u64).to_be_bytes());
    digest.update(collision_keys);
    digest.finalize().into()
}

fn authorization_mode_id(mode: AuthorizationMode) -> Result<i64, ProviderRateStoreError> {
    match mode {
        AuthorizationMode::UserAuthorized => Ok(1),
        AuthorizationMode::Licensed => Ok(2),
        AuthorizationMode::PublicInterface | AuthorizationMode::UserOwnedLocal => {
            Err(ProviderRateStoreError::Conflict)
        }
    }
}

const fn digest_algorithm_id(algorithm: DigestAlgorithm) -> i64 {
    match algorithm {
        DigestAlgorithm::Sha256 => 1,
        DigestAlgorithm::Blake3 => 2,
    }
}

fn authorization_subject_row_digest(
    mode_id: i64,
    algorithm_id: i64,
    evidence: [u8; 32],
    subject: &market_squawk_domain::SourceIdentifier,
) -> [u8; 32] {
    let mut digest = Sha256::new();
    digest.update(b"market-squawk/provider-authorization-subject/v1\0");
    digest.update(mode_id.to_be_bytes());
    digest.update(algorithm_id.to_be_bytes());
    digest.update(evidence);
    digest.update((subject.as_str().len() as u64).to_be_bytes());
    digest.update(subject.as_str().as_bytes());
    digest.finalize().into()
}

fn state_digest(
    group_id: [u8; 16],
    policy_digest: [u8; 32],
    version: i64,
    state_json: &[u8],
) -> [u8; 32] {
    let mut digest = Sha256::new();
    digest.update(b"market-squawk/provider-rate-state/v1\0");
    digest.update(group_id);
    digest.update(policy_digest);
    digest.update(version.to_be_bytes());
    digest.update(state_json);
    digest.finalize().into()
}

fn sha256_bytes(digest: EvidenceDigest) -> Result<[u8; 32], ProviderRateStoreError> {
    if digest.algorithm() != DigestAlgorithm::Sha256 {
        return Err(ProviderRateStoreError::Corrupt);
    }
    Ok(digest.bytes())
}

fn fixed_bytes<const N: usize>(value: Vec<u8>) -> Result<[u8; N], ProviderRateStoreError> {
    value
        .try_into()
        .map_err(|_| ProviderRateStoreError::Corrupt)
}

fn checked_timestamp_add(
    now: Timestamp,
    delay_nanos: u64,
) -> Result<Timestamp, ProviderRateStoreError> {
    let delay = i64::try_from(delay_nanos).map_err(|_| ProviderRateStoreError::Clock)?;
    now.unix_nanos()
        .checked_add(delay)
        .map(Timestamp::from_unix_nanos)
        .ok_or(ProviderRateStoreError::Clock)
}

fn map_sql(_error: rusqlite::Error) -> ProviderRateStoreError {
    ProviderRateStoreError::Unavailable
}

#[cfg(test)]
mod tests {
    use std::num::{NonZeroU16, NonZeroU32, NonZeroU64};

    use market_squawk_sources::{
        BackoffPolicy, BudgetScope, EndpointPolicy, ProviderBudgetPolicy, ProviderRateDecision,
    };
    use sha2::{Digest as _, Sha256};

    use super::*;

    #[test]
    fn logical_checkpoint_restores_durable_budget_without_process_run_or_permit_state()
    -> Result<(), Box<dyn std::error::Error>> {
        let source_root = tempfile::tempdir()?;
        let source_path = source_root.path().join("provider-rate.sqlite3");
        let source = SqliteProviderRateStore::try_open(&source_path)?;
        let now = Timestamp::from_unix_nanos(1_000_000_000);
        let run_id = source.start_run(now)?;
        let policy = ProviderBudgetPolicy::try_new(
            BudgetScope::new(market_squawk_domain::SourceIdentifier::try_from(
                "checkpoint-test",
            )?),
            NonZeroU32::new(1).ok_or("nonzero request limit")?,
            NonZeroU64::new(60_000_000_000).ok_or("nonzero window")?,
            NonZeroU16::new(1).ok_or("nonzero concurrency")?,
            BackoffPolicy::try_new(
                NonZeroU64::new(1_000_000).ok_or("nonzero backoff")?,
                NonZeroU64::new(60_000_000_000).ok_or("nonzero backoff maximum")?,
                0,
            )?,
        )?;
        let declaration = ProviderRateDeclaration::try_for_endpoint(
            policy,
            &EndpointPolicy::try_new(["https://provider-rate.test/"])?,
        )?;
        let registration = source.register(run_id, &declaration, now)?;
        assert!(matches!(
            source.try_acquire(run_id, registration, now)?,
            ProviderRateDecision::Ready(_)
        ));

        let retained = source.retain_logical_checkpoint()?;
        let checkpoint = retained.bytes().to_vec();
        let authority_revision_sha256 = retained.authority_revision_sha256();
        retained.revalidate_emitted(
            u64::try_from(checkpoint.len())?,
            Sha256::digest(&checkpoint).into(),
        )?;
        drop(source);

        let restore_root = tempfile::tempdir()?;
        let restored = SqliteProviderRateStore::restore_logical_fresh(
            restore_root.path().join("provider-rate.sqlite3"),
            &checkpoint,
            authority_revision_sha256,
        )?;
        let connection = restored.connection()?;
        let active_runs: i64 = connection.query_row(
            "SELECT COUNT(*) FROM provider_rate_runs WHERE status = 'active'",
            [],
            |row| row.get(0),
        )?;
        assert_eq!(active_runs, 0);
        drop(connection);

        let restored_run = restored.start_run(now)?;
        let restored_registration = restored.register(restored_run, &declaration, now)?;
        assert!(matches!(
            restored.try_acquire(restored_run, restored_registration, now)?,
            ProviderRateDecision::WaitUntil(_)
        ));
        Ok(())
    }
}
