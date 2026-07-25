//! SQLite-backed aggregate provider request and connection admission.

use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;

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
        Ok(Self { path })
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
    let row: Option<(Vec<u8>, Vec<u8>, Vec<u8>, Vec<u8>, i64)> = transaction
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
