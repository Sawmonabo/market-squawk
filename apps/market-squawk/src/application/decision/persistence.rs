//! Capability-confined, append-only SQLite persistence for investment decisions.

use std::fmt;
use std::time::Duration;

use market_squawk_decisions::{AppendOutcome, DecisionAuthority};
use market_squawk_platform::{
    DecisionDatabaseFileGuard, DecisionDatabaseLocation, DecisionDatabaseWriterGuard,
};
use rusqlite::{
    Connection, OpenFlags, OptionalExtension as _, Transaction, TransactionBehavior, params,
};
use sha2::{Digest as _, Sha256};

use super::DecisionApplicationError;
use super::codec::{EncodedRecord, RecoveryContext};

const SCHEMA_VERSION: i64 = 1;
const BUSY_TIMEOUT: Duration = Duration::from_secs(5);
const SQLITE_PAGE_SIZE: i64 = 4_096;
const SQLITE_MAX_PAGE_COUNT: i64 = 131_072;
const MAX_RECORDS: usize = 65_536;
const MAX_RECORD_KEY_BYTES: usize = 260;
const MAX_RECORD_BYTES: usize = 16 * 1024 * 1024;
const MAX_JOURNAL_BYTES: usize = 256 * 1024 * 1024;

/// One retained SQLite connection and the capabilities proving exclusive durable authority.
pub(super) struct DecisionJournal {
    connection: Connection,
    location: DecisionDatabaseLocation,
    database_file: DecisionDatabaseFileGuard,
    _writer_guard: DecisionDatabaseWriterGuard,
}

impl fmt::Debug for DecisionJournal {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("DecisionJournal([CAPABILITY-CONFINED SQLITE WRITER])")
    }
}

impl DecisionJournal {
    pub(super) fn open(
        location: DecisionDatabaseLocation,
    ) -> Result<Self, DecisionApplicationError> {
        let database_file = location
            .prepare_database_file()
            .map_err(|_error| DecisionApplicationError::Persistence)?;
        let writer_guard = location
            .acquire_writer()
            .map_err(|_error| DecisionApplicationError::Persistence)?;
        location
            .validate_for_open()
            .map_err(|_error| DecisionApplicationError::Persistence)?;
        database_file
            .validate_identity()
            .map_err(|_error| DecisionApplicationError::Persistence)?;
        let flags = OpenFlags::SQLITE_OPEN_READ_WRITE
            | OpenFlags::SQLITE_OPEN_CREATE
            | OpenFlags::SQLITE_OPEN_NO_MUTEX;
        let connection = Connection::open_with_flags(location.path(), flags)
            .map_err(|_error| DecisionApplicationError::Persistence)?;
        location
            .validate_for_open()
            .map_err(|_error| DecisionApplicationError::Persistence)?;
        database_file
            .validate_identity()
            .map_err(|_error| DecisionApplicationError::Persistence)?;
        configure(&connection, &location)?;
        initialize(&connection)?;
        verify_integrity(&connection)?;
        location
            .validate_sqlite_sidecars()
            .map_err(|_error| DecisionApplicationError::Persistence)?;
        Ok(Self {
            connection,
            location,
            database_file,
            _writer_guard: writer_guard,
        })
    }

    pub(super) fn recover(
        &self,
        authority: &mut DecisionAuthority,
        context: &mut RecoveryContext,
    ) -> Result<(), DecisionApplicationError> {
        self.validate_capabilities()?;
        let limit = i64::try_from(MAX_RECORDS + 1)
            .map_err(|_error| DecisionApplicationError::InvalidPersistentState)?;
        let mut statement = self
            .connection
            .prepare(
                "SELECT sequence, kind, record_key, payload_json, payload_sha256
                 FROM decision_records ORDER BY sequence ASC LIMIT ?1",
            )
            .map_err(|_error| DecisionApplicationError::Persistence)?;
        let mut rows = statement
            .query([limit])
            .map_err(|_error| DecisionApplicationError::Persistence)?;
        let mut expected_sequence = 1_i64;
        let mut count = 0_usize;
        let mut total_bytes = 0_usize;
        while let Some(row) = rows
            .next()
            .map_err(|_error| DecisionApplicationError::InvalidPersistentState)?
        {
            count = count
                .checked_add(1)
                .ok_or(DecisionApplicationError::InvalidPersistentState)?;
            if count > MAX_RECORDS {
                return Err(DecisionApplicationError::InvalidPersistentState);
            }
            let sequence = row
                .get::<_, i64>(0)
                .map_err(|_error| DecisionApplicationError::InvalidPersistentState)?;
            let kind = row
                .get::<_, i64>(1)
                .map_err(|_error| DecisionApplicationError::InvalidPersistentState)?;
            let key = row
                .get::<_, String>(2)
                .map_err(|_error| DecisionApplicationError::InvalidPersistentState)?;
            let payload = row
                .get::<_, Vec<u8>>(3)
                .map_err(|_error| DecisionApplicationError::InvalidPersistentState)?;
            let digest = row
                .get::<_, Vec<u8>>(4)
                .map_err(|_error| DecisionApplicationError::InvalidPersistentState)?;
            if sequence != expected_sequence
                || key.is_empty()
                || key.len() > MAX_RECORD_KEY_BYTES
                || payload.is_empty()
                || payload.len() > MAX_RECORD_BYTES
                || digest.len() != 32
                || !valid_kind(kind)
            {
                return Err(DecisionApplicationError::InvalidPersistentState);
            }
            expected_sequence = expected_sequence
                .checked_add(1)
                .ok_or(DecisionApplicationError::InvalidPersistentState)?;
            total_bytes = total_bytes
                .checked_add(payload.len())
                .ok_or(DecisionApplicationError::InvalidPersistentState)?;
            if total_bytes > MAX_JOURNAL_BYTES || sha256(&payload).as_slice() != digest.as_slice() {
                return Err(DecisionApplicationError::InvalidPersistentState);
            }
            context.apply(authority, kind, &key, &payload)?;
        }
        drop(rows);
        drop(statement);
        self.validate_capabilities()
    }

    pub(super) fn append(
        &self,
        record: &EncodedRecord,
    ) -> Result<AppendOutcome, DecisionApplicationError> {
        if record.key.is_empty()
            || record.key.len() > MAX_RECORD_KEY_BYTES
            || record.payload.is_empty()
            || record.payload.len() > MAX_RECORD_BYTES
            || !valid_kind(record.kind)
            || sha256(&record.payload) != record.digest
        {
            return Err(DecisionApplicationError::InvalidPersistentState);
        }
        self.validate_capabilities()?;
        let transaction =
            Transaction::new_unchecked(&self.connection, TransactionBehavior::Immediate)
                .map_err(|_error| DecisionApplicationError::Persistence)?;
        let existing = transaction
            .query_row(
                "SELECT payload_json, payload_sha256 FROM decision_records
                 WHERE kind = ?1 AND record_key = ?2",
                params![record.kind, record.key],
                |row| Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, Vec<u8>>(1)?)),
            )
            .optional()
            .map_err(|_error| DecisionApplicationError::Persistence)?;
        if let Some((payload, digest)) = existing {
            if payload == record.payload && digest.as_slice() == record.digest.as_slice() {
                transaction
                    .commit()
                    .map_err(|_error| DecisionApplicationError::Persistence)?;
                self.validate_capabilities()?;
                return Ok(AppendOutcome::AlreadyPresent);
            }
            return Err(DecisionApplicationError::InvalidPersistentState);
        }
        let (count, total_bytes) = transaction
            .query_row(
                "SELECT COUNT(*), COALESCE(SUM(length(payload_json)), 0)
                 FROM decision_records",
                [],
                |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)),
            )
            .map_err(|_error| DecisionApplicationError::Persistence)?;
        let record_bytes = i64::try_from(record.payload.len())
            .map_err(|_error| DecisionApplicationError::InvalidPersistentState)?;
        let maximum_records = i64::try_from(MAX_RECORDS)
            .map_err(|_error| DecisionApplicationError::InvalidPersistentState)?;
        let maximum_bytes = i64::try_from(MAX_JOURNAL_BYTES)
            .map_err(|_error| DecisionApplicationError::InvalidPersistentState)?;
        if count < 0
            || total_bytes < 0
            || count >= maximum_records
            || total_bytes
                .checked_add(record_bytes)
                .is_none_or(|next| next > maximum_bytes)
        {
            return Err(DecisionApplicationError::Capacity);
        }
        let next_sequence = transaction
            .query_row(
                "SELECT COALESCE(MAX(sequence), 0) + 1 FROM decision_records",
                [],
                |row| row.get::<_, i64>(0),
            )
            .map_err(|_error| DecisionApplicationError::Persistence)?;
        let expected_next = count
            .checked_add(1)
            .ok_or(DecisionApplicationError::InvalidPersistentState)?;
        if next_sequence != expected_next || next_sequence <= 0 {
            return Err(DecisionApplicationError::InvalidPersistentState);
        }
        transaction
            .execute(
                "INSERT INTO decision_records
                 (sequence, kind, record_key, payload_json, payload_sha256)
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                params![
                    next_sequence,
                    record.kind,
                    record.key,
                    record.payload,
                    record.digest.as_slice()
                ],
            )
            .map_err(|_error| DecisionApplicationError::Persistence)?;
        transaction
            .commit()
            .map_err(|_error| DecisionApplicationError::Persistence)?;
        // Any failure after COMMIT is ambiguous and poisons the caller before it can acknowledge.
        self.validate_capabilities()?;
        Ok(AppendOutcome::Appended)
    }

    fn validate_capabilities(&self) -> Result<(), DecisionApplicationError> {
        self.location
            .validate_for_open()
            .map_err(|_error| DecisionApplicationError::Persistence)?;
        self.database_file
            .validate_identity()
            .map_err(|_error| DecisionApplicationError::Persistence)?;
        self.location
            .validate_sqlite_sidecars()
            .map_err(|_error| DecisionApplicationError::Persistence)
    }
}

fn configure(
    connection: &Connection,
    location: &DecisionDatabaseLocation,
) -> Result<(), DecisionApplicationError> {
    connection
        .busy_timeout(BUSY_TIMEOUT)
        .map_err(|_error| DecisionApplicationError::Persistence)?;
    connection
        .pragma_update(None, "foreign_keys", "ON")
        .map_err(|_error| DecisionApplicationError::Persistence)?;
    let page_size = connection
        .query_row("PRAGMA page_size", [], |row| row.get::<_, i64>(0))
        .map_err(|_error| DecisionApplicationError::Persistence)?;
    if page_size != SQLITE_PAGE_SIZE {
        return Err(DecisionApplicationError::InvalidPersistentState);
    }
    let mode = connection
        .query_row("PRAGMA journal_mode=WAL", [], |row| row.get::<_, String>(0))
        .map_err(|_error| DecisionApplicationError::Persistence)?;
    if !mode.eq_ignore_ascii_case("wal") {
        return Err(DecisionApplicationError::Persistence);
    }
    connection
        .pragma_update(None, "synchronous", "FULL")
        .map_err(|_error| DecisionApplicationError::Persistence)?;
    connection
        .pragma_update(None, "wal_autocheckpoint", 1_000_i64)
        .map_err(|_error| DecisionApplicationError::Persistence)?;
    let max_page_count = connection
        .query_row(
            &format!("PRAGMA max_page_count={SQLITE_MAX_PAGE_COUNT}"),
            [],
            |row| row.get::<_, i64>(0),
        )
        .map_err(|_error| DecisionApplicationError::Persistence)?;
    if max_page_count != SQLITE_MAX_PAGE_COUNT {
        return Err(DecisionApplicationError::Persistence);
    }
    location
        .validate_sqlite_sidecars()
        .map_err(|_error| DecisionApplicationError::Persistence)
}

fn initialize(connection: &Connection) -> Result<(), DecisionApplicationError> {
    let version = connection
        .query_row("PRAGMA user_version", [], |row| row.get::<_, i64>(0))
        .map_err(|_error| DecisionApplicationError::Persistence)?;
    if version == 0 {
        connection
            .execute_batch(
                "BEGIN IMMEDIATE;
                 CREATE TABLE decision_records (
                    sequence INTEGER PRIMARY KEY CHECK (sequence > 0),
                    kind INTEGER NOT NULL CHECK (kind BETWEEN 1 AND 6),
                    record_key TEXT NOT NULL CHECK (
                        length(record_key) > 0 AND length(CAST(record_key AS BLOB)) <= 260
                    ),
                    payload_json BLOB NOT NULL CHECK (
                        length(payload_json) > 0 AND length(payload_json) <= 16777216
                    ),
                    payload_sha256 BLOB NOT NULL CHECK (length(payload_sha256) = 32),
                    UNIQUE (kind, record_key)
                 ) STRICT;
                 PRAGMA user_version = 1;
                 COMMIT;",
            )
            .map_err(|_error| DecisionApplicationError::Persistence)?;
    } else if version != SCHEMA_VERSION {
        return Err(DecisionApplicationError::InvalidPersistentState);
    }
    Ok(())
}

fn verify_integrity(connection: &Connection) -> Result<(), DecisionApplicationError> {
    let result = connection
        .query_row("PRAGMA integrity_check(1)", [], |row| {
            row.get::<_, String>(0)
        })
        .map_err(|_error| DecisionApplicationError::InvalidPersistentState)?;
    if result == "ok" {
        Ok(())
    } else {
        Err(DecisionApplicationError::InvalidPersistentState)
    }
}

const fn valid_kind(kind: i64) -> bool {
    matches!(kind, 1..=6)
}

fn sha256(bytes: &[u8]) -> [u8; 32] {
    Sha256::digest(bytes).into()
}
