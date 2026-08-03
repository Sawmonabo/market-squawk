//! Capability-confined, append-only SQLite persistence for investment decisions.

use std::{
    fmt,
    fs::{File, OpenOptions},
    io::Cursor,
    sync::Arc,
    time::Duration,
};

use market_squawk_decisions::{
    AppendOutcome, DecisionAuthority, DecisionRepository, DecisionRepositoryLimits,
};
use market_squawk_platform::{
    DecisionDatabaseFileGuard, DecisionDatabaseLocation, DecisionDatabaseWriterGuard,
};
use rusqlite::{
    Connection, MAIN_DB, OpenFlags, OptionalExtension as _, Transaction, TransactionBehavior,
    params,
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
const MAX_BACKUP_BYTES: usize = (SQLITE_PAGE_SIZE as usize) * (SQLITE_MAX_PAGE_COUNT as usize);
const BACKUP_PAGE_BATCH: i32 = 128;
const BACKUP_PAGE_PAUSE: Duration = Duration::from_millis(10);
const EXPECTED_SCHEMA_SQL: &str = "CREATE TABLE decision_records (
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
                 ) STRICT";

/// Immutable SQLite image and semantic identity produced by the live journal owner.
pub(in crate::application::decision) struct DecisionJournalBackup {
    bytes: Arc<[u8]>,
    semantic_sha256: [u8; 32],
    content_sha256: [u8; 32],
}

impl DecisionJournalBackup {
    pub(in crate::application::decision) fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    pub(in crate::application::decision) const fn semantic_sha256(&self) -> [u8; 32] {
        self.semantic_sha256
    }

    pub(in crate::application::decision) const fn content_sha256(&self) -> [u8; 32] {
        self.content_sha256
    }
}

impl fmt::Debug for DecisionJournalBackup {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DecisionJournalBackup")
            .field("byte_length", &self.bytes.len())
            .field("semantic_sha256", &"[SHA-256]")
            .field("content_sha256", &"[SHA-256]")
            .finish()
    }
}

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
        verify_schema(&connection)?;
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
    ) -> Result<[u8; 32], DecisionApplicationError> {
        self.validate_capabilities()?;
        verify_integrity(&self.connection)?;
        verify_schema(&self.connection)?;
        let semantic = visit_records(
            &self.connection,
            |_sequence, kind, key, payload, _digest| context.apply(authority, kind, key, payload),
        )?;
        self.validate_capabilities()?;
        Ok(semantic)
    }

    /// Creates one full, transactionally consistent SQLite image through SQLite's online-backup
    /// API. The live database, WAL, and shared-memory files are never copied.
    pub(in crate::application::decision) fn online_backup(
        &self,
    ) -> Result<DecisionJournalBackup, DecisionApplicationError> {
        self.validate_capabilities()?;
        verify_integrity(&self.connection)?;
        verify_schema(&self.connection)?;
        let source_semantic = semantic_digest(&self.connection)?;
        let mut destination =
            Connection::open_in_memory().map_err(|_error| DecisionApplicationError::Persistence)?;
        let backup = rusqlite::backup::Backup::new(&self.connection, &mut destination)
            .map_err(|_error| DecisionApplicationError::Persistence)?;
        backup
            .run_to_completion(BACKUP_PAGE_BATCH, BACKUP_PAGE_PAUSE, None)
            .map_err(|_error| DecisionApplicationError::Persistence)?;
        drop(backup);
        disable_trusted_schema(&destination)?;
        verify_integrity(&destination)?;
        verify_schema(&destination)?;
        let destination_semantic = semantic_digest(&destination)?;
        if destination_semantic != source_semantic {
            return Err(DecisionApplicationError::InvalidPersistentState);
        }
        let serialized = destination
            .serialize(MAIN_DB)
            .map_err(|_error| DecisionApplicationError::Persistence)?;
        if serialized.is_empty() || serialized.len() > MAX_BACKUP_BYTES {
            return Err(DecisionApplicationError::InvalidPersistentState);
        }
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(serialized.len())
            .map_err(|_error| DecisionApplicationError::Allocation)?;
        bytes.extend_from_slice(&serialized);
        let content_sha256 = sha256(&bytes);
        Ok(DecisionJournalBackup {
            bytes: bytes.into(),
            semantic_sha256: source_semantic,
            content_sha256,
        })
    }

    /// Installs a verified owner-issued image only at a previously unused database location.
    /// The image is decoded as SQLite and copied through the online-backup API before normal
    /// [`super::DecisionApplication::open`] recovery is allowed to acquire the writer lease.
    pub(in crate::application::decision) fn restore_fresh(
        location: &DecisionDatabaseLocation,
        limits: DecisionRepositoryLimits,
        bytes: &[u8],
    ) -> Result<[u8; 32], DecisionApplicationError> {
        if bytes.is_empty() || bytes.len() > MAX_BACKUP_BYTES {
            return Err(DecisionApplicationError::InvalidPersistentState);
        }
        let mut source =
            Connection::open_in_memory().map_err(|_error| DecisionApplicationError::Persistence)?;
        source
            .deserialize_read_exact(MAIN_DB, Cursor::new(bytes), bytes.len(), true)
            .map_err(|_error| DecisionApplicationError::InvalidPersistentState)?;
        disable_trusted_schema(&source)?;
        verify_integrity(&source)?;
        verify_schema(&source)?;
        let repository = DecisionRepository::try_new(limits)?;
        let mut authority = DecisionAuthority::new(repository);
        let mut recovery = RecoveryContext::try_new()?;
        let source_semantic =
            visit_records(&source, |_sequence, kind, key, payload, _payload_sha256| {
                recovery.apply(&mut authority, kind, key, payload)
            })?;
        let writer_guard = location
            .acquire_writer()
            .map_err(|_error| DecisionApplicationError::Persistence)?;
        location
            .validate_for_open()
            .map_err(|_error| DecisionApplicationError::Persistence)?;
        let reserved_file = reserve_fresh_database(location)?;
        let flags = OpenFlags::SQLITE_OPEN_READ_WRITE
            | OpenFlags::SQLITE_OPEN_NO_MUTEX
            | OpenFlags::SQLITE_OPEN_NOFOLLOW;
        let mut destination = Connection::open_with_flags(location.path(), flags)
            .map_err(|_error| DecisionApplicationError::Persistence)?;
        let database_file = location
            .open_database_file()
            .map_err(|_error| DecisionApplicationError::Persistence)?;
        database_file
            .validate_identity()
            .map_err(|_error| DecisionApplicationError::Persistence)?;
        let opened_file = database_file
            .try_clone_file()
            .map_err(|_error| DecisionApplicationError::Persistence)?;
        if !same_file_identity(&reserved_file, &opened_file)? {
            return Err(DecisionApplicationError::Persistence);
        }
        let backup = rusqlite::backup::Backup::new(&source, &mut destination)
            .map_err(|_error| DecisionApplicationError::Persistence)?;
        backup
            .run_to_completion(BACKUP_PAGE_BATCH, BACKUP_PAGE_PAUSE, None)
            .map_err(|_error| DecisionApplicationError::Persistence)?;
        drop(backup);
        disable_trusted_schema(&destination)?;
        verify_integrity(&destination)?;
        verify_schema(&destination)?;
        if semantic_digest(&destination)? != source_semantic {
            return Err(DecisionApplicationError::InvalidPersistentState);
        }
        destination
            .close()
            .map_err(|(_connection, _error)| DecisionApplicationError::Persistence)?;
        reserved_file
            .sync_all()
            .map_err(|_error| DecisionApplicationError::Persistence)?;
        database_file
            .validate_identity()
            .map_err(|_error| DecisionApplicationError::Persistence)?;
        location
            .validate_sqlite_sidecars()
            .map_err(|_error| DecisionApplicationError::Persistence)?;
        if !same_file_identity(&reserved_file, &opened_file)? {
            return Err(DecisionApplicationError::Persistence);
        }
        drop(opened_file);
        drop(reserved_file);
        drop(database_file);
        drop(writer_guard);
        Ok(source_semantic)
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

fn reserve_fresh_database(
    location: &DecisionDatabaseLocation,
) -> Result<File, DecisionApplicationError> {
    let mut options = OpenOptions::new();
    options.read(true).write(true).create_new(true);
    configure_private_creation(&mut options);
    let file = options
        .open(location.path())
        .map_err(|_error| DecisionApplicationError::Persistence)?;
    location
        .validate_for_open()
        .map_err(|_error| DecisionApplicationError::Persistence)?;
    Ok(file)
}

fn same_file_identity(left: &File, right: &File) -> Result<bool, DecisionApplicationError> {
    use cap_fs_ext::MetadataExt as _;

    let left = cap_std::fs::File::from_std(
        left.try_clone()
            .map_err(|_error| DecisionApplicationError::Persistence)?,
    )
    .metadata()
    .map_err(|_error| DecisionApplicationError::Persistence)?;
    let right = cap_std::fs::File::from_std(
        right
            .try_clone()
            .map_err(|_error| DecisionApplicationError::Persistence)?,
    )
    .metadata()
    .map_err(|_error| DecisionApplicationError::Persistence)?;
    Ok(left.is_file() && right.is_file() && left.dev() == right.dev() && left.ino() == right.ino())
}

#[cfg(unix)]
fn configure_private_creation(options: &mut OpenOptions) {
    use std::os::unix::fs::OpenOptionsExt as _;

    options.mode(0o600);
}

#[cfg(not(unix))]
fn configure_private_creation(_options: &mut OpenOptions) {}

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
    disable_trusted_schema(connection)?;
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

fn verify_schema(connection: &Connection) -> Result<(), DecisionApplicationError> {
    let version = connection
        .query_row("PRAGMA user_version", [], |row| row.get::<_, i64>(0))
        .map_err(|_error| DecisionApplicationError::InvalidPersistentState)?;
    if version != SCHEMA_VERSION {
        return Err(DecisionApplicationError::InvalidPersistentState);
    }
    let page_size = connection
        .query_row("PRAGMA page_size", [], |row| row.get::<_, i64>(0))
        .map_err(|_error| DecisionApplicationError::InvalidPersistentState)?;
    if page_size != SQLITE_PAGE_SIZE {
        return Err(DecisionApplicationError::InvalidPersistentState);
    }
    let mut statement = connection
        .prepare(
            "SELECT type, name, tbl_name, sql
             FROM sqlite_schema
             WHERE name NOT LIKE 'sqlite_%'
             ORDER BY type, name LIMIT 2",
        )
        .map_err(|_error| DecisionApplicationError::InvalidPersistentState)?;
    let mut rows = statement
        .query([])
        .map_err(|_error| DecisionApplicationError::InvalidPersistentState)?;
    let row = rows
        .next()
        .map_err(|_error| DecisionApplicationError::InvalidPersistentState)?
        .ok_or(DecisionApplicationError::InvalidPersistentState)?;
    let object_type = row
        .get::<_, String>(0)
        .map_err(|_error| DecisionApplicationError::InvalidPersistentState)?;
    let name = row
        .get::<_, String>(1)
        .map_err(|_error| DecisionApplicationError::InvalidPersistentState)?;
    let table_name = row
        .get::<_, String>(2)
        .map_err(|_error| DecisionApplicationError::InvalidPersistentState)?;
    let sql = row
        .get::<_, String>(3)
        .map_err(|_error| DecisionApplicationError::InvalidPersistentState)?;
    if object_type != "table"
        || name != "decision_records"
        || table_name != "decision_records"
        || !sql
            .split_whitespace()
            .eq(EXPECTED_SCHEMA_SQL.split_whitespace())
        || rows
            .next()
            .map_err(|_error| DecisionApplicationError::InvalidPersistentState)?
            .is_some()
    {
        return Err(DecisionApplicationError::InvalidPersistentState);
    }
    Ok(())
}

fn disable_trusted_schema(connection: &Connection) -> Result<(), DecisionApplicationError> {
    connection
        .pragma_update(None, "trusted_schema", "OFF")
        .map_err(|_error| DecisionApplicationError::Persistence)
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

fn semantic_digest(connection: &Connection) -> Result<[u8; 32], DecisionApplicationError> {
    visit_records(connection, |_sequence, _kind, _key, _payload, _digest| {
        Ok(())
    })
}

fn visit_records(
    connection: &Connection,
    mut visit: impl FnMut(i64, i64, &str, &[u8], &[u8]) -> Result<(), DecisionApplicationError>,
) -> Result<[u8; 32], DecisionApplicationError> {
    let limit = i64::try_from(MAX_RECORDS + 1)
        .map_err(|_error| DecisionApplicationError::InvalidPersistentState)?;
    let mut statement = connection
        .prepare(
            "SELECT sequence, kind, record_key, payload_json, payload_sha256
             FROM decision_records ORDER BY sequence ASC LIMIT ?1",
        )
        .map_err(|_error| DecisionApplicationError::InvalidPersistentState)?;
    let mut rows = statement
        .query([limit])
        .map_err(|_error| DecisionApplicationError::InvalidPersistentState)?;
    let mut expected_sequence = 1_i64;
    let mut count = 0_usize;
    let mut total_bytes = 0_usize;
    let mut semantic = semantic_digest_start();
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
            || sha256(&payload).as_slice() != digest.as_slice()
        {
            return Err(DecisionApplicationError::InvalidPersistentState);
        }
        expected_sequence = expected_sequence
            .checked_add(1)
            .ok_or(DecisionApplicationError::InvalidPersistentState)?;
        total_bytes = total_bytes
            .checked_add(payload.len())
            .ok_or(DecisionApplicationError::InvalidPersistentState)?;
        if total_bytes > MAX_JOURNAL_BYTES {
            return Err(DecisionApplicationError::InvalidPersistentState);
        }
        visit(sequence, kind, &key, &payload, &digest)?;
        update_semantic_digest(&mut semantic, sequence, kind, &key, &payload, &digest)?;
    }
    Ok(semantic.finalize().into())
}

fn semantic_digest_start() -> Sha256 {
    let mut digest = Sha256::new();
    digest.update(b"market-squawk/decision-journal-authority/v1\0");
    digest.update(SCHEMA_VERSION.to_be_bytes());
    digest
}

fn update_semantic_digest(
    digest: &mut Sha256,
    sequence: i64,
    kind: i64,
    key: &str,
    payload: &[u8],
    payload_sha256: &[u8],
) -> Result<(), DecisionApplicationError> {
    let key_length = u64::try_from(key.len())
        .map_err(|_error| DecisionApplicationError::InvalidPersistentState)?;
    let payload_length = u64::try_from(payload.len())
        .map_err(|_error| DecisionApplicationError::InvalidPersistentState)?;
    digest.update(sequence.to_be_bytes());
    digest.update(kind.to_be_bytes());
    digest.update(key_length.to_be_bytes());
    digest.update(key.as_bytes());
    digest.update(payload_length.to_be_bytes());
    digest.update(payload);
    digest.update(payload_sha256);
    Ok(())
}
