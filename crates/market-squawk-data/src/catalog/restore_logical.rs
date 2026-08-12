//! Canonical logical-catalog proof for exact backup-restore descendants.

use rusqlite::limits::Limit;
use rusqlite::types::ValueRef;
use rusqlite::{Connection, Transaction};
use sha2::{Digest as _, Sha256};
use tokio_util::sync::CancellationToken;

use super::backup::{VerifiedBackupCatalog, open_immutable_backup};
use super::storage::{verify_integrity, verify_migration_identities};
use super::types::MAX_SQLITE_RECORD_BYTES;
use super::{Catalog, CatalogError, map_catalog_location_error};

const AUTHORITY_EVENTS_TABLE: &str = "analytical_artifact_root_authority_events";
const MAX_RESTORE_SCHEMA_OBJECTS: usize = 512;
const MAX_RESTORE_TABLES: usize = 128;
const MAX_RESTORE_COLUMNS_PER_TABLE: usize = 64;
const MAX_RESTORE_IDENTIFIER_BYTES: usize = 255;
const MAX_RESTORE_SCHEMA_SQL_BYTES: usize = 64 * 1024;
const MAX_RESTORE_QUERY_BYTES: usize = 256 * 1024;
const MAX_RESTORE_SIDECAR_BYTES: u64 = 64 * 1024 * 1024;
const MAX_RESTORE_CATALOG_BYTES: u64 = 16 * 1024 * 1024 * 1024;

/// Exact non-authority logical state retained from the immutable source backup.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct RestoreCatalogBaseline {
    header: [u8; 32],
    rows: [u8; 32],
}

struct RestoreSchemaState {
    digest: [u8; 32],
    tables: Vec<String>,
}

impl Catalog {
    pub(crate) fn verified_restore_baseline(
        backup: &VerifiedBackupCatalog,
        cancellation: &CancellationToken,
    ) -> Result<RestoreCatalogBaseline, CatalogError> {
        if backup.receipt().byte_length() > MAX_RESTORE_CATALOG_BYTES {
            return Err(CatalogError::BackupRestoreConflict);
        }
        backup.revalidate()?;
        let connection = open_immutable_backup(backup.location().path())?;
        let sqlite_length_limit = i32::try_from(MAX_SQLITE_RECORD_BYTES)
            .map_err(|_| CatalogError::InvalidConfiguration)?;
        connection.set_limit(Limit::SQLITE_LIMIT_LENGTH, sqlite_length_limit)?;
        connection.pragma_update(None, "trusted_schema", "OFF")?;
        connection.pragma_update(None, "query_only", "ON")?;
        verify_migration_identities(&connection)?;
        verify_integrity(&connection)?;
        backup.revalidate()?;

        let transaction = connection.unchecked_transaction()?;
        let baseline = capture_baseline(&transaction, cancellation)?;
        transaction.commit()?;
        verify_migration_identities(&connection)?;
        verify_integrity(&connection)?;
        connection.close().map_err(|(_, error)| error)?;
        backup.revalidate()?;
        Ok(baseline)
    }

    pub(super) fn verify_restore_baseline(
        &self,
        expected: RestoreCatalogBaseline,
        cancellation: &CancellationToken,
    ) -> Result<(), CatalogError> {
        self._catalog_file
            .validate_identity()
            .map_err(map_catalog_location_error)?;
        let transaction = self.connection.unchecked_transaction()?;
        let snapshot_boundary = self
            ._catalog_file
            .retain_restore_scan_state(MAX_RESTORE_CATALOG_BYTES, MAX_RESTORE_SIDECAR_BYTES)
            .map_err(map_catalog_location_error)?;
        transaction.query_row("SELECT rootpage FROM sqlite_schema LIMIT 1", [], |_| Ok(()))?;
        snapshot_boundary
            .revalidate_durable()
            .map_err(map_catalog_location_error)?;
        let scan_boundary = self
            ._catalog_file
            .retain_restore_scan_state(MAX_RESTORE_CATALOG_BYTES, MAX_RESTORE_SIDECAR_BYTES)
            .map_err(map_catalog_location_error)?;
        // Native table scans are canonical here because the target began as the exact retained
        // physical backup copy and the only admitted descendant writes are to the excluded
        // append-only authority table under unchanged schema.
        verify_baseline(&transaction, expected, cancellation)?;
        scan_boundary
            .revalidate()
            .map_err(map_catalog_location_error)?;
        snapshot_boundary
            .revalidate_durable()
            .map_err(map_catalog_location_error)?;
        transaction.commit()?;
        self._catalog_file
            .validate_identity()
            .map_err(map_catalog_location_error)?;
        Ok(())
    }
}

fn capture_baseline(
    transaction: &Transaction<'_>,
    cancellation: &CancellationToken,
) -> Result<RestoreCatalogBaseline, CatalogError> {
    let schema = schema_state(transaction, cancellation)?;
    let header = header_digest(transaction, schema.digest)?;
    let rows = row_digest(transaction, &schema.tables, cancellation)?;
    Ok(RestoreCatalogBaseline { header, rows })
}

fn verify_baseline(
    transaction: &Transaction<'_>,
    expected: RestoreCatalogBaseline,
    cancellation: &CancellationToken,
) -> Result<(), CatalogError> {
    let schema = schema_state(transaction, cancellation)?;
    if header_digest(transaction, schema.digest)? != expected.header {
        return Err(CatalogError::BackupRestoreConflict);
    }
    if row_digest(transaction, &schema.tables, cancellation)? != expected.rows {
        return Err(CatalogError::BackupRestoreConflict);
    }
    Ok(())
}

fn schema_state(
    connection: &Connection,
    cancellation: &CancellationToken,
) -> Result<RestoreSchemaState, CatalogError> {
    let limit = i64::try_from(MAX_RESTORE_SCHEMA_OBJECTS)
        .map_err(|_| CatalogError::BackupRestoreConflict)?
        .checked_add(1)
        .ok_or(CatalogError::BackupRestoreConflict)?;
    let mut statement = connection.prepare(
        "SELECT type, name, tbl_name, sql FROM sqlite_schema \
         ORDER BY type COLLATE BINARY, name COLLATE BINARY, tbl_name COLLATE BINARY, \
                  COALESCE(sql, '') COLLATE BINARY LIMIT ?1",
    )?;
    let mut rows = statement.query([limit])?;
    let mut digest = Sha256::new();
    digest.update(b"market-squawk/restore-catalog-schema/v1");
    let mut tables = Vec::new();
    let mut count = 0_usize;
    while let Some(row) = rows.next()? {
        check_cancellation(cancellation)?;
        count = count
            .checked_add(1)
            .ok_or(CatalogError::BackupRestoreConflict)?;
        if count > MAX_RESTORE_SCHEMA_OBJECTS {
            return Err(CatalogError::BackupRestoreConflict);
        }
        let kind = bounded_text(row.get_ref(0)?, MAX_RESTORE_IDENTIFIER_BYTES)?;
        let name = bounded_text(row.get_ref(1)?, MAX_RESTORE_IDENTIFIER_BYTES)?;
        let table = bounded_text(row.get_ref(2)?, MAX_RESTORE_IDENTIFIER_BYTES)?;
        let sql = optional_bounded_text(row.get_ref(3)?, MAX_RESTORE_SCHEMA_SQL_BYTES)?;
        update_bytes(&mut digest, kind.as_bytes())?;
        update_bytes(&mut digest, name.as_bytes())?;
        update_bytes(&mut digest, table.as_bytes())?;
        update_optional_bytes(&mut digest, sql.map(str::as_bytes))?;
        if kind == "table" {
            if tables.len() >= MAX_RESTORE_TABLES {
                return Err(CatalogError::BackupRestoreConflict);
            }
            tables
                .try_reserve_exact(1)
                .map_err(|_| CatalogError::Allocation)?;
            tables.push(name.to_owned());
        }
    }
    digest.update(
        u64::try_from(count)
            .map_err(|_| CatalogError::BackupRestoreConflict)?
            .to_be_bytes(),
    );
    Ok(RestoreSchemaState {
        digest: digest.finalize().into(),
        tables,
    })
}

fn header_digest(connection: &Connection, schema: [u8; 32]) -> Result<[u8; 32], CatalogError> {
    let mut digest = Sha256::new();
    digest.update(b"market-squawk/restore-catalog-header/v1");
    digest.update(schema);
    for pragma in [
        "application_id",
        "user_version",
        "schema_version",
        "page_size",
        "auto_vacuum",
    ] {
        update_bytes(&mut digest, pragma.as_bytes())?;
        let query = format!("PRAGMA {pragma}");
        let value: i64 = connection.query_row(&query, [], |row| row.get(0))?;
        digest.update(value.to_be_bytes());
    }
    let encoding: String = connection.query_row("PRAGMA encoding", [], |row| row.get(0))?;
    update_bytes(&mut digest, b"encoding")?;
    update_bytes(&mut digest, encoding.as_bytes())?;
    Ok(digest.finalize().into())
}

fn row_digest(
    connection: &Connection,
    tables: &[String],
    cancellation: &CancellationToken,
) -> Result<[u8; 32], CatalogError> {
    let mut digest = Sha256::new();
    digest.update(b"market-squawk/restore-catalog-rows/v1");
    for table in tables {
        check_cancellation(cancellation)?;
        update_bytes(&mut digest, table.as_bytes())?;
        if table == AUTHORITY_EVENTS_TABLE {
            digest.update(0_u64.to_be_bytes());
            continue;
        }
        let columns = table_columns(connection, table)?;
        let query = table_scan_query(table, &columns)?;
        let mut statement = connection.prepare(&query)?;
        let mut rows = statement.query([])?;
        let mut row_count = 0_u64;
        while let Some(row) = rows.next()? {
            check_cancellation(cancellation)?;
            row_count = row_count
                .checked_add(1)
                .ok_or(CatalogError::BackupRestoreConflict)?;
            for index in 0..columns.len() {
                update_value(&mut digest, row.get_ref(index)?)?;
            }
        }
        digest.update(row_count.to_be_bytes());
    }
    digest.update(
        u64::try_from(tables.len())
            .map_err(|_| CatalogError::BackupRestoreConflict)?
            .to_be_bytes(),
    );
    Ok(digest.finalize().into())
}

fn table_columns(connection: &Connection, table: &str) -> Result<Vec<String>, CatalogError> {
    let limit = i64::try_from(MAX_RESTORE_COLUMNS_PER_TABLE)
        .map_err(|_| CatalogError::BackupRestoreConflict)?
        .checked_add(1)
        .ok_or(CatalogError::BackupRestoreConflict)?;
    let mut statement =
        connection.prepare("SELECT name FROM pragma_table_xinfo(?1) ORDER BY cid LIMIT ?2")?;
    let mut rows = statement.query((table, limit))?;
    let mut columns = Vec::new();
    while let Some(row) = rows.next()? {
        if columns.len() >= MAX_RESTORE_COLUMNS_PER_TABLE {
            return Err(CatalogError::BackupRestoreConflict);
        }
        let column = bounded_text(row.get_ref(0)?, MAX_RESTORE_IDENTIFIER_BYTES)?;
        columns
            .try_reserve_exact(1)
            .map_err(|_| CatalogError::Allocation)?;
        columns.push(column.to_owned());
    }
    if columns.is_empty() {
        return Err(CatalogError::BackupRestoreConflict);
    }
    Ok(columns)
}

fn table_scan_query(table: &str, columns: &[String]) -> Result<String, CatalogError> {
    let mut query = String::new();
    let identifier_bytes = columns.iter().try_fold(table.len(), |total, column| {
        total
            .checked_add(column.len())
            .ok_or(CatalogError::BackupRestoreConflict)
    })?;
    let separators = columns
        .len()
        .checked_mul(3)
        .ok_or(CatalogError::BackupRestoreConflict)?;
    let capacity = identifier_bytes
        .checked_mul(2)
        .and_then(|bytes| bytes.checked_add(separators))
        .and_then(|bytes| bytes.checked_add(32))
        .ok_or(CatalogError::BackupRestoreConflict)?;
    if capacity > MAX_RESTORE_QUERY_BYTES {
        return Err(CatalogError::BackupRestoreConflict);
    }
    query
        .try_reserve_exact(capacity)
        .map_err(|_| CatalogError::Allocation)?;
    query.push_str("SELECT ");
    for (index, column) in columns.iter().enumerate() {
        if index != 0 {
            query.push(',');
        }
        push_quoted_identifier(&mut query, column)?;
    }
    query.push_str(" FROM ");
    push_quoted_identifier(&mut query, table)?;
    query.push_str(" NOT INDEXED");
    if query.len() > MAX_RESTORE_QUERY_BYTES {
        return Err(CatalogError::BackupRestoreConflict);
    }
    Ok(query)
}

fn push_quoted_identifier(query: &mut String, identifier: &str) -> Result<(), CatalogError> {
    require_bounded_identifier(identifier)?;
    query.push('"');
    for character in identifier.chars() {
        if character == '"' {
            query.push('"');
        }
        query.push(character);
    }
    query.push('"');
    if query.len() > MAX_RESTORE_QUERY_BYTES {
        return Err(CatalogError::BackupRestoreConflict);
    }
    Ok(())
}

fn update_value(digest: &mut Sha256, value: ValueRef<'_>) -> Result<(), CatalogError> {
    match value {
        ValueRef::Null => digest.update([0]),
        ValueRef::Integer(value) => {
            digest.update([1]);
            digest.update(value.to_be_bytes());
        }
        ValueRef::Real(value) => {
            digest.update([2]);
            let normalized = if value == 0.0 { 0.0 } else { value };
            digest.update(normalized.to_bits().to_be_bytes());
        }
        ValueRef::Text(value) => {
            digest.update([3]);
            update_bytes(digest, value)?;
        }
        ValueRef::Blob(value) => {
            digest.update([4]);
            update_bytes(digest, value)?;
        }
    }
    Ok(())
}

fn update_optional_bytes(digest: &mut Sha256, value: Option<&[u8]>) -> Result<(), CatalogError> {
    match value {
        Some(value) => {
            digest.update([1]);
            update_bytes(digest, value)
        }
        None => {
            digest.update([0]);
            Ok(())
        }
    }
}

fn update_bytes(digest: &mut Sha256, value: &[u8]) -> Result<(), CatalogError> {
    digest.update(
        u64::try_from(value.len())
            .map_err(|_| CatalogError::BackupRestoreConflict)?
            .to_be_bytes(),
    );
    digest.update(value);
    Ok(())
}

fn require_bounded_identifier(value: &str) -> Result<(), CatalogError> {
    if value.is_empty() || value.len() > MAX_RESTORE_IDENTIFIER_BYTES {
        Err(CatalogError::BackupRestoreConflict)
    } else {
        Ok(())
    }
}

fn bounded_text(value: ValueRef<'_>, maximum_bytes: usize) -> Result<&str, CatalogError> {
    let ValueRef::Text(bytes) = value else {
        return Err(CatalogError::BackupRestoreConflict);
    };
    if bytes.is_empty() || bytes.len() > maximum_bytes {
        return Err(CatalogError::BackupRestoreConflict);
    }
    std::str::from_utf8(bytes).map_err(|_| CatalogError::BackupRestoreConflict)
}

fn optional_bounded_text(
    value: ValueRef<'_>,
    maximum_bytes: usize,
) -> Result<Option<&str>, CatalogError> {
    match value {
        ValueRef::Null => Ok(None),
        ValueRef::Text(bytes) if bytes.len() <= maximum_bytes => std::str::from_utf8(bytes)
            .map(Some)
            .map_err(|_| CatalogError::BackupRestoreConflict),
        ValueRef::Integer(_) | ValueRef::Real(_) | ValueRef::Text(_) | ValueRef::Blob(_) => {
            Err(CatalogError::BackupRestoreConflict)
        }
    }
}

fn check_cancellation(cancellation: &CancellationToken) -> Result<(), CatalogError> {
    if cancellation.is_cancelled() {
        Err(CatalogError::AnalyticalEvidenceCancelled)
    } else {
        Ok(())
    }
}
