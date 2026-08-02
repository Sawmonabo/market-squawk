//! Exact fail-closed preflight for authority-changing catalog migrations.

use market_squawk_domain::{SourceId, Timestamp};
use rusqlite::{OptionalExtension as _, Transaction, params};

use super::storage::{parse_digest, verify_integrity};
use super::types::CatalogError;
use crate::rights::{RightsBasis, SourceRightsDecision};
use crate::{RightsDecisionInput, SourceOperation};

const LEGACY_RIGHTS_TABLE_SQL: &str = "CREATE TABLE source_rights (
    rights_id BLOB PRIMARY KEY CHECK (length(rights_id) = 32),
    source_id TEXT NOT NULL REFERENCES sources(source_id),
    payload_algorithm INTEGER NOT NULL CHECK (payload_algorithm IN (1, 2)),
    payload_digest BLOB NOT NULL CHECK (length(payload_digest) = 32),
    retrieved_at_ns INTEGER NOT NULL,
    terms_url TEXT NOT NULL CHECK (length(CAST(terms_url AS BLOB)) BETWEEN 1 AND 2048),
    terms_algorithm INTEGER NOT NULL CHECK (terms_algorithm IN (1, 2)),
    terms_digest BLOB NOT NULL CHECK (length(terms_digest) = 32),
    authorization_algorithm INTEGER NOT NULL CHECK (authorization_algorithm IN (1, 2)),
    authorization_digest BLOB NOT NULL CHECK (length(authorization_digest) = 32),
    authorization_expires_at_ns INTEGER,
    operation_mask INTEGER NOT NULL CHECK (operation_mask > 0 AND operation_mask <= 63),
    admitted_at_ns INTEGER NOT NULL,
    CHECK (retrieved_at_ns <= admitted_at_ns),
    CHECK (
        authorization_expires_at_ns IS NULL
        OR admitted_at_ns < authorization_expires_at_ns
    )
) STRICT";

const LEGACY_RIGHTS_UPDATE_TRIGGER_SQL: &str = "CREATE TRIGGER source_rights_immutable_update
BEFORE UPDATE ON source_rights BEGIN
    SELECT RAISE(ABORT, 'source rights are immutable');
END";

const LEGACY_RIGHTS_DELETE_TRIGGER_SQL: &str = "CREATE TRIGGER source_rights_immutable_delete
BEFORE DELETE ON source_rights BEGIN
    SELECT RAISE(ABORT, 'source rights are immutable');
END";

const LEGACY_RIGHTS_COLUMNS: [(&str, &str, bool, bool); 13] = [
    ("rights_id", "BLOB", true, true),
    ("source_id", "TEXT", true, false),
    ("payload_algorithm", "INTEGER", true, false),
    ("payload_digest", "BLOB", true, false),
    ("retrieved_at_ns", "INTEGER", true, false),
    ("terms_url", "TEXT", true, false),
    ("terms_algorithm", "INTEGER", true, false),
    ("terms_digest", "BLOB", true, false),
    ("authorization_algorithm", "INTEGER", true, false),
    ("authorization_digest", "BLOB", true, false),
    ("authorization_expires_at_ns", "INTEGER", false, false),
    ("operation_mask", "INTEGER", true, false),
    ("admitted_at_ns", "INTEGER", true, false),
];

pub(super) fn preflight_research_use_migration(
    transaction: &Transaction<'_>,
) -> Result<(), CatalogError> {
    verify_integrity(transaction)?;
    verify_exact_legacy_schema(transaction)?;
    verify_legacy_rights_rows(transaction)?;
    verify_ingest_rights_links(transaction)
}

fn verify_exact_legacy_schema(transaction: &Transaction<'_>) -> Result<(), CatalogError> {
    let table_sql: String = transaction.query_row(
        "SELECT sql FROM sqlite_schema WHERE type='table' AND name='source_rights'",
        [],
        |row| row.get(0),
    )?;
    if table_sql != LEGACY_RIGHTS_TABLE_SQL {
        return Err(CatalogError::CorruptCatalog);
    }
    for (name, expected) in [
        (
            "source_rights_immutable_update",
            LEGACY_RIGHTS_UPDATE_TRIGGER_SQL,
        ),
        (
            "source_rights_immutable_delete",
            LEGACY_RIGHTS_DELETE_TRIGGER_SQL,
        ),
    ] {
        let actual: String = transaction.query_row(
            "SELECT sql FROM sqlite_schema WHERE type='trigger' AND name=?1
             AND tbl_name='source_rights'",
            [name],
            |row| row.get(0),
        )?;
        if actual != expected {
            return Err(CatalogError::CorruptCatalog);
        }
    }
    let trigger_count: i64 = transaction.query_row(
        "SELECT COUNT(*) FROM sqlite_schema WHERE type='trigger' AND tbl_name='source_rights'",
        [],
        |row| row.get(0),
    )?;
    let index_shape: Option<(i64, String, i64)> = transaction
        .query_row("PRAGMA index_list(source_rights)", [], |row| {
            Ok((row.get(2)?, row.get(3)?, row.get(4)?))
        })
        .optional()?;
    let index_count: i64 = transaction.query_row(
        "SELECT COUNT(*) FROM pragma_index_list('source_rights')",
        [],
        |row| row.get(0),
    )?;
    if trigger_count != 2 || index_count != 1 || index_shape != Some((1, "pk".to_owned(), 0)) {
        return Err(CatalogError::CorruptCatalog);
    }
    let mut statement = transaction.prepare("PRAGMA table_info(source_rights)")?;
    let rows = statement.query_map([], |row| {
        Ok((
            row.get::<_, String>(1)?,
            row.get::<_, String>(2)?,
            row.get::<_, i64>(3)? != 0,
            row.get::<_, i64>(5)? != 0,
        ))
    })?;
    let mut observed = Vec::new();
    observed
        .try_reserve_exact(LEGACY_RIGHTS_COLUMNS.len())
        .map_err(|_| CatalogError::Allocation)?;
    for row in rows {
        observed.push(row?);
    }
    if observed.len() != LEGACY_RIGHTS_COLUMNS.len()
        || observed
            .iter()
            .zip(LEGACY_RIGHTS_COLUMNS)
            .any(|(observed, expected)| {
                observed.0 != expected.0
                    || observed.1 != expected.1
                    || observed.2 != expected.2
                    || observed.3 != expected.3
            })
    {
        return Err(CatalogError::CorruptCatalog);
    }
    Ok(())
}

fn verify_legacy_rights_rows(transaction: &Transaction<'_>) -> Result<(), CatalogError> {
    let mut statement = transaction.prepare(
        "SELECT rights_id, source_id, payload_algorithm, payload_digest, retrieved_at_ns,
                terms_url, terms_algorithm, terms_digest, authorization_algorithm,
                authorization_digest, authorization_expires_at_ns, operation_mask,
                admitted_at_ns
         FROM source_rights ORDER BY rights_id",
    )?;
    let mut rows = statement.query([])?;
    while let Some(row) = rows.next()? {
        let rights_id: Vec<u8> = row.get(0)?;
        let source_id: String = row.get(1)?;
        let payload_algorithm: i64 = row.get(2)?;
        let payload_digest: Vec<u8> = row.get(3)?;
        let retrieved_at_ns: i64 = row.get(4)?;
        let terms_url: String = row.get(5)?;
        let terms_algorithm: i64 = row.get(6)?;
        let terms_digest: Vec<u8> = row.get(7)?;
        let authorization_algorithm: i64 = row.get(8)?;
        let authorization_digest: Vec<u8> = row.get(9)?;
        let authorization_expires_at_ns: Option<i64> = row.get(10)?;
        let operation_mask: i64 = row.get(11)?;
        let admitted_at_ns: i64 = row.get(12)?;
        let operation_mask = u8::try_from(operation_mask)
            .ok()
            .filter(|mask| (1..=63).contains(mask))
            .ok_or(CatalogError::CorruptCatalog)?;
        let permitted_operations = all_source_operations()
            .into_iter()
            .filter(|operation| operation_mask & operation.mask() != 0)
            .collect();
        let decision = SourceRightsDecision::try_new_legacy(RightsDecisionInput {
            source_id: SourceId::try_from(source_id.as_str())
                .map_err(|_| CatalogError::CorruptCatalog)?,
            payload_digest: parse_digest(payload_algorithm, &payload_digest)?,
            retrieved_at: Timestamp::from_unix_nanos(retrieved_at_ns),
            basis: RightsBasis::reviewed_terms(
                terms_url,
                parse_digest(terms_algorithm, &terms_digest)?,
            )
            .map_err(|_| CatalogError::CorruptCatalog)?,
            authorization_evidence: parse_digest(authorization_algorithm, &authorization_digest)?,
            authorization_expires_at: authorization_expires_at_ns.map(Timestamp::from_unix_nanos),
            permitted_operations,
        })
        .map_err(|_| CatalogError::CorruptCatalog)?;
        decision
            .validate_at(Timestamp::from_unix_nanos(admitted_at_ns))
            .map_err(|_| CatalogError::CorruptCatalog)?;
        if rights_id.as_slice() != decision.fingerprint() {
            return Err(CatalogError::CorruptCatalog);
        }
    }
    Ok(())
}

fn verify_ingest_rights_links(transaction: &Transaction<'_>) -> Result<(), CatalogError> {
    let invalid: bool = transaction.query_row(
        "SELECT EXISTS(
             SELECT 1
             FROM ingest_runs AS run
             JOIN source_rights AS rights USING (rights_id)
             WHERE run.source_id <> rights.source_id
                OR run.payload_algorithm <> rights.payload_algorithm
                OR run.payload_digest <> rights.payload_digest
                OR run.requested_at_ns < rights.admitted_at_ns
                OR (
                    rights.authorization_expires_at_ns IS NOT NULL
                    AND run.requested_at_ns >= rights.authorization_expires_at_ns
                )
                OR (rights.operation_mask & CASE run.operation
                    WHEN 'retrieve' THEN 1
                    WHEN 'display' THEN 2
                    WHEN 'persist' THEN 4
                    WHEN 'cache' THEN 8
                    WHEN 'redistribute' THEN 16
                    WHEN 'train' THEN 32
                    ELSE 0
                END) = 0
         )",
        params![],
        |row| row.get(0),
    )?;
    if invalid {
        Err(CatalogError::CorruptCatalog)
    } else {
        Ok(())
    }
}

fn all_source_operations() -> [SourceOperation; 6] {
    [
        SourceOperation::Retrieve,
        SourceOperation::Display,
        SourceOperation::Persist,
        SourceOperation::Cache,
        SourceOperation::Redistribute,
        SourceOperation::Train,
    ]
}
