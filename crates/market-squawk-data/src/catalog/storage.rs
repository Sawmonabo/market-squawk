//! Internal path, migration, row-conversion, and transaction helpers.

use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use market_squawk_domain::{
    DigestAlgorithm, EvidenceDigest, InstrumentDefinition, InstrumentId, SymbolIdentityRecord,
    Timestamp,
};
use rusqlite::{Connection, OptionalExtension as _, Transaction, params};
use serde::de::DeserializeOwned;
use sha2::{Digest as _, Sha256};
use uuid::Uuid;

use super::authority::append_legacy_authority_requirement;
use super::types::{CatalogError, CatalogResultLimits, IngestReservation};
use crate::migrations::MIGRATIONS;
use crate::rights::SourceRightsDecision;
use crate::{IngestIdentity, SourceOperation};

const MAX_ARTIFACT_REFERENCE_BYTES: usize = 1_024;
const MAX_ARTIFACT_COMPONENT_BYTES: usize = 255;
const MAX_ARTIFACT_DEPTH: usize = 32;
const MIN_RESERVED_RESULT_RECORD_BYTES: usize = 32;
pub(super) const CATALOG_APPLICATION_ID: i64 = 0x4d53_514b;

pub(super) struct ExistingReservation {
    pub(super) reservation: IngestReservation,
    source_id: String,
    payload_algorithm: i64,
    payload_digest: Vec<u8>,
    operation: String,
    rights_id: Vec<u8>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum AppendOutcome {
    Inserted,
    Replay,
}

pub(super) struct ResultBudget {
    max_record_bytes: usize,
    remaining_bytes: usize,
}

impl ResultBudget {
    pub(super) fn new(limits: CatalogResultLimits) -> Self {
        Self {
            max_record_bytes: limits.max_record_bytes(),
            remaining_bytes: limits.max_result_bytes(),
        }
    }

    pub(super) fn charge<const N: usize>(
        &mut self,
        components: [usize; N],
    ) -> Result<(), CatalogError> {
        let record_bytes = components
            .into_iter()
            .try_fold(0_usize, |total, component| total.checked_add(component));
        let Some(record_bytes) = record_bytes else {
            return Err(CatalogError::ResultByteLimitExceeded);
        };
        if record_bytes > self.max_record_bytes || record_bytes > self.remaining_bytes {
            return Err(CatalogError::ResultByteLimitExceeded);
        }
        self.remaining_bytes -= record_bytes;
        Ok(())
    }

    pub(super) fn bounded_row_capacity(&self, requested: usize) -> usize {
        requested.min(self.remaining_bytes / MIN_RESERVED_RESULT_RECORD_BYTES)
    }

    pub(super) fn charge_many(
        &mut self,
        count: usize,
        bytes_per_record: usize,
    ) -> Result<(), CatalogError> {
        if bytes_per_record > self.max_record_bytes {
            return Err(CatalogError::ResultByteLimitExceeded);
        }
        let bytes = count
            .checked_mul(bytes_per_record)
            .ok_or(CatalogError::ResultByteLimitExceeded)?;
        if bytes > self.remaining_bytes {
            return Err(CatalogError::ResultByteLimitExceeded);
        }
        self.remaining_bytes -= bytes;
        Ok(())
    }
}

impl ExistingReservation {
    pub(super) fn matches(&self, request: &IngestIdentity, rights: &SourceRightsDecision) -> bool {
        let (algorithm, digest) = digest_columns(request.payload_digest());
        self.source_id == request.source_id().as_str()
            && self.payload_algorithm == algorithm
            && self.payload_digest == digest
            && SourceOperation::from_database_name(&self.operation) == Some(request.operation())
            && self.rights_id == rights.fingerprint()
    }
}

pub(super) fn prepare_local_path(path: &Path) -> Result<PathBuf, CatalogError> {
    let parent = path.parent().ok_or(CatalogError::UnsafePath)?;
    let parent = parent.canonicalize()?;
    let file_name = path.file_name().ok_or(CatalogError::UnsafePath)?;
    let prepared = parent.join(file_name);
    match fs::symlink_metadata(&prepared) {
        Ok(metadata) if metadata.is_file() => {}
        Ok(_) => return Err(CatalogError::UnsafePath),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    Ok(prepared)
}

pub(super) fn apply_migrations(
    connection: &mut Connection,
    catalog_identity: [u8; 32],
) -> Result<(), CatalogError> {
    validate_migration_registry()?;
    let applied = read_applied_migrations(connection)?;
    validate_applied_migrations(&applied)?;
    if applied.len() == MIGRATIONS.len() {
        return Ok(());
    }
    let legacy_root_migration_required = matches!(applied.len(), 3 | 4);
    let applied_at = now_timestamp()?;
    let transaction = connection.transaction()?;
    for migration in &MIGRATIONS[applied.len()..] {
        if migration.version == 8 {
            super::migration_preflight::preflight_research_use_migration(&transaction)?;
        }
        transaction.execute_batch(migration.sql)?;
        if migration.version == 5 && legacy_root_migration_required {
            let legacy_schema_version = u64::try_from(applied.len())
                .map_err(|_| CatalogError::MigrationRegistryMismatch)?;
            append_legacy_authority_requirement(
                &transaction,
                catalog_identity,
                legacy_schema_version,
            )?;
        }
        transaction.execute(
            "INSERT INTO schema_migrations(version, sha256, applied_at_ns) VALUES (?1, ?2, ?3)",
            params![migration.version, migration.sha256, applied_at.unix_nanos()],
        )?;
    }
    transaction.commit()?;
    Ok(())
}

pub(super) fn verify_migration_identities(connection: &Connection) -> Result<(), CatalogError> {
    validate_migration_registry()?;
    let applied = read_applied_migrations(connection)?;
    validate_applied_migrations(&applied)?;
    if applied.len() != MIGRATIONS.len() {
        return Err(CatalogError::MigrationRegistryMismatch);
    }
    Ok(())
}

fn validate_migration_registry() -> Result<(), CatalogError> {
    for (index, migration) in MIGRATIONS.iter().enumerate() {
        let expected_version = i64::try_from(index)
            .map_err(|_| CatalogError::MigrationRegistryMismatch)?
            .checked_add(1)
            .ok_or(CatalogError::MigrationRegistryMismatch)?;
        if migration.version != expected_version
            || sha256(migration.sql.as_bytes()) != *migration.sha256
        {
            return Err(CatalogError::MigrationRegistryMismatch);
        }
    }
    Ok(())
}

fn read_applied_migrations(connection: &Connection) -> Result<Vec<(i64, Vec<u8>)>, CatalogError> {
    let table_exists: bool = connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE type='table' AND name='schema_migrations')",
        [],
        |row| row.get(0),
    )?;
    let mut applied = Vec::new();
    if table_exists {
        let read_limit = MIGRATIONS
            .len()
            .checked_add(1)
            .and_then(|value| i64::try_from(value).ok())
            .ok_or(CatalogError::MigrationRegistryMismatch)?;
        applied
            .try_reserve_exact(MIGRATIONS.len().saturating_add(1))
            .map_err(|_| CatalogError::Allocation)?;
        let mut statement = connection
            .prepare("SELECT version, sha256 FROM schema_migrations ORDER BY version LIMIT ?1")?;
        let rows = statement.query_map([read_limit], |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, Vec<u8>>(1)?))
        })?;
        for row in rows {
            applied.push(row?);
        }
    }
    Ok(applied)
}

fn validate_applied_migrations(applied: &[(i64, Vec<u8>)]) -> Result<(), CatalogError> {
    if applied.len() > MIGRATIONS.len() {
        return Err(CatalogError::MigrationRegistryMismatch);
    }
    for (index, (version, digest)) in applied.iter().enumerate() {
        let migration = MIGRATIONS
            .get(index)
            .ok_or(CatalogError::MigrationRegistryMismatch)?;
        if *version != migration.version {
            return Err(CatalogError::MigrationRegistryMismatch);
        }
        if digest.as_slice() != migration.sha256 {
            return Err(CatalogError::MigrationDigestMismatch { version: *version });
        }
    }
    Ok(())
}

pub(super) fn initialize_catalog_identity(connection: &Connection) -> Result<(), CatalogError> {
    let application_id: i64 =
        connection.query_row("PRAGMA application_id", [], |row| row.get(0))?;
    if application_id == CATALOG_APPLICATION_ID {
        return Ok(());
    }
    let user_objects: i64 = connection.query_row(
        "SELECT COUNT(*) FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%'",
        [],
        |row| row.get(0),
    )?;
    if application_id != 0 || user_objects != 0 {
        return Err(CatalogError::ForeignCatalog);
    }
    connection.pragma_update(None, "application_id", CATALOG_APPLICATION_ID)?;
    Ok(())
}

pub(super) fn verify_integrity(connection: &Connection) -> Result<(), CatalogError> {
    let application_id: i64 =
        connection.query_row("PRAGMA application_id", [], |row| row.get(0))?;
    if application_id != CATALOG_APPLICATION_ID {
        return Err(CatalogError::ForeignCatalog);
    }
    let result: String = connection.query_row("PRAGMA integrity_check(1)", [], |row| row.get(0))?;
    if result != "ok" {
        return Err(CatalogError::CorruptCatalog);
    }
    let violation: Option<i64> = connection
        .query_row(
            "SELECT rowid FROM pragma_foreign_key_check LIMIT 1",
            [],
            |row| row.get(0),
        )
        .optional()?;
    if violation.is_some() {
        return Err(CatalogError::CorruptCatalog);
    }
    Ok(())
}

pub(super) fn pragma_bool(connection: &Connection, pragma: &str) -> Result<bool, CatalogError> {
    let value: i64 = connection.query_row(pragma, [], |row| row.get(0))?;
    match value {
        0 => Ok(false),
        1 => Ok(true),
        _ => Err(CatalogError::CorruptCatalog),
    }
}

pub(super) fn persist_instrument_children(
    transaction: &Transaction<'_>,
    instrument: &InstrumentDefinition,
    observed_at: Timestamp,
) -> Result<(), CatalogError> {
    for identifier in instrument.identifiers() {
        let json = serde_json::to_string(identifier)?;
        let digest = sha256(json.as_bytes());
        let inserted = transaction.execute(
            "INSERT OR IGNORE INTO instrument_identifiers
             (instrument_id, identifier_json, identifier_digest) VALUES (?1, ?2, ?3)",
            params![instrument.instrument_id().to_string(), json, digest],
        )?;
        if inserted == 0 {
            let existing: String = transaction.query_row(
                "SELECT identifier_json FROM instrument_identifiers
                 WHERE instrument_id=?1 AND identifier_digest=?2",
                params![instrument.instrument_id().to_string(), digest],
                |row| row.get(0),
            )?;
            if existing != json {
                return Err(CatalogError::EvidenceConflict);
            }
        }
    }
    let provider_records = instrument.provider_identities().iter().chain(
        instrument
            .provider_identity_conflicts()
            .iter()
            .flat_map(|conflict| conflict.competing_assertions()),
    );
    for provider in provider_records {
        let json = serde_json::to_string(provider)?;
        let digest = sha256(json.as_bytes());
        let inserted = transaction.execute(
            "INSERT OR IGNORE INTO provider_instrument_ids
             (instrument_id, source_id, provider_instrument_id, record_json, record_digest)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                instrument.instrument_id().to_string(),
                provider.source_id().as_str(),
                provider.provider_instrument_id().as_str(),
                json,
                digest
            ],
        )?;
        if inserted == 0 {
            let existing: (String, String) = transaction.query_row(
                "SELECT instrument_id, record_json FROM provider_instrument_ids
                 WHERE source_id=?1 AND provider_instrument_id=?2 AND record_digest=?3",
                params![
                    provider.source_id().as_str(),
                    provider.provider_instrument_id().as_str(),
                    digest
                ],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )?;
            if existing.0 != instrument.instrument_id().to_string() || existing.1 != json {
                return Err(CatalogError::EvidenceConflict);
            }
        }
    }
    for mapping in instrument.venue_mappings() {
        let symbol = SymbolIdentityRecord::new(
            instrument.instrument_id(),
            mapping.venue_id().clone(),
            mapping.venue_symbol().clone(),
            market_squawk_domain::EffectiveInterval::new(observed_at, None)
                .map_err(|_| CatalogError::InvalidRecord)?,
        );
        let json = serde_json::to_string(&symbol)?;
        persist_symbol(transaction, &symbol, &json)?;
    }
    Ok(())
}

pub(super) fn persist_symbol(
    transaction: &Transaction<'_>,
    symbol: &SymbolIdentityRecord,
    json: &str,
) -> Result<AppendOutcome, CatalogError> {
    transaction.execute(
        "INSERT OR IGNORE INTO venues(venue_id, first_observed_at_ns) VALUES (?1, ?2)",
        params![
            symbol.venue_id().as_str(),
            symbol.validity().starts_at().unix_nanos()
        ],
    )?;
    let digest = sha256(json.as_bytes());
    let existing: Option<(String, Vec<u8>)> = transaction
        .query_row(
            "SELECT record_json, record_digest FROM symbol_history
             WHERE instrument_id=?1 AND venue_id=?2 AND venue_symbol=?3 AND starts_at_ns=?4",
            params![
                symbol.instrument_id().to_string(),
                symbol.venue_id().as_str(),
                symbol.venue_symbol().as_str(),
                symbol.validity().starts_at().unix_nanos()
            ],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    if let Some((existing_json, existing_digest)) = existing {
        return if existing_json == json && existing_digest.as_slice() == digest {
            Ok(AppendOutcome::Replay)
        } else {
            Err(CatalogError::EvidenceConflict)
        };
    }
    transaction.execute(
        "INSERT INTO symbol_history
         (instrument_id, venue_id, venue_symbol, starts_at_ns, ends_at_ns, record_json,
          record_digest)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        params![
            symbol.instrument_id().to_string(),
            symbol.venue_id().as_str(),
            symbol.venue_symbol().as_str(),
            symbol.validity().starts_at().unix_nanos(),
            symbol.validity().ends_at().map(Timestamp::unix_nanos),
            json,
            digest
        ],
    )?;
    Ok(AppendOutcome::Inserted)
}

pub(super) fn persist_rights(
    transaction: &Transaction<'_>,
    rights: &SourceRightsDecision,
    admitted_at: Timestamp,
) -> Result<AppendOutcome, CatalogError> {
    let (payload_algorithm, payload_digest) = digest_columns(rights.payload_digest());
    let (basis_algorithm, basis_digest) = digest_columns(rights.basis().digest());
    let (basis_root_algorithm, basis_root_digest) = rights
        .basis()
        .root_identity_digest()
        .map(digest_columns)
        .map_or((None, None), |(algorithm, digest)| {
            (Some(algorithm), Some(digest))
        });
    let (authorization_algorithm, authorization_digest) =
        digest_columns(rights.authorization_evidence());
    let inserted = transaction.execute(
        "INSERT OR IGNORE INTO source_rights
         (rights_id, source_id, payload_algorithm, payload_digest, retrieved_at_ns,
          basis_reference, basis_algorithm, basis_digest, authorization_algorithm,
          authorization_digest, authorization_expires_at_ns, operation_mask, admitted_at_ns,
          basis_kind, basis_root_algorithm, basis_root_digest, fingerprint_version)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16,
                 ?17)",
        params![
            rights.fingerprint(),
            rights.source_id().as_str(),
            payload_algorithm,
            payload_digest,
            rights.retrieved_at().unix_nanos(),
            rights.basis().reference(),
            basis_algorithm,
            basis_digest,
            authorization_algorithm,
            authorization_digest,
            rights.authorization_expires_at().map(Timestamp::unix_nanos),
            i64::from(rights.operation_mask()),
            admitted_at.unix_nanos(),
            rights.basis().kind().database_name(),
            basis_root_algorithm,
            basis_root_digest,
            i64::from(rights.fingerprint_version()),
        ],
    )?;
    if inserted == 0 {
        require_admitted_rights(transaction, rights)?;
        Ok(AppendOutcome::Replay)
    } else {
        Ok(AppendOutcome::Inserted)
    }
}

pub(super) fn require_admitted_rights(
    transaction: &Transaction<'_>,
    rights: &SourceRightsDecision,
) -> Result<(), CatalogError> {
    let (payload_algorithm, payload_digest) = digest_columns(rights.payload_digest());
    let (basis_algorithm, basis_digest) = digest_columns(rights.basis().digest());
    let (basis_root_algorithm, basis_root_digest) = rights
        .basis()
        .root_identity_digest()
        .map(digest_columns)
        .map_or((None, None), |(algorithm, digest)| {
            (Some(algorithm), Some(digest))
        });
    let (authorization_algorithm, authorization_digest) =
        digest_columns(rights.authorization_evidence());
    let matches: bool = transaction.query_row(
        "SELECT EXISTS(
             SELECT 1 FROM source_rights
             WHERE rights_id=?1 AND source_id=?2
               AND payload_algorithm=?3 AND payload_digest=?4
               AND retrieved_at_ns=?5 AND basis_reference=?6
               AND basis_algorithm=?7 AND basis_digest=?8
               AND authorization_algorithm=?9 AND authorization_digest=?10
               AND authorization_expires_at_ns IS ?11 AND operation_mask=?12
               AND basis_kind=?13
               AND basis_root_algorithm IS ?14 AND basis_root_digest IS ?15
               AND fingerprint_version=?16
               AND retrieved_at_ns <= admitted_at_ns
               AND (
                   authorization_expires_at_ns IS NULL
                   OR admitted_at_ns < authorization_expires_at_ns
               )
         )",
        params![
            rights.fingerprint(),
            rights.source_id().as_str(),
            payload_algorithm,
            payload_digest,
            rights.retrieved_at().unix_nanos(),
            rights.basis().reference(),
            basis_algorithm,
            basis_digest,
            authorization_algorithm,
            authorization_digest,
            rights.authorization_expires_at().map(Timestamp::unix_nanos),
            i64::from(rights.operation_mask()),
            rights.basis().kind().database_name(),
            basis_root_algorithm,
            basis_root_digest,
            i64::from(rights.fingerprint_version()),
        ],
        |row| row.get(0),
    )?;
    if matches {
        Ok(())
    } else {
        Err(CatalogError::RightsNotAdmitted)
    }
}

pub(super) fn existing_reservation(
    transaction: &Transaction<'_>,
    request: &IngestIdentity,
    catalog_id: Uuid,
) -> Result<Option<ExistingReservation>, CatalogError> {
    let row = transaction
        .query_row(
            "SELECT run_id, source_id, payload_algorithm, payload_digest, operation,
                    rights_id, requested_at_ns
             FROM ingest_runs
             WHERE source_id=?1 AND operation=?2 AND idempotency_key=?3",
            params![
                request.source_id().as_str(),
                request.operation().database_name(),
                request.idempotency_key()
            ],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, Vec<u8>>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, Vec<u8>>(5)?,
                    row.get::<_, i64>(6)?,
                ))
            },
        )
        .optional()?;
    row.map(
        |(run, source, algorithm, digest, operation, rights, requested)| {
            Ok(ExistingReservation {
                reservation: IngestReservation {
                    run_id: Uuid::parse_str(&run).map_err(|_| CatalogError::CorruptCatalog)?,
                    requested_at: Timestamp::from_unix_nanos(requested),
                    catalog_id,
                },
                source_id: source,
                payload_algorithm: algorithm,
                payload_digest: digest,
                operation,
                rights_id: rights,
            })
        },
    )
    .transpose()
}

pub(super) fn require_instrument(
    transaction: &Transaction<'_>,
    instrument_id: InstrumentId,
) -> Result<(), CatalogError> {
    let exists: bool = transaction.query_row(
        "SELECT EXISTS(SELECT 1 FROM instruments WHERE instrument_id=?1)",
        [instrument_id.to_string()],
        |row| row.get(0),
    )?;
    if exists {
        Ok(())
    } else {
        Err(CatalogError::UnknownInstrument)
    }
}

pub(super) fn require_reserved_run(
    transaction: &Transaction<'_>,
    run_id: Uuid,
) -> Result<(), CatalogError> {
    let state: Option<String> = transaction
        .query_row(
            "SELECT state FROM ingest_runs WHERE run_id=?1",
            [run_id.to_string()],
            |row| row.get(0),
        )
        .optional()?;
    if state.as_deref() == Some("reserved") {
        Ok(())
    } else {
        Err(CatalogError::RunStateConflict)
    }
}

pub(super) fn query_records<T: DeserializeOwned>(
    connection: &Connection,
    sql: &str,
    instrument_id: InstrumentId,
    remaining: &mut usize,
    budget: &mut ResultBudget,
) -> Result<Vec<T>, CatalogError> {
    if *remaining == 0 {
        return Ok(Vec::new());
    }
    let row_limit = i64::try_from(*remaining).map_err(|_| CatalogError::InvalidLimit)?;
    let mut statement = connection.prepare(sql)?;
    let rows = statement.query_map(params![instrument_id.to_string(), row_limit], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, Vec<u8>>(1)?))
    })?;
    let mut records = Vec::new();
    records
        .try_reserve_exact(budget.bounded_row_capacity(*remaining))
        .map_err(|_| CatalogError::Allocation)?;
    for row in rows {
        let (json, digest) = row?;
        budget.charge([json.len(), digest.len()])?;
        if digest.len() != 32 || sha256(json.as_bytes()).as_slice() != digest {
            return Err(CatalogError::CorruptCatalog);
        }
        records.push(serde_json::from_str(&json)?);
    }
    *remaining = remaining.saturating_sub(records.len());
    Ok(records)
}

pub(super) fn append_audit(
    transaction: &Transaction<'_>,
    event_type: &str,
    subject_id: &str,
    details_digest: [u8; 32],
    occurred_at: Timestamp,
) -> Result<(), CatalogError> {
    transaction.execute(
        "INSERT INTO audit_events(event_type, subject_id, details_digest, occurred_at_ns)
         VALUES (?1, ?2, ?3, ?4)",
        params![
            event_type,
            subject_id,
            details_digest,
            occurred_at.unix_nanos()
        ],
    )?;
    Ok(())
}

pub(super) fn digest_columns(digest: EvidenceDigest) -> (i64, [u8; 32]) {
    (
        match digest.algorithm() {
            DigestAlgorithm::Sha256 => 1,
            DigestAlgorithm::Blake3 => 2,
        },
        digest.bytes(),
    )
}

pub(super) fn parse_digest(algorithm: i64, bytes: &[u8]) -> Result<EvidenceDigest, CatalogError> {
    let algorithm = match algorithm {
        1 => DigestAlgorithm::Sha256,
        2 => DigestAlgorithm::Blake3,
        _ => return Err(CatalogError::CorruptCatalog),
    };
    let bytes: [u8; 32] = bytes.try_into().map_err(|_| CatalogError::CorruptCatalog)?;
    Ok(EvidenceDigest::new(algorithm, bytes))
}

pub(super) fn sha256(bytes: &[u8]) -> [u8; 32] {
    Sha256::digest(bytes).into()
}

pub(super) fn valid_text(value: &str, max: usize) -> bool {
    !value.is_empty() && value.len() <= max && !value.chars().any(char::is_control)
}

pub(super) fn valid_artifact_reference(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_ARTIFACT_REFERENCE_BYTES
        && !value.starts_with('/')
        && !value.contains('\\')
        && value.split('/').count() <= MAX_ARTIFACT_DEPTH
        && value.split('/').all(valid_artifact_component)
}

fn valid_artifact_component(component: &str) -> bool {
    let mut bytes = component.bytes();
    let Some(first) = bytes.next() else {
        return false;
    };
    component.len() <= MAX_ARTIFACT_COMPONENT_BYTES
        && (first.is_ascii_lowercase() || first.is_ascii_digit())
        && bytes.all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'-' | b'_' | b'.')
        })
        && !component.ends_with('.')
        && !is_windows_reserved_name(component)
}

fn is_windows_reserved_name(component: &str) -> bool {
    let Some(base) = component.split('.').next() else {
        return true;
    };
    let upper = base.to_ascii_uppercase();
    matches!(upper.as_str(), "CON" | "PRN" | "AUX" | "NUL")
        || upper
            .strip_prefix("COM")
            .or_else(|| upper.strip_prefix("LPT"))
            .is_some_and(|suffix| {
                matches!(suffix, "1" | "2" | "3" | "4" | "5" | "6" | "7" | "8" | "9")
            })
}

pub(super) fn now_timestamp() -> Result<Timestamp, CatalogError> {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| CatalogError::InvalidConfiguration)?;
    let nanos =
        i64::try_from(elapsed.as_nanos()).map_err(|_| CatalogError::InvalidConfiguration)?;
    Ok(Timestamp::from_unix_nanos(nanos))
}

/// Advances the durable authority clock only after ruling out local wall-clock rollback.
pub(super) fn trusted_catalog_now(
    transaction: &Transaction<'_>,
) -> Result<Timestamp, CatalogError> {
    let wall_now = now_timestamp()?;
    let durable_ns: i64 = transaction.query_row(
        "SELECT last_timestamp_ns FROM catalog_authority_clock WHERE singleton=1",
        [],
        |row| row.get(0),
    )?;
    if wall_now.unix_nanos() < durable_ns {
        return Err(CatalogError::AuthorityClockRollback);
    }
    let changed = transaction.execute(
        "UPDATE catalog_authority_clock SET last_timestamp_ns=?1
         WHERE singleton=1 AND last_timestamp_ns<=?1",
        [wall_now.unix_nanos()],
    )?;
    if changed != 1 {
        return Err(CatalogError::CorruptCatalog);
    }
    Ok(wall_now)
}
