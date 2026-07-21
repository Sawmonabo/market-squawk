//! Append-only SQLite row persistence and collision checks.

use market_squawk_domain::{RevisionNumber, Timestamp};
use market_squawk_sources::{
    CanonicalObservationFamily, ObservedRevisionBatch, ObservedRevisionError,
    ObservedRevisionRecord,
};
use rusqlite::{OptionalExtension as _, Transaction, params};

use super::canonical::{encoded_provider_order, version_kind_name};
use super::stored::StoredVersionRow;
use super::{
    BATCH_CANONICAL_VERSION, FAMILY_ENCODING_VERSION, PAYLOAD_EVIDENCE_VERSION,
    VERSION_EVIDENCE_VERSION, map_persistence_error,
};
use crate::catalog::storage::{digest_columns, sha256};

pub(super) fn insert_version(
    transaction: &Transaction<'_>,
    record: &ObservedRevisionRecord,
    revision: RevisionNumber,
    assigned_at: Timestamp,
) -> Result<(), ObservedRevisionError> {
    let family = record.family();
    let (family_algorithm, family_digest) = digest_columns(family.identity());
    let (version_algorithm, version_digest) = digest_columns(record.version().identity());
    let (payload_algorithm, payload_digest) = digest_columns(record.semantic_payload().identity());
    let order = encoded_provider_order(record.provider_order())?;
    let changed = transaction
        .execute(
            "INSERT INTO observed_revision_versions
             (source_id, family_algorithm, family_digest, revision, version_kind,
              version_algorithm, version_digest, version_evidence_version, version_evidence,
              payload_algorithm, payload_digest, payload_evidence_version, payload_evidence,
              provider_order_evidence_version, provider_coordinate_json, provider_tie_breaker,
              assigned_at_ns)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15,
                     ?16, ?17)",
            params![
                family.source_id().as_str(),
                family_algorithm,
                family_digest,
                i64::from(revision.get()),
                version_kind_name(record.version().kind()),
                version_algorithm,
                version_digest,
                VERSION_EVIDENCE_VERSION,
                record.version().exact_evidence(),
                payload_algorithm,
                payload_digest,
                PAYLOAD_EVIDENCE_VERSION,
                record.semantic_payload().exact_evidence(),
                order.version,
                order.coordinate_json,
                order.tie_breaker,
                assigned_at.unix_nanos()
            ],
        )
        .map_err(map_persistence_error)?;
    if changed == 1 {
        Ok(())
    } else {
        Err(ObservedRevisionError::PersistenceUnavailable)
    }
}

pub(super) fn persist_family(
    transaction: &Transaction<'_>,
    family: &CanonicalObservationFamily,
) -> Result<(), ObservedRevisionError> {
    let (algorithm, digest) = digest_columns(family.identity());
    let changed = transaction
        .execute(
            "INSERT OR IGNORE INTO observed_revision_families
             (source_id, family_algorithm, family_digest, family_encoding_version,
              family_evidence) VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                family.source_id().as_str(),
                algorithm,
                digest,
                FAMILY_ENCODING_VERSION,
                family.exact_bytes()
            ],
        )
        .map_err(map_persistence_error)?;
    if changed == 1 {
        return Ok(());
    }
    validate_retained_family(transaction, family)
}

pub(super) fn validate_retained_family(
    connection: &rusqlite::Connection,
    family: &CanonicalObservationFamily,
) -> Result<(), ObservedRevisionError> {
    let (algorithm, digest) = digest_columns(family.identity());
    let retained: Option<(i64, Vec<u8>)> = connection
        .query_row(
            "SELECT family_encoding_version, family_evidence
             FROM observed_revision_families
             WHERE source_id=?1 AND family_algorithm=?2 AND family_digest=?3",
            params![family.source_id().as_str(), algorithm, digest],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()
        .map_err(map_persistence_error)?;
    match retained {
        None => Ok(()),
        Some((FAMILY_ENCODING_VERSION, exact))
            if exact == family.exact_bytes() && sha256(&exact) == digest =>
        {
            Ok(())
        }
        Some(_) => Err(ObservedRevisionError::Conflict),
    }
}

pub(super) fn load_version_identity(
    transaction: &Transaction<'_>,
    record: &ObservedRevisionRecord,
) -> Result<Option<StoredVersionRow>, ObservedRevisionError> {
    let (family_algorithm, family_digest) = digest_columns(record.family().identity());
    let (version_algorithm, version_digest) = digest_columns(record.version().identity());
    transaction
        .query_row(
            "SELECT revision, version_kind, version_algorithm, version_digest,
                    version_evidence_version, version_evidence, payload_algorithm,
                    payload_digest, payload_evidence_version, payload_evidence,
                    provider_order_evidence_version, provider_coordinate_json,
                    provider_tie_breaker, assigned_at_ns
             FROM observed_revision_versions
             WHERE source_id=?1 AND family_algorithm=?2 AND family_digest=?3
               AND version_kind=?4 AND version_algorithm=?5 AND version_digest=?6",
            params![
                record.family().source_id().as_str(),
                family_algorithm,
                family_digest,
                version_kind_name(record.version().kind()),
                version_algorithm,
                version_digest
            ],
            StoredVersionRow::read,
        )
        .optional()
        .map_err(map_persistence_error)
}

pub(super) fn load_frontier(
    transaction: &Transaction<'_>,
    family: &CanonicalObservationFamily,
) -> Result<Option<StoredVersionRow>, ObservedRevisionError> {
    let (family_algorithm, family_digest) = digest_columns(family.identity());
    transaction
        .query_row(
            "SELECT revision, version_kind, version_algorithm, version_digest,
                    version_evidence_version, version_evidence, payload_algorithm,
                    payload_digest, payload_evidence_version, payload_evidence,
                    provider_order_evidence_version, provider_coordinate_json,
                    provider_tie_breaker, assigned_at_ns
             FROM observed_revision_versions
             WHERE source_id=?1 AND family_algorithm=?2 AND family_digest=?3
             ORDER BY revision DESC LIMIT 1",
            params![family.source_id().as_str(), family_algorithm, family_digest],
            StoredVersionRow::read,
        )
        .optional()
        .map_err(map_persistence_error)
}

pub(super) fn persist_batch_member(
    transaction: &Transaction<'_>,
    source_id: &str,
    batch_digest: [u8; 32],
    ordinal: usize,
    record: &ObservedRevisionRecord,
    revision: RevisionNumber,
) -> Result<(), ObservedRevisionError> {
    let ordinal =
        i64::try_from(ordinal).map_err(|_| ObservedRevisionError::RecordLimitExceeded {
            max: market_squawk_sources::MAX_OBSERVED_REVISION_BATCH_RECORDS,
        })?;
    let (family_algorithm, family_digest) = digest_columns(record.family().identity());
    let (version_algorithm, version_digest) = digest_columns(record.version().identity());
    let values = (
        source_id,
        family_algorithm,
        family_digest,
        i64::from(revision.get()),
        version_kind_name(record.version().kind()),
        version_algorithm,
        version_digest,
    );
    let changed = transaction
        .execute(
            "INSERT OR IGNORE INTO observed_revision_batch_members
             (source_id, batch_algorithm, batch_digest, ordinal, family_algorithm,
              family_digest, revision, version_kind, version_algorithm, version_digest)
             VALUES (?1, 1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            params![
                values.0,
                batch_digest,
                ordinal,
                values.1,
                values.2,
                values.3,
                values.4,
                values.5,
                values.6
            ],
        )
        .map_err(map_persistence_error)?;
    if changed == 1 {
        return Ok(());
    }
    let retained: (String, i64, Vec<u8>, i64, String, i64, Vec<u8>) = transaction
        .query_row(
            "SELECT source_id, family_algorithm, family_digest, revision, version_kind,
                    version_algorithm, version_digest
             FROM observed_revision_batch_members
             WHERE batch_algorithm=1 AND batch_digest=?1 AND ordinal=?2",
            params![batch_digest, ordinal],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                    row.get(6)?,
                ))
            },
        )
        .map_err(map_persistence_error)?;
    if retained.0 == values.0
        && retained.1 == values.1
        && retained.2 == values.2
        && retained.3 == values.3
        && retained.4 == values.4
        && retained.5 == values.5
        && retained.6 == values.6
    {
        Ok(())
    } else {
        Err(ObservedRevisionError::Conflict)
    }
}

pub(super) fn persist_batch(
    transaction: &Transaction<'_>,
    batch: &ObservedRevisionBatch,
    batch_digest: [u8; 32],
    assigned_at: Timestamp,
) -> Result<(), ObservedRevisionError> {
    let input_count =
        i64::try_from(batch.input_len()).map_err(|_| ObservedRevisionError::ByteCountOverflow)?;
    let unique_count = i64::try_from(batch.unique_records().len())
        .map_err(|_| ObservedRevisionError::ByteCountOverflow)?;
    let changed = transaction
        .execute(
            "INSERT OR IGNORE INTO observed_revision_batches
             (batch_algorithm, batch_digest, canonical_version, source_id, input_count,
              unique_count, assigned_at_ns) VALUES (1, ?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                batch_digest,
                BATCH_CANONICAL_VERSION,
                batch.source_id().as_str(),
                input_count,
                unique_count,
                assigned_at.unix_nanos()
            ],
        )
        .map_err(map_persistence_error)?;
    if changed == 0 {
        let retained: (i64, String, i64, i64) = transaction
            .query_row(
                "SELECT canonical_version, source_id, input_count, unique_count
                 FROM observed_revision_batches
                 WHERE batch_algorithm=1 AND batch_digest=?1",
                [batch_digest],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .map_err(map_persistence_error)?;
        if retained
            != (
                BATCH_CANONICAL_VERSION,
                batch.source_id().as_str().to_owned(),
                input_count,
                unique_count,
            )
        {
            return Err(ObservedRevisionError::Conflict);
        }
    }
    let retained_members: i64 = transaction
        .query_row(
            "SELECT COUNT(*) FROM observed_revision_batch_members
             WHERE batch_algorithm=1 AND batch_digest=?1",
            [batch_digest],
            |row| row.get(0),
        )
        .map_err(map_persistence_error)?;
    if retained_members == unique_count {
        Ok(())
    } else {
        Err(ObservedRevisionError::CorruptAuthorityState)
    }
}

pub(super) fn require_source(
    transaction: &Transaction<'_>,
    source_id: &str,
) -> Result<(), ObservedRevisionError> {
    let exists: bool = transaction
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM sources WHERE source_id=?1)",
            [source_id],
            |row| row.get(0),
        )
        .map_err(map_persistence_error)?;
    if exists {
        Ok(())
    } else {
        Err(ObservedRevisionError::PersistenceUnavailable)
    }
}
