//! Consistent bounded relational snapshots for analytical backup authority.

use std::collections::BTreeMap;

use market_squawk_domain::{SourceIdentifier, Timestamp};
use rusqlite::limits::Limit;
use rusqlite::{Connection, Transaction};
use uuid::Uuid;

use super::authority::read_authority_snapshot_without_endpoint;
use super::backup::{VerifiedBackupCatalog, open_immutable_backup};
use super::storage::{verify_integrity, verify_migration_identities};
use super::types::MAX_SQLITE_RECORD_BYTES;
use super::{Catalog, CatalogError};
use crate::authority_transition::AuthoritySnapshot;
use crate::authority_transition::evidence::{
    ArtifactEvidenceRow, CatalogEvidenceSnapshot, EvidenceError, EvidenceSnapshotRequest,
    GenerationEvidenceRow, GenerationObjectEvidenceRow, ManifestEvidenceRow,
    QueryArtifactEvidenceRow,
};
use crate::{DatasetId, DatasetSchemaRef, DatasetSchemaRegistry, GenerationKind, Sha256Digest};

impl Catalog {
    /// Captures authority and analytical relationships from one consistent live read transaction.
    pub(crate) fn analytical_evidence_snapshot(
        &self,
        request: EvidenceSnapshotRequest,
    ) -> Result<(AuthoritySnapshot, CatalogEvidenceSnapshot), CatalogError> {
        let transaction = self.connection.unchecked_transaction()?;
        let snapshot = evidence_snapshot(&transaction, request)?;
        transaction.commit()?;
        Ok(snapshot)
    }

    /// Captures exact read-only evidence from a retained immutable backup lease.
    ///
    /// This path does not create a writer sidecar, run migrations, or mutate the backup. The
    /// exact receipt, retained file identity, compiled migrations, and SQLite integrity are
    /// revalidated before and after the single read transaction.
    pub(crate) fn verified_backup_evidence(
        backup: &VerifiedBackupCatalog,
        request: EvidenceSnapshotRequest,
    ) -> Result<(AuthoritySnapshot, CatalogEvidenceSnapshot), CatalogError> {
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
        let snapshot = evidence_snapshot(&transaction, request)?;
        transaction.commit()?;
        verify_migration_identities(&connection)?;
        verify_integrity(&connection)?;
        connection.close().map_err(|(_, error)| error)?;
        backup.revalidate()?;
        Ok(snapshot)
    }
}

fn evidence_snapshot(
    transaction: &Transaction<'_>,
    request: EvidenceSnapshotRequest,
) -> Result<(AuthoritySnapshot, CatalogEvidenceSnapshot), CatalogError> {
    let authority = read_authority_snapshot_without_endpoint(transaction)?;
    let limits = request.limits();
    let artifacts = read_artifacts(transaction, limits.max_artifacts())?;
    let mut remaining_references = limits
        .max_references()
        .checked_sub(artifacts.len())
        .ok_or(CatalogError::AnalyticalEvidenceLimitExceeded)?;
    let manifests = read_manifests(transaction, remaining_references)?;
    remaining_references = remaining_references
        .checked_sub(manifests.len())
        .ok_or(CatalogError::AnalyticalEvidenceLimitExceeded)?;
    let (generations, generation_objects) = read_generations(transaction, remaining_references)?;
    remaining_references = remaining_references
        .checked_sub(generation_objects)
        .ok_or(CatalogError::AnalyticalEvidenceLimitExceeded)?;
    let query_artifacts =
        read_query_artifacts(transaction, request.cutoff(), remaining_references)?;
    let evidence = CatalogEvidenceSnapshot::try_new(
        request,
        artifacts,
        manifests,
        generations,
        query_artifacts,
    )
    .map_err(map_evidence_error)?;
    Ok((authority, evidence))
}

fn read_artifacts(
    connection: &Connection,
    maximum: usize,
) -> Result<Vec<ArtifactEvidenceRow>, CatalogError> {
    let limit = limit_with_sentinel(maximum)?;
    let mut statement = connection.prepare(
        "SELECT artifact_id, run_id, relative_reference, content_algorithm, content_digest, \
                size_bytes FROM artifacts ORDER BY artifact_id LIMIT ?1",
    )?;
    let mut rows = statement.query([limit])?;
    let mut result = Vec::new();
    while let Some(row) = rows.next()? {
        require_capacity(&result, maximum)?;
        let algorithm: i64 = row.get(3)?;
        result.push(
            ArtifactEvidenceRow::try_new(
                parse_uuid(row.get::<_, String>(0)?)?,
                parse_uuid(row.get::<_, String>(1)?)?,
                row.get::<_, String>(2)?,
                parse_sha256(algorithm, row.get::<_, Vec<u8>>(4)?)?,
                parse_positive_u64(row.get(5)?)?,
            )
            .map_err(map_evidence_error)?,
        );
    }
    Ok(result)
}

fn read_manifests(
    connection: &Connection,
    maximum: usize,
) -> Result<Vec<ManifestEvidenceRow>, CatalogError> {
    let limit = limit_with_sentinel(maximum)?;
    let mut statement = connection.prepare(
        "SELECT manifest_id, dataset_name, schema_version, artifact_id, content_algorithm, \
                content_digest FROM dataset_manifests ORDER BY manifest_id LIMIT ?1",
    )?;
    let mut rows = statement.query([limit])?;
    let mut result = Vec::new();
    while let Some(row) = rows.next()? {
        require_capacity(&result, maximum)?;
        let dataset = row.get::<_, String>(1)?;
        let algorithm: i64 = row.get(4)?;
        result.push(
            ManifestEvidenceRow::try_new(
                parse_uuid(row.get::<_, String>(0)?)?,
                DatasetId::try_from(dataset.as_str()).map_err(|_| CatalogError::CorruptCatalog)?,
                parse_positive_u32(row.get(2)?)?,
                parse_uuid(row.get::<_, String>(3)?)?,
                parse_sha256(algorithm, row.get::<_, Vec<u8>>(5)?)?,
            )
            .map_err(map_evidence_error)?,
        );
    }
    Ok(result)
}

#[derive(Debug)]
struct GenerationHeader {
    dataset_id: DatasetId,
    dataset_key: String,
    manifest_version: u64,
    content_hash: Sha256Digest,
    lineage_hash: Sha256Digest,
    row_count: u64,
    total_bytes: u64,
    schema: DatasetSchemaRef,
    anchor_manifest_id: Uuid,
    parent_version: Option<u64>,
    kind: GenerationKind,
}

fn read_generations(
    connection: &Connection,
    maximum_references: usize,
) -> Result<(Vec<GenerationEvidenceRow>, usize), CatalogError> {
    let headers = read_generation_headers(connection, maximum_references)?;
    let mut objects = read_generation_objects(connection, maximum_references)?;
    let object_count = objects.values().try_fold(0_usize, |total, members| {
        total
            .checked_add(members.len())
            .ok_or(CatalogError::AnalyticalEvidenceLimitExceeded)
    })?;
    let mut result = Vec::with_capacity(headers.len());
    for header in headers {
        let key = (header.dataset_key.clone(), header.manifest_version);
        let generation_objects = objects.remove(&key).ok_or(CatalogError::CorruptCatalog)?;
        result.push(
            GenerationEvidenceRow::try_new(
                header.dataset_id,
                header.manifest_version,
                header.content_hash,
                header.lineage_hash,
                header.row_count,
                header.total_bytes,
                header.schema,
                header.anchor_manifest_id,
                header.parent_version,
                header.kind,
                generation_objects,
            )
            .map_err(map_evidence_error)?,
        );
    }
    if !objects.is_empty() {
        return Err(CatalogError::CorruptCatalog);
    }
    Ok((result, object_count))
}

fn read_generation_headers(
    connection: &Connection,
    maximum: usize,
) -> Result<Vec<GenerationHeader>, CatalogError> {
    let limit = limit_with_sentinel(maximum)?;
    let mut statement = connection.prepare(
        "SELECT dataset_id, manifest_version, content_hash, lineage_hash, row_count, total_bytes, \
                schema_name, schema_version, schema_fingerprint, anchor_manifest_id, \
                parent_version, generation_kind \
         FROM analytical_generations ORDER BY dataset_id, manifest_version LIMIT ?1",
    )?;
    let mut rows = statement.query([limit])?;
    let mut result = Vec::new();
    while let Some(row) = rows.next()? {
        require_capacity(&result, maximum)?;
        let dataset_key: String = row.get(0)?;
        let parent: Option<i64> = row.get(10)?;
        let kind: String = row.get(11)?;
        let schema_name: String = row.get(6)?;
        let schema_version =
            u16::try_from(row.get::<_, i64>(7)?).map_err(|_| CatalogError::CorruptCatalog)?;
        let schema_fingerprint: [u8; 32] = row
            .get::<_, Vec<u8>>(8)?
            .try_into()
            .map_err(|_| CatalogError::CorruptCatalog)?;
        let schema = DatasetSchemaRef::try_new(
            &schema_name,
            market_squawk_domain::SchemaVersion::new(schema_version)
                .map_err(|_| CatalogError::CorruptCatalog)?,
            schema_fingerprint,
        )
        .map_err(|_| CatalogError::CorruptCatalog)?;
        DatasetSchemaRegistry::local()
            .resolve(&schema)
            .map_err(|_| CatalogError::CorruptCatalog)?;
        result.push(GenerationHeader {
            dataset_id: DatasetId::try_from(dataset_key.as_str())
                .map_err(|_| CatalogError::CorruptCatalog)?,
            dataset_key,
            manifest_version: parse_positive_u64(row.get(1)?)?,
            content_hash: parse_sha256(1, row.get::<_, Vec<u8>>(2)?)?,
            lineage_hash: parse_sha256(1, row.get::<_, Vec<u8>>(3)?)?,
            row_count: parse_positive_u64(row.get(4)?)?,
            total_bytes: parse_positive_u64(row.get(5)?)?,
            schema,
            anchor_manifest_id: parse_uuid(row.get::<_, String>(9)?)?,
            parent_version: parent.map(parse_positive_u64).transpose()?,
            kind: match kind.as_str() {
                "ingest" => GenerationKind::Ingest,
                "compaction" => GenerationKind::Compaction,
                _ => return Err(CatalogError::CorruptCatalog),
            },
        });
    }
    Ok(result)
}

fn read_generation_objects(
    connection: &Connection,
    maximum: usize,
) -> Result<BTreeMap<(String, u64), Vec<GenerationObjectEvidenceRow>>, CatalogError> {
    let limit = limit_with_sentinel(maximum)?;
    let mut statement = connection.prepare(
        "SELECT dataset_id, manifest_version, ordinal, artifact_id, content_hash, row_count, \
                size_bytes, lineage_hash FROM analytical_generation_objects \
         ORDER BY dataset_id, manifest_version, ordinal LIMIT ?1",
    )?;
    let mut rows = statement.query([limit])?;
    let mut result: BTreeMap<(String, u64), Vec<GenerationObjectEvidenceRow>> = BTreeMap::new();
    let mut observed = 0_usize;
    while let Some(row) = rows.next()? {
        if observed >= maximum {
            return Err(CatalogError::AnalyticalEvidenceLimitExceeded);
        }
        observed = observed
            .checked_add(1)
            .ok_or(CatalogError::AnalyticalEvidenceLimitExceeded)?;
        let dataset: String = row.get(0)?;
        DatasetId::try_from(dataset.as_str()).map_err(|_| CatalogError::CorruptCatalog)?;
        let version = parse_positive_u64(row.get(1)?)?;
        let ordinal =
            usize::try_from(row.get::<_, i64>(2)?).map_err(|_| CatalogError::CorruptCatalog)?;
        let members = result.entry((dataset, version)).or_default();
        if ordinal != members.len() {
            return Err(CatalogError::CorruptCatalog);
        }
        members.push(
            GenerationObjectEvidenceRow::try_new(
                parse_uuid(row.get::<_, String>(3)?)?,
                parse_sha256(1, row.get::<_, Vec<u8>>(4)?)?,
                parse_positive_u64(row.get(5)?)?,
                parse_positive_u64(row.get(6)?)?,
                parse_sha256(1, row.get::<_, Vec<u8>>(7)?)?,
            )
            .map_err(map_evidence_error)?,
        );
    }
    Ok(result)
}

fn read_query_artifacts(
    connection: &Connection,
    cutoff: Timestamp,
    maximum: usize,
) -> Result<Vec<QueryArtifactEvidenceRow>, CatalogError> {
    let limit = limit_with_sentinel(maximum)?;
    let mut statement = connection.prepare(
        "SELECT reservations.reservation_id, reservations.owner, reservations.request_algorithm, \
                reservations.request_digest, results.artifact_id, results.relative_reference, \
                results.content_algorithm, results.content_digest, results.size_bytes, \
                reservations.expires_at_ns \
         FROM query_artifact_reservations AS reservations \
         JOIN query_artifact_results AS results USING (reservation_id) \
         WHERE reservations.state='published' AND reservations.expires_at_ns>?1 \
         ORDER BY reservations.reservation_id LIMIT ?2",
    )?;
    let mut rows = statement.query((cutoff.unix_nanos(), limit))?;
    let mut result = Vec::new();
    while let Some(row) = rows.next()? {
        require_capacity(&result, maximum)?;
        let request_algorithm: i64 = row.get(2)?;
        let content_algorithm: i64 = row.get(6)?;
        result.push(
            QueryArtifactEvidenceRow::try_new(
                parse_uuid(row.get::<_, String>(0)?)?,
                SourceIdentifier::try_from(row.get::<_, String>(1)?)
                    .map_err(|_| CatalogError::CorruptCatalog)?,
                parse_sha256(request_algorithm, row.get::<_, Vec<u8>>(3)?)?,
                parse_uuid(row.get::<_, String>(4)?)?,
                row.get::<_, String>(5)?,
                parse_sha256(content_algorithm, row.get::<_, Vec<u8>>(7)?)?,
                parse_positive_u64(row.get(8)?)?,
                Timestamp::from_unix_nanos(row.get(9)?),
            )
            .map_err(map_evidence_error)?,
        );
    }
    Ok(result)
}

fn require_capacity<T>(items: &[T], maximum: usize) -> Result<(), CatalogError> {
    if items.len() >= maximum {
        Err(CatalogError::AnalyticalEvidenceLimitExceeded)
    } else {
        Ok(())
    }
}

fn limit_with_sentinel(maximum: usize) -> Result<i64, CatalogError> {
    let limit = maximum
        .checked_add(1)
        .ok_or(CatalogError::AnalyticalEvidenceLimitExceeded)?;
    i64::try_from(limit).map_err(|_| CatalogError::AnalyticalEvidenceLimitExceeded)
}

fn parse_uuid(value: String) -> Result<Uuid, CatalogError> {
    let value = Uuid::parse_str(&value).map_err(|_| CatalogError::CorruptCatalog)?;
    if value.is_nil() {
        Err(CatalogError::CorruptCatalog)
    } else {
        Ok(value)
    }
}

fn parse_sha256(algorithm: i64, value: Vec<u8>) -> Result<Sha256Digest, CatalogError> {
    if algorithm != 1 {
        return Err(CatalogError::CorruptCatalog);
    }
    value
        .try_into()
        .map(Sha256Digest::new)
        .map_err(|_| CatalogError::CorruptCatalog)
}

fn parse_positive_u64(value: i64) -> Result<u64, CatalogError> {
    u64::try_from(value)
        .ok()
        .filter(|value| *value > 0)
        .ok_or(CatalogError::CorruptCatalog)
}

fn parse_positive_u32(value: i64) -> Result<u32, CatalogError> {
    u32::try_from(value)
        .ok()
        .filter(|value| *value > 0)
        .ok_or(CatalogError::CorruptCatalog)
}

fn map_evidence_error(error: EvidenceError) -> CatalogError {
    match error {
        EvidenceError::ResourceLimitExceeded => CatalogError::AnalyticalEvidenceLimitExceeded,
        EvidenceError::Cancelled => CatalogError::AnalyticalEvidenceCancelled,
        _ => CatalogError::AnalyticalEvidenceInvalid,
    }
}
