//! Consistent bounded relational snapshots for analytical backup authority.

use std::collections::BTreeMap;

use market_squawk_domain::{SourceIdentifier, Timestamp};
use market_squawk_platform::SealedResearchRawClaim;
use rusqlite::limits::Limit;
use rusqlite::{Connection, Transaction};
use sha2::{Digest as _, Sha256};
use uuid::Uuid;

use super::authority::read_authority_snapshot_without_endpoint;
use super::backup::{VerifiedBackupCatalog, open_immutable_backup};
use super::provider_capture::raw_claim_digest;
use super::storage::{verify_integrity, verify_migration_identities};
use super::types::MAX_SQLITE_RECORD_BYTES;
use super::{Catalog, CatalogError};
use crate::authority_transition::AuthoritySnapshot;
use crate::authority_transition::evidence::{
    ArtifactEvidenceRow, CatalogEvidenceSnapshot, EvidenceError, EvidenceSnapshotRequest,
    GenerationEvidenceRow, GenerationObjectEvidenceRow, GenerationParentEvidenceRow,
    ManifestEvidenceRow, QueryArtifactEvidenceRow,
};
use crate::manifest::{DatasetBuildSpecDigest, GenerationParentRelation};
use crate::{
    DatasetId, DatasetManifestRef, DatasetSchemaRef, DatasetSchemaRegistry, GenerationKind,
    Sha256Digest,
};

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
    let (generations, generation_references) = read_generations(transaction, remaining_references)?;
    remaining_references = remaining_references
        .checked_sub(generation_references)
        .ok_or(CatalogError::AnalyticalEvidenceLimitExceeded)?;
    let query_artifacts =
        read_query_artifacts(transaction, request.cutoff(), remaining_references)?;
    remaining_references = remaining_references
        .checked_sub(query_artifacts.len())
        .ok_or(CatalogError::AnalyticalEvidenceLimitExceeded)?;
    validate_provider_relation_integrity(transaction)?;
    let provider_relation_rows = read_provider_relation_rows(transaction, remaining_references)?;
    let evidence = CatalogEvidenceSnapshot::try_new_with_provider_relation_rows(
        request,
        artifacts,
        manifests,
        generations,
        query_artifacts,
        provider_relation_rows,
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
        "SELECT artifact_id, run_id, publication_ordinal, relative_reference,
                content_algorithm, content_digest, size_bytes
         FROM artifacts ORDER BY artifact_id LIMIT ?1",
    )?;
    let mut rows = statement.query([limit])?;
    let mut result = Vec::new();
    while let Some(row) = rows.next()? {
        require_capacity(&result, maximum)?;
        let ordinal =
            u16::try_from(row.get::<_, i64>(2)?).map_err(|_| CatalogError::CorruptCatalog)?;
        let algorithm: i64 = row.get(4)?;
        result.push(
            ArtifactEvidenceRow::try_new(
                parse_uuid(row.get::<_, String>(0)?)?,
                parse_uuid(row.get::<_, String>(1)?)?,
                ordinal,
                row.get::<_, String>(3)?,
                parse_sha256(algorithm, row.get::<_, Vec<u8>>(5)?)?,
                parse_positive_u64(row.get(6)?)?,
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
    generation_sequence: u64,
    dataset_id: DatasetId,
    dataset_key: String,
    manifest_version: u64,
    content_hash: Sha256Digest,
    lineage_hash: Sha256Digest,
    row_count: u64,
    total_bytes: u64,
    schema: DatasetSchemaRef,
    anchor_manifest_id: Uuid,
    kind: GenerationKind,
    build_spec_digest: Option<DatasetBuildSpecDigest>,
}

fn read_generations(
    connection: &Connection,
    maximum_references: usize,
) -> Result<(Vec<GenerationEvidenceRow>, usize), CatalogError> {
    let headers = read_generation_headers(connection, maximum_references)?;
    let mut objects = read_generation_objects(connection, maximum_references)?;
    let mut parents = read_generation_parents(connection, maximum_references)?;
    let object_count = objects.values().try_fold(0_usize, |total, members| {
        total
            .checked_add(members.len())
            .ok_or(CatalogError::AnalyticalEvidenceLimitExceeded)
    })?;
    let parent_count = parents.values().try_fold(0_usize, |total, members| {
        total
            .checked_add(members.len())
            .ok_or(CatalogError::AnalyticalEvidenceLimitExceeded)
    })?;
    let reference_count = object_count
        .checked_add(parent_count)
        .ok_or(CatalogError::AnalyticalEvidenceLimitExceeded)?;
    if reference_count > maximum_references {
        return Err(CatalogError::AnalyticalEvidenceLimitExceeded);
    }
    let mut result = Vec::with_capacity(headers.len());
    for header in headers {
        let key = (header.dataset_key.clone(), header.manifest_version);
        let generation_objects = objects.remove(&key).ok_or(CatalogError::CorruptCatalog)?;
        let generation_parents = parents.remove(&key).unwrap_or_default();
        result.push(
            GenerationEvidenceRow::try_new(
                header.generation_sequence,
                header.dataset_id,
                header.manifest_version,
                header.content_hash,
                header.lineage_hash,
                header.row_count,
                header.total_bytes,
                header.schema,
                header.anchor_manifest_id,
                header.kind,
                header.build_spec_digest,
                generation_parents,
                generation_objects,
            )
            .map_err(map_evidence_error)?,
        );
    }
    if !objects.is_empty() || !parents.is_empty() {
        return Err(CatalogError::CorruptCatalog);
    }
    Ok((result, reference_count))
}

fn read_generation_headers(
    connection: &Connection,
    maximum: usize,
) -> Result<Vec<GenerationHeader>, CatalogError> {
    let limit = limit_with_sentinel(maximum)?;
    let mut statement = connection.prepare(
        "SELECT generation_sequence, dataset_id, manifest_version, content_hash, lineage_hash, \
                row_count, total_bytes, \
                schema_name, schema_version, schema_fingerprint, anchor_manifest_id, \
                generation_kind, build_spec_digest \
         FROM analytical_generations ORDER BY dataset_id, manifest_version LIMIT ?1",
    )?;
    let mut rows = statement.query([limit])?;
    let mut result = Vec::new();
    while let Some(row) = rows.next()? {
        require_capacity(&result, maximum)?;
        let dataset_key: String = row.get(1)?;
        let kind: String = row.get(11)?;
        let schema_name: String = row.get(7)?;
        let schema_version =
            u16::try_from(row.get::<_, i64>(8)?).map_err(|_| CatalogError::CorruptCatalog)?;
        let schema_fingerprint: [u8; 32] = row
            .get::<_, Vec<u8>>(9)?
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
            generation_sequence: parse_positive_u64(row.get(0)?)?,
            dataset_id: DatasetId::try_from(dataset_key.as_str())
                .map_err(|_| CatalogError::CorruptCatalog)?,
            dataset_key,
            manifest_version: parse_positive_u64(row.get(2)?)?,
            content_hash: parse_sha256(1, row.get::<_, Vec<u8>>(3)?)?,
            lineage_hash: parse_sha256(1, row.get::<_, Vec<u8>>(4)?)?,
            row_count: parse_positive_u64(row.get(5)?)?,
            total_bytes: parse_positive_u64(row.get(6)?)?,
            schema,
            anchor_manifest_id: parse_uuid(row.get::<_, String>(10)?)?,
            kind: GenerationKind::from_database_name(&kind).ok_or(CatalogError::CorruptCatalog)?,
            build_spec_digest: row
                .get::<_, Option<Vec<u8>>>(12)?
                .map(parse_build_spec_digest)
                .transpose()?,
        });
    }
    Ok(result)
}

fn read_generation_parents(
    connection: &Connection,
    maximum: usize,
) -> Result<BTreeMap<(String, u64), Vec<GenerationParentEvidenceRow>>, CatalogError> {
    let limit = limit_with_sentinel(maximum)?;
    let mut statement = connection.prepare(
        "SELECT child_dataset_id, child_manifest_version, ordinal, relation, \
                parent_generation_sequence, parent_dataset_id, parent_manifest_version, \
                parent_schema_name, parent_schema_version, parent_schema_fingerprint, \
                parent_content_hash \
         FROM analytical_generation_parents \
         ORDER BY child_dataset_id, child_manifest_version, ordinal LIMIT ?1",
    )?;
    let mut rows = statement.query([limit])?;
    let mut result: BTreeMap<(String, u64), Vec<GenerationParentEvidenceRow>> = BTreeMap::new();
    let mut observed = 0_usize;
    while let Some(row) = rows.next()? {
        if observed >= maximum {
            return Err(CatalogError::AnalyticalEvidenceLimitExceeded);
        }
        observed = observed
            .checked_add(1)
            .ok_or(CatalogError::AnalyticalEvidenceLimitExceeded)?;
        let child_dataset: String = row.get(0)?;
        DatasetId::try_from(child_dataset.as_str()).map_err(|_| CatalogError::CorruptCatalog)?;
        let child_version = parse_positive_u64(row.get(1)?)?;
        let ordinal =
            usize::try_from(row.get::<_, i64>(2)?).map_err(|_| CatalogError::CorruptCatalog)?;
        let members = result.entry((child_dataset, child_version)).or_default();
        if ordinal != members.len() {
            return Err(CatalogError::CorruptCatalog);
        }

        let relation = GenerationParentRelation::from_database_name(&row.get::<_, String>(3)?)
            .ok_or(CatalogError::CorruptCatalog)?;
        let parent_sequence = parse_positive_u64(row.get(4)?)?;
        let parent_dataset_key: String = row.get(5)?;
        let parent_dataset = DatasetId::try_from(parent_dataset_key.as_str())
            .map_err(|_| CatalogError::CorruptCatalog)?;
        let parent_version = parse_positive_u64(row.get(6)?)?;
        let parent_schema_name: String = row.get(7)?;
        let parent_schema_version =
            u16::try_from(row.get::<_, i64>(8)?).map_err(|_| CatalogError::CorruptCatalog)?;
        let parent_schema_fingerprint: [u8; 32] = row
            .get::<_, Vec<u8>>(9)?
            .try_into()
            .map_err(|_| CatalogError::CorruptCatalog)?;
        let parent_schema = DatasetSchemaRef::try_new(
            &parent_schema_name,
            market_squawk_domain::SchemaVersion::new(parent_schema_version)
                .map_err(|_| CatalogError::CorruptCatalog)?,
            parent_schema_fingerprint,
        )
        .map_err(|_| CatalogError::CorruptCatalog)?;
        DatasetSchemaRegistry::local()
            .resolve(&parent_schema)
            .map_err(|_| CatalogError::CorruptCatalog)?;
        let parent = DatasetManifestRef::try_new_with_schema(
            parent_dataset,
            parent_version,
            parent_schema,
            parse_sha256(1, row.get::<_, Vec<u8>>(10)?)?,
        )
        .map_err(|_| CatalogError::CorruptCatalog)?;
        members.push(
            GenerationParentEvidenceRow::try_new(parent_sequence, relation, parent)
                .map_err(map_evidence_error)?,
        );
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

type ProviderRelationEvidenceRow = (Box<str>, Box<[u8]>, Sha256Digest, u64);

fn read_provider_relation_rows(
    connection: &Connection,
    maximum: usize,
) -> Result<Vec<ProviderRelationEvidenceRow>, CatalogError> {
    let mut result = Vec::new();
    read_sealed_raw_object_evidence(connection, maximum, &mut result)?;
    read_provider_logical_evidence(connection, maximum, &mut result)?;
    read_provider_option_evidence(connection, maximum, &mut result)?;
    read_direct_provider_input_evidence(connection, maximum, &mut result)?;
    read_market_event_selection_evidence(connection, maximum, &mut result)?;
    Ok(result)
}

fn read_direct_provider_input_evidence(
    connection: &Connection,
    maximum: usize,
    result: &mut Vec<ProviderRelationEvidenceRow>,
) -> Result<(), CatalogError> {
    const CAPTURE_RELATION: &str = "ingest_run_provider_capture_bindings";
    let mut capture_statement = connection.prepare(
        "SELECT run_id, input_ordinal, output_artifact_ordinal, object_input_ordinal,
                binding_digest, source_id
         FROM ingest_run_provider_capture_bindings
         ORDER BY run_id, input_ordinal LIMIT ?1",
    )?;
    let mut rows = capture_statement.query([limit_with_sentinel(maximum)?])?;
    while let Some(row) = rows.next()? {
        require_capacity(result, maximum)?;
        let run = parse_uuid(row.get::<_, String>(0)?)?;
        let input_ordinal: i64 = row.get(1)?;
        let output_ordinal: i64 = row.get(2)?;
        let object_input_ordinal: i64 = row.get(3)?;
        let binding = parse_sha256(1, row.get::<_, Vec<u8>>(4)?)?;
        let source: String = row.get(5)?;
        if !(0..=4095).contains(&input_ordinal)
            || !(0..=1023).contains(&output_ordinal)
            || !(0..=4095).contains(&object_input_ordinal)
            || SourceIdentifier::try_from(source.clone()).is_err()
        {
            return Err(CatalogError::CorruptCatalog);
        }
        let mut digest = ProviderRowDigest::new(CAPTURE_RELATION)?;
        digest.bytes(run.as_bytes())?;
        digest.integer(input_ordinal);
        digest.integer(output_ordinal);
        digest.integer(object_input_ordinal);
        digest.digest(binding);
        digest.text(&source)?;
        result.push(provider_relation_row(
            CAPTURE_RELATION,
            run_ordinal_primary_key(run, input_ordinal)?,
            digest.finish(),
            0,
        ));
    }

    const PUBLICATION_RELATION: &str = "ingest_run_provider_publication_bindings";
    let mut publication_statement = connection.prepare(
        "SELECT run_id, input_ordinal, output_artifact_ordinal, object_input_ordinal,
                publication_digest, publication_kind, source_id,
                response_binding_digest, event_binding_digest, composite_binding_digest,
                option_binding_digest, logical_binding_digest
         FROM ingest_run_provider_publication_bindings
         ORDER BY run_id, input_ordinal LIMIT ?1",
    )?;
    let mut rows = publication_statement.query([limit_with_sentinel(maximum)?])?;
    while let Some(row) = rows.next()? {
        require_capacity(result, maximum)?;
        let run = parse_uuid(row.get::<_, String>(0)?)?;
        let input_ordinal: i64 = row.get(1)?;
        let output_ordinal: i64 = row.get(2)?;
        let object_input_ordinal: i64 = row.get(3)?;
        let publication = parse_sha256(1, row.get::<_, Vec<u8>>(4)?)?;
        let kind: String = row.get(5)?;
        let source: String = row.get(6)?;
        let response: Option<Vec<u8>> = row.get(7)?;
        let event: Option<Vec<u8>> = row.get(8)?;
        let composite: Option<Vec<u8>> = row.get(9)?;
        let option: Option<Vec<u8>> = row.get(10)?;
        let logical: Option<Vec<u8>> = row.get(11)?;
        if !(0..=4095).contains(&input_ordinal)
            || !(0..=1023).contains(&output_ordinal)
            || !(0..=4095).contains(&object_input_ordinal)
            || SourceIdentifier::try_from(source.clone()).is_err()
        {
            return Err(CatalogError::CorruptCatalog);
        }
        let mut digest = ProviderRowDigest::new(PUBLICATION_RELATION)?;
        digest.bytes(run.as_bytes())?;
        digest.integer(input_ordinal);
        digest.integer(output_ordinal);
        digest.integer(object_input_ordinal);
        digest.digest(publication);
        digest.text(&kind)?;
        digest.text(&source)?;
        digest.optional_bytes(response.as_deref())?;
        digest.optional_bytes(event.as_deref())?;
        digest.optional_bytes(composite.as_deref())?;
        digest.optional_bytes(option.as_deref())?;
        digest.optional_bytes(logical.as_deref())?;
        result.push(provider_relation_row(
            PUBLICATION_RELATION,
            run_ordinal_primary_key(run, input_ordinal)?,
            digest.finish(),
            0,
        ));
    }
    Ok(())
}

fn validate_provider_relation_integrity(connection: &Connection) -> Result<(), CatalogError> {
    let invalid: i64 = connection.query_row(
        "SELECT EXISTS (
             SELECT 1
             FROM provider_capture_recovery_capacity AS capacity
             WHERE capacity.singleton != 1
                OR capacity.physical_claims != (SELECT COUNT(*) FROM sealed_raw_objects)
                OR capacity.physical_bytes !=
                   COALESCE((SELECT SUM(object.size_bytes) FROM sealed_raw_objects AS object), 0)
             UNION ALL
             SELECT 1
             FROM provider_logical_publication_bindings AS binding
             WHERE binding.required_family_count != (
                       SELECT COUNT(*)
                       FROM provider_logical_publication_required_families AS family
                       WHERE family.binding_digest=binding.binding_digest)
                OR binding.object_count != (
                       SELECT COUNT(*)
                       FROM provider_logical_publication_objects AS object
                       WHERE object.binding_digest=binding.binding_digest)
                OR binding.partition_count != (
                       SELECT COUNT(*)
                       FROM provider_logical_publication_partitions AS partition
                       WHERE partition.binding_digest=binding.binding_digest)
                OR binding.canonical_partition_count != (
                       SELECT COUNT(*)
                       FROM provider_logical_publication_canonical_expectations AS expected
                       WHERE expected.binding_digest=binding.binding_digest)
                OR (SELECT MIN(family.family_ordinal)
                    FROM provider_logical_publication_required_families AS family
                    WHERE family.binding_digest=binding.binding_digest) != 0
                OR (SELECT MAX(family.family_ordinal)
                    FROM provider_logical_publication_required_families AS family
                    WHERE family.binding_digest=binding.binding_digest)
                   != binding.required_family_count - 1
                OR (SELECT MIN(object.object_ordinal)
                    FROM provider_logical_publication_objects AS object
                    WHERE object.binding_digest=binding.binding_digest) != 0
                OR (SELECT MAX(object.object_ordinal)
                    FROM provider_logical_publication_objects AS object
                    WHERE object.binding_digest=binding.binding_digest)
                   != binding.object_count - 1
                OR (binding.canonical_partition_count > 0 AND (
                       (SELECT MIN(expected.partition_ordinal)
                        FROM provider_logical_publication_canonical_expectations AS expected
                        WHERE expected.binding_digest=binding.binding_digest) != 0
                    OR (SELECT MAX(expected.partition_ordinal)
                        FROM provider_logical_publication_canonical_expectations AS expected
                        WHERE expected.binding_digest=binding.binding_digest)
                       != binding.canonical_partition_count - 1))
             UNION ALL
             SELECT 1
             FROM provider_logical_publication_objects AS object
             WHERE NOT EXISTS (
                 SELECT 1 FROM sealed_raw_objects AS claim
                 WHERE claim.raw_claim_digest=object.raw_claim_digest
                   AND claim.physical_receipt_digest=object.physical_receipt_digest
                   AND claim.raw_claim_kind='logical_object')
             UNION ALL
             SELECT 1
             FROM provider_logical_publication_partitions AS partition
             WHERE NOT EXISTS (
                 SELECT 1
                 FROM provider_logical_publication_required_families AS family
                 JOIN sealed_raw_objects AS claim
                   ON claim.raw_claim_digest=partition.raw_claim_digest
                  AND claim.physical_receipt_digest=partition.physical_receipt_digest
                 WHERE family.binding_digest=partition.binding_digest
                   AND family.family_ordinal=partition.partition_family_ordinal
                   AND family.family=partition.partition_family
                   AND claim.raw_claim_kind='logical_object')
             UNION ALL
             SELECT 1
             FROM provider_logical_publication_canonical_expectations AS expected
             WHERE NOT EXISTS (
                 SELECT 1
                 FROM provider_logical_publication_partitions AS native
                 JOIN provider_logical_publication_partitions AS row_map
                   ON row_map.binding_digest=native.binding_digest
                 WHERE native.binding_digest=expected.binding_digest
                   AND native.partition_family='provider_native'
                   AND native.partition_ordinal=expected.aligned_native_partition
                   AND row_map.partition_family='canonical_row_map'
                   AND row_map.partition_ordinal=expected.aligned_row_map_partition
                   AND native.first_item_ordinal=expected.first_row_ordinal
                   AND native.item_count=expected.row_count
                   AND row_map.first_item_ordinal=expected.first_row_ordinal
                   AND row_map.item_count=expected.row_count)
             UNION ALL
             SELECT 1
             FROM provider_option_market_bindings AS binding
             WHERE NOT EXISTS (
                       SELECT 1
                       FROM provider_option_market_binding_native_lineage AS native
                       WHERE native.option_binding_digest=binding.option_binding_digest
                         AND native.row_count=binding.canonical_row_count)
                OR binding.canonical_row_count != (
                       SELECT COUNT(*)
                       FROM provider_option_market_binding_rows AS row
                       WHERE row.option_binding_digest=binding.option_binding_digest)
                OR (binding.canonical_row_count > 0 AND (
                       (SELECT MIN(row.canonical_row_ordinal)
                        FROM provider_option_market_binding_rows AS row
                        WHERE row.option_binding_digest=binding.option_binding_digest) != 0
                    OR (SELECT MAX(row.canonical_row_ordinal)
                        FROM provider_option_market_binding_rows AS row
                        WHERE row.option_binding_digest=binding.option_binding_digest)
                       != binding.canonical_row_count - 1))
             UNION ALL
             SELECT 1
             FROM provider_market_event_selection_index AS selected
             WHERE NOT EXISTS (
                 SELECT 1
                 FROM ingest_run_provider_publication_bindings AS publication
                 WHERE publication.publication_digest=selected.publication_digest
                   AND publication.publication_kind=selected.publication_kind
                   AND publication.source_id=selected.source_id)
             LIMIT 1
         )",
        [],
        |row| row.get(0),
    )?;
    if invalid == 0 {
        Ok(())
    } else {
        Err(CatalogError::CorruptCatalog)
    }
}

fn read_sealed_raw_object_evidence(
    connection: &Connection,
    maximum: usize,
    result: &mut Vec<ProviderRelationEvidenceRow>,
) -> Result<(), CatalogError> {
    const RELATION: &str = "sealed_raw_objects";
    let mut statement = connection.prepare(
        "SELECT raw_claim_digest, raw_claim_kind, physical_receipt_digest,
                relative_reference, content_digest, size_bytes, integrity_chunk_bytes,
                unit_count, raw_claim_json, recorded_at_ns
         FROM sealed_raw_objects ORDER BY raw_claim_digest LIMIT ?1",
    )?;
    let mut rows = statement.query([limit_with_sentinel(maximum)?])?;
    while let Some(row) = rows.next()? {
        require_capacity(result, maximum)?;
        let claim_digest = parse_sha256(1, row.get::<_, Vec<u8>>(0)?)?;
        let claim_kind: String = row.get(1)?;
        let physical_receipt = parse_sha256(1, row.get::<_, Vec<u8>>(2)?)?;
        let relative_reference: String = row.get(3)?;
        let content_digest = parse_sha256(1, row.get::<_, Vec<u8>>(4)?)?;
        let size_bytes_raw: i64 = row.get(5)?;
        let size_bytes = parse_positive_u64(size_bytes_raw)?;
        let integrity_chunk_bytes_raw: Option<i64> = row.get(6)?;
        let integrity_chunk_bytes = integrity_chunk_bytes_raw
            .map(parse_positive_u64)
            .transpose()?;
        let unit_count_raw: i64 = row.get(7)?;
        let unit_count = parse_positive_u64(unit_count_raw)?;
        let claim_json: String = row.get(8)?;
        let recorded_at_ns: i64 = row.get(9)?;
        if relative_reference.is_empty()
            || relative_reference.len() > 1_024
            || claim_json.len() < 2
            || claim_json.len() > 2_097_152
            || raw_claim_digest(claim_json.as_bytes()).bytes() != claim_digest.bytes()
        {
            return Err(CatalogError::CorruptCatalog);
        }
        let claim: SealedResearchRawClaim = serde_json::from_str(&claim_json)?;
        if serde_json::to_string(&claim)? != claim_json {
            return Err(CatalogError::CorruptCatalog);
        }
        let claim_matches = match &claim {
            SealedResearchRawClaim::JournalSegment(claim) => {
                claim_kind == "journal_segment"
                    && integrity_chunk_bytes.is_none()
                    && size_bytes <= 536_870_912
                    && unit_count <= 64
                    && claim.relative_reference() == relative_reference
                    && claim.content_digest().bytes() == content_digest.bytes()
                    && claim.size_bytes() == size_bytes
                    && u64::try_from(claim.frames().len()).ok() == Some(unit_count)
                    && claim.physical_receipt_digest().bytes() == physical_receipt.bytes()
            }
            SealedResearchRawClaim::LogicalObject(claim) => {
                claim_kind == "logical_object"
                    && integrity_chunk_bytes == Some(claim.integrity_chunk_bytes())
                    && size_bytes <= 68_719_476_736
                    && unit_count <= 4_096
                    && claim.relative_reference() == relative_reference
                    && claim.content_digest().bytes() == content_digest.bytes()
                    && claim.size_bytes() == size_bytes
                    && u64::try_from(claim.chunks().len()).ok() == Some(unit_count)
                    && claim.physical_receipt_digest().bytes() == physical_receipt.bytes()
            }
        };
        if !claim_matches {
            return Err(CatalogError::CorruptCatalog);
        }
        let mut digest = ProviderRowDigest::new(RELATION)?;
        digest.digest(claim_digest);
        digest.text(&claim_kind)?;
        digest.digest(physical_receipt);
        digest.text(&relative_reference)?;
        digest.digest(content_digest);
        digest.integer(size_bytes_raw);
        digest.optional_integer(integrity_chunk_bytes_raw);
        digest.integer(unit_count_raw);
        digest.text(&claim_json)?;
        digest.integer(recorded_at_ns);
        result.push(provider_relation_row(
            RELATION,
            digest_primary_key(claim_digest),
            digest.finish(),
            size_bytes,
        ));
    }
    Ok(())
}

fn read_provider_logical_evidence(
    connection: &Connection,
    maximum: usize,
    result: &mut Vec<ProviderRelationEvidenceRow>,
) -> Result<(), CatalogError> {
    read_provider_logical_bindings(connection, maximum, result)?;
    read_provider_logical_families(connection, maximum, result)?;
    read_provider_logical_objects(connection, maximum, result)?;
    read_provider_logical_partitions(connection, maximum, result)?;
    read_provider_logical_expectations(connection, maximum, result)
}

fn read_provider_logical_bindings(
    connection: &Connection,
    maximum: usize,
    result: &mut Vec<ProviderRelationEvidenceRow>,
) -> Result<(), CatalogError> {
    const RELATION: &str = "provider_logical_publication_bindings";
    let mut statement = connection.prepare(
        "SELECT binding_digest, binding_format_version, source_id, terminal_receipt_digest,
                terminal_json, required_family_count, object_count, partition_count,
                canonical_partition_count, recorded_at_ns
         FROM provider_logical_publication_bindings ORDER BY binding_digest LIMIT ?1",
    )?;
    let mut rows = statement.query([limit_with_sentinel(maximum)?])?;
    while let Some(row) = rows.next()? {
        require_capacity(result, maximum)?;
        let binding = parse_sha256(1, row.get::<_, Vec<u8>>(0)?)?;
        let format: i64 = row.get(1)?;
        let source: String = row.get(2)?;
        SourceIdentifier::try_from(source.clone()).map_err(|_| CatalogError::CorruptCatalog)?;
        let terminal_receipt = parse_sha256(1, row.get::<_, Vec<u8>>(3)?)?;
        let terminal_json: Vec<u8> = row.get(4)?;
        validate_json(&terminal_json, 2_097_152)?;
        let families: i64 = row.get(5)?;
        let objects: i64 = row.get(6)?;
        let partitions: i64 = row.get(7)?;
        let canonical: i64 = row.get(8)?;
        let recorded: i64 = row.get(9)?;
        if format != 1
            || !(1..=6).contains(&families)
            || !(1..=64).contains(&objects)
            || !(1..=4_096).contains(&partitions)
            || !(0..=1_024).contains(&canonical)
        {
            return Err(CatalogError::CorruptCatalog);
        }
        let mut digest = ProviderRowDigest::new(RELATION)?;
        digest.digest(binding);
        digest.integer(format);
        digest.text(&source)?;
        digest.digest(terminal_receipt);
        digest.bytes(&terminal_json)?;
        digest.integer(families);
        digest.integer(objects);
        digest.integer(partitions);
        digest.integer(canonical);
        digest.integer(recorded);
        result.push(provider_relation_row(
            RELATION,
            digest_primary_key(binding),
            digest.finish(),
            0,
        ));
    }
    Ok(())
}

fn read_provider_logical_families(
    connection: &Connection,
    maximum: usize,
    result: &mut Vec<ProviderRelationEvidenceRow>,
) -> Result<(), CatalogError> {
    const RELATION: &str = "provider_logical_publication_required_families";
    let mut statement = connection.prepare(
        "SELECT binding_digest, family_ordinal, family
         FROM provider_logical_publication_required_families
         ORDER BY binding_digest, family_ordinal LIMIT ?1",
    )?;
    let mut rows = statement.query([limit_with_sentinel(maximum)?])?;
    while let Some(row) = rows.next()? {
        require_capacity(result, maximum)?;
        let binding = parse_sha256(1, row.get::<_, Vec<u8>>(0)?)?;
        let ordinal: i64 = row.get(1)?;
        let family: String = row.get(2)?;
        if !(0..=5).contains(&ordinal) || !logical_family(&family) {
            return Err(CatalogError::CorruptCatalog);
        }
        let mut digest = ProviderRowDigest::new(RELATION)?;
        digest.digest(binding);
        digest.integer(ordinal);
        digest.text(&family)?;
        result.push(provider_relation_row(
            RELATION,
            digest_ordinal_primary_key(binding, ordinal)?,
            digest.finish(),
            0,
        ));
    }
    Ok(())
}

fn read_provider_logical_objects(
    connection: &Connection,
    maximum: usize,
    result: &mut Vec<ProviderRelationEvidenceRow>,
) -> Result<(), CatalogError> {
    const RELATION: &str = "provider_logical_publication_objects";
    let mut statement = connection.prepare(
        "SELECT binding_digest, object_ordinal, object_role, semantic_identity,
                raw_claim_digest, physical_receipt_digest
         FROM provider_logical_publication_objects
         ORDER BY binding_digest, object_ordinal LIMIT ?1",
    )?;
    let mut rows = statement.query([limit_with_sentinel(maximum)?])?;
    while let Some(row) = rows.next()? {
        require_capacity(result, maximum)?;
        let binding = parse_sha256(1, row.get::<_, Vec<u8>>(0)?)?;
        let ordinal: i64 = row.get(1)?;
        let role: String = row.get(2)?;
        let semantic = parse_sha256(1, row.get::<_, Vec<u8>>(3)?)?;
        let raw_claim = parse_sha256(1, row.get::<_, Vec<u8>>(4)?)?;
        let physical = parse_sha256(1, row.get::<_, Vec<u8>>(5)?)?;
        if !(0..=63).contains(&ordinal)
            || !matches!(
                role.as_str(),
                "catalog" | "provider_payload" | "expanded_payload" | "provider_component"
            )
        {
            return Err(CatalogError::CorruptCatalog);
        }
        let mut digest = ProviderRowDigest::new(RELATION)?;
        digest.digest(binding);
        digest.integer(ordinal);
        digest.text(&role)?;
        digest.digest(semantic);
        digest.digest(raw_claim);
        digest.digest(physical);
        result.push(provider_relation_row(
            RELATION,
            digest_ordinal_primary_key(binding, ordinal)?,
            digest.finish(),
            0,
        ));
    }
    Ok(())
}

fn read_provider_logical_partitions(
    connection: &Connection,
    maximum: usize,
    result: &mut Vec<ProviderRelationEvidenceRow>,
) -> Result<(), CatalogError> {
    const RELATION: &str = "provider_logical_publication_partitions";
    let mut statement = connection.prepare(
        "SELECT binding_digest, partition_family_ordinal, partition_family,
                partition_ordinal, first_item_ordinal, item_count, schema_identity,
                semantic_digest, raw_claim_digest, physical_receipt_digest
         FROM provider_logical_publication_partitions
         ORDER BY binding_digest, partition_family_ordinal, partition_ordinal LIMIT ?1",
    )?;
    let mut rows = statement.query([limit_with_sentinel(maximum)?])?;
    while let Some(row) = rows.next()? {
        require_capacity(result, maximum)?;
        let binding = parse_sha256(1, row.get::<_, Vec<u8>>(0)?)?;
        let family_ordinal: i64 = row.get(1)?;
        let family: String = row.get(2)?;
        let partition_ordinal: i64 = row.get(3)?;
        let first_item: i64 = row.get(4)?;
        let item_count: i64 = row.get(5)?;
        let schema = parse_sha256(1, row.get::<_, Vec<u8>>(6)?)?;
        let semantic = parse_sha256(1, row.get::<_, Vec<u8>>(7)?)?;
        let raw_claim = parse_sha256(1, row.get::<_, Vec<u8>>(8)?)?;
        let physical = parse_sha256(1, row.get::<_, Vec<u8>>(9)?)?;
        if !(0..=5).contains(&family_ordinal)
            || !logical_family(&family)
            || !(0..=4_095).contains(&partition_ordinal)
            || first_item < 0
            || !(1..=4_294_967_295).contains(&item_count)
        {
            return Err(CatalogError::CorruptCatalog);
        }
        let mut digest = ProviderRowDigest::new(RELATION)?;
        digest.digest(binding);
        digest.integer(family_ordinal);
        digest.text(&family)?;
        digest.integer(partition_ordinal);
        digest.integer(first_item);
        digest.integer(item_count);
        digest.digest(schema);
        digest.digest(semantic);
        digest.digest(raw_claim);
        digest.digest(physical);
        result.push(provider_relation_row(
            RELATION,
            digest_pair_ordinal_primary_key(binding, family_ordinal, partition_ordinal)?,
            digest.finish(),
            0,
        ));
    }
    Ok(())
}

fn read_provider_logical_expectations(
    connection: &Connection,
    maximum: usize,
    result: &mut Vec<ProviderRelationEvidenceRow>,
) -> Result<(), CatalogError> {
    const RELATION: &str = "provider_logical_publication_canonical_expectations";
    let mut statement = connection.prepare(
        "SELECT binding_digest, partition_ordinal, first_row_ordinal, row_count,
                schema_identity, semantic_digest, aligned_native_partition,
                aligned_row_map_partition
         FROM provider_logical_publication_canonical_expectations
         ORDER BY binding_digest, partition_ordinal LIMIT ?1",
    )?;
    let mut rows = statement.query([limit_with_sentinel(maximum)?])?;
    while let Some(row) = rows.next()? {
        require_capacity(result, maximum)?;
        let binding = parse_sha256(1, row.get::<_, Vec<u8>>(0)?)?;
        let ordinal: i64 = row.get(1)?;
        let first: i64 = row.get(2)?;
        let count: i64 = row.get(3)?;
        let schema = parse_sha256(1, row.get::<_, Vec<u8>>(4)?)?;
        let semantic = parse_sha256(1, row.get::<_, Vec<u8>>(5)?)?;
        let native: i64 = row.get(6)?;
        let row_map: i64 = row.get(7)?;
        if !(0..=1_023).contains(&ordinal)
            || first < 0
            || !(1..=4_294_967_295).contains(&count)
            || !(0..=4_095).contains(&native)
            || !(0..=4_095).contains(&row_map)
        {
            return Err(CatalogError::CorruptCatalog);
        }
        let mut digest = ProviderRowDigest::new(RELATION)?;
        digest.digest(binding);
        digest.integer(ordinal);
        digest.integer(first);
        digest.integer(count);
        digest.digest(schema);
        digest.digest(semantic);
        digest.integer(native);
        digest.integer(row_map);
        result.push(provider_relation_row(
            RELATION,
            digest_ordinal_primary_key(binding, ordinal)?,
            digest.finish(),
            0,
        ));
    }
    Ok(())
}

fn read_provider_option_evidence(
    connection: &Connection,
    maximum: usize,
    result: &mut Vec<ProviderRelationEvidenceRow>,
) -> Result<(), CatalogError> {
    read_provider_option_bindings(connection, maximum, result)?;
    read_provider_option_native_lineage(connection, maximum, result)?;
    read_provider_option_rows(connection, maximum, result)
}

fn read_provider_option_bindings(
    connection: &Connection,
    maximum: usize,
    result: &mut Vec<ProviderRelationEvidenceRow>,
) -> Result<(), CatalogError> {
    const RELATION: &str = "provider_option_market_bindings";
    let mut statement = connection.prepare(
        "SELECT option_binding_digest, binding_format_version, capture_observation_digest,
                sealed_capture_receipt_digest, publication_kind,
                canonical_schema_fingerprint, canonical_content_digest, canonical_row_count,
                scope_json, scope_digest, completeness_json, completeness_digest,
                filter_json, filter_digest, underlying_instrument_id, available_at_ns,
                received_at_ns, ingested_at_ns, disposition, row_mapping_digest, recorded_at_ns
         FROM provider_option_market_bindings ORDER BY option_binding_digest LIMIT ?1",
    )?;
    let mut rows = statement.query([limit_with_sentinel(maximum)?])?;
    while let Some(row) = rows.next()? {
        require_capacity(result, maximum)?;
        let binding = parse_sha256(1, row.get::<_, Vec<u8>>(0)?)?;
        let format: i64 = row.get(1)?;
        let capture = parse_sha256(1, row.get::<_, Vec<u8>>(2)?)?;
        let sealed_receipt = parse_sha256(1, row.get::<_, Vec<u8>>(3)?)?;
        let kind: String = row.get(4)?;
        let schema = parse_sha256(1, row.get::<_, Vec<u8>>(5)?)?;
        let content = parse_sha256(1, row.get::<_, Vec<u8>>(6)?)?;
        let row_count: i64 = row.get(7)?;
        let scope: Vec<u8> = row.get(8)?;
        let scope_digest = parse_sha256(1, row.get::<_, Vec<u8>>(9)?)?;
        let completeness: Vec<u8> = row.get(10)?;
        let completeness_digest = parse_sha256(1, row.get::<_, Vec<u8>>(11)?)?;
        let filter: Vec<u8> = row.get(12)?;
        let filter_digest = parse_sha256(1, row.get::<_, Vec<u8>>(13)?)?;
        let underlying: Vec<u8> = row.get(14)?;
        let available: i64 = row.get(15)?;
        let received: i64 = row.get(16)?;
        let ingested: i64 = row.get(17)?;
        let disposition: String = row.get(18)?;
        let row_mapping = parse_sha256(1, row.get::<_, Vec<u8>>(19)?)?;
        let recorded: i64 = row.get(20)?;
        validate_json(&scope, 67_108_864)?;
        validate_json(&completeness, 1_048_576)?;
        validate_json(&filter, 4_194_304)?;
        let underlying = parse_uuid_blob(&underlying)?;
        if format != 1
            || !matches!(kind.as_str(), "option_snapshots" | "option_expirations")
            || !(0..=100_000).contains(&row_count)
            || available > ingested
            || received > ingested
            || !matches!(disposition.as_str(), "complete" | "unavailable")
        {
            return Err(CatalogError::CorruptCatalog);
        }
        let mut digest = ProviderRowDigest::new(RELATION)?;
        digest.digest(binding);
        digest.integer(format);
        digest.digest(capture);
        digest.digest(sealed_receipt);
        digest.text(&kind)?;
        digest.digest(schema);
        digest.digest(content);
        digest.integer(row_count);
        digest.bytes(&scope)?;
        digest.digest(scope_digest);
        digest.bytes(&completeness)?;
        digest.digest(completeness_digest);
        digest.bytes(&filter)?;
        digest.digest(filter_digest);
        digest.bytes(underlying.as_bytes())?;
        digest.integer(available);
        digest.integer(received);
        digest.integer(ingested);
        digest.text(&disposition)?;
        digest.digest(row_mapping);
        digest.integer(recorded);
        result.push(provider_relation_row(
            RELATION,
            digest_primary_key(binding),
            digest.finish(),
            0,
        ));
    }
    Ok(())
}

fn read_provider_option_native_lineage(
    connection: &Connection,
    maximum: usize,
    result: &mut Vec<ProviderRelationEvidenceRow>,
) -> Result<(), CatalogError> {
    const RELATION: &str = "provider_option_market_binding_native_lineage";
    let mut statement = connection.prepare(
        "SELECT option_binding_digest, schema_version, implementation, schema_fingerprint,
                row_count, batch_digest, batch_sidecar_payload, batch_sidecar_digest
         FROM provider_option_market_binding_native_lineage
         ORDER BY option_binding_digest LIMIT ?1",
    )?;
    let mut rows = statement.query([limit_with_sentinel(maximum)?])?;
    while let Some(row) = rows.next()? {
        require_capacity(result, maximum)?;
        let binding = parse_sha256(1, row.get::<_, Vec<u8>>(0)?)?;
        let schema_version: i64 = row.get(1)?;
        let implementation: String = row.get(2)?;
        let schema = parse_sha256(1, row.get::<_, Vec<u8>>(3)?)?;
        let row_count: i64 = row.get(4)?;
        let batch = parse_sha256(1, row.get::<_, Vec<u8>>(5)?)?;
        let sidecar: Vec<u8> = row.get(6)?;
        let sidecar_digest = parse_sha256(1, row.get::<_, Vec<u8>>(7)?)?;
        if schema_version <= 0
            || implementation.is_empty()
            || implementation.len() > 128
            || !(0..=100_000).contains(&row_count)
            || sidecar.is_empty()
            || sidecar.len() > 4_194_304
            || Sha256Digest::new(Sha256::digest(&sidecar).into()) != sidecar_digest
        {
            return Err(CatalogError::CorruptCatalog);
        }
        let mut digest = ProviderRowDigest::new(RELATION)?;
        digest.digest(binding);
        digest.integer(schema_version);
        digest.text(&implementation)?;
        digest.digest(schema);
        digest.integer(row_count);
        digest.digest(batch);
        digest.bytes(&sidecar)?;
        digest.digest(sidecar_digest);
        result.push(provider_relation_row(
            RELATION,
            digest_primary_key(binding),
            digest.finish(),
            0,
        ));
    }
    Ok(())
}

fn read_provider_option_rows(
    connection: &Connection,
    maximum: usize,
    result: &mut Vec<ProviderRelationEvidenceRow>,
) -> Result<(), CatalogError> {
    const RELATION: &str = "provider_option_market_binding_rows";
    let mut statement = connection.prepare(
        "SELECT option_binding_digest, capture_observation_digest, canonical_row_ordinal,
                canonical_row_digest, native_semantic_payload, native_semantic_digest,
                capture_page_ordinal, physical_frame_ordinal, payload_digest,
                received_at_ns, source_sequence
         FROM provider_option_market_binding_rows
         ORDER BY option_binding_digest, canonical_row_ordinal LIMIT ?1",
    )?;
    let mut rows = statement.query([limit_with_sentinel(maximum)?])?;
    while let Some(row) = rows.next()? {
        require_capacity(result, maximum)?;
        let binding = parse_sha256(1, row.get::<_, Vec<u8>>(0)?)?;
        let capture = parse_sha256(1, row.get::<_, Vec<u8>>(1)?)?;
        let ordinal: i64 = row.get(2)?;
        let canonical = parse_sha256(1, row.get::<_, Vec<u8>>(3)?)?;
        let native_payload: Vec<u8> = row.get(4)?;
        let native = parse_sha256(1, row.get::<_, Vec<u8>>(5)?)?;
        let page: i64 = row.get(6)?;
        let frame: i64 = row.get(7)?;
        let payload = parse_sha256(1, row.get::<_, Vec<u8>>(8)?)?;
        let received: i64 = row.get(9)?;
        let source_sequence: Option<Vec<u8>> = row.get(10)?;
        if !(0..=99_999).contains(&ordinal)
            || native_payload.is_empty()
            || native_payload.len() > 65_536
            || Sha256Digest::new(Sha256::digest(&native_payload).into()) != native
            || !(0..=63).contains(&page)
            || !(0..=63).contains(&frame)
        {
            return Err(CatalogError::CorruptCatalog);
        }
        validate_optional_u64_blob(source_sequence.as_deref())?;
        let mut digest = ProviderRowDigest::new(RELATION)?;
        digest.digest(binding);
        digest.digest(capture);
        digest.integer(ordinal);
        digest.digest(canonical);
        digest.bytes(&native_payload)?;
        digest.digest(native);
        digest.integer(page);
        digest.integer(frame);
        digest.digest(payload);
        digest.integer(received);
        digest.optional_bytes(source_sequence.as_deref())?;
        result.push(provider_relation_row(
            RELATION,
            digest_ordinal_primary_key(binding, ordinal)?,
            digest.finish(),
            0,
        ));
    }
    Ok(())
}

fn read_market_event_selection_evidence(
    connection: &Connection,
    maximum: usize,
    result: &mut Vec<ProviderRelationEvidenceRow>,
) -> Result<(), CatalogError> {
    const RELATION: &str = "provider_market_event_selection_index";
    let mut statement = connection.prepare(
        "SELECT publication_digest, publication_kind, publication_row_ordinal,
                component_kind, component_binding_digest, component_row_ordinal,
                canonical_event_digest, source_id, instrument_id, venue_id, event_kind,
                source_timestamp_ns, received_at_ns, available_at_ns, ingested_at_ns,
                connection_generation_be, source_sequence_be, provider_event_id,
                coordinate_digest
         FROM provider_market_event_selection_index
         ORDER BY publication_digest, publication_row_ordinal LIMIT ?1",
    )?;
    let mut rows = statement.query([limit_with_sentinel(maximum)?])?;
    while let Some(row) = rows.next()? {
        require_capacity(result, maximum)?;
        let publication = parse_sha256(1, row.get::<_, Vec<u8>>(0)?)?;
        let publication_kind: String = row.get(1)?;
        let publication_ordinal: i64 = row.get(2)?;
        let component_kind: String = row.get(3)?;
        let component = parse_sha256(1, row.get::<_, Vec<u8>>(4)?)?;
        let component_ordinal: i64 = row.get(5)?;
        let canonical = parse_sha256(1, row.get::<_, Vec<u8>>(6)?)?;
        let source: String = row.get(7)?;
        SourceIdentifier::try_from(source.clone()).map_err(|_| CatalogError::CorruptCatalog)?;
        let instrument_bytes: Vec<u8> = row.get(8)?;
        let instrument = parse_uuid_blob(&instrument_bytes)?;
        let venue: String = row.get(9)?;
        let event_kind: String = row.get(10)?;
        let source_timestamp: Option<i64> = row.get(11)?;
        let received: i64 = row.get(12)?;
        let available: i64 = row.get(13)?;
        let ingested: i64 = row.get(14)?;
        let connection_generation: Vec<u8> = row.get(15)?;
        let source_sequence: Option<Vec<u8>> = row.get(16)?;
        let provider_event_id: String = row.get(17)?;
        let coordinate = parse_sha256(1, row.get::<_, Vec<u8>>(18)?)?;
        validate_nonzero_u64_blob(&connection_generation)?;
        validate_optional_u64_blob(source_sequence.as_deref())?;
        let kind_matches = match publication_kind.as_str() {
            "response_market_event" => component_kind == "response",
            "event_microbatch" => component_kind == "stream",
            "composite_response_event" => {
                matches!(component_kind.as_str(), "response" | "stream")
            }
            _ => false,
        };
        if !kind_matches
            || !(0..=127).contains(&publication_ordinal)
            || !(0..=63).contains(&component_ordinal)
            || (publication_kind != "composite_response_event"
                && publication_ordinal != component_ordinal)
            || venue.is_empty()
            || venue.len() > 128
            || !matches!(
                event_kind.as_str(),
                "trade"
                    | "quote"
                    | "book_snapshot"
                    | "book_delta"
                    | "auction"
                    | "trading_halt"
                    | "instrument_status"
                    | "corporate_action"
            )
            || received > available
            || available > ingested
            || provider_event_id.is_empty()
            || provider_event_id.len() > 512
        {
            return Err(CatalogError::CorruptCatalog);
        }
        let mut digest = ProviderRowDigest::new(RELATION)?;
        digest.digest(publication);
        digest.text(&publication_kind)?;
        digest.integer(publication_ordinal);
        digest.text(&component_kind)?;
        digest.digest(component);
        digest.integer(component_ordinal);
        digest.digest(canonical);
        digest.text(&source)?;
        digest.bytes(instrument.as_bytes())?;
        digest.text(&venue)?;
        digest.text(&event_kind)?;
        digest.optional_integer(source_timestamp);
        digest.integer(received);
        digest.integer(available);
        digest.integer(ingested);
        digest.bytes(&connection_generation)?;
        digest.optional_bytes(source_sequence.as_deref())?;
        digest.text(&provider_event_id)?;
        digest.digest(coordinate);
        result.push(provider_relation_row(
            RELATION,
            digest_ordinal_primary_key(publication, publication_ordinal)?,
            digest.finish(),
            0,
        ));
    }
    Ok(())
}

struct ProviderRowDigest(Sha256);

impl ProviderRowDigest {
    fn new(relation: &str) -> Result<Self, CatalogError> {
        let mut digest = Sha256::new();
        digest.update(b"market-squawk/provider-catalog-relation-row/v1");
        hash_length_prefixed(&mut digest, relation.as_bytes())?;
        Ok(Self(digest))
    }

    fn integer(&mut self, value: i64) {
        self.0.update([1]);
        self.0.update(value.to_be_bytes());
    }

    fn optional_integer(&mut self, value: Option<i64>) {
        match value {
            Some(value) => {
                self.0.update([2, 1]);
                self.0.update(value.to_be_bytes());
            }
            None => self.0.update([2, 0]),
        }
    }

    fn digest(&mut self, value: Sha256Digest) {
        self.0.update([3]);
        self.0.update(value.bytes());
    }

    fn text(&mut self, value: &str) -> Result<(), CatalogError> {
        self.0.update([4]);
        hash_length_prefixed(&mut self.0, value.as_bytes())
    }

    fn bytes(&mut self, value: &[u8]) -> Result<(), CatalogError> {
        self.0.update([5]);
        hash_length_prefixed(&mut self.0, value)
    }

    fn optional_bytes(&mut self, value: Option<&[u8]>) -> Result<(), CatalogError> {
        match value {
            Some(value) => {
                self.0.update([6, 1]);
                hash_length_prefixed(&mut self.0, value)
            }
            None => {
                self.0.update([6, 0]);
                Ok(())
            }
        }
    }

    fn finish(self) -> Sha256Digest {
        Sha256Digest::new(self.0.finalize().into())
    }
}

fn provider_relation_row(
    relation: &'static str,
    primary_key: Box<[u8]>,
    row_content_digest: Sha256Digest,
    accounted_object_bytes: u64,
) -> ProviderRelationEvidenceRow {
    (
        relation.into(),
        primary_key,
        row_content_digest,
        accounted_object_bytes,
    )
}

fn digest_primary_key(digest: Sha256Digest) -> Box<[u8]> {
    Box::from(digest.bytes())
}

fn digest_ordinal_primary_key(
    digest: Sha256Digest,
    ordinal: i64,
) -> Result<Box<[u8]>, CatalogError> {
    let ordinal = u64::try_from(ordinal).map_err(|_| CatalogError::CorruptCatalog)?;
    let mut key = [0_u8; 40];
    key[..32].copy_from_slice(&digest.bytes());
    key[32..].copy_from_slice(&ordinal.to_be_bytes());
    Ok(Box::from(key))
}

fn run_ordinal_primary_key(run: Uuid, ordinal: i64) -> Result<Box<[u8]>, CatalogError> {
    let ordinal = u64::try_from(ordinal).map_err(|_| CatalogError::CorruptCatalog)?;
    let mut key = [0_u8; 24];
    key[..16].copy_from_slice(run.as_bytes());
    key[16..].copy_from_slice(&ordinal.to_be_bytes());
    Ok(Box::from(key))
}

fn digest_pair_ordinal_primary_key(
    digest: Sha256Digest,
    first: i64,
    second: i64,
) -> Result<Box<[u8]>, CatalogError> {
    let first = u64::try_from(first).map_err(|_| CatalogError::CorruptCatalog)?;
    let second = u64::try_from(second).map_err(|_| CatalogError::CorruptCatalog)?;
    let mut key = [0_u8; 48];
    key[..32].copy_from_slice(&digest.bytes());
    key[32..40].copy_from_slice(&first.to_be_bytes());
    key[40..].copy_from_slice(&second.to_be_bytes());
    Ok(Box::from(key))
}

fn hash_length_prefixed(digest: &mut Sha256, value: &[u8]) -> Result<(), CatalogError> {
    let length =
        u64::try_from(value.len()).map_err(|_| CatalogError::AnalyticalEvidenceLimitExceeded)?;
    digest.update(length.to_be_bytes());
    digest.update(value);
    Ok(())
}

fn validate_json(value: &[u8], maximum: usize) -> Result<(), CatalogError> {
    if value.len() < 2 || value.len() > maximum {
        return Err(CatalogError::CorruptCatalog);
    }
    let mut deserializer = serde_json::Deserializer::from_slice(value);
    let _: serde::de::IgnoredAny = serde::Deserialize::deserialize(&mut deserializer)?;
    deserializer.end().map_err(Into::into)
}

fn parse_uuid_blob(value: &[u8]) -> Result<Uuid, CatalogError> {
    let value = Uuid::from_slice(value).map_err(|_| CatalogError::CorruptCatalog)?;
    if value.is_nil() {
        Err(CatalogError::CorruptCatalog)
    } else {
        Ok(value)
    }
}

fn validate_nonzero_u64_blob(value: &[u8]) -> Result<(), CatalogError> {
    let value: [u8; 8] = value.try_into().map_err(|_| CatalogError::CorruptCatalog)?;
    if u64::from_be_bytes(value) == 0 {
        Err(CatalogError::CorruptCatalog)
    } else {
        Ok(())
    }
}

fn validate_optional_u64_blob(value: Option<&[u8]>) -> Result<(), CatalogError> {
    value.map_or(Ok(()), |value| {
        <[u8; 8]>::try_from(value)
            .map(|_| ())
            .map_err(|_| CatalogError::CorruptCatalog)
    })
}

fn logical_family(value: &str) -> bool {
    matches!(
        value,
        "decoded_event"
            | "provider_native"
            | "canonical_row_map"
            | "resolver_assertion"
            | "resolver_outcome"
            | "resolver_conflict"
    )
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

fn parse_build_spec_digest(value: Vec<u8>) -> Result<DatasetBuildSpecDigest, CatalogError> {
    DatasetBuildSpecDigest::try_new(value.try_into().map_err(|_| CatalogError::CorruptCatalog)?)
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
