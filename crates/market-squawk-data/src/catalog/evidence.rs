//! Consistent bounded relational snapshots for analytical backup authority.

use crate::authority_transition::evidence::canonical::EvidenceDigest;
use tokio_util::sync::CancellationToken;

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

mod generations;
mod market_events;
mod physical;
use crate::authority_transition::evidence::{
    ArtifactEvidenceRow, CatalogEvidenceSnapshot, EvidenceError, EvidenceSnapshotRequest,
    GenerationEvidenceHeader, GenerationObjectEvidenceRow, GenerationParentEvidenceRow,
    ManifestEvidenceRow, MarketEventArchiveEvidenceRow, PhysicalArtifactEvidence,
    ProviderCatalogRelation, ProviderCatalogRelationEvidenceRow, QueryArtifactEvidenceRow,
};
use crate::manifest::{DatasetBuildSpecDigest, GenerationParentRelation};
use crate::{
    DatasetId, DatasetManifestRef, DatasetSchemaRef, DatasetSchemaRegistry, GenerationKind,
    Sha256Digest,
};

impl Catalog {
    /// Computes a compact exact summary under one consistent live read transaction.
    pub(crate) fn analytical_evidence_snapshot(
        &self,
        request: EvidenceSnapshotRequest,
        cancellation: &CancellationToken,
    ) -> Result<(AuthoritySnapshot, CatalogEvidenceSnapshot), CatalogError> {
        with_cancellation(&self.connection, cancellation, || {
            let transaction = self.connection.unchecked_transaction()?;
            let snapshot = evidence_snapshot(&transaction, request, cancellation)?;
            transaction.commit()?;
            Ok(snapshot)
        })
    }

    /// The consumer runs inside the exact retained immutable catalog's read transaction.
    pub(crate) fn verified_backup_evidence<T>(
        backup: &VerifiedBackupCatalog,
        request: EvidenceSnapshotRequest,
        cancellation: &CancellationToken,
        consume: impl FnOnce(&Connection, &CatalogEvidenceSnapshot) -> Result<T, CatalogError>,
    ) -> Result<(AuthoritySnapshot, CatalogEvidenceSnapshot, T), CatalogError> {
        backup.revalidate()?;
        let connection = open_immutable_backup(backup.location().path())?;
        let sqlite_length_limit = i32::try_from(MAX_SQLITE_RECORD_BYTES)
            .map_err(|_| CatalogError::InvalidConfiguration)?;
        connection.set_limit(Limit::SQLITE_LIMIT_LENGTH, sqlite_length_limit)?;
        connection.pragma_update(None, "trusted_schema", "OFF")?;
        connection.pragma_update(None, "query_only", "ON")?;
        // SQLite's existing bounded page cache and file-backed temporary sorter keep the
        // working set independent of the retained history, including ORDER BY spill.
        let result = with_cancellation(&connection, cancellation, || {
            verify_migration_identities(&connection)?;
            verify_integrity(&connection)?;
            backup.revalidate()?;
            let transaction = connection.unchecked_transaction()?;
            let (authority, evidence) = evidence_snapshot(&transaction, request, cancellation)?;
            let result = consume(&transaction, &evidence)?;
            transaction.commit()?;
            verify_migration_identities(&connection)?;
            verify_integrity(&connection)?;
            backup.revalidate()?;
            Ok((authority, evidence, result))
        });
        connection.close().map_err(|(_, error)| error)?;
        backup.revalidate()?;
        result
    }
}

fn with_cancellation<T>(
    connection: &Connection,
    cancellation: &CancellationToken,
    consume: impl FnOnce() -> Result<T, CatalogError>,
) -> Result<T, CatalogError> {
    check_cancellation(cancellation)?;
    let previous_temp_store: i64 =
        connection.pragma_query_value(None, "temp_store", |row| row.get(0))?;
    connection.pragma_update(None, "temp_store", "FILE")?;
    let token = cancellation.clone();
    connection.progress_handler(1_000, Some(move || token.is_cancelled()))?;
    let result = consume();
    let cleanup = connection.progress_handler::<fn() -> bool>(0, None);
    let restore_temp_store = connection.pragma_update(None, "temp_store", previous_temp_store);
    check_cancellation(cancellation)?;
    cleanup?;
    restore_temp_store?;
    result
}
fn check_cancellation(cancellation: &CancellationToken) -> Result<(), CatalogError> {
    if cancellation.is_cancelled() {
        Err(CatalogError::AnalyticalEvidenceCancelled)
    } else {
        Ok(())
    }
}
fn count(connection: &Connection, table: &str) -> Result<u64, CatalogError> {
    // table is always one of the closed code-owned relation names.
    let value: i64 = connection.query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
        row.get(0)
    })?;
    parse_nonnegative_u64(value)
}
fn add(total: &mut u64, value: u64) -> Result<(), CatalogError> {
    *total = total
        .checked_add(value)
        .ok_or(CatalogError::AnalyticalEvidenceLimitExceeded)?;
    Ok(())
}
fn check_count(expected: u64, observed: u64) -> Result<(), CatalogError> {
    if expected == observed {
        Ok(())
    } else {
        Err(CatalogError::CorruptCatalog)
    }
}
fn evidence_snapshot(
    transaction: &Transaction<'_>,
    request: EvidenceSnapshotRequest,
    cancellation: &CancellationToken,
) -> Result<(AuthoritySnapshot, CatalogEvidenceSnapshot), CatalogError> {
    check_cancellation(cancellation)?;
    let authority = read_authority_snapshot_without_endpoint(transaction)?;
    generations::validate_relations(transaction, request)?;
    validate_provider_relation_integrity(transaction)?;
    market_events::validate_integrity(transaction)?;
    let mut digest = EvidenceDigest::new(request.cutoff());
    let mut references = 0;
    let mut physical_count = 0;
    let mut total_bytes = 0;
    let mut physical_bytes = 0;
    let mut account = |bytes: u64| -> Result<(), CatalogError> {
        if bytes > request.limits().max_object_bytes() {
            return Err(CatalogError::AnalyticalEvidenceInvalid);
        }
        add(&mut physical_count, 1)?;
        add(&mut physical_bytes, bytes)?;
        if physical_bytes > request.limits().max_total_bytes() {
            return Err(CatalogError::AnalyticalEvidenceLimitExceeded);
        }
        Ok(())
    };
    let expected = count(transaction, "artifacts")?;
    digest
        .section("artifacts", expected)
        .map_err(map_evidence_error)?;
    let mut observed = 0;
    read_artifacts(transaction, &mut |row| {
        check_cancellation(cancellation)?;
        digest.artifact(&row).map_err(map_evidence_error)?;
        account(row.size_bytes())?;
        add(&mut observed, 1)
    })?;
    check_count(expected, observed)?;
    add(&mut references, observed)?;
    let expected = count(transaction, "dataset_manifests")?;
    digest
        .section("manifests", expected)
        .map_err(map_evidence_error)?;
    observed = 0;
    read_manifests(transaction, &mut |row| {
        check_cancellation(cancellation)?;
        digest.manifest(&row).map_err(map_evidence_error)?;
        add(&mut observed, 1)
    })?;
    check_count(expected, observed)?;
    add(&mut references, observed)?;
    generations::stream(transaction, &mut digest, &mut references, cancellation)?;
    let expected=parse_nonnegative_u64(transaction.query_row("SELECT COUNT(*) FROM query_artifact_reservations AS reservations JOIN query_artifact_results AS results USING(reservation_id) WHERE reservations.state='published' AND reservations.expires_at_ns>?1",[request.cutoff().unix_nanos()],|row|row.get::<_, i64>(0))?)?;
    digest
        .section("live-query-artifacts", expected)
        .map_err(map_evidence_error)?;
    observed = 0;
    read_query_artifacts(transaction, request.cutoff(), &mut |row| {
        check_cancellation(cancellation)?;
        digest.query_artifact(&row).map_err(map_evidence_error)?;
        account(row.size_bytes())?;
        add(&mut observed, 1)
    })?;
    check_count(expected, observed)?;
    add(&mut references, observed)?;
    let expected = count(transaction, "market_event_archive_objects")?;
    digest
        .section("market-event-archives", expected)
        .map_err(map_evidence_error)?;
    observed = 0;
    market_events::read_archives(transaction, &mut |row| {
        check_cancellation(cancellation)?;
        digest.archive(&row).map_err(map_evidence_error)?;
        account(row.size_bytes())?;
        add(&mut observed, 1)
    })?;
    check_count(expected, observed)?;
    add(&mut references, observed)?;
    {
        let mut prepared = transaction
            .prepare("SELECT size_bytes FROM sec_prepared_indexes ORDER BY generation_digest")?;
        let mut rows = prepared.query([])?;
        while let Some(row) = rows.next()? {
            check_cancellation(cancellation)?;
            account(parse_positive_u64(row.get(0)?)?)?;
        }
    }
    add(&mut total_bytes, physical_bytes)?;
    let expected = ProviderCatalogRelation::ALL
        .iter()
        .try_fold(0_u64, |sum, relation| {
            sum.checked_add(count(transaction, relation.database_name())?)
                .ok_or(CatalogError::AnalyticalEvidenceLimitExceeded)
        })?;
    digest
        .section("provider-catalog-relations", expected)
        .map_err(map_evidence_error)?;
    let mut sink = ProviderEvidenceSink {
        digest: &mut digest,
        cancellation,
        request,
        previous: None,
        count: 0,
        total_bytes: &mut total_bytes,
    };
    read_provider_relation_rows(transaction, &mut sink)?;
    check_count(expected, sink.count)?;
    add(&mut references, sink.count)?;
    let evidence = CatalogEvidenceSnapshot::new(
        request,
        physical_count,
        physical_bytes,
        references,
        total_bytes,
        digest.finish().map_err(map_evidence_error)?,
    );
    Ok((authority, evidence))
}

fn read_artifacts(
    connection: &Connection,
    consume: &mut impl FnMut(ArtifactEvidenceRow) -> Result<(), CatalogError>,
) -> Result<(), CatalogError> {
    let mut statement = connection.prepare(
        "SELECT artifact_id, run_id, publication_ordinal, relative_reference,
                content_algorithm, content_digest, size_bytes
         FROM artifacts ORDER BY lower(artifact_id)",
    )?;
    let mut rows = statement.query([])?;
    while let Some(row) = rows.next()? {
        let ordinal =
            u16::try_from(row.get::<_, i64>(2)?).map_err(|_| CatalogError::CorruptCatalog)?;
        let algorithm: i64 = row.get(4)?;
        consume(
            ArtifactEvidenceRow::try_new(
                parse_uuid(row.get::<_, String>(0)?)?,
                parse_uuid(row.get::<_, String>(1)?)?,
                ordinal,
                row.get::<_, String>(3)?,
                parse_sha256(algorithm, row.get::<_, Vec<u8>>(5)?)?,
                parse_positive_u64(row.get(6)?)?,
            )
            .map_err(map_evidence_error)?,
        )?;
    }
    Ok(())
}

fn read_manifests(
    connection: &Connection,
    consume: &mut impl FnMut(ManifestEvidenceRow) -> Result<(), CatalogError>,
) -> Result<(), CatalogError> {
    let mut statement = connection.prepare(
        "SELECT manifest_id, dataset_name, schema_version, artifact_id, content_algorithm, \
                content_digest FROM dataset_manifests ORDER BY lower(manifest_id)",
    )?;
    let mut rows = statement.query([])?;
    while let Some(row) = rows.next()? {
        let dataset = row.get::<_, String>(1)?;
        let algorithm: i64 = row.get(4)?;
        consume(
            ManifestEvidenceRow::try_new(
                parse_uuid(row.get::<_, String>(0)?)?,
                DatasetId::try_from(dataset.as_str()).map_err(|_| CatalogError::CorruptCatalog)?,
                parse_positive_u32(row.get(2)?)?,
                parse_uuid(row.get::<_, String>(3)?)?,
                parse_sha256(algorithm, row.get::<_, Vec<u8>>(5)?)?,
            )
            .map_err(map_evidence_error)?,
        )?;
    }
    Ok(())
}

fn read_query_artifacts(
    connection: &Connection,
    cutoff: Timestamp,
    consume: &mut impl FnMut(QueryArtifactEvidenceRow) -> Result<(), CatalogError>,
) -> Result<(), CatalogError> {
    let mut statement = connection.prepare(
        "SELECT reservations.reservation_id, reservations.owner, reservations.request_algorithm, \
                reservations.request_digest, results.artifact_id, results.relative_reference, \
                results.content_algorithm, results.content_digest, results.size_bytes, \
                reservations.expires_at_ns \
         FROM query_artifact_reservations AS reservations \
         JOIN query_artifact_results AS results USING (reservation_id) \
         WHERE reservations.state='published' AND reservations.expires_at_ns>?1 \
         ORDER BY lower(reservations.reservation_id)",
    )?;
    let mut rows = statement.query([cutoff.unix_nanos()])?;
    while let Some(row) = rows.next()? {
        let request_algorithm: i64 = row.get(2)?;
        let content_algorithm: i64 = row.get(6)?;
        consume(
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
        )?;
    }
    Ok(())
}

type ProviderRelationEvidenceRow = (Box<str>, Box<[u8]>, Sha256Digest, u64);

struct ProviderEvidenceSink<'a> {
    digest: &'a mut EvidenceDigest,
    cancellation: &'a CancellationToken,
    request: EvidenceSnapshotRequest,
    previous: Option<(ProviderCatalogRelation, Box<[u8]>)>,
    count: u64,
    total_bytes: &'a mut u64,
}
impl ProviderEvidenceSink<'_> {
    fn push(&mut self, row: ProviderRelationEvidenceRow) -> Result<(), CatalogError> {
        check_cancellation(self.cancellation)?;
        let (name, key, content, bytes) = row;
        let relation = ProviderCatalogRelation::from_database_name(&name)
            .ok_or(CatalogError::AnalyticalEvidenceInvalid)?;
        if self
            .previous
            .as_ref()
            .is_some_and(|(previous, previous_key)| {
                (*previous, previous_key.as_ref()) >= (relation, key.as_ref())
            })
        {
            return Err(CatalogError::AnalyticalEvidenceInvalid);
        }
        let row =
            ProviderCatalogRelationEvidenceRow::try_new(relation, key.clone(), content, bytes)
                .map_err(map_evidence_error)?;
        if bytes > self.request.limits().max_object_bytes() {
            return Err(CatalogError::AnalyticalEvidenceInvalid);
        }
        add(self.total_bytes, bytes)?;
        if *self.total_bytes > self.request.limits().max_total_bytes() {
            return Err(CatalogError::AnalyticalEvidenceLimitExceeded);
        }
        self.digest
            .provider_relation(&row)
            .map_err(map_evidence_error)?;
        add(&mut self.count, 1)?;
        self.previous = Some((relation, key));
        Ok(())
    }
}
fn read_provider_relation_rows(
    connection: &Connection,
    result: &mut ProviderEvidenceSink<'_>,
) -> Result<(), CatalogError> {
    read_sealed_raw_object_evidence(connection, result)?;
    read_provider_logical_bindings(connection, result)?;
    read_provider_logical_families(connection, result)?;
    read_provider_logical_objects(connection, result)?;
    read_provider_logical_partitions(connection, result)?;
    read_provider_logical_expectations(connection, result)?;
    read_provider_option_bindings(connection, result)?;
    read_provider_option_native_lineage(connection, result)?;
    read_provider_option_rows(connection, result)?;
    market_events::read_relations(connection, result, true)?;
    read_direct_provider_input_evidence(connection, result)?;
    read_provider_logical_original_evidence(connection, result)?;
    read_provider_capture_original_evidence(connection, result)?;
    read_provider_logical_partition_artifacts(connection, result)?;
    market_events::read_relations(connection, result, false)?;
    read_native_reference_evidence(connection, result)?;
    read_indexed_resource_evidence(connection, result)
}

// Hash one stored record at a time. Model bytes and filing chunks are never accumulated here.
fn read_indexed_resource_evidence(
    connection: &Connection,
    result: &mut ProviderEvidenceSink<'_>,
) -> Result<(), CatalogError> {
    use rusqlite::types::ValueRef;

    for (relation, query) in [
        (
            "forecast_inventory_vintages",
            "SELECT printf('%019d',sequence), sequence, vintage_id, request_hash, product_token, artifact_id, instrument_id, created_at, available_at, expires_at, record, record_sha256 FROM forecast_inventory_vintages ORDER BY sequence",
        ),
        (
            "forecast_inventory_outcomes",
            "SELECT printf('%019d',sequence), sequence, outcome_id, vintage_id, target_at, record, record_sha256 FROM forecast_inventory_outcomes ORDER BY sequence",
        ),
        (
            "model_inventory_series",
            "SELECT model_id, model_id, bundle_id FROM model_inventory_series ORDER BY model_id",
        ),
        (
            "chart_projection_headers",
            "SELECT hex(source_sha256), source_sha256, projection_sha256, row_count, series_count, first_time, last_time, metadata FROM chart_projection_headers ORDER BY source_sha256",
        ),
        (
            "chart_projection_rows",
            "SELECT hex(source_sha256)||'/'||printf('%019d',ordinal), source_sha256, ordinal, time_nanos, payload, payload_sha256 FROM chart_projection_rows ORDER BY source_sha256,ordinal",
        ),
        (
            "model_inventory_records",
            "SELECT printf('%019d',sequence), sequence, model_id, bundle_id, bundle_version, candidate_directory, product_token, record, record_sha256, chain_sha256 FROM model_inventory_records ORDER BY sequence",
        ),
        (
            "sec_prepared_indexes",
            "SELECT hex(generation_digest),generation_digest,source_artifact_id,artifact_id,relative_reference,content_algorithm,content_digest,size_bytes,created_at_ns FROM sec_prepared_indexes ORDER BY generation_digest",
        ),
    ] {
        let mut statement = connection.prepare(query)?;
        let columns = statement.column_count();
        let mut rows = statement.query([])?;
        while let Some(row) = rows.next()? {
            check_cancellation(result.cancellation)?;
            let key: String = row.get(0)?;
            let mut digest = ProviderRowDigest::new(relation)?;
            for index in 1..columns {
                match row.get_ref(index)? {
                    ValueRef::Integer(value) => digest.integer(value),
                    ValueRef::Text(value) => digest.text(
                        std::str::from_utf8(value).map_err(|_| CatalogError::CorruptCatalog)?,
                    )?,
                    ValueRef::Blob(value) => digest.bytes(value)?,
                    ValueRef::Null => digest.optional_bytes(None)?,
                    _ => return Err(CatalogError::CorruptCatalog),
                }
            }
            result.push(provider_relation_row(
                relation,
                key.into_bytes().into_boxed_slice(),
                digest.finish(),
                0,
            ))?;
        }
    }
    Ok(())
}

fn read_direct_provider_input_evidence(
    connection: &Connection,
    result: &mut ProviderEvidenceSink<'_>,
) -> Result<(), CatalogError> {
    const CAPTURE_RELATION: &str = "ingest_run_provider_capture_bindings";
    let mut capture_statement = connection.prepare(
        "SELECT run_id, input_ordinal, output_artifact_ordinal, object_input_ordinal,
                binding_digest, source_id, metadata_dependency_digest
         FROM ingest_run_provider_capture_bindings
         ORDER BY lower(run_id), input_ordinal",
    )?;
    let mut rows = capture_statement.query([])?;
    while let Some(row) = rows.next()? {
        check_cancellation(result.cancellation)?;
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
        digest.optional_bytes(row.get::<_, Option<Vec<u8>>>(6)?.as_deref())?;
        result.push(provider_relation_row(
            CAPTURE_RELATION,
            run_ordinal_primary_key(run, input_ordinal)?,
            digest.finish(),
            0,
        ))?;
    }

    const PUBLICATION_RELATION: &str = "ingest_run_provider_publication_bindings";
    let mut publication_statement = connection.prepare(
        "SELECT run_id, input_ordinal, output_artifact_ordinal, object_input_ordinal,
                publication_digest, publication_kind, source_id,
                response_binding_digest, event_binding_digest, composite_binding_digest,
                option_binding_digest, logical_binding_digest, active_dataset_id, active_commit_sequence
         FROM ingest_run_provider_publication_bindings
         ORDER BY lower(run_id), input_ordinal",
    )?;
    let mut rows = publication_statement.query([])?;
    while let Some(row) = rows.next()? {
        check_cancellation(result.cancellation)?;
        let run = parse_uuid(row.get::<_, String>(0)?)?;
        let input_ordinal: i64 = row.get(1)?;
        let output_ordinal: Option<i64> = row.get(2)?;
        let object_input_ordinal: Option<i64> = row.get(3)?;
        let publication = parse_sha256(1, row.get::<_, Vec<u8>>(4)?)?;
        let kind: String = row.get(5)?;
        let source: String = row.get(6)?;
        let response: Option<Vec<u8>> = row.get(7)?;
        let event: Option<Vec<u8>> = row.get(8)?;
        let composite: Option<Vec<u8>> = row.get(9)?;
        let option: Option<Vec<u8>> = row.get(10)?;
        let logical: Option<Vec<u8>> = row.get(11)?;
        let active_dataset: Option<String> = row.get(12)?;
        let active_sequence: Option<i64> = row.get(13)?;
        if !(0..=4095).contains(&input_ordinal)
            || match (
                &active_dataset,
                active_sequence,
                output_ordinal,
                object_input_ordinal,
            ) {
                (None, None, Some(output), Some(input)) => {
                    !(0..=1023).contains(&output)
                        || !(0..=4095).contains(&input)
                        || !matches!(
                            kind.as_str(),
                            "option_snapshots" | "option_expirations" | "provider_logical"
                        )
                }
                (Some(dataset), Some(sequence), None, None) => {
                    sequence <= 0
                        || input_ordinal != 0
                        || DatasetId::try_from(dataset.as_str()).is_err()
                        || !matches!(
                            kind.as_str(),
                            "response_market_event"
                                | "event_microbatch"
                                | "composite_response_event"
                        )
                }
                _ => true,
            }
            || SourceIdentifier::try_from(source.clone()).is_err()
        {
            return Err(CatalogError::CorruptCatalog);
        }
        let mut digest = ProviderRowDigest::new(PUBLICATION_RELATION)?;
        digest.bytes(run.as_bytes())?;
        digest.integer(input_ordinal);
        digest.optional_integer(output_ordinal);
        digest.optional_integer(object_input_ordinal);
        digest.digest(publication);
        digest.text(&kind)?;
        digest.text(&source)?;
        digest.optional_bytes(response.as_deref())?;
        digest.optional_bytes(event.as_deref())?;
        digest.optional_bytes(composite.as_deref())?;
        digest.optional_bytes(option.as_deref())?;
        digest.optional_bytes(logical.as_deref())?;
        digest.optional_bytes(active_dataset.as_deref().map(str::as_bytes))?;
        digest.optional_integer(active_sequence);
        result.push(provider_relation_row(
            PUBLICATION_RELATION,
            run_ordinal_primary_key(run, input_ordinal)?,
            digest.finish(),
            0,
        ))?;
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
             SELECT 1 FROM market_data_native_reference_captures AS capture
             WHERE NOT EXISTS (SELECT 1 FROM market_data_instrument_revisions AS revision
                     WHERE revision.revision_digest=capture.origin_revision_digest
                       AND revision.published_at_ns<=capture.retained_at_ns)
                OR NOT EXISTS (SELECT 1 FROM sealed_raw_objects AS raw
                     WHERE raw.raw_claim_digest=capture.raw_claim_digest
                       AND raw.physical_receipt_digest=capture.physical_receipt_digest
                       AND raw.recorded_at_ns<=capture.retained_at_ns)
             UNION ALL
             SELECT 1 FROM provider_logical_originals AS original
             WHERE original.object_count!=(SELECT COUNT(*) FROM provider_logical_original_objects
                       WHERE coordinate_digest=original.coordinate_digest)
                OR (SELECT MIN(object_ordinal) FROM provider_logical_original_objects
                       WHERE coordinate_digest=original.coordinate_digest)!=0
                OR (SELECT MAX(object_ordinal) FROM provider_logical_original_objects
                       WHERE coordinate_digest=original.coordinate_digest)!=original.object_count-1
                OR NOT EXISTS (SELECT 1 FROM source_rights AS rights
                       WHERE rights.rights_id=original.rights_id AND rights.source_id=original.source_id
                         AND rights.payload_algorithm=1 AND rights.payload_digest=original.original_digest
                         AND (rights.operation_mask & 4)<>0 AND rights.admitted_at_ns<=original.retained_at_ns
                         AND (rights.authorization_expires_at_ns IS NULL OR rights.authorization_expires_at_ns>original.retained_at_ns))
                OR (original.publication_digest IS NOT NULL AND NOT EXISTS (
                       SELECT 1 FROM provider_logical_publication_bindings AS binding
                       JOIN ingest_run_provider_publication_bindings AS input ON input.publication_digest=binding.binding_digest
                       JOIN ingest_runs AS run ON run.run_id=input.run_id AND run.state='succeeded'
                       JOIN dataset_manifests AS anchor ON anchor.run_id=run.run_id
                       JOIN analytical_generations AS generation ON generation.anchor_manifest_id=anchor.manifest_id
                       WHERE binding.binding_digest=original.publication_digest AND binding.source_id=original.source_id
                         AND input.publication_kind='provider_logical' AND generation.dataset_id=original.dataset_id
                         AND binding.object_count=original.object_count))
             UNION ALL
             SELECT 1 FROM provider_logical_original_objects AS object
             JOIN provider_logical_originals AS original ON original.coordinate_digest=object.coordinate_digest
             WHERE NOT EXISTS (SELECT 1 FROM sealed_raw_objects AS claim
                       WHERE claim.raw_claim_digest=object.raw_claim_digest
                         AND claim.physical_receipt_digest=object.physical_receipt_digest AND claim.raw_claim_kind='logical_object')
                OR (original.publication_digest IS NOT NULL AND NOT EXISTS (
                       SELECT 1 FROM provider_logical_publication_objects AS published
                       WHERE published.binding_digest=original.publication_digest AND published.object_ordinal=object.object_ordinal
                         AND published.object_role=object.object_role AND published.semantic_identity=object.semantic_identity
                         AND published.raw_claim_digest=object.raw_claim_digest AND published.physical_receipt_digest=object.physical_receipt_digest))
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
    result: &mut ProviderEvidenceSink<'_>,
) -> Result<(), CatalogError> {
    const RELATION: &str = "sealed_raw_objects";
    let mut statement = connection.prepare(
        "SELECT raw_claim_digest, raw_claim_kind, physical_receipt_digest,
                relative_reference, content_digest, size_bytes, integrity_chunk_bytes,
                unit_count, raw_claim_json, recorded_at_ns
         FROM sealed_raw_objects ORDER BY raw_claim_digest",
    )?;
    let mut rows = statement.query([])?;
    while let Some(row) = rows.next()? {
        check_cancellation(result.cancellation)?;
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
                    && unit_count
                        <= market_squawk_sources::MAX_PROVIDER_EVENT_MICROBATCH_FRAMES as u64
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
        ))?;
    }
    Ok(())
}

fn read_provider_logical_original_evidence(
    connection: &Connection,
    result: &mut ProviderEvidenceSink<'_>,
) -> Result<(), CatalogError> {
    const RELATION: &str = "provider_logical_originals";
    let mut statement = connection.prepare(
        "SELECT coordinate_digest, dataset_id, source_id, native_schema_digest,
                source_revision_digest, original_digest, received_at_ns, checkpoint_digest,
                checkpoint_bytes, object_count, object_set_digest, rights_id, custody_digest,
                retained_at_ns, publication_digest, published_at_ns,
                registered_source_revision_digest, source_revision_kind
         FROM provider_logical_originals ORDER BY coordinate_digest",
    )?;
    let mut rows = statement.query([])?;
    while let Some(row) = rows.next()? {
        check_cancellation(result.cancellation)?;
        let coordinate = parse_sha256(1, row.get::<_, Vec<u8>>(0)?)?;
        let dataset: String = row.get(1)?;
        let source: String = row.get(2)?;
        DatasetId::try_from(dataset.as_str()).map_err(|_| CatalogError::CorruptCatalog)?;
        market_squawk_domain::SourceId::try_from(source.as_str())
            .map_err(|_| CatalogError::CorruptCatalog)?;
        let native = parse_sha256(1, row.get::<_, Vec<u8>>(3)?)?;
        let revision = parse_sha256(1, row.get::<_, Vec<u8>>(4)?)?;
        let original = parse_sha256(1, row.get::<_, Vec<u8>>(5)?)?;
        let received: i64 = row.get(6)?;
        let checkpoint_digest = parse_sha256(1, row.get::<_, Vec<u8>>(7)?)?;
        let checkpoint: Vec<u8> = row.get(8)?;
        let objects: i64 = row.get(9)?;
        let object_set = parse_sha256(1, row.get::<_, Vec<u8>>(10)?)?;
        let rights = parse_sha256(1, row.get::<_, Vec<u8>>(11)?)?;
        let custody = parse_sha256(1, row.get::<_, Vec<u8>>(12)?)?;
        let retained: i64 = row.get(13)?;
        let publication: Option<Vec<u8>> = row.get(14)?;
        let published: Option<i64> = row.get(15)?;
        let registered_revision = parse_sha256(1, row.get::<_, Vec<u8>>(16)?)?;
        let revision_kind: String = row.get(17)?;
        if !matches!(revision_kind.as_str(), "metadata" | "contract_payload")
            || (revision_kind == "metadata" && revision != registered_revision)
            || checkpoint.is_empty()
            || checkpoint.len() > super::MAX_PROVIDER_LOGICAL_ORIGINAL_CHECKPOINT_BYTES
            || Sha256Digest::new(Sha256::digest(&checkpoint).into()) != checkpoint_digest
            || !(1..=64).contains(&objects)
            || received > retained
            || publication.is_some() != published.is_some()
            || published.is_some_and(|time| time < retained)
        {
            return Err(CatalogError::CorruptCatalog);
        }
        if let Some(publication) = &publication {
            parse_sha256(1, publication.clone())?;
        }
        let mut digest = ProviderRowDigest::new(RELATION)?;
        digest.digest(coordinate);
        digest.text(&dataset)?;
        digest.text(&source)?;
        digest.digest(native);
        digest.digest(revision);
        digest.digest(registered_revision);
        digest.text(&revision_kind)?;
        digest.digest(original);
        digest.integer(received);
        digest.digest(checkpoint_digest);
        digest.bytes(&checkpoint)?;
        digest.integer(objects);
        digest.digest(object_set);
        digest.digest(rights);
        digest.digest(custody);
        digest.integer(retained);
        digest.optional_bytes(publication.as_deref())?;
        digest.optional_integer(published);
        result.push(provider_relation_row(
            RELATION,
            digest_primary_key(coordinate),
            digest.finish(),
            0,
        ))?;
    }
    read_provider_logical_original_objects(connection, result)
}

fn read_provider_logical_bindings(
    connection: &Connection,
    result: &mut ProviderEvidenceSink<'_>,
) -> Result<(), CatalogError> {
    const RELATION: &str = "provider_logical_publication_bindings";
    let mut statement = connection.prepare(
        "SELECT binding_digest, binding_format_version, source_id, terminal_receipt_digest,
                terminal_json, required_family_count, object_count, partition_count,
                canonical_partition_count, recorded_at_ns
         FROM provider_logical_publication_bindings ORDER BY binding_digest",
    )?;
    let mut rows = statement.query([])?;
    while let Some(row) = rows.next()? {
        check_cancellation(result.cancellation)?;
        let binding = parse_sha256(1, row.get::<_, Vec<u8>>(0)?)?;
        super::provider_logical::load_provider_logical_publication_binding(
            connection,
            market_squawk_domain::EvidenceDigest::new(
                market_squawk_domain::DigestAlgorithm::Sha256,
                binding.bytes(),
            ),
        )?
        .ok_or(CatalogError::CorruptCatalog)?;

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
        ))?;
    }
    Ok(())
}

fn read_provider_logical_families(
    connection: &Connection,
    result: &mut ProviderEvidenceSink<'_>,
) -> Result<(), CatalogError> {
    const RELATION: &str = "provider_logical_publication_required_families";
    let mut statement = connection.prepare(
        "SELECT binding_digest, family_ordinal, family
         FROM provider_logical_publication_required_families
         ORDER BY binding_digest, family_ordinal",
    )?;
    let mut rows = statement.query([])?;
    while let Some(row) = rows.next()? {
        check_cancellation(result.cancellation)?;
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
        ))?;
    }
    Ok(())
}

fn read_provider_logical_objects(
    connection: &Connection,
    result: &mut ProviderEvidenceSink<'_>,
) -> Result<(), CatalogError> {
    const RELATION: &str = "provider_logical_publication_objects";
    let mut statement = connection.prepare(
        "SELECT binding_digest, object_ordinal, object_role, semantic_identity,
                raw_claim_digest, physical_receipt_digest
         FROM provider_logical_publication_objects
         ORDER BY binding_digest, object_ordinal",
    )?;
    let mut rows = statement.query([])?;
    while let Some(row) = rows.next()? {
        check_cancellation(result.cancellation)?;
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
        ))?;
    }
    Ok(())
}

fn read_provider_logical_original_objects(
    connection: &Connection,
    result: &mut ProviderEvidenceSink<'_>,
) -> Result<(), CatalogError> {
    const RELATION: &str = "provider_logical_original_objects";
    let mut statement = connection.prepare(
        "SELECT coordinate_digest, object_ordinal, object_role, semantic_identity,
                raw_claim_digest, physical_receipt_digest
         FROM provider_logical_original_objects
         ORDER BY coordinate_digest, object_ordinal",
    )?;
    let mut rows = statement.query([])?;
    while let Some(row) = rows.next()? {
        check_cancellation(result.cancellation)?;
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
        ))?;
    }
    Ok(())
}

fn read_provider_logical_partitions(
    connection: &Connection,
    result: &mut ProviderEvidenceSink<'_>,
) -> Result<(), CatalogError> {
    const RELATION: &str = "provider_logical_publication_partitions";
    let mut statement = connection.prepare(
        "SELECT binding_digest, partition_family_ordinal, partition_family,
                partition_ordinal, first_item_ordinal, item_count, schema_identity,
                semantic_digest, raw_claim_digest, physical_receipt_digest
         FROM provider_logical_publication_partitions
         ORDER BY binding_digest, partition_family_ordinal, partition_ordinal",
    )?;
    let mut rows = statement.query([])?;
    while let Some(row) = rows.next()? {
        check_cancellation(result.cancellation)?;
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
        ))?;
    }
    Ok(())
}

fn read_provider_logical_expectations(
    connection: &Connection,
    result: &mut ProviderEvidenceSink<'_>,
) -> Result<(), CatalogError> {
    const RELATION: &str = "provider_logical_publication_canonical_expectations";
    let mut statement = connection.prepare(
        "SELECT binding_digest, partition_ordinal, first_row_ordinal, row_count,
                schema_identity, semantic_digest, aligned_native_partition,
                aligned_row_map_partition
         FROM provider_logical_publication_canonical_expectations
         ORDER BY binding_digest, partition_ordinal",
    )?;
    let mut rows = statement.query([])?;
    while let Some(row) = rows.next()? {
        check_cancellation(result.cancellation)?;
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
        ))?;
    }
    Ok(())
}

fn read_provider_option_bindings(
    connection: &Connection,
    result: &mut ProviderEvidenceSink<'_>,
) -> Result<(), CatalogError> {
    const RELATION: &str = "provider_option_market_bindings";
    let mut statement = connection.prepare(
        "SELECT option_binding_digest, binding_format_version, capture_observation_digest,
                sealed_capture_receipt_digest, publication_kind,
                canonical_schema_fingerprint, canonical_content_digest, canonical_row_count,
                scope_json, scope_digest, completeness_json, completeness_digest,
                filter_json, filter_digest, underlying_instrument_id, available_at_ns,
                received_at_ns, ingested_at_ns, disposition, row_mapping_digest, recorded_at_ns
         FROM provider_option_market_bindings ORDER BY option_binding_digest",
    )?;
    let mut rows = statement.query([])?;
    while let Some(row) = rows.next()? {
        check_cancellation(result.cancellation)?;
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
        ))?;
    }
    Ok(())
}

fn read_provider_option_native_lineage(
    connection: &Connection,
    result: &mut ProviderEvidenceSink<'_>,
) -> Result<(), CatalogError> {
    const RELATION: &str = "provider_option_market_binding_native_lineage";
    let mut statement = connection.prepare(
        "SELECT option_binding_digest, schema_version, implementation, schema_fingerprint,
                row_count, batch_digest, batch_sidecar_payload, batch_sidecar_digest
         FROM provider_option_market_binding_native_lineage
         ORDER BY option_binding_digest",
    )?;
    let mut rows = statement.query([])?;
    while let Some(row) = rows.next()? {
        check_cancellation(result.cancellation)?;
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
        ))?;
    }
    Ok(())
}

fn read_provider_option_rows(
    connection: &Connection,
    result: &mut ProviderEvidenceSink<'_>,
) -> Result<(), CatalogError> {
    const RELATION: &str = "provider_option_market_binding_rows";
    let mut statement = connection.prepare(
        "SELECT option_binding_digest, capture_observation_digest, canonical_row_ordinal,
                canonical_row_digest, native_semantic_payload, native_semantic_digest,
                capture_page_ordinal, physical_frame_ordinal, payload_digest,
                received_at_ns, source_sequence
         FROM provider_option_market_binding_rows
         ORDER BY option_binding_digest, canonical_row_ordinal",
    )?;
    let mut rows = statement.query([])?;
    while let Some(row) = rows.next()? {
        check_cancellation(result.cancellation)?;
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
        ))?;
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

fn parse_nonnegative_u64(value: i64) -> Result<u64, CatalogError> {
    u64::try_from(value).map_err(|_| CatalogError::CorruptCatalog)
}

fn sqlite_integer(value: u64) -> Result<i64, CatalogError> {
    i64::try_from(value).map_err(|_| CatalogError::CorruptCatalog)
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

fn read_provider_capture_original_evidence(
    connection: &Connection,
    result: &mut ProviderEvidenceSink<'_>,
) -> Result<(), CatalogError> {
    const RELATION: &str = "provider_capture_originals";
    let mut statement=connection.prepare("SELECT session_digest,ordinal,rights_id,retained_at_ns FROM provider_capture_originals ORDER BY original_digest")?;
    let mut rows = statement.query([])?;
    while let Some(row) = rows.next()? {
        check_cancellation(result.cancellation)?;
        let session = parse_sha256(1, row.get::<_, Vec<u8>>(0)?)?;
        let ordinal = row.get::<_, u16>(1)?;
        let original = super::provider_capture::original::load(
            connection,
            market_squawk_domain::EvidenceDigest::new(
                market_squawk_domain::DigestAlgorithm::Sha256,
                session.bytes(),
            ),
            ordinal,
        )?
        .ok_or(CatalogError::CorruptCatalog)?;
        let rights = parse_sha256(1, row.get::<_, Vec<u8>>(2)?)?;
        let retained = row.get::<_, i64>(3)?;
        if retained < original.decoded_at().unix_nanos() {
            return Err(CatalogError::CorruptCatalog);
        }
        let mut digest = ProviderRowDigest::new(RELATION)?;
        digest.digest(session);
        digest.integer(i64::from(ordinal));
        digest.digest(Sha256Digest::new(original.digest().bytes()));
        digest.digest(rights);
        digest.integer(retained);
        digest.optional_bytes(
            original
                .published_binding()
                .map(|v| v.bytes())
                .as_ref()
                .map(|v| v.as_slice()),
        )?;
        result.push(provider_relation_row(
            RELATION,
            digest_primary_key(Sha256Digest::new(original.digest().bytes())),
            digest.finish(),
            0,
        ))?;
    }
    Ok(())
}

fn read_native_reference_evidence(
    connection: &Connection,
    result: &mut ProviderEvidenceSink<'_>,
) -> Result<(), CatalogError> {
    const RELATION: &str = "market_data_native_reference_captures";
    let mut statement = connection.prepare(
        "SELECT identity_digest, origin_revision_digest, coordinate_json, raw_claim_digest,
                physical_receipt_digest, custody_digest, retained_at_ns
         FROM market_data_native_reference_captures ORDER BY identity_digest",
    )?;
    let mut rows = statement.query([])?;
    while let Some(row) = rows.next()? {
        check_cancellation(result.cancellation)?;
        let identity = parse_sha256(1, row.get::<_, Vec<u8>>(0)?)?;
        let origin = parse_sha256(1, row.get::<_, Vec<u8>>(1)?)?;
        let coordinate: String = row.get(2)?;
        let claim = parse_sha256(1, row.get::<_, Vec<u8>>(3)?)?;
        let physical = parse_sha256(1, row.get::<_, Vec<u8>>(4)?)?;
        let custody = parse_sha256(1, row.get::<_, Vec<u8>>(5)?)?;
        let retained_at: i64 = row.get(6)?;
        if !(2..=32_768).contains(&coordinate.len()) || retained_at <= 0 {
            return Err(CatalogError::CorruptCatalog);
        }
        let mut digest = ProviderRowDigest::new(RELATION)?;
        digest.digest(identity);
        digest.digest(origin);
        digest.text(&coordinate)?;
        digest.digest(claim);
        digest.digest(physical);
        digest.digest(custody);
        digest.integer(retained_at);
        result.push(provider_relation_row(
            RELATION,
            digest_primary_key(identity),
            digest.finish(),
            0,
        ))?;
    }
    Ok(())
}

fn read_provider_logical_partition_artifacts(
    connection: &Connection,
    result: &mut ProviderEvidenceSink<'_>,
) -> Result<(), CatalogError> {
    const RELATION: &str = "ingest_run_provider_logical_partition_artifacts";
    let mut statement = connection.prepare(
        "SELECT run_id, partition_ordinal, logical_binding_digest, output_artifact_ordinal, object_input_ordinal
         FROM ingest_run_provider_logical_partition_artifacts
         ORDER BY lower(run_id), partition_ordinal",
    )?;
    let mut rows = statement.query([])?;
    while let Some(row) = rows.next()? {
        check_cancellation(result.cancellation)?;
        let run = parse_uuid(row.get::<_, String>(0)?)?;
        let ordinal: i64 = row.get(1)?;
        let binding = parse_sha256(1, row.get::<_, Vec<u8>>(2)?)?;
        let output: i64 = row.get(3)?;
        let local: i64 = row.get(4)?;
        if !(0..=1023).contains(&ordinal)
            || !(0..=1023).contains(&output)
            || !(0..=1023).contains(&local)
        {
            return Err(CatalogError::CorruptCatalog);
        }
        let mut digest = ProviderRowDigest::new(RELATION)?;
        digest.bytes(run.as_bytes())?;
        digest.integer(ordinal);
        digest.digest(binding);
        digest.integer(output);
        digest.integer(local);
        result.push(provider_relation_row(
            RELATION,
            run_ordinal_primary_key(run, ordinal)?,
            digest.finish(),
            0,
        ))?;
    }
    Ok(())
}
