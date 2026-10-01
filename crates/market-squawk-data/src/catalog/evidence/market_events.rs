//! Exact logical event relations and separately retained physical archives for backup evidence.

use rusqlite::types::ValueRef;

use super::*;

pub(super) fn read_archives(
    connection: &Connection,
    maximum: usize,
) -> Result<Vec<MarketEventArchiveEvidenceRow>, CatalogError> {
    let mut statement = connection.prepare(
        "SELECT content_digest,relative_reference,schema_name,schema_version,schema_fingerprint,
                size_bytes,row_count,created_at_ns,published_at_ns
         FROM market_event_archive_objects ORDER BY content_digest LIMIT ?1",
    )?;
    let mut rows = statement.query([limit_with_sentinel(maximum)?])?;
    let mut result = Vec::new();
    while let Some(row) = rows.next()? {
        require_capacity(&result, maximum)?;
        let schema = DatasetSchemaRef::try_new(
            row.get::<_, String>(2)?,
            market_squawk_domain::SchemaVersion::new(
                u16::try_from(row.get::<_, i64>(3)?).map_err(|_| CatalogError::CorruptCatalog)?,
            )
            .map_err(|_| CatalogError::CorruptCatalog)?,
            row.get::<_, Vec<u8>>(4)?
                .try_into()
                .map_err(|_| CatalogError::CorruptCatalog)?,
        )
        .map_err(|_| CatalogError::CorruptCatalog)?;
        result.push(
            MarketEventArchiveEvidenceRow::try_new(
                row.get::<_, String>(1)?,
                parse_sha256(1, row.get::<_, Vec<u8>>(0)?)?,
                schema,
                parse_positive_u64(row.get(5)?)?,
                parse_positive_u64(row.get(6)?)?,
                Timestamp::from_unix_nanos(row.get(7)?),
                Timestamp::from_unix_nanos(row.get(8)?),
            )
            .map_err(map_evidence_error)?,
        );
    }
    Ok(result)
}

pub(super) fn read_relations(
    connection: &Connection,
    maximum: usize,
    result: &mut Vec<ProviderRelationEvidenceRow>,
) -> Result<(), CatalogError> {
    // Explicit field-complete projections: identity and original clocks remain distinct from placement.
    for (relation, query) in [
        ("market_event_storage_heads",
         "SELECT dataset_id,dataset_id,committed_sequence,content_digest
          FROM market_event_storage_heads ORDER BY dataset_id LIMIT ?1"),
        ("market_event_commits",
         "SELECT dataset_id||'/'||printf('%019d',commit_sequence),dataset_id,commit_sequence,
                 run_id,publication_digest,publication_kind,schema_name,schema_version,
                 schema_fingerprint,previous_content_digest,content_digest,lineage_digest,
                 row_count,available_at_ns
          FROM market_event_commits ORDER BY dataset_id,commit_sequence LIMIT ?1"),
        ("market_event_active_rows",
         "SELECT hex(active.publication_digest)||'/'||printf('%019d',active.publication_row_ordinal),
                 active.publication_digest,active.publication_row_ordinal,active.event_json,
                 indexed.canonical_event_digest
          FROM market_event_active_rows AS active
          JOIN provider_market_event_selection_index AS indexed
            ON indexed.publication_digest=active.publication_digest
           AND indexed.publication_row_ordinal=active.publication_row_ordinal
          ORDER BY active.publication_digest,active.publication_row_ordinal LIMIT ?1"),
        ("market_event_archive_objects",
         "SELECT hex(content_digest),content_digest,relative_reference,schema_name,schema_version,
                 schema_fingerprint,size_bytes,row_count,created_at_ns,published_at_ns
          FROM market_event_archive_objects ORDER BY content_digest LIMIT ?1"),
        ("market_event_archive_memberships",
         "SELECT hex(publication_digest),publication_digest,dataset_id,commit_sequence,
                 object_content_digest,first_row,row_count
          FROM market_event_archive_memberships ORDER BY publication_digest LIMIT ?1"),
        ("market_event_archive_progress",
         "SELECT dataset_id,dataset_id,archived_sequence
          FROM market_event_archive_progress ORDER BY dataset_id LIMIT ?1"),
        ("provider_market_event_selection_index",
         "SELECT hex(publication_digest)||'/'||printf('%019d',publication_row_ordinal),
                 dataset_id,commit_sequence,publication_digest,publication_kind,publication_row_ordinal,
                 component_kind,component_binding_digest,component_row_ordinal,canonical_event_digest,
                 source_id,instrument_id,venue_id,event_kind,source_timestamp_ns,received_at_ns,
                 available_at_ns,ingested_at_ns,connection_generation_be,source_sequence_be,
                 provider_event_id,coordinate_digest,cohort_key,provider_product,provider_channel
          FROM provider_market_event_selection_index ORDER BY publication_digest,publication_row_ordinal LIMIT ?1"),
    ] {
        let mut statement = connection.prepare(query)?;
        let columns = statement.column_count();
        let mut rows = statement.query([limit_with_sentinel(maximum)?])?;
        while let Some(row) = rows.next()? {
            require_capacity(result, maximum)?;
            let key: String = row.get(0)?;
            if relation == "market_event_commits" {
                let dataset: String = row.get(1)?;
                let dataset = DatasetId::try_from(dataset.as_str()).map_err(|_| CatalogError::CorruptCatalog)?;
                let sequence = parse_positive_u64(row.get(2)?)?;
                super::super::market_event_store::load_market_event_commit(connection, &dataset, sequence)?
                    .ok_or(CatalogError::CorruptCatalog)?;
            }
            if relation == "market_event_active_rows" {
                let payload = row.get_ref(3)?.as_blob().map_err(|_| CatalogError::CorruptCatalog)?;
                let expected = row.get_ref(4)?.as_blob().map_err(|_| CatalogError::CorruptCatalog)?;
                if Sha256::digest(payload).as_slice() != expected {
                    return Err(CatalogError::CorruptCatalog);
                }
            }
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
            result.push(provider_relation_row(relation,key.into_bytes().into_boxed_slice(),digest.finish(),0));
        }
    }
    Ok(())
}

pub(super) fn validate_integrity(connection: &Connection) -> Result<(), CatalogError> {
    // SQLite integrity alone does not replay guarded transitions or prove whole-publication coverage.
    let invalid: bool = connection.query_row(
        "SELECT EXISTS (
          SELECT 1 FROM market_event_storage_heads AS head
          WHERE head.committed_sequence<>(SELECT COUNT(*) FROM market_event_commits WHERE dataset_id=head.dataset_id)
             OR (head.committed_sequence>0 AND NOT EXISTS (
                 SELECT 1 FROM market_event_commits AS committed WHERE committed.dataset_id=head.dataset_id
                   AND committed.commit_sequence=head.committed_sequence AND committed.content_digest=head.content_digest))
          UNION ALL
          SELECT 1 FROM market_event_commits AS committed
          WHERE NOT EXISTS (SELECT 1 FROM market_event_complete_commits AS complete
              JOIN ingest_runs AS run ON run.run_id=complete.run_id
              WHERE complete.publication_digest=committed.publication_digest
                AND run.state='succeeded' AND run.completed_at_ns=complete.available_at_ns)
          UNION ALL
          SELECT 1 FROM market_event_active_rows AS active
          WHERE NOT EXISTS (SELECT 1 FROM provider_market_event_selection_index AS indexed
              WHERE indexed.publication_digest=active.publication_digest
                AND indexed.publication_row_ordinal=active.publication_row_ordinal)
          UNION ALL
          SELECT 1 FROM provider_market_event_selection_index AS indexed
          WHERE NOT EXISTS (SELECT 1 FROM market_event_commits AS committed
              WHERE committed.dataset_id=indexed.dataset_id AND committed.commit_sequence=indexed.commit_sequence
                AND committed.publication_digest=indexed.publication_digest
                AND committed.publication_kind=indexed.publication_kind
                AND indexed.publication_row_ordinal>=0 AND indexed.publication_row_ordinal<committed.row_count)
          UNION ALL
          SELECT 1 FROM market_event_archive_memberships AS member
          WHERE NOT EXISTS (
              SELECT 1 FROM market_event_commits AS committed
              JOIN market_event_archive_objects AS object ON object.content_digest=member.object_content_digest
              WHERE committed.dataset_id=member.dataset_id AND committed.commit_sequence=member.commit_sequence
                AND committed.publication_digest=member.publication_digest AND committed.row_count=member.row_count
                AND object.schema_name=committed.schema_name AND object.schema_version=committed.schema_version
                AND object.schema_fingerprint=committed.schema_fingerprint
                AND member.first_row>=0 AND member.first_row<=object.row_count
                AND member.row_count<=object.row_count-member.first_row
                AND object.published_at_ns>=committed.available_at_ns)
             OR EXISTS (SELECT 1 FROM market_event_archive_memberships AS other
                WHERE other.object_content_digest=member.object_content_digest
                  AND other.publication_digest<>member.publication_digest
                  AND other.first_row<member.first_row+member.row_count
                  AND member.first_row<other.first_row+other.row_count)
          UNION ALL
          SELECT 1 FROM market_event_archive_objects AS object
          WHERE object.row_count<>COALESCE((SELECT SUM(row_count) FROM market_event_archive_memberships
              WHERE object_content_digest=object.content_digest),0)
          UNION ALL
          SELECT 1 FROM market_event_archive_progress AS progress
          WHERE NOT EXISTS (SELECT 1 FROM market_event_storage_heads AS head
              WHERE head.dataset_id=progress.dataset_id AND progress.archived_sequence<=head.committed_sequence)
             OR progress.archived_sequence<>(SELECT COUNT(*) FROM market_event_archive_memberships
                  WHERE dataset_id=progress.dataset_id AND commit_sequence<=progress.archived_sequence)
          UNION ALL
          SELECT 1 FROM market_event_archive_memberships AS member
          WHERE NOT EXISTS (SELECT 1 FROM market_event_archive_progress AS progress
              WHERE progress.dataset_id=member.dataset_id AND progress.archived_sequence>=member.commit_sequence)
          LIMIT 1
        )",
        [], |row| row.get(0),
    )?;
    if invalid {
        Err(CatalogError::CorruptCatalog)
    } else {
        Ok(())
    }
}
