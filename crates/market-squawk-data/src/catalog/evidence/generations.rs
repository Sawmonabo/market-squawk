//! Streaming generation replay. Only two headers and one ordered edge/object are resident.
use super::*;
use crate::authority_transition::evidence::GenerationPlanEvidence;
use crate::manifest::{MAX_DERIVED_GENERATION_PARENTS, compare_manifest_refs};

const HEADERS: &str = "SELECT generation_sequence,dataset_id,manifest_version,content_hash,lineage_hash,
 row_count,total_bytes,schema_name,schema_version,schema_fingerprint,anchor_manifest_id,generation_kind,build_spec_digest
 FROM analytical_generations";

pub(super) fn schema(
    row: &rusqlite::Row<'_>,
    name: usize,
    version: usize,
    fingerprint: usize,
) -> Result<DatasetSchemaRef, CatalogError> {
    let schema = DatasetSchemaRef::try_new(
        row.get::<_, String>(name)?,
        market_squawk_domain::SchemaVersion::new(
            u16::try_from(row.get::<_, i64>(version)?).map_err(|_| CatalogError::CorruptCatalog)?,
        )
        .map_err(|_| CatalogError::CorruptCatalog)?,
        row.get::<_, Vec<u8>>(fingerprint)?
            .try_into()
            .map_err(|_| CatalogError::CorruptCatalog)?,
    )
    .map_err(|_| CatalogError::CorruptCatalog)?;
    DatasetSchemaRegistry::local()
        .resolve(&schema)
        .map_err(|_| CatalogError::CorruptCatalog)?;
    Ok(schema)
}
fn header(row: &rusqlite::Row<'_>) -> Result<GenerationEvidenceHeader, CatalogError> {
    let dataset: String = row.get(1)?;
    let kind = GenerationKind::from_database_name(&row.get::<_, String>(11)?)
        .ok_or(CatalogError::CorruptCatalog)?;
    let build_spec_digest = row
        .get::<_, Option<Vec<u8>>>(12)?
        .map(parse_build_spec_digest)
        .transpose()?;
    if (kind == GenerationKind::Derived) != build_spec_digest.is_some() {
        return Err(CatalogError::CorruptCatalog);
    }
    Ok(GenerationEvidenceHeader {
        generation_sequence: parse_positive_u64(row.get(0)?)?,
        dataset_id: DatasetId::try_from(dataset.as_str())
            .map_err(|_| CatalogError::CorruptCatalog)?,
        manifest_version: parse_positive_u64(row.get(2)?)?,
        content_hash: parse_sha256(1, row.get(3)?)?,
        lineage_hash: parse_sha256(1, row.get(4)?)?,
        row_count: parse_positive_u64(row.get(5)?)?,
        total_bytes: parse_positive_u64(row.get(6)?)?,
        schema: schema(row, 7, 8, 9)?,
        anchor_manifest_id: parse_uuid(row.get(10)?)?,
        kind,
        build_spec_digest,
    })
}

pub(super) fn validate_relations(
    connection: &Connection,
    request: EvidenceSnapshotRequest,
) -> Result<(), CatalogError> {
    // Keys and joins replace whole-history BTreeMaps. Grouping/sorting spills through SQLite.
    let invalid:bool=connection.query_row(
        "SELECT EXISTS (
         SELECT 1 FROM artifacts GROUP BY lower(run_id)
          HAVING COUNT(*)>1024 OR MIN(publication_ordinal)<>0 OR MAX(publication_ordinal)<>COUNT(*)-1 OR COUNT(DISTINCT publication_ordinal)<>COUNT(*)
         UNION ALL
         SELECT 1 FROM dataset_manifests m LEFT JOIN artifacts a ON a.artifact_id=m.artifact_id
          WHERE a.artifact_id IS NULL OR a.publication_ordinal<>(SELECT COUNT(*)-1 FROM artifacts group_member WHERE group_member.run_id=a.run_id)
         UNION ALL
         SELECT 1 FROM dataset_manifests m JOIN artifacts a ON a.artifact_id=m.artifact_id GROUP BY a.run_id HAVING COUNT(*)<>1
         UNION ALL
         SELECT 1 FROM analytical_generations g LEFT JOIN dataset_manifests m ON m.manifest_id=g.anchor_manifest_id
          WHERE m.manifest_id IS NULL OR m.dataset_name<>g.dataset_id OR m.schema_version<>g.schema_version OR m.content_digest<>g.content_hash
         UNION ALL
         SELECT 1 FROM analytical_generation_objects o
          LEFT JOIN analytical_generations g ON g.dataset_id=o.dataset_id AND g.manifest_version=o.manifest_version
          LEFT JOIN artifacts a ON a.artifact_id=o.artifact_id
          WHERE g.dataset_id IS NULL OR a.artifact_id IS NULL OR a.content_digest<>o.content_hash OR a.size_bytes<>o.size_bytes
         UNION ALL
         SELECT 1 FROM analytical_generation_objects GROUP BY artifact_id HAVING MIN(row_count)<>MAX(row_count)
         UNION ALL
         SELECT 1 FROM analytical_generation_parents p LEFT JOIN analytical_generations g
          ON g.dataset_id=p.child_dataset_id AND g.manifest_version=p.child_manifest_version WHERE g.dataset_id IS NULL
         UNION ALL
         SELECT 1 FROM analytical_generations GROUP BY generation_sequence HAVING COUNT(*)<>1
         UNION ALL SELECT 1 FROM dataset_manifests GROUP BY lower(manifest_id) HAVING COUNT(*)<>1
         UNION ALL SELECT 1 FROM query_artifact_reservations WHERE state='published' AND expires_at_ns>?1 GROUP BY lower(reservation_id) HAVING COUNT(*)<>1
         UNION ALL
         SELECT 1 FROM (SELECT relative_reference FROM artifacts UNION ALL
            SELECT r.relative_reference FROM query_artifact_reservations q JOIN query_artifact_results r USING(reservation_id)
             WHERE q.state='published' AND q.expires_at_ns>?1
            UNION ALL SELECT relative_reference FROM market_event_archive_objects
            UNION ALL SELECT relative_reference FROM sec_prepared_indexes)
          GROUP BY relative_reference HAVING COUNT(*)<>1
         UNION ALL
         SELECT 1 FROM (SELECT artifact_id FROM artifacts UNION ALL
            SELECT r.artifact_id FROM query_artifact_reservations q JOIN query_artifact_results r USING(reservation_id)
             WHERE q.state='published' AND q.expires_at_ns>?1
            UNION ALL SELECT artifact_id FROM sec_prepared_indexes)
          GROUP BY lower(artifact_id) HAVING COUNT(*)<>1
         )",[request.cutoff().unix_nanos()],|row|row.get(0))?;
    if invalid {
        Err(CatalogError::AnalyticalEvidenceInvalid)
    } else {
        Ok(())
    }
}

pub(super) fn stream(
    connection: &Connection,
    digest: &mut EvidenceDigest,
    references: &mut u64,
    cancellation: &CancellationToken,
) -> Result<(), CatalogError> {
    let expected = count(connection, "analytical_generations")?;
    digest
        .section("generations", expected)
        .map_err(map_evidence_error)?;
    let mut statement =
        connection.prepare(&format!("{HEADERS} ORDER BY dataset_id,manifest_version"))?;
    let mut rows = statement.query([])?;
    let mut previous: Option<(GenerationEvidenceHeader, u64)> = None;
    let mut observed = 0;
    while let Some(row) = rows.next()? {
        check_cancellation(cancellation)?;
        let generation = header(row)?;
        let predecessor = previous
            .as_ref()
            .filter(|(prior, _)| prior.dataset_id == generation.dataset_id);
        if predecessor.map_or(generation.manifest_version != 1, |(prior, _)| {
            prior.manifest_version.checked_add(1) != Some(generation.manifest_version)
                || prior.schema != generation.schema
        }) {
            return Err(CatalogError::AnalyticalEvidenceInvalid);
        }
        digest
            .generation_header(&generation)
            .map_err(map_evidence_error)?;
        stream_parents(connection, &generation, digest, references, cancellation)?;
        let objects = stream_objects(
            connection,
            &generation,
            predecessor,
            digest,
            references,
            cancellation,
        )?;
        previous = Some((generation, objects));
        add(&mut observed, 1)?;
    }
    check_count(expected, observed)
}
fn stream_parents(
    connection: &Connection,
    child: &GenerationEvidenceHeader,
    digest: &mut EvidenceDigest,
    references: &mut u64,
    cancellation: &CancellationToken,
) -> Result<(), CatalogError> {
    let args = (
        child.dataset_id.as_str(),
        sqlite_integer(child.manifest_version)?,
    );
    let expected=parse_nonnegative_u64(connection.query_row("SELECT COUNT(*) FROM analytical_generation_parents WHERE child_dataset_id=?1 AND child_manifest_version=?2",args,|row|row.get::<_, i64>(0))?)?;
    if expected > MAX_DERIVED_GENERATION_PARENTS as u64
        || match child.kind {
            GenerationKind::Ingest if child.manifest_version == 1 => expected != 0,
            GenerationKind::Ingest | GenerationKind::Compaction => expected != 1,
            GenerationKind::Derived => expected == 0,
        }
    {
        return Err(CatalogError::AnalyticalEvidenceInvalid);
    }
    digest
        .section("parents", expected)
        .map_err(map_evidence_error)?;
    let mut statement=connection.prepare("SELECT ordinal,relation,parent_generation_sequence,parent_dataset_id,parent_manifest_version,
        parent_schema_name,parent_schema_version,parent_schema_fingerprint,parent_content_hash
        FROM analytical_generation_parents WHERE child_dataset_id=?1 AND child_manifest_version=?2 ORDER BY ordinal")?;
    let mut rows = statement.query(args)?;
    let mut observed = 0;
    let mut previous: Option<DatasetManifestRef> = None;
    while let Some(row) = rows.next()? {
        check_cancellation(cancellation)?;
        check_count(observed, parse_nonnegative_u64(row.get::<_, i64>(0)?)?)?;
        let relation = GenerationParentRelation::from_database_name(&row.get::<_, String>(1)?)
            .ok_or(CatalogError::CorruptCatalog)?;
        let sequence = parse_positive_u64(row.get(2)?)?;
        let dataset = DatasetId::try_from(row.get::<_, String>(3)?.as_str())
            .map_err(|_| CatalogError::CorruptCatalog)?;
        let version = parse_positive_u64(row.get(4)?)?;
        let parent = DatasetManifestRef::try_new_with_schema(
            dataset,
            version,
            schema(row, 5, 6, 7)?,
            parse_sha256(1, row.get(8)?)?,
        )
        .map_err(|_| CatalogError::CorruptCatalog)?;
        if previous
            .as_ref()
            .is_some_and(|prior| compare_manifest_refs(prior, &parent).is_ge())
        {
            return Err(CatalogError::AnalyticalEvidenceInvalid);
        }
        let mut retained = connection.prepare(&format!(
            "{HEADERS} WHERE dataset_id=?1 AND manifest_version=?2"
        ))?;
        let mut retained = retained.query((
            parent.dataset_id().as_str(),
            sqlite_integer(parent.manifest_version())?,
        ))?;
        let retained = header(retained.next()?.ok_or(CatalogError::CorruptCatalog)?)?;
        if retained.generation_sequence != sequence
            || sequence >= child.generation_sequence
            || retained.schema != *parent.schema()
            || retained.content_hash != parent.content_hash()
        {
            return Err(CatalogError::AnalyticalEvidenceInvalid);
        }
        let valid = match child.kind {
            GenerationKind::Derived => relation == GenerationParentRelation::DerivedInput,
            GenerationKind::Ingest | GenerationKind::Compaction => {
                let expected = if child.kind == GenerationKind::Ingest {
                    GenerationParentRelation::AppendPredecessor
                } else {
                    GenerationParentRelation::CompactionPredecessor
                };
                relation == expected
                    && parent.dataset_id() == &child.dataset_id
                    && parent.manifest_version().checked_add(1) == Some(child.manifest_version)
                    && parent.schema() == &child.schema
            }
        };
        if !valid {
            return Err(CatalogError::AnalyticalEvidenceInvalid);
        }
        let edge = GenerationParentEvidenceRow::try_new(sequence, relation, parent.clone())
            .map_err(map_evidence_error)?;
        digest.parent(observed, &edge).map_err(map_evidence_error)?;
        previous = Some(parent);
        add(&mut observed, 1)?;
    }
    check_count(expected, observed)?;
    add(references, observed)
}
fn stream_objects(
    connection: &Connection,
    generation: &GenerationEvidenceHeader,
    previous: Option<&(GenerationEvidenceHeader, u64)>,
    digest: &mut EvidenceDigest,
    references: &mut u64,
    cancellation: &CancellationToken,
) -> Result<u64, CatalogError> {
    let args = (
        generation.dataset_id.as_str(),
        sqlite_integer(generation.manifest_version)?,
    );
    let expected=parse_nonnegative_u64(connection.query_row("SELECT COUNT(*) FROM analytical_generation_objects WHERE dataset_id=?1 AND manifest_version=?2",args,|row|row.get::<_, i64>(0))?)?;
    if expected == 0 {
        return Err(CatalogError::AnalyticalEvidenceInvalid);
    }
    match generation.kind {
        GenerationKind::Ingest => {
            let run:String=connection.query_row("SELECT a.run_id FROM dataset_manifests m JOIN artifacts a ON a.artifact_id=m.artifact_id WHERE m.manifest_id=?1",[generation.anchor_manifest_id.to_string()],|row|row.get(0))?;
            let group = parse_nonnegative_u64(connection.query_row(
                "SELECT COUNT(*) FROM artifacts WHERE run_id=?1",
                [&run],
                |row| row.get::<_, i64>(0),
            )?)?;
            let inherited = previous.map_or(0, |(_, count)| *count);
            if expected > 1024
                || inherited.checked_add(group) != Some(expected)
                || group == 0
                || group > 1024
            {
                return Err(CatalogError::AnalyticalEvidenceInvalid);
            }
            let invalid:bool=connection.query_row("SELECT EXISTS (
                SELECT 1 FROM analytical_generation_objects current LEFT JOIN artifacts a
                  ON a.run_id=?3 AND a.publication_ordinal=current.ordinal-?4
                 WHERE current.dataset_id=?1 AND current.manifest_version=?2 AND current.ordinal>=?4
                   AND (a.artifact_id IS NULL OR current.artifact_id<>a.artifact_id)
                UNION ALL
                SELECT 1 FROM analytical_generation_objects current LEFT JOIN analytical_generation_objects prior
                  ON prior.dataset_id=current.dataset_id AND prior.manifest_version=current.manifest_version-1 AND prior.ordinal=current.ordinal
                 WHERE current.dataset_id=?1 AND current.manifest_version=?2 AND current.ordinal<?4
                   AND (prior.artifact_id IS NULL OR current.content_hash<>prior.content_hash OR current.row_count<>prior.row_count
                        OR current.size_bytes<>prior.size_bytes OR current.lineage_hash<>prior.lineage_hash))",
                rusqlite::params![generation.dataset_id.as_str(),sqlite_integer(generation.manifest_version)?,run,sqlite_integer(inherited)?],|row|row.get(0))?;
            if invalid {
                return Err(CatalogError::AnalyticalEvidenceInvalid);
            }
        }
        GenerationKind::Compaction => {
            let (previous, _) = previous.ok_or(CatalogError::AnalyticalEvidenceInvalid)?;
            if expected != 1
                || generation.row_count != previous.row_count
                || generation.lineage_hash != previous.lineage_hash
            {
                return Err(CatalogError::AnalyticalEvidenceInvalid);
            }
        }
        GenerationKind::Derived => {}
    }
    digest
        .section("objects", expected)
        .map_err(map_evidence_error)?;
    let mut plan = GenerationPlanEvidence::new(generation).map_err(map_evidence_error)?;
    let mut statement=connection.prepare("SELECT ordinal,artifact_id,content_hash,row_count,size_bytes,lineage_hash FROM analytical_generation_objects WHERE dataset_id=?1 AND manifest_version=?2 ORDER BY ordinal")?;
    let mut rows = statement.query(args)?;
    let mut observed = 0;
    let mut previous_content = None;
    while let Some(row) = rows.next()? {
        check_cancellation(cancellation)?;
        check_count(observed, parse_nonnegative_u64(row.get::<_, i64>(0)?)?)?;
        let object = GenerationObjectEvidenceRow::try_new(
            parse_uuid(row.get(1)?)?,
            parse_sha256(1, row.get(2)?)?,
            parse_positive_u64(row.get(3)?)?,
            parse_positive_u64(row.get(4)?)?,
            parse_sha256(1, row.get(5)?)?,
        )
        .map_err(map_evidence_error)?;
        if generation.kind == GenerationKind::Derived
            && previous_content.is_some_and(|prior| prior >= object.content_hash())
        {
            return Err(CatalogError::AnalyticalEvidenceInvalid);
        }
        previous_content = Some(object.content_hash());
        plan.object(&object).map_err(map_evidence_error)?;
        digest
            .object(observed, &object)
            .map_err(map_evidence_error)?;
        add(&mut observed, 1)?;
    }
    check_count(expected, observed)?;
    plan.finish(generation, expected)
        .map_err(map_evidence_error)?;
    add(references, observed)?;
    Ok(observed)
}
