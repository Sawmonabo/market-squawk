//! Exact generation membership without copying inherited publication closures.

use super::*;

/// SQL expressions passed here are fixed source-code identifiers/placeholders, never input data.
/// The correlated search stops at the requested ancestor, retaining exact parent identities.
/// Sequence pruning is valid because every parent predates its immutable child.
pub(crate) fn generation_contains_origin_sql(selected: &str, origin: &str) -> String {
    format!(
        "EXISTS (
         WITH RECURSIVE exact_ancestry(sequence) AS (
             SELECT generation_sequence FROM analytical_generations
             WHERE generation_sequence={selected}
             UNION
             SELECT parent.generation_sequence
             FROM exact_ancestry AS lineage
             JOIN analytical_generations AS child ON child.generation_sequence=lineage.sequence
             JOIN analytical_generation_parents AS edge
               ON edge.child_dataset_id=child.dataset_id
              AND edge.child_manifest_version=child.manifest_version
             JOIN analytical_available_generations AS parent
               ON parent.generation_sequence=edge.parent_generation_sequence
              AND parent.dataset_id=edge.parent_dataset_id
              AND parent.manifest_version=edge.parent_manifest_version
              AND parent.schema_name=edge.parent_schema_name
              AND parent.schema_version=edge.parent_schema_version
              AND parent.schema_fingerprint=edge.parent_schema_fingerprint
              AND parent.content_hash=edge.parent_content_hash
             WHERE lineage.sequence>{origin} AND parent.generation_sequence>={origin}
               AND parent.generation_sequence<lineage.sequence
         ) SELECT 1 FROM exact_ancestry WHERE sequence={origin} LIMIT 1
         )"
    )
}

pub(crate) fn generation_contains_origin(
    connection: &Connection,
    selected_sequence: i64,
    origin_sequence: i64,
) -> Result<bool, ManifestCatalogError> {
    if origin_sequence <= 0 || selected_sequence < origin_sequence {
        return Ok(false);
    }
    connection
        .query_row(
            &format!("SELECT {}", generation_contains_origin_sql("?1", "?2")),
            params![selected_sequence, origin_sequence],
            |row| row.get(0),
        )
        .map_err(ManifestCatalogError::from)
}

pub(super) fn exact_generation_sequence(
    connection: &Connection,
    manifest: &DatasetManifestRef,
) -> Result<i64, ManifestCatalogError> {
    connection
        .query_row(
            "SELECT generation_sequence FROM analytical_available_generations
         WHERE dataset_id=?1 AND manifest_version=?2 AND schema_name=?3
           AND schema_version=?4 AND schema_fingerprint=?5 AND content_hash=?6",
            params![
                manifest.dataset_id().as_str(),
                to_i64(manifest.manifest_version())?,
                manifest.schema().name(),
                i64::from(manifest.schema().version().get()),
                manifest.schema().fingerprint().as_slice(),
                manifest.content_hash().bytes()
            ],
            |row| row.get(0),
        )
        .optional()?
        .ok_or(ManifestCatalogError::GenerationConflict)
}

pub(super) fn publication_origin(
    connection: &Connection,
    manifest: &DatasetManifestRef,
    digest: EvidenceDigest,
    kind: &str,
) -> Result<Option<(i64, Uuid)>, ManifestCatalogError> {
    validate_publication_kind(kind)?;
    let selected = exact_generation_sequence(connection, manifest)?;
    let mut statement = connection.prepare(
        "SELECT input.generation_sequence, publication.run_id
         FROM ingest_run_provider_publication_bindings AS publication
         JOIN analytical_generation_source_inputs AS input
           ON input.run_id=publication.run_id AND input.source_id=publication.source_id
         JOIN analytical_generation_provider_publication_bindings AS direct
           ON direct.generation_sequence=input.generation_sequence
          AND direct.publication_digest=publication.publication_digest
          AND direct.publication_kind=publication.publication_kind
          AND direct.run_id=publication.run_id AND direct.source_id=publication.source_id
         WHERE publication.publication_digest=?1 AND publication.publication_kind=?2",
    )?;
    let mut rows = statement.query(params![digest.bytes(), kind])?;
    let mut matched = None;
    while let Some(row) = rows.next()? {
        let origin = row.get(0)?;
        if generation_contains_origin(connection, selected, origin)? {
            if matched.is_some() {
                return Err(ManifestCatalogError::CorruptCatalog);
            }
            let run = Uuid::parse_str(&row.get::<_, String>(1)?)
                .map_err(|_| ManifestCatalogError::CorruptCatalog)?;
            matched = Some((origin, run));
        }
    }
    Ok(matched)
}

pub(super) fn capture_is_member(
    connection: &Connection,
    manifest: &DatasetManifestRef,
    digest: EvidenceDigest,
) -> Result<bool, ManifestCatalogError> {
    let selected = exact_generation_sequence(connection, manifest)?;
    let mut statement = connection.prepare(
        "SELECT direct.generation_sequence
         FROM ingest_run_provider_capture_bindings AS original
         JOIN analytical_generation_source_inputs AS input
           ON input.run_id=original.run_id AND input.source_id=original.source_id
         JOIN analytical_generation_provider_capture_bindings AS direct
           ON direct.generation_sequence=input.generation_sequence
          AND direct.binding_digest=original.binding_digest
          AND direct.run_id=original.run_id AND direct.source_id=original.source_id
         WHERE original.binding_digest=?1",
    )?;
    let mut rows = statement.query([digest.bytes()])?;
    while let Some(row) = rows.next()? {
        if generation_contains_origin(connection, selected, row.get(0)?)? {
            return Ok(true);
        }
    }
    Ok(false)
}

pub(super) fn validate_publication_kind(kind: &str) -> Result<(), ManifestCatalogError> {
    if matches!(
        kind,
        "response_market_event"
            | "event_microbatch"
            | "composite_response_event"
            | "option_snapshots"
            | "option_expirations"
            | "provider_logical"
    ) {
        Ok(())
    } else {
        Err(ManifestCatalogError::GenerationConflict)
    }
}

pub(super) fn publication_page(
    connection: &Connection,
    manifest: &DatasetManifestRef,
    after: Option<EvidenceDigest>,
    limit: usize,
    options: bool,
) -> Result<Vec<(EvidenceDigest, String)>, ManifestCatalogError> {
    validate_page(after, limit)?;
    let sequence = exact_generation_sequence(connection, manifest)?;
    let member = generation_contains_origin_sql("?1", "publication.generation_sequence");
    let kinds = if options {
        "'option_snapshots','option_expirations'"
    } else {
        "'response_market_event','event_microbatch','composite_response_event'"
    };
    let mut statement = connection.prepare(&format!(
        "SELECT publication.publication_digest, MIN(publication.publication_kind),
                COUNT(DISTINCT publication.publication_kind)
         FROM analytical_generation_provider_publication_bindings AS publication
         WHERE (?2 IS NULL OR publication.publication_digest>?2)
           AND publication.publication_kind IN ({kinds}) AND {member}
         GROUP BY publication.publication_digest
         ORDER BY publication.publication_digest LIMIT ?3"
    ))?;
    let after = after.map(|digest| digest.bytes());
    let mut rows = statement.query(params![
        sequence,
        after.as_ref().map(|bytes| bytes.as_slice()),
        to_i64(limit as u64)?
    ])?;
    let mut page = Vec::new();
    while let Some(row) = rows.next()? {
        if row.get::<_, i64>(2)? != 1 {
            return Err(ManifestCatalogError::CorruptCatalog);
        }
        page.push((
            parse_digest(&row.get::<_, Vec<u8>>(0)?)?.evidence(),
            row.get(1)?,
        ));
    }
    Ok(page)
}

pub(super) fn capture_page(
    connection: &Connection,
    manifest: &DatasetManifestRef,
    after: Option<EvidenceDigest>,
    limit: usize,
) -> Result<Vec<EvidenceDigest>, ManifestCatalogError> {
    validate_page(after, limit)?;
    let sequence = exact_generation_sequence(connection, manifest)?;
    let member = generation_contains_origin_sql("?1", "binding.generation_sequence");
    let mut statement = connection.prepare(&format!(
        "SELECT DISTINCT binding.binding_digest
         FROM analytical_generation_provider_capture_bindings AS binding
         WHERE (?2 IS NULL OR binding.binding_digest>?2) AND {member}
         ORDER BY binding.binding_digest LIMIT ?3"
    ))?;
    let after = after.map(|digest| digest.bytes());
    let mut rows = statement.query(params![
        sequence,
        after.as_ref().map(|bytes| bytes.as_slice()),
        to_i64(limit as u64)?
    ])?;
    let mut page = Vec::new();
    while let Some(row) = rows.next()? {
        page.push(parse_digest(&row.get::<_, Vec<u8>>(0)?)?.evidence());
    }
    Ok(page)
}

fn validate_page(after: Option<EvidenceDigest>, limit: usize) -> Result<(), ManifestCatalogError> {
    if limit == 0
        || limit > MAX_GENERATION_CAPTURE_INPUTS
        || after.is_some_and(|digest| digest.algorithm() != DigestAlgorithm::Sha256)
    {
        return Err(ManifestCatalogError::CaptureInputLimitExceeded {
            max: MAX_GENERATION_CAPTURE_INPUTS,
        });
    }
    Ok(())
}
