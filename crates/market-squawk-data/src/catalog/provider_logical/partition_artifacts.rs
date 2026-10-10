//! Ordered canonical-partition placement beneath one unchanged logical publication binding.

use super::super::provider_capture::provider_artifact_input_coordinates_are_ordered;
use super::*;

impl Catalog {
    pub(crate) fn provider_logical_partition_inputs_match_for_run(
        &self,
        run_id: Uuid,
        binding: &SealedProviderLogicalPublicationBinding,
        coordinates: &[ProviderArtifactInputCoordinate],
    ) -> Result<bool, CatalogError> {
        partition_inputs_match(&self.connection, run_id, binding, coordinates)
    }
}

pub(super) fn retain(
    connection: &Connection,
    run_id: Uuid,
    binding: EvidenceDigest,
    coordinates: &[ProviderArtifactInputCoordinate],
) -> Result<(), CatalogError> {
    for (ordinal, coordinate) in coordinates.iter().enumerate() {
        connection.execute(
            "INSERT INTO ingest_run_provider_logical_partition_artifacts
             (run_id, logical_binding_digest, partition_ordinal,
              output_artifact_ordinal, object_input_ordinal) VALUES (?1,?2,?3,?4,?5)",
            params![
                run_id.to_string(),
                binding.bytes(),
                to_i64(ordinal)?,
                to_i64(coordinate.output_artifact_ordinal())?,
                to_i64(coordinate.object_input_ordinal())?
            ],
        )?;
    }
    Ok(())
}

pub(crate) fn partition_inputs_match(
    connection: &Connection,
    run_id: Uuid,
    binding: &SealedProviderLogicalPublicationBinding,
    coordinates: &[ProviderArtifactInputCoordinate],
) -> Result<bool, CatalogError> {
    let (retained_run, retained) = validate_retained(
        connection,
        binding.binding_digest(),
        binding.canonical_partitions(),
    )?;
    Ok(run_id == retained_run && retained == coordinates)
}

/// Reopens the exact creating run, including its output placement. Inherited generation edges
/// retain this original placement; compaction does not create a new provider publication.
pub(super) fn validate_retained(
    connection: &Connection,
    binding: EvidenceDigest,
    canonical: &[CanonicalPartitionExpectation],
) -> Result<(Uuid, Vec<ProviderArtifactInputCoordinate>), CatalogError> {
    let (run, state): (String, String) = connection
        .query_row(
            "SELECT input.run_id, run.state FROM ingest_run_provider_publication_bindings AS input
         JOIN ingest_runs AS run ON run.run_id=input.run_id
         WHERE input.publication_digest=?1 AND input.logical_binding_digest=?1
           AND input.publication_kind='provider_logical' AND input.input_ordinal=0
           AND input.output_artifact_ordinal=0 AND input.object_input_ordinal=0",
            [binding.bytes()],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?
        .ok_or(CatalogError::CorruptCatalog)?;
    let run_id = Uuid::parse_str(&run).map_err(|_| CatalogError::CorruptCatalog)?;
    let (inputs, captures, artifacts): (i64, i64, i64) = connection.query_row(
        "SELECT (SELECT COUNT(*) FROM ingest_run_provider_publication_bindings WHERE run_id=?1),
                (SELECT COUNT(*) FROM ingest_run_provider_capture_bindings WHERE run_id=?1),
                (SELECT COUNT(*) FROM artifacts WHERE run_id=?1)",
        [&run],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
    )?;
    if inputs != 1
        || captures != 0
        || !(1..=1024).contains(&artifacts)
        || canonical.is_empty()
        || canonical.len() > MAX_PROVIDER_CANONICAL_PARTITIONS
        || !matches!(state.as_str(), "reserved" | "succeeded")
    {
        return Err(CatalogError::CorruptCatalog);
    }
    let mut statement = connection.prepare(
        "SELECT partition_ordinal, output_artifact_ordinal, object_input_ordinal, logical_binding_digest
         FROM ingest_run_provider_logical_partition_artifacts
         WHERE run_id=?1 ORDER BY partition_ordinal LIMIT 1025",
    )?;
    let mut rows = statement.query([&run])?;
    let mut coordinates = Vec::new();
    coordinates
        .try_reserve_exact(canonical.len())
        .map_err(|_| CatalogError::Allocation)?;
    let mut expected_rows =
        vec![0_u64; usize::try_from(artifacts).map_err(|_| CatalogError::CorruptCatalog)?];
    while let Some(row) = rows.next()? {
        let ordinal: i64 = row.get(0)?;
        if ordinal != to_i64(coordinates.len())?
            || coordinates.len() == canonical.len()
            || parse_digest(1, &row.get::<_, Vec<u8>>(3)?)? != binding
        {
            return Err(CatalogError::CorruptCatalog);
        }
        let output =
            usize::try_from(row.get::<_, i64>(1)?).map_err(|_| CatalogError::CorruptCatalog)?;
        let local =
            usize::try_from(row.get::<_, i64>(2)?).map_err(|_| CatalogError::CorruptCatalog)?;
        let coordinate = ProviderArtifactInputCoordinate::try_new(output, local)
            .map_err(|_| CatalogError::CorruptCatalog)?;
        let count = expected_rows
            .get_mut(output)
            .ok_or(CatalogError::CorruptCatalog)?;
        *count = count
            .checked_add(u64::from(
                canonical[coordinates.len()].row_range().item_count().get(),
            ))
            .ok_or(CatalogError::CorruptCatalog)?;
        coordinates.push(coordinate);
    }
    if coordinates.len() != canonical.len()
        || !provider_artifact_input_coordinates_are_ordered(&coordinates)
        || expected_rows.contains(&0)
    {
        return Err(CatalogError::CorruptCatalog);
    }
    if state == "succeeded" {
        let mut statement = connection.prepare(
            "SELECT output.publication_ordinal, object.row_count
             FROM artifacts AS output
             JOIN dataset_manifests AS anchor ON anchor.run_id=output.run_id
             JOIN analytical_generations AS generation ON generation.anchor_manifest_id=anchor.manifest_id
               AND generation.generation_kind='ingest'
             JOIN analytical_generation_source_inputs AS source
               ON source.generation_sequence=generation.generation_sequence AND source.run_id=output.run_id
             JOIN analytical_generation_objects AS object ON object.dataset_id=generation.dataset_id
               AND object.manifest_version=generation.manifest_version AND object.artifact_id=output.artifact_id
             WHERE output.run_id=?1 ORDER BY output.publication_ordinal LIMIT 1025",
        )?;
        let mut rows = statement.query([&run])?;
        let mut count = 0;
        while let Some(row) = rows.next()? {
            if row.get::<_, i64>(0)? != to_i64(count)?
                || u64::try_from(row.get::<_, i64>(1)?).ok() != expected_rows.get(count).copied()
            {
                return Err(CatalogError::CorruptCatalog);
            }
            count += 1;
        }
        if count != expected_rows.len() {
            return Err(CatalogError::CorruptCatalog);
        }
    }
    Ok((run_id, coordinates))
}
