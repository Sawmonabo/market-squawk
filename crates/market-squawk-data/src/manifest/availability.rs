//! Bounded successful-source closure and publication availability, shared by every writer.

use market_squawk_domain::Timestamp;
use rusqlite::{Connection, OptionalExtension as _, Transaction, params};

use super::{DatasetManifestRef, ManifestCatalogError};
use crate::catalog::trusted_catalog_now;

const MAX_GENERATION_SOURCE_RUNS: usize = 4_096;

pub(crate) fn finalize_generation_availability(
    transaction: &Transaction<'_>,
    manifest: &DatasetManifestRef,
) -> Result<Timestamp, ManifestCatalogError> {
    let sequence: i64 = transaction.query_row(
        "SELECT generation_sequence FROM analytical_generations
         WHERE dataset_id=?1 AND manifest_version=?2 AND content_hash=?3",
        params![
            manifest.dataset_id().as_str(),
            i64::try_from(manifest.manifest_version())
                .map_err(|_| ManifestCatalogError::CountOverflow)?,
            manifest.content_hash().bytes()
        ],
        |row| row.get(0),
    )?;
    // An exact publication replay must retain its original clock.
    if let Some(available) = transaction
        .query_row(
            "SELECT effective_available_at_ns FROM analytical_generation_source_availability_proofs
         WHERE generation_sequence=?1",
            [sequence],
            |row| row.get::<_, i64>(0),
        )
        .optional()?
    {
        return Ok(Timestamp::from_unix_nanos(available));
    }
    insert_generation_transitive_source_runs(transaction, sequence)?;
    let (count, succeeded, completed): (i64, bool, Option<i64>) = transaction.query_row(
        "SELECT COUNT(*), COALESCE(MIN(run.state='succeeded' AND run.completed_at_ns IS NOT NULL), 0),
                MAX(run.completed_at_ns)
         FROM analytical_generation_transitive_source_runs AS input
         JOIN ingest_runs AS run USING (run_id) WHERE input.generation_sequence=?1",
        [sequence], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
    )?;
    let completed = completed
        .filter(|_| succeeded)
        .ok_or(ManifestCatalogError::SourceRunsIncomplete)?;
    // All objects, parent edges, bindings and run-success transitions precede this sample.
    let now = trusted_catalog_now(transaction)?;
    if completed > now.unix_nanos() {
        return Err(ManifestCatalogError::SourceRunsIncomplete);
    }
    transaction.execute(
        "INSERT INTO analytical_generation_source_availability_proofs
         (generation_sequence, transaction_available_at_ns, source_run_count,
          source_runs_completed_at_ns, effective_available_at_ns)
         VALUES (?1, ?2, ?3, ?4, ?2)",
        params![sequence, now.unix_nanos(), count, completed],
    )?;
    Ok(now)
}

fn insert_generation_transitive_source_runs(
    transaction: &Transaction<'_>,
    generation_sequence: i64,
) -> Result<(), ManifestCatalogError> {
    if generation_sequence <= 0 {
        return Err(ManifestCatalogError::CorruptCatalog);
    }
    let (candidate_count, distinct_runs): (i64, i64) = transaction.query_row(
        "WITH candidates AS (
             SELECT direct.run_id, direct.source_id, direct.rights_id
             FROM analytical_generation_source_inputs AS direct
             WHERE direct.generation_sequence=?1
             UNION
             SELECT parent_input.run_id, parent_input.source_id, parent_input.rights_id
             FROM analytical_generations AS child
             JOIN analytical_generation_parents AS edge
               ON edge.child_dataset_id=child.dataset_id
              AND edge.child_manifest_version=child.manifest_version
             JOIN analytical_generation_transitive_source_runs AS parent_input
               ON parent_input.generation_sequence=edge.parent_generation_sequence
             WHERE child.generation_sequence=?1
         )
         SELECT COUNT(*), COUNT(DISTINCT run_id) FROM candidates",
        [generation_sequence],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    let candidate_count =
        usize::try_from(candidate_count).map_err(|_| ManifestCatalogError::CorruptCatalog)?;
    if candidate_count == 0
        || candidate_count > MAX_GENERATION_SOURCE_RUNS
        || i64::try_from(candidate_count).ok() != Some(distinct_runs)
    {
        return Err(ManifestCatalogError::SourceRunInputLimitExceeded {
            max: MAX_GENERATION_SOURCE_RUNS,
        });
    }
    let inserted = transaction.execute(
        "INSERT INTO analytical_generation_transitive_source_runs
         (generation_sequence, input_ordinal, run_id, source_id, rights_id)
         WITH candidates AS (
             SELECT direct.run_id, direct.source_id, direct.rights_id
             FROM analytical_generation_source_inputs AS direct
             WHERE direct.generation_sequence=?1
             UNION
             SELECT parent_input.run_id, parent_input.source_id, parent_input.rights_id
             FROM analytical_generations AS child
             JOIN analytical_generation_parents AS edge
               ON edge.child_dataset_id=child.dataset_id
              AND edge.child_manifest_version=child.manifest_version
             JOIN analytical_generation_transitive_source_runs AS parent_input
               ON parent_input.generation_sequence=edge.parent_generation_sequence
             WHERE child.generation_sequence=?1
         )
         SELECT ?1, ROW_NUMBER() OVER (ORDER BY run_id, source_id, rights_id) - 1,
                run_id, source_id, rights_id
         FROM candidates
         ORDER BY run_id, source_id, rights_id",
        [generation_sequence],
    )?;
    if inserted != candidate_count
        || !generation_transitive_source_runs_match(transaction, generation_sequence)?
    {
        return Err(ManifestCatalogError::CorruptCatalog);
    }
    Ok(())
}

fn generation_transitive_source_runs_match(
    connection: &Connection,
    generation_sequence: i64,
) -> Result<bool, ManifestCatalogError> {
    connection
        .query_row(
            "WITH candidates AS (
                 SELECT direct.run_id, direct.source_id, direct.rights_id
                 FROM analytical_generation_source_inputs AS direct
                 WHERE direct.generation_sequence=?1
                 UNION
                 SELECT parent_input.run_id, parent_input.source_id, parent_input.rights_id
                 FROM analytical_generations AS child
                 JOIN analytical_generation_parents AS edge
                   ON edge.child_dataset_id=child.dataset_id
                  AND edge.child_manifest_version=child.manifest_version
                 JOIN analytical_generation_transitive_source_runs AS parent_input
                   ON parent_input.generation_sequence=edge.parent_generation_sequence
                 WHERE child.generation_sequence=?1
             ), expected AS (
                 SELECT ROW_NUMBER() OVER (ORDER BY run_id, source_id, rights_id) - 1
                            AS input_ordinal,
                        run_id, source_id, rights_id
                 FROM candidates
             ), actual AS (
                 SELECT input_ordinal, run_id, source_id, rights_id
                 FROM analytical_generation_transitive_source_runs
                 WHERE generation_sequence=?1
             )
             SELECT NOT EXISTS(SELECT * FROM expected EXCEPT SELECT * FROM actual)
                AND NOT EXISTS(SELECT * FROM actual EXCEPT SELECT * FROM expected)",
            [generation_sequence],
            |row| row.get(0),
        )
        .map_err(ManifestCatalogError::from)
}
