//! Direct-source and exact-parent publication availability, shared by every writer.

use market_squawk_domain::Timestamp;
use rusqlite::{OptionalExtension as _, Transaction, params};

use super::{DatasetManifestRef, ManifestCatalogError};
use crate::catalog::trusted_catalog_now;

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
    let (count, succeeded, direct_completed): (i64, bool, Option<i64>) = transaction.query_row(
        "SELECT COUNT(*), COALESCE(MIN(run.state='succeeded' AND run.completed_at_ns IS NOT NULL), 1),
                MAX(run.completed_at_ns)
         FROM analytical_generation_source_inputs AS input
         JOIN ingest_runs AS run USING (run_id) WHERE input.generation_sequence=?1",
        [sequence], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
    )?;
    let (expected_parents, actual_parents, parent_completed): (i64, i64, Option<i64>) = transaction
        .query_row(
            "SELECT generation.parent_count, COUNT(proof.generation_sequence),
                MAX(proof.source_runs_completed_at_ns)
         FROM analytical_generations AS generation
         LEFT JOIN analytical_generation_parents AS edge
           ON edge.child_dataset_id=generation.dataset_id
          AND edge.child_manifest_version=generation.manifest_version
         LEFT JOIN analytical_generation_source_availability_proofs AS proof
           ON proof.generation_sequence=edge.parent_generation_sequence
         WHERE generation.generation_sequence=?1
         GROUP BY generation.generation_sequence",
            [sequence],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )?;
    let completed = direct_completed
        .into_iter()
        .chain(parent_completed)
        .max()
        .filter(|_| succeeded && count <= 1 && actual_parents == expected_parents)
        .ok_or(ManifestCatalogError::SourceRunsIncomplete)?;
    // All objects, parent edges, bindings and run-success transitions precede this sample.
    let now = trusted_catalog_now(transaction)?;
    if completed > now.unix_nanos() {
        return Err(ManifestCatalogError::SourceRunsIncomplete);
    }
    transaction.execute(
        "INSERT INTO analytical_generation_source_availability_proofs
         (generation_sequence, transaction_available_at_ns, direct_source_run_count,
          source_runs_completed_at_ns, effective_available_at_ns)
         VALUES (?1, ?2, ?3, ?4, ?2)",
        params![sequence, now.unix_nanos(), count, completed],
    )?;
    Ok(now)
}
