//! Atomic derived generation publication under a freshly revalidated permit.

use market_squawk_domain::Timestamp;
use rusqlite::{OptionalExtension as _, Transaction, params};
use uuid::Uuid;

use super::catalog::{
    DerivedOutputObjectInput, PublishedDerivedGeneration, ResearchUseCatalogError,
    retention_operation_name,
};
use super::identity::{output_reservation_digest_parts, research_use_mask, to_i64, to_i64_usize};
use super::{DerivedPublicationInput, DerivedRetentionOperation};
use crate::manifest::{
    ManifestCatalogError, propagate_generation_market_bar_history_inputs,
    propagate_generation_provider_capture_bindings,
    propagate_generation_provider_publication_bindings,
};
use crate::{DatasetId, DatasetManifestRef, DatasetSchemaRegistry, GenerationParentRelation};

pub(super) fn publish(
    transaction: &Transaction<'_>,
    session_id: Uuid,
    now: Timestamp,
    input: DerivedPublicationInput,
) -> Result<PublishedDerivedGeneration, ResearchUseCatalogError> {
    validate_permit(transaction, session_id, now, &input)?;
    DatasetSchemaRegistry::local()
        .resolve(input.schema())
        .map_err(|_| ResearchUseCatalogError::InvalidPublication)?;
    if input.plan().dataset_id().as_str().is_empty() {
        return Err(ResearchUseCatalogError::InvalidPublication);
    }
    let output_group_id = input.digest().bytes();
    let already_exists: bool = transaction.query_row(
        "SELECT EXISTS(
            SELECT 1 FROM derived_output_groups
            WHERE output_group_id=?1 OR decision_id=?2
         )",
        params![output_group_id, input.decision_digest().bytes()],
        |row| row.get(0),
    )?;
    if already_exists {
        return Err(ResearchUseCatalogError::InvalidPublication);
    }
    let anchor_manifest_id = validate_outputs(transaction, session_id, now, &input)?;
    let parent_sequences = validate_parents(transaction, &input)?;
    let version = next_version(transaction, input.plan().dataset_id())?;
    reject_schema_change(transaction, &input)?;

    transaction.execute(
        "INSERT INTO analytical_generations
         (dataset_id, manifest_version, content_hash, lineage_hash, row_count, total_bytes,
          schema_name, schema_version, schema_fingerprint, anchor_manifest_id, generation_kind,
          parent_count, build_spec_digest, created_at_ns)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, 'derived', ?11, ?12, ?13)",
        params![
            input.plan().dataset_id().as_str(),
            to_i64(version)?,
            input.plan().content_hash().bytes(),
            input.plan().lineage_digest().bytes(),
            to_i64(input.plan().row_count())?,
            to_i64(input.plan().total_bytes())?,
            input.schema().name(),
            i64::from(input.schema().version().get()),
            input.schema().fingerprint(),
            anchor_manifest_id.to_string(),
            to_i64_usize(parent_sequences.len())?,
            input.build_spec_digest().digest().bytes(),
            now.unix_nanos(),
        ],
    )?;
    let generation_sequence = positive_u64(transaction.last_insert_rowid())?;
    for (ordinal, (object, planned)) in input
        .objects()
        .iter()
        .zip(input.plan().objects())
        .enumerate()
    {
        transaction.execute(
            "INSERT INTO analytical_generation_objects
             (dataset_id, manifest_version, ordinal, artifact_id, content_hash, row_count,
              size_bytes, lineage_hash)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            params![
                input.plan().dataset_id().as_str(),
                to_i64(version)?,
                to_i64_usize(ordinal)?,
                object.artifact_id().to_string(),
                planned.content_hash().bytes(),
                to_i64(planned.row_count())?,
                to_i64(planned.size_bytes())?,
                planned.lineage_digest().bytes(),
            ],
        )?;
    }
    for (ordinal, (parent, sequence)) in input
        .parents()
        .iter()
        .zip(parent_sequences.iter().copied())
        .enumerate()
    {
        transaction.execute(
            "INSERT INTO analytical_generation_parents
             (child_dataset_id, child_manifest_version, ordinal, relation,
              parent_generation_sequence, parent_dataset_id, parent_manifest_version,
              parent_schema_name, parent_schema_version, parent_schema_fingerprint,
              parent_content_hash)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
            params![
                input.plan().dataset_id().as_str(),
                to_i64(version)?,
                to_i64_usize(ordinal)?,
                GenerationParentRelation::DerivedInput.database_name(),
                to_i64(sequence)?,
                parent.dataset_id().as_str(),
                to_i64(parent.manifest_version())?,
                parent.schema().name(),
                i64::from(parent.schema().version().get()),
                parent.schema().fingerprint(),
                parent.content_hash().bytes(),
            ],
        )?;
    }
    let generation_sequence_i64 = to_i64(generation_sequence)?;
    propagate_generation_provider_capture_bindings(transaction, generation_sequence_i64)
        .map_err(map_manifest_lineage_error)?;
    propagate_generation_provider_publication_bindings(transaction, generation_sequence_i64)
        .map_err(map_manifest_lineage_error)?;
    propagate_generation_market_bar_history_inputs(transaction, generation_sequence_i64)
        .map_err(map_manifest_lineage_error)?;
    for (ordinal, object) in input.objects().iter().enumerate() {
        transaction.execute(
            "INSERT INTO derived_output_group_members
             (output_group_id, ordinal, run_id, artifact_id, retention_operation, rights_id)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                output_group_id,
                to_i64_usize(ordinal)?,
                object.run_id().to_string(),
                object.artifact_id().to_string(),
                retention_operation_name(object.operation()),
                object.rights_id(),
            ],
        )?;
    }
    let retention_operation = input
        .objects()
        .first()
        .map(|object| retention_operation_name(object.operation()))
        .ok_or(ResearchUseCatalogError::InvalidPublication)?;
    transaction.execute(
        "INSERT INTO derived_output_groups
         (output_group_id, decision_id, parent_graph_digest, dataset_id, schema_name,
          schema_version, schema_fingerprint, build_spec_digest, plan_content_hash,
          plan_lineage_hash, row_count, object_count, total_bytes, retention_operation,
          anchor_artifact_id, anchor_manifest_id, committed_at_ns)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15,
                 ?16, ?17)",
        params![
            output_group_id,
            input.decision_digest().bytes(),
            input.graph_digest().bytes(),
            input.plan().dataset_id().as_str(),
            input.schema().name(),
            i64::from(input.schema().version().get()),
            input.schema().fingerprint(),
            input.build_spec_digest().digest().bytes(),
            input.plan().content_hash().bytes(),
            input.plan().lineage_digest().bytes(),
            to_i64(input.plan().row_count())?,
            to_i64_usize(input.objects().len())?,
            to_i64(input.plan().total_bytes())?,
            retention_operation,
            input.anchor_artifact_id().to_string(),
            anchor_manifest_id.to_string(),
            now.unix_nanos(),
        ],
    )?;
    transaction.execute(
        "INSERT INTO derived_generation_authorizations
         (generation_sequence, decision_id, output_group_id, requested_use, graph_digest,
          build_spec_digest)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        params![
            to_i64(generation_sequence)?,
            input.decision_digest().bytes(),
            output_group_id,
            input.requested_use().database_name(),
            input.graph_digest().bytes(),
            input.build_spec_digest().digest().bytes(),
        ],
    )?;
    complete_output_runs(transaction, now, &input)?;
    transaction.execute(
        "INSERT INTO audit_events(event_type, subject_id, details_digest, occurred_at_ns)
         VALUES ('derived-generation.authorized', ?1, ?2, ?3)",
        params![
            encode_hex(output_group_id),
            output_group_id,
            now.unix_nanos()
        ],
    )?;
    let manifest = DatasetManifestRef::try_new_with_schema(
        DatasetId::try_from(input.plan().dataset_id().as_str())
            .map_err(|_| ResearchUseCatalogError::InvalidPublication)?,
        version,
        input.schema().clone(),
        input.plan().content_hash(),
    )
    .map_err(|_| ResearchUseCatalogError::InvalidPublication)?;
    Ok(PublishedDerivedGeneration::new(
        generation_sequence,
        manifest,
        output_group_id,
    ))
}

fn validate_permit(
    transaction: &Transaction<'_>,
    session_id: Uuid,
    now: Timestamp,
    input: &DerivedPublicationInput,
) -> Result<(), ResearchUseCatalogError> {
    if input.permit().session_id() != session_id {
        return Err(ResearchUseCatalogError::InvalidPermitSession);
    }
    if now >= input.permit().expires_at() {
        return Err(ResearchUseCatalogError::Expired);
    }
    let decision_matches: bool = transaction.query_row(
        "SELECT EXISTS(
            SELECT 1 FROM research_use_decisions
            WHERE decision_id=?1 AND graph_digest=?2 AND requested_use=?3
              AND outcome='allowed' AND expires_at_ns=?4
              AND decided_at_ns<=?5 AND ?5<expires_at_ns
         )",
        params![
            input.decision_digest().bytes(),
            input.graph_digest().bytes(),
            input.requested_use().database_name(),
            input.permit().expires_at().unix_nanos(),
            now.unix_nanos(),
        ],
        |row| row.get(0),
    )?;
    if !decision_matches {
        return Err(ResearchUseCatalogError::Expired);
    }
    let expired: bool = transaction.query_row(
        "SELECT EXISTS(
            SELECT 1
            FROM research_use_decision_sources AS source
            LEFT JOIN source_research_use_grants AS grant
              ON grant.research_grant_id=source.selected_research_grant_id
            LEFT JOIN source_rights AS rights ON rights.rights_id=source.rights_id
            WHERE source.decision_id=?1
              AND (
                  source.selection_outcome<>'selected'
                  OR grant.research_grant_id IS NULL
                  OR rights.rights_id IS NULL
                  OR (grant.authorization_expires_at_ns IS NOT NULL
                      AND grant.authorization_expires_at_ns<=?2)
                  OR (rights.authorization_expires_at_ns IS NOT NULL
                      AND rights.authorization_expires_at_ns<=?2)
              )
         )",
        params![input.decision_digest().bytes(), now.unix_nanos()],
        |row| row.get(0),
    )?;
    if expired {
        return Err(ResearchUseCatalogError::Expired);
    }
    let revoked: bool = transaction.query_row(
        "SELECT EXISTS(
            SELECT 1
            FROM research_use_decision_sources AS source
            JOIN source_research_use_revocations AS revocation
              ON revocation.research_grant_id=source.selected_research_grant_id
            WHERE source.decision_id=?1
              AND revocation.effective_at_ns<=?2 AND revocation.recorded_at_ns<=?2
              AND (revocation.use_mask & ?3)<>0
         )",
        params![
            input.decision_digest().bytes(),
            now.unix_nanos(),
            research_use_mask(input.requested_use()),
        ],
        |row| row.get(0),
    )?;
    if revoked {
        Err(ResearchUseCatalogError::Revoked)
    } else {
        Ok(())
    }
}

fn validate_outputs(
    transaction: &Transaction<'_>,
    session_id: Uuid,
    now: Timestamp,
    input: &DerivedPublicationInput,
) -> Result<Uuid, ResearchUseCatalogError> {
    for object in input.objects() {
        let stored = transaction
            .query_row(
                "SELECT run.requested_at_ns, run.operation, run.rights_id, run.state,
                        run.payload_algorithm, run.payload_digest, rights.operation_mask,
                        rights.authorization_expires_at_ns, artifact.content_algorithm,
                        artifact.content_digest, artifact.size_bytes
                 FROM ingest_runs AS run
                 JOIN source_rights AS rights ON rights.rights_id=run.rights_id
                 JOIN artifacts AS artifact ON artifact.run_id=run.run_id
                 WHERE run.run_id=?1 AND artifact.artifact_id=?2",
                params![
                    object.run_id().to_string(),
                    object.artifact_id().to_string()
                ],
                |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, Vec<u8>>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, i64>(4)?,
                        row.get::<_, Vec<u8>>(5)?,
                        row.get::<_, i64>(6)?,
                        row.get::<_, Option<i64>>(7)?,
                        row.get::<_, i64>(8)?,
                        row.get::<_, Vec<u8>>(9)?,
                        row.get::<_, i64>(10)?,
                    ))
                },
            )
            .optional()?
            .ok_or(ResearchUseCatalogError::InvalidPublication)?;
        let operation = parse_operation(&stored.1)?;
        let rights_id = super::identity::parse_digest(stored.2)?;
        let required_operation_mask = match operation {
            DerivedRetentionOperation::Persist => 4,
            DerivedRetentionOperation::Cache => 8,
        };
        let metadata = DerivedOutputObjectInput::try_new(
            object.artifact_id(),
            object.content_hash(),
            object.row_count(),
            object.size_bytes(),
            object.lineage_digest(),
        )?;
        let expected_reservation = output_reservation_digest_parts(
            session_id,
            object.run_id(),
            Timestamp::from_unix_nanos(stored.0),
            operation,
            rights_id,
            &metadata,
        );
        if operation != object.operation()
            || rights_id != object.rights_id()
            || !matches!(stored.3.as_str(), "reserved" | "succeeded")
            || stored.4 != 1
            || stored.5.as_slice() != object.content_hash().bytes()
            || stored.6 & required_operation_mask == 0
            || stored.7.is_some_and(|expiry| now.unix_nanos() >= expiry)
            || stored.8 != 1
            || stored.9.as_slice() != object.content_hash().bytes()
            || u64::try_from(stored.10).ok() != Some(object.size_bytes())
            || object.reservation_digest() != expected_reservation
        {
            return Err(ResearchUseCatalogError::InvalidPublication);
        }
    }
    let anchor = transaction
        .query_row(
            "SELECT manifest_id, dataset_name, schema_version, content_algorithm,
                    content_digest, created_at_ns
             FROM dataset_manifests WHERE artifact_id=?1",
            [input.anchor_artifact_id().to_string()],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, i64>(3)?,
                    row.get::<_, Vec<u8>>(4)?,
                    row.get::<_, i64>(5)?,
                ))
            },
        )
        .optional()?
        .ok_or(ResearchUseCatalogError::InvalidPublication)?;
    if anchor.1 != input.plan().dataset_id().as_str()
        || anchor.2 != i64::from(input.schema().version().get())
        || anchor.3 != 1
        || anchor.4.as_slice() != input.plan().content_hash().bytes()
        || anchor.5 > now.unix_nanos()
    {
        return Err(ResearchUseCatalogError::InvalidPublication);
    }
    Uuid::parse_str(&anchor.0).map_err(|_| ResearchUseCatalogError::CorruptCatalog)
}

fn validate_parents(
    transaction: &Transaction<'_>,
    input: &DerivedPublicationInput,
) -> Result<Vec<u64>, ResearchUseCatalogError> {
    let mut sequences = Vec::new();
    sequences
        .try_reserve_exact(input.parents().len())
        .map_err(|_| ResearchUseCatalogError::LimitExceeded)?;
    for parent in input.parents() {
        let sequence = transaction
            .query_row(
                "SELECT generation_sequence FROM analytical_generations
                 WHERE dataset_id=?1 AND manifest_version=?2 AND schema_name=?3
                   AND schema_version=?4 AND schema_fingerprint=?5 AND content_hash=?6",
                params![
                    parent.dataset_id().as_str(),
                    to_i64(parent.manifest_version())?,
                    parent.schema().name(),
                    i64::from(parent.schema().version().get()),
                    parent.schema().fingerprint(),
                    parent.content_hash().bytes(),
                ],
                |row| row.get::<_, i64>(0),
            )
            .optional()?
            .ok_or(ResearchUseCatalogError::CorruptCatalog)?;
        sequences.push(positive_u64(sequence)?);
    }
    Ok(sequences)
}

fn next_version(
    transaction: &Transaction<'_>,
    dataset_id: &DatasetId,
) -> Result<u64, ResearchUseCatalogError> {
    let current: i64 = transaction.query_row(
        "SELECT COALESCE(MAX(manifest_version), 0)
         FROM analytical_generations WHERE dataset_id=?1",
        [dataset_id.as_str()],
        |row| row.get(0),
    )?;
    let current = u64::try_from(current).map_err(|_| ResearchUseCatalogError::CorruptCatalog)?;
    current
        .checked_add(1)
        .ok_or(ResearchUseCatalogError::LimitExceeded)
}

fn reject_schema_change(
    transaction: &Transaction<'_>,
    input: &DerivedPublicationInput,
) -> Result<(), ResearchUseCatalogError> {
    let latest = transaction
        .query_row(
            "SELECT schema_name, schema_version, schema_fingerprint
             FROM analytical_generations WHERE dataset_id=?1
             ORDER BY manifest_version DESC LIMIT 1",
            [input.plan().dataset_id().as_str()],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, Vec<u8>>(2)?,
                ))
            },
        )
        .optional()?;
    if latest.is_some_and(|latest| {
        latest.0 != input.schema().name()
            || latest.1 != i64::from(input.schema().version().get())
            || latest.2.as_slice() != input.schema().fingerprint()
    }) {
        Err(ResearchUseCatalogError::InvalidPublication)
    } else {
        Ok(())
    }
}

fn complete_output_runs(
    transaction: &Transaction<'_>,
    now: Timestamp,
    input: &DerivedPublicationInput,
) -> Result<(), ResearchUseCatalogError> {
    for object in input.objects() {
        let changed = transaction.execute(
            "UPDATE ingest_runs SET state='succeeded', completed_at_ns=?1
             WHERE run_id=?2 AND state='reserved' AND completed_at_ns IS NULL",
            params![now.unix_nanos(), object.run_id().to_string()],
        )?;
        if changed == 0 {
            let valid: bool = transaction.query_row(
                "SELECT EXISTS(
                    SELECT 1 FROM ingest_runs
                    WHERE run_id=?1 AND state='succeeded'
                      AND completed_at_ns IS NOT NULL AND completed_at_ns<=?2
                 )",
                params![object.run_id().to_string(), now.unix_nanos()],
                |row| row.get(0),
            )?;
            if !valid {
                return Err(ResearchUseCatalogError::InvalidPublication);
            }
        }
    }
    Ok(())
}

fn parse_operation(value: &str) -> Result<DerivedRetentionOperation, ResearchUseCatalogError> {
    match value {
        "persist" => Ok(DerivedRetentionOperation::Persist),
        "cache" => Ok(DerivedRetentionOperation::Cache),
        _ => Err(ResearchUseCatalogError::InvalidPublication),
    }
}

fn positive_u64(value: i64) -> Result<u64, ResearchUseCatalogError> {
    u64::try_from(value)
        .ok()
        .filter(|value| *value > 0)
        .ok_or(ResearchUseCatalogError::CorruptCatalog)
}

fn map_manifest_lineage_error(error: ManifestCatalogError) -> ResearchUseCatalogError {
    match error {
        ManifestCatalogError::Sqlite(error) => ResearchUseCatalogError::Sqlite(error),
        ManifestCatalogError::CaptureInputLimitExceeded { .. }
        | ManifestCatalogError::MarketBarHistoryInputLimitExceeded { .. }
        | ManifestCatalogError::CountOverflow => ResearchUseCatalogError::LimitExceeded,
        _ => ResearchUseCatalogError::CorruptCatalog,
    }
}

fn encode_hex(bytes: [u8; 32]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(64);
    for byte in bytes {
        encoded.push(char::from(HEX[usize::from(byte >> 4)]));
        encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    encoded
}
