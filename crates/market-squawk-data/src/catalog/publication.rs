//! Controlled artifact, manifest, and immutable audit metadata.

use market_squawk_domain::{SchemaVersion, SourceIdentifier, Timestamp};
use rusqlite::{OptionalExtension as _, Transaction, params};
use uuid::Uuid;

use super::provider_capture::retain_ordered_prepared_provider_capture_bindings;
use super::provider_macro_plan::retain_completed_provider_macro_plan_for_run;
use super::storage::{
    ResultBudget, append_audit, digest_columns, parse_digest, require_reserved_run,
    trusted_catalog_now,
};
use super::types::*;
use super::{
    PreparedProviderCaptureBinding, PreparedProviderOptionMarketBinding,
    PreparedProviderPublicationBinding, ProviderArtifactInputCoordinate,
    retain_prepared_provider_capture_binding, retain_prepared_provider_option_market_binding,
    retain_prepared_provider_publication_binding,
    retain_sealed_provider_logical_publication_binding,
};
use market_squawk_sources::SealedProviderLogicalPublicationBinding;

/// Closed raw-input state admitted by the sole artifact/manifest transaction.
#[derive(Clone, Copy)]
pub(crate) enum PublicationSourceEvidence<'a> {
    /// The local or derived publication introduces no provider raw input.
    NoNewRawInput,
    /// The provider publication consumes one exact prepared live binding.
    Provider(
        &'a PreparedProviderCaptureBinding,
        ProviderArtifactInputCoordinate,
    ),
    /// One complete macro plan consumes every prepared capture in exact chunk order.
    ProviderMacroPlan(
        &'a [PreparedProviderCaptureBinding],
        &'a [ProviderArtifactInputCoordinate],
    ),
    /// One completed staged macro plan links its already retained ordered evidence atomically.
    StagedProviderMacroPlan(
        &'a super::ProviderMacroPlanPublicationCommit,
        &'a [ProviderArtifactInputCoordinate],
    ),
    /// The provider publication consumes one exact typed event/composite binding.
    ProviderEvent(
        &'a PreparedProviderPublicationBinding,
        ProviderArtifactInputCoordinate,
    ),
    /// The provider publication consumes one exact sealed option-market binding.
    ProviderOptionMarket(
        &'a PreparedProviderOptionMarketBinding,
        ProviderArtifactInputCoordinate,
    ),
    /// The provider publication consumes one exact streamed logical-publication binding.
    ProviderLogical(
        &'a SealedProviderLogicalPublicationBinding,
        ProviderArtifactInputCoordinate,
    ),
}

/// One atomically published ordered artifact group and its exact durable dataset manifest.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PublishedIngest {
    artifacts: Box<[ArtifactRecord]>,
    manifest: DatasetManifestRecord,
}

impl PublishedIngest {
    pub(super) fn new(artifacts: Vec<ArtifactRecord>, manifest: DatasetManifestRecord) -> Self {
        Self {
            artifacts: artifacts.into_boxed_slice(),
            manifest,
        }
    }

    /// Returns the durable controlled artifacts in run-local publication order.
    pub const fn artifacts(&self) -> &[ArtifactRecord] {
        &self.artifacts
    }

    /// Returns the durable dataset manifest.
    pub const fn manifest(&self) -> &DatasetManifestRecord {
        &self.manifest
    }

    fn semantically_matches(
        &self,
        artifacts: &[ArtifactRecord],
        manifest: &DatasetManifestRecord,
    ) -> bool {
        self.artifacts.as_ref() == artifacts && &self.manifest == manifest
    }
}

impl Catalog {
    /// Atomically binds controlled artifact and manifest metadata to a reservation.
    pub fn publish_artifact_manifest(
        &self,
        reservation: &IngestReservation,
        artifacts: &[ArtifactRecord],
        manifest: &DatasetManifestRecord,
    ) -> Result<PublishedIngest, CatalogError> {
        self.publish_artifact_manifest_with_source_evidence(
            reservation,
            artifacts,
            manifest,
            PublicationSourceEvidence::NoNewRawInput,
        )
    }

    pub(crate) fn publish_artifact_manifest_with_source_evidence(
        &self,
        reservation: &IngestReservation,
        artifacts: &[ArtifactRecord],
        manifest: &DatasetManifestRecord,
        source_evidence: PublicationSourceEvidence<'_>,
    ) -> Result<PublishedIngest, CatalogError> {
        if reservation.catalog_id != self.catalog_id {
            return Err(CatalogError::InvalidReservationCapability);
        }
        let Some(anchor) = artifacts.last() else {
            return Err(CatalogError::ManifestArtifactMismatch);
        };
        if artifacts.len() > 1024 || anchor.artifact_id != manifest.artifact_id {
            return Err(CatalogError::ManifestArtifactMismatch);
        }
        if artifacts
            .iter()
            .any(|artifact| artifact.created_at < reservation.requested_at)
            || artifacts
                .iter()
                .any(|artifact| manifest.created_at < artifact.created_at)
        {
            return Err(CatalogError::PublicationTimeConflict);
        }
        let transaction = self.connection.unchecked_transaction()?;
        let catalog_now = trusted_catalog_now(&transaction)?;
        let publication = publish_artifact_manifest_in_transaction(
            &transaction,
            self.result_bytes,
            reservation,
            artifacts,
            manifest,
            source_evidence,
            catalog_now,
        )?;
        transaction.commit()?;
        Ok(publication)
    }

    pub(crate) const fn result_limits(&self) -> CatalogResultLimits {
        self.result_bytes
    }

    pub(crate) fn provider_publication_input_matches_for_run(
        &self,
        run_id: Uuid,
        publication_digest: market_squawk_domain::EvidenceDigest,
        publication_kind: &str,
        source_id: &str,
        coordinate: ProviderArtifactInputCoordinate,
    ) -> Result<bool, CatalogError> {
        retained_publication_input_matches(
            &self.connection,
            run_id,
            publication_digest,
            publication_kind,
            source_id,
            coordinate,
        )
    }

    /// Loads immutable manifest metadata by opaque identity.
    pub fn manifest(
        &self,
        manifest_id: Uuid,
    ) -> Result<Option<DatasetManifestRecord>, CatalogError> {
        let mut budget = ResultBudget::new(self.result_bytes);
        let row = self
            .connection
            .query_row(
                "SELECT dataset_name, schema_version, artifact_id, content_algorithm,
                        content_digest, created_at_ns
                 FROM dataset_manifests WHERE manifest_id=?1",
                [manifest_id.to_string()],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, i64>(3)?,
                        row.get::<_, Vec<u8>>(4)?,
                        row.get::<_, i64>(5)?,
                    ))
                },
            )
            .optional()?;
        row.map(|(dataset, schema, artifact, algorithm, digest, created)| {
            budget.charge([dataset.len(), artifact.len(), digest.len()])?;
            DatasetManifestRecord::try_from_stored(
                manifest_id,
                SourceIdentifier::try_from(dataset).map_err(|_| CatalogError::CorruptCatalog)?,
                SchemaVersion::new(
                    u16::try_from(schema).map_err(|_| CatalogError::CorruptCatalog)?,
                )
                .map_err(|_| CatalogError::CorruptCatalog)?,
                Uuid::parse_str(&artifact).map_err(|_| CatalogError::CorruptCatalog)?,
                parse_digest(algorithm, &digest)?,
                Timestamp::from_unix_nanos(created),
            )
        })
        .transpose()
    }

    /// Loads immutable controlled-artifact metadata by opaque identity.
    pub fn artifact(&self, artifact_id: Uuid) -> Result<Option<ArtifactRecord>, CatalogError> {
        let mut budget = ResultBudget::new(self.result_bytes);
        let row = self
            .connection
            .query_row(
                "SELECT relative_reference, content_algorithm, content_digest,
                        size_bytes, created_at_ns
                 FROM artifacts WHERE artifact_id=?1",
                [artifact_id.to_string()],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, Vec<u8>>(2)?,
                        row.get::<_, i64>(3)?,
                        row.get::<_, i64>(4)?,
                    ))
                },
            )
            .optional()?;
        row.map(|(reference, algorithm, digest, size, created)| {
            budget.charge([reference.len(), digest.len()])?;
            ArtifactRecord::try_from_stored(
                artifact_id,
                reference,
                parse_digest(algorithm, &digest)?,
                u64::try_from(size).map_err(|_| CatalogError::CorruptCatalog)?,
                Timestamp::from_unix_nanos(created),
            )
        })
        .transpose()
    }

    /// Returns newest-first immutable audit records within the requested bound.
    pub fn audit_events(&self, limit: CatalogLimit) -> Result<Vec<AuditEvent>, CatalogError> {
        self.enforce_limit(limit)?;
        let mut budget = ResultBudget::new(self.result_bytes);
        let mut statement = self.connection.prepare(
            "SELECT sequence, event_type, subject_id, details_digest, occurred_at_ns
             FROM audit_events ORDER BY sequence DESC LIMIT ?1",
        )?;
        let row_limit = i64::try_from(limit.get()).map_err(|_| CatalogError::InvalidLimit)?;
        let rows = statement.query_map([row_limit], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, Vec<u8>>(3)?,
                row.get::<_, i64>(4)?,
            ))
        })?;
        let mut events = Vec::new();
        events
            .try_reserve_exact(budget.bounded_row_capacity(limit.get()))
            .map_err(|_| CatalogError::Allocation)?;
        for row in rows {
            let (sequence, event_type, subject_id, digest, occurred_at) = row?;
            budget.charge([event_type.len(), subject_id.len(), digest.len()])?;
            events.push(AuditEvent {
                sequence: u64::try_from(sequence).map_err(|_| CatalogError::CorruptCatalog)?,
                event_type,
                subject_id,
                details_digest: parse_digest(1, &digest)?,
                occurred_at: Timestamp::from_unix_nanos(occurred_at),
            });
        }
        Ok(events)
    }
}

pub(crate) fn publish_artifact_manifest_in_transaction(
    transaction: &Transaction<'_>,
    result_limits: CatalogResultLimits,
    reservation: &IngestReservation,
    artifacts: &[ArtifactRecord],
    manifest: &DatasetManifestRecord,
    source_evidence: PublicationSourceEvidence<'_>,
    catalog_now: Timestamp,
) -> Result<PublishedIngest, CatalogError> {
    require_reserved_run(transaction, reservation.run_id)?;
    let Some(anchor) = artifacts.last() else {
        return Err(CatalogError::ManifestArtifactMismatch);
    };
    if artifacts.len() > 1024
        || anchor.artifact_id != manifest.artifact_id
        || artifacts
            .iter()
            .any(|artifact| manifest.created_at < artifact.created_at)
        || manifest.created_at > catalog_now
        || artifacts.iter().enumerate().any(|(ordinal, artifact)| {
            artifact.created_at < reservation.requested_at
                || artifact.created_at > catalog_now
                || artifacts[..ordinal].iter().any(|prior| {
                    prior.artifact_id == artifact.artifact_id
                        || prior.relative_reference == artifact.relative_reference
                })
        })
    {
        return Err(CatalogError::ManifestArtifactMismatch);
    }
    let mut budget = ResultBudget::new(result_limits);
    if let Some(existing) = publication_for_run(transaction, reservation.run_id, &mut budget)? {
        return if existing.semantically_matches(artifacts, manifest)
            && publication_source_evidence_matches(
                transaction,
                reservation.run_id,
                source_evidence,
            )? {
            Ok(existing)
        } else {
            Err(CatalogError::EvidenceConflict)
        };
    }
    for artifact in artifacts {
        let collision: bool = transaction.query_row(
            "SELECT EXISTS(
                 SELECT 1 FROM artifacts
                 WHERE artifact_id=?1 OR relative_reference=?2
             )",
            params![
                artifact.artifact_id.to_string(),
                artifact.relative_reference
            ],
            |row| row.get(0),
        )?;
        if collision {
            return Err(CatalogError::EvidenceConflict);
        }
    }
    let (manifest_algorithm, manifest_digest) = digest_columns(manifest.content_digest);
    let manifest_collision: bool = transaction.query_row(
        "SELECT EXISTS(
             SELECT 1 FROM dataset_manifests
             WHERE manifest_id=?1
                OR (dataset_name=?2 AND content_algorithm=?3 AND content_digest=?4)
         )",
        params![
            manifest.manifest_id.to_string(),
            manifest.dataset_name.as_str(),
            manifest_algorithm,
            manifest_digest
        ],
        |row| row.get(0),
    )?;
    if manifest_collision {
        return Err(CatalogError::EvidenceConflict);
    }
    for (ordinal, artifact) in artifacts.iter().enumerate() {
        let (algorithm, digest) = digest_columns(artifact.content_digest);
        let inserted = transaction.execute(
            "INSERT INTO artifacts
             (artifact_id, run_id, publication_ordinal, relative_reference,
              content_algorithm, content_digest, size_bytes, created_at_ns)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            params![
                artifact.artifact_id.to_string(),
                reservation.run_id.to_string(),
                i64::try_from(ordinal).map_err(|_| CatalogError::InvalidRecord)?,
                artifact.relative_reference,
                algorithm,
                digest,
                i64::try_from(artifact.size_bytes).map_err(|_| CatalogError::InvalidRecord)?,
                artifact.created_at.unix_nanos(),
            ],
        )?;
        if inserted != 1 {
            return Err(CatalogError::EvidenceConflict);
        }
    }
    match source_evidence {
        PublicationSourceEvidence::NoNewRawInput => {
            if transaction.query_row(
                "SELECT EXISTS(
                    SELECT 1 FROM ingest_run_provider_capture_bindings WHERE run_id=?1
                    UNION ALL
                    SELECT 1 FROM ingest_run_provider_publication_bindings WHERE run_id=?1
                 )",
                [reservation.run_id.to_string()],
                |row| row.get::<_, bool>(0),
            )? {
                return Err(CatalogError::ProviderCaptureConflict);
            }
        }
        PublicationSourceEvidence::Provider(binding, coordinate) => {
            retain_prepared_provider_capture_binding(
                transaction,
                reservation.run_id,
                binding,
                coordinate,
                catalog_now,
            )?;
        }
        PublicationSourceEvidence::ProviderMacroPlan(bindings, coordinates) => {
            retain_ordered_prepared_provider_capture_bindings(
                transaction,
                reservation.run_id,
                bindings,
                coordinates,
                catalog_now,
            )?;
        }
        PublicationSourceEvidence::StagedProviderMacroPlan(commit, coordinates) => {
            retain_completed_provider_macro_plan_for_run(
                transaction,
                reservation.run_id,
                commit,
                coordinates,
                catalog_now,
            )?;
        }
        PublicationSourceEvidence::ProviderEvent(binding, coordinate) => {
            retain_prepared_provider_publication_binding(
                transaction,
                reservation.run_id,
                binding,
                coordinate,
                catalog_now,
            )?;
        }
        PublicationSourceEvidence::ProviderOptionMarket(binding, coordinate) => {
            retain_prepared_provider_option_market_binding(
                transaction,
                reservation.run_id,
                binding,
                coordinate,
                catalog_now,
            )?;
        }
        PublicationSourceEvidence::ProviderLogical(binding, coordinate) => {
            retain_sealed_provider_logical_publication_binding(
                transaction,
                reservation.run_id,
                binding,
                coordinate,
                catalog_now,
            )?;
        }
    }
    let inserted = transaction.execute(
        "INSERT INTO dataset_manifests
         (manifest_id, run_id, dataset_name, schema_version, artifact_id,
          content_algorithm, content_digest, created_at_ns)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
        params![
            manifest.manifest_id.to_string(),
            reservation.run_id.to_string(),
            manifest.dataset_name.as_str(),
            i64::from(manifest.schema_version.get()),
            manifest.artifact_id.to_string(),
            manifest_algorithm,
            manifest_digest,
            manifest.created_at.unix_nanos()
        ],
    )?;
    if inserted != 1 {
        return Err(CatalogError::EvidenceConflict);
    }
    append_audit(
        transaction,
        "dataset.manifest-published",
        &manifest.manifest_id.to_string(),
        manifest_digest,
        catalog_now,
    )?;
    Ok(PublishedIngest::new(artifacts.to_vec(), manifest.clone()))
}

fn publication_source_evidence_matches(
    transaction: &Transaction<'_>,
    run_id: Uuid,
    source_evidence: PublicationSourceEvidence<'_>,
) -> Result<bool, CatalogError> {
    let capture_count: i64 = transaction.query_row(
        "SELECT COUNT(*) FROM ingest_run_provider_capture_bindings WHERE run_id=?1",
        [run_id.to_string()],
        |row| row.get(0),
    )?;
    let publication_count: i64 = transaction.query_row(
        "SELECT COUNT(*) FROM ingest_run_provider_publication_bindings WHERE run_id=?1",
        [run_id.to_string()],
        |row| row.get(0),
    )?;
    match source_evidence {
        PublicationSourceEvidence::NoNewRawInput => {
            Ok(capture_count == 0 && publication_count == 0)
        }
        PublicationSourceEvidence::Provider(binding, coordinate) => Ok(publication_count == 0
            && capture_count == 1
            && retained_capture_input_matches(
                transaction,
                run_id,
                0,
                binding.binding_digest(),
                binding.source_id().as_str(),
                binding.record_count(),
                coordinate,
            )?),
        PublicationSourceEvidence::ProviderMacroPlan(bindings, coordinates) => {
            if publication_count != 0
                || usize::try_from(capture_count).ok() != Some(bindings.len())
                || coordinates.len() != bindings.len()
                || !super::provider_capture::provider_artifact_input_coordinates_are_ordered(
                    coordinates,
                )
            {
                return Ok(false);
            }
            for (ordinal, (binding, coordinate)) in bindings.iter().zip(coordinates).enumerate() {
                if !retained_capture_input_matches(
                    transaction,
                    run_id,
                    ordinal,
                    binding.binding_digest(),
                    binding.source_id().as_str(),
                    binding.record_count(),
                    *coordinate,
                )? {
                    return Ok(false);
                }
            }
            Ok(true)
        }
        PublicationSourceEvidence::StagedProviderMacroPlan(commit, coordinates) => {
            Ok(publication_count == 0
                && usize::try_from(capture_count).ok() == Some(coordinates.len())
                && super::provider_macro_plan::completed_provider_macro_plan_inputs_match_run(
                    transaction,
                    run_id,
                    commit,
                    coordinates,
                )?)
        }
        PublicationSourceEvidence::ProviderEvent(binding, coordinate) => Ok(capture_count == 0
            && publication_count == 1
            && retained_publication_input_matches(
                transaction,
                run_id,
                binding.publication_digest(),
                binding.publication_kind_name(),
                binding.source_id(),
                coordinate,
            )?),
        PublicationSourceEvidence::ProviderOptionMarket(binding, coordinate) => Ok(capture_count
            == 0
            && publication_count == 1
            && retained_publication_input_matches(
                transaction,
                run_id,
                binding.publication_digest(),
                binding.publication_kind_name(),
                binding.source_id().as_str(),
                coordinate,
            )?),
        PublicationSourceEvidence::ProviderLogical(binding, coordinate) => Ok(capture_count == 0
            && publication_count == 1
            && retained_publication_input_matches(
                transaction,
                run_id,
                binding.binding_digest(),
                "provider_logical",
                binding.terminal().source_id().as_str(),
                coordinate,
            )?),
    }
}

fn retained_capture_input_matches(
    transaction: &Transaction<'_>,
    run_id: Uuid,
    input_ordinal: usize,
    binding_digest: market_squawk_domain::EvidenceDigest,
    source_id: &str,
    record_count: usize,
    coordinate: ProviderArtifactInputCoordinate,
) -> Result<bool, CatalogError> {
    transaction
        .query_row(
            "SELECT input.output_artifact_ordinal, input.object_input_ordinal,
                    input.binding_digest, input.source_id, binding.canonical_record_count
             FROM ingest_run_provider_capture_bindings AS input
             JOIN provider_capture_bindings AS binding USING (binding_digest)
             WHERE input.run_id=?1 AND input.input_ordinal=?2",
            params![
                run_id.to_string(),
                i64::try_from(input_ordinal).map_err(|_| CatalogError::InvalidRecord)?,
            ],
            |row| {
                Ok(usize::try_from(row.get::<_, i64>(0)?).ok()
                    == Some(coordinate.output_artifact_ordinal())
                    && usize::try_from(row.get::<_, i64>(1)?).ok()
                        == Some(coordinate.object_input_ordinal())
                    && row.get::<_, Vec<u8>>(2)? == binding_digest.bytes()
                    && row.get::<_, String>(3)? == source_id
                    && usize::try_from(row.get::<_, i64>(4)?).ok() == Some(record_count))
            },
        )
        .optional()
        .map(|value| value == Some(true))
        .map_err(Into::into)
}

fn retained_publication_input_matches(
    transaction: &rusqlite::Connection,
    run_id: Uuid,
    publication_digest: market_squawk_domain::EvidenceDigest,
    publication_kind: &str,
    source_id: &str,
    coordinate: ProviderArtifactInputCoordinate,
) -> Result<bool, CatalogError> {
    transaction
        .query_row(
            "SELECT input_ordinal, output_artifact_ordinal, object_input_ordinal,
                    publication_digest, publication_kind, source_id
             FROM ingest_run_provider_publication_bindings WHERE run_id=?1",
            [run_id.to_string()],
            |row| {
                Ok(row.get::<_, i64>(0)? == 0
                    && usize::try_from(row.get::<_, i64>(1)?).ok()
                        == Some(coordinate.output_artifact_ordinal())
                    && usize::try_from(row.get::<_, i64>(2)?).ok()
                        == Some(coordinate.object_input_ordinal())
                    && row.get::<_, Vec<u8>>(3)? == publication_digest.bytes()
                    && row.get::<_, String>(4)? == publication_kind
                    && row.get::<_, String>(5)? == source_id)
            },
        )
        .optional()
        .map(|value| value == Some(true))
        .map_err(Into::into)
}

pub(super) fn publication_for_run(
    transaction: &rusqlite::Transaction<'_>,
    run_id: Uuid,
    budget: &mut ResultBudget,
) -> Result<Option<PublishedIngest>, CatalogError> {
    let mut statement = transaction.prepare(
        "SELECT publication_ordinal, artifact_id, relative_reference, content_algorithm,
                content_digest, size_bytes, created_at_ns
         FROM artifacts WHERE run_id=?1
         ORDER BY publication_ordinal LIMIT 1025",
    )?;
    let rows = statement.query_map([run_id.to_string()], |row| {
        Ok((
            row.get::<_, i64>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, String>(2)?,
            row.get::<_, i64>(3)?,
            row.get::<_, Vec<u8>>(4)?,
            row.get::<_, i64>(5)?,
            row.get::<_, i64>(6)?,
        ))
    })?;
    let mut artifacts = Vec::new();
    artifacts
        .try_reserve_exact(1024)
        .map_err(|_| CatalogError::Allocation)?;
    for row in rows {
        if artifacts.len() == 1024 {
            return Err(CatalogError::CorruptCatalog);
        }
        let (ordinal, artifact_id, reference, algorithm, digest, size, created_at) = row?;
        if ordinal != i64::try_from(artifacts.len()).map_err(|_| CatalogError::CorruptCatalog)? {
            return Err(CatalogError::CorruptCatalog);
        }
        budget.charge([artifact_id.len(), reference.len(), digest.len()])?;
        artifacts.push(ArtifactRecord::try_from_stored(
            Uuid::parse_str(&artifact_id).map_err(|_| CatalogError::CorruptCatalog)?,
            reference,
            parse_digest(algorithm, &digest)?,
            u64::try_from(size).map_err(|_| CatalogError::CorruptCatalog)?,
            Timestamp::from_unix_nanos(created_at),
        )?);
    }
    if artifacts.is_empty() {
        let orphan_manifest: bool = transaction.query_row(
            "SELECT EXISTS(SELECT 1 FROM dataset_manifests WHERE run_id=?1)",
            [run_id.to_string()],
            |row| row.get(0),
        )?;
        if orphan_manifest {
            return Err(CatalogError::CorruptCatalog);
        }
        return Ok(None);
    }
    let anchor_id = artifacts
        .last()
        .ok_or(CatalogError::CorruptCatalog)?
        .artifact_id;
    let manifest = transaction
        .query_row(
            "SELECT manifest_id, dataset_name, schema_version, artifact_id,
                    content_algorithm, content_digest, created_at_ns
             FROM dataset_manifests WHERE run_id=?1",
            [run_id.to_string()],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, i64>(4)?,
                    row.get::<_, Vec<u8>>(5)?,
                    row.get::<_, i64>(6)?,
                ))
            },
        )
        .optional()?
        .ok_or(CatalogError::CorruptCatalog)?;
    budget.charge([
        manifest.0.len(),
        manifest.1.len(),
        manifest.3.len(),
        manifest.5.len(),
    ])?;
    let schema_version = u16::try_from(manifest.2).map_err(|_| CatalogError::CorruptCatalog)?;
    let retained_anchor = Uuid::parse_str(&manifest.3).map_err(|_| CatalogError::CorruptCatalog)?;
    if retained_anchor != anchor_id {
        return Err(CatalogError::CorruptCatalog);
    }
    let manifest = DatasetManifestRecord::try_from_stored(
        Uuid::parse_str(&manifest.0).map_err(|_| CatalogError::CorruptCatalog)?,
        SourceIdentifier::try_from(manifest.1).map_err(|_| CatalogError::CorruptCatalog)?,
        SchemaVersion::new(schema_version).map_err(|_| CatalogError::CorruptCatalog)?,
        retained_anchor,
        parse_digest(manifest.4, &manifest.5)?,
        Timestamp::from_unix_nanos(manifest.6),
    )?;
    Ok(Some(PublishedIngest::new(artifacts, manifest)))
}
