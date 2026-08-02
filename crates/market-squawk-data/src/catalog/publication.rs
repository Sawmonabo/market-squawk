//! Controlled artifact, manifest, and immutable audit metadata.

use market_squawk_domain::{SchemaVersion, SourceIdentifier, Timestamp};
use rusqlite::{OptionalExtension as _, params};
use uuid::Uuid;

use super::storage::{
    ResultBudget, append_audit, digest_columns, parse_digest, require_reserved_run,
    trusted_catalog_now,
};
use super::types::*;

/// One atomically published artifact and its exact durable dataset manifest.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PublishedIngest {
    artifact: ArtifactRecord,
    manifest: DatasetManifestRecord,
}

impl PublishedIngest {
    pub(super) const fn new(artifact: ArtifactRecord, manifest: DatasetManifestRecord) -> Self {
        Self { artifact, manifest }
    }

    /// Returns the durable controlled artifact.
    pub const fn artifact(&self) -> &ArtifactRecord {
        &self.artifact
    }

    /// Returns the durable dataset manifest.
    pub const fn manifest(&self) -> &DatasetManifestRecord {
        &self.manifest
    }

    fn semantically_matches(
        &self,
        artifact: &ArtifactRecord,
        manifest: &DatasetManifestRecord,
    ) -> bool {
        self.artifact.relative_reference == artifact.relative_reference
            && self.artifact.content_digest == artifact.content_digest
            && self.artifact.size_bytes == artifact.size_bytes
            && self.manifest.dataset_name == manifest.dataset_name
            && self.manifest.schema_version == manifest.schema_version
            && self.manifest.content_digest == manifest.content_digest
    }
}

impl Catalog {
    /// Atomically binds controlled artifact and manifest metadata to a reservation.
    pub fn publish_artifact_manifest(
        &self,
        reservation: &IngestReservation,
        artifact: &ArtifactRecord,
        manifest: &DatasetManifestRecord,
    ) -> Result<PublishedIngest, CatalogError> {
        if reservation.catalog_id != self.catalog_id {
            return Err(CatalogError::InvalidReservationCapability);
        }
        if artifact.artifact_id != manifest.artifact_id {
            return Err(CatalogError::ManifestArtifactMismatch);
        }
        if artifact.created_at < reservation.requested_at
            || manifest.created_at < artifact.created_at
        {
            return Err(CatalogError::PublicationTimeConflict);
        }
        let transaction = self.connection.unchecked_transaction()?;
        let catalog_now = trusted_catalog_now(&transaction)?;
        require_reserved_run(&transaction, reservation.run_id)?;
        let mut budget = ResultBudget::new(self.result_bytes);
        if let Some(existing) = publication_for_run(&transaction, reservation.run_id, &mut budget)?
        {
            return if existing.semantically_matches(artifact, manifest) {
                Ok(existing)
            } else {
                Err(CatalogError::EvidenceConflict)
            };
        }
        let (artifact_algorithm, artifact_digest) = digest_columns(artifact.content_digest);
        let artifact_size =
            i64::try_from(artifact.size_bytes).map_err(|_| CatalogError::InvalidRecord)?;
        let existing_artifact: Option<(String, String, i64, Vec<u8>, i64, i64)> = transaction
            .query_row(
                "SELECT run_id, relative_reference, content_algorithm, content_digest,
                        size_bytes, created_at_ns
                 FROM artifacts WHERE artifact_id=?1",
                [artifact.artifact_id.to_string()],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                        row.get(5)?,
                    ))
                },
            )
            .optional()?;
        let (manifest_algorithm, manifest_digest) = digest_columns(manifest.content_digest);
        let existing_manifest: Option<(String, i64, String, i64, Vec<u8>, i64)> = transaction
            .query_row(
                "SELECT dataset_name, schema_version, artifact_id, content_algorithm,
                        content_digest, created_at_ns
                 FROM dataset_manifests WHERE manifest_id=?1",
                [manifest.manifest_id.to_string()],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                        row.get(5)?,
                    ))
                },
            )
            .optional()?;
        if let (Some(artifact_row), Some(manifest_row)) = (&existing_artifact, &existing_manifest) {
            let exact_artifact = artifact_row.0 == reservation.run_id.to_string()
                && artifact_row.1 == artifact.relative_reference
                && artifact_row.2 == artifact_algorithm
                && artifact_row.3.as_slice() == artifact_digest
                && artifact_row.4 == artifact_size
                && artifact_row.5 == artifact.created_at.unix_nanos();
            let exact_manifest = manifest_row.0 == manifest.dataset_name.as_str()
                && manifest_row.1 == i64::from(manifest.schema_version.get())
                && manifest_row.2 == manifest.artifact_id.to_string()
                && manifest_row.3 == manifest_algorithm
                && manifest_row.4.as_slice() == manifest_digest
                && manifest_row.5 == manifest.created_at.unix_nanos();
            return if exact_artifact && exact_manifest {
                Ok(PublishedIngest::new(artifact.clone(), manifest.clone()))
            } else {
                Err(CatalogError::EvidenceConflict)
            };
        }
        if existing_artifact.is_some()
            || existing_manifest.is_some()
            || transaction.query_row(
                "SELECT EXISTS(SELECT 1 FROM artifacts WHERE relative_reference=?1)",
                [&artifact.relative_reference],
                |row| row.get::<_, bool>(0),
            )?
            || transaction.query_row(
                "SELECT EXISTS(
                    SELECT 1 FROM dataset_manifests
                    WHERE dataset_name=?1 AND content_algorithm=?2 AND content_digest=?3
                 )",
                params![
                    manifest.dataset_name.as_str(),
                    manifest_algorithm,
                    manifest_digest
                ],
                |row| row.get::<_, bool>(0),
            )?
        {
            return Err(CatalogError::EvidenceConflict);
        }
        transaction.execute(
            "INSERT INTO artifacts
             (artifact_id, run_id, relative_reference, content_algorithm, content_digest,
              size_bytes, created_at_ns)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![
                artifact.artifact_id.to_string(),
                reservation.run_id.to_string(),
                artifact.relative_reference,
                artifact_algorithm,
                artifact_digest,
                artifact_size,
                artifact.created_at.unix_nanos()
            ],
        )?;
        transaction.execute(
            "INSERT INTO dataset_manifests
             (manifest_id, dataset_name, schema_version, artifact_id, content_algorithm,
              content_digest, created_at_ns)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![
                manifest.manifest_id.to_string(),
                manifest.dataset_name.as_str(),
                i64::from(manifest.schema_version.get()),
                manifest.artifact_id.to_string(),
                manifest_algorithm,
                manifest_digest,
                manifest.created_at.unix_nanos()
            ],
        )?;
        append_audit(
            &transaction,
            "dataset.manifest-published",
            &manifest.manifest_id.to_string(),
            manifest_digest,
            catalog_now,
        )?;
        transaction.commit()?;
        Ok(PublishedIngest::new(artifact.clone(), manifest.clone()))
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

pub(super) fn publication_for_run(
    transaction: &rusqlite::Transaction<'_>,
    run_id: Uuid,
    budget: &mut ResultBudget,
) -> Result<Option<PublishedIngest>, CatalogError> {
    let artifact = transaction
        .query_row(
            "SELECT artifact_id, relative_reference, content_algorithm, content_digest,
                    size_bytes, created_at_ns
             FROM artifacts WHERE run_id=?1",
            [run_id.to_string()],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, Vec<u8>>(3)?,
                    row.get::<_, i64>(4)?,
                    row.get::<_, i64>(5)?,
                ))
            },
        )
        .optional()?;
    let Some((artifact_id, reference, algorithm, digest, size, created_at)) = artifact else {
        return Ok(None);
    };
    budget.charge([artifact_id.len(), reference.len(), digest.len()])?;
    let artifact_id = Uuid::parse_str(&artifact_id).map_err(|_| CatalogError::CorruptCatalog)?;
    let artifact = ArtifactRecord::try_from_stored(
        artifact_id,
        reference,
        parse_digest(algorithm, &digest)?,
        u64::try_from(size).map_err(|_| CatalogError::CorruptCatalog)?,
        Timestamp::from_unix_nanos(created_at),
    )?;
    let manifest = transaction
        .query_row(
            "SELECT manifest_id, dataset_name, schema_version, content_algorithm,
                    content_digest, created_at_ns
             FROM dataset_manifests WHERE artifact_id=?1",
            [artifact_id.to_string()],
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
        .ok_or(CatalogError::CorruptCatalog)?;
    budget.charge([manifest.0.len(), manifest.1.len(), manifest.4.len()])?;
    let schema_version = u16::try_from(manifest.2).map_err(|_| CatalogError::CorruptCatalog)?;
    let manifest = DatasetManifestRecord::try_from_stored(
        Uuid::parse_str(&manifest.0).map_err(|_| CatalogError::CorruptCatalog)?,
        SourceIdentifier::try_from(manifest.1).map_err(|_| CatalogError::CorruptCatalog)?,
        SchemaVersion::new(schema_version).map_err(|_| CatalogError::CorruptCatalog)?,
        artifact_id,
        parse_digest(manifest.3, &manifest.4)?,
        Timestamp::from_unix_nanos(manifest.5),
    )?;
    Ok(Some(PublishedIngest::new(artifact, manifest)))
}
