//! SQLite-backed immutable analytical generation storage.

use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use market_squawk_domain::{DigestAlgorithm, EvidenceDigest, SourceId, Timestamp};
use market_squawk_platform::CatalogLocation;
use rusqlite::{Connection, OpenFlags, OptionalExtension as _, TransactionBehavior, params};
use thiserror::Error;
use uuid::Uuid;

use super::{
    DatasetId, DatasetManifestRef, ManifestObject, ManifestPlan, ManifestPlanError, Sha256Digest,
};
use crate::{ArtifactRecord, DatasetManifestRecord};

/// One manifest-pinned object resolved from immutable catalog metadata.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PinnedManifestObject {
    artifact_id: Uuid,
    relative_reference: String,
    object: ManifestObject,
}

impl PinnedManifestObject {
    /// Returns controlled artifact identity.
    pub const fn artifact_id(&self) -> Uuid {
        self.artifact_id
    }

    /// Returns the portable reference below the artifact root.
    pub fn relative_reference(&self) -> &str {
        &self.relative_reference
    }

    /// Returns immutable object metadata.
    pub const fn object(&self) -> &ManifestObject {
        &self.object
    }
}

/// Complete immutable generation resolved by exact manifest reference.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PinnedDataset {
    manifest: DatasetManifestRef,
    plan: ManifestPlan,
    objects: Vec<PinnedManifestObject>,
}

impl PinnedDataset {
    /// Returns the exact reader pin.
    pub const fn manifest(&self) -> &DatasetManifestRef {
        &self.manifest
    }

    /// Returns the immutable manifest plan.
    pub const fn plan(&self) -> &ManifestPlan {
        &self.plan
    }

    /// Returns objects in stable row order.
    pub fn objects(&self) -> &[PinnedManifestObject] {
        &self.objects
    }
}

/// SQLite-backed immutable analytical generation registry.
pub struct AnalyticalManifestCatalog {
    connection: Mutex<Connection>,
    max_objects_per_generation: usize,
    catalog_path: PathBuf,
}

impl fmt::Debug for AnalyticalManifestCatalog {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AnalyticalManifestCatalog")
            .field("connection", &"[SQLITE CONNECTION]")
            .field(
                "max_objects_per_generation",
                &self.max_objects_per_generation,
            )
            .finish()
    }
}

impl AnalyticalManifestCatalog {
    /// Opens the Task 3 catalog after analytical and query-artifact migrations are applied.
    pub fn open(
        location: &CatalogLocation,
        max_objects_per_generation: usize,
    ) -> Result<Self, ManifestCatalogError> {
        if max_objects_per_generation == 0 || max_objects_per_generation > 1024 {
            return Err(ManifestCatalogError::InvalidConfiguration);
        }
        location.validate_for_open()?;
        let flags = OpenFlags::SQLITE_OPEN_READ_WRITE
            | OpenFlags::SQLITE_OPEN_NO_MUTEX
            | OpenFlags::SQLITE_OPEN_NOFOLLOW;
        let connection = Connection::open_with_flags(location.path(), flags)?;
        location.validate_for_open()?;
        connection.pragma_update(None, "foreign_keys", "ON")?;
        connection.pragma_update(None, "trusted_schema", "OFF")?;
        let migrated: bool = connection.query_row(
            "SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE version=4)",
            [],
            |row| row.get(0),
        )?;
        if !migrated {
            return Err(ManifestCatalogError::MigrationMissing);
        }
        Ok(Self {
            connection: Mutex::new(connection),
            max_objects_per_generation,
            catalog_path: location.path().to_path_buf(),
        })
    }

    pub(crate) fn catalog_path(&self) -> &Path {
        &self.catalog_path
    }

    /// Builds the exact next ingest plan while the process-owned catalog writer is serialized.
    pub(crate) fn preview_append(
        &self,
        dataset_id: DatasetId,
        object: ManifestObject,
    ) -> Result<ManifestPlan, ManifestCatalogError> {
        let connection = self.lock()?;
        let previous = load_latest(&connection, &dataset_id)?;
        ManifestPlan::append(
            dataset_id,
            previous.as_ref().map(PinnedDataset::plan),
            object,
            self.max_objects_per_generation,
        )
        .map_err(Into::into)
    }

    /// Builds a one-object compaction plan preserving the exact prior semantics.
    pub(crate) fn preview_compaction(
        &self,
        previous: &DatasetManifestRef,
        compacted: ManifestObject,
    ) -> Result<ManifestPlan, ManifestCatalogError> {
        let connection = self.lock()?;
        let previous = load_pinned(&connection, previous)?;
        ManifestPlan::compact(previous.plan(), compacted).map_err(Into::into)
    }

    /// Commits one complete generation using `BEGIN IMMEDIATE` after the Task 3 anchor exists.
    pub(crate) fn commit_generation(
        &self,
        plan: &ManifestPlan,
        artifact: &ArtifactRecord,
        anchor: &DatasetManifestRecord,
        kind: GenerationKind,
    ) -> Result<DatasetManifestRef, ManifestCatalogError> {
        if anchor.artifact_id() != artifact.artifact_id()
            || sha256_from_evidence(anchor.content_digest())? != plan.content_hash
            || sha256_from_evidence(artifact.content_digest())?
                != plan
                    .objects
                    .last()
                    .ok_or(ManifestCatalogError::CorruptCatalog)?
                    .content_hash
        {
            return Err(ManifestCatalogError::AnchorMismatch);
        }
        let mut connection = self.lock()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        if let Some(existing) = manifest_for_anchor(&transaction, anchor.manifest_id())? {
            if existing.content_hash == plan.content_hash && existing.dataset_id == plan.dataset_id
            {
                transaction.commit()?;
                return Ok(existing);
            }
            return Err(ManifestCatalogError::GenerationConflict);
        }
        let previous = load_latest(&transaction, &plan.dataset_id)?;
        let current_source = source_for_artifact(&transaction, artifact.artifact_id())?;
        if let Some(previous) = previous.as_ref()
            && generation_source(&transaction, previous.manifest())? != current_source
        {
            return Err(ManifestCatalogError::SourceMismatch);
        }
        let expected = match kind {
            GenerationKind::Ingest => ManifestPlan::append(
                plan.dataset_id.clone(),
                previous.as_ref().map(PinnedDataset::plan),
                plan.objects
                    .last()
                    .cloned()
                    .ok_or(ManifestCatalogError::CorruptCatalog)?,
                self.max_objects_per_generation,
            )?,
            GenerationKind::Compaction => {
                let previous = previous
                    .as_ref()
                    .ok_or(ManifestCatalogError::GenerationConflict)?;
                ManifestPlan::compact(
                    previous.plan(),
                    plan.objects
                        .last()
                        .cloned()
                        .ok_or(ManifestCatalogError::CorruptCatalog)?,
                )?
            }
        };
        if expected != *plan {
            return Err(ManifestCatalogError::GenerationConflict);
        }
        let version = previous_version(&transaction, &plan.dataset_id)?
            .checked_add(1)
            .ok_or(ManifestCatalogError::CountOverflow)?;
        let parent = version.checked_sub(1).filter(|value| *value > 0);
        transaction.execute(
            "INSERT INTO analytical_generations
             (dataset_id, manifest_version, content_hash, lineage_hash, row_count, total_bytes,
              schema_version, anchor_manifest_id, parent_version, generation_kind, created_at_ns)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
            params![
                plan.dataset_id.as_str(),
                to_i64(version)?,
                plan.content_hash.bytes(),
                plan.lineage_digest.bytes(),
                to_i64(plan.row_count)?,
                to_i64(plan.total_bytes)?,
                i64::from(anchor.schema_version().get()),
                anchor.manifest_id().to_string(),
                parent.map(to_i64).transpose()?,
                kind.database_name(),
                anchor.created_at().unix_nanos(),
            ],
        )?;
        let prior_objects = previous
            .as_ref()
            .map(PinnedDataset::objects)
            .unwrap_or_default();
        match kind {
            GenerationKind::Ingest => {
                for (ordinal, prior) in prior_objects.iter().enumerate() {
                    insert_generation_object(
                        &transaction,
                        &plan.dataset_id,
                        version,
                        ordinal,
                        prior.artifact_id,
                        &prior.object,
                    )?;
                }
                insert_generation_object(
                    &transaction,
                    &plan.dataset_id,
                    version,
                    prior_objects.len(),
                    artifact.artifact_id(),
                    plan.objects
                        .last()
                        .ok_or(ManifestCatalogError::CorruptCatalog)?,
                )?;
            }
            GenerationKind::Compaction => insert_generation_object(
                &transaction,
                &plan.dataset_id,
                version,
                0,
                artifact.artifact_id(),
                plan.objects
                    .last()
                    .ok_or(ManifestCatalogError::CorruptCatalog)?,
            )?,
        }
        let manifest =
            DatasetManifestRef::try_new(plan.dataset_id.clone(), version, plan.content_hash)?;
        transaction.commit()?;
        Ok(manifest)
    }

    /// Resolves only the explicitly supplied immutable generation.
    pub fn pinned(
        &self,
        manifest: &DatasetManifestRef,
    ) -> Result<PinnedDataset, ManifestCatalogError> {
        let connection = self.lock()?;
        load_pinned(&connection, manifest)
    }

    /// Returns the current generation only as an explicit pin, never as a directory inference.
    pub fn latest(
        &self,
        dataset_id: &DatasetId,
    ) -> Result<Option<DatasetManifestRef>, ManifestCatalogError> {
        let connection = self.lock()?;
        Ok(load_latest(&connection, dataset_id)?.map(|value| value.manifest))
    }

    /// Resolves the immutable generation anchored by one Task 3 ingest run, when present.
    pub fn for_run(&self, run_id: Uuid) -> Result<Option<PinnedDataset>, ManifestCatalogError> {
        let connection = self.lock()?;
        let reference = connection
            .query_row(
                "SELECT generations.dataset_id, generations.manifest_version,
                        generations.content_hash
                 FROM analytical_generations AS generations
                 JOIN dataset_manifests AS manifests
                   ON manifests.manifest_id=generations.anchor_manifest_id
                 JOIN artifacts ON artifacts.artifact_id=manifests.artifact_id
                 WHERE artifacts.run_id=?1",
                [run_id.to_string()],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, Vec<u8>>(2)?,
                    ))
                },
            )
            .optional()?
            .map(|(dataset, version, content)| {
                DatasetManifestRef::try_new(
                    DatasetId::try_from(dataset.as_str())?,
                    from_i64(version)?,
                    parse_digest(&content)?,
                )
                .map_err(ManifestCatalogError::from)
            })
            .transpose()?;
        reference
            .as_ref()
            .map(|reference| load_pinned(&connection, reference))
            .transpose()
    }

    /// Returns the source-rights namespace that owns one immutable generation.
    pub fn source_id(
        &self,
        manifest: &DatasetManifestRef,
    ) -> Result<SourceId, ManifestCatalogError> {
        let connection = self.lock()?;
        generation_source(&connection, manifest)
    }

    /// Returns generation objects and query results whose exclusive expiry is still in the future.
    pub(crate) fn referenced_hashes(
        &self,
        now: Timestamp,
    ) -> Result<Vec<Sha256Digest>, ManifestCatalogError> {
        let connection = self.lock()?;
        let mut statement = connection.prepare(
            "SELECT content_hash FROM analytical_generation_objects
             UNION
             SELECT results.content_digest
             FROM query_artifact_results AS results
             JOIN query_artifact_reservations AS reservations USING (reservation_id)
             WHERE results.content_algorithm=1
               AND reservations.state='published'
               AND reservations.expires_at_ns>?1
             ORDER BY 1",
        )?;
        let hashes = statement
            .query_map([now.unix_nanos()], |row| row.get::<_, Vec<u8>>(0))?
            .map(|row| parse_digest(&row?))
            .collect::<Result<Vec<_>, ManifestCatalogError>>()?;
        Ok(hashes)
    }

    /// Re-checks durable generation reachability immediately before orphan quarantine.
    pub(crate) fn is_referenced(
        &self,
        content_hash: Sha256Digest,
        now: Timestamp,
    ) -> Result<bool, ManifestCatalogError> {
        let connection = self.lock()?;
        connection
            .query_row(
                "SELECT EXISTS(
                    SELECT 1 FROM analytical_generation_objects WHERE content_hash=?1
                    UNION ALL
                    SELECT 1
                    FROM query_artifact_results AS results
                    JOIN query_artifact_reservations AS reservations USING (reservation_id)
                    WHERE results.content_algorithm=1
                      AND results.content_digest=?1
                      AND reservations.state='published'
                      AND reservations.expires_at_ns>?2
                 )",
                params![content_hash.bytes().as_slice(), now.unix_nanos()],
                |row| row.get(0),
            )
            .map_err(Into::into)
    }

    fn lock(&self) -> Result<std::sync::MutexGuard<'_, Connection>, ManifestCatalogError> {
        self.connection
            .lock()
            .map_err(|_| ManifestCatalogError::LockPoisoned)
    }
}

/// How one immutable generation changes its predecessor's object set.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GenerationKind {
    /// Append one newly ingested object.
    Ingest,
    /// Replace all prior objects with one equivalent compacted object.
    Compaction,
}

impl GenerationKind {
    const fn database_name(self) -> &'static str {
        match self {
            Self::Ingest => "ingest",
            Self::Compaction => "compaction",
        }
    }
}

/// Immutable generation catalog failure.
#[derive(Debug, Error)]
pub enum ManifestCatalogError {
    /// Object ceiling is zero or excessive.
    #[error("analytical manifest configuration is invalid")]
    InvalidConfiguration,
    /// Task 3 did not apply the digest-bound analytical migration.
    #[error("analytical catalog migration is missing")]
    MigrationMissing,
    /// Artifact, Task 3 manifest, and analytical plan identities disagree.
    #[error("analytical manifest anchor mismatch")]
    AnchorMismatch,
    /// The latest generation changed or an idempotency replay differs.
    #[error("analytical manifest generation conflicts with retained state")]
    GenerationConflict,
    /// A dataset cannot combine independently admitted source-rights namespaces implicitly.
    #[error("analytical dataset source identity conflicts with its prior generation")]
    SourceMismatch,
    /// Stored generation metadata does not reconstruct exactly.
    #[error("analytical manifest catalog is corrupt")]
    CorruptCatalog,
    /// Row, byte, version, or ordinal conversion overflowed.
    #[error("analytical manifest count overflow")]
    CountOverflow,
    /// The connection lock was poisoned.
    #[error("analytical manifest catalog lock is unavailable")]
    LockPoisoned,
    /// Pure manifest invariant failed.
    #[error("analytical manifest plan is invalid")]
    Plan(#[from] ManifestPlanError),
    /// Prepared catalog path validation failed.
    #[error("analytical catalog path is invalid")]
    Path(#[from] market_squawk_platform::PathError),
    /// SQLite rejected a transaction or retained invariant.
    #[error("analytical catalog SQLite operation failed")]
    Sqlite(#[from] rusqlite::Error),
}

fn load_latest(
    connection: &Connection,
    dataset_id: &DatasetId,
) -> Result<Option<PinnedDataset>, ManifestCatalogError> {
    let reference = connection
        .query_row(
            "SELECT manifest_version, content_hash FROM analytical_generations
             WHERE dataset_id=?1 ORDER BY manifest_version DESC LIMIT 1",
            [dataset_id.as_str()],
            |row| Ok((row.get::<_, i64>(0)?, row.get::<_, Vec<u8>>(1)?)),
        )
        .optional()?
        .map(|(version, digest)| {
            DatasetManifestRef::try_new(
                dataset_id.clone(),
                from_i64(version)?,
                parse_digest(&digest)?,
            )
            .map_err(ManifestCatalogError::from)
        })
        .transpose()?;
    reference
        .as_ref()
        .map(|reference| load_pinned(connection, reference))
        .transpose()
}

fn load_pinned(
    connection: &Connection,
    reference: &DatasetManifestRef,
) -> Result<PinnedDataset, ManifestCatalogError> {
    let header = connection
        .query_row(
            "SELECT content_hash, lineage_hash, row_count, total_bytes
             FROM analytical_generations WHERE dataset_id=?1 AND manifest_version=?2",
            params![
                reference.dataset_id.as_str(),
                to_i64(reference.manifest_version)?
            ],
            |row| {
                Ok((
                    row.get::<_, Vec<u8>>(0)?,
                    row.get::<_, Vec<u8>>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, i64>(3)?,
                ))
            },
        )
        .optional()?
        .ok_or(ManifestCatalogError::GenerationConflict)?;
    if parse_digest(&header.0)? != reference.content_hash {
        return Err(ManifestCatalogError::GenerationConflict);
    }
    let mut statement = connection.prepare(
        "SELECT objects.artifact_id, artifacts.relative_reference, objects.content_hash,
                objects.row_count, objects.size_bytes, objects.lineage_hash
         FROM analytical_generation_objects AS objects
         JOIN artifacts USING (artifact_id)
         WHERE objects.dataset_id=?1 AND objects.manifest_version=?2
         ORDER BY objects.ordinal",
    )?;
    let rows = statement.query_map(
        params![
            reference.dataset_id.as_str(),
            to_i64(reference.manifest_version)?
        ],
        |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, Vec<u8>>(2)?,
                row.get::<_, i64>(3)?,
                row.get::<_, i64>(4)?,
                row.get::<_, Vec<u8>>(5)?,
            ))
        },
    )?;
    let mut objects = Vec::new();
    for row in rows {
        let (artifact_id, relative_reference, content, rows, bytes, lineage) = row?;
        objects.push(PinnedManifestObject {
            artifact_id: Uuid::parse_str(&artifact_id)
                .map_err(|_| ManifestCatalogError::CorruptCatalog)?,
            relative_reference,
            object: ManifestObject::try_new(
                parse_digest(&content)?,
                from_i64(rows)?,
                from_i64(bytes)?,
                parse_digest(&lineage)?,
            )?,
        });
    }
    let plan = ManifestPlan::from_objects(
        reference.dataset_id.clone(),
        objects.iter().map(|value| value.object.clone()).collect(),
    )?;
    if plan.content_hash != reference.content_hash
        || plan.lineage_digest != parse_digest(&header.1)?
        || plan.row_count != from_i64(header.2)?
        || plan.total_bytes != from_i64(header.3)?
    {
        return Err(ManifestCatalogError::CorruptCatalog);
    }
    Ok(PinnedDataset {
        manifest: reference.clone(),
        plan,
        objects,
    })
}

fn manifest_for_anchor(
    connection: &Connection,
    anchor: Uuid,
) -> Result<Option<DatasetManifestRef>, ManifestCatalogError> {
    connection
        .query_row(
            "SELECT dataset_id, manifest_version, content_hash FROM analytical_generations
             WHERE anchor_manifest_id=?1",
            [anchor.to_string()],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, Vec<u8>>(2)?,
                ))
            },
        )
        .optional()?
        .map(|(dataset, version, digest)| {
            DatasetManifestRef::try_new(
                DatasetId::try_from(dataset.as_str())?,
                from_i64(version)?,
                parse_digest(&digest)?,
            )
            .map_err(ManifestCatalogError::from)
        })
        .transpose()
}

fn generation_source(
    connection: &Connection,
    manifest: &DatasetManifestRef,
) -> Result<SourceId, ManifestCatalogError> {
    let source: String = connection
        .query_row(
            "SELECT runs.source_id
             FROM analytical_generations AS generations
             JOIN dataset_manifests AS manifests
               ON manifests.manifest_id=generations.anchor_manifest_id
             JOIN artifacts ON artifacts.artifact_id=manifests.artifact_id
             JOIN ingest_runs AS runs ON runs.run_id=artifacts.run_id
             WHERE generations.dataset_id=?1 AND generations.manifest_version=?2",
            params![
                manifest.dataset_id().as_str(),
                to_i64(manifest.manifest_version())?
            ],
            |row| row.get(0),
        )
        .optional()?
        .ok_or(ManifestCatalogError::GenerationConflict)?;
    SourceId::try_from(source.as_str()).map_err(|_| ManifestCatalogError::CorruptCatalog)
}

fn source_for_artifact(
    connection: &Connection,
    artifact_id: Uuid,
) -> Result<SourceId, ManifestCatalogError> {
    let source: String = connection
        .query_row(
            "SELECT runs.source_id FROM artifacts
             JOIN ingest_runs AS runs USING (run_id)
             WHERE artifacts.artifact_id=?1",
            [artifact_id.to_string()],
            |row| row.get(0),
        )
        .optional()?
        .ok_or(ManifestCatalogError::AnchorMismatch)?;
    SourceId::try_from(source.as_str()).map_err(|_| ManifestCatalogError::CorruptCatalog)
}

fn previous_version(
    connection: &Connection,
    dataset_id: &DatasetId,
) -> Result<u64, ManifestCatalogError> {
    let value: Option<i64> = connection.query_row(
        "SELECT MAX(manifest_version) FROM analytical_generations WHERE dataset_id=?1",
        [dataset_id.as_str()],
        |row| row.get(0),
    )?;
    value
        .map(from_i64)
        .transpose()
        .map(|value| value.unwrap_or(0))
}

fn insert_generation_object(
    connection: &Connection,
    dataset_id: &DatasetId,
    version: u64,
    ordinal: usize,
    artifact_id: Uuid,
    object: &ManifestObject,
) -> Result<(), ManifestCatalogError> {
    connection.execute(
        "INSERT INTO analytical_generation_objects
         (dataset_id, manifest_version, ordinal, artifact_id, content_hash,
          row_count, size_bytes, lineage_hash)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
        params![
            dataset_id.as_str(),
            to_i64(version)?,
            i64::try_from(ordinal).map_err(|_| ManifestCatalogError::CountOverflow)?,
            artifact_id.to_string(),
            object.content_hash.bytes(),
            to_i64(object.row_count)?,
            to_i64(object.size_bytes)?,
            object.lineage_digest.bytes(),
        ],
    )?;
    Ok(())
}

fn sha256_from_evidence(value: EvidenceDigest) -> Result<Sha256Digest, ManifestCatalogError> {
    if !matches!(value.algorithm(), DigestAlgorithm::Sha256) {
        return Err(ManifestCatalogError::AnchorMismatch);
    }
    Ok(Sha256Digest::new(value.bytes()))
}

fn parse_digest(value: &[u8]) -> Result<Sha256Digest, ManifestCatalogError> {
    let bytes: [u8; 32] = value
        .try_into()
        .map_err(|_| ManifestCatalogError::CorruptCatalog)?;
    Ok(Sha256Digest::new(bytes))
}

fn to_i64(value: u64) -> Result<i64, ManifestCatalogError> {
    i64::try_from(value).map_err(|_| ManifestCatalogError::CountOverflow)
}

fn from_i64(value: i64) -> Result<u64, ManifestCatalogError> {
    u64::try_from(value).map_err(|_| ManifestCatalogError::CorruptCatalog)
}
