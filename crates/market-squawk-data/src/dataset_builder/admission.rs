//! Durable admission of exact Task 11 exports for native Python and model validation.

use rusqlite::{OptionalExtension as _, params};

use super::export::{FeatureLabelPythonExport, encode};
use super::{DatasetBuildError, DatasetBuilderService, FeatureLabelDataset};
use crate::{CatalogEndpointIdentity, GenerationKind, PythonDatasetCatalogError};

/// Immutable catalog-backed identity of one Task 11 Python export.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PythonDatasetAdmission {
    export: FeatureLabelPythonExport,
    catalog_identity: CatalogEndpointIdentity,
}

impl PythonDatasetAdmission {
    /// Returns the exact canonical export bytes admitted by the Task 11 producer.
    pub fn export(&self) -> &FeatureLabelPythonExport {
        &self.export
    }

    /// Returns the exact catalog endpoint identity that admitted the export.
    pub const fn catalog_identity(&self) -> CatalogEndpointIdentity {
        self.catalog_identity
    }
}

pub(super) fn register(
    builder: &DatasetBuilderService<'_>,
    dataset: &FeatureLabelDataset,
) -> Result<PythonDatasetAdmission, DatasetBuildError> {
    if builder.service.pinned(dataset.manifest())? != dataset.pinned {
        return Err(DatasetBuildError::InvalidInputGeneration);
    }
    let export = encode(dataset)?;
    let authority = builder
        .authority
        .lock()
        .map_err(|_| DatasetBuildError::AuthorityLockPoisoned)?;
    let catalog_identity = authority.catalog_endpoint_identity()?;
    let manifest = dataset.manifest();
    let dataset_id = manifest.dataset_id().as_str();
    let manifest_version = manifest.manifest_version();
    let manifest_version_sql =
        i64::try_from(manifest_version).map_err(|_| PythonDatasetCatalogError::LimitExceeded)?;
    let export_hash = export.content_hash();
    let export_bytes = export.bytes();
    authority.with_python_dataset_transaction(|transaction, now| {
        let generation_matches = transaction
            .query_row(
                "SELECT content_hash, schema_name, schema_version, schema_fingerprint,
                        generation_kind, parent_count, build_spec_digest
                 FROM analytical_generations
                 WHERE dataset_id=?1 AND manifest_version=?2",
                params![dataset_id, manifest_version_sql],
                |row| {
                    Ok((
                        row.get::<_, Vec<u8>>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, i64>(2)?,
                        row.get::<_, Vec<u8>>(3)?,
                        row.get::<_, String>(4)?,
                        row.get::<_, i64>(5)?,
                        row.get::<_, Option<Vec<u8>>>(6)?,
                    ))
                },
            )
            .optional()?;
        let Some((content, schema_name, schema_version, schema, kind, parents, build)) =
            generation_matches
        else {
            return Err(PythonDatasetCatalogError::UnknownAdmission);
        };
        let expected_parent_count = i64::try_from(dataset.pinned.parents().len())
            .map_err(|_| PythonDatasetCatalogError::LimitExceeded)?;
        if content != manifest.content_hash().bytes()
            || schema_name != manifest.schema().name()
            || u16::try_from(schema_version).ok() != Some(manifest.schema().version().get())
            || schema != manifest.schema().fingerprint()
            || kind != "derived"
            || dataset.pinned.generation_kind() != GenerationKind::Derived
            || parents != expected_parent_count
            || build.as_deref() != Some(dataset.build_spec_digest().digest().bytes().as_slice())
        {
            return Err(PythonDatasetCatalogError::CorruptAdmission);
        }

        let object_count: i64 = transaction.query_row(
            "SELECT COUNT(*) FROM analytical_generation_objects
             WHERE dataset_id=?1 AND manifest_version=?2",
            params![dataset_id, manifest_version_sql],
            |row| row.get(0),
        )?;
        if usize::try_from(object_count).ok() != Some(dataset.pinned.objects().len()) {
            return Err(PythonDatasetCatalogError::CorruptAdmission);
        }
        for (ordinal, object) in dataset.pinned.objects().iter().enumerate() {
            let ordinal =
                i64::try_from(ordinal).map_err(|_| PythonDatasetCatalogError::LimitExceeded)?;
            let matched: bool = transaction.query_row(
                "SELECT EXISTS(
                    SELECT 1
                    FROM analytical_generation_objects AS object
                    JOIN artifacts AS artifact ON artifact.artifact_id=object.artifact_id
                    WHERE object.dataset_id=?1 AND object.manifest_version=?2
                      AND object.ordinal=?3 AND object.artifact_id=?4
                      AND object.content_hash=?5 AND object.row_count=?6
                      AND object.size_bytes=?7 AND object.lineage_hash=?8
                      AND artifact.relative_reference=?9
                )",
                params![
                    dataset_id,
                    manifest_version_sql,
                    ordinal,
                    object.artifact_id().to_string(),
                    object.object().content_hash().bytes(),
                    i64::try_from(object.object().row_count())
                        .map_err(|_| PythonDatasetCatalogError::LimitExceeded)?,
                    i64::try_from(object.object().size_bytes())
                        .map_err(|_| PythonDatasetCatalogError::LimitExceeded)?,
                    object.object().lineage_digest().bytes(),
                    object.relative_reference(),
                ],
                |row| row.get(0),
            )?;
            if !matched {
                return Err(PythonDatasetCatalogError::CorruptAdmission);
            }
        }

        let inserted = transaction.execute(
            "INSERT OR IGNORE INTO python_dataset_admissions
             (export_sha256, catalog_identity, dataset_id, manifest_version, descriptor_json,
              selection_digest_version, registered_at_ns)
             VALUES (?1, ?2, ?3, ?4, ?5, 1, ?6)",
            params![
                export_hash.bytes(),
                catalog_identity.bytes(),
                dataset_id,
                manifest_version_sql,
                export_bytes,
                now.unix_nanos(),
            ],
        )?;
        let retained: Option<(Vec<u8>, String, i64, Vec<u8>, i64)> = transaction
            .query_row(
                "SELECT catalog_identity, dataset_id, manifest_version, descriptor_json,
                        selection_digest_version
                 FROM python_dataset_admissions WHERE export_sha256=?1",
                params![export_hash.bytes()],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                    ))
                },
            )
            .optional()?;
        if retained.as_ref().is_none_or(
            |(catalog, retained_dataset, version, bytes, digest_version)| {
                catalog.as_slice() != catalog_identity.bytes()
                    || retained_dataset != dataset_id
                    || u64::try_from(*version).ok() != Some(manifest_version)
                    || bytes.as_slice() != export_bytes
                    || *digest_version != 1
            },
        ) {
            return Err(PythonDatasetCatalogError::CorruptAdmission);
        }
        if inserted == 1 {
            transaction.execute(
                "INSERT INTO audit_events
                 (event_type, subject_id, details_digest, occurred_at_ns)
                 VALUES ('python-dataset.admitted', ?1, ?2, ?3)",
                params![
                    format!("{dataset_id}:{manifest_version}"),
                    export_hash.bytes(),
                    now.unix_nanos(),
                ],
            )?;
        }
        Ok(())
    })?;
    Ok(PythonDatasetAdmission {
        export,
        catalog_identity,
    })
}
