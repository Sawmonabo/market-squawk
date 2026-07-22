//! Exact catalog/root first-bind crash recovery tests.

use std::time::Duration;

use market_squawk_platform::LocalPaths;
use rusqlite::{Connection, params};

use super::{AnalyticalDataService, CompactionRequest, IngestError};
use crate::authority_transition::{AuthorityTransitionService, FirstBindCheckpoint};
use crate::migrations::MIGRATIONS;
use crate::{
    AnalyticalManifestCatalog, CatalogAuthority, CatalogConfig, CatalogError, CatalogLimit,
    CatalogResultLimits, DatasetId, DatasetManifestRef, DatasetSchemaRegistry, ObjectStoreConfig,
    ParquetObjectStore, Sha256Digest,
};

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[test]
fn compaction_identity_binds_exact_source_schema() -> TestResult {
    let registry = DatasetSchemaRegistry::local();
    let source_v1 = DatasetManifestRef::try_new_with_schema(
        DatasetId::try_from("fred-gdp")?,
        4,
        registry.canonical_research_observations()?,
        Sha256Digest::new([7; 32]),
    )?;
    let source_v2 = DatasetManifestRef::try_new_with_schema(
        DatasetId::try_from("fred-gdp")?,
        4,
        registry.canonical_feature_labels()?,
        Sha256Digest::new([7; 32]),
    )?;

    assert_ne!(
        CompactionRequest::new(source_v1).payload_digest(),
        CompactionRequest::new(source_v2).payload_digest()
    );
    Ok(())
}

#[test]
fn ordinary_open_does_not_implicitly_initialize_authority() -> TestResult {
    let directory = tempfile::tempdir()?;
    let paths = LocalPaths::prepare(directory.path().join("market-squawk"))?;
    let location = paths.catalog()?.clone();
    let result = AnalyticalDataService::open(
        CatalogAuthority::open(test_catalog_config(location.clone())?)?,
        AnalyticalManifestCatalog::open(&location, 8)?,
        paths.artifacts()?.clone(),
        ObjectStoreConfig::try_new(1024 * 1024, 32, Duration::from_secs(60))?,
    );

    assert!(matches!(
        result,
        Err(IngestError::Catalog(
            CatalogError::ArtifactRootAuthorityInitializationRequired
        ))
    ));
    let initialized = AnalyticalDataService::initialize(
        CatalogAuthority::open(test_catalog_config(location.clone())?)?,
        AnalyticalManifestCatalog::open(&location, 8)?,
        paths.artifacts()?.clone(),
        ObjectStoreConfig::try_new(1024 * 1024, 32, Duration::from_secs(60))?,
    )?;
    drop(initialized);
    let reopened = AnalyticalDataService::open(
        CatalogAuthority::open(test_catalog_config(location.clone())?)?,
        AnalyticalManifestCatalog::open(&location, 8)?,
        paths.artifacts()?.clone(),
        ObjectStoreConfig::try_new(1024 * 1024, 32, Duration::from_secs(60))?,
    )?;
    drop(reopened);
    Ok(())
}

#[test]
fn explicitly_migrates_exact_version_three_and_four_legacy_roots() -> TestResult {
    for legacy_version in [3_usize, 4] {
        let directory = tempfile::tempdir()?;
        let paths = LocalPaths::prepare(directory.path().join("market-squawk"))?;
        let location = paths.catalog()?.clone();
        prepare_legacy_catalog(&location, legacy_version)?;
        let authority = CatalogAuthority::open(test_catalog_config(location.clone())?)?;
        ParquetObjectStore::create_legacy_root_fixture(
            paths.artifacts()?.clone(),
            test_object_config()?,
            authority.artifact_root_binding(),
        )?;

        let migrated = AnalyticalDataService::migrate_legacy(
            authority,
            AnalyticalManifestCatalog::open(&location, 8)?,
            paths.artifacts()?.clone(),
            test_object_config()?,
        )?;
        drop(migrated);

        let reopened = AnalyticalDataService::open(
            CatalogAuthority::open(test_catalog_config(location.clone())?)?,
            AnalyticalManifestCatalog::open(&location, 8)?,
            paths.artifacts()?.clone(),
            test_object_config()?,
        )?;
        drop(reopened);
    }
    Ok(())
}

#[test]
fn recovers_exact_first_bind_after_each_durable_checkpoint() -> TestResult {
    for checkpoint in [
        FirstBindCheckpoint::CatalogPrepared,
        FirstBindCheckpoint::MarkerPending,
        FirstBindCheckpoint::MarkerFinal,
        FirstBindCheckpoint::BindingPending,
        FirstBindCheckpoint::BindingFinal,
        FirstBindCheckpoint::CatalogBound,
    ] {
        let directory = tempfile::tempdir()?;
        let paths = LocalPaths::prepare(directory.path().join("market-squawk"))?;
        let location = paths.catalog()?.clone();
        AuthorityTransitionService::initialize_fault_fixture(
            CatalogAuthority::open(test_catalog_config(location.clone())?)?,
            paths.artifacts()?.clone(),
            test_object_config()?,
            checkpoint,
        )?;

        let recovered = AnalyticalDataService::initialize(
            CatalogAuthority::open(test_catalog_config(location.clone())?)?,
            AnalyticalManifestCatalog::open(&location, 8)?,
            paths.artifacts()?.clone(),
            test_object_config()?,
        )?;
        drop(recovered);

        let reopened = AnalyticalDataService::open(
            CatalogAuthority::open(test_catalog_config(location.clone())?)?,
            AnalyticalManifestCatalog::open(&location, 8)?,
            paths.artifacts()?.clone(),
            test_object_config()?,
        )?;
        drop(reopened);
    }
    Ok(())
}

fn prepare_legacy_catalog(
    location: &market_squawk_platform::CatalogLocation,
    migration_count: usize,
) -> TestResult {
    let catalog_file = location.prepare_catalog_file()?;
    drop(catalog_file);
    let connection = Connection::open(location.path())?;
    connection.pragma_update(None, "application_id", 0x4d53_514b_i64)?;
    for migration in MIGRATIONS.iter().take(migration_count) {
        connection.execute_batch(migration.sql)?;
        connection.execute(
            "INSERT INTO schema_migrations(version, sha256, applied_at_ns) VALUES (?1, ?2, ?3)",
            params![migration.version, migration.sha256, 1_i64],
        )?;
    }
    Ok(())
}

fn test_object_config() -> Result<ObjectStoreConfig, crate::ParquetStoreError> {
    ObjectStoreConfig::try_new(1024 * 1024, 32, Duration::from_secs(60))
}

fn test_catalog_config(
    location: market_squawk_platform::CatalogLocation,
) -> Result<CatalogConfig, crate::CatalogError> {
    CatalogConfig::try_new(
        location,
        Duration::from_millis(750),
        CatalogLimit::new(32)?,
        CatalogResultLimits::try_new(1024 * 1024, 8 * 1024 * 1024)?,
    )
}
