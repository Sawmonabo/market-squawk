// Rust #159105: this macOS-only test-link diagnostic is caused by the measured
// `__eh_frame` exceeding arm64 compact-unwind's 24-bit offset range.
#![allow(linker_messages)]

use std::error::Error;
use std::num::{NonZeroU16, NonZeroU32, NonZeroU64, NonZeroUsize};
use std::str::FromStr as _;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use arrow::array::{StringArray, TimestampNanosecondArray, UInt32Array};
use market_squawk_data::{
    AnalyticalDataService, AnalyticalFeatureDatasetSelection, AnalyticalManifestCatalog,
    AnalyticalObservationReadRequest, AnalyticalObservationTemplate, AnalyticalReadError,
    AnalyticalReadLimit, CatalogAuthority, CatalogConfig, CatalogError, CatalogLimit,
    CatalogResultLimits, ChronologicalSplitPolicy, CommittedDataset, CompactionRequest,
    ComponentAdjustmentEvidence, ComponentKind, ComponentScope, ComponentSelector, ComponentValue,
    CorporateActionAdjustment, CorporateActionLimits, CorporateActionPolicy,
    CorporateActionSensitivity, DatasetBuildError, DatasetBuildInputs, DatasetBuildLimits,
    DatasetBuildPolicy, DatasetBuildRequest, DatasetBuilder, DatasetId, DatasetManifestRef,
    DatasetOutputAuthorization, DatasetSchemaRegistry, FeatureLabelComponentInput,
    FeatureLabelComponentSpec, IngestError, IngestIdentity, MAX_RETAINED_PYTHON_DATASET_ADMISSIONS,
    MAX_RETAINED_PYTHON_DATASET_DESCRIPTOR_BYTES, ManifestCatalogError, MissingValuePolicy,
    ObjectStoreConfig, ObservationFamilyKey, ObservationKnowledgeRange, ParquetStoreError,
    PointInTimeLimits, PointInTimePolicy, PointInTimeRevisionMode, QueryArtifactReservationInput,
    QueryError, QueryLimits, QueryRequest, QueryResult, ResearchArrowBatch, ResearchIngestService,
    ResearchQueryEngine, ResearchUse, ResearchUseGrantInput, ResearchUseLimits, ResearchUseSet,
    RightsBasis, RightsDecisionInput, Sha256Digest, SourceOperation, UniverseId, UniverseLimits,
    UniverseMembership, extraction_batch_digest,
};
use market_squawk_domain::{
    AuthorizationBasis, ChecksumCapability, CoverageDelay, DataQuality, DeliveryEvidence,
    DigestAlgorithm, EffectiveInterval, EvidenceDigest, ExactPayloadEvidence, InstrumentId,
    MacroObservation, MetadataRevision, PayloadReference, ResearchContext, ResearchObservation,
    ResearchProvenance, ResearchProvenanceInput, ResearchTemporalCoordinate, ResearchTime,
    RevisionBoundPayloadEvidence, RevisionNumber, SchemaVersion, SequenceCapability, SourceId,
    SourceIdentifier, Timestamp, UniverseMembershipObservation,
};
use market_squawk_platform::LocalPaths;
use market_squawk_sources::{
    AuthorizationGrant, AuthorizationMode, AvailabilityEvidence as SourceAvailabilityEvidence,
    CanonicalObservationPayload, CoverageDomain, DiscoveryRequest, ExtractionBatch,
    ExtractionRecord, ExtractionRequest, ExtractionRevisionEvidence, ExtractionRevisionPlan,
    FreshnessPolicy, HistoricalCapability, NetworkAccessPolicy, ObservedProviderOrder,
    SourceCapabilities, SourceClass, SourceCoverage, SourceMetadata, SourceMetadataInput,
    SourceObject, SourceProtocolProfile,
};
use rusqlite::params;
use rust_decimal::Decimal;
use sha2::{Digest as _, Sha256};
use tokio_util::sync::CancellationToken;

type TestResult = Result<(), Box<dyn Error>>;

const ARTIFACT_QUERY: &str = "SELECT a.value FROM observations
     CROSS JOIN (VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9)) AS a(value)
     CROSS JOIN (VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9)) AS b(value)
     CROSS JOIN (VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9)) AS c(value)
     CROSS JOIN (VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9)) AS d(value)
     CROSS JOIN (VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9)) AS e(value)";

#[test]
fn a_live_service_excludes_a_second_catalog_from_the_same_artifact_root() -> TestResult {
    let first_directory = tempfile::tempdir()?;
    let first_paths = LocalPaths::prepare(first_directory.path().join("market-squawk"))?;
    let second_directory = tempfile::tempdir()?;
    let second_paths = LocalPaths::prepare(second_directory.path().join("market-squawk"))?;
    let first_location = first_paths.catalog()?.clone();
    let second_location = second_paths.catalog()?.clone();
    let store_config = ObjectStoreConfig::try_new(1024 * 1024, 32, Duration::from_secs(60))?;
    let first = AnalyticalDataService::initialize(
        CatalogAuthority::open(test_catalog_config(first_location.clone())?)?,
        AnalyticalManifestCatalog::open(&first_location, 8)?,
        first_paths.artifacts()?.clone(),
        store_config,
    )?;

    let conflicting = AnalyticalDataService::initialize(
        CatalogAuthority::open(test_catalog_config(second_location.clone())?)?,
        AnalyticalManifestCatalog::open(&second_location, 8)?,
        first_paths.artifacts()?.clone(),
        store_config,
    );

    assert!(matches!(
        conflicting,
        Err(IngestError::Parquet(
            ParquetStoreError::RootAuthorityAlreadyOwned
        ))
    ));
    drop(first);

    let remapped = AnalyticalDataService::open(
        CatalogAuthority::open(test_catalog_config(first_location.clone())?)?,
        AnalyticalManifestCatalog::open(&first_location, 8)?,
        second_paths.artifacts()?.clone(),
        store_config,
    );
    assert!(matches!(
        remapped,
        Err(IngestError::Parquet(ParquetStoreError::RootCatalogMismatch))
    ));
    let restarted = AnalyticalDataService::open(
        CatalogAuthority::open(test_catalog_config(first_location.clone())?)?,
        AnalyticalManifestCatalog::open(&first_location, 8)?,
        first_paths.artifacts()?.clone(),
        store_config,
    )?;
    drop(restarted);
    Ok(())
}

#[cfg(unix)]
#[test]
fn analytical_composition_rejects_catalog_replacement_between_opens() -> TestResult {
    let first_directory = tempfile::tempdir()?;
    let first_paths = LocalPaths::prepare(first_directory.path().join("market-squawk"))?;
    let first_location = first_paths.catalog()?.clone();
    let authority = CatalogAuthority::open(test_catalog_config(first_location.clone())?)?;

    let replacement_directory = tempfile::tempdir()?;
    let replacement_paths =
        LocalPaths::prepare(replacement_directory.path().join("market-squawk"))?;
    let replacement_location = replacement_paths.catalog()?.clone();
    drop(CatalogAuthority::open(test_catalog_config(
        replacement_location.clone(),
    )?)?);

    let displaced = first_location
        .path()
        .with_file_name("displaced-catalog.sqlite3");
    std::fs::rename(first_location.path(), displaced)?;
    std::fs::rename(replacement_location.path(), first_location.path())?;
    let manifests = AnalyticalManifestCatalog::open(&first_location, 8)?;

    let composition = AnalyticalDataService::initialize(
        authority,
        manifests,
        first_paths.artifacts()?.clone(),
        ObjectStoreConfig::try_new(1024 * 1024, 32, Duration::from_secs(60))?,
    );

    assert!(matches!(
        composition,
        Err(IngestError::CatalogCompositionMismatch)
    ));
    Ok(())
}

#[test]
fn a_catalog_rejects_a_replacement_directory_at_the_same_artifact_path() -> TestResult {
    let directory = tempfile::tempdir()?;
    let local_root = directory.path().join("market-squawk");
    let paths = LocalPaths::prepare(&local_root)?;
    let location = paths.catalog()?.clone();
    let artifact_path = paths.artifacts()?.root().to_path_buf();
    let store_config = ObjectStoreConfig::try_new(1024 * 1024, 32, Duration::from_secs(60))?;
    let service = AnalyticalDataService::initialize(
        CatalogAuthority::open(test_catalog_config(location.clone())?)?,
        AnalyticalManifestCatalog::open(&location, 8)?,
        paths.artifacts()?.clone(),
        store_config,
    )?;
    drop(service);
    drop(paths);

    std::fs::rename(&artifact_path, local_root.join("replaced-artifacts"))?;
    std::fs::create_dir(&artifact_path)?;
    let replacement_paths = LocalPaths::prepare(&local_root)?;
    let replacement = AnalyticalDataService::open(
        CatalogAuthority::open(test_catalog_config(location.clone())?)?,
        AnalyticalManifestCatalog::open(&location, 8)?,
        replacement_paths.artifacts()?.clone(),
        store_config,
    );

    assert!(matches!(
        replacement,
        Err(IngestError::Parquet(ParquetStoreError::RootCatalogMismatch))
    ));
    Ok(())
}

#[test]
fn a_legacy_v4_catalog_requires_explicit_root_migration_after_replacement() -> TestResult {
    let directory = tempfile::tempdir()?;
    let local_root = directory.path().join("market-squawk");
    let paths = LocalPaths::prepare(&local_root)?;
    let location = paths.catalog()?.clone();
    let artifact_path = paths.artifacts()?.root().to_path_buf();
    drop(create_legacy_catalog(&location, 4)?);
    drop(paths);

    std::fs::rename(&artifact_path, local_root.join("legacy-artifacts"))?;
    std::fs::create_dir(&artifact_path)?;
    for _attempt in 0..2 {
        let replacement_paths = LocalPaths::prepare(&local_root)?;
        let replacement = AnalyticalDataService::open(
            CatalogAuthority::open(test_catalog_config(location.clone())?)?,
            AnalyticalManifestCatalog::open(&location, 8)?,
            replacement_paths.artifacts()?.clone(),
            ObjectStoreConfig::try_new(1024 * 1024, 32, Duration::from_secs(60))?,
        );
        assert!(matches!(
            replacement,
            Err(IngestError::Catalog(
                CatalogError::ArtifactRootMigrationRequired
            ))
        ));
    }
    Ok(())
}

#[test]
fn a_v2_catalog_with_artifacts_cannot_fabricate_root_authority() -> TestResult {
    let directory = tempfile::tempdir()?;
    let paths = LocalPaths::prepare(directory.path().join("market-squawk"))?;
    let location = paths.catalog()?.clone();
    let catalog_config = test_catalog_config(location.clone())?;
    let connection = create_legacy_catalog(&location, 2)?;
    connection.execute_batch("BEGIN;")?;
    connection.execute(
        "INSERT INTO sources VALUES ('v2-source', zeroblob(32), 1, 1)",
        [],
    )?;
    connection.execute(
        "INSERT INTO source_revisions VALUES ('v2-source', zeroblob(32), '{}', 1)",
        [],
    )?;
    let rights_id = legacy_rights_id(
        "v2-source",
        digest(0),
        Timestamp::from_unix_nanos(1),
        "https://example.test/terms",
        digest(0),
        digest(0),
        None,
        4,
    );
    connection.execute(
        "INSERT INTO source_rights VALUES (
             ?1, 'v2-source', 1, zeroblob(32), 1,
             'https://example.test/terms', 1, zeroblob(32), 1, zeroblob(32), NULL, 4, 1
         )",
        [rights_id.as_slice()],
    )?;
    connection.execute(
        "INSERT INTO ingest_runs VALUES (
             '00000000-0000-0000-0000-000000000001', 'v2-artifact', 'v2-source',
             1, zeroblob(32), 'persist', ?1, 'succeeded', 1, 2
         )",
        [rights_id.as_slice()],
    )?;
    connection.execute_batch(
        "INSERT INTO artifacts VALUES (
             '00000000-0000-0000-0000-000000000002',
             '00000000-0000-0000-0000-000000000001',
             'objects/sha256/00/fixture.parquet', 1, zeroblob(32), 1, 2
         );
         COMMIT;",
    )?;
    drop(connection);

    let service = AnalyticalDataService::initialize(
        CatalogAuthority::open(catalog_config)?,
        AnalyticalManifestCatalog::open(&location, 8)?,
        paths.artifacts()?.clone(),
        ObjectStoreConfig::try_new(1024 * 1024, 32, Duration::from_secs(60))?,
    );
    assert!(matches!(
        service,
        Err(IngestError::Catalog(
            CatalogError::ArtifactRootAuthorityTransitionConflict
        ))
    ));
    Ok(())
}

#[test]
fn legacy_rights_fingerprint_corruption_blocks_authority_migration() -> TestResult {
    let directory = tempfile::tempdir()?;
    let paths = LocalPaths::prepare(directory.path().join("market-squawk"))?;
    let location = paths.catalog()?.clone();
    let connection = create_legacy_catalog(&location, 2)?;
    connection.execute_batch(
        "PRAGMA foreign_keys = ON;
         BEGIN;
         INSERT INTO sources VALUES ('corrupt-v2-source', zeroblob(32), 1, 1);
         INSERT INTO source_revisions VALUES ('corrupt-v2-source', zeroblob(32), '{}', 1);
         INSERT INTO source_rights VALUES (
             zeroblob(32), 'corrupt-v2-source', 1, zeroblob(32), 1,
             'https://example.test/terms', 1, zeroblob(32), 1, zeroblob(32), NULL, 4, 1
         );
         COMMIT;",
    )?;
    drop(connection);

    assert!(matches!(
        CatalogAuthority::open(test_catalog_config(location)?),
        Err(CatalogError::CorruptCatalog)
    ));
    Ok(())
}

#[test]
fn ingest_rights_guards_reject_future_mismatch_and_expired_legacy_run() -> TestResult {
    let current_directory = tempfile::tempdir()?;
    let current_paths = LocalPaths::prepare(current_directory.path().join("current"))?;
    let current_location = current_paths.catalog()?.clone();
    drop(CatalogAuthority::open(test_catalog_config(
        current_location.clone(),
    )?)?);
    let current = rusqlite::Connection::open(current_location.path())?;
    current.execute_batch(
        "PRAGMA foreign_keys = ON;
         BEGIN;
         INSERT INTO sources VALUES ('source-a', zeroblob(32), 1, 1);
         INSERT INTO source_revisions VALUES ('source-a', zeroblob(32), '{}', 1);
         INSERT INTO sources VALUES ('source-b', zeroblob(32), 1, 1);
         INSERT INTO source_revisions VALUES ('source-b', zeroblob(32), '{}', 1);
         INSERT INTO source_rights VALUES (
             X'0707070707070707070707070707070707070707070707070707070707070707',
             'source-a', 1, X'0101010101010101010101010101010101010101010101010101010101010101',
             1, 'https://example.test/terms', 1, zeroblob(32), 1, zeroblob(32),
             NULL, 4, 1, 'reviewed_terms', NULL, NULL, 2
         );",
    )?;
    let mismatch = current.execute(
        "INSERT INTO ingest_runs VALUES (
             '00000000-0000-0000-0000-000000000010', 'mismatch', 'source-b',
             1, X'0202020202020202020202020202020202020202020202020202020202020202',
             'train',
             X'0707070707070707070707070707070707070707070707070707070707070707',
             'reserved', 2, NULL
         )",
        [],
    );
    assert!(mismatch.is_err());
    current.execute_batch("ROLLBACK;")?;

    let legacy_directory = tempfile::tempdir()?;
    let legacy_paths = LocalPaths::prepare(legacy_directory.path().join("legacy"))?;
    let legacy_location = legacy_paths.catalog()?.clone();
    let legacy = create_legacy_catalog(&legacy_location, 2)?;
    let rights_id = legacy_rights_id(
        "expired-source",
        digest(1),
        Timestamp::from_unix_nanos(1),
        "https://example.test/terms",
        digest(0),
        digest(0),
        Some(Timestamp::from_unix_nanos(10)),
        4,
    );
    legacy.execute_batch(
        "PRAGMA foreign_keys = ON;
         BEGIN;
         INSERT INTO sources VALUES ('expired-source', zeroblob(32), 1, 1);
         INSERT INTO source_revisions VALUES ('expired-source', zeroblob(32), '{}', 1);",
    )?;
    legacy.execute(
        "INSERT INTO source_rights VALUES (
             ?1, 'expired-source', 1,
             X'0101010101010101010101010101010101010101010101010101010101010101',
             1, 'https://example.test/terms', 1, zeroblob(32), 1, zeroblob(32), 10, 4, 1
         )",
        [rights_id.as_slice()],
    )?;
    legacy.execute(
        "INSERT INTO ingest_runs VALUES (
             '00000000-0000-0000-0000-000000000011', 'expired', 'expired-source',
             1, X'0101010101010101010101010101010101010101010101010101010101010101',
             'persist', ?1, 'reserved', 10, NULL
         )",
        [rights_id.as_slice()],
    )?;
    legacy.execute_batch("COMMIT;")?;
    drop(legacy);
    assert!(matches!(
        CatalogAuthority::open(test_catalog_config(legacy_location)?),
        Err(CatalogError::CorruptCatalog)
    ));
    Ok(())
}

#[tokio::test]
async fn cross_root_artifact_authority_fails_before_publication_or_bind() -> TestResult {
    let first_directory = tempfile::tempdir()?;
    let first_paths = LocalPaths::prepare(first_directory.path().join("market-squawk"))?;
    let second_directory = tempfile::tempdir()?;
    let second_paths = LocalPaths::prepare(second_directory.path().join("market-squawk"))?;
    let first_location = first_paths.catalog()?.clone();
    let second_location = second_paths.catalog()?.clone();
    let store_config = ObjectStoreConfig::try_new(8 * 1024 * 1024, 8192, Duration::from_secs(60))?;
    let (first, committed) = initialized_service_with_dataset(
        &first_paths,
        test_catalog_config(first_location.clone())?,
        store_config,
    )
    .await?;
    let second = AnalyticalDataService::initialize(
        CatalogAuthority::open(test_catalog_config(second_location.clone())?)?,
        AnalyticalManifestCatalog::open(&second_location, 8)?,
        second_paths.artifacts()?.clone(),
        store_config,
    )?;
    let before = count_published_objects(second_paths.artifacts()?.root())?;
    let result = ResearchQueryEngine::from_pinned_dataset(
        committed.pinned().clone(),
        "observations",
        first.object_store(),
        CancellationToken::new(),
    )
    .await?
    .with_artifact_publication(second.query_artifact_publication());

    assert!(matches!(result, Err(QueryError::ArtifactRootMismatch)));
    assert_eq!(
        count_published_objects(second_paths.artifacts()?.root())?,
        before
    );
    Ok(())
}

#[tokio::test]
async fn authorized_query_artifact_survives_restart_until_expiry() -> TestResult {
    let directory = tempfile::tempdir()?;
    let paths = LocalPaths::prepare(directory.path().join("market-squawk"))?;
    let location = paths.catalog()?.clone();
    let catalog_config = CatalogConfig::try_new(
        location.clone(),
        Duration::from_millis(750),
        CatalogLimit::new(32)?,
        CatalogResultLimits::try_new(1024 * 1024, 8 * 1024 * 1024)?,
    )?;
    let store_config = ObjectStoreConfig::try_new(8 * 1024 * 1024, 8192, Duration::from_secs(60))?;
    let (service, committed) =
        initialized_service_with_dataset(&paths, catalog_config.clone(), store_config).await?;
    let store = service.object_store();
    let limits = QueryLimits::try_new(
        100_000,
        4 * 1024 * 1024,
        64 * 1024 * 1024,
        1,
        512,
        512,
        Duration::from_secs(5),
    )?;
    let request = QueryRequest::try_new(committed.manifest().clone(), ARTIFACT_QUERY)?;
    let wall_nanos = i64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos())?;
    let expires_at = Timestamp::from_unix_nanos(wall_nanos).checked_add_nanos(120_000_000_000)?;
    let owner = SourceIdentifier::try_from("research-session-1")?;
    assert!(matches!(
        service
            .reserve_query_artifact(
                QueryArtifactReservationInput::try_new(
                    owner.clone(),
                    request.artifact_identity(&limits),
                    limits.max_bytes(),
                    Timestamp::from_unix_nanos(i64::MAX),
                )?,
                &CancellationToken::new(),
            )
            .await,
        Err(IngestError::Catalog(CatalogError::QueryArtifactExpired))
    ));
    let reservation = service
        .reserve_query_artifact(
            QueryArtifactReservationInput::try_new(
                owner.clone(),
                request.artifact_identity(&limits),
                limits.max_bytes(),
                expires_at,
            )?,
            &CancellationToken::new(),
        )
        .await?;
    let publisher = service.query_artifact_publication();
    let engine = ResearchQueryEngine::from_pinned_dataset(
        committed.pinned().clone(),
        "observations",
        service.object_store(),
        CancellationToken::new(),
    )
    .await?
    .with_artifact_publication(publisher)?;
    let result = engine
        .query(
            request.with_artifact_reservation(reservation),
            limits,
            CancellationToken::new(),
        )
        .await?;
    let QueryResult::Artifact {
        object,
        artifact,
        ownership,
    } = result
    else {
        return Err("expected authorized artifact result".into());
    };
    assert_eq!(ownership.owner(), &owner);
    assert_eq!(ownership.expires_at(), expires_at);
    assert_eq!(ownership.artifact_id(), artifact.artifact_id());
    assert!(store.verify(&object)?);
    drop(engine);
    drop(store);
    drop(service);

    let service = AnalyticalDataService::open(
        CatalogAuthority::open(catalog_config.clone())?,
        AnalyticalManifestCatalog::open(&location, 8)?,
        paths.artifacts()?.clone(),
        store_config,
    )?;
    let before_expiry = object.created_at().checked_add_nanos(61_000_000_000)?;
    assert!(before_expiry < expires_at);
    assert_eq!(
        service
            .recover_orphans(before_expiry, CancellationToken::new())
            .await?
            .quarantined(),
        0
    );
    assert!(service.object_store().verify(&object)?);
    drop(service);

    let restarted = AnalyticalDataService::open(
        CatalogAuthority::open(catalog_config)?,
        AnalyticalManifestCatalog::open(&location, 8)?,
        paths.artifacts()?.clone(),
        store_config,
    )?;
    assert!(restarted.object_store().verify(&object)?);
    let expired = expires_at.checked_add_nanos(61_000_000_000)?;
    assert_eq!(
        restarted
            .recover_orphans(expired, CancellationToken::new())
            .await?
            .quarantined(),
        1
    );
    Ok(())
}

#[tokio::test]
async fn query_artifact_writer_memory_is_pre_admitted_by_the_object_store() -> TestResult {
    let directory = tempfile::tempdir()?;
    let paths = LocalPaths::prepare(directory.path().join("market-squawk"))?;
    let location = paths.catalog()?.clone();
    let (service, committed) = initialized_service_with_dataset(
        &paths,
        test_catalog_config(location.clone())?,
        ObjectStoreConfig::try_new(1024 * 1024, 100_000, Duration::from_secs(60))?,
    )
    .await?;
    let limits = QueryLimits::try_new(
        100_000,
        4 * 1024 * 1024,
        64 * 1024 * 1024,
        1,
        512,
        512,
        Duration::from_secs(5),
    )?;
    let request = QueryRequest::try_new(committed.manifest().clone(), ARTIFACT_QUERY)?;
    let wall_nanos = i64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos())?;
    let reservation = service
        .reserve_query_artifact(
            QueryArtifactReservationInput::try_new(
                SourceIdentifier::try_from("writer-memory-owner")?,
                request.artifact_identity(&limits),
                limits.max_bytes(),
                Timestamp::from_unix_nanos(wall_nanos).checked_add_nanos(120_000_000_000)?,
            )?,
            &CancellationToken::new(),
        )
        .await?;
    let result = ResearchQueryEngine::from_pinned_dataset(
        committed.pinned().clone(),
        "observations",
        service.object_store(),
        CancellationToken::new(),
    )
    .await?
    .with_artifact_publication(service.query_artifact_publication())?
    .query(
        request.with_artifact_reservation(reservation),
        limits,
        CancellationToken::new(),
    )
    .await;
    assert!(matches!(
        result,
        Err(QueryError::Artifact(
            ParquetStoreError::StagingLimitExceeded
        ))
    ));
    Ok(())
}

#[tokio::test]
async fn rights_bound_ingest_replays_one_complete_pinned_generation() -> TestResult {
    let directory = tempfile::tempdir()?;
    let paths = LocalPaths::prepare(directory.path().join("market-squawk"))?;
    let location = paths.catalog()?.clone();
    let catalog_config = CatalogConfig::try_new(
        location.clone(),
        Duration::from_millis(750),
        CatalogLimit::new(32)?,
        CatalogResultLimits::try_new(1024 * 1024, 8 * 1024 * 1024)?,
    )?;
    let authority = CatalogAuthority::open(catalog_config.clone())?;
    let source = local_source()?;
    authority.register_source(&source, Timestamp::from_unix_nanos(10))?;
    let batch = extraction_batch()?;
    let revisions = provider_revision_plan(&batch)?;
    let payload_digest = extraction_batch_digest(&batch)?;
    let rights = authority.admit_source_rights(RightsDecisionInput {
        source_id: source.source_id().clone(),
        payload_digest,
        retrieved_at: Timestamp::from_unix_nanos(15),
        basis: RightsBasis::reviewed_terms("https://example.test/terms/v1", digest(31))?,
        authorization_evidence: digest(32),
        authorization_expires_at: Some(Timestamp::from_unix_nanos(i64::MAX)),
        permitted_operations: vec![SourceOperation::Persist],
    })?;
    let reservation = authority.reserve_ingest(
        &IngestIdentity::try_new(
            source.source_id().clone(),
            payload_digest,
            SourceOperation::Persist,
            "fred:gdp:2026q1:v1",
        )?,
        &rights,
    )?;
    let service = AnalyticalDataService::initialize(
        authority,
        AnalyticalManifestCatalog::open(&location, 8)?,
        paths.artifacts()?.clone(),
        ObjectStoreConfig::try_new(8 * 1024 * 1024, 1024, Duration::from_secs(60))?,
    )?;

    let first = service
        .ingest_with_revision_plan(
            reservation.clone(),
            batch.clone(),
            revisions.clone(),
            CancellationToken::new(),
        )
        .await?;
    assert_eq!(first.manifest().schema_version().get(), 3);
    let replay = service
        .ingest_with_revision_plan(reservation, batch, revisions, CancellationToken::new())
        .await?;
    assert_eq!(first, replay);
    let batches = service
        .object_store()
        .read_pinned(first.pinned(), &CancellationToken::new())?;
    let observations = batches
        .into_iter()
        .map(ResearchArrowBatch::try_from_record_batch)
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .map(|batch| batch.observations())
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(observations.iter().map(Vec::len).sum::<usize>(), 1);
    let Some(ResearchObservation::Macro(observation)) =
        observations.first().and_then(|batch| batch.first())
    else {
        return Err("expected one rebound macro observation".into());
    };
    assert_eq!(observation.context().time().revision().get(), 1);
    let query = ResearchQueryEngine::from_pinned_dataset(
        first.pinned().clone(),
        "observations",
        service.object_store(),
        CancellationToken::new(),
    )
    .await?;
    assert!(matches!(
        query
            .query(
                QueryRequest::try_new(
                    first.manifest().clone(),
                    "SELECT source_id, effective_at FROM observations",
                )?,
                QueryLimits::try_new(
                    10,
                    64 * 1024,
                    8 * 1024 * 1024,
                    1,
                    128,
                    128,
                    Duration::from_secs(1),
                )?,
                CancellationToken::new(),
            )
            .await?,
        QueryResult::Inline { .. }
    ));
    let pinned_memory = query
        .query(
            QueryRequest::try_new(
                first.manifest().clone(),
                "SELECT source_id, effective_at FROM observations",
            )?,
            QueryLimits::try_new(10, 8 * 1024, 8 * 1024, 1, 128, 128, Duration::from_secs(1))?,
            CancellationToken::new(),
        )
        .await;
    assert!(matches!(
        pinned_memory,
        Err(QueryError::MemoryLimitExceeded { limit: 8192 })
    ));

    let first_pinned = first.pinned().clone();
    let compaction = CompactionRequest::new(first.manifest().clone());
    drop(query);
    drop(service);
    let authority = CatalogAuthority::open(catalog_config)?;
    let rights = authority.admit_source_rights(RightsDecisionInput {
        source_id: source.source_id().clone(),
        payload_digest: compaction.payload_digest(),
        retrieved_at: Timestamp::from_unix_nanos(15),
        basis: RightsBasis::reviewed_terms("https://example.test/terms/v1", digest(31))?,
        authorization_evidence: digest(32),
        authorization_expires_at: Some(Timestamp::from_unix_nanos(i64::MAX)),
        permitted_operations: vec![SourceOperation::Persist],
    })?;
    let reservation = authority.reserve_ingest(
        &IngestIdentity::try_new(
            source.source_id().clone(),
            compaction.payload_digest(),
            SourceOperation::Persist,
            "fred:gdp:compact:v1",
        )?,
        &rights,
    )?;
    let service = AnalyticalDataService::open(
        authority,
        AnalyticalManifestCatalog::open(&location, 8)?,
        paths.artifacts()?.clone(),
        ObjectStoreConfig::try_new(8 * 1024 * 1024, 1024, Duration::from_secs(60))?,
    )?;
    let compacted = service
        .compact(reservation, compaction, CancellationToken::new())
        .await?;
    assert_eq!(compacted.manifest().manifest_version(), 2);
    assert_eq!(
        compacted.pinned().plan().row_count(),
        first_pinned.plan().row_count()
    );
    assert_eq!(
        compacted.pinned().plan().lineage_digest(),
        first_pinned.plan().lineage_digest()
    );
    assert_eq!(compacted.pinned().objects().len(), 1);
    assert!(
        !service
            .object_store()
            .read_pinned(&first_pinned, &CancellationToken::new())?
            .is_empty()
    );

    let store = service.object_store();
    let deferred = ResearchQueryEngine::from_pinned_dataset(
        compacted.pinned().clone(),
        "observations",
        Arc::clone(&store),
        CancellationToken::new(),
    )
    .await?;
    let deadline = deferred
        .query(
            QueryRequest::try_new(
                compacted.manifest().clone(),
                "SELECT source_id FROM observations",
            )?,
            QueryLimits::try_new(
                10,
                64 * 1024,
                8 * 1024 * 1024,
                1,
                128,
                128,
                Duration::from_nanos(1),
            )?,
            CancellationToken::new(),
        )
        .await;
    assert!(matches!(deadline, Err(QueryError::DeadlineExceeded)));
    let cancelled = CancellationToken::new();
    cancelled.cancel();
    assert!(matches!(
        deferred
            .query(
                QueryRequest::try_new(
                    compacted.manifest().clone(),
                    "SELECT source_id FROM observations",
                )?,
                QueryLimits::try_new(
                    10,
                    64 * 1024,
                    8 * 1024 * 1024,
                    1,
                    128,
                    128,
                    Duration::from_secs(1),
                )?,
                cancelled,
            )
            .await,
        Err(QueryError::Cancelled)
    ));

    #[cfg(unix)]
    {
        use std::os::unix::fs::symlink;

        let reference = compacted.pinned().objects()[0].relative_reference();
        let exact_path = paths.artifacts()?.root().join(reference);
        let held_path = exact_path.with_extension("parquet.held");
        std::fs::rename(&exact_path, &held_path)?;
        symlink(
            held_path
                .file_name()
                .ok_or("missing held object filename")?,
            &exact_path,
        )?;
        let symlinked = deferred
            .query(
                QueryRequest::try_new(
                    compacted.manifest().clone(),
                    "SELECT source_id FROM observations",
                )?,
                QueryLimits::try_new(
                    10,
                    64 * 1024,
                    8 * 1024 * 1024,
                    1,
                    128,
                    128,
                    Duration::from_secs(1),
                )?,
                CancellationToken::new(),
            )
            .await;
        assert!(matches!(symlinked, Err(QueryError::Artifact(_))));
        std::fs::remove_file(&exact_path)?;
        std::fs::rename(&held_path, &exact_path)?;
    }

    Ok(())
}

#[test]
fn dataset_inputs_reject_a_transaction_from_another_instrument() -> TestResult {
    let instrument = InstrumentId::from_str("0187f5f1-6fc2-7fa2-bf05-2ce5354c55c1")?;
    let other_instrument = InstrumentId::from_str("0187f5f1-6fc2-7fa2-bf05-2ce5354c55c2")?;
    let parent = DatasetManifestRef::try_new_with_schema(
        DatasetId::try_from("transaction-observations")?,
        1,
        DatasetSchemaRegistry::local().canonical_research_observations()?,
        Sha256Digest::new([91; 32]),
    )?;
    let component_spec = FeatureLabelComponentSpec::try_new(
        ComponentKind::Feature,
        ComponentScope::Instrument,
        CorporateActionSensitivity::NotApplicable,
        "transaction-score",
        NonZeroU32::MIN,
    )?;
    let component = FeatureLabelComponentInput::try_new(
        component_spec.clone(),
        ComponentValue::missing(SourceIdentifier::try_from("not-observed")?),
        vec![ComponentSelector::new(ObservationFamilyKey::Transaction {
            source_id: SourceId::try_from("broker-export")?,
            instrument_id: Some(other_instrument),
            account_id: SourceIdentifier::try_from("taxable-account")?,
            source_record_id: SourceIdentifier::try_from("broker-transaction-1")?,
        })],
        ComponentAdjustmentEvidence::NotApplicable,
    )?;
    let example = market_squawk_data::DatasetExample::try_new(
        "transaction-example",
        instrument,
        Timestamp::from_unix_nanos(80),
        Timestamp::from_unix_nanos(100),
        vec![component],
    )?;
    let result = DatasetBuildInputs::try_new(
        vec![parent.clone()],
        UniverseId::try_from("us-equities.historical")?,
        vec![UniverseMembership::new(
            instrument,
            EffectiveInterval::new(Timestamp::from_unix_nanos(1), None)?,
            market_squawk_domain::AvailabilityEvidence::evidenced(
                Timestamp::from_unix_nanos(1),
                SourceIdentifier::try_from("constituent-publication")?,
            ),
            parent,
            digest(92),
        )],
        vec![component_spec],
        vec![example],
    );

    assert!(matches!(result, Err(DatasetBuildError::InvalidRequest)));
    Ok(())
}

#[tokio::test]
async fn point_in_time_builder_publishes_one_authorized_queryable_generation() -> TestResult {
    let directory = tempfile::tempdir()?;
    let paths = LocalPaths::prepare(directory.path().join("market-squawk"))?;
    let location = paths.catalog()?.clone();
    let catalog_config = test_catalog_config(location)?;
    let store_config = ObjectStoreConfig::try_new(8 * 1024 * 1024, 1024, Duration::from_secs(60))?;
    let (service, source) =
        initialized_service_with_universe(&paths, catalog_config, store_config).await?;
    let instrument = InstrumentId::from_str("0187f5f1-6fc2-7fa2-bf05-2ce5354c55c1")?;
    let feature = FeatureLabelComponentSpec::try_new(
        ComponentKind::Feature,
        ComponentScope::Global,
        CorporateActionSensitivity::NotApplicable,
        "cpi-surprise",
        NonZeroU32::MIN,
    )?;
    let label = FeatureLabelComponentSpec::try_new(
        ComponentKind::Label,
        ComponentScope::Global,
        CorporateActionSensitivity::NotApplicable,
        "gdp-next-release",
        NonZeroU32::MIN,
    )?;
    let missing_feature = FeatureLabelComponentInput::try_new(
        feature.clone(),
        ComponentValue::missing(SourceIdentifier::try_from("not-observed")?),
        vec![ComponentSelector::new(ObservationFamilyKey::Macro {
            source_id: SourceId::try_from("fred-local-fixture")?,
            series: SourceIdentifier::try_from("CPI")?,
            effective: ResearchTemporalCoordinate::exact(Timestamp::from_unix_nanos(90)),
        })],
        ComponentAdjustmentEvidence::NotApplicable,
    )?;
    let observed_label = FeatureLabelComponentInput::try_new(
        label.clone(),
        ComponentValue::decimal(
            Decimal::new(123_456, 2),
            Some(SourceIdentifier::try_from("USD")?),
            None,
        )?,
        vec![ComponentSelector::new(ObservationFamilyKey::Macro {
            source_id: SourceId::try_from("fred-local-fixture")?,
            series: SourceIdentifier::try_from("GDP")?,
            effective: ResearchTemporalCoordinate::exact(Timestamp::from_unix_nanos(90)),
        })],
        ComponentAdjustmentEvidence::NotApplicable,
    )?;
    let cutoff = Timestamp::from_unix_nanos(80);
    let label_cutoff = Timestamp::from_unix_nanos(100);
    let inputs = DatasetBuildInputs::try_new(
        vec![source.manifest().clone()],
        UniverseId::try_from("us-equities.historical")?,
        vec![UniverseMembership::new(
            instrument,
            EffectiveInterval::new(Timestamp::from_unix_nanos(1), None)?,
            market_squawk_domain::AvailabilityEvidence::evidenced(
                Timestamp::from_unix_nanos(1),
                SourceIdentifier::try_from("constituent-publication")?,
            ),
            source.manifest().clone(),
            CanonicalObservationPayload::try_from_observation(&universe_membership_observation()?)?
                .identity(),
        )],
        vec![feature.clone(), label.clone()],
        vec![market_squawk_data::DatasetExample::try_new(
            "us-gdp-example-1",
            instrument,
            cutoff,
            label_cutoff,
            vec![missing_feature.clone(), observed_label.clone()],
        )?],
    )?;
    let split = ChronologicalSplitPolicy::try_new(
        Timestamp::from_unix_nanos(100),
        Timestamp::from_unix_nanos(200),
        Timestamp::from_unix_nanos(300),
    )?;
    let pit = PointInTimePolicy::try_new(NonZeroU32::MIN, PointInTimeRevisionMode::LatestKnown)?;
    let policy = DatasetBuildPolicy::new(
        split,
        pit,
        CorporateActionPolicy::new(CorporateActionAdjustment::Raw, NonZeroU32::MIN),
        MissingValuePolicy::Preserve,
        SourceIdentifier::try_from("dataset-builder-rust-v1")?,
    );
    let research_limits = ResearchUseLimits::try_new(
        8,
        32,
        32,
        8,
        1024 * 1024,
        Duration::from_secs(2),
        Duration::from_secs(30),
    )?;
    let limits = DatasetBuildLimits::try_new(
        128,
        8,
        8,
        64,
        4 * 1024 * 1024,
        Duration::from_secs(5),
        PointInTimeLimits::try_new(128, 128, 8, 128, 1024 * 1024)?,
        UniverseLimits::try_new(16, 1024 * 1024)?,
        CorporateActionLimits::try_new(
            NonZeroUsize::new(16).ok_or("nonzero action limit")?,
            NonZeroUsize::new(1024 * 1024).ok_or("nonzero action byte limit")?,
        )?,
    )?;
    let output_authorization = DatasetOutputAuthorization::try_new(
        SourceId::try_from("market-squawk.derived")?,
        RightsBasis::reviewed_terms("https://example.test/local-derived/v1", digest(62))?,
        digest(63),
        None,
    )?;
    let fabricated_inputs = DatasetBuildInputs::try_new(
        vec![source.manifest().clone()],
        UniverseId::try_from("us-equities.historical")?,
        vec![UniverseMembership::new(
            instrument,
            EffectiveInterval::new(Timestamp::from_unix_nanos(1), None)?,
            market_squawk_domain::AvailabilityEvidence::evidenced(
                Timestamp::from_unix_nanos(1),
                SourceIdentifier::try_from("constituent-publication")?,
            ),
            source.manifest().clone(),
            digest(61),
        )],
        vec![feature, label],
        vec![market_squawk_data::DatasetExample::try_new(
            "us-gdp-example-1",
            instrument,
            cutoff,
            label_cutoff,
            vec![missing_feature, observed_label],
        )?],
    )?;
    let fabricated = DatasetBuildRequest::try_new(
        market_squawk_data::DatasetId::try_from("derived.feature-labels.gdp-v1")?,
        fabricated_inputs,
        policy.clone(),
        ResearchUse::LocalAnalysis,
        research_limits,
        output_authorization.clone(),
        limits,
    )?;
    let fabricated_result = service
        .dataset_builder()
        .build(fabricated, CancellationToken::new())
        .await;
    assert!(
        matches!(
            fabricated_result,
            Err(DatasetBuildError::UniverseEvidenceMismatch)
        ),
        "unexpected fabricated-membership result: {fabricated_result:?}"
    );
    let request = DatasetBuildRequest::try_new(
        market_squawk_data::DatasetId::try_from("derived.feature-labels.gdp-v1")?,
        inputs,
        policy,
        ResearchUse::LocalAnalysis,
        research_limits,
        output_authorization,
        limits,
    )?;

    let built = service
        .dataset_builder()
        .build(request.clone(), CancellationToken::new())
        .await?;
    assert_eq!(built.pinned().plan().row_count(), 2);
    assert_eq!(built.split_counts().train_examples(), 1);
    assert_eq!(built.split_counts().validation_examples(), 0);
    assert_eq!(built.split_counts().test_examples(), 0);
    let export = built.python_export()?;
    assert_eq!(
        export.content_hash().bytes(),
        <[u8; 32]>::from(Sha256::digest(export.bytes()))
    );
    let export_json: serde_json::Value = serde_json::from_slice(export.bytes())?;
    assert_eq!(export_json["schema_version"], 2);
    assert_eq!(
        export_json["dataset"]["build_spec_sha256"],
        hex_digest(built.build_spec_digest().digest().bytes())
    );
    assert_eq!(
        export_json["dataset"]["universe_id"],
        "us-equities.historical"
    );
    assert_eq!(export_json["split_policy"]["train_end_unix_nanos"], 100);
    assert_eq!(export_json["objects"][0]["row_count"], 2);
    let replayed = service
        .dataset_builder()
        .build(request, CancellationToken::new())
        .await?;
    assert_eq!(replayed.manifest(), built.manifest());
    let cancellation = CancellationToken::new();
    let deadline = Instant::now() + Duration::from_secs(30);
    let reader = service.analytical_reader();
    let listed = reader
        .latest(built.manifest().dataset_id(), deadline, &cancellation)?
        .ok_or("missing built feature dataset")?;
    assert_eq!(
        listed.python_export_sha256(),
        Some(export.content_hash()),
        "the public generation record must expose the exact admitted Python export"
    );
    let feature_page = reader.feature_datasets(
        None,
        AnalyticalReadLimit::try_new(8)?,
        deadline,
        &cancellation,
    )?;
    assert!(!feature_page.has_more());
    let registered = feature_page
        .datasets()
        .iter()
        .find(|dataset| dataset.generation().manifest() == built.manifest())
        .ok_or("built feature dataset is absent from the public registry")?;
    assert_eq!(registered.python_export_sha256(), export.content_hash());
    assert_eq!(registered.policy_digest(), built.policy_digest());
    assert_eq!(registered.universe_digest(), built.universe_digest());
    assert_eq!(registered.universe_id().as_str(), "us-equities.historical");
    assert_eq!(registered.split_counts(), built.split_counts());
    assert_eq!(
        registered
            .source_ids()
            .iter()
            .map(SourceId::as_str)
            .collect::<Vec<_>>(),
        vec!["fred-local-fixture"]
    );
    let overlap_candidate = built.manifest().dataset_id().clone();
    let legacy_only_candidate = DatasetId::try_from("derived.feature-labels.legacy-only-snapshot")?;
    let legacy_candidates = [overlap_candidate.clone(), legacy_only_candidate];
    let snapshot = reader.feature_dataset_snapshot(
        AnalyticalFeatureDatasetSelection::Page { after: None },
        &legacy_candidates,
        AnalyticalReadLimit::try_new(8)?,
        deadline,
        &cancellation,
    )?;
    assert_eq!(snapshot.available(), 1);
    assert_eq!(snapshot.datasets().len(), 1);
    assert_eq!(
        snapshot.overlapping_legacy_dataset_ids(),
        std::slice::from_ref(&overlap_candidate)
    );
    let exhausted_snapshot = reader.feature_dataset_snapshot(
        AnalyticalFeatureDatasetSelection::Page {
            after: Some(&overlap_candidate),
        },
        &legacy_candidates,
        AnalyticalReadLimit::try_new(8)?,
        deadline,
        &cancellation,
    )?;
    assert_eq!(exhausted_snapshot.available(), 0);
    assert!(exhausted_snapshot.datasets().is_empty());
    assert_eq!(
        exhausted_snapshot.overlapping_legacy_dataset_ids(),
        std::slice::from_ref(&overlap_candidate)
    );
    let exact_snapshot = reader.feature_dataset_snapshot(
        AnalyticalFeatureDatasetSelection::Exact(&overlap_candidate),
        std::slice::from_ref(&overlap_candidate),
        AnalyticalReadLimit::try_new(1)?,
        deadline,
        &cancellation,
    )?;
    assert_eq!(exact_snapshot.available(), 1);
    assert_eq!(exact_snapshot.datasets().len(), 1);
    assert_eq!(
        exact_snapshot.overlapping_legacy_dataset_ids(),
        std::slice::from_ref(&overlap_candidate)
    );

    let cancelled = CancellationToken::new();
    cancelled.cancel();
    assert!(matches!(
        reader.feature_dataset_snapshot(
            AnalyticalFeatureDatasetSelection::Page { after: None },
            &legacy_candidates,
            AnalyticalReadLimit::try_new(8)?,
            Instant::now() + Duration::from_secs(30),
            &cancelled,
        ),
        Err(AnalyticalReadError::Manifest(
            ManifestCatalogError::Cancelled
        ))
    ));
    assert!(matches!(
        reader.feature_dataset_snapshot(
            AnalyticalFeatureDatasetSelection::Page { after: None },
            &legacy_candidates,
            AnalyticalReadLimit::try_new(8)?,
            Instant::now(),
            &CancellationToken::new(),
        ),
        Err(AnalyticalReadError::Manifest(
            ManifestCatalogError::DeadlineExceeded
        ))
    ));

    let bounds_paths = LocalPaths::prepare(directory.path().join("admission-bounds"))?;
    let bounds_location = bounds_paths.catalog()?.clone();
    drop(CatalogAuthority::open(test_catalog_config(
        bounds_location.clone(),
    )?)?);
    let bounds = rusqlite::Connection::open(bounds_location.path())?;
    bounds.pragma_update(None, "foreign_keys", "OFF")?;
    let retained: (i64, i64) = bounds.query_row(
        "SELECT retained_rows, retained_descriptor_bytes
         FROM python_dataset_admission_retention
         WHERE singleton=1",
        [],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    assert_eq!(retained, (0, 0));
    bounds.execute(
        "INSERT INTO python_dataset_admissions
         (export_sha256, catalog_identity, dataset_id, manifest_version, descriptor_json,
          selection_digest_version, registered_at_ns)
         VALUES (?1, ?2, 'bounds-fixture', 1, ?3, 1, 1)",
        params![[1_u8; 32], [2_u8; 32], b"{}".as_slice()],
    )?;
    assert_eq!(
        bounds.query_row(
            "SELECT retained_rows, retained_descriptor_bytes
             FROM python_dataset_admission_retention
             WHERE singleton=1",
            [],
            |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)),
        )?,
        (1, 2)
    );
    bounds.execute(
        "UPDATE python_dataset_admission_retention
         SET retained_rows=?1, retained_descriptor_bytes=?2
         WHERE singleton=1",
        params![
            i64::try_from(MAX_RETAINED_PYTHON_DATASET_ADMISSIONS)?,
            i64::try_from(MAX_RETAINED_PYTHON_DATASET_DESCRIPTOR_BYTES)?
        ],
    )?;
    assert_eq!(
        bounds.execute(
            "INSERT OR IGNORE INTO python_dataset_admissions
             (export_sha256, catalog_identity, dataset_id, manifest_version, descriptor_json,
              selection_digest_version, registered_at_ns)
             VALUES (?1, ?2, 'bounds-fixture', 1, ?3, 1, 1)",
            params![[1_u8; 32], [2_u8; 32], b"{}".as_slice()],
        )?,
        0,
        "an ignored immutable replay must not consume retained budget"
    );
    assert!(
        bounds
            .execute(
                "INSERT INTO python_dataset_admissions
                 (export_sha256, catalog_identity, dataset_id, manifest_version, descriptor_json,
                  selection_digest_version, registered_at_ns)
                 VALUES (?1, ?2, 'bounds-overflow', 1, ?3, 1, 2)",
                params![[3_u8; 32], [2_u8; 32], b"{}".as_slice()],
            )
            .is_err(),
        "a new retained admission must not cross either immutable ceiling"
    );
    assert!(
        bounds
            .execute(
                "UPDATE python_dataset_admission_retention
                 SET retained_rows=?1
                 WHERE singleton=1",
                [i64::try_from(MAX_RETAINED_PYTHON_DATASET_ADMISSIONS)? + 1],
            )
            .is_err(),
        "the schema ceiling itself must reject an over-limit retained count"
    );
    assert!(
        bounds
            .execute(
                "UPDATE python_dataset_admission_retention
                 SET retained_descriptor_bytes=?1
                 WHERE singleton=1",
                [i64::try_from(MAX_RETAINED_PYTHON_DATASET_DESCRIPTOR_BYTES)? + 1],
            )
            .is_err(),
        "the schema ceiling itself must reject over-limit retained descriptor bytes"
    );
    let query = ResearchQueryEngine::from_pinned_dataset(
        built.pinned().clone(),
        "components",
        service.object_store(),
        CancellationToken::new(),
    )
    .await?;
    let result = query
        .query(
            QueryRequest::try_new(
                built.manifest().clone(),
                "SELECT component_kind, component_name, missing_reason FROM components \
                 ORDER BY component_kind, component_name",
            )?,
            QueryLimits::try_new(8, 64 * 1024, 1024 * 1024, 1, 64, 64, Duration::from_secs(1))?,
            CancellationToken::new(),
        )
        .await?;
    assert!(matches!(result, QueryResult::Inline { .. }));
    Ok(())
}

#[tokio::test]
async fn analytical_reader_keeps_manifest_authority_and_observation_evidence_closed() -> TestResult
{
    let directory = tempfile::tempdir()?;
    let paths = LocalPaths::prepare(directory.path().join("reader"))?;
    let (service, committed) = initialized_service_with_dataset(
        &paths,
        test_catalog_config(paths.catalog()?.clone())?,
        ObjectStoreConfig::try_new(1024 * 1024, 32, Duration::from_secs(60))?,
    )
    .await?;
    let reader = service.analytical_reader();
    let limit = AnalyticalReadLimit::try_new(1)?;
    let cancellation = CancellationToken::new();
    let deadline = Instant::now() + Duration::from_secs(30);

    let datasets = reader.datasets(None, limit, deadline, &cancellation)?;
    assert!(!datasets.has_more());
    assert_eq!(datasets.generations().len(), 1);
    let listed = &datasets.generations()[0];
    assert_eq!(listed.manifest(), committed.manifest());
    assert_eq!(listed.source_id().as_str(), "fred-local-fixture");
    assert_eq!(
        reader
            .latest(committed.manifest().dataset_id(), deadline, &cancellation)?
            .ok_or("missing latest generation")?
            .manifest(),
        committed.manifest()
    );
    assert_eq!(
        reader
            .exact(committed.manifest(), deadline, &cancellation)?
            .source_id(),
        listed.source_id()
    );
    let history = reader.history(
        committed.manifest().dataset_id(),
        None,
        limit,
        deadline,
        &cancellation,
    )?;
    assert_eq!(history.generations().len(), 1);
    assert_eq!(
        reader.source_owner(committed.manifest(), deadline, &cancellation)?,
        *listed.source_id()
    );

    let request = AnalyticalObservationReadRequest::try_new(
        committed.manifest().clone(),
        AnalyticalObservationTemplate::Macro,
        Vec::new(),
        Some(ObservationKnowledgeRange::try_new(
            Timestamp::from_unix_nanos(90),
            Timestamp::from_unix_nanos(110),
        )?),
    )?;
    let observed = reader
        .read_observations(
            request,
            QueryLimits::try_new(
                1,
                64 * 1024,
                8 * 1024 * 1024,
                1,
                128,
                128,
                Duration::from_secs(10),
            )?,
            deadline,
            cancellation,
        )
        .await?;
    assert_eq!(observed.source_id().as_str(), "fred-local-fixture");
    let QueryResult::Inline { batches, .. } = observed.output().result() else {
        return Err("expected one inline fixed-template result".into());
    };
    let batch = batches.first().ok_or("missing fixed-template batch")?;
    assert_eq!(batch.num_rows(), 1);
    let schema = batch.schema();
    let string = |name| -> Result<&StringArray, Box<dyn Error>> {
        Ok(batch
            .column(schema.index_of(name)?)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or("fixed-template string column changed")?)
    };
    assert_eq!(string("observation_kind")?.value(0), "macro");
    assert_eq!(string("source_id")?.value(0), "fred-local-fixture");
    assert_eq!(string("quality")?.value(0), "official_delayed");
    let revisions = batch
        .column(schema.index_of("revision")?)
        .as_any()
        .downcast_ref::<UInt32Array>()
        .ok_or("fixed-template revision column changed")?;
    assert_eq!(revisions.value(0), 1);
    for (name, expected) in [
        ("available_at", 100),
        ("effective_at", 90),
        ("published_at", 100),
        ("superseded_at", 200),
    ] {
        let values = batch
            .column(schema.index_of(name)?)
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .ok_or("fixed-template time column changed")?;
        assert_eq!(values.value(0), expected);
    }
    Ok(())
}

async fn initialized_service_with_dataset(
    paths: &LocalPaths,
    catalog_config: CatalogConfig,
    store_config: ObjectStoreConfig,
) -> Result<(AnalyticalDataService, CommittedDataset), Box<dyn Error>> {
    initialized_service_with_batch(paths, catalog_config, store_config, extraction_batch()?).await
}

async fn initialized_service_with_universe(
    paths: &LocalPaths,
    catalog_config: CatalogConfig,
    store_config: ObjectStoreConfig,
) -> Result<(AnalyticalDataService, CommittedDataset), Box<dyn Error>> {
    initialized_service_with_batch(
        paths,
        catalog_config,
        store_config,
        dataset_extraction_batch()?,
    )
    .await
}

async fn initialized_service_with_batch(
    paths: &LocalPaths,
    catalog_config: CatalogConfig,
    store_config: ObjectStoreConfig,
    batch: ExtractionBatch,
) -> Result<(AnalyticalDataService, CommittedDataset), Box<dyn Error>> {
    let location = paths.catalog()?.clone();
    let authority = CatalogAuthority::open(catalog_config)?;
    let source = local_source()?;
    authority.register_source(&source, Timestamp::from_unix_nanos(10))?;
    authority.register_source(
        &local_source_for("market-squawk.derived")?,
        Timestamp::from_unix_nanos(10),
    )?;
    let payload_digest = extraction_batch_digest(&batch)?;
    let rights = authority.admit_source_rights(RightsDecisionInput {
        source_id: source.source_id().clone(),
        payload_digest,
        retrieved_at: Timestamp::from_unix_nanos(15),
        basis: RightsBasis::reviewed_terms("https://example.test/terms/v1", digest(31))?,
        authorization_evidence: digest(32),
        authorization_expires_at: Some(Timestamp::from_unix_nanos(i64::MAX)),
        permitted_operations: vec![SourceOperation::Persist],
    })?;
    authority.admit_research_use_grant(ResearchUseGrantInput::try_new(
        rights.rights_id(),
        ResearchUseSet::try_new(vec![ResearchUse::LocalAnalysis])?,
        digest(33),
        Some(Timestamp::from_unix_nanos(i64::MAX)),
    )?)?;
    let reservation = authority.reserve_ingest(
        &IngestIdentity::try_new(
            source.source_id().clone(),
            payload_digest,
            SourceOperation::Persist,
            "fred:gdp:query-fixture:v1",
        )?,
        &rights,
    )?;
    let service = AnalyticalDataService::initialize(
        authority,
        AnalyticalManifestCatalog::open(&location, 8)?,
        paths.artifacts()?.clone(),
        store_config,
    )?;
    let committed = service
        .ingest(reservation, batch, CancellationToken::new())
        .await?;
    Ok((service, committed))
}

fn extraction_batch() -> Result<ExtractionBatch, Box<dyn Error>> {
    extraction_batch_with_membership(false)
}

fn dataset_extraction_batch() -> Result<ExtractionBatch, Box<dyn Error>> {
    extraction_batch_with_membership(true)
}

fn extraction_batch_with_membership(
    include_membership: bool,
) -> Result<ExtractionBatch, Box<dyn Error>> {
    let discovery = DiscoveryRequest::try_new(
        SourceIdentifier::try_from("fred-gdp")?,
        Some(Timestamp::from_unix_nanos(90)),
        NonZeroU16::new(1).ok_or("nonzero discovery limit")?,
        Timestamp::from_unix_nanos(1_000),
    )?;
    let object = SourceObject::try_new(
        SourceId::try_from("fred-local-fixture")?,
        MetadataRevision::new(SourceIdentifier::try_from("revision-1")?),
        &discovery,
        SourceIdentifier::try_from("gdp-2026q1")?,
        SourceIdentifier::try_from("application-json")?,
        ExactPayloadEvidence::from_content_digest(digest(4)),
        EffectiveInterval::new(Timestamp::from_unix_nanos(0), None)?,
        Some(Timestamp::from_unix_nanos(100)),
        Some(1024),
    )?;
    let request = ExtractionRequest::try_new(
        object,
        NonZeroU32::new(if include_membership { 2 } else { 1 }).ok_or("nonzero record limit")?,
        NonZeroU64::new(1024 * 1024).ok_or("nonzero byte limit")?,
        Timestamp::from_unix_nanos(1_000),
    )?;
    let payload = serde_json::to_vec(&macro_observation()?)?;
    let evidence = EvidenceDigest::new(DigestAlgorithm::Sha256, Sha256::digest(&payload).into());
    let macro_record = ExtractionRecord::try_new(
        &request,
        SourceIdentifier::try_from("market-squawk-research-v3")?,
        ExactPayloadEvidence::from_content_digest(evidence),
        Timestamp::from_unix_nanos(90),
        Some(Timestamp::from_unix_nanos(100)),
        SourceAvailabilityEvidence::Observed {
            available_at: Timestamp::from_unix_nanos(100),
            evidence: SourceIdentifier::try_from("fred-release")?,
        },
        SourceIdentifier::try_from("revision-1")?,
        Some(Timestamp::from_unix_nanos(200)),
        payload.into(),
    )?;
    let mut records = vec![macro_record];
    if !include_membership {
        return Ok(ExtractionBatch::try_new(&request, records)?);
    }
    let membership_payload = serde_json::to_vec(&universe_membership_observation()?)?;
    let membership_evidence = EvidenceDigest::new(
        DigestAlgorithm::Sha256,
        Sha256::digest(&membership_payload).into(),
    );
    let membership_record = ExtractionRecord::try_new(
        &request,
        SourceIdentifier::try_from("market-squawk-research-v3")?,
        ExactPayloadEvidence::from_content_digest(membership_evidence),
        Timestamp::from_unix_nanos(1),
        Some(Timestamp::from_unix_nanos(1)),
        SourceAvailabilityEvidence::Observed {
            available_at: Timestamp::from_unix_nanos(1),
            evidence: SourceIdentifier::try_from("constituent-publication")?,
        },
        SourceIdentifier::try_from("revision-1")?,
        None,
        membership_payload.into(),
    )?;
    records.push(membership_record);
    Ok(ExtractionBatch::try_new(&request, records)?)
}

fn provider_revision_plan(
    batch: &ExtractionBatch,
) -> Result<ExtractionRevisionPlan, Box<dyn Error>> {
    let evidence = batch
        .records()
        .iter()
        .map(|record| {
            let version = record.revision().as_str().as_bytes();
            let published = record
                .published_time()
                .cloned()
                .ok_or("provider fixture must retain publication order")?;
            let order = ObservedProviderOrder::try_new(published, version)?;
            ExtractionRevisionEvidence::provider_supplied(version, order).map_err(Into::into)
        })
        .collect::<Result<Vec<_>, Box<dyn Error>>>()?;
    Ok(ExtractionRevisionPlan::try_new(evidence)?)
}

fn macro_observation() -> Result<ResearchObservation, Box<dyn Error>> {
    let context = ResearchContext::new(
        ResearchProvenance::try_new(ResearchProvenanceInput {
            source_id: SourceId::try_from("fred-local-fixture")?,
            instrument_id: None,
            venue_id: None,
            source_identifier: SourceIdentifier::try_from("GDP:2026Q1:v1")?,
            source_timestamp: None,
            received_at: Timestamp::from_unix_nanos(110),
            ingested_at: Timestamp::from_unix_nanos(120),
            quality: DataQuality::OfficialDelayed,
            payload_reference: PayloadReference::SourceReference(SourceIdentifier::try_from(
                "fred:gdp:2026q1",
            )?),
            availability: market_squawk_domain::AvailabilityEvidence::evidenced(
                Timestamp::from_unix_nanos(100),
                SourceIdentifier::try_from("fred-release")?,
            ),
        })?,
        ResearchTime::new(
            Timestamp::from_unix_nanos(90),
            Some(Timestamp::from_unix_nanos(100)),
            RevisionNumber::new(17)?,
            Some(Timestamp::from_unix_nanos(200)),
        )?,
    )?;
    Ok(ResearchObservation::Macro(MacroObservation::new(
        context,
        SourceIdentifier::try_from("GDP")?,
        Decimal::new(123_456, 2),
        SourceIdentifier::try_from("USD")?,
    )))
}

fn universe_membership_observation() -> Result<ResearchObservation, Box<dyn Error>> {
    let instrument = InstrumentId::from_str("0187f5f1-6fc2-7fa2-bf05-2ce5354c55c1")?;
    let context = ResearchContext::new(
        ResearchProvenance::try_new(ResearchProvenanceInput {
            source_id: SourceId::try_from("fred-local-fixture")?,
            instrument_id: Some(instrument),
            venue_id: None,
            source_identifier: SourceIdentifier::try_from("us-equities:member:fixture")?,
            source_timestamp: None,
            received_at: Timestamp::from_unix_nanos(1),
            ingested_at: Timestamp::from_unix_nanos(1),
            quality: DataQuality::OfficialDelayed,
            payload_reference: PayloadReference::SourceReference(SourceIdentifier::try_from(
                "constituents:us-equities:fixture",
            )?),
            availability: market_squawk_domain::AvailabilityEvidence::evidenced(
                Timestamp::from_unix_nanos(1),
                SourceIdentifier::try_from("constituent-publication")?,
            ),
        })?,
        ResearchTime::new(
            Timestamp::from_unix_nanos(1),
            Some(Timestamp::from_unix_nanos(1)),
            RevisionNumber::new(1)?,
            None,
        )?,
    )?;
    Ok(ResearchObservation::UniverseMembership(
        UniverseMembershipObservation::new(
            context,
            SourceIdentifier::try_from("us-equities.historical")?,
            EffectiveInterval::new(Timestamp::from_unix_nanos(1), None)?,
        )?,
    ))
}

fn local_source() -> Result<SourceMetadata, Box<dyn Error>> {
    local_source_for("fred-local-fixture")
}

fn local_source_for(source_id: &str) -> Result<SourceMetadata, Box<dyn Error>> {
    let effective = EffectiveInterval::new(Timestamp::from_unix_nanos(0), None)?;
    Ok(SourceMetadata::try_new(SourceMetadataInput::new(
        SchemaVersion::CURRENT,
        SourceId::try_from(source_id)?,
        RevisionBoundPayloadEvidence::new(
            MetadataRevision::new(SourceIdentifier::try_from("revision-1")?),
            ExactPayloadEvidence::from_content_digest(digest(1)),
        ),
        SourceClass::LocalFile,
        SourceIdentifier::try_from("local")?,
        AuthorizationGrant::new(
            AuthorizationMode::UserOwnedLocal,
            AuthorizationBasis::new(SourceIdentifier::try_from("user-owned-file")?),
            ExactPayloadEvidence::from_content_digest(digest(2)),
            effective,
        ),
        SourceCoverage::try_non_instrument(
            ExactPayloadEvidence::from_content_digest(digest(3)),
            effective,
            CoverageDomain::Macroeconomic,
            CoverageDelay::Delayed(1),
            DeliveryEvidence::Unknown,
        )?,
        DataQuality::OfficialDelayed,
        NetworkAccessPolicy::Denied,
        FreshnessPolicy::try_new(1, 1, 1, 1, 0)?,
        None,
        SourceCapabilities::new(
            false,
            true,
            SequenceCapability::Unsupported,
            ChecksumCapability::Unsupported,
            HistoricalCapability::RevisionPreserving,
            false,
        ),
        SourceProtocolProfile::NotLive,
    ))?)
}

fn digest(byte: u8) -> EvidenceDigest {
    EvidenceDigest::new(DigestAlgorithm::Sha256, [byte; 32])
}

fn create_legacy_catalog(
    location: &market_squawk_platform::CatalogLocation,
    migration_count: usize,
) -> Result<rusqlite::Connection, Box<dyn Error>> {
    let migrations = [
        (1_i64, include_str!("../migrations/0001_control.sql")),
        (2_i64, include_str!("../migrations/0002_instruments.sql")),
        (3_i64, include_str!("../migrations/0003_analytical.sql")),
        (
            4_i64,
            include_str!("../migrations/0004_query_artifacts.sql"),
        ),
    ];
    if migration_count == 0 || migration_count > migrations.len() {
        return Err(rusqlite::Error::InvalidParameterName("migration_count".to_owned()).into());
    }
    drop(location.prepare_catalog_file()?);
    let connection = rusqlite::Connection::open(location.path())?;
    connection.execute_batch(
        "PRAGMA application_id = 1297305931;
         PRAGMA foreign_keys = ON;
         BEGIN;",
    )?;
    for (version, sql) in migrations.iter().take(migration_count) {
        connection.execute_batch(sql)?;
        let digest: [u8; 32] = Sha256::digest(sql.as_bytes()).into();
        connection.execute(
            "INSERT INTO schema_migrations(version, sha256, applied_at_ns)
             VALUES (?1, ?2, ?3)",
            params![version, digest.as_slice(), version],
        )?;
    }
    connection.execute_batch("COMMIT;")?;
    Ok(connection)
}

#[allow(
    clippy::too_many_arguments,
    reason = "the legacy catalog digest fixture must expose every v3 canonical field explicitly"
)]
fn legacy_rights_id(
    source_id: &str,
    payload_digest: EvidenceDigest,
    retrieved_at: Timestamp,
    terms_url: &str,
    terms_digest: EvidenceDigest,
    authorization_digest: EvidenceDigest,
    authorization_expires_at: Option<Timestamp>,
    operation_mask: u8,
) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hash_length_prefixed(&mut hasher, source_id.as_bytes());
    hash_evidence_digest(&mut hasher, payload_digest);
    hasher.update(retrieved_at.unix_nanos().to_be_bytes());
    hash_length_prefixed(&mut hasher, terms_url.as_bytes());
    hash_evidence_digest(&mut hasher, terms_digest);
    hash_evidence_digest(&mut hasher, authorization_digest);
    match authorization_expires_at {
        Some(expiry) => {
            hasher.update([1]);
            hasher.update(expiry.unix_nanos().to_be_bytes());
        }
        None => hasher.update([0]),
    }
    hasher.update([operation_mask]);
    hasher.finalize().into()
}

fn hash_evidence_digest(hasher: &mut Sha256, digest: EvidenceDigest) {
    hasher.update([match digest.algorithm() {
        DigestAlgorithm::Sha256 => 1,
        DigestAlgorithm::Blake3 => 2,
    }]);
    hasher.update(digest.bytes());
}

fn hash_length_prefixed(hasher: &mut Sha256, value: &[u8]) {
    hasher.update(u64::try_from(value.len()).unwrap_or(u64::MAX).to_be_bytes());
    hasher.update(value);
}

fn hex_digest(bytes: [u8; 32]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn test_catalog_config(
    location: market_squawk_platform::CatalogLocation,
) -> Result<CatalogConfig, CatalogError> {
    CatalogConfig::try_new(
        location,
        Duration::from_millis(750),
        CatalogLimit::new(32)?,
        CatalogResultLimits::try_new(1024 * 1024, 8 * 1024 * 1024)?,
    )
}

fn count_published_objects(root: &std::path::Path) -> Result<usize, std::io::Error> {
    let objects = root.join("objects/sha256");
    let mut count = 0_usize;
    for prefix in std::fs::read_dir(objects)? {
        let prefix = prefix?;
        if prefix.file_type()?.is_dir() {
            count = count.saturating_add(std::fs::read_dir(prefix.path())?.count());
        }
    }
    Ok(count)
}
