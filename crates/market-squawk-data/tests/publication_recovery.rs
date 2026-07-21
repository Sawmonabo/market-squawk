use std::error::Error;
use std::num::{NonZeroU16, NonZeroU32, NonZeroU64};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use market_squawk_data::{
    AnalyticalDataService, AnalyticalManifestCatalog, CatalogAuthority, CatalogConfig,
    CatalogError, CatalogLimit, CatalogResultLimits, CommittedDataset, CompactionRequest,
    IngestError, IngestIdentity, ObjectStoreConfig, ParquetStoreError,
    QueryArtifactReservationInput, QueryError, QueryLimits, QueryRequest, QueryResult,
    ResearchArrowBatch, ResearchIngestService, ResearchQueryEngine, RightsDecisionInput,
    SourceOperation, extraction_batch_digest,
};
use market_squawk_domain::{
    AuthorizationBasis, ChecksumCapability, CoverageDelay, DataQuality, DeliveryEvidence,
    DigestAlgorithm, EffectiveInterval, EvidenceDigest, ExactPayloadEvidence, MacroObservation,
    MetadataRevision, PayloadReference, ResearchContext, ResearchObservation, ResearchProvenance,
    ResearchProvenanceInput, ResearchTime, RevisionBoundPayloadEvidence, RevisionNumber,
    SchemaVersion, SequenceCapability, SourceId, SourceIdentifier, Timestamp,
};
use market_squawk_platform::LocalPaths;
use market_squawk_sources::{
    AuthorizationGrant, AuthorizationMode, AvailabilityEvidence as SourceAvailabilityEvidence,
    CoverageDomain, DiscoveryRequest, ExtractionBatch, ExtractionRecord, ExtractionRequest,
    FreshnessPolicy, HistoricalCapability, NetworkAccessPolicy, SourceCapabilities, SourceClass,
    SourceCoverage, SourceMetadata, SourceMetadataInput, SourceObject, SourceProtocolProfile,
};
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
    let store_config = ObjectStoreConfig::try_new(1024 * 1024, 32, Duration::from_secs(60))?;
    let service = AnalyticalDataService::initialize(
        CatalogAuthority::open(test_catalog_config(location.clone())?)?,
        AnalyticalManifestCatalog::open(&location, 8)?,
        paths.artifacts()?.clone(),
        store_config,
    )?;
    drop(service);
    drop(paths);

    let connection = rusqlite::Connection::open(location.path())?;
    connection.execute_batch(
        "INSERT INTO query_artifact_reservations(
             reservation_id, owner, request_algorithm, request_digest, max_bytes,
             requested_at_ns, expires_at_ns, state, bound_at_ns
         ) VALUES (
             '00000000-0000-0000-0000-000000000001', 'legacy-v4', 1, zeroblob(32), 1,
             1, 2, 'reserved', NULL
         );
         DROP TRIGGER analytical_artifact_root_authority_events_immutable_update;
         DROP TRIGGER analytical_artifact_root_authority_events_immutable_delete;
         DROP TRIGGER analytical_artifact_root_authority_events_append_guard;
         DROP TABLE analytical_artifact_root_authority_events;
         DELETE FROM schema_migrations WHERE version = 5;",
    )?;
    drop(connection);

    std::fs::rename(&artifact_path, local_root.join("legacy-artifacts"))?;
    std::fs::create_dir(&artifact_path)?;
    for _attempt in 0..2 {
        let replacement_paths = LocalPaths::prepare(&local_root)?;
        let replacement = AnalyticalDataService::open(
            CatalogAuthority::open(test_catalog_config(location.clone())?)?,
            AnalyticalManifestCatalog::open(&location, 8)?,
            replacement_paths.artifacts()?.clone(),
            store_config,
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
    drop(CatalogAuthority::open(catalog_config.clone())?);

    let connection = rusqlite::Connection::open(location.path())?;
    connection.execute_batch(
        "PRAGMA foreign_keys = ON;
         BEGIN;
         INSERT INTO sources VALUES ('v2-source', zeroblob(32), 1, 1);
         INSERT INTO source_revisions VALUES ('v2-source', zeroblob(32), '{}', 1);
         INSERT INTO source_rights VALUES (
             zeroblob(32), 'v2-source', 1, zeroblob(32), 1,
             'https://example.test/terms', 1, zeroblob(32), 1, zeroblob(32), NULL, 4, 1
         );
         INSERT INTO ingest_runs VALUES (
             '00000000-0000-0000-0000-000000000001', 'v2-artifact', 'v2-source',
             1, zeroblob(32), 'persist', zeroblob(32), 'succeeded', 1, 2
         );
         INSERT INTO artifacts VALUES (
             '00000000-0000-0000-0000-000000000002',
             '00000000-0000-0000-0000-000000000001',
             'objects/sha256/00/fixture.parquet', 1, zeroblob(32), 1, 2
         );
         DROP TABLE query_artifact_results;
         DROP TABLE query_artifact_reservations;
         DROP TABLE analytical_generation_objects;
         DROP TABLE analytical_generations;
         DROP TRIGGER analytical_artifact_root_authority_events_immutable_update;
         DROP TRIGGER analytical_artifact_root_authority_events_immutable_delete;
         DROP TRIGGER analytical_artifact_root_authority_events_append_guard;
         DROP TABLE analytical_artifact_root_authority_events;
         DELETE FROM schema_migrations WHERE version >= 3;
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
    let payload_digest = extraction_batch_digest(&batch)?;
    let rights = authority.admit_source_rights(RightsDecisionInput {
        source_id: source.source_id().clone(),
        payload_digest,
        retrieved_at: Timestamp::from_unix_nanos(15),
        terms_url: "https://example.test/terms/v1".to_owned(),
        terms_digest: digest(31),
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
        .ingest(reservation.clone(), batch.clone(), CancellationToken::new())
        .await?;
    let replay = service
        .ingest(reservation, batch, CancellationToken::new())
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
        terms_url: "https://example.test/terms/v1".to_owned(),
        terms_digest: digest(31),
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

async fn initialized_service_with_dataset(
    paths: &LocalPaths,
    catalog_config: CatalogConfig,
    store_config: ObjectStoreConfig,
) -> Result<(AnalyticalDataService, CommittedDataset), Box<dyn Error>> {
    let location = paths.catalog()?.clone();
    let authority = CatalogAuthority::open(catalog_config)?;
    let source = local_source()?;
    authority.register_source(&source, Timestamp::from_unix_nanos(10))?;
    let batch = extraction_batch()?;
    let payload_digest = extraction_batch_digest(&batch)?;
    let rights = authority.admit_source_rights(RightsDecisionInput {
        source_id: source.source_id().clone(),
        payload_digest,
        retrieved_at: Timestamp::from_unix_nanos(15),
        terms_url: "https://example.test/terms/v1".to_owned(),
        terms_digest: digest(31),
        authorization_evidence: digest(32),
        authorization_expires_at: Some(Timestamp::from_unix_nanos(i64::MAX)),
        permitted_operations: vec![SourceOperation::Persist],
    })?;
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
        NonZeroU32::new(1).ok_or("nonzero record limit")?,
        NonZeroU64::new(1024 * 1024).ok_or("nonzero byte limit")?,
        Timestamp::from_unix_nanos(1_000),
    )?;
    let payload = serde_json::to_vec(&macro_observation()?)?;
    let evidence = EvidenceDigest::new(DigestAlgorithm::Sha256, Sha256::digest(&payload).into());
    let record = ExtractionRecord::try_new(
        &request,
        SourceIdentifier::try_from("market-squawk-research-v1")?,
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
    Ok(ExtractionBatch::try_new(&request, vec![record])?)
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
            RevisionNumber::new(1)?,
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

fn local_source() -> Result<SourceMetadata, Box<dyn Error>> {
    let effective = EffectiveInterval::new(Timestamp::from_unix_nanos(0), None)?;
    Ok(SourceMetadata::try_new(SourceMetadataInput::new(
        SchemaVersion::CURRENT,
        SourceId::try_from("fred-local-fixture")?,
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
