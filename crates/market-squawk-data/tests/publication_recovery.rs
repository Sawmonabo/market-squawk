use std::error::Error;
use std::num::{NonZeroU16, NonZeroU32, NonZeroU64};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use market_squawk_data::{
    AnalyticalDataService, AnalyticalManifestCatalog, CatalogAuthority, CatalogConfig,
    CatalogError, CatalogLimit, CatalogResultLimits, CompactionRequest, DatasetId,
    DatasetManifestRef, IngestError, IngestIdentity, ObjectStoreConfig, ParquetObjectStore,
    QueryArtifactReservationInput, QueryError, QueryLimits, QueryRequest, QueryResult,
    ResearchArrowBatch, ResearchIngestService, ResearchQueryEngine, RightsDecisionInput,
    Sha256Digest, SourceOperation, extraction_batch_digest,
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

#[tokio::test]
async fn publication_is_content_addressed_idempotent_and_recovers_orphans() -> TestResult {
    let directory = tempfile::tempdir()?;
    let paths = LocalPaths::prepare(directory.path().join("market-squawk"))?;
    let store = ParquetObjectStore::open(
        paths.artifacts()?.clone(),
        ObjectStoreConfig::try_new(1024 * 1024, 2, Duration::from_secs(60))?,
    )?;
    let schema = Schema::new(vec![Field::new("value", DataType::Int64, false)]);
    let batch = RecordBatch::try_new(
        schema.into(),
        vec![std::sync::Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef],
    )?;
    let first = store.publish(&batch, &CancellationToken::new()).await?;
    let repeated = store.publish(&batch, &CancellationToken::new()).await?;
    assert_eq!(first, repeated);
    assert!(store.verify(&first)?);

    let report = store
        .collect_orphans(&[], first.created_at().checked_add_nanos(59_000_000_000)?)
        .await?;
    assert_eq!(report.quarantined(), 0);
    assert!(store.verify(&first)?);
    drop(store);

    let store = ParquetObjectStore::open(
        paths.artifacts()?.clone(),
        ObjectStoreConfig::try_new(1024 * 1024, 2, Duration::from_secs(60))?,
    )?;
    let report = store
        .collect_orphans(&[], first.created_at().checked_add_nanos(61_000_000_000)?)
        .await?;
    assert_eq!(report.quarantined(), 1);
    assert_eq!(report.deleted(), 0);
    let report = store
        .collect_orphans(&[], first.created_at().checked_add_nanos(122_000_000_000)?)
        .await?;
    assert_eq!(report.deleted(), 1);
    Ok(())
}

#[tokio::test]
async fn cancelled_publication_never_exposes_a_final_object() -> TestResult {
    let directory = tempfile::tempdir()?;
    let paths = LocalPaths::prepare(directory.path().join("market-squawk"))?;
    let store = ParquetObjectStore::open(
        paths.artifacts()?.clone(),
        ObjectStoreConfig::try_new(1024 * 1024, 32, Duration::from_secs(60))?,
    )?;
    let batch = RecordBatch::try_new(
        Schema::new(vec![Field::new("value", DataType::Int64, false)]).into(),
        vec![std::sync::Arc::new(Int64Array::from(vec![1])) as ArrayRef],
    )?;
    let cancellation = CancellationToken::new();
    cancellation.cancel();
    assert!(store.publish(&batch, &cancellation).await.is_err());
    assert!(store.published_objects()?.is_empty());
    Ok(())
}

#[tokio::test]
async fn recovery_waits_for_the_in_flight_publication_lease() -> TestResult {
    let directory = tempfile::tempdir()?;
    let paths = LocalPaths::prepare(directory.path().join("market-squawk"))?;
    let store = Arc::new(ParquetObjectStore::open(
        paths.artifacts()?.clone(),
        ObjectStoreConfig::try_new(1024 * 1024, 2, Duration::from_secs(60))?,
    )?);
    let batch = RecordBatch::try_new(
        Schema::new(vec![Field::new("value", DataType::Int64, false)]).into(),
        vec![Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef],
    )?;
    let cancellation = CancellationToken::new();
    let lease = store.begin_publication(&cancellation).await?;
    let published = store
        .publish_under_lease(&batch, &cancellation, &lease)
        .await?;

    let barrier = Arc::new(tokio::sync::Barrier::new(2));
    let recovery_barrier = Arc::clone(&barrier);
    let recovery_store = Arc::clone(&store);
    let recovery_now = published.created_at().checked_add_nanos(61_000_000_000)?;
    let referenced = [published.content_hash()];
    let recovery = tokio::spawn(async move {
        recovery_barrier.wait().await;
        recovery_store
            .collect_orphans(&referenced, recovery_now)
            .await
    });
    barrier.wait().await;
    tokio::task::yield_now().await;
    assert!(!recovery.is_finished());
    assert!(store.verify(&published)?);

    drop(lease);

    let report = recovery.await??;
    assert_eq!(report.quarantined(), 0);
    assert!(store.verify(&published)?);
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
    let service = AnalyticalDataService::open(
        CatalogAuthority::open(catalog_config.clone())?,
        AnalyticalManifestCatalog::open(&location, 8)?,
        paths.artifacts()?.clone(),
        store_config,
    )?;
    let store = service.object_store();
    let manifest = DatasetManifestRef::try_new(
        DatasetId::try_from("authorized-query-result")?,
        1,
        Sha256Digest::new([41; 32]),
    )?;
    let batch = RecordBatch::try_new(
        Schema::new(vec![Field::new("value", DataType::Int64, false)]).into(),
        vec![Arc::new(Int64Array::from_iter_values(0..100_000)) as ArrayRef],
    )?;
    let limits = QueryLimits::try_new(
        100_000,
        4 * 1024 * 1024,
        64 * 1024 * 1024,
        1,
        128,
        128,
        Duration::from_secs(5),
    )?;
    let request = QueryRequest::try_new(manifest.clone(), "SELECT value FROM observations")?;
    let wall_nanos = i64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos())?;
    let expires_at = Timestamp::from_unix_nanos(wall_nanos).checked_add_nanos(120_000_000_000)?;
    let owner = SourceIdentifier::try_from("research-session-1")?;
    assert!(matches!(
        service.reserve_query_artifact(QueryArtifactReservationInput::try_new(
            owner.clone(),
            request.artifact_identity(&limits),
            limits.max_bytes(),
            Timestamp::from_unix_nanos(i64::MAX),
        )?),
        Err(IngestError::Catalog(CatalogError::QueryArtifactExpired))
    ));
    let reservation = service.reserve_query_artifact(QueryArtifactReservationInput::try_new(
        owner.clone(),
        request.artifact_identity(&limits),
        limits.max_bytes(),
        expires_at,
    )?)?;
    let publisher = service.query_artifact_publisher();
    let engine = ResearchQueryEngine::from_pinned_batches(manifest, "observations", vec![batch])?
        .with_artifact_publisher(Arc::clone(&store), publisher);
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
        service.recover_orphans(before_expiry).await?.quarantined(),
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
    assert_eq!(restarted.recover_orphans(expired).await?.quarantined(), 1);
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
    let service = AnalyticalDataService::open(
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

    let newest = store
        .published_objects()?
        .into_iter()
        .map(|object| object.created_at())
        .max()
        .ok_or("missing published object")?;
    let report = store
        .collect_orphans(&[], newest.checked_add_nanos(61_000_000_000)?)
        .await?;
    assert!(report.quarantined() > 0);
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
                CancellationToken::new(),
            )
            .await,
        Err(QueryError::Artifact(_))
    ));
    Ok(())
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
