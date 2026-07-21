use std::collections::HashSet;
use std::error::Error;
use std::num::{NonZeroU16, NonZeroU32, NonZeroU64};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::execution::memory_pool::GreedyMemoryPool;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::physical_plan::{ExecutionPlan, collect};
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

use super::super::ResearchQueryEngine;
use super::*;
use crate::{
    AnalyticalDataService, AnalyticalManifestCatalog, CatalogAuthority, CatalogConfig,
    CatalogLimit, CatalogResultLimits, DatasetId, DatasetManifestRef, IngestIdentity,
    ObjectStoreConfig, ParquetStoreError, QueryArtifactReservationInput, QueryLimits, QueryRequest,
    QueryResult, ResearchIngestService, RightsDecisionInput, Sha256Digest, SourceOperation,
    extraction_batch_digest,
};

type TestResult = Result<(), Box<dyn Error>>;

const ARTIFACT_QUERY: &str = "SELECT a.value FROM observations
     CROSS JOIN (VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9)) AS a(value)
     CROSS JOIN (VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9)) AS b(value)
     CROSS JOIN (VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9)) AS c(value)
     CROSS JOIN (VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9)) AS d(value)
     CROSS JOIN (VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9)) AS e(value)";

#[tokio::test]
async fn arbitrary_batches_cannot_attach_durable_publication_authority() -> TestResult {
    let (_directory, service, _pinned) = published_dataset_fixture().await?;
    let fabricated_manifest = DatasetManifestRef::try_new(
        DatasetId::try_from("fabricated-query-source")?,
        1,
        Sha256Digest::new([99; 32]),
    )?;
    let batch = RecordBatch::try_new(
        Schema::new(vec![Field::new("value", DataType::Int64, false)]).into(),
        vec![Arc::new(Int64Array::from(vec![1])) as ArrayRef],
    )?;
    let engine =
        ResearchQueryEngine::from_pinned_batches(fabricated_manifest, "observations", vec![batch])?;

    assert!(matches!(
        engine.with_artifact_publication(service.query_artifact_publication()),
        Err(QueryError::InvalidSource)
    ));
    Ok(())
}

#[tokio::test]
async fn pinned_io_is_joined_and_repeated_scans_share_admitted_metadata() -> TestResult {
    let _blocking_worker_serial = BlockingIoSupervisor::acquire_test_serial_guard().await;
    assert!(matches!(
        map_capture_error(ParquetStoreError::Cancelled),
        QueryError::Cancelled
    ));

    let mut range_file = tempfile::tempfile()?;
    std::io::Write::write_all(&mut range_file, &[7_u8; 4096])?;
    let cancellation = CancellationToken::new();
    let supervisor = BlockingIoSupervisor::new(cancellation.clone());
    let mut barrier = supervisor.install_test_range_barrier()?;
    let range_pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(8192));
    let read_supervisor = supervisor.clone();
    let read = tokio::spawn(read_exact_range(
        Arc::new(Mutex::new(range_file)),
        0..4096,
        range_pool,
        read_supervisor,
    ));
    barrier.wait_until_entered().await?;
    assert_eq!(
        BlockingIoSupervisor::globally_available(),
        BlockingIoSupervisor::global_limit() - 1
    );
    cancellation.cancel();
    let boundary = tokio::time::timeout(Duration::from_millis(50), read).await;
    assert_eq!(
        BlockingIoSupervisor::globally_available(),
        BlockingIoSupervisor::global_limit() - 1
    );
    barrier.release()?;
    supervisor.drain().await;
    assert!(
        matches!(boundary, Ok(Ok(Err(_)))),
        "cancelled range read did not return while its blocking worker remained held"
    );
    assert_eq!(supervisor.active(), 0);
    assert_eq!(
        BlockingIoSupervisor::globally_available(),
        BlockingIoSupervisor::global_limit()
    );

    let (_directory, service, pinned) = published_dataset_fixture().await?;
    let engine = ResearchQueryEngine::from_pinned_dataset(
        pinned,
        "pinned",
        service.object_store(),
        CancellationToken::new(),
    )
    .await?;
    let object_store_url = ObjectStoreUrl::parse(PINNED_STORE_URL)?;

    const REJECTED_LIMIT: usize = 20 * 1024;
    let rejected_pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(REJECTED_LIMIT));
    let rejected_registry = Arc::new(PinnedObjectStoreRegistry::default());
    let rejected_runtime = RuntimeEnvBuilder::new()
        .with_memory_pool(Arc::clone(&rejected_pool))
        .with_object_store_registry(rejected_registry.clone())
        .build_arc()?;
    let rejected_context = SessionContext::new_with_config_rt(Default::default(), rejected_runtime);
    let rejected_input = MemoryConsumer::new("market-squawk-query-input").register(&rejected_pool);
    rejected_input.try_grow(engine.source.retained_bytes()?)?;
    let rejected_prior_bytes = rejected_pool.reserved();
    let rejected_supervisor = BlockingIoSupervisor::new(CancellationToken::new());
    let rejected = engine
        .source
        .register(
            &rejected_context,
            &engine.table_name,
            &rejected_supervisor,
            &rejected_input,
            &rejected_registry,
            u64::try_from(REJECTED_LIMIT)?,
        )
        .await;
    assert!(
        matches!(
            rejected,
            Err(QueryError::MemoryLimitExceeded { limit })
                if limit == u64::try_from(REJECTED_LIMIT)?
        ),
        "unexpected rejected admission: {rejected:?}"
    );
    assert_eq!(rejected_pool.reserved(), rejected_prior_bytes);
    assert!(!rejected_context.table_exist("rejected")?);
    assert!(
        rejected_context
            .runtime_env()
            .object_store(&object_store_url)
            .is_err()
    );

    const ACCEPTED_LIMIT: usize = 2 * 1024 * 1024;
    let tight_pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(ACCEPTED_LIMIT));
    let registry = Arc::new(PinnedObjectStoreRegistry::default());
    let runtime = RuntimeEnvBuilder::new()
        .with_memory_pool(Arc::clone(&tight_pool))
        .with_object_store_registry(registry.clone())
        .build_arc()?;
    let context = SessionContext::new_with_config_rt(Default::default(), runtime);
    let input_memory = MemoryConsumer::new("market-squawk-query-input").register(&tight_pool);
    input_memory.try_grow(engine.source.retained_bytes()?)?;
    let scan_supervisor = BlockingIoSupervisor::new(CancellationToken::new());
    engine
        .source
        .register(
            &context,
            &engine.table_name,
            &scan_supervisor,
            &input_memory,
            &registry,
            u64::try_from(ACCEPTED_LIMIT)?,
        )
        .await?;
    assert!(context.table_exist("pinned")?);
    assert!(
        context
            .runtime_env()
            .object_store(&object_store_url)
            .is_ok()
    );

    let dataframe = context
        .sql(
            "SELECT left_side.source_id FROM pinned left_side \
             JOIN pinned right_side ON left_side.source_id = right_side.source_id LIMIT 1",
        )
        .await?;
    let physical = dataframe.create_physical_plan().await?;
    let mut source_identities = Vec::new();
    collect_source_identities(&physical, &mut source_identities);
    assert_eq!(source_identities.len(), 2);
    assert_eq!(
        source_identities.into_iter().collect::<HashSet<_>>().len(),
        1
    );
    let retained_before_execute = tight_pool.reserved();
    let retained_physical = Arc::clone(&physical);
    let results = collect(physical, context.task_ctx()).await?;
    assert_eq!(results.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
    assert_eq!(tight_pool.reserved(), retained_before_execute);
    drop(retained_physical);

    let cancellation_plan = context
        .sql("SELECT source_id FROM pinned")
        .await?
        .create_physical_plan()
        .await?;
    let source = first_immutable_source(&cancellation_plan)
        .ok_or("custom immutable source plan was not installed")?;
    let retained_before_cancelled_execute = tight_pool.reserved();
    let mut scan_barrier = scan_supervisor.install_test_range_barrier()?;
    let cancelled_stream = source.execute(0, context.task_ctx())?;
    scan_barrier.wait_until_entered().await?;
    drop(cancelled_stream);
    scan_barrier.release()?;
    scan_supervisor.drain().await;
    assert_eq!(scan_supervisor.active(), 0);
    assert_eq!(tight_pool.reserved(), retained_before_cancelled_execute);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn durable_query_artifact_bind_has_deterministic_cancellation_precedence() -> TestResult {
    let _blocking_worker_serial = BlockingIoSupervisor::acquire_test_serial_guard().await;
    for (checkpoint, expect_receipt) in [
        (
            crate::catalog::QueryArtifactBindCheckpoint::BeforeCommit,
            false,
        ),
        (
            crate::catalog::QueryArtifactBindCheckpoint::AfterCommit,
            true,
        ),
    ] {
        let (_directory, service, pinned) = published_dataset_fixture().await?;
        let limits = QueryLimits::try_new(
            100_000,
            4 * 1024 * 1024,
            64 * 1024 * 1024,
            1,
            512,
            512,
            Duration::from_secs(5),
        )?;
        let request = QueryRequest::try_new(pinned.manifest().clone(), ARTIFACT_QUERY)?;
        let wall_nanos = i64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos())?;
        let reservation = service
            .reserve_query_artifact(
                QueryArtifactReservationInput::try_new(
                    SourceIdentifier::try_from("bind-precedence-owner")?,
                    request.artifact_identity(&limits),
                    limits.max_bytes(),
                    Timestamp::from_unix_nanos(wall_nanos).checked_add_nanos(120_000_000_000)?,
                )?,
                &CancellationToken::new(),
            )
            .await?;
        let publication = service.query_artifact_publication();
        let mut barrier = publication.install_test_bind_barrier(checkpoint)?;
        let engine = ResearchQueryEngine::from_pinned_dataset(
            pinned,
            "observations",
            service.object_store(),
            CancellationToken::new(),
        )
        .await?
        .with_artifact_publication(publication)?;
        let cancellation = CancellationToken::new();
        let query_cancellation = cancellation.clone();
        let query = tokio::spawn(async move {
            engine
                .query(
                    request.with_artifact_reservation(reservation),
                    limits,
                    query_cancellation,
                )
                .await
        });
        barrier.wait_until_entered().await?;
        cancellation.cancel();
        barrier.release()?;
        let result = query.await?;
        assert_eq!(
            matches!(result, Ok(QueryResult::Artifact { .. })),
            expect_receipt
        );
        assert_eq!(
            matches!(result, Err(QueryError::Cancelled)),
            !expect_receipt
        );
    }

    let (_directory, service, pinned) = published_dataset_fixture().await?;
    let limits = QueryLimits::try_new(
        100_000,
        4 * 1024 * 1024,
        64 * 1024 * 1024,
        1,
        512,
        512,
        Duration::from_secs(5),
    )?
    .with_test_bind_precommit_deadline(tokio::time::Instant::now());
    let request = QueryRequest::try_new(pinned.manifest().clone(), ARTIFACT_QUERY)?;
    let wall_nanos = i64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos())?;
    let reservation = service
        .reserve_query_artifact(
            QueryArtifactReservationInput::try_new(
                SourceIdentifier::try_from("deadline-precedence-owner")?,
                request.artifact_identity(&limits),
                limits.max_bytes(),
                Timestamp::from_unix_nanos(wall_nanos).checked_add_nanos(120_000_000_000)?,
            )?,
            &CancellationToken::new(),
        )
        .await?;
    let publication = service.query_artifact_publication();
    let mut barrier = publication
        .install_test_bind_barrier(crate::catalog::QueryArtifactBindCheckpoint::BeforeCommit)?;
    let engine = ResearchQueryEngine::from_pinned_dataset(
        pinned,
        "observations",
        service.object_store(),
        CancellationToken::new(),
    )
    .await?
    .with_artifact_publication(publication)?;
    let query = tokio::spawn(async move {
        engine
            .query(
                request.with_artifact_reservation(reservation),
                limits,
                CancellationToken::new(),
            )
            .await
    });
    barrier.wait_until_entered().await?;
    barrier.release()?;
    let result = query.await?;
    assert!(
        matches!(result, Err(QueryError::DeadlineExceeded)),
        "deadline elapsed at the precommit barrier but the bind outcome was {result:?}"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelled_query_artifact_writer_retains_admission_until_reaped() -> TestResult {
    let _blocking_worker_serial = BlockingIoSupervisor::acquire_test_serial_guard().await;
    let available_before = BlockingIoSupervisor::globally_available();
    assert_eq!(available_before, BlockingIoSupervisor::global_limit());
    let (_directory, service, pinned) = published_dataset_fixture().await?;
    let limits = QueryLimits::try_new(
        100_000,
        4 * 1024 * 1024,
        64 * 1024 * 1024,
        1,
        512,
        512,
        Duration::from_secs(5),
    )?;
    let request = QueryRequest::try_new(pinned.manifest().clone(), ARTIFACT_QUERY)?;
    let wall_nanos = i64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos())?;
    let reservation = service
        .reserve_query_artifact(
            QueryArtifactReservationInput::try_new(
                SourceIdentifier::try_from("held-writer-owner")?,
                request.artifact_identity(&limits),
                limits.max_bytes(),
                Timestamp::from_unix_nanos(wall_nanos).checked_add_nanos(120_000_000_000)?,
            )?,
            &CancellationToken::new(),
        )
        .await?;
    let publication = service.query_artifact_publication();
    let mut barrier = publication.install_test_writer_barrier()?;
    let engine = ResearchQueryEngine::from_pinned_dataset(
        pinned,
        "observations",
        service.object_store(),
        CancellationToken::new(),
    )
    .await?
    .with_artifact_publication(publication)?;
    let cancellation = CancellationToken::new();
    let query_cancellation = cancellation.clone();
    let query = tokio::spawn(async move {
        engine
            .query(
                request.with_artifact_reservation(reservation),
                limits,
                query_cancellation,
            )
            .await
    });
    barrier.wait_until_entered().await?;
    let memory_retained_before_cancel = barrier.memory_retained();
    let global_retained_before_cancel =
        BlockingIoSupervisor::globally_available() == available_before.saturating_sub(1);
    cancellation.cancel();
    let boundary = tokio::time::timeout(Duration::from_millis(50), query).await;
    let memory_retained_after_cancel = barrier.memory_retained();
    let global_retained_after_cancel =
        BlockingIoSupervisor::globally_available() == available_before.saturating_sub(1);
    barrier.release()?;
    tokio::time::timeout(Duration::from_secs(1), async {
        while barrier.memory_retained()
            || BlockingIoSupervisor::globally_available() != available_before
        {
            tokio::task::yield_now().await;
        }
    })
    .await?;
    assert!(
        matches!(boundary, Ok(Ok(Err(QueryError::Cancelled)))),
        "cancelled artifact query did not return while its writer remained held: {boundary:?}"
    );
    assert!(
        memory_retained_before_cancel
            && memory_retained_after_cancel
            && global_retained_before_cancel
            && global_retained_after_cancel,
        "held writer lost ownership: memory before={memory_retained_before_cancel}, memory after={memory_retained_after_cancel}, global before={global_retained_before_cancel}, global after={global_retained_after_cancel}"
    );
    Ok(())
}

async fn published_dataset_fixture()
-> Result<(tempfile::TempDir, AnalyticalDataService, PinnedDataset), Box<dyn Error>> {
    let directory = tempfile::tempdir()?;
    let paths = LocalPaths::prepare(directory.path().join("market-squawk"))?;
    let location = paths.catalog()?.clone();
    let authority = CatalogAuthority::open(CatalogConfig::try_new(
        location.clone(),
        Duration::from_millis(750),
        CatalogLimit::new(32)?,
        CatalogResultLimits::try_new(1024 * 1024, 8 * 1024 * 1024)?,
    )?)?;
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
    let committed = service
        .ingest(reservation, batch, CancellationToken::new())
        .await?;
    let pinned = committed.pinned().clone();
    Ok((directory, service, pinned))
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

fn collect_source_identities(plan: &Arc<dyn ExecutionPlan>, identities: &mut Vec<usize>) {
    if let Some(source) = plan.downcast_ref::<scan::ImmutableSourcePlan>() {
        identities.push(source.storage_identity());
    }
    for child in plan.children() {
        collect_source_identities(child, identities);
    }
}

fn first_immutable_source(plan: &Arc<dyn ExecutionPlan>) -> Option<&scan::ImmutableSourcePlan> {
    if let Some(source) = plan.downcast_ref::<scan::ImmutableSourcePlan>() {
        return Some(source);
    }
    plan.children()
        .into_iter()
        .find_map(|child| first_immutable_source(child))
}
