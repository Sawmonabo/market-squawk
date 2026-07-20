use std::error::Error;
use std::num::{NonZeroU16, NonZeroU32, NonZeroU64};
use std::time::Duration;

use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use market_squawk_data::{
    AnalyticalDataService, AnalyticalManifestCatalog, CatalogAuthority, CatalogConfig,
    CatalogLimit, CatalogResultLimits, CompactionRequest, IngestIdentity, ObjectStoreConfig,
    ParquetObjectStore, QueryLimits, QueryRequest, QueryResult, ResearchArrowBatch,
    ResearchIngestService, ResearchQueryEngine, RightsDecisionInput, SourceOperation,
    extraction_batch_digest,
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

    let report =
        store.collect_orphans(&[], first.created_at().checked_add_nanos(61_000_000_000)?)?;
    assert_eq!(report.quarantined(), 1);
    assert_eq!(report.deleted(), 0);
    let report =
        store.collect_orphans(&[], first.created_at().checked_add_nanos(122_000_000_000)?)?;
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
