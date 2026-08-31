// Rust #159105: this macOS-only test-link diagnostic is caused by the measured
// `__eh_frame` exceeding arm64 compact-unwind's 24-bit offset range.
#![allow(linker_messages)]

use std::error::Error;
use std::num::{NonZeroU16, NonZeroU32, NonZeroU64, NonZeroUsize};
use std::str::FromStr as _;
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use arrow::array::{Array as _, BinaryArray, StringArray};
use bytes::Bytes;
use chrono::{DateTime, Utc};
use market_squawk_data::{
    AnalyticalBackupLimits, AnalyticalBackupLocation, AnalyticalDataService,
    AnalyticalFundNavReadLimit, AnalyticalFundNavReadRequest, AnalyticalMacroLatestKnownRequest,
    AnalyticalMacroSeriesAllowlist, AnalyticalManifestCatalog, AnalyticalObservationReadRequest,
    AnalyticalObservationTemplate, AnalyticalReadError, AnalyticalReadLimit, AnalyticalRestoreMode,
    AnalyticalRestoreTarget, CanonicalMarketBarHistoryRequest, CatalogAuthority, CatalogConfig,
    CatalogError, CatalogLimit, CatalogResultLimits, ChronologicalSplitPolicy, CommittedDataset,
    CompactionRequest, ComponentAdjustmentEvidence, ComponentKind, ComponentScope,
    ComponentSelector, ComponentValue, CorporateActionAdjustment, CorporateActionLimits,
    CorporateActionPlan, CorporateActionPolicy, CorporateActionSensitivity, DatasetBuildError,
    DatasetBuildInputs, DatasetBuildLimits, DatasetBuildPolicy, DatasetBuildPrecommitAuthority,
    DatasetBuildRequest, DatasetBuilder, DatasetId, DatasetManifestRef, DatasetOutputAuthorization,
    DatasetSchemaRegistry, FEATURE_LABEL_RETURN_UNIT, FeatureDatasetProductContract,
    FeatureDatasetProductionError, FeatureDatasetProductionProofV1,
    FeatureDatasetProductionPublicationDisposition, FeatureDatasetProductionPublisher,
    FeatureLabelComponentInput, FeatureLabelComponentSpec, ForecastDatasetReadLimits,
    FundNavDateRange, IngestError, IngestIdentity, ManifestCatalogError,
    MarketDataInstrumentSynchronization, MarketHistorySelectionPolicy, MissingValuePolicy,
    ObjectStoreConfig, ObservationFamilyKey, ParquetStoreError, PointInTimeLimits,
    PointInTimePolicy, PointInTimeRevisionMode, ProviderMacroPlanChunkInput,
    ProviderMacroPlanPublicationInput, ProviderMacroPlanSemantics,
    ProviderMarketEventPublicationKind, ProviderPublicationInput, PythonDatasetCatalogError,
    QueryArtifactReservationInput, QueryError, QueryLimits, QueryRequest, QueryResult,
    ResearchArrowBatch, ResearchIngestService, ResearchQueryEngine, ResearchUse,
    ResearchUseGrantInput, ResearchUseLimits, ResearchUseRequest, ResearchUseSet, RightsBasis,
    RightsDecisionInput, SecResearchDisposition, SecResearchFamily, SecResearchReadError,
    SecResearchReadRequest, Sha256Digest, SourceOperation, UniverseId, UniverseLimits,
    UniverseMembership, extraction_provider_payload_digest,
    provider_market_event_publication_digest,
};
use market_squawk_domain::{
    AggressorSide, AssetClass, AuthorizationBasis,
    AvailabilityEvidence as DomainAvailabilityEvidence, BarTimeSemantics, BarTimestampBasis,
    CanonicalStateDigest, CanonicalizationRule, ChecksumCapability, CompanyIdentityObservation,
    CompanyIdentityObservationInput, CompanyIdentitySurface, ConnectionGeneration, CoverageDelay,
    CoverageStatus, Currency, DataQuality, DecodedLiveProvenanceInput, DeliveryEvidence,
    DigestAlgorithm, EffectiveInterval, EvidenceDigest, ExactPayloadEvidence, FilingObservation,
    FundNavCompleteness, FundNavCorrectionState, FundNavDisposition, FundNavEntitlementEvidence,
    FundNavFinality, FundNavLineage, FundNavNativeSchema, FundNavObservation,
    FundNavObservationInput, FundNavRevisionEvidence, FundNavValuationBasis, FundNavValue,
    InstrumentId, LiveEventClass, LiveEvidenceBinding, LiveProvenance, MacroObservation,
    MarketBarAdjustment, MarketBarObservation, MarketBarSessionEvidence, MarketBarSessionKind,
    MarketDataInstrumentDefinition, MarketDataInstrumentDefinitionInput, MarketEvent,
    MetadataRevision, Money, PayloadReference, PriceTicks, ProviderChannel,
    ProviderIdentityEvidence, ProviderIdentityRecord, ProviderIdentityRecordInput,
    ProviderInstrumentId, ProviderProduct, QuantityLots, ResearchContext, ResearchObservation,
    ResearchProvenance, ResearchProvenanceInput, ResearchTemporalCoordinate, ResearchTime,
    RevisionBoundPayloadEvidence, RevisionNumber, RuleVersion, SchemaVersion, SequenceCapability,
    SourceId, SourceIdentifier, Timestamp, TradeEvent, UniverseMembershipObservation, VenueId,
    VenueMapping, VenueSymbol, feature_dataset_macro_components_v1,
};
use market_squawk_platform::{
    LocalPaths, RawCaptureRecord, ResearchObjectControl, ResearchObjectControlError,
    ResearchObjectControlPoint, SealedResearchJournalStore, SealedResearchJournalStoreError,
};
use market_squawk_sources::{
    ApiEndpointRule, AuthorizationGrant, AuthorizationMode,
    AvailabilityEvidence as SourceAvailabilityEvidence, BackoffPolicy, BudgetScope,
    CanonicalObservationPayload, CompleteMarketBarHistoryV1, CoverageDomain, CoverageTopology,
    DiscoveryRequest, EndpointPolicy, ExtractionBatch, ExtractionRecord, ExtractionRequest,
    ExtractionRevisionEvidence, ExtractionRevisionPlan, FreshnessPolicy, HistoricalCapability,
    HttpRequestBounds, InstrumentCoverage, NetworkAccessPolicy, ObservedProviderOrder, PathScope,
    ProviderBudgetPolicy, ProviderCaptureMaterial, ProviderCapturePageReceipt,
    ProviderCaptureSemanticBinding, ProviderCaptureSetReceipt, ProviderCaptureTerminalDisposition,
    ProviderEventMicrobatchMaterial, ProviderMarketEventBatch,
    ProviderMarketEventNativeLineageBatch, ProviderNativeLineageBatchBuilder,
    ProviderNativeLineageImplementation, SealedProviderCaptureBinding,
    SealedProviderEventMicrobatchBinding, SealedProviderPublicationBinding, SourceCapabilities,
    SourceClass, SourceCoverage, SourceMetadata, SourceMetadataInput, SourceObject,
    SourceObjectCaptureIdentity, SourceProtocolProfile,
};
use rusqlite::params;
use rust_decimal::Decimal;
use sha2::{Digest as _, Sha256};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

type TestResult = Result<(), Box<dyn Error>>;

const ARTIFACT_QUERY: &str = "SELECT a.value FROM observations
     CROSS JOIN (VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9)) AS a(value)
     CROSS JOIN (VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9)) AS b(value)
     CROSS JOIN (VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9)) AS c(value)
     CROSS JOIN (VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9)) AS d(value)
     CROSS JOIN (VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9)) AS e(value)";

const MACRO_SNAPSHOT_SERIES: [&str; 11] = [
    "federal-reserve-board:h15:H15%2FH15%2FRIFLGFCM01_N.B",
    "federal-reserve-board:h15:H15%2FH15%2FRIFLGFCM03_N.B",
    "federal-reserve-board:h15:H15%2FH15%2FRIFLGFCM06_N.B",
    "federal-reserve-board:h15:H15%2FH15%2FRIFLGFCY01_N.B",
    "federal-reserve-board:h15:H15%2FH15%2FRIFLGFCY02_N.B",
    "federal-reserve-board:h15:H15%2FH15%2FRIFLGFCY03_N.B",
    "federal-reserve-board:h15:H15%2FH15%2FRIFLGFCY05_N.B",
    "federal-reserve-board:h15:H15%2FH15%2FRIFLGFCY07_N.B",
    "federal-reserve-board:h15:H15%2FH15%2FRIFLGFCY10_N.B",
    "federal-reserve-board:h15:H15%2FH15%2FRIFLGFCY20_N.B",
    "federal-reserve-board:h15:H15%2FH15%2FRIFLGFCY30_N.B",
];

const COMPLETE_HISTORY_DAY_NS: i64 = 86_400_000_000_000;
const COMPLETE_HISTORY_FIRST_BAR_NS: i64 = 10 * COMPLETE_HISTORY_DAY_NS;
const COMPLETE_HISTORY_SECOND_BAR_NS: i64 = 11 * COMPLETE_HISTORY_DAY_NS;
const COMPLETE_HISTORY_REQUEST_END_NS: i64 = 12 * COMPLETE_HISTORY_DAY_NS;
const COMPLETE_HISTORY_RECEIVED_AT_NS: i64 = 30 * COMPLETE_HISTORY_DAY_NS;
const COMPLETE_HISTORY_SHORT_RECEIVED_AT_NS: i64 = 31 * COMPLETE_HISTORY_DAY_NS;
const COMPLETE_HISTORY_NEWER_RECEIVED_AT_NS: i64 = 32 * COMPLETE_HISTORY_DAY_NS;

#[derive(Debug, Default)]
struct RejectDatasetPublication {
    committed: AtomicBool,
}

#[derive(Debug)]
struct AllowProviderEventPublication;

#[derive(Debug, Default)]
struct CancelDuringRawVerification {
    verification_chunks: AtomicUsize,
}

impl ResearchObjectControl for CancelDuringRawVerification {
    fn checkpoint(
        &self,
        point: ResearchObjectControlPoint,
    ) -> Result<(), ResearchObjectControlError> {
        if matches!(
            point,
            ResearchObjectControlPoint::BeforeVerificationChunk { .. }
        ) && self.verification_chunks.fetch_add(1, Ordering::AcqRel) >= 1
        {
            Err(ResearchObjectControlError::Cancelled)
        } else {
            Ok(())
        }
    }
}

impl market_squawk_data::IngestPrecommitAuthority for AllowProviderEventPublication {
    fn validate_precommit(&self) -> Result<(), IngestError> {
        Ok(())
    }
}

impl DatasetBuildPrecommitAuthority for RejectDatasetPublication {
    fn validate_precommit(&self) -> Result<(), DatasetBuildError> {
        Err(DatasetBuildError::PublicationAuthorityRevoked)
    }

    fn commit_succeeded(&self) {
        self.committed.store(true, Ordering::Release);
    }
}

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
             1, zeroblob(32), 'persist', ?1, 'reserved', 1, NULL
         )",
        [rights_id.as_slice()],
    )?;
    connection.execute_batch(
        "INSERT INTO artifacts
         (artifact_id, run_id, publication_ordinal, relative_reference,
          content_algorithm, content_digest, size_bytes, created_at_ns)
         VALUES (
             '00000000-0000-0000-0000-000000000002',
             '00000000-0000-0000-0000-000000000001', 0,
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

    let repeated_request = QueryRequest::try_new(committed.manifest().clone(), ARTIFACT_QUERY)?;
    let repeated_reservation = service
        .reserve_query_artifact(
            QueryArtifactReservationInput::try_new(
                owner.clone(),
                repeated_request.artifact_identity(&limits),
                limits.max_bytes(),
                expires_at,
            )?,
            &CancellationToken::new(),
        )
        .await?;
    let repeated = engine
        .query(
            repeated_request.with_artifact_reservation(repeated_reservation),
            limits,
            CancellationToken::new(),
        )
        .await?;
    let QueryResult::Artifact {
        object: repeated_object,
        artifact: repeated_artifact,
        ownership: repeated_ownership,
    } = repeated
    else {
        return Err("expected repeated authorized artifact result".into());
    };
    assert_eq!(
        repeated_object.relative_reference(),
        object.relative_reference()
    );
    assert_eq!(repeated_artifact.artifact_id(), artifact.artifact_id());
    service
        .query_artifact_publication()
        .read_verified_bytes(
            &repeated_object,
            &repeated_artifact,
            &repeated_ownership,
            4 * 1024 * 1024,
            tokio::time::Instant::now() + Duration::from_secs(5),
            &CancellationToken::new(),
        )
        .await?;
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
async fn rights_bound_ingest_replays_generation_and_company_identity() -> TestResult {
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
    let payload_digest = extraction_provider_payload_digest(&batch);
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
    let analytical_dataset = DatasetId::try_from(batch.request().object().dataset().as_str())?;
    let company = company_identity(source.source_id().clone(), payload_digest, "Example", 200)?;

    let first = service
        .ingest_with_revision_plan_and_company_identity(
            reservation.clone(),
            analytical_dataset.clone(),
            batch.clone(),
            revisions.clone(),
            company.clone(),
            CancellationToken::new(),
        )
        .await?;
    assert_eq!(first.manifest().schema_version().get(), 3);
    let search = service.company_identities().search(
        "example",
        4,
        Instant::now() + Duration::from_secs(1),
        &CancellationToken::new(),
    )?;
    assert_eq!(search.matches().len(), 1);
    assert_eq!(search.matches()[0].observation(), &company);
    let replay = service
        .ingest_with_revision_plan_and_company_identity(
            reservation.clone(),
            analytical_dataset.clone(),
            batch.clone(),
            revisions.clone(),
            company_identity(source.source_id().clone(), payload_digest, "Example", 201)?,
            CancellationToken::new(),
        )
        .await?;
    assert_eq!(first, replay);
    let conflict = service
        .ingest_with_revision_plan_and_company_identity(
            reservation,
            analytical_dataset.clone(),
            batch,
            revisions,
            company_identity(
                source.source_id().clone(),
                payload_digest,
                "Conflicting",
                201,
            )?,
            CancellationToken::new(),
        )
        .await;
    assert!(matches!(
        conflict,
        Err(IngestError::Catalog(CatalogError::EvidenceConflict))
    ));
    let raw_store = paths.sealed_research_journal_store()?;
    let rejected_input = provider_macro_plan_input(&raw_store, analytical_dataset.clone())?;
    let macro_payload_digest = rejected_input.publication_digest();
    let macro_identity = IngestIdentity::try_new(
        source.source_id().clone(),
        macro_payload_digest,
        SourceOperation::Persist,
        "fred:gdp:three-input-plan:v1",
    )?;
    let macro_reservation = service
        .reserve_source_ingest(
            &source,
            Timestamp::from_unix_nanos(10),
            RightsDecisionInput {
                source_id: source.source_id().clone(),
                payload_digest: macro_payload_digest,
                retrieved_at: Timestamp::from_unix_nanos(300),
                basis: RightsBasis::reviewed_terms("https://example.test/terms/v1", digest(31))?,
                authorization_evidence: digest(32),
                authorization_expires_at: Some(Timestamp::from_unix_nanos(i64::MAX)),
                permitted_operations: vec![SourceOperation::Persist],
            },
            &macro_identity,
            &CancellationToken::new(),
        )
        .await?;
    let rejected_pending = service
        .prepare_provider_macro_plan_publication(macro_reservation.clone(), rejected_input)?;
    let rollback_probe = rusqlite::Connection::open(location.path())?;
    rollback_probe.execute_batch(
        "CREATE TRIGGER reject_multi_artifact_manifest
         BEFORE INSERT ON dataset_manifests
         BEGIN
             SELECT RAISE(ABORT, 'multi-artifact rollback proof');
         END;",
    )?;
    assert!(
        rejected_pending
            .commit(
                &service,
                CancellationToken::new(),
                Arc::new(AllowProviderEventPublication),
            )
            .await
            .is_err()
    );
    let retained_manifest_count: i64 =
        rollback_probe.query_row("SELECT COUNT(*) FROM dataset_manifests", [], |row| {
            row.get(0)
        })?;
    let retained_direct_binding_count: i64 = rollback_probe.query_row(
        "SELECT COUNT(*) FROM ingest_run_provider_capture_bindings",
        [],
        |row| row.get(0),
    )?;
    assert_eq!(retained_manifest_count, 1);
    assert_eq!(retained_direct_binding_count, 0);
    rollback_probe.execute_batch("DROP TRIGGER reject_multi_artifact_manifest;")?;
    drop(rollback_probe);
    let orphan_recovery_now = Timestamp::from_unix_nanos(i64::try_from(
        SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos(),
    )?)
    .checked_add_nanos(61_000_000_000)?;
    let recovery = service
        .recover_orphans(orphan_recovery_now, CancellationToken::new())
        .await?;
    assert_eq!(recovery.quarantined(), 2);
    assert!(
        !service
            .object_store()
            .read_pinned(first.pinned(), &CancellationToken::new())?
            .is_empty(),
        "the committed prefix must remain live while every failed group object is quarantined"
    );
    let retry_input = provider_macro_plan_input(&raw_store, analytical_dataset)?;
    assert_eq!(retry_input.publication_digest(), macro_payload_digest);
    let macro_receipt = service
        .prepare_provider_macro_plan_publication(macro_reservation, retry_input)?
        .commit(
            &service,
            CancellationToken::new(),
            Arc::new(AllowProviderEventPublication),
        )
        .await?;
    assert_eq!(macro_receipt.total_chunks(), 3);
    assert_eq!(macro_receipt.total_rows(), 3);
    assert_eq!(macro_receipt.manifest().manifest_version(), 2);
    let macro_pinned = service.pinned(macro_receipt.manifest())?;
    assert_eq!(macro_pinned.objects().len(), 3);
    assert_eq!(macro_pinned.objects()[0], first.pinned().objects()[0]);
    let committed_recovery_now = Timestamp::from_unix_nanos(i64::try_from(
        SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos(),
    )?)
    .checked_add_nanos(61_000_000_000)?;
    assert_eq!(
        service
            .recover_orphans(committed_recovery_now, CancellationToken::new())
            .await?
            .quarantined(),
        0,
        "no object in the committed prefix plus two-object suffix may be quarantined"
    );
    let owned =
        service.generation_owned_provider_capture_evidence(macro_receipt.manifest(), &raw_store)?;
    assert_eq!(owned.objects().len(), 2);
    assert_eq!(owned.objects()[0].publication_ordinal(), 0);
    assert_eq!(owned.objects()[0].generation_object_ordinal(), 1);
    assert_eq!(owned.objects()[0].inputs().len(), 2);
    assert_eq!(owned.objects()[0].inputs()[0].input_ordinal(), 0);
    assert_eq!(owned.objects()[0].inputs()[0].object_input_ordinal(), 0);
    assert_eq!(owned.objects()[0].inputs()[1].input_ordinal(), 1);
    assert_eq!(owned.objects()[0].inputs()[1].object_input_ordinal(), 1);
    assert_eq!(owned.objects()[1].publication_ordinal(), 1);
    assert_eq!(owned.objects()[1].generation_object_ordinal(), 2);
    assert_eq!(owned.objects()[1].inputs().len(), 1);
    assert_eq!(owned.objects()[1].inputs()[0].input_ordinal(), 2);
    assert_eq!(owned.objects()[1].inputs()[0].object_input_ordinal(), 0);
    assert_ne!(
        owned.objects()[0].object().object().content_hash(),
        owned.objects()[1].object().object().content_hash()
    );
    let macro_restart = macro_receipt.restart_selector();
    let backup_paths = LocalPaths::prepare(directory.path().join("multi-artifact-backup"))?;
    let backup_location = AnalyticalBackupLocation::try_new(
        backup_paths.catalog()?.clone(),
        backup_paths.artifacts()?.clone(),
    )?;
    let backup_limits =
        AnalyticalBackupLimits::try_new(64, 256, 64 * 1024 * 1024, 8 * 1024 * 1024, 1024 * 1024)?;
    let backup_cutoff = Timestamp::from_unix_nanos(i64::try_from(
        SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos(),
    )?)
    .checked_add_nanos(1_000_000_000)?;
    let verified_backup = service
        .backup_service()
        .create(
            backup_location,
            backup_cutoff,
            backup_limits,
            &CancellationToken::new(),
        )
        .await?;
    let batches = service
        .object_store()
        .read_pinned(&macro_pinned, &CancellationToken::new())?;
    let observations = batches
        .into_iter()
        .map(ResearchArrowBatch::try_from_record_batch)
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .map(|batch| batch.observations())
        .collect::<Result<Vec<_>, _>>()?;
    let direct_series = observations
        .iter()
        .flatten()
        .map(|observation| match observation {
            ResearchObservation::Macro(observation) => Ok(observation.series().as_str()),
            _ => Err("expected only macro observations"),
        })
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(direct_series, ["GDP", "GDP-0", "GDP-1", "GDP-2"]);
    let query = ResearchQueryEngine::from_pinned_dataset(
        macro_pinned.clone(),
        "observations",
        service.object_store(),
        CancellationToken::new(),
    )
    .await?;
    let QueryResult::Inline { batches, .. } = query
        .query(
            QueryRequest::try_new(
                macro_receipt.manifest().clone(),
                "SELECT macro_series FROM observations",
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
        .await?
    else {
        return Err("expected inline manifest-order macro query".into());
    };
    let mut queried_series = Vec::new();
    queried_series.try_reserve_exact(4)?;
    for batch in &batches {
        let series = batch
            .column_by_name("macro_series")
            .and_then(|column| column.as_any().downcast_ref::<StringArray>())
            .ok_or("macro query omitted the series column")?;
        for row in 0..series.len() {
            queried_series.push(series.value(row));
        }
    }
    assert_eq!(queried_series, ["GDP", "GDP-0", "GDP-1", "GDP-2"]);
    let pinned_memory = query
        .query(
            QueryRequest::try_new(
                macro_receipt.manifest().clone(),
                "SELECT macro_series FROM observations",
            )?,
            QueryLimits::try_new(10, 8 * 1024, 8 * 1024, 1, 128, 128, Duration::from_secs(1))?,
            CancellationToken::new(),
        )
        .await;
    assert!(matches!(
        pinned_memory,
        Err(QueryError::MemoryLimitExceeded { limit: 8192 })
    ));

    let source_pinned = macro_pinned.clone();
    let compaction = CompactionRequest::new(macro_receipt.manifest().clone());
    drop(query);
    let restored_paths = LocalPaths::prepare(directory.path().join("multi-artifact-restored"))?;
    let restored_location = restored_paths.catalog()?.clone();
    let restored_catalog_config = test_catalog_config(restored_location.clone())?;
    let restored_object_config =
        ObjectStoreConfig::try_new(8 * 1024 * 1024, 1024, Duration::from_secs(60))?;
    let restored = verified_backup.restore(
        AnalyticalRestoreTarget::try_new(
            restored_catalog_config.clone(),
            restored_paths.artifacts()?.clone(),
            8,
            restored_object_config,
            AnalyticalRestoreMode::Fresh,
        )?,
        &CancellationToken::new(),
    )?;
    drop(restored);
    let restored = AnalyticalDataService::open(
        CatalogAuthority::open(restored_catalog_config)?,
        AnalyticalManifestCatalog::open(&restored_location, 8)?,
        restored_paths.artifacts()?.clone(),
        restored_object_config,
    )?;
    let restored_pinned = restored.pinned(macro_receipt.manifest())?;
    assert_eq!(restored_pinned, macro_pinned);
    assert_eq!(
        restored.verify_provider_macro_plan_restart(&macro_restart)?,
        macro_pinned
    );
    let restored_owned = restored
        .generation_owned_provider_capture_evidence(macro_receipt.manifest(), &raw_store)?;
    assert_eq!(restored_owned, owned);
    assert_eq!(restored_owned.receipt_digest(), owned.receipt_digest());
    for object in restored_owned.objects() {
        assert!(!object.inputs().is_empty());
        for input in object.inputs() {
            assert!(!input.binding().physical_claims().is_empty());
        }
    }
    drop(restored);
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
    let restarted_macro = service.verify_provider_macro_plan_restart(&macro_restart)?;
    assert_eq!(restarted_macro, source_pinned);
    drop(restarted_macro);
    let compacted = service
        .compact(reservation, compaction, CancellationToken::new())
        .await?;
    assert_eq!(compacted.manifest().manifest_version(), 3);
    assert_eq!(
        compacted.pinned().plan().row_count(),
        source_pinned.plan().row_count()
    );
    assert_eq!(
        compacted.pinned().plan().lineage_digest(),
        source_pinned.plan().lineage_digest()
    );
    assert_eq!(compacted.pinned().objects().len(), 1);
    assert!(
        !service
            .object_store()
            .read_pinned(&source_pinned, &CancellationToken::new())?
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

    exercise_sec_exact_origin_point_in_time_restart().await?;

    Ok(())
}

async fn exercise_sec_exact_origin_point_in_time_restart() -> TestResult {
    let directory = tempfile::tempdir()?;
    let paths = LocalPaths::prepare(directory.path().join("sec-exact-restart"))?;
    let location = paths.catalog()?.clone();
    let catalog_config = test_catalog_config(location.clone())?;
    let source = sec_research_source()?;
    let authority = CatalogAuthority::open(catalog_config.clone())?;
    authority.register_source(&source, Timestamp::from_unix_nanos(10))?;
    let store_config = ObjectStoreConfig::try_new(64 * 1024 * 1024, 64, Duration::from_secs(60))?;
    let service = AnalyticalDataService::initialize(
        authority,
        AnalyticalManifestCatalog::open(&location, 8)?,
        paths.artifacts()?.clone(),
        store_config,
    )?;
    let raw_store = paths.sealed_research_journal_store()?;
    let base_ns = i64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos())?;
    let fixture = sec_research_capture_fixture(base_ns)?;
    let payload_digest = extraction_provider_payload_digest(&fixture.batch);
    let company = sec_research_company_identity(payload_digest, base_ns)?;
    let company_json = serde_json::to_string(&company)?;
    let company_digest = EvidenceDigest::new(
        DigestAlgorithm::Sha256,
        Sha256::digest(company_json.as_bytes()).into(),
    );
    let identity = IngestIdentity::try_new(
        source.source_id().clone(),
        payload_digest,
        SourceOperation::Persist,
        "sec:submissions:exact-restart:v1",
    )?;
    let cancellation = CancellationToken::new();
    let reservation = service
        .reserve_source_ingest(
            &source,
            Timestamp::from_unix_nanos(10),
            RightsDecisionInput {
                source_id: source.source_id().clone(),
                payload_digest,
                retrieved_at: Timestamp::from_unix_nanos(base_ns),
                basis: RightsBasis::reviewed_terms(
                    "https://www.sec.gov/os/accessing-edgar-data",
                    digest(211),
                )?,
                authorization_evidence: digest(212),
                authorization_expires_at: Some(Timestamp::from_unix_nanos(i64::MAX)),
                permitted_operations: vec![SourceOperation::Persist],
            },
            &identity,
            &cancellation,
        )
        .await?;
    let SecResearchCaptureFixture {
        batch,
        capture_material,
        revision_plan,
        native_rows,
    } = fixture;
    let analytical_dataset = DatasetId::try_from(batch.request().object().dataset().as_str())?;
    let (expectation, request) = capture_material.into_whole_seal_parts();
    let token = expectation
        .try_rejoin(request.seal(&raw_store)?)?
        .try_into_whole()?;
    let mut native = ProviderNativeLineageBatchBuilder::try_new(
        ProviderNativeLineageImplementation::SecEdgarV1,
        &batch,
    )?;
    for row in &native_rows {
        native.try_push(row)?;
    }
    let native = native.finish()?;
    let binding =
        SealedProviderCaptureBinding::try_whole(token, batch, native, vec![0; native_rows.len()])?;
    let committed = service
        .ingest_provider_publication(
            reservation,
            analytical_dataset,
            ProviderPublicationInput::try_new(binding, revision_plan)?
                .with_company_identity(company),
            cancellation,
        )
        .await?;
    let binding_digests = service.provider_capture_binding_digests(committed.manifest())?;
    assert_eq!(binding_digests.len(), 1);
    let retained_binding = service.provider_capture_binding_evidence(
        committed.manifest(),
        binding_digests[0],
        &raw_store,
    )?;
    let physical = retained_binding
        .physical_claims()
        .first()
        .ok_or("missing retained SEC physical claim")?;
    let verification_control = CancelDuringRawVerification::default();
    assert!(matches!(
        raw_store.open_verified_claim_with_control(physical.claim(), &verification_control),
        Err(SealedResearchJournalStoreError::ObjectControl(
            ResearchObjectControlError::Cancelled
        ))
    ));
    assert!(
        verification_control
            .verification_chunks
            .load(Ordering::Acquire)
            >= 2
    );
    let company_search = service.company_identities().search(
        "0000320193",
        2,
        Instant::now() + Duration::from_secs(5),
        &CancellationToken::new(),
    )?;
    assert_eq!(company_search.matches().len(), 1);
    let generation_completed_at = company_search.matches()[0].completed_at();
    let knowledge_at = generation_completed_at.checked_add_nanos(1_000_000_000)?;
    let limits = PointInTimeLimits::try_new(8, 8, 8, 8, 8 * 1024 * 1024)?;
    let binding_conflict = service
        .sec_research_reader()
        .select(
            SecResearchReadRequest::try_new(
                committed.manifest().clone(),
                SecResearchFamily::Submissions,
                digest(219),
                company_digest,
                knowledge_at,
                ResearchTemporalCoordinate::exact(knowledge_at),
                PointInTimeRevisionMode::LatestKnown,
                limits,
                64 * 1024 * 1024,
            )?,
            &raw_store,
            Instant::now() + Duration::from_secs(30),
            CancellationToken::new(),
        )
        .await;
    assert!(matches!(
        binding_conflict,
        Err(SecResearchReadError::OriginMismatch | SecResearchReadError::ProviderBindingMismatch)
    ));
    let request = SecResearchReadRequest::try_new(
        committed.manifest().clone(),
        SecResearchFamily::Submissions,
        binding_digests[0],
        company_digest,
        knowledge_at,
        ResearchTemporalCoordinate::exact(knowledge_at),
        PointInTimeRevisionMode::LatestKnown,
        limits,
        64 * 1024 * 1024,
    )?;
    let selected = service
        .sec_research_reader()
        .select(
            request,
            &raw_store,
            Instant::now() + Duration::from_secs(30),
            CancellationToken::new(),
        )
        .await?;
    assert_eq!(selected.disposition(), SecResearchDisposition::Selected);
    assert_eq!(selected.decoded_rows().len(), 5);
    assert_eq!(selected.origin().manifest(), committed.manifest());
    assert_eq!(selected.origin().object_ordinal(), 0);
    assert_eq!(
        selected.origin().object_content_digest().bytes(),
        committed.pinned().objects()[0]
            .object()
            .content_hash()
            .bytes()
    );
    assert_eq!(
        selected.receipt().provider_binding_digest(),
        binding_digests[0]
    );
    assert_eq!(
        selected.receipt().company_observation_digest(),
        company_digest
    );
    assert!(
        selected
            .exclusions()
            .iter()
            .any(|row| row.knowledge().available_after_cutoff())
    );
    assert!(
        selected
            .exclusions()
            .iter()
            .any(|row| row.knowledge().received_after_cutoff())
    );
    assert!(
        selected
            .exclusions()
            .iter()
            .any(|row| row.knowledge().ingested_after_cutoff())
    );
    assert!(selected.exclusions().iter().any(|row| {
        row.point_in_time_reasons().is_some_and(|reasons| {
            reasons.contains(market_squawk_data::PointInTimeExclusionReason::EffectiveAfterCutoff)
        })
    }));
    let generation_pending = service
        .sec_research_reader()
        .select(
            SecResearchReadRequest::try_new(
                committed.manifest().clone(),
                SecResearchFamily::Submissions,
                binding_digests[0],
                company_digest,
                generation_completed_at.checked_add_nanos(-1)?,
                ResearchTemporalCoordinate::exact(knowledge_at),
                PointInTimeRevisionMode::LatestKnown,
                limits,
                64 * 1024 * 1024,
            )?,
            &raw_store,
            Instant::now() + Duration::from_secs(30),
            CancellationToken::new(),
        )
        .await?;
    assert_eq!(
        generation_pending.disposition(),
        SecResearchDisposition::Unavailable
    );
    assert!(
        generation_pending
            .exclusions()
            .iter()
            .all(|row| row.knowledge().generation_completed_after_cutoff())
    );

    let expected = selected.clone();
    drop(generation_pending);
    drop(selected);
    drop(committed);
    drop(service);
    drop(raw_store);
    let restarted = AnalyticalDataService::open(
        CatalogAuthority::open(catalog_config)?,
        AnalyticalManifestCatalog::open(&location, 8)?,
        paths.artifacts()?.clone(),
        store_config,
    )?;
    let reopened_raw_store = paths.sealed_research_journal_store()?;
    let replay = restarted
        .sec_research_reader()
        .verify_restart(
            &expected,
            &reopened_raw_store,
            Instant::now() + Duration::from_secs(30),
            CancellationToken::new(),
        )
        .await?;
    assert_eq!(replay, expected);
    Ok(())
}

#[test]
fn dataset_inputs_reject_a_transaction_from_another_instrument() -> TestResult {
    let instrument = dataset_membership_instrument()?;
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
        ResearchTemporalCoordinate::exact(Timestamp::from_unix_nanos(80)),
        None,
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
async fn point_in_time_builder_publishes_one_authorized_queryable_phase_one_generation()
-> TestResult {
    let directory = tempfile::tempdir()?;
    let paths = LocalPaths::prepare(directory.path().join("market-squawk"))?;
    let location = paths.catalog()?.clone();
    let catalog_config = test_catalog_config(location.clone())?;
    let store_config = ObjectStoreConfig::try_new(8 * 1024 * 1024, 1024, Duration::from_secs(60))?;
    let (service, publisher, source, market_bars) =
        initialized_service_with_universe(&paths, catalog_config.clone(), store_config).await?;
    let instrument = dataset_membership_instrument()?;
    let research_limits = ResearchUseLimits::try_new(
        8,
        32,
        32,
        8,
        1024 * 1024,
        Duration::from_secs(2),
        Duration::from_secs(30),
    )?;
    let preflight_request = ResearchUseRequest::try_new(
        vec![source.manifest().clone()],
        ResearchUse::LocalAnalysis,
        research_limits,
    )?;
    let preflight = service
        .dataset_builder()
        .preflight_research_use(preflight_request.clone(), &CancellationToken::new())?;
    assert_eq!(preflight.request(), &preflight_request);
    assert_eq!(preflight.research_use(), ResearchUse::LocalAnalysis);
    assert_ne!(preflight.decision_digest().bytes(), [0; 32]);
    assert_ne!(preflight.graph_digest().bytes(), [0; 32]);
    assert!(preflight.expires_at() > Timestamp::from_unix_nanos(0));
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
        ResearchTemporalCoordinate::exact(Timestamp::from_unix_nanos(80)),
        None,
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
        ResearchTemporalCoordinate::exact(Timestamp::from_unix_nanos(80)),
        Some(ResearchTemporalCoordinate::exact(
            Timestamp::from_unix_nanos(100),
        )),
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
    let output_dataset = DatasetId::try_from("derived.feature-labels.gdp-v1")?;
    assert_eq!(
        preflight.request(),
        &ResearchUseRequest::try_new(
            inputs.parents().to_vec(),
            ResearchUse::LocalAnalysis,
            research_limits,
        )?
    );
    let request = DatasetBuildRequest::try_new(
        output_dataset.clone(),
        inputs,
        policy,
        ResearchUse::LocalAnalysis,
        research_limits,
        output_authorization,
        limits,
    )?;

    let rejected_authority = Arc::new(RejectDatasetPublication::default());
    let rejected = service
        .dataset_builder()
        .build_with_precommit_authority(
            request.clone(),
            CancellationToken::new(),
            rejected_authority.clone(),
        )
        .await;
    assert!(matches!(
        rejected,
        Err(DatasetBuildError::PublicationAuthorityRevoked)
    ));
    assert!(!rejected_authority.committed.load(Ordering::Acquire));
    let rejected_lookup = service.analytical_reader().latest(
        &output_dataset,
        Instant::now() + Duration::from_secs(30),
        &CancellationToken::new(),
    )?;
    assert!(rejected_lookup.is_none());

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
    assert_eq!(export_json["schema_version"], 4);
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
    let cancellation = CancellationToken::new();
    let deadline = Instant::now() + Duration::from_secs(30);
    let reader = service.analytical_reader();
    let phase_one = reader
        .latest(built.manifest().dataset_id(), deadline, &cancellation)?
        .ok_or("missing phase-one feature generation")?;
    assert_eq!(phase_one.python_export_sha256(), None);
    assert!(
        reader
            .feature_dataset(
                FeatureDatasetProductContract::PriceReturnMacroContextFixedHorizonForwardReturnAnalysisV1,
                built.manifest().dataset_id(),
                deadline,
                &cancellation,
            )?
            .is_none(),
        "an immutable analytical generation is not a product before receipt admission"
    );

    let replayed = service
        .dataset_builder()
        .build(request.clone(), CancellationToken::new())
        .await?;
    assert_eq!(replayed.manifest(), built.manifest());

    assert!(matches!(
        AnalyticalObservationReadRequest::try_new(
            source.manifest().clone(),
            AnalyticalObservationTemplate::UniverseMembership,
            vec![instrument],
            None,
        ),
        Err(AnalyticalReadError::UniverseMembershipReadMustBeExhaustive)
    ));
    let membership_request =
        AnalyticalObservationReadRequest::try_universe_membership(source.manifest().clone())?;
    let membership = reader
        .read_observations(
            membership_request.clone(),
            QueryLimits::try_new(
                1,
                256 * 1024,
                16 * 1024 * 1024,
                1,
                64,
                64,
                Duration::from_secs(1),
            )?,
            deadline,
            cancellation.clone(),
        )
        .await?;
    assert_eq!(membership.request(), &membership_request);
    assert_eq!(membership.output().manifest(), source.manifest());
    let membership_rows: usize = match membership.output().result() {
        QueryResult::Inline { batches, .. } => batches.iter().map(|batch| batch.num_rows()).sum(),
        QueryResult::Artifact { .. } => return Err("membership result was not inline".into()),
    };
    assert_eq!(membership_rows, 1);
    let saturated = reader
        .read_observations(
            AnalyticalObservationReadRequest::try_new(
                source.manifest().clone(),
                AnalyticalObservationTemplate::All,
                Vec::new(),
                None,
            )?,
            QueryLimits::try_new(
                1,
                256 * 1024,
                16 * 1024 * 1024,
                1,
                64,
                64,
                Duration::from_secs(1),
            )?,
            deadline,
            cancellation.clone(),
        )
        .await;
    assert!(matches!(
        saturated,
        Err(AnalyticalReadError::Query(QueryError::RowLimitExceeded {
            limit: 1
        }))
    ));
    let listed = reader
        .latest(built.manifest().dataset_id(), deadline, &cancellation)?
        .ok_or("missing built feature dataset")?;
    assert_eq!(
        listed.python_export_sha256(),
        None,
        "a phase-one analytical generation must not claim product admission"
    );
    let feature_page = reader.feature_datasets(
        FeatureDatasetProductContract::PriceReturnMacroContextFixedHorizonForwardReturnAnalysisV1,
        None,
        AnalyticalReadLimit::try_new(8)?,
        deadline,
        &cancellation,
    )?;
    assert!(!feature_page.has_more());
    assert_eq!(feature_page.available(), 0);
    assert!(feature_page.datasets().is_empty());
    assert!(feature_page.overlapping_legacy_dataset_ids().is_empty());

    let macro_parent = source.manifest().clone();
    let production_request = closed_price_return_request(
        macro_parent.clone(),
        market_bars.manifest().clone(),
        instrument,
        research_limits,
        false,
    )?;
    let production_dataset = service
        .dataset_builder()
        .build(production_request.clone(), CancellationToken::new())
        .await?;
    let production_row_limit = usize::try_from(production_dataset.pinned().plan().row_count())?;
    let production_contract =
        FeatureDatasetProductContract::PriceReturnMacroContextFixedHorizonForwardReturnAnalysisV1;
    assert!(
        reader
            .feature_dataset(
                production_contract,
                production_dataset.manifest().dataset_id(),
                deadline,
                &cancellation,
            )?
            .is_none(),
        "the closed-recipe phase-one generation must remain invisible before publication"
    );
    let wall_nanos = i64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos())?;
    let attested_at = Timestamp::from_unix_nanos(wall_nanos).checked_sub_nanos(1_000_000_000)?;
    let currentness_expires_at =
        Timestamp::from_unix_nanos(wall_nanos).checked_add_nanos(120_000_000_000)?;
    let published = publisher.publish(
        &service,
        production_contract,
        &production_request,
        &production_dataset,
        closed_price_return_proof(
            &production_request,
            macro_parent.clone(),
            attested_at,
            currentness_expires_at,
            96,
        )?,
        &CancellationToken::new(),
    )?;
    assert_eq!(
        published.disposition(),
        FeatureDatasetProductionPublicationDisposition::Published
    );
    assert_eq!(published.contract(), production_contract);
    let production_identity = published.receipt().production_identity();
    let receipt_sha256 = published.receipt().receipt_sha256();
    let retained_receipt = published.receipt().canonical_json().to_vec();
    let admitted = reader
        .feature_dataset(
            production_contract,
            production_dataset.manifest().dataset_id(),
            deadline,
            &cancellation,
        )?
        .ok_or("closed price-return product is absent after atomic admission")?;
    assert_eq!(
        admitted.generation().manifest(),
        production_dataset.manifest()
    );
    assert_eq!(admitted.product_contract(), production_contract);
    assert_eq!(
        admitted.production_receipt().production_identity(),
        production_identity
    );
    assert_eq!(
        admitted.production_receipt().canonical_json(),
        retained_receipt
    );
    assert!(
        reader
            .feature_dataset(
                FeatureDatasetProductContract::PriceReturnMacroContextFixedHorizonForwardReturnTrainingV1,
                production_dataset.manifest().dataset_id(),
                deadline,
                &cancellation,
            )?
            .is_none(),
        "an analysis admission must never authorize training"
    );
    let same_session_replay = publisher.publish(
        &service,
        production_contract,
        &production_request,
        &production_dataset,
        closed_price_return_proof(
            &production_request,
            macro_parent.clone(),
            attested_at,
            currentness_expires_at,
            96,
        )?,
        &CancellationToken::new(),
    )?;
    assert_eq!(
        same_session_replay.disposition(),
        FeatureDatasetProductionPublicationDisposition::Replay
    );
    assert_eq!(
        same_session_replay.receipt().receipt_sha256(),
        receipt_sha256
    );
    let conflicting = publisher.publish(
        &service,
        production_contract,
        &production_request,
        &production_dataset,
        closed_price_return_proof(
            &production_request,
            macro_parent.clone(),
            attested_at,
            currentness_expires_at,
            97,
        )?,
        &CancellationToken::new(),
    );
    assert!(matches!(
        conflicting,
        Err(FeatureDatasetProductionError::Dataset(
            DatasetBuildError::PythonDataset(
                PythonDatasetCatalogError::ConflictingProductionAdmission
            )
        ))
    ));
    let successor_request = closed_price_return_request(
        source.manifest().clone(),
        market_bars.manifest().clone(),
        instrument,
        research_limits,
        true,
    )?;
    let successor_dataset = service
        .dataset_builder()
        .build(successor_request.clone(), CancellationToken::new())
        .await?;
    assert_eq!(
        successor_dataset.manifest().manifest_version(),
        production_dataset
            .manifest()
            .manifest_version()
            .checked_add(1)
            .ok_or("successor manifest version overflow")?
    );
    let successor_publication = publisher.publish(
        &service,
        production_contract,
        &successor_request,
        &successor_dataset,
        closed_price_return_proof(
            &successor_request,
            macro_parent.clone(),
            attested_at,
            currentness_expires_at,
            98,
        )?,
        &CancellationToken::new(),
    )?;
    assert_eq!(
        successor_publication.disposition(),
        FeatureDatasetProductionPublicationDisposition::Published
    );
    assert_eq!(
        reader
            .feature_dataset(
                production_contract,
                production_dataset.manifest().dataset_id(),
                deadline,
                &cancellation,
            )?
            .ok_or("latest product lookup is absent after successor admission")?
            .generation()
            .manifest(),
        successor_dataset.manifest(),
        "dataset-ID lookup must retain explicit latest-version semantics"
    );
    let backup_paths = LocalPaths::prepare(directory.path().join("feature-product-backup"))?;
    let backup_location = AnalyticalBackupLocation::try_new(
        backup_paths.catalog()?.clone(),
        backup_paths.artifacts()?.clone(),
    )?;
    let backup_limits =
        AnalyticalBackupLimits::try_new(64, 256, 64 * 1024 * 1024, 8 * 1024 * 1024, 1024 * 1024)?;
    let backup_cutoff = Timestamp::from_unix_nanos(i64::try_from(
        SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos(),
    )?)
    .checked_add_nanos(1_000_000_000)?;
    let verified_backup = service
        .backup_service()
        .create(
            backup_location,
            backup_cutoff,
            backup_limits,
            &CancellationToken::new(),
        )
        .await?;

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
            QueryLimits::try_new(
                8,
                256 * 1024,
                16 * 1024 * 1024,
                1,
                64,
                64,
                Duration::from_secs(1),
            )?,
            CancellationToken::new(),
        )
        .await?;
    assert!(matches!(result, QueryResult::Inline { .. }));
    drop(result);
    drop(query);
    drop(admitted);
    drop(same_session_replay);
    drop(published);
    drop(successor_publication);
    drop(reader);
    drop(publisher);
    drop(service);

    let (reopened_composition, _onboarding_catalog) =
        AnalyticalDataService::open_with_provider_onboarding(
            CatalogAuthority::open(catalog_config)?,
            AnalyticalManifestCatalog::open(&location, 8)?,
            paths.artifacts()?.clone(),
            store_config,
        )?;
    let (reopened, reopened_publisher) = reopened_composition.into_parts();
    let reopened_reader = reopened.analytical_reader();
    let reopened_phase_one = reopened_reader
        .latest(
            built.manifest().dataset_id(),
            Instant::now() + Duration::from_secs(30),
            &CancellationToken::new(),
        )?
        .ok_or("restarted phase-one feature generation is absent")?;
    assert_eq!(reopened_phase_one.manifest(), built.manifest());
    assert_eq!(reopened_phase_one.python_export_sha256(), None);
    assert!(
        reopened_reader
            .feature_dataset(
                FeatureDatasetProductContract::PriceReturnMacroContextFixedHorizonForwardReturnAnalysisV1,
                built.manifest().dataset_id(),
                Instant::now() + Duration::from_secs(30),
                &CancellationToken::new(),
            )?
            .is_none()
    );
    let retention = rusqlite::Connection::open_with_flags(
        location.path(),
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY
            | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX
            | rusqlite::OpenFlags::SQLITE_OPEN_NOFOLLOW,
    )?;
    let retained: (i64, i64) = retention.query_row(
        "SELECT retained_rows, retained_payload_bytes
         FROM feature_dataset_production_admission_retention
         WHERE singleton=1",
        [],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    assert_eq!(retained.0, 2);
    assert!(retained.1 > 0);
    let historical_v1 = reopened_reader
        .forecast_dataset_evidence(
            production_contract,
            production_dataset.manifest(),
            Timestamp::from_unix_nanos(200),
            ForecastDatasetReadLimits::try_new(production_row_limit, 1024 * 1024)?,
            Instant::now() + Duration::from_secs(30),
            CancellationToken::new(),
        )
        .await?;
    assert_eq!(
        historical_v1.dataset().generation().manifest(),
        production_dataset.manifest(),
        "an exact historical manifest must remain selectable after its successor is admitted"
    );
    assert_eq!(
        historical_v1
            .dataset()
            .production_receipt()
            .production_identity(),
        production_identity
    );
    assert_eq!(
        historical_v1
            .dataset()
            .production_receipt()
            .receipt_sha256(),
        receipt_sha256
    );
    assert_eq!(
        historical_v1
            .dataset()
            .production_receipt()
            .canonical_json(),
        retained_receipt
    );
    assert_eq!(
        reopened_reader
            .feature_dataset(
                production_contract,
                production_dataset.manifest().dataset_id(),
                Instant::now() + Duration::from_secs(30),
                &CancellationToken::new(),
            )?
            .ok_or("latest product lookup is absent after restart")?
            .generation()
            .manifest(),
        successor_dataset.manifest()
    );
    let restart_replay = reopened_publisher.publish(
        &reopened,
        production_contract,
        &production_request,
        &production_dataset,
        closed_price_return_proof(
            &production_request,
            macro_parent,
            attested_at,
            currentness_expires_at,
            96,
        )?,
        &CancellationToken::new(),
    )?;
    assert_eq!(
        restart_replay.disposition(),
        FeatureDatasetProductionPublicationDisposition::Replay
    );
    assert_eq!(restart_replay.receipt().receipt_sha256(), receipt_sha256);
    let training_products = reopened_reader.feature_datasets(
        FeatureDatasetProductContract::PriceReturnMacroContextFixedHorizonForwardReturnTrainingV1,
        None,
        AnalyticalReadLimit::try_new(1)?,
        Instant::now() + Duration::from_secs(30),
        &CancellationToken::new(),
    )?;
    assert_eq!(training_products.available(), 0);
    assert!(training_products.datasets().is_empty());
    let restored_paths = LocalPaths::prepare(directory.path().join("feature-product-restored"))?;
    let restored_location = restored_paths.catalog()?.clone();
    let restored = verified_backup.restore(
        AnalyticalRestoreTarget::try_new(
            test_catalog_config(restored_location)?,
            restored_paths.artifacts()?.clone(),
            8,
            store_config,
            AnalyticalRestoreMode::Fresh,
        )?,
        &CancellationToken::new(),
    )?;
    let relocated_evidence = restored
        .analytical_reader()
        .forecast_dataset_evidence(
            production_contract,
            production_dataset.manifest(),
            Timestamp::from_unix_nanos(200),
            ForecastDatasetReadLimits::try_new(production_row_limit, 1024 * 1024)?,
            Instant::now() + Duration::from_secs(30),
            CancellationToken::new(),
        )
        .await;
    assert!(matches!(
        relocated_evidence,
        Err(AnalyticalReadError::Manifest(
            ManifestCatalogError::CorruptCatalog
        ))
    ));
    Ok(())
}

#[tokio::test]
async fn analytical_reader_keeps_manifest_authority_and_observation_evidence_closed() -> TestResult
{
    let directory = tempfile::tempdir()?;
    let paths = LocalPaths::prepare(directory.path().join("reader"))?;
    let location = paths.catalog()?.clone();
    let catalog_config = test_catalog_config(location.clone())?;
    let store_config = ObjectStoreConfig::try_new(1024 * 1024, 32, Duration::from_secs(60))?;
    let authority = CatalogAuthority::open(catalog_config.clone())?;
    let source = local_source()?;
    authority.register_source(&source, Timestamp::from_unix_nanos(10))?;
    let batch = macro_snapshot_extraction_batch()?;
    let revisions = macro_snapshot_revision_plan(&batch)?;
    let payload_digest = extraction_provider_payload_digest(&batch);
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
            "fred:h15:macro-snapshot-fixture:v1",
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
        .ingest_with_revision_plan(
            reservation,
            DatasetId::try_from(batch.request().object().dataset().as_str())?,
            batch,
            revisions,
            CancellationToken::new(),
        )
        .await?;
    for persisted in service
        .object_store()
        .read_pinned(committed.pinned(), &CancellationToken::new())?
    {
        let projected_series = persisted
            .column_by_name("macro_series")
            .and_then(|column| column.as_any().downcast_ref::<StringArray>())
            .ok_or("missing Macro series projection")?;
        let payloads = persisted
            .column_by_name("payload_json")
            .and_then(|column| column.as_any().downcast_ref::<BinaryArray>())
            .ok_or("missing Macro payload projection")?;
        for row in 0..persisted.num_rows() {
            let ResearchObservation::Macro(payload) =
                serde_json::from_slice::<ResearchObservation>(payloads.value(row))?
            else {
                return Err("Macro fixture payload changed variant".into());
            };
            assert_eq!(projected_series.value(row), payload.series().as_str());
        }
    }
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

    let allowlist = AnalyticalMacroSeriesAllowlist::try_from_code_owned_identifiers(
        MACRO_SNAPSHOT_SERIES
            .iter()
            .map(|series| SourceIdentifier::try_from(*series))
            .collect::<Result<Vec<_>, _>>()?,
    )?;
    let effective_date_cutoff = market_squawk_domain::CalendarDate::new(2026, 8, 11)?;
    let request = AnalyticalMacroLatestKnownRequest::try_new(
        committed.manifest().clone(),
        source.source_id().clone(),
        Timestamp::from_unix_nanos(250),
        effective_date_cutoff,
        allowlist,
    )?;
    assert_eq!(request.required_query_rows(), 89);
    let observed = reader
        .read_macro_latest_known_snapshot(
            request.clone(),
            QueryLimits::try_new(
                request.required_query_rows(),
                256 * 1024,
                64 * 1024 * 1024,
                1,
                2_048,
                2_048,
                Duration::from_secs(10),
            )?,
            deadline,
            cancellation,
        )
        .await?;
    assert_eq!(observed.source_id().as_str(), "fred-local-fixture");
    assert_eq!(observed.output().manifest(), committed.manifest());
    assert_eq!(observed.observations().len(), MACRO_SNAPSHOT_SERIES.len());
    assert_eq!(
        observed
            .observations()
            .iter()
            .map(|observation| observation.series().as_str())
            .collect::<Vec<_>>(),
        request
            .series_allowlist()
            .series()
            .iter()
            .map(SourceIdentifier::as_str)
            .collect::<Vec<_>>()
    );
    let corrected = observed
        .observations()
        .iter()
        .find(|observation| observation.series().as_str() == MACRO_SNAPSHOT_SERIES[0])
        .ok_or("missing corrected Macro snapshot series")?;
    assert_eq!(corrected.context().time().revision().get(), 2);
    assert_eq!(
        corrected.context().time().effective().calendar_date_value(),
        Some(market_squawk_domain::CalendarDate::new(2026, 8, 10)?)
    );
    assert_eq!(
        corrected.value().observed_value(),
        Some(Decimal::new(425, 2))
    );
    let QueryResult::Inline { batches, .. } = observed.output().result() else {
        return Err("expected one inline Macro snapshot result".into());
    };
    assert_eq!(
        batches
            .iter()
            .map(arrow::record_batch::RecordBatch::num_rows)
            .sum::<usize>(),
        MACRO_SNAPSHOT_SERIES.len(),
        "engine-owned latest-date/latest-revision selection must cross the typed boundary bounded"
    );
    assert_ne!(observed.output().result_digest().bytes(), [0; 32]);
    assert_ne!(observed.selection_digest().bytes(), [0; 32]);
    let selection_digest = observed.selection_digest();
    let candidate_result_digest = observed.output().result_digest();
    let expected_observations = observed.observations().to_vec();
    let manifest = committed.manifest().clone();
    drop(observed);
    drop(committed);
    drop(reader);
    drop(service);

    let restarted = AnalyticalDataService::open(
        CatalogAuthority::open(catalog_config)?,
        AnalyticalManifestCatalog::open(&location, 8)?,
        paths.artifacts()?.clone(),
        store_config,
    )?;
    let replayed = restarted
        .analytical_reader()
        .read_macro_latest_known_snapshot(
            request.clone(),
            QueryLimits::try_new(
                request.required_query_rows(),
                256 * 1024,
                64 * 1024 * 1024,
                1,
                2_048,
                2_048,
                Duration::from_secs(10),
            )?,
            Instant::now() + Duration::from_secs(30),
            CancellationToken::new(),
        )
        .await?;
    assert_eq!(replayed.output().manifest(), &manifest);
    assert_eq!(replayed.output().result_digest(), candidate_result_digest);
    assert_eq!(replayed.selection_digest(), selection_digest);
    assert_eq!(replayed.observations(), expected_observations);
    Ok(())
}

#[tokio::test]
async fn provider_market_event_publication_is_restart_queryable() -> TestResult {
    let directory = tempfile::tempdir()?;
    let paths = LocalPaths::prepare(directory.path().join("provider-market-event"))?;
    let location = paths.catalog()?.clone();
    let catalog_config = test_catalog_config(location.clone())?;
    let authority = CatalogAuthority::open(catalog_config.clone())?;
    let source = market_bar_source()?;
    authority.register_source(&source, Timestamp::from_unix_nanos(10))?;
    let capture_store = Arc::new(paths.sealed_research_journal_store()?);
    let (publication, expected_claim, expected_event) =
        sealed_market_event_microbatch(&capture_store)?;
    let publication_digest = provider_market_event_publication_digest(&publication)?;
    let rights = authority.admit_source_rights(RightsDecisionInput {
        source_id: source.source_id().clone(),
        payload_digest: publication_digest,
        retrieved_at: Timestamp::from_unix_nanos(500),
        basis: RightsBasis::reviewed_terms("https://example.test/alpaca-terms/v1", digest(41))?,
        authorization_evidence: digest(43),
        authorization_expires_at: Some(Timestamp::from_unix_nanos(i64::MAX)),
        permitted_operations: vec![SourceOperation::Persist],
    })?;
    let reservation = authority.reserve_ingest(
        &IngestIdentity::try_new(
            source.source_id().clone(),
            publication_digest,
            SourceOperation::Persist,
            "alpaca:iex:events:fixture:v1",
        )?,
        &rights,
    )?;
    let service = AnalyticalDataService::initialize(
        authority,
        AnalyticalManifestCatalog::open(&location, 8)?,
        paths.artifacts()?.clone(),
        ObjectStoreConfig::try_new(1024 * 1024, 32, Duration::from_secs(60))?,
    )?;
    let committed = service
        .ingest_provider_market_events(
            reservation,
            DatasetId::try_from("alpaca-live-events-fixture")?,
            publication,
            CancellationToken::new(),
            Arc::new(AllowProviderEventPublication),
        )
        .await?;
    let manifest = committed.manifest().clone();
    let selectors = service.provider_market_event_publications(&manifest)?;
    assert_eq!(selectors.len(), 1);
    assert_eq!(selectors[0].publication_digest(), publication_digest);
    assert_eq!(
        selectors[0].publication_kind(),
        ProviderMarketEventPublicationKind::EventMicrobatch
    );
    drop(committed);
    drop(service);
    drop(capture_store);

    let restarted = AnalyticalDataService::open(
        CatalogAuthority::open(catalog_config)?,
        AnalyticalManifestCatalog::open(&location, 8)?,
        paths.artifacts()?.clone(),
        ObjectStoreConfig::try_new(1024 * 1024, 32, Duration::from_secs(60))?,
    )?;
    let capture_store = paths.sealed_research_journal_store()?;
    let restarted_selectors = restarted.provider_market_event_publications(&manifest)?;
    assert_eq!(restarted_selectors, selectors);
    let reopened = restarted
        .read_provider_market_event_publication(
            &manifest,
            restarted_selectors[0],
            &capture_store,
            CancellationToken::new(),
        )
        .await?;
    assert_eq!(reopened.events(), &[expected_event]);
    let evidence = restarted.provider_market_event_publication_evidence(
        &manifest,
        restarted_selectors[0],
        &capture_store,
    )?;
    let event = evidence
        .event()
        .ok_or("missing event microbatch evidence")?;
    assert_eq!(event.physical_claim(), &expected_claim);
    assert_eq!(
        event.capture().frames()[0].source_sequence(),
        Some(u64::MAX)
    );
    assert_eq!(event.rows()[0].source_sequence(), Some(u64::MAX));
    Ok(())
}

#[tokio::test]
async fn complete_alpaca_history_is_exact_clock_safe_and_restart_selectable() -> TestResult {
    let directory = tempfile::tempdir()?;
    let paths = LocalPaths::prepare(directory.path().join("complete-alpaca-history"))?;
    let location = paths.catalog()?.clone();
    let catalog_config = test_catalog_config(location.clone())?;
    let instrument_id = InstrumentId::from_str("0187f5f1-6fc2-7fa2-bf05-2ce5354c55c1")?;
    let definition = complete_history_market_data_definition(instrument_id)?;
    let definition_json = serde_json::to_string(&definition)?;
    let definition_digest = EvidenceDigest::new(
        DigestAlgorithm::Sha256,
        Sha256::digest(definition_json.as_bytes()).into(),
    );
    assert!(
        complete_history_semantic(
            Timestamp::from_unix_nanos(COMPLETE_HISTORY_FIRST_BAR_NS),
            Timestamp::from_unix_nanos(COMPLETE_HISTORY_REQUEST_END_NS),
            instrument_id,
            definition_digest,
            vec![
                Timestamp::from_unix_nanos(COMPLETE_HISTORY_SECOND_BAR_NS),
                Timestamp::from_unix_nanos(COMPLETE_HISTORY_FIRST_BAR_NS),
            ],
        )
        .is_err(),
        "unordered calendar expectations must fail before a capture graph can be minted"
    );

    let authority = CatalogAuthority::open(catalog_config.clone())?;
    let source = complete_history_source(instrument_id)?;
    let calendar_source = complete_history_calendar_source(instrument_id)?;
    authority.register_source(&source, Timestamp::from_unix_nanos(10))?;
    authority.register_source(&calendar_source, Timestamp::from_unix_nanos(10))?;
    let service = AnalyticalDataService::initialize(
        authority,
        AnalyticalManifestCatalog::open(&location, 8)?,
        paths.artifacts()?.clone(),
        ObjectStoreConfig::try_new(8 * 1024 * 1024, 64, Duration::from_secs(60))?,
    )?;
    let definition_synchronizer = service.market_data_instrument_synchronization();
    let synchronized = definition_synchronizer.synchronize(
        MarketDataInstrumentSynchronization::try_new(vec![definition], 1)?,
        Instant::now() + Duration::from_secs(10),
        &CancellationToken::new(),
    )?;
    assert_eq!((synchronized.inserted(), synchronized.replayed()), (1, 0));
    drop(definition_synchronizer);
    let capture_store = Arc::new(paths.sealed_research_journal_store()?);
    let older_wide = publish_complete_history_fixture(
        &service,
        &source,
        &capture_store,
        complete_history_capture_fixture(
            instrument_id,
            definition_digest,
            "alpaca-aapl-iex-daily-adjusted-history-older-wide-v1",
            COMPLETE_HISTORY_FIRST_BAR_NS,
            COMPLETE_HISTORY_REQUEST_END_NS,
            &[
                COMPLETE_HISTORY_FIRST_BAR_NS,
                COMPLETE_HISTORY_SECOND_BAR_NS,
            ],
            COMPLETE_HISTORY_RECEIVED_AT_NS,
            1,
        )?,
        "alpaca:paper-iex:complete-daily-history:aapl:older-wide:v1",
    )
    .await?;

    let cutoff = Timestamp::from_unix_nanos(i64::MAX - 1);
    let reader = service.analytical_reader();
    let older_current = reader
        .read_canonical_market_bar_history(
            complete_history_request(
                instrument_id,
                COMPLETE_HISTORY_FIRST_BAR_NS,
                COMPLETE_HISTORY_REQUEST_END_NS,
                cutoff,
                None,
            )?,
            Instant::now() + Duration::from_secs(30),
            CancellationToken::new(),
        )
        .await?
        .ok_or("missing complete history after publication")?;
    assert_eq!(older_current.selection().policy_version(), 1);
    assert_eq!(
        older_current.selection().policy_digest().bytes(),
        [
            0x57, 0x56, 0x23, 0x79, 0xa3, 0x91, 0x67, 0xc9, 0x92, 0x42, 0x6f, 0x8d, 0x54, 0x14,
            0x58, 0xd2, 0xcd, 0x42, 0x98, 0xe6, 0xa4, 0x2f, 0x3d, 0x72, 0x0b, 0xec, 0x11, 0x1e,
            0x6f, 0x32, 0x1c, 0x8f,
        ]
    );
    assert_eq!(
        older_current.selection().pinned().manifest(),
        older_wide.manifest()
    );
    assert_eq!(
        older_current.read_receipt().origin_manifest(),
        older_wide.manifest()
    );
    assert_eq!(older_current.bars().len(), 2);
    assert_eq!(
        older_current
            .bars()
            .iter()
            .map(|bar| bar.time_semantics().provider_timestamp())
            .collect::<Vec<_>>(),
        vec![
            Timestamp::from_unix_nanos(COMPLETE_HISTORY_FIRST_BAR_NS),
            Timestamp::from_unix_nanos(COMPLETE_HISTORY_SECOND_BAR_NS),
        ]
    );
    let older_origin_receipt = older_current.selection().receipt();
    assert_eq!(
        older_origin_receipt.origin_manifest(),
        older_wide.manifest()
    );
    assert!(older_origin_receipt.current_research_eligible());
    assert!(!older_origin_receipt.point_in_time_eligible());
    assert!(!older_origin_receipt.backtest_eligible());
    assert!(!older_origin_receipt.retrospective_training_eligible());
    assert_eq!(
        older_origin_receipt.source_id().as_str(),
        "alpaca-basic-iex-market-data"
    );
    assert_eq!(
        older_origin_receipt.provider_instrument_id().as_str(),
        "AAPL"
    );
    assert_eq!(older_origin_receipt.venue_id().as_str(), "iex");
    assert_eq!(older_origin_receipt.feed().as_str(), "iex");
    assert_eq!(older_origin_receipt.interval().as_str(), "1Day");
    assert_eq!(older_origin_receipt.adjustment(), MarketBarAdjustment::All);
    assert_eq!(
        older_origin_receipt.timestamp_basis(),
        BarTimestampBasis::PeriodStart
    );
    assert_eq!(
        older_origin_receipt.session_kind(),
        MarketBarSessionKind::ProviderDefined
    );
    assert_eq!(
        older_origin_receipt.session_ruleset().as_str(),
        "alpaca-v3-iex-utc-range-returned-dates-v2"
    );
    assert_eq!(
        older_origin_receipt.graph_purpose().as_str(),
        "alpaca-iex-historical-bars-and-calendar/v1"
    );
    assert_eq!(older_origin_receipt.currency(), Currency::try_from("USD")?);
    assert_ne!(
        older_origin_receipt.capture_receipt_digest().bytes(),
        [0; 32]
    );
    assert!(
        older_current
            .bars()
            .iter()
            .all(|bar| { bar.context().provenance().quality() == DataQuality::Aggregated })
    );
    assert_eq!(
        older_origin_receipt.expected_provider_timestamps(),
        [
            Timestamp::from_unix_nanos(COMPLETE_HISTORY_FIRST_BAR_NS),
            Timestamp::from_unix_nanos(COMPLETE_HISTORY_SECOND_BAR_NS),
        ]
    );
    let older_origin_receipt_digest = older_origin_receipt.receipt_digest();
    let older_expected_bars = older_current.bars().to_vec();
    let premature_cutoff = Timestamp::from_unix_nanos(
        older_origin_receipt
            .published_at()
            .unix_nanos()
            .checked_sub(1)
            .ok_or("publication cutoff underflow")?,
    );
    assert!(
        reader
            .read_canonical_market_bar_history(
                complete_history_request(
                    instrument_id,
                    COMPLETE_HISTORY_FIRST_BAR_NS,
                    COMPLETE_HISTORY_REQUEST_END_NS,
                    premature_cutoff,
                    None,
                )?,
                Instant::now() + Duration::from_secs(30),
                CancellationToken::new(),
            )
            .await?
            .is_none(),
        "a complete local-first-observed window is unknowable before publication"
    );
    let exact_origin = reader
        .read_canonical_market_bar_history(
            complete_history_request(
                instrument_id,
                COMPLETE_HISTORY_FIRST_BAR_NS,
                COMPLETE_HISTORY_REQUEST_END_NS,
                cutoff,
                Some(older_wide.manifest()),
            )?,
            Instant::now() + Duration::from_secs(30),
            CancellationToken::new(),
        )
        .await?
        .ok_or("exact origin history pin did not resolve")?;
    assert_eq!(
        exact_origin.selection().receipt().receipt_digest(),
        older_origin_receipt_digest
    );
    assert_eq!(exact_origin.bars(), older_expected_bars);
    drop(exact_origin);
    drop(older_current);
    drop(reader);

    let short = publish_complete_history_fixture(
        &service,
        &source,
        &capture_store,
        complete_history_capture_fixture(
            instrument_id,
            definition_digest,
            "alpaca-aapl-iex-daily-adjusted-history-short-v1",
            COMPLETE_HISTORY_SECOND_BAR_NS,
            COMPLETE_HISTORY_REQUEST_END_NS,
            &[COMPLETE_HISTORY_SECOND_BAR_NS],
            COMPLETE_HISTORY_SHORT_RECEIVED_AT_NS,
            2,
        )?,
        "alpaca:paper-iex:complete-daily-history:aapl:short:v1",
    )
    .await?;
    let short_result = service
        .analytical_reader()
        .read_canonical_market_bar_history(
            complete_history_request(
                instrument_id,
                COMPLETE_HISTORY_SECOND_BAR_NS,
                COMPLETE_HISTORY_REQUEST_END_NS,
                cutoff,
                None,
            )?,
            Instant::now() + Duration::from_secs(30),
            CancellationToken::new(),
        )
        .await?
        .ok_or("exact short history window did not resolve")?;
    assert_eq!(
        short_result.selection().pinned().manifest(),
        short.manifest()
    );
    assert_eq!(
        short_result.read_receipt().origin_manifest(),
        short.manifest()
    );
    assert_eq!(short_result.bars().len(), 1);
    assert_eq!(
        short_result.bars()[0].time_semantics().provider_timestamp(),
        Timestamp::from_unix_nanos(COMPLETE_HISTORY_SECOND_BAR_NS)
    );
    let short_origin_receipt_digest = short_result.selection().receipt().receipt_digest();
    drop(short_result);

    let newer_wide = publish_complete_history_fixture(
        &service,
        &source,
        &capture_store,
        complete_history_capture_fixture(
            instrument_id,
            definition_digest,
            "alpaca-aapl-iex-daily-adjusted-history-newer-wide-v1",
            COMPLETE_HISTORY_FIRST_BAR_NS,
            COMPLETE_HISTORY_REQUEST_END_NS,
            &[
                COMPLETE_HISTORY_FIRST_BAR_NS,
                COMPLETE_HISTORY_SECOND_BAR_NS,
            ],
            COMPLETE_HISTORY_NEWER_RECEIVED_AT_NS,
            3,
        )?,
        "alpaca:paper-iex:complete-daily-history:aapl:newer-wide:v1",
    )
    .await?;
    let newer_wide_result = service
        .analytical_reader()
        .read_canonical_market_bar_history(
            complete_history_request(
                instrument_id,
                COMPLETE_HISTORY_FIRST_BAR_NS,
                COMPLETE_HISTORY_REQUEST_END_NS,
                cutoff,
                None,
            )?,
            Instant::now() + Duration::from_secs(30),
            CancellationToken::new(),
        )
        .await?
        .ok_or("newer exact wide history window did not resolve")?;
    assert_eq!(
        newer_wide_result.selection().pinned().manifest(),
        newer_wide.manifest()
    );
    assert_eq!(
        newer_wide_result.read_receipt().origin_manifest(),
        newer_wide.manifest()
    );
    assert_ne!(newer_wide_result.bars(), older_expected_bars);
    let newer_origin_receipt_digest = newer_wide_result.selection().receipt().receipt_digest();
    let newer_expected_bars = newer_wide_result.bars().to_vec();
    drop(newer_wide_result);

    let compacted_newer = compact_complete_history_fixture(
        &service,
        &source,
        newer_wide.manifest(),
        "alpaca:paper-iex:complete-daily-history:aapl:newer-wide:compact:v1",
        Timestamp::from_unix_nanos(40 * COMPLETE_HISTORY_DAY_NS),
    )
    .await?;
    assert_eq!(compacted_newer.manifest().manifest_version(), 2);
    let compacted_newer_manifest = compacted_newer.manifest().clone();
    drop(compacted_newer);

    let compacted_older = compact_complete_history_fixture(
        &service,
        &source,
        older_wide.manifest(),
        "alpaca:paper-iex:complete-daily-history:aapl:older-wide:compact:v1",
        Timestamp::from_unix_nanos(41 * COMPLETE_HISTORY_DAY_NS),
    )
    .await?;
    assert_eq!(compacted_older.manifest().manifest_version(), 2);
    let compacted_older_manifest = compacted_older.manifest().clone();
    drop(compacted_older);

    let inherited_older = service
        .analytical_reader()
        .read_canonical_market_bar_history(
            complete_history_request(
                instrument_id,
                COMPLETE_HISTORY_FIRST_BAR_NS,
                COMPLETE_HISTORY_REQUEST_END_NS,
                cutoff,
                Some(&compacted_older_manifest),
            )?,
            Instant::now() + Duration::from_secs(30),
            CancellationToken::new(),
        )
        .await?
        .ok_or("older compacted generation did not inherit complete-history lineage")?;
    assert_eq!(
        inherited_older.selection().pinned().manifest(),
        &compacted_older_manifest
    );
    assert_eq!(
        inherited_older.read_receipt().origin_manifest(),
        older_wide.manifest()
    );
    assert_eq!(
        inherited_older.selection().receipt().receipt_digest(),
        older_origin_receipt_digest
    );
    assert_eq!(inherited_older.bars(), older_expected_bars);
    drop(inherited_older);

    let selected = service
        .analytical_reader()
        .read_canonical_market_bar_history(
            complete_history_request(
                instrument_id,
                COMPLETE_HISTORY_FIRST_BAR_NS,
                COMPLETE_HISTORY_REQUEST_END_NS,
                cutoff,
                None,
            )?,
            Instant::now() + Duration::from_secs(30),
            CancellationToken::new(),
        )
        .await?
        .ok_or("newer exact origin was shadowed by an older origin descendant")?;
    assert_eq!(
        selected.selection().pinned().manifest(),
        &compacted_newer_manifest
    );
    assert_eq!(
        selected.read_receipt().origin_manifest(),
        newer_wide.manifest()
    );
    assert_eq!(
        selected.selection().receipt().receipt_digest(),
        newer_origin_receipt_digest
    );
    assert_eq!(selected.bars(), newer_expected_bars);
    let selected_selection_digest = selected.selection().selection_digest();
    let selected_result_digest = selected.read_receipt().result_digest();
    let selected_history_content_digest = selected.read_receipt().history_content_digest();
    drop(selected);

    let short_after_compaction = service
        .analytical_reader()
        .read_canonical_market_bar_history(
            complete_history_request(
                instrument_id,
                COMPLETE_HISTORY_SECOND_BAR_NS,
                COMPLETE_HISTORY_REQUEST_END_NS,
                cutoff,
                None,
            )?,
            Instant::now() + Duration::from_secs(30),
            CancellationToken::new(),
        )
        .await?
        .ok_or("short request substituted a different history window")?;
    assert_eq!(
        short_after_compaction.selection().pinned().manifest(),
        short.manifest()
    );
    assert_eq!(
        short_after_compaction
            .selection()
            .receipt()
            .receipt_digest(),
        short_origin_receipt_digest
    );
    assert_eq!(short_after_compaction.bars().len(), 1);
    drop(short_after_compaction);

    let short_first_lineage = service.provider_capture_binding_digests(short.manifest())?;
    assert_eq!(short_first_lineage.len(), 1);
    let short_append = publish_complete_history_fixture(
        &service,
        &source,
        &capture_store,
        complete_history_capture_fixture(
            instrument_id,
            definition_digest,
            "alpaca-aapl-iex-daily-adjusted-history-short-v1",
            COMPLETE_HISTORY_REQUEST_END_NS,
            COMPLETE_HISTORY_REQUEST_END_NS + 2 * COMPLETE_HISTORY_DAY_NS,
            &[COMPLETE_HISTORY_REQUEST_END_NS + COMPLETE_HISTORY_DAY_NS],
            COMPLETE_HISTORY_NEWER_RECEIVED_AT_NS + COMPLETE_HISTORY_DAY_NS,
            4,
        )?,
        "alpaca:paper-iex:complete-daily-history:aapl:short:append:v1",
    )
    .await?;
    let short_cumulative_lineage =
        service.provider_capture_binding_digests(short_append.manifest())?;
    assert_eq!(short_cumulative_lineage.len(), 2);
    assert!(
        short_first_lineage
            .iter()
            .all(|digest| short_cumulative_lineage.contains(digest))
    );
    let short_owned = service
        .generation_owned_provider_capture_evidence(short_append.manifest(), &capture_store)?;
    let short_direct_digests = short_owned
        .objects()
        .iter()
        .flat_map(|object| object.inputs())
        .map(|input| input.binding().binding_digest())
        .collect::<Vec<_>>();
    assert_eq!(short_direct_digests.len(), 1);
    assert!(short_cumulative_lineage.contains(&short_direct_digests[0]));
    assert!(!short_first_lineage.contains(&short_direct_digests[0]));
    assert_eq!(
        short_owned.objects()[0].object(),
        short_append
            .pinned()
            .objects()
            .last()
            .ok_or("missing appended history object")?
    );
    assert_eq!(
        short_owned.objects()[0].object().object().row_count(),
        u64::try_from(
            short_owned.objects()[0].inputs()[0]
                .binding()
                .record_count()
        )?
    );
    let short_append_manifest = short_append.manifest().clone();
    let expected_short_owned = short_owned.clone();
    drop(short_owned);
    drop(short_append);

    assert!(
        service
            .analytical_reader()
            .read_canonical_market_bar_history(
                complete_history_request(
                    instrument_id,
                    COMPLETE_HISTORY_FIRST_BAR_NS - COMPLETE_HISTORY_DAY_NS,
                    COMPLETE_HISTORY_REQUEST_END_NS,
                    cutoff,
                    None,
                )?,
                Instant::now() + Duration::from_secs(30),
                CancellationToken::new(),
            )
            .await?
            .is_none(),
        "an unserved fixed window must remain unavailable"
    );
    drop(newer_wide);
    drop(short);
    drop(older_wide);
    drop(service);

    let restarted = AnalyticalDataService::open(
        CatalogAuthority::open(catalog_config)?,
        AnalyticalManifestCatalog::open(&location, 8)?,
        paths.artifacts()?.clone(),
        ObjectStoreConfig::try_new(8 * 1024 * 1024, 64, Duration::from_secs(60))?,
    )?;
    restarted
        .recover_provider_capture_store(Arc::clone(&capture_store), &CancellationToken::new())
        .await?;
    let restart_cutoff = Timestamp::from_unix_nanos(i64::MAX);
    let replayed = restarted
        .analytical_reader()
        .read_canonical_market_bar_history(
            complete_history_request(
                instrument_id,
                COMPLETE_HISTORY_FIRST_BAR_NS,
                COMPLETE_HISTORY_REQUEST_END_NS,
                restart_cutoff,
                None,
            )?,
            Instant::now() + Duration::from_secs(30),
            CancellationToken::new(),
        )
        .await?
        .ok_or("restart did not recover latest complete history")?;
    assert_eq!(
        replayed.selection().pinned().manifest(),
        &compacted_newer_manifest
    );
    assert_eq!(
        replayed.selection().receipt().receipt_digest(),
        newer_origin_receipt_digest
    );
    assert_ne!(
        replayed.selection().selection_digest(),
        selected_selection_digest
    );
    assert_ne!(
        replayed.read_receipt().result_digest(),
        selected_result_digest
    );
    assert_eq!(
        replayed.read_receipt().history_content_digest(),
        selected_history_content_digest
    );
    assert_eq!(replayed.bars(), newer_expected_bars);
    drop(replayed);
    assert_eq!(
        restarted
            .generation_owned_provider_capture_evidence(&short_append_manifest, &capture_store,)?,
        expected_short_owned
    );

    let ambiguity = rusqlite::Connection::open(location.path())?;
    ambiguity.execute_batch("DROP TRIGGER market_bar_history_publications_immutable_update;")?;
    let older_origin_receipt_digest = older_origin_receipt_digest.bytes();
    assert_eq!(
        ambiguity.execute(
            "UPDATE market_bar_history_publications
             SET provider_instrument_id='AAPL.AMBIGUOUS'
             WHERE publication_receipt_digest=?1",
            params![older_origin_receipt_digest.as_slice()],
        )?,
        1
    );
    drop(ambiguity);
    let ambiguous = restarted
        .analytical_reader()
        .read_canonical_market_bar_history(
            complete_history_request(
                instrument_id,
                COMPLETE_HISTORY_FIRST_BAR_NS,
                COMPLETE_HISTORY_REQUEST_END_NS,
                restart_cutoff,
                None,
            )?,
            Instant::now() + Duration::from_secs(30),
            CancellationToken::new(),
        )
        .await;
    // Selecting the changed row would reach the receipt/row comparison and return CorruptCatalog;
    // this earlier mismatch proves the canonical exact-one-series gate rejected both coordinates.
    assert!(matches!(
        ambiguous,
        Err(AnalyticalReadError::Manifest(
            ManifestCatalogError::MarketBarHistoryMismatch
        ))
    ));
    drop(restarted);

    let revision_corruption = rusqlite::Connection::open(location.path())?;
    revision_corruption.execute_batch("DROP TRIGGER source_revisions_immutable_update;")?;
    assert_eq!(
        revision_corruption.execute(
            "UPDATE source_revisions
             SET metadata_json=json_set(metadata_json, '$.provider', 'corrupt-provider')
             WHERE source_id=?1",
            ["alpaca-iex-calendar-reference"],
        )?,
        1
    );
    drop(revision_corruption);

    let corrupt_restart = AnalyticalDataService::open(
        CatalogAuthority::open(test_catalog_config(location.clone())?)?,
        AnalyticalManifestCatalog::open(&location, 8)?,
        paths.artifacts()?.clone(),
        ObjectStoreConfig::try_new(8 * 1024 * 1024, 64, Duration::from_secs(60))?,
    )?;
    assert!(matches!(
        corrupt_restart
            .generation_owned_provider_capture_evidence(&short_append_manifest, &capture_store),
        Err(IngestError::Catalog(CatalogError::CorruptCatalog))
    ));
    Ok(())
}

#[tokio::test]
async fn fund_nav_schema_pit_publication_and_restart_remain_one_exact_vertical() -> TestResult {
    let directory = tempfile::tempdir()?;
    let paths = LocalPaths::prepare(directory.path().join("fund-nav"))?;
    let location = paths.catalog()?.clone();
    let authority = CatalogAuthority::open(test_catalog_config(location.clone())?)?;
    let source = local_source()?;
    authority.register_source(&source, Timestamp::from_unix_nanos(10))?;
    let batch = fund_nav_extraction_batch()?;
    let payload_digest = extraction_provider_payload_digest(&batch);
    let rights = authority.admit_source_rights(RightsDecisionInput {
        source_id: source.source_id().clone(),
        payload_digest,
        retrieved_at: Timestamp::from_unix_nanos(200),
        basis: RightsBasis::reviewed_terms("https://example.test/fund-nav-terms/v1", digest(71))?,
        authorization_evidence: digest(72),
        authorization_expires_at: Some(Timestamp::from_unix_nanos(i64::MAX)),
        permitted_operations: vec![SourceOperation::Persist],
    })?;
    let reservation = authority.reserve_ingest(
        &IngestIdentity::try_new(
            source.source_id().clone(),
            payload_digest,
            SourceOperation::Persist,
            "fund-nav:share-class:2026-08-10:v1",
        )?,
        &rights,
    )?;
    let service = AnalyticalDataService::initialize(
        authority,
        AnalyticalManifestCatalog::open(&location, 8)?,
        paths.artifacts()?.clone(),
        ObjectStoreConfig::try_new(1024 * 1024, 32, Duration::from_secs(60))?,
    )?;
    let committed = service
        .ingest_with_revision_plan(
            reservation,
            DatasetId::try_from(batch.request().object().dataset().as_str())?,
            batch,
            fund_nav_revision_plan()?,
            CancellationToken::new(),
        )
        .await?;
    let manifest = committed.manifest().clone();
    let parquet = service
        .object_store()
        .read_pinned(committed.pinned(), &CancellationToken::new())?;
    let kinds = parquet
        .first()
        .and_then(|batch| batch.column_by_name("observation_kind"))
        .and_then(|column| column.as_any().downcast_ref::<StringArray>())
        .ok_or("missing Fund NAV kind projection")?;
    assert_eq!(
        kinds.iter().collect::<Vec<_>>(),
        vec![Some("fund_nav"), Some("fund_nav")]
    );
    drop(committed);
    drop(service);

    let restarted = AnalyticalDataService::open(
        CatalogAuthority::open(test_catalog_config(location.clone())?)?,
        AnalyticalManifestCatalog::open(&location, 8)?,
        paths.artifacts()?.clone(),
        ObjectStoreConfig::try_new(1024 * 1024, 32, Duration::from_secs(60))?,
    )?;
    let instrument = fund_nav_instrument()?;
    let reader = restarted.analytical_reader();
    let query_limits = || {
        QueryLimits::try_new(
            16,
            256 * 1024,
            16 * 1024 * 1024,
            1,
            256,
            256,
            Duration::from_secs(10),
        )
    };
    let date = market_squawk_domain::CalendarDate::new(2026, 8, 10)?;
    let latest = reader
        .read_fund_nav_history(
            AnalyticalFundNavReadRequest::try_new(
                manifest.clone(),
                instrument,
                Timestamp::from_unix_nanos(250),
                Some(FundNavDateRange::try_new(date, date)?),
                PointInTimeRevisionMode::LatestKnown,
                AnalyticalFundNavReadLimit::try_new(8)?,
            )?,
            query_limits()?,
            Instant::now() + Duration::from_secs(30),
            CancellationToken::new(),
        )
        .await?;
    assert_eq!(latest.output().manifest(), &manifest);
    assert_eq!(latest.observations().len(), 1);
    assert_eq!(
        latest.observations()[0].context().time().revision().get(),
        2
    );
    assert_eq!(
        latest.observations()[0].value(),
        FundNavValue::Observed(Money::new(
            Decimal::new(10_125, 2),
            Currency::try_from("USD")?,
        ))
    );

    let all_known = reader
        .read_fund_nav_history(
            AnalyticalFundNavReadRequest::try_new(
                manifest,
                instrument,
                Timestamp::from_unix_nanos(250),
                Some(FundNavDateRange::try_new(date, date)?),
                PointInTimeRevisionMode::AllKnown,
                AnalyticalFundNavReadLimit::try_new(8)?,
            )?,
            query_limits()?,
            Instant::now() + Duration::from_secs(30),
            CancellationToken::new(),
        )
        .await?;
    assert_eq!(
        all_known
            .observations()
            .iter()
            .map(|nav| nav.context().time().revision().get())
            .collect::<Vec<_>>(),
        vec![1, 2]
    );
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
) -> Result<
    (
        AnalyticalDataService,
        FeatureDatasetProductionPublisher,
        CommittedDataset,
        CommittedDataset,
    ),
    Box<dyn Error>,
> {
    let location = paths.catalog()?.clone();
    let authority = CatalogAuthority::open(catalog_config)?;
    let membership_source = local_source()?;
    let market_source = market_bar_source()?;
    authority.register_source(&membership_source, Timestamp::from_unix_nanos(10))?;
    authority.register_source(&market_source, Timestamp::from_unix_nanos(10))?;
    authority.register_source(
        &local_source_for("market-squawk.derived")?,
        Timestamp::from_unix_nanos(10),
    )?;

    let membership_batch = dataset_extraction_batch()?;
    let membership_payload = extraction_provider_payload_digest(&membership_batch);
    let membership_rights = authority.admit_source_rights(RightsDecisionInput {
        source_id: membership_source.source_id().clone(),
        payload_digest: membership_payload,
        retrieved_at: Timestamp::from_unix_nanos(15),
        basis: RightsBasis::reviewed_terms("https://example.test/terms/v1", digest(31))?,
        authorization_evidence: digest(32),
        authorization_expires_at: Some(Timestamp::from_unix_nanos(i64::MAX)),
        permitted_operations: vec![SourceOperation::Persist],
    })?;
    authority.admit_research_use_grant(ResearchUseGrantInput::try_new(
        membership_rights.rights_id(),
        ResearchUseSet::try_new(vec![ResearchUse::LocalAnalysis])?,
        digest(33),
        Some(Timestamp::from_unix_nanos(i64::MAX)),
    )?)?;
    let membership_reservation = authority.reserve_ingest(
        &IngestIdentity::try_new(
            membership_source.source_id().clone(),
            membership_payload,
            SourceOperation::Persist,
            "fred:gdp:query-fixture:v1",
        )?,
        &membership_rights,
    )?;

    let market_fixture = closed_price_return_market_bar_fixture()?;
    let market_payload = extraction_provider_payload_digest(&market_fixture.batch);
    let market_rights = authority.admit_source_rights(RightsDecisionInput {
        source_id: market_source.source_id().clone(),
        payload_digest: market_payload,
        retrieved_at: Timestamp::from_unix_nanos(110),
        basis: RightsBasis::reviewed_terms("https://example.test/alpaca-terms/v1", digest(51))?,
        authorization_evidence: digest(52),
        authorization_expires_at: Some(Timestamp::from_unix_nanos(i64::MAX)),
        permitted_operations: vec![SourceOperation::Persist],
    })?;
    authority.admit_research_use_grant(ResearchUseGrantInput::try_new(
        market_rights.rights_id(),
        ResearchUseSet::try_new(vec![ResearchUse::LocalAnalysis])?,
        digest(53),
        Some(Timestamp::from_unix_nanos(i64::MAX)),
    )?)?;
    let market_reservation = authority.reserve_ingest(
        &IngestIdentity::try_new(
            market_source.source_id().clone(),
            market_payload,
            SourceOperation::Persist,
            "alpaca:iex:closed-price-return-fixture:v1",
        )?,
        &market_rights,
    )?;

    let (composition, _onboarding_catalog) =
        AnalyticalDataService::initialize_with_provider_onboarding(
            authority,
            AnalyticalManifestCatalog::open(&location, 8)?,
            paths.artifacts()?.clone(),
            store_config,
        )?;
    let (service, publisher) = composition.into_parts();
    let membership_dataset =
        DatasetId::try_from(membership_batch.request().object().dataset().as_str())?;
    let membership = service
        .ingest(
            membership_reservation,
            membership_dataset,
            membership_batch,
            CancellationToken::new(),
        )
        .await?;
    let ClosedPriceReturnMarketBarFixture {
        batch: market_batch,
        capture_material,
        revision_plan,
        native_rows,
    } = market_fixture;
    let market_dataset = DatasetId::try_from(market_batch.request().object().dataset().as_str())?;
    let raw_store = paths.sealed_research_journal_store()?;
    let (expectation, request) = capture_material.into_whole_seal_parts();
    let token = expectation
        .try_rejoin(request.seal(&raw_store)?)?
        .try_into_whole()?;
    let mut native = ProviderNativeLineageBatchBuilder::try_new(
        ProviderNativeLineageImplementation::AlpacaHistoricalBarV1,
        &market_batch,
    )?;
    for row in &native_rows {
        native.try_push(row)?;
    }
    let native = native.finish()?;
    let binding = SealedProviderCaptureBinding::try_whole(
        token,
        market_batch,
        native,
        vec![0; native_rows.len()],
    )?;
    let market_bars = service
        .ingest_provider_publication(
            market_reservation,
            market_dataset,
            ProviderPublicationInput::try_new(binding, revision_plan)?,
            CancellationToken::new(),
        )
        .await?;
    Ok((service, publisher, membership, market_bars))
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
    let payload_digest = extraction_provider_payload_digest(&batch);
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
    let analytical_dataset = DatasetId::try_from(batch.request().object().dataset().as_str())?;
    let committed = service
        .ingest(
            reservation,
            analytical_dataset,
            batch,
            CancellationToken::new(),
        )
        .await?;
    Ok((service, committed))
}

fn extraction_batch() -> Result<ExtractionBatch, Box<dyn Error>> {
    extraction_batch_with_membership(false)
}

fn provider_macro_plan_input(
    raw_store: &SealedResearchJournalStore,
    analytical_dataset: DatasetId,
) -> Result<ProviderMacroPlanPublicationInput, Box<dyn Error>> {
    const CHUNK_COUNT: usize = 3;
    let source_id = SourceId::try_from("fred-local-fixture")?;
    let metadata_revision = MetadataRevision::new(SourceIdentifier::try_from("revision-1")?);
    let provider_dataset = SourceIdentifier::try_from("gdp-2026q1")?;
    let mut chunks = Vec::new();
    chunks.try_reserve_exact(CHUNK_COUNT)?;
    for chunk_ordinal in 0..CHUNK_COUNT {
        let received_at = Timestamp::from_unix_nanos(
            300_i64
                .checked_add(i64::try_from(chunk_ordinal)?)
                .ok_or("provider macro capture time overflow")?,
        );
        let body = Bytes::from(format!("{{\"chunk\":{chunk_ordinal}}}"));
        let body_digest = EvidenceDigest::new(
            DigestAlgorithm::Sha256,
            Sha256::digest(body.as_ref()).into(),
        );
        let capture = ProviderCaptureSetReceipt::try_new(
            source_id.clone(),
            metadata_revision.clone(),
            provider_dataset.clone(),
            digest(u8::try_from(180 + chunk_ordinal)?),
            ProviderCaptureTerminalDisposition::StandaloneResponse,
            vec![ProviderCapturePageReceipt::try_new(
                0,
                digest(u8::try_from(190 + chunk_ordinal)?),
                None,
                None,
                200,
                u64::try_from(body.len())?,
                body_digest,
                received_at,
            )?],
        )?;
        let capture_material = ProviderCaptureMaterial::try_new(
            capture,
            vec![RawCaptureRecord::try_new_live(
                Uuid::from_u128(10_000 + u128::try_from(chunk_ordinal)? * 2),
                Arc::from(source_id.as_str()),
                Uuid::from_u128(10_001 + u128::try_from(chunk_ordinal)? * 2),
                Some(0),
                None,
                DateTime::<Utc>::from_timestamp_nanos(received_at.unix_nanos()),
                body,
            )?],
        )?;
        let discovery = DiscoveryRequest::try_new(
            provider_dataset.clone(),
            Some(Timestamp::from_unix_nanos(90)),
            NonZeroU16::MIN,
            Timestamp::from_unix_nanos(1_000),
        )?;
        let object = SourceObject::try_new_with_capture_identity(
            source_id.clone(),
            metadata_revision.clone(),
            &discovery,
            provider_dataset.clone(),
            SourceIdentifier::try_from("application-json")?,
            ExactPayloadEvidence::from_content_digest(capture_material.receipt().content_digest()),
            SourceObjectCaptureIdentity::try_from_capture(capture_material.receipt())?,
            EffectiveInterval::new(Timestamp::from_unix_nanos(0), None)?,
            Some(Timestamp::from_unix_nanos(100)),
            SourceAvailabilityEvidence::Observed {
                available_at: Timestamp::from_unix_nanos(100),
                evidence: SourceIdentifier::try_from("fred-release")?,
            },
            Some(capture_material.receipt().total_body_bytes()),
        )?;
        let request = ExtractionRequest::try_new(
            object,
            NonZeroU32::MIN,
            NonZeroU64::new(1024 * 1024).ok_or("nonzero provider macro byte limit")?,
            Timestamp::from_unix_nanos(1_000),
        )?;
        let payload = serde_json::to_vec(&macro_observation(Some(chunk_ordinal))?)?;
        let record = ExtractionRecord::try_new(
            &request,
            SourceIdentifier::try_from("market-squawk-research-v3")?,
            ExactPayloadEvidence::from_content_digest(EvidenceDigest::new(
                DigestAlgorithm::Sha256,
                Sha256::digest(&payload).into(),
            )),
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
        let batch = ExtractionBatch::try_new(&request, vec![record])?
            .try_bind_provider_capture(capture_material.receipt())?;
        let (expectation, seal_request) = capture_material.into_whole_seal_parts();
        let token = expectation
            .try_rejoin(seal_request.seal(raw_store)?)?
            .try_into_whole()?;
        let mut native = ProviderNativeLineageBatchBuilder::try_new(
            ProviderNativeLineageImplementation::FredAlfredSeriesObservationsV1,
            &batch,
        )?;
        native.try_set_batch_sidecar(&serde_json::json!({
            "family": "fred_alfred_series_observations",
            "chunk": chunk_ordinal,
        }))?;
        native.try_push(&serde_json::json!({
            "raw_value": "1234.56",
            "chunk": chunk_ordinal,
        }))?;
        let native = native.finish()?;
        let sidecar = native
            .batch_sidecar()
            .ok_or("provider macro native sidecar is absent")?;
        let semantics = ProviderMacroPlanSemantics::try_new(
            SourceIdentifier::try_from("fred-alfred-page-semantics-v1")?,
            native.schema().fingerprint(),
            sidecar.semantic_payload_digest(),
            sidecar.semantic_payload().to_vec().into_boxed_slice(),
        )?;
        let binding = SealedProviderCaptureBinding::try_whole(token, batch, native, vec![0])?;
        let revisions = ExtractionRevisionPlan::try_new_with_native_lineage(vec![
            ExtractionRevisionEvidence::provider_supplied(
                b"revision-1",
                ObservedProviderOrder::try_new(
                    ResearchTemporalCoordinate::exact(Timestamp::from_unix_nanos(100)),
                    b"revision-1",
                )?,
            )?,
        ])?;
        chunks.push(ProviderMacroPlanChunkInput::try_new(
            u16::try_from(chunk_ordinal)?,
            u16::try_from(CHUNK_COUNT)?,
            digest(u8::try_from(200 + chunk_ordinal)?),
            digest(210),
            semantics,
            binding,
            revisions,
        )?);
    }
    Ok(ProviderMacroPlanPublicationInput::try_new(
        analytical_dataset,
        digest(211),
        u64::try_from(CHUNK_COUNT)?,
        chunks,
    )?)
}

struct ClosedPriceReturnMarketBarFixture {
    batch: ExtractionBatch,
    capture_material: ProviderCaptureMaterial,
    revision_plan: ExtractionRevisionPlan,
    native_rows: Vec<serde_json::Value>,
}

fn closed_price_return_market_bar_fixture()
-> Result<ClosedPriceReturnMarketBarFixture, Box<dyn Error>> {
    let source_id = SourceId::try_from("alpaca-historical-fixture")?;
    let metadata_revision = MetadataRevision::new(SourceIdentifier::try_from("alpaca-revision-1")?);
    let dataset = SourceIdentifier::try_from("alpaca-iex-bars-closed-price-return-fixture")?;
    let received_at = Timestamp::from_unix_nanos(110);
    let body = Bytes::from_static(
        br#"{"bars":[{"symbol":"AAPL","t":80,"c":10000},{"symbol":"AAPL","t":90,"c":10000},{"symbol":"AAPL","t":100,"c":10000}],"next_page_token":null}"#,
    );
    let body_digest = EvidenceDigest::new(DigestAlgorithm::Sha256, Sha256::digest(&body).into());
    let capture = ProviderCaptureSetReceipt::try_new(
        source_id.clone(),
        metadata_revision.clone(),
        dataset.clone(),
        digest(54),
        ProviderCaptureTerminalDisposition::ExhaustedWithoutNextPage,
        vec![ProviderCapturePageReceipt::try_new(
            0,
            digest(55),
            None,
            None,
            200,
            u64::try_from(body.len())?,
            body_digest,
            received_at,
        )?],
    )?;
    let capture_material = ProviderCaptureMaterial::try_new(
        capture,
        vec![RawCaptureRecord::try_new_live(
            Uuid::from_u128(9_001),
            Arc::from(source_id.as_str()),
            Uuid::from_u128(9_002),
            Some(0),
            None,
            DateTime::<Utc>::from_timestamp_nanos(received_at.unix_nanos()),
            body,
        )?],
    )?;
    let discovery = DiscoveryRequest::try_new(
        SourceIdentifier::try_from("alpaca-iex-bars-closed-price-return-fixture")?,
        None,
        NonZeroU16::MIN,
        Timestamp::from_unix_nanos(1_000),
    )?;
    let object = SourceObject::try_new_with_capture_identity(
        source_id,
        metadata_revision,
        &discovery,
        SourceIdentifier::try_from("alpaca-iex-bars:closed-price-return-fixture")?,
        SourceIdentifier::try_from("application-json")?,
        ExactPayloadEvidence::from_content_digest(capture_material.receipt().content_digest()),
        SourceObjectCaptureIdentity::try_from_capture(capture_material.receipt())?,
        EffectiveInterval::new(Timestamp::from_unix_nanos(80), None)?,
        None,
        SourceAvailabilityEvidence::LocalFirstObserved {
            observed_at: received_at,
        },
        Some(4096),
    )?;
    let request = ExtractionRequest::try_new(
        object,
        NonZeroU32::new(3).ok_or("nonzero market-bar record limit")?,
        NonZeroU64::new(1024 * 1024).ok_or("nonzero market-bar byte limit")?,
        Timestamp::from_unix_nanos(1_000),
    )?;
    let instrument = dataset_membership_instrument()?;
    let mut records = Vec::new();
    let mut native_rows = Vec::new();
    records.try_reserve_exact(3)?;
    native_rows.try_reserve_exact(3)?;
    for (ordinal, (effective, available, close_cents, source_record, revision)) in [
        (
            80_i64,
            85_i64,
            10_000_i64,
            "closed-bar-80",
            "closed-bar-80-v1",
        ),
        (
            90_i64,
            95_i64,
            10_000_i64,
            "closed-bar-90",
            "closed-bar-90-v1",
        ),
        (
            100_i64,
            105_i64,
            10_000_i64,
            "closed-bar-100",
            "closed-bar-100-v1",
        ),
    ]
    .into_iter()
    .enumerate()
    {
        let observation = market_bar_observation(
            instrument,
            "AAPL",
            effective,
            available,
            source_record,
            close_cents,
        )?;
        let payload = serde_json::to_vec(&observation)?;
        records.push(ExtractionRecord::try_new(
            &request,
            SourceIdentifier::try_from("market-squawk-research-v3")?,
            ExactPayloadEvidence::from_content_digest(EvidenceDigest::new(
                DigestAlgorithm::Sha256,
                Sha256::digest(&payload).into(),
            )),
            Timestamp::from_unix_nanos(effective),
            None,
            SourceAvailabilityEvidence::LocalFirstObserved {
                observed_at: Timestamp::from_unix_nanos(available),
            },
            SourceIdentifier::try_from(revision)?,
            None,
            payload.into(),
        )?);
        native_rows.push(serde_json::json!({
            "symbol": "AAPL",
            "timestamp_ns": effective,
            "close_cents": close_cents,
            "source_version": revision,
            "feed": "iex",
            "timeframe": "1Day",
            "adjustment": "all",
            "provider_row_ordinal": ordinal,
        }));
    }
    let batch = ExtractionBatch::try_new(&request, records)?
        .try_bind_provider_capture(capture_material.receipt())?;
    Ok(ClosedPriceReturnMarketBarFixture {
        batch,
        capture_material,
        revision_plan: closed_price_return_market_bar_revision_plan()?,
        native_rows,
    })
}

fn closed_price_return_market_bar_revision_plan() -> Result<ExtractionRevisionPlan, Box<dyn Error>>
{
    let evidence = [
        ("closed-bar-80-v1", 85_i64),
        ("closed-bar-90-v1", 95_i64),
        ("closed-bar-100-v1", 105_i64),
    ]
    .into_iter()
    .map(|(revision, observed_at)| {
        ExtractionRevisionEvidence::provider_supplied(
            revision.as_bytes(),
            ObservedProviderOrder::try_new(
                ResearchTemporalCoordinate::exact(Timestamp::from_unix_nanos(observed_at)),
                revision.as_bytes(),
            )?,
        )
        .map_err(Into::into)
    })
    .collect::<Result<Vec<_>, Box<dyn Error>>>()?;
    Ok(ExtractionRevisionPlan::try_new_with_native_lineage(
        evidence,
    )?)
}

struct SecResearchCaptureFixture {
    batch: ExtractionBatch,
    capture_material: ProviderCaptureMaterial,
    revision_plan: ExtractionRevisionPlan,
    native_rows: Vec<serde_json::Value>,
}

fn sec_research_capture_fixture(base_ns: i64) -> Result<SecResearchCaptureFixture, Box<dyn Error>> {
    let source_id = SourceId::try_from("sec-edgar")?;
    let metadata_revision =
        MetadataRevision::new(SourceIdentifier::try_from("sec-edgar-contract-v1")?);
    let dataset = SourceIdentifier::try_from("sec-submissions-exact-restart")?;
    let received_at = Timestamp::from_unix_nanos(base_ns);
    let body = Bytes::from_static(
        b"{\"cik\":\"0000320193\",\"filings\":{\"recent\":\"bounded-fixture\"}}",
    );
    let body_digest = EvidenceDigest::new(DigestAlgorithm::Sha256, Sha256::digest(&body).into());
    let capture = ProviderCaptureSetReceipt::try_new(
        source_id.clone(),
        metadata_revision.clone(),
        dataset.clone(),
        digest(213),
        ProviderCaptureTerminalDisposition::StandaloneResponse,
        vec![ProviderCapturePageReceipt::try_new(
            0,
            digest(214),
            None,
            None,
            200,
            u64::try_from(body.len())?,
            body_digest,
            received_at,
        )?],
    )?;
    let capture_material = ProviderCaptureMaterial::try_new(
        capture,
        vec![RawCaptureRecord::try_new_live(
            Uuid::from_u128(8_001),
            Arc::from(source_id.as_str()),
            Uuid::from_u128(8_002),
            Some(0),
            None,
            DateTime::<Utc>::from_timestamp_nanos(base_ns),
            body,
        )?],
    )?;
    let discovery = DiscoveryRequest::try_new(
        dataset.clone(),
        None,
        NonZeroU16::MIN,
        Timestamp::from_unix_nanos(
            base_ns
                .checked_add(1_000_000)
                .ok_or("SEC discovery timestamp overflow")?,
        ),
    )?;
    let object = SourceObject::try_new_with_capture_identity(
        source_id,
        metadata_revision,
        &discovery,
        dataset.clone(),
        SourceIdentifier::try_from("application/json")?,
        ExactPayloadEvidence::from_content_digest(capture_material.receipt().content_digest()),
        SourceObjectCaptureIdentity::try_from_capture(capture_material.receipt())?,
        EffectiveInterval::new(Timestamp::from_unix_nanos(base_ns - 10_000_000), None)?,
        None,
        SourceAvailabilityEvidence::Observed {
            available_at: received_at,
            evidence: SourceIdentifier::try_from("sec-edgar-submissions-response")?,
        },
        Some(capture_material.receipt().total_body_bytes()),
    )?;
    let request = ExtractionRequest::try_new(
        object,
        NonZeroU32::new(5).ok_or("nonzero SEC record ceiling")?,
        NonZeroU64::new(1024 * 1024).ok_or("nonzero SEC byte ceiling")?,
        Timestamp::from_unix_nanos(
            base_ns
                .checked_add(5_000_000_000)
                .ok_or("SEC extraction deadline overflow")?,
        ),
    )?;
    let future_ns = i64::MAX - 10_000_000;
    let rows = [
        (
            "0000320193-26-000001",
            "10-K",
            base_ns - 10_000_000,
            base_ns,
            base_ns,
            base_ns + 1_000_000,
            "sec-filing-current-v1",
        ),
        (
            "0000320193-26-000002",
            "8-K",
            base_ns - 9_000_000,
            base_ns,
            future_ns - 1_000_000,
            future_ns,
            "sec-filing-future-received-v1",
        ),
        (
            "0000320193-26-000003",
            "10-Q",
            base_ns - 8_000_000,
            base_ns,
            base_ns,
            future_ns,
            "sec-filing-future-ingested-v1",
        ),
        (
            "0000320193-26-000004",
            "424B5",
            base_ns - 7_000_000,
            future_ns - 1_000_000,
            base_ns,
            future_ns,
            "sec-filing-future-available-v1",
        ),
        (
            "0000320193-26-000005",
            "S-8",
            future_ns,
            base_ns,
            base_ns,
            base_ns + 1_000_000,
            "sec-filing-future-effective-v1",
        ),
    ];
    let mut records = Vec::new();
    let mut revision_evidence = Vec::new();
    let mut native_rows = Vec::new();
    records.try_reserve_exact(rows.len())?;
    revision_evidence.try_reserve_exact(rows.len())?;
    native_rows.try_reserve_exact(rows.len())?;
    for (ordinal, (accession, form, effective, available, received, ingested, source_version)) in
        rows.into_iter().enumerate()
    {
        let observation = sec_filing_observation(
            accession,
            form,
            effective,
            available,
            received,
            ingested,
            body_digest,
            source_version,
        )?;
        let payload = serde_json::to_vec(&observation)?;
        records.push(ExtractionRecord::try_new_with_time(
            &request,
            SourceIdentifier::try_from("market-squawk-research-v3")?,
            ExactPayloadEvidence::from_content_digest(EvidenceDigest::new(
                DigestAlgorithm::Sha256,
                Sha256::digest(&payload).into(),
            )),
            ResearchTemporalCoordinate::exact(Timestamp::from_unix_nanos(effective)),
            Some(ResearchTemporalCoordinate::exact(
                Timestamp::from_unix_nanos(available),
            )),
            SourceAvailabilityEvidence::Observed {
                available_at: Timestamp::from_unix_nanos(available),
                evidence: SourceIdentifier::try_from("sec-filing-publication-clock")?,
            },
            SourceIdentifier::try_from(source_version)?,
            None,
            payload.into(),
        )?);
        revision_evidence.push(ExtractionRevisionEvidence::provider_supplied(
            source_version.as_bytes(),
            ObservedProviderOrder::try_new(
                ResearchTemporalCoordinate::exact(Timestamp::from_unix_nanos(
                    base_ns
                        .checked_add(i64::try_from(ordinal)?)
                        .ok_or("SEC provider order overflow")?,
                )),
                source_version.as_bytes(),
            )?,
        )?);
        native_rows.push(serde_json::json!({
            "cik": "0000320193",
            "accession": accession,
            "form": form,
            "row_ordinal": ordinal,
            "source_version": source_version,
        }));
    }
    let batch = ExtractionBatch::try_new(&request, records)?
        .try_bind_provider_capture(capture_material.receipt())?;
    Ok(SecResearchCaptureFixture {
        batch,
        capture_material,
        revision_plan: ExtractionRevisionPlan::try_new_with_native_lineage(revision_evidence)?,
        native_rows,
    })
}

#[allow(
    clippy::too_many_arguments,
    reason = "the restart fixture keeps all four point-in-time clocks explicit"
)]
fn sec_filing_observation(
    accession: &str,
    form: &str,
    effective_ns: i64,
    available_ns: i64,
    received_ns: i64,
    ingested_ns: i64,
    body_digest: EvidenceDigest,
    source_record: &str,
) -> Result<ResearchObservation, Box<dyn Error>> {
    let context = ResearchContext::new(
        ResearchProvenance::try_new(ResearchProvenanceInput {
            source_id: SourceId::try_from("sec-edgar")?,
            instrument_id: Some(dataset_membership_instrument()?),
            venue_id: None,
            source_identifier: SourceIdentifier::try_from(source_record)?,
            source_timestamp: None,
            received_at: Timestamp::from_unix_nanos(received_ns),
            ingested_at: Timestamp::from_unix_nanos(ingested_ns),
            quality: DataQuality::OfficialDelayed,
            payload_reference: PayloadReference::ContentHash(
                market_squawk_domain::PayloadHash::new(
                    DigestAlgorithm::Sha256,
                    body_digest.bytes(),
                ),
            ),
            availability: DomainAvailabilityEvidence::evidenced(
                Timestamp::from_unix_nanos(available_ns),
                SourceIdentifier::try_from("sec-filing-publication-clock")?,
            ),
        })?,
        ResearchTime::new(
            Timestamp::from_unix_nanos(effective_ns),
            Some(Timestamp::from_unix_nanos(available_ns)),
            RevisionNumber::new(1)?,
            None,
        )?,
    )?;
    Ok(ResearchObservation::Filing(FilingObservation::new(
        context,
        SourceIdentifier::try_from(form)?,
        SourceIdentifier::try_from(accession)?,
    )?))
}

struct CompleteHistoryCaptureFixture {
    batch: ExtractionBatch,
    capture_material: ProviderCaptureMaterial,
    revision_plan: ExtractionRevisionPlan,
    native_rows: Vec<serde_json::Value>,
    received_at: Timestamp,
}

#[allow(
    clippy::too_many_arguments,
    reason = "the typed request keeps its exact window and stable market-bar series coordinates"
)]
fn complete_history_request(
    instrument_id: InstrumentId,
    requested_start_ns: i64,
    requested_end_ns: i64,
    knowledge_cutoff: Timestamp,
    exact_manifest: Option<&DatasetManifestRef>,
) -> Result<CanonicalMarketBarHistoryRequest, ManifestCatalogError> {
    let requested_start = Timestamp::from_unix_nanos(requested_start_ns);
    let requested_end = Timestamp::from_unix_nanos(requested_end_ns);
    let policy = MarketHistorySelectionPolicy::COMPLETE_DAILY_ADJUSTED_V1;
    match exact_manifest {
        Some(manifest) => CanonicalMarketBarHistoryRequest::try_exact(
            instrument_id,
            requested_start,
            requested_end,
            policy,
            knowledge_cutoff,
            manifest.clone(),
        ),
        None => CanonicalMarketBarHistoryRequest::try_latest(
            instrument_id,
            requested_start,
            requested_end,
            policy,
            knowledge_cutoff,
        ),
    }
}

async fn publish_complete_history_fixture(
    service: &AnalyticalDataService,
    source: &SourceMetadata,
    capture_store: &market_squawk_platform::SealedResearchJournalStore,
    fixture: CompleteHistoryCaptureFixture,
    ingest_key: &str,
) -> Result<CommittedDataset, Box<dyn Error>> {
    let CompleteHistoryCaptureFixture {
        batch,
        capture_material,
        revision_plan,
        native_rows,
        received_at,
    } = fixture;
    let payload_digest = extraction_provider_payload_digest(&batch);
    let identity = IngestIdentity::try_new(
        source.source_id().clone(),
        payload_digest,
        SourceOperation::Persist,
        ingest_key,
    )?;
    let cancellation = CancellationToken::new();
    let reservation = service
        .reserve_source_ingest(
            source,
            Timestamp::from_unix_nanos(10),
            RightsDecisionInput {
                source_id: source.source_id().clone(),
                payload_digest,
                retrieved_at: received_at,
                basis: RightsBasis::reviewed_terms(
                    "https://example.test/alpaca-paper-iex-history-terms/v1",
                    digest(111),
                )?,
                authorization_evidence: digest(112),
                authorization_expires_at: Some(Timestamp::from_unix_nanos(i64::MAX)),
                permitted_operations: vec![SourceOperation::Persist],
            },
            &identity,
            &cancellation,
        )
        .await?;
    let analytical_dataset = DatasetId::try_from(batch.request().object().dataset().as_str())?;
    let (expectation, request) = capture_material.into_whole_seal_parts();
    let sealed = request.seal(capture_store)?;
    let token = expectation.try_rejoin(sealed)?.try_into_whole()?;
    let mut native = ProviderNativeLineageBatchBuilder::try_new(
        ProviderNativeLineageImplementation::AlpacaHistoricalBarV1,
        &batch,
    )?;
    for row in &native_rows {
        native.try_push(row)?;
    }
    let native = native.finish()?;
    let binding =
        SealedProviderCaptureBinding::try_whole(token, batch, native, vec![0; native_rows.len()])?;
    Ok(service
        .ingest_provider_publication(
            reservation,
            analytical_dataset,
            ProviderPublicationInput::try_new(binding, revision_plan)?,
            cancellation,
        )
        .await?)
}

async fn compact_complete_history_fixture(
    service: &AnalyticalDataService,
    source: &SourceMetadata,
    manifest: &DatasetManifestRef,
    ingest_key: &str,
    retrieved_at: Timestamp,
) -> Result<CommittedDataset, Box<dyn Error>> {
    let compaction = CompactionRequest::new(manifest.clone());
    let payload_digest = compaction.payload_digest();
    let identity = IngestIdentity::try_new(
        source.source_id().clone(),
        payload_digest,
        SourceOperation::Persist,
        ingest_key,
    )?;
    let cancellation = CancellationToken::new();
    let reservation = service
        .reserve_source_ingest(
            source,
            Timestamp::from_unix_nanos(10),
            RightsDecisionInput {
                source_id: source.source_id().clone(),
                payload_digest,
                retrieved_at,
                basis: RightsBasis::reviewed_terms(
                    "https://example.test/alpaca-paper-iex-history-terms/v1",
                    digest(111),
                )?,
                authorization_evidence: digest(112),
                authorization_expires_at: Some(Timestamp::from_unix_nanos(i64::MAX)),
                permitted_operations: vec![SourceOperation::Persist],
            },
            &identity,
            &cancellation,
        )
        .await?;
    Ok(service
        .compact(reservation, compaction, cancellation)
        .await?)
}

#[allow(
    clippy::too_many_arguments,
    reason = "the fixture varies exact window, immutable dataset, clocks, and content independently"
)]
fn complete_history_capture_fixture(
    instrument_id: InstrumentId,
    instrument_revision_digest: EvidenceDigest,
    dataset: &str,
    requested_start_ns: i64,
    requested_end_ns: i64,
    expected_provider_timestamps_ns: &[i64],
    received_at_ns: i64,
    variant: u8,
) -> Result<CompleteHistoryCaptureFixture, Box<dyn Error>> {
    if expected_provider_timestamps_ns.is_empty() || variant == 0 {
        return Err("complete-history fixture requires timestamps and a nonzero variant".into());
    }
    let source_id = SourceId::try_from("alpaca-basic-iex-market-data")?;
    let metadata_revision = MetadataRevision::new(SourceIdentifier::try_from(
        "alpaca-history-source-revision-v1",
    )?);
    let dataset = SourceIdentifier::try_from(dataset)?;
    let bar_received_at = Timestamp::from_unix_nanos(received_at_ns);
    let bar_body = Bytes::from(
        format!(
            "{{\"variant\":{variant},\"requested_start_ns\":{requested_start_ns},\"requested_end_ns\":{requested_end_ns},\"bars\":{expected_provider_timestamps_ns:?},\"next_page_token\":null}}"
        )
        .into_bytes(),
    );
    let bar_body_digest =
        EvidenceDigest::new(DigestAlgorithm::Sha256, Sha256::digest(&bar_body).into());
    let digest_base = 140_u8
        .checked_add(
            variant
                .checked_mul(8)
                .ok_or("complete-history digest variant overflow")?,
        )
        .ok_or("complete-history digest base overflow")?;
    let uuid_base = 1_000_u128
        .checked_add(u128::from(variant).saturating_mul(10))
        .ok_or("complete-history UUID base overflow")?;
    let bar_capture = ProviderCaptureSetReceipt::try_new(
        source_id.clone(),
        metadata_revision.clone(),
        dataset.clone(),
        digest(digest_base),
        ProviderCaptureTerminalDisposition::ExhaustedWithoutNextPage,
        vec![ProviderCapturePageReceipt::try_new(
            0,
            digest(digest_base + 1),
            None,
            None,
            200,
            u64::try_from(bar_body.len())?,
            bar_body_digest,
            bar_received_at,
        )?],
    )?;
    let bar_material = ProviderCaptureMaterial::try_new(
        bar_capture,
        vec![RawCaptureRecord::try_new_live(
            Uuid::from_u128(uuid_base),
            Arc::from(source_id.as_str()),
            Uuid::from_u128(uuid_base + 1),
            Some(0),
            None,
            DateTime::<Utc>::from_timestamp_nanos(bar_received_at.unix_nanos()),
            bar_body,
        )?],
    )?;

    let calendar_received_at = Timestamp::from_unix_nanos(
        received_at_ns
            .checked_add(1)
            .ok_or("calendar receive timestamp overflow")?,
    );
    let calendar_body = Bytes::from(
        format!(
            "{{\"variant\":{variant},\"expected_provider_timestamps_ns\":{expected_provider_timestamps_ns:?}}}"
        )
        .into_bytes(),
    );
    let calendar_body_digest = EvidenceDigest::new(
        DigestAlgorithm::Sha256,
        Sha256::digest(&calendar_body).into(),
    );
    let calendar_source_id = SourceId::try_from("alpaca-iex-calendar-reference")?;
    let calendar_metadata_revision = MetadataRevision::new(SourceIdentifier::try_from(
        "alpaca-iex-calendar-reference-revision-v1",
    )?);
    let calendar_capture = ProviderCaptureSetReceipt::try_new(
        calendar_source_id.clone(),
        calendar_metadata_revision,
        dataset.clone(),
        digest(digest_base + 2),
        ProviderCaptureTerminalDisposition::StandaloneResponse,
        vec![ProviderCapturePageReceipt::try_new(
            0,
            digest(digest_base + 3),
            None,
            None,
            200,
            u64::try_from(calendar_body.len())?,
            calendar_body_digest,
            calendar_received_at,
        )?],
    )?;
    let calendar_material = ProviderCaptureMaterial::try_new(
        calendar_capture,
        vec![RawCaptureRecord::try_new_live(
            Uuid::from_u128(uuid_base + 2),
            Arc::from(calendar_source_id.as_str()),
            Uuid::from_u128(uuid_base + 3),
            Some(0),
            None,
            DateTime::<Utc>::from_timestamp_nanos(calendar_received_at.unix_nanos()),
            calendar_body,
        )?],
    )?;

    let discovery = DiscoveryRequest::try_new(
        dataset.clone(),
        None,
        NonZeroU16::MIN,
        Timestamp::from_unix_nanos(
            received_at_ns
                .checked_add(10)
                .ok_or("history discovery timestamp overflow")?,
        ),
    )?;
    let object = SourceObject::try_new_with_capture_identity(
        source_id.clone(),
        metadata_revision,
        &discovery,
        dataset.clone(),
        SourceIdentifier::try_from("application/vnd.alpaca.iex-bars+json")?,
        ExactPayloadEvidence::from_content_digest(bar_material.receipt().content_digest()),
        SourceObjectCaptureIdentity::try_from_capture(bar_material.receipt())?,
        EffectiveInterval::new(
            Timestamp::from_unix_nanos(requested_start_ns),
            Some(Timestamp::from_unix_nanos(requested_end_ns)),
        )?,
        None,
        SourceAvailabilityEvidence::LocalFirstObserved {
            observed_at: bar_received_at,
        },
        Some(bar_material.receipt().total_body_bytes()),
    )?;
    let request = ExtractionRequest::try_new(
        object,
        NonZeroU32::new(u32::try_from(expected_provider_timestamps_ns.len())?)
            .ok_or("nonzero complete-history record limit")?,
        NonZeroU64::new(1024 * 1024).ok_or("nonzero complete-history byte limit")?,
        Timestamp::from_unix_nanos(
            received_at_ns
                .checked_add(20)
                .ok_or("history extraction deadline overflow")?,
        ),
    )?;
    let mut records = Vec::new();
    records.try_reserve_exact(expected_provider_timestamps_ns.len())?;
    let mut revision_evidence = Vec::new();
    revision_evidence.try_reserve_exact(expected_provider_timestamps_ns.len())?;
    let mut native_rows = Vec::new();
    native_rows.try_reserve_exact(expected_provider_timestamps_ns.len())?;
    for (ordinal, provider_timestamp_ns) in
        expected_provider_timestamps_ns.iter().copied().enumerate()
    {
        let source_version = format!("alpaca-aapl-{variant}-day-{}-v1", ordinal + 1);
        let observation = complete_history_market_bar_observation(
            instrument_id,
            provider_timestamp_ns,
            bar_received_at,
            bar_body_digest,
            &source_version,
            i64::from(variant)
                .checked_mul(100)
                .and_then(|offset| offset.checked_add(i64::try_from(ordinal).ok()?))
                .ok_or("complete-history price variant overflow")?,
        )?;
        let payload = serde_json::to_vec(&observation)?;
        records.push(ExtractionRecord::try_new(
            &request,
            SourceIdentifier::try_from("market-squawk-research-v3")?,
            ExactPayloadEvidence::from_content_digest(EvidenceDigest::new(
                DigestAlgorithm::Sha256,
                Sha256::digest(&payload).into(),
            )),
            Timestamp::from_unix_nanos(provider_timestamp_ns),
            None,
            SourceAvailabilityEvidence::LocalFirstObserved {
                observed_at: bar_received_at,
            },
            SourceIdentifier::try_from(source_version.as_str())?,
            None,
            payload.into(),
        )?);
        revision_evidence.push(ExtractionRevisionEvidence::provider_supplied(
            source_version.as_bytes(),
            ObservedProviderOrder::try_new(
                ResearchTemporalCoordinate::exact(bar_received_at),
                source_version.as_bytes(),
            )?,
        )?);
        native_rows.push(serde_json::json!({
            "symbol": "AAPL",
            "timestamp_ns": provider_timestamp_ns,
            "source_version": source_version,
            "feed": "iex",
            "timeframe": "1Day",
            "adjustment": "all",
            "variant": variant,
            "provider_row_ordinal": ordinal,
        }));
    }
    let batch = ExtractionBatch::try_new(&request, records)?;
    let semantic = complete_history_semantic(
        Timestamp::from_unix_nanos(requested_start_ns),
        Timestamp::from_unix_nanos(requested_end_ns),
        instrument_id,
        instrument_revision_digest,
        expected_provider_timestamps_ns
            .iter()
            .copied()
            .map(Timestamp::from_unix_nanos)
            .collect(),
    )?;
    let capture_material = ProviderCaptureMaterial::try_combine_request_graph_with_semantic(
        batch.request().object().source_id().clone(),
        batch.request().object().metadata_revision().clone(),
        dataset,
        vec![bar_material, calendar_material],
        ProviderCaptureSemanticBinding::CompleteMarketBarHistoryV1(semantic),
    )?;
    let batch = batch.try_bind_provider_capture(capture_material.receipt())?;
    Ok(CompleteHistoryCaptureFixture {
        batch,
        capture_material,
        revision_plan: ExtractionRevisionPlan::try_new_with_native_lineage(revision_evidence)?,
        native_rows,
        received_at: bar_received_at,
    })
}

fn complete_history_semantic(
    requested_start: Timestamp,
    requested_end: Timestamp,
    instrument_id: InstrumentId,
    instrument_revision_digest: EvidenceDigest,
    expected_provider_timestamps: Vec<Timestamp>,
) -> Result<CompleteMarketBarHistoryV1, Box<dyn Error>> {
    Ok(CompleteMarketBarHistoryV1::try_new(
        requested_start,
        requested_end,
        instrument_id,
        instrument_revision_digest,
        digest(125),
        ProviderInstrumentId::try_from("AAPL")?,
        VenueId::try_from("iex")?,
        SourceIdentifier::try_from("iex")?,
        SourceIdentifier::try_from("1Day")?,
        MarketBarAdjustment::All,
        BarTimestampBasis::PeriodStart,
        MarketBarSessionKind::ProviderDefined,
        SourceIdentifier::try_from("alpaca-v3-iex-utc-range-returned-dates-v2")?,
        SourceIdentifier::try_from("alpaca-iex-historical-bars-and-calendar/v1")?,
        0,
        1,
        expected_provider_timestamps,
        digest(126),
    )?)
}

fn complete_history_market_bar_observation(
    instrument_id: InstrumentId,
    provider_timestamp_ns: i64,
    received_at: Timestamp,
    provider_body_digest: EvidenceDigest,
    source_record: &str,
    ordinal: i64,
) -> Result<ResearchObservation, Box<dyn Error>> {
    let ingested_at = Timestamp::from_unix_nanos(
        received_at
            .unix_nanos()
            .checked_add(2)
            .ok_or("complete-history ingest timestamp overflow")?,
    );
    let context = ResearchContext::new(
        ResearchProvenance::try_new(ResearchProvenanceInput {
            source_id: SourceId::try_from("alpaca-basic-iex-market-data")?,
            instrument_id: Some(instrument_id),
            venue_id: Some(VenueId::try_from("iex")?),
            source_identifier: SourceIdentifier::try_from(source_record)?,
            source_timestamp: Some(Timestamp::from_unix_nanos(provider_timestamp_ns)),
            received_at,
            ingested_at,
            quality: DataQuality::Aggregated,
            payload_reference: PayloadReference::ContentHash(
                market_squawk_domain::PayloadHash::new(
                    DigestAlgorithm::Sha256,
                    provider_body_digest.bytes(),
                ),
            ),
            availability: DomainAvailabilityEvidence::local_first_observed(received_at),
        })?,
        ResearchTime::new(
            Timestamp::from_unix_nanos(provider_timestamp_ns),
            None,
            RevisionNumber::new(1)?,
            None,
        )?,
    )?;
    let currency = Currency::try_from("USD")?;
    let session = MarketBarSessionEvidence::try_new(
        MarketBarSessionKind::ProviderDefined,
        SourceIdentifier::try_from("alpaca-v3-iex-utc-range-returned-dates-v2")?,
        digest(126),
    )?;
    let time_semantics = BarTimeSemantics::try_new(
        Timestamp::from_unix_nanos(provider_timestamp_ns),
        Timestamp::from_unix_nanos(
            provider_timestamp_ns
                .checked_add(COMPLETE_HISTORY_DAY_NS)
                .ok_or("complete-history bar boundary overflow")?,
        ),
        BarTimestampBasis::PeriodStart,
        session,
    )?;
    let close_cents = 10_000_i64
        .checked_add(ordinal.saturating_mul(100))
        .ok_or("complete-history close overflow")?;
    Ok(ResearchObservation::MarketBar(MarketBarObservation::new(
        context,
        ProviderInstrumentId::try_from("AAPL")?,
        SourceIdentifier::try_from("iex")?,
        SourceIdentifier::try_from("1Day")?,
        time_semantics,
        MarketBarAdjustment::All,
        Money::new(Decimal::new(close_cents - 25, 2), currency),
        Money::new(Decimal::new(close_cents + 75, 2), currency),
        Money::new(Decimal::new(close_cents - 100, 2), currency),
        Money::new(Decimal::new(close_cents, 2), currency),
        Decimal::new(1_000_000 + ordinal.saturating_mul(10_000), 0),
        Some(500 + u64::try_from(ordinal)?),
        Some(Money::new(Decimal::new(close_cents - 10, 2), currency)),
    )?))
}

fn complete_history_market_data_definition(
    instrument_id: InstrumentId,
) -> Result<MarketDataInstrumentDefinition, Box<dyn Error>> {
    let effective = EffectiveInterval::new(
        Timestamp::from_unix_nanos(
            COMPLETE_HISTORY_FIRST_BAR_NS
                .checked_sub(COMPLETE_HISTORY_DAY_NS)
                .ok_or("complete-history definition start underflow")?,
        ),
        None,
    )?;
    let exact = |byte| ExactPayloadEvidence::from_content_digest(digest(byte));
    Ok(MarketDataInstrumentDefinition::try_new(
        MarketDataInstrumentDefinitionInput {
            instrument_id,
            reference_evidence: RevisionBoundPayloadEvidence::new(
                MetadataRevision::new(SourceIdentifier::try_from(
                    "complete-alpaca-history-definition-v1",
                )?),
                exact(127),
            ),
            effective_interval: effective,
            asset_class: AssetClass::Equity,
            display_name: None,
            quote_currency: Currency::try_from("USD")?,
            quote_currency_evidence: exact(128),
            venue_mappings: vec![VenueMapping::new(
                VenueId::try_from("iex")?,
                VenueSymbol::try_from("AAPL")?,
            )],
            provider_identities: vec![ProviderIdentityRecord::new(ProviderIdentityRecordInput {
                instrument_id,
                source_id: SourceId::try_from("alpaca-basic-iex-market-data")?,
                provider_instrument_id: ProviderInstrumentId::try_from("AAPL")?,
                evidence: ProviderIdentityEvidence::from_content_digest(digest(129)),
                source_timestamp: Some(Timestamp::from_unix_nanos(COMPLETE_HISTORY_FIRST_BAR_NS)),
                observed_at: Timestamp::from_unix_nanos(
                    COMPLETE_HISTORY_FIRST_BAR_NS
                        .checked_add(1)
                        .ok_or("complete-history provider observation overflow")?,
                ),
                metadata_revision: MetadataRevision::new(SourceIdentifier::try_from(
                    "alpaca-aapl-provider-identity-v1",
                )?),
                validity: effective,
                supersedes: None,
            })],
            identifiers: Vec::new(),
        },
    )?)
}

fn complete_history_source(instrument_id: InstrumentId) -> Result<SourceMetadata, Box<dyn Error>> {
    complete_history_source_for(
        instrument_id,
        "alpaca-basic-iex-market-data",
        "alpaca-history-source-revision-v1",
        132,
        133,
    )
}

fn complete_history_calendar_source(
    instrument_id: InstrumentId,
) -> Result<SourceMetadata, Box<dyn Error>> {
    complete_history_source_for(
        instrument_id,
        "alpaca-iex-calendar-reference",
        "alpaca-iex-calendar-reference-revision-v1",
        134,
        135,
    )
}

fn complete_history_source_for(
    instrument_id: InstrumentId,
    source_id: &str,
    metadata_revision: &str,
    revision_digest_byte: u8,
    coverage_digest_byte: u8,
) -> Result<SourceMetadata, Box<dyn Error>> {
    let effective = EffectiveInterval::new(Timestamp::from_unix_nanos(0), None)?;
    let provider = SourceIdentifier::try_from("alpaca-market-data")?;
    let authorization = AuthorizationGrant::new(
        AuthorizationMode::UserAuthorized,
        AuthorizationBasis::new(SourceIdentifier::try_from("fixture-paper-iex-credential")?),
        ExactPayloadEvidence::from_content_digest(digest(131)),
        effective,
    );
    let budget = ProviderBudgetPolicy::try_new(
        BudgetScope::for_authorization(provider.clone(), &authorization)?,
        NonZeroU32::new(200).ok_or("nonzero complete-history request limit")?,
        NonZeroU64::new(60_000_000_000).ok_or("nonzero complete-history request window")?,
        NonZeroU16::new(2).ok_or("nonzero complete-history concurrency")?,
        BackoffPolicy::try_new(
            NonZeroU64::new(1_000_000_000).ok_or("nonzero initial history backoff")?,
            NonZeroU64::new(60_000_000_000).ok_or("nonzero maximum history backoff")?,
            1_000,
        )?,
    )?;
    Ok(SourceMetadata::try_new(SourceMetadataInput::new(
        SchemaVersion::CURRENT,
        SourceId::try_from(source_id)?,
        RevisionBoundPayloadEvidence::new(
            MetadataRevision::new(SourceIdentifier::try_from(metadata_revision)?),
            ExactPayloadEvidence::from_content_digest(digest(revision_digest_byte)),
        ),
        SourceClass::Broker,
        provider,
        authorization,
        SourceCoverage::try_instrument(
            ExactPayloadEvidence::from_content_digest(digest(coverage_digest_byte)),
            effective,
            vec![AssetClass::Equity],
            CoverageTopology::partial_venues(vec![VenueId::try_from("iex")?])?,
            InstrumentCoverage::enumerated(vec![instrument_id])?,
            None,
            CoverageDelay::Delayed(1),
            DeliveryEvidence::AuthorizedBroker,
        )?,
        DataQuality::Aggregated,
        NetworkAccessPolicy::Allowlisted(EndpointPolicy::try_from_api_rules(
            vec![ApiEndpointRule::try_new(
                "https://data.alpaca.markets/v2/stocks",
                PathScope::Descendants,
                Vec::new(),
                1,
                1024,
            )?],
            HttpRequestBounds::default(),
        )?),
        FreshnessPolicy::try_new(1, 1, 1, 1, 0)?,
        Some(budget),
        SourceCapabilities::new(
            false,
            true,
            SequenceCapability::Unsupported,
            ChecksumCapability::Unsupported,
            HistoricalCapability::Historical,
            false,
        ),
        SourceProtocolProfile::NotLive,
    ))?)
}

fn market_bar_observation(
    instrument: InstrumentId,
    symbol: &str,
    effective: i64,
    available: i64,
    source_record: &str,
    close_cents: i64,
) -> Result<ResearchObservation, Box<dyn Error>> {
    let observed_at = Timestamp::from_unix_nanos(available);
    let context = ResearchContext::new(
        ResearchProvenance::try_new(ResearchProvenanceInput {
            source_id: SourceId::try_from("alpaca-historical-fixture")?,
            instrument_id: Some(instrument),
            venue_id: Some(VenueId::try_from("iex")?),
            source_identifier: SourceIdentifier::try_from(source_record)?,
            source_timestamp: Some(Timestamp::from_unix_nanos(effective)),
            received_at: observed_at,
            ingested_at: observed_at,
            quality: DataQuality::Aggregated,
            payload_reference: PayloadReference::ContentHash(
                market_squawk_domain::PayloadHash::new(DigestAlgorithm::Sha256, [43; 32]),
            ),
            availability: DomainAvailabilityEvidence::local_first_observed(observed_at),
        })?,
        ResearchTime::new(
            Timestamp::from_unix_nanos(effective),
            None,
            RevisionNumber::new(1)?,
            None,
        )?,
    )?;
    let currency = Currency::try_from("USD")?;
    let time_semantics = BarTimeSemantics::try_new(
        Timestamp::from_unix_nanos(effective),
        Timestamp::from_unix_nanos(
            effective
                .checked_add(5)
                .ok_or("market bar completion overflow")?,
        ),
        BarTimestampBasis::PeriodStart,
        market_bar_session()?,
    )?;
    Ok(ResearchObservation::MarketBar(MarketBarObservation::new(
        context,
        ProviderInstrumentId::try_from(symbol)?,
        SourceIdentifier::try_from("iex")?,
        SourceIdentifier::try_from("1Day")?,
        time_semantics,
        MarketBarAdjustment::Raw,
        Money::new(Decimal::new(10_000, 2), currency),
        Money::new(Decimal::new(10_200, 2), currency),
        Money::new(Decimal::new(9_900, 2), currency),
        Money::new(Decimal::new(close_cents, 2), currency),
        Decimal::new(1_000_000, 0),
        Some(500),
        Some(Money::new(Decimal::new(10_050, 2), currency)),
    )?))
}

fn market_bar_session() -> Result<MarketBarSessionEvidence, Box<dyn Error>> {
    Ok(MarketBarSessionEvidence::try_new(
        MarketBarSessionKind::Regular,
        SourceIdentifier::try_from("iex-regular-session-rules-2024")?,
        EvidenceDigest::new(DigestAlgorithm::Sha256, [46; 32]),
    )?)
}

fn market_bar_instrument(suffix: u128) -> Result<InstrumentId, Box<dyn Error>> {
    Ok(InstrumentId::from_str(&format!(
        "0187f5f1-6fc2-7fa2-bf05-{suffix:012x}"
    ))?)
}

fn market_bar_source() -> Result<SourceMetadata, Box<dyn Error>> {
    let effective = EffectiveInterval::new(Timestamp::from_unix_nanos(0), None)?;
    let provider = SourceIdentifier::try_from("alpaca-market-data")?;
    let authorization = AuthorizationGrant::new(
        AuthorizationMode::UserAuthorized,
        AuthorizationBasis::new(SourceIdentifier::try_from("fixture-user-credential")?),
        ExactPayloadEvidence::from_content_digest(digest(45)),
        effective,
    );
    let budget = ProviderBudgetPolicy::try_new(
        BudgetScope::for_authorization(provider.clone(), &authorization)?,
        NonZeroU32::new(200).ok_or("nonzero provider request limit")?,
        NonZeroU64::new(60_000_000_000).ok_or("nonzero provider request window")?,
        NonZeroU16::new(2).ok_or("nonzero provider concurrency")?,
        BackoffPolicy::try_new(
            NonZeroU64::new(1_000_000_000).ok_or("nonzero initial backoff")?,
            NonZeroU64::new(60_000_000_000).ok_or("nonzero maximum backoff")?,
            1_000,
        )?,
    )?;
    Ok(SourceMetadata::try_new(SourceMetadataInput::new(
        SchemaVersion::CURRENT,
        SourceId::try_from("alpaca-historical-fixture")?,
        RevisionBoundPayloadEvidence::new(
            MetadataRevision::new(SourceIdentifier::try_from("alpaca-revision-1")?),
            ExactPayloadEvidence::from_content_digest(digest(44)),
        ),
        SourceClass::Broker,
        provider,
        authorization,
        SourceCoverage::try_instrument(
            ExactPayloadEvidence::from_content_digest(digest(46)),
            effective,
            vec![AssetClass::Equity],
            CoverageTopology::partial_venues(vec![VenueId::try_from("iex")?])?,
            InstrumentCoverage::enumerated(vec![
                market_bar_instrument(1)?,
                market_bar_instrument(2)?,
            ])?,
            None,
            CoverageDelay::Delayed(1),
            DeliveryEvidence::AuthorizedBroker,
        )?,
        DataQuality::Aggregated,
        NetworkAccessPolicy::Allowlisted(EndpointPolicy::try_from_api_rules(
            vec![ApiEndpointRule::try_new(
                "https://data.alpaca.markets/v2/stocks",
                PathScope::Descendants,
                Vec::new(),
                1,
                1024,
            )?],
            HttpRequestBounds::default(),
        )?),
        FreshnessPolicy::try_new(1, 1, 1, 1, 0)?,
        Some(budget),
        SourceCapabilities::new(
            false,
            true,
            SequenceCapability::Unsupported,
            ChecksumCapability::Unsupported,
            HistoricalCapability::Historical,
            false,
        ),
        SourceProtocolProfile::NotLive,
    ))?)
}

fn sealed_market_event_microbatch(
    store: &SealedResearchJournalStore,
) -> Result<
    (
        SealedProviderPublicationBinding,
        market_squawk_platform::SealedResearchJournalSegmentClaim,
        MarketEvent,
    ),
    Box<dyn Error>,
> {
    let source_id = SourceId::try_from("alpaca-historical-fixture")?;
    let revision = MetadataRevision::new(SourceIdentifier::try_from("alpaca-revision-1")?);
    let dataset = SourceIdentifier::try_from("alpaca-live-events-fixture")?;
    let payload = Bytes::from_static(b"{\"T\":\"t\",\"S\":\"AAPL\",\"p\":101.5,\"s\":2}");
    let payload_digest =
        EvidenceDigest::new(DigestAlgorithm::Sha256, Sha256::digest(&payload).into());
    let received_at = Timestamp::from_unix_nanos(500);
    let material = ProviderEventMicrobatchMaterial::try_new(
        source_id.clone(),
        revision.clone(),
        dataset.clone(),
        SourceIdentifier::try_from("alpaca-live-events-fixture:batch-1")?,
        vec![RawCaptureRecord::try_new_live(
            Uuid::from_u128(7_001),
            Arc::from(source_id.as_str()),
            Uuid::from_u128(7_002),
            Some(u64::MAX),
            Some(DateTime::<Utc>::from_timestamp_nanos(490)),
            DateTime::<Utc>::from_timestamp_nanos(received_at.unix_nanos()),
            payload,
        )?],
    )?;
    let (expectation, seal_request) = material.into_sealing_parts();
    let token = expectation.try_rejoin(seal_request.seal(store)?)?;
    let live_binding = LiveEvidenceBinding::new(
        source_id.clone(),
        SourceIdentifier::try_from("alpaca-live-session-1")?,
        revision.clone(),
        AuthorizationBasis::new(SourceIdentifier::try_from("fixture-user-credential")?),
        VenueId::try_from("iex")?,
        market_bar_instrument(1)?,
        ConnectionGeneration::new(1)?,
        ProviderProduct::new(SourceIdentifier::try_from("AAPL")?),
        ProviderChannel::new(SourceIdentifier::try_from("trades")?),
        LiveEventClass::Trade,
        SourceIdentifier::try_from("alpaca-live-trade-1")?,
        payload_digest,
        CanonicalStateDigest::new(
            digest(201),
            CanonicalizationRule::new(
                SourceIdentifier::try_from("market-squawk.fixture.trade-v1")?,
                RuleVersion::new(1)?,
            ),
        ),
        None,
    )?;
    let event = MarketEvent::Trade(TradeEvent::new(
        LiveProvenance::decoded(DecodedLiveProvenanceInput::new(
            live_binding,
            Some(Timestamp::from_unix_nanos(490)),
            received_at,
            received_at,
            Timestamp::from_unix_nanos(501),
            DataQuality::DirectUnverified,
            CoverageStatus::Sufficient,
            PayloadReference::SourceReference(SourceIdentifier::try_from("alpaca-live-frame-1")?),
        ))?,
        PriceTicks::new(10_150),
        QuantityLots::new(2)?,
        AggressorSide::Buy,
        None,
    )?);
    let batch =
        ProviderMarketEventBatch::try_new(source_id, revision, dataset, vec![event.clone()])?;
    let native = ProviderMarketEventNativeLineageBatch::try_new(
        ProviderNativeLineageImplementation::SchwabStreamerMarketDataV1,
        &batch,
        vec![Bytes::from_static(b"{\"frame\":0}")],
        Some(Bytes::from_static(b"{\"connection_generation\":1}")),
    )?;
    let binding = SealedProviderEventMicrobatchBinding::try_new(token, batch, native, vec![0])?;
    let claim = binding.persisted_receipt().segment().claim().clone();
    Ok((binding.into(), claim, event))
}

fn fund_nav_instrument() -> Result<InstrumentId, Box<dyn Error>> {
    Ok(InstrumentId::from_str(
        "0187f5f1-6fc2-7fa2-bf05-00000000f00d",
    )?)
}

fn fund_nav_extraction_batch() -> Result<ExtractionBatch, Box<dyn Error>> {
    let date = market_squawk_domain::CalendarDate::new(2026, 8, 10)?;
    let published_date = market_squawk_domain::CalendarDate::new(2026, 8, 11)?;
    let discovery = DiscoveryRequest::try_new(
        SourceIdentifier::try_from("fund-nav-fixture")?,
        None,
        NonZeroU16::MIN,
        Timestamp::from_unix_nanos(1_000),
    )?;
    let object_evidence = ExactPayloadEvidence::from_content_digest(digest(73));
    let object = SourceObject::try_new(
        SourceId::try_from("fred-local-fixture")?,
        MetadataRevision::new(SourceIdentifier::try_from("revision-1")?),
        &discovery,
        SourceIdentifier::try_from("fund-nav:share-class:2026-08-10")?,
        SourceIdentifier::try_from("application-json")?,
        object_evidence,
        EffectiveInterval::new(Timestamp::from_unix_nanos(100), None)?,
        None,
        Some(4096),
    )?;
    let request = ExtractionRequest::try_new(
        object,
        NonZeroU32::new(2).ok_or("nonzero Fund NAV record limit")?,
        NonZeroU64::new(1024 * 1024).ok_or("nonzero Fund NAV byte limit")?,
        Timestamp::from_unix_nanos(1_000),
    )?;
    let specifications = [
        (120_i64, 125_i64, 130_i64, 10_100_i64, "nav-v1", 81_u8),
        (180_i64, 185_i64, 190_i64, 10_125_i64, "nav-v2", 82_u8),
    ];
    let mut records = Vec::new();
    for (received, ingested, canonical_published, amount, revision, row_digest) in specifications {
        let observation = fund_nav_observation(
            date,
            published_date,
            received,
            ingested,
            canonical_published,
            amount,
            revision,
            row_digest,
        )?;
        let payload = serde_json::to_vec(&observation)?;
        records.push(ExtractionRecord::try_new_with_time(
            &request,
            SourceIdentifier::try_from("market-squawk-research-v3")?,
            ExactPayloadEvidence::from_content_digest(EvidenceDigest::new(
                DigestAlgorithm::Sha256,
                Sha256::digest(&payload).into(),
            )),
            ResearchTemporalCoordinate::calendar_date(date),
            Some(ResearchTemporalCoordinate::calendar_date(published_date)),
            SourceAvailabilityEvidence::LocalFirstObserved {
                observed_at: Timestamp::from_unix_nanos(received),
            },
            SourceIdentifier::try_from(revision)?,
            None,
            payload.into(),
        )?);
    }
    Ok(ExtractionBatch::try_new(&request, records)?)
}

#[allow(
    clippy::too_many_arguments,
    reason = "the fixture keeps every NAV clock explicit"
)]
fn fund_nav_observation(
    nav_date: market_squawk_domain::CalendarDate,
    published_date: market_squawk_domain::CalendarDate,
    received: i64,
    ingested: i64,
    canonical_published: i64,
    amount: i64,
    source_revision: &str,
    row_digest: u8,
) -> Result<ResearchObservation, Box<dyn Error>> {
    let received_at = Timestamp::from_unix_nanos(received);
    let raw_row = ExactPayloadEvidence::from_content_digest(digest(row_digest));
    let context = ResearchContext::new(
        ResearchProvenance::try_new(ResearchProvenanceInput {
            source_id: SourceId::try_from("fred-local-fixture")?,
            instrument_id: Some(fund_nav_instrument()?),
            venue_id: None,
            source_identifier: SourceIdentifier::try_from(format!(
                "fund-nav:share-class:{nav_date}:{source_revision}"
            ))?,
            source_timestamp: None,
            received_at,
            ingested_at: Timestamp::from_unix_nanos(ingested),
            quality: DataQuality::DirectVerified,
            payload_reference: PayloadReference::ContentHash(
                market_squawk_domain::PayloadHash::new(
                    raw_row.content_digest().algorithm(),
                    raw_row.content_digest().bytes(),
                ),
            ),
            availability: DomainAvailabilityEvidence::local_first_observed(received_at),
        })?,
        ResearchTime::try_new_with_coordinates(
            ResearchTemporalCoordinate::calendar_date(nav_date),
            Some(ResearchTemporalCoordinate::calendar_date(published_date)),
            RevisionNumber::new(1)?,
            None,
        )?,
    )?;
    let native_schema = FundNavNativeSchema::new(
        MetadataRevision::new(SourceIdentifier::try_from("fund-nav-contract-v1")?),
        ExactPayloadEvidence::from_content_digest(digest(74)),
        SourceIdentifier::try_from("fund-nav-native-row")?,
        MetadataRevision::new(SourceIdentifier::try_from("native-v1")?),
        ExactPayloadEvidence::from_content_digest(digest(75)),
    );
    let lineage = FundNavLineage::try_new(
        native_schema,
        FundNavEntitlementEvidence::Gated {
            generation: NonZeroU64::MIN,
            evidence: digest(76),
        },
        digest(77),
        ExactPayloadEvidence::from_content_digest(digest(73)),
        raw_row,
        Some(digest(78)),
        digest(79),
        FundNavCompleteness::Complete,
        FundNavDisposition::Returned,
    )?;
    let revision_evidence = FundNavRevisionEvidence::try_new(
        Some(SourceIdentifier::try_from(source_revision)?),
        if source_revision == "nav-v1" {
            FundNavCorrectionState::Original
        } else {
            FundNavCorrectionState::Corrected
        },
        FundNavFinality::Final,
        (source_revision != "nav-v1").then(|| digest(81)),
        None,
    )?;
    let currency = Currency::try_from("USD")?;
    Ok(ResearchObservation::FundNav(FundNavObservation::try_new(
        FundNavObservationInput {
            context,
            provider_instrument_id: ProviderInstrumentId::try_from("FUNDX")?,
            instrument_reference_revision: MetadataRevision::new(SourceIdentifier::try_from(
                "fund-share-class-reference-v1",
            )?),
            provider_product: ProviderProduct::new(SourceIdentifier::try_from("fundamentals")?),
            provider_channel: ProviderChannel::new(SourceIdentifier::try_from("daily-nav")?),
            nav_date,
            valuation_basis: FundNavValuationBasis::PerShare,
            currency,
            value: FundNavValue::Observed(Money::new(Decimal::new(amount, 2), currency)),
            canonical_published_at: Timestamp::from_unix_nanos(canonical_published),
            lineage,
            revision_evidence,
        },
    )?))
}

fn fund_nav_revision_plan() -> Result<ExtractionRevisionPlan, Box<dyn Error>> {
    let evidence = [("nav-v1", 120_i64), ("nav-v2", 180_i64)]
        .into_iter()
        .map(|(version, order)| {
            ExtractionRevisionEvidence::provider_supplied(
                version.as_bytes(),
                ObservedProviderOrder::try_new(
                    ResearchTemporalCoordinate::exact(Timestamp::from_unix_nanos(order)),
                    version.as_bytes(),
                )?,
            )
            .map_err(Into::into)
        })
        .collect::<Result<Vec<_>, Box<dyn Error>>>()?;
    Ok(ExtractionRevisionPlan::try_new(evidence)?)
}

fn company_identity(
    source_id: SourceId,
    parent_digest: EvidenceDigest,
    conformed_name: &str,
    ingested_at: i64,
) -> Result<CompanyIdentityObservation, Box<dyn Error>> {
    Ok(CompanyIdentityObservation::try_new(
        CompanyIdentityObservationInput {
            schema_version: SchemaVersion::CURRENT,
            source_id,
            provider_company_id: SourceIdentifier::try_from("CIK0000000001")?,
            surface: CompanyIdentitySurface::SecSubmissions,
            conformed_name: conformed_name.to_owned(),
            former_names: Vec::new(),
            entity_type: Some("operating".to_owned()),
            sic: Some("3571".to_owned()),
            sic_description: Some("Electronic Computers".to_owned()),
            associations: Vec::new(),
            parent_ingest_payload_evidence: ExactPayloadEvidence::from_content_digest(
                parent_digest,
            ),
            identity_payload_evidence: ExactPayloadEvidence::from_content_digest(digest(90)),
            received_at: Timestamp::from_unix_nanos(100),
            availability: DomainAvailabilityEvidence::local_first_observed(
                Timestamp::from_unix_nanos(100),
            ),
            ingested_at: Timestamp::from_unix_nanos(ingested_at),
            quality: DataQuality::OfficialDelayed,
        },
    )?)
}

fn dataset_extraction_batch() -> Result<ExtractionBatch, Box<dyn Error>> {
    extraction_batch_with_membership(true)
}

fn closed_price_return_request(
    membership_parent: DatasetManifestRef,
    market_bar_parent: DatasetManifestRef,
    instrument: InstrumentId,
    research_limits: ResearchUseLimits,
    include_successor_example: bool,
) -> Result<DatasetBuildRequest, Box<dyn Error>> {
    let feature = FeatureLabelComponentSpec::try_new(
        ComponentKind::Feature,
        ComponentScope::Instrument,
        CorporateActionSensitivity::RequiresAdjustment,
        "research.price-return",
        NonZeroU32::MIN,
    )?;
    let label = FeatureLabelComponentSpec::try_new(
        ComponentKind::Label,
        ComponentScope::Instrument,
        CorporateActionSensitivity::RequiresAdjustment,
        "research.fixed-horizon-forward-return",
        NonZeroU32::MIN,
    )?;
    let adjustment_policy =
        CorporateActionPolicy::new(CorporateActionAdjustment::SplitAdjusted, NonZeroU32::MIN);
    let action_limits = CorporateActionLimits::try_new(
        NonZeroUsize::new(16).ok_or("nonzero action limit")?,
        NonZeroUsize::new(1024 * 1024).ok_or("nonzero action byte limit")?,
    )?;
    let feature_plan = CorporateActionPlan::try_build(
        adjustment_policy,
        Timestamp::from_unix_nanos(100),
        Timestamp::from_unix_nanos(100),
        Vec::new(),
        action_limits,
    )?;
    let label_plan = CorporateActionPlan::try_build(
        adjustment_policy,
        Timestamp::from_unix_nanos(110),
        Timestamp::from_unix_nanos(110),
        Vec::new(),
        action_limits,
    )?;
    let return_unit = Some(SourceIdentifier::try_from(FEATURE_LABEL_RETURN_UNIT)?);
    let feature_input = FeatureLabelComponentInput::try_new(
        feature.clone(),
        ComponentValue::decimal(Decimal::ZERO, return_unit.clone(), None)?,
        vec![
            ComponentSelector::new(closed_price_return_market_bar_family(instrument, 80)?),
            ComponentSelector::new(closed_price_return_market_bar_family(instrument, 90)?),
        ],
        ResearchTemporalCoordinate::exact(Timestamp::from_unix_nanos(90)),
        None,
        ComponentAdjustmentEvidence::try_applied(
            adjustment_policy,
            feature_plan.content_hash(),
            feature_plan.audit_hash(),
            digest(84),
        )?,
    )?;
    let label_input = FeatureLabelComponentInput::try_new(
        label.clone(),
        ComponentValue::decimal(Decimal::ZERO, return_unit, None)?,
        vec![ComponentSelector::new(
            closed_price_return_market_bar_family(instrument, 100)?,
        )],
        ResearchTemporalCoordinate::exact(Timestamp::from_unix_nanos(90)),
        Some(ResearchTemporalCoordinate::exact(
            Timestamp::from_unix_nanos(100),
        )),
        ComponentAdjustmentEvidence::try_applied(
            adjustment_policy,
            label_plan.content_hash(),
            label_plan.audit_hash(),
            digest(84),
        )?,
    )?;
    let mut component_specs = vec![feature.clone()];
    let mut macro_inputs = Vec::new();
    macro_inputs.try_reserve_exact(feature_dataset_macro_components_v1().len())?;
    for descriptor in feature_dataset_macro_components_v1() {
        let specification = FeatureLabelComponentSpec::try_new(
            ComponentKind::Feature,
            ComponentScope::Global,
            CorporateActionSensitivity::NotApplicable,
            descriptor.component_name(),
            NonZeroU32::MIN,
        )?;
        macro_inputs.push(FeatureLabelComponentInput::try_new(
            specification.clone(),
            ComponentValue::decimal(
                Decimal::new(i64::from(descriptor.position()) + 1, 2),
                Some(SourceIdentifier::try_from(descriptor.unit())?),
                None,
            )?,
            vec![ComponentSelector::new(ObservationFamilyKey::Macro {
                source_id: SourceId::try_from("fred-local-fixture")?,
                series: SourceIdentifier::try_from("GDP")?,
                effective: ResearchTemporalCoordinate::exact(Timestamp::from_unix_nanos(90)),
            })],
            ResearchTemporalCoordinate::exact(Timestamp::from_unix_nanos(90)),
            None,
            ComponentAdjustmentEvidence::NotApplicable,
        )?);
        component_specs.push(specification);
    }
    component_specs.push(label.clone());
    let membership_evidence =
        CanonicalObservationPayload::try_from_observation(&universe_membership_observation()?)?
            .identity();
    let mut components = Vec::new();
    components.try_reserve_exact(macro_inputs.len() + 2)?;
    components.push(feature_input);
    components.extend(macro_inputs);
    components.push(label_input);
    let mut examples = vec![
        market_squawk_data::DatasetExample::try_new_with_temporal_cutoffs(
            "aapl-price-return-example-1",
            instrument,
            Timestamp::from_unix_nanos(100),
            Timestamp::from_unix_nanos(110),
            ResearchTemporalCoordinate::exact(Timestamp::from_unix_nanos(90)),
            ResearchTemporalCoordinate::exact(Timestamp::from_unix_nanos(100)),
            components.clone(),
        )?,
    ];
    if include_successor_example {
        examples.push(
            market_squawk_data::DatasetExample::try_new_with_temporal_cutoffs(
                "aapl-price-return-example-2",
                instrument,
                Timestamp::from_unix_nanos(100),
                Timestamp::from_unix_nanos(110),
                ResearchTemporalCoordinate::exact(Timestamp::from_unix_nanos(90)),
                ResearchTemporalCoordinate::exact(Timestamp::from_unix_nanos(100)),
                components,
            )?,
        );
    }
    let inputs = DatasetBuildInputs::try_new(
        vec![membership_parent.clone(), market_bar_parent],
        UniverseId::try_from("us-equities.historical")?,
        vec![UniverseMembership::new(
            instrument,
            EffectiveInterval::new(Timestamp::from_unix_nanos(1), None)?,
            market_squawk_domain::AvailabilityEvidence::evidenced(
                Timestamp::from_unix_nanos(1),
                SourceIdentifier::try_from("constituent-publication")?,
            ),
            membership_parent,
            membership_evidence,
        )],
        component_specs,
        examples,
    )?;
    let policy = DatasetBuildPolicy::new(
        ChronologicalSplitPolicy::try_new(
            Timestamp::from_unix_nanos(120),
            Timestamp::from_unix_nanos(200),
            Timestamp::from_unix_nanos(300),
        )?,
        PointInTimePolicy::try_new(NonZeroU32::MIN, PointInTimeRevisionMode::LatestKnown)?,
        adjustment_policy,
        MissingValuePolicy::Reject,
        SourceIdentifier::try_from("price-return-macro-context-fixed-horizon-forward-return-v1")?,
    );
    let limits = DatasetBuildLimits::try_new(
        128,
        8,
        feature_dataset_macro_components_v1().len() + 2,
        64,
        4 * 1024 * 1024,
        Duration::from_secs(5),
        PointInTimeLimits::try_new(128, 128, 8, 128, 1024 * 1024)?,
        UniverseLimits::try_new(16, 1024 * 1024)?,
        action_limits,
    )?;
    Ok(DatasetBuildRequest::try_new(
        DatasetId::try_from("derived.feature-labels.price-return-v1")?,
        inputs,
        policy,
        ResearchUse::LocalAnalysis,
        research_limits,
        DatasetOutputAuthorization::try_new(
            SourceId::try_from("market-squawk.derived")?,
            RightsBasis::reviewed_terms("https://example.test/local-derived/v1", digest(62))?,
            digest(63),
            None,
        )?,
        limits,
    )?)
}

fn closed_price_return_market_bar_family(
    instrument: InstrumentId,
    effective: i64,
) -> Result<ObservationFamilyKey, Box<dyn Error>> {
    Ok(ObservationFamilyKey::MarketBar {
        source_id: SourceId::try_from("alpaca-historical-fixture")?,
        instrument_id: instrument,
        venue_id: VenueId::try_from("iex")?,
        provider_instrument_id: ProviderInstrumentId::try_from("AAPL")?,
        feed: SourceIdentifier::try_from("iex")?,
        interval: SourceIdentifier::try_from("1Day")?,
        adjustment: MarketBarAdjustment::Raw,
        timestamp_basis: BarTimestampBasis::PeriodStart,
        session: market_bar_session()?,
        effective: ResearchTemporalCoordinate::exact(Timestamp::from_unix_nanos(effective)),
    })
}

fn closed_price_return_proof(
    request: &DatasetBuildRequest,
    macro_parent: DatasetManifestRef,
    attested_at: Timestamp,
    currentness_expires_at: Timestamp,
    return_kernel_digest_byte: u8,
) -> Result<FeatureDatasetProductionProofV1, Box<dyn Error>> {
    Ok(FeatureDatasetProductionProofV1::try_from_request_evidence(
        request,
        digest(85),
        digest(86),
        digest(87),
        digest(88),
        digest(89),
        digest(90),
        digest(91),
        digest(92),
        digest(93),
        digest(94),
        vec![macro_parent],
        digest(95),
        digest(96),
        digest(return_kernel_digest_byte),
        attested_at,
        currentness_expires_at,
    )?)
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
    let payload = serde_json::to_vec(&macro_observation(None)?)?;
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

fn macro_snapshot_extraction_batch() -> Result<ExtractionBatch, Box<dyn Error>> {
    let discovery = DiscoveryRequest::try_new(
        SourceIdentifier::try_from("h15-treasury-constant-maturities")?,
        None,
        NonZeroU16::MIN,
        Timestamp::from_unix_nanos(1_000),
    )?;
    let object = SourceObject::try_new(
        SourceId::try_from("fred-local-fixture")?,
        MetadataRevision::new(SourceIdentifier::try_from("revision-1")?),
        &discovery,
        SourceIdentifier::try_from("h15-treasury-full-history-fixture")?,
        SourceIdentifier::try_from("text-csv")?,
        ExactPayloadEvidence::from_content_digest(digest(93)),
        EffectiveInterval::new(Timestamp::from_unix_nanos(0), None)?,
        None,
        Some(64 * 1024),
    )?;
    let request = ExtractionRequest::try_new(
        object,
        NonZeroU32::new(14).ok_or("nonzero Macro snapshot record limit")?,
        NonZeroU64::new(1024 * 1024).ok_or("nonzero Macro snapshot byte limit")?,
        Timestamp::from_unix_nanos(1_000),
    )?;
    let older = market_squawk_domain::CalendarDate::new(2026, 8, 9)?;
    let current = market_squawk_domain::CalendarDate::new(2026, 8, 10)?;
    let future_knowledge = market_squawk_domain::CalendarDate::new(2026, 8, 11)?;
    let mut specifications = vec![
        (
            MACRO_SNAPSHOT_SERIES[0],
            older,
            90_i64,
            95_i64,
            Decimal::new(400, 2),
            "macro-s0-older-v1".to_owned(),
        ),
        (
            MACRO_SNAPSHOT_SERIES[0],
            current,
            100_i64,
            105_i64,
            Decimal::new(410, 2),
            "macro-s0-current-v1".to_owned(),
        ),
        (
            MACRO_SNAPSHOT_SERIES[0],
            current,
            150_i64,
            155_i64,
            Decimal::new(425, 2),
            "macro-s0-current-v2".to_owned(),
        ),
        (
            MACRO_SNAPSHOT_SERIES[0],
            future_knowledge,
            300_i64,
            305_i64,
            Decimal::new(450, 2),
            "macro-s0-future-knowledge-v1".to_owned(),
        ),
    ];
    for (index, series) in MACRO_SNAPSHOT_SERIES.iter().enumerate().skip(1) {
        let index = i64::try_from(index)?;
        specifications.push((
            *series,
            current,
            100_i64
                .checked_add(index)
                .ok_or("Macro fixture time overflow")?,
            110_i64
                .checked_add(index)
                .ok_or("Macro fixture time overflow")?,
            Decimal::new(
                300_i64
                    .checked_add(index)
                    .ok_or("Macro fixture value overflow")?,
                2,
            ),
            format!("macro-s{index}-current-v1"),
        ));
    }
    let mut records = Vec::new();
    records.try_reserve_exact(specifications.len())?;
    for (series, effective_date, received, ingested, value, revision) in specifications {
        let observation = macro_snapshot_observation(
            series,
            effective_date,
            received,
            ingested,
            value,
            &revision,
        )?;
        let payload = serde_json::to_vec(&observation)?;
        records.push(ExtractionRecord::try_new_with_time(
            &request,
            SourceIdentifier::try_from("market-squawk-research-v3")?,
            ExactPayloadEvidence::from_content_digest(EvidenceDigest::new(
                DigestAlgorithm::Sha256,
                Sha256::digest(&payload).into(),
            )),
            ResearchTemporalCoordinate::calendar_date(effective_date),
            None,
            SourceAvailabilityEvidence::LocalFirstObserved {
                observed_at: Timestamp::from_unix_nanos(received),
            },
            SourceIdentifier::try_from(revision)?,
            None,
            payload.into(),
        )?);
    }
    Ok(ExtractionBatch::try_new(&request, records)?)
}

fn macro_snapshot_revision_plan(
    batch: &ExtractionBatch,
) -> Result<ExtractionRevisionPlan, Box<dyn Error>> {
    let evidence = batch
        .records()
        .iter()
        .map(|record| {
            let version = record.revision().as_str().as_bytes();
            let observed_at = record
                .available_at()
                .ok_or("Macro fixture must carry conservative availability")?;
            let order = ObservedProviderOrder::try_new(
                ResearchTemporalCoordinate::exact(observed_at),
                version,
            )?;
            ExtractionRevisionEvidence::provider_supplied(version, order).map_err(Into::into)
        })
        .collect::<Result<Vec<_>, Box<dyn Error>>>()?;
    Ok(ExtractionRevisionPlan::try_new(evidence)?)
}

fn macro_snapshot_observation(
    series: &str,
    effective_date: market_squawk_domain::CalendarDate,
    received: i64,
    ingested: i64,
    value: Decimal,
    occurrence: &str,
) -> Result<ResearchObservation, Box<dyn Error>> {
    let received_at = Timestamp::from_unix_nanos(received);
    let context = ResearchContext::new(
        ResearchProvenance::try_new(ResearchProvenanceInput {
            source_id: SourceId::try_from("fred-local-fixture")?,
            instrument_id: None,
            venue_id: None,
            source_identifier: SourceIdentifier::try_from(format!("frb-ddp:h15:{occurrence}"))?,
            source_timestamp: None,
            received_at,
            ingested_at: Timestamp::from_unix_nanos(ingested),
            quality: DataQuality::OfficialDelayed,
            payload_reference: PayloadReference::SourceReference(SourceIdentifier::try_from(
                "federal-reserve-board:h15:fixture",
            )?),
            availability: DomainAvailabilityEvidence::local_first_observed(received_at),
        })?,
        ResearchTime::try_new_with_coordinates(
            ResearchTemporalCoordinate::calendar_date(effective_date),
            None,
            RevisionNumber::new(1)?,
            None,
        )?,
    )?;
    Ok(ResearchObservation::Macro(MacroObservation::new(
        context,
        SourceIdentifier::try_from(series)?,
        value,
        SourceIdentifier::try_from("percent-per-year")?,
    )))
}

fn macro_observation(chunk_ordinal: Option<usize>) -> Result<ResearchObservation, Box<dyn Error>> {
    let (source_identifier, series) = chunk_ordinal.map_or_else(
        || ("GDP:2026Q1:v1".to_owned(), "GDP".to_owned()),
        |value| {
            (
                format!("GDP:2026Q1:chunk-{value}:v1"),
                format!("GDP-{value}"),
            )
        },
    );
    let context = ResearchContext::new(
        ResearchProvenance::try_new(ResearchProvenanceInput {
            source_id: SourceId::try_from("fred-local-fixture")?,
            instrument_id: None,
            venue_id: None,
            source_identifier: SourceIdentifier::try_from(source_identifier.as_str())?,
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
        SourceIdentifier::try_from(series.as_str())?,
        Decimal::new(
            123_456_i64
                .checked_add(i64::try_from(chunk_ordinal.unwrap_or_default())?)
                .ok_or("provider macro value overflow")?,
            2,
        ),
        SourceIdentifier::try_from("USD")?,
    )))
}

fn universe_membership_observation() -> Result<ResearchObservation, Box<dyn Error>> {
    let instrument = dataset_membership_instrument()?;
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

fn dataset_membership_instrument() -> Result<InstrumentId, Box<dyn Error>> {
    Ok(InstrumentId::from_str(
        "0187f5f1-6fc2-7fa2-bf05-2ce5354c55c1",
    )?)
}

fn local_source() -> Result<SourceMetadata, Box<dyn Error>> {
    local_source_for("fred-local-fixture")
}

fn sec_research_source() -> Result<SourceMetadata, Box<dyn Error>> {
    let effective = EffectiveInterval::new(Timestamp::from_unix_nanos(0), None)?;
    let provider = SourceIdentifier::try_from("sec")?;
    let authorization = AuthorizationGrant::new(
        AuthorizationMode::PublicInterface,
        AuthorizationBasis::new(SourceIdentifier::try_from("sec-public-edgar")?),
        ExactPayloadEvidence::from_content_digest(digest(216)),
        effective,
    );
    let budget = ProviderBudgetPolicy::try_new(
        BudgetScope::for_authorization(provider.clone(), &authorization)?,
        NonZeroU32::new(8).ok_or("nonzero SEC provider request limit")?,
        NonZeroU64::new(1_000_000_000).ok_or("nonzero SEC provider request window")?,
        NonZeroU16::new(4).ok_or("nonzero SEC provider concurrency")?,
        BackoffPolicy::try_new(
            NonZeroU64::new(1_000_000_000).ok_or("nonzero initial SEC backoff")?,
            NonZeroU64::new(60_000_000_000).ok_or("nonzero maximum SEC backoff")?,
            0,
        )?,
    )?;
    Ok(SourceMetadata::try_new(SourceMetadataInput::new(
        SchemaVersion::CURRENT,
        SourceId::try_from("sec-edgar")?,
        RevisionBoundPayloadEvidence::new(
            MetadataRevision::new(SourceIdentifier::try_from("sec-edgar-contract-v1")?),
            ExactPayloadEvidence::from_content_digest(digest(215)),
        ),
        SourceClass::RegulatoryFiling,
        provider,
        authorization,
        SourceCoverage::try_non_instrument(
            ExactPayloadEvidence::from_content_digest(digest(217)),
            effective,
            CoverageDomain::RegulatoryFilings,
            CoverageDelay::Delayed(1),
            DeliveryEvidence::Unknown,
        )?,
        DataQuality::OfficialDelayed,
        NetworkAccessPolicy::Allowlisted(EndpointPolicy::try_from_api_rules(
            vec![ApiEndpointRule::try_new(
                "https://data.sec.gov/submissions",
                PathScope::Descendants,
                Vec::new(),
                1,
                16_384_u16,
            )?],
            HttpRequestBounds::default(),
        )?),
        FreshnessPolicy::try_new(1, 1, 1, 1, 0)?,
        Some(budget),
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

fn sec_research_company_identity(
    parent_digest: EvidenceDigest,
    base_ns: i64,
) -> Result<CompanyIdentityObservation, Box<dyn Error>> {
    Ok(CompanyIdentityObservation::try_new(
        CompanyIdentityObservationInput {
            schema_version: SchemaVersion::CURRENT,
            source_id: SourceId::try_from("sec-edgar")?,
            provider_company_id: SourceIdentifier::try_from("0000320193")?,
            surface: CompanyIdentitySurface::SecSubmissions,
            conformed_name: "Apple Inc.".to_owned(),
            former_names: Vec::new(),
            entity_type: Some("operating".to_owned()),
            sic: Some("3571".to_owned()),
            sic_description: Some("Electronic Computers".to_owned()),
            associations: Vec::new(),
            parent_ingest_payload_evidence: ExactPayloadEvidence::from_content_digest(
                parent_digest,
            ),
            identity_payload_evidence: ExactPayloadEvidence::from_content_digest(digest(218)),
            received_at: Timestamp::from_unix_nanos(base_ns),
            availability: DomainAvailabilityEvidence::evidenced(
                Timestamp::from_unix_nanos(base_ns),
                SourceIdentifier::try_from("sec-submissions-company-identity")?,
            ),
            ingested_at: Timestamp::from_unix_nanos(
                base_ns
                    .checked_add(1_000_000)
                    .ok_or("SEC company ingest timestamp overflow")?,
            ),
            quality: DataQuality::OfficialDelayed,
        },
    )?)
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
