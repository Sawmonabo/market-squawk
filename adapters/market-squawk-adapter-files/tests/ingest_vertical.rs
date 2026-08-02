// Rust #159105: this macOS-only test-link diagnostic is caused by the measured
// `__eh_frame` exceeding arm64 compact-unwind's 24-bit offset range.
#![allow(linker_messages)]

//! One release-critical local-format to manifest-pinned query proof.

use std::error::Error;
use std::fs;
use std::io::{Cursor, Write as _};
use std::num::{NonZeroU16, NonZeroU32, NonZeroU64};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use arrow::array::{Array as _, ArrayRef, Decimal128Array, StringArray, UInt8Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use market_squawk_adapter_files::{
    ExtractionClock, ExtractionClockError, ExtractionClockReading, ExtractionLimits,
    ExtractionLimitsInput, FileAdapterError, FileExtractionSource,
};
use market_squawk_data::{
    AnalyticalDataService, AnalyticalManifestCatalog, CatalogAuthority, CatalogConfig,
    CatalogError, CatalogLimit, CatalogResultLimits, DatasetId, IngestIdentity, ObjectStoreConfig,
    QueryLimits, QueryRequest, QueryResult, ResearchIngestService, ResearchQueryEngine,
    RightsBasis, RightsDecisionInput, SourceOperation, extraction_batch_digest,
    extraction_provider_payload_digest,
};
use market_squawk_domain::{
    AuthorizationBasis, ChecksumCapability, CoverageDelay, DataQuality, DeliveryEvidence,
    DigestAlgorithm, EffectiveInterval, EvidenceDigest, ExactPayloadEvidence, InstrumentId,
    MetadataRevision, ResearchObservation, ResearchTemporalCoordinate,
    RevisionBoundPayloadEvidence, SchemaVersion, SequenceCapability, SourceId, SourceIdentifier,
    Timestamp,
};
use market_squawk_platform::{LocalPaths, UserAuthorizedInputRoot};
use market_squawk_sources::{
    AuthoritativeSourceRegistry, AuthorizationGrant, AuthorizationMode, CoverageDomain,
    DiscoveryRequest, ExtractionAuthority, ExtractionBatch, ExtractionError, ExtractionRequest,
    ExtractionSource, ExtractionSourceError, FreshnessPolicy, HistoricalCapability,
    NetworkAccessPolicy, SourceCapabilities, SourceClass, SourceCoverage, SourceMetadata,
    SourceMetadataInput, SourceMetadataProvider, SourceProtocolProfile,
};
use parquet::arrow::ArrowWriter;
use rusqlite::Connection;
use sha2::{Digest as _, Sha256};
use tokio_util::sync::CancellationToken;
use zip::ZipWriter;
use zip::write::SimpleFileOptions;

const MANIFEST: &[u8] = include_bytes!("../fixtures/manifest.json");
const EXPECTED_FORMAT_ROWS: usize = 10;

type TestResult<T = ()> = Result<T, Box<dyn Error>>;

const FIRST_INSTRUMENT: &str = "11111111-1111-4111-8111-111111111111";
const SECOND_INSTRUMENT: &str = "22222222-2222-4222-8222-222222222222";

#[derive(Debug)]
struct AdvancingClock {
    origin: Instant,
    next_offset_nanos: Mutex<u64>,
}

struct AuthorizedLocalSource {
    source: FileExtractionSource,
    authority: ExtractionAuthority,
    _registry: AuthoritativeSourceRegistry,
}

#[tokio::test]
async fn instrument_binding_is_explicit_exact_and_replay_bound() -> TestResult {
    let directory = tempfile::tempdir()?;
    let state = tempfile::tempdir()?;
    fs::write(
        directory.path().join("explicit.csv"),
        b"id,ticker,value\nrow-1,AAPL,1.00\n",
    )?;
    fs::write(
        directory.path().join("unscoped.csv"),
        b"id,ticker,value\nrow-2,MSFT,2.00\n",
    )?;
    let manifest = binding_manifest(Some(serde_json::json!({
        "kind": "internal_instrument",
        "instrument_id": FIRST_INSTRUMENT,
    })))?;
    let original = authorized_local_source(
        directory.path(),
        state.path().join("original"),
        "manifest-original.json",
        &manifest,
    )?;
    let discovery = DiscoveryRequest::try_new(
        SourceIdentifier::try_from("alternative-prices")?,
        None,
        NonZeroU16::new(2).ok_or("nonzero result limit")?,
        Timestamp::from_unix_nanos(10_000_000_000),
    )?;
    let discovered = original
        .source
        .discover_files(&original.authority, &discovery, &CancellationToken::new())
        .await?;
    let explicit = discovered
        .objects()
        .iter()
        .find(|object| object.object_id().as_str() == "explicit")
        .cloned()
        .ok_or("explicit object was not discovered")?;
    let unscoped = discovered
        .objects()
        .iter()
        .find(|object| object.object_id().as_str() == "unscoped")
        .cloned()
        .ok_or("unscoped object was not discovered")?;
    let rediscovered = original
        .source
        .discover_files(&original.authority, &discovery, &CancellationToken::new())
        .await?;
    assert_eq!(rediscovered.objects(), discovered.objects());

    let explicit_batch = extract_one(&original, explicit.clone()).await?;
    let explicit_replay = extract_one(&original, explicit.clone()).await?;
    assert_eq!(
        extraction_batch_digest(&explicit_batch)?,
        extraction_batch_digest(&explicit_replay)?
    );
    assert_eq!(
        observation_instrument(&explicit_batch)?,
        Some(FIRST_INSTRUMENT.parse::<InstrumentId>()?)
    );
    assert_eq!(
        observation_instrument(&extract_one(&original, unscoped).await?)?,
        None
    );

    for (name, invalid_binding) in [
        ("missing", None),
        (
            "ticker",
            Some(serde_json::json!({
                "kind": "internal_instrument",
                "instrument_id": "AAPL",
            })),
        ),
        (
            "nil",
            Some(serde_json::json!({
                "kind": "internal_instrument",
                "instrument_id": "00000000-0000-0000-0000-000000000000",
            })),
        ),
    ] {
        let invalid = binding_manifest(invalid_binding)?;
        let invalid_source = unregistered_local_source(
            directory.path(),
            state.path().join(format!("invalid-{name}")),
            &format!("manifest-invalid-{name}.json"),
            &invalid,
        )?;
        assert!(matches!(
            invalid_source,
            Err(FileAdapterError::InvalidManifest)
        ));
    }

    let changed_manifest = binding_manifest(Some(serde_json::json!({
        "kind": "internal_instrument",
        "instrument_id": SECOND_INSTRUMENT,
    })))?;
    let changed = authorized_local_source(
        directory.path(),
        state.path().join("changed"),
        "manifest-changed.json",
        &changed_manifest,
    )?;
    let changed_discovery = changed
        .source
        .discover_files(&changed.authority, &discovery, &CancellationToken::new())
        .await?;
    let changed_explicit = changed_discovery
        .objects()
        .iter()
        .find(|object| object.object_id().as_str() == "explicit")
        .cloned()
        .ok_or("changed explicit object was not discovered")?;
    assert_eq!(
        changed_explicit.evidence().content_digest(),
        explicit.evidence().content_digest()
    );
    assert_ne!(changed_explicit.evidence(), explicit.evidence());
    let old_request = ExtractionRequest::try_new(
        explicit,
        NonZeroU32::new(1).ok_or("nonzero record limit")?,
        NonZeroU64::new(1024 * 1024).ok_or("nonzero byte limit")?,
        Timestamp::from_unix_nanos(10_000_000_000),
    )?;
    assert_eq!(
        changed
            .source
            .extract_file(&changed.authority, &old_request, &CancellationToken::new())
            .await
            .err()
            .ok_or("old binding replay unexpectedly succeeded")?,
        FileAdapterError::ObjectEvidenceMismatch
    );
    assert_eq!(
        observation_instrument(&extract_one(&changed, changed_explicit).await?)?,
        Some(SECOND_INSTRUMENT.parse::<InstrumentId>()?)
    );
    Ok(())
}

fn binding_manifest(
    explicit_binding: Option<serde_json::Value>,
) -> Result<Vec<u8>, serde_json::Error> {
    let object =
        |object_id: &str, path: &str, instrument_binding: serde_json::Value| -> serde_json::Value {
            serde_json::json!({
                "dataset": "alternative-prices",
                "object_id": object_id,
                "path": path,
                "format": { "kind": "csv", "delimiter": 44 },
                "effective_at": 100,
                "published_at": 150,
                "revision": "revision-1",
                "revision_number": 1,
                "superseded_at": null,
                "record_time": {
                    "effective": ResearchTemporalCoordinate::exact(
                        Timestamp::from_unix_nanos(100)
                    ),
                    "published": ResearchTemporalCoordinate::exact(
                        Timestamp::from_unix_nanos(150)
                    ),
                    "superseded": null,
                },
                "instrument_binding": instrument_binding,
                "row_policy": {
                    "identity_field": "id",
                    "fields": [{
                        "source": "value",
                        "field": "price",
                        "decimal_scale": 2,
                        "unit": "USD",
                    }],
                },
            })
        };
    let mut explicit = object(
        "explicit",
        "explicit.csv",
        explicit_binding
            .clone()
            .unwrap_or_else(|| serde_json::json!({ "kind": "unscoped" })),
    );
    if explicit_binding.is_none() {
        let _ = explicit
            .as_object_mut()
            .and_then(|object| object.remove("instrument_binding"));
    }
    serde_json::to_vec(&serde_json::json!({
        "schema_version": 3,
        "objects": [
            explicit,
            object(
                "unscoped",
                "unscoped.csv",
                serde_json::json!({ "kind": "unscoped" }),
            ),
        ],
    }))
}

fn authorized_local_source(
    input_root: &std::path::Path,
    state_root: std::path::PathBuf,
    manifest_name: &str,
    manifest: &[u8],
) -> TestResult<AuthorizedLocalSource> {
    let source = unregistered_local_source(input_root, state_root, manifest_name, manifest)??;
    let mut registry = AuthoritativeSourceRegistry::try_new_ephemeral_for_diagnostics()?;
    let registered =
        registry.register(source.metadata().clone(), Timestamp::from_unix_nanos(300))?;
    let authority = registry.extraction_authority(&registered, &source)?;
    Ok(AuthorizedLocalSource {
        source,
        authority,
        _registry: registry,
    })
}

fn unregistered_local_source(
    input_root: &std::path::Path,
    state_root: std::path::PathBuf,
    manifest_name: &str,
    manifest: &[u8],
) -> TestResult<Result<FileExtractionSource, FileAdapterError>> {
    fs::write(input_root.join(manifest_name), manifest)?;
    let root = UserAuthorizedInputRoot::open(fs::canonicalize(input_root)?)?;
    let manifest_input = root
        .resolve(manifest_name)?
        .open_bounded(u64::try_from(manifest.len())?)?
        .read_bounded()?;
    Ok(FileExtractionSource::try_new_with_clock(
        local_metadata_for(manifest)?,
        root,
        state_root,
        manifest_input,
        ExtractionLimits::try_new(ExtractionLimitsInput::standard())?,
        Arc::new(AdvancingClock {
            origin: Instant::now(),
            next_offset_nanos: Mutex::new(0),
        }),
    ))
}

async fn extract_one(
    source: &AuthorizedLocalSource,
    object: market_squawk_sources::SourceObject,
) -> TestResult<ExtractionBatch> {
    let request = ExtractionRequest::try_new(
        object,
        NonZeroU32::new(1).ok_or("nonzero record limit")?,
        NonZeroU64::new(1024 * 1024).ok_or("nonzero byte limit")?,
        Timestamp::from_unix_nanos(10_000_000_000),
    )?;
    Ok(source
        .source
        .extract_file(&source.authority, &request, &CancellationToken::new())
        .await?)
}

fn observation_instrument(batch: &ExtractionBatch) -> TestResult<Option<InstrumentId>> {
    let observation: ResearchObservation = serde_json::from_slice(batch.records()[0].payload())?;
    let ResearchObservation::AlternativeData(observation) = observation else {
        return Err("local binding extraction produced the wrong observation kind".into());
    };
    Ok(observation.context().provenance().instrument_id())
}

impl ExtractionClock for AdvancingClock {
    fn observe(&self) -> Result<ExtractionClockReading, ExtractionClockError> {
        let mut next = self
            .next_offset_nanos
            .lock()
            .map_err(|_| ExtractionClockError::Unavailable)?;
        let offset = *next;
        *next = next.checked_add(1).ok_or(ExtractionClockError::Range)?;
        let wall = i64::try_from(offset)
            .ok()
            .and_then(|offset| 300_i64.checked_add(offset))
            .ok_or(ExtractionClockError::Range)?;
        let monotonic = self
            .origin
            .checked_add(Duration::from_nanos(offset))
            .ok_or(ExtractionClockError::Range)?;
        Ok(ExtractionClockReading::new(
            Timestamp::from_unix_nanos(wall),
            monotonic,
        ))
    }
}

#[tokio::test]
async fn every_local_format_survives_rights_publish_restart_and_exact_query() -> TestResult {
    let directory = tempfile::tempdir()?;
    let representation_state_directory = tempfile::tempdir()?;
    write_format_fixtures(directory.path())?;
    fs::write(directory.path().join("manifest.json"), MANIFEST)?;
    let representation_state_root = representation_state_directory.path().join("authority");

    let (root, ownership_authority) = UserAuthorizedInputRoot::open_with_ownership_authority(
        fs::canonicalize(directory.path())?,
    )?;
    let manifest_input = root
        .resolve("manifest.json")?
        .open_bounded(u64::try_from(MANIFEST.len())?)?
        .read_bounded()?;
    let local_rights_basis = RightsBasis::user_owned_local(
        ownership_authority.issue_manifest_evidence(&manifest_input)?,
    );
    let metadata = local_metadata()?;
    let source = FileExtractionSource::try_new_with_clock(
        metadata.clone(),
        root,
        &representation_state_root,
        manifest_input,
        ExtractionLimits::try_new(ExtractionLimitsInput::standard())?,
        Arc::new(AdvancingClock {
            origin: Instant::now(),
            next_offset_nanos: Mutex::new(0),
        }),
    )?;
    let mut extraction_registry = AuthoritativeSourceRegistry::try_new_ephemeral_for_diagnostics()?;
    let registered =
        extraction_registry.register(metadata.clone(), Timestamp::from_unix_nanos(300))?;
    let extraction_authority = extraction_registry.extraction_authority(&registered, &source)?;
    let discovery = DiscoveryRequest::try_new(
        SourceIdentifier::try_from("alternative-prices")?,
        None,
        NonZeroU16::new(EXPECTED_FORMAT_ROWS as u16).ok_or("nonzero format count")?,
        Timestamp::from_unix_nanos(10_000_000_000),
    )?;
    let discovered = source
        .discover_files(&extraction_authority, &discovery, &CancellationToken::new())
        .await?;
    assert_eq!(discovered.objects().len(), EXPECTED_FORMAT_ROWS);
    let original_csv = discovered
        .objects()
        .iter()
        .find(|object| object.object_id().as_str() == "csv-fixture")
        .cloned()
        .ok_or("original CSV discovery is absent")?;
    let rediscovered_csv = source
        .discover_files(&extraction_authority, &discovery, &CancellationToken::new())
        .await?
        .objects()
        .iter()
        .find(|object| object.object_id().as_str() == "csv-fixture")
        .cloned()
        .ok_or("repeat CSV discovery is absent")?;
    assert_eq!(rediscovered_csv.evidence(), original_csv.evidence());
    assert_eq!(rediscovered_csv.availability(), original_csv.availability());

    let mut batches = Vec::with_capacity(EXPECTED_FORMAT_ROWS);
    for object in discovered.objects() {
        let request = ExtractionRequest::try_new(
            object.clone(),
            NonZeroU32::new(1).ok_or("nonzero record limit")?,
            NonZeroU64::new(1024 * 1024).ok_or("nonzero byte limit")?,
            Timestamp::from_unix_nanos(10_000_000_000),
        )?;
        batches.push(
            source
                .extract_file(&extraction_authority, &request, &CancellationToken::new())
                .await?,
        );
    }
    for (object, batch) in discovered.objects().iter().zip(&batches) {
        assert_eq!(batch.records()[0].availability(), object.availability());
        let observation: ResearchObservation =
            serde_json::from_slice(batch.records()[0].payload())?;
        let ResearchObservation::AlternativeData(observation) = observation else {
            return Err("local extraction produced the wrong observation kind".into());
        };
        let provenance = observation.context().provenance();
        assert_eq!(
            provenance.availability().conservative_available_at(),
            object.availability().conservative_available_at()
        );
        assert!(
            object
                .availability()
                .conservative_available_at()
                .is_some_and(|available_at| available_at < provenance.received_at())
        );
        assert!(provenance.received_at() < provenance.ingested_at());
    }

    fs::write(
        directory.path().join("prices.csv"),
        b"id,value\nfirst,1.00\nsecond,not-a-decimal\n",
    )?;
    let bounded_csv = source
        .discover_files(&extraction_authority, &discovery, &CancellationToken::new())
        .await?
        .objects()
        .iter()
        .find(|object| object.object_id().as_str() == "csv-fixture")
        .cloned()
        .ok_or("bounded CSV discovery is absent")?;
    let bounded_error = source
        .extract(
            extraction_authority.clone(),
            ExtractionRequest::try_new(
                bounded_csv,
                NonZeroU32::new(2).ok_or("nonzero record limit")?,
                NonZeroU64::new(1).ok_or("nonzero byte limit")?,
                Timestamp::from_unix_nanos(10_000_000_000),
            )?,
            CancellationToken::new(),
        )
        .await
        .err()
        .ok_or("one-byte extraction request unexpectedly built an output batch")?;
    assert!(matches!(
        bounded_error,
        ExtractionSourceError::Contract(ExtractionError::ByteLimitExceeded { requested: 1 })
    ));

    fs::write(
        directory.path().join("prices.csv"),
        b"id,value\nwithin-limit,1.00\nafter-limit,\"unterminated\n",
    )?;
    let request_limited_csv = source
        .discover_files(&extraction_authority, &discovery, &CancellationToken::new())
        .await?
        .objects()
        .iter()
        .find(|object| object.object_id().as_str() == "csv-fixture")
        .cloned()
        .ok_or("request-limited CSV discovery is absent")?;
    let request_limit_error = source
        .extract(
            extraction_authority.clone(),
            ExtractionRequest::try_new(
                request_limited_csv,
                NonZeroU32::new(1).ok_or("nonzero record limit")?,
                NonZeroU64::new(1024 * 1024).ok_or("nonzero byte limit")?,
                Timestamp::from_unix_nanos(10_000_000_000),
            )?,
            CancellationToken::new(),
        )
        .await
        .err()
        .ok_or("CSV content after the request row limit was unexpectedly parsed")?;
    assert!(matches!(
        request_limit_error,
        ExtractionSourceError::Contract(ExtractionError::RecordLimitExceeded { requested: 1 })
    ));

    fs::write(
        directory.path().join("prices.csv"),
        b"id,value\nduplicate,1.00\nduplicate,2.00\n",
    )?;
    let duplicate_csv = source
        .discover_files(&extraction_authority, &discovery, &CancellationToken::new())
        .await?
        .objects()
        .iter()
        .find(|object| object.object_id().as_str() == "csv-fixture")
        .cloned()
        .ok_or("duplicate CSV discovery is absent")?;
    let duplicate_error = source
        .extract_file(
            &extraction_authority,
            &ExtractionRequest::try_new(
                duplicate_csv,
                NonZeroU32::new(2).ok_or("nonzero record limit")?,
                NonZeroU64::new(1024 * 1024).ok_or("nonzero byte limit")?,
                Timestamp::from_unix_nanos(10_000_000_000),
            )?,
            &CancellationToken::new(),
        )
        .await
        .err()
        .ok_or("duplicate manifest row identity unexpectedly emitted records")?;
    assert_eq!(
        duplicate_error,
        market_squawk_adapter_files::FileAdapterError::DuplicateField
    );

    fs::write(
        directory.path().join("prices.csv"),
        b"id,value\ncsv-row,99.00\n",
    )?;
    let changed_csv = source
        .discover_files(&extraction_authority, &discovery, &CancellationToken::new())
        .await?
        .objects()
        .iter()
        .find(|object| object.object_id().as_str() == "csv-fixture")
        .cloned()
        .ok_or("changed CSV discovery is absent")?;
    assert_ne!(changed_csv.evidence(), original_csv.evidence());
    assert!(
        changed_csv
            .availability()
            .conservative_available_at()
            .zip(original_csv.availability().conservative_available_at())
            .is_some_and(|(changed, original)| changed > original)
    );
    let changed_availability = changed_csv.availability().clone();
    let changed_csv = source
        .extract_file(
            &extraction_authority,
            &ExtractionRequest::try_new(
                changed_csv,
                NonZeroU32::new(1).ok_or("nonzero record limit")?,
                NonZeroU64::new(1024 * 1024).ok_or("nonzero byte limit")?,
                Timestamp::from_unix_nanos(10_000_000_000),
            )?,
            &CancellationToken::new(),
        )
        .await?;
    assert_eq!(
        changed_csv.records()[0].availability(),
        &changed_availability
    );

    let paths = LocalPaths::prepare(directory.path().join("state"))?;
    let location = paths.catalog()?.clone();
    let catalog_config = CatalogConfig::try_new(
        location.clone(),
        Duration::from_millis(750),
        CatalogLimit::new(64)?,
        CatalogResultLimits::try_new(1024 * 1024, 8 * 1024 * 1024)?,
    )?;
    let authority = CatalogAuthority::open(catalog_config.clone())?;
    authority.register_source(&metadata, Timestamp::from_unix_nanos(10))?;

    let mut admitted = Vec::with_capacity(EXPECTED_FORMAT_ROWS);
    for batch in batches {
        let key = idempotency_key(&batch);
        let payload_digest = extraction_provider_payload_digest(&batch);
        let rights = admit_rights(
            &authority,
            metadata.source_id().clone(),
            payload_digest,
            &local_rights_basis,
        )?;
        let reservation = authority.reserve_ingest(
            &IngestIdentity::try_new(
                metadata.source_id().clone(),
                payload_digest,
                SourceOperation::Persist,
                key,
            )?,
            &rights,
        )?;
        admitted.push((reservation, batch));
    }

    fs::write(
        directory.path().join("prices.csv"),
        b"id,value\ncsv-row,1.00\n",
    )?;
    drop(source);
    let root = UserAuthorizedInputRoot::open(fs::canonicalize(directory.path())?)?;
    let manifest_input = root
        .resolve("manifest.json")?
        .open_bounded(u64::try_from(MANIFEST.len())?)?
        .read_bounded()?;
    let restarted_source = FileExtractionSource::try_new_with_clock(
        metadata.clone(),
        root,
        &representation_state_root,
        manifest_input,
        ExtractionLimits::try_new(ExtractionLimitsInput::standard())?,
        Arc::new(AdvancingClock {
            origin: Instant::now(),
            next_offset_nanos: Mutex::new(10_000),
        }),
    )?;
    let restarted_object = restarted_source
        .discover_files(&extraction_authority, &discovery, &CancellationToken::new())
        .await?
        .objects()
        .iter()
        .find(|object| object.object_id().as_str() == "csv-fixture")
        .cloned()
        .ok_or("restarted CSV discovery is absent")?;
    assert_eq!(restarted_object.evidence(), original_csv.evidence());
    assert_eq!(restarted_object.availability(), original_csv.availability());
    let restarted_batch = restarted_source
        .extract_file(
            &extraction_authority,
            &ExtractionRequest::try_new(
                restarted_object,
                NonZeroU32::new(1).ok_or("nonzero record limit")?,
                NonZeroU64::new(1024 * 1024).ok_or("nonzero byte limit")?,
                Timestamp::from_unix_nanos(10_000_000_000),
            )?,
            &CancellationToken::new(),
        )
        .await?;
    let restarted_digest = extraction_provider_payload_digest(&restarted_batch);
    let restarted_rights = admit_rights(
        &authority,
        metadata.source_id().clone(),
        restarted_digest,
        &local_rights_basis,
    )?;
    let restarted_reservation = authority.reserve_ingest(
        &IngestIdentity::try_new(
            metadata.source_id().clone(),
            restarted_digest,
            SourceOperation::Persist,
            "local-file:csv-fixture",
        )?,
        &restarted_rights,
    )?;

    let changed_digest = extraction_provider_payload_digest(&changed_csv);
    let changed_rights = admit_rights(
        &authority,
        metadata.source_id().clone(),
        changed_digest,
        &local_rights_basis,
    )?;
    let conflict = authority.reserve_ingest(
        &IngestIdentity::try_new(
            metadata.source_id().clone(),
            changed_digest,
            SourceOperation::Persist,
            "local-file:csv-fixture",
        )?,
        &changed_rights,
    );
    assert!(matches!(conflict, Err(CatalogError::IdempotencyConflict)));

    let object_config = ObjectStoreConfig::try_new(8 * 1024 * 1024, 1024, Duration::from_secs(60))?;
    let service = AnalyticalDataService::initialize(
        authority,
        AnalyticalManifestCatalog::open(&location, 16)?,
        paths.artifacts()?.clone(),
        object_config,
    )?;
    let mut final_manifest = None;
    let mut committed_csv = None;
    for (reservation, batch) in admitted {
        let is_csv = batch.request().object().object_id().as_str() == "csv-fixture";
        let analytical_dataset = DatasetId::try_from(batch.request().object().dataset().as_str())?;
        let committed = service
            .ingest(
                reservation.clone(),
                analytical_dataset.clone(),
                batch.clone(),
                CancellationToken::new(),
            )
            .await?;
        let replayed = service
            .ingest(
                reservation,
                analytical_dataset,
                batch,
                CancellationToken::new(),
            )
            .await?;
        assert_eq!(replayed, committed);
        if is_csv {
            committed_csv = Some(committed.clone());
        }
        final_manifest = Some(committed.manifest().clone());
    }
    let restarted_commit = service
        .ingest(
            restarted_reservation,
            DatasetId::try_from(restarted_batch.request().object().dataset().as_str())?,
            restarted_batch,
            CancellationToken::new(),
        )
        .await?;
    assert_eq!(
        restarted_commit,
        committed_csv.ok_or("CSV was not committed before restart replay")?
    );
    let final_manifest = final_manifest.ok_or("no local format was published")?;
    drop(service);

    let restarted = AnalyticalDataService::open(
        CatalogAuthority::open(catalog_config)?,
        AnalyticalManifestCatalog::open(&location, 16)?,
        paths.artifacts()?.clone(),
        object_config,
    )?;
    let pinned = restarted.pinned(&final_manifest)?;
    assert_eq!(pinned.plan().row_count(), EXPECTED_FORMAT_ROWS as u64);
    let engine = ResearchQueryEngine::from_pinned_dataset(
        pinned,
        "observations",
        restarted.object_store(),
        CancellationToken::new(),
    )
    .await?;
    let result = engine
        .query(
            QueryRequest::try_new(
                final_manifest,
                "SELECT source_identifier, value_mantissa, value_scale \
                 FROM observations ORDER BY value_mantissa",
            )?,
            QueryLimits::try_new(
                16,
                64 * 1024,
                8 * 1024 * 1024,
                1,
                128,
                128,
                Duration::from_secs(2),
            )?,
            CancellationToken::new(),
        )
        .await?;
    assert_exact_query_rows(result)?;
    Ok(())
}

fn admit_rights(
    authority: &CatalogAuthority,
    source_id: SourceId,
    payload_digest: EvidenceDigest,
    basis: &RightsBasis,
) -> Result<market_squawk_data::RegisteredRightsGrant, CatalogError> {
    authority.admit_source_rights(RightsDecisionInput {
        source_id,
        payload_digest,
        retrieved_at: Timestamp::from_unix_nanos(15),
        basis: basis.clone(),
        authorization_evidence: digest(32),
        authorization_expires_at: Some(Timestamp::from_unix_nanos(i64::MAX)),
        permitted_operations: vec![SourceOperation::Persist],
    })
}

fn idempotency_key(batch: &ExtractionBatch) -> String {
    format!(
        "local-file:{}",
        batch.request().object().object_id().as_str()
    )
}

fn assert_exact_query_rows(result: QueryResult) -> TestResult {
    let QueryResult::Inline { batches, .. } = result else {
        return Err("small exact query unexpectedly produced an artifact".into());
    };
    let mut rows = Vec::new();
    for batch in batches {
        let identifiers = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or("source_identifier query type changed")?;
        let mantissas = batch
            .column(1)
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .ok_or("value_mantissa query type changed")?;
        let scales = batch
            .column(2)
            .as_any()
            .downcast_ref::<UInt8Array>()
            .ok_or("value_scale query type changed")?;
        for row in 0..batch.num_rows() {
            rows.push((
                identifiers.value(row).to_owned(),
                mantissas.value(row),
                scales.value(row),
            ));
        }
    }
    assert_eq!(
        rows,
        [
            ("csv-row", 1_i128, 0_u8),
            ("tsv-row", 2, 0),
            ("json-row", 3, 0),
            ("ndjson-row", 4, 0),
            ("xml-row", 5, 0),
            ("excel-row", 6, 0),
            ("parquet-row", 7, 0),
            ("sqlite-row", 8, 0),
            ("ofx-row", 9, 0),
            ("qfx-row", 10, 0),
        ]
        .map(|(id, mantissa, scale)| (id.to_owned(), mantissa, scale))
    );
    Ok(())
}

fn write_format_fixtures(root: &std::path::Path) -> TestResult {
    fs::write(root.join("prices.csv"), b"id,value\ncsv-row,1.00\n")?;
    fs::write(root.join("prices.tsv"), b"id\tvalue\ntsv-row\t2.00\n")?;
    fs::write(
        root.join("prices.json"),
        br#"[{"id":"json-row","value":3.00}]"#,
    )?;
    fs::write(
        root.join("prices.ndjson"),
        b"{\"id\":\"ndjson-row\",\"value\":4.00e0}\n",
    )?;
    fs::write(
        root.join("prices.xml"),
        b"<rows><row><id>xml-row</id><value>5.00</value></row></rows>",
    )?;
    fs::write(root.join("prices.xlsx"), xlsx_fixture()?)?;
    fs::write(root.join("prices.parquet"), parquet_fixture()?)?;
    write_sqlite_fixture(&root.join("prices.sqlite3"))?;
    fs::write(root.join("prices.ofx"), legacy_ofx_fixture())?;
    fs::write(root.join("prices.qfx"), xml_ofx_fixture())?;
    Ok(())
}

fn xlsx_fixture() -> TestResult<Vec<u8>> {
    let sheet = br#"<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"><sheetData><row r="1"><c r="A1" t="inlineStr"><is><t>id</t></is></c><c r="B1" t="inlineStr"><is><t>value</t></is></c></row><row r="2"><c r="A2" t="inlineStr"><is><t>excel-row</t></is></c><c r="B2" t="inlineStr"><is><t>6.00</t></is></c></row></sheetData></worksheet>"#;
    let mut archive = ZipWriter::new(Cursor::new(Vec::new()));
    let files: [(&str, &[u8]); 5] = [
        (
            "[Content_Types].xml",
            br#"<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types"><Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/><Default Extension="xml" ContentType="application/xml"/><Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/><Override PartName="/xl/worksheets/sheet1.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/></Types>"#,
        ),
        (
            "_rels/.rels",
            br#"<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/></Relationships>"#,
        ),
        (
            "xl/workbook.xml",
            br#"<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships"><sheets><sheet name="Data" sheetId="1" r:id="rId1"/></sheets></workbook>"#,
        ),
        (
            "xl/_rels/workbook.xml.rels",
            br#"<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet1.xml"/></Relationships>"#,
        ),
        ("xl/worksheets/sheet1.xml", sheet),
    ];
    for (name, bytes) in files {
        archive.start_file(name, SimpleFileOptions::default())?;
        archive.write_all(bytes)?;
    }
    Ok(archive.finish()?.into_inner())
}

fn parquet_fixture() -> TestResult<Vec<u8>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("value", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(StringArray::from(vec!["parquet-row"])) as ArrayRef,
            Arc::new(StringArray::from(vec!["7.00"])) as ArrayRef,
        ],
    )?;
    let mut writer = ArrowWriter::try_new(Vec::new(), schema, None)?;
    writer.write(&batch)?;
    Ok(writer.into_inner()?)
}

fn write_sqlite_fixture(path: &std::path::Path) -> TestResult {
    let connection = Connection::open(path)?;
    connection.execute_batch(
        "CREATE TABLE prices(id TEXT PRIMARY KEY, value TEXT NOT NULL);\
         INSERT INTO prices(id, value) VALUES ('sqlite-row', '8.00');",
    )?;
    Ok(())
}

fn legacy_ofx_fixture() -> Vec<u8> {
    b"OFXHEADER:100\nDATA:OFXSGML\nVERSION:160\nSECURITY:NONE\nENCODING:USASCII\nCHARSET:USASCII\nCOMPRESSION:NONE\nOLDFILEUID:NONE\nNEWFILEUID:NONE\n\n<OFX><BANKMSGSRSV1><STMTTRNRS><STMTRS><CURDEF>USD\n<BANKACCTFROM><BANKID>123456789\n<ACCTID>acct-1\n<ACCTTYPE>CHECKING\n</BANKACCTFROM><BANKTRANLIST><DTSTART>20260701000000\n<DTEND>20260718120000[-4:EDT]\n<STMTTRN><TRNTYPE>CREDIT\n<DTPOSTED>20260718120000[-4:EDT]\n<TRNAMT>9.00\n<FITID>ofx-row\n</STMTTRN></BANKTRANLIST><LEDGERBAL><BALAMT>100.00\n<DTASOF>20260718120000[-4:EDT]\n</LEDGERBAL></STMTRS></STMTTRNRS></BANKMSGSRSV1></OFX>".to_vec()
}

fn xml_ofx_fixture() -> Vec<u8> {
    br#"<?xml version="1.0" encoding="UTF-8"?><?OFX OFXHEADER="200" VERSION="230" SECURITY="NONE" OLDFILEUID="NONE" NEWFILEUID="NONE"?><OFX><BANKMSGSRSV1><STMTTRNRS><STMTRS><CURDEF>USD</CURDEF><BANKACCTFROM><BANKID>123456789</BANKID><ACCTID>acct-1</ACCTID><ACCTTYPE>CHECKING</ACCTTYPE></BANKACCTFROM><BANKTRANLIST><DTSTART>20260701000000</DTSTART><DTEND>20260718120000[-4:EDT]</DTEND><STMTTRN><TRNTYPE>CREDIT</TRNTYPE><DTPOSTED>20260718120000[-4:EDT]</DTPOSTED><TRNAMT>10.00</TRNAMT><FITID>qfx-row</FITID></STMTTRN></BANKTRANLIST><LEDGERBAL><BALAMT>100.00</BALAMT><DTASOF>20260718120000[-4:EDT]</DTASOF></LEDGERBAL></STMTRS></STMTTRNRS></BANKMSGSRSV1></OFX>"#.to_vec()
}

fn local_metadata() -> Result<SourceMetadata, Box<dyn Error>> {
    local_metadata_for(MANIFEST)
}

fn local_metadata_for(manifest: &[u8]) -> Result<SourceMetadata, Box<dyn Error>> {
    let evidence = ExactPayloadEvidence::from_content_digest(EvidenceDigest::new(
        DigestAlgorithm::Sha256,
        Sha256::digest(manifest).into(),
    ));
    let effective = EffectiveInterval::new(Timestamp::from_unix_nanos(0), None)?;
    Ok(SourceMetadata::try_new(SourceMetadataInput::new(
        SchemaVersion::CURRENT,
        SourceId::try_from("local-files-vertical")?,
        RevisionBoundPayloadEvidence::new(
            MetadataRevision::new(SourceIdentifier::try_from("manifest-revision-1")?),
            evidence.clone(),
        ),
        SourceClass::LocalFile,
        SourceIdentifier::try_from("user-owned-local-files")?,
        AuthorizationGrant::new(
            AuthorizationMode::UserOwnedLocal,
            AuthorizationBasis::new(SourceIdentifier::try_from("user-owned-file")?),
            evidence.clone(),
            effective,
        ),
        SourceCoverage::try_non_instrument(
            evidence,
            effective,
            CoverageDomain::AlternativeData,
            CoverageDelay::Delayed(1),
            DeliveryEvidence::Unknown,
        )?,
        DataQuality::DirectUnverified,
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
