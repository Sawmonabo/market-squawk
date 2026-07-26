//! Closed Arrow, Parquet, DataFusion, and typed Python-handoff release runner.

#[path = "benchmark_support/python_admission.rs"]
mod python_admission;

use std::num::NonZeroU32;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::{Int64Array, UInt64Array};
use market_squawk_domain::{
    AlternativeDataObservation, AvailabilityEvidence, DataQuality, DigestAlgorithm, EvidenceDigest,
    PayloadReference, ResearchContext, ResearchObservation, ResearchProvenance,
    ResearchProvenanceInput, ResearchTemporalCoordinate, ResearchTime, RevisionNumber, SourceId,
    SourceIdentifier, Timestamp,
};
use market_squawk_platform::LocalPaths;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::{GenerationKind, PinnedDataset, PinnedManifestObject, pinned_dataset_retained_bytes};
use crate::{
    AnalyticalDataService, AnalyticalManifestCatalog, CatalogAuthority, CatalogConfig,
    CatalogLimit, CatalogResultLimits, DatasetId, DatasetManifestRef, ManifestObject, ManifestPlan,
    ObjectStoreConfig, PointInTimeCandidate, PointInTimeLimits, PointInTimePolicy,
    PointInTimeRequest, PointInTimeRevisionMode, PointInTimeService, QueryLimits, QueryRequest,
    QueryResult, ResearchArrowBatch, ResearchQueryEngine, Sha256Digest,
};

const MAX_PHYSICAL_ROWS: usize = 4_096;
const MAX_REQUESTED_ROWS: u64 = 100_000_000;
const MAX_QUERY_ITERATIONS: u64 = 64;
const MIN_QUERY_ITERATIONS: u64 = 8;

/// Bounded distribution for one exact storage operation.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct StorageLatency {
    operations: u64,
    rows: u64,
    elapsed_nanos: u64,
    rows_per_second: u64,
    p50_nanos: u64,
    p95_nanos: u64,
    p99_nanos: u64,
    maximum_nanos: u64,
}

/// Complete real-storage measurement result.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ReleaseEvidenceStorageResult {
    requested_rows: u64,
    measured_rows: u64,
    physical_rows_per_object: u64,
    unique_parquet_objects: u64,
    parquet_content_sha256: [u8; 32],
    parquet_size_bytes: u64,
    arrow_conversion: StorageLatency,
    parquet_publication: StorageLatency,
    parquet_read: StorageLatency,
    datafusion_query: StorageLatency,
    point_in_time_selected_rows: u64,
    point_in_time_content_sha256: [u8; 32],
    point_in_time_audit_sha256: [u8; 32],
    point_in_time_retained_bytes: usize,
    python_verified_rows: u64,
    python_selected_rows_per_verification: u64,
    python_export_sha256: [u8; 32],
    python_catalog_identity: [u8; 32],
    python_selection_sha256: [u8; 32],
    python_dataset_admission_revalidation: StorageLatency,
}

/// Release storage fixture or operation failure.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum ReleaseEvidenceStorageError {
    /// The caller-selected bounds or fixed fixture were invalid.
    #[error("release-evidence storage fixture is invalid")]
    InvalidFixture,
    /// Controlled local catalog or object-store composition failed.
    #[error("release-evidence analytical storage initialization failed")]
    Initialization,
    /// Arrow canonicalization failed.
    #[error("release-evidence Arrow conversion failed")]
    Arrow,
    /// Parquet publication or immutable read failed.
    #[error("release-evidence Parquet operation failed")]
    Parquet,
    /// Pinned DataFusion query execution failed.
    #[error("release-evidence DataFusion query failed")]
    DataFusion,
    /// Bounded point-in-time selection failed.
    #[error("release-evidence point-in-time selection failed")]
    PointInTime,
    /// Typed Python dataset handoff admission failed.
    #[error("release-evidence Python handoff failed")]
    PythonHandoff,
}

/// Runs real canonical Arrow conversion, confined Parquet publication/read, pinned DataFusion SQL,
/// and the native typed Python handoff boundary.
///
/// Repeated Parquet publication uses identical canonical bytes, so content addressing keeps one
/// durable object while still performing every encode, stage, fsync, hash, and finalization.
///
/// # Errors
///
/// Returns a typed failure on invalid bounds, authority composition, storage, query, or handoff
/// failure. The supplied root is a benchmark-owned scratch directory.
pub async fn run_release_evidence_storage(
    root: &Path,
    requested_rows: u64,
) -> Result<ReleaseEvidenceStorageResult, ReleaseEvidenceStorageError> {
    if requested_rows == 0 || requested_rows > MAX_REQUESTED_ROWS {
        return Err(ReleaseEvidenceStorageError::InvalidFixture);
    }
    let physical_rows = usize::try_from(requested_rows.min(MAX_PHYSICAL_ROWS as u64))
        .map_err(|_| ReleaseEvidenceStorageError::InvalidFixture)?;
    let repetitions = requested_rows
        .checked_add(
            u64::try_from(physical_rows)
                .map_err(|_| ReleaseEvidenceStorageError::InvalidFixture)?
                .saturating_sub(1),
        )
        .and_then(|value| value.checked_div(u64::try_from(physical_rows).ok()?))
        .ok_or(ReleaseEvidenceStorageError::InvalidFixture)?;
    let measured_rows = repetitions
        .checked_mul(
            u64::try_from(physical_rows)
                .map_err(|_| ReleaseEvidenceStorageError::InvalidFixture)?,
        )
        .ok_or(ReleaseEvidenceStorageError::InvalidFixture)?;
    let observations = observations(physical_rows)?;
    let dataset = source_identifier("release-evidence-observations")?;
    let request_digest = EvidenceDigest::new(DigestAlgorithm::Sha256, [81; 32]);

    let mut arrow_samples = Vec::new();
    arrow_samples
        .try_reserve_exact(
            usize::try_from(repetitions)
                .map_err(|_| ReleaseEvidenceStorageError::InvalidFixture)?,
        )
        .map_err(|_| ReleaseEvidenceStorageError::Arrow)?;
    let arrow_started = Instant::now();
    let mut converted = None;
    for _ in 0..repetitions {
        let started = Instant::now();
        let batch = ResearchArrowBatch::try_from_observations(
            dataset.clone(),
            request_digest,
            observations.clone(),
        )
        .map_err(|_| ReleaseEvidenceStorageError::Arrow)?;
        arrow_samples.push(nanos(started.elapsed()));
        converted = Some(batch);
    }
    let arrow_elapsed = nanos(arrow_started.elapsed());
    let converted = converted.ok_or(ReleaseEvidenceStorageError::Arrow)?;
    let dataset_batch = converted.dataset_batch();
    let lineage = converted
        .lineage_digest()
        .map_err(|_| ReleaseEvidenceStorageError::Arrow)?;
    if lineage.algorithm() != DigestAlgorithm::Sha256 {
        return Err(ReleaseEvidenceStorageError::Arrow);
    }

    let paths =
        LocalPaths::prepare(root).map_err(|_| ReleaseEvidenceStorageError::Initialization)?;
    let location = paths
        .catalog()
        .map_err(|_| ReleaseEvidenceStorageError::Initialization)?
        .clone();
    let catalog = CatalogConfig::try_new(
        location.clone(),
        Duration::from_millis(750),
        CatalogLimit::new(64).map_err(|_| ReleaseEvidenceStorageError::Initialization)?,
        CatalogResultLimits::try_new(1024 * 1024, 16 * 1024 * 1024)
            .map_err(|_| ReleaseEvidenceStorageError::Initialization)?,
    )
    .map_err(|_| ReleaseEvidenceStorageError::Initialization)?;
    let authority =
        CatalogAuthority::open(catalog).map_err(|_| ReleaseEvidenceStorageError::Initialization)?;
    let manifests = AnalyticalManifestCatalog::open(&location, 8)
        .map_err(|_| ReleaseEvidenceStorageError::Initialization)?;
    let service = AnalyticalDataService::initialize(
        authority,
        manifests,
        paths
            .artifacts()
            .map_err(|_| ReleaseEvidenceStorageError::Initialization)?
            .clone(),
        ObjectStoreConfig::try_new(
            256 * 1024 * 1024,
            MAX_PHYSICAL_ROWS,
            Duration::from_secs(60),
        )
        .map_err(|_| ReleaseEvidenceStorageError::Initialization)?,
    )
    .map_err(|_| ReleaseEvidenceStorageError::Initialization)?;
    let store = service.object_store();
    let cancellation = CancellationToken::new();
    let publication = store
        .begin_publication(&cancellation)
        .await
        .map_err(|_| ReleaseEvidenceStorageError::Parquet)?;
    let mut parquet_samples = Vec::new();
    parquet_samples
        .try_reserve_exact(
            usize::try_from(repetitions)
                .map_err(|_| ReleaseEvidenceStorageError::InvalidFixture)?,
        )
        .map_err(|_| ReleaseEvidenceStorageError::Parquet)?;
    let parquet_started = Instant::now();
    let mut published: Option<crate::PublishedObject> = None;
    for _ in 0..repetitions {
        let started = Instant::now();
        let object = store
            .publish_dataset_under_lease(&dataset_batch, &cancellation, &publication)
            .await
            .map_err(|_| ReleaseEvidenceStorageError::Parquet)?;
        parquet_samples.push(nanos(started.elapsed()));
        if let Some(existing) = published.as_ref()
            && (object.content_hash() != existing.content_hash()
                || object.relative_reference() != existing.relative_reference()
                || object.size_bytes() != existing.size_bytes()
                || object.row_count() != existing.row_count())
        {
            return Err(ReleaseEvidenceStorageError::Parquet);
        }
        published = Some(object);
    }
    let parquet_elapsed = nanos(parquet_started.elapsed());
    drop(publication);
    let published = published.ok_or(ReleaseEvidenceStorageError::Parquet)?;
    let object = ManifestObject::try_new(
        published.content_hash(),
        published.row_count(),
        published.size_bytes(),
        Sha256Digest::new(lineage.bytes()),
    )
    .map_err(|_| ReleaseEvidenceStorageError::Parquet)?;
    let dataset_id = DatasetId::try_from("release-evidence-observations")
        .map_err(|_| ReleaseEvidenceStorageError::Parquet)?;
    let plan = ManifestPlan::append(dataset_id.clone(), None, object.clone(), 1)
        .map_err(|_| ReleaseEvidenceStorageError::Parquet)?;
    let manifest = DatasetManifestRef::try_new_with_schema(
        dataset_id,
        1,
        converted.schema_ref().clone(),
        plan.content_hash(),
    )
    .map_err(|_| ReleaseEvidenceStorageError::Parquet)?;
    let candidates = observations
        .iter()
        .cloned()
        .map(|observation| PointInTimeCandidate::new(observation, manifest.clone()))
        .collect::<Vec<_>>();
    let point_in_time = PointInTimeService::new()
        .select(
            &PointInTimeRequest::try_new(
                PointInTimePolicy::try_new(NonZeroU32::MIN, PointInTimeRevisionMode::LatestKnown)
                    .map_err(|_| ReleaseEvidenceStorageError::PointInTime)?,
                Timestamp::from_unix_nanos(130),
                None,
                ResearchTemporalCoordinate::exact(Timestamp::from_unix_nanos(90)),
                None,
                PointInTimeLimits::try_new(
                    physical_rows,
                    physical_rows,
                    physical_rows,
                    physical_rows,
                    16 * 1024 * 1024,
                )
                .map_err(|_| ReleaseEvidenceStorageError::PointInTime)?,
            )
            .map_err(|_| ReleaseEvidenceStorageError::PointInTime)?,
            &candidates,
            &cancellation,
            Instant::now() + Duration::from_secs(10),
        )
        .await
        .map_err(|_| ReleaseEvidenceStorageError::PointInTime)?;
    if point_in_time.records().len() != physical_rows
        || !point_in_time.exclusions().is_empty()
        || point_in_time.revision_counts().current() != physical_rows
    {
        return Err(ReleaseEvidenceStorageError::PointInTime);
    }
    let point_in_time_selected_rows = u64::try_from(point_in_time.records().len())
        .map_err(|_| ReleaseEvidenceStorageError::PointInTime)?;
    let point_in_time_content_sha256 = point_in_time.content_identity().bytes();
    let point_in_time_audit_sha256 = point_in_time.audit_identity().bytes();
    let point_in_time_retained_bytes = point_in_time.retained_bytes();
    let objects = vec![PinnedManifestObject {
        artifact_id: Uuid::from_u128(0x7b8b8d9f_6777_4bfa_8d5c_10ce489eb091),
        relative_reference: published.relative_reference().into(),
        object,
    }]
    .into_boxed_slice();
    let parents = Vec::new().into_boxed_slice();
    let retained_bytes = pinned_dataset_retained_bytes(&manifest, &plan, &parents, &objects)
        .map_err(|_| ReleaseEvidenceStorageError::Parquet)?;
    let pinned = PinnedDataset {
        manifest,
        plan,
        generation_kind: GenerationKind::Ingest,
        build_spec_digest: None,
        parents,
        objects,
        retained_bytes,
    };

    let mut read_samples = Vec::new();
    read_samples
        .try_reserve_exact(
            usize::try_from(repetitions)
                .map_err(|_| ReleaseEvidenceStorageError::InvalidFixture)?,
        )
        .map_err(|_| ReleaseEvidenceStorageError::Parquet)?;
    let read_started = Instant::now();
    for _ in 0..repetitions {
        let started = Instant::now();
        let batches = store
            .read_pinned_async(&pinned, &cancellation)
            .await
            .map_err(|_| ReleaseEvidenceStorageError::Parquet)?;
        let rows = batches.iter().try_fold(0_u64, |total, batch| {
            total.checked_add(u64::try_from(batch.num_rows()).ok()?)
        });
        if rows
            != Some(
                u64::try_from(physical_rows)
                    .map_err(|_| ReleaseEvidenceStorageError::InvalidFixture)?,
            )
        {
            return Err(ReleaseEvidenceStorageError::Parquet);
        }
        read_samples.push(nanos(started.elapsed()));
    }
    let read_elapsed = nanos(read_started.elapsed());

    let engine = ResearchQueryEngine::from_pinned_dataset(
        pinned.clone(),
        "observations",
        Arc::clone(&store),
        cancellation.clone(),
    )
    .await
    .map_err(|_| ReleaseEvidenceStorageError::DataFusion)?;
    let query_iterations = repetitions
        .min(MAX_QUERY_ITERATIONS)
        .max(MIN_QUERY_ITERATIONS.min(repetitions));
    let mut query_samples = Vec::new();
    query_samples
        .try_reserve_exact(
            usize::try_from(query_iterations)
                .map_err(|_| ReleaseEvidenceStorageError::InvalidFixture)?,
        )
        .map_err(|_| ReleaseEvidenceStorageError::DataFusion)?;
    let query_started = Instant::now();
    for _ in 0..query_iterations {
        let started = Instant::now();
        let output = engine
            .query_pinned(
                QueryRequest::try_new(
                    pinned.manifest().clone(),
                    "SELECT COUNT(*) AS row_count FROM observations",
                )
                .map_err(|_| ReleaseEvidenceStorageError::DataFusion)?,
                QueryLimits::try_new(
                    1,
                    64 * 1024,
                    16 * 1024 * 1024,
                    1,
                    128,
                    128,
                    Duration::from_secs(10),
                )
                .map_err(|_| ReleaseEvidenceStorageError::DataFusion)?,
                cancellation.clone(),
            )
            .await
            .map_err(|_| ReleaseEvidenceStorageError::DataFusion)?;
        validate_count(output.result(), physical_rows)?;
        query_samples.push(nanos(started.elapsed()));
    }
    let query_elapsed = nanos(query_started.elapsed());

    drop(engine);
    drop(pinned);
    drop(store);
    drop(service);
    let python = python_admission::measure(&root.join("python-admission"), requested_rows)
        .await
        .map_err(|_| ReleaseEvidenceStorageError::PythonHandoff)?;
    if python.requested_rows != requested_rows {
        return Err(ReleaseEvidenceStorageError::PythonHandoff);
    }

    Ok(ReleaseEvidenceStorageResult {
        requested_rows,
        measured_rows,
        physical_rows_per_object: u64::try_from(physical_rows)
            .map_err(|_| ReleaseEvidenceStorageError::InvalidFixture)?,
        unique_parquet_objects: 1,
        parquet_content_sha256: published.content_hash().bytes(),
        parquet_size_bytes: published.size_bytes(),
        arrow_conversion: distribution(arrow_samples, measured_rows, arrow_elapsed)?,
        parquet_publication: distribution(parquet_samples, measured_rows, parquet_elapsed)?,
        parquet_read: distribution(read_samples, measured_rows, read_elapsed)?,
        datafusion_query: distribution(
            query_samples,
            query_iterations
                .checked_mul(
                    u64::try_from(physical_rows)
                        .map_err(|_| ReleaseEvidenceStorageError::InvalidFixture)?,
                )
                .ok_or(ReleaseEvidenceStorageError::InvalidFixture)?,
            query_elapsed,
        )?,
        point_in_time_selected_rows,
        point_in_time_content_sha256,
        point_in_time_audit_sha256,
        point_in_time_retained_bytes,
        python_verified_rows: python.measured_rows,
        python_selected_rows_per_verification: python.selected_rows_per_verification,
        python_export_sha256: python.export_sha256,
        python_catalog_identity: python.catalog_identity,
        python_selection_sha256: python.selection_sha256,
        python_dataset_admission_revalidation: distribution(
            python.samples,
            python.measured_rows,
            python.elapsed_nanos,
        )?,
    })
}

fn observations(count: usize) -> Result<Vec<ResearchObservation>, ReleaseEvidenceStorageError> {
    let mut observations = Vec::new();
    observations
        .try_reserve_exact(count)
        .map_err(|_| ReleaseEvidenceStorageError::Arrow)?;
    for index in 0..count {
        let context = ResearchContext::new(
            ResearchProvenance::try_new(ResearchProvenanceInput {
                source_id: SourceId::try_from("release-evidence-source")
                    .map_err(|_| ReleaseEvidenceStorageError::InvalidFixture)?,
                instrument_id: None,
                venue_id: None,
                source_identifier: source_identifier(&format!("release-row-{index}"))?,
                source_timestamp: None,
                received_at: Timestamp::from_unix_nanos(110),
                ingested_at: Timestamp::from_unix_nanos(120),
                quality: DataQuality::Modeled,
                payload_reference: PayloadReference::SourceReference(source_identifier(&format!(
                    "release-payload-{index}"
                ))?),
                availability: AvailabilityEvidence::local_first_observed(
                    Timestamp::from_unix_nanos(105),
                ),
            })
            .map_err(|_| ReleaseEvidenceStorageError::InvalidFixture)?,
            ResearchTime::new(
                Timestamp::from_unix_nanos(90),
                Some(Timestamp::from_unix_nanos(100)),
                RevisionNumber::new(1).map_err(|_| ReleaseEvidenceStorageError::InvalidFixture)?,
                None,
            )
            .map_err(|_| ReleaseEvidenceStorageError::InvalidFixture)?,
        )
        .map_err(|_| ReleaseEvidenceStorageError::InvalidFixture)?;
        observations.push(ResearchObservation::AlternativeData(
            AlternativeDataObservation::new(
                context,
                source_identifier("release-dataset")?,
                source_identifier("release-field")?,
                Decimal::new(
                    i64::try_from(index)
                        .map_err(|_| ReleaseEvidenceStorageError::InvalidFixture)?,
                    2,
                ),
                Some(source_identifier("ratio")?),
            ),
        ));
    }
    Ok(observations)
}

fn source_identifier(value: &str) -> Result<SourceIdentifier, ReleaseEvidenceStorageError> {
    SourceIdentifier::try_from(value).map_err(|_| ReleaseEvidenceStorageError::InvalidFixture)
}

fn validate_count(
    result: &QueryResult,
    expected: usize,
) -> Result<(), ReleaseEvidenceStorageError> {
    let QueryResult::Inline { batches, .. } = result else {
        return Err(ReleaseEvidenceStorageError::DataFusion);
    };
    let batch = batches
        .first()
        .filter(|_| batches.len() == 1)
        .ok_or(ReleaseEvidenceStorageError::DataFusion)?;
    if batch.num_rows() != 1 || batch.num_columns() != 1 {
        return Err(ReleaseEvidenceStorageError::DataFusion);
    }
    let expected = u64::try_from(expected).map_err(|_| ReleaseEvidenceStorageError::DataFusion)?;
    let value = batch
        .column(0)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .map(|array| array.value(0))
        .or_else(|| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .and_then(|array| u64::try_from(array.value(0)).ok())
        });
    (value == Some(expected))
        .then_some(())
        .ok_or(ReleaseEvidenceStorageError::DataFusion)
}

fn distribution(
    mut samples: Vec<u64>,
    rows: u64,
    elapsed_nanos: u64,
) -> Result<StorageLatency, ReleaseEvidenceStorageError> {
    if samples.is_empty() || rows == 0 || elapsed_nanos == 0 {
        return Err(ReleaseEvidenceStorageError::InvalidFixture);
    }
    samples.sort_unstable();
    let operations =
        u64::try_from(samples.len()).map_err(|_| ReleaseEvidenceStorageError::InvalidFixture)?;
    let maximum_nanos = *samples
        .last()
        .ok_or(ReleaseEvidenceStorageError::InvalidFixture)?;
    Ok(StorageLatency {
        operations,
        rows,
        elapsed_nanos,
        rows_per_second: u64::try_from(
            u128::from(rows)
                .checked_mul(1_000_000_000)
                .and_then(|value| value.checked_div(u128::from(elapsed_nanos)))
                .ok_or(ReleaseEvidenceStorageError::InvalidFixture)?,
        )
        .map_err(|_| ReleaseEvidenceStorageError::InvalidFixture)?,
        p50_nanos: quantile(&samples, 50)?,
        p95_nanos: quantile(&samples, 95)?,
        p99_nanos: quantile(&samples, 99)?,
        maximum_nanos,
    })
}

fn quantile(sorted: &[u64], percentile: usize) -> Result<u64, ReleaseEvidenceStorageError> {
    let rank = sorted
        .len()
        .checked_mul(percentile)
        .and_then(|value| value.checked_add(99))
        .map(|value| value / 100)
        .and_then(|value| value.checked_sub(1))
        .ok_or(ReleaseEvidenceStorageError::InvalidFixture)?;
    sorted
        .get(rank)
        .copied()
        .ok_or(ReleaseEvidenceStorageError::InvalidFixture)
}

fn nanos(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}
