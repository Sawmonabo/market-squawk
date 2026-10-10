//! Closed feature-only query and durable source-authenticated study epochs.

use super::{AnalyticalFeatureDataset, AnalyticalReadCapability, AnalyticalReadError};
use crate::{
    DatasetManifestRef, FeatureDatasetInputEpoch, FeatureDatasetProductContract, PinnedQueryOutput,
    QueryError, QueryLimits, QueryRequest, QueryResult, ResearchQueryEngine,
};
use arrow::array::FixedSizeBinaryArray;
use sha2::Digest as _;
use std::{sync::Arc, time::Instant};
use tokio_util::sync::CancellationToken;

/// One source-authenticated coordinate. The view cannot reveal later cohort rows.
#[derive(Clone, Copy, Debug)]
pub struct FeatureDatasetInputCoordinate<'a> {
    dataset: &'a AnalyticalFeatureDataset,
    epoch: &'a FeatureDatasetInputEpoch,
    rows: &'a [super::forecast::ForecastFeatureRow],
}
impl<'a> FeatureDatasetInputCoordinate<'a> {
    pub const fn dataset(&self) -> &'a AnalyticalFeatureDataset {
        self.dataset
    }
    pub const fn epoch(&self) -> &'a FeatureDatasetInputEpoch {
        self.epoch
    }
    pub const fn rows(&self) -> &'a [super::forecast::ForecastFeatureRow] {
        self.rows
    }
}

/// Moved ownership of one admitted coordinate and its existing native feature rows.
#[derive(Clone, Debug)]
pub struct OwnedFeatureDatasetInputCoordinate {
    dataset: Arc<AnalyticalFeatureDataset>,
    epoch: FeatureDatasetInputEpoch,
    rows: Box<[super::forecast::ForecastFeatureRow]>,
}
impl OwnedFeatureDatasetInputCoordinate {
    pub fn coordinate(&self) -> FeatureDatasetInputCoordinate<'_> {
        FeatureDatasetInputCoordinate {
            dataset: &self.dataset,
            epoch: &self.epoch,
            rows: &self.rows,
        }
    }
    pub const fn epoch(&self) -> &FeatureDatasetInputEpoch {
        &self.epoch
    }
    pub fn rows(&self) -> &[super::forecast::ForecastFeatureRow] {
        &self.rows
    }
    /// Returns owned coordinate bytes, excluding the one shared dataset summary.
    pub fn retained_bytes(&self) -> usize {
        std::mem::size_of::<Self>()
            + self.epoch.retained_bytes()
            + self
                .rows
                .iter()
                .map(super::forecast::ForecastFeatureRow::retained_bytes)
                .sum::<usize>()
    }
    /// Charge this once for each distinct shared publication summary retained by a cohort.
    pub fn shared_dataset_retained_bytes(&self) -> usize {
        self.dataset.retained_bytes()
    }
}

/// Exact admitted feature rows and their data-owned original input epochs.
#[derive(Debug)]
pub struct FeatureDatasetInputEpochOutput {
    dataset: AnalyticalFeatureDataset,
    query_output: PinnedQueryOutput,
    epochs: Box<[FeatureDatasetInputEpoch]>,
    rows: Box<[super::forecast::ForecastFeatureRow]>,
    maximum_bytes: usize,
    retained_bytes: usize,
}
impl FeatureDatasetInputEpochOutput {
    /// Returns the measured retained query, row, epoch and summary allocation charge.
    pub const fn retained_bytes(&self) -> usize {
        self.retained_bytes
    }

    pub fn coordinate(&self, index: usize) -> Option<FeatureDatasetInputCoordinate<'_>> {
        let epoch = self.epochs.get(index)?;
        let width = self
            .dataset
            .product_contract()
            .macro_components()
            .len()
            .checked_add(1)?;
        let start = index.checked_mul(width)?;
        Some(FeatureDatasetInputCoordinate {
            dataset: &self.dataset,
            epoch,
            rows: self.rows.get(start..start.checked_add(width)?)?,
        })
    }
    pub fn into_coordinates(
        self,
    ) -> Result<
        (
            AnalyticalFeatureDataset,
            PinnedQueryOutput,
            Box<[OwnedFeatureDatasetInputCoordinate]>,
        ),
        AnalyticalReadError,
    > {
        let count = self.epochs.len();
        let additional = count
            .checked_mul(std::mem::size_of::<OwnedFeatureDatasetInputCoordinate>())
            .and_then(|bytes| bytes.checked_add(self.dataset.retained_bytes()))
            .and_then(|bytes| {
                self.rows
                    .len()
                    .checked_mul(std::mem::size_of::<super::forecast::ForecastFeatureRow>())
                    .and_then(|rows| bytes.checked_add(rows))
            })
            .ok_or(AnalyticalReadError::InvalidLimit)?;
        if self
            .retained_bytes
            .checked_add(additional)
            .is_none_or(|bytes| bytes > self.maximum_bytes)
        {
            return Err(AnalyticalReadError::InvalidLimit);
        }
        let width = self.dataset.product_contract().macro_components().len() + 1;
        let shared = Arc::new(self.dataset.clone());
        let mut coordinates = Vec::new();
        coordinates
            .try_reserve_exact(count)
            .map_err(|_| AnalyticalReadError::InvalidLimit)?;
        let mut rows = self.rows.into_vec().into_iter();
        for epoch in self.epochs.into_vec() {
            let mut coordinate_rows = Vec::new();
            coordinate_rows
                .try_reserve_exact(width)
                .map_err(|_| AnalyticalReadError::InvalidLimit)?;
            for _ in 0..width {
                coordinate_rows.push(rows.next().ok_or(AnalyticalReadError::InvalidInputEpoch)?);
            }
            coordinates.push(OwnedFeatureDatasetInputCoordinate {
                dataset: Arc::clone(&shared),
                epoch,
                rows: coordinate_rows.into_boxed_slice(),
            });
        }
        if rows.next().is_some() {
            return Err(AnalyticalReadError::InvalidInputEpoch);
        }
        Ok((
            self.dataset,
            self.query_output,
            coordinates.into_boxed_slice(),
        ))
    }
    pub fn rows(&self) -> &[super::forecast::ForecastFeatureRow] {
        &self.rows
    }

    pub const fn dataset(&self) -> &AnalyticalFeatureDataset {
        &self.dataset
    }
    pub const fn query_output(&self) -> &PinnedQueryOutput {
        &self.query_output
    }
    pub fn epochs(&self) -> &[FeatureDatasetInputEpoch] {
        &self.epochs
    }
    pub fn into_parts(
        self,
    ) -> (
        AnalyticalFeatureDataset,
        PinnedQueryOutput,
        Box<[FeatureDatasetInputEpoch]>,
        Box<[super::forecast::ForecastFeatureRow]>,
    ) {
        (self.dataset, self.query_output, self.epochs, self.rows)
    }
}

impl AnalyticalReadCapability {
    /// Reopens one exact admitted cohort through a fixed feature-only query. Every published
    /// example is retained with its original availability and explicit study qualification.
    pub async fn feature_dataset_input_epochs(
        &self,
        expected_contract: FeatureDatasetProductContract,
        manifest: &DatasetManifestRef,
        limits: QueryLimits,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<FeatureDatasetInputEpochOutput, AnalyticalReadError> {
        if cancellation.is_cancelled() {
            return Err(QueryError::Cancelled.into());
        }
        if Instant::now() >= deadline {
            return Err(QueryError::DeadlineExceeded.into());
        }
        let retained = self
            .manifests
            .read_feature_dataset_snapshot(
                expected_contract,
                crate::manifest::CatalogFeatureDatasetSelection::ExactManifest(manifest),
                &[],
                1,
                deadline,
                &cancellation,
            )?
            .datasets
            .into_iter()
            .next()
            .ok_or(AnalyticalReadError::ForecastDatasetUnavailable)?;
        let pinned = retained.pinned.clone();
        let dataset = AnalyticalFeatureDataset::from_catalog(retained, expected_contract)?;
        if dataset.generation().manifest() != manifest {
            return Err(AnalyticalReadError::InvalidInputEpoch);
        }
        let child = cancellation.child_token();
        let guard = child.clone().drop_guard();
        let build_engine = ResearchQueryEngine::from_pinned_dataset(
            pinned,
            "observations",
            Arc::clone(&self.objects),
            child.clone(),
        );
        tokio::pin!(build_engine);
        let engine = tokio::select! {
            biased;
            _ = cancellation.cancelled() => return Err(QueryError::Cancelled.into()),
            _ = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => return Err(QueryError::DeadlineExceeded.into()),
            value = build_engine.as_mut() => value?,
        };
        let query = QueryRequest::try_new(
            manifest.clone(),
            "SELECT * FROM observations WHERE component_kind = 1 ORDER BY decision_on, decision_at, instrument_id, example_id, component_name, component_version",
        )?;
        let execution = engine.query_pinned(query, limits, child.clone());
        tokio::pin!(execution);
        let output = tokio::select! {
            biased;
            _ = cancellation.cancelled() => { child.cancel(); let _ = execution.as_mut().await; return Err(QueryError::Cancelled.into()); }
            _ = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => { child.cancel(); let _ = execution.as_mut().await; return Err(QueryError::DeadlineExceeded.into()); }
            value = execution.as_mut() => value?,
        };
        drop(guard);
        let QueryResult::Inline {
            batches,
            byte_count,
        } = output.result()
        else {
            return Err(AnalyticalReadError::InputEpochResultRequiresInline);
        };
        let mut retained_bytes =
            usize::try_from(*byte_count).map_err(|_| AnalyticalReadError::InvalidLimit)?;
        let maximum_bytes =
            usize::try_from(limits.max_bytes()).map_err(|_| AnalyticalReadError::InvalidLimit)?;
        let row_count = batches.iter().try_fold(0_usize, |total, batch| {
            total
                .checked_add(batch.num_rows())
                .ok_or(AnalyticalReadError::InvalidLimit)
        })?;
        let row_storage = row_count
            .checked_mul(std::mem::size_of::<super::forecast::ForecastFeatureRow>())
            .ok_or(AnalyticalReadError::InvalidLimit)?;
        retained_bytes = retained_bytes
            .checked_add(row_storage)
            .and_then(|bytes| bytes.checked_add(dataset.retained_bytes()))
            .ok_or(AnalyticalReadError::InvalidLimit)?;
        if retained_bytes > maximum_bytes {
            return Err(AnalyticalReadError::InvalidLimit);
        }
        let mut epochs = Vec::<FeatureDatasetInputEpoch>::new();
        let mut rows = Vec::new();
        rows.try_reserve_exact(row_count)
            .map_err(|_| AnalyticalReadError::InvalidLimit)?;
        let mut names = std::collections::BTreeSet::new();
        let mut expected_names = std::collections::BTreeSet::new();
        expected_names.insert(expected_contract.feature_component_name());
        expected_names.extend(
            expected_contract
                .macro_components()
                .iter()
                .map(|value| value.component_name()),
        );
        for batch in batches {
            for index in 0..batch.num_rows() {
                if index % 128 == 0 {
                    if cancellation.is_cancelled() {
                        return Err(QueryError::Cancelled.into());
                    }
                    if Instant::now() >= deadline {
                        return Err(QueryError::DeadlineExceeded.into());
                    }
                }
                let transient = crate::python_dataset::input_epoch_bytes(batch, index)?
                    .ok_or(AnalyticalReadError::InvalidInputEpoch)?
                    .len()
                    .checked_mul(16)
                    .ok_or(AnalyticalReadError::InvalidLimit)?;
                if retained_bytes
                    .checked_add(transient)
                    .is_none_or(|bytes| bytes > maximum_bytes)
                {
                    return Err(AnalyticalReadError::InvalidLimit);
                }
                // Native row admission checks exact canonical columns and the epoch's row binding.
                let (_canonical, row) = super::forecast::decode_row(batch, index)?;
                if row.component_kind() != 1 || !matches!(row.target_coordinate_kind(), 3 | 4 | 5) {
                    return Err(AnalyticalReadError::InvalidInputEpoch);
                }
                retained_bytes = retained_bytes
                    .checked_add(
                        row.retained_bytes()
                            - std::mem::size_of::<super::forecast::ForecastFeatureRow>(),
                    )
                    .ok_or(AnalyticalReadError::InvalidLimit)?;
                if retained_bytes > maximum_bytes {
                    return Err(AnalyticalReadError::InvalidLimit);
                }
                rows.push(row);
                let bytes = crate::python_dataset::input_epoch_bytes(batch, index)?
                    .ok_or(AnalyticalReadError::InvalidInputEpoch)?;
                let epoch = FeatureDatasetInputEpoch::decode(bytes)
                    .map_err(|_| AnalyticalReadError::InvalidInputEpoch)?;
                let policy = dataset
                    .study_policy()
                    .ok_or(AnalyticalReadError::InvalidInputEpoch)?;
                if epoch
                    .study_policy()
                    .map_err(|_| AnalyticalReadError::InvalidInputEpoch)?
                    != *policy
                    || epoch.basis() != policy.basis()
                    || epoch.population_basis() != dataset.population_basis()
                    || epoch.purpose() != policy.purpose()
                    || epoch.snapshot_as_of() != policy.snapshot_as_of()
                    || Some(epoch.source_snapshot_digest()) != dataset.source_snapshot_digest()
                    || epoch.limitations() != policy.limitations()
                {
                    return Err(AnalyticalReadError::InvalidInputEpoch);
                }
                if epoch.calculated_at() > dataset.production_receipt().admitted_at()
                    || !dataset
                        .generation()
                        .parents()
                        .iter()
                        .any(|parent| parent.manifest() == epoch.source_manifest())
                {
                    return Err(AnalyticalReadError::InvalidInputEpoch);
                }
                let same = epochs.last().is_some_and(|previous| {
                    previous.example_id() == epoch.example_id()
                        && previous.instrument_id() == epoch.instrument_id()
                        && previous.decision_coordinate() == epoch.decision_coordinate()
                });
                if same {
                    if epochs.last() != Some(&epoch) {
                        return Err(AnalyticalReadError::InvalidInputEpoch);
                    }
                } else {
                    if !epochs.is_empty() && names != expected_names {
                        return Err(AnalyticalReadError::InvalidInputEpoch);
                    }
                    names.clear();
                    retained_bytes = retained_bytes
                        .checked_add(epoch.retained_bytes())
                        .ok_or(AnalyticalReadError::InvalidLimit)?;
                    if retained_bytes > maximum_bytes {
                        return Err(AnalyticalReadError::InvalidLimit);
                    }
                    epochs
                        .try_reserve(1)
                        .map_err(|_| AnalyticalReadError::InvalidLimit)?;
                    epochs.push(epoch);
                }
                let values = batch
                    .column_by_name("component_name")
                    .and_then(|array| array.as_any().downcast_ref::<FixedSizeBinaryArray>())
                    .ok_or(AnalyticalReadError::InvalidInputEpoch)?;
                let raw = values.value(index);
                let end = raw
                    .iter()
                    .position(|value| *value == 0)
                    .unwrap_or(raw.len());
                let name = std::str::from_utf8(&raw[..end])
                    .map_err(|_| AnalyticalReadError::InvalidInputEpoch)?;
                if !names.insert(name) {
                    return Err(AnalyticalReadError::InvalidInputEpoch);
                }
            }
        }
        if epochs.is_empty() || names != expected_names {
            return Err(AnalyticalReadError::InvalidInputEpoch);
        }
        let counts = dataset.split_counts();
        let count = counts
            .train_examples()
            .checked_add(counts.validation_examples())
            .and_then(|value| value.checked_add(counts.test_examples()))
            .ok_or(AnalyticalReadError::InvalidLimit)?;
        if epochs.len() != count {
            return Err(AnalyticalReadError::InvalidInputEpoch);
        }
        Ok(FeatureDatasetInputEpochOutput {
            dataset,
            query_output: output,
            epochs: epochs.into_boxed_slice(),
            rows: rows.into_boxed_slice(),
            maximum_bytes,
            retained_bytes,
        })
    }
}

/// A sealed coordinate reference. Only the complete data reader can mint it; serialized
/// caller data cannot acquire source authority. Loading retains one coordinate at a time.
#[derive(Clone, Debug)]
pub struct FeatureDatasetInputCoordinateHandle {
    store: Arc<CoordinateStore>,
    ordinal: usize,
}

#[derive(Debug)]
struct CoordinateStore {
    dataset: Arc<AnalyticalFeatureDataset>,
    connection: std::sync::Mutex<rusqlite::Connection>,
    _directory: Arc<crate::OperationScratchDirectory>,
    count: usize,
    identities: Box<[(usize, [u8; 32])]>,
    deadline: Instant,
    cancellation: CancellationToken,
}

impl FeatureDatasetInputCoordinateHandle {
    pub fn load(&self) -> Result<OwnedFeatureDatasetInputCoordinate, AnalyticalReadError> {
        if self.store.cancellation.is_cancelled() {
            return Err(QueryError::Cancelled.into());
        }
        if Instant::now() >= self.store.deadline {
            return Err(QueryError::DeadlineExceeded.into());
        }
        let connection = self
            .store
            .connection
            .lock()
            .map_err(|_| AnalyticalReadError::InvalidInputEpoch)?;
        let (expected_bytes, expected_digest) = *self
            .store
            .identities
            .get(self.ordinal)
            .ok_or(AnalyticalReadError::InvalidInputEpoch)?;
        let bytes: Vec<u8> = connection
            .query_row(
                "SELECT substr(payload,1,?2) FROM coordinates WHERE ordinal=?1",
                rusqlite::params![
                    self.ordinal as i64,
                    i64::try_from(expected_bytes.saturating_add(1))
                        .map_err(|_| AnalyticalReadError::InvalidLimit)?
                ],
                |row| row.get(0),
            )
            .map_err(|_| AnalyticalReadError::InvalidInputEpoch)?;
        if bytes.len() != expected_bytes
            || <[u8; 32]>::from(sha2::Sha256::digest(&bytes)) != expected_digest
        {
            return Err(AnalyticalReadError::InvalidInputEpoch);
        }
        let reader = arrow::ipc::reader::StreamReader::try_new(std::io::Cursor::new(bytes), None)
            .map_err(|_| AnalyticalReadError::InvalidInputEpoch)?;
        let mut rows = Vec::new();
        let mut epoch = None;
        for batch in reader {
            let batch = batch.map_err(|_| AnalyticalReadError::InvalidInputEpoch)?;
            for index in 0..batch.num_rows() {
                let (_, row) = super::forecast::decode_row(&batch, index)?;
                let current = FeatureDatasetInputEpoch::decode(
                    crate::python_dataset::input_epoch_bytes(&batch, index)?
                        .ok_or(AnalyticalReadError::InvalidInputEpoch)?,
                )
                .map_err(|_| AnalyticalReadError::InvalidInputEpoch)?;
                if epoch.as_ref().is_some_and(|prior| prior != &current) {
                    return Err(AnalyticalReadError::InvalidInputEpoch);
                }
                epoch = Some(current);
                rows.push(row);
            }
        }
        Ok(OwnedFeatureDatasetInputCoordinate {
            dataset: Arc::clone(&self.store.dataset),
            epoch: epoch.ok_or(AnalyticalReadError::InvalidInputEpoch)?,
            rows: rows.into_boxed_slice(),
        })
    }
    pub const fn ordinal(&self) -> usize {
        self.ordinal
    }
    /// Resolves another coordinate only within the same complete authenticated query.
    pub fn at(&self, ordinal: usize) -> Option<Self> {
        (ordinal < self.store.count).then(|| Self {
            store: Arc::clone(&self.store),
            ordinal,
        })
    }
    pub fn retained_bytes(&self) -> usize {
        std::mem::size_of::<Self>()
    }
}

/// Complete authenticated query receipt with a consumable disk-backed coordinate sequence.
#[derive(Debug)]
pub struct FeatureDatasetInputEpochCursor {
    store: Arc<CoordinateStore>,
    query_output: PinnedQueryOutput,
    count: usize,
    next: usize,
}
impl FeatureDatasetInputEpochCursor {
    pub fn dataset(&self) -> &AnalyticalFeatureDataset {
        &self.store.dataset
    }
    pub fn query_output(&self) -> &PinnedQueryOutput {
        &self.query_output
    }
    /// Shares the operation lease for downstream private staging and restart cleanup.
    pub fn operation_scratch(&self) -> Arc<crate::OperationScratchDirectory> {
        Arc::clone(&self.store._directory)
    }
    pub const fn len(&self) -> usize {
        self.count
    }
    pub const fn is_empty(&self) -> bool {
        self.count == 0
    }
    /// Reopens one original authenticated coordinate without retaining the rest of the query.
    pub fn coordinate(
        &self,
        ordinal: usize,
    ) -> Result<Option<OwnedFeatureDatasetInputCoordinate>, AnalyticalReadError> {
        if ordinal >= self.count {
            return Ok(None);
        }
        FeatureDatasetInputCoordinateHandle {
            store: Arc::clone(&self.store),
            ordinal,
        }
        .load()
        .map(Some)
    }
    /// Repeatable complete traversal with one native coordinate resident at a time.
    pub fn coordinates(
        &self,
    ) -> impl Iterator<Item = Result<OwnedFeatureDatasetInputCoordinate, AnalyticalReadError>> + '_
    {
        (0..self.count).map(|ordinal| {
            self.coordinate(ordinal)?
                .ok_or(AnalyticalReadError::InvalidInputEpoch)
        })
    }
    pub fn next_coordinate(
        &mut self,
    ) -> Result<
        Option<(
            FeatureDatasetInputCoordinateHandle,
            OwnedFeatureDatasetInputCoordinate,
        )>,
        AnalyticalReadError,
    > {
        if self.next == self.count {
            return Ok(None);
        }
        let handle = FeatureDatasetInputCoordinateHandle {
            store: Arc::clone(&self.store),
            ordinal: self.next,
        };
        let coordinate = handle.load()?;
        self.next += 1;
        Ok(Some((handle, coordinate)))
    }
}

impl AnalyticalReadCapability {
    /// Validates every source-bound epoch and component before exposing an ordered cursor.
    /// SQL sorting uses the query engine's operation-local spill; decoded historical rows are
    /// never retained as a full Arrow/native pair.
    pub async fn feature_dataset_input_epoch_cursor(
        &self,
        expected_contract: FeatureDatasetProductContract,
        manifest: &DatasetManifestRef,
        limits: QueryLimits,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<FeatureDatasetInputEpochCursor, AnalyticalReadError> {
        let retained = self
            .manifests
            .read_feature_dataset_snapshot(
                expected_contract,
                crate::manifest::CatalogFeatureDatasetSelection::ExactManifest(manifest),
                &[],
                1,
                deadline,
                &cancellation,
            )?
            .datasets
            .into_iter()
            .next()
            .ok_or(AnalyticalReadError::ForecastDatasetUnavailable)?;
        let pinned = retained.pinned.clone();
        let dataset = Arc::new(AnalyticalFeatureDataset::from_catalog(
            retained,
            expected_contract,
        )?);
        if dataset.generation().manifest() != manifest {
            return Err(AnalyticalReadError::InvalidInputEpoch);
        }
        let directory = self
            .objects
            .operation_scratch()
            .map_err(|_| AnalyticalReadError::InvalidLimit)?;
        let connection = rusqlite::Connection::open(directory.path().join("coordinates.sqlite"))
            .map_err(|_| AnalyticalReadError::InvalidLimit)?;
        connection.execute_batch("PRAGMA cache_size=-2048; PRAGMA temp_store=FILE; CREATE TABLE coordinates(ordinal INTEGER PRIMARY KEY,payload BLOB NOT NULL); BEGIN IMMEDIATE;")
            .map_err(|_| AnalyticalReadError::InvalidLimit)?;
        let connection = std::sync::Mutex::new(connection);
        let child = cancellation.child_token();
        let _guard = child.clone().drop_guard();
        let engine = ResearchQueryEngine::from_pinned_dataset(
            pinned,
            "observations",
            Arc::clone(&self.objects),
            child.clone(),
        )
        .await?;
        let request = QueryRequest::try_new(
            manifest.clone(),
            "SELECT * FROM observations WHERE component_kind = 1 ORDER BY decision_on, decision_at, instrument_id, example_id, component_name, component_version",
        )?;
        let expected_names: std::collections::BTreeSet<&str> =
            std::iter::once(expected_contract.feature_component_name())
                .chain(
                    expected_contract
                        .macro_components()
                        .iter()
                        .map(|value| value.component_name()),
                )
                .collect();
        let width = expected_names.len();
        let mut names = std::collections::BTreeSet::new();
        let mut current_epoch: Option<FeatureDatasetInputEpoch> = None;
        let mut previous_epoch: Option<FeatureDatasetInputEpoch> = None;
        let mut fragments = Vec::new();
        let mut count = 0usize;
        let mut identities = Vec::new();
        let mut consume = |batch: arrow::record_batch::RecordBatch| -> Result<(), QueryError> {
            for index in 0..batch.num_rows() {
                if child.is_cancelled() {
                    return Err(QueryError::Cancelled);
                }
                if Instant::now() >= deadline {
                    return Err(QueryError::DeadlineExceeded);
                }
                let epoch_bytes = crate::python_dataset::input_epoch_bytes(&batch, index)
                    .map_err(|_| QueryError::InvalidSource)?
                    .ok_or(QueryError::InvalidSource)?;
                if (epoch_bytes.len() as u64)
                    .checked_mul(16)
                    .is_none_or(|bytes| bytes > limits.max_memory_bytes())
                {
                    return Err(QueryError::InvalidSource);
                }
                let (_, row) = super::forecast::decode_row(&batch, index)
                    .map_err(|_| QueryError::InvalidSource)?;
                if row.component_kind() != 1 || !matches!(row.target_coordinate_kind(), 3 | 4 | 5) {
                    return Err(QueryError::InvalidSource);
                }
                let epoch = FeatureDatasetInputEpoch::decode(
                    crate::python_dataset::input_epoch_bytes(&batch, index)
                        .map_err(|_| QueryError::InvalidSource)?
                        .ok_or(QueryError::InvalidSource)?,
                )
                .map_err(|_| QueryError::InvalidSource)?;
                let policy = dataset.study_policy().ok_or(QueryError::InvalidSource)?;
                if epoch
                    .study_policy()
                    .map_err(|_| QueryError::InvalidSource)?
                    != *policy
                    || epoch.basis() != policy.basis()
                    || epoch.population_basis() != dataset.population_basis()
                    || epoch.purpose() != policy.purpose()
                    || epoch.snapshot_as_of() != policy.snapshot_as_of()
                    || Some(epoch.source_snapshot_digest()) != dataset.source_snapshot_digest()
                    || epoch.limitations() != policy.limitations()
                    || epoch.calculated_at() > dataset.production_receipt().admitted_at()
                    || !dataset
                        .generation()
                        .parents()
                        .iter()
                        .any(|parent| parent.manifest() == epoch.source_manifest())
                {
                    return Err(QueryError::InvalidSource);
                }
                if let Some(previous) = &current_epoch {
                    if previous.example_id() != epoch.example_id()
                        || previous.instrument_id() != epoch.instrument_id()
                        || previous.decision_coordinate() != epoch.decision_coordinate()
                        || previous != &epoch
                    {
                        return Err(QueryError::InvalidSource);
                    }
                } else {
                    if previous_epoch.as_ref().is_some_and(|prior| {
                        prior.example_id() == epoch.example_id()
                            && prior.instrument_id() == epoch.instrument_id()
                            && prior.decision_coordinate() == epoch.decision_coordinate()
                    }) {
                        return Err(QueryError::InvalidSource);
                    }
                    current_epoch = Some(epoch);
                }
                if !names.insert(row.component_name().to_owned()) {
                    return Err(QueryError::InvalidSource);
                }
                fragments.push(batch.slice(index, 1));
                if fragments.len() == width {
                    if names
                        .iter()
                        .map(String::as_str)
                        .ne(expected_names.iter().copied())
                    {
                        return Err(QueryError::InvalidSource);
                    }
                    let combined = arrow::compute::concat_batches(&batch.schema(), &fragments)
                        .map_err(|_| QueryError::InvalidSource)?;
                    let mut bytes = Vec::new();
                    {
                        let mut writer = arrow::ipc::writer::StreamWriter::try_new(
                            &mut bytes,
                            &combined.schema(),
                        )
                        .map_err(|_| QueryError::InvalidSource)?;
                        writer
                            .write(&combined)
                            .map_err(|_| QueryError::InvalidSource)?;
                        writer.finish().map_err(|_| QueryError::InvalidSource)?;
                    }
                    if bytes.len() as u64 > limits.max_memory_bytes() {
                        return Err(QueryError::InvalidSource);
                    }
                    identities.push((bytes.len(), <[u8; 32]>::from(sha2::Sha256::digest(&bytes))));
                    connection
                        .lock()
                        .map_err(|_| QueryError::InvalidSource)?
                        .execute(
                            "INSERT INTO coordinates VALUES (?1,?2)",
                            rusqlite::params![count as i64, bytes],
                        )
                        .map_err(|_| QueryError::InvalidSource)?;
                    count = count.checked_add(1).ok_or(QueryError::InvalidSource)?;
                    fragments.clear();
                    names.clear();
                    previous_epoch = current_epoch.take();
                }
            }
            Ok(())
        };
        let output = {
            let execution =
                engine.query_pinned_consume(request, limits, child.clone(), &mut consume);
            tokio::pin!(execution);
            tokio::select! {
                biased;
                _ = cancellation.cancelled() => { child.cancel(); let _ = execution.as_mut().await; return Err(QueryError::Cancelled.into()); }
                _ = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => { child.cancel(); let _ = execution.as_mut().await; return Err(QueryError::DeadlineExceeded.into()); }
                result = execution.as_mut() => result?,
            }
        };
        drop(consume);
        let counts = dataset.split_counts();
        let expected_count = counts
            .train_examples()
            .checked_add(counts.validation_examples())
            .and_then(|n| n.checked_add(counts.test_examples()))
            .ok_or(AnalyticalReadError::InvalidLimit)?;
        if count == 0 || count != expected_count || !fragments.is_empty() {
            return Err(AnalyticalReadError::InvalidInputEpoch);
        }
        connection
            .lock()
            .map_err(|_| AnalyticalReadError::InvalidInputEpoch)?
            .execute_batch("COMMIT; PRAGMA query_only=ON;")
            .map_err(|_| AnalyticalReadError::InvalidLimit)?;
        Ok(FeatureDatasetInputEpochCursor {
            store: Arc::new(CoordinateStore {
                dataset,
                connection,
                _directory: Arc::new(directory),
                count,
                identities: identities.into_boxed_slice(),
                deadline,
                cancellation,
            }),
            query_output: output,
            count,
            next: 0,
        })
    }
}
