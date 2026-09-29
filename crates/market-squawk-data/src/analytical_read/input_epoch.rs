//! Closed feature-only query and durable source-authenticated study epochs.

use super::{AnalyticalFeatureDataset, AnalyticalReadCapability, AnalyticalReadError};
use crate::{
    DatasetManifestRef, FeatureDatasetInputEpoch, FeatureDatasetProductContract, PinnedQueryOutput,
    QueryError, QueryLimits, QueryRequest, QueryResult, ResearchQueryEngine,
};
use arrow::array::FixedSizeBinaryArray;
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
