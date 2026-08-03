//! Bounded, pinned feature-dataset evidence for native forecast preparation.

use std::{num::NonZeroU64, time::Instant};

use arrow::{
    array::{
        Array as _, Decimal128Array, FixedSizeBinaryArray, Float64Array, TimestampNanosecondArray,
        UInt8Array, UInt32Array,
    },
    record_batch::RecordBatch,
};
use market_squawk_domain::{InstrumentId, Timestamp};
use tokio_util::sync::CancellationToken;

use super::{AnalyticalFeatureDataset, AnalyticalReadCapability, AnalyticalReadError};
use crate::manifest::CatalogFeatureDatasetSelection;
use crate::python_dataset::{finish_selection_hash, new_selection_hasher, update_selection_hash};
use crate::{
    CatalogEndpointIdentity, DatasetManifestRef, PythonDatasetCatalogError, PythonDatasetRow,
    PythonDatasetValue, Sha256Digest,
};

const MAX_FORECAST_ROWS: usize = 100_000;
const MAX_FORECAST_BYTES: usize = 256 * 1024 * 1024;

/// Bounded work and retained-memory policy for one forecast-evidence materialization.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ForecastDatasetReadLimits {
    max_rows: usize,
    max_bytes: usize,
}

impl ForecastDatasetReadLimits {
    /// Constructs limits under the installed feature-dataset ceilings.
    pub fn try_new(max_rows: usize, max_bytes: usize) -> Result<Self, AnalyticalReadError> {
        if max_rows == 0
            || max_rows > MAX_FORECAST_ROWS
            || max_bytes == 0
            || max_bytes > MAX_FORECAST_BYTES
        {
            return Err(AnalyticalReadError::InvalidLimit);
        }
        Ok(Self {
            max_rows,
            max_bytes,
        })
    }
}

/// Exact typed value retained by one selected feature/label row.
#[derive(Clone, Debug, PartialEq)]
pub enum ForecastFeatureValue {
    /// Finite statistical value.
    Float(f64),
    /// Exact decimal value.
    Decimal { mantissa: i128, scale: u8 },
    /// Explicit missing marker.
    Missing,
}

/// One verified selected component row in immutable object order.
#[derive(Clone, Debug, PartialEq)]
pub struct ForecastFeatureRow {
    instrument_id: InstrumentId,
    cutoff_at: Timestamp,
    component_kind: u8,
    component_name: Box<str>,
    component_version: u32,
    value: ForecastFeatureValue,
    lineage_sha256: Sha256Digest,
}

impl ForecastFeatureRow {
    pub const fn instrument_id(&self) -> InstrumentId {
        self.instrument_id
    }

    pub const fn cutoff_at(&self) -> Timestamp {
        self.cutoff_at
    }

    pub const fn component_kind(&self) -> u8 {
        self.component_kind
    }

    pub fn component_name(&self) -> &str {
        &self.component_name
    }

    pub const fn component_version(&self) -> u32 {
        self.component_version
    }

    pub const fn value(&self) -> &ForecastFeatureValue {
        &self.value
    }

    pub const fn lineage_sha256(&self) -> Sha256Digest {
        self.lineage_sha256
    }
}

/// Recomputable exact catalog, generation, selection, and cutoff fence.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ForecastDatasetEvidenceFence {
    manifest: DatasetManifestRef,
    catalog_identity: CatalogEndpointIdentity,
    export_sha256: Sha256Digest,
    selection_sha256: Sha256Digest,
    selected_rows: NonZeroU64,
    as_of: Timestamp,
}

impl ForecastDatasetEvidenceFence {
    pub const fn manifest(&self) -> &DatasetManifestRef {
        &self.manifest
    }

    pub const fn catalog_identity(&self) -> CatalogEndpointIdentity {
        self.catalog_identity
    }

    pub const fn export_sha256(&self) -> Sha256Digest {
        self.export_sha256
    }

    pub const fn selection_sha256(&self) -> Sha256Digest {
        self.selection_sha256
    }

    pub const fn selected_rows(&self) -> NonZeroU64 {
        self.selected_rows
    }

    pub const fn as_of(&self) -> Timestamp {
        self.as_of
    }
}

/// Exact Python-admitted feature generation and its bounded selected rows.
#[derive(Debug)]
pub struct ForecastDatasetEvidence {
    dataset: AnalyticalFeatureDataset,
    fence: ForecastDatasetEvidenceFence,
    rows: Box<[ForecastFeatureRow]>,
}

impl ForecastDatasetEvidence {
    pub const fn dataset(&self) -> &AnalyticalFeatureDataset {
        &self.dataset
    }

    pub const fn fence(&self) -> &ForecastDatasetEvidenceFence {
        &self.fence
    }

    pub fn rows(&self) -> &[ForecastFeatureRow] {
        &self.rows
    }
}

impl AnalyticalReadCapability {
    /// Materializes one exact Python-admitted generation under a point-in-time cutoff.
    pub async fn forecast_dataset_evidence(
        &self,
        manifest: &DatasetManifestRef,
        as_of: Timestamp,
        limits: ForecastDatasetReadLimits,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<ForecastDatasetEvidence, AnalyticalReadError> {
        if cancellation.is_cancelled() || Instant::now() >= deadline {
            return Err(AnalyticalReadError::InvalidLimit);
        }
        let page = self.manifests.read_feature_dataset_snapshot(
            CatalogFeatureDatasetSelection::Exact(manifest.dataset_id()),
            &[],
            1,
            deadline,
            &cancellation,
        )?;
        let retained = page
            .datasets
            .into_iter()
            .next()
            .ok_or(AnalyticalReadError::ForecastDatasetUnavailable)?;
        if retained.pinned.manifest() != manifest {
            return Err(AnalyticalReadError::ForecastDatasetUnavailable);
        }
        let export_sha256 = retained.export_sha256;
        let catalog_identity =
            CatalogEndpointIdentity::try_from_bytes(self.manifests.catalog_binding()).ok_or(
                AnalyticalReadError::Manifest(crate::ManifestCatalogError::CorruptCatalog),
            )?;
        let batches = self
            .objects
            .read_pinned_bounded_async(
                &retained.pinned,
                limits.max_rows,
                limits.max_bytes,
                &cancellation,
            )
            .await
            .map_err(AnalyticalReadError::from)?;
        let mut rows = Vec::new();
        let mut hasher = new_selection_hasher(catalog_identity, export_sha256, as_of);
        for batch in batches {
            for index in 0..batch.num_rows() {
                if index % 128 == 0 && (cancellation.is_cancelled() || Instant::now() >= deadline) {
                    return Err(AnalyticalReadError::Query(crate::QueryError::Cancelled));
                }
                let (canonical, view) = decode_row(&batch, index)?;
                if view.cutoff_at <= as_of {
                    if rows.len() >= limits.max_rows {
                        return Err(AnalyticalReadError::InvalidLimit);
                    }
                    update_selection_hash(&mut hasher, &canonical);
                    rows.push(view);
                }
            }
        }
        let selected_rows = NonZeroU64::new(
            u64::try_from(rows.len()).map_err(|_| AnalyticalReadError::InvalidLimit)?,
        )
        .ok_or(AnalyticalReadError::InvalidLimit)?;
        let selection_sha256 = finish_selection_hash(hasher, rows.len())?;
        let dataset = AnalyticalFeatureDataset::from_catalog(retained)?;
        Ok(ForecastDatasetEvidence {
            dataset,
            fence: ForecastDatasetEvidenceFence {
                manifest: manifest.clone(),
                catalog_identity,
                export_sha256,
                selection_sha256,
                selected_rows,
                as_of,
            },
            rows: rows.into_boxed_slice(),
        })
    }

    /// Re-reads and proves equality with one previously retained exact evidence fence.
    pub async fn revalidate_forecast_dataset_evidence(
        &self,
        expected: &ForecastDatasetEvidenceFence,
        limits: ForecastDatasetReadLimits,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<(), AnalyticalReadError> {
        let observed = self
            .forecast_dataset_evidence(
                expected.manifest(),
                expected.as_of(),
                limits,
                deadline,
                cancellation,
            )
            .await?;
        if observed.fence() != expected {
            return Err(AnalyticalReadError::Manifest(
                crate::ManifestCatalogError::CorruptCatalog,
            ));
        }
        Ok(())
    }
}

fn decode_row(
    batch: &RecordBatch,
    index: usize,
) -> Result<(PythonDatasetRow, ForecastFeatureRow), AnalyticalReadError> {
    let fixed = |name| {
        batch
            .column_by_name(name)
            .and_then(|array| array.as_any().downcast_ref::<FixedSizeBinaryArray>())
            .ok_or(PythonDatasetCatalogError::CorruptAdmission)
    };
    let example = padded_text(fixed("example_id")?, index)?;
    let instrument_bytes: [u8; 16] = fixed("instrument_id")?
        .value(index)
        .try_into()
        .map_err(|_| PythonDatasetCatalogError::CorruptAdmission)?;
    let instrument_id = InstrumentId::try_from(uuid::Uuid::from_bytes(instrument_bytes))
        .map_err(|_| PythonDatasetCatalogError::CorruptAdmission)?;
    let cutoff_at = Timestamp::from_unix_nanos(
        batch
            .column_by_name("cutoff_at")
            .and_then(|array| array.as_any().downcast_ref::<TimestampNanosecondArray>())
            .ok_or(PythonDatasetCatalogError::CorruptAdmission)?
            .value(index),
    );
    let split = uint8(batch, "split")?.value(index);
    let component_kind = uint8(batch, "component_kind")?.value(index);
    let component_name = padded_text(fixed("component_name")?, index)?;
    let component_version = batch
        .column_by_name("component_version")
        .and_then(|array| array.as_any().downcast_ref::<UInt32Array>())
        .ok_or(PythonDatasetCatalogError::CorruptAdmission)?
        .value(index);
    let floats = batch
        .column_by_name("value_f64")
        .and_then(|array| array.as_any().downcast_ref::<Float64Array>())
        .ok_or(PythonDatasetCatalogError::CorruptAdmission)?;
    let decimals = batch
        .column_by_name("value_decimal_mantissa")
        .and_then(|array| array.as_any().downcast_ref::<Decimal128Array>())
        .ok_or(PythonDatasetCatalogError::CorruptAdmission)?;
    let scales = uint8(batch, "value_decimal_scale")?;
    let missing = fixed("missing_reason")?;
    let (canonical_value, value) = if !floats.is_null(index) {
        let value = floats.value(index);
        (
            PythonDatasetValue::Float(value),
            ForecastFeatureValue::Float(value),
        )
    } else if !decimals.is_null(index) && !scales.is_null(index) {
        let mantissa = decimals.value(index);
        let scale = scales.value(index);
        (
            PythonDatasetValue::Decimal { mantissa, scale },
            ForecastFeatureValue::Decimal { mantissa, scale },
        )
    } else if !missing.is_null(index) {
        (
            PythonDatasetValue::Missing(padded_text(missing, index)?.into()),
            ForecastFeatureValue::Missing,
        )
    } else {
        return Err(PythonDatasetCatalogError::CorruptAdmission.into());
    };
    let unit = optional_padded(fixed("unit")?, index)?;
    let currency = optional_padded(fixed("currency")?, index)?;
    let lineage: [u8; 32] = fixed("lineage_sha256")?
        .value(index)
        .try_into()
        .map_err(|_| PythonDatasetCatalogError::CorruptAdmission)?;
    let canonical = PythonDatasetRow::try_new(
        example,
        instrument_bytes,
        cutoff_at,
        split,
        component_kind,
        component_name,
        component_version,
        canonical_value,
        unit,
        currency,
        lineage,
    )?;
    Ok((
        canonical,
        ForecastFeatureRow {
            instrument_id,
            cutoff_at,
            component_kind,
            component_name: component_name.into(),
            component_version,
            value,
            lineage_sha256: Sha256Digest::new(lineage),
        },
    ))
}

fn uint8<'a>(
    batch: &'a RecordBatch,
    name: &str,
) -> Result<&'a UInt8Array, PythonDatasetCatalogError> {
    batch
        .column_by_name(name)
        .and_then(|array| array.as_any().downcast_ref::<UInt8Array>())
        .ok_or(PythonDatasetCatalogError::CorruptAdmission)
}

fn optional_padded(
    array: &FixedSizeBinaryArray,
    index: usize,
) -> Result<Option<&str>, PythonDatasetCatalogError> {
    if array.is_null(index) {
        Ok(None)
    } else {
        padded_text(array, index).map(Some)
    }
}

fn padded_text(
    array: &FixedSizeBinaryArray,
    index: usize,
) -> Result<&str, PythonDatasetCatalogError> {
    let bytes = array.value(index);
    let end = bytes
        .iter()
        .position(|byte| *byte == 0)
        .unwrap_or(bytes.len());
    if end == 0 || bytes[end..].iter().any(|byte| *byte != 0) {
        return Err(PythonDatasetCatalogError::CorruptAdmission);
    }
    std::str::from_utf8(&bytes[..end]).map_err(|_| PythonDatasetCatalogError::CorruptAdmission)
}
