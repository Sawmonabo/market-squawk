//! Bounded, pinned feature-dataset evidence for native forecast preparation.

use std::{num::NonZeroU64, time::Instant};

use arrow::{
    array::{
        Array as _, Decimal128Array, FixedSizeBinaryArray, Float64Array, TimestampNanosecondArray,
        UInt8Array, UInt32Array,
    },
    record_batch::RecordBatch,
};
use market_squawk_domain::{InstrumentId, ResearchTemporalCoordinate, Timestamp};
use tokio_util::sync::CancellationToken;
use sha2::{Digest as _, Sha256};

use super::{AnalyticalFeatureDataset, AnalyticalReadCapability, AnalyticalReadError};
use crate::manifest::CatalogFeatureDatasetSelection;
use crate::python_dataset::{finish_selection_hash, new_selection_hasher, update_selection_hash};
use crate::{
    CatalogEndpointIdentity, DatasetManifestRef, DatasetSplit, FeatureDatasetProductContract,
    PythonDatasetCatalogError, PythonDatasetRow, PythonDatasetValue, Sha256Digest,
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
    example_id: Box<str>,
    instrument_id: InstrumentId,
    source_selection_as_of: Timestamp,
    label_selection_as_of: Option<Timestamp>,
    decision_coordinate: ResearchTemporalCoordinate,
    observed_effective_at: Option<Timestamp>,
    label_effective_at: Option<Timestamp>,
    target_coordinate_kind: u8,
    split: DatasetSplit,
    component_kind: u8,
    component_name: Box<str>,
    component_version: u32,
    value: ForecastFeatureValue,
    lineage_sha256: Sha256Digest,
    origin_basis: Option<crate::FixedHorizonOriginBasis>,
    origin_series_sha256: Option<Sha256Digest>,
    origin_observation_sha256: Option<Sha256Digest>,
}

impl ForecastFeatureRow {
    pub fn example_id(&self) -> &str {
        &self.example_id
    }
    pub fn retained_bytes(&self) -> usize {
        std::mem::size_of::<Self>() + self.example_id.len() + self.component_name.len()
    }

    pub const fn instrument_id(&self) -> InstrumentId {
        self.instrument_id
    }

    pub const fn source_selection_as_of(&self) -> Timestamp {
        self.source_selection_as_of
    }

    pub const fn label_selection_as_of(&self) -> Option<Timestamp> {
        self.label_selection_as_of
    }
    pub const fn decision_at(&self) -> Option<Timestamp> {
        self.decision_coordinate.exact_timestamp()
    }
    pub const fn decision_coordinate(&self) -> &ResearchTemporalCoordinate {
        &self.decision_coordinate
    }

    /// Returns the exact effective coordinate of the observed feature state, when the published
    /// example retained timestamp precision.
    pub const fn observed_effective_at(&self) -> Option<Timestamp> {
        self.observed_effective_at
    }

    /// Returns the exact effective coordinate of the terminal label, when the published example
    /// retained timestamp precision.
    pub const fn label_effective_at(&self) -> Option<Timestamp> {
        self.label_effective_at
    }

    /// Returns the closed publication tag: `1` is an exact terminal pair and `2` is explicitly
    /// unsupported effective precision.
    pub const fn target_coordinate_kind(&self) -> u8 {
        self.target_coordinate_kind
    }

    /// Returns the exact chronological split retained by the admitted dataset row.
    pub const fn split(&self) -> DatasetSplit {
        self.split
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
    probability_event_target: Option<crate::ProbabilityEventTarget>,
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

/// One binary outcome selected only from an original receipt-admitted LocalAnalysis dataset.
/// There is no caller constructor or deserialization path into this authority.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ForecastProbabilityOutcome {
    event: crate::ProbabilityEventTarget,
    origin_series_sha256: Sha256Digest,
    origin_observation_sha256: Sha256Digest,
    instrument_id: InstrumentId,
    origin: Timestamp,
    origin_basis: crate::FixedHorizonOriginBasis,
    target_at: Timestamp,
    label_maturity: Timestamp,
    available_at: Timestamp,
    value: bool,
    lineage_sha256: Sha256Digest,
    production_receipt_sha256: Sha256Digest,
    fence: ForecastDatasetEvidenceFence,
}
impl ForecastProbabilityOutcome {
    /// Stable original economic observation identity across independently retained captures.
    /// This creates no outcome authority; original history receipt admission remains required.
    pub fn original_observation_identity(bar: &market_squawk_domain::MarketBarObservation) -> Result<Sha256Digest, AnalyticalReadError> {
        probability_origin_observation(bar)
    }
    /// Requires the exact original economic values and native time coordinate. A new capture
    /// may provide new provenance, but cannot revise the saved event's starting observation.
    pub fn matches_origin_observation(&self, bar: &market_squawk_domain::MarketBarObservation) -> bool {
        probability_origin_series(bar).is_ok_and(|digest| digest == self.origin_series_sha256)
            && probability_origin_observation(bar).is_ok_and(|digest| digest == self.origin_observation_sha256)
    }
    pub const fn origin_observation_sha256(&self) -> Sha256Digest { self.origin_observation_sha256 }
    pub const fn origin_series_sha256(&self) -> Sha256Digest { self.origin_series_sha256 }
    pub const fn event(&self) -> crate::ProbabilityEventTarget { self.event }
    pub const fn instrument_id(&self) -> InstrumentId { self.instrument_id }
    pub const fn origin(&self) -> Timestamp { self.origin }
    pub const fn origin_basis(&self) -> crate::FixedHorizonOriginBasis { self.origin_basis }
    pub const fn target_at(&self) -> Timestamp { self.target_at }
    pub const fn label_maturity(&self) -> Timestamp { self.label_maturity }
    pub const fn available_at(&self) -> Timestamp { self.available_at }
    pub const fn value(&self) -> bool { self.value }
    pub const fn lineage_sha256(&self) -> Sha256Digest { self.lineage_sha256 }
    pub const fn production_receipt_sha256(&self) -> Sha256Digest { self.production_receipt_sha256 }
    pub const fn fence(&self) -> &ForecastDatasetEvidenceFence { &self.fence }
    pub const fn quality(&self) -> market_squawk_domain::DataQuality {
        match self.event {
            crate::ProbabilityEventTarget::ProfitAfterCosts { .. } => market_squawk_domain::DataQuality::Modeled,
            _ => market_squawk_domain::DataQuality::Aggregated,
        }
    }
}

impl ForecastDatasetEvidence {
    /// Original descriptor meaning after its export hash and production receipt were verified.
    pub const fn probability_event_target(&self) -> Option<crate::ProbabilityEventTarget> {
        self.probability_event_target
    }

    /// Selects one actual matured label; absent/missing observations remain unavailable.
    pub fn select_probability_outcome(
        &self,
        expected_event: crate::ProbabilityEventTarget,
        instrument: InstrumentId,
        origin: Timestamp,
        target_at: Timestamp,
    ) -> Result<Option<ForecastProbabilityOutcome>, AnalyticalReadError> {
        let invalid = || AnalyticalReadError::PythonDataset(PythonDatasetCatalogError::CorruptAdmission);
        if expected_event.validate().is_err() || origin >= target_at { return Err(invalid()); }
        if self.dataset.product_contract().required_use() != crate::ResearchUse::LocalAnalysis
            || !self.dataset.product_contract().admits_probability_target(expected_event)
            || self.probability_event_target != Some(expected_event) {
            return Ok(None);
        }
        let mut selected = self.rows.iter().filter(|row| {
            row.instrument_id == instrument && row.observed_effective_at == Some(origin)
                && row.label_effective_at == Some(target_at) && row.component_kind == 2
                && row.component_name.as_ref() == expected_event.label_component_name()
                && row.component_version == 1
        });
        let Some(row) = selected.next() else { return Ok(None); };
        if selected.next().is_some() || row.lineage_sha256.bytes() == [0;32] { return Err(invalid()); }
        let value = match row.value {
            ForecastFeatureValue::Missing => return Ok(None),
            ForecastFeatureValue::Decimal { mantissa, scale } => {
                let value = rust_decimal::Decimal::try_from_i128_with_scale(mantissa, u32::from(scale)).map_err(|_| invalid())?;
                if value != rust_decimal::Decimal::ZERO && value != rust_decimal::Decimal::ONE { return Err(invalid()); }
                value == rust_decimal::Decimal::ONE
            }
            ForecastFeatureValue::Float(_) => return Err(invalid()),
        };
        let origin_basis = row.origin_basis.ok_or_else(invalid)?;
        let label_maturity = match expected_event {
            crate::ProbabilityEventTarget::ProfitAfterCosts { policy } => target_at.checked_add_nanos(policy.maximum_exit_lag_nanos).map_err(|_| invalid())?,
            _ => target_at,
        };
        let available_at = row.label_selection_as_of.ok_or_else(invalid)?.max(label_maturity);
        if available_at > self.fence.as_of { return Ok(None); }
        if row.source_selection_as_of > self.fence.as_of
            || row.decision_at().is_none_or(|decision| decision < origin || decision >= target_at) {
            return Err(invalid());
        }
        Ok(Some(ForecastProbabilityOutcome {
            event: expected_event, origin_series_sha256: row.origin_series_sha256.ok_or_else(invalid)?,
            origin_observation_sha256: row.origin_observation_sha256.ok_or_else(invalid)?,
            instrument_id: instrument, origin, origin_basis, target_at,
            label_maturity, available_at, value, lineage_sha256: row.lineage_sha256,
            production_receipt_sha256: self.dataset.production_receipt().receipt_sha256(),
            fence: self.fence.clone(),
        }))
    }
}

fn probability_origin_series(bar: &market_squawk_domain::MarketBarObservation) -> Result<Sha256Digest, AnalyticalReadError> {
    let provenance = bar.context().provenance();
    let bytes = serde_json::to_vec(&(provenance.instrument_id(), provenance.source_id(), provenance.venue_id(),
        bar.provider_instrument_id(), bar.feed(), bar.interval(), bar.adjustment(), bar.currency(),
        bar.time_semantics().timestamp_basis(),
        bar.time_semantics().session().map(|session| (session.kind(), session.ruleset())),
        bar.time_semantics().nominal_daily_date().map(|date| date.ruleset())))
        .map_err(|_| AnalyticalReadError::PythonDataset(PythonDatasetCatalogError::CorruptAdmission))?;
    Ok(Sha256Digest::new(Sha256::digest(bytes).into()))
}

fn probability_origin_observation(bar: &market_squawk_domain::MarketBarObservation) -> Result<Sha256Digest, AnalyticalReadError> {
    // Acquisition timestamps and native payload digests remain in the full input epoch and
    // source receipt. Equality here proves the economic coordinate across separate acquisitions.
    let bytes = serde_json::to_vec(&(
        probability_origin_series(bar)?.bytes(),
        (bar.time_semantics().effective_coordinate(), bar.time_semantics().period_start(),
            bar.time_semantics().period_end_exclusive()),
        (bar.open(), bar.high(), bar.low(), bar.close(), bar.volume(), bar.trade_count(), bar.vwap()),
    )).map_err(|_| AnalyticalReadError::PythonDataset(PythonDatasetCatalogError::CorruptAdmission))?;
    Ok(Sha256Digest::new(Sha256::digest(bytes).into()))
}

impl AnalyticalReadCapability {
    /// Materializes one exact Python-admitted generation under a point-in-time cutoff.
    pub async fn forecast_dataset_evidence(
        &self,
        expected_contract: FeatureDatasetProductContract,
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
            expected_contract,
            CatalogFeatureDatasetSelection::ExactManifest(manifest),
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
        let probability_event_target = crate::python_dataset::feature_dataset_summary(
            &retained.descriptor, export_sha256,
        )?.probability_event_target;
        let pinned = retained.pinned.clone();
        let dataset = AnalyticalFeatureDataset::from_catalog(retained, expected_contract)?;
        let catalog_identity =
            CatalogEndpointIdentity::try_from_bytes(self.manifests.catalog_binding()).ok_or(
                AnalyticalReadError::Manifest(crate::ManifestCatalogError::CorruptCatalog),
            )?;
        let batches = self
            .objects
            .read_pinned_bounded_async(&pinned, limits.max_rows, limits.max_bytes, &cancellation)
            .await
            .map_err(AnalyticalReadError::from)?;
        let mut rows = Vec::new();
        let mut hasher = new_selection_hasher(catalog_identity, export_sha256, as_of);
        for batch in batches {
            for index in 0..batch.num_rows() {
                if index % 128 == 0 && (cancellation.is_cancelled() || Instant::now() >= deadline) {
                    return Err(AnalyticalReadError::Query(crate::QueryError::Cancelled));
                }
                let (canonical, mut view) = decode_row(&batch, index)?;
                let epoch = crate::FeatureDatasetInputEpoch::decode(
                    crate::python_dataset::input_epoch_bytes(&batch, index)?
                        .ok_or(AnalyticalReadError::InvalidInputEpoch)?,
                )
                .map_err(|_| AnalyticalReadError::InvalidInputEpoch)?;
                view.origin_basis = epoch.fixed_horizon_origin_basis();
                view.origin_series_sha256 = epoch.market_bar().map(probability_origin_series).transpose()?;
                view.origin_observation_sha256 = epoch.market_bar().map(probability_origin_observation).transpose()?;
                if epoch.population_basis() != dataset.population_basis() {
                    return Err(AnalyticalReadError::InvalidInputEpoch);
                }
                if canonical.available_as_of(as_of) {
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
            probability_event_target,
        })
    }

    /// Re-reads and proves equality with one previously retained exact evidence fence.
    pub async fn revalidate_forecast_dataset_evidence(
        &self,
        expected_contract: FeatureDatasetProductContract,
        expected: &ForecastDatasetEvidenceFence,
        limits: ForecastDatasetReadLimits,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<(), AnalyticalReadError> {
        let observed = self
            .forecast_dataset_evidence(
                expected_contract,
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

pub(super) fn decode_row(
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
    let source_selection_as_of = Timestamp::from_unix_nanos(
        batch
            .column_by_name("source_selection_as_of")
            .and_then(|array| array.as_any().downcast_ref::<TimestampNanosecondArray>())
            .ok_or(PythonDatasetCatalogError::CorruptAdmission)?
            .value(index),
    );
    let decision_coordinate = crate::python_dataset::decision_coordinate(batch, index)?;
    let label_selection_as_of = optional_timestamp(batch, "label_selection_as_of", index)?;
    let observed_effective_at = optional_timestamp(batch, "observed_effective_at", index)?;
    let label_effective_at = optional_timestamp(batch, "label_effective_at", index)?;
    let target_coordinate_kind = uint8(batch, "target_coordinate_kind")?.value(index);
    let split_tag = uint8(batch, "split")?.value(index);
    let split = match split_tag {
        1 => DatasetSplit::Train,
        2 => DatasetSplit::Validation,
        3 => DatasetSplit::Test,
        _ => return Err(PythonDatasetCatalogError::CorruptAdmission.into()),
    };
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
        source_selection_as_of,
        label_selection_as_of,
        decision_coordinate.clone(),
        observed_effective_at,
        label_effective_at,
        target_coordinate_kind,
        split_tag,
        component_kind,
        component_name,
        component_version,
        canonical_value,
        unit,
        currency,
        lineage,
        crate::python_dataset::input_epoch_bytes(batch, index)?,
    )?;
    Ok((
        canonical,
        ForecastFeatureRow {
            example_id: example.into(),
            instrument_id,
            source_selection_as_of,
            label_selection_as_of,
            decision_coordinate,
            observed_effective_at,
            label_effective_at,
            target_coordinate_kind,
            split,
            component_kind,
            component_name: component_name.into(),
            component_version,
            value,
            lineage_sha256: Sha256Digest::new(lineage),
            origin_basis: None,
            origin_series_sha256: None,
            origin_observation_sha256: None,
        },
    ))
}

fn optional_timestamp(
    batch: &RecordBatch,
    name: &str,
    index: usize,
) -> Result<Option<Timestamp>, PythonDatasetCatalogError> {
    let values = batch
        .column_by_name(name)
        .and_then(|array| array.as_any().downcast_ref::<TimestampNanosecondArray>())
        .ok_or(PythonDatasetCatalogError::CorruptAdmission)?;
    Ok((!values.is_null(index)).then(|| Timestamp::from_unix_nanos(values.value(index))))
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
