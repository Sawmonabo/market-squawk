//! Admission from an owned non-forgeable Task 11 pinned-query receipt.

use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroU32;

use arrow::array::{
    Array as _, Decimal128Array, FixedSizeBinaryArray, Float64Array, TimestampNanosecondArray,
    UInt8Array, UInt32Array,
};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use market_squawk_data::{
    DatasetSchemaRegistry, PinnedInstrumentDefinitions, PinnedQueryOutput, QueryResult,
    Sha256Digest,
};
use market_squawk_domain::{
    BasisPoints, DigestAlgorithm, InstrumentExecutionTerms, InstrumentId, PriceTicks, QuantityLots,
    SourceIdentifier, Timestamp,
};
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive as _;
use sha2::{Digest as _, Sha256};
use uuid::Uuid;

use super::{
    BacktestDataset, BacktestDatasetInput, BacktestLimits, BacktestObservation,
    BacktestObservationInput, HistoricalUniverseStatus, ResearchFeatureValue,
};
use crate::engine::BacktestError;

pub const EVENT_AT_COMPONENT: &str = "market_squawk.backtest.event_at_unix_nanos";
pub const AVAILABLE_AT_COMPONENT: &str = "market_squawk.backtest.available_at_unix_nanos";
pub const STALE_AT_COMPONENT: &str = "market_squawk.backtest.stale_at_unix_nanos";
pub const MID_PRICE_COMPONENT: &str = "market_squawk.backtest.mid_price_ticks";
pub const SPREAD_COMPONENT: &str = "market_squawk.backtest.spread_basis_points";
pub const DEPTH_COMPONENT: &str = "market_squawk.backtest.executable_depth_lots";
pub const UNIVERSE_COMPONENT: &str = "market_squawk.backtest.universe_status";

const EXAMPLE_ID: usize = 0;
const INSTRUMENT_ID: usize = 1;
const CUTOFF_AT: usize = 2;
const COMPONENT_KIND: usize = 4;
const COMPONENT_NAME: usize = 5;
const COMPONENT_VERSION: usize = 6;
const VALUE_F64: usize = 7;
const VALUE_DECIMAL: usize = 8;
const VALUE_SCALE: usize = 9;
const MISSING_REASON: usize = 12;
const LINEAGE: usize = 13;
const FEATURE_KIND: u8 = 1;
const LABEL_KIND: u8 = 2;
const RESERVED_VERSION: u32 = 1;

pub(super) fn from_pinned_query(
    output: PinnedQueryOutput,
    instrument_definitions: PinnedInstrumentDefinitions,
    limits: BacktestLimits,
) -> Result<BacktestDataset, BacktestError> {
    let canonical = DatasetSchemaRegistry::local().canonical_feature_labels()?;
    if output.manifest().schema() != &canonical
        || output.object_graph_digest().algorithm() != DigestAlgorithm::Sha256
        || output.query_identity().algorithm() != DigestAlgorithm::Sha256
        || output.result_digest().algorithm() != DigestAlgorithm::Sha256
    {
        return Err(BacktestError::InvalidDataset);
    }
    let object_graph_digest = digest(output.object_graph_digest().bytes())?;
    let point_in_time_audit = digest(output.query_identity().bytes())?;
    let point_in_time_content = digest(output.result_digest().bytes())?;
    let instrument_definition_content = instrument_definitions.content_identity();
    let instrument_definition_audit = instrument_definitions.audit_identity();
    let manifest = output.manifest().clone();
    let QueryResult::Inline {
        batches,
        byte_count,
    } = output.result()
    else {
        return Err(BacktestError::PinnedInputRequiresInlineBatches);
    };
    let byte_count = usize::try_from(*byte_count).map_err(|_| BacktestError::LimitExceeded)?;
    if batches.is_empty() || byte_count == 0 || byte_count > limits.max_retained_bytes {
        return Err(BacktestError::LimitExceeded);
    }

    if instrument_definitions.instrument_count() == 0 {
        return Err(BacktestError::InvalidDataset);
    }

    let expected = DatasetSchemaRegistry::local().resolve(&canonical)?;
    let mut groups = BTreeMap::<GroupKey, Group>::new();
    let mut row_count = 0_usize;
    for batch in batches {
        validate_schema(batch, &expected)?;
        row_count = row_count
            .checked_add(batch.num_rows())
            .ok_or(BacktestError::LimitExceeded)?;
        if row_count > limits.max_observations.saturating_mul(1_024) {
            return Err(BacktestError::LimitExceeded);
        }
        admit_batch(batch, &instrument_definitions, &mut groups)?;
    }
    if groups.is_empty() || groups.len() > limits.max_observations {
        return Err(BacktestError::LimitExceeded);
    }

    let mut used_instruments = BTreeSet::new();
    let mut observations = Vec::new();
    observations
        .try_reserve_exact(groups.len())
        .map_err(|_| BacktestError::LimitExceeded)?;
    for (key, group) in groups {
        used_instruments.insert(key.instrument_id);
        observations.push(group.finish(key)?);
    }
    if used_instruments.len() != instrument_definitions.instrument_count()
        || instrument_definitions
            .instrument_ids()
            .any(|instrument| !used_instruments.contains(&instrument))
    {
        return Err(BacktestError::InvalidDataset);
    }
    BacktestDataset::try_new(BacktestDatasetInput {
        manifest,
        object_graph_digest,
        point_in_time_content,
        point_in_time_audit,
        instrument_definition_content,
        instrument_definition_audit,
        observations,
    })
}

fn validate_schema(
    batch: &RecordBatch,
    expected: &arrow::datatypes::Schema,
) -> Result<(), BacktestError> {
    if batch.num_columns() != expected.fields().len()
        || batch
            .schema()
            .fields()
            .iter()
            .zip(expected.fields())
            .any(|(actual, expected)| {
                actual.name() != expected.name()
                    || actual.data_type() != expected.data_type()
                    || actual.is_nullable() != expected.is_nullable()
            })
    {
        return Err(BacktestError::InvalidDataset);
    }
    Ok(())
}

fn admit_batch(
    batch: &RecordBatch,
    instrument_definitions: &PinnedInstrumentDefinitions,
    groups: &mut BTreeMap<GroupKey, Group>,
) -> Result<(), BacktestError> {
    let examples = array::<FixedSizeBinaryArray>(batch, EXAMPLE_ID)?;
    let instruments = array::<FixedSizeBinaryArray>(batch, INSTRUMENT_ID)?;
    let cutoffs = array::<TimestampNanosecondArray>(batch, CUTOFF_AT)?;
    let kinds = array::<UInt8Array>(batch, COMPONENT_KIND)?;
    let names = array::<FixedSizeBinaryArray>(batch, COMPONENT_NAME)?;
    let versions = array::<UInt32Array>(batch, COMPONENT_VERSION)?;
    let float_values = array::<Float64Array>(batch, VALUE_F64)?;
    let decimal_values = array::<Decimal128Array>(batch, VALUE_DECIMAL)?;
    let scales = array::<UInt8Array>(batch, VALUE_SCALE)?;
    let missing = array::<FixedSizeBinaryArray>(batch, MISSING_REASON)?;
    let lineages = array::<FixedSizeBinaryArray>(batch, LINEAGE)?;
    if decimal_values.data_type() != &DataType::Decimal128(38, 0) {
        return Err(BacktestError::InvalidDataset);
    }
    for row in 0..batch.num_rows() {
        if kinds.is_null(row) {
            return Err(BacktestError::InvalidDataset);
        }
        match kinds.value(row) {
            LABEL_KIND => continue,
            FEATURE_KIND => {}
            _ => return Err(BacktestError::InvalidDataset),
        }
        let instrument_id = instrument(instruments, row)?;
        let cutoff = Timestamp::from_unix_nanos(required(cutoffs, row)?);
        let execution_terms = instrument_definitions
            .execution_terms_at(instrument_id, cutoff)
            .ok_or(BacktestError::InvalidDataset)?;
        let key = GroupKey {
            cutoff,
            instrument_id,
            example_id: fixed_text(examples, row)?.to_owned(),
        };
        let version =
            NonZeroU32::new(required(versions, row)?).ok_or(BacktestError::InvalidDataset)?;
        let name = fixed_text(names, row)?;
        let lineage = fixed_digest(lineages, row)?;
        let group = groups
            .entry(key)
            .or_insert_with(|| Group::new(execution_terms));
        if group.execution_terms != execution_terms || !group.names.insert(name.to_owned()) {
            return Err(BacktestError::InvalidDataset);
        }
        group.hash_component(name, version, lineage)?;
        let value = ComponentValueReader {
            float_values,
            decimal_values,
            scales,
            missing,
            row,
        };
        group.apply(name, version, value)?;
    }
    Ok(())
}

#[derive(Debug, Eq, Ord, PartialEq, PartialOrd)]
struct GroupKey {
    cutoff: Timestamp,
    instrument_id: InstrumentId,
    example_id: String,
}

#[derive(Debug)]
struct Group {
    execution_terms: InstrumentExecutionTerms,
    event_at: Option<Timestamp>,
    available_at: Option<Timestamp>,
    stale_at: Option<Timestamp>,
    mid_seen: bool,
    mid_price: Option<PriceTicks>,
    spread: Option<BasisPoints>,
    depth: Option<QuantityLots>,
    universe: Option<HistoricalUniverseStatus>,
    features: Vec<ResearchFeatureValue>,
    names: BTreeSet<String>,
    lineage: Sha256,
}

impl Group {
    fn new(execution_terms: InstrumentExecutionTerms) -> Self {
        let mut lineage = Sha256::new();
        lineage.update(b"market-squawk/backtest-observation-lineage/v1");
        Self {
            execution_terms,
            event_at: None,
            available_at: None,
            stale_at: None,
            mid_seen: false,
            mid_price: None,
            spread: None,
            depth: None,
            universe: None,
            features: Vec::new(),
            names: BTreeSet::new(),
            lineage,
        }
    }

    fn hash_component(
        &mut self,
        name: &str,
        version: NonZeroU32,
        lineage: [u8; 32],
    ) -> Result<(), BacktestError> {
        self.lineage.update(
            u64::try_from(name.len())
                .map_err(|_| BacktestError::LimitExceeded)?
                .to_be_bytes(),
        );
        self.lineage.update(name.as_bytes());
        self.lineage.update(version.get().to_be_bytes());
        self.lineage.update(lineage);
        Ok(())
    }

    fn apply(
        &mut self,
        name: &str,
        version: NonZeroU32,
        value: ComponentValueReader<'_>,
    ) -> Result<(), BacktestError> {
        let reserved = matches!(
            name,
            EVENT_AT_COMPONENT
                | AVAILABLE_AT_COMPONENT
                | STALE_AT_COMPONENT
                | MID_PRICE_COMPONENT
                | SPREAD_COMPONENT
                | DEPTH_COMPONENT
                | UNIVERSE_COMPONENT
        );
        if reserved && version.get() != RESERVED_VERSION {
            return Err(BacktestError::InvalidDataset);
        }
        match name {
            EVENT_AT_COMPONENT => self.event_at = Some(value.exact_timestamp()?),
            AVAILABLE_AT_COMPONENT => self.available_at = Some(value.exact_timestamp()?),
            STALE_AT_COMPONENT => self.stale_at = Some(value.exact_timestamp()?),
            MID_PRICE_COMPONENT => {
                self.mid_seen = true;
                self.mid_price = value.optional_exact_i64()?.map(PriceTicks::new);
            }
            SPREAD_COMPONENT => {
                let raw =
                    i32::try_from(value.exact_i64()?).map_err(|_| BacktestError::InvalidDataset)?;
                self.spread = Some(BasisPoints::new(raw));
            }
            DEPTH_COMPONENT => {
                self.depth = Some(
                    QuantityLots::new(value.exact_i64()?)
                        .map_err(|_| BacktestError::InvalidDataset)?,
                );
            }
            UNIVERSE_COMPONENT => {
                self.universe = Some(match value.exact_i64()? {
                    1 => HistoricalUniverseStatus::Eligible,
                    2 => HistoricalUniverseStatus::Ineligible,
                    3 => HistoricalUniverseStatus::Delisted,
                    _ => return Err(BacktestError::InvalidDataset),
                });
            }
            _ => {
                if let Some(number) = value.optional_feature()? {
                    self.features.push(ResearchFeatureValue::try_new(
                        SourceIdentifier::try_from(name)?,
                        version,
                        number,
                    )?);
                }
            }
        }
        Ok(())
    }

    fn finish(self, key: GroupKey) -> Result<BacktestObservation, BacktestError> {
        if !self.mid_seen {
            return Err(BacktestError::InvalidDataset);
        }
        BacktestObservation::try_new(BacktestObservationInput {
            execution_terms: self.execution_terms,
            event_at: self.event_at.ok_or(BacktestError::InvalidDataset)?,
            available_at: self.available_at.ok_or(BacktestError::InvalidDataset)?,
            decision_at: key.cutoff,
            stale_at: self.stale_at.ok_or(BacktestError::InvalidDataset)?,
            mid_price: self.mid_price,
            spread_basis_points: self.spread.ok_or(BacktestError::InvalidDataset)?,
            executable_depth: self.depth.ok_or(BacktestError::InvalidDataset)?,
            universe: self.universe.ok_or(BacktestError::InvalidDataset)?,
            features: self.features,
            lineage_digest: Sha256Digest::new(self.lineage.finalize().into()),
        })
    }
}

struct ComponentValueReader<'batch> {
    float_values: &'batch Float64Array,
    decimal_values: &'batch Decimal128Array,
    scales: &'batch UInt8Array,
    missing: &'batch FixedSizeBinaryArray,
    row: usize,
}

impl ComponentValueReader<'_> {
    fn exact_timestamp(&self) -> Result<Timestamp, BacktestError> {
        Ok(Timestamp::from_unix_nanos(self.exact_i64()?))
    }

    fn exact_i64(&self) -> Result<i64, BacktestError> {
        self.optional_exact_i64()?
            .ok_or(BacktestError::InvalidDataset)
    }

    fn optional_exact_i64(&self) -> Result<Option<i64>, BacktestError> {
        if !self.missing.is_null(self.row) {
            if self.float_values.is_null(self.row)
                && self.decimal_values.is_null(self.row)
                && self.scales.is_null(self.row)
            {
                return Ok(None);
            }
            return Err(BacktestError::InvalidDataset);
        }
        if !self.float_values.is_null(self.row)
            || self.decimal_values.is_null(self.row)
            || self.scales.is_null(self.row)
            || self.scales.value(self.row) != 0
        {
            return Err(BacktestError::InvalidDataset);
        }
        i64::try_from(self.decimal_values.value(self.row))
            .map(Some)
            .map_err(|_| BacktestError::InvalidDataset)
    }

    fn optional_feature(&self) -> Result<Option<f64>, BacktestError> {
        if !self.missing.is_null(self.row) {
            if self.float_values.is_null(self.row) && self.decimal_values.is_null(self.row) {
                return Ok(None);
            }
            return Err(BacktestError::InvalidDataset);
        }
        match (
            self.float_values.is_null(self.row),
            self.decimal_values.is_null(self.row),
        ) {
            (false, true) => {
                let value = self.float_values.value(self.row);
                value
                    .is_finite()
                    .then_some(Some(value))
                    .ok_or(BacktestError::InvalidDataset)
            }
            (true, false) if !self.scales.is_null(self.row) => {
                let decimal = Decimal::try_from_i128_with_scale(
                    self.decimal_values.value(self.row),
                    u32::from(self.scales.value(self.row)),
                )
                .map_err(|_| BacktestError::InvalidDataset)?;
                decimal
                    .to_f64()
                    .filter(|value| value.is_finite())
                    .map(Some)
                    .ok_or(BacktestError::InvalidDataset)
            }
            _ => Err(BacktestError::InvalidDataset),
        }
    }
}

fn array<T: 'static>(batch: &RecordBatch, index: usize) -> Result<&T, BacktestError> {
    batch
        .column(index)
        .as_any()
        .downcast_ref::<T>()
        .ok_or(BacktestError::InvalidDataset)
}

fn required<T: arrow::array::ArrowPrimitiveType>(
    values: &arrow::array::PrimitiveArray<T>,
    row: usize,
) -> Result<T::Native, BacktestError> {
    if values.is_null(row) {
        Err(BacktestError::InvalidDataset)
    } else {
        Ok(values.value(row))
    }
}

fn fixed_text(values: &FixedSizeBinaryArray, row: usize) -> Result<&str, BacktestError> {
    if values.is_null(row) {
        return Err(BacktestError::InvalidDataset);
    }
    let bytes = values.value(row);
    let end = bytes
        .iter()
        .position(|byte| *byte == 0)
        .unwrap_or(bytes.len());
    if end == 0
        || bytes
            .get(end..)
            .is_some_and(|tail| tail.iter().any(|byte| *byte != 0))
    {
        return Err(BacktestError::InvalidDataset);
    }
    std::str::from_utf8(&bytes[..end]).map_err(|_| BacktestError::InvalidDataset)
}

fn instrument(values: &FixedSizeBinaryArray, row: usize) -> Result<InstrumentId, BacktestError> {
    if values.is_null(row) {
        return Err(BacktestError::InvalidDataset);
    }
    let bytes: [u8; 16] = values
        .value(row)
        .try_into()
        .map_err(|_| BacktestError::InvalidDataset)?;
    InstrumentId::try_from(Uuid::from_bytes(bytes)).map_err(|_| BacktestError::InvalidDataset)
}

fn fixed_digest(values: &FixedSizeBinaryArray, row: usize) -> Result<[u8; 32], BacktestError> {
    if values.is_null(row) {
        return Err(BacktestError::InvalidDataset);
    }
    let digest: [u8; 32] = values
        .value(row)
        .try_into()
        .map_err(|_| BacktestError::InvalidDataset)?;
    if digest == [0; 32] {
        return Err(BacktestError::InvalidDataset);
    }
    Ok(digest)
}

fn digest(bytes: [u8; 32]) -> Result<Sha256Digest, BacktestError> {
    if bytes == [0; 32] {
        Err(BacktestError::InvalidDataset)
    } else {
        Ok(Sha256Digest::new(bytes))
    }
}
