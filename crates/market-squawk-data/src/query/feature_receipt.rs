//! Producer receipt for one exact canonical point-in-time feature value.

use std::mem::size_of;
use std::num::NonZeroU32;

use arrow::array::{
    Array as _, Decimal128Array, FixedSizeBinaryArray, TimestampNanosecondArray, UInt8Array,
    UInt32Array,
};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use market_squawk_domain::{
    Currency, DigestAlgorithm, EvidenceDigest, InstrumentId, SourceIdentifier, Timestamp,
};
use rust_decimal::Decimal;
use uuid::Uuid;

use super::{PinnedQueryOutput, QueryError, QueryResult};
use crate::DatasetManifestRef;

const EXAMPLE_ID_COLUMN: usize = 0;
const INSTRUMENT_ID_COLUMN: usize = 1;
const CUTOFF_AT_COLUMN: usize = 2;
const COMPONENT_KIND_COLUMN: usize = 3;
const COMPONENT_NAME_COLUMN: usize = 4;
const COMPONENT_VERSION_COLUMN: usize = 5;
const VALUE_COLUMN: usize = 6;
const SCALE_COLUMN: usize = 7;
const UNIT_COLUMN: usize = 8;
const CURRENCY_COLUMN: usize = 9;
const LINEAGE_COLUMN: usize = 10;

/// Exact decimal feature row issued only by the canonical feature query path.
///
/// This type has no public constructor or deserializer. Its value, feature identity, temporal
/// coordinate, instrument, lineage, and complete pinned-query identities all come from the same
/// registered base row.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PinnedFeatureMonetaryValue {
    manifest: DatasetManifestRef,
    object_graph_digest: EvidenceDigest,
    query_identity: EvidenceDigest,
    result_digest: EvidenceDigest,
    row: usize,
    example_id: SourceIdentifier,
    instrument_id: InstrumentId,
    cutoff_at: Timestamp,
    component_name: String,
    component_version: NonZeroU32,
    mantissa: i128,
    precision: u8,
    scale: u8,
    unit: Option<SourceIdentifier>,
    currency: Currency,
    lineage_digest: EvidenceDigest,
}

impl PinnedFeatureMonetaryValue {
    pub(super) fn try_from_output(
        output: &PinnedQueryOutput,
        selected_row: usize,
    ) -> Result<Self, QueryError> {
        let QueryResult::Inline { batches, .. } = output.result() else {
            return Err(QueryError::MonetaryValueRequiresInlineResult);
        };
        let batch = single_result_row(batches)?;
        let row = 0;
        let kinds = array::<UInt8Array>(batch, COMPONENT_KIND_COLUMN)?;
        if kinds.is_null(row) || kinds.value(row) != 1 {
            return Err(QueryError::InvalidMonetaryCell);
        }
        let example_id =
            SourceIdentifier::try_from(required_fixed_text(batch, EXAMPLE_ID_COLUMN, row)?)
                .map_err(|_| QueryError::InvalidMonetaryCell)?;
        let instrument_id = required_instrument(batch, INSTRUMENT_ID_COLUMN, row)?;
        let cutoff_at = required_timestamp(batch, CUTOFF_AT_COLUMN, row)?;
        let component_name = required_fixed_text(batch, COMPONENT_NAME_COLUMN, row)?.to_owned();
        let versions = array::<UInt32Array>(batch, COMPONENT_VERSION_COLUMN)?;
        let component_version =
            NonZeroU32::new(versions.value(row)).ok_or(QueryError::InvalidMonetaryCell)?;
        let values = array::<Decimal128Array>(batch, VALUE_COLUMN)?;
        let scales = array::<UInt8Array>(batch, SCALE_COLUMN)?;
        let currencies = array::<FixedSizeBinaryArray>(batch, CURRENCY_COLUMN)?;
        if values.is_null(row) || scales.is_null(row) || currencies.is_null(row) {
            return Err(QueryError::InvalidMonetaryCell);
        }
        let (precision, physical_scale) = match values.data_type() {
            DataType::Decimal128(precision, physical_scale) => (*precision, *physical_scale),
            _ => return Err(QueryError::InvalidMonetaryCell),
        };
        if physical_scale != 0 {
            return Err(QueryError::InvalidMonetaryCell);
        }
        let scale = scales.value(row);
        if u32::from(scale) > Decimal::MAX_SCALE {
            return Err(QueryError::UnsupportedMonetaryScale);
        }
        let mantissa = values.value(row);
        Decimal::try_from_i128_with_scale(mantissa, u32::from(scale))
            .map_err(|_| QueryError::InvalidMonetaryCell)?;
        let currency = Currency::try_from(fixed_text(currencies, row)?)
            .map_err(|_| QueryError::InvalidMonetaryCell)?;
        let unit = optional_source_identifier(batch, UNIT_COLUMN, row)?;
        let lineages = array::<FixedSizeBinaryArray>(batch, LINEAGE_COLUMN)?;
        if lineages.is_null(row) {
            return Err(QueryError::InvalidMonetaryCell);
        }
        let lineage: [u8; 32] = lineages
            .value(row)
            .try_into()
            .map_err(|_| QueryError::InvalidMonetaryCell)?;
        Ok(Self {
            manifest: output.manifest().clone(),
            object_graph_digest: output.object_graph_digest(),
            query_identity: output.query_identity(),
            result_digest: output.result_digest(),
            row: selected_row,
            example_id,
            instrument_id,
            cutoff_at,
            component_name,
            component_version,
            mantissa,
            precision,
            scale,
            unit,
            currency,
            lineage_digest: EvidenceDigest::new(DigestAlgorithm::Sha256, lineage),
        })
    }

    /// Returns a checked conservative retained-byte charge for this owned receipt.
    pub fn retained_bytes(&self) -> Option<usize> {
        size_of::<Self>()
            .checked_add(self.manifest.dataset_id().as_str().len())?
            .checked_add(self.manifest.schema().name().len())?
            .checked_add(self.example_id.retained_bytes())?
            .checked_add(self.component_name.capacity())?
            .checked_add(
                self.unit
                    .as_ref()
                    .map_or(0, SourceIdentifier::retained_bytes),
            )
    }

    /// Returns the exact queried generation.
    pub const fn manifest(&self) -> &DatasetManifestRef {
        &self.manifest
    }

    /// Returns the complete pinned object-graph identity.
    pub const fn object_graph_digest(&self) -> EvidenceDigest {
        self.object_graph_digest
    }

    /// Returns the fixed query and execution-limit identity.
    pub const fn query_identity(&self) -> EvidenceDigest {
        self.query_identity
    }

    /// Returns the exact Arrow IPC result identity.
    pub const fn result_digest(&self) -> EvidenceDigest {
        self.result_digest
    }

    /// Returns the stable selected-row offset.
    pub const fn row(&self) -> usize {
        self.row
    }

    /// Returns the producer's stable example identity.
    pub const fn example_id(&self) -> &SourceIdentifier {
        &self.example_id
    }

    /// Returns the feature row's instrument.
    pub const fn instrument_id(&self) -> InstrumentId {
        self.instrument_id
    }

    /// Returns the point-in-time feature cutoff.
    pub const fn cutoff_at(&self) -> Timestamp {
        self.cutoff_at
    }

    /// Returns the canonical feature name read from the row.
    pub fn component_name(&self) -> &str {
        &self.component_name
    }

    /// Returns the nonzero feature version read from the row.
    pub const fn component_version(&self) -> NonZeroU32 {
        self.component_version
    }

    /// Returns the exact Decimal128 mantissa.
    pub const fn mantissa(&self) -> i128 {
        self.mantissa
    }

    /// Returns the admitted Arrow precision.
    pub const fn precision(&self) -> u8 {
        self.precision
    }

    /// Returns the exact semantic scale.
    pub const fn scale(&self) -> u8 {
        self.scale
    }

    /// Returns the row unit when supplied.
    pub const fn unit(&self) -> Option<&SourceIdentifier> {
        self.unit.as_ref()
    }

    /// Returns the exact row currency.
    pub const fn currency(&self) -> Currency {
        self.currency
    }

    /// Returns the producer's complete selected-input lineage identity.
    pub const fn lineage_digest(&self) -> EvidenceDigest {
        self.lineage_digest
    }

    /// Converts the retained mantissa and scale without rounding.
    ///
    /// # Errors
    ///
    /// Returns [`QueryError::InvalidMonetaryCell`] if the retained value cannot be represented.
    pub fn decimal(&self) -> Result<Decimal, QueryError> {
        Decimal::try_from_i128_with_scale(self.mantissa, u32::from(self.scale))
            .map_err(|_| QueryError::InvalidMonetaryCell)
    }
}

fn single_result_row(batches: &[RecordBatch]) -> Result<&RecordBatch, QueryError> {
    let mut nonempty = batches.iter().filter(|batch| batch.num_rows() != 0);
    let batch = nonempty.next().ok_or(QueryError::MonetaryCellOutOfBounds)?;
    if batch.num_rows() != 1 || nonempty.next().is_some() {
        return Err(QueryError::InvalidMonetaryCell);
    }
    Ok(batch)
}

fn array<T: 'static>(batch: &RecordBatch, column: usize) -> Result<&T, QueryError> {
    batch
        .columns()
        .get(column)
        .and_then(|value| value.as_any().downcast_ref::<T>())
        .ok_or(QueryError::InvalidMonetaryCell)
}

fn required_fixed_text(batch: &RecordBatch, column: usize, row: usize) -> Result<&str, QueryError> {
    fixed_text(array::<FixedSizeBinaryArray>(batch, column)?, row)
}

fn optional_source_identifier(
    batch: &RecordBatch,
    column: usize,
    row: usize,
) -> Result<Option<SourceIdentifier>, QueryError> {
    let values = array::<FixedSizeBinaryArray>(batch, column)?;
    if values.is_null(row) {
        Ok(None)
    } else {
        SourceIdentifier::try_from(fixed_text(values, row)?)
            .map(Some)
            .map_err(|_| QueryError::InvalidMonetaryCell)
    }
}

fn required_instrument(
    batch: &RecordBatch,
    column: usize,
    row: usize,
) -> Result<InstrumentId, QueryError> {
    let values = array::<FixedSizeBinaryArray>(batch, column)?;
    if values.is_null(row) {
        return Err(QueryError::InvalidMonetaryCell);
    }
    let bytes: [u8; 16] = values
        .value(row)
        .try_into()
        .map_err(|_| QueryError::InvalidMonetaryCell)?;
    InstrumentId::try_from(Uuid::from_bytes(bytes)).map_err(|_| QueryError::InvalidMonetaryCell)
}

fn fixed_text(values: &FixedSizeBinaryArray, row: usize) -> Result<&str, QueryError> {
    if values.is_null(row) {
        return Err(QueryError::InvalidMonetaryCell);
    }
    let bytes = values.value(row);
    let end = bytes
        .iter()
        .position(|byte| *byte == 0)
        .unwrap_or(bytes.len());
    if end == 0 || bytes[end..].iter().any(|byte| *byte != 0) {
        return Err(QueryError::InvalidMonetaryCell);
    }
    std::str::from_utf8(&bytes[..end]).map_err(|_| QueryError::InvalidMonetaryCell)
}

fn required_timestamp(
    batch: &RecordBatch,
    column: usize,
    row: usize,
) -> Result<Timestamp, QueryError> {
    let values = array::<TimestampNanosecondArray>(batch, column)?;
    if values.is_null(row) {
        Err(QueryError::InvalidMonetaryCell)
    } else {
        Ok(Timestamp::from_unix_nanos(values.value(row)))
    }
}
