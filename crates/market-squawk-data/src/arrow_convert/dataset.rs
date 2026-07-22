//! Generic registered-dataset Arrow publication contract.

use arrow::array::{
    Array as _, Decimal128Array, FixedSizeBinaryArray, Float64Array, UInt8Array, UInt32Array,
};
use arrow::record_batch::RecordBatch;
use market_squawk_domain::{Currency, InstrumentId, SourceIdentifier};
use uuid::Uuid;

use super::ArrowConversionError;
use crate::schema::{
    BUILD_DIGEST_KEY, DATASET_KEY, DatasetSchemaRef, DatasetSchemaRegistry,
    FEATURE_LABEL_SCHEMA_NAME, POLICY_DIGEST_KEY, REQUEST_DIGEST_KEY, RESEARCH_SCHEMA_NAME,
    UNIVERSE_DIGEST_KEY, decode_hex, schema_ref_from_metadata,
};

/// A nonempty Arrow record batch validated against one exact registered dataset schema.
#[derive(Clone, Debug)]
pub struct DatasetArrowBatch {
    pub(super) schema_ref: DatasetSchemaRef,
    pub(super) batch: RecordBatch,
}

impl DatasetArrowBatch {
    /// Validates a batch's complete registered schema identity, bindings, and field layout.
    pub fn try_new(
        schema_ref: DatasetSchemaRef,
        batch: RecordBatch,
    ) -> Result<Self, ArrowConversionError> {
        if batch.num_rows() == 0 {
            return Err(ArrowConversionError::EmptyBatch);
        }
        let registry = DatasetSchemaRegistry::local();
        let expected = registry.resolve(&schema_ref)?;
        let retained = schema_ref_from_metadata(batch.schema().as_ref())?;
        if retained != schema_ref
            || batch.schema().fields() != expected.fields()
            || expected
                .metadata()
                .iter()
                .any(|(key, value)| batch.schema().metadata().get(key) != Some(value))
            || batch
                .schema()
                .fields()
                .iter()
                .zip(batch.columns())
                .any(|(field, column)| !field.is_nullable() && column.null_count() != 0)
        {
            return Err(ArrowConversionError::InvalidSchema);
        }
        validate_batch_metadata(&schema_ref, &batch)?;
        if schema_ref.name() == FEATURE_LABEL_SCHEMA_NAME {
            validate_feature_label_batch(&batch)?;
        }
        Ok(Self { schema_ref, batch })
    }

    /// Recovers and validates the complete identity retained in Arrow schema metadata.
    pub fn try_from_record_batch(batch: RecordBatch) -> Result<Self, ArrowConversionError> {
        let schema_ref = schema_ref_from_metadata(batch.schema().as_ref())?;
        Self::try_new(schema_ref, batch)
    }

    /// Returns the exact registered dataset-schema identity.
    pub const fn schema_ref(&self) -> &DatasetSchemaRef {
        &self.schema_ref
    }

    /// Returns the immutable validated Arrow batch.
    pub const fn record_batch(&self) -> &RecordBatch {
        &self.batch
    }
}

fn validate_batch_metadata(
    schema_ref: &DatasetSchemaRef,
    batch: &RecordBatch,
) -> Result<(), ArrowConversionError> {
    let batch_schema = batch.schema();
    let metadata = batch_schema.metadata();
    let dynamic_keys: &[&str] = match schema_ref.name() {
        RESEARCH_SCHEMA_NAME => &[DATASET_KEY, REQUEST_DIGEST_KEY],
        FEATURE_LABEL_SCHEMA_NAME => &[
            DATASET_KEY,
            BUILD_DIGEST_KEY,
            UNIVERSE_DIGEST_KEY,
            POLICY_DIGEST_KEY,
        ],
        _ => return Err(ArrowConversionError::UnexpectedDatasetSchema),
    };
    let stable_count = DatasetSchemaRegistry::local()
        .resolve(schema_ref)?
        .metadata()
        .len();
    let expected_count = stable_count
        .checked_add(dynamic_keys.len())
        .ok_or(ArrowConversionError::InvalidSchemaMetadata)?;
    if metadata.len() != expected_count
        || dynamic_keys.iter().any(|key| !metadata.contains_key(*key))
    {
        return Err(ArrowConversionError::InvalidSchemaMetadata);
    }
    SourceIdentifier::try_from(
        metadata
            .get(DATASET_KEY)
            .ok_or(ArrowConversionError::InvalidSchemaMetadata)?
            .as_str(),
    )
    .map_err(|_| ArrowConversionError::InvalidSchemaMetadata)?;
    for key in dynamic_keys
        .iter()
        .copied()
        .filter(|key| *key != DATASET_KEY)
    {
        if metadata
            .get(key)
            .and_then(|value| decode_hex(value))
            .is_none()
        {
            return Err(ArrowConversionError::InvalidSchemaMetadata);
        }
    }
    Ok(())
}

fn validate_feature_label_batch(batch: &RecordBatch) -> Result<(), ArrowConversionError> {
    let fixed = |name| {
        batch
            .column_by_name(name)
            .and_then(|array| array.as_any().downcast_ref::<FixedSizeBinaryArray>())
            .ok_or(ArrowConversionError::InvalidSchema)
    };
    let example_ids = fixed("example_id")?;
    let instrument_ids = fixed("instrument_id")?;
    let names = fixed("component_name")?;
    let splits = batch
        .column_by_name("split")
        .and_then(|array| array.as_any().downcast_ref::<UInt8Array>())
        .ok_or(ArrowConversionError::InvalidSchema)?;
    let kinds = batch
        .column_by_name("component_kind")
        .and_then(|array| array.as_any().downcast_ref::<UInt8Array>())
        .ok_or(ArrowConversionError::InvalidSchema)?;
    let versions = batch
        .column_by_name("component_version")
        .and_then(|array| array.as_any().downcast_ref::<UInt32Array>())
        .ok_or(ArrowConversionError::InvalidSchema)?;
    let float_values = batch
        .column_by_name("value_f64")
        .and_then(|array| array.as_any().downcast_ref::<Float64Array>())
        .ok_or(ArrowConversionError::InvalidSchema)?;
    let decimal_values = batch
        .column_by_name("value_decimal_mantissa")
        .and_then(|array| array.as_any().downcast_ref::<Decimal128Array>())
        .ok_or(ArrowConversionError::InvalidSchema)?;
    let decimal_scales = batch
        .column_by_name("value_decimal_scale")
        .and_then(|array| array.as_any().downcast_ref::<UInt8Array>())
        .ok_or(ArrowConversionError::InvalidSchema)?;
    let units = fixed("unit")?;
    let currencies = fixed("currency")?;
    let missing = fixed("missing_reason")?;
    let lineages = batch
        .column_by_name("lineage_sha256")
        .and_then(|array| array.as_any().downcast_ref::<FixedSizeBinaryArray>())
        .ok_or(ArrowConversionError::InvalidSchema)?;

    for row in 0..batch.num_rows() {
        let example_id = padded_text(example_ids, row)?;
        let component_name = padded_text(names, row)?;
        let instrument_is_canonical = Uuid::from_slice(instrument_ids.value(row))
            .ok()
            .and_then(|value| InstrumentId::try_from(value).ok())
            .is_some();
        let has_float = !float_values.is_null(row);
        let has_decimal = !decimal_values.is_null(row);
        let has_missing = !missing.is_null(row);
        let selected_values = usize::from(has_float)
            .checked_add(usize::from(has_decimal))
            .and_then(|count| count.checked_add(usize::from(has_missing)))
            .ok_or(ArrowConversionError::InvalidFeatureLabelRow)?;
        let currency_is_canonical = currencies.is_null(row) || {
            padded_text(currencies, row).is_ok_and(|value| {
                Currency::try_from(value).is_ok_and(|currency| currency.as_str() == value)
            })
        };
        let unit_is_canonical =
            units.is_null(row) || padded_text(units, row).is_ok_and(canonical_unit);
        let missing_is_canonical = !has_missing
            || padded_text(missing, row).is_ok_and(|value| canonical_identifier(value, 256));
        if !canonical_identifier(example_id, 256)
            || !instrument_is_canonical
            || !canonical_identifier(component_name, 256)
            || !matches!(splits.value(row), 1..=3)
            || !matches!(kinds.value(row), 1..=2)
            || versions.value(row) == 0
            || decimal_values.is_null(row) != decimal_scales.is_null(row)
            || (!decimal_scales.is_null(row) && decimal_scales.value(row) > 28)
            || selected_values != 1
            || (has_float && !float_values.value(row).is_finite())
            || !unit_is_canonical
            || !currency_is_canonical
            || !missing_is_canonical
            || lineages.value_length() != 32
        {
            return Err(ArrowConversionError::InvalidFeatureLabelRow);
        }
    }
    Ok(())
}

fn padded_text(array: &FixedSizeBinaryArray, row: usize) -> Result<&str, ArrowConversionError> {
    let bytes = array.value(row);
    let end = bytes
        .iter()
        .position(|byte| *byte == 0)
        .unwrap_or(bytes.len());
    if end == 0 || bytes[end..].iter().any(|byte| *byte != 0) {
        return Err(ArrowConversionError::InvalidFeatureLabelRow);
    }
    std::str::from_utf8(&bytes[..end]).map_err(|_| ArrowConversionError::InvalidFeatureLabelRow)
}

fn canonical_identifier(value: &str, max_bytes: usize) -> bool {
    !value.is_empty()
        && value.len() <= max_bytes
        && value
            .as_bytes()
            .first()
            .is_some_and(u8::is_ascii_alphanumeric)
        && value
            .as_bytes()
            .last()
            .is_some_and(u8::is_ascii_alphanumeric)
        && value.bytes().all(|byte| {
            byte.is_ascii_lowercase()
                || byte.is_ascii_digit()
                || matches!(byte, b'-' | b'_' | b'.' | b':' | b'/')
        })
}

fn canonical_unit(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 32
        && value.bytes().all(|byte| {
            byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'/' | b'%')
        })
}
