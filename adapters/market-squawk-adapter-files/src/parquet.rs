//! Bounded Parquet metadata validation and flat Arrow row extraction.

use std::collections::{BTreeMap, BTreeSet};

use arrow::array::{
    Array, Decimal128Array, Int8Array, Int16Array, Int32Array, Int64Array, LargeStringArray,
    StringArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
};
use arrow::datatypes::DataType;
use bytes::Bytes;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use crate::{CellValue, FileAdapterError, ParseBudget, ParsedRow, ParserLimit};

const PARQUET_MAGIC: &[u8; 4] = b"PAR1";
const METADATA_BYTES_PER_CONFIGURED_COLUMN: usize = 1_024;
const ARRAY_FIXED_BYTES_PER_COLUMN: usize = 4_096;
const ARRAY_BYTES_PER_CELL: usize = 64;
const MAXIMUM_BATCH_ROWS: usize = 256;

#[derive(Clone, Copy, Debug)]
struct ParquetLayout {
    logical_bytes: usize,
    columns: usize,
}

pub(crate) fn parse(
    bytes: &[u8],
    budget: &mut ParseBudget<'_>,
) -> Result<Vec<ParsedRow>, FileAdapterError> {
    let footer_bytes = validate_footer(bytes, budget)?;
    let metadata_bound = budget
        .limits
        .input
        .max_columns
        .checked_mul(METADATA_BYTES_PER_CONFIGURED_COLUMN)
        .and_then(|bytes| bytes.checked_add(footer_bytes))
        .ok_or(FileAdapterError::LimitExceeded(ParserLimit::DecodedBytes))?;
    // The builder parses Thrift metadata and constructs schema objects before it can expose the
    // validated metadata. The validated footer plus a closed per-column structural allowance is
    // admitted before builder construction.
    budget.pre_admit_dynamic_bytes(metadata_bound)?;
    budget.allocation_bytes(bytes.len())?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(Bytes::copy_from_slice(bytes))
        .map_err(|_| FileAdapterError::UnsafeParquet)?;
    let layout = validate_metadata(&builder, budget)?;
    let schema = builder.schema().clone();
    let mut names = BTreeSet::new();
    for field in schema.fields() {
        validate_flat_data_type(field.data_type())?;
        budget.text(field.name().len())?;
        let name = budget.owned_text(field.name())?;
        budget.set_entry::<String>()?;
        if !names.insert(name) {
            return Err(FileAdapterError::DuplicateField);
        }
    }

    let (batch_size, batch_allocation_bound) = decoded_batch_policy(layout, budget)?;
    let limit = budget
        .row_limit()
        .checked_add(1)
        .ok_or(FileAdapterError::LimitExceeded(ParserLimit::Records))?;
    let mut reader = builder
        .with_batch_size(batch_size)
        .with_limit(limit)
        .build()
        .map_err(|_| FileAdapterError::UnsafeParquet)?;
    budget.checkpoint()?;
    let mut rows = Vec::new();
    loop {
        budget.checkpoint()?;
        // Iterator advancement may materialize a complete Arrow batch before returning control.
        // The closed schema/logical bound is checked against the remaining retained budget first.
        budget.ensure_dynamic_bytes(batch_allocation_bound)?;
        let Some(batch) = reader.next() else {
            break;
        };
        budget.checkpoint()?;
        let batch = batch.map_err(|_| FileAdapterError::UnsafeParquet)?;
        if batch.num_columns() != schema.fields().len() {
            return Err(FileAdapterError::UnsafeParquet);
        }
        let actual_batch_bytes = batch.get_array_memory_size();
        if actual_batch_bytes > batch_allocation_bound {
            return Err(FileAdapterError::LimitExceeded(ParserLimit::DecodedBytes));
        }
        budget.allocation_bytes(actual_batch_bytes)?;
        for row_index in 0..batch.num_rows() {
            budget.record()?;
            let mut fields = BTreeMap::new();
            for (field, array) in schema.fields().iter().zip(batch.columns()) {
                let value = value_at(array.as_ref(), row_index, budget)?;
                let name = budget.owned_text(field.name())?;
                budget.map_entry::<String, CellValue>()?;
                if fields.insert(name, value).is_some() {
                    return Err(FileAdapterError::DuplicateField);
                }
            }
            budget.fields(fields.len())?;
            let row = ParsedRow::try_new(fields, budget)?;
            budget.reserve_vec_slot(&mut rows)?;
            rows.push(row);
        }
    }
    Ok(rows)
}

fn validate_flat_data_type(data_type: &DataType) -> Result<(), FileAdapterError> {
    if matches!(
        data_type,
        DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Decimal128(_, _)
            | DataType::Null
            | DataType::Float16
            | DataType::Float32
            | DataType::Float64
            | DataType::Boolean
            | DataType::Binary
            | DataType::LargeBinary
            | DataType::BinaryView
            | DataType::Utf8View
    ) {
        Ok(())
    } else {
        Err(FileAdapterError::UnsafeParquet)
    }
}

fn validate_footer(bytes: &[u8], budget: &ParseBudget<'_>) -> Result<usize, FileAdapterError> {
    if bytes.len() < 12 || bytes.get(..4) != Some(PARQUET_MAGIC.as_slice()) {
        return Err(FileAdapterError::UnsafeParquet);
    }
    let footer_start = bytes
        .len()
        .checked_sub(8)
        .ok_or(FileAdapterError::UnsafeParquet)?;
    if bytes.get(footer_start + 4..) != Some(PARQUET_MAGIC.as_slice()) {
        return Err(FileAdapterError::UnsafeParquet);
    }
    let footer_length = u32::from_le_bytes(
        bytes
            .get(footer_start..footer_start + 4)
            .ok_or(FileAdapterError::UnsafeParquet)?
            .try_into()
            .map_err(|_| FileAdapterError::UnsafeParquet)?,
    );
    let footer_length = usize::try_from(footer_length)
        .map_err(|_| FileAdapterError::LimitExceeded(ParserLimit::MetadataBytes))?;
    if footer_length > budget.limits.input.max_text_bytes
        || footer_length
            .checked_add(12)
            .is_none_or(|minimum| minimum > bytes.len())
    {
        return Err(if footer_length > budget.limits.input.max_text_bytes {
            FileAdapterError::LimitExceeded(ParserLimit::MetadataBytes)
        } else {
            FileAdapterError::UnsafeParquet
        });
    }
    Ok(footer_length)
}

fn validate_metadata(
    builder: &ParquetRecordBatchReaderBuilder<Bytes>,
    budget: &mut ParseBudget<'_>,
) -> Result<ParquetLayout, FileAdapterError> {
    let metadata = builder.metadata();
    if metadata.num_row_groups() > budget.limits.input.max_row_groups {
        return Err(FileAdapterError::LimitExceeded(ParserLimit::RowGroups));
    }
    let physical_columns = metadata.file_metadata().schema_descr().num_columns();
    budget.columns(physical_columns)?;
    budget.columns(builder.schema().fields().len())?;
    let rows = usize::try_from(metadata.file_metadata().num_rows())
        .map_err(|_| FileAdapterError::UnsafeParquet)?;
    if rows > budget.row_limit() {
        return Err(budget.row_limit_error());
    }
    let mut logical_bytes = 0_u64;
    for group in metadata.row_groups() {
        let bytes =
            u64::try_from(group.total_byte_size()).map_err(|_| FileAdapterError::UnsafeParquet)?;
        logical_bytes = logical_bytes
            .checked_add(bytes)
            .ok_or(FileAdapterError::LimitExceeded(
                ParserLimit::DecompressedBytes,
            ))?;
    }
    if logical_bytes > budget.limits.input.max_decompressed_bytes {
        return Err(FileAdapterError::LimitExceeded(
            ParserLimit::DecompressedBytes,
        ));
    }
    Ok(ParquetLayout {
        logical_bytes: usize::try_from(logical_bytes)
            .map_err(|_| FileAdapterError::LimitExceeded(ParserLimit::DecompressedBytes))?,
        columns: builder.schema().fields().len(),
    })
}

fn decoded_batch_policy(
    layout: ParquetLayout,
    budget: &ParseBudget<'_>,
) -> Result<(usize, usize), FileAdapterError> {
    let fixed_bytes = layout
        .columns
        .checked_mul(ARRAY_FIXED_BYTES_PER_COLUMN)
        .ok_or(FileAdapterError::LimitExceeded(ParserLimit::DecodedBytes))?;
    // A selected variable-width value must be represented at least once in the validated
    // uncompressed column chunks. Multiplying the aggregate logical bytes by batch rows covers
    // worst-case dictionary expansion; the per-cell term covers offsets, validity, and fixed
    // primitive values.
    let per_row_bytes = layout
        .columns
        .checked_mul(ARRAY_BYTES_PER_CELL)
        .and_then(|bytes| bytes.checked_add(layout.logical_bytes.max(1)))
        .ok_or(FileAdapterError::LimitExceeded(ParserLimit::DecodedBytes))?;
    let available = budget.remaining_retained_bytes()? / 2;
    let row_capacity = available
        .checked_sub(fixed_bytes)
        .ok_or(FileAdapterError::LimitExceeded(ParserLimit::DecodedBytes))?
        / per_row_bytes;
    let batch_size = row_capacity.min(budget.row_limit()).min(MAXIMUM_BATCH_ROWS);
    if batch_size == 0 {
        return Err(FileAdapterError::LimitExceeded(ParserLimit::DecodedBytes));
    }
    let allocation_bound = per_row_bytes
        .checked_mul(batch_size)
        .and_then(|bytes| bytes.checked_add(fixed_bytes))
        .ok_or(FileAdapterError::LimitExceeded(ParserLimit::DecodedBytes))?;
    budget.ensure_dynamic_bytes(allocation_bound)?;
    Ok((batch_size, allocation_bound))
}

fn value_at(
    array: &dyn Array,
    row: usize,
    budget: &mut ParseBudget<'_>,
) -> Result<CellValue, FileAdapterError> {
    budget.cell()?;
    if array.is_null(row) {
        return Ok(CellValue::Null);
    }
    let text = match array.data_type() {
        DataType::Utf8 => budget.owned_text(downcast::<StringArray>(array)?.value(row))?,
        DataType::LargeUtf8 => {
            budget.owned_text(downcast::<LargeStringArray>(array)?.value(row))?
        }
        DataType::Int8 => budget.formatted_text(
            4,
            format_args!("{}", downcast::<Int8Array>(array)?.value(row)),
        )?,
        DataType::Int16 => budget.formatted_text(
            6,
            format_args!("{}", downcast::<Int16Array>(array)?.value(row)),
        )?,
        DataType::Int32 => budget.formatted_text(
            11,
            format_args!("{}", downcast::<Int32Array>(array)?.value(row)),
        )?,
        DataType::Int64 => budget.formatted_text(
            20,
            format_args!("{}", downcast::<Int64Array>(array)?.value(row)),
        )?,
        DataType::UInt8 => budget.formatted_text(
            3,
            format_args!("{}", downcast::<UInt8Array>(array)?.value(row)),
        )?,
        DataType::UInt16 => budget.formatted_text(
            5,
            format_args!("{}", downcast::<UInt16Array>(array)?.value(row)),
        )?,
        DataType::UInt32 => budget.formatted_text(
            10,
            format_args!("{}", downcast::<UInt32Array>(array)?.value(row)),
        )?,
        DataType::UInt64 => budget.formatted_text(
            20,
            format_args!("{}", downcast::<UInt64Array>(array)?.value(row)),
        )?,
        DataType::Decimal128(_, scale) => format_decimal(
            downcast::<Decimal128Array>(array)?.value(row),
            *scale,
            budget,
        )?,
        DataType::Null => return Ok(CellValue::Null),
        DataType::Float16
        | DataType::Float32
        | DataType::Float64
        | DataType::Boolean
        | DataType::Binary
        | DataType::LargeBinary
        | DataType::BinaryView
        | DataType::Utf8View => return Ok(CellValue::Unsupported),
        _ => return Err(FileAdapterError::UnsafeParquet),
    };
    Ok(CellValue::Text(text))
}

fn downcast<T: 'static>(array: &dyn Array) -> Result<&T, FileAdapterError> {
    array
        .as_any()
        .downcast_ref::<T>()
        .ok_or(FileAdapterError::UnsafeParquet)
}

fn format_decimal(
    value: i128,
    scale: i8,
    budget: &mut ParseBudget<'_>,
) -> Result<String, FileAdapterError> {
    let negative = value.is_negative();
    let digits = budget.formatted_text(39, format_args!("{}", value.unsigned_abs()))?;
    let sign_bytes = usize::from(negative);
    let output_bytes = if scale > 0 {
        let scale = usize::try_from(scale).map_err(|_| FileAdapterError::UnsafeParquet)?;
        if digits.len() <= scale {
            sign_bytes
                .checked_add(2)
                .and_then(|bytes| bytes.checked_add(scale))
        } else {
            sign_bytes
                .checked_add(digits.len())
                .and_then(|bytes| bytes.checked_add(1))
        }
    } else if scale < 0 {
        sign_bytes
            .checked_add(digits.len())
            .and_then(|bytes| bytes.checked_add(usize::from(scale.unsigned_abs())))
    } else {
        sign_bytes.checked_add(digits.len())
    }
    .ok_or(FileAdapterError::LimitExceeded(ParserLimit::TextBytes))?;
    budget.text(output_bytes)?;
    let mut output = budget.string_with_capacity(output_bytes)?;
    if negative {
        output.push('-');
    }
    if scale > 0 {
        let scale = usize::try_from(scale).map_err(|_| FileAdapterError::UnsafeParquet)?;
        if digits.len() <= scale {
            output.push_str("0.");
            output.extend(std::iter::repeat_n('0', scale - digits.len()));
            output.push_str(&digits);
        } else {
            let decimal = digits
                .len()
                .checked_sub(scale)
                .ok_or(FileAdapterError::UnsafeParquet)?;
            let (whole, fractional) = digits.split_at(decimal);
            output.push_str(whole);
            output.push('.');
            output.push_str(fractional);
        }
    } else if scale < 0 {
        output.push_str(&digits);
        output.extend(std::iter::repeat_n('0', usize::from(scale.unsigned_abs())));
    } else {
        output.push_str(&digits);
    }
    if output.len() != output_bytes {
        return Err(FileAdapterError::UnsafeParquet);
    }
    Ok(output)
}
