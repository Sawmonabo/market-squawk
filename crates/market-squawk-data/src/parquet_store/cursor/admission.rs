//! Conservative decoder admission and page-index bounds for requested cursor batches.

use arrow::datatypes::{DataType, SchemaRef};
use parquet::{basic::Encoding, file::metadata::ParquetMetaData};
use tokio_util::sync::CancellationToken;

use super::ParquetStoreError;

// The sync File reader fetches compressed pages, not complete column chunks. Public
// Parquet metadata does not expose uncompressed page-header sizes before decoding;
// retain the conservative chunk bound for decoder scratch. Offset-index decoded byte
// counts can, however, bound the pages intersecting each requested output batch.
pub(super) fn admit_offset_index(
    metadata: &ParquetMetaData,
    data_end: u64,
    budget: usize,
) -> Result<(), ParquetStoreError> {
    let mut first = data_end;
    let mut last = 0;
    for column in metadata
        .row_groups()
        .iter()
        .flat_map(|group| group.columns())
    {
        match (column.offset_index_offset(), column.offset_index_length()) {
            (Some(offset), Some(length)) => {
                let offset =
                    u64::try_from(offset).map_err(|_| ParquetStoreError::ObjectMetadataMismatch)?;
                let length =
                    u64::try_from(length).map_err(|_| ParquetStoreError::ObjectMetadataMismatch)?;
                let end = offset
                    .checked_add(length)
                    .ok_or(ParquetStoreError::SizeOverflow)?;
                if offset < 4 || length == 0 || end > data_end {
                    return Err(ParquetStoreError::ObjectMetadataMismatch);
                }
                first = first.min(offset);
                last = last.max(end);
            }
            (None, None) => {}
            _ => return Err(ParquetStoreError::ObjectMetadataMismatch),
        }
    }
    let span =
        usize::try_from(last.saturating_sub(first)).map_err(|_| ParquetStoreError::SizeOverflow)?;
    // Keep the same conservative serialized-to-resident allowance as footer admission.
    let working = span
        .checked_mul(16)
        .and_then(|bytes| bytes.checked_add(metadata.memory_size()))
        .ok_or(ParquetStoreError::SizeOverflow)?;
    if working > budget {
        return Err(ParquetStoreError::ReadLimitExceeded);
    }
    Ok(())
}

#[derive(Default)]
struct BatchPageBytes {
    last_batch: Option<usize>,
    current: usize,
    maximum: usize,
}

impl BatchPageBytes {
    // A page's complete decoded bytes bound any subset of its rows. Sweep the
    // batches that intersect each page without allocating a per-row/per-batch index.
    fn include(
        &mut self,
        first_row: usize,
        end_row: usize,
        bytes: usize,
        batch_rows: usize,
        start_row: usize,
    ) -> Result<(), ParquetStoreError> {
        if end_row <= start_row {
            return Ok(());
        }
        let first_batch = first_row.saturating_sub(start_row) / batch_rows;
        let last_batch = (end_row - 1 - start_row) / batch_rows;
        if self.last_batch != Some(first_batch) {
            self.current = 0;
        }
        self.current = self
            .current
            .checked_add(bytes)
            .ok_or(ParquetStoreError::SizeOverflow)?;
        self.maximum = self.maximum.max(self.current);
        if last_batch != first_batch {
            self.current = bytes;
        }
        self.last_batch = Some(last_batch);
        Ok(())
    }
}

pub(super) fn admit_cursor_working_set(
    metadata: &ParquetMetaData,
    schema: &SchemaRef,
    projection: Option<&[usize]>,
    requested_rows: usize,
    start_row: usize,
    budget: usize,
    cancellation: &CancellationToken,
) -> Result<(), ParquetStoreError> {
    let total_rows = usize::try_from(metadata.file_metadata().num_rows())
        .map_err(|_| ParquetStoreError::SizeOverflow)?;
    let batch_rows = requested_rows.min(total_rows);
    if batch_rows == 0
        || metadata
            .row_groups()
            .iter()
            .any(|group| group.num_columns() != schema.fields().len())
    {
        return Err(ParquetStoreError::ObjectMetadataMismatch);
    }
    // The page iterators clone the current offset indexes from retained metadata.
    let mut working = metadata
        .memory_size()
        .checked_mul(2)
        .and_then(|bytes| bytes.checked_add(64 * 1024))
        .ok_or(ParquetStoreError::SizeOverflow)?;
    for (column_index, field) in schema.fields().iter().enumerate() {
        if projection.is_some_and(|columns| columns.binary_search(&column_index).is_err()) {
            continue;
        }
        let width = match field.data_type() {
            DataType::Boolean | DataType::Int8 | DataType::UInt8 => Some(1),
            DataType::Int16 | DataType::UInt16 => Some(2),
            DataType::Int32 | DataType::UInt32 | DataType::Date32 | DataType::Float32 => Some(4),
            DataType::Int64
            | DataType::UInt64
            | DataType::Float64
            | DataType::Date64
            | DataType::Timestamp(_, _) => Some(8),
            DataType::Decimal128(_, _) => Some(16),
            DataType::FixedSizeBinary(width) => {
                Some(usize::try_from(*width).map_err(|_| ParquetStoreError::SizeOverflow)?)
            }
            DataType::Utf8 | DataType::Binary | DataType::LargeUtf8 | DataType::LargeBinary => None,
            _ => return Err(ParquetStoreError::ReadLimitExceeded),
        };
        let mut output = BatchPageBytes::default();
        let mut group_start = 0_usize;
        let mut previous_scratch = 0_usize;
        let mut decoder = 0_usize;
        for (group_index, group) in metadata.row_groups().iter().enumerate() {
            if cancellation.is_cancelled() {
                return Err(ParquetStoreError::Cancelled);
            }
            let rows =
                usize::try_from(group.num_rows()).map_err(|_| ParquetStoreError::SizeOverflow)?;
            let group_end = group_start
                .checked_add(rows)
                .ok_or(ParquetStoreError::SizeOverflow)?;
            let column = group.column(column_index);
            let uncompressed = usize::try_from(column.uncompressed_size())
                .map_err(|_| ParquetStoreError::SizeOverflow)?;
            let compressed = usize::try_from(column.compressed_size())
                .map_err(|_| ParquetStoreError::SizeOverflow)?;
            // Fixed-width dictionary decoding retains both its original bytes and the
            // decoded value vector while installing the dictionary.
            let scratch = if width.is_some() && column.dictionary_page_offset().is_some() {
                uncompressed
                    .checked_mul(2)
                    .ok_or(ParquetStoreError::SizeOverflow)?
            } else {
                uncompressed
            };
            // A decoder replacement can temporarily retain the preceding group's last
            // page while constructing the next. Within a group its total bounds both.
            decoder = decoder.max(
                compressed
                    .checked_add(scratch)
                    .and_then(|bytes| bytes.checked_add(previous_scratch))
                    .ok_or(ParquetStoreError::SizeOverflow)?,
            );
            previous_scratch = scratch;
            if width.is_none() {
                let plain = column
                    .encodings()
                    .all(|encoding| matches!(encoding, Encoding::PLAIN | Encoding::RLE));
                let index = metadata
                    .offset_index()
                    .and_then(|groups| groups.get(group_index))
                    .and_then(|columns| columns.get(column_index));
                if let Some((index, sizes)) = index.and_then(|index| {
                    plain.then_some(index).and_then(|index| {
                        index
                            .unencoded_byte_array_data_bytes()
                            .map(|sizes| (index, sizes))
                    })
                }) {
                    let pages = index.page_locations();
                    if pages.len() != sizes.len() || (rows > 0 && pages.is_empty()) {
                        return Err(ParquetStoreError::ObjectMetadataMismatch);
                    }
                    let mut sum = 0_usize;
                    for (page_index, (page, bytes)) in pages.iter().zip(sizes).enumerate() {
                        if cancellation.is_cancelled() {
                            return Err(ParquetStoreError::Cancelled);
                        }
                        let first = usize::try_from(page.first_row_index)
                            .map_err(|_| ParquetStoreError::ObjectMetadataMismatch)?;
                        let end = pages
                            .get(page_index + 1)
                            .map_or(Ok(rows), |next| usize::try_from(next.first_row_index))
                            .map_err(|_| ParquetStoreError::ObjectMetadataMismatch)?;
                        if (page_index == 0 && first != 0) || first >= end || end > rows {
                            return Err(ParquetStoreError::ObjectMetadataMismatch);
                        }
                        let bytes = usize::try_from(*bytes)
                            .map_err(|_| ParquetStoreError::ObjectMetadataMismatch)?;
                        sum = sum
                            .checked_add(bytes)
                            .ok_or(ParquetStoreError::SizeOverflow)?;
                        // The plain decoder's capacity estimate includes each 4-byte
                        // value length, even though those prefixes are not Arrow data.
                        let reserved = (end - first)
                            .checked_mul(4)
                            .and_then(|prefixes| bytes.checked_add(prefixes))
                            .ok_or(ParquetStoreError::SizeOverflow)?;
                        output.include(
                            group_start + first,
                            group_start + end,
                            reserved,
                            batch_rows,
                            start_row,
                        )?;
                    }
                    if column
                        .unencoded_byte_array_data_bytes()
                        .is_some_and(|bytes| usize::try_from(bytes).ok() != Some(sum))
                    {
                        return Err(ParquetStoreError::ObjectMetadataMismatch);
                    }
                } else {
                    // Missing size indexes and expanding encodings retain the prior
                    // conservative bound; never infer uniform variable-width rows.
                    let bytes = if plain {
                        Some(uncompressed)
                    } else {
                        rows.checked_mul(uncompressed)
                    }
                    .ok_or(ParquetStoreError::SizeOverflow)?;
                    if !plain {
                        decoder = decoder.max(
                            compressed
                                .checked_add(uncompressed)
                                .and_then(|scratch| {
                                    bytes
                                        .checked_mul(2)
                                        .and_then(|values| scratch.checked_add(values))
                                })
                                .ok_or(ParquetStoreError::SizeOverflow)?,
                        );
                    }
                    output.include(group_start, group_end, bytes, batch_rows, start_row)?;
                }
            }
            group_start = group_end;
        }
        if group_start != total_rows {
            return Err(ParquetStoreError::ObjectMetadataMismatch);
        }
        let values = match width {
            Some(width) => batch_rows.checked_mul(width),
            None => batch_rows
                .checked_add(1)
                .and_then(|rows| rows.checked_mul(8))
                .and_then(|offsets| offsets.checked_add(output.maximum)),
        }
        .ok_or(ParquetStoreError::SizeOverflow)?;
        working = working.checked_add(decoder)
            .and_then(|bytes| values.checked_mul(2).and_then(|values| bytes.checked_add(values)))
            // Definition/repetition buffers and validity accompany requested rows.
            .and_then(|bytes| batch_rows.checked_mul(8).and_then(|levels| bytes.checked_add(levels)))
            .ok_or(ParquetStoreError::SizeOverflow)?;
        if working > budget {
            return Err(ParquetStoreError::ReadLimitExceeded);
        }
    }
    Ok(())
}
