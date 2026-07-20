//! Checked retained-memory and serialized-byte accounting helpers.

use std::error::Error as StdError;
use std::io::Write;
use std::mem::{size_of, size_of_val};

use arrow::datatypes::{Schema, SchemaRef};
use datafusion::error::DataFusionError;
use datafusion::execution::memory_pool::MemoryReservation;

use super::QueryError;
use super::source::PinnedRangeMemoryError;

#[derive(Debug, Default)]
pub(super) struct CountingWriter {
    pub(super) byte_count: u64,
}

impl Write for CountingWriter {
    fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
        let bytes = u64::try_from(buffer.len()).map_err(std::io::Error::other)?;
        self.byte_count = self
            .byte_count
            .checked_add(bytes)
            .ok_or_else(|| std::io::Error::other("IPC byte count overflow"))?;
        Ok(buffer.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

pub(super) fn schema_retained_bytes(schema: &SchemaRef) -> Result<usize, QueryError> {
    let fields = schema.fields().iter().try_fold(0_usize, |total, field| {
        total
            .checked_add(size_of_val(field.as_ref()))
            .and_then(|value| value.checked_add(field.name().len()))
            .ok_or(QueryError::SizeOverflow)
    })?;
    schema.metadata().iter().try_fold(
        size_of::<Schema>()
            .checked_add(fields)
            .ok_or(QueryError::SizeOverflow)?,
        |total, (key, value)| {
            total
                .checked_add(key.len())
                .and_then(|value_total| value_total.checked_add(value.len()))
                .ok_or(QueryError::SizeOverflow)
        },
    )
}

pub(super) fn reserve_memory(
    reservation: &MemoryReservation,
    additional: usize,
    limit: u64,
) -> Result<(), QueryError> {
    reservation
        .try_grow(additional)
        .map_err(|_| QueryError::MemoryLimitExceeded { limit })
}

pub(super) fn resize_memory(
    reservation: &MemoryReservation,
    size: usize,
    limit: u64,
) -> Result<(), QueryError> {
    reservation
        .try_resize(size)
        .map_err(|_| QueryError::MemoryLimitExceeded { limit })
}

pub(super) fn map_datafusion(error: DataFusionError, limit: u64) -> QueryError {
    if datafusion_memory_error(&error) {
        QueryError::MemoryLimitExceeded { limit }
    } else {
        QueryError::DataFusion(error)
    }
}

fn datafusion_memory_error(error: &DataFusionError) -> bool {
    if error_chain_has_pinned_memory_marker(error) {
        return true;
    }
    match error {
        DataFusionError::ResourcesExhausted(_) => true,
        DataFusionError::Context(_, source) => datafusion_memory_error(source),
        _ => false,
    }
}

fn error_chain_has_pinned_memory_marker(error: &(dyn StdError + 'static)) -> bool {
    let mut current = Some(error);
    while let Some(source) = current {
        if source.downcast_ref::<PinnedRangeMemoryError>().is_some() {
            return true;
        }
        current = source.source();
    }
    false
}

pub(super) fn valid_table_name(value: &str) -> bool {
    let mut bytes = value.bytes();
    bytes.next().is_some_and(|first| first.is_ascii_lowercase())
        && value.len() <= 64
        && bytes.all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
}
