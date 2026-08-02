//! Strict UTF-8 CSV and TSV record extraction.

use std::collections::{BTreeMap, BTreeSet};

use csv::{ByteRecord, ReaderBuilder};

use crate::{CellValue, FileAdapterError, ParseBudget, ParsedRow};

pub(crate) fn parse(
    bytes: &[u8],
    delimiter: u8,
    budget: &mut ParseBudget<'_>,
) -> Result<Vec<ParsedRow>, FileAdapterError> {
    budget
        .decompressed(u64::try_from(bytes.len()).map_err(|_| FileAdapterError::InvalidRecord)?)?;
    // The csv reader owns a persistent header record and one reused data record. Their combined
    // logical contents cannot exceed the bounded source slice; the doubled admission covers
    // allocator growth before either ByteRecord can allocate.
    budget.pre_admit_dynamic_bytes(bytes.len())?;
    let mut reader = ReaderBuilder::new()
        .delimiter(delimiter)
        .has_headers(true)
        .flexible(false)
        .from_reader(bytes);
    let header_record = reader
        .byte_headers()
        .map_err(|_| FileAdapterError::InvalidRecord)?;
    budget.fields(header_record.len())?;
    budget.columns(header_record.len())?;
    if header_record.is_empty() {
        return Err(FileAdapterError::InvalidRecord);
    }
    let mut headers = budget.vec_with_capacity(header_record.len())?;
    let mut unique = BTreeSet::new();
    for header in header_record {
        budget.text(header.len())?;
        let header = std::str::from_utf8(header).map_err(|_| FileAdapterError::InvalidRecord)?;
        if header.is_empty() || unique.contains(header) {
            return Err(if header.is_empty() {
                FileAdapterError::InvalidRecord
            } else {
                FileAdapterError::DuplicateField
            });
        }
        budget.set_entry::<&str>()?;
        let _ = unique.insert(header);
        let header = budget.owned_text(header)?;
        headers.push(header);
    }

    let mut rows = Vec::new();
    let mut record = ByteRecord::new();
    loop {
        budget.checkpoint()?;
        let position = usize::try_from(reader.position().byte())
            .map_err(|_| FileAdapterError::InvalidRecord)?;
        let remaining = bytes
            .get(position..)
            .ok_or(FileAdapterError::InvalidRecord)?;
        if remaining.iter().all(|byte| matches!(*byte, b'\r' | b'\n')) {
            break;
        }
        budget.record()?;
        if !reader
            .read_byte_record(&mut record)
            .map_err(|_| FileAdapterError::InvalidRecord)?
        {
            break;
        }
        budget.fields(record.len())?;
        if record.len() != headers.len() {
            return Err(FileAdapterError::InvalidRecord);
        }
        let mut fields = BTreeMap::new();
        for (header, value) in headers.iter().zip(record.iter()) {
            budget.text(value.len())?;
            let value = std::str::from_utf8(value).map_err(|_| FileAdapterError::InvalidRecord)?;
            let value = budget.owned_text(value)?;
            let header = budget.owned_text(header)?;
            budget.map_entry::<String, CellValue>()?;
            fields.insert(header, CellValue::Text(value));
        }
        let row = ParsedRow::try_new(fields, budget)?;
        budget.reserve_vec_slot(&mut rows)?;
        rows.push(row);
    }
    Ok(rows)
}
