//! Network-free streaming XML row extraction with all DTD/entity surfaces denied.

use std::borrow::Cow;
use std::collections::BTreeMap;

use quick_xml::Reader;
use quick_xml::events::Event;

use crate::{CellValue, FileAdapterError, ParseBudget, ParsedRow, ParserLimit};

pub(crate) fn parse(
    bytes: &[u8],
    record_element: &str,
    budget: &mut ParseBudget<'_>,
) -> Result<Vec<ParsedRow>, FileAdapterError> {
    budget
        .decompressed(u64::try_from(bytes.len()).map_err(|_| FileAdapterError::InvalidRecord)?)?;
    let mut reader = Reader::from_reader(bytes);
    reader.config_mut().check_end_names = true;
    reader.config_mut().expand_empty_elements = true;
    let mut buffer = Vec::new();
    let mut depth = 0_usize;
    let mut row_depth = None;
    let mut row = None::<BTreeMap<String, CellValue>>;
    let mut field = None::<(String, usize, String)>;
    let mut rows = Vec::new();
    let mut declaration_seen = false;
    let mut root_seen = false;
    let mut root_closed = false;

    loop {
        budget.checkpoint()?;
        match reader
            .read_event_into(&mut buffer)
            .map_err(|_| FileAdapterError::InvalidRecord)?
        {
            Event::Start(start) => {
                if root_closed || depth == 0 && root_seen {
                    return Err(FileAdapterError::InvalidRecord);
                }
                if depth == 0 {
                    root_seen = true;
                }
                depth = depth
                    .checked_add(1)
                    .ok_or(FileAdapterError::LimitExceeded(ParserLimit::NestingDepth))?;
                budget.depth(depth)?;
                if !start.attributes_raw().is_empty() {
                    return Err(FileAdapterError::InvalidRecord);
                }
                let qualified_name = start.name();
                let name = std::str::from_utf8(qualified_name.as_ref())
                    .map_err(|_| FileAdapterError::InvalidRecord)?;
                budget.text(name.len())?;
                match row_depth {
                    None if name == record_element => {
                        budget.record()?;
                        row_depth = Some(depth);
                        row = Some(BTreeMap::new());
                    }
                    Some(active_depth) if depth == active_depth.saturating_add(1) => {
                        if field.is_some() {
                            return Err(FileAdapterError::InvalidRecord);
                        }
                        let name = budget.owned_text(name)?;
                        field = Some((name, depth, String::new()));
                    }
                    Some(_) => return Err(FileAdapterError::InvalidRecord),
                    None => {}
                }
            }
            Event::End(end) => {
                let qualified_name = end.name();
                let name = std::str::from_utf8(qualified_name.as_ref())
                    .map_err(|_| FileAdapterError::InvalidRecord)?;
                if field.as_ref().is_some_and(|(field_name, field_depth, _)| {
                    *field_depth == depth && field_name == name
                }) {
                    let (field_name, _, value) =
                        field.take().ok_or(FileAdapterError::InvalidRecord)?;
                    budget.text(value.len())?;
                    let current = row.as_mut().ok_or(FileAdapterError::InvalidRecord)?;
                    budget.map_entry::<String, CellValue>()?;
                    if current.insert(field_name, CellValue::Text(value)).is_some() {
                        return Err(FileAdapterError::DuplicateField);
                    }
                } else if row_depth == Some(depth) && name == record_element {
                    let completed = row.take().ok_or(FileAdapterError::InvalidRecord)?;
                    budget.fields(completed.len())?;
                    let row = ParsedRow::try_new(completed, budget)?;
                    budget.reserve_vec_slot(&mut rows)?;
                    rows.push(row);
                    row_depth = None;
                } else if row_depth.is_some() && field.is_some() {
                    return Err(FileAdapterError::InvalidRecord);
                }
                if depth == 1 {
                    root_closed = true;
                }
                depth = depth
                    .checked_sub(1)
                    .ok_or(FileAdapterError::InvalidRecord)?;
            }
            Event::Text(text) => {
                admit_decoded_text(text.len(), budget)?;
                let text = text.decode().map_err(|_| FileAdapterError::InvalidRecord)?;
                if let Cow::Owned(value) = &text {
                    budget.string_allocation(value)?;
                }
                budget.text(text.len())?;
                if let Some((_, _, value)) = field.as_mut() {
                    budget.append_string(value, &text)?;
                    budget.text(value.len())?;
                } else if !text.chars().all(char::is_whitespace) {
                    return Err(FileAdapterError::InvalidRecord);
                }
            }
            Event::CData(text) => {
                admit_decoded_text(text.len(), budget)?;
                let text = text.decode().map_err(|_| FileAdapterError::InvalidRecord)?;
                if let Cow::Owned(value) = &text {
                    budget.string_allocation(value)?;
                }
                let (_, _, value) = field.as_mut().ok_or(FileAdapterError::InvalidRecord)?;
                budget.append_string(value, &text)?;
                budget.text(value.len())?;
            }
            Event::Decl(_) if depth == 0 && !declaration_seen && !root_seen => {
                declaration_seen = true;
            }
            Event::DocType(_) | Event::GeneralRef(_) | Event::PI(_) | Event::Comment(_) => {
                return Err(FileAdapterError::UnsafeXml);
            }
            Event::Decl(_) => return Err(FileAdapterError::InvalidRecord),
            Event::Eof => break,
            Event::Empty(_) => return Err(FileAdapterError::InvalidRecord),
        }
        buffer.clear();
    }
    if depth != 0
        || !root_seen
        || !root_closed
        || row_depth.is_some()
        || row.is_some()
        || field.is_some()
    {
        return Err(FileAdapterError::InvalidRecord);
    }
    Ok(rows)
}

fn admit_decoded_text(
    encoded_bytes: usize,
    budget: &ParseBudget<'_>,
) -> Result<(), FileAdapterError> {
    let decoded_bound = encoded_bytes
        .checked_mul(3)
        .ok_or(FileAdapterError::LimitExceeded(ParserLimit::DecodedBytes))?;
    budget.ensure_dynamic_bytes(decoded_bound)
}
