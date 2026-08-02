//! Strict flat worksheet-to-row decoding.

use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet};

use quick_xml::events::Event;

use super::xml::{
    DocumentState, SPREADSHEET_NAMESPACE, admit_decoded_text, attributes, end_local_name, enter,
    leave, local_name, next_event, require_whitespace, safe_reader,
};
use crate::{CellValue, FileAdapterError, FormulaPolicy, ParseBudget, ParsedRow, ParserLimit};

#[derive(Debug)]
struct RowBuilder {
    number: u64,
    cells: BTreeMap<usize, CellValue>,
}

#[derive(Debug)]
struct CellBuilder {
    column: usize,
    kind: Option<String>,
    value: String,
    has_value: bool,
    has_formula: bool,
    has_inline_container: bool,
    collecting_value: bool,
    collecting_formula: bool,
}

pub(super) fn parse(
    bytes: &[u8],
    shared_strings: &[String],
    formula_policy: FormulaPolicy,
    budget: &mut ParseBudget<'_>,
) -> Result<Vec<ParsedRow>, FileAdapterError> {
    let mut reader = safe_reader(bytes);
    let mut buffer = Vec::new();
    let mut depth = 0_usize;
    let mut elements = Vec::<String>::new();
    let mut sheet_data_seen = false;
    let mut in_sheet_data = false;
    let mut row = None::<RowBuilder>;
    let mut cell = None::<CellBuilder>;
    let mut headers = None::<Vec<String>>;
    let mut rows = Vec::new();
    let mut document = DocumentState::new("worksheet", SPREADSHEET_NAMESPACE, None);

    loop {
        budget.checkpoint()?;
        match next_event(&mut reader, &mut buffer)? {
            Event::Start(start) => {
                let attributes = attributes(&start, budget)?;
                document.start(&start, &attributes, depth)?;
                enter(&mut depth, budget)?;
                let name = local_name(&start, budget)?;
                let parent = elements.last().map(String::as_str);
                match name.as_str() {
                    "worksheet" if parent.is_none() && depth == 1 => {}
                    "sheetData" => {
                        if parent != Some("worksheet")
                            || depth != 2
                            || sheet_data_seen
                            || in_sheet_data
                        {
                            return Err(FileAdapterError::InvalidRecord);
                        }
                        sheet_data_seen = true;
                        in_sheet_data = true;
                    }
                    "row" => {
                        if parent != Some("sheetData") || depth != 3 || !in_sheet_data {
                            return Err(FileAdapterError::InvalidRecord);
                        }
                        if row.is_some() {
                            return Err(FileAdapterError::InvalidRecord);
                        }
                        if headers.is_some() {
                            budget.record()?;
                        }
                        let number = attributes
                            .get("r")
                            .ok_or(FileAdapterError::InvalidRecord)?
                            .parse::<u64>()
                            .map_err(|_| FileAdapterError::InvalidRecord)?;
                        row = Some(RowBuilder {
                            number,
                            cells: BTreeMap::new(),
                        });
                    }
                    "c" => {
                        if parent != Some("row") || depth != 4 || row.is_none() {
                            return Err(FileAdapterError::InvalidRecord);
                        }
                        if cell.is_some() {
                            return Err(FileAdapterError::InvalidRecord);
                        }
                        budget.cell()?;
                        let reference =
                            attributes.get("r").ok_or(FileAdapterError::InvalidRecord)?;
                        let number = row.as_ref().ok_or(FileAdapterError::InvalidRecord)?.number;
                        let kind = attributes
                            .get("t")
                            .map(|kind| budget.owned_text(kind))
                            .transpose()?;
                        cell = Some(CellBuilder {
                            column: column_index(reference, number)?,
                            kind,
                            value: String::new(),
                            has_value: false,
                            has_formula: false,
                            has_inline_container: false,
                            collecting_value: false,
                            collecting_formula: false,
                        });
                    }
                    "v" => {
                        if parent != Some("c") || depth != 5 {
                            return Err(FileAdapterError::InvalidRecord);
                        }
                        let active = cell.as_mut().ok_or(FileAdapterError::InvalidRecord)?;
                        if active.collecting_value
                            || active.has_value
                            || active.kind.as_deref() == Some("inlineStr")
                        {
                            return Err(FileAdapterError::InvalidRecord);
                        }
                        active.collecting_value = true;
                    }
                    "t" => {
                        if !matches!(parent, Some("is" | "r")) || !matches!(depth, 6 | 7) {
                            return Err(FileAdapterError::InvalidRecord);
                        }
                        let active = cell.as_mut().ok_or(FileAdapterError::InvalidRecord)?;
                        if active.collecting_value
                            || active.has_value
                            || active.kind.as_deref() != Some("inlineStr")
                        {
                            return Err(FileAdapterError::InvalidRecord);
                        }
                        active.collecting_value = true;
                    }
                    "f" => {
                        if parent != Some("c") || depth != 5 {
                            return Err(FileAdapterError::InvalidRecord);
                        }
                        if matches!(formula_policy, FormulaPolicy::Reject) {
                            return Err(FileAdapterError::UnsafeSpreadsheet);
                        }
                        let active = cell.as_mut().ok_or(FileAdapterError::InvalidRecord)?;
                        if active.has_formula {
                            return Err(FileAdapterError::InvalidRecord);
                        }
                        active.has_formula = true;
                        active.collecting_formula = true;
                    }
                    "is" => {
                        let active = cell.as_mut().ok_or(FileAdapterError::InvalidRecord)?;
                        if parent != Some("c")
                            || depth != 5
                            || active.kind.as_deref() != Some("inlineStr")
                            || active.has_value
                            || active.has_inline_container
                        {
                            return Err(FileAdapterError::InvalidRecord);
                        }
                        active.has_inline_container = true;
                    }
                    "r" => {
                        if parent != Some("is") || depth != 6 || cell.is_none() {
                            return Err(FileAdapterError::InvalidRecord);
                        }
                    }
                    _ if in_sheet_data => return Err(FileAdapterError::InvalidRecord),
                    _ => {}
                }
                budget.reserve_vec_slot(&mut elements)?;
                elements.push(name);
            }
            Event::End(end) => {
                let name = end_local_name(&end, budget)?;
                match name.as_str() {
                    "v" | "t" if cell.is_some() => {
                        let active = cell.as_mut().ok_or(FileAdapterError::InvalidRecord)?;
                        active.collecting_value = false;
                        active.has_value = true;
                    }
                    "f" if cell.is_some() => {
                        cell.as_mut()
                            .ok_or(FileAdapterError::InvalidRecord)?
                            .collecting_formula = false;
                    }
                    "c" => {
                        let completed = cell.take().ok_or(FileAdapterError::InvalidRecord)?;
                        let value = finish_cell(completed, shared_strings, budget)?;
                        let active = row.as_mut().ok_or(FileAdapterError::InvalidRecord)?;
                        budget.map_entry::<usize, CellValue>()?;
                        if active.cells.insert(value.0, value.1).is_some() {
                            return Err(FileAdapterError::DuplicateField);
                        }
                    }
                    "row" if in_sheet_data => {
                        let completed = row.take().ok_or(FileAdapterError::InvalidRecord)?;
                        if let Some(existing) = &headers {
                            let row = finish_data_row(completed, existing, budget)?;
                            budget.reserve_vec_slot(&mut rows)?;
                            rows.push(row);
                        } else {
                            headers = Some(finish_header_row(completed, budget)?);
                        }
                    }
                    "sheetData" => in_sheet_data = false,
                    _ => {}
                }
                document.end(&end, depth)?;
                let opened = elements.pop().ok_or(FileAdapterError::InvalidRecord)?;
                if opened != name {
                    return Err(FileAdapterError::InvalidRecord);
                }
                leave(&mut depth)?;
            }
            Event::Text(text) => {
                admit_decoded_text(text.len(), budget)?;
                let text = text.decode().map_err(|_| FileAdapterError::InvalidRecord)?;
                if let Cow::Owned(value) = &text {
                    budget.string_allocation(value)?;
                }
                if let Some(active) = cell.as_mut() {
                    if active.collecting_value {
                        budget.append_string(&mut active.value, &text)?;
                        budget.text(active.value.len())?;
                    } else if !active.collecting_formula {
                        require_whitespace(&text)?;
                    }
                } else {
                    require_whitespace(&text)?;
                }
            }
            Event::Decl(_) => document.declaration(depth)?,
            Event::Eof => break,
            Event::DocType(_) | Event::GeneralRef(_) | Event::PI(_) | Event::Comment(_) => {
                return Err(FileAdapterError::UnsafeSpreadsheet);
            }
            Event::CData(_) | Event::Empty(_) => {
                return Err(FileAdapterError::InvalidRecord);
            }
        }
        buffer.clear();
    }
    document.finish(depth)?;
    if !sheet_data_seen || in_sheet_data || row.is_some() || cell.is_some() || headers.is_none() {
        return Err(FileAdapterError::InvalidRecord);
    }
    Ok(rows)
}

fn finish_cell(
    cell: CellBuilder,
    shared_strings: &[String],
    budget: &mut ParseBudget<'_>,
) -> Result<(usize, CellValue), FileAdapterError> {
    if cell.collecting_value || cell.collecting_formula || cell.has_formula && !cell.has_value {
        return Err(FileAdapterError::UnsafeSpreadsheet);
    }
    let value = match cell.kind.as_deref() {
        Some("s") => {
            let index = cell
                .value
                .parse::<usize>()
                .map_err(|_| FileAdapterError::InvalidRecord)?;
            let value = shared_strings
                .get(index)
                .ok_or(FileAdapterError::InvalidRecord)?;
            let value = budget.owned_text(value)?;
            CellValue::Text(value)
        }
        Some("b" | "e" | "d") => CellValue::Unsupported,
        Some("inlineStr" | "str" | "n") | None if cell.has_value => CellValue::Text(cell.value),
        Some("inlineStr" | "str" | "n") | None => CellValue::Null,
        Some(_) => return Err(FileAdapterError::InvalidRecord),
    };
    Ok((cell.column, value))
}

fn finish_header_row(
    mut row: RowBuilder,
    budget: &mut ParseBudget<'_>,
) -> Result<Vec<String>, FileAdapterError> {
    let width = row
        .cells
        .last_key_value()
        .map(|(column, _)| column.saturating_add(1))
        .ok_or(FileAdapterError::InvalidRecord)?;
    budget.columns(width)?;
    budget.fields(width)?;
    let mut headers = budget.vec_with_capacity(width)?;
    let mut unique = BTreeSet::new();
    for column in 0..width {
        let value = row
            .cells
            .remove(&column)
            .ok_or(FileAdapterError::InvalidRecord)?;
        let CellValue::Text(header) = value else {
            return Err(FileAdapterError::InvalidRecord);
        };
        budget.text(header.len())?;
        let retained = budget.owned_text(&header)?;
        if header.is_empty() || unique.contains(&retained) {
            return Err(FileAdapterError::DuplicateField);
        }
        budget.set_entry::<String>()?;
        let _ = unique.insert(retained);
        headers.push(header);
    }
    Ok(headers)
}

fn finish_data_row(
    mut row: RowBuilder,
    headers: &[String],
    budget: &mut ParseBudget<'_>,
) -> Result<ParsedRow, FileAdapterError> {
    if row
        .cells
        .last_key_value()
        .is_some_and(|(column, _)| *column >= headers.len())
    {
        return Err(FileAdapterError::InvalidRecord);
    }
    let mut fields = BTreeMap::new();
    for (column, header) in headers.iter().enumerate() {
        let header = budget.owned_text(header)?;
        budget.map_entry::<String, CellValue>()?;
        fields.insert(header, row.cells.remove(&column).unwrap_or(CellValue::Null));
    }
    budget.fields(fields.len())?;
    ParsedRow::try_new(fields, budget)
}

fn column_index(reference: &str, row_number: u64) -> Result<usize, FileAdapterError> {
    let split = reference
        .find(|character: char| character.is_ascii_digit())
        .ok_or(FileAdapterError::InvalidRecord)?;
    let (column, row) = reference.split_at(split);
    if column.is_empty()
        || !column.bytes().all(|byte| byte.is_ascii_uppercase())
        || row.parse::<u64>().ok() != Some(row_number)
    {
        return Err(FileAdapterError::InvalidRecord);
    }
    let mut value = 0_usize;
    for byte in column.bytes() {
        value = value
            .checked_mul(26)
            .and_then(|value| value.checked_add(usize::from(byte - b'A') + 1))
            .ok_or(FileAdapterError::LimitExceeded(ParserLimit::Columns))?;
    }
    value.checked_sub(1).ok_or(FileAdapterError::InvalidRecord)
}
