//! Fail-closed XLSX package validation and flat worksheet extraction.

mod package;
mod worksheet;
mod xml;

use crate::{FileAdapterError, FormulaPolicy, ParseBudget, ParsedRow};

pub(crate) fn parse(
    bytes: &[u8],
    formula_policy: FormulaPolicy,
    budget: &mut ParseBudget<'_>,
) -> Result<Vec<ParsedRow>, FileAdapterError> {
    let workbook = package::read(bytes, budget)?;
    let mut rows = Vec::new();
    for sheet in workbook.sheet_parts() {
        budget.checkpoint()?;
        let sheet_rows = worksheet::parse(
            workbook.sheet(sheet)?,
            workbook.shared_strings(),
            formula_policy,
            budget,
        )?;
        for row in sheet_rows {
            budget.reserve_vec_slot(&mut rows)?;
            rows.push(row);
        }
    }
    Ok(rows)
}
