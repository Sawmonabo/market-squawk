//! Closed OPC relationship graphs and workbook sheet binding.

use std::collections::{BTreeMap, BTreeSet};

use super::super::xml::{
    OFFICE_RELATIONSHIPS_NAMESPACE, PACKAGE_RELATIONSHIPS_NAMESPACE, SPREADSHEET_NAMESPACE,
    visit_starts,
};
use crate::{FileAdapterError, ParseBudget, ParserLimit};

const OFFICE_DOCUMENT_RELATIONSHIP: &str =
    "http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument";
const WORKSHEET_RELATIONSHIP: &str =
    "http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet";
const SHARED_STRINGS_RELATIONSHIP: &str =
    "http://schemas.openxmlformats.org/officeDocument/2006/relationships/sharedStrings";
const CORE_PROPERTIES_RELATIONSHIP: &str =
    "http://schemas.openxmlformats.org/package/2006/relationships/metadata/core-properties";
const EXTENDED_PROPERTIES_RELATIONSHIP: &str =
    "http://schemas.openxmlformats.org/officeDocument/2006/relationships/extended-properties";
const CUSTOM_PROPERTIES_RELATIONSHIP: &str =
    "http://schemas.openxmlformats.org/officeDocument/2006/relationships/custom-properties";
const STYLES_RELATIONSHIP: &str =
    "http://schemas.openxmlformats.org/officeDocument/2006/relationships/styles";
const THEME_RELATIONSHIP: &str =
    "http://schemas.openxmlformats.org/officeDocument/2006/relationships/theme";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum RelationshipKind {
    OfficeDocument,
    Worksheet,
    SharedStrings,
    CoreProperties,
    ExtendedProperties,
    CustomProperties,
    Styles,
    Theme,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum RelationshipOwner {
    Package,
    Workbook,
}

#[derive(Debug)]
pub(super) struct Relationship {
    pub(super) target: String,
    pub(super) kind: RelationshipKind,
}

pub(super) fn parse(
    bytes: &[u8],
    owner: RelationshipOwner,
    budget: &mut ParseBudget<'_>,
) -> Result<BTreeMap<String, Relationship>, FileAdapterError> {
    let mut relationships = BTreeMap::new();
    visit_starts(
        bytes,
        budget,
        "Relationships",
        PACKAGE_RELATIONSHIPS_NAMESPACE,
        None,
        |name, parent, depth, attributes, budget| {
            if name == "Relationships" && parent.is_none() && depth == 1 {
                if attributes.len() != 1 {
                    return Err(FileAdapterError::UnsafeSpreadsheet);
                }
                return Ok(());
            }
            if name != "Relationship"
                || parent != Some("Relationships")
                || depth != 2
                || attributes.len() != 3
            {
                return Err(FileAdapterError::UnsafeSpreadsheet);
            }
            let id = attributes
                .get("Id")
                .ok_or(FileAdapterError::UnsafeSpreadsheet)?;
            let id = budget.owned_text(id)?;
            let target = attributes
                .get("Target")
                .ok_or(FileAdapterError::UnsafeSpreadsheet)?;
            let relationship_type = attributes
                .get("Type")
                .ok_or(FileAdapterError::UnsafeSpreadsheet)?;
            let kind = match (owner, relationship_type.as_str()) {
                (RelationshipOwner::Package, OFFICE_DOCUMENT_RELATIONSHIP) => {
                    RelationshipKind::OfficeDocument
                }
                (RelationshipOwner::Workbook, WORKSHEET_RELATIONSHIP) => {
                    RelationshipKind::Worksheet
                }
                (RelationshipOwner::Workbook, SHARED_STRINGS_RELATIONSHIP) => {
                    RelationshipKind::SharedStrings
                }
                (RelationshipOwner::Package, CORE_PROPERTIES_RELATIONSHIP) => {
                    RelationshipKind::CoreProperties
                }
                (RelationshipOwner::Package, EXTENDED_PROPERTIES_RELATIONSHIP) => {
                    RelationshipKind::ExtendedProperties
                }
                (RelationshipOwner::Package, CUSTOM_PROPERTIES_RELATIONSHIP) => {
                    RelationshipKind::CustomProperties
                }
                (RelationshipOwner::Workbook, STYLES_RELATIONSHIP) => RelationshipKind::Styles,
                (RelationshipOwner::Workbook, THEME_RELATIONSHIP) => RelationshipKind::Theme,
                _ => return Err(FileAdapterError::UnsafeSpreadsheet),
            };
            if kind != RelationshipKind::Worksheet
                && relationships
                    .values()
                    .any(|relationship: &Relationship| relationship.kind == kind)
            {
                return Err(FileAdapterError::UnsafeSpreadsheet);
            }
            let target = normalize_target(owner, target, budget)?;
            budget.map_entry::<String, Relationship>()?;
            if relationships
                .insert(id, Relationship { target, kind })
                .is_some()
            {
                return Err(FileAdapterError::DuplicateField);
            }
            Ok(())
        },
    )?;
    Ok(relationships)
}

pub(super) fn parse_workbook(
    bytes: &[u8],
    relationships: &BTreeMap<String, Relationship>,
    budget: &mut ParseBudget<'_>,
) -> Result<Vec<String>, FileAdapterError> {
    let mut sheets = Vec::new();
    let mut ids = BTreeSet::new();
    let mut sheets_container_seen = false;
    let max_sheets = budget.limits.input.max_sheets;
    visit_starts(
        bytes,
        budget,
        "workbook",
        SPREADSHEET_NAMESPACE,
        Some(("xmlns:r", OFFICE_RELATIONSHIPS_NAMESPACE)),
        |name, parent, depth, attributes, budget| {
            if name == "sheets" {
                if parent != Some("workbook") || depth != 2 || sheets_container_seen {
                    return Err(FileAdapterError::UnsafeSpreadsheet);
                }
                sheets_container_seen = true;
                return Ok(());
            }
            if name != "sheet" {
                return Ok(());
            }
            if parent != Some("sheets") || depth != 3 {
                return Err(FileAdapterError::UnsafeSpreadsheet);
            }
            let relation_id = attributes
                .get("r:id")
                .ok_or(FileAdapterError::UnsafeSpreadsheet)?;
            let _sheet_name = attributes
                .get("name")
                .ok_or(FileAdapterError::UnsafeSpreadsheet)?;
            let retained_id = budget.owned_text(relation_id)?;
            if ids.contains(&retained_id) {
                return Err(FileAdapterError::DuplicateField);
            }
            budget.set_entry::<String>()?;
            let _ = ids.insert(retained_id);
            let relationship = relationships
                .get(relation_id)
                .ok_or(FileAdapterError::UnsafeSpreadsheet)?;
            if relationship.kind != RelationshipKind::Worksheet {
                return Err(FileAdapterError::UnsafeSpreadsheet);
            }
            let target = budget.owned_text(&relationship.target)?;
            budget.reserve_vec_slot(&mut sheets)?;
            sheets.push(target);
            if sheets.len() > max_sheets {
                return Err(FileAdapterError::LimitExceeded(ParserLimit::Sheets));
            }
            Ok(())
        },
    )?;
    let worksheet_count = relationships
        .values()
        .filter(|relationship| relationship.kind == RelationshipKind::Worksheet)
        .count();
    if !sheets_container_seen || sheets.is_empty() || ids.len() != worksheet_count {
        return Err(FileAdapterError::UnsafeSpreadsheet);
    }
    Ok(sheets)
}

pub(super) fn is_worksheet_part(part: &str) -> bool {
    part.strip_prefix("xl/worksheets/")
        .is_some_and(|name| !name.is_empty() && !name.contains('/') && name.ends_with(".xml"))
}

fn normalize_target(
    owner: RelationshipOwner,
    target: &str,
    budget: &mut ParseBudget<'_>,
) -> Result<String, FileAdapterError> {
    if target.contains(['\\', ':', '%', '?', '#', '\0']) || target.starts_with('/') {
        return Err(FileAdapterError::UnsafeSpreadsheet);
    }
    let mut components = Vec::new();
    if owner == RelationshipOwner::Workbook {
        budget.reserve_vec_slot(&mut components)?;
        components.push("xl");
    }
    for component in target.split('/') {
        match component {
            "" => return Err(FileAdapterError::UnsafeSpreadsheet),
            "." => {}
            ".." => {
                let _ = components
                    .pop()
                    .ok_or(FileAdapterError::UnsafeSpreadsheet)?;
            }
            value => {
                budget.reserve_vec_slot(&mut components)?;
                components.push(value);
            }
        }
    }
    if components.is_empty() {
        return Err(FileAdapterError::UnsafeSpreadsheet);
    }
    let normalized_bytes = components.iter().try_fold(0_usize, |total, component| {
        total
            .checked_add(component.len())
            .ok_or(FileAdapterError::LimitExceeded(ParserLimit::TextBytes))
    })?;
    let normalized_bytes = normalized_bytes
        .checked_add(components.len().saturating_sub(1))
        .ok_or(FileAdapterError::LimitExceeded(ParserLimit::TextBytes))?;
    budget.text(normalized_bytes)?;
    let mut normalized = budget.string_with_capacity(normalized_bytes)?;
    for (index, component) in components.into_iter().enumerate() {
        if index != 0 {
            normalized.push('/');
        }
        normalized.push_str(component);
    }
    Ok(normalized)
}
