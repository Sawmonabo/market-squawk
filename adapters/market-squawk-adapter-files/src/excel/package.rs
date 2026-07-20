//! XLSX package graph validation and workbook-part composition.

use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet};

use quick_xml::events::Event;

mod archive;
mod content_types;
mod relationships;

use archive::{read_package, required_part};
use content_types::{
    CORE_PROPERTIES_CONTENT_TYPE, CUSTOM_PROPERTIES_CONTENT_TYPE, ContentTypes,
    EXTENDED_PROPERTIES_CONTENT_TYPE, SHARED_STRINGS_CONTENT_TYPE, STYLES_CONTENT_TYPE,
    THEME_CONTENT_TYPE, WORKBOOK_CONTENT_TYPE, WORKSHEET_CONTENT_TYPE,
};
use relationships::{
    Relationship, RelationshipKind, RelationshipOwner, is_worksheet_part, parse, parse_workbook,
};

use super::xml::{
    DocumentState, SPREADSHEET_NAMESPACE, admit_decoded_text, attributes, end_local_name, enter,
    leave, local_name, next_event, require_whitespace, safe_reader,
};
use crate::{FileAdapterError, ParseBudget, ParserLimit};

const CONTENT_TYPES: &str = "[Content_Types].xml";
const ROOT_RELATIONSHIPS: &str = "_rels/.rels";
const WORKBOOK: &str = "xl/workbook.xml";
const WORKBOOK_RELATIONSHIPS: &str = "xl/_rels/workbook.xml.rels";
const SHARED_STRINGS: &str = "xl/sharedStrings.xml";

pub(super) struct WorkbookPackage {
    parts: BTreeMap<String, Vec<u8>>,
    sheet_parts: Vec<String>,
    shared_strings: Vec<String>,
}

impl WorkbookPackage {
    pub(super) fn sheet_parts(&self) -> &[String] {
        &self.sheet_parts
    }

    pub(super) fn sheet(&self, name: &str) -> Result<&[u8], FileAdapterError> {
        required_part(&self.parts, name)
    }

    pub(super) fn shared_strings(&self) -> &[String] {
        &self.shared_strings
    }
}

pub(super) fn read(
    bytes: &[u8],
    budget: &mut ParseBudget<'_>,
) -> Result<WorkbookPackage, FileAdapterError> {
    let parts = read_package(bytes, budget)?;
    for name in parts.keys().filter(|name| name.ends_with(".rels")) {
        if name != ROOT_RELATIONSHIPS && name != WORKBOOK_RELATIONSHIPS {
            return Err(FileAdapterError::UnsafeSpreadsheet);
        }
    }

    let content_types = content_types::validate(required_part(&parts, CONTENT_TYPES)?, budget)?;
    content_types::validate_coverage(&parts, &content_types, CONTENT_TYPES)?;
    content_types::require(&content_types, WORKBOOK, WORKBOOK_CONTENT_TYPE)?;

    let root_relationships = parse(
        required_part(&parts, ROOT_RELATIONSHIPS)?,
        RelationshipOwner::Package,
        budget,
    )?;
    let mut office_documents = root_relationships
        .values()
        .filter(|relationship| relationship.kind == RelationshipKind::OfficeDocument);
    let root = office_documents
        .next()
        .ok_or(FileAdapterError::UnsafeSpreadsheet)?;
    if office_documents.next().is_some() || root.target != WORKBOOK {
        return Err(FileAdapterError::UnsafeSpreadsheet);
    }
    for relationship in root_relationships.values() {
        match relationship.kind {
            RelationshipKind::OfficeDocument => {}
            RelationshipKind::CoreProperties => require_related_part(
                &parts,
                &content_types,
                relationship,
                "docProps/core.xml",
                CORE_PROPERTIES_CONTENT_TYPE,
            )?,
            RelationshipKind::ExtendedProperties => require_related_part(
                &parts,
                &content_types,
                relationship,
                "docProps/app.xml",
                EXTENDED_PROPERTIES_CONTENT_TYPE,
            )?,
            RelationshipKind::CustomProperties => require_related_part(
                &parts,
                &content_types,
                relationship,
                "docProps/custom.xml",
                CUSTOM_PROPERTIES_CONTENT_TYPE,
            )?,
            _ => return Err(FileAdapterError::UnsafeSpreadsheet),
        }
    }

    let relationships = parse(
        required_part(&parts, WORKBOOK_RELATIONSHIPS)?,
        RelationshipOwner::Workbook,
        budget,
    )?;
    let mut targets = BTreeSet::new();
    for relationship in relationships.values() {
        if targets.contains(relationship.target.as_str()) {
            return Err(FileAdapterError::UnsafeSpreadsheet);
        }
        budget.set_entry::<&str>()?;
        let _ = targets.insert(relationship.target.as_str());
        match relationship.kind {
            RelationshipKind::Worksheet => {
                if !is_worksheet_part(&relationship.target) {
                    return Err(FileAdapterError::UnsafeSpreadsheet);
                }
                let _ = required_part(&parts, &relationship.target)?;
                content_types::require(
                    &content_types,
                    &relationship.target,
                    WORKSHEET_CONTENT_TYPE,
                )?;
            }
            RelationshipKind::SharedStrings => {
                if relationship.target != SHARED_STRINGS {
                    return Err(FileAdapterError::UnsafeSpreadsheet);
                }
                let _ = required_part(&parts, SHARED_STRINGS)?;
                content_types::require(
                    &content_types,
                    SHARED_STRINGS,
                    SHARED_STRINGS_CONTENT_TYPE,
                )?;
            }
            RelationshipKind::Styles => require_related_part(
                &parts,
                &content_types,
                relationship,
                "xl/styles.xml",
                STYLES_CONTENT_TYPE,
            )?,
            RelationshipKind::Theme => require_related_part(
                &parts,
                &content_types,
                relationship,
                "xl/theme/theme1.xml",
                THEME_CONTENT_TYPE,
            )?,
            RelationshipKind::OfficeDocument
            | RelationshipKind::CoreProperties
            | RelationshipKind::ExtendedProperties
            | RelationshipKind::CustomProperties => {
                return Err(FileAdapterError::UnsafeSpreadsheet);
            }
        }
    }
    let sheet_parts = parse_workbook(required_part(&parts, WORKBOOK)?, &relationships, budget)?;
    let shared_strings = relationships
        .values()
        .find(|relationship| relationship.kind == RelationshipKind::SharedStrings)
        .map(|relationship| {
            parse_shared_strings(required_part(&parts, &relationship.target)?, budget)
        })
        .transpose()?
        .unwrap_or_default();
    Ok(WorkbookPackage {
        parts,
        sheet_parts,
        shared_strings,
    })
}

fn require_related_part(
    parts: &BTreeMap<String, Vec<u8>>,
    content_types: &ContentTypes,
    relationship: &Relationship,
    expected_part: &str,
    expected_content_type: &str,
) -> Result<(), FileAdapterError> {
    if relationship.target != expected_part {
        return Err(FileAdapterError::UnsafeSpreadsheet);
    }
    let _ = required_part(parts, expected_part)?;
    content_types::require(content_types, expected_part, expected_content_type)
}

fn parse_shared_strings(
    bytes: &[u8],
    budget: &mut ParseBudget<'_>,
) -> Result<Vec<String>, FileAdapterError> {
    let mut reader = safe_reader(bytes);
    let mut buffer = Vec::new();
    let mut depth = 0_usize;
    let mut current = None::<String>;
    let mut in_text = false;
    let mut values = Vec::new();
    let mut document = DocumentState::new("sst", SPREADSHEET_NAMESPACE, None);
    loop {
        budget.checkpoint()?;
        match next_event(&mut reader, &mut buffer)? {
            Event::Start(start) => {
                let attributes = attributes(&start, budget)?;
                document.start(&start, &attributes, depth)?;
                enter(&mut depth, budget)?;
                let name = local_name(&start, budget)?;
                if name == "si" {
                    if current.is_some() {
                        return Err(FileAdapterError::InvalidRecord);
                    }
                    current = Some(String::new());
                } else if name == "t" && current.is_some() {
                    in_text = true;
                }
            }
            Event::End(end) => {
                let name = end_local_name(&end, budget)?;
                if name == "t" {
                    in_text = false;
                } else if name == "si" {
                    let value = current.take().ok_or(FileAdapterError::InvalidRecord)?;
                    budget.text(value.len())?;
                    budget.reserve_vec_slot(&mut values)?;
                    values.push(value);
                    if values.len() > budget.limits.input.max_cells {
                        return Err(FileAdapterError::LimitExceeded(ParserLimit::Cells));
                    }
                }
                document.end(&end, depth)?;
                leave(&mut depth)?;
            }
            Event::Text(text) if in_text => {
                admit_decoded_text(text.len(), budget)?;
                let text = text.decode().map_err(|_| FileAdapterError::InvalidRecord)?;
                if let Cow::Owned(text) = &text {
                    budget.string_allocation(text)?;
                }
                let value = current.as_mut().ok_or(FileAdapterError::InvalidRecord)?;
                budget.append_string(value, &text)?;
                budget.text(text.len())?;
            }
            Event::Text(text) => {
                admit_decoded_text(text.len(), budget)?;
                let text = text.decode().map_err(|_| FileAdapterError::InvalidRecord)?;
                if let Cow::Owned(text) = &text {
                    budget.string_allocation(text)?;
                }
                require_whitespace(&text)?;
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
    if current.is_some() || in_text {
        return Err(FileAdapterError::InvalidRecord);
    }
    Ok(values)
}
