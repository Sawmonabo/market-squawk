//! Closed OPC content-type declaration and retained-part coverage.

use std::collections::BTreeMap;

use super::super::xml::{CONTENT_TYPES_NAMESPACE, visit_starts};
use crate::{FileAdapterError, ParseBudget};

pub(super) const WORKBOOK_CONTENT_TYPE: &str =
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml";
pub(super) const WORKSHEET_CONTENT_TYPE: &str =
    "application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml";
pub(super) const SHARED_STRINGS_CONTENT_TYPE: &str =
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sharedStrings+xml";
pub(super) const CORE_PROPERTIES_CONTENT_TYPE: &str =
    "application/vnd.openxmlformats-package.core-properties+xml";
pub(super) const EXTENDED_PROPERTIES_CONTENT_TYPE: &str =
    "application/vnd.openxmlformats-officedocument.extended-properties+xml";
pub(super) const CUSTOM_PROPERTIES_CONTENT_TYPE: &str =
    "application/vnd.openxmlformats-officedocument.custom-properties+xml";
pub(super) const STYLES_CONTENT_TYPE: &str =
    "application/vnd.openxmlformats-officedocument.spreadsheetml.styles+xml";
pub(super) const THEME_CONTENT_TYPE: &str =
    "application/vnd.openxmlformats-officedocument.theme+xml";

#[derive(Debug)]
pub(super) struct ContentTypes {
    defaults: BTreeMap<String, String>,
    overrides: BTreeMap<String, String>,
}

pub(super) fn validate(
    bytes: &[u8],
    budget: &mut ParseBudget<'_>,
) -> Result<ContentTypes, FileAdapterError> {
    let mut defaults = BTreeMap::new();
    let mut overrides = BTreeMap::new();
    visit_starts(
        bytes,
        budget,
        "Types",
        CONTENT_TYPES_NAMESPACE,
        None,
        |name, parent, depth, attributes, budget| {
            match name {
                "Types" if parent.is_none() && depth == 1 => {
                    if attributes.len() != 1 {
                        return Err(FileAdapterError::UnsafeSpreadsheet);
                    }
                    return Ok(());
                }
                "Default" => {
                    if parent != Some("Types") || depth != 2 || attributes.len() != 2 {
                        return Err(FileAdapterError::UnsafeSpreadsheet);
                    }
                    let extension = attributes
                        .get("Extension")
                        .ok_or(FileAdapterError::UnsafeSpreadsheet)?;
                    let content_type = attributes
                        .get("ContentType")
                        .ok_or(FileAdapterError::UnsafeSpreadsheet)?;
                    reject_active(Some(extension), content_type, budget)?;
                    if extension.is_empty()
                        || !extension.bytes().all(|byte| byte.is_ascii_alphanumeric())
                    {
                        return Err(FileAdapterError::UnsafeSpreadsheet);
                    }
                    budget.ensure_dynamic_bytes(extension.len())?;
                    let extension = extension.to_ascii_lowercase();
                    budget.string_allocation(&extension)?;
                    let content_type = budget.owned_text(content_type)?;
                    budget.map_entry::<String, String>()?;
                    if defaults.insert(extension, content_type).is_some() {
                        return Err(FileAdapterError::UnsafeSpreadsheet);
                    }
                }
                "Override" => {
                    if parent != Some("Types") || depth != 2 || attributes.len() != 2 {
                        return Err(FileAdapterError::UnsafeSpreadsheet);
                    }
                    let part = normalize_part_name(
                        attributes
                            .get("PartName")
                            .ok_or(FileAdapterError::UnsafeSpreadsheet)?,
                        budget,
                    )?;
                    let content_type = attributes
                        .get("ContentType")
                        .ok_or(FileAdapterError::UnsafeSpreadsheet)?;
                    reject_active(None, content_type, budget)?;
                    let content_type = budget.owned_text(content_type)?;
                    budget.map_entry::<String, String>()?;
                    if overrides.insert(part, content_type).is_some() {
                        return Err(FileAdapterError::UnsafeSpreadsheet);
                    }
                }
                _ => return Err(FileAdapterError::UnsafeSpreadsheet),
            }
            Ok(())
        },
    )?;
    Ok(ContentTypes {
        defaults,
        overrides,
    })
}

pub(super) fn validate_coverage(
    parts: &BTreeMap<String, Vec<u8>>,
    content_types: &ContentTypes,
    content_types_part: &str,
) -> Result<(), FileAdapterError> {
    if content_types
        .overrides
        .keys()
        .any(|part| !parts.contains_key(part))
    {
        return Err(FileAdapterError::UnsafeSpreadsheet);
    }
    for part in parts
        .keys()
        .filter(|part| part.as_str() != content_types_part)
    {
        if content_types.overrides.contains_key(part) {
            continue;
        }
        let extension = part
            .rsplit_once('.')
            .map(|(_, extension)| extension)
            .filter(|extension| !extension.is_empty())
            .ok_or(FileAdapterError::UnsafeSpreadsheet)?;
        if !content_types
            .defaults
            .keys()
            .any(|candidate| candidate.eq_ignore_ascii_case(extension))
        {
            return Err(FileAdapterError::UnsafeSpreadsheet);
        }
    }
    Ok(())
}

pub(super) fn require(
    content_types: &ContentTypes,
    part: &str,
    expected: &str,
) -> Result<(), FileAdapterError> {
    let actual = content_types.overrides.get(part).or_else(|| {
        part.rsplit_once('.').and_then(|(_, extension)| {
            content_types
                .defaults
                .iter()
                .find(|(candidate, _)| candidate.eq_ignore_ascii_case(extension))
                .map(|(_, content_type)| content_type)
        })
    });
    if actual.map(String::as_str) == Some(expected) {
        Ok(())
    } else {
        Err(FileAdapterError::UnsafeSpreadsheet)
    }
}

fn reject_active(
    extension: Option<&String>,
    content_type: &str,
    budget: &mut ParseBudget<'_>,
) -> Result<(), FileAdapterError> {
    budget.ensure_dynamic_bytes(content_type.len())?;
    let lower = content_type.to_ascii_lowercase();
    budget.string_allocation(&lower)?;
    let disallowed = extension.is_some_and(|value| value.eq_ignore_ascii_case("bin"))
        || lower.contains("macroenabled")
        || lower.contains("vba")
        || lower.contains("activex")
        || lower.contains("oleobject");
    if disallowed {
        Err(FileAdapterError::UnsafeSpreadsheet)
    } else {
        Ok(())
    }
}

fn normalize_part_name(
    part: &str,
    budget: &mut ParseBudget<'_>,
) -> Result<String, FileAdapterError> {
    let part = part
        .strip_prefix('/')
        .ok_or(FileAdapterError::UnsafeSpreadsheet)?;
    if part.is_empty()
        || part.contains(['\\', ':', '%', '?', '#', '\0'])
        || part
            .split('/')
            .any(|component| component.is_empty() || matches!(component, "." | ".."))
    {
        return Err(FileAdapterError::UnsafeSpreadsheet);
    }
    budget.owned_text(part)
}
