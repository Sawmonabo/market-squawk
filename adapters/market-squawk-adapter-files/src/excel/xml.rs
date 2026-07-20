//! Shared network-free XML primitives for OOXML package parts.

use std::borrow::Cow;
use std::collections::BTreeMap;

use quick_xml::events::{BytesStart, Event};
use quick_xml::{Reader, XmlVersion};

use crate::{FileAdapterError, ParseBudget, ParserLimit};

pub(super) const CONTENT_TYPES_NAMESPACE: &str =
    "http://schemas.openxmlformats.org/package/2006/content-types";
pub(super) const PACKAGE_RELATIONSHIPS_NAMESPACE: &str =
    "http://schemas.openxmlformats.org/package/2006/relationships";
pub(super) const SPREADSHEET_NAMESPACE: &str =
    "http://schemas.openxmlformats.org/spreadsheetml/2006/main";
pub(super) const OFFICE_RELATIONSHIPS_NAMESPACE: &str =
    "http://schemas.openxmlformats.org/officeDocument/2006/relationships";

pub(super) struct DocumentState {
    expected_root: &'static str,
    expected_namespace: &'static str,
    required_prefix: Option<(&'static str, &'static str)>,
    declaration_seen: bool,
    root_seen: bool,
    root_closed: bool,
}

impl DocumentState {
    pub(super) fn new(
        expected_root: &'static str,
        expected_namespace: &'static str,
        required_prefix: Option<(&'static str, &'static str)>,
    ) -> Self {
        Self {
            expected_root,
            expected_namespace,
            required_prefix,
            declaration_seen: false,
            root_seen: false,
            root_closed: false,
        }
    }

    pub(super) fn start(
        &mut self,
        start: &BytesStart<'_>,
        attributes: &BTreeMap<String, String>,
        depth: usize,
    ) -> Result<(), FileAdapterError> {
        let qualified_name = start.name();
        let qualified = std::str::from_utf8(qualified_name.as_ref())
            .map_err(|_| FileAdapterError::UnsafeSpreadsheet)?;
        if qualified.contains(':') || self.root_closed {
            return Err(FileAdapterError::UnsafeSpreadsheet);
        }
        if depth == 0 {
            if self.root_seen
                || qualified != self.expected_root
                || attributes.get("xmlns").map(String::as_str) != Some(self.expected_namespace)
            {
                return Err(FileAdapterError::UnsafeSpreadsheet);
            }
            if let Some((prefix, namespace)) = self.required_prefix
                && attributes.get(prefix).map(String::as_str) != Some(namespace)
            {
                return Err(FileAdapterError::UnsafeSpreadsheet);
            }
            self.root_seen = true;
        } else if !self.root_seen
            || attributes
                .keys()
                .any(|key| key == "xmlns" || key.starts_with("xmlns:"))
        {
            return Err(FileAdapterError::UnsafeSpreadsheet);
        }
        Ok(())
    }

    pub(super) fn end(
        &mut self,
        end: &quick_xml::events::BytesEnd<'_>,
        depth: usize,
    ) -> Result<(), FileAdapterError> {
        let qualified_name = end.name();
        let qualified = std::str::from_utf8(qualified_name.as_ref())
            .map_err(|_| FileAdapterError::UnsafeSpreadsheet)?;
        if qualified.contains(':') || !self.root_seen || self.root_closed || depth == 0 {
            return Err(FileAdapterError::UnsafeSpreadsheet);
        }
        if depth == 1 {
            if qualified != self.expected_root {
                return Err(FileAdapterError::UnsafeSpreadsheet);
            }
            self.root_closed = true;
        }
        Ok(())
    }

    pub(super) fn declaration(&mut self, depth: usize) -> Result<(), FileAdapterError> {
        if depth != 0 || self.declaration_seen || self.root_seen {
            return Err(FileAdapterError::UnsafeSpreadsheet);
        }
        self.declaration_seen = true;
        Ok(())
    }

    pub(super) fn finish(&self, depth: usize) -> Result<(), FileAdapterError> {
        if depth != 0 || !self.root_seen || !self.root_closed {
            Err(FileAdapterError::UnsafeSpreadsheet)
        } else {
            Ok(())
        }
    }
}

pub(super) fn visit_starts(
    bytes: &[u8],
    budget: &mut ParseBudget<'_>,
    expected_root: &'static str,
    expected_namespace: &'static str,
    required_prefix: Option<(&'static str, &'static str)>,
    mut visit: impl FnMut(
        &str,
        Option<&str>,
        usize,
        &BTreeMap<String, String>,
        &mut ParseBudget<'_>,
    ) -> Result<(), FileAdapterError>,
) -> Result<(), FileAdapterError> {
    let mut reader = safe_reader(bytes);
    let mut buffer = Vec::new();
    let mut depth = 0_usize;
    let mut elements = Vec::<String>::new();
    let mut document = DocumentState::new(expected_root, expected_namespace, required_prefix);
    loop {
        budget.checkpoint()?;
        match next_event(&mut reader, &mut buffer)? {
            Event::Start(start) => {
                let attributes = attributes(&start, budget)?;
                document.start(&start, &attributes, depth)?;
                enter(&mut depth, budget)?;
                let name = local_name(&start, budget)?;
                visit(
                    &name,
                    elements.last().map(String::as_str),
                    depth,
                    &attributes,
                    budget,
                )?;
                budget.reserve_vec_slot(&mut elements)?;
                elements.push(name);
            }
            Event::End(end) => {
                document.end(&end, depth)?;
                let _ = elements.pop().ok_or(FileAdapterError::UnsafeSpreadsheet)?;
                leave(&mut depth)?;
            }
            Event::Text(text) => {
                admit_decoded_text(text.len(), budget)?;
                let text = text.decode().map_err(|_| FileAdapterError::InvalidRecord)?;
                if let Cow::Owned(value) = &text {
                    budget.string_allocation(value)?;
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
    document.finish(depth)
}

pub(super) fn safe_reader(bytes: &[u8]) -> Reader<&[u8]> {
    let mut reader = Reader::from_reader(bytes);
    reader.config_mut().check_end_names = true;
    reader.config_mut().expand_empty_elements = true;
    reader
}

pub(super) fn next_event<'a>(
    reader: &mut Reader<&[u8]>,
    buffer: &'a mut Vec<u8>,
) -> Result<Event<'a>, FileAdapterError> {
    reader
        .read_event_into(buffer)
        .map_err(|_| FileAdapterError::InvalidRecord)
}

pub(super) fn enter(depth: &mut usize, budget: &ParseBudget<'_>) -> Result<(), FileAdapterError> {
    *depth = depth
        .checked_add(1)
        .ok_or(FileAdapterError::LimitExceeded(ParserLimit::NestingDepth))?;
    budget.depth(*depth)
}

pub(super) fn leave(depth: &mut usize) -> Result<(), FileAdapterError> {
    *depth = depth
        .checked_sub(1)
        .ok_or(FileAdapterError::InvalidRecord)?;
    Ok(())
}

pub(super) fn local_name(
    start: &BytesStart<'_>,
    budget: &mut ParseBudget<'_>,
) -> Result<String, FileAdapterError> {
    let name = start.local_name();
    budget.owned_text(
        std::str::from_utf8(name.as_ref()).map_err(|_| FileAdapterError::InvalidRecord)?,
    )
}

pub(super) fn end_local_name(
    end: &quick_xml::events::BytesEnd<'_>,
    budget: &mut ParseBudget<'_>,
) -> Result<String, FileAdapterError> {
    let name = end.local_name();
    budget.owned_text(
        std::str::from_utf8(name.as_ref()).map_err(|_| FileAdapterError::InvalidRecord)?,
    )
}

pub(super) fn attributes(
    start: &BytesStart<'_>,
    budget: &mut ParseBudget<'_>,
) -> Result<BTreeMap<String, String>, FileAdapterError> {
    let mut values = BTreeMap::new();
    for attribute in start.attributes() {
        let attribute = attribute.map_err(|_| FileAdapterError::InvalidRecord)?;
        let key = budget.owned_text(
            std::str::from_utf8(attribute.key.as_ref())
                .map_err(|_| FileAdapterError::InvalidRecord)?,
        )?;
        admit_decoded_text(attribute.value.as_ref().len(), budget)?;
        let value = attribute
            .normalized_value(XmlVersion::Implicit1_0)
            .map_err(|_| FileAdapterError::UnsafeSpreadsheet)?
            .into_owned();
        budget.text(value.len())?;
        budget.string_allocation(&value)?;
        budget.map_entry::<String, String>()?;
        if values.insert(key, value).is_some() {
            return Err(FileAdapterError::DuplicateField);
        }
    }
    Ok(values)
}

pub(super) fn admit_decoded_text(
    encoded_bytes: usize,
    budget: &ParseBudget<'_>,
) -> Result<(), FileAdapterError> {
    let decoded_bound = encoded_bytes
        .checked_mul(3)
        .ok_or(FileAdapterError::LimitExceeded(ParserLimit::DecodedBytes))?;
    budget.ensure_dynamic_bytes(decoded_bound)
}

pub(super) fn require_whitespace(value: &str) -> Result<(), FileAdapterError> {
    if value.chars().all(char::is_whitespace) {
        Ok(())
    } else {
        Err(FileAdapterError::InvalidRecord)
    }
}
