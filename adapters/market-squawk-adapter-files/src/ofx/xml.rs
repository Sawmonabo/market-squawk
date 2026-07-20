//! DTD-free streaming parser for XML-based OFX 2.x exports.

use quick_xml::Reader;
use quick_xml::escape::resolve_xml_entity;
use quick_xml::events::Event;

use crate::{FileAdapterError, ParseBudget, ParsedRow};

use super::collector::Collector;

pub(super) fn parse(
    input: &str,
    account_id: &str,
    currency: &str,
    budget: &mut ParseBudget<'_>,
) -> Result<Vec<ParsedRow>, FileAdapterError> {
    let mut collector = Collector::new(account_id, currency, budget);
    let mut reader = Reader::from_str(input);
    reader.config_mut().check_end_names = true;
    reader.config_mut().expand_empty_elements = true;
    loop {
        collector.checkpoint()?;
        match reader
            .read_event()
            .map_err(|_| FileAdapterError::UnsafeOfx)?
        {
            Event::Start(start) => {
                if !start.attributes_raw().is_empty() {
                    return Err(FileAdapterError::UnsafeOfx);
                }
                let qualified_name = start.name();
                let name = std::str::from_utf8(qualified_name.as_ref())
                    .map_err(|_| FileAdapterError::UnsafeOfx)?;
                collector.start(name)?;
            }
            Event::End(end) => {
                let qualified_name = end.name();
                let name = std::str::from_utf8(qualified_name.as_ref())
                    .map_err(|_| FileAdapterError::UnsafeOfx)?;
                collector.end(name)?;
            }
            Event::Text(text) => {
                let value = text.decode().map_err(|_| FileAdapterError::UnsafeOfx)?;
                if collector.root_closed() {
                    collector.trailing_document_whitespace(&value)?;
                } else {
                    collector.text(&value)?;
                }
            }
            Event::CData(text) => {
                let value = text.decode().map_err(|_| FileAdapterError::UnsafeOfx)?;
                collector.text(&value)?;
            }
            Event::GeneralRef(reference) => {
                if let Some(character) = reference
                    .resolve_char_ref()
                    .map_err(|_| FileAdapterError::UnsafeOfx)?
                {
                    if !valid_xml_character(character) {
                        return Err(FileAdapterError::UnsafeOfx);
                    }
                    collector.text(character.encode_utf8(&mut [0; 4]))?;
                } else {
                    let name = reference
                        .decode()
                        .map_err(|_| FileAdapterError::UnsafeOfx)?;
                    let value = if name == "nbsp" {
                        Some(" ")
                    } else {
                        resolve_xml_entity(&name)
                    };
                    collector.text(value.ok_or(FileAdapterError::UnsafeOfx)?)?;
                }
            }
            Event::Eof => break,
            Event::Decl(_)
            | Event::DocType(_)
            | Event::PI(_)
            | Event::Comment(_)
            | Event::Empty(_) => return Err(FileAdapterError::UnsafeOfx),
        }
    }
    collector.finish()
}

fn valid_xml_character(character: char) -> bool {
    matches!(character, '\u{9}' | '\u{a}' | '\u{d}')
        || ('\u{20}'..='\u{d7ff}').contains(&character)
        || ('\u{e000}'..='\u{fffd}').contains(&character)
        || ('\u{10000}'..='\u{10ffff}').contains(&character)
}
