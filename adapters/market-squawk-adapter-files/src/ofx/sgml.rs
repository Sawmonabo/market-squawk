//! Owned, bounded tokenizer for legacy OFX SGML with omitted leaf closing tags.

use quick_xml::escape::unescape_with;

use crate::{FileAdapterError, ParseBudget, ParsedRow};

use super::collector::Collector;

pub(super) fn parse(
    input: &str,
    account_id: &str,
    currency: &str,
    budget: &mut ParseBudget<'_>,
) -> Result<Vec<ParsedRow>, FileAdapterError> {
    let mut collector = Collector::new(account_id, currency, budget);
    let mut open = Vec::<OpenTag>::new();
    let mut cursor = 0_usize;
    while cursor < input.len() {
        collector.checkpoint()?;
        let remaining = input.get(cursor..).ok_or(FileAdapterError::UnsafeOfx)?;
        let Some(relative) = remaining.find('<') else {
            collector.trailing_document_whitespace(remaining)?;
            break;
        };
        let tag_start = cursor
            .checked_add(relative)
            .ok_or(FileAdapterError::UnsafeOfx)?;
        let text = input
            .get(cursor..tag_start)
            .ok_or(FileAdapterError::UnsafeOfx)?;
        if !text.trim().is_empty() {
            let value = unescape_with(text.trim(), |entity| (entity == "nbsp").then_some(" "))
                .map_err(|_| FileAdapterError::UnsafeOfx)?;
            collector.text(&value)?;
            open.last_mut().ok_or(FileAdapterError::UnsafeOfx)?.has_text = true;
        }
        let close_relative = input
            .get(tag_start + 1..)
            .and_then(|rest| rest.find('>'))
            .ok_or(FileAdapterError::UnsafeOfx)?;
        let tag_end = tag_start
            .checked_add(close_relative)
            .and_then(|value| value.checked_add(1))
            .ok_or(FileAdapterError::UnsafeOfx)?;
        let raw = input
            .get(tag_start + 1..tag_end)
            .ok_or(FileAdapterError::UnsafeOfx)?;
        if raw.starts_with('!')
            || raw.starts_with('?')
            || raw.ends_with('/')
            || raw.chars().any(char::is_whitespace)
        {
            return Err(FileAdapterError::UnsafeOfx);
        }
        let (closing, name) = raw
            .strip_prefix('/')
            .map_or((false, raw), |name| (true, name));
        if !closing && open.last().is_some_and(|tag| tag.has_text) {
            close_implicit_leaf(&mut collector, &mut open)?;
        }
        if closing {
            if open
                .last()
                .is_some_and(|tag| tag.has_text && tag.name != name)
            {
                close_implicit_leaf(&mut collector, &mut open)?;
            }
            let tag = open.pop().ok_or(FileAdapterError::UnsafeOfx)?;
            if tag.name != name {
                return Err(FileAdapterError::UnsafeOfx);
            }
            collector.end(name)?;
        } else {
            collector.start(name)?;
            let name = name.to_owned();
            collector.reserve_vec_slot(&mut open)?;
            open.push(OpenTag {
                name,
                has_text: false,
            });
        }
        cursor = tag_end.checked_add(1).ok_or(FileAdapterError::UnsafeOfx)?;
    }
    if open.last().is_some_and(|tag| tag.has_text) {
        close_implicit_leaf(&mut collector, &mut open)?;
    }
    if !open.is_empty() {
        return Err(FileAdapterError::UnsafeOfx);
    }
    collector.finish()
}

#[derive(Debug)]
struct OpenTag {
    name: String,
    has_text: bool,
}

fn close_implicit_leaf(
    collector: &mut Collector<'_, '_>,
    open: &mut Vec<OpenTag>,
) -> Result<(), FileAdapterError> {
    let leaf = open.pop().ok_or(FileAdapterError::UnsafeOfx)?;
    collector.end(&leaf.name)
}
