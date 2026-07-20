//! Bounded XML wire-format helpers shared by the XBRL parser and normalizer.

use std::collections::BTreeMap;

use market_squawk_domain::{CalendarDate, XbrlAccuracy, XbrlAccuracyValue, XbrlSign};
use quick_xml::Reader;
use quick_xml::events::BytesStart;

use crate::SecParserLimits;

use super::SecXbrlError;

const MAX_ATTRIBUTES: usize = 64;

pub(super) fn attributes(
    reader: &Reader<&[u8]>,
    start: &BytesStart<'_>,
    limits: SecParserLimits,
) -> Result<BTreeMap<String, String>, SecXbrlError> {
    let mut values = BTreeMap::new();
    for attribute in start.attributes() {
        if values.len() >= MAX_ATTRIBUTES {
            return Err(SecXbrlError::AttributeLimitExceeded);
        }
        let attribute = attribute?;
        let key = name_text(attribute.key.as_ref(), limits)?.to_owned();
        let value = attribute
            .decoded_and_normalized_value(quick_xml::XmlVersion::Implicit1_0, reader.decoder())?
            .into_owned();
        if value.len() > limits.string_bytes() {
            return Err(SecXbrlError::StringLimitExceeded);
        }
        if values.insert(key, value).is_some() {
            return Err(SecXbrlError::DuplicateAttribute);
        }
    }
    Ok(values)
}

pub(super) fn attr<'a>(attributes: &'a BTreeMap<String, String>, key: &str) -> Option<&'a str> {
    attributes
        .get(key)
        .or_else(|| {
            attributes
                .iter()
                .find(|(candidate, _)| local_name(candidate) == key)
                .map(|(_, value)| value)
        })
        .map(String::as_str)
}

pub(super) fn required_attr(
    attributes: &BTreeMap<String, String>,
    key: &str,
) -> Result<String, SecXbrlError> {
    attr(attributes, key)
        .map(str::to_owned)
        .ok_or(SecXbrlError::MissingAttribute)
}

pub(super) fn name_text(bytes: &[u8], limits: SecParserLimits) -> Result<&str, SecXbrlError> {
    if bytes.len() > limits.string_bytes() {
        return Err(SecXbrlError::StringLimitExceeded);
    }
    std::str::from_utf8(bytes).map_err(|_| SecXbrlError::InvalidUtf8)
}

pub(super) fn local_name(name: &str) -> &str {
    name.rsplit(':').next().unwrap_or(name)
}

pub(super) fn append_bounded(
    target: &mut String,
    value: &str,
    max: usize,
) -> Result<(), SecXbrlError> {
    if target
        .len()
        .checked_add(value.len())
        .is_none_or(|length| length > max)
    {
        return Err(SecXbrlError::StringLimitExceeded);
    }
    target.push_str(value);
    Ok(())
}

pub(super) fn parse_i32(value: &str) -> Result<i32, SecXbrlError> {
    value.parse().map_err(|_| SecXbrlError::InvalidNumericFact)
}

pub(super) fn parse_sign(value: &str) -> Result<XbrlSign, SecXbrlError> {
    match value {
        "-" => Ok(XbrlSign::Negative),
        "+" => Ok(XbrlSign::Positive),
        _ => Err(SecXbrlError::InvalidNumericFact),
    }
}

pub(super) fn is_true(value: &str) -> bool {
    matches!(value, "true" | "1")
}

pub(super) fn parse_accuracy(
    attributes: &BTreeMap<String, String>,
) -> Result<XbrlAccuracy, SecXbrlError> {
    match (attr(attributes, "decimals"), attr(attributes, "precision")) {
        (Some(_), Some(_)) => Err(SecXbrlError::ConflictingAccuracy),
        (Some(value), None) => Ok(XbrlAccuracy::Decimals(parse_accuracy_value(value)?)),
        (None, Some(value)) => Ok(XbrlAccuracy::Precision(parse_accuracy_value(value)?)),
        (None, None) => Ok(XbrlAccuracy::Unspecified),
    }
}

fn parse_accuracy_value(value: &str) -> Result<XbrlAccuracyValue, SecXbrlError> {
    if value.eq_ignore_ascii_case("INF") {
        Ok(XbrlAccuracyValue::Infinite)
    } else {
        Ok(XbrlAccuracyValue::Finite(parse_i32(value)?))
    }
}

pub(super) fn transform_numeric(value: &str, format: Option<&str>) -> Result<String, SecXbrlError> {
    match format {
        None => Ok(value.trim().to_owned()),
        Some(format) if format.contains("num-dot-decimal") || format.contains("numdotdecimal") => {
            let transformed: String = value
                .chars()
                .filter(|character| *character != ',' && !character.is_whitespace())
                .collect();
            if transformed.is_empty() {
                Err(SecXbrlError::InvalidNumericFact)
            } else {
                Ok(transformed)
            }
        }
        Some(_) => Err(SecXbrlError::UnsupportedTransform),
    }
}

pub(super) fn parse_date(value: &str) -> Result<CalendarDate, SecXbrlError> {
    let mut parts = value.split('-');
    let year = parts
        .next()
        .and_then(|part| part.parse().ok())
        .ok_or(SecXbrlError::InvalidDate)?;
    let month = parts
        .next()
        .and_then(|part| part.parse().ok())
        .ok_or(SecXbrlError::InvalidDate)?;
    let day = parts
        .next()
        .and_then(|part| part.parse().ok())
        .ok_or(SecXbrlError::InvalidDate)?;
    if parts.next().is_some() {
        return Err(SecXbrlError::InvalidDate);
    }
    CalendarDate::new(year, month, day).map_err(|_| SecXbrlError::InvalidDate)
}

pub(super) fn hex_prefix(bytes: &[u8], count: usize) -> String {
    let mut text = String::with_capacity(count * 2);
    for byte in bytes.iter().take(count) {
        use std::fmt::Write as _;
        let _ignored = write!(&mut text, "{byte:02x}");
    }
    text
}
