//! Bounded OFX 1.x SGML and OFX 2.x XML statement extraction.

mod collector;
mod sgml;
mod xml;

use std::borrow::Cow;
use std::collections::BTreeMap;

use encoding_rs::WINDOWS_1252;

use crate::{FileAdapterError, ParseBudget, ParsedRow};

const OFX_ROOT: &[u8] = b"<OFX>";
const LEGACY_HEADERS: [&str; 9] = [
    "OFXHEADER",
    "DATA",
    "VERSION",
    "SECURITY",
    "ENCODING",
    "CHARSET",
    "COMPRESSION",
    "OLDFILEUID",
    "NEWFILEUID",
];

pub(crate) fn parse(
    bytes: &[u8],
    account_id: &str,
    currency: &str,
    budget: &mut ParseBudget<'_>,
) -> Result<Vec<ParsedRow>, FileAdapterError> {
    let root = find_root(bytes)?;
    let prefix = bytes.get(..root).ok_or(FileAdapterError::UnsafeOfx)?;
    let body = bytes.get(root..).ok_or(FileAdapterError::UnsafeOfx)?;
    // Encoding conversion, entity unescaping, and owned tag tokens cannot exceed the bounded raw
    // body. Admit their allocator growth before the decoder or tokenizer can allocate.
    let decoded_bound = body
        .len()
        .checked_mul(3)
        .ok_or(FileAdapterError::LimitExceeded(
            crate::ParserLimit::DecodedBytes,
        ))?;
    budget.pre_admit_dynamic_bytes(decoded_bound)?;
    if prefix.starts_with(b"OFXHEADER:") {
        require_header_separator(prefix)?;
        let headers = legacy_headers(prefix, budget)?;
        validate_legacy_headers(&headers)?;
        let decoded = decode_legacy(body, &headers)?;
        sgml::parse(&decoded, account_id, currency, budget)
    } else {
        validate_xml_preamble(prefix, budget)?;
        let body = std::str::from_utf8(body).map_err(|_| FileAdapterError::UnsafeOfx)?;
        xml::parse(body, account_id, currency, budget)
    }
}

fn find_root(bytes: &[u8]) -> Result<usize, FileAdapterError> {
    bytes
        .windows(OFX_ROOT.len())
        .position(|window| window == OFX_ROOT)
        .ok_or(FileAdapterError::UnsafeOfx)
}

fn require_header_separator(prefix: &[u8]) -> Result<(), FileAdapterError> {
    if !(prefix.ends_with(b"\n\n") || prefix.ends_with(b"\r\n\r\n") || prefix.ends_with(b"\r\r")) {
        return Err(FileAdapterError::UnsafeOfx);
    }
    Ok(())
}

fn legacy_headers<'a>(
    prefix: &'a [u8],
    budget: &mut ParseBudget<'_>,
) -> Result<BTreeMap<&'a str, &'a str>, FileAdapterError> {
    let prefix = std::str::from_utf8(prefix).map_err(|_| FileAdapterError::UnsafeOfx)?;
    let mut headers = BTreeMap::new();
    for line in prefix
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
    {
        let (name, value) = line.split_once(':').ok_or(FileAdapterError::UnsafeOfx)?;
        if !LEGACY_HEADERS.contains(&name) || value.is_empty() || headers.contains_key(name) {
            return Err(FileAdapterError::UnsafeOfx);
        }
        budget.map_entry::<&str, &str>()?;
        headers.insert(name, value);
    }
    if headers.len() != LEGACY_HEADERS.len() {
        return Err(FileAdapterError::UnsafeOfx);
    }
    Ok(headers)
}

fn validate_legacy_headers(headers: &BTreeMap<&str, &str>) -> Result<(), FileAdapterError> {
    let version = header(headers, "VERSION")?;
    let valid_version = version.len() == 3
        && version.bytes().all(|byte| byte.is_ascii_digit())
        && version.starts_with('1');
    if header(headers, "OFXHEADER")? != "100"
        || header(headers, "DATA")? != "OFXSGML"
        || !valid_version
        || header(headers, "SECURITY")? != "NONE"
        || header(headers, "COMPRESSION")? != "NONE"
    {
        return Err(FileAdapterError::UnsafeOfx);
    }
    Ok(())
}

fn decode_legacy<'a>(
    body: &'a [u8],
    headers: &BTreeMap<&str, &str>,
) -> Result<Cow<'a, str>, FileAdapterError> {
    let encoding = header(headers, "ENCODING")?;
    let charset = header(headers, "CHARSET")?;
    match (encoding, charset) {
        ("USASCII", "1252") => Ok(WINDOWS_1252.decode_without_bom_handling(body).0),
        ("USASCII", "USASCII") if body.iter().all(u8::is_ascii) => std::str::from_utf8(body)
            .map(Cow::Borrowed)
            .map_err(|_| FileAdapterError::UnsafeOfx),
        ("UNICODE", "UTF-8") | ("USASCII", "UTF-8") => std::str::from_utf8(body)
            .map(Cow::Borrowed)
            .map_err(|_| FileAdapterError::UnsafeOfx),
        _ => Err(FileAdapterError::UnsafeOfx),
    }
}

fn validate_xml_preamble(
    prefix: &[u8],
    budget: &mut ParseBudget<'_>,
) -> Result<(), FileAdapterError> {
    let prefix = std::str::from_utf8(prefix).map_err(|_| FileAdapterError::UnsafeOfx)?;
    let mut remaining = prefix.trim();
    if remaining.starts_with("<?xml") {
        let end = remaining.find("?>").ok_or(FileAdapterError::UnsafeOfx)?;
        remaining = remaining
            .get(end + 2..)
            .ok_or(FileAdapterError::UnsafeOfx)?
            .trim();
    }
    if !remaining.starts_with("<?OFX ") || !remaining.ends_with("?>") {
        return Err(FileAdapterError::UnsafeOfx);
    }
    let attributes = remaining
        .strip_prefix("<?OFX ")
        .and_then(|value| value.strip_suffix("?>"))
        .ok_or(FileAdapterError::UnsafeOfx)?;
    let mut values = BTreeMap::new();
    for attribute in attributes.split_ascii_whitespace() {
        let (name, quoted) = attribute
            .split_once('=')
            .ok_or(FileAdapterError::UnsafeOfx)?;
        let value = quoted
            .strip_prefix('"')
            .and_then(|value| value.strip_suffix('"'))
            .ok_or(FileAdapterError::UnsafeOfx)?;
        if !matches!(
            name,
            "OFXHEADER" | "VERSION" | "SECURITY" | "OLDFILEUID" | "NEWFILEUID"
        ) || values.contains_key(name)
        {
            return Err(FileAdapterError::UnsafeOfx);
        }
        budget.map_entry::<&str, &str>()?;
        values.insert(name, value);
    }
    let version = header(&values, "VERSION")?;
    if values.len() != 5
        || header(&values, "OFXHEADER")? != "200"
        || version.len() != 3
        || !version.bytes().all(|byte| byte.is_ascii_digit())
        || !version.starts_with('2')
        || header(&values, "SECURITY")? != "NONE"
    {
        return Err(FileAdapterError::UnsafeOfx);
    }
    Ok(())
}

fn header<'a>(headers: &'a BTreeMap<&str, &str>, name: &str) -> Result<&'a str, FileAdapterError> {
    headers
        .get(name)
        .copied()
        .ok_or(FileAdapterError::UnsafeOfx)
}
