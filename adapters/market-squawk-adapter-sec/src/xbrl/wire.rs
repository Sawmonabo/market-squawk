//! Bounded namespace-aware XML wire helpers shared by the XBRL parser and normalizer.

use std::collections::{BTreeMap, BTreeSet};

use market_squawk_domain::{
    CalendarDate, XbrlAccuracy, XbrlAccuracyValue, XbrlQualifiedName, XbrlSign, XbrlText,
};
use quick_xml::NsReader;
use quick_xml::events::BytesStart;
use quick_xml::name::{NamespaceResolver, QName, ResolveResult};

use crate::SecParserLimits;

use super::SecXbrlError;

pub(super) const IX_NAMESPACE: &str = "http://www.xbrl.org/2013/inlineXBRL";
pub(super) const XBRLI_NAMESPACE: &str = "http://www.xbrl.org/2003/instance";
pub(super) const XBRLDI_NAMESPACE: &str = "http://xbrl.org/2006/xbrldi";
const XML_NAMESPACE: &str = "http://www.w3.org/XML/1998/namespace";
const XSI_NAMESPACE: &str = "http://www.w3.org/2001/XMLSchema-instance";
const MAX_ATTRIBUTES: usize = 64;
const DOT_DECIMAL_TRANSFORMS: &[(&str, &str)] = &[
    (
        "http://www.xbrl.org/inlineXBRL/transformation/2010-04-20",
        "numdotdecimal",
    ),
    (
        "http://www.xbrl.org/inlineXBRL/transformation/2011-07-31",
        "numdotdecimal",
    ),
    (
        "http://www.xbrl.org/inlineXBRL/transformation/2015-02-26",
        "numdotdecimal",
    ),
    (
        "http://www.xbrl.org/inlineXBRL/transformation/2020-02-12",
        "num-dot-decimal",
    ),
    (
        "http://www.xbrl.org/inlineXBRL/transformation/2022-02-16",
        "num-dot-decimal",
    ),
];

const SEMANTIC_ATTRIBUTE_NAMES: &[&str] = &[
    "arcrole",
    "contextRef",
    "continuedAt",
    "decimals",
    "dimension",
    "format",
    "fromRefs",
    "id",
    "lang",
    "linkRole",
    "name",
    "nil",
    "order",
    "precision",
    "scale",
    "scheme",
    "sign",
    "toRefs",
    "unitRef",
];

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct ResolvedAttributes {
    values: Vec<ResolvedAttribute>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct ResolvedAttribute {
    pub(super) name: XbrlQualifiedName,
    pub(super) value: String,
}

impl ResolvedAttributes {
    pub(super) fn required_unqualified(&self, key: &str) -> Result<String, SecXbrlError> {
        self.unqualified(key)
            .map(str::to_owned)
            .ok_or(SecXbrlError::MissingAttribute)
    }

    pub(super) fn unqualified(&self, key: &str) -> Option<&str> {
        self.values
            .iter()
            .find(|attribute| {
                attribute.name.namespace_uri().is_none()
                    && attribute.name.local_name().as_str() == key
            })
            .map(|attribute| attribute.value.as_str())
    }

    pub(super) fn xml_or_unqualified(&self, key: &str) -> Option<&str> {
        self.namespaced(XML_NAMESPACE, key)
            .or_else(|| self.unqualified(key))
    }

    pub(super) fn namespaced(&self, namespace: &str, local: &str) -> Option<&str> {
        self.values
            .iter()
            .find(|attribute| {
                attribute.name.namespace_uri().map(|uri| uri.as_str()) == Some(namespace)
                    && attribute.name.local_name().as_str() == local
            })
            .map(|attribute| attribute.value.as_str())
    }

    pub(super) fn xsi_nil(&self) -> Option<&str> {
        self.namespaced(XSI_NAMESPACE, "nil")
    }

    pub(super) fn values(&self) -> &[ResolvedAttribute] {
        &self.values
    }
}

pub(super) fn attributes(
    reader: &NsReader<&[u8]>,
    start: &BytesStart<'_>,
    limits: SecParserLimits,
) -> Result<ResolvedAttributes, SecXbrlError> {
    let mut values = Vec::new();
    let mut expanded = BTreeSet::new();
    let mut semantic_counts = BTreeMap::<String, usize>::new();
    for attribute in start.attributes() {
        if values.len() >= MAX_ATTRIBUTES {
            return Err(SecXbrlError::AttributeLimitExceeded);
        }
        let attribute = attribute?;
        if attribute.key.as_namespace_binding().is_some() {
            continue;
        }
        let name = resolve_name(reader.resolver(), attribute.key, false, limits)?;
        let value = attribute
            .decoded_and_normalized_value(quick_xml::XmlVersion::Implicit1_0, reader.decoder())?
            .into_owned();
        if value.len() > limits.string_bytes() {
            return Err(SecXbrlError::StringLimitExceeded);
        }
        let expanded_key = (
            name.namespace_uri().map(|uri| uri.as_str().to_owned()),
            name.local_name().as_str().to_owned(),
        );
        if !expanded.insert(expanded_key) {
            return Err(SecXbrlError::DuplicateAttribute);
        }
        if SEMANTIC_ATTRIBUTE_NAMES.contains(&name.local_name().as_str()) {
            let count = semantic_counts
                .entry(name.local_name().as_str().to_owned())
                .or_insert(0);
            *count += 1;
            if *count > 1 {
                return Err(SecXbrlError::AmbiguousSemanticAttribute);
            }
        }
        values.push(ResolvedAttribute { name, value });
    }
    Ok(ResolvedAttributes { values })
}

pub(super) fn resolve_element_name(
    resolution: ResolveResult<'_>,
    name: QName<'_>,
    limits: SecParserLimits,
) -> Result<XbrlQualifiedName, SecXbrlError> {
    resolve_name_result(resolution, name, limits)
}

pub(super) fn resolve_qname_value(
    resolver: &NamespaceResolver,
    value: &str,
    limits: SecParserLimits,
) -> Result<XbrlQualifiedName, SecXbrlError> {
    if value.len() > limits.string_bytes() {
        return Err(SecXbrlError::StringLimitExceeded);
    }
    let qname = QName(value.as_bytes());
    let resolution = resolver.resolve_prefix(qname.prefix(), true);
    resolve_name_result(resolution, qname, limits)
}

fn resolve_name(
    resolver: &NamespaceResolver,
    name: QName<'_>,
    use_default: bool,
    limits: SecParserLimits,
) -> Result<XbrlQualifiedName, SecXbrlError> {
    let resolution = resolver.resolve_prefix(name.prefix(), use_default);
    resolve_name_result(resolution, name, limits)
}

fn resolve_name_result(
    resolution: ResolveResult<'_>,
    name: QName<'_>,
    limits: SecParserLimits,
) -> Result<XbrlQualifiedName, SecXbrlError> {
    let lexical = name_text(name.as_ref(), limits)?;
    match resolution {
        ResolveResult::Bound(namespace) => {
            let namespace = name_text(namespace.as_ref(), limits)?;
            XbrlQualifiedName::try_new(lexical, namespace).map_err(Into::into)
        }
        ResolveResult::Unbound => XbrlQualifiedName::unqualified(lexical).map_err(Into::into),
        ResolveResult::Unknown(_) => Err(SecXbrlError::UnknownNamespacePrefix),
    }
}

pub(super) fn is_element(name: &XbrlQualifiedName, namespace: &str, local: &str) -> bool {
    name.namespace_uri().map(|uri| uri.as_str()) == Some(namespace)
        && name.local_name().as_str() == local
}

pub(super) fn name_text(bytes: &[u8], limits: SecParserLimits) -> Result<&str, SecXbrlError> {
    if bytes.len() > limits.string_bytes() {
        return Err(SecXbrlError::StringLimitExceeded);
    }
    std::str::from_utf8(bytes).map_err(|_| SecXbrlError::InvalidUtf8)
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
    attributes: &ResolvedAttributes,
) -> Result<XbrlAccuracy, SecXbrlError> {
    match (
        attributes.unqualified("decimals"),
        attributes.unqualified("precision"),
    ) {
        (Some(_), Some(_)) => Err(SecXbrlError::ConflictingAccuracy),
        (Some(value), None) => Ok(XbrlAccuracy::Decimals(parse_accuracy_value(value, false)?)),
        (None, Some(value)) => Ok(XbrlAccuracy::Precision(parse_accuracy_value(value, true)?)),
        (None, None) => Ok(XbrlAccuracy::Unspecified),
    }
}

fn parse_accuracy_value(value: &str, precision: bool) -> Result<XbrlAccuracyValue, SecXbrlError> {
    if value.eq_ignore_ascii_case("INF") {
        Ok(XbrlAccuracyValue::Infinite)
    } else {
        let value = parse_i32(value)?;
        if precision && value <= 0 {
            Err(SecXbrlError::InvalidNumericFact)
        } else {
            Ok(XbrlAccuracyValue::Finite(value))
        }
    }
}

pub(super) fn transform_numeric(
    value: &str,
    format: Option<&XbrlQualifiedName>,
) -> Result<String, SecXbrlError> {
    match format {
        None => Ok(value.trim().to_owned()),
        Some(format) if is_supported_dot_decimal_transform(format) => {
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

fn is_supported_dot_decimal_transform(format: &XbrlQualifiedName) -> bool {
    DOT_DECIMAL_TRANSFORMS.iter().any(|(namespace, local)| {
        format.namespace_uri().map(XbrlText::as_str) == Some(*namespace)
            && format.local_name().as_str() == *local
    })
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
