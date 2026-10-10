//! Bounded namespace-aware XML wire helpers shared by the XBRL parser and normalizer.

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

/// Measures conservative XML scratch from a borrowed bounded pass before allocating the
/// namespace-aware reader. The pass owns only the pull reader's open-name buffers and a
/// depth-bounded stack; those are admitted first and dropped before the real parse begins.
/// Namespace and attribute maxima come from this exact input, not the schema-wide ceiling.
pub(super) fn parser_scratch_reservation(
    bytes: &[u8],
    limits: SecParserLimits,
    cancellation: &tokio_util::sync::CancellationToken,
) -> Result<usize, SecXbrlError> {
    use quick_xml::{Reader, events::Event};
    use std::mem::size_of;
    let overflow = || SecXbrlError::RetainedOutputLimitExceeded;
    let depth_slots = limits.depth().checked_add(1).ok_or_else(overflow)?;
    // A borrowed lexical upper bound precedes reader construction: every element name occurs
    // immediately after `<` or `</` and stops at XML whitespace, `/`, or `>`. Tokens inside
    // comments/CDATA may only overestimate it. This does not admit syntax; the real parse does.
    let name_bound = bytes
        .split(|byte| *byte == b'<')
        .skip(1)
        .map(|tail| tail.strip_prefix(b"/").unwrap_or(tail))
        .map(|tail| {
            tail.iter()
                .take_while(|byte| {
                    !matches!(
                        **byte,
                        b' ' | b'\t' | b'\r' | b'\n' | b'/' | b'>' | b'!' | b'?'
                    )
                })
                .count()
        })
        .max()
        .unwrap_or(0);
    let names = name_bound
        .checked_mul(depth_slots)
        .ok_or_else(overflow)?
        .min(bytes.len());
    // Depth+1 also covers the next rejected start event, which the reader owns before returning
    // it. Duplicate checking is disabled only in this sizing pass, avoiding a key table here.
    let preflight = names
        .checked_mul(2)
        .and_then(|n| n.checked_add(depth_slots.checked_mul(8 * size_of::<usize>())?))
        .and_then(|n| n.checked_add(size_of::<Reader<&[u8]>>() + 256))
        .ok_or_else(overflow)?;
    if preflight > limits.retained_output_bytes() {
        return Err(overflow());
    }
    let mut frames = Vec::<(usize, usize, usize)>::new();
    frames
        .try_reserve_exact(depth_slots)
        .map_err(|_| overflow())?;
    if frames.capacity() > depth_slots.saturating_mul(2) {
        return Err(overflow());
    }
    let mut reader = Reader::from_reader(bytes);
    reader.config_mut().check_end_names = false;
    reader.config_mut().allow_unmatched_ends = true;
    let mut open_names = 0usize;
    let mut namespace_bytes = XML_NAMESPACE.len() + 64;
    let mut namespace_count = 2usize;
    let mut max_open_names = 0usize;
    let mut max_namespace_bytes = namespace_bytes;
    let mut max_namespace_count = namespace_count;
    let mut max_namespace = XML_NAMESPACE.len();
    let mut max_name = 0usize;
    let mut max_depth = 0usize;
    let mut max_attributes = 0usize;
    let mut max_all_attributes = 0usize;
    let mut max_attribute_dynamic = 0usize;
    let mut max_decoded_attribute = 0usize;
    let mut max_decoded_text = 0usize;
    loop {
        super::check_xbrl_cancelled(cancellation)?;
        let event = reader.read_event()?;
        let empty = matches!(&event, Event::Empty(_));
        match event {
            Event::Start(start) | Event::Empty(start) => {
                let name_bytes = start.name().as_ref().len();
                if name_bytes > limits.string_bytes() {
                    return Err(SecXbrlError::StringLimitExceeded);
                }
                let mut ns_bytes = 0usize;
                let mut ns_count = 0usize;
                let mut attributes = 0usize;
                let mut all_attributes = 0usize;
                let mut dynamic = 0usize;
                for attribute in start.attributes().with_checks(false) {
                    let attribute = attribute?;
                    all_attributes = all_attributes.checked_add(1).ok_or_else(overflow)?;
                    if attribute.key.as_namespace_binding().is_some() {
                        ns_bytes = super::checked_retained_sum([
                            ns_bytes,
                            attribute.key.as_ref().len(),
                            attribute.value.len(),
                        ])?;
                        ns_count = ns_count.checked_add(1).ok_or_else(overflow)?;
                        max_namespace = max_namespace.max(attribute.value.len());
                        continue;
                    }
                    attributes = attributes.checked_add(1).ok_or_else(overflow)?;
                    if attributes > MAX_ATTRIBUTES {
                        return Err(SecXbrlError::AttributeLimitExceeded);
                    }
                    if attribute.key.as_ref().len() > limits.string_bytes() {
                        return Err(SecXbrlError::StringLimitExceeded);
                    }
                    let decoded = reader
                        .decoder()
                        .encoding()
                        .new_decoder_without_bom_handling()
                        .max_utf8_buffer_length(attribute.value.len())
                        .ok_or_else(overflow)?;
                    max_decoded_attribute = max_decoded_attribute.max(decoded);
                    dynamic = super::checked_retained_sum([
                        dynamic,
                        attribute
                            .key
                            .as_ref()
                            .len()
                            .checked_mul(2)
                            .ok_or_else(overflow)?,
                        decoded,
                    ])?;
                }
                let depth = frames.len().checked_add(1).ok_or_else(overflow)?;
                if depth > limits.depth() {
                    return Err(SecXbrlError::DepthLimitExceeded);
                }
                max_depth = max_depth.max(depth);
                max_name = max_name.max(name_bytes);
                max_open_names =
                    max_open_names.max(open_names.checked_add(name_bytes).ok_or_else(overflow)?);
                max_namespace_bytes = max_namespace_bytes
                    .max(namespace_bytes.checked_add(ns_bytes).ok_or_else(overflow)?);
                max_namespace_count = max_namespace_count
                    .max(namespace_count.checked_add(ns_count).ok_or_else(overflow)?);
                max_attributes = max_attributes.max(attributes);
                max_all_attributes = max_all_attributes.max(all_attributes);
                max_attribute_dynamic = max_attribute_dynamic.max(dynamic);
                if !empty {
                    open_names = open_names.checked_add(name_bytes).ok_or_else(overflow)?;
                    namespace_bytes = namespace_bytes.checked_add(ns_bytes).ok_or_else(overflow)?;
                    namespace_count = namespace_count.checked_add(ns_count).ok_or_else(overflow)?;
                    frames.push((name_bytes, ns_bytes, ns_count));
                }
            }
            Event::End(end) => {
                if end.name().as_ref().len() > limits.string_bytes() {
                    return Err(SecXbrlError::StringLimitExceeded);
                }
                max_name = max_name.max(end.name().as_ref().len());
                let (names, ns_bytes, ns_count) =
                    frames.pop().ok_or(SecXbrlError::UnexpectedEof)?;
                open_names -= names;
                namespace_bytes -= ns_bytes;
                namespace_count -= ns_count;
            }
            Event::Text(text) => {
                let decoded = reader
                    .decoder()
                    .encoding()
                    .new_decoder_without_bom_handling()
                    .max_utf8_buffer_length(text.len())
                    .ok_or_else(overflow)?;
                max_decoded_text = max_decoded_text.max(decoded);
            }
            Event::CData(text) => {
                let decoded = reader
                    .decoder()
                    .encoding()
                    .new_decoder_without_bom_handling()
                    .max_utf8_buffer_length(text.len())
                    .ok_or_else(overflow)?;
                max_decoded_text = max_decoded_text.max(decoded);
            }
            Event::DocType(_) => return Err(SecXbrlError::DoctypeForbidden),
            Event::Eof => break,
            _ => {}
        }
    }
    if !frames.is_empty() {
        return Err(SecXbrlError::UnexpectedEof);
    }
    // quick-xml 0.41: four machine words per namespace binding, one per open-name
    // index, two per duplicate attribute range. Include minimum Vec capacities and
    // the owned end-name for expand_empty_elements. These coexist with event scratch.
    let reader_owned = super::checked_retained_sum([
        max_open_names,
        max_namespace_bytes,
        max_name,
        max_namespace_count
            .max(4)
            .checked_mul(4 * size_of::<usize>())
            .ok_or_else(overflow)?,
        max_depth
            .max(4)
            .checked_mul(size_of::<usize>())
            .ok_or_else(overflow)?,
        max_all_attributes
            .max(4)
            .checked_mul(2 * size_of::<usize>())
            .ok_or_else(overflow)?,
    ])?
    .checked_mul(2)
    .ok_or_else(overflow)?;
    let attribute_owned = super::checked_retained_sum([
        max_attributes
            .max(4)
            .checked_mul(size_of::<ResolvedAttribute>())
            .ok_or_else(overflow)?,
        max_attribute_dynamic,
        max_attributes
            .checked_mul(max_namespace)
            .ok_or_else(overflow)?,
        max_name.checked_mul(2).ok_or_else(overflow)?,
        max_namespace,
        max_decoded_attribute,
    ])?
    .checked_mul(4)
    .ok_or_else(overflow)?;
    // Decoding and unescaping can each own one complete text buffer. Attribute/QName
    // resolution can also coexist with copies made by state.start before draft admission.
    let event_owned = attribute_owned.max(max_decoded_text.checked_mul(4).ok_or_else(overflow)?);
    super::checked_retained_sum([reader_owned, event_owned, size_of::<NsReader<&[u8]>>(), 256])
}

pub(super) fn attributes(
    reader: &NsReader<&[u8]>,
    start: &BytesStart<'_>,
    limits: SecParserLimits,
) -> Result<ResolvedAttributes, SecXbrlError> {
    let mut values = Vec::<ResolvedAttribute>::new();
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
        if values
            .iter()
            .any(|existing| existing.name.same_expanded_name(&name))
        {
            return Err(SecXbrlError::DuplicateAttribute);
        }
        if SEMANTIC_ATTRIBUTE_NAMES.contains(&name.local_name().as_str())
            && values
                .iter()
                .any(|existing| existing.name.local_name() == name.local_name())
        {
            return Err(SecXbrlError::AmbiguousSemanticAttribute);
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
        Some(format)
            if is_element(
                format,
                "http://www.xbrl.org/inlineXBRL/transformation/2020-02-12",
                "fixed-zero",
            ) =>
        {
            // Transformation Registry 4 section 4.93 accepts any xs:string as zero.
            // The normalizer separately retains the source text, scale and sign.
            Ok("0".to_owned())
        }
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
        Some(format)
            if format.namespace_uri().map(XbrlText::as_str)
                == Some("http://www.sec.gov/inlineXBRL/transformation/2015-08-31")
                && format.local_name().as_str() == "numwordsen" =>
        {
            super::number_words::transform(value)
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
