//! Versioned, bounded evidence retained for normalized XBRL numeric facts.

use std::collections::BTreeSet;
use std::fmt;
use std::str::FromStr;

use rust_decimal::Decimal;
use serde::de::{IgnoredAny, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize};

use crate::{CalendarDate, EvidenceDigest, ExactPayloadEvidence, SourceIdentifier, Timestamp};

/// Current schema version for [`XbrlFactEvidence`].
pub const XBRL_FACT_EVIDENCE_SCHEMA_VERSION: u16 = 2;

/// Maximum dimensions retained for one XBRL context.
pub const MAX_XBRL_DIMENSIONS: usize = 128;

/// Maximum structural events retained for one context or typed-member graph.
pub const MAX_XBRL_GRAPH_EVENTS: usize = 4_096;

/// Maximum measures retained on either side of one XBRL divide unit.
pub const MAX_XBRL_UNIT_MEASURES: usize = 64;

/// Maximum fact references retained for one Inline XBRL relationship endpoint.
pub const MAX_XBRL_RELATIONSHIP_REFS: usize = 128;

/// Maximum Inline XBRL relationships retained for one occurrence.
pub const MAX_XBRL_RELATIONSHIPS: usize = 128;

/// A bounded exact XBRL text fragment.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct XbrlText(String);

impl XbrlText {
    /// Maximum UTF-8 bytes retained by one exact text fragment.
    pub const MAX_LENGTH: usize = 65_536;

    /// Returns the exact retained text.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Returns heap bytes retained by the owned string allocation.
    pub fn retained_bytes(&self) -> usize {
        self.0.capacity()
    }
}

impl TryFrom<String> for XbrlText {
    type Error = XbrlEvidenceError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        if value.len() > Self::MAX_LENGTH {
            Err(XbrlEvidenceError::TextTooLong)
        } else {
            Ok(Self(value))
        }
    }
}

impl TryFrom<&str> for XbrlText {
    type Error = XbrlEvidenceError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::try_from(value.to_owned())
    }
}

impl<'de> Deserialize<'de> for XbrlText {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Self::try_from(String::deserialize(deserializer)?).map_err(serde::de::Error::custom)
    }
}

/// A source QName together with its authoritative expanded name.
///
/// The lexical QName retains the filing's prefix for audit evidence. Semantic comparisons must use
/// [`Self::same_expanded_name`], which compares only namespace URI and local name.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(deny_unknown_fields)]
pub struct XbrlQualifiedName {
    source_qname: SourceIdentifier,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    namespace_uri: Option<XbrlText>,
    local_name: SourceIdentifier,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct XbrlQualifiedNameWire {
    source_qname: SourceIdentifier,
    #[serde(default)]
    namespace_uri: Option<XbrlText>,
    local_name: SourceIdentifier,
}

impl<'de> Deserialize<'de> for XbrlQualifiedName {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = XbrlQualifiedNameWire::deserialize(deserializer)?;
        let candidate = match wire.namespace_uri {
            Some(namespace_uri) => {
                Self::try_new(wire.source_qname.as_str(), namespace_uri.as_str())
            }
            None => Self::unqualified(wire.source_qname.as_str()),
        }
        .map_err(serde::de::Error::custom)?;
        if candidate.local_name != wire.local_name {
            return Err(serde::de::Error::custom(
                XbrlEvidenceError::QualifiedNameMismatch,
            ));
        }
        Ok(candidate)
    }
}

impl XbrlQualifiedName {
    /// Constructs a source QName resolved to an authoritative namespace URI.
    pub fn try_new(source_qname: &str, namespace_uri: &str) -> Result<Self, XbrlEvidenceError> {
        if namespace_uri.is_empty() {
            return Err(XbrlEvidenceError::EmptyRequiredText);
        }
        let local_name = validate_source_qname(source_qname)?;
        Ok(Self {
            source_qname: SourceIdentifier::try_from(source_qname)
                .map_err(|_| XbrlEvidenceError::InvalidQualifiedName)?,
            namespace_uri: Some(XbrlText::try_from(namespace_uri)?),
            local_name: SourceIdentifier::try_from(local_name)
                .map_err(|_| XbrlEvidenceError::InvalidQualifiedName)?,
        })
    }

    /// Constructs an explicitly unqualified XML name.
    pub fn unqualified(source_name: &str) -> Result<Self, XbrlEvidenceError> {
        let local_name = validate_source_qname(source_name)?;
        if source_name.contains(':') {
            return Err(XbrlEvidenceError::UnboundQualifiedName);
        }
        Ok(Self {
            source_qname: SourceIdentifier::try_from(source_name)
                .map_err(|_| XbrlEvidenceError::InvalidQualifiedName)?,
            namespace_uri: None,
            local_name: SourceIdentifier::try_from(local_name)
                .map_err(|_| XbrlEvidenceError::InvalidQualifiedName)?,
        })
    }

    /// Returns the exact source QName, including its lexical prefix when present.
    pub const fn source_qname(&self) -> &SourceIdentifier {
        &self.source_qname
    }

    /// Returns the resolved namespace URI, or `None` for an explicitly unqualified name.
    pub const fn namespace_uri(&self) -> Option<&XbrlText> {
        self.namespace_uri.as_ref()
    }

    /// Returns the expanded local name.
    pub const fn local_name(&self) -> &SourceIdentifier {
        &self.local_name
    }

    /// Reports whether two source QNames resolve to the same expanded name.
    pub fn same_expanded_name(&self, other: &Self) -> bool {
        self.namespace_uri == other.namespace_uri && self.local_name == other.local_name
    }
}

fn validate_source_qname(source_qname: &str) -> Result<&str, XbrlEvidenceError> {
    if source_qname.is_empty() {
        return Err(XbrlEvidenceError::EmptyRequiredText);
    }
    let mut parts = source_qname.split(':');
    let first = parts
        .next()
        .ok_or(XbrlEvidenceError::InvalidQualifiedName)?;
    let second = parts.next();
    if parts.next().is_some() || first.is_empty() || second.is_some_and(str::is_empty) {
        return Err(XbrlEvidenceError::InvalidQualifiedName);
    }
    Ok(second.unwrap_or(first))
}

/// One event in a bounded, non-recursive XML evidence graph.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "snake_case", tag = "kind")]
pub enum XbrlXmlEvent {
    /// Opens one element.
    Start { name: XbrlQualifiedName },
    /// Retains one source attribute immediately after its owning start event.
    Attribute {
        name: XbrlQualifiedName,
        value: XbrlText,
    },
    /// Retains source character content.
    Text { value: XbrlText },
    /// Closes one element.
    End { name: XbrlQualifiedName },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(transparent)]
struct BoundedXmlEvents(Vec<XbrlXmlEvent>);

impl BoundedXmlEvents {
    fn try_new(events: Vec<XbrlXmlEvent>) -> Result<Self, XbrlEvidenceError> {
        if events.len() > MAX_XBRL_GRAPH_EVENTS {
            return Err(XbrlEvidenceError::TooManyGraphEvents);
        }
        validate_xml_events(&events)?;
        Ok(Self(events.into_boxed_slice().into_vec()))
    }
}

struct BoundedXmlEventsVisitor;

impl<'de> Visitor<'de> for BoundedXmlEventsVisitor {
    type Value = BoundedXmlEvents;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a bounded balanced XBRL XML event list")
    }

    fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let mut events = Vec::with_capacity(sequence.size_hint().unwrap_or(0).min(32));
        while events.len() < MAX_XBRL_GRAPH_EVENTS {
            let Some(event) = sequence.next_element()? else {
                return BoundedXmlEvents::try_new(events).map_err(serde::de::Error::custom);
            };
            events.push(event);
        }
        if sequence.next_element::<IgnoredAny>()?.is_some() {
            Err(serde::de::Error::custom(
                XbrlEvidenceError::TooManyGraphEvents,
            ))
        } else {
            BoundedXmlEvents::try_new(events).map_err(serde::de::Error::custom)
        }
    }
}

impl<'de> Deserialize<'de> for BoundedXmlEvents {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_seq(BoundedXmlEventsVisitor)
    }
}

fn validate_xml_events(events: &[XbrlXmlEvent]) -> Result<(), XbrlEvidenceError> {
    let mut stack = Vec::<&XbrlQualifiedName>::new();
    let mut attributes_open = false;
    let mut attributes = BTreeSet::<(&Option<XbrlText>, &SourceIdentifier)>::new();
    for event in events {
        match event {
            XbrlXmlEvent::Start { name } => {
                stack.push(name);
                attributes_open = true;
                attributes.clear();
            }
            XbrlXmlEvent::Attribute { name, .. } => {
                if stack.is_empty() || !attributes_open {
                    return Err(XbrlEvidenceError::InvalidGraphStructure);
                }
                if !attributes.insert((&name.namespace_uri, &name.local_name)) {
                    return Err(XbrlEvidenceError::DuplicateGraphAttribute);
                }
            }
            XbrlXmlEvent::Text { .. } => {
                if stack.is_empty() {
                    return Err(XbrlEvidenceError::InvalidGraphStructure);
                }
                attributes_open = false;
            }
            XbrlXmlEvent::End { name } => {
                attributes_open = false;
                let start = stack
                    .pop()
                    .ok_or(XbrlEvidenceError::InvalidGraphStructure)?;
                if start != name {
                    return Err(XbrlEvidenceError::InvalidGraphStructure);
                }
            }
        }
    }
    if stack.is_empty() {
        Ok(())
    } else {
        Err(XbrlEvidenceError::InvalidGraphStructure)
    }
}

/// Bounded balanced source-only XML evidence retained without recursive ownership.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct XbrlContextGraph {
    events: BoundedXmlEvents,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct XbrlContextGraphWire {
    events: BoundedXmlEvents,
}

impl<'de> Deserialize<'de> for XbrlContextGraph {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = XbrlContextGraphWire::deserialize(deserializer)?;
        Self::try_new(wire.events.0).map_err(serde::de::Error::custom)
    }
}

impl XbrlContextGraph {
    /// Constructs a bounded balanced XML evidence graph.
    pub fn try_new(events: Vec<XbrlXmlEvent>) -> Result<Self, XbrlEvidenceError> {
        Ok(Self {
            events: BoundedXmlEvents::try_new(events)?,
        })
    }

    /// Constructs an empty graph for a context with no segment or scenario content.
    pub const fn empty() -> Self {
        Self {
            events: BoundedXmlEvents(Vec::new()),
        }
    }

    /// Returns ordered structural events.
    pub fn events(&self) -> &[XbrlXmlEvent] {
        &self.events.0
    }
}

/// Source-reported entity identity for an XBRL context.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct XbrlEntity {
    scheme: XbrlText,
    value: XbrlText,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct XbrlEntityWire {
    scheme: XbrlText,
    value: XbrlText,
}

impl<'de> Deserialize<'de> for XbrlEntity {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = XbrlEntityWire::deserialize(deserializer)?;
        Self::try_new(wire.scheme.as_str(), wire.value.as_str()).map_err(serde::de::Error::custom)
    }
}

impl XbrlEntity {
    /// Constructs bounded scheme and identifier evidence.
    pub fn try_new(scheme: &str, value: &str) -> Result<Self, XbrlEvidenceError> {
        if scheme.is_empty() || value.is_empty() {
            return Err(XbrlEvidenceError::EmptyRequiredText);
        }
        Ok(Self {
            scheme: scheme.try_into()?,
            value: value.try_into()?,
        })
    }

    /// Returns the source-reported identifier scheme.
    pub const fn scheme(&self) -> &XbrlText {
        &self.scheme
    }

    /// Returns the source-reported identifier.
    pub const fn value(&self) -> &XbrlText {
        &self.value
    }
}

/// Exact period semantics from an XBRL context.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "snake_case", tag = "kind")]
pub enum XbrlPeriod {
    /// A fact measured at one date.
    Instant {
        /// Measurement date.
        instant: CalendarDate,
    },
    /// A fact measured over a non-inverted date interval.
    Duration {
        /// Inclusive source-reported start date.
        start: CalendarDate,
        /// Source-reported end date.
        end: CalendarDate,
    },
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields, rename_all = "snake_case", tag = "kind")]
enum XbrlPeriodWire {
    Instant {
        instant: CalendarDate,
    },
    Duration {
        start: CalendarDate,
        end: CalendarDate,
    },
}

impl<'de> Deserialize<'de> for XbrlPeriod {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        match XbrlPeriodWire::deserialize(deserializer)? {
            XbrlPeriodWire::Instant { instant } => Ok(Self::instant(instant)),
            XbrlPeriodWire::Duration { start, end } => {
                Self::duration(start, end).map_err(serde::de::Error::custom)
            }
        }
    }
}

impl XbrlPeriod {
    /// Constructs instant-period evidence.
    pub const fn instant(instant: CalendarDate) -> Self {
        Self::Instant { instant }
    }

    /// Constructs duration-period evidence.
    pub fn duration(start: CalendarDate, end: CalendarDate) -> Result<Self, XbrlEvidenceError> {
        if start > end {
            Err(XbrlEvidenceError::InvertedPeriod)
        } else {
            Ok(Self::Duration { start, end })
        }
    }
}

/// Finite or source-reported infinite XBRL accuracy.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case", tag = "kind", content = "value")]
pub enum XbrlAccuracyValue {
    /// Signed finite decimals or precision value.
    Finite(i32),
    /// The source used `INF`.
    Infinite,
}

/// Mutually exclusive XBRL decimals/precision evidence.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case", tag = "kind", content = "value")]
pub enum XbrlAccuracy {
    /// The fact supplied a `decimals` attribute.
    Decimals(XbrlAccuracyValue),
    /// The fact supplied a `precision` attribute.
    Precision(XbrlAccuracyValue),
    /// Neither accuracy attribute was supplied.
    Unspecified,
}

/// Inline-XBRL sign transform retained independently from lexical text.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum XbrlSign {
    /// Positive sign transform.
    Positive,
    /// Negative sign transform.
    Negative,
}

/// Explicit or typed XBRL dimension member.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "snake_case", tag = "kind")]
pub enum XbrlDimensionMember {
    /// QName of an explicit member.
    Explicit { member: XbrlQualifiedName },
    /// Bounded source-only graph of a typed member whose taxonomy semantics were not validated.
    Typed {
        source_graph: XbrlContextGraph,
        validation: XbrlTypedMemberValidation,
    },
}

/// Validation authority retained for a typed-member value.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum XbrlTypedMemberValidation {
    /// The parser retained bounded source structure but did not resolve taxonomy semantics.
    SourceOnly,
}

/// One context dimension with explicit segment/scenario placement.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct XbrlDimensionEvidence {
    dimension: XbrlQualifiedName,
    member: XbrlDimensionMember,
    location: XbrlDimensionLocation,
}

impl XbrlDimensionEvidence {
    /// Constructs dimension evidence.
    pub const fn new(
        dimension: XbrlQualifiedName,
        member: XbrlDimensionMember,
        location: XbrlDimensionLocation,
    ) -> Self {
        Self {
            dimension,
            member,
            location,
        }
    }

    /// Returns the resolved dimension QName.
    pub const fn dimension(&self) -> &XbrlQualifiedName {
        &self.dimension
    }

    /// Returns explicit or source-only typed-member evidence.
    pub const fn member(&self) -> &XbrlDimensionMember {
        &self.member
    }

    /// Returns whether the dimension appeared in segment or scenario.
    pub const fn location(&self) -> XbrlDimensionLocation {
        self.location
    }
}

/// Context container carrying an XBRL dimension.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum XbrlDimensionLocation {
    /// Entity segment.
    Segment,
    /// Context scenario.
    Scenario,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(transparent)]
struct BoundedDimensions(Vec<XbrlDimensionEvidence>);

impl BoundedDimensions {
    fn try_new(dimensions: Vec<XbrlDimensionEvidence>) -> Result<Self, XbrlEvidenceError> {
        if dimensions.len() > MAX_XBRL_DIMENSIONS {
            Err(XbrlEvidenceError::TooManyDimensions)
        } else {
            Ok(Self(dimensions.into_boxed_slice().into_vec()))
        }
    }
}

struct BoundedDimensionsVisitor;

impl<'de> Visitor<'de> for BoundedDimensionsVisitor {
    type Value = BoundedDimensions;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a bounded XBRL dimension list")
    }

    fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let mut dimensions = Vec::with_capacity(sequence.size_hint().unwrap_or(0).min(16));
        while dimensions.len() < MAX_XBRL_DIMENSIONS {
            let Some(dimension) = sequence.next_element()? else {
                return Ok(BoundedDimensions(dimensions.into_boxed_slice().into_vec()));
            };
            dimensions.push(dimension);
        }
        if sequence.next_element::<IgnoredAny>()?.is_some() {
            Err(serde::de::Error::custom(
                XbrlEvidenceError::TooManyDimensions,
            ))
        } else {
            Ok(BoundedDimensions(dimensions.into_boxed_slice().into_vec()))
        }
    }
}

impl<'de> Deserialize<'de> for BoundedDimensions {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_seq(BoundedDimensionsVisitor)
    }
}

/// One simple or divided XBRL unit expression with lexical and expanded measure QNames.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(transparent)]
pub struct XbrlUnitExpression(XbrlUnitExpressionKind);

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "snake_case", tag = "kind")]
enum XbrlUnitExpressionKind {
    /// One measure QName.
    Measure { measure: XbrlQualifiedName },
    /// A nonempty numerator divided by a nonempty denominator.
    Divide {
        numerator: Vec<XbrlQualifiedName>,
        denominator: Vec<XbrlQualifiedName>,
    },
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields, rename_all = "snake_case", tag = "kind")]
enum XbrlUnitExpressionWire {
    Measure {
        measure: XbrlQualifiedName,
    },
    Divide {
        numerator: BoundedQualifiedNames,
        denominator: BoundedQualifiedNames,
    },
}

impl<'de> Deserialize<'de> for XbrlUnitExpression {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        match XbrlUnitExpressionWire::deserialize(deserializer)? {
            XbrlUnitExpressionWire::Measure { measure } => Ok(Self::measure(measure)),
            XbrlUnitExpressionWire::Divide {
                numerator,
                denominator,
            } => Self::divide(numerator.0, denominator.0).map_err(serde::de::Error::custom),
        }
    }
}

impl XbrlUnitExpression {
    /// Constructs a simple measure unit.
    pub const fn measure(measure: XbrlQualifiedName) -> Self {
        Self(XbrlUnitExpressionKind::Measure { measure })
    }

    /// Constructs a divided unit and rejects empty or cancelling sides.
    pub fn divide(
        numerator: Vec<XbrlQualifiedName>,
        denominator: Vec<XbrlQualifiedName>,
    ) -> Result<Self, XbrlEvidenceError> {
        if numerator.is_empty() || denominator.is_empty() {
            return Err(XbrlEvidenceError::EmptyUnitSide);
        }
        if numerator.len() > MAX_XBRL_UNIT_MEASURES || denominator.len() > MAX_XBRL_UNIT_MEASURES {
            return Err(XbrlEvidenceError::TooManyUnitMeasures);
        }
        if numerator.iter().any(|left| {
            denominator
                .iter()
                .any(|right| left.same_expanded_name(right))
        }) {
            return Err(XbrlEvidenceError::CancellingUnitMeasure);
        }
        Ok(Self(XbrlUnitExpressionKind::Divide {
            numerator: numerator.into_boxed_slice().into_vec(),
            denominator: denominator.into_boxed_slice().into_vec(),
        }))
    }

    /// Returns a stable lexical identifier while preserving the full typed expression separately.
    pub fn source_identifier(&self) -> Result<SourceIdentifier, XbrlEvidenceError> {
        let value = match &self.0 {
            XbrlUnitExpressionKind::Measure { measure } => {
                measure.source_qname().as_str().to_owned()
            }
            XbrlUnitExpressionKind::Divide {
                numerator,
                denominator,
            } => format!(
                "divide({}/{})",
                join_source_qnames(numerator),
                join_source_qnames(denominator)
            ),
        };
        SourceIdentifier::try_from(value).map_err(|_| XbrlEvidenceError::UnitIdentifierTooLong)
    }

    /// Reports semantic equality using expanded-name measure multisets on each side.
    pub fn same_semantics(&self, other: &Self) -> bool {
        match (&self.0, &other.0) {
            (
                XbrlUnitExpressionKind::Measure { measure: left },
                XbrlUnitExpressionKind::Measure { measure: right },
            ) => left.same_expanded_name(right),
            (
                XbrlUnitExpressionKind::Divide {
                    numerator: left_numerator,
                    denominator: left_denominator,
                },
                XbrlUnitExpressionKind::Divide {
                    numerator: right_numerator,
                    denominator: right_denominator,
                },
            ) => {
                expanded_name_multisets_equal(left_numerator, right_numerator)
                    && expanded_name_multisets_equal(left_denominator, right_denominator)
            }
            _ => false,
        }
    }

    /// Returns the simple measure, if this is not a divide unit.
    pub const fn measure_name(&self) -> Option<&XbrlQualifiedName> {
        match &self.0 {
            XbrlUnitExpressionKind::Measure { measure } => Some(measure),
            XbrlUnitExpressionKind::Divide { .. } => None,
        }
    }

    /// Returns numerator and denominator measures for a divide unit.
    pub fn divide_parts(&self) -> Option<(&[XbrlQualifiedName], &[XbrlQualifiedName])> {
        match &self.0 {
            XbrlUnitExpressionKind::Measure { .. } => None,
            XbrlUnitExpressionKind::Divide {
                numerator,
                denominator,
            } => Some((numerator, denominator)),
        }
    }
}

fn join_source_qnames(names: &[XbrlQualifiedName]) -> String {
    names
        .iter()
        .map(|name| name.source_qname().as_str())
        .collect::<Vec<_>>()
        .join("*")
}

fn expanded_name_multisets_equal(left: &[XbrlQualifiedName], right: &[XbrlQualifiedName]) -> bool {
    if left.len() != right.len() {
        return false;
    }
    let mut matched = vec![false; right.len()];
    for left_name in left {
        let Some((index, _)) = right.iter().enumerate().find(|(index, right_name)| {
            !matched[*index] && left_name.same_expanded_name(right_name)
        }) else {
            return false;
        };
        matched[index] = true;
    }
    true
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(transparent)]
struct BoundedQualifiedNames(Vec<XbrlQualifiedName>);

struct BoundedQualifiedNamesVisitor;

impl<'de> Visitor<'de> for BoundedQualifiedNamesVisitor {
    type Value = BoundedQualifiedNames;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a bounded XBRL unit measure list")
    }

    fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let mut names = Vec::with_capacity(sequence.size_hint().unwrap_or(0).min(8));
        while names.len() < MAX_XBRL_UNIT_MEASURES {
            let Some(name) = sequence.next_element()? else {
                return Ok(BoundedQualifiedNames(names.into_boxed_slice().into_vec()));
            };
            names.push(name);
        }
        if sequence.next_element::<IgnoredAny>()?.is_some() {
            Err(serde::de::Error::custom(
                XbrlEvidenceError::TooManyUnitMeasures,
            ))
        } else {
            Ok(BoundedQualifiedNames(names.into_boxed_slice().into_vec()))
        }
    }
}

impl<'de> Deserialize<'de> for BoundedQualifiedNames {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_seq(BoundedQualifiedNamesVisitor)
    }
}

/// One retained Inline XBRL relationship edge.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct XbrlRelationshipEvidence {
    arcrole: SourceIdentifier,
    from_refs: Vec<SourceIdentifier>,
    to_refs: Vec<SourceIdentifier>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    link_role: Option<SourceIdentifier>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    order: Option<XbrlText>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct XbrlRelationshipEvidenceWire {
    arcrole: SourceIdentifier,
    from_refs: BoundedSourceIdentifiers,
    to_refs: BoundedSourceIdentifiers,
    #[serde(default)]
    link_role: Option<SourceIdentifier>,
    #[serde(default)]
    order: Option<XbrlText>,
}

impl<'de> Deserialize<'de> for XbrlRelationshipEvidence {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = XbrlRelationshipEvidenceWire::deserialize(deserializer)?;
        Self::try_new(
            wire.arcrole,
            wire.from_refs.0,
            wire.to_refs.0,
            wire.link_role,
            wire.order,
        )
        .map_err(serde::de::Error::custom)
    }
}

impl XbrlRelationshipEvidence {
    /// Constructs a bounded many-to-many Inline XBRL relationship edge.
    pub fn try_new(
        arcrole: SourceIdentifier,
        from_refs: Vec<SourceIdentifier>,
        to_refs: Vec<SourceIdentifier>,
        link_role: Option<SourceIdentifier>,
        order: Option<XbrlText>,
    ) -> Result<Self, XbrlEvidenceError> {
        validate_source_identifier_set(&from_refs, MAX_XBRL_RELATIONSHIP_REFS, true)?;
        validate_source_identifier_set(&to_refs, MAX_XBRL_RELATIONSHIP_REFS, true)?;
        Ok(Self {
            arcrole,
            from_refs: from_refs.into_boxed_slice().into_vec(),
            to_refs: to_refs.into_boxed_slice().into_vec(),
            link_role,
            order,
        })
    }

    /// Returns source occurrence IDs at the relationship's origin.
    pub fn from_refs(&self) -> &[SourceIdentifier] {
        &self.from_refs
    }

    /// Returns source occurrence IDs at the relationship's destination.
    pub fn to_refs(&self) -> &[SourceIdentifier] {
        &self.to_refs
    }
}

/// Bounded nesting, continuation, and relationship evidence incident to one occurrence.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct XbrlOccurrenceRelationships {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    parent_occurrence_id: Option<SourceIdentifier>,
    child_occurrence_ids: Vec<SourceIdentifier>,
    continuation_chain: Vec<SourceIdentifier>,
    relationships: Vec<XbrlRelationshipEvidence>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct XbrlOccurrenceRelationshipsWire {
    #[serde(default)]
    parent_occurrence_id: Option<SourceIdentifier>,
    child_occurrence_ids: BoundedSourceIdentifiers,
    continuation_chain: BoundedSourceIdentifiers,
    relationships: BoundedRelationships,
}

impl<'de> Deserialize<'de> for XbrlOccurrenceRelationships {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = XbrlOccurrenceRelationshipsWire::deserialize(deserializer)?;
        Self::try_new(
            wire.parent_occurrence_id,
            wire.child_occurrence_ids.0,
            wire.continuation_chain.0,
            wire.relationships.0,
        )
        .map_err(serde::de::Error::custom)
    }
}

impl XbrlOccurrenceRelationships {
    /// Constructs bounded relationship evidence without discarding source occurrences.
    pub fn try_new(
        parent_occurrence_id: Option<SourceIdentifier>,
        child_occurrence_ids: Vec<SourceIdentifier>,
        continuation_chain: Vec<SourceIdentifier>,
        relationships: Vec<XbrlRelationshipEvidence>,
    ) -> Result<Self, XbrlEvidenceError> {
        validate_source_identifier_set(&child_occurrence_ids, MAX_XBRL_RELATIONSHIP_REFS, false)?;
        validate_source_identifier_set(&continuation_chain, MAX_XBRL_RELATIONSHIP_REFS, false)?;
        if relationships.len() > MAX_XBRL_RELATIONSHIPS {
            return Err(XbrlEvidenceError::TooManyRelationships);
        }
        Ok(Self {
            parent_occurrence_id,
            child_occurrence_ids: child_occurrence_ids.into_boxed_slice().into_vec(),
            continuation_chain: continuation_chain.into_boxed_slice().into_vec(),
            relationships: relationships.into_boxed_slice().into_vec(),
        })
    }

    /// Constructs evidence for an occurrence with no graph edges.
    pub const fn empty() -> Self {
        Self {
            parent_occurrence_id: None,
            child_occurrence_ids: Vec::new(),
            continuation_chain: Vec::new(),
            relationships: Vec::new(),
        }
    }

    /// Validates that graph edges do not self-reference their owning occurrence.
    fn validate_owner(&self, occurrence_id: &SourceIdentifier) -> Result<(), XbrlEvidenceError> {
        if self.parent_occurrence_id.as_ref() == Some(occurrence_id)
            || self.child_occurrence_ids.contains(occurrence_id)
        {
            Err(XbrlEvidenceError::SelfReferentialOccurrence)
        } else {
            Ok(())
        }
    }
}

fn validate_source_identifier_set(
    values: &[SourceIdentifier],
    max: usize,
    require_nonempty: bool,
) -> Result<(), XbrlEvidenceError> {
    if require_nonempty && values.is_empty() {
        return Err(XbrlEvidenceError::EmptyRelationshipEndpoint);
    }
    if values.len() > max {
        return Err(XbrlEvidenceError::TooManyRelationshipRefs);
    }
    let mut unique = BTreeSet::new();
    if values.iter().any(|value| !unique.insert(value.as_str())) {
        return Err(XbrlEvidenceError::DuplicateRelationshipRef);
    }
    Ok(())
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(transparent)]
struct BoundedSourceIdentifiers(Vec<SourceIdentifier>);

struct BoundedSourceIdentifiersVisitor;

impl<'de> Visitor<'de> for BoundedSourceIdentifiersVisitor {
    type Value = BoundedSourceIdentifiers;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a bounded XBRL source-reference list")
    }

    fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let mut values = Vec::with_capacity(sequence.size_hint().unwrap_or(0).min(16));
        while values.len() < MAX_XBRL_RELATIONSHIP_REFS {
            let Some(value) = sequence.next_element()? else {
                return Ok(BoundedSourceIdentifiers(
                    values.into_boxed_slice().into_vec(),
                ));
            };
            values.push(value);
        }
        if sequence.next_element::<IgnoredAny>()?.is_some() {
            Err(serde::de::Error::custom(
                XbrlEvidenceError::TooManyRelationshipRefs,
            ))
        } else {
            Ok(BoundedSourceIdentifiers(
                values.into_boxed_slice().into_vec(),
            ))
        }
    }
}

impl<'de> Deserialize<'de> for BoundedSourceIdentifiers {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_seq(BoundedSourceIdentifiersVisitor)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(transparent)]
struct BoundedRelationships(Vec<XbrlRelationshipEvidence>);

struct BoundedRelationshipsVisitor;

impl<'de> Visitor<'de> for BoundedRelationshipsVisitor {
    type Value = BoundedRelationships;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a bounded Inline XBRL relationship list")
    }

    fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let mut values = Vec::with_capacity(sequence.size_hint().unwrap_or(0).min(8));
        while values.len() < MAX_XBRL_RELATIONSHIPS {
            let Some(value) = sequence.next_element()? else {
                return Ok(BoundedRelationships(values.into_boxed_slice().into_vec()));
            };
            values.push(value);
        }
        if sequence.next_element::<IgnoredAny>()?.is_some() {
            Err(serde::de::Error::custom(
                XbrlEvidenceError::TooManyRelationships,
            ))
        } else {
            Ok(BoundedRelationships(values.into_boxed_slice().into_vec()))
        }
    }
}

impl<'de> Deserialize<'de> for BoundedRelationships {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_seq(BoundedRelationshipsVisitor)
    }
}

/// Duplicate classification retained without discarding any source occurrence.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum XbrlDuplicateClass {
    /// No duplicate occurrence was found.
    Unique,
    /// Occurrences are complete duplicates.
    Complete,
    /// Numeric occurrences agree under the pinned rounding rules.
    ConsistentNumeric,
    /// Occurrences represent language alternatives.
    MultiLanguageAlternative,
    /// Occurrences conflict and require explicit consumer handling.
    Inconsistent,
    /// The parser retained occurrences without a conclusive class.
    Unclassified,
}

/// Versioned duplicate-group evidence.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct XbrlDuplicateEvidence {
    classification: XbrlDuplicateClass,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    group_id: Option<SourceIdentifier>,
    ruleset: SourceIdentifier,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct XbrlDuplicateEvidenceWire {
    classification: XbrlDuplicateClass,
    #[serde(default)]
    group_id: Option<SourceIdentifier>,
    ruleset: SourceIdentifier,
}

impl<'de> Deserialize<'de> for XbrlDuplicateEvidence {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = XbrlDuplicateEvidenceWire::deserialize(deserializer)?;
        Self::try_new(wire.classification, wire.group_id, wire.ruleset)
            .map_err(serde::de::Error::custom)
    }
}

impl XbrlDuplicateEvidence {
    /// Constructs duplicate evidence; non-unique classes require a stable group identity.
    pub fn try_new(
        classification: XbrlDuplicateClass,
        group_id: Option<SourceIdentifier>,
        ruleset: SourceIdentifier,
    ) -> Result<Self, XbrlEvidenceError> {
        if classification != XbrlDuplicateClass::Unique && group_id.is_none() {
            return Err(XbrlEvidenceError::MissingDuplicateGroup);
        }
        Ok(Self {
            classification,
            group_id,
            ruleset,
        })
    }
}

/// Authority status for taxonomy metadata attached to one fact.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum XbrlTaxonomyStatus {
    /// A caller declared this metadata; the parser did not resolve or validate the taxonomy set.
    CallerDeclaredUnresolved,
}

/// Bounded taxonomy metadata whose validation authority is explicit.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct XbrlTaxonomySet {
    digest: EvidenceDigest,
    version: SourceIdentifier,
    status: XbrlTaxonomyStatus,
}

impl XbrlTaxonomySet {
    /// Retains caller-declared taxonomy metadata without claiming resolution or validation.
    pub const fn declared(digest: EvidenceDigest, version: SourceIdentifier) -> Self {
        Self {
            digest,
            version,
            status: XbrlTaxonomyStatus::CallerDeclaredUnresolved,
        }
    }

    /// Returns the explicit validation authority.
    pub const fn status(&self) -> XbrlTaxonomyStatus {
        self.status
    }
}

/// Unvalidated construction input for [`XbrlFactEvidence`].
#[derive(Clone, Debug)]
pub struct XbrlFactEvidenceInput {
    pub occurrence_id: SourceIdentifier,
    pub accession: SourceIdentifier,
    pub context_id: SourceIdentifier,
    pub unit_id: SourceIdentifier,
    pub concept: XbrlQualifiedName,
    pub unit: XbrlUnitExpression,
    pub entity: XbrlEntity,
    pub period: XbrlPeriod,
    pub accuracy: XbrlAccuracy,
    pub lexical_value: XbrlText,
    /// Optional parser-ruleset output used for numeric conversion while exact presentation text is retained.
    pub transformed_lexeme: Option<XbrlText>,
    pub inline_scale: Option<i32>,
    pub inline_sign: Option<XbrlSign>,
    pub dimensions: Vec<XbrlDimensionEvidence>,
    pub context_graph: XbrlContextGraph,
    pub occurrence_relationships: XbrlOccurrenceRelationships,
    pub language: Option<SourceIdentifier>,
    pub duplicate: XbrlDuplicateEvidence,
    pub taxonomy_set: XbrlTaxonomySet,
    pub source_payload: ExactPayloadEvidence,
    pub parser_ruleset: SourceIdentifier,
    pub rounding_ruleset: SourceIdentifier,
    pub evaluated_at: Timestamp,
}

/// Versioned evidence sufficient to audit one normalized numeric XBRL occurrence.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct XbrlFactEvidence {
    schema_version: u16,
    occurrence_id: SourceIdentifier,
    accession: SourceIdentifier,
    context_id: SourceIdentifier,
    unit_id: SourceIdentifier,
    concept: XbrlQualifiedName,
    unit: XbrlUnitExpression,
    entity: XbrlEntity,
    period: XbrlPeriod,
    accuracy: XbrlAccuracy,
    lexical_value: XbrlText,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    transformed_lexeme: Option<XbrlText>,
    inline_scale: Option<i32>,
    inline_sign: Option<XbrlSign>,
    dimensions: BoundedDimensions,
    context_graph: XbrlContextGraph,
    occurrence_relationships: XbrlOccurrenceRelationships,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    language: Option<SourceIdentifier>,
    duplicate: XbrlDuplicateEvidence,
    taxonomy_set: XbrlTaxonomySet,
    source_payload: ExactPayloadEvidence,
    parser_ruleset: SourceIdentifier,
    rounding_ruleset: SourceIdentifier,
    evaluated_at: Timestamp,
}

impl XbrlFactEvidence {
    /// Validates and constructs immutable XBRL occurrence evidence.
    pub fn try_new(input: XbrlFactEvidenceInput) -> Result<Self, XbrlEvidenceError> {
        if input
            .inline_scale
            .is_some_and(|scale| !(-28..=28).contains(&scale))
        {
            return Err(XbrlEvidenceError::ScaleOutOfRange);
        }
        if matches!(
            input.accuracy,
            XbrlAccuracy::Precision(XbrlAccuracyValue::Finite(value)) if value <= 0
        ) {
            return Err(XbrlEvidenceError::InvalidAccuracy);
        }
        input
            .occurrence_relationships
            .validate_owner(&input.occurrence_id)?;
        let dimensions = BoundedDimensions::try_new(input.dimensions)?;
        let candidate = Self {
            schema_version: XBRL_FACT_EVIDENCE_SCHEMA_VERSION,
            occurrence_id: input.occurrence_id,
            accession: input.accession,
            context_id: input.context_id,
            unit_id: input.unit_id,
            concept: input.concept,
            unit: input.unit,
            entity: input.entity,
            period: input.period,
            accuracy: input.accuracy,
            lexical_value: input.lexical_value,
            transformed_lexeme: input.transformed_lexeme,
            inline_scale: input.inline_scale,
            inline_sign: input.inline_sign,
            dimensions,
            context_graph: input.context_graph,
            occurrence_relationships: input.occurrence_relationships,
            language: input.language,
            duplicate: input.duplicate,
            taxonomy_set: input.taxonomy_set,
            source_payload: input.source_payload,
            parser_ruleset: input.parser_ruleset,
            rounding_ruleset: input.rounding_ruleset,
            evaluated_at: input.evaluated_at,
        };
        candidate.normalized_value()?;
        Ok(candidate)
    }

    /// Returns the source occurrence identity.
    pub const fn occurrence_id(&self) -> &SourceIdentifier {
        &self.occurrence_id
    }

    /// Returns the SEC accession carrying the occurrence.
    pub const fn accession(&self) -> &SourceIdentifier {
        &self.accession
    }

    /// Returns the exact XBRL context identity referenced by the occurrence.
    pub const fn context_id(&self) -> &SourceIdentifier {
        &self.context_id
    }

    /// Returns source period semantics.
    pub const fn period(&self) -> XbrlPeriod {
        self.period
    }

    /// Returns the source lexical and resolved concept QName.
    pub const fn concept(&self) -> &XbrlQualifiedName {
        &self.concept
    }

    /// Returns the source lexical and resolved unit expression.
    pub const fn unit(&self) -> &XbrlUnitExpression {
        &self.unit
    }

    /// Returns the complete bounded dimensions supplied by the XBRL context.
    pub fn dimensions(&self) -> &[XbrlDimensionEvidence] {
        &self.dimensions.0
    }

    /// Returns bounded segment/scenario structure.
    pub const fn context_graph(&self) -> &XbrlContextGraph {
        &self.context_graph
    }

    /// Returns nesting, continuation, and explanatory-relationship evidence.
    pub const fn occurrence_relationships(&self) -> &XbrlOccurrenceRelationships {
        &self.occurrence_relationships
    }

    /// Returns the exact decimal after applying retained scale and sign transforms.
    pub fn normalized_value(&self) -> Result<Decimal, XbrlEvidenceError> {
        let numeric_lexeme = self
            .transformed_lexeme
            .as_ref()
            .unwrap_or(&self.lexical_value);
        let mut value = Decimal::from_str(numeric_lexeme.as_str())
            .map_err(|_| XbrlEvidenceError::InvalidNumericLexeme)?;
        let scale = self.inline_scale.unwrap_or(0);
        if scale >= 0 {
            for _ in 0..scale {
                value = value
                    .checked_mul(Decimal::TEN)
                    .ok_or(XbrlEvidenceError::NumericOverflow)?;
            }
        } else {
            for _ in scale..0 {
                value = value
                    .checked_div(Decimal::TEN)
                    .ok_or(XbrlEvidenceError::NumericOverflow)?;
            }
        }
        if self.inline_sign == Some(XbrlSign::Negative) {
            value = value
                .checked_mul(Decimal::NEGATIVE_ONE)
                .ok_or(XbrlEvidenceError::NumericOverflow)?;
        }
        Ok(value.normalize())
    }

    /// Validates that this evidence produces the canonical exact decimal.
    pub fn validate_value(&self, value: Decimal) -> Result<(), XbrlEvidenceError> {
        if self.normalized_value()? == value.normalize() {
            Ok(())
        } else {
            Err(XbrlEvidenceError::NormalizedValueMismatch)
        }
    }

    /// Validates canonical concept, unit, and value against this exact occurrence evidence.
    pub fn validate_observation(
        &self,
        concept: &SourceIdentifier,
        unit: &SourceIdentifier,
        value: Decimal,
    ) -> Result<(), XbrlEvidenceError> {
        let evidence_unit = self.unit.source_identifier()?;
        if self.concept.source_qname() != concept || &evidence_unit != unit {
            return Err(XbrlEvidenceError::ObservationIdentityMismatch);
        }
        self.validate_value(value)
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct XbrlFactEvidenceWire {
    schema_version: u16,
    occurrence_id: SourceIdentifier,
    accession: SourceIdentifier,
    context_id: SourceIdentifier,
    unit_id: SourceIdentifier,
    concept: XbrlQualifiedName,
    unit: XbrlUnitExpression,
    entity: XbrlEntity,
    period: XbrlPeriod,
    accuracy: XbrlAccuracy,
    lexical_value: XbrlText,
    #[serde(default)]
    transformed_lexeme: Option<XbrlText>,
    inline_scale: Option<i32>,
    inline_sign: Option<XbrlSign>,
    dimensions: BoundedDimensions,
    context_graph: XbrlContextGraph,
    occurrence_relationships: XbrlOccurrenceRelationships,
    #[serde(default)]
    language: Option<SourceIdentifier>,
    duplicate: XbrlDuplicateEvidence,
    taxonomy_set: XbrlTaxonomySet,
    source_payload: ExactPayloadEvidence,
    parser_ruleset: SourceIdentifier,
    rounding_ruleset: SourceIdentifier,
    evaluated_at: Timestamp,
}

impl<'de> Deserialize<'de> for XbrlFactEvidence {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = XbrlFactEvidenceWire::deserialize(deserializer)?;
        if wire.schema_version != XBRL_FACT_EVIDENCE_SCHEMA_VERSION {
            return Err(serde::de::Error::custom(
                XbrlEvidenceError::UnsupportedSchemaVersion,
            ));
        }
        Self::try_new(XbrlFactEvidenceInput {
            occurrence_id: wire.occurrence_id,
            accession: wire.accession,
            context_id: wire.context_id,
            unit_id: wire.unit_id,
            concept: wire.concept,
            unit: wire.unit,
            entity: wire.entity,
            period: wire.period,
            accuracy: wire.accuracy,
            lexical_value: wire.lexical_value,
            transformed_lexeme: wire.transformed_lexeme,
            inline_scale: wire.inline_scale,
            inline_sign: wire.inline_sign,
            dimensions: wire.dimensions.0,
            context_graph: wire.context_graph,
            occurrence_relationships: wire.occurrence_relationships,
            language: wire.language,
            duplicate: wire.duplicate,
            taxonomy_set: wire.taxonomy_set,
            source_payload: wire.source_payload,
            parser_ruleset: wire.parser_ruleset,
            rounding_ruleset: wire.rounding_ruleset,
            evaluated_at: wire.evaluated_at,
        })
        .map_err(serde::de::Error::custom)
    }
}

/// Failure to construct or bind XBRL occurrence evidence.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum XbrlEvidenceError {
    EmptyRequiredText,
    TextTooLong,
    InvalidQualifiedName,
    UnboundQualifiedName,
    QualifiedNameMismatch,
    TooManyGraphEvents,
    InvalidGraphStructure,
    DuplicateGraphAttribute,
    InvertedPeriod,
    TooManyDimensions,
    EmptyUnitSide,
    TooManyUnitMeasures,
    CancellingUnitMeasure,
    UnitIdentifierTooLong,
    EmptyRelationshipEndpoint,
    TooManyRelationshipRefs,
    DuplicateRelationshipRef,
    TooManyRelationships,
    SelfReferentialOccurrence,
    MissingDuplicateGroup,
    InvalidAccuracy,
    ScaleOutOfRange,
    InvalidNumericLexeme,
    NumericOverflow,
    NormalizedValueMismatch,
    ObservationIdentityMismatch,
    UnsupportedSchemaVersion,
}

impl fmt::Display for XbrlEvidenceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let message = match self {
            Self::EmptyRequiredText => "required XBRL text is empty",
            Self::TextTooLong => "XBRL text exceeds its byte bound",
            Self::InvalidQualifiedName => "XBRL QName lexical form is invalid",
            Self::UnboundQualifiedName => "prefixed XBRL QName lacks namespace authority",
            Self::QualifiedNameMismatch => "XBRL QName local name disagrees with its lexical form",
            Self::TooManyGraphEvents => "XBRL XML evidence graph exceeds its event bound",
            Self::InvalidGraphStructure => "XBRL XML evidence graph is not balanced",
            Self::DuplicateGraphAttribute => "XBRL XML evidence contains a duplicate attribute",
            Self::InvertedPeriod => "XBRL duration start is after its end",
            Self::TooManyDimensions => "XBRL context exceeds its dimension bound",
            Self::EmptyUnitSide => "XBRL divide unit requires nonempty numerator and denominator",
            Self::TooManyUnitMeasures => "XBRL unit exceeds its measure bound",
            Self::CancellingUnitMeasure => {
                "XBRL divide unit repeats one expanded measure on both sides"
            }
            Self::UnitIdentifierTooLong => "XBRL unit lexical identifier exceeds its bound",
            Self::EmptyRelationshipEndpoint => "Inline XBRL relationship endpoint is empty",
            Self::TooManyRelationshipRefs => {
                "Inline XBRL relationship endpoint exceeds its reference bound"
            }
            Self::DuplicateRelationshipRef => {
                "Inline XBRL relationship endpoint repeats a source reference"
            }
            Self::TooManyRelationships => "Inline XBRL occurrence exceeds its relationship bound",
            Self::SelfReferentialOccurrence => "Inline XBRL nesting edge is self-referential",
            Self::MissingDuplicateGroup => "non-unique XBRL occurrence requires a duplicate group",
            Self::InvalidAccuracy => "XBRL precision must be positive or infinite",
            Self::ScaleOutOfRange => "Inline XBRL scale exceeds Decimal capacity",
            Self::InvalidNumericLexeme => "XBRL numeric lexical value is invalid",
            Self::NumericOverflow => "XBRL numeric transform exceeds Decimal capacity",
            Self::NormalizedValueMismatch => "XBRL evidence does not produce the canonical value",
            Self::ObservationIdentityMismatch => {
                "XBRL evidence concept or unit does not match the canonical observation"
            }
            Self::UnsupportedSchemaVersion => "XBRL evidence schema version is unsupported",
        };
        formatter.write_str(message)
    }
}

impl std::error::Error for XbrlEvidenceError {}
