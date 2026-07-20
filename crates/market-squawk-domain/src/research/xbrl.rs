//! Versioned, bounded evidence retained for normalized XBRL numeric facts.

use std::fmt;
use std::str::FromStr;

use rust_decimal::Decimal;
use serde::de::{IgnoredAny, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize};

use crate::{CalendarDate, EvidenceDigest, ExactPayloadEvidence, SourceIdentifier, Timestamp};

/// Current schema version for [`XbrlFactEvidence`].
pub const XBRL_FACT_EVIDENCE_SCHEMA_VERSION: u16 = 1;

/// Maximum dimensions retained for one XBRL context.
pub const MAX_XBRL_DIMENSIONS: usize = 128;

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
    Explicit { member: SourceIdentifier },
    /// Canonical retained representation and exact digest of a typed member.
    Typed {
        canonical_value: XbrlText,
        payload_digest: EvidenceDigest,
    },
}

/// One context dimension with explicit segment/scenario placement.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct XbrlDimensionEvidence {
    dimension: SourceIdentifier,
    member: XbrlDimensionMember,
    location: XbrlDimensionLocation,
}

impl XbrlDimensionEvidence {
    /// Constructs dimension evidence.
    pub const fn new(
        dimension: SourceIdentifier,
        member: XbrlDimensionMember,
        location: XbrlDimensionLocation,
    ) -> Self {
        Self {
            dimension,
            member,
            location,
        }
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

/// Exact taxonomy-set identity used to interpret one fact.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct XbrlTaxonomySet {
    digest: EvidenceDigest,
    version: SourceIdentifier,
}

impl XbrlTaxonomySet {
    /// Constructs taxonomy-set evidence.
    pub const fn new(digest: EvidenceDigest, version: SourceIdentifier) -> Self {
        Self { digest, version }
    }
}

/// Unvalidated construction input for [`XbrlFactEvidence`].
#[derive(Clone, Debug)]
pub struct XbrlFactEvidenceInput {
    pub occurrence_id: SourceIdentifier,
    pub accession: SourceIdentifier,
    pub context_id: SourceIdentifier,
    pub unit_id: SourceIdentifier,
    pub entity: XbrlEntity,
    pub period: XbrlPeriod,
    pub accuracy: XbrlAccuracy,
    pub lexical_value: XbrlText,
    /// Optional parser-ruleset output used for numeric conversion while exact presentation text is retained.
    pub transformed_lexeme: Option<XbrlText>,
    pub inline_scale: Option<i32>,
    pub inline_sign: Option<XbrlSign>,
    pub dimensions: Vec<XbrlDimensionEvidence>,
    pub segment_evidence: Option<XbrlText>,
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
    entity: XbrlEntity,
    period: XbrlPeriod,
    accuracy: XbrlAccuracy,
    lexical_value: XbrlText,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    transformed_lexeme: Option<XbrlText>,
    inline_scale: Option<i32>,
    inline_sign: Option<XbrlSign>,
    dimensions: BoundedDimensions,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    segment_evidence: Option<XbrlText>,
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
        let dimensions = BoundedDimensions::try_new(input.dimensions)?;
        let candidate = Self {
            schema_version: XBRL_FACT_EVIDENCE_SCHEMA_VERSION,
            occurrence_id: input.occurrence_id,
            accession: input.accession,
            context_id: input.context_id,
            unit_id: input.unit_id,
            entity: input.entity,
            period: input.period,
            accuracy: input.accuracy,
            lexical_value: input.lexical_value,
            transformed_lexeme: input.transformed_lexeme,
            inline_scale: input.inline_scale,
            inline_sign: input.inline_sign,
            dimensions,
            segment_evidence: input.segment_evidence,
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

    /// Returns source period semantics.
    pub const fn period(&self) -> XbrlPeriod {
        self.period
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
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct XbrlFactEvidenceWire {
    schema_version: u16,
    occurrence_id: SourceIdentifier,
    accession: SourceIdentifier,
    context_id: SourceIdentifier,
    unit_id: SourceIdentifier,
    entity: XbrlEntity,
    period: XbrlPeriod,
    accuracy: XbrlAccuracy,
    lexical_value: XbrlText,
    #[serde(default)]
    transformed_lexeme: Option<XbrlText>,
    inline_scale: Option<i32>,
    inline_sign: Option<XbrlSign>,
    dimensions: BoundedDimensions,
    #[serde(default)]
    segment_evidence: Option<XbrlText>,
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
            entity: wire.entity,
            period: wire.period,
            accuracy: wire.accuracy,
            lexical_value: wire.lexical_value,
            transformed_lexeme: wire.transformed_lexeme,
            inline_scale: wire.inline_scale,
            inline_sign: wire.inline_sign,
            dimensions: wire.dimensions.0,
            segment_evidence: wire.segment_evidence,
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
    InvertedPeriod,
    TooManyDimensions,
    MissingDuplicateGroup,
    ScaleOutOfRange,
    InvalidNumericLexeme,
    NumericOverflow,
    NormalizedValueMismatch,
    UnsupportedSchemaVersion,
}

impl fmt::Display for XbrlEvidenceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let message = match self {
            Self::EmptyRequiredText => "required XBRL text is empty",
            Self::TextTooLong => "XBRL text exceeds its byte bound",
            Self::InvertedPeriod => "XBRL duration start is after its end",
            Self::TooManyDimensions => "XBRL context exceeds its dimension bound",
            Self::MissingDuplicateGroup => "non-unique XBRL occurrence requires a duplicate group",
            Self::ScaleOutOfRange => "Inline XBRL scale exceeds Decimal capacity",
            Self::InvalidNumericLexeme => "XBRL numeric lexical value is invalid",
            Self::NumericOverflow => "XBRL numeric transform exceeds Decimal capacity",
            Self::NormalizedValueMismatch => "XBRL evidence does not produce the canonical value",
            Self::UnsupportedSchemaVersion => "XBRL evidence schema version is unsupported",
        };
        formatter.write_str(message)
    }
}

impl std::error::Error for XbrlEvidenceError {}
