//! Research provenance with explicit conservative availability semantics.

use std::cmp::Ordering;
use std::num::{NonZeroU16, NonZeroU32};

use serde::{Deserialize, Deserializer, Serialize};

use super::{PayloadReference, ProvenanceError, ensure_current_schema};
use crate::{
    CalendarDate, DataQuality, InstrumentId, SchemaVersion, SourceId, SourceIdentifier, Timestamp,
    VenueId,
};

const RESEARCH_TEMPORAL_SCHEMA_VERSION: u16 = 2;

/// Evidence for when research data became available to a point-in-time consumer.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, tag = "kind", rename_all = "snake_case")]
pub enum AvailabilityEvidence {
    /// Source/audit evidence establishes the availability time.
    Evidenced {
        /// Evidenced availability time.
        available_at: Timestamp,
        /// Source record, release calendar, or audit evidence identity.
        evidence: SourceIdentifier,
    },
    /// The local system first observed the object at this time.
    LocalFirstObserved {
        /// Conservative bound based on the first local observation.
        observed_at: Timestamp,
    },
    /// A time was inferred but is not admitted as point-in-time evidence by default.
    Inferred {
        /// Inferred time retained for analysis.
        inferred_at: Timestamp,
        /// Versioned method or source field used for the inference.
        method: SourceIdentifier,
    },
    /// Historical availability cannot be established.
    Unknown,
}

impl AvailabilityEvidence {
    /// Constructs evidenced availability with a retained audit reference.
    pub const fn evidenced(available_at: Timestamp, evidence: SourceIdentifier) -> Self {
        Self::Evidenced {
            available_at,
            evidence,
        }
    }

    /// Constructs conservative evidence from the first local observation.
    pub const fn local_first_observed(observed_at: Timestamp) -> Self {
        Self::LocalFirstObserved { observed_at }
    }

    /// Retains an inferred time without promoting it to point-in-time evidence.
    pub const fn inferred(inferred_at: Timestamp, method: SourceIdentifier) -> Self {
        Self::Inferred {
            inferred_at,
            method,
        }
    }

    /// Represents unknown historical availability.
    pub const fn unknown() -> Self {
        Self::Unknown
    }

    /// Returns a safe default point-in-time cutoff.
    ///
    /// Evidenced times and times first observed locally are conservative. Inferred and unknown
    /// times return
    /// `None` so a default point-in-time builder fails closed rather than admitting look-ahead.
    pub const fn conservative_available_at(&self) -> Option<Timestamp> {
        match self {
            Self::Evidenced { available_at, .. } => Some(*available_at),
            Self::LocalFirstObserved { observed_at } => Some(*observed_at),
            Self::Inferred { .. } | Self::Unknown => None,
        }
    }

    /// Returns any reported time, including a non-authoritative inferred time.
    pub const fn reported_at(&self) -> Option<Timestamp> {
        match self {
            Self::Evidenced { available_at, .. } => Some(*available_at),
            Self::LocalFirstObserved { observed_at } => Some(*observed_at),
            Self::Inferred { inferred_at, .. } => Some(*inferred_at),
            Self::Unknown => None,
        }
    }

    /// Returns whether the default point-in-time policy may use this evidence.
    pub const fn is_point_in_time_evidenced(&self) -> bool {
        Self::conservative_available_at(self).is_some()
    }
}

/// Provenance carried only by research observations.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ResearchProvenance {
    schema_version: SchemaVersion,
    source_id: SourceId,
    instrument_id: Option<InstrumentId>,
    venue_id: Option<VenueId>,
    source_identifier: SourceIdentifier,
    source_timestamp: Option<Timestamp>,
    received_at: Timestamp,
    ingested_at: Timestamp,
    quality: DataQuality,
    payload_reference: PayloadReference,
    availability: AvailabilityEvidence,
}

/// Complete current-schema input for constructing [`ResearchProvenance`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResearchProvenanceInput {
    /// Source namespace.
    pub source_id: SourceId,
    /// Stable internal instrument identity when applicable.
    pub instrument_id: Option<InstrumentId>,
    /// Venue identity when applicable.
    pub venue_id: Option<VenueId>,
    /// Source-native record identifier.
    pub source_identifier: SourceIdentifier,
    /// Source-authored timestamp when known.
    pub source_timestamp: Option<Timestamp>,
    /// Time the source payload reached this process.
    pub received_at: Timestamp,
    /// Time the canonical record was ingested locally.
    pub ingested_at: Timestamp,
    /// Evidentiary data-quality class.
    pub quality: DataQuality,
    /// Content digest or opaque source record identity with no inherent immutability guarantee.
    pub payload_reference: PayloadReference,
    /// Explicit point-in-time availability evidence.
    pub availability: AvailabilityEvidence,
}

impl ResearchProvenance {
    /// Constructs research-only provenance using the current schema.
    ///
    /// # Errors
    ///
    /// Rejects receive or reported availability times after local ingestion.
    pub fn try_new(input: ResearchProvenanceInput) -> Result<Self, ProvenanceError> {
        if input.received_at > input.ingested_at {
            return Err(ProvenanceError::ReceivedAfterIngested);
        }
        if input
            .availability
            .reported_at()
            .is_some_and(|available| available > input.ingested_at)
        {
            return Err(ProvenanceError::AvailabilityAfterIngested);
        }
        Ok(Self {
            schema_version: SchemaVersion::CURRENT,
            source_id: input.source_id,
            instrument_id: input.instrument_id,
            venue_id: input.venue_id,
            source_identifier: input.source_identifier,
            source_timestamp: input.source_timestamp,
            received_at: input.received_at,
            ingested_at: input.ingested_at,
            quality: input.quality,
            payload_reference: input.payload_reference,
            availability: input.availability,
        })
    }

    fn try_from_wire(wire: ResearchProvenanceWire) -> Result<Self, ProvenanceError> {
        let ResearchProvenanceWire {
            schema_version,
            source_id,
            instrument_id,
            venue_id,
            source_identifier,
            source_timestamp,
            received_at,
            ingested_at,
            quality,
            payload_reference,
            availability,
        } = wire;
        ensure_current_schema(schema_version)?;
        Self::try_new(ResearchProvenanceInput {
            source_id,
            instrument_id,
            venue_id,
            source_identifier,
            source_timestamp,
            received_at,
            ingested_at,
            quality,
            payload_reference,
            availability,
        })
    }

    /// Returns the record schema version.
    pub const fn schema_version(&self) -> SchemaVersion {
        self.schema_version
    }

    /// Returns the source namespace.
    pub const fn source_id(&self) -> &SourceId {
        &self.source_id
    }

    /// Returns the stable instrument identity when applicable.
    pub const fn instrument_id(&self) -> Option<InstrumentId> {
        self.instrument_id
    }

    /// Returns the venue identity when applicable.
    pub const fn venue_id(&self) -> Option<&VenueId> {
        self.venue_id.as_ref()
    }

    /// Returns the source-native record identifier.
    pub const fn source_identifier(&self) -> &SourceIdentifier {
        &self.source_identifier
    }

    /// Returns the source timestamp without manufacturing one.
    pub const fn source_timestamp(&self) -> Option<Timestamp> {
        self.source_timestamp
    }

    /// Returns when the source payload reached this process.
    pub const fn received_at(&self) -> Timestamp {
        self.received_at
    }

    /// Returns when the canonical record was ingested locally.
    pub const fn ingested_at(&self) -> Timestamp {
        self.ingested_at
    }

    /// Returns the record's evidentiary quality class.
    pub const fn quality(&self) -> DataQuality {
        self.quality
    }

    /// Returns the retained content digest or opaque source record identity.
    ///
    /// A source reference has no inherent immutability guarantee.
    pub const fn payload_reference(&self) -> &PayloadReference {
        &self.payload_reference
    }

    /// Returns explicit availability evidence, including unknown.
    pub const fn availability(&self) -> &AvailabilityEvidence {
        &self.availability
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ResearchProvenanceWire {
    schema_version: SchemaVersion,
    source_id: SourceId,
    instrument_id: Option<InstrumentId>,
    venue_id: Option<VenueId>,
    source_identifier: SourceIdentifier,
    source_timestamp: Option<Timestamp>,
    received_at: Timestamp,
    ingested_at: Timestamp,
    quality: DataQuality,
    payload_reference: PayloadReference,
    availability: AvailabilityEvidence,
}

impl<'de> Deserialize<'de> for ResearchProvenance {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = ResearchProvenanceWire::deserialize(deserializer)?;
        Self::try_from_wire(wire).map_err(serde::de::Error::custom)
    }
}

/// A one-based revision number for a research observation.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(transparent)]
pub struct RevisionNumber(NonZeroU32);

impl RevisionNumber {
    /// Constructs a one-based revision number.
    ///
    /// # Errors
    ///
    /// Returns [`ProvenanceError::ZeroRevision`] for zero.
    pub fn new(value: u32) -> Result<Self, ProvenanceError> {
        NonZeroU32::new(value)
            .map(Self)
            .ok_or(ProvenanceError::ZeroRevision)
    }

    /// Returns the primitive revision number.
    pub const fn get(self) -> u32 {
        self.0.get()
    }
}

/// Precision retained by one research temporal coordinate.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ResearchTemporalPrecision {
    /// An exact UTC instant.
    ExactTimestamp,
    /// A civil calendar date with no time of day or time zone.
    CalendarDate,
    /// A source-authored named period with no fabricated day or instant.
    SourcePeriod,
}

impl ResearchTemporalPrecision {
    /// Returns the stable analytical storage label.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ExactTimestamp => "exact_timestamp",
            Self::CalendarDate => "calendar_date",
            Self::SourcePeriod => "source_period",
        }
    }
}

/// One provider-qualified research period whose source code and sortable ordinal are retained.
#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ResearchPeriod {
    scheme: SourceIdentifier,
    year: u16,
    ordinal: NonZeroU16,
    code: SourceIdentifier,
}

impl ResearchPeriod {
    /// Constructs a source-qualified period without converting it to a calendar day.
    ///
    /// The scheme defines the provider/frequency ordering domain, the ordinal orders periods
    /// within a year, and the code preserves the exact provider representation such as `M13`.
    ///
    /// # Errors
    ///
    /// Rejects year zero.
    pub fn try_new(
        scheme: SourceIdentifier,
        year: u16,
        ordinal: NonZeroU16,
        code: SourceIdentifier,
    ) -> Result<Self, ProvenanceError> {
        if year == 0 {
            return Err(ProvenanceError::InvalidResearchPeriod);
        }
        Ok(Self {
            scheme,
            year,
            ordinal,
            code,
        })
    }

    /// Returns the provider/frequency ordering namespace.
    pub const fn scheme(&self) -> &SourceIdentifier {
        &self.scheme
    }

    /// Returns the source-authored year.
    pub const fn year(&self) -> u16 {
        self.year
    }

    /// Returns the one-based provider-defined ordinal within the year.
    pub const fn ordinal(&self) -> NonZeroU16 {
        self.ordinal
    }

    /// Returns the exact provider period code.
    pub const fn code(&self) -> &SourceIdentifier {
        &self.code
    }
}

impl PartialOrd for ResearchPeriod {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        if self.scheme != other.scheme {
            return None;
        }
        let ordering = (self.year, self.ordinal).cmp(&(other.year, other.ordinal));
        if ordering == Ordering::Equal && self.code != other.code {
            None
        } else {
            Some(ordering)
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(
    deny_unknown_fields,
    tag = "precision",
    content = "value",
    rename_all = "snake_case"
)]
enum ResearchTemporalValue {
    ExactTimestamp(Timestamp),
    CalendarDate(CalendarDate),
    SourcePeriod(ResearchPeriod),
}

/// A research time coordinate that preserves source precision without inventing midnight.
///
/// Coordinates are ordered only at the same precision; provider periods additionally require the
/// same scheme. Cross-precision and cross-scheme comparisons are intentionally unordered and
/// therefore fail closed in range predicates.
#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ResearchTemporalCoordinate {
    schema_version: u16,
    coordinate: ResearchTemporalValue,
}

impl ResearchTemporalCoordinate {
    /// Constructs an exact-instant coordinate.
    pub const fn exact(timestamp: Timestamp) -> Self {
        Self {
            schema_version: RESEARCH_TEMPORAL_SCHEMA_VERSION,
            coordinate: ResearchTemporalValue::ExactTimestamp(timestamp),
        }
    }

    /// Constructs a calendar-precision coordinate.
    pub const fn calendar_date(date: CalendarDate) -> Self {
        Self {
            schema_version: RESEARCH_TEMPORAL_SCHEMA_VERSION,
            coordinate: ResearchTemporalValue::CalendarDate(date),
        }
    }

    /// Returns the retained source precision.
    pub const fn source_period(period: ResearchPeriod) -> Self {
        Self {
            schema_version: RESEARCH_TEMPORAL_SCHEMA_VERSION,
            coordinate: ResearchTemporalValue::SourcePeriod(period),
        }
    }

    /// Returns the retained source precision.
    pub const fn precision(&self) -> ResearchTemporalPrecision {
        match &self.coordinate {
            ResearchTemporalValue::ExactTimestamp(_) => ResearchTemporalPrecision::ExactTimestamp,
            ResearchTemporalValue::CalendarDate(_) => ResearchTemporalPrecision::CalendarDate,
            ResearchTemporalValue::SourcePeriod(_) => ResearchTemporalPrecision::SourcePeriod,
        }
    }

    /// Returns the exact instant only when the source supplied exact-instant precision.
    pub const fn exact_timestamp(&self) -> Option<Timestamp> {
        match &self.coordinate {
            ResearchTemporalValue::ExactTimestamp(timestamp) => Some(*timestamp),
            ResearchTemporalValue::CalendarDate(_) | ResearchTemporalValue::SourcePeriod(_) => None,
        }
    }

    /// Returns the civil date only when the source supplied calendar-date precision.
    pub const fn calendar_date_value(&self) -> Option<CalendarDate> {
        match &self.coordinate {
            ResearchTemporalValue::ExactTimestamp(_) => None,
            ResearchTemporalValue::CalendarDate(date) => Some(*date),
            ResearchTemporalValue::SourcePeriod(_) => None,
        }
    }

    /// Returns the exact provider period only when the source supplied period precision.
    pub const fn source_period_value(&self) -> Option<&ResearchPeriod> {
        match &self.coordinate {
            ResearchTemporalValue::SourcePeriod(period) => Some(period),
            ResearchTemporalValue::ExactTimestamp(_) | ResearchTemporalValue::CalendarDate(_) => {
                None
            }
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ResearchTemporalCoordinateWire {
    schema_version: u16,
    coordinate: ResearchTemporalValue,
}

impl<'de> Deserialize<'de> for ResearchTemporalCoordinate {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = ResearchTemporalCoordinateWire::deserialize(deserializer)?;
        let supported = wire.schema_version == RESEARCH_TEMPORAL_SCHEMA_VERSION
            || (wire.schema_version == 1
                && !matches!(&wire.coordinate, ResearchTemporalValue::SourcePeriod(_)));
        if !supported {
            return Err(serde::de::Error::custom(
                ProvenanceError::UnsupportedResearchTemporalSchema {
                    found: wire.schema_version,
                },
            ));
        }
        Ok(Self {
            schema_version: RESEARCH_TEMPORAL_SCHEMA_VERSION,
            coordinate: wire.coordinate,
        })
    }
}

impl From<Timestamp> for ResearchTemporalCoordinate {
    fn from(value: Timestamp) -> Self {
        Self::exact(value)
    }
}

impl From<CalendarDate> for ResearchTemporalCoordinate {
    fn from(value: CalendarDate) -> Self {
        Self::calendar_date(value)
    }
}

impl From<ResearchPeriod> for ResearchTemporalCoordinate {
    fn from(value: ResearchPeriod) -> Self {
        Self::source_period(value)
    }
}

impl PartialOrd for ResearchTemporalCoordinate {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match (&self.coordinate, &other.coordinate) {
            (
                ResearchTemporalValue::ExactTimestamp(left),
                ResearchTemporalValue::ExactTimestamp(right),
            ) => left.partial_cmp(right),
            (
                ResearchTemporalValue::CalendarDate(left),
                ResearchTemporalValue::CalendarDate(right),
            ) => left.partial_cmp(right),
            (
                ResearchTemporalValue::SourcePeriod(left),
                ResearchTemporalValue::SourcePeriod(right),
            ) => left.partial_cmp(right),
            _ => None,
        }
    }
}

impl PartialEq<ResearchTemporalCoordinate> for Timestamp {
    fn eq(&self, other: &ResearchTemporalCoordinate) -> bool {
        other
            .exact_timestamp()
            .is_some_and(|timestamp| *self == timestamp)
    }
}

impl PartialOrd<ResearchTemporalCoordinate> for Timestamp {
    fn partial_cmp(&self, other: &ResearchTemporalCoordinate) -> Option<Ordering> {
        other
            .exact_timestamp()
            .and_then(|timestamp| self.partial_cmp(&timestamp))
    }
}

impl PartialEq<Timestamp> for ResearchTemporalCoordinate {
    fn eq(&self, other: &Timestamp) -> bool {
        other == self
    }
}

impl PartialOrd<Timestamp> for ResearchTemporalCoordinate {
    fn partial_cmp(&self, other: &Timestamp) -> Option<Ordering> {
        self.exact_timestamp()
            .and_then(|timestamp| timestamp.partial_cmp(other))
    }
}

/// Effective, publication, revision, and supersession time for research data.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ResearchTime {
    schema_version: u16,
    effective: ResearchTemporalCoordinate,
    published: Option<ResearchTemporalCoordinate>,
    revision: RevisionNumber,
    superseded: Option<ResearchTemporalCoordinate>,
}

impl ResearchTime {
    /// Constructs research time metadata without inventing publication or supersession times.
    ///
    /// # Errors
    ///
    /// Rejects a superseding time at or before a known publication time.
    pub fn new(
        effective_at: Timestamp,
        published_at: Option<Timestamp>,
        revision: RevisionNumber,
        superseded_at: Option<Timestamp>,
    ) -> Result<Self, ProvenanceError> {
        Self::try_new_with_coordinates(
            ResearchTemporalCoordinate::exact(effective_at),
            published_at.map(ResearchTemporalCoordinate::exact),
            revision,
            superseded_at.map(ResearchTemporalCoordinate::exact),
        )
    }

    /// Constructs research time while preserving exact, calendar-date, or provider-period
    /// precision.
    ///
    /// # Errors
    ///
    /// Rejects supersession that is not provably later than publication, including coordinates
    /// from incomparable precision or provider schemes.
    pub fn try_new_with_coordinates(
        effective: ResearchTemporalCoordinate,
        published: Option<ResearchTemporalCoordinate>,
        revision: RevisionNumber,
        superseded: Option<ResearchTemporalCoordinate>,
    ) -> Result<Self, ProvenanceError> {
        if let (Some(published), Some(superseded)) = (published.as_ref(), superseded.as_ref()) {
            match superseded.partial_cmp(published) {
                Some(Ordering::Greater) => {}
                Some(Ordering::Less | Ordering::Equal) | None => {
                    return Err(ProvenanceError::SupersededNotAfterPublished);
                }
            }
        }
        Ok(Self {
            schema_version: RESEARCH_TEMPORAL_SCHEMA_VERSION,
            effective,
            published,
            revision,
            superseded,
        })
    }

    /// Returns the observation's reference or effective coordinate without precision loss.
    pub const fn effective(&self) -> &ResearchTemporalCoordinate {
        &self.effective
    }

    /// Returns the publication coordinate when supplied.
    pub fn published(&self) -> Option<&ResearchTemporalCoordinate> {
        self.published.as_ref()
    }

    /// Returns the one-based revision number.
    pub const fn revision(&self) -> RevisionNumber {
        self.revision
    }

    /// Returns the coordinate at which this revision ceased being current.
    pub fn superseded(&self) -> Option<&ResearchTemporalCoordinate> {
        self.superseded.as_ref()
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CurrentResearchTimeWire {
    schema_version: u16,
    effective: ResearchTemporalCoordinate,
    published: Option<ResearchTemporalCoordinate>,
    revision: RevisionNumber,
    superseded: Option<ResearchTemporalCoordinate>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct LegacyResearchTimeWire {
    effective_at: Timestamp,
    published_at: Option<Timestamp>,
    revision: RevisionNumber,
    superseded_at: Option<Timestamp>,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum ResearchTimeWire {
    Current(CurrentResearchTimeWire),
    Legacy(LegacyResearchTimeWire),
}

impl<'de> Deserialize<'de> for ResearchTime {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        match ResearchTimeWire::deserialize(deserializer)? {
            ResearchTimeWire::Current(wire) => {
                if wire.schema_version != RESEARCH_TEMPORAL_SCHEMA_VERSION {
                    return Err(serde::de::Error::custom(
                        ProvenanceError::UnsupportedResearchTemporalSchema {
                            found: wire.schema_version,
                        },
                    ));
                }
                Self::try_new_with_coordinates(
                    wire.effective,
                    wire.published,
                    wire.revision,
                    wire.superseded,
                )
                .map_err(serde::de::Error::custom)
            }
            ResearchTimeWire::Legacy(wire) => Self::new(
                wire.effective_at,
                wire.published_at,
                wire.revision,
                wire.superseded_at,
            )
            .map_err(serde::de::Error::custom),
        }
    }
}

/// Research provenance combined with revision and effective/publication time.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ResearchContext {
    provenance: ResearchProvenance,
    time: ResearchTime,
}

impl ResearchContext {
    /// Combines research provenance and time after point-in-time ordering checks.
    ///
    /// # Errors
    ///
    /// Rejects reported availability before an exact publication instant.
    pub fn new(
        provenance: ResearchProvenance,
        time: ResearchTime,
    ) -> Result<Self, ProvenanceError> {
        if let (Some(published), Some(available)) = (
            time.published
                .as_ref()
                .and_then(|value| value.exact_timestamp()),
            provenance.availability.reported_at(),
        ) && available < published
        {
            return Err(ProvenanceError::AvailabilityBeforePublished);
        }
        Ok(Self { provenance, time })
    }

    /// Returns research-only provenance.
    pub const fn provenance(&self) -> &ResearchProvenance {
        &self.provenance
    }

    /// Returns research-specific revision and time metadata.
    pub const fn time(&self) -> &ResearchTime {
        &self.time
    }

    /// Rebinds only the durable one-based revision while preserving all source-authored context.
    ///
    /// This is the sole post-normalization mutation supported by the canonical research context.
    /// Effective, publication, supersession, availability, provenance, and payload evidence remain
    /// byte-for-byte unchanged.
    pub fn with_revision(&self, revision: RevisionNumber) -> Self {
        Self {
            provenance: self.provenance.clone(),
            time: ResearchTime {
                schema_version: self.time.schema_version,
                effective: self.time.effective.clone(),
                published: self.time.published.clone(),
                revision,
                superseded: self.time.superseded.clone(),
            },
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ResearchContextWire {
    provenance: ResearchProvenance,
    time: ResearchTime,
}

impl<'de> Deserialize<'de> for ResearchContext {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = ResearchContextWire::deserialize(deserializer)?;
        Self::new(wire.provenance, wire.time).map_err(serde::de::Error::custom)
    }
}
