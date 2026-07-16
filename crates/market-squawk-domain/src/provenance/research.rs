//! Research provenance with explicit conservative availability semantics.

use std::num::NonZeroU32;

use serde::{Deserialize, Deserializer, Serialize};

use super::{PayloadReference, ProvenanceError, ensure_current_schema};
use crate::{
    DataQuality, InstrumentId, SchemaVersion, SourceId, SourceIdentifier, Timestamp, VenueId,
};

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
        /// Conservative local first-observed bound.
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

    /// Constructs conservative local first-observed evidence.
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
    /// Evidenced and local-first-observed times are conservative. Inferred and unknown times return
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
    /// Immutable payload evidence.
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

    /// Returns retained payload evidence.
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

/// Effective, publication, revision, and supersession time for research data.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub struct ResearchTime {
    effective_at: Timestamp,
    published_at: Option<Timestamp>,
    revision: RevisionNumber,
    superseded_at: Option<Timestamp>,
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
        if let (Some(published), Some(superseded)) = (published_at, superseded_at)
            && superseded <= published
        {
            return Err(ProvenanceError::SupersededNotAfterPublished);
        }
        Ok(Self {
            effective_at,
            published_at,
            revision,
            superseded_at,
        })
    }

    /// Returns the observation's reference or effective time.
    pub const fn effective_at(self) -> Timestamp {
        self.effective_at
    }

    /// Returns publication time when supplied.
    pub const fn published_at(self) -> Option<Timestamp> {
        self.published_at
    }

    /// Returns the one-based revision number.
    pub const fn revision(self) -> RevisionNumber {
        self.revision
    }

    /// Returns when this revision ceased being current.
    pub const fn superseded_at(self) -> Option<Timestamp> {
        self.superseded_at
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ResearchTimeWire {
    effective_at: Timestamp,
    published_at: Option<Timestamp>,
    revision: RevisionNumber,
    superseded_at: Option<Timestamp>,
}

impl<'de> Deserialize<'de> for ResearchTime {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = ResearchTimeWire::deserialize(deserializer)?;
        Self::new(
            wire.effective_at,
            wire.published_at,
            wire.revision,
            wire.superseded_at,
        )
        .map_err(serde::de::Error::custom)
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
    /// Rejects reported availability before publication and supersession at or before conservative
    /// availability.
    pub fn new(
        provenance: ResearchProvenance,
        time: ResearchTime,
    ) -> Result<Self, ProvenanceError> {
        if let (Some(published), Some(available)) =
            (time.published_at, provenance.availability.reported_at())
            && available < published
        {
            return Err(ProvenanceError::AvailabilityBeforePublished);
        }
        if let (Some(superseded), Some(available)) = (
            time.superseded_at,
            provenance.availability.conservative_available_at(),
        ) && superseded <= available
        {
            return Err(ProvenanceError::SupersededNotAfterAvailable);
        }
        Ok(Self { provenance, time })
    }

    /// Returns research-only provenance.
    pub const fn provenance(&self) -> &ResearchProvenance {
        &self.provenance
    }

    /// Returns research-specific revision and time metadata.
    pub const fn time(&self) -> ResearchTime {
        self.time
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
