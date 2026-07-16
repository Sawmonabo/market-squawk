//! Validated record provenance and point-in-time research metadata.

use std::fmt;
use std::num::NonZeroU32;

use serde::{Deserialize, Deserializer, Serialize};

use crate::{
    DataQuality, InstrumentId, SchemaVersion, SourceId, SourceIdentifier, Timestamp, VenueId,
};

/// Hash algorithm identifying how a retained payload digest was produced.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PayloadHashAlgorithm {
    /// SHA-256 digest.
    Sha256,
    /// BLAKE3 digest.
    Blake3,
}

/// An algorithm-qualified 256-bit content digest.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub struct PayloadHash {
    algorithm: PayloadHashAlgorithm,
    digest: [u8; 32],
}

impl PayloadHash {
    /// Constructs an algorithm-qualified digest.
    pub const fn new(algorithm: PayloadHashAlgorithm, digest: [u8; 32]) -> Self {
        Self { algorithm, digest }
    }

    /// Returns the digest algorithm.
    pub const fn algorithm(self) -> PayloadHashAlgorithm {
        self.algorithm
    }

    /// Returns the digest bytes.
    pub const fn digest(self) -> [u8; 32] {
        self.digest
    }
}

/// Durable evidence identifying the exact source payload behind a canonical record.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum PayloadReference {
    /// Algorithm-qualified content digest.
    ContentHash(PayloadHash),
    /// Bounded provider, object-store, file-manifest, or capture-record reference.
    SourceReference(SourceIdentifier),
}

/// A provenance or research-time invariant failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProvenanceError {
    /// Local receive time is later than local ingestion time.
    ReceivedAfterIngested,
    /// Point-in-time availability is later than ingestion time.
    AvailableAfterIngested,
    /// A source claims availability before its known publication time.
    AvailableBeforePublished,
    /// A superseding revision is not strictly later than publication.
    SupersededNotAfterPublished,
    /// A superseding revision is not strictly later than initial availability.
    SupersededNotAfterAvailable,
    /// Revision numbers are one-based.
    ZeroRevision,
}

impl fmt::Display for ProvenanceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ReceivedAfterIngested => {
                formatter.write_str("receive time must not be later than ingestion time")
            }
            Self::AvailableAfterIngested => {
                formatter.write_str("availability time must not be later than ingestion time")
            }
            Self::AvailableBeforePublished => {
                formatter.write_str("availability time must not precede known publication time")
            }
            Self::SupersededNotAfterPublished => {
                formatter.write_str("superseded time must be later than known publication time")
            }
            Self::SupersededNotAfterAvailable => {
                formatter.write_str("superseded time must be later than initial availability time")
            }
            Self::ZeroRevision => formatter.write_str("revision number must be nonzero"),
        }
    }
}

impl std::error::Error for ProvenanceError {}

/// Provenance common to canonical live and research records.
///
/// `available_at` is when the observation could first be used point-in-time. It can precede the
/// local `received_at` for historical extraction, so only each value's relationship to local
/// `ingested_at` is universally ordered.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct Provenance {
    schema_version: SchemaVersion,
    source_id: SourceId,
    instrument_id: Option<InstrumentId>,
    venue_id: Option<VenueId>,
    source_identifier: SourceIdentifier,
    source_timestamp: Option<Timestamp>,
    received_at: Timestamp,
    available_at: Timestamp,
    ingested_at: Timestamp,
    quality: DataQuality,
    payload_reference: PayloadReference,
}

impl Provenance {
    /// Constructs ordered record provenance without manufacturing optional identifiers or times.
    ///
    /// # Errors
    ///
    /// Returns [`ProvenanceError::ReceivedAfterIngested`] or
    /// [`ProvenanceError::AvailableAfterIngested`] for impossible local ordering.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        schema_version: SchemaVersion,
        source_id: SourceId,
        instrument_id: Option<InstrumentId>,
        venue_id: Option<VenueId>,
        source_identifier: SourceIdentifier,
        source_timestamp: Option<Timestamp>,
        received_at: Timestamp,
        available_at: Timestamp,
        ingested_at: Timestamp,
        quality: DataQuality,
        payload_reference: PayloadReference,
    ) -> Result<Self, ProvenanceError> {
        if received_at > ingested_at {
            return Err(ProvenanceError::ReceivedAfterIngested);
        }
        if available_at > ingested_at {
            return Err(ProvenanceError::AvailableAfterIngested);
        }
        Ok(Self {
            schema_version,
            source_id,
            instrument_id,
            venue_id,
            source_identifier,
            source_timestamp,
            received_at,
            available_at,
            ingested_at,
            quality,
            payload_reference,
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

    /// Returns the source timestamp without inventing a missing value.
    pub const fn source_timestamp(&self) -> Option<Timestamp> {
        self.source_timestamp
    }

    /// Returns when the source payload reached this process.
    pub const fn received_at(&self) -> Timestamp {
        self.received_at
    }

    /// Returns when the observation first became usable point-in-time.
    pub const fn available_at(&self) -> Timestamp {
        self.available_at
    }

    /// Returns when the canonical record was ingested locally.
    pub const fn ingested_at(&self) -> Timestamp {
        self.ingested_at
    }

    /// Returns the record's evidentiary quality class.
    pub const fn quality(&self) -> DataQuality {
        self.quality
    }

    /// Returns the retained payload evidence.
    pub const fn payload_reference(&self) -> &PayloadReference {
        &self.payload_reference
    }
}

#[derive(Deserialize)]
struct ProvenanceWire {
    schema_version: SchemaVersion,
    source_id: SourceId,
    instrument_id: Option<InstrumentId>,
    venue_id: Option<VenueId>,
    source_identifier: SourceIdentifier,
    source_timestamp: Option<Timestamp>,
    received_at: Timestamp,
    available_at: Timestamp,
    ingested_at: Timestamp,
    quality: DataQuality,
    payload_reference: PayloadReference,
}

impl<'de> Deserialize<'de> for Provenance {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = ProvenanceWire::deserialize(deserializer)?;
        Self::new(
            wire.schema_version,
            wire.source_id,
            wire.instrument_id,
            wire.venue_id,
            wire.source_identifier,
            wire.source_timestamp,
            wire.received_at,
            wire.available_at,
            wire.ingested_at,
            wire.quality,
            wire.payload_reference,
        )
        .map_err(serde::de::Error::custom)
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
    /// Constructs research time metadata without manufacturing an unavailable publication time.
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

    /// Returns the publication time when the source supplies it.
    pub const fn published_at(self) -> Option<Timestamp> {
        self.published_at
    }

    /// Returns the one-based revision number.
    pub const fn revision(self) -> RevisionNumber {
        self.revision
    }

    /// Returns when this revision ceased being the current vintage.
    pub const fn superseded_at(self) -> Option<Timestamp> {
        self.superseded_at
    }
}

#[derive(Deserialize)]
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

/// Provenance combined with research-specific point-in-time semantics.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ResearchContext {
    provenance: Provenance,
    time: ResearchTime,
}

impl ResearchContext {
    /// Combines provenance and research time after cross-contract ordering checks.
    ///
    /// # Errors
    ///
    /// Rejects availability before known publication and supersession at or before availability.
    pub fn new(provenance: Provenance, time: ResearchTime) -> Result<Self, ProvenanceError> {
        if let Some(published_at) = time.published_at
            && provenance.available_at < published_at
        {
            return Err(ProvenanceError::AvailableBeforePublished);
        }
        if let Some(superseded_at) = time.superseded_at
            && superseded_at <= provenance.available_at
        {
            return Err(ProvenanceError::SupersededNotAfterAvailable);
        }
        Ok(Self { provenance, time })
    }

    /// Returns common record provenance.
    pub const fn provenance(&self) -> &Provenance {
        &self.provenance
    }

    /// Returns research-specific point-in-time metadata.
    pub const fn time(&self) -> ResearchTime {
        self.time
    }
}

#[derive(Deserialize)]
struct ResearchContextWire {
    provenance: Provenance,
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
