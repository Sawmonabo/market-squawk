//! Live provenance with an explicit decode-before-qualification boundary.

use serde::{Deserialize, Deserializer, Serialize};

use super::{PayloadReference, ProvenanceError, ensure_current_schema};
use crate::{
    ConnectionGeneration, DataQuality, ExecutionEligibility, InstrumentId, QualificationEvidence,
    QualificationEvidenceId, SchemaVersion, SourceCoverageEvidence, SourceId, SourceIdentifier,
    Timestamp, VenueId,
};

/// Runtime status of a live provenance quality assertion.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LiveVerificationState {
    /// Decoder output has not completed live qualification.
    DecodedUnqualified,
    /// This process successfully evaluated the retained qualification evidence.
    QualifiedCurrent,
    /// A persisted verified label is readable for audit but requires current requalification.
    RecordedRequiresRequalification,
}

/// Provenance carried by every canonical live market event.
///
/// A decoder creates this type with [`Self::decoded`], which rejects `DirectVerified`. After book,
/// sequence, checksum, timing, status, precision, and coverage validation, the live evaluator may
/// consume it with [`Self::promote`] and a successful [`QualificationEvidence`]. This makes the
/// decode-before-book boundary explicit without creating a circular dependency.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct LiveProvenance {
    schema_version: SchemaVersion,
    source_id: SourceId,
    instrument_id: Option<InstrumentId>,
    venue_id: Option<VenueId>,
    source_identifier: SourceIdentifier,
    source_timestamp: Option<Timestamp>,
    received_at: Timestamp,
    ingested_at: Timestamp,
    quality: DataQuality,
    connection_generation: ConnectionGeneration,
    coverage: SourceCoverageEvidence,
    payload_reference: PayloadReference,
    qualification_evidence_id: Option<QualificationEvidenceId>,
    #[serde(skip_serializing)]
    verification_state: LiveVerificationState,
}

impl LiveProvenance {
    /// Constructs unqualified decoder output using the current schema.
    ///
    /// # Errors
    ///
    /// Rejects `DirectVerified` and local receive time after ingestion.
    #[allow(clippy::too_many_arguments)]
    pub fn decoded(
        source_id: SourceId,
        instrument_id: Option<InstrumentId>,
        venue_id: Option<VenueId>,
        source_identifier: SourceIdentifier,
        source_timestamp: Option<Timestamp>,
        received_at: Timestamp,
        ingested_at: Timestamp,
        quality: DataQuality,
        connection_generation: ConnectionGeneration,
        coverage: SourceCoverageEvidence,
        payload_reference: PayloadReference,
    ) -> Result<Self, ProvenanceError> {
        if quality == DataQuality::DirectVerified {
            return Err(ProvenanceError::UnqualifiedDirectVerified);
        }
        if received_at > ingested_at {
            return Err(ProvenanceError::ReceivedAfterIngested);
        }
        Ok(Self {
            schema_version: SchemaVersion::CURRENT,
            source_id,
            instrument_id,
            venue_id,
            source_identifier,
            source_timestamp,
            received_at,
            ingested_at,
            quality,
            connection_generation,
            coverage,
            payload_reference,
            qualification_evidence_id: None,
            verification_state: LiveVerificationState::DecodedUnqualified,
        })
    }

    /// Promotes decoded provenance only with matching, eligible, direct-verified evidence.
    ///
    /// # Errors
    ///
    /// Rejects ineligible evidence and any identity, generation, timing, or coverage mismatch.
    pub fn promote(
        mut self,
        qualification: &QualificationEvidence,
    ) -> Result<Self, ProvenanceError> {
        if qualification.execution_eligibility() != ExecutionEligibility::Eligible
            || qualification.quality() != DataQuality::DirectVerified
        {
            return Err(ProvenanceError::QualificationNotEligible);
        }
        if qualification.source_id() != &self.source_id
            || Some(qualification.instrument_id()) != self.instrument_id
            || Some(qualification.venue_id()) != self.venue_id.as_ref()
            || qualification.connection_generation() != self.connection_generation
        {
            return Err(ProvenanceError::QualificationIdentityMismatch);
        }
        if qualification.source_coverage() != self.coverage {
            return Err(ProvenanceError::QualificationCoverageMismatch);
        }
        let market_timing = qualification
            .timing()
            .latest_market_event()
            .ok_or(ProvenanceError::QualificationTimingMismatch)?;
        if market_timing.source_timestamp() != self.source_timestamp
            || market_timing.received_at() != self.received_at
        {
            return Err(ProvenanceError::QualificationTimingMismatch);
        }
        self.quality = qualification.quality();
        self.qualification_evidence_id = Some(qualification.evidence_id().clone());
        self.verification_state = LiveVerificationState::QualifiedCurrent;
        Ok(self)
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

    /// Returns the source timestamp without inventing one.
    pub const fn source_timestamp(&self) -> Option<Timestamp> {
        self.source_timestamp
    }

    /// Returns when the source payload reached this process.
    pub const fn received_at(&self) -> Timestamp {
        self.received_at
    }

    /// Returns when the canonical event was ingested locally.
    pub const fn ingested_at(&self) -> Timestamp {
        self.ingested_at
    }

    /// Returns evaluator-derived or explicitly unqualified data quality.
    pub const fn quality(&self) -> DataQuality {
        self.quality
    }

    /// Returns the connection generation carried by every live event.
    pub const fn connection_generation(&self) -> ConnectionGeneration {
        self.connection_generation
    }

    /// Returns explicit source coverage carried by every live event.
    pub const fn coverage(&self) -> SourceCoverageEvidence {
        self.coverage
    }

    /// Returns retained payload evidence.
    pub const fn payload_reference(&self) -> &PayloadReference {
        &self.payload_reference
    }

    /// Returns the qualification audit identity only after successful promotion.
    pub const fn qualification_evidence_id(&self) -> Option<&QualificationEvidenceId> {
        self.qualification_evidence_id.as_ref()
    }

    /// Returns the runtime interpretation of the recorded quality assertion.
    pub const fn verification_state(&self) -> LiveVerificationState {
        self.verification_state
    }

    /// Returns whether this process produced a current qualification proof.
    pub const fn is_currently_qualified(&self) -> bool {
        matches!(
            self.verification_state,
            LiveVerificationState::QualifiedCurrent
        )
    }

    /// Returns whether an archival verified label must be requalified before live use.
    pub const fn requires_requalification(&self) -> bool {
        matches!(
            self.verification_state,
            LiveVerificationState::RecordedRequiresRequalification
        )
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct LiveProvenanceWire {
    schema_version: SchemaVersion,
    source_id: SourceId,
    instrument_id: Option<InstrumentId>,
    venue_id: Option<VenueId>,
    source_identifier: SourceIdentifier,
    source_timestamp: Option<Timestamp>,
    received_at: Timestamp,
    ingested_at: Timestamp,
    quality: DataQuality,
    connection_generation: ConnectionGeneration,
    coverage: SourceCoverageEvidence,
    payload_reference: PayloadReference,
    qualification_evidence_id: Option<QualificationEvidenceId>,
}

impl<'de> Deserialize<'de> for LiveProvenance {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = LiveProvenanceWire::deserialize(deserializer)?;
        ensure_current_schema(wire.schema_version).map_err(serde::de::Error::custom)?;
        if wire.quality == DataQuality::DirectVerified && wire.qualification_evidence_id.is_none() {
            return Err(serde::de::Error::custom(
                ProvenanceError::MissingQualificationEvidenceId,
            ));
        }
        if wire.quality != DataQuality::DirectVerified && wire.qualification_evidence_id.is_some() {
            return Err(serde::de::Error::custom(
                ProvenanceError::QualificationNotEligible,
            ));
        }
        if wire.received_at > wire.ingested_at {
            return Err(serde::de::Error::custom(
                ProvenanceError::ReceivedAfterIngested,
            ));
        }
        let verification_state = if wire.quality == DataQuality::DirectVerified {
            LiveVerificationState::RecordedRequiresRequalification
        } else {
            LiveVerificationState::DecodedUnqualified
        };
        Ok(Self {
            schema_version: wire.schema_version,
            source_id: wire.source_id,
            instrument_id: wire.instrument_id,
            venue_id: wire.venue_id,
            source_identifier: wire.source_identifier,
            source_timestamp: wire.source_timestamp,
            received_at: wire.received_at,
            ingested_at: wire.ingested_at,
            quality: wire.quality,
            connection_generation: wire.connection_generation,
            coverage: wire.coverage,
            payload_reference: wire.payload_reference,
            qualification_evidence_id: wire.qualification_evidence_id,
            verification_state,
        })
    }
}
