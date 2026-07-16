//! Archive-safe live provenance without execution authority.

use serde::{Deserialize, Deserializer, Serialize};

use super::{PayloadReference, ProvenanceError, ensure_current_schema};
use crate::{
    CoverageStatus, DataQuality, ExecutionEligibility, InstrumentId, LiveEvidenceBinding,
    SchemaVersion, SourceId, SourceIdentifier, Timestamp, VenueId,
};

/// Origin of the classification retained by a provenance record.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LiveRecordState {
    /// Decoder output has no retained assessment reference.
    Decoded,
    /// An archival classification retains an assessment reference but no runtime authority.
    RecordedAssessment,
}

/// Cohesive parameters for unqualified decoder output.
#[derive(Clone, Debug)]
pub struct DecodedLiveProvenanceInput {
    binding: LiveEvidenceBinding,
    source_timestamp: Option<Timestamp>,
    received_at: Timestamp,
    available_at: Timestamp,
    ingested_at: Timestamp,
    recorded_quality: DataQuality,
    recorded_coverage: CoverageStatus,
    payload_reference: PayloadReference,
}

impl DecodedLiveProvenanceInput {
    /// Collects decoder output. Validation occurs in [`LiveProvenance::decoded`].
    #[expect(
        clippy::too_many_arguments,
        reason = "decoder provenance timestamps and evidence must be captured atomically"
    )]
    pub const fn new(
        binding: LiveEvidenceBinding,
        source_timestamp: Option<Timestamp>,
        received_at: Timestamp,
        available_at: Timestamp,
        ingested_at: Timestamp,
        recorded_quality: DataQuality,
        recorded_coverage: CoverageStatus,
        payload_reference: PayloadReference,
    ) -> Self {
        Self {
            binding,
            source_timestamp,
            received_at,
            available_at,
            ingested_at,
            recorded_quality,
            recorded_coverage,
            payload_reference,
        }
    }
}

/// Cohesive parameters for an archival assessment assertion.
#[derive(Clone, Debug)]
pub struct RecordedLiveProvenanceInput {
    binding: LiveEvidenceBinding,
    source_timestamp: Option<Timestamp>,
    received_at: Timestamp,
    available_at: Timestamp,
    ingested_at: Timestamp,
    recorded_quality: DataQuality,
    recorded_coverage: CoverageStatus,
    payload_reference: PayloadReference,
    assessment_reference: SourceIdentifier,
}

impl RecordedLiveProvenanceInput {
    /// Collects an archival classification and its durable assessment reference.
    #[expect(
        clippy::too_many_arguments,
        reason = "an archival assertion must retain its evidence reference atomically"
    )]
    pub const fn new(
        binding: LiveEvidenceBinding,
        source_timestamp: Option<Timestamp>,
        received_at: Timestamp,
        available_at: Timestamp,
        ingested_at: Timestamp,
        recorded_quality: DataQuality,
        recorded_coverage: CoverageStatus,
        payload_reference: PayloadReference,
        assessment_reference: SourceIdentifier,
    ) -> Self {
        Self {
            binding,
            source_timestamp,
            received_at,
            available_at,
            ingested_at,
            recorded_quality,
            recorded_coverage,
            payload_reference,
            assessment_reference,
        }
    }
}

/// Provenance carried by every canonical live market event.
///
/// This is a serializable archive/research record, never an execution capability. In particular,
/// a recorded `DirectVerified` classification is only a historical assertion linked to retained
/// evidence. [`Self::execution_eligibility`] therefore always returns `Ineligible`. The stateful
/// live-plane evaluator introduced with the source registry owns the future short-lived,
/// non-serializable execution token; the domain crate intentionally defines no such token.
///
/// ```compile_fail
/// use market_squawk_domain::LiveProvenance;
/// fn cannot_authorize(record: &LiveProvenance) {
///     let _authority = record.current_execution_authority();
/// }
/// ```
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct LiveProvenance {
    schema_version: SchemaVersion,
    binding: LiveEvidenceBinding,
    source_timestamp: Option<Timestamp>,
    received_at: Timestamp,
    available_at: Timestamp,
    ingested_at: Timestamp,
    recorded_quality: DataQuality,
    recorded_coverage: CoverageStatus,
    payload_reference: PayloadReference,
    assessment_reference: Option<SourceIdentifier>,
    #[serde(skip_serializing)]
    record_state: LiveRecordState,
}

impl LiveProvenance {
    /// Constructs unqualified decoder output using the current schema.
    ///
    /// # Errors
    ///
    /// Rejects `DirectVerified`, a local-time order other than
    /// `received_at <= available_at <= ingested_at`, and a content-hash mismatch.
    pub fn decoded(input: DecodedLiveProvenanceInput) -> Result<Self, ProvenanceError> {
        if input.recorded_quality == DataQuality::DirectVerified {
            return Err(ProvenanceError::UnqualifiedDirectVerified);
        }
        validate_common(
            &input.binding,
            input.received_at,
            input.available_at,
            input.ingested_at,
            &input.payload_reference,
        )?;
        Ok(Self {
            schema_version: SchemaVersion::CURRENT,
            binding: input.binding,
            source_timestamp: input.source_timestamp,
            received_at: input.received_at,
            available_at: input.available_at,
            ingested_at: input.ingested_at,
            recorded_quality: input.recorded_quality,
            recorded_coverage: input.recorded_coverage,
            payload_reference: input.payload_reference,
            assessment_reference: None,
            record_state: LiveRecordState::Decoded,
        })
    }

    /// Constructs an archival classification using the current schema.
    ///
    /// This does not and cannot mint current execution authority.
    ///
    /// # Errors
    ///
    /// Rejects a local-time order other than `received_at <= available_at <= ingested_at` and a
    /// content-hash mismatch.
    pub fn recorded(input: RecordedLiveProvenanceInput) -> Result<Self, ProvenanceError> {
        validate_common(
            &input.binding,
            input.received_at,
            input.available_at,
            input.ingested_at,
            &input.payload_reference,
        )?;
        Ok(Self {
            schema_version: SchemaVersion::CURRENT,
            binding: input.binding,
            source_timestamp: input.source_timestamp,
            received_at: input.received_at,
            available_at: input.available_at,
            ingested_at: input.ingested_at,
            recorded_quality: input.recorded_quality,
            recorded_coverage: input.recorded_coverage,
            payload_reference: input.payload_reference,
            assessment_reference: Some(input.assessment_reference),
            record_state: LiveRecordState::RecordedAssessment,
        })
    }

    /// Returns the record schema version.
    pub const fn schema_version(&self) -> SchemaVersion {
        self.schema_version
    }
    /// Returns the immutable complete evidence binding.
    pub const fn binding(&self) -> &LiveEvidenceBinding {
        &self.binding
    }
    /// Returns the source namespace.
    pub const fn source_id(&self) -> &SourceId {
        self.binding.source_id()
    }
    /// Returns the stable instrument identity.
    pub const fn instrument_id(&self) -> Option<InstrumentId> {
        Some(self.binding.instrument_id())
    }
    /// Returns the venue identity.
    pub const fn venue_id(&self) -> Option<&VenueId> {
        Some(self.binding.venue_id())
    }
    /// Returns the source-native record identifier.
    pub const fn source_identifier(&self) -> &SourceIdentifier {
        self.binding.source_identifier()
    }
    /// Returns the source timestamp without inventing one.
    pub const fn source_timestamp(&self) -> Option<Timestamp> {
        self.source_timestamp
    }
    /// Returns when the source payload reached this process.
    pub const fn received_at(&self) -> Timestamp {
        self.received_at
    }
    /// Returns when the received live observation became available to local consumers.
    pub const fn available_at(&self) -> Timestamp {
        self.available_at
    }
    /// Returns when the canonical event was ingested locally.
    pub const fn ingested_at(&self) -> Timestamp {
        self.ingested_at
    }
    /// Returns the archival data-quality classification, not live authority.
    pub const fn recorded_quality(&self) -> DataQuality {
        self.recorded_quality
    }
    /// Compatibility view of the archival classification.
    pub const fn quality(&self) -> DataQuality {
        self.recorded_quality
    }
    /// Returns the connection generation.
    pub const fn connection_generation(&self) -> crate::ConnectionGeneration {
        self.binding.connection_generation()
    }
    /// Returns the compact archival coverage result.
    pub const fn recorded_coverage(&self) -> CoverageStatus {
        self.recorded_coverage
    }
    /// Returns retained payload evidence.
    pub const fn payload_reference(&self) -> &PayloadReference {
        &self.payload_reference
    }
    /// Returns the retained assessment reference when this is an archival assertion.
    pub const fn assessment_reference(&self) -> Option<&SourceIdentifier> {
        self.assessment_reference.as_ref()
    }
    /// Returns how the archival classification entered this in-memory value.
    pub const fn record_state(&self) -> LiveRecordState {
        self.record_state
    }

    /// Returns archive-facing execution eligibility.
    ///
    /// The result is unconditionally `Ineligible`; only the future stateful live-plane evaluator
    /// may produce its private, short-lived execution token.
    pub const fn execution_eligibility(&self) -> ExecutionEligibility {
        ExecutionEligibility::Ineligible
    }

    /// Returns true because every archive/decoder record requires live-plane requalification.
    pub const fn requires_requalification(&self) -> bool {
        true
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct LiveProvenanceWire {
    schema_version: SchemaVersion,
    binding: LiveEvidenceBinding,
    source_timestamp: Option<Timestamp>,
    received_at: Timestamp,
    available_at: Timestamp,
    ingested_at: Timestamp,
    recorded_quality: DataQuality,
    recorded_coverage: CoverageStatus,
    payload_reference: PayloadReference,
    assessment_reference: Option<SourceIdentifier>,
}

impl<'de> Deserialize<'de> for LiveProvenance {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = LiveProvenanceWire::deserialize(deserializer)?;
        ensure_current_schema(wire.schema_version).map_err(serde::de::Error::custom)?;
        if wire.recorded_quality == DataQuality::DirectVerified
            && wire.assessment_reference.is_none()
        {
            return Err(serde::de::Error::custom(
                ProvenanceError::MissingAssessmentReference,
            ));
        }
        validate_common(
            &wire.binding,
            wire.received_at,
            wire.available_at,
            wire.ingested_at,
            &wire.payload_reference,
        )
        .map_err(serde::de::Error::custom)?;
        let record_state = if wire.assessment_reference.is_some() {
            LiveRecordState::RecordedAssessment
        } else {
            LiveRecordState::Decoded
        };
        Ok(Self {
            schema_version: wire.schema_version,
            binding: wire.binding,
            source_timestamp: wire.source_timestamp,
            received_at: wire.received_at,
            available_at: wire.available_at,
            ingested_at: wire.ingested_at,
            recorded_quality: wire.recorded_quality,
            recorded_coverage: wire.recorded_coverage,
            payload_reference: wire.payload_reference,
            assessment_reference: wire.assessment_reference,
            record_state,
        })
    }
}

fn validate_common(
    binding: &LiveEvidenceBinding,
    received_at: Timestamp,
    available_at: Timestamp,
    ingested_at: Timestamp,
    payload_reference: &PayloadReference,
) -> Result<(), ProvenanceError> {
    if available_at < received_at {
        return Err(ProvenanceError::AvailabilityBeforeReceived);
    }
    if available_at > ingested_at {
        return Err(ProvenanceError::AvailabilityAfterIngested);
    }
    if let PayloadReference::ContentHash(hash) = payload_reference
        && (hash.algorithm() != binding.payload_digest().algorithm()
            || hash.digest() != binding.payload_digest().bytes())
    {
        return Err(ProvenanceError::PayloadDigestMismatch);
    }
    Ok(())
}
