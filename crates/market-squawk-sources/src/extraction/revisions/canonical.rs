//! Canonical natural-family encodings shared with point-in-time selection.

use market_squawk_domain::{
    DigestAlgorithm, EvidenceDigest, ResearchContext, ResearchObservation,
    ResearchTemporalCoordinate, SourceId, SourceIdentifier,
};

use super::semantic::encode_exact;
use super::{
    ObservedRevisionError, PitV1CanonicalEncoder, PitV1EncodingControl, PitV1EncodingError,
};

const PIT_FAMILY_DOMAIN: &str = "market-squawk/pit/family";
const MAX_CANONICAL_OBSERVATION_FAMILY_BYTES: usize = 64 * 1024;

/// Exact PIT-v1 natural-family bytes and their SHA-256 identity.
///
/// Digest equality is never sufficient authority: [`Self::exact_bytes`] is retained so durable
/// authorities can compare the complete canonical evidence on every digest hit.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CanonicalObservationFamily {
    source_id: SourceId,
    exact_bytes: Box<[u8]>,
    identity: EvidenceDigest,
}

impl CanonicalObservationFamily {
    /// Constructs the exact Macro family used by the point-in-time selector.
    ///
    /// The encoding is `MSQPIT`, PIT identity schema `1`, the length-framed
    /// `market-squawk/pit/family` domain, Macro tag `3`, source, series, and the exact effective
    /// coordinate. Value, revision, ingestion time, and whole-response evidence are excluded.
    ///
    /// # Errors
    ///
    /// Returns a checked byte, allocation, or canonical-coordinate error.
    pub fn macro_observation(
        source_id: &SourceId,
        series: &SourceIdentifier,
        effective: &ResearchTemporalCoordinate,
    ) -> Result<Self, ObservedRevisionError> {
        Self::encode(source_id.clone(), &|encoder| {
            encoder.u8(3)?;
            encoder.str(source_id.as_str())?;
            encoder.str(series.as_str())?;
            encode_coordinate(encoder, effective)
        })
    }

    /// Derives the exact variant-specific family from a finalized canonical observation.
    ///
    /// Payload, revision, publication, availability, ingestion, and manifest evidence are
    /// deliberately excluded. The resulting bytes are exactly compatible with PIT identity
    /// schema version 1.
    ///
    /// # Errors
    ///
    /// Returns [`ObservedRevisionError::CanonicalEncoding`] when a variant requiring an
    /// instrument lacks one, or when its temporal coordinate cannot be encoded exactly.
    pub fn try_from_observation(
        observation: &ResearchObservation,
    ) -> Result<Self, ObservedRevisionError> {
        let context = observation_context(observation);
        let provenance = context.provenance();
        let source_id = provenance.source_id().clone();
        Self::encode(source_id, &|encoder| {
            let required_instrument = || {
                provenance
                    .instrument_id()
                    .ok_or(PitV1EncodingError::Encoding)
            };
            match observation {
                ResearchObservation::Filing(value) => {
                    encoder.u8(1)?;
                    encoder.str(provenance.source_id().as_str())?;
                    encoder.bytes(required_instrument()?.as_uuid().as_bytes())?;
                    encoder.str(value.accession().as_str())
                }
                ResearchObservation::Fundamental(value) => {
                    encoder.u8(2)?;
                    encoder.str(provenance.source_id().as_str())?;
                    encoder.bytes(required_instrument()?.as_uuid().as_bytes())?;
                    encoder.str(provenance.source_identifier().as_str())?;
                    encoder.str(value.concept().as_str())?;
                    encoder.str(value.unit().as_str())?;
                    encode_coordinate(encoder, context.time().effective())
                }
                ResearchObservation::Macro(value) => {
                    encoder.u8(3)?;
                    encoder.str(provenance.source_id().as_str())?;
                    encoder.str(value.series().as_str())?;
                    encode_coordinate(encoder, context.time().effective())
                }
                ResearchObservation::PortfolioPosition(value) => {
                    encoder.u8(4)?;
                    encoder.str(provenance.source_id().as_str())?;
                    encoder.bytes(required_instrument()?.as_uuid().as_bytes())?;
                    encoder.str(value.account_id().as_str())?;
                    encode_coordinate(encoder, context.time().effective())
                }
                ResearchObservation::Transaction(value) => {
                    encoder.u8(5)?;
                    encoder.str(provenance.source_id().as_str())?;
                    encoder.str(value.account_id().as_str())?;
                    encoder.str(value.source_record_id().as_str())
                }
                ResearchObservation::CorporateAction(_) => {
                    encoder.u8(6)?;
                    encoder.str(provenance.source_id().as_str())?;
                    encoder.bytes(required_instrument()?.as_uuid().as_bytes())?;
                    encoder.str(provenance.source_identifier().as_str())
                }
                ResearchObservation::AlternativeData(value) => {
                    encoder.u8(7)?;
                    encoder.str(provenance.source_id().as_str())?;
                    match provenance.instrument_id() {
                        Some(instrument) => {
                            encoder.u8(1)?;
                            encoder.bytes(instrument.as_uuid().as_bytes())?;
                        }
                        None => encoder.u8(0)?,
                    }
                    encoder.str(provenance.source_identifier().as_str())?;
                    encoder.str(value.dataset().as_str())?;
                    encoder.str(value.field().as_str())?;
                    encode_coordinate(encoder, context.time().effective())
                }
            }
        })
    }

    /// Returns the single source bound into the canonical family.
    pub const fn source_id(&self) -> &SourceId {
        &self.source_id
    }

    /// Returns the complete canonical bytes retained for collision-safe comparison.
    pub fn exact_bytes(&self) -> &[u8] {
        &self.exact_bytes
    }

    /// Returns the SHA-256 identity of the exact canonical bytes.
    pub const fn identity(&self) -> EvidenceDigest {
        self.identity
    }

    pub(super) fn retained_bytes(&self) -> Result<usize, ObservedRevisionError> {
        self.source_id
            .retained_bytes()
            .checked_add(self.exact_bytes.len())
            .ok_or(ObservedRevisionError::ByteCountOverflow)
    }

    fn encode<F>(source_id: SourceId, encode: &F) -> Result<Self, ObservedRevisionError>
    where
        F: Fn(&mut PitV1CanonicalEncoder<'_>) -> Result<(), PitV1EncodingError>,
    {
        let mut control = NoopEncodingControl;
        let (exact_bytes, identity) = encode_exact(
            PIT_FAMILY_DOMAIN,
            "observation_family",
            MAX_CANONICAL_OBSERVATION_FAMILY_BYTES,
            &mut control,
            encode,
        )?;
        if identity.algorithm() != DigestAlgorithm::Sha256 {
            return Err(ObservedRevisionError::CanonicalEncoding);
        }
        Ok(Self {
            source_id,
            exact_bytes,
            identity,
        })
    }
}

fn observation_context(observation: &ResearchObservation) -> &ResearchContext {
    match observation {
        ResearchObservation::Filing(value) => value.context(),
        ResearchObservation::Fundamental(value) => value.context(),
        ResearchObservation::Macro(value) => value.context(),
        ResearchObservation::PortfolioPosition(value) => value.context(),
        ResearchObservation::Transaction(value) => value.context(),
        ResearchObservation::CorporateAction(value) => value.context(),
        ResearchObservation::AlternativeData(value) => value.context(),
    }
}

fn encode_coordinate(
    encoder: &mut PitV1CanonicalEncoder<'_>,
    coordinate: &ResearchTemporalCoordinate,
) -> Result<(), PitV1EncodingError> {
    if let Some(timestamp) = coordinate.exact_timestamp() {
        encoder.u8(1)?;
        encoder.i64(timestamp.unix_nanos())
    } else if let Some(date) = coordinate.calendar_date_value() {
        encoder.u8(2)?;
        encoder.u16(date.year())?;
        encoder.u8(date.month())?;
        encoder.u8(date.day())
    } else if let Some(period) = coordinate.source_period_value() {
        encoder.u8(3)?;
        encoder.str(period.scheme().as_str())?;
        encoder.u16(period.year())?;
        encoder.u16(period.ordinal().get())?;
        encoder.str(period.code().as_str())
    } else {
        Err(PitV1EncodingError::Encoding)
    }
}

struct NoopEncodingControl;

impl PitV1EncodingControl for NoopEncodingControl {
    fn checkpoint(&mut self) -> Result<(), PitV1EncodingError> {
        Ok(())
    }
}
