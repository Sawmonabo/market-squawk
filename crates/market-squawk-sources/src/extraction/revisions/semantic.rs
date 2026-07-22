//! Revision-independent exact PIT-v1 observation payloads.

use market_squawk_domain::{
    CorporateActionKind, DigestAlgorithm, EvidenceDigest, MacroMissingValue, PositionSide,
    QuantityLots, ResearchObservation, SourceIdentifier, XbrlFactEvidence,
};
use rust_decimal::Decimal;

use super::ObservedRevisionError;
use super::evidence::ObservedSemanticPayload;

pub(super) mod serializer;

use serializer::{PitV1CanonicalEncoder, PitV1EncodingControl, PitV1EncodingError};

const PIT_PAYLOAD_DOMAIN: &str = "market-squawk/pit/payload";

/// Exact PIT-v1 payload bytes excluding context, provenance, time, and revision.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CanonicalObservationPayload {
    exact_bytes: Box<[u8]>,
    identity: EvidenceDigest,
}

impl CanonicalObservationPayload {
    /// Constructs a filing payload from its revision-independent fields.
    pub fn filing(
        form_type: &SourceIdentifier,
        accession: &SourceIdentifier,
    ) -> Result<Self, ObservedRevisionError> {
        Self::encode(&|encoder| {
            encoder.u8(1)?;
            encoder.str(form_type.as_str())?;
            encoder.str(accession.as_str())
        })
    }

    /// Constructs an exact fundamental payload with optional occurrence-level XBRL evidence.
    pub fn fundamental(
        concept: &SourceIdentifier,
        value: Decimal,
        unit: &SourceIdentifier,
        xbrl_evidence: Option<&XbrlFactEvidence>,
    ) -> Result<Self, ObservedRevisionError> {
        Self::encode(&|encoder| {
            encoder.u8(2)?;
            encoder.str(concept.as_str())?;
            encode_decimal(encoder, value)?;
            encoder.str(unit.as_str())?;
            encode_optional_serializable(encoder, xbrl_evidence)
        })
    }

    /// Constructs a macro payload carrying an exact observed decimal.
    pub fn macro_observed(
        series: &SourceIdentifier,
        value: Decimal,
        unit: &SourceIdentifier,
    ) -> Result<Self, ObservedRevisionError> {
        Self::encode(&|encoder| {
            encoder.u8(3)?;
            encoder.str(series.as_str())?;
            encoder.u8(1)?;
            encode_decimal(encoder, value)?;
            encoder.str(unit.as_str())
        })
    }

    /// Constructs a macro payload carrying provider-native missing-value evidence.
    pub fn macro_missing(
        series: &SourceIdentifier,
        missing: &MacroMissingValue,
        unit: &SourceIdentifier,
    ) -> Result<Self, ObservedRevisionError> {
        Self::encode(&|encoder| {
            encoder.u8(3)?;
            encoder.str(series.as_str())?;
            encoder.u8(2)?;
            encoder.str(missing.marker().as_str())?;
            encoder.option_str(missing.reason().map(SourceIdentifier::as_str))?;
            encoder.str(unit.as_str())
        })
    }

    /// Constructs an exact portfolio-position payload.
    pub fn portfolio_position(
        account_id: &SourceIdentifier,
        side: PositionSide,
        absolute_quantity: QuantityLots,
    ) -> Result<Self, ObservedRevisionError> {
        Self::encode(&|encoder| {
            encoder.u8(4)?;
            encoder.str(account_id.as_str())?;
            encoder.u8(match side {
                PositionSide::Long => 1,
                PositionSide::Short => 2,
            })?;
            encoder.i64(absolute_quantity.get())
        })
    }

    /// Constructs an exact transaction payload.
    pub fn transaction(
        account_id: &SourceIdentifier,
        transaction_type: &SourceIdentifier,
        source_record_id: &SourceIdentifier,
    ) -> Result<Self, ObservedRevisionError> {
        Self::encode(&|encoder| {
            encoder.u8(5)?;
            encoder.str(account_id.as_str())?;
            encoder.str(transaction_type.as_str())?;
            encoder.str(source_record_id.as_str())
        })
    }

    /// Constructs an exact typed corporate-action payload.
    pub fn corporate_action(action: &CorporateActionKind) -> Result<Self, ObservedRevisionError> {
        Self::encode(&|encoder| {
            encoder.u8(6)?;
            encoder.serializable(action)
        })
    }

    /// Constructs an exact alternative-data scalar payload.
    pub fn alternative_data(
        dataset: &SourceIdentifier,
        field: &SourceIdentifier,
        value: Decimal,
        unit: Option<&SourceIdentifier>,
    ) -> Result<Self, ObservedRevisionError> {
        Self::encode(&|encoder| {
            encoder.u8(7)?;
            encoder.str(dataset.as_str())?;
            encoder.str(field.as_str())?;
            encode_decimal(encoder, value)?;
            encoder.option_str(unit.map(SourceIdentifier::as_str))
        })
    }

    /// Derives only the variant payload from a finalized observation.
    ///
    /// Context, provenance, effective/publication coordinates, and revision are deliberately
    /// excluded. Producers should prefer the typed constructors before final observation creation.
    pub fn try_from_observation(
        observation: &ResearchObservation,
    ) -> Result<Self, ObservedRevisionError> {
        let mut control = NoopEncodingControl;
        Self::try_from_observation_with_control(observation, &mut control)
    }

    /// Derives the payload while preserving a caller's cooperative encoding control.
    #[doc(hidden)]
    pub fn try_from_observation_with_control(
        observation: &ResearchObservation,
        control: &mut dyn PitV1EncodingControl,
    ) -> Result<Self, ObservedRevisionError> {
        match observation {
            ResearchObservation::Filing(value) => Self::encode_with_control(control, &|encoder| {
                encoder.u8(1)?;
                encoder.str(value.form_type().as_str())?;
                encoder.str(value.accession().as_str())
            }),
            ResearchObservation::Fundamental(value) => {
                Self::encode_with_control(control, &|encoder| {
                    encoder.u8(2)?;
                    encoder.str(value.concept().as_str())?;
                    encode_decimal(encoder, value.value())?;
                    encoder.str(value.unit().as_str())?;
                    encode_optional_serializable(encoder, value.xbrl_evidence())
                })
            }
            ResearchObservation::Macro(value) => Self::encode_with_control(control, &|encoder| {
                encoder.u8(3)?;
                encoder.str(value.series().as_str())?;
                if let Some(observed) = value.value().observed_value() {
                    encoder.u8(1)?;
                    encode_decimal(encoder, observed)?;
                } else if let Some(missing) = value.value().missing_value() {
                    encoder.u8(2)?;
                    encoder.str(missing.marker().as_str())?;
                    encoder.option_str(missing.reason().map(SourceIdentifier::as_str))?;
                } else {
                    return Err(PitV1EncodingError::Encoding);
                }
                encoder.str(value.unit().as_str())
            }),
            ResearchObservation::PortfolioPosition(value) => {
                Self::encode_with_control(control, &|encoder| {
                    encoder.u8(4)?;
                    encoder.str(value.account_id().as_str())?;
                    encoder.u8(match value.side() {
                        PositionSide::Long => 1,
                        PositionSide::Short => 2,
                    })?;
                    encoder.i64(value.absolute_quantity().get())
                })
            }
            ResearchObservation::Transaction(value) => {
                Self::encode_with_control(control, &|encoder| {
                    encoder.u8(5)?;
                    encoder.str(value.account_id().as_str())?;
                    encoder.str(value.transaction_type().as_str())?;
                    encoder.str(value.source_record_id().as_str())
                })
            }
            ResearchObservation::CorporateAction(value) => {
                Self::encode_with_control(control, &|encoder| {
                    encoder.u8(6)?;
                    encoder.serializable(value.action())
                })
            }
            ResearchObservation::UniverseMembership(value) => {
                Self::encode_with_control(control, &|encoder| {
                    encoder.u8(8)?;
                    encoder.str(value.universe().as_str())?;
                    encoder.i64(value.effective_interval().starts_at().unix_nanos())?;
                    match value.effective_interval().ends_at() {
                        Some(end) => {
                            encoder.u8(1)?;
                            encoder.i64(end.unix_nanos())
                        }
                        None => encoder.u8(0),
                    }
                })
            }
            ResearchObservation::AlternativeData(value) => {
                Self::encode_with_control(control, &|encoder| {
                    encoder.u8(7)?;
                    encoder.str(value.dataset().as_str())?;
                    encoder.str(value.field().as_str())?;
                    encode_decimal(encoder, value.value())?;
                    encoder.option_str(value.unit().map(SourceIdentifier::as_str))
                })
            }
        }
    }

    /// Returns complete canonical payload bytes retained for collision-safe comparison.
    pub fn exact_bytes(&self) -> &[u8] {
        &self.exact_bytes
    }

    /// Returns the PIT-v1 domain-separated SHA-256 payload identity.
    pub const fn identity(&self) -> EvidenceDigest {
        self.identity
    }

    fn encode<F>(encode: &F) -> Result<Self, ObservedRevisionError>
    where
        F: Fn(&mut PitV1CanonicalEncoder<'_>) -> Result<(), PitV1EncodingError>,
    {
        let mut control = NoopEncodingControl;
        Self::encode_with_control(&mut control, encode)
    }

    fn encode_with_control<F>(
        control: &mut dyn PitV1EncodingControl,
        encode: &F,
    ) -> Result<Self, ObservedRevisionError>
    where
        F: Fn(&mut PitV1CanonicalEncoder<'_>) -> Result<(), PitV1EncodingError>,
    {
        let (exact_bytes, identity) = encode_exact(
            PIT_PAYLOAD_DOMAIN,
            "semantic_payload",
            super::MAX_OBSERVED_SEMANTIC_PAYLOAD_BYTES,
            control,
            encode,
        )?;
        Ok(Self {
            exact_bytes,
            identity,
        })
    }
}

impl TryFrom<&CanonicalObservationPayload> for ObservedSemanticPayload {
    type Error = ObservedRevisionError;

    fn try_from(value: &CanonicalObservationPayload) -> Result<Self, Self::Error> {
        Self::try_from_bytes(value.exact_bytes())
    }
}

pub(super) fn encode_exact<F>(
    domain: &str,
    field: &'static str,
    max_encoded_bytes: usize,
    control: &mut dyn PitV1EncodingControl,
    encode: &F,
) -> Result<(Box<[u8]>, EvidenceDigest), ObservedRevisionError>
where
    F: Fn(&mut PitV1CanonicalEncoder<'_>) -> Result<(), PitV1EncodingError>,
{
    let (expected_identity, expected_len) = {
        let mut encoder = PitV1CanonicalEncoder::new_bounded(domain, max_encoded_bytes, control)
            .map_err(|error| map_encoding_error(error, field, max_encoded_bytes))?;
        encode(&mut encoder)
            .map_err(|error| map_encoding_error(error, field, max_encoded_bytes))?;
        encoder.finish_with_len()
    };
    let (identity, bytes, actual_len) = {
        let mut encoder = PitV1CanonicalEncoder::collecting_exact_bounded(
            domain,
            expected_len,
            max_encoded_bytes,
            control,
        )
        .map_err(|error| map_encoding_error(error, field, max_encoded_bytes))?;
        encode(&mut encoder)
            .map_err(|error| map_encoding_error(error, field, max_encoded_bytes))?;
        encoder
            .finish_with_bytes()
            .map_err(|error| map_encoding_error(error, field, max_encoded_bytes))?
    };
    if identity != expected_identity || actual_len != expected_len || bytes.len() != expected_len {
        return Err(ObservedRevisionError::CanonicalEncoding);
    }
    Ok((
        bytes.into_boxed_slice(),
        EvidenceDigest::new(DigestAlgorithm::Sha256, identity),
    ))
}

fn encode_decimal(
    encoder: &mut PitV1CanonicalEncoder<'_>,
    value: Decimal,
) -> Result<(), PitV1EncodingError> {
    let normalized = value.normalize();
    encoder.i128(normalized.mantissa())?;
    encoder.u32(normalized.scale())
}

fn encode_optional_serializable<T: serde::Serialize>(
    encoder: &mut PitV1CanonicalEncoder<'_>,
    value: Option<&T>,
) -> Result<(), PitV1EncodingError> {
    match value {
        Some(value) => {
            encoder.u8(1)?;
            encoder.serializable(value)
        }
        None => encoder.u8(0),
    }
}

fn map_encoding_error(
    error: PitV1EncodingError,
    field: &'static str,
    max: usize,
) -> ObservedRevisionError {
    match error {
        PitV1EncodingError::Encoding => ObservedRevisionError::CanonicalEncoding,
        PitV1EncodingError::AllocationFailure => ObservedRevisionError::AllocationFailure,
        PitV1EncodingError::AccountingOverflow => ObservedRevisionError::ByteCountOverflow,
        PitV1EncodingError::LimitExceeded => {
            ObservedRevisionError::EvidenceLimitExceeded { field, max }
        }
        PitV1EncodingError::Cancelled => ObservedRevisionError::Cancelled,
        PitV1EncodingError::DeadlineExceeded => ObservedRevisionError::DeadlineExceeded,
    }
}

struct NoopEncodingControl;

impl PitV1EncodingControl for NoopEncodingControl {
    fn checkpoint(&mut self) -> Result<(), PitV1EncodingError> {
        Ok(())
    }
}
