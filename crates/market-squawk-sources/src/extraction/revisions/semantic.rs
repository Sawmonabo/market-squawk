//! Revision-independent exact PIT-v2 observation payloads.

use market_squawk_domain::{
    CorporateActionKind, DigestAlgorithm, EvidenceDigest, FundamentalFactContext,
    MacroMissingValue, PositionSide, QuantityLots, ResearchObservation, SourceIdentifier,
    XbrlFactEvidence,
};
use rust_decimal::Decimal;

use super::super::{ProviderNativeLineageRow, ProviderNativeLineageSchema};
use super::ObservedRevisionError;
use super::evidence::ObservedSemanticPayload;

pub(super) mod serializer;

use serializer::{PitV1CanonicalEncoder, PitV1EncodingControl, PitV1EncodingError};

const PIT_PAYLOAD_DOMAIN: &str = "market-squawk/pit/payload";
const PIT_PROVIDER_NATIVE_PAYLOAD_DOMAIN: &str = "market-squawk/pit/provider-native-payload";

/// Exact PIT-v2 payload bytes excluding provenance, time, and assigned revision.
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
        fact_context: &FundamentalFactContext,
        xbrl_evidence: Option<&XbrlFactEvidence>,
    ) -> Result<Self, ObservedRevisionError> {
        Self::encode(&|encoder| {
            encoder.u8(2)?;
            encoder.str(concept.as_str())?;
            encode_decimal(encoder, value)?;
            encoder.serializable(fact_context)?;
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
                    encoder.serializable(value.fact_context())?;
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
            ResearchObservation::MarketBar(value) => {
                Self::encode_with_control(control, &|encoder| {
                    encoder.u8(10)?;
                    encoder.str(value.provider_instrument_id().as_str())?;
                    encoder.str(value.feed().as_str())?;
                    encoder.str(value.interval().as_str())?;
                    encoder.u8(market_bar_adjustment_tag(value.adjustment()))?;
                    encode_market_bar_time(encoder, value.time_semantics())?;
                    encode_decimal(encoder, value.open().amount())?;
                    encode_decimal(encoder, value.high().amount())?;
                    encode_decimal(encoder, value.low().amount())?;
                    encode_decimal(encoder, value.close().amount())?;
                    encoder.str(value.currency().as_str())?;
                    encode_decimal(encoder, value.volume())?;
                    match value.trade_count() {
                        Some(count) => {
                            encoder.u8(1)?;
                            encoder.u64(count)?;
                        }
                        None => encoder.u8(0)?,
                    }
                    match value.vwap() {
                        Some(vwap) => {
                            encoder.u8(1)?;
                            encode_decimal(encoder, vwap.amount())
                        }
                        None => encoder.u8(0),
                    }
                })
            }
            ResearchObservation::FundNav(value) => Self::encode_with_control(control, &|encoder| {
                encoder.u8(11)?;
                encoder.str(value.provider_instrument_id().as_str())?;
                encoder.str(
                    value
                        .instrument_reference_revision()
                        .as_source_identifier()
                        .as_str(),
                )?;
                encoder.str(value.provider_product().as_source_identifier().as_str())?;
                encoder.str(value.provider_channel().as_source_identifier().as_str())?;
                encode_calendar_date(encoder, value.nav_date())?;
                encoder.u8(fund_nav_valuation_basis_tag(value.valuation_basis()))?;
                encoder.str(value.currency().as_str())?;
                match value.value() {
                    market_squawk_domain::FundNavValue::Observed(money) => {
                        encoder.u8(1)?;
                        encode_decimal(encoder, money.amount())?;
                    }
                    market_squawk_domain::FundNavValue::Missing(missing) => {
                        encoder.u8(2)?;
                        encoder.u8(fund_nav_missing_tag(missing))?;
                    }
                }
                encoder.i64(value.canonical_published_at().unix_nanos())?;
                encoder.serializable(value.lineage())?;
                encoder.serializable(value.revision_evidence())
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

    /// Returns the PIT-v2 domain-separated SHA-256 payload identity.
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

fn encode_calendar_date(
    encoder: &mut PitV1CanonicalEncoder<'_>,
    date: market_squawk_domain::CalendarDate,
) -> Result<(), PitV1EncodingError> {
    encoder.u16(date.year())?;
    encoder.u8(date.month())?;
    encoder.u8(date.day())
}

const fn fund_nav_valuation_basis_tag(basis: market_squawk_domain::FundNavValuationBasis) -> u8 {
    match basis {
        market_squawk_domain::FundNavValuationBasis::PerShare => 1,
    }
}

const fn fund_nav_missing_tag(missing: market_squawk_domain::FundNavMissingState) -> u8 {
    match missing {
        market_squawk_domain::FundNavMissingState::NotYetPublished => 1,
        market_squawk_domain::FundNavMissingState::Unsupported => 2,
        market_squawk_domain::FundNavMissingState::SourceMissing => 3,
        market_squawk_domain::FundNavMissingState::Invalid => 4,
        market_squawk_domain::FundNavMissingState::Unavailable => 5,
    }
}

const fn market_bar_adjustment_tag(adjustment: market_squawk_domain::MarketBarAdjustment) -> u8 {
    match adjustment {
        market_squawk_domain::MarketBarAdjustment::Raw => 1,
        market_squawk_domain::MarketBarAdjustment::Split => 2,
        market_squawk_domain::MarketBarAdjustment::Dividend => 3,
        market_squawk_domain::MarketBarAdjustment::SpinOff => 4,
        market_squawk_domain::MarketBarAdjustment::All => 5,
    }
}

fn encode_market_bar_time(
    encoder: &mut PitV1CanonicalEncoder<'_>,
    semantics: &market_squawk_domain::BarTimeSemantics,
) -> Result<(), PitV1EncodingError> {
    encoder.i64(semantics.period_start().unix_nanos())?;
    encoder.i64(semantics.period_end_exclusive().unix_nanos())?;
    encoder.u8(bar_timestamp_basis_tag(semantics.timestamp_basis()))?;
    let session = semantics.session();
    encoder.u8(market_bar_session_kind_tag(session.kind()))?;
    encoder.str(session.ruleset().as_str())?;
    let evidence = session.evidence();
    encoder.u8(digest_algorithm_tag(evidence.algorithm()))?;
    encoder.bytes(&evidence.bytes())
}

const fn bar_timestamp_basis_tag(basis: market_squawk_domain::BarTimestampBasis) -> u8 {
    match basis {
        market_squawk_domain::BarTimestampBasis::PeriodStart => 1,
        market_squawk_domain::BarTimestampBasis::PeriodEnd => 2,
    }
}

const fn market_bar_session_kind_tag(kind: market_squawk_domain::MarketBarSessionKind) -> u8 {
    match kind {
        market_squawk_domain::MarketBarSessionKind::Regular => 1,
        market_squawk_domain::MarketBarSessionKind::Extended => 2,
        market_squawk_domain::MarketBarSessionKind::Continuous => 3,
        market_squawk_domain::MarketBarSessionKind::ProviderDefined => 4,
    }
}

const fn digest_algorithm_tag(algorithm: DigestAlgorithm) -> u8 {
    match algorithm {
        DigestAlgorithm::Sha256 => 1,
        DigestAlgorithm::Blake3 => 2,
    }
}

impl TryFrom<&CanonicalObservationPayload> for ObservedSemanticPayload {
    type Error = ObservedRevisionError;

    fn try_from(value: &CanonicalObservationPayload) -> Result<Self, Self::Error> {
        Self::try_from_bytes(value.exact_bytes())
    }
}

impl ObservedSemanticPayload {
    pub(super) fn try_from_canonical_and_native(
        canonical: &CanonicalObservationPayload,
        schema: ProviderNativeLineageSchema,
        native_row: &ProviderNativeLineageRow,
    ) -> Result<Self, ObservedRevisionError> {
        let mut control = NoopEncodingControl;
        let (exact_bytes, _) = encode_exact(
            PIT_PROVIDER_NATIVE_PAYLOAD_DOMAIN,
            "semantic_payload",
            super::MAX_OBSERVED_SEMANTIC_PAYLOAD_BYTES,
            &mut control,
            &|encoder| {
                encoder.bytes(canonical.exact_bytes())?;
                encoder.u16(schema.version())?;
                encoder.u8(schema.implementation().tag())?;
                encode_digest(encoder, schema.fingerprint())?;
                encode_digest(encoder, native_row.semantic_payload_digest())
            },
        )?;
        Self::try_from_bytes(&exact_bytes)
    }
}

fn encode_digest(
    encoder: &mut PitV1CanonicalEncoder<'_>,
    digest: EvidenceDigest,
) -> Result<(), PitV1EncodingError> {
    encoder.u8(digest_algorithm_tag(digest.algorithm()))?;
    encoder.bytes(&digest.bytes())
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
