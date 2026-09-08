//! Canonical natural-family encodings shared with point-in-time selection.

use market_squawk_domain::{
    DigestAlgorithm, EvidenceDigest, FundamentalPeriod, ResearchContext, ResearchObservation,
    ResearchTemporalCoordinate, SourceId, SourceIdentifier,
};

use super::semantic::encode_exact;
use super::{
    ObservedRevisionError, PitV1CanonicalEncoder, PitV1EncodingControl, PitV1EncodingError,
};

const PIT_FAMILY_DOMAIN: &str = "market-squawk/pit/family";
const MAX_CANONICAL_OBSERVATION_FAMILY_BYTES: usize = 64 * 1024;

/// Exact PIT-v2 natural-family bytes and their SHA-256 identity.
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
    /// The encoding is `MSQPIT`, PIT identity schema `2`, the length-framed
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
    /// schema version 2.
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
                    encoder.str(value.concept().as_str())?;
                    encoder.str(value.unit().as_str())?;
                    encode_fundamental_family_context(encoder, value.fact_context())
                }
                ResearchObservation::Macro(value) => {
                    encoder.u8(3)?;
                    encoder.str(provenance.source_id().as_str())?;
                    encoder.str(value.series().as_str())?;
                    encode_coordinate(encoder, context.time().effective())
                }
                ResearchObservation::MarketBar(value) => {
                    encoder.u8(10)?;
                    encoder.str(provenance.source_id().as_str())?;
                    encoder.bytes(required_instrument()?.as_uuid().as_bytes())?;
                    encoder.str(
                        provenance
                            .venue_id()
                            .ok_or(PitV1EncodingError::Encoding)?
                            .as_str(),
                    )?;
                    encoder.str(value.provider_instrument_id().as_str())?;
                    encoder.str(value.feed().as_str())?;
                    encoder.str(value.interval().as_str())?;
                    encoder.u8(market_bar_adjustment_tag(value.adjustment()))?;
                    encode_market_bar_series_semantics(encoder, value.time_semantics())?;
                    encode_coordinate(encoder, context.time().effective())
                }
                ResearchObservation::FundNav(value) => {
                    encoder.u8(11)?;
                    encoder.str(provenance.source_id().as_str())?;
                    encoder.bytes(required_instrument()?.as_uuid().as_bytes())?;
                    encoder.str(value.provider_product().as_source_identifier().as_str())?;
                    encoder.str(value.provider_channel().as_source_identifier().as_str())?;
                    encoder.str(value.provider_instrument_id().as_str())?;
                    encode_calendar_date(encoder, value.nav_date())?;
                    encoder.u8(fund_nav_valuation_basis_tag(value.valuation_basis()))?;
                    encoder.str(value.currency().as_str())
                }
                ResearchObservation::PortfolioPosition(value) => {
                    encoder.u8(4)?;
                    encoder.str(provenance.source_id().as_str())?;
                    encoder.bytes(required_instrument()?.as_uuid().as_bytes())?;
                    encoder.str(value.account_id().as_str())?;
                    encode_coordinate(encoder, context.time().effective())
                }
                ResearchObservation::Transaction(value) => {
                    match provenance.instrument_id() {
                        Some(instrument) => {
                            // Tag 9 extends the current PIT schema with instrument-scoped
                            // transactions while preserving tag 5 for account-scoped records.
                            encoder.u8(9)?;
                            encoder.str(provenance.source_id().as_str())?;
                            encoder.bytes(instrument.as_uuid().as_bytes())?;
                        }
                        None => {
                            encoder.u8(5)?;
                            encoder.str(provenance.source_id().as_str())?;
                        }
                    }
                    encoder.str(value.account_id().as_str())?;
                    encoder.str(value.source_record_id().as_str())
                }
                ResearchObservation::CorporateAction(_) => {
                    encoder.u8(6)?;
                    encoder.str(provenance.source_id().as_str())?;
                    encoder.bytes(required_instrument()?.as_uuid().as_bytes())?;
                    encoder.str(provenance.source_identifier().as_str())
                }
                ResearchObservation::UniverseMembership(value) => {
                    encoder.u8(8)?;
                    encoder.str(provenance.source_id().as_str())?;
                    encoder.bytes(required_instrument()?.as_uuid().as_bytes())?;
                    encoder.str(provenance.source_identifier().as_str())?;
                    encoder.str(value.universe().as_str())
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
        ResearchObservation::MarketBar(value) => value.context(),
        ResearchObservation::FundNav(value) => value.context(),
        ResearchObservation::PortfolioPosition(value) => value.context(),
        ResearchObservation::Transaction(value) => value.context(),
        ResearchObservation::CorporateAction(value) => value.context(),
        ResearchObservation::UniverseMembership(value) => value.context(),
        ResearchObservation::AlternativeData(value) => value.context(),
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

const fn market_bar_adjustment_tag(adjustment: market_squawk_domain::MarketBarAdjustment) -> u8 {
    match adjustment {
        market_squawk_domain::MarketBarAdjustment::Raw => 1,
        market_squawk_domain::MarketBarAdjustment::Split => 2,
        market_squawk_domain::MarketBarAdjustment::Dividend => 3,
        market_squawk_domain::MarketBarAdjustment::SpinOff => 4,
        market_squawk_domain::MarketBarAdjustment::All => 5,
    }
}

fn encode_market_bar_series_semantics(
    encoder: &mut PitV1CanonicalEncoder<'_>,
    semantics: &market_squawk_domain::BarTimeSemantics,
) -> Result<(), PitV1EncodingError> {
    encoder.u8(bar_timestamp_basis_tag(semantics.timestamp_basis()))?;
    encode_market_bar_session(encoder, semantics.session())
}

fn encode_market_bar_session(
    encoder: &mut PitV1CanonicalEncoder<'_>,
    session: &market_squawk_domain::MarketBarSessionEvidence,
) -> Result<(), PitV1EncodingError> {
    encoder.u8(market_bar_session_kind_tag(session.kind()))?;
    encoder.str(session.ruleset().as_str())
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

fn encode_fundamental_period(
    encoder: &mut PitV1CanonicalEncoder<'_>,
    period: FundamentalPeriod,
) -> Result<(), PitV1EncodingError> {
    match period {
        FundamentalPeriod::Instant { instant } => {
            encoder.u8(1)?;
            encoder.u16(instant.year())?;
            encoder.u8(instant.month())?;
            encoder.u8(instant.day())
        }
        FundamentalPeriod::Duration { start, end } => {
            encoder.u8(2)?;
            encoder.u16(start.year())?;
            encoder.u8(start.month())?;
            encoder.u8(start.day())?;
            encoder.u16(end.year())?;
            encoder.u8(end.month())?;
            encoder.u8(end.day())
        }
    }
}

fn encode_fundamental_family_context(
    encoder: &mut PitV1CanonicalEncoder<'_>,
    context: &market_squawk_domain::FundamentalFactContext,
) -> Result<(), PitV1EncodingError> {
    encode_fundamental_period(encoder, context.period())?;
    encoder.serializable(context.dimensions())?;
    encoder.serializable(&context.consolidation())
}

struct NoopEncodingControl;

impl PitV1EncodingControl for NoopEncodingControl {
    fn checkpoint(&mut self) -> Result<(), PitV1EncodingError> {
        Ok(())
    }
}
