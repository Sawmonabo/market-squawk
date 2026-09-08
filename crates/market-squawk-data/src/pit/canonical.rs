//! Explicit versioned canonical family, payload, provenance, and evidence encodings.

use market_squawk_domain::{
    AvailabilityEvidence, DataQuality, DigestAlgorithm, FundamentalPeriod, PayloadReference,
    PositionSide, ResearchObservation, ResearchTemporalCoordinate,
};
use rust_decimal::Decimal;
use serde::Serialize;

#[path = "canonical/serializer.rs"]
mod serializer;

pub(super) use serializer::{CanonicalEncoder, CanonicalEncodingError};

use super::model::observation_context;
use super::retained::{OperationControl, RetainedBudget};
use super::{
    PointInTimeCandidate, PointInTimeError, PointInTimePolicy, PointInTimeRequest,
    PointInTimeRevisionMode, PointInTimeRevisionState,
};
use crate::Sha256Digest;

const FAMILY_DOMAIN: &str = "market-squawk/pit/family";
const PAYLOAD_DOMAIN: &str = "market-squawk/pit/payload";
const PROVENANCE_DOMAIN: &str = "market-squawk/pit/provenance";
const EVIDENCE_DOMAIN: &str = "market-squawk/pit/evidence";
pub(super) const CONTENT_DOMAIN: &str = "market-squawk/pit/content";
pub(super) const AUDIT_DOMAIN: &str = "market-squawk/pit/audit";

pub(super) struct FamilyEncoding {
    pub(super) bytes: Vec<u8>,
    pub(super) identity: Sha256Digest,
}

pub(super) fn family_encoding<'a>(
    candidate: &PointInTimeCandidate,
    control: &mut OperationControl,
    budget: &mut RetainedBudget,
) -> Result<FamilyEncoding, PointInTimeError<'a>> {
    let mut counting = CanonicalEncoder::new(FAMILY_DOMAIN, control).map_err(map_error)?;
    encode_candidate_family(&mut counting, candidate).map_err(map_error)?;
    let (expected_identity, expected_len) = counting.finish_with_len();
    budget.charge(expected_len)?;

    let mut collecting = CanonicalEncoder::collecting_exact(FAMILY_DOMAIN, expected_len, control)
        .map_err(map_error)?;
    encode_candidate_family(&mut collecting, candidate).map_err(map_error)?;
    let (identity, bytes, actual_len) = collecting.finish_with_bytes().map_err(map_error)?;
    if identity != expected_identity
        || actual_len != expected_len
        || bytes.len() != expected_len
        || bytes.capacity() != expected_len
    {
        return Err(PointInTimeError::CanonicalEncoding);
    }
    Ok(FamilyEncoding { bytes, identity })
}

pub(super) fn payload_identity<'a>(
    candidate: &PointInTimeCandidate,
    control: &mut OperationControl,
) -> Result<Sha256Digest, PointInTimeError<'a>> {
    let mut encoder = CanonicalEncoder::new(PAYLOAD_DOMAIN, control).map_err(map_error)?;
    match candidate.observation() {
        ResearchObservation::Filing(value) => {
            encoder.u8(1).map_err(map_error)?;
            encoder.str(value.form_type().as_str()).map_err(map_error)?;
            encoder.str(value.accession().as_str()).map_err(map_error)?;
        }
        ResearchObservation::Fundamental(value) => {
            encoder.u8(2).map_err(map_error)?;
            encoder.str(value.concept().as_str()).map_err(map_error)?;
            encode_decimal(&mut encoder, value.value()).map_err(map_error)?;
            encoder
                .serializable(value.fact_context())
                .map_err(map_error)?;
            encode_optional_serializable(&mut encoder, value.xbrl_evidence()).map_err(map_error)?;
        }
        ResearchObservation::Macro(value) => {
            encoder.u8(3).map_err(map_error)?;
            encoder.str(value.series().as_str()).map_err(map_error)?;
            if let Some(observed) = value.value().observed_value() {
                encoder.u8(1).map_err(map_error)?;
                encode_decimal(&mut encoder, observed).map_err(map_error)?;
            } else if let Some(missing) = value.value().missing_value() {
                encoder.u8(2).map_err(map_error)?;
                encoder.str(missing.marker().as_str()).map_err(map_error)?;
                encoder
                    .option_str(missing.reason().map(|reason| reason.as_str()))
                    .map_err(map_error)?;
            } else {
                return Err(PointInTimeError::CanonicalEncoding);
            }
            encoder.str(value.unit().as_str()).map_err(map_error)?;
        }
        ResearchObservation::MarketBar(value) => {
            encoder.u8(10).map_err(map_error)?;
            encoder
                .str(value.provider_instrument_id().as_str())
                .map_err(map_error)?;
            encoder.str(value.feed().as_str()).map_err(map_error)?;
            encoder.str(value.interval().as_str()).map_err(map_error)?;
            encoder
                .u8(market_bar_adjustment_tag(value.adjustment()))
                .map_err(map_error)?;
            encode_market_bar_time(&mut encoder, value.time_semantics()).map_err(map_error)?;
            encode_decimal(&mut encoder, value.open().amount()).map_err(map_error)?;
            encode_decimal(&mut encoder, value.high().amount()).map_err(map_error)?;
            encode_decimal(&mut encoder, value.low().amount()).map_err(map_error)?;
            encode_decimal(&mut encoder, value.close().amount()).map_err(map_error)?;
            encoder.str(value.currency().as_str()).map_err(map_error)?;
            encode_decimal(&mut encoder, value.volume()).map_err(map_error)?;
            match value.trade_count() {
                Some(count) => {
                    encoder.u8(1).map_err(map_error)?;
                    encoder.u64(count).map_err(map_error)?;
                }
                None => encoder.u8(0).map_err(map_error)?,
            }
            match value.vwap() {
                Some(vwap) => {
                    encoder.u8(1).map_err(map_error)?;
                    encode_decimal(&mut encoder, vwap.amount()).map_err(map_error)?;
                }
                None => encoder.u8(0).map_err(map_error)?,
            }
        }
        ResearchObservation::FundNav(value) => {
            encoder.u8(11).map_err(map_error)?;
            encoder
                .str(value.provider_instrument_id().as_str())
                .map_err(map_error)?;
            encoder
                .str(
                    value
                        .instrument_reference_revision()
                        .as_source_identifier()
                        .as_str(),
                )
                .map_err(map_error)?;
            encoder
                .str(value.provider_product().as_source_identifier().as_str())
                .map_err(map_error)?;
            encoder
                .str(value.provider_channel().as_source_identifier().as_str())
                .map_err(map_error)?;
            encode_calendar_date(&mut encoder, value.nav_date()).map_err(map_error)?;
            encoder
                .u8(fund_nav_valuation_basis_tag(value.valuation_basis()))
                .map_err(map_error)?;
            encoder.str(value.currency().as_str()).map_err(map_error)?;
            match value.value() {
                market_squawk_domain::FundNavValue::Observed(money) => {
                    encoder.u8(1).map_err(map_error)?;
                    encode_decimal(&mut encoder, money.amount()).map_err(map_error)?;
                }
                market_squawk_domain::FundNavValue::Missing(missing) => {
                    encoder.u8(2).map_err(map_error)?;
                    encoder
                        .u8(fund_nav_missing_tag(missing))
                        .map_err(map_error)?;
                }
            }
            encode_timestamp(&mut encoder, value.canonical_published_at()).map_err(map_error)?;
            encoder.serializable(value.lineage()).map_err(map_error)?;
            encoder
                .serializable(value.revision_evidence())
                .map_err(map_error)?;
        }
        ResearchObservation::PortfolioPosition(value) => {
            encoder.u8(4).map_err(map_error)?;
            encoder
                .str(value.account_id().as_str())
                .map_err(map_error)?;
            encoder
                .u8(match value.side() {
                    PositionSide::Long => 1,
                    PositionSide::Short => 2,
                })
                .map_err(map_error)?;
            encoder
                .i64(value.absolute_quantity().get())
                .map_err(map_error)?;
        }
        ResearchObservation::Transaction(value) => {
            encoder.u8(5).map_err(map_error)?;
            encoder
                .str(value.account_id().as_str())
                .map_err(map_error)?;
            encoder
                .str(value.transaction_type().as_str())
                .map_err(map_error)?;
            encoder
                .str(value.source_record_id().as_str())
                .map_err(map_error)?;
        }
        ResearchObservation::CorporateAction(value) => {
            encoder.u8(6).map_err(map_error)?;
            encoder.serializable(value.action()).map_err(map_error)?;
        }
        ResearchObservation::UniverseMembership(value) => {
            encoder.u8(8).map_err(map_error)?;
            encoder.str(value.universe().as_str()).map_err(map_error)?;
            encode_timestamp(&mut encoder, value.effective_interval().starts_at())
                .map_err(map_error)?;
            encode_optional_timestamp(&mut encoder, value.effective_interval().ends_at())
                .map_err(map_error)?;
        }
        ResearchObservation::AlternativeData(value) => {
            encoder.u8(7).map_err(map_error)?;
            encoder.str(value.dataset().as_str()).map_err(map_error)?;
            encoder.str(value.field().as_str()).map_err(map_error)?;
            encode_decimal(&mut encoder, value.value()).map_err(map_error)?;
            encoder
                .option_str(value.unit().map(|unit| unit.as_str()))
                .map_err(map_error)?;
        }
    }
    Ok(encoder.finish())
}

pub(super) fn provenance_identity<'a>(
    candidate: &PointInTimeCandidate,
    control: &mut OperationControl,
) -> Result<Sha256Digest, PointInTimeError<'a>> {
    let mut encoder = CanonicalEncoder::new(PROVENANCE_DOMAIN, control).map_err(map_error)?;
    let provenance = observation_context(candidate.observation()).provenance();
    encoder
        .u16(provenance.schema_version().get())
        .map_err(map_error)?;
    encoder
        .str(provenance.source_id().as_str())
        .map_err(map_error)?;
    match provenance.instrument_id() {
        Some(instrument) => {
            encoder.u8(1).map_err(map_error)?;
            encoder
                .bytes(instrument.as_uuid().as_bytes())
                .map_err(map_error)?;
        }
        None => encoder.u8(0).map_err(map_error)?,
    }
    encoder
        .option_str(provenance.venue_id().map(|venue| venue.as_str()))
        .map_err(map_error)?;
    encoder
        .str(provenance.source_identifier().as_str())
        .map_err(map_error)?;
    encode_optional_timestamp(&mut encoder, provenance.source_timestamp()).map_err(map_error)?;
    encode_timestamp(&mut encoder, provenance.received_at()).map_err(map_error)?;
    encode_timestamp(&mut encoder, provenance.ingested_at()).map_err(map_error)?;
    encoder
        .u8(data_quality_tag(provenance.quality()))
        .map_err(map_error)?;
    encode_payload_reference(&mut encoder, provenance.payload_reference()).map_err(map_error)?;
    encode_availability(&mut encoder, provenance.availability()).map_err(map_error)?;
    Ok(encoder.finish())
}

pub(super) fn evidence_identity<'a>(
    candidate: &PointInTimeCandidate,
    family_identity: Sha256Digest,
    payload_identity: Sha256Digest,
    provenance_identity: Sha256Digest,
    control: &mut OperationControl,
) -> Result<Sha256Digest, PointInTimeError<'a>> {
    let mut encoder = CanonicalEncoder::new(EVIDENCE_DOMAIN, control).map_err(map_error)?;
    encoder.digest(family_identity).map_err(map_error)?;
    encoder.digest(payload_identity).map_err(map_error)?;
    encoder.digest(provenance_identity).map_err(map_error)?;
    encode_temporal_context(&mut encoder, candidate).map_err(map_error)?;
    let manifest = candidate.source_manifest();
    encoder
        .str(manifest.dataset_id().as_str())
        .map_err(map_error)?;
    encoder
        .u64(manifest.manifest_version())
        .map_err(map_error)?;
    encoder.str(manifest.schema().name()).map_err(map_error)?;
    encoder
        .u16(manifest.schema().version().get())
        .map_err(map_error)?;
    encoder
        .bytes(&manifest.schema().fingerprint())
        .map_err(map_error)?;
    encoder.digest(manifest.content_hash()).map_err(map_error)?;
    Ok(encoder.finish())
}

pub(super) fn encode_request(
    encoder: &mut CanonicalEncoder<'_>,
    request: &PointInTimeRequest,
) -> Result<(), CanonicalEncodingError> {
    encode_policy(encoder, request.policy())?;
    encode_timestamp(encoder, request.as_of())?;
    encode_optional_coordinate(encoder, request.publication_cutoff())?;
    encode_coordinate(encoder, request.effective_cutoff())?;
    encode_optional_coordinate(encoder, request.label_cutoff())
}

fn encode_policy(
    encoder: &mut CanonicalEncoder<'_>,
    policy: PointInTimePolicy,
) -> Result<(), CanonicalEncodingError> {
    encoder.u32(policy.version().get())?;
    encoder.u8(match policy.revision_mode() {
        PointInTimeRevisionMode::LatestKnown => 1,
        PointInTimeRevisionMode::AllKnown => 2,
    })
}

pub(super) fn encode_revision_state(
    encoder: &mut CanonicalEncoder<'_>,
    state: PointInTimeRevisionState,
) -> Result<(), CanonicalEncodingError> {
    encoder.u8(match state {
        PointInTimeRevisionState::Current => 1,
        PointInTimeRevisionState::Superseded => 2,
        PointInTimeRevisionState::SupersessionIncomparable => 3,
    })
}

fn encode_candidate_family(
    encoder: &mut CanonicalEncoder<'_>,
    candidate: &PointInTimeCandidate,
) -> Result<(), CanonicalEncodingError> {
    let context = observation_context(candidate.observation());
    let provenance = context.provenance();
    let required_instrument = || {
        provenance
            .instrument_id()
            .ok_or(CanonicalEncodingError::Encoding)
    };
    match candidate.observation() {
        ResearchObservation::Filing(value) => {
            encoder.u8(1)?;
            encoder.str(provenance.source_id().as_str())?;
            encoder.bytes(required_instrument()?.as_uuid().as_bytes())?;
            encoder.str(value.accession().as_str())?;
        }
        ResearchObservation::Fundamental(value) => {
            encoder.u8(2)?;
            encoder.str(provenance.source_id().as_str())?;
            encoder.bytes(required_instrument()?.as_uuid().as_bytes())?;
            encoder.str(value.concept().as_str())?;
            encoder.str(value.unit().as_str())?;
            encode_fundamental_family_context(encoder, value.fact_context())?;
        }
        ResearchObservation::Macro(value) => {
            encoder.u8(3)?;
            encoder.str(provenance.source_id().as_str())?;
            encoder.str(value.series().as_str())?;
            encode_coordinate(encoder, context.time().effective())?;
        }
        ResearchObservation::MarketBar(value) => {
            encoder.u8(10)?;
            encoder.str(provenance.source_id().as_str())?;
            encoder.bytes(required_instrument()?.as_uuid().as_bytes())?;
            encoder.str(
                provenance
                    .venue_id()
                    .ok_or(CanonicalEncodingError::Encoding)?
                    .as_str(),
            )?;
            encoder.str(value.provider_instrument_id().as_str())?;
            encoder.str(value.feed().as_str())?;
            encoder.str(value.interval().as_str())?;
            encoder.u8(market_bar_adjustment_tag(value.adjustment()))?;
            encode_market_bar_series_semantics(encoder, value.time_semantics())?;
            encode_coordinate(encoder, context.time().effective())?;
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
            encoder.str(value.currency().as_str())?;
        }
        ResearchObservation::PortfolioPosition(value) => {
            encoder.u8(4)?;
            encoder.str(provenance.source_id().as_str())?;
            encoder.bytes(required_instrument()?.as_uuid().as_bytes())?;
            encoder.str(value.account_id().as_str())?;
            encode_coordinate(encoder, context.time().effective())?;
        }
        ResearchObservation::Transaction(value) => {
            match provenance.instrument_id() {
                Some(instrument) => {
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
            encoder.str(value.source_record_id().as_str())?;
        }
        ResearchObservation::CorporateAction(_) => {
            encoder.u8(6)?;
            encoder.str(provenance.source_id().as_str())?;
            encoder.bytes(required_instrument()?.as_uuid().as_bytes())?;
            encoder.str(provenance.source_identifier().as_str())?;
        }
        ResearchObservation::UniverseMembership(value) => {
            encoder.u8(8)?;
            encoder.str(provenance.source_id().as_str())?;
            encoder.bytes(required_instrument()?.as_uuid().as_bytes())?;
            encoder.str(provenance.source_identifier().as_str())?;
            encoder.str(value.universe().as_str())?;
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
            encode_coordinate(encoder, context.time().effective())?;
        }
    }
    Ok(())
}

fn encode_temporal_context(
    encoder: &mut CanonicalEncoder<'_>,
    candidate: &PointInTimeCandidate,
) -> Result<(), CanonicalEncodingError> {
    let time = observation_context(candidate.observation()).time();
    encode_coordinate(encoder, time.effective())?;
    encode_optional_coordinate(encoder, time.published())?;
    encoder.u32(time.revision().get())?;
    encode_optional_coordinate(encoder, time.superseded())
}

pub(super) fn encode_coordinate(
    encoder: &mut CanonicalEncoder<'_>,
    coordinate: &ResearchTemporalCoordinate,
) -> Result<(), CanonicalEncodingError> {
    if let Some(timestamp) = coordinate.exact_timestamp() {
        encoder.u8(1)?;
        encode_timestamp(encoder, timestamp)
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
        Err(CanonicalEncodingError::Encoding)
    }
}

fn encode_fundamental_period(
    encoder: &mut CanonicalEncoder<'_>,
    period: FundamentalPeriod,
) -> Result<(), CanonicalEncodingError> {
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
    encoder: &mut CanonicalEncoder<'_>,
    context: &market_squawk_domain::FundamentalFactContext,
) -> Result<(), CanonicalEncodingError> {
    encode_fundamental_period(encoder, context.period())?;
    encoder.serializable(context.dimensions())?;
    encoder.serializable(&context.consolidation())
}

fn encode_optional_coordinate(
    encoder: &mut CanonicalEncoder<'_>,
    coordinate: Option<&ResearchTemporalCoordinate>,
) -> Result<(), CanonicalEncodingError> {
    match coordinate {
        Some(value) => {
            encoder.u8(1)?;
            encode_coordinate(encoder, value)
        }
        None => encoder.u8(0),
    }
}

fn encode_timestamp(
    encoder: &mut CanonicalEncoder<'_>,
    timestamp: market_squawk_domain::Timestamp,
) -> Result<(), CanonicalEncodingError> {
    encoder.i64(timestamp.unix_nanos())
}

fn encode_optional_timestamp(
    encoder: &mut CanonicalEncoder<'_>,
    timestamp: Option<market_squawk_domain::Timestamp>,
) -> Result<(), CanonicalEncodingError> {
    match timestamp {
        Some(value) => {
            encoder.u8(1)?;
            encode_timestamp(encoder, value)
        }
        None => encoder.u8(0),
    }
}

fn encode_decimal(
    encoder: &mut CanonicalEncoder<'_>,
    value: Decimal,
) -> Result<(), CanonicalEncodingError> {
    let normalized = value.normalize();
    encoder.i128(normalized.mantissa())?;
    encoder.u32(normalized.scale())
}

fn encode_calendar_date(
    encoder: &mut CanonicalEncoder<'_>,
    date: market_squawk_domain::CalendarDate,
) -> Result<(), CanonicalEncodingError> {
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

fn encode_market_bar_series_semantics(
    encoder: &mut CanonicalEncoder<'_>,
    semantics: &market_squawk_domain::BarTimeSemantics,
) -> Result<(), CanonicalEncodingError> {
    encoder.u8(bar_timestamp_basis_tag(semantics.timestamp_basis()))?;
    encode_market_bar_session_family(encoder, semantics.session())
}

fn encode_market_bar_time(
    encoder: &mut CanonicalEncoder<'_>,
    semantics: &market_squawk_domain::BarTimeSemantics,
) -> Result<(), CanonicalEncodingError> {
    encoder.i64(semantics.period_start().unix_nanos())?;
    encoder.i64(semantics.period_end_exclusive().unix_nanos())?;
    encoder.u8(bar_timestamp_basis_tag(semantics.timestamp_basis()))?;
    encode_market_bar_session(encoder, semantics.session())
}

fn encode_market_bar_session_family(
    encoder: &mut CanonicalEncoder<'_>,
    session: &market_squawk_domain::MarketBarSessionEvidence,
) -> Result<(), CanonicalEncodingError> {
    encoder.u8(market_bar_session_kind_tag(session.kind()))?;
    encoder.str(session.ruleset().as_str())
}

fn encode_market_bar_session(
    encoder: &mut CanonicalEncoder<'_>,
    session: &market_squawk_domain::MarketBarSessionEvidence,
) -> Result<(), CanonicalEncodingError> {
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

fn encode_payload_reference(
    encoder: &mut CanonicalEncoder<'_>,
    reference: &PayloadReference,
) -> Result<(), CanonicalEncodingError> {
    match reference {
        PayloadReference::ContentHash(hash) => {
            encoder.u8(1)?;
            encoder.u8(match hash.algorithm() {
                DigestAlgorithm::Sha256 => 1,
                DigestAlgorithm::Blake3 => 2,
            })?;
            encoder.bytes(&hash.digest())
        }
        PayloadReference::SourceReference(reference) => {
            encoder.u8(2)?;
            encoder.str(reference.as_str())
        }
    }
}

fn encode_availability(
    encoder: &mut CanonicalEncoder<'_>,
    availability: &AvailabilityEvidence,
) -> Result<(), CanonicalEncodingError> {
    match availability {
        AvailabilityEvidence::Evidenced {
            available_at,
            evidence,
        } => {
            encoder.u8(1)?;
            encode_timestamp(encoder, *available_at)?;
            encoder.str(evidence.as_str())
        }
        AvailabilityEvidence::LocalFirstObserved { observed_at } => {
            encoder.u8(2)?;
            encode_timestamp(encoder, *observed_at)
        }
        AvailabilityEvidence::Inferred {
            inferred_at,
            method,
        } => {
            encoder.u8(3)?;
            encode_timestamp(encoder, *inferred_at)?;
            encoder.str(method.as_str())
        }
        AvailabilityEvidence::Unknown => encoder.u8(4),
    }
}

const fn data_quality_tag(quality: DataQuality) -> u8 {
    match quality {
        DataQuality::DirectVerified => 1,
        DataQuality::DirectUnverified => 2,
        DataQuality::OfficialDelayed => 3,
        DataQuality::Aggregated => 4,
        DataQuality::Indicative => 5,
        DataQuality::Modeled => 6,
        DataQuality::Estimated => 7,
        DataQuality::Stale => 8,
        DataQuality::Quarantined => 9,
    }
}

fn encode_optional_serializable<T: Serialize>(
    encoder: &mut CanonicalEncoder<'_>,
    value: Option<&T>,
) -> Result<(), CanonicalEncodingError> {
    match value {
        Some(value) => {
            encoder.u8(1)?;
            encoder.serializable(value)
        }
        None => encoder.u8(0),
    }
}

pub(super) const fn map_error<'a>(error: CanonicalEncodingError) -> PointInTimeError<'a> {
    match error {
        CanonicalEncodingError::Encoding => PointInTimeError::CanonicalEncoding,
        CanonicalEncodingError::AllocationFailure => PointInTimeError::AllocationFailure,
        CanonicalEncodingError::AccountingOverflow => PointInTimeError::AccountingOverflow,
        CanonicalEncodingError::Cancelled => PointInTimeError::Cancelled,
        CanonicalEncodingError::DeadlineExceeded => PointInTimeError::DeadlineExceeded,
    }
}
