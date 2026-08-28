//! Provider-qualified canonical mapping seams.
//!
//! Named REST fields can map directly into validated domain records. Provider instrument search
//! remains a typed candidate until the shared identity authority resolves it. Streamer numeric
//! fields require an explicitly versioned dictionary bound to nonzero evidence; this crate never
//! guesses a numeric field meaning.

use std::collections::{BTreeMap, BTreeSet};

use market_squawk_domain::{
    BarTimeSemantics, BookLevel, Currency, DigestAlgorithm, EvidenceDigest, InstrumentId,
    LiveProvenance, LotSize, MarketBarAdjustment, MarketEvent, ProviderInstrumentId, QuoteEvent,
    SourceIdentifier, TickSize, Timestamp, VenueId,
};
use rust_decimal::Decimal;
use serde::Serialize;
use sha2::{Digest as _, Sha256};
use thiserror::Error;

use crate::{
    ExecutedRestResponse, FundamentalField, InstrumentResponse, MarketDataService, NativeField,
    NativeFieldEntry, NativeNumber, NativeScalar, OptionChain, OptionContract, OptionContractField,
    OptionSide, ParsedNative, ProviderIdentifier, QuoteComponentField, SchwabCapabilityCurrentness,
    SchwabInstrument, SchwabOAuthAuthorityReceipt, SchwabPriceHistoryCapabilityObservation,
    SchwabQuote, SchwabUserPreferenceEvidence, StreamerDataBatch, StreamerNativeValue,
};

/// Exact Schwab symbol bound to a shared provider-instrument identity by external registry proof.
///
/// Schwab option symbols can contain spaces while the shared `ProviderInstrumentId` grammar does
/// not. The mapping is therefore explicit and evidence-bound rather than reconstructed by string
/// normalization inside this adapter.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SchwabResolvedProviderIdentity {
    provider_symbol: ProviderIdentifier,
    provider_instrument_id: ProviderInstrumentId,
    resolution_evidence: EvidenceDigest,
}

impl SchwabResolvedProviderIdentity {
    pub fn try_new(
        provider_symbol: ProviderIdentifier,
        provider_instrument_id: ProviderInstrumentId,
        resolution_evidence: EvidenceDigest,
    ) -> Result<Self, SchwabCanonicalError> {
        if resolution_evidence.bytes() == [0; 32] {
            return Err(SchwabCanonicalError::InvalidIdentity);
        }
        Ok(Self {
            provider_symbol,
            provider_instrument_id,
            resolution_evidence,
        })
    }
    pub const fn provider_symbol(&self) -> &ProviderIdentifier {
        &self.provider_symbol
    }
    pub const fn provider_instrument_id(&self) -> &ProviderInstrumentId {
        &self.provider_instrument_id
    }
    pub const fn resolution_evidence(&self) -> EvidenceDigest {
        self.resolution_evidence
    }
}

/// Why a named quote was retained but not promoted to a canonical market event.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SchwabQuoteAbstention {
    NoQuotedSide,
    IncompleteBid,
    IncompleteAsk,
    ZeroSize,
}

/// One exact provider quote mapping result. Abstention is data, not silent row loss.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SchwabQuoteCanonicalOutcome {
    Mapped {
        provider_instrument_id: ProviderInstrumentId,
        resolution_evidence: EvidenceDigest,
        event: Box<MarketEvent>,
    },
    Abstained {
        provider_instrument_id: ProviderInstrumentId,
        resolution_evidence: EvidenceDigest,
        reason: SchwabQuoteAbstention,
    },
}

/// Maps one named REST quote into the canonical live-event family.
pub fn canonicalize_quote(
    quote: &SchwabQuote,
    identity: SchwabResolvedProviderIdentity,
    provenance: LiveProvenance,
    tick_size: TickSize,
    lot_size: LotSize,
) -> Result<SchwabQuoteCanonicalOutcome, SchwabCanonicalError> {
    if identity.provider_symbol() != quote.symbol() {
        return Err(SchwabCanonicalError::IdentityMismatch);
    }
    let resolution_evidence = identity.resolution_evidence;
    let provider_instrument_id = identity.provider_instrument_id;
    let bid = quote_side(
        quote.quote_fields(),
        QuoteComponentField::BidPrice,
        QuoteComponentField::BidSize,
        tick_size,
        lot_size,
        SchwabQuoteAbstention::IncompleteBid,
    )?;
    let ask = quote_side(
        quote.quote_fields(),
        QuoteComponentField::AskPrice,
        QuoteComponentField::AskSize,
        tick_size,
        lot_size,
        SchwabQuoteAbstention::IncompleteAsk,
    )?;
    let bid = match bid {
        SideMapping::Level(level) => Some(level),
        SideMapping::Absent => None,
        SideMapping::Abstain(reason) => {
            return Ok(SchwabQuoteCanonicalOutcome::Abstained {
                provider_instrument_id,
                resolution_evidence,
                reason,
            });
        }
    };
    let ask = match ask {
        SideMapping::Level(level) => Some(level),
        SideMapping::Absent => None,
        SideMapping::Abstain(reason) => {
            return Ok(SchwabQuoteCanonicalOutcome::Abstained {
                provider_instrument_id,
                resolution_evidence,
                reason,
            });
        }
    };
    if bid.is_none() && ask.is_none() {
        return Ok(SchwabQuoteCanonicalOutcome::Abstained {
            provider_instrument_id,
            resolution_evidence,
            reason: SchwabQuoteAbstention::NoQuotedSide,
        });
    }
    let event = QuoteEvent::new(provenance, bid, ask)
        .map(MarketEvent::Quote)
        .map_err(|_| SchwabCanonicalError::DomainInvariant)?;
    Ok(SchwabQuoteCanonicalOutcome::Mapped {
        provider_instrument_id,
        resolution_evidence,
        event: Box::new(event),
    })
}

enum SideMapping {
    Absent,
    Level(BookLevel),
    Abstain(SchwabQuoteAbstention),
}

fn quote_side(
    fields: &[NativeFieldEntry<QuoteComponentField>],
    price_field: QuoteComponentField,
    size_field: QuoteComponentField,
    tick_size: TickSize,
    lot_size: LotSize,
    incomplete: SchwabQuoteAbstention,
) -> Result<SideMapping, SchwabCanonicalError> {
    let price = named_number(fields, price_field)?;
    let size = named_number(fields, size_field)?;
    let (Some(price), Some(size)) = (price, size) else {
        return if price.is_none() && size.is_none() {
            Ok(SideMapping::Absent)
        } else {
            Ok(SideMapping::Abstain(incomplete))
        };
    };
    let price = market_squawk_domain::PriceTicks::try_from_decimal(price, tick_size)
        .map_err(|_| SchwabCanonicalError::InexactScale)?;
    let size = market_squawk_domain::QuantityLots::try_from_decimal(size, lot_size)
        .map_err(|_| SchwabCanonicalError::InexactScale)?;
    if size.get() == 0 {
        return Ok(SideMapping::Abstain(SchwabQuoteAbstention::ZeroSize));
    }
    BookLevel::new(price, size)
        .map(SideMapping::Level)
        .map_err(|_| SchwabCanonicalError::DomainInvariant)
}

fn named_number<K: Eq>(
    fields: &[NativeFieldEntry<K>],
    name: K,
) -> Result<Option<Decimal>, SchwabCanonicalError> {
    let value = fields.iter().find(|field| field.name() == &name);
    match value.map(NativeFieldEntry::value) {
        None | Some(NativeScalar::Null) => Ok(None),
        Some(NativeScalar::Number(value)) => parse_decimal(value).map(Some),
        Some(NativeScalar::Bool(_) | NativeScalar::Text(_)) => {
            Err(SchwabCanonicalError::SemanticTypeMismatch)
        }
    }
}

/// Adapter-private semantic inputs for one price-history response before its common capture seal.
///
/// This request deliberately has no source coordinates, sealed receipt, publication revision, or
/// published clock. Those authority-bearing facts arrive only through the consuming
/// `SealedProviderCaptureMaterial` rejoin in the REST publication boundary.
#[derive(Debug)]
pub(crate) struct SchwabDailyPriceHistoryCandidateRequest<'a> {
    pub(crate) capability: SchwabPriceHistoryCapabilityObservation,
    pub(crate) oauth_authority: SchwabOAuthAuthorityReceipt,
    pub(crate) user_preference: &'a SchwabUserPreferenceEvidence,
    pub(crate) response: &'a ExecutedRestResponse,
    pub(crate) instrument_id: InstrumentId,
    pub(crate) instrument_revision_digest: EvidenceDigest,
    pub(crate) admitted_plan_digest: EvidenceDigest,
    pub(crate) identity: SchwabResolvedProviderIdentity,
    pub(crate) venue_id: VenueId,
    pub(crate) feed: SourceIdentifier,
    pub(crate) interval: SourceIdentifier,
    pub(crate) adjustment: MarketBarAdjustment,
    pub(crate) currency: Currency,
    /// Calendar-authority periods in the exact provider-response order.
    pub(crate) time_semantics: Vec<BarTimeSemantics>,
    pub(crate) completeness_evidence: EvidenceDigest,
    pub(crate) ingested_at: Timestamp,
}

/// A validated semantic projection that cannot grant publication authority by itself.
///
/// It is intentionally crate-private, non-cloneable, and non-serializable. It preserves the pure
/// identity/calendar/OHLCV work while the final point-in-time record remains unavailable until the
/// shared consuming capture witness exists.
pub(crate) struct SchwabPendingPriceHistoryCandidate {
    pub(crate) capability: SchwabPriceHistoryCapabilityObservation,
    pub(crate) oauth_authority: SchwabOAuthAuthorityReceipt,
    pub(crate) user_preference_observation_sha256: [u8; 32],
    pub(crate) market_data_permission: Option<Box<str>>,
    pub(crate) response_observation_sha256: [u8; 32],
    pub(crate) requested_start: Timestamp,
    pub(crate) requested_end: Timestamp,
    pub(crate) response_received_at: Timestamp,
    pub(crate) instrument_id: InstrumentId,
    pub(crate) instrument_revision_digest: EvidenceDigest,
    pub(crate) admitted_plan_digest: EvidenceDigest,
    pub(crate) identity: SchwabResolvedProviderIdentity,
    pub(crate) venue_id: VenueId,
    pub(crate) feed: SourceIdentifier,
    pub(crate) interval: SourceIdentifier,
    pub(crate) adjustment: MarketBarAdjustment,
    pub(crate) currency: Currency,
    pub(crate) completeness_evidence: EvidenceDigest,
    pub(crate) ingested_at: Timestamp,
    pub(crate) bars: Box<[SchwabPendingPriceHistoryBar]>,
    pub(crate) mapping_digest: EvidenceDigest,
}

/// One exact provider bar after pure semantic/calendar validation but before PIT publication.
#[derive(Serialize)]
pub(crate) struct SchwabPendingPriceHistoryBar {
    pub(crate) source_identifier: SourceIdentifier,
    pub(crate) provider_timestamp: Timestamp,
    pub(crate) time_semantics: BarTimeSemantics,
    pub(crate) open: Decimal,
    pub(crate) high: Decimal,
    pub(crate) low: Decimal,
    pub(crate) close: Decimal,
    pub(crate) volume: Decimal,
}

/// Retains reusable pure history mapping while withholding final revision/PIT authority.
pub(crate) fn prepare_price_history_candidate(
    request: SchwabDailyPriceHistoryCandidateRequest<'_>,
) -> Result<SchwabPendingPriceHistoryCandidate, SchwabCanonicalError> {
    let (requested_start, requested_end) = crate::vertical::admitted_daily_range(request.response)
        .map_err(|_| SchwabCanonicalError::PendingHistoryBinding)?;
    let crate::SchwabRestPayload::PriceHistory(parsed) = request.response.payload() else {
        return Err(SchwabCanonicalError::PendingHistoryBinding);
    };
    let history = parsed.value();
    if request.identity.provider_symbol().as_str() != history.symbol.as_str()
        || history.empty
        || history.candles().is_empty()
        || request.time_semantics.len() != history.candles().len()
    {
        return Err(SchwabCanonicalError::IdentityMismatch);
    }
    if request.instrument_revision_digest.bytes() == [0; 32]
        || request.admitted_plan_digest.bytes() == [0; 32]
        || request.identity.resolution_evidence().bytes() == [0; 32]
        || request.completeness_evidence.bytes() == [0; 32]
    {
        return Err(SchwabCanonicalError::PendingHistoryBinding);
    }
    let ingested_seconds = u64::try_from(request.ingested_at.unix_nanos())
        .ok()
        .map(|value| value / 1_000_000_000)
        .ok_or(SchwabCanonicalError::PendingHistoryBinding)?;
    if request.capability.currentness(
        request.oauth_authority,
        request.user_preference,
        request.response,
        ingested_seconds,
    ) != SchwabCapabilityCurrentness::Current
    {
        return Err(SchwabCanonicalError::PendingHistoryBinding);
    }
    let receipt = request.response.capture().receipt();
    let response_received_at = millis_to_timestamp(receipt.received_at_unix_millis())?;
    if request.ingested_at < response_received_at {
        return Err(SchwabCanonicalError::PendingHistoryBinding);
    }

    let mut bars = Vec::with_capacity(history.candles().len());
    for (candle, time_semantics) in history.candles().iter().zip(request.time_semantics) {
        let provider_timestamp = millis_to_timestamp(candle.datetime_millis)?;
        if time_semantics.provider_timestamp() != provider_timestamp
            || provider_timestamp < requested_start
            || provider_timestamp >= requested_end
            || time_semantics.period_start() >= time_semantics.period_end_exclusive()
            || response_received_at < time_semantics.period_end_exclusive()
            || bars
                .last()
                .is_some_and(|previous: &SchwabPendingPriceHistoryBar| {
                    previous.provider_timestamp >= provider_timestamp
                })
        {
            return Err(SchwabCanonicalError::CompletenessMismatch);
        }
        let open = parse_decimal(&candle.open)?;
        let high = parse_decimal(&candle.high)?;
        let low = parse_decimal(&candle.low)?;
        let close = parse_decimal(&candle.close)?;
        let volume = parse_decimal(&candle.volume)?;
        if [open, high, low, close]
            .into_iter()
            .any(|price| price <= Decimal::ZERO)
            || low > high
            || open < low
            || open > high
            || close < low
            || close > high
            || volume.is_sign_negative()
        {
            return Err(SchwabCanonicalError::DomainInvariant);
        }
        let source_identifier = SourceIdentifier::try_from(format!(
            "schwab-daily-history:{}:{}",
            history.symbol.as_str(),
            candle.datetime_millis
        ))
        .map_err(|_| SchwabCanonicalError::DomainInvariant)?;
        bars.push(SchwabPendingPriceHistoryBar {
            source_identifier,
            provider_timestamp,
            time_semantics,
            open,
            high,
            low,
            close,
            volume: volume.normalize(),
        });
    }

    let user_preference_observation_sha256 =
        crate::vertical::user_preference_receipt_digest(request.user_preference);
    let market_data_permission = request
        .user_preference
        .bootstrap()
        .value()
        .market_data_permission()
        .map(Into::into);
    let response_observation_sha256 = crate::vertical::rest_receipt_digest(request.response);
    let accounting = request.response.accounting();
    let wire = PendingHistoryDigestWire {
        version: 1,
        family: "schwab.pending-daily-price-history",
        capability_receipt_sha256: request.capability.receipt_sha256(),
        oauth_generation: request.oauth_authority.generation().get(),
        oauth_access_issued_at_unix_seconds: request
            .oauth_authority
            .access_issued_at_unix_seconds(),
        oauth_access_expires_at_unix_seconds: request
            .oauth_authority
            .access_expires_at_unix_seconds(),
        oauth_refresh_authorized_at_unix_seconds: request
            .oauth_authority
            .refresh_authorized_at_unix_seconds(),
        oauth_refresh_expires_at_unix_seconds: request
            .oauth_authority
            .refresh_expires_at_unix_seconds(),
        user_preference_observation_sha256,
        response_observation_sha256,
        request_url: receipt.request_url(),
        request_sha256: receipt.request_sha256(),
        response_sha256: receipt.body_sha256(),
        response_bytes: receipt.body_bytes(),
        response_status: receipt.status(),
        response_received_at_unix_millis: receipt.received_at_unix_millis(),
        token_generation: receipt.token_generation().get(),
        requested_items: accounting.requested,
        returned_items: accounting.returned,
        missing_items: accounting.missing,
        unexpected_items: accounting.unexpected,
        provider_records: accounting.provider_records,
        requested_start,
        requested_end,
        instrument_id: request.instrument_id,
        instrument_revision_digest: request.instrument_revision_digest,
        admitted_plan_digest: request.admitted_plan_digest,
        provider_symbol: request.identity.provider_symbol().as_str(),
        provider_instrument_id: request.identity.provider_instrument_id(),
        resolution_evidence: request.identity.resolution_evidence(),
        venue_id: &request.venue_id,
        feed: &request.feed,
        interval: &request.interval,
        adjustment: request.adjustment,
        currency: request.currency,
        completeness_evidence: request.completeness_evidence,
        response_received_at,
        ingested_at: request.ingested_at,
        bars: &bars,
    };
    let encoded =
        serde_json::to_vec(&wire).map_err(|_| SchwabCanonicalError::PendingHistoryBinding)?;
    let mapping_digest =
        EvidenceDigest::new(DigestAlgorithm::Sha256, Sha256::digest(encoded).into());

    Ok(SchwabPendingPriceHistoryCandidate {
        capability: request.capability,
        oauth_authority: request.oauth_authority,
        user_preference_observation_sha256,
        market_data_permission,
        response_observation_sha256,
        requested_start,
        requested_end,
        response_received_at,
        instrument_id: request.instrument_id,
        instrument_revision_digest: request.instrument_revision_digest,
        admitted_plan_digest: request.admitted_plan_digest,
        identity: request.identity,
        venue_id: request.venue_id,
        feed: request.feed,
        interval: request.interval,
        adjustment: request.adjustment,
        currency: request.currency,
        completeness_evidence: request.completeness_evidence,
        ingested_at: request.ingested_at,
        bars: bars.into_boxed_slice(),
        mapping_digest,
    })
}

#[derive(Serialize)]
struct PendingHistoryDigestWire<'a> {
    version: u16,
    family: &'static str,
    capability_receipt_sha256: [u8; 32],
    oauth_generation: u64,
    oauth_access_issued_at_unix_seconds: u64,
    oauth_access_expires_at_unix_seconds: u64,
    oauth_refresh_authorized_at_unix_seconds: u64,
    oauth_refresh_expires_at_unix_seconds: u64,
    user_preference_observation_sha256: [u8; 32],
    response_observation_sha256: [u8; 32],
    request_url: &'a str,
    request_sha256: [u8; 32],
    response_sha256: [u8; 32],
    response_bytes: u64,
    response_status: u16,
    response_received_at_unix_millis: u64,
    token_generation: u64,
    requested_items: u64,
    returned_items: u64,
    missing_items: u64,
    unexpected_items: u64,
    provider_records: u64,
    requested_start: Timestamp,
    requested_end: Timestamp,
    instrument_id: InstrumentId,
    instrument_revision_digest: EvidenceDigest,
    admitted_plan_digest: EvidenceDigest,
    provider_symbol: &'a str,
    provider_instrument_id: &'a ProviderInstrumentId,
    resolution_evidence: EvidenceDigest,
    venue_id: &'a VenueId,
    feed: &'a SourceIdentifier,
    interval: &'a SourceIdentifier,
    adjustment: MarketBarAdjustment,
    currency: Currency,
    completeness_evidence: EvidenceDigest,
    response_received_at: Timestamp,
    ingested_at: Timestamp,
    bars: &'a [SchwabPendingPriceHistoryBar],
}

#[cfg(test)]
impl SchwabPendingPriceHistoryCandidate {
    pub(crate) const fn instrument_id(&self) -> InstrumentId {
        self.instrument_id
    }

    pub(crate) const fn provider_instrument_id(&self) -> &ProviderInstrumentId {
        self.identity.provider_instrument_id()
    }

    pub(crate) const fn provider_symbol(&self) -> &ProviderIdentifier {
        self.identity.provider_symbol()
    }

    pub(crate) fn bars(&self) -> &[SchwabPendingPriceHistoryBar] {
        &self.bars
    }

    pub(crate) const fn mapping_digest(&self) -> EvidenceDigest {
        self.mapping_digest
    }
}

#[cfg(test)]
impl SchwabPendingPriceHistoryBar {
    pub(crate) const fn provider_timestamp(&self) -> Timestamp {
        self.provider_timestamp
    }

    pub(crate) const fn time_semantics(&self) -> &BarTimeSemantics {
        &self.time_semantics
    }

    pub(crate) const fn open(&self) -> Decimal {
        self.open
    }

    pub(crate) const fn close(&self) -> Decimal {
        self.close
    }
}

/// Provider-null-aware canonical field used by option and reference candidates.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SchwabCanonicalField<T> {
    Absent,
    Null,
    Value(T),
}

/// Provider-qualified option snapshot awaiting shared canonical option/instrument identity.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SchwabOptionSnapshotCandidate {
    pub underlying: ProviderIdentifier,
    pub contract: ProviderIdentifier,
    pub side: OptionSide,
    pub expiration_group: Box<str>,
    pub strike_group: Box<str>,
    pub observed_at: Timestamp,
    pub response_sha256: [u8; 32],
    pub bid: SchwabCanonicalField<Decimal>,
    pub ask: SchwabCanonicalField<Decimal>,
    pub last: SchwabCanonicalField<Decimal>,
    pub mark: SchwabCanonicalField<Decimal>,
    pub strike: SchwabCanonicalField<Decimal>,
    pub implied_volatility: SchwabCanonicalField<Decimal>,
    pub delta: SchwabCanonicalField<Decimal>,
    pub gamma: SchwabCanonicalField<Decimal>,
    pub theta: SchwabCanonicalField<Decimal>,
    pub vega: SchwabCanonicalField<Decimal>,
    pub rho: SchwabCanonicalField<Decimal>,
    pub volume: SchwabCanonicalField<Decimal>,
    pub open_interest: SchwabCanonicalField<Decimal>,
}

/// One option contract mapping disposition.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SchwabOptionCandidateOutcome {
    Mapped(Box<SchwabOptionSnapshotCandidate>),
    Abstained {
        expiration_group: Box<str>,
        strike_group: Box<str>,
        side: OptionSide,
        reason: SchwabOptionCandidateAbstention,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SchwabOptionCandidateAbstention {
    MissingContractSymbol,
    CrossedQuote,
}

/// Produces provider-qualified option candidates while retaining explicit per-contract abstention.
pub fn canonicalize_option_chain(
    parsed: &ParsedNative<OptionChain>,
    observed_at: Timestamp,
) -> Result<Vec<SchwabOptionCandidateOutcome>, SchwabCanonicalError> {
    let underlying = parsed.value().symbol().clone();
    parsed
        .value()
        .contracts()
        .iter()
        .map(|contract| option_candidate(contract, &underlying, parsed.raw_sha256(), observed_at))
        .collect()
}

fn option_candidate(
    contract: &OptionContract,
    underlying: &ProviderIdentifier,
    response_sha256: [u8; 32],
    observed_at: Timestamp,
) -> Result<SchwabOptionCandidateOutcome, SchwabCanonicalError> {
    let Some(contract_symbol) = option_text(contract, OptionContractField::Symbol)? else {
        return Ok(SchwabOptionCandidateOutcome::Abstained {
            expiration_group: contract.expiration_group().into(),
            strike_group: contract.strike_group().into(),
            side: contract.side(),
            reason: SchwabOptionCandidateAbstention::MissingContractSymbol,
        });
    };
    let contract_symbol = ProviderIdentifier::try_new(contract_symbol)
        .map_err(|_| SchwabCanonicalError::InvalidIdentity)?;
    let implied_volatility = option_number(contract, OptionContractField::Volatility)?;
    let delta = option_number(contract, OptionContractField::Delta)?;
    let gamma = option_number(contract, OptionContractField::Gamma)?;
    let theta = option_number(contract, OptionContractField::Theta)?;
    let vega = option_number(contract, OptionContractField::Vega)?;
    let rho = option_number(contract, OptionContractField::Rho)?;
    let bid = option_number(contract, OptionContractField::Bid)?;
    let ask = option_number(contract, OptionContractField::Ask)?;
    if matches!((&bid, &ask), (SchwabCanonicalField::Value(bid), SchwabCanonicalField::Value(ask)) if bid > ask)
    {
        return Ok(SchwabOptionCandidateOutcome::Abstained {
            expiration_group: contract.expiration_group().into(),
            strike_group: contract.strike_group().into(),
            side: contract.side(),
            reason: SchwabOptionCandidateAbstention::CrossedQuote,
        });
    }
    Ok(SchwabOptionCandidateOutcome::Mapped(Box::new(
        SchwabOptionSnapshotCandidate {
            underlying: underlying.clone(),
            contract: contract_symbol,
            side: contract.side(),
            expiration_group: contract.expiration_group().into(),
            strike_group: contract.strike_group().into(),
            observed_at,
            response_sha256,
            bid,
            ask,
            last: option_number(contract, OptionContractField::Last)?,
            mark: option_number(contract, OptionContractField::Mark)?,
            strike: option_number(contract, OptionContractField::StrikePrice)?,
            implied_volatility,
            delta,
            gamma,
            theta,
            vega,
            rho,
            volume: option_number(contract, OptionContractField::TotalVolume)?,
            open_interest: option_number(contract, OptionContractField::OpenInterest)?,
        },
    )))
}

fn option_number(
    contract: &OptionContract,
    name: OptionContractField,
) -> Result<SchwabCanonicalField<Decimal>, SchwabCanonicalError> {
    scalar_field(contract.fields(), name, parse_decimal)
}

fn option_text(
    contract: &OptionContract,
    name: OptionContractField,
) -> Result<Option<String>, SchwabCanonicalError> {
    match contract.fields().iter().find(|field| field.name() == &name) {
        None => Ok(None),
        Some(field) if matches!(field.value(), NativeScalar::Null) => Ok(None),
        Some(field) => field
            .value()
            .text()
            .map(str::to_owned)
            .ok_or(SchwabCanonicalError::SemanticTypeMismatch)
            .map(Some),
    }
}

fn scalar_field<K: Eq, T>(
    fields: &[NativeFieldEntry<K>],
    name: K,
    parse: impl FnOnce(&NativeNumber) -> Result<T, SchwabCanonicalError>,
) -> Result<SchwabCanonicalField<T>, SchwabCanonicalError> {
    match fields.iter().find(|field| field.name() == &name) {
        None => Ok(SchwabCanonicalField::Absent),
        Some(field) => match field.value() {
            NativeScalar::Null => Ok(SchwabCanonicalField::Null),
            NativeScalar::Number(number) => parse(number).map(SchwabCanonicalField::Value),
            NativeScalar::Bool(_) | NativeScalar::Text(_) => {
                Err(SchwabCanonicalError::SemanticTypeMismatch)
            }
        },
    }
}

/// Provider-qualified instrument/reference candidate. This never mints canonical identity.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SchwabInstrumentCandidate {
    pub cusip: SchwabCanonicalField<Box<str>>,
    pub symbol: SchwabCanonicalField<Box<str>>,
    pub description: SchwabCanonicalField<Box<str>>,
    pub exchange: SchwabCanonicalField<Box<str>>,
    pub asset_type: SchwabCanonicalField<Box<str>>,
    pub fields: Box<[NativeFieldEntry<Box<str>>]>,
    pub fundamentals: Box<[NativeFieldEntry<FundamentalField>]>,
    pub response_sha256: [u8; 32],
}

pub fn canonicalize_instrument_candidates(
    parsed: &ParsedNative<InstrumentResponse>,
) -> Result<Vec<SchwabInstrumentCandidate>, SchwabCanonicalError> {
    parsed
        .value()
        .instruments()
        .iter()
        .map(|instrument| instrument_candidate(instrument, parsed.raw_sha256()))
        .collect()
}

fn instrument_candidate(
    instrument: &SchwabInstrument,
    response_sha256: [u8; 32],
) -> Result<SchwabInstrumentCandidate, SchwabCanonicalError> {
    let candidate = SchwabInstrumentCandidate {
        cusip: native_text_field(&instrument.cusip),
        symbol: native_text_field(&instrument.symbol),
        description: native_text_field(&instrument.description),
        exchange: native_text_field(&instrument.exchange),
        asset_type: native_text_field(&instrument.asset_type),
        fields: instrument.fields.clone(),
        fundamentals: instrument.fundamental.clone(),
        response_sha256,
    };
    if matches!(
        candidate.cusip,
        SchwabCanonicalField::Absent | SchwabCanonicalField::Null
    ) && matches!(
        candidate.symbol,
        SchwabCanonicalField::Absent | SchwabCanonicalField::Null
    ) {
        return Err(SchwabCanonicalError::InvalidIdentity);
    }
    Ok(candidate)
}

fn native_text_field(value: &NativeField<Box<str>>) -> SchwabCanonicalField<Box<str>> {
    match value {
        NativeField::Absent => SchwabCanonicalField::Absent,
        NativeField::Null => SchwabCanonicalField::Null,
        NativeField::Value(value) => SchwabCanonicalField::Value(value.clone()),
    }
}

/// Closed semantic names admitted from a separately frozen Streamer dictionary.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum SchwabStreamerSemanticField {
    Symbol,
    BidPrice,
    AskPrice,
    LastPrice,
    MarkPrice,
    BidSize,
    AskSize,
    LastSize,
    QuoteTime,
    TradeTime,
    Sequence,
    OpenPrice,
    HighPrice,
    LowPrice,
    ClosePrice,
    Volume,
    NetChange,
    PercentChange,
    ImpliedVolatility,
    Delta,
    Gamma,
    Theta,
    Vega,
    Rho,
    OpenInterest,
    StrikePrice,
    Expiration,
    SecurityStatus,
    BidBook,
    AskBook,
}

/// Versioned field dictionary authority tied to exact reviewed evidence.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SchwabStreamerFieldDictionary {
    service: MarketDataService,
    version: SourceIdentifier,
    evidence: EvidenceDigest,
    fields: BTreeMap<u16, SchwabStreamerSemanticField>,
}

impl SchwabStreamerFieldDictionary {
    pub fn try_new(
        service: MarketDataService,
        version: SourceIdentifier,
        evidence: EvidenceDigest,
        fields: Vec<(u16, SchwabStreamerSemanticField)>,
    ) -> Result<Self, SchwabCanonicalError> {
        if evidence.bytes() == [0; 32] || fields.is_empty() {
            return Err(SchwabCanonicalError::DictionaryInvalid);
        }
        let mut mapped = BTreeMap::new();
        let mut semantic = BTreeSet::new();
        for (field_id, meaning) in fields {
            if mapped.insert(field_id, meaning).is_some() || !semantic.insert(meaning) {
                return Err(SchwabCanonicalError::DictionaryInvalid);
            }
        }
        Ok(Self {
            service,
            version,
            evidence,
            fields: mapped,
        })
    }

    pub const fn service(&self) -> MarketDataService {
        self.service
    }
    pub const fn version(&self) -> &SourceIdentifier {
        &self.version
    }
    pub const fn evidence(&self) -> EvidenceDigest {
        self.evidence
    }
}

/// One semantic Streamer field after dictionary resolution; exact nested values remain native.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SchwabCanonicalStreamerField {
    pub meaning: SchwabStreamerSemanticField,
    pub value: StreamerNativeValue,
}

/// Provider-qualified, dictionary-resolved Streamer record ready for family-specific publication.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SchwabCanonicalStreamerRecord {
    pub service: MarketDataService,
    pub command: Box<str>,
    pub provider_identifier: ProviderIdentifier,
    pub provider_envelope_timestamp: Option<Timestamp>,
    pub dictionary_version: SourceIdentifier,
    pub dictionary_evidence: EvidenceDigest,
    pub fields: Box<[SchwabCanonicalStreamerField]>,
}

/// Resolves every numeric Streamer field through one exact dictionary and rejects unknown IDs.
pub fn canonicalize_streamer_batch(
    batch: &StreamerDataBatch,
    dictionary: &SchwabStreamerFieldDictionary,
) -> Result<Vec<SchwabCanonicalStreamerRecord>, SchwabCanonicalError> {
    if batch.service != dictionary.service {
        return Err(SchwabCanonicalError::DictionaryMismatch);
    }
    let provider_envelope_timestamp = batch
        .timestamp_millis
        .map(millis_to_timestamp)
        .transpose()?;
    batch
        .content
        .iter()
        .map(|content| {
            let fields = content
                .fields
                .iter()
                .map(|field| {
                    dictionary
                        .fields
                        .get(&field.field_id)
                        .copied()
                        .map(|meaning| SchwabCanonicalStreamerField {
                            meaning,
                            value: field.value.clone(),
                        })
                        .ok_or(SchwabCanonicalError::UnknownStreamerField {
                            field_id: field.field_id,
                        })
                })
                .collect::<Result<Vec<_>, _>>()?;
            Ok(SchwabCanonicalStreamerRecord {
                service: batch.service,
                command: batch.command.clone(),
                provider_identifier: content.key.clone(),
                provider_envelope_timestamp,
                dictionary_version: dictionary.version.clone(),
                dictionary_evidence: dictionary.evidence,
                fields: fields.into_boxed_slice(),
            })
        })
        .collect()
}

/// Promotes one dictionary-resolved level-one record into a validated canonical quote event.
/// Other Streamer families retain their provider-qualified typed record until their family mapper
/// can prove the required sequence, book, clock, and identity invariants.
pub fn canonicalize_streamer_quote_record(
    record: &SchwabCanonicalStreamerRecord,
    identity: SchwabResolvedProviderIdentity,
    provenance: LiveProvenance,
    tick_size: TickSize,
    lot_size: LotSize,
) -> Result<SchwabQuoteCanonicalOutcome, SchwabCanonicalError> {
    if !matches!(
        record.service,
        MarketDataService::LevelOneEquities
            | MarketDataService::LevelOneOptions
            | MarketDataService::LevelOneFutures
            | MarketDataService::LevelOneFuturesOptions
            | MarketDataService::LevelOneForex
    ) {
        return Err(SchwabCanonicalError::UnsupportedCanonicalFamily);
    }
    if identity.provider_symbol() != &record.provider_identifier {
        return Err(SchwabCanonicalError::IdentityMismatch);
    }
    let resolution_evidence = identity.resolution_evidence;
    let provider_instrument_id = identity.provider_instrument_id;
    let bid = streamer_side(
        &record.fields,
        SchwabStreamerSemanticField::BidPrice,
        SchwabStreamerSemanticField::BidSize,
        tick_size,
        lot_size,
        SchwabQuoteAbstention::IncompleteBid,
    )?;
    let ask = streamer_side(
        &record.fields,
        SchwabStreamerSemanticField::AskPrice,
        SchwabStreamerSemanticField::AskSize,
        tick_size,
        lot_size,
        SchwabQuoteAbstention::IncompleteAsk,
    )?;
    let resolve = |side| match side {
        SideMapping::Level(level) => Ok(Some(level)),
        SideMapping::Absent => Ok(None),
        SideMapping::Abstain(reason) => Err(reason),
    };
    let bid = match resolve(bid) {
        Ok(value) => value,
        Err(reason) => {
            return Ok(SchwabQuoteCanonicalOutcome::Abstained {
                provider_instrument_id: provider_instrument_id.clone(),
                resolution_evidence,
                reason,
            });
        }
    };
    let ask = match resolve(ask) {
        Ok(value) => value,
        Err(reason) => {
            return Ok(SchwabQuoteCanonicalOutcome::Abstained {
                provider_instrument_id: provider_instrument_id.clone(),
                resolution_evidence,
                reason,
            });
        }
    };
    if bid.is_none() && ask.is_none() {
        return Ok(SchwabQuoteCanonicalOutcome::Abstained {
            provider_instrument_id: provider_instrument_id.clone(),
            resolution_evidence,
            reason: SchwabQuoteAbstention::NoQuotedSide,
        });
    }
    let event = QuoteEvent::new(provenance, bid, ask)
        .map(MarketEvent::Quote)
        .map_err(|_| SchwabCanonicalError::DomainInvariant)?;
    Ok(SchwabQuoteCanonicalOutcome::Mapped {
        provider_instrument_id,
        resolution_evidence,
        event: Box::new(event),
    })
}

fn streamer_side(
    fields: &[SchwabCanonicalStreamerField],
    price: SchwabStreamerSemanticField,
    size: SchwabStreamerSemanticField,
    tick_size: TickSize,
    lot_size: LotSize,
    incomplete: SchwabQuoteAbstention,
) -> Result<SideMapping, SchwabCanonicalError> {
    let price = streamer_number(fields, price)?;
    let size = streamer_number(fields, size)?;
    let (Some(price), Some(size)) = (price, size) else {
        return if price.is_none() && size.is_none() {
            Ok(SideMapping::Absent)
        } else {
            Ok(SideMapping::Abstain(incomplete))
        };
    };
    let price = market_squawk_domain::PriceTicks::try_from_decimal(price, tick_size)
        .map_err(|_| SchwabCanonicalError::InexactScale)?;
    let size = market_squawk_domain::QuantityLots::try_from_decimal(size, lot_size)
        .map_err(|_| SchwabCanonicalError::InexactScale)?;
    if size.get() == 0 {
        return Ok(SideMapping::Abstain(SchwabQuoteAbstention::ZeroSize));
    }
    BookLevel::new(price, size)
        .map(SideMapping::Level)
        .map_err(|_| SchwabCanonicalError::DomainInvariant)
}

fn streamer_number(
    fields: &[SchwabCanonicalStreamerField],
    meaning: SchwabStreamerSemanticField,
) -> Result<Option<Decimal>, SchwabCanonicalError> {
    match fields.iter().find(|field| field.meaning == meaning) {
        None => Ok(None),
        Some(field) => match &field.value {
            StreamerNativeValue::Scalar(NativeScalar::Null) => Ok(None),
            StreamerNativeValue::Scalar(NativeScalar::Number(number)) => {
                parse_decimal(number).map(Some)
            }
            StreamerNativeValue::Scalar(NativeScalar::Bool(_) | NativeScalar::Text(_))
            | StreamerNativeValue::Sequence(_)
            | StreamerNativeValue::Fields(_) => Err(SchwabCanonicalError::SemanticTypeMismatch),
        },
    }
}

fn parse_decimal(number: &NativeNumber) -> Result<Decimal, SchwabCanonicalError> {
    Decimal::from_str_exact(number.as_str()).map_err(|_| SchwabCanonicalError::InvalidDecimal)
}

fn millis_to_timestamp(value: u64) -> Result<Timestamp, SchwabCanonicalError> {
    let nanos = millis_to_nanos(value)?;
    Ok(Timestamp::from_unix_nanos(nanos))
}

fn millis_to_nanos(value: u64) -> Result<i64, SchwabCanonicalError> {
    i64::try_from(value)
        .ok()
        .and_then(|value| value.checked_mul(1_000_000))
        .ok_or(SchwabCanonicalError::TimestampOverflow)
}

/// Fail-closed mapping failure. Provider values are never included in errors.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum SchwabCanonicalError {
    #[error("Schwab canonical identity does not match the provider response")]
    IdentityMismatch,
    #[error("Schwab canonical identity is invalid")]
    InvalidIdentity,
    #[error("Schwab canonical response/context cardinality differs")]
    CardinalityMismatch,
    #[error("Schwab daily-history calendar completeness does not match the exact response")]
    CompletenessMismatch,
    #[error("Schwab pending daily-history response, authority, semantics, or clocks differ")]
    PendingHistoryBinding,
    #[error("Schwab named field has an unexpected semantic scalar type")]
    SemanticTypeMismatch,
    #[error("Schwab exact decimal is invalid")]
    InvalidDecimal,
    #[error("Schwab exact value does not fit the canonical tick or lot scale")]
    InexactScale,
    #[error("Schwab timestamp exceeds the canonical clock domain")]
    TimestampOverflow,
    #[error("Schwab canonical domain invariant rejected the mapped observation")]
    DomainInvariant,
    #[error("Schwab Streamer field dictionary is invalid")]
    DictionaryInvalid,
    #[error("Schwab Streamer field dictionary does not match the service")]
    DictionaryMismatch,
    #[error("Schwab Streamer field {field_id} is absent from the frozen dictionary")]
    UnknownStreamerField { field_id: u16 },
    #[error("Schwab Streamer family does not yet satisfy canonical event invariants")]
    UnsupportedCanonicalFamily,
}
