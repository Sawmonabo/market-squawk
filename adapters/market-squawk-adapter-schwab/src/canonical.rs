//! Provider-qualified canonical mapping seams.
//!
//! Named REST fields can map directly into validated domain records. Provider instrument search
//! remains a typed candidate until the shared identity authority resolves it. Streamer numeric
//! fields require an explicitly versioned dictionary bound to nonzero evidence; this crate never
//! guesses a numeric field meaning.

use std::collections::{BTreeMap, BTreeSet};

use market_squawk_domain::{
    BarTimeSemantics, BookLevel, Currency, EvidenceDigest, LiveProvenance, LotSize,
    MarketBarAdjustment, MarketBarObservation, MarketEvent, Money, ProviderInstrumentId,
    QuoteEvent, ResearchContext, SourceIdentifier, TickSize, Timestamp,
};
use rust_decimal::Decimal;
use thiserror::Error;

use crate::{
    FundamentalField, HistoricalCandle, InstrumentResponse, MarketDataService, NativeField,
    NativeFieldEntry, NativeNumber, NativeScalar, OptionChain, OptionContract, OptionContractField,
    OptionSide, ParsedNative, PriceHistoryResponse, ProviderIdentifier, QuoteComponentField,
    SchwabInstrument, SchwabQuote, StreamerDataBatch, StreamerNativeValue,
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

/// Caller-supplied, identity-resolved context for exactly one historical candle.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SchwabHistoricalBarContext {
    pub context: ResearchContext,
    pub identity: SchwabResolvedProviderIdentity,
    pub feed: SourceIdentifier,
    pub interval: SourceIdentifier,
    pub time_semantics: BarTimeSemantics,
    pub adjustment: MarketBarAdjustment,
    pub currency: Currency,
}

/// Maps a complete price-history response into validated point-in-time market bars.
pub fn canonicalize_price_history(
    response: &PriceHistoryResponse,
    contexts: Vec<SchwabHistoricalBarContext>,
) -> Result<Vec<MarketBarObservation>, SchwabCanonicalError> {
    if contexts.len() != response.candles().len() {
        return Err(SchwabCanonicalError::CardinalityMismatch);
    }
    response
        .candles()
        .iter()
        .zip(contexts)
        .map(|(candle, context)| canonicalize_candle(response, candle, context))
        .collect()
}

fn canonicalize_candle(
    response: &PriceHistoryResponse,
    candle: &HistoricalCandle,
    context: SchwabHistoricalBarContext,
) -> Result<MarketBarObservation, SchwabCanonicalError> {
    if context.identity.provider_symbol().as_str() != response.symbol.as_str()
        || context.time_semantics.provider_timestamp().unix_nanos()
            != millis_to_nanos(candle.datetime_millis)?
    {
        return Err(SchwabCanonicalError::IdentityMismatch);
    }
    MarketBarObservation::new(
        context.context,
        context.identity.provider_instrument_id,
        context.feed,
        context.interval,
        context.time_semantics,
        context.adjustment,
        Money::new(parse_decimal(&candle.open)?, context.currency),
        Money::new(parse_decimal(&candle.high)?, context.currency),
        Money::new(parse_decimal(&candle.low)?, context.currency),
        Money::new(parse_decimal(&candle.close)?, context.currency),
        parse_decimal(&candle.volume)?,
        None,
        None,
    )
    .map_err(|_| SchwabCanonicalError::DomainInvariant)
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
            implied_volatility: option_number(contract, OptionContractField::Volatility)?,
            delta: option_number(contract, OptionContractField::Delta)?,
            gamma: option_number(contract, OptionContractField::Gamma)?,
            theta: option_number(contract, OptionContractField::Theta)?,
            vega: option_number(contract, OptionContractField::Vega)?,
            rho: option_number(contract, OptionContractField::Rho)?,
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
