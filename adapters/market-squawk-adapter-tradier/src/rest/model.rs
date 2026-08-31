use std::collections::BTreeSet;
use std::sync::Arc;

use market_squawk_domain::{
    CalendarDate, ConnectionGeneration, DataQuality, EvidenceDigest, InstrumentId,
    MetadataRevision, SourceId, SourceIdentifier, Timestamp, VenueId,
};
use market_squawk_sources::RawMarketFrame;
use rust_decimal::Decimal;

use crate::config::{MAX_QUOTE_SYMBOLS, validate_symbol};
use crate::{TradierInstrumentKind, TradierRateLimitEvidence};

use super::TradierRestError;

/// Bounded exact-symbol REST quote request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TradierQuoteRequest {
    symbols: Box<[SourceIdentifier]>,
    include_greeks: bool,
}

impl TradierQuoteRequest {
    /// Constructs a duplicate-free request for at most 100 provider symbols.
    ///
    /// # Errors
    ///
    /// Rejects an empty/oversized request, duplicate symbols, or invalid Tradier syntax.
    pub fn try_new(
        symbols: Vec<SourceIdentifier>,
        include_greeks: bool,
    ) -> Result<Self, TradierRestError> {
        if symbols.is_empty() || symbols.len() > MAX_QUOTE_SYMBOLS {
            return Err(TradierRestError::InvalidRequest);
        }
        let mut unique = BTreeSet::new();
        for symbol in &symbols {
            validate_symbol(symbol.as_str()).map_err(|_| TradierRestError::InvalidRequest)?;
            if !unique.insert(symbol.as_str()) {
                return Err(TradierRestError::DuplicateObservation);
            }
        }
        Ok(Self {
            symbols: symbols.into_boxed_slice(),
            include_greeks,
        })
    }

    /// Returns the exact requested symbols.
    pub fn symbols(&self) -> &[SourceIdentifier] {
        &self.symbols
    }

    /// Returns whether the provider should include available option Greeks.
    pub const fn include_greeks(&self) -> bool {
        self.include_greeks
    }
}

/// Exact response-level REST provenance retained by every normalized observation.
#[derive(Clone, Debug)]
pub struct TradierRestEvidence {
    frame: RawMarketFrame,
    digest: EvidenceDigest,
    rate_limit: TradierRateLimitEvidence,
    request_url: Box<str>,
}

impl TradierRestEvidence {
    pub(super) fn new(
        frame: RawMarketFrame,
        digest: EvidenceDigest,
        rate_limit: TradierRateLimitEvidence,
        request_url: String,
    ) -> Self {
        Self {
            frame,
            digest,
            rate_limit,
            request_url: request_url.into_boxed_str(),
        }
    }

    /// Returns the logical source identity used for this exact response.
    pub fn source_id(&self) -> &SourceId {
        self.frame.source_id()
    }

    /// Returns the exact logical-source metadata revision.
    pub fn metadata_revision(&self) -> &MetadataRevision {
        self.frame.metadata_revision()
    }

    /// Returns the exact registry connection generation that minted this response frame.
    pub fn connection_generation(&self) -> ConnectionGeneration {
        self.frame.connection_generation()
    }

    /// Returns the trusted local receive timestamp.
    pub const fn received_at(&self) -> Timestamp {
        self.frame.received_at()
    }

    /// Returns SHA-256 evidence for the exact response bytes.
    pub const fn payload_digest(&self) -> EvidenceDigest {
        self.digest
    }

    /// Returns the exact immutable response bytes retained for audit/reprocessing.
    pub fn payload(&self) -> &[u8] {
        self.frame.payload()
    }

    /// Returns the complete rate-limit evidence from the same response.
    pub const fn rate_limit(&self) -> TradierRateLimitEvidence {
        self.rate_limit
    }

    /// Returns the allowlist-authorized request URL; it contains no bearer credential.
    pub fn request_url(&self) -> &str {
        &self.request_url
    }

    /// Returns the exact registry-minted raw frame for persistence/capture composition.
    pub const fn raw_frame(&self) -> &RawMarketFrame {
        &self.frame
    }
}

/// One exact top-of-book side from a bounded REST quote snapshot.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TradierQuoteSide {
    price: Decimal,
    quantity: Decimal,
    exchange: SourceIdentifier,
    at: Timestamp,
}

impl TradierQuoteSide {
    pub(super) const fn new(
        price: Decimal,
        quantity: Decimal,
        exchange: SourceIdentifier,
        at: Timestamp,
    ) -> Self {
        Self {
            price,
            quantity,
            exchange,
            at,
        }
    }

    /// Returns the exact decimal price.
    pub const fn price(&self) -> Decimal {
        self.price
    }

    /// Returns shares for equities/ETFs or contracts for options.
    pub const fn quantity(&self) -> Decimal {
        self.quantity
    }

    /// Returns the provider exchange code for this side.
    pub const fn exchange(&self) -> &SourceIdentifier {
        &self.exchange
    }

    /// Returns the provider timestamp for this side.
    pub const fn at(&self) -> Timestamp {
        self.at
    }
}

/// One normalized bounded REST quote bootstrap observation.
#[derive(Clone, Debug)]
pub struct TradierQuoteSnapshot {
    symbol: SourceIdentifier,
    instrument: InstrumentId,
    instrument_kind: TradierInstrumentKind,
    venue: VenueId,
    quality: DataQuality,
    last: Option<Decimal>,
    trade_at: Option<Timestamp>,
    bid: Option<TradierQuoteSide>,
    ask: Option<TradierQuoteSide>,
    evidence: Arc<TradierRestEvidence>,
}

impl TradierQuoteSnapshot {
    #[allow(
        clippy::too_many_arguments,
        reason = "normalized quote provenance remains explicit"
    )]
    pub(super) const fn new(
        symbol: SourceIdentifier,
        instrument: InstrumentId,
        instrument_kind: TradierInstrumentKind,
        venue: VenueId,
        quality: DataQuality,
        last: Option<Decimal>,
        trade_at: Option<Timestamp>,
        bid: Option<TradierQuoteSide>,
        ask: Option<TradierQuoteSide>,
        evidence: Arc<TradierRestEvidence>,
    ) -> Self {
        Self {
            symbol,
            instrument,
            instrument_kind,
            venue,
            quality,
            last,
            trade_at,
            bid,
            ask,
            evidence,
        }
    }

    /// Returns the exact provider symbol.
    pub const fn symbol(&self) -> &SourceIdentifier {
        &self.symbol
    }

    /// Returns the stable internal instrument identity.
    pub const fn instrument(&self) -> InstrumentId {
        self.instrument
    }

    /// Returns provider-specific quantity semantics.
    pub const fn instrument_kind(&self) -> TradierInstrumentKind {
        self.instrument_kind
    }

    /// Returns the truthful logical venue/surface identity.
    pub const fn venue(&self) -> &VenueId {
        &self.venue
    }

    /// Returns the non-promotable logical-source quality ceiling.
    pub const fn quality(&self) -> DataQuality {
        self.quality
    }

    /// Returns the most recent exact trade/index value when supplied.
    pub const fn last(&self) -> Option<Decimal> {
        self.last
    }

    /// Returns the provider timestamp associated with `last`.
    pub const fn trade_at(&self) -> Option<Timestamp> {
        self.trade_at
    }

    /// Returns the normalized bid, if the provider supplied a complete valid side.
    pub const fn bid(&self) -> Option<&TradierQuoteSide> {
        self.bid.as_ref()
    }

    /// Returns the normalized ask, if the provider supplied a complete valid side.
    pub const fn ask(&self) -> Option<&TradierQuoteSide> {
        self.ask.as_ref()
    }

    /// Returns exact response provenance shared with this observation.
    pub fn evidence(&self) -> &TradierRestEvidence {
        &self.evidence
    }
}

/// Complete exact-set quote bootstrap response.
#[derive(Clone, Debug)]
pub struct TradierQuoteBatch {
    observations: Box<[TradierQuoteSnapshot]>,
    evidence: Arc<TradierRestEvidence>,
}

impl TradierQuoteBatch {
    pub(super) fn new(
        observations: Vec<TradierQuoteSnapshot>,
        evidence: Arc<TradierRestEvidence>,
    ) -> Self {
        Self {
            observations: observations.into_boxed_slice(),
            evidence,
        }
    }

    /// Returns one normalized observation for every requested symbol.
    pub fn observations(&self) -> &[TradierQuoteSnapshot] {
        &self.observations
    }

    /// Returns exact response-level provenance.
    pub fn evidence(&self) -> &TradierRestEvidence {
        &self.evidence
    }
}

/// One provider-derived benchmark value, never an executable market price.
#[derive(Clone, Debug)]
pub struct TradierDerivedIndexObservation {
    symbol: SourceIdentifier,
    instrument: InstrumentId,
    venue: VenueId,
    value: Decimal,
    effective_at: Timestamp,
    quality: DataQuality,
    evidence: Arc<TradierRestEvidence>,
}

impl TradierDerivedIndexObservation {
    pub(super) const fn new(
        symbol: SourceIdentifier,
        instrument: InstrumentId,
        venue: VenueId,
        value: Decimal,
        effective_at: Timestamp,
        evidence: Arc<TradierRestEvidence>,
    ) -> Self {
        Self {
            symbol,
            instrument,
            venue,
            value,
            effective_at,
            quality: DataQuality::Modeled,
            evidence,
        }
    }

    /// Returns the exact provider index symbol.
    pub const fn symbol(&self) -> &SourceIdentifier {
        &self.symbol
    }

    /// Returns the stable internal benchmark identity.
    pub const fn instrument(&self) -> InstrumentId {
        self.instrument
    }

    /// Returns the explicit derived-index logical venue.
    pub const fn venue(&self) -> &VenueId {
        &self.venue
    }

    /// Returns the exact decimal provider-derived value.
    pub const fn value(&self) -> Decimal {
        self.value
    }

    /// Returns the provider effective timestamp.
    pub const fn effective_at(&self) -> Timestamp {
        self.effective_at
    }

    /// Always returns `Modeled`; this type cannot represent execution-quality data.
    pub const fn quality(&self) -> DataQuality {
        self.quality
    }

    /// Returns exact response provenance shared with this observation.
    pub fn evidence(&self) -> &TradierRestEvidence {
        &self.evidence
    }
}

/// Complete exact-set provider-derived index response.
#[derive(Clone, Debug)]
pub struct TradierDerivedIndexBatch {
    observations: Box<[TradierDerivedIndexObservation]>,
    evidence: Arc<TradierRestEvidence>,
}

impl TradierDerivedIndexBatch {
    pub(super) fn new(
        observations: Vec<TradierDerivedIndexObservation>,
        evidence: Arc<TradierRestEvidence>,
    ) -> Self {
        Self {
            observations: observations.into_boxed_slice(),
            evidence,
        }
    }

    /// Returns every requested derived benchmark observation.
    pub fn observations(&self) -> &[TradierDerivedIndexObservation] {
        &self.observations
    }

    /// Returns exact response-level provenance.
    pub fn evidence(&self) -> &TradierRestEvidence {
        &self.evidence
    }
}

/// OCC option side reported by Tradier.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum TradierOptionSide {
    /// Call option.
    Call,
    /// Put option.
    Put,
}

/// Provider-calculated option Greeks and implied-volatility observations.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TradierOptionGreeks {
    delta: Option<Decimal>,
    gamma: Option<Decimal>,
    theta: Option<Decimal>,
    vega: Option<Decimal>,
    rho: Option<Decimal>,
    phi: Option<Decimal>,
    bid_implied_volatility: Option<Decimal>,
    midpoint_implied_volatility: Option<Decimal>,
    ask_implied_volatility: Option<Decimal>,
    model_volatility: Option<Decimal>,
    provider_updated_at: Option<Box<str>>,
    quality: DataQuality,
}

impl TradierOptionGreeks {
    #[allow(
        clippy::too_many_arguments,
        reason = "provider Greek fields remain explicitly named"
    )]
    pub(super) const fn new(
        delta: Option<Decimal>,
        gamma: Option<Decimal>,
        theta: Option<Decimal>,
        vega: Option<Decimal>,
        rho: Option<Decimal>,
        phi: Option<Decimal>,
        bid_implied_volatility: Option<Decimal>,
        midpoint_implied_volatility: Option<Decimal>,
        ask_implied_volatility: Option<Decimal>,
        model_volatility: Option<Decimal>,
        provider_updated_at: Option<Box<str>>,
    ) -> Self {
        Self {
            delta,
            gamma,
            theta,
            vega,
            rho,
            phi,
            bid_implied_volatility,
            midpoint_implied_volatility,
            ask_implied_volatility,
            model_volatility,
            provider_updated_at,
            quality: DataQuality::Modeled,
        }
    }

    /// Returns delta.
    pub const fn delta(&self) -> Option<Decimal> {
        self.delta
    }

    /// Returns gamma.
    pub const fn gamma(&self) -> Option<Decimal> {
        self.gamma
    }

    /// Returns theta.
    pub const fn theta(&self) -> Option<Decimal> {
        self.theta
    }

    /// Returns vega.
    pub const fn vega(&self) -> Option<Decimal> {
        self.vega
    }

    /// Returns rho.
    pub const fn rho(&self) -> Option<Decimal> {
        self.rho
    }

    /// Returns phi.
    pub const fn phi(&self) -> Option<Decimal> {
        self.phi
    }

    /// Returns bid implied volatility.
    pub const fn bid_implied_volatility(&self) -> Option<Decimal> {
        self.bid_implied_volatility
    }

    /// Returns midpoint implied volatility.
    pub const fn midpoint_implied_volatility(&self) -> Option<Decimal> {
        self.midpoint_implied_volatility
    }

    /// Returns ask implied volatility.
    pub const fn ask_implied_volatility(&self) -> Option<Decimal> {
        self.ask_implied_volatility
    }

    /// Returns the provider model volatility.
    pub const fn model_volatility(&self) -> Option<Decimal> {
        self.model_volatility
    }

    /// Returns the bounded provider update timestamp text when supplied.
    pub fn provider_updated_at(&self) -> Option<&str> {
        self.provider_updated_at.as_deref()
    }

    /// Always returns `Modeled`; provider-calculated Greeks are not market-price evidence.
    pub const fn quality(&self) -> DataQuality {
        self.quality
    }
}

/// One normalized option-chain contract with exact response provenance.
#[derive(Clone, Debug)]
pub struct TradierOptionContract {
    symbol: SourceIdentifier,
    root_symbol: SourceIdentifier,
    side: TradierOptionSide,
    strike: Decimal,
    contract_size: Decimal,
    expiration: CalendarDate,
    bid: Option<Decimal>,
    ask: Option<Decimal>,
    last: Option<Decimal>,
    bid_size_contracts: Option<Decimal>,
    ask_size_contracts: Option<Decimal>,
    volume: Option<Decimal>,
    open_interest: Option<Decimal>,
    greeks: Option<TradierOptionGreeks>,
    quality: DataQuality,
    evidence: Arc<TradierRestEvidence>,
}

impl TradierOptionContract {
    #[allow(
        clippy::too_many_arguments,
        reason = "option-chain provider fields remain explicit"
    )]
    pub(super) const fn new(
        symbol: SourceIdentifier,
        root_symbol: SourceIdentifier,
        side: TradierOptionSide,
        strike: Decimal,
        contract_size: Decimal,
        expiration: CalendarDate,
        bid: Option<Decimal>,
        ask: Option<Decimal>,
        last: Option<Decimal>,
        bid_size_contracts: Option<Decimal>,
        ask_size_contracts: Option<Decimal>,
        volume: Option<Decimal>,
        open_interest: Option<Decimal>,
        greeks: Option<TradierOptionGreeks>,
        evidence: Arc<TradierRestEvidence>,
    ) -> Self {
        Self {
            symbol,
            root_symbol,
            side,
            strike,
            contract_size,
            expiration,
            bid,
            ask,
            last,
            bid_size_contracts,
            ask_size_contracts,
            volume,
            open_interest,
            greeks,
            quality: DataQuality::Aggregated,
            evidence,
        }
    }

    /// Returns the exact OCC provider symbol.
    pub const fn symbol(&self) -> &SourceIdentifier {
        &self.symbol
    }

    /// Returns the exact provider root symbol, including adjusted-contract roots.
    pub const fn root_symbol(&self) -> &SourceIdentifier {
        &self.root_symbol
    }

    /// Returns call or put.
    pub const fn side(&self) -> TradierOptionSide {
        self.side
    }

    /// Returns the exact decimal strike.
    pub const fn strike(&self) -> Decimal {
        self.strike
    }

    /// Returns underlying units represented by one contract.
    pub const fn contract_size(&self) -> Decimal {
        self.contract_size
    }

    /// Returns expiration with calendar-date precision.
    pub const fn expiration(&self) -> CalendarDate {
        self.expiration
    }

    /// Returns the bid price.
    pub const fn bid(&self) -> Option<Decimal> {
        self.bid
    }

    /// Returns the ask price.
    pub const fn ask(&self) -> Option<Decimal> {
        self.ask
    }

    /// Returns the most recent trade price.
    pub const fn last(&self) -> Option<Decimal> {
        self.last
    }

    /// Returns bid size in contracts.
    pub const fn bid_size_contracts(&self) -> Option<Decimal> {
        self.bid_size_contracts
    }

    /// Returns ask size in contracts.
    pub const fn ask_size_contracts(&self) -> Option<Decimal> {
        self.ask_size_contracts
    }

    /// Returns session volume in contracts.
    pub const fn volume(&self) -> Option<Decimal> {
        self.volume
    }

    /// Returns open interest in contracts.
    pub const fn open_interest(&self) -> Option<Decimal> {
        self.open_interest
    }

    /// Returns provider-calculated Greeks when supplied.
    pub const fn greeks(&self) -> Option<&TradierOptionGreeks> {
        self.greeks.as_ref()
    }

    /// Always returns the consolidated-securities `Aggregated` ceiling.
    pub const fn quality(&self) -> DataQuality {
        self.quality
    }

    /// Returns exact response provenance shared with this observation.
    pub fn evidence(&self) -> &TradierRestEvidence {
        &self.evidence
    }
}

/// One bounded, exact-date option chain.
#[derive(Clone, Debug)]
pub struct TradierOptionChain {
    underlying: SourceIdentifier,
    expiration: CalendarDate,
    contracts: Box<[TradierOptionContract]>,
    evidence: Arc<TradierRestEvidence>,
}

impl TradierOptionChain {
    pub(super) fn new(
        underlying: SourceIdentifier,
        expiration: CalendarDate,
        contracts: Vec<TradierOptionContract>,
        evidence: Arc<TradierRestEvidence>,
    ) -> Self {
        Self {
            underlying,
            expiration,
            contracts: contracts.into_boxed_slice(),
            evidence,
        }
    }

    /// Returns the exact provider underlying symbol.
    pub const fn underlying(&self) -> &SourceIdentifier {
        &self.underlying
    }

    /// Returns the requested expiration date.
    pub const fn expiration(&self) -> CalendarDate {
        self.expiration
    }

    /// Returns the bounded provider contracts.
    pub fn contracts(&self) -> &[TradierOptionContract] {
        &self.contracts
    }

    /// Returns exact response-level provenance.
    pub fn evidence(&self) -> &TradierRestEvidence {
        &self.evidence
    }
}
