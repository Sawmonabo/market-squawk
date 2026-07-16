//! Validated payloads carried by [`super::MarketEvent`].

use serde::{Deserialize, Deserializer, Serialize};

use super::{
    AggressorSide, AuctionPhase, BookChange, BookLevel, CorporateActionKind, HaltTransition,
    MarketDepth, MarketEventError, Provenance, SequenceNumber, SourceIdentifier, Timestamp,
    TradingStatus, validate_book, validate_market_provenance,
};
use crate::{PriceTicks, QuantityLots};

/// Executed trade payload.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct TradeEvent {
    provenance: Provenance,
    price: PriceTicks,
    quantity: QuantityLots,
    aggressor_side: AggressorSide,
}

impl TradeEvent {
    /// Constructs a positive-quantity venue trade.
    pub fn new(
        provenance: Provenance,
        price: PriceTicks,
        quantity: QuantityLots,
        aggressor_side: AggressorSide,
    ) -> Result<Self, MarketEventError> {
        validate_market_provenance(&provenance, true)?;
        if quantity.get() == 0 {
            return Err(MarketEventError::ZeroQuantity);
        }
        Ok(Self {
            provenance,
            price,
            quantity,
            aggressor_side,
        })
    }

    /// Returns common provenance.
    pub const fn provenance(&self) -> &Provenance {
        &self.provenance
    }

    /// Returns the executed tick price.
    pub const fn price(&self) -> PriceTicks {
        self.price
    }

    /// Returns the executed lot quantity.
    pub const fn quantity(&self) -> QuantityLots {
        self.quantity
    }

    /// Returns inferred or source-supplied aggressor direction.
    pub const fn aggressor_side(&self) -> AggressorSide {
        self.aggressor_side
    }
}

#[derive(Deserialize)]
struct TradeEventWire {
    provenance: Provenance,
    price: PriceTicks,
    quantity: QuantityLots,
    aggressor_side: AggressorSide,
}

impl<'de> Deserialize<'de> for TradeEvent {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = TradeEventWire::deserialize(deserializer)?;
        Self::new(
            wire.provenance,
            wire.price,
            wire.quantity,
            wire.aggressor_side,
        )
        .map_err(serde::de::Error::custom)
    }
}

/// One- or two-sided quote payload.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct QuoteEvent {
    provenance: Provenance,
    bid: Option<BookLevel>,
    ask: Option<BookLevel>,
}

impl QuoteEvent {
    /// Constructs a nonempty, uncrossed venue quote.
    pub fn new(
        provenance: Provenance,
        bid: Option<BookLevel>,
        ask: Option<BookLevel>,
    ) -> Result<Self, MarketEventError> {
        validate_market_provenance(&provenance, true)?;
        if bid.is_none() && ask.is_none() {
            return Err(MarketEventError::EmptyQuote);
        }
        if let (Some(bid_level), Some(ask_level)) = (bid, ask)
            && bid_level.price() >= ask_level.price()
        {
            return Err(MarketEventError::CrossedMarket);
        }
        Ok(Self {
            provenance,
            bid,
            ask,
        })
    }

    /// Returns common provenance.
    pub const fn provenance(&self) -> &Provenance {
        &self.provenance
    }

    /// Returns the best bid when present.
    pub const fn bid(&self) -> Option<BookLevel> {
        self.bid
    }

    /// Returns the best ask when present.
    pub const fn ask(&self) -> Option<BookLevel> {
        self.ask
    }
}

#[derive(Deserialize)]
struct QuoteEventWire {
    provenance: Provenance,
    bid: Option<BookLevel>,
    ask: Option<BookLevel>,
}

impl<'de> Deserialize<'de> for QuoteEvent {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = QuoteEventWire::deserialize(deserializer)?;
        Self::new(wire.provenance, wire.bid, wire.ask).map_err(serde::de::Error::custom)
    }
}

/// Complete order-book image for a source connection generation.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct BookSnapshotEvent {
    provenance: Provenance,
    depth: MarketDepth,
    bids: Vec<BookLevel>,
    asks: Vec<BookLevel>,
    sequence: Option<SequenceNumber>,
}

impl BookSnapshotEvent {
    /// Constructs an uncrossed snapshot in strict best-to-worst side order.
    pub fn new(
        provenance: Provenance,
        depth: MarketDepth,
        bids: Vec<BookLevel>,
        asks: Vec<BookLevel>,
        sequence: Option<SequenceNumber>,
    ) -> Result<Self, MarketEventError> {
        validate_market_provenance(&provenance, true)?;
        validate_book(&bids, &asks)?;
        Ok(Self {
            provenance,
            depth,
            bids,
            asks,
            sequence,
        })
    }

    /// Returns common provenance.
    pub const fn provenance(&self) -> &Provenance {
        &self.provenance
    }

    /// Returns the source-supplied depth class.
    pub const fn depth(&self) -> MarketDepth {
        self.depth
    }

    /// Returns bid levels in strict descending price order.
    pub fn bids(&self) -> &[BookLevel] {
        &self.bids
    }

    /// Returns ask levels in strict ascending price order.
    pub fn asks(&self) -> &[BookLevel] {
        &self.asks
    }

    /// Returns the source sequence when supplied.
    pub const fn sequence(&self) -> Option<SequenceNumber> {
        self.sequence
    }
}

#[derive(Deserialize)]
struct BookSnapshotEventWire {
    provenance: Provenance,
    depth: MarketDepth,
    bids: Vec<BookLevel>,
    asks: Vec<BookLevel>,
    sequence: Option<SequenceNumber>,
}

impl<'de> Deserialize<'de> for BookSnapshotEvent {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = BookSnapshotEventWire::deserialize(deserializer)?;
        Self::new(
            wire.provenance,
            wire.depth,
            wire.bids,
            wire.asks,
            wire.sequence,
        )
        .map_err(serde::de::Error::custom)
    }
}

/// Nonempty incremental order-book changes for one source message.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct BookDeltaEvent {
    provenance: Provenance,
    depth: MarketDepth,
    changes: Vec<BookChange>,
    sequence: Option<SequenceNumber>,
}

impl BookDeltaEvent {
    /// Constructs one atomic nonempty provider delta.
    pub fn new(
        provenance: Provenance,
        depth: MarketDepth,
        changes: Vec<BookChange>,
        sequence: Option<SequenceNumber>,
    ) -> Result<Self, MarketEventError> {
        validate_market_provenance(&provenance, true)?;
        if changes.is_empty() {
            return Err(MarketEventError::EmptyBookDelta);
        }
        Ok(Self {
            provenance,
            depth,
            changes,
            sequence,
        })
    }

    /// Returns common provenance.
    pub const fn provenance(&self) -> &Provenance {
        &self.provenance
    }

    /// Returns the source-supplied depth class.
    pub const fn depth(&self) -> MarketDepth {
        self.depth
    }

    /// Returns the atomic provider change set.
    pub fn changes(&self) -> &[BookChange] {
        &self.changes
    }

    /// Returns the source sequence when supplied.
    pub const fn sequence(&self) -> Option<SequenceNumber> {
        self.sequence
    }
}

#[derive(Deserialize)]
struct BookDeltaEventWire {
    provenance: Provenance,
    depth: MarketDepth,
    changes: Vec<BookChange>,
    sequence: Option<SequenceNumber>,
}

impl<'de> Deserialize<'de> for BookDeltaEvent {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = BookDeltaEventWire::deserialize(deserializer)?;
        Self::new(wire.provenance, wire.depth, wire.changes, wire.sequence)
            .map_err(serde::de::Error::custom)
    }
}

/// Auction indication or result.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct AuctionEvent {
    provenance: Provenance,
    phase: AuctionPhase,
    indicative_price: Option<PriceTicks>,
    paired_quantity: QuantityLots,
}

impl AuctionEvent {
    /// Constructs a venue-scoped auction payload.
    pub fn new(
        provenance: Provenance,
        phase: AuctionPhase,
        indicative_price: Option<PriceTicks>,
        paired_quantity: QuantityLots,
    ) -> Result<Self, MarketEventError> {
        validate_market_provenance(&provenance, true)?;
        Ok(Self {
            provenance,
            phase,
            indicative_price,
            paired_quantity,
        })
    }

    /// Returns common provenance.
    pub const fn provenance(&self) -> &Provenance {
        &self.provenance
    }

    /// Returns the venue auction phase.
    pub const fn phase(&self) -> AuctionPhase {
        self.phase
    }

    /// Returns the indicative or clearing price when supplied.
    pub const fn indicative_price(&self) -> Option<PriceTicks> {
        self.indicative_price
    }

    /// Returns the paired auction lot quantity.
    pub const fn paired_quantity(&self) -> QuantityLots {
        self.paired_quantity
    }
}

#[derive(Deserialize)]
struct AuctionEventWire {
    provenance: Provenance,
    phase: AuctionPhase,
    indicative_price: Option<PriceTicks>,
    paired_quantity: QuantityLots,
}

impl<'de> Deserialize<'de> for AuctionEvent {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = AuctionEventWire::deserialize(deserializer)?;
        Self::new(
            wire.provenance,
            wire.phase,
            wire.indicative_price,
            wire.paired_quantity,
        )
        .map_err(serde::de::Error::custom)
    }
}

/// Trading-halt or resumption payload with a source reason code.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct TradingHaltEvent {
    provenance: Provenance,
    transition: HaltTransition,
    reason: SourceIdentifier,
}

impl TradingHaltEvent {
    /// Constructs a venue-scoped halt transition.
    pub fn new(
        provenance: Provenance,
        transition: HaltTransition,
        reason: SourceIdentifier,
    ) -> Result<Self, MarketEventError> {
        validate_market_provenance(&provenance, true)?;
        Ok(Self {
            provenance,
            transition,
            reason,
        })
    }

    /// Returns common provenance.
    pub const fn provenance(&self) -> &Provenance {
        &self.provenance
    }

    /// Returns whether the halt began or ended.
    pub const fn transition(&self) -> HaltTransition {
        self.transition
    }

    /// Returns the source-native halt reason code.
    pub const fn reason(&self) -> &SourceIdentifier {
        &self.reason
    }
}

#[derive(Deserialize)]
struct TradingHaltEventWire {
    provenance: Provenance,
    transition: HaltTransition,
    reason: SourceIdentifier,
}

impl<'de> Deserialize<'de> for TradingHaltEvent {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = TradingHaltEventWire::deserialize(deserializer)?;
        Self::new(wire.provenance, wire.transition, wire.reason).map_err(serde::de::Error::custom)
    }
}

/// Instrument trading-status update.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct InstrumentStatusEvent {
    provenance: Provenance,
    status: TradingStatus,
}

impl InstrumentStatusEvent {
    /// Constructs a venue-scoped status payload.
    pub fn new(provenance: Provenance, status: TradingStatus) -> Result<Self, MarketEventError> {
        validate_market_provenance(&provenance, true)?;
        Ok(Self { provenance, status })
    }

    /// Returns common provenance.
    pub const fn provenance(&self) -> &Provenance {
        &self.provenance
    }

    /// Returns the updated reference trading status.
    pub const fn status(&self) -> TradingStatus {
        self.status
    }
}

#[derive(Deserialize)]
struct InstrumentStatusEventWire {
    provenance: Provenance,
    status: TradingStatus,
}

impl<'de> Deserialize<'de> for InstrumentStatusEvent {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = InstrumentStatusEventWire::deserialize(deserializer)?;
        Self::new(wire.provenance, wire.status).map_err(serde::de::Error::custom)
    }
}

/// Corporate action distributed on a live channel.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct CorporateActionEvent {
    provenance: Provenance,
    effective_at: Timestamp,
    action: CorporateActionKind,
}

impl CorporateActionEvent {
    /// Constructs an instrument-scoped action and validates relational variants.
    pub fn new(
        provenance: Provenance,
        effective_at: Timestamp,
        action: CorporateActionKind,
    ) -> Result<Self, MarketEventError> {
        let instrument_id = validate_market_provenance(&provenance, false)?;
        match &action {
            CorporateActionKind::Merger { successor } if *successor == instrument_id => {
                return Err(MarketEventError::SelfMerger);
            }
            CorporateActionKind::SymbolChange { previous, current } if previous == current => {
                return Err(MarketEventError::UnchangedSymbol);
            }
            _ => {}
        }
        Ok(Self {
            provenance,
            effective_at,
            action,
        })
    }

    /// Returns common provenance.
    pub const fn provenance(&self) -> &Provenance {
        &self.provenance
    }

    /// Returns the action's effective time.
    pub const fn effective_at(&self) -> Timestamp {
        self.effective_at
    }

    /// Returns the typed corporate action.
    pub const fn action(&self) -> &CorporateActionKind {
        &self.action
    }
}

#[derive(Deserialize)]
struct CorporateActionEventWire {
    provenance: Provenance,
    effective_at: Timestamp,
    action: CorporateActionKind,
}

impl<'de> Deserialize<'de> for CorporateActionEvent {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = CorporateActionEventWire::deserialize(deserializer)?;
        Self::new(wire.provenance, wire.effective_at, wire.action).map_err(serde::de::Error::custom)
    }
}
