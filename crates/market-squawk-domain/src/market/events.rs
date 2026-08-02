//! Validated payloads carried by [`super::MarketEvent`].

use serde::{Deserialize, Deserializer, Serialize};

use super::{
    AggressorSide, AuctionPhase, BookChange, BookLevel, CorporateActionInvariantError,
    CorporateActionKind, HaltTransition, LiveProvenance, MarketDepth, MarketEventError,
    SequenceNumber, SourceIdentifier, Timestamp, TradingStatus, validate_book, validate_book_depth,
    validate_market_provenance,
};
use crate::{PriceTicks, QuantityLots};

/// Executed trade payload.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct TradeEvent {
    provenance: LiveProvenance,
    price: PriceTicks,
    quantity: QuantityLots,
    aggressor_side: AggressorSide,
}

impl TradeEvent {
    /// Constructs a positive-quantity venue trade.
    pub fn new(
        provenance: LiveProvenance,
        price: PriceTicks,
        quantity: QuantityLots,
        aggressor_side: AggressorSide,
    ) -> Result<Self, MarketEventError> {
        validate_market_provenance(&provenance, true, crate::LiveEventClass::Trade)?;
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
    pub const fn provenance(&self) -> &LiveProvenance {
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
#[serde(deny_unknown_fields)]
struct TradeEventWire {
    provenance: LiveProvenance,
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
    provenance: LiveProvenance,
    bid: Option<BookLevel>,
    ask: Option<BookLevel>,
}

impl QuoteEvent {
    /// Constructs a nonempty, uncrossed venue quote.
    pub fn new(
        provenance: LiveProvenance,
        bid: Option<BookLevel>,
        ask: Option<BookLevel>,
    ) -> Result<Self, MarketEventError> {
        validate_market_provenance(&provenance, true, crate::LiveEventClass::Quote)?;
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
    pub const fn provenance(&self) -> &LiveProvenance {
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
#[serde(deny_unknown_fields)]
struct QuoteEventWire {
    provenance: LiveProvenance,
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
    provenance: LiveProvenance,
    depth: MarketDepth,
    bids: Vec<BookLevel>,
    asks: Vec<BookLevel>,
    sequence: Option<SequenceNumber>,
}

impl BookSnapshotEvent {
    /// Constructs an uncrossed snapshot in strict best-to-worst side order.
    pub fn new(
        provenance: LiveProvenance,
        depth: MarketDepth,
        bids: Vec<BookLevel>,
        asks: Vec<BookLevel>,
        sequence: Option<SequenceNumber>,
    ) -> Result<Self, MarketEventError> {
        validate_market_provenance(&provenance, true, crate::LiveEventClass::BookSnapshot)?;
        validate_book_depth(&provenance, depth)?;
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
    pub const fn provenance(&self) -> &LiveProvenance {
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
#[serde(deny_unknown_fields)]
struct BookSnapshotEventWire {
    provenance: LiveProvenance,
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
    provenance: LiveProvenance,
    depth: MarketDepth,
    changes: Vec<BookChange>,
    sequence: Option<SequenceNumber>,
}

impl BookDeltaEvent {
    /// Constructs one atomic nonempty provider delta.
    pub fn new(
        provenance: LiveProvenance,
        depth: MarketDepth,
        changes: Vec<BookChange>,
        sequence: Option<SequenceNumber>,
    ) -> Result<Self, MarketEventError> {
        validate_market_provenance(&provenance, true, crate::LiveEventClass::BookDelta)?;
        validate_book_depth(&provenance, depth)?;
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
    pub const fn provenance(&self) -> &LiveProvenance {
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
#[serde(deny_unknown_fields)]
struct BookDeltaEventWire {
    provenance: LiveProvenance,
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
    provenance: LiveProvenance,
    phase: AuctionPhase,
    indicative_price: Option<PriceTicks>,
    paired_quantity: QuantityLots,
}

impl AuctionEvent {
    /// Constructs a venue-scoped auction payload.
    pub fn new(
        provenance: LiveProvenance,
        phase: AuctionPhase,
        indicative_price: Option<PriceTicks>,
        paired_quantity: QuantityLots,
    ) -> Result<Self, MarketEventError> {
        validate_market_provenance(&provenance, true, crate::LiveEventClass::Auction)?;
        Ok(Self {
            provenance,
            phase,
            indicative_price,
            paired_quantity,
        })
    }

    /// Returns common provenance.
    pub const fn provenance(&self) -> &LiveProvenance {
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
#[serde(deny_unknown_fields)]
struct AuctionEventWire {
    provenance: LiveProvenance,
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
    provenance: LiveProvenance,
    transition: HaltTransition,
    reason: SourceIdentifier,
}

impl TradingHaltEvent {
    /// Constructs a venue-scoped halt transition.
    pub fn new(
        provenance: LiveProvenance,
        transition: HaltTransition,
        reason: SourceIdentifier,
    ) -> Result<Self, MarketEventError> {
        validate_market_provenance(&provenance, true, crate::LiveEventClass::TradingHalt)?;
        Ok(Self {
            provenance,
            transition,
            reason,
        })
    }

    /// Returns common provenance.
    pub const fn provenance(&self) -> &LiveProvenance {
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
#[serde(deny_unknown_fields)]
struct TradingHaltEventWire {
    provenance: LiveProvenance,
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
    provenance: LiveProvenance,
    status: TradingStatus,
}

impl InstrumentStatusEvent {
    /// Constructs a venue-scoped status payload.
    pub fn new(
        provenance: LiveProvenance,
        status: TradingStatus,
    ) -> Result<Self, MarketEventError> {
        validate_market_provenance(&provenance, true, crate::LiveEventClass::InstrumentStatus)?;
        Ok(Self { provenance, status })
    }

    /// Returns common provenance.
    pub const fn provenance(&self) -> &LiveProvenance {
        &self.provenance
    }

    /// Returns the updated reference trading status.
    pub const fn status(&self) -> TradingStatus {
        self.status
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct InstrumentStatusEventWire {
    provenance: LiveProvenance,
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
    provenance: LiveProvenance,
    effective_at: Timestamp,
    action: CorporateActionKind,
}

impl CorporateActionEvent {
    /// Constructs an instrument-scoped action and validates relational variants.
    pub fn new(
        provenance: LiveProvenance,
        effective_at: Timestamp,
        action: CorporateActionKind,
    ) -> Result<Self, MarketEventError> {
        let instrument_id =
            validate_market_provenance(&provenance, false, crate::LiveEventClass::CorporateAction)?;
        action
            .validate_for_instrument(instrument_id)
            .map_err(|error| match error {
                CorporateActionInvariantError::SelfMerger => MarketEventError::SelfMerger,
                CorporateActionInvariantError::SelfSpinoff => MarketEventError::SelfSpinoff,
                CorporateActionInvariantError::NonPositiveMonetaryAmount => {
                    MarketEventError::NonPositiveCorporateActionAmount
                }
            })?;
        if let CorporateActionKind::SymbolChange {
            venue_id,
            previous,
            current,
        } = &action
        {
            let provenance_venue = provenance
                .venue_id()
                .ok_or(MarketEventError::MissingVenue)?;
            if provenance_venue != venue_id {
                return Err(MarketEventError::CorporateActionVenueMismatch);
            }
            if previous == current {
                return Err(MarketEventError::UnchangedSymbol);
            }
        }
        Ok(Self {
            provenance,
            effective_at,
            action,
        })
    }

    /// Returns common provenance.
    pub const fn provenance(&self) -> &LiveProvenance {
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
#[serde(deny_unknown_fields)]
struct CorporateActionEventWire {
    provenance: LiveProvenance,
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
