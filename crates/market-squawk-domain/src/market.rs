//! Canonical live-market event family and validated market payload primitives.

use std::fmt;
use std::num::NonZeroU32;

use rust_decimal::Decimal;
use serde::{Deserialize, Deserializer, Serialize};

use crate::{
    InstrumentId, LiveProvenance, MarketDepth, Money, PriceTicks, QuantityLots, SequenceNumber,
    SourceIdentifier, Timestamp, TradingStatus, VenueId, VenueSymbol,
};

#[path = "market/events.rs"]
mod events;

pub use events::{
    AuctionEvent, BookDeltaEvent, BookSnapshotEvent, CorporateActionEvent, InstrumentStatusEvent,
    QuoteEvent, TradeEvent, TradingHaltEvent,
};

/// Side of a displayed order book.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MarketSide {
    /// Buy-side liquidity.
    Bid,
    /// Sell-side liquidity.
    Ask,
}

/// Inferred or venue-supplied aggressor direction for a trade.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AggressorSide {
    /// Buyer initiated.
    Buy,
    /// Seller initiated.
    Sell,
    /// Source does not establish the aggressor.
    Unknown,
}

/// Venue auction phase.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AuctionPhase {
    /// Opening auction.
    Opening,
    /// Closing auction.
    Closing,
    /// Volatility or reopening auction.
    Volatility,
    /// Other venue-defined auction.
    Other,
}

/// Whether a trading halt began or ended.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum HaltTransition {
    /// Trading entered a halted state.
    Halted,
    /// Trading resumed after a halt.
    Resumed,
}

/// A strictly positive displayed price level.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub struct BookLevel {
    price: PriceTicks,
    quantity: QuantityLots,
}

impl BookLevel {
    /// Constructs a displayed level.
    ///
    /// # Errors
    ///
    /// Returns [`MarketEventError::ZeroQuantity`] because zero means deletion only in deltas.
    pub fn new(price: PriceTicks, quantity: QuantityLots) -> Result<Self, MarketEventError> {
        if quantity.get() == 0 {
            Err(MarketEventError::ZeroQuantity)
        } else {
            Ok(Self { price, quantity })
        }
    }

    /// Returns the integer tick price.
    pub const fn price(self) -> PriceTicks {
        self.price
    }

    /// Returns the positive integer lot quantity.
    pub const fn quantity(self) -> QuantityLots {
        self.quantity
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct BookLevelWire {
    price: PriceTicks,
    quantity: QuantityLots,
}

impl<'de> Deserialize<'de> for BookLevel {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = BookLevelWire::deserialize(deserializer)?;
        Self::new(wire.price, wire.quantity).map_err(serde::de::Error::custom)
    }
}

/// An incremental book change; zero quantity explicitly deletes the level.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BookChange {
    side: MarketSide,
    price: PriceTicks,
    quantity: QuantityLots,
}

impl BookChange {
    /// Constructs an update, retaining zero as the venue-standard delete operation.
    pub const fn new(side: MarketSide, price: PriceTicks, quantity: QuantityLots) -> Self {
        Self {
            side,
            price,
            quantity,
        }
    }

    /// Returns the affected side.
    pub const fn side(self) -> MarketSide {
        self.side
    }

    /// Returns the affected price.
    pub const fn price(self) -> PriceTicks {
        self.price
    }

    /// Returns the new quantity; zero means delete.
    pub const fn quantity(self) -> QuantityLots {
        self.quantity
    }
}

/// Exact merger consideration retained from the source record.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, tag = "kind", rename_all = "snake_case")]
pub enum MergerConsideration {
    /// The source identifies a merger but does not provide complete economic terms.
    #[default]
    Unspecified,
    /// Stock-only consideration expressed as new units per old unit.
    Stock {
        /// New successor units issued.
        numerator: NonZeroU32,
        /// Old subject units surrendered.
        denominator: NonZeroU32,
    },
    /// Cash-only consideration per acquired unit.
    Cash {
        /// Exact cash amount with explicit currency.
        amount: Money,
    },
    /// Stock and cash consideration per acquired unit.
    Mixed {
        /// New successor units issued.
        numerator: NonZeroU32,
        /// Old subject units surrendered.
        denominator: NonZeroU32,
        /// Exact cash component with explicit currency.
        cash: Money,
    },
}

/// A typed corporate action shared by live and research payloads.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, tag = "kind", rename_all = "snake_case")]
pub enum CorporateActionKind {
    /// Share split with an exact nonzero ratio.
    Split {
        /// New units in the split ratio.
        numerator: NonZeroU32,
        /// Old units in the split ratio.
        denominator: NonZeroU32,
    },
    /// Cash distribution with explicit currency.
    CashDividend {
        /// Amount per entitled unit.
        amount: Money,
    },
    /// Distribution of a distinct instrument at an exact nonzero ratio.
    Spinoff {
        /// Stable identity of the distributed instrument.
        distributed_instrument: InstrumentId,
        /// Distributed units received.
        numerator: NonZeroU32,
        /// Subject units held.
        denominator: NonZeroU32,
    },
    /// Return of invested capital with explicit currency.
    ReturnOfCapital {
        /// Amount returned per entitled unit.
        amount: Money,
    },
    /// Merger into a distinct stable internal instrument.
    Merger {
        /// Successor instrument.
        successor: InstrumentId,
        /// Exact source-supplied economics, or an explicit incomplete legacy state.
        #[serde(default)]
        consideration: MergerConsideration,
    },
    /// Instrument delisting with no invented successor.
    Delisting,
    /// Venue symbol change for the same stable instrument.
    SymbolChange {
        /// Venue namespace in which the symbol changed.
        venue_id: VenueId,
        /// Prior venue symbol.
        previous: VenueSymbol,
        /// New venue symbol.
        current: VenueSymbol,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CorporateActionInvariantError {
    SelfMerger,
    SelfSpinoff,
    NonPositiveMonetaryAmount,
}

impl CorporateActionKind {
    pub(crate) fn validate_for_instrument(
        &self,
        instrument_id: InstrumentId,
    ) -> Result<(), CorporateActionInvariantError> {
        match self {
            Self::Merger { successor, .. } if *successor == instrument_id => {
                return Err(CorporateActionInvariantError::SelfMerger);
            }
            Self::Spinoff {
                distributed_instrument,
                ..
            } if *distributed_instrument == instrument_id => {
                return Err(CorporateActionInvariantError::SelfSpinoff);
            }
            _ => {}
        }

        let monetary_amount = match self {
            Self::CashDividend { amount } | Self::ReturnOfCapital { amount } => Some(*amount),
            Self::Merger {
                consideration: MergerConsideration::Cash { amount },
                ..
            } => Some(*amount),
            Self::Merger {
                consideration: MergerConsideration::Mixed { cash, .. },
                ..
            } => Some(*cash),
            _ => None,
        };
        if monetary_amount.is_some_and(|amount| amount.amount() <= Decimal::ZERO) {
            Err(CorporateActionInvariantError::NonPositiveMonetaryAmount)
        } else {
            Ok(())
        }
    }
}

/// A canonical live-market event.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(
    deny_unknown_fields,
    tag = "event",
    content = "payload",
    rename_all = "snake_case"
)]
pub enum MarketEvent {
    /// Executed trade.
    Trade(TradeEvent),
    /// One- or two-sided quote.
    Quote(QuoteEvent),
    /// Complete order-book image for a connection generation.
    BookSnapshot(BookSnapshotEvent),
    /// Incremental order-book changes.
    BookDelta(BookDeltaEvent),
    /// Auction indication or result.
    Auction(AuctionEvent),
    /// Trading halt or resumption.
    TradingHalt(TradingHaltEvent),
    /// Instrument trading-status update.
    InstrumentStatus(InstrumentStatusEvent),
    /// Corporate action distributed through a live market channel.
    CorporateAction(CorporateActionEvent),
}

/// A canonical market-payload invariant failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MarketEventError {
    /// A live instrument-scoped event lacks stable instrument identity.
    MissingInstrument,
    /// A venue-scoped live event lacks venue identity.
    MissingVenue,
    /// Provenance is bound to a different live-event class.
    ProvenanceEventClassMismatch,
    /// A book payload depth differs from its bound book-state depth.
    ProvenanceDepthMismatch,
    /// A displayed or executed quantity is zero.
    ZeroQuantity,
    /// A quote or book is crossed.
    CrossedMarket,
    /// A quote contains neither a bid nor an ask.
    EmptyQuote,
    /// Snapshot levels are duplicated or not in canonical best-to-worst order.
    InvalidBookOrdering {
        /// Side whose ordering is invalid.
        side: MarketSide,
    },
    /// A delta contains no changes.
    EmptyBookDelta,
    /// A merger successor is the same stable instrument.
    SelfMerger,
    /// A spinoff distributes the same stable instrument.
    SelfSpinoff,
    /// A corporate-action monetary distribution or consideration is not strictly positive.
    NonPositiveCorporateActionAmount,
    /// A symbol-change action does not change the symbol.
    UnchangedSymbol,
    /// A symbol-change action's venue disagrees with record provenance.
    CorporateActionVenueMismatch,
}

impl fmt::Display for MarketEventError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingInstrument => formatter.write_str("market event requires an instrument"),
            Self::MissingVenue => formatter.write_str("market event requires a venue"),
            Self::ProvenanceEventClassMismatch => {
                formatter.write_str("market payload class does not match provenance binding")
            }
            Self::ProvenanceDepthMismatch => {
                formatter.write_str("market payload depth does not match provenance binding")
            }
            Self::ZeroQuantity => formatter.write_str("market quantity must be positive"),
            Self::CrossedMarket => formatter.write_str("best bid must be below best ask"),
            Self::EmptyQuote => formatter.write_str("quote requires a bid or ask"),
            Self::InvalidBookOrdering { side } => {
                write!(
                    formatter,
                    "{side:?} levels are not in strict canonical order"
                )
            }
            Self::EmptyBookDelta => formatter.write_str("book delta requires at least one change"),
            Self::SelfMerger => {
                formatter.write_str("merger successor must be a distinct instrument")
            }
            Self::SelfSpinoff => {
                formatter.write_str("spinoff distribution must be a distinct instrument")
            }
            Self::NonPositiveCorporateActionAmount => {
                formatter.write_str("corporate-action monetary amount must be positive")
            }
            Self::UnchangedSymbol => formatter.write_str("symbol change requires distinct symbols"),
            Self::CorporateActionVenueMismatch => {
                formatter.write_str("symbol-change venue must match event provenance")
            }
        }
    }
}

impl std::error::Error for MarketEventError {}

pub(super) fn validate_market_provenance(
    provenance: &LiveProvenance,
    venue_required: bool,
    expected_class: crate::LiveEventClass,
) -> Result<InstrumentId, MarketEventError> {
    let instrument_id = provenance
        .instrument_id()
        .ok_or(MarketEventError::MissingInstrument)?;
    if venue_required && provenance.venue_id().is_none() {
        return Err(MarketEventError::MissingVenue);
    }
    if provenance.binding().event_class() != expected_class {
        return Err(MarketEventError::ProvenanceEventClassMismatch);
    }
    Ok(instrument_id)
}

pub(super) fn validate_book_depth(
    provenance: &LiveProvenance,
    depth: MarketDepth,
) -> Result<(), MarketEventError> {
    if provenance
        .binding()
        .book_state()
        .map(crate::BookStateBinding::depth)
        != Some(depth)
    {
        return Err(MarketEventError::ProvenanceDepthMismatch);
    }
    Ok(())
}

pub(super) fn validate_book(
    bids: &[BookLevel],
    asks: &[BookLevel],
) -> Result<(), MarketEventError> {
    if bids.windows(2).any(|pair| pair[0].price <= pair[1].price) {
        return Err(MarketEventError::InvalidBookOrdering {
            side: MarketSide::Bid,
        });
    }
    if asks.windows(2).any(|pair| pair[0].price >= pair[1].price) {
        return Err(MarketEventError::InvalidBookOrdering {
            side: MarketSide::Ask,
        });
    }
    if let (Some(bid), Some(ask)) = (bids.first(), asks.first())
        && bid.price >= ask.price
    {
        return Err(MarketEventError::CrossedMarket);
    }
    Ok(())
}
