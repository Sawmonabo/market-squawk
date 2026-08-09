//! Exact-lexeme, order-identity-preserving Kraken level-3 decoder and book state.

use std::collections::HashSet;

use chrono::DateTime;
use market_squawk_domain::{
    DataQuality, InstrumentId, MarketDepth, SourceId, SourceIdentifier, Timestamp, VenueId,
};
use market_squawk_sources::{
    MAX_DECODED_BOOK_ITEMS, MAX_RAW_FRAME_BYTES, ProviderBookSide, ProviderDecimalLexeme,
    ProviderPrice, ProviderQuantity,
};
use rust_decimal::Decimal;
use thiserror::Error;

use super::config::{KrakenL3Config, KrakenL3Depth};
use super::messages::{
    EnvelopeKind, Heartbeat, Level3Envelope, Pong, SnapshotData, SnapshotOrder, StatusEnvelope,
    SubscribeAck, UpdateData, UpdateOrder, WireError, classify, ensure_array_bound, exact_decimal,
};
use crate::messages::validate_warnings;

const CHECKSUM_PRICE_LEVELS: usize = 10;

/// Generation-local synchronization state for one configured product.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum KrakenL3DecoderState {
    /// A successful acknowledgement and fresh snapshot are required.
    AwaitingSnapshot,
    /// The snapshot and every accepted update passed protocol and checksum validation.
    Healthy,
    /// Updates are isolated until a fresh checksum-valid snapshot is received.
    Quarantined,
}

/// Exact individual resting order retained from Kraken.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct KrakenL3Order {
    order_id: SourceIdentifier,
    side: ProviderBookSide,
    price: ProviderPrice,
    quantity: ProviderQuantity,
    timestamp: Timestamp,
}

impl KrakenL3Order {
    /// Returns the stable provider order identity.
    pub const fn order_id(&self) -> &SourceIdentifier {
        &self.order_id
    }

    /// Returns the book side.
    pub const fn side(&self) -> ProviderBookSide {
        self.side
    }

    /// Returns the exact provider price lexeme and checked decimal.
    pub const fn price(&self) -> &ProviderPrice {
        &self.price
    }

    /// Returns the exact provider quantity lexeme and checked decimal.
    pub const fn quantity(&self) -> &ProviderQuantity {
        &self.quantity
    }

    /// Returns the provider's order insertion or amendment timestamp.
    pub const fn timestamp(&self) -> Timestamp {
        self.timestamp
    }
}

/// Meaning of one order-level event.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum KrakenL3OrderEventKind {
    /// Order originated in an initializing snapshot.
    Snapshot,
    /// A newly visible order was added.
    Add,
    /// The remaining visible quantity was reduced.
    Modify,
    /// The order was removed.
    Delete,
}

/// One validated provider order event in wire order.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct KrakenL3OrderEvent {
    kind: KrakenL3OrderEventKind,
    order: KrakenL3Order,
}

impl KrakenL3OrderEvent {
    /// Returns the event meaning.
    pub const fn kind(&self) -> KrakenL3OrderEventKind {
        self.kind
    }

    /// Returns exact provider order evidence.
    pub const fn order(&self) -> &KrakenL3Order {
        &self.order
    }
}

/// Whether a committed batch initialized or incrementally changed the book.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum KrakenL3BatchKind {
    /// Fresh complete retained-depth image.
    Snapshot,
    /// Message-atomic incremental change set.
    Update,
}

/// Checksum-validated order-level batch for one mapped instrument.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct KrakenL3BookBatch {
    source_id: SourceId,
    venue_id: VenueId,
    symbol: SourceIdentifier,
    instrument: InstrumentId,
    kind: KrakenL3BatchKind,
    local_generation_ordinal: u64,
    timestamp: Timestamp,
    checksum: u32,
    events: Vec<KrakenL3OrderEvent>,
}

impl KrakenL3BookBatch {
    /// Returns the immutable registered source identity.
    pub const fn source_id(&self) -> &SourceId {
        &self.source_id
    }

    /// Returns the direct venue identity.
    pub const fn venue_id(&self) -> &VenueId {
        &self.venue_id
    }

    /// Returns the exact Kraken symbol.
    pub const fn symbol(&self) -> &SourceIdentifier {
        &self.symbol
    }

    /// Returns the mapped internal instrument.
    pub const fn instrument(&self) -> InstrumentId {
        self.instrument
    }

    /// Returns snapshot/update semantics.
    pub const fn kind(&self) -> KrakenL3BatchKind {
        self.kind
    }

    /// Returns a local connection-generation ordinal.
    ///
    /// Kraken supplies no L3 sequence field. This ordinal is never provider sequence evidence and
    /// resets on reconnect; the provider checksum is the synchronization authority.
    pub const fn local_generation_ordinal(&self) -> u64 {
        self.local_generation_ordinal
    }

    /// Returns the provider market-message timestamp.
    pub const fn timestamp(&self) -> Timestamp {
        self.timestamp
    }

    /// Returns the validated provider checksum.
    pub const fn checksum(&self) -> u32 {
        self.checksum
    }

    /// Returns validated order events in provider wire order.
    pub fn events(&self) -> &[KrakenL3OrderEvent] {
        &self.events
    }

    /// Returns the explicit provider depth class.
    pub const fn market_depth(&self) -> MarketDepth {
        MarketDepth::OrderLevel
    }

    /// Returns the maximum data-quality class this adapter may claim.
    pub const fn quality_ceiling(&self) -> DataQuality {
        DataQuality::DirectUnverified
    }
}

/// Validated connection/control message.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum KrakenL3Control {
    /// Connection liveness only; never market freshness.
    Heartbeat,
    /// Application ping response; connection liveness only.
    Pong,
    /// Exchange engine reported `online`.
    Online,
    /// One configured symbol's authenticated subscription was acknowledged.
    Subscribed {
        /// Exact provider symbol.
        symbol: SourceIdentifier,
        /// Stable mapped instrument.
        instrument: InstrumentId,
    },
}

/// Fully validated classification of one Kraken L3 application message.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum KrakenL3DecodeOutcome {
    /// One order-level market batch.
    Book(KrakenL3BookBatch),
    /// Connection/control traffic that does not refresh market data.
    Control(KrakenL3Control),
}

#[derive(Clone, Debug, Default)]
struct OrderBook {
    bids: Vec<KrakenL3Order>,
    asks: Vec<KrakenL3Order>,
}

impl OrderBook {
    fn len(&self) -> usize {
        self.bids.len().saturating_add(self.asks.len())
    }

    fn side(&self, side: ProviderBookSide) -> &[KrakenL3Order] {
        match side {
            ProviderBookSide::Bid => &self.bids,
            ProviderBookSide::Ask => &self.asks,
        }
    }

    fn side_mut(&mut self, side: ProviderBookSide) -> &mut Vec<KrakenL3Order> {
        match side {
            ProviderBookSide::Bid => &mut self.bids,
            ProviderBookSide::Ask => &mut self.asks,
        }
    }

    fn find(&self, order_id: &SourceIdentifier) -> Option<(ProviderBookSide, usize)> {
        self.bids
            .iter()
            .position(|order| order.order_id == *order_id)
            .map(|index| (ProviderBookSide::Bid, index))
            .or_else(|| {
                self.asks
                    .iter()
                    .position(|order| order.order_id == *order_id)
                    .map(|index| (ProviderBookSide::Ask, index))
            })
    }

    fn truncate(&mut self, depth: usize) {
        truncate_side(&mut self.bids, depth);
        truncate_side(&mut self.asks, depth);
    }
}

#[derive(Debug)]
struct ProductState {
    symbol: SourceIdentifier,
    instrument: InstrumentId,
    subscribed: bool,
    state: KrakenL3DecoderState,
    book: OrderBook,
    local_ordinal: u64,
    last_timestamp: Option<Timestamp>,
    last_checksum: Option<u32>,
}

impl ProductState {
    fn reset_for_reconnect(&mut self) {
        self.subscribed = false;
        self.state = KrakenL3DecoderState::AwaitingSnapshot;
        self.book.bids.clear();
        self.book.asks.clear();
        self.local_ordinal = 0;
        self.last_timestamp = None;
        self.last_checksum = None;
    }
}

/// Stateful multi-product decoder for one authenticated WebSocket connection generation.
#[derive(Debug)]
pub struct KrakenL3Decoder {
    source_id: SourceId,
    venue_id: VenueId,
    depth: KrakenL3Depth,
    max_message_bytes: usize,
    products: Vec<ProductState>,
}

impl KrakenL3Decoder {
    /// Constructs bounded empty state from an already validated authenticated configuration.
    ///
    /// # Errors
    ///
    /// Returns an allocation error if bounded generation state cannot be reserved.
    pub fn try_new(config: &KrakenL3Config) -> Result<Self, KrakenL3DecodeError> {
        let mut products = Vec::new();
        products
            .try_reserve_exact(config.products().len())
            .map_err(|_| KrakenL3DecodeError::Allocation)?;
        for mapping in config.products() {
            products.push(ProductState {
                symbol: SourceIdentifier::try_from(mapping.symbol())
                    .map_err(|_| KrakenL3DecodeError::MalformedPayload)?,
                instrument: mapping.instrument(),
                subscribed: false,
                state: KrakenL3DecoderState::AwaitingSnapshot,
                book: OrderBook::default(),
                local_ordinal: 0,
                last_timestamp: None,
                last_checksum: None,
            });
        }
        Ok(Self {
            source_id: config.metadata().source_id().clone(),
            venue_id: VenueId::try_from("kraken")
                .map_err(|_| KrakenL3DecodeError::MalformedPayload)?,
            depth: config.retained_price_levels(),
            max_message_bytes: config.max_message_bytes(),
            products,
        })
    }

    /// Clears all connection-generation state before a reconnect and fresh token/subscription.
    pub fn reset_for_reconnect(&mut self) {
        for product in &mut self.products {
            product.reset_for_reconnect();
        }
    }

    /// Returns the synchronization state for one configured symbol.
    pub fn state(&self, symbol: &str) -> Option<KrakenL3DecoderState> {
        self.product(symbol).map(|product| product.state)
    }

    /// Returns the last committed checksum for one configured symbol.
    pub fn last_checksum(&self, symbol: &str) -> Option<u32> {
        self.product(symbol)
            .and_then(|product| product.last_checksum)
    }

    /// Returns a retained order by its exact provider identity.
    pub fn order(&self, symbol: &str, order_id: &str) -> Option<&KrakenL3Order> {
        let product = self.product(symbol)?;
        product
            .book
            .bids
            .iter()
            .chain(&product.book.asks)
            .find(|order| order.order_id.as_str() == order_id)
    }

    /// Returns the number of retained individual orders for one product.
    pub fn order_count(&self, symbol: &str) -> Option<usize> {
        self.product(symbol).map(|product| {
            product
                .book
                .bids
                .len()
                .saturating_add(product.book.asks.len())
        })
    }

    /// Parses, validates, and atomically applies one bounded application message.
    ///
    /// Any rejected message quarantines every product on the affected connection generation.
    /// Heartbeats and acknowledgements never update market freshness. A checksum-valid fresh
    /// snapshot is the only in-generation recovery path; reconnect callers must call
    /// [`Self::reset_for_reconnect`] and obtain a fresh provider token.
    ///
    /// # Errors
    ///
    /// Rejects malformed evidence, unknown products, unsupported state transitions, invalid exact
    /// numbers, crossed books, checksum mismatches, or an exhausted local ordinal.
    pub fn decode_payload(
        &mut self,
        payload: &[u8],
    ) -> Result<KrakenL3DecodeOutcome, KrakenL3DecodeError> {
        if payload.len() > self.max_message_bytes || payload.len() > MAX_RAW_FRAME_BYTES {
            self.quarantine_all();
            return Err(KrakenL3DecodeError::MessageTooLarge);
        }
        let outcome = self.decode_inner(payload);
        if outcome.is_err() {
            self.quarantine_all();
        }
        outcome
    }

    fn decode_inner(
        &mut self,
        payload: &[u8],
    ) -> Result<KrakenL3DecodeOutcome, KrakenL3DecodeError> {
        match classify(payload).map_err(map_wire_error)? {
            EnvelopeKind::Level3 => self.decode_book(payload),
            EnvelopeKind::Heartbeat => validate_heartbeat(payload),
            EnvelopeKind::Status => validate_status(payload),
            EnvelopeKind::SubscribeAck => self.validate_ack(payload),
            EnvelopeKind::Pong => validate_pong(payload),
        }
    }

    fn decode_book(
        &mut self,
        payload: &[u8],
    ) -> Result<KrakenL3DecodeOutcome, KrakenL3DecodeError> {
        let envelope: Level3Envelope<'_> =
            serde_json::from_slice(payload).map_err(|_| KrakenL3DecodeError::MalformedPayload)?;
        if envelope.channel != "level3"
            || ensure_array_bound(envelope.data, 1).map_err(map_wire_error)? != 1
        {
            return Err(KrakenL3DecodeError::MalformedPayload);
        }
        match envelope.kind {
            "snapshot" => self.decode_snapshot(envelope.data),
            "update" => self.decode_update(envelope.data),
            _ => Err(KrakenL3DecodeError::MalformedPayload),
        }
    }

    fn decode_snapshot(
        &mut self,
        raw_data: &serde_json::value::RawValue,
    ) -> Result<KrakenL3DecodeOutcome, KrakenL3DecodeError> {
        let data = parse_single::<SnapshotData<'_>>(raw_data)?;
        let product_index = self
            .product_index(data.symbol)
            .ok_or(KrakenL3DecodeError::UnknownProduct)?;
        if !self.products[product_index].subscribed
            || self.products[product_index].state == KrakenL3DecoderState::Healthy
        {
            return Err(KrakenL3DecodeError::ResynchronizationRequired);
        }
        let message_timestamp = parse_timestamp(data.timestamp)?;
        if self.products[product_index]
            .last_timestamp
            .is_some_and(|previous| message_timestamp < previous)
        {
            return Err(KrakenL3DecodeError::TimestampRegression);
        }
        let bid_count =
            ensure_array_bound(data.bids, MAX_DECODED_BOOK_ITEMS).map_err(map_wire_error)?;
        let ask_count =
            ensure_array_bound(data.asks, MAX_DECODED_BOOK_ITEMS).map_err(map_wire_error)?;
        let total = bid_count
            .checked_add(ask_count)
            .filter(|total| *total <= MAX_DECODED_BOOK_ITEMS)
            .ok_or(KrakenL3DecodeError::TooManyOrders {
                max: MAX_DECODED_BOOK_ITEMS,
            })?;
        if self
            .orders_outside(product_index)
            .checked_add(total)
            .is_none_or(|orders| orders > MAX_DECODED_BOOK_ITEMS)
        {
            return Err(KrakenL3DecodeError::TooManyOrders {
                max: MAX_DECODED_BOOK_ITEMS,
            });
        }
        let bid_wire: Vec<SnapshotOrder<'_>> = serde_json::from_str(data.bids.get())
            .map_err(|_| KrakenL3DecodeError::MalformedPayload)?;
        let ask_wire: Vec<SnapshotOrder<'_>> = serde_json::from_str(data.asks.get())
            .map_err(|_| KrakenL3DecodeError::MalformedPayload)?;
        if bid_wire.len() != bid_count || ask_wire.len() != ask_count {
            return Err(KrakenL3DecodeError::MalformedPayload);
        }
        let mut identities = HashSet::new();
        identities
            .try_reserve(total)
            .map_err(|_| KrakenL3DecodeError::Allocation)?;
        let bids = parse_snapshot_side(
            &bid_wire,
            ProviderBookSide::Bid,
            message_timestamp,
            &mut identities,
        )?;
        let asks = parse_snapshot_side(
            &ask_wire,
            ProviderBookSide::Ask,
            message_timestamp,
            &mut identities,
        )?;
        let mut candidate = OrderBook { bids, asks };
        validate_side_order(&candidate.bids, ProviderBookSide::Bid, self.depth.get())?;
        validate_side_order(&candidate.asks, ProviderBookSide::Ask, self.depth.get())?;
        validate_uncrossed(&candidate)?;
        let expected = parse_checksum(data.checksum)?;
        let computed = level3_crc32(&candidate);
        if expected != computed {
            return Err(KrakenL3DecodeError::ChecksumMismatch { expected, computed });
        }
        candidate.truncate(self.depth.get());
        let next_ordinal = self.products[product_index]
            .local_ordinal
            .checked_add(1)
            .ok_or(KrakenL3DecodeError::OrdinalOverflow)?;
        let mut events = Vec::new();
        events
            .try_reserve_exact(total)
            .map_err(|_| KrakenL3DecodeError::Allocation)?;
        events.extend(
            candidate
                .bids
                .iter()
                .chain(&candidate.asks)
                .cloned()
                .map(|order| KrakenL3OrderEvent {
                    kind: KrakenL3OrderEventKind::Snapshot,
                    order,
                }),
        );
        let source_id = self.source_id.clone();
        let venue_id = self.venue_id.clone();
        let product = &mut self.products[product_index];
        product.book = candidate;
        product.local_ordinal = next_ordinal;
        product.last_timestamp = Some(message_timestamp);
        product.last_checksum = Some(computed);
        product.state = KrakenL3DecoderState::Healthy;
        Ok(KrakenL3DecodeOutcome::Book(KrakenL3BookBatch {
            source_id,
            venue_id,
            symbol: product.symbol.clone(),
            instrument: product.instrument,
            kind: KrakenL3BatchKind::Snapshot,
            local_generation_ordinal: next_ordinal,
            timestamp: message_timestamp,
            checksum: computed,
            events,
        }))
    }

    fn decode_update(
        &mut self,
        raw_data: &serde_json::value::RawValue,
    ) -> Result<KrakenL3DecodeOutcome, KrakenL3DecodeError> {
        let data = parse_single::<UpdateData<'_>>(raw_data)?;
        let product_index = self
            .product_index(data.symbol)
            .ok_or(KrakenL3DecodeError::UnknownProduct)?;
        let product = &self.products[product_index];
        if !product.subscribed || product.state != KrakenL3DecoderState::Healthy {
            return Err(KrakenL3DecodeError::ResynchronizationRequired);
        }
        let message_timestamp = parse_timestamp(data.timestamp)?;
        if product
            .last_timestamp
            .is_some_and(|previous| message_timestamp < previous)
        {
            return Err(KrakenL3DecodeError::TimestampRegression);
        }
        let bid_count = count_optional_array(data.bids)?;
        let ask_count = count_optional_array(data.asks)?;
        let total = bid_count
            .checked_add(ask_count)
            .filter(|total| *total > 0 && *total <= MAX_DECODED_BOOK_ITEMS)
            .ok_or(KrakenL3DecodeError::TooManyOrders {
                max: MAX_DECODED_BOOK_ITEMS,
            })?;
        let bid_wire = parse_optional_update_array(data.bids, bid_count)?;
        let ask_wire = parse_optional_update_array(data.asks, ask_count)?;
        let mut events = Vec::new();
        events
            .try_reserve_exact(total)
            .map_err(|_| KrakenL3DecodeError::Allocation)?;
        events.extend(parse_update_side(
            bid_wire,
            ProviderBookSide::Bid,
            message_timestamp,
        )?);
        events.extend(parse_update_side(
            ask_wire,
            ProviderBookSide::Ask,
            message_timestamp,
        )?);
        let expected = parse_checksum(data.checksum)?;
        let next_ordinal = self.products[product_index]
            .local_ordinal
            .checked_add(1)
            .ok_or(KrakenL3DecodeError::OrdinalOverflow)?;
        let maximum_product_orders = MAX_DECODED_BOOK_ITEMS
            .checked_sub(self.orders_outside(product_index))
            .ok_or(KrakenL3DecodeError::TooManyOrders {
                max: MAX_DECODED_BOOK_ITEMS,
            })?;

        let source_id = self.source_id.clone();
        let venue_id = self.venue_id.clone();
        let product = &mut self.products[product_index];
        let mut undo = Vec::new();
        undo.try_reserve_exact(events.len())
            .map_err(|_| KrakenL3DecodeError::Allocation)?;
        for event in &events {
            match apply_event(&mut product.book, event) {
                Ok(inverse) => undo.push(inverse),
                Err(error) => {
                    if !rollback(&mut product.book, undo) {
                        product.book = OrderBook::default();
                    }
                    return Err(error);
                }
            }
        }
        if retained_order_count(&product.book, self.depth.get()) > maximum_product_orders {
            if !rollback(&mut product.book, undo) {
                product.book = OrderBook::default();
            }
            return Err(KrakenL3DecodeError::TooManyOrders {
                max: MAX_DECODED_BOOK_ITEMS,
            });
        }
        let validation = validate_uncrossed(&product.book).and_then(|()| {
            let computed = level3_crc32(&product.book);
            if expected == computed {
                Ok(computed)
            } else {
                Err(KrakenL3DecodeError::ChecksumMismatch { expected, computed })
            }
        });
        let computed = match validation {
            Ok(computed) => computed,
            Err(error) => {
                if !rollback(&mut product.book, undo) {
                    product.book = OrderBook::default();
                }
                return Err(error);
            }
        };
        product.book.truncate(self.depth.get());
        product.local_ordinal = next_ordinal;
        product.last_timestamp = Some(message_timestamp);
        product.last_checksum = Some(computed);
        Ok(KrakenL3DecodeOutcome::Book(KrakenL3BookBatch {
            source_id,
            venue_id,
            symbol: product.symbol.clone(),
            instrument: product.instrument,
            kind: KrakenL3BatchKind::Update,
            local_generation_ordinal: next_ordinal,
            timestamp: message_timestamp,
            checksum: computed,
            events,
        }))
    }

    fn validate_ack(
        &mut self,
        payload: &[u8],
    ) -> Result<KrakenL3DecodeOutcome, KrakenL3DecodeError> {
        let ack: SubscribeAck<'_> =
            serde_json::from_slice(payload).map_err(|_| KrakenL3DecodeError::MalformedPayload)?;
        let result = ack
            .result
            .as_ref()
            .ok_or(KrakenL3DecodeError::SubscriptionRejected)?;
        validate_warnings(result.warnings).map_err(|_| KrakenL3DecodeError::MalformedPayload)?;
        parse_timestamp(ack.time_in)?;
        parse_timestamp(ack.time_out)?;
        if ack.method != "subscribe"
            || !ack.success
            || ack.error.is_some()
            || ack.req_id == Some(0)
            || result.channel != "level3"
            || result.depth != self.depth.get()
            || !result.snapshot
        {
            return Err(KrakenL3DecodeError::SubscriptionRejected);
        }
        let product_index = self
            .product_index(result.symbol)
            .ok_or(KrakenL3DecodeError::UnknownProduct)?;
        let product = &mut self.products[product_index];
        if product.subscribed {
            return Err(KrakenL3DecodeError::SubscriptionRejected);
        }
        product.subscribed = true;
        Ok(KrakenL3DecodeOutcome::Control(
            KrakenL3Control::Subscribed {
                symbol: product.symbol.clone(),
                instrument: product.instrument,
            },
        ))
    }

    fn product(&self, symbol: &str) -> Option<&ProductState> {
        self.products
            .iter()
            .find(|product| product.symbol.as_str() == symbol)
    }

    fn product_index(&self, symbol: &str) -> Option<usize> {
        self.products
            .iter()
            .position(|product| product.symbol.as_str() == symbol)
    }

    fn orders_outside(&self, product_index: usize) -> usize {
        self.products
            .iter()
            .enumerate()
            .filter(|(index, _product)| *index != product_index)
            .fold(0_usize, |total, (_index, product)| {
                total.saturating_add(product.book.len())
            })
    }

    fn quarantine_all(&mut self) {
        for product in &mut self.products {
            product.state = KrakenL3DecoderState::Quarantined;
        }
    }
}

fn parse_single<'a, T>(raw: &'a serde_json::value::RawValue) -> Result<T, KrakenL3DecodeError>
where
    T: serde::Deserialize<'a>,
{
    let mut values: Vec<T> =
        serde_json::from_str(raw.get()).map_err(|_| KrakenL3DecodeError::MalformedPayload)?;
    if values.len() != 1 {
        return Err(KrakenL3DecodeError::MalformedPayload);
    }
    values.pop().ok_or(KrakenL3DecodeError::MalformedPayload)
}

fn parse_snapshot_side<'a>(
    wire: &'a [SnapshotOrder<'a>],
    side: ProviderBookSide,
    message_timestamp: Timestamp,
    identities: &mut HashSet<&'a str>,
) -> Result<Vec<KrakenL3Order>, KrakenL3DecodeError> {
    let mut orders = Vec::new();
    orders
        .try_reserve_exact(wire.len())
        .map_err(|_| KrakenL3DecodeError::Allocation)?;
    for value in wire {
        if !identities.insert(value.order_id) {
            return Err(KrakenL3DecodeError::DuplicateOrder);
        }
        orders.push(parse_order(
            value.order_id,
            side,
            value.limit_price,
            value.order_qty,
            value.timestamp,
            message_timestamp,
            false,
        )?);
    }
    Ok(orders)
}

fn count_optional_array(
    raw: Option<&serde_json::value::RawValue>,
) -> Result<usize, KrakenL3DecodeError> {
    raw.map_or(Ok(0), |value| {
        ensure_array_bound(value, MAX_DECODED_BOOK_ITEMS).map_err(map_wire_error)
    })
}

fn parse_optional_update_array<'a>(
    raw: Option<&'a serde_json::value::RawValue>,
    expected: usize,
) -> Result<Vec<UpdateOrder<'a>>, KrakenL3DecodeError> {
    let Some(raw) = raw else {
        return Ok(Vec::new());
    };
    let values: Vec<UpdateOrder<'_>> =
        serde_json::from_str(raw.get()).map_err(|_| KrakenL3DecodeError::MalformedPayload)?;
    if values.len() != expected {
        return Err(KrakenL3DecodeError::MalformedPayload);
    }
    Ok(values)
}

fn parse_update_side(
    wire: Vec<UpdateOrder<'_>>,
    side: ProviderBookSide,
    message_timestamp: Timestamp,
) -> Result<Vec<KrakenL3OrderEvent>, KrakenL3DecodeError> {
    let mut events = Vec::new();
    events
        .try_reserve_exact(wire.len())
        .map_err(|_| KrakenL3DecodeError::Allocation)?;
    for value in wire {
        let kind = match value.event {
            "add" => KrakenL3OrderEventKind::Add,
            "modify" => KrakenL3OrderEventKind::Modify,
            "delete" => KrakenL3OrderEventKind::Delete,
            _ => return Err(KrakenL3DecodeError::MalformedPayload),
        };
        let order = parse_order(
            value.order_id,
            side,
            value.limit_price,
            value.order_qty,
            value.timestamp,
            message_timestamp,
            kind == KrakenL3OrderEventKind::Delete,
        )?;
        events.push(KrakenL3OrderEvent { kind, order });
    }
    Ok(events)
}

fn parse_order(
    order_id: &str,
    side: ProviderBookSide,
    price: &serde_json::value::RawValue,
    quantity: &serde_json::value::RawValue,
    timestamp: &str,
    message_timestamp: Timestamp,
    allow_zero_quantity: bool,
) -> Result<KrakenL3Order, KrakenL3DecodeError> {
    let order_timestamp = parse_timestamp(timestamp)?;
    if order_timestamp > message_timestamp {
        return Err(KrakenL3DecodeError::TimestampRegression);
    }
    let price = parse_decimal(price)?;
    if price.decimal() <= Decimal::ZERO {
        return Err(KrakenL3DecodeError::InexactValue);
    }
    let quantity = parse_decimal(quantity)?;
    if quantity.decimal() < Decimal::ZERO
        || (!allow_zero_quantity && quantity.decimal() == Decimal::ZERO)
    {
        return Err(KrakenL3DecodeError::InexactValue);
    }
    Ok(KrakenL3Order {
        order_id: SourceIdentifier::try_from(order_id)
            .map_err(|_| KrakenL3DecodeError::MalformedPayload)?,
        side,
        price: ProviderPrice::new(price),
        quantity: ProviderQuantity::new(quantity),
        timestamp: order_timestamp,
    })
}

fn parse_decimal(
    raw: &serde_json::value::RawValue,
) -> Result<ProviderDecimalLexeme, KrakenL3DecodeError> {
    let value = exact_decimal(raw).map_err(map_wire_error)?;
    ProviderDecimalLexeme::try_new(value).map_err(|_| KrakenL3DecodeError::InexactValue)
}

fn parse_checksum(raw: &serde_json::value::RawValue) -> Result<u32, KrakenL3DecodeError> {
    let value = exact_decimal(raw).map_err(map_wire_error)?;
    if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(KrakenL3DecodeError::MalformedPayload);
    }
    value
        .parse::<u32>()
        .map_err(|_| KrakenL3DecodeError::MalformedPayload)
}

fn parse_timestamp(value: &str) -> Result<Timestamp, KrakenL3DecodeError> {
    let parsed =
        DateTime::parse_from_rfc3339(value).map_err(|_| KrakenL3DecodeError::MalformedPayload)?;
    let nanos = parsed
        .timestamp()
        .checked_mul(1_000_000_000)
        .and_then(|base| base.checked_add(i64::from(parsed.timestamp_subsec_nanos())))
        .ok_or(KrakenL3DecodeError::InexactValue)?;
    Ok(Timestamp::from_unix_nanos(nanos))
}

fn validate_side_order(
    orders: &[KrakenL3Order],
    side: ProviderBookSide,
    retained_depth: usize,
) -> Result<(), KrakenL3DecodeError> {
    let mut previous = None;
    let mut levels = 0_usize;
    for order in orders {
        let price = order.price.value().decimal();
        if order.side != side || order.quantity.value().decimal() <= Decimal::ZERO {
            return Err(KrakenL3DecodeError::InvalidBook);
        }
        if previous != Some(price) {
            if previous.is_some_and(|prior| match side {
                ProviderBookSide::Bid => prior < price,
                ProviderBookSide::Ask => prior > price,
            }) {
                return Err(KrakenL3DecodeError::InvalidBook);
            }
            levels = levels
                .checked_add(1)
                .ok_or(KrakenL3DecodeError::Allocation)?;
            previous = Some(price);
        }
    }
    if levels > retained_depth {
        return Err(KrakenL3DecodeError::InvalidBook);
    }
    Ok(())
}

fn validate_uncrossed(book: &OrderBook) -> Result<(), KrakenL3DecodeError> {
    if let (Some(bid), Some(ask)) = (book.bids.first(), book.asks.first())
        && bid.price.value().decimal() >= ask.price.value().decimal()
    {
        return Err(KrakenL3DecodeError::CrossedBook);
    }
    Ok(())
}

#[derive(Debug)]
enum Undo {
    RemoveAdded {
        side: ProviderBookSide,
        index: usize,
        order_id: SourceIdentifier,
    },
    RestoreModified {
        side: ProviderBookSide,
        index: usize,
        prior: KrakenL3Order,
    },
    RestoreDeleted {
        side: ProviderBookSide,
        index: usize,
        prior: KrakenL3Order,
    },
}

fn apply_event(
    book: &mut OrderBook,
    event: &KrakenL3OrderEvent,
) -> Result<Undo, KrakenL3DecodeError> {
    match event.kind {
        KrakenL3OrderEventKind::Snapshot => Err(KrakenL3DecodeError::MalformedPayload),
        KrakenL3OrderEventKind::Add => {
            if event.order.quantity.value().decimal() <= Decimal::ZERO
                || book.find(&event.order.order_id).is_some()
            {
                return Err(KrakenL3DecodeError::DuplicateOrder);
            }
            let side = event.order.side;
            let orders = book.side_mut(side);
            orders
                .try_reserve(1)
                .map_err(|_| KrakenL3DecodeError::Allocation)?;
            let index = insertion_index(orders, side, event.order.price.value().decimal());
            orders.insert(index, event.order.clone());
            Ok(Undo::RemoveAdded {
                side,
                index,
                order_id: event.order.order_id.clone(),
            })
        }
        KrakenL3OrderEventKind::Modify => {
            let (side, index) = book
                .find(&event.order.order_id)
                .ok_or(KrakenL3DecodeError::UnknownOrder)?;
            if side != event.order.side {
                return Err(KrakenL3DecodeError::UnknownOrder);
            }
            let orders = book.side_mut(side);
            let current = orders.get(index).ok_or(KrakenL3DecodeError::InvalidBook)?;
            if current.price.value().decimal() != event.order.price.value().decimal()
                || event.order.quantity.value().decimal() <= Decimal::ZERO
                || event.order.quantity.value().decimal() >= current.quantity.value().decimal()
                || event.order.timestamp < current.timestamp
            {
                return Err(KrakenL3DecodeError::InvalidOrderTransition);
            }
            let prior = current.clone();
            let target = orders
                .get_mut(index)
                .ok_or(KrakenL3DecodeError::InvalidBook)?;
            *target = event.order.clone();
            Ok(Undo::RestoreModified { side, index, prior })
        }
        KrakenL3OrderEventKind::Delete => {
            let (side, index) = book
                .find(&event.order.order_id)
                .ok_or(KrakenL3DecodeError::UnknownOrder)?;
            if side != event.order.side {
                return Err(KrakenL3DecodeError::UnknownOrder);
            }
            let current = book
                .side(side)
                .get(index)
                .ok_or(KrakenL3DecodeError::InvalidBook)?;
            if current.price.value().decimal() != event.order.price.value().decimal()
                || event.order.timestamp < current.timestamp
            {
                return Err(KrakenL3DecodeError::InvalidOrderTransition);
            }
            let prior = book.side_mut(side).remove(index);
            Ok(Undo::RestoreDeleted { side, index, prior })
        }
    }
}

fn rollback(book: &mut OrderBook, undo: Vec<Undo>) -> bool {
    for inverse in undo.into_iter().rev() {
        let restored = match inverse {
            Undo::RemoveAdded {
                side,
                index,
                order_id,
            } => {
                let orders = book.side_mut(side);
                if orders.get(index).map(|order| &order.order_id) == Some(&order_id) {
                    orders.remove(index);
                    true
                } else {
                    false
                }
            }
            Undo::RestoreModified { side, index, prior } => {
                let orders = book.side_mut(side);
                if orders.get(index).map(|order| &order.order_id) == Some(&prior.order_id) {
                    if let Some(order) = orders.get_mut(index) {
                        *order = prior;
                        true
                    } else {
                        false
                    }
                } else {
                    false
                }
            }
            Undo::RestoreDeleted { side, index, prior } => {
                let orders = book.side_mut(side);
                if index <= orders.len()
                    && !orders.iter().any(|order| order.order_id == prior.order_id)
                {
                    orders.insert(index, prior);
                    true
                } else {
                    false
                }
            }
        };
        if !restored {
            return false;
        }
    }
    true
}

fn insertion_index(orders: &[KrakenL3Order], side: ProviderBookSide, price: Decimal) -> usize {
    orders
        .iter()
        .position(|order| match side {
            ProviderBookSide::Bid => order.price.value().decimal() < price,
            ProviderBookSide::Ask => order.price.value().decimal() > price,
        })
        .unwrap_or(orders.len())
}

fn truncate_side(orders: &mut Vec<KrakenL3Order>, depth: usize) {
    let mut previous = None;
    let mut levels = 0_usize;
    let mut retained_orders = orders.len();
    for (index, order) in orders.iter().enumerate() {
        let price = order.price.value().decimal();
        if previous != Some(price) {
            levels = levels.saturating_add(1);
            previous = Some(price);
            if levels > depth {
                retained_orders = index;
                break;
            }
        }
    }
    orders.truncate(retained_orders);
}

fn retained_order_count(book: &OrderBook, depth: usize) -> usize {
    retained_side_order_count(&book.bids, depth)
        .saturating_add(retained_side_order_count(&book.asks, depth))
}

fn retained_side_order_count(orders: &[KrakenL3Order], depth: usize) -> usize {
    let mut previous = None;
    let mut levels = 0_usize;
    for (index, order) in orders.iter().enumerate() {
        let price = order.price.value().decimal();
        if previous != Some(price) {
            levels = levels.saturating_add(1);
            previous = Some(price);
            if levels > depth {
                return index;
            }
        }
    }
    orders.len()
}

fn level3_crc32(book: &OrderBook) -> u32 {
    let mut checksum = Crc32::new();
    hash_side(&mut checksum, &book.asks);
    hash_side(&mut checksum, &book.bids);
    checksum.finalize()
}

fn hash_side(checksum: &mut Crc32, orders: &[KrakenL3Order]) {
    let mut previous = None;
    let mut levels = 0_usize;
    for order in orders {
        let price = order.price.value().decimal();
        if previous != Some(price) {
            levels = levels.saturating_add(1);
            previous = Some(price);
            if levels > CHECKSUM_PRICE_LEVELS {
                break;
            }
        }
        append_checksum_component(checksum, order.price.value().as_str().as_bytes());
        append_checksum_component(checksum, order.quantity.value().as_str().as_bytes());
    }
}

fn append_checksum_component(checksum: &mut Crc32, value: &[u8]) {
    let first_significant = value
        .iter()
        .position(|byte| *byte != b'0' && *byte != b'.')
        .unwrap_or(value.len());
    for component in value[first_significant..].split(|byte| *byte == b'.') {
        checksum.update(component);
    }
}

struct Crc32(u32);

impl Crc32 {
    const fn new() -> Self {
        Self(u32::MAX)
    }

    fn update(&mut self, bytes: &[u8]) {
        for byte in bytes {
            self.0 ^= u32::from(*byte);
            for _ in 0..8 {
                let mask = 0_u32.wrapping_sub(self.0 & 1);
                self.0 = (self.0 >> 1) ^ (0xedb8_8320 & mask);
            }
        }
    }

    const fn finalize(self) -> u32 {
        !self.0
    }
}

fn validate_heartbeat(payload: &[u8]) -> Result<KrakenL3DecodeOutcome, KrakenL3DecodeError> {
    let heartbeat: Heartbeat<'_> =
        serde_json::from_slice(payload).map_err(|_| KrakenL3DecodeError::MalformedPayload)?;
    if heartbeat.channel != "heartbeat" {
        return Err(KrakenL3DecodeError::MalformedPayload);
    }
    Ok(KrakenL3DecodeOutcome::Control(KrakenL3Control::Heartbeat))
}

fn validate_status(payload: &[u8]) -> Result<KrakenL3DecodeOutcome, KrakenL3DecodeError> {
    let status: StatusEnvelope<'_> =
        serde_json::from_slice(payload).map_err(|_| KrakenL3DecodeError::MalformedPayload)?;
    let value = status
        .data
        .first()
        .ok_or(KrakenL3DecodeError::MalformedPayload)?;
    if status.channel != "status"
        || status.kind != "update"
        || status.data.len() != 1
        || value.system != "online"
        || value.api_version.is_empty()
        || value.version.is_empty()
        || value.connection_id == 0
    {
        return Err(KrakenL3DecodeError::ResynchronizationRequired);
    }
    Ok(KrakenL3DecodeOutcome::Control(KrakenL3Control::Online))
}

fn validate_pong(payload: &[u8]) -> Result<KrakenL3DecodeOutcome, KrakenL3DecodeError> {
    let pong: Pong<'_> =
        serde_json::from_slice(payload).map_err(|_| KrakenL3DecodeError::MalformedPayload)?;
    parse_timestamp(pong.time_in)?;
    parse_timestamp(pong.time_out)?;
    if pong.method != "pong" || pong.req_id == Some(0) {
        return Err(KrakenL3DecodeError::MalformedPayload);
    }
    Ok(KrakenL3DecodeOutcome::Control(KrakenL3Control::Pong))
}

fn map_wire_error(error: WireError) -> KrakenL3DecodeError {
    match error {
        WireError::Malformed | WireError::Unsupported => KrakenL3DecodeError::MalformedPayload,
        WireError::TooManyItems => KrakenL3DecodeError::TooManyOrders {
            max: MAX_DECODED_BOOK_ITEMS,
        },
    }
}

/// Authenticated Kraken level-3 decode failure.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum KrakenL3DecodeError {
    /// A frame exceeds the configured or global raw-frame limit.
    #[error("Kraken level-3 message exceeds its configured bound")]
    MessageTooLarge,
    /// JSON shape or exact provider identity is malformed.
    #[error("Kraken level-3 message is malformed")]
    MalformedPayload,
    /// An exact financial value cannot be represented or violates positivity.
    #[error("Kraken level-3 value is inexact or outside its domain")]
    InexactValue,
    /// A message exceeds the bounded order count.
    #[error("Kraken level-3 message exceeds {max} orders")]
    TooManyOrders {
        /// Maximum accepted individual orders.
        max: usize,
    },
    /// A message references a symbol not mapped into this connection.
    #[error("Kraken level-3 message references an unknown product")]
    UnknownProduct,
    /// A subscription acknowledgement is absent, inconsistent, or duplicated.
    #[error("Kraken level-3 subscription was rejected or inconsistent")]
    SubscriptionRejected,
    /// The connection requires a fresh snapshot before accepting updates.
    #[error("Kraken level-3 stream requires resynchronization")]
    ResynchronizationRequired,
    /// An order identity was duplicated.
    #[error("Kraken level-3 order identity is duplicated")]
    DuplicateOrder,
    /// A modify or delete references an order absent from retained state.
    #[error("Kraken level-3 update references an unknown order")]
    UnknownOrder,
    /// An order transition violates identity, price, quantity, or time invariants.
    #[error("Kraken level-3 order transition is invalid")]
    InvalidOrderTransition,
    /// Side ordering, quantity, or retained-depth invariants are invalid.
    #[error("Kraken level-3 book invariant is invalid")]
    InvalidBook,
    /// The best bid is not below the best ask.
    #[error("Kraken level-3 book is crossed")]
    CrossedBook,
    /// Provider message, recovery snapshot, or order time regressed.
    #[error("Kraken level-3 timestamp regressed")]
    TimestampRegression,
    /// Provider checksum does not match the complete candidate state.
    #[error("Kraken level-3 checksum mismatch: expected {expected}, computed {computed}")]
    ChecksumMismatch {
        /// Provider checksum.
        expected: u32,
        /// Locally computed checksum.
        computed: u32,
    },
    /// The local diagnostic ordinal cannot advance.
    #[error("Kraken level-3 local generation ordinal overflow")]
    OrdinalOverflow,
    /// A bounded allocation could not be reserved.
    #[error("Kraken level-3 bounded allocation failed")]
    Allocation,
}
