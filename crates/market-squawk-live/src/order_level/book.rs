use std::cmp::Ordering;

use market_squawk_domain::{
    ChecksumEvidence, ChecksumIntegrity, DataQuality, MarketDepth, PriceTicks, QuantityLots,
    SequenceCapability, SequenceEvidence, SequenceIntegrity, SequenceNumber,
    SequenceValidationRule, SourceIdentifier, Timestamp,
};
use market_squawk_sources::{MarketFreshness, ProviderOrderChangeReason};
use thiserror::Error;

use super::batch::{
    OrderLevelBatch, OrderLevelBatchError, OrderLevelBatchInput, OrderLevelBatchPayload,
};
use super::model::{
    OrderLevelBatchKind, OrderLevelDeleteQuantity, OrderLevelEvent, OrderLevelLimits,
    OrderLevelOperation, OrderLevelPhase, OrderLevelPriority, OrderLevelPriorityUpdate,
    OrderLevelQuarantineReason, OrderLevelRoute, OrderLevelVisibleOrder, UnknownOrderDisposition,
};
use crate::BookSide;

const INITIAL_ORDER_CAPACITY: usize = 1_024;

/// One retained provider order with generation-local read-model coordinates.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OrderLevelEntry {
    order_id: SourceIdentifier,
    side: BookSide,
    price: PriceTicks,
    quantity: QuantityLots,
    provider_order_timestamp: Option<Timestamp>,
    provider_priority: Option<OrderLevelPriority>,
    first_seen_in: OrderLevelBatchKind,
    last_updated_in: OrderLevelBatchKind,
    last_source_timestamp: Timestamp,
    last_received_at: Timestamp,
    arrival_ordinal: u64,
}

impl OrderLevelEntry {
    /// Returns the stable provider order identity.
    pub const fn order_id(&self) -> &SourceIdentifier {
        &self.order_id
    }

    /// Returns bid or ask side.
    pub const fn side(&self) -> BookSide {
        self.side
    }

    /// Returns exact instrument ticks.
    pub const fn price(&self) -> PriceTicks {
        self.price
    }

    /// Returns exact remaining lots.
    pub const fn quantity(&self) -> QuantityLots {
        self.quantity
    }

    /// Returns provider order time when the feed supplied it.
    pub const fn provider_order_timestamp(&self) -> Option<Timestamp> {
        self.provider_order_timestamp
    }

    /// Returns exact provider-defined priority when supplied.
    pub const fn provider_priority(&self) -> Option<&OrderLevelPriority> {
        self.provider_priority.as_ref()
    }

    /// Returns whether the order first appeared in a snapshot or incremental event.
    pub const fn first_seen_in(&self) -> OrderLevelBatchKind {
        self.first_seen_in
    }

    /// Returns whether the most recent mutation was part of snapshot handoff or an update.
    pub const fn last_updated_in(&self) -> OrderLevelBatchKind {
        self.last_updated_in
    }

    /// Returns the most recent provider event time affecting this order.
    pub const fn last_source_timestamp(&self) -> Timestamp {
        self.last_source_timestamp
    }

    /// Returns the most recent local receive time affecting this order.
    pub const fn last_received_at(&self) -> Timestamp {
        self.last_received_at
    }

    /// Returns deterministic generation-local arrival order.
    ///
    /// This is presentation ordering only and is never provider queue priority or sequence.
    pub const fn arrival_ordinal(&self) -> u64 {
        self.arrival_ordinal
    }

    fn from_visible(
        order: OrderLevelVisibleOrder,
        batch_kind: OrderLevelBatchKind,
        source_timestamp: Timestamp,
        received_at: Timestamp,
        arrival_ordinal: u64,
    ) -> Self {
        let (order_id, side, price, quantity, provider_order_timestamp, provider_priority) =
            order.into_parts();
        Self {
            order_id,
            side,
            price,
            quantity,
            provider_order_timestamp,
            provider_priority,
            first_seen_in: batch_kind,
            last_updated_in: batch_kind,
            last_source_timestamp: source_timestamp,
            last_received_at: received_at,
            arrival_ordinal,
        }
    }

    fn try_clone_fallible(&self) -> Result<Self, OrderLevelBookError> {
        Ok(Self {
            order_id: try_clone_identifier(&self.order_id)?,
            side: self.side,
            price: self.price,
            quantity: self.quantity,
            provider_order_timestamp: self.provider_order_timestamp,
            provider_priority: self
                .provider_priority
                .as_ref()
                .map(try_clone_priority)
                .transpose()?,
            first_seen_in: self.first_seen_in,
            last_updated_in: self.last_updated_in,
            last_source_timestamp: self.last_source_timestamp,
            last_received_at: self.last_received_at,
            arrival_ordinal: self.arrival_ordinal,
        })
    }
}

/// One deterministic aggregate derived from the order-level source of truth.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PriceLevelProjection {
    side: BookSide,
    price: PriceTicks,
    quantity: QuantityLots,
    order_count: u32,
}

impl PriceLevelProjection {
    /// Returns bid or ask side.
    pub const fn side(self) -> BookSide {
        self.side
    }

    /// Returns the exact normalized price.
    pub const fn price(self) -> PriceTicks {
        self.price
    }

    /// Returns checked aggregate quantity.
    pub const fn quantity(self) -> QuantityLots {
        self.quantity
    }

    /// Returns the number of distinct provider orders at this price.
    pub const fn order_count(self) -> u32 {
        self.order_count
    }
}

/// Owned source-preserving price-level projection for legacy/read-only consumers.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OrderLevelPriceProjection {
    route: OrderLevelRoute,
    batch_identifier: SourceIdentifier,
    revision: u64,
    phase: OrderLevelPhase,
    quality: DataQuality,
    freshness: MarketFreshness,
    source_timestamp: Timestamp,
    received_at: Timestamp,
    available_at: Timestamp,
    provider_sequence: Option<SequenceNumber>,
    diagnostic_ordinal: Option<u64>,
    sequence: SequenceEvidence,
    checksum: ChecksumEvidence,
    bids: Vec<PriceLevelProjection>,
    asks: Vec<PriceLevelProjection>,
}

impl OrderLevelPriceProjection {
    /// Returns the source route from which every level was derived.
    pub const fn route(&self) -> &OrderLevelRoute {
        &self.route
    }

    /// Returns the exact provider batch/frame reference backing this projection.
    pub const fn batch_identifier(&self) -> &SourceIdentifier {
        &self.batch_identifier
    }

    /// Returns the order-level revision projected into this immutable view.
    pub const fn revision(&self) -> u64 {
        self.revision
    }

    /// Returns source state, including quarantine when applicable.
    pub const fn phase(&self) -> OrderLevelPhase {
        self.phase
    }

    /// Returns the effective display quality; this projection never grants execution authority.
    pub const fn quality(&self) -> DataQuality {
        self.quality
    }

    /// Returns market-event freshness independent of heartbeat liveness.
    pub const fn freshness(&self) -> MarketFreshness {
        self.freshness
    }

    /// Returns the latest provider timestamp.
    pub const fn source_timestamp(&self) -> Timestamp {
        self.source_timestamp
    }

    /// Returns the latest local receive time.
    pub const fn received_at(&self) -> Timestamp {
        self.received_at
    }

    /// Returns when this committed order-level state became available to local readers.
    pub const fn available_at(&self) -> Timestamp {
        self.available_at
    }

    /// Returns the last provider sequence, never a local ordinal.
    pub const fn provider_sequence(&self) -> Option<SequenceNumber> {
        self.provider_sequence
    }

    /// Returns a local diagnostic ordinal with no provider-sequence authority.
    pub const fn diagnostic_ordinal(&self) -> Option<u64> {
        self.diagnostic_ordinal
    }

    /// Returns complete sequence capability evidence.
    pub const fn sequence_evidence(&self) -> &SequenceEvidence {
        &self.sequence
    }

    /// Returns complete checksum capability evidence.
    pub const fn checksum_evidence(&self) -> &ChecksumEvidence {
        &self.checksum
    }

    /// Returns explicit price-level depth for this derived view.
    pub const fn market_depth(&self) -> MarketDepth {
        MarketDepth::PriceLevel
    }

    /// Returns bid levels in strict best-to-worst order.
    pub fn bids(&self) -> &[PriceLevelProjection] {
        &self.bids
    }

    /// Returns ask levels in strict best-to-worst order.
    pub fn asks(&self) -> &[PriceLevelProjection] {
        &self.asks
    }
}

/// Receipt for one atomically committed snapshot or update transaction.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OrderLevelCommit {
    revision: u64,
    kind: OrderLevelBatchKind,
    order_count: usize,
    provider_sequence: Option<SequenceNumber>,
    diagnostic_ordinal: Option<u64>,
    quality: DataQuality,
    available_at: Timestamp,
}

impl OrderLevelCommit {
    /// Returns the newly committed read-model revision.
    pub const fn revision(self) -> u64 {
        self.revision
    }

    /// Returns snapshot or update semantics.
    pub const fn kind(self) -> OrderLevelBatchKind {
        self.kind
    }

    /// Returns retained individual-order count.
    pub const fn order_count(self) -> usize {
        self.order_count
    }

    /// Returns the committed provider sequence when supported.
    pub const fn provider_sequence(self) -> Option<SequenceNumber> {
        self.provider_sequence
    }

    /// Returns the diagnostic ordinal when the provider supplies no sequence.
    pub const fn diagnostic_ordinal(self) -> Option<u64> {
        self.diagnostic_ordinal
    }

    /// Returns effective display quality, never execution authority.
    pub const fn quality(self) -> DataQuality {
        self.quality
    }

    /// Returns when the committed state became available to local readers.
    pub const fn available_at(self) -> Timestamp {
        self.available_at
    }
}

/// Bounded generation-owned order-level source of truth.
///
/// Mutation requires `&mut self`; no internal lock or alternate writer exists. Each batch is
/// applied to an unpublished, fallibly allocated candidate and becomes visible only after all
/// route, sequence, checksum, mutation, depth, arithmetic, and crossed-book checks pass.
#[derive(Debug)]
pub struct OrderLevelBook {
    route: OrderLevelRoute,
    limits: OrderLevelLimits,
    phase: OrderLevelPhase,
    revision: u64,
    orders: Vec<OrderLevelEntry>,
    next_arrival_ordinal: u64,
    sequence_rule: Option<SequenceValidationRule>,
    snapshot_provider_sequence: Option<SequenceNumber>,
    provider_sequence: Option<SequenceNumber>,
    diagnostic_ordinal: Option<u64>,
    sequence: Option<SequenceEvidence>,
    checksum: Option<ChecksumEvidence>,
    freshness: MarketFreshness,
    source_timestamp: Option<Timestamp>,
    received_at: Option<Timestamp>,
    available_at: Option<Timestamp>,
    last_batch_identifier: Option<SourceIdentifier>,
}

impl OrderLevelBook {
    /// Allocates one bounded empty generation owner before source publication begins.
    ///
    /// # Errors
    ///
    /// Returns an allocation failure without creating partially admitted state.
    pub fn try_new(
        route: OrderLevelRoute,
        limits: OrderLevelLimits,
    ) -> Result<Self, OrderLevelBookError> {
        let mut orders = Vec::new();
        orders
            .try_reserve_exact(limits.max_orders().min(INITIAL_ORDER_CAPACITY))
            .map_err(|_| OrderLevelBookError::Allocation)?;
        Ok(Self {
            route,
            limits,
            phase: OrderLevelPhase::AwaitingSnapshot,
            revision: 0,
            orders,
            next_arrival_ordinal: 1,
            sequence_rule: None,
            snapshot_provider_sequence: None,
            provider_sequence: None,
            diagnostic_ordinal: None,
            sequence: None,
            checksum: None,
            freshness: MarketFreshness::Uninitialized,
            source_timestamp: None,
            received_at: None,
            available_at: None,
            last_batch_identifier: None,
        })
    }

    /// Constructs and applies one batch under this book's quarantine authority.
    pub fn apply_input(
        &mut self,
        input: OrderLevelBatchInput,
    ) -> Result<OrderLevelCommit, OrderLevelBookError> {
        let batch = OrderLevelBatch::try_new(input).map_err(|error| {
            self.isolate(OrderLevelQuarantineReason::Mutation);
            OrderLevelBookError::Batch(error)
        })?;
        self.apply(batch)
    }

    /// Atomically applies one checked provider transaction.
    ///
    /// A rejected checksum, sequence, mutation, allocation, arithmetic, or crossed candidate does
    /// not change retained orders. The generation is quarantined before the error returns.
    pub fn apply(
        &mut self,
        batch: OrderLevelBatch,
    ) -> Result<OrderLevelCommit, OrderLevelBookError> {
        match self.apply_inner(batch) {
            Ok(commit) => Ok(commit),
            Err((reason, error)) => {
                self.isolate(reason);
                Err(error)
            }
        }
    }

    /// Clears isolated state and requires a fresh complete snapshot for this generation.
    ///
    /// Existing vector capacity is retained; no new writer or provider sequence is synthesized.
    pub fn begin_resnapshot(&mut self) -> Result<u64, OrderLevelBookError> {
        let revision = self
            .revision
            .checked_add(1)
            .ok_or(OrderLevelBookError::RevisionOverflow)?;
        self.orders.clear();
        self.phase = OrderLevelPhase::AwaitingSnapshot;
        self.revision = revision;
        self.next_arrival_ordinal = 1;
        self.sequence_rule = None;
        self.snapshot_provider_sequence = None;
        self.provider_sequence = None;
        self.diagnostic_ordinal = None;
        self.sequence = None;
        self.checksum = None;
        self.freshness = MarketFreshness::Uninitialized;
        self.source_timestamp = None;
        self.received_at = None;
        self.available_at = None;
        self.last_batch_identifier = None;
        Ok(revision)
    }

    /// Explicitly quarantines the current generation while retaining last-known state for review.
    pub fn quarantine(&mut self, reason: OrderLevelQuarantineReason) {
        self.isolate(reason);
    }

    /// Marks the latest market-bearing observation stale without changing connection health.
    pub fn mark_stale(&mut self) -> Result<u64, OrderLevelBookError> {
        let last_market_at = self
            .received_at
            .ok_or(OrderLevelBookError::SnapshotRequired)?;
        let revision = self
            .revision
            .checked_add(1)
            .ok_or(OrderLevelBookError::RevisionOverflow)?;
        self.freshness = MarketFreshness::Stale { last_market_at };
        self.revision = revision;
        Ok(revision)
    }

    /// Returns the immutable route owning this book.
    pub const fn route(&self) -> &OrderLevelRoute {
        &self.route
    }

    /// Returns configured resource bounds.
    pub const fn limits(&self) -> OrderLevelLimits {
        self.limits
    }

    /// Returns current synchronization/quarantine phase.
    pub const fn phase(&self) -> OrderLevelPhase {
        self.phase
    }

    /// Returns monotonically increasing read-model revision.
    pub const fn revision(&self) -> u64 {
        self.revision
    }

    /// Returns retained individual orders in stable provider-order-ID order.
    pub fn orders(&self) -> &[OrderLevelEntry] {
        &self.orders
    }

    /// Returns an allocation-checked bounded owned order page for an asynchronous reader.
    pub fn try_owned_orders(
        &self,
        maximum_orders: usize,
    ) -> Result<Vec<OrderLevelEntry>, OrderLevelBookError> {
        if maximum_orders == 0 || maximum_orders > self.limits.max_orders() {
            return Err(OrderLevelBookError::InvalidReadLimit);
        }
        let count = self.orders.len().min(maximum_orders);
        let mut orders = Vec::new();
        orders
            .try_reserve_exact(count)
            .map_err(|_error| OrderLevelBookError::Allocation)?;
        for order in self.orders.iter().take(count) {
            orders.push(order.try_clone_fallible()?);
        }
        Ok(orders)
    }

    /// Returns one order by exact provider identity.
    pub fn order(&self, order_id: &SourceIdentifier) -> Option<&OrderLevelEntry> {
        self.find(order_id).ok().map(|index| &self.orders[index])
    }

    /// Returns the last atomically committed provider batch/frame reference.
    pub const fn last_batch_identifier(&self) -> Option<&SourceIdentifier> {
        self.last_batch_identifier.as_ref()
    }

    /// Returns explicit source depth without implying execution quality.
    pub const fn market_depth(&self) -> MarketDepth {
        MarketDepth::OrderLevel
    }

    /// Returns the effective display classification.
    pub const fn quality(&self) -> DataQuality {
        match self.phase {
            OrderLevelPhase::Quarantined(_) => DataQuality::Quarantined,
            OrderLevelPhase::AwaitingSnapshot | OrderLevelPhase::Healthy => match self.freshness {
                MarketFreshness::Stale { .. } => DataQuality::Stale,
                MarketFreshness::Uninitialized | MarketFreshness::Fresh { .. } => {
                    DataQuality::DirectUnverified
                }
            },
        }
    }

    /// Returns market freshness independent of heartbeat liveness.
    pub const fn freshness(&self) -> MarketFreshness {
        self.freshness
    }

    /// Returns when the latest atomically committed state became available to local readers.
    pub const fn available_at(&self) -> Option<Timestamp> {
        self.available_at
    }

    /// Derives a deterministic checked price-level view without changing order-level state.
    ///
    /// Duplicate-price provider orders remain distinct in [`Self::orders`]. This method sums them
    /// only in a separately typed `MarketDepth::PriceLevel` projection carrying the original
    /// route, generation, integrity, quality, and freshness evidence.
    pub fn project_price_levels(
        &self,
    ) -> Result<OrderLevelPriceProjection, OrderLevelProjectionError> {
        if self.orders.is_empty() || self.phase == OrderLevelPhase::AwaitingSnapshot {
            return Err(OrderLevelProjectionError::Unavailable);
        }
        let source_timestamp = self
            .source_timestamp
            .ok_or(OrderLevelProjectionError::Unavailable)?;
        let received_at = self
            .received_at
            .ok_or(OrderLevelProjectionError::Unavailable)?;
        let available_at = self
            .available_at
            .ok_or(OrderLevelProjectionError::Unavailable)?;
        let batch_identifier = self
            .last_batch_identifier
            .as_ref()
            .ok_or(OrderLevelProjectionError::Unavailable)?
            .clone();
        let sequence = self
            .sequence
            .as_ref()
            .ok_or(OrderLevelProjectionError::Unavailable)?
            .clone();
        let checksum = self
            .checksum
            .as_ref()
            .ok_or(OrderLevelProjectionError::Unavailable)?
            .clone();
        let (bids, asks) = project_orders(&self.orders, self.limits)?;
        Ok(OrderLevelPriceProjection {
            route: self.route.clone(),
            batch_identifier,
            revision: self.revision,
            phase: self.phase,
            quality: self.quality(),
            freshness: self.freshness,
            source_timestamp,
            received_at,
            available_at,
            provider_sequence: self.provider_sequence,
            diagnostic_ordinal: self.diagnostic_ordinal,
            sequence,
            checksum,
            bids,
            asks,
        })
    }

    fn apply_inner(
        &mut self,
        batch: OrderLevelBatch,
    ) -> Result<OrderLevelCommit, (OrderLevelQuarantineReason, OrderLevelBookError)> {
        if batch.route != self.route {
            return Err((
                OrderLevelQuarantineReason::RouteMismatch,
                OrderLevelBookError::RouteMismatch,
            ));
        }
        if batch.payload.kind() == OrderLevelBatchKind::Update
            && self.phase != OrderLevelPhase::Healthy
        {
            return Err((
                OrderLevelQuarantineReason::Snapshot,
                OrderLevelBookError::SnapshotRequired,
            ));
        }
        validate_integrity(&batch)?;
        self.validate_cursor(&batch)?;
        let next_revision = self.revision.checked_add(1).ok_or((
            OrderLevelQuarantineReason::Resource,
            OrderLevelBookError::RevisionOverflow,
        ))?;
        let kind = batch.payload.kind();
        let additional_orders = maximum_new_orders(batch.payload.events())?;
        let mut next_arrival = if kind == OrderLevelBatchKind::Snapshot {
            1
        } else {
            self.next_arrival_ordinal
        };
        let candidate = match batch.payload {
            OrderLevelBatchPayload::Snapshot {
                snapshot_source_timestamp,
                snapshot_received_at,
                orders,
                replay,
            } => {
                let mut candidate = self.snapshot_candidate(
                    orders,
                    snapshot_source_timestamp,
                    snapshot_received_at,
                    additional_orders,
                    &mut next_arrival,
                )?;
                apply_events(&mut candidate, replay, self.limits, &mut next_arrival)?;
                candidate
            }
            OrderLevelBatchPayload::Update { events } => {
                let mut candidate = try_clone_orders(&self.orders, self.limits, additional_orders)?;
                apply_events(&mut candidate, events, self.limits, &mut next_arrival)?;
                candidate
            }
        };
        validate_candidate(&candidate, self.limits)?;
        let provider_sequence = batch.sequence.observed_sequence();
        let snapshot_provider_sequence = if kind == OrderLevelBatchKind::Snapshot {
            batch.sequence.snapshot_sequence()
        } else {
            self.snapshot_provider_sequence
        };
        self.orders = candidate;
        self.phase = OrderLevelPhase::Healthy;
        self.revision = next_revision;
        self.next_arrival_ordinal = next_arrival;
        self.sequence_rule = batch.sequence_rule;
        self.snapshot_provider_sequence = snapshot_provider_sequence;
        self.provider_sequence = provider_sequence;
        self.diagnostic_ordinal = batch.diagnostic_ordinal;
        self.sequence = Some(batch.sequence);
        self.checksum = Some(batch.checksum);
        self.freshness = batch.freshness;
        self.source_timestamp = Some(batch.source_timestamp);
        self.received_at = Some(batch.received_at);
        self.available_at = Some(batch.available_at);
        self.last_batch_identifier = Some(batch.batch_identifier);
        Ok(OrderLevelCommit {
            revision: next_revision,
            kind,
            order_count: self.orders.len(),
            provider_sequence,
            diagnostic_ordinal: self.diagnostic_ordinal,
            quality: self.quality(),
            available_at: batch.available_at,
        })
    }

    fn validate_cursor(
        &self,
        batch: &OrderLevelBatch,
    ) -> Result<(), (OrderLevelQuarantineReason, OrderLevelBookError)> {
        if self
            .source_timestamp
            .is_some_and(|previous| batch.source_timestamp < previous)
            || self
                .received_at
                .is_some_and(|previous| batch.received_at < previous)
            || self
                .available_at
                .is_some_and(|previous| batch.available_at < previous)
        {
            return Err((
                OrderLevelQuarantineReason::Mutation,
                OrderLevelBookError::TimestampRegression,
            ));
        }
        match batch.sequence.capability() {
            SequenceCapability::Provided => {
                if batch.payload.kind() == OrderLevelBatchKind::Update {
                    if self.sequence_rule != batch.sequence_rule
                        || self.snapshot_provider_sequence != batch.sequence.snapshot_sequence()
                    {
                        return Err((
                            OrderLevelQuarantineReason::Sequence,
                            OrderLevelBookError::SequenceStateMismatch,
                        ));
                    }
                    let first = batch
                        .payload
                        .events()
                        .first()
                        .and_then(OrderLevelEvent::provider_sequence)
                        .ok_or((
                            OrderLevelQuarantineReason::Sequence,
                            OrderLevelBookError::SequenceStateMismatch,
                        ))?;
                    let previous = self.provider_sequence.ok_or((
                        OrderLevelQuarantineReason::Sequence,
                        OrderLevelBookError::SequenceStateMismatch,
                    ))?;
                    let rule = batch.sequence_rule.ok_or((
                        OrderLevelQuarantineReason::Sequence,
                        OrderLevelBookError::SequenceStateMismatch,
                    ))?;
                    validate_sequence_progression(previous, first, rule)?;
                    let expected_evidence_previous = if batch.payload.events().len() == 1 {
                        Some(previous)
                    } else {
                        batch
                            .payload
                            .events()
                            .get(batch.payload.events().len() - 2)
                            .and_then(OrderLevelEvent::provider_sequence)
                    };
                    if batch.sequence.previous_sequence() != expected_evidence_previous {
                        return Err((
                            OrderLevelQuarantineReason::Sequence,
                            OrderLevelBookError::SequenceStateMismatch,
                        ));
                    }
                }
            }
            SequenceCapability::Unsupported => {
                if batch.payload.kind() == OrderLevelBatchKind::Update {
                    let valid = match (self.diagnostic_ordinal, batch.diagnostic_ordinal) {
                        (Some(previous), Some(observed)) => {
                            previous.checked_add(1) == Some(observed)
                        }
                        (None, None) => true,
                        (Some(_), None) | (None, Some(_)) => false,
                    };
                    if !valid {
                        return Err((
                            OrderLevelQuarantineReason::Sequence,
                            OrderLevelBookError::DiagnosticOrdinalMismatch,
                        ));
                    }
                }
            }
        }
        Ok(())
    }

    fn snapshot_candidate(
        &self,
        orders: Vec<OrderLevelVisibleOrder>,
        source_timestamp: Timestamp,
        received_at: Timestamp,
        additional_orders: usize,
        next_arrival: &mut u64,
    ) -> Result<Vec<OrderLevelEntry>, (OrderLevelQuarantineReason, OrderLevelBookError)> {
        if orders.len() > self.limits.max_orders() {
            return Err((
                OrderLevelQuarantineReason::Book,
                OrderLevelBookError::OrderCapacityExceeded,
            ));
        }
        let capacity = candidate_capacity(orders.len(), additional_orders, self.limits)?;
        let mut candidate = Vec::new();
        candidate.try_reserve_exact(capacity).map_err(|_| {
            (
                OrderLevelQuarantineReason::Resource,
                OrderLevelBookError::Allocation,
            )
        })?;
        for order in orders {
            let arrival = take_arrival_ordinal(next_arrival)?;
            candidate.push(OrderLevelEntry::from_visible(
                order,
                OrderLevelBatchKind::Snapshot,
                source_timestamp,
                received_at,
                arrival,
            ));
        }
        candidate.sort_by(|left, right| left.order_id.cmp(&right.order_id));
        if candidate
            .windows(2)
            .any(|pair| pair[0].order_id == pair[1].order_id)
        {
            return Err((
                OrderLevelQuarantineReason::Mutation,
                OrderLevelBookError::DuplicateOrder,
            ));
        }
        Ok(candidate)
    }

    fn find(&self, order_id: &SourceIdentifier) -> Result<usize, usize> {
        self.orders
            .binary_search_by(|order| order.order_id.cmp(order_id))
    }

    fn isolate(&mut self, reason: OrderLevelQuarantineReason) {
        self.phase = OrderLevelPhase::Quarantined(reason);
        if let Some(next) = self.revision.checked_add(1) {
            self.revision = next;
        }
    }
}

fn validate_integrity(
    batch: &OrderLevelBatch,
) -> Result<(), (OrderLevelQuarantineReason, OrderLevelBookError)> {
    match batch.sequence.integrity() {
        SequenceIntegrity::Valid | SequenceIntegrity::NotSupported => {}
        SequenceIntegrity::Invalid | SequenceIntegrity::Uninitialized => {
            return Err((
                OrderLevelQuarantineReason::Sequence,
                OrderLevelBookError::SequenceIntegrity,
            ));
        }
    }
    match batch.checksum.integrity() {
        ChecksumIntegrity::Valid | ChecksumIntegrity::NotSupported => Ok(()),
        ChecksumIntegrity::Failed | ChecksumIntegrity::Unchecked => Err((
            OrderLevelQuarantineReason::Checksum,
            OrderLevelBookError::ChecksumIntegrity,
        )),
    }
}

fn validate_sequence_progression(
    previous: SequenceNumber,
    observed: SequenceNumber,
    rule: SequenceValidationRule,
) -> Result<(), (OrderLevelQuarantineReason, OrderLevelBookError)> {
    let valid = match rule {
        SequenceValidationRule::Consecutive => previous
            .checked_next()
            .is_ok_and(|expected| expected == observed),
        SequenceValidationRule::Monotonic => observed > previous,
    };
    if valid {
        Ok(())
    } else {
        Err((
            OrderLevelQuarantineReason::Sequence,
            OrderLevelBookError::SequenceStateMismatch,
        ))
    }
}

fn try_clone_orders(
    orders: &[OrderLevelEntry],
    limits: OrderLevelLimits,
    additional_orders: usize,
) -> Result<Vec<OrderLevelEntry>, (OrderLevelQuarantineReason, OrderLevelBookError)> {
    let capacity = candidate_capacity(orders.len(), additional_orders, limits)?;
    let mut candidate = Vec::new();
    candidate.try_reserve_exact(capacity).map_err(|_| {
        (
            OrderLevelQuarantineReason::Resource,
            OrderLevelBookError::Allocation,
        )
    })?;
    for order in orders {
        candidate.push(
            order
                .try_clone_fallible()
                .map_err(|error| (OrderLevelQuarantineReason::Resource, error))?,
        );
    }
    Ok(candidate)
}

fn maximum_new_orders(
    events: &[OrderLevelEvent],
) -> Result<usize, (OrderLevelQuarantineReason, OrderLevelBookError)> {
    events
        .iter()
        .flat_map(OrderLevelEvent::operations)
        .try_fold(0_usize, |count, operation| {
            if matches!(operation, OrderLevelOperation::Open(_)) {
                count.checked_add(1).ok_or((
                    OrderLevelQuarantineReason::Resource,
                    OrderLevelBookError::NumericOverflow,
                ))
            } else {
                Ok(count)
            }
        })
}

fn candidate_capacity(
    retained: usize,
    additional_orders: usize,
    limits: OrderLevelLimits,
) -> Result<usize, (OrderLevelQuarantineReason, OrderLevelBookError)> {
    retained
        .checked_add(additional_orders)
        .map(|required| required.min(limits.max_orders()))
        .ok_or((
            OrderLevelQuarantineReason::Resource,
            OrderLevelBookError::NumericOverflow,
        ))
}

fn apply_events(
    orders: &mut Vec<OrderLevelEntry>,
    events: Vec<OrderLevelEvent>,
    limits: OrderLevelLimits,
    next_arrival: &mut u64,
) -> Result<(), (OrderLevelQuarantineReason, OrderLevelBookError)> {
    for event in events {
        let (_sequence, _diagnostic, source_timestamp, received_at, operations) =
            event.into_parts();
        for operation in operations {
            apply_operation(
                orders,
                operation,
                source_timestamp,
                received_at,
                limits,
                next_arrival,
            )?;
        }
    }
    Ok(())
}

fn apply_operation(
    orders: &mut Vec<OrderLevelEntry>,
    operation: OrderLevelOperation,
    source_timestamp: Timestamp,
    received_at: Timestamp,
    limits: OrderLevelLimits,
    next_arrival: &mut u64,
) -> Result<(), (OrderLevelQuarantineReason, OrderLevelBookError)> {
    match operation {
        OrderLevelOperation::CursorOnly => Ok(()),
        OrderLevelOperation::Open(order) => {
            let index = find_in(orders, order.order_id());
            let insertion = match index {
                Ok(_) => return mutation_error(OrderLevelBookError::DuplicateOrder),
                Err(index) => index,
            };
            if orders.len() == limits.max_orders() {
                return book_error(OrderLevelBookError::OrderCapacityExceeded);
            }
            let arrival = take_arrival_ordinal(next_arrival)?;
            orders.insert(
                insertion,
                OrderLevelEntry::from_visible(
                    order,
                    OrderLevelBatchKind::Update,
                    source_timestamp,
                    received_at,
                    arrival,
                ),
            );
            Ok(())
        }
        OrderLevelOperation::Match {
            order_id,
            side,
            price,
            quantity,
        } => {
            if quantity.get() == 0 {
                return mutation_error(OrderLevelBookError::InvalidQuantity);
            }
            let index = find_in(orders, &order_id)
                .map_err(|_| mutation_pair(OrderLevelBookError::UnknownOrder))?;
            let current = &orders[index];
            if current.side != side || current.price != price || quantity > current.quantity {
                return mutation_error(OrderLevelBookError::MutationMismatch);
            }
            if quantity == current.quantity {
                orders.remove(index);
            } else {
                let next = current
                    .quantity
                    .checked_sub(quantity)
                    .map_err(|_| mutation_pair(OrderLevelBookError::NumericOverflow))?;
                let current = &mut orders[index];
                current.quantity = next;
                current.last_updated_in = OrderLevelBatchKind::Update;
                current.last_source_timestamp = source_timestamp;
                current.last_received_at = received_at;
            }
            Ok(())
        }
        OrderLevelOperation::Done {
            order_id,
            side,
            price,
            quantity,
            provider_order_timestamp,
            unknown_order,
        } => {
            let index = match find_in(orders, &order_id) {
                Ok(index) => index,
                Err(_) if unknown_order == UnknownOrderDisposition::CursorOnly => return Ok(()),
                Err(_) => return mutation_error(OrderLevelBookError::UnknownOrder),
            };
            let current = &orders[index];
            if side.is_some_and(|expected| expected != current.side)
                || price.is_some_and(|expected| expected != current.price)
                || provider_order_timestamp
                    .zip(current.provider_order_timestamp)
                    .is_some_and(|(observed, prior)| observed < prior)
            {
                return mutation_error(OrderLevelBookError::MutationMismatch);
            }
            match quantity {
                OrderLevelDeleteQuantity::ExactRemaining(expected)
                    if expected == current.quantity => {}
                OrderLevelDeleteQuantity::ZeroMeansDelete => {}
                OrderLevelDeleteQuantity::ExactRemaining(_)
                | OrderLevelDeleteQuantity::Unavailable => {
                    return mutation_error(OrderLevelBookError::MutationMismatch);
                }
            }
            orders.remove(index);
            Ok(())
        }
        OrderLevelOperation::Change {
            order_id,
            reason,
            side,
            previous_price,
            previous_quantity,
            new_price,
            new_quantity,
            provider_order_timestamp,
            priority,
            unknown_order,
        } => {
            let index = match find_in(orders, &order_id) {
                Ok(index) => index,
                Err(_) if unknown_order == UnknownOrderDisposition::CursorOnly => return Ok(()),
                Err(_) => return mutation_error(OrderLevelBookError::UnknownOrder),
            };
            let current = &orders[index];
            if current.side != side
                || previous_price.is_some_and(|expected| expected != current.price)
                || previous_quantity.is_some_and(|expected| expected != current.quantity)
                || provider_order_timestamp
                    .zip(current.provider_order_timestamp)
                    .is_some_and(|(observed, prior)| observed < prior)
            {
                return mutation_error(OrderLevelBookError::MutationMismatch);
            }
            validate_change_shape(
                reason,
                previous_price,
                previous_quantity,
                new_price,
                new_quantity,
            )?;
            let next_price = new_price.unwrap_or(current.price);
            let next_quantity = new_quantity.unwrap_or(current.quantity);
            if next_quantity.get() == 0 {
                return mutation_error(OrderLevelBookError::InvalidQuantity);
            }
            let current = &mut orders[index];
            current.price = next_price;
            current.quantity = next_quantity;
            if let Some(timestamp) = provider_order_timestamp {
                current.provider_order_timestamp = Some(timestamp);
            }
            match priority {
                OrderLevelPriorityUpdate::Preserve => {}
                OrderLevelPriorityUpdate::Replace(priority) => {
                    current.provider_priority = priority;
                }
            }
            current.last_updated_in = OrderLevelBatchKind::Update;
            current.last_source_timestamp = source_timestamp;
            current.last_received_at = received_at;
            Ok(())
        }
    }
}

fn validate_change_shape(
    reason: ProviderOrderChangeReason,
    previous_price: Option<PriceTicks>,
    previous_quantity: Option<QuantityLots>,
    new_price: Option<PriceTicks>,
    new_quantity: Option<QuantityLots>,
) -> Result<(), (OrderLevelQuarantineReason, OrderLevelBookError)> {
    let valid = match reason {
        ProviderOrderChangeReason::SelfTradePrevention => {
            matches!(
                (previous_price, previous_quantity, new_price, new_quantity),
                (Some(_), Some(previous), None, Some(next))
                    if next <= previous && next.get() > 0
            ) || matches!(
                (previous_price, previous_quantity, new_price, new_quantity),
                (None, None, None, None)
            )
        }
        ProviderOrderChangeReason::ModifyOrder => {
            matches!(
                (previous_price, previous_quantity, new_price, new_quantity),
                (Some(_), Some(_), Some(_), Some(quantity)) if quantity.get() > 0
            ) || matches!(
                (previous_price, previous_quantity, new_price, new_quantity),
                (None, None, Some(_), Some(quantity)) if quantity.get() > 0
            )
        }
    };
    if valid {
        Ok(())
    } else {
        mutation_error(OrderLevelBookError::MutationMismatch)
    }
}

fn validate_candidate(
    orders: &[OrderLevelEntry],
    limits: OrderLevelLimits,
) -> Result<(), (OrderLevelQuarantineReason, OrderLevelBookError)> {
    if orders.len() > limits.max_orders() {
        return book_error(OrderLevelBookError::OrderCapacityExceeded);
    }
    if orders
        .windows(2)
        .any(|pair| pair[0].order_id >= pair[1].order_id)
    {
        return mutation_error(OrderLevelBookError::DuplicateOrder);
    }
    if orders.iter().any(|order| order.quantity.get() == 0) {
        return mutation_error(OrderLevelBookError::InvalidQuantity);
    }
    let best_bid = orders
        .iter()
        .filter(|order| order.side == BookSide::Bid)
        .map(|order| order.price)
        .max();
    let best_ask = orders
        .iter()
        .filter(|order| order.side == BookSide::Ask)
        .map(|order| order.price)
        .min();
    if best_bid.zip(best_ask).is_some_and(|(bid, ask)| bid >= ask) {
        return book_error(OrderLevelBookError::CrossedBook);
    }
    Ok(())
}

fn project_orders(
    orders: &[OrderLevelEntry],
    limits: OrderLevelLimits,
) -> Result<(Vec<PriceLevelProjection>, Vec<PriceLevelProjection>), OrderLevelProjectionError> {
    let mut sorted = Vec::new();
    sorted
        .try_reserve_exact(orders.len())
        .map_err(|_| OrderLevelProjectionError::Allocation)?;
    sorted.extend(
        orders
            .iter()
            .map(|order| (order.side, order.price, order.quantity)),
    );
    sorted
        .sort_unstable_by(|left, right| compare_side_price(&(left.0, left.1), &(right.0, right.1)));

    let mut aggregate = Vec::<PriceLevelProjection>::new();
    aggregate
        .try_reserve_exact(orders.len())
        .map_err(|_| OrderLevelProjectionError::Allocation)?;
    for (side, price, quantity) in sorted {
        if let Some(level) = aggregate
            .last_mut()
            .filter(|level| level.side == side && level.price == price)
        {
            let quantity = level
                .quantity
                .get()
                .checked_add(quantity.get())
                .and_then(|value| QuantityLots::new(value).ok())
                .ok_or(OrderLevelProjectionError::NumericOverflow)?;
            level.quantity = quantity;
            level.order_count = level
                .order_count
                .checked_add(1)
                .ok_or(OrderLevelProjectionError::NumericOverflow)?;
        } else {
            aggregate.push(PriceLevelProjection {
                side,
                price,
                quantity,
                order_count: 1,
            });
        }
    }
    let depth = limits.price_level_depth().get();
    let mut bids = Vec::new();
    bids.try_reserve_exact(depth.min(aggregate.len()))
        .map_err(|_| OrderLevelProjectionError::Allocation)?;
    let mut asks = Vec::new();
    asks.try_reserve_exact(depth.min(aggregate.len()))
        .map_err(|_| OrderLevelProjectionError::Allocation)?;
    for level in aggregate {
        match level.side {
            BookSide::Bid => bids.push(level),
            BookSide::Ask => asks.push(level),
        }
    }
    bids.sort_unstable_by_key(|level| std::cmp::Reverse(level.price));
    asks.sort_unstable_by_key(|level| level.price);
    bids.truncate(depth);
    asks.truncate(depth);
    Ok((bids, asks))
}

fn compare_side_price(left: &(BookSide, PriceTicks), right: &(BookSide, PriceTicks)) -> Ordering {
    side_rank(left.0)
        .cmp(&side_rank(right.0))
        .then_with(|| left.1.cmp(&right.1))
}

const fn side_rank(side: BookSide) -> u8 {
    match side {
        BookSide::Bid => 0,
        BookSide::Ask => 1,
    }
}

fn find_in(orders: &[OrderLevelEntry], order_id: &SourceIdentifier) -> Result<usize, usize> {
    orders.binary_search_by(|order| order.order_id.cmp(order_id))
}

fn take_arrival_ordinal(
    next: &mut u64,
) -> Result<u64, (OrderLevelQuarantineReason, OrderLevelBookError)> {
    let current = *next;
    *next = (*next).checked_add(1).ok_or((
        OrderLevelQuarantineReason::Resource,
        OrderLevelBookError::ArrivalOrdinalOverflow,
    ))?;
    Ok(current)
}

fn try_clone_identifier(value: &SourceIdentifier) -> Result<SourceIdentifier, OrderLevelBookError> {
    let mut clone = String::new();
    clone
        .try_reserve_exact(value.as_str().len())
        .map_err(|_| OrderLevelBookError::Allocation)?;
    clone.push_str(value.as_str());
    SourceIdentifier::try_from(clone).map_err(|_| OrderLevelBookError::IdentifierInvariant)
}

fn try_clone_priority(
    value: &OrderLevelPriority,
) -> Result<OrderLevelPriority, OrderLevelBookError> {
    Ok(OrderLevelPriority::new(
        value.value(),
        try_clone_identifier(value.rule())?,
    ))
}

fn mutation_pair(error: OrderLevelBookError) -> (OrderLevelQuarantineReason, OrderLevelBookError) {
    (OrderLevelQuarantineReason::Mutation, error)
}

fn mutation_error<T>(
    error: OrderLevelBookError,
) -> Result<T, (OrderLevelQuarantineReason, OrderLevelBookError)> {
    Err(mutation_pair(error))
}

fn book_error<T>(
    error: OrderLevelBookError,
) -> Result<T, (OrderLevelQuarantineReason, OrderLevelBookError)> {
    Err((OrderLevelQuarantineReason::Book, error))
}

/// Order-level state mutation failure.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum OrderLevelBookError {
    /// Batch construction failed before candidate publication.
    #[error("invalid order-level batch: {0}")]
    Batch(#[from] OrderLevelBatchError),
    /// Batch route did not match this generation owner.
    #[error("order-level batch route does not match the book owner")]
    RouteMismatch,
    /// An update arrived without a current healthy snapshot.
    #[error("order-level update requires a healthy initialized snapshot")]
    SnapshotRequired,
    /// Sequence integrity was invalid or incomplete.
    #[error("order-level sequence integrity failed")]
    SequenceIntegrity,
    /// Checksum integrity was failed or incomplete.
    #[error("order-level checksum integrity failed")]
    ChecksumIntegrity,
    /// Sequence rule, snapshot anchor, cursor, or evidence predecessor contradicted current state.
    #[error("order-level sequence state is inconsistent")]
    SequenceStateMismatch,
    /// A local diagnostic ordinal was absent, repeated, skipped, or regressed.
    #[error("order-level local diagnostic ordinal is inconsistent")]
    DiagnosticOrdinalMismatch,
    /// Provider or receive time regressed against the committed generation state.
    #[error("order-level market timestamp regressed")]
    TimestampRegression,
    /// Snapshot or update repeated a provider order identity.
    #[error("order-level provider order identity is duplicated")]
    DuplicateOrder,
    /// A required provider order was not retained.
    #[error("order-level mutation references an unknown order")]
    UnknownOrder,
    /// Provider side, price, quantity, timestamp, or reason contradicted retained state.
    #[error("order-level mutation contradicts retained state")]
    MutationMismatch,
    /// Visible quantity or decrement semantics were invalid.
    #[error("order-level quantity is invalid")]
    InvalidQuantity,
    /// Retained individual-order capacity was exhausted.
    #[error("order-level individual-order capacity exceeded")]
    OrderCapacityExceeded,
    /// A read requested zero orders or exceeded this book's admitted bound.
    #[error("order-level read limit is outside the admitted book bound")]
    InvalidReadLimit,
    /// Candidate best bid was at or above best ask.
    #[error("order-level candidate book is crossed")]
    CrossedBook,
    /// Checked quantity arithmetic failed.
    #[error("order-level numeric arithmetic overflowed")]
    NumericOverflow,
    /// A bounded candidate or evidence clone could not be allocated.
    #[error("order-level bounded allocation failed")]
    Allocation,
    /// Read-model revision cannot advance.
    #[error("order-level revision overflow")]
    RevisionOverflow,
    /// Deterministic local arrival ordering cannot advance.
    #[error("order-level arrival ordinal overflow")]
    ArrivalOrdinalOverflow,
    /// A validated bounded source identifier could not be reconstructed.
    #[error("order-level identifier invariant failed")]
    IdentifierInvariant,
}

/// Price-level projection failure.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum OrderLevelProjectionError {
    /// No initialized retained book can be projected.
    #[error("order-level projection is unavailable before snapshot initialization")]
    Unavailable,
    /// Projection scratch/output allocation failed.
    #[error("order-level projection allocation failed")]
    Allocation,
    /// Checked aggregate quantity or order count overflowed.
    #[error("order-level projection numeric overflow")]
    NumericOverflow,
}
