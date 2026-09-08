use market_squawk_domain::{
    ChecksumCapability, ChecksumEvidence, ChecksumTarget, DataQuality, MarketDepth,
    SequenceCapability, SequenceEvidence, SequenceIntegrity, SequenceNumber,
    SequenceValidationRule, SourceIdentifier, Timestamp,
};
use market_squawk_sources::{MAX_DECODED_BOOK_ITEMS, MarketFreshness};
use std::mem::size_of;
use thiserror::Error;

use super::model::{
    MAX_ORDER_LEVEL_ORDERS, OrderLevelBatchKind, OrderLevelEvent, OrderLevelRoute,
    OrderLevelVisibleOrder, max_batch_events,
};

/// Transaction payload applied atomically to one order-level book.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum OrderLevelBatchPayload {
    /// Complete retained snapshot plus a contiguous replay suffix captured during handoff.
    Snapshot {
        /// Provider time for the complete snapshot image.
        snapshot_source_timestamp: Timestamp,
        /// Local receive time for the complete snapshot image.
        snapshot_received_at: Timestamp,
        /// Individual orders in provider snapshot order.
        orders: Vec<OrderLevelVisibleOrder>,
        /// Provider events after the snapshot cursor and before publication.
        replay: Vec<OrderLevelEvent>,
    },
    /// One or more incremental provider events.
    Update {
        /// Events in exact provider wire order.
        events: Vec<OrderLevelEvent>,
    },
}

impl OrderLevelBatchPayload {
    /// Returns snapshot or incremental semantics.
    pub const fn kind(&self) -> OrderLevelBatchKind {
        match self {
            Self::Snapshot { .. } => OrderLevelBatchKind::Snapshot,
            Self::Update { .. } => OrderLevelBatchKind::Update,
        }
    }

    /// Returns the replay or incremental event sequence.
    pub fn events(&self) -> &[OrderLevelEvent] {
        match self {
            Self::Snapshot { replay, .. } => replay,
            Self::Update { events } => events,
        }
    }

    fn dynamic_retained_bytes(&self) -> Option<usize> {
        match self {
            Self::Snapshot { orders, replay, .. } => orders
                .capacity()
                .checked_mul(size_of::<OrderLevelVisibleOrder>())?
                .checked_add(orders.iter().try_fold(0_usize, |total, order| {
                    total.checked_add(order.dynamic_retained_bytes()?)
                })?)?
                .checked_add(
                    replay
                        .capacity()
                        .checked_mul(size_of::<OrderLevelEvent>())?,
                )?
                .checked_add(replay.iter().try_fold(0_usize, |total, event| {
                    total.checked_add(event.dynamic_retained_bytes()?)
                })?),
            Self::Update { events } => events
                .capacity()
                .checked_mul(size_of::<OrderLevelEvent>())?
                .checked_add(events.iter().try_fold(0_usize, |total, event| {
                    total.checked_add(event.dynamic_retained_bytes()?)
                })?),
        }
    }
}

/// Cohesive constructor input for one source-identity-preserving order-level transaction.
#[derive(Clone, Debug)]
pub struct OrderLevelBatchInput {
    route: OrderLevelRoute,
    batch_identifier: SourceIdentifier,
    source_timestamp: Timestamp,
    received_at: Timestamp,
    available_at: Timestamp,
    quality: DataQuality,
    freshness: MarketFreshness,
    sequence_rule: Option<SequenceValidationRule>,
    sequence: SequenceEvidence,
    checksum: ChecksumEvidence,
    diagnostic_ordinal: Option<u64>,
    payload: OrderLevelBatchPayload,
}

impl OrderLevelBatchInput {
    /// Collects exact provider, timing, integrity, and payload evidence for checked construction.
    #[expect(
        clippy::too_many_arguments,
        reason = "source, timing, integrity, and payload evidence must enter atomically"
    )]
    pub const fn new(
        route: OrderLevelRoute,
        batch_identifier: SourceIdentifier,
        source_timestamp: Timestamp,
        received_at: Timestamp,
        available_at: Timestamp,
        quality: DataQuality,
        freshness: MarketFreshness,
        sequence_rule: Option<SequenceValidationRule>,
        sequence: SequenceEvidence,
        checksum: ChecksumEvidence,
        diagnostic_ordinal: Option<u64>,
        payload: OrderLevelBatchPayload,
    ) -> Self {
        Self {
            route,
            batch_identifier,
            source_timestamp,
            received_at,
            available_at,
            quality,
            freshness,
            sequence_rule,
            sequence,
            checksum,
            diagnostic_ordinal,
            payload,
        }
    }
}

/// Checked order-level transaction ready for a generation-owned single writer.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OrderLevelBatch {
    pub(super) route: OrderLevelRoute,
    pub(super) batch_identifier: SourceIdentifier,
    pub(super) source_timestamp: Timestamp,
    pub(super) received_at: Timestamp,
    pub(super) available_at: Timestamp,
    pub(super) quality: DataQuality,
    pub(super) freshness: MarketFreshness,
    pub(super) sequence_rule: Option<SequenceValidationRule>,
    pub(super) sequence: SequenceEvidence,
    pub(super) checksum: ChecksumEvidence,
    pub(super) diagnostic_ordinal: Option<u64>,
    pub(super) payload: OrderLevelBatchPayload,
}

impl OrderLevelBatch {
    /// Validates a bounded transaction without minting current execution authority.
    ///
    /// `DirectVerified` is rejected at this boundary. Order-level depth and authentication remain
    /// independent of execution-quality qualification.
    ///
    /// # Errors
    ///
    /// Rejects identity, depth, quality, timing, sequence/checksum capability, and payload-shape
    /// contradictions. Failed integrity results remain constructible so the book owner can
    /// quarantine atomically while preserving its last committed image.
    pub fn try_new(input: OrderLevelBatchInput) -> Result<Self, OrderLevelBatchError> {
        validate_quality(input.quality)?;
        validate_freshness(input.freshness, input.received_at)?;
        validate_availability(input.received_at, input.available_at)?;
        validate_payload(&input.payload, input.source_timestamp, input.received_at)?;
        validate_sequence(
            &input.route,
            input.sequence_rule,
            &input.sequence,
            input.diagnostic_ordinal,
            &input.payload,
        )?;
        validate_checksum(&input.route, &input.checksum)?;
        Ok(Self {
            route: input.route,
            batch_identifier: input.batch_identifier,
            source_timestamp: input.source_timestamp,
            received_at: input.received_at,
            available_at: input.available_at,
            quality: input.quality,
            freshness: input.freshness,
            sequence_rule: input.sequence_rule,
            sequence: input.sequence,
            checksum: input.checksum,
            diagnostic_ordinal: input.diagnostic_ordinal,
            payload: input.payload,
        })
    }

    /// Returns the exact source/venue/instrument/generation route.
    pub const fn route(&self) -> &OrderLevelRoute {
        &self.route
    }

    /// Returns the source-native batch or frame identity.
    pub const fn batch_identifier(&self) -> &SourceIdentifier {
        &self.batch_identifier
    }

    /// Returns snapshot or incremental semantics.
    pub const fn kind(&self) -> OrderLevelBatchKind {
        self.payload.kind()
    }

    /// Returns the latest provider timestamp in this transaction.
    pub const fn source_timestamp(&self) -> Timestamp {
        self.source_timestamp
    }

    /// Returns the latest receive time in this transaction.
    pub const fn received_at(&self) -> Timestamp {
        self.received_at
    }

    /// Returns when the complete transaction became available to local readers.
    pub const fn available_at(&self) -> Timestamp {
        self.available_at
    }

    /// Returns the archival quality ceiling, never current execution authority.
    pub const fn quality(&self) -> DataQuality {
        self.quality
    }

    /// Returns market freshness independent of connection heartbeat state.
    pub const fn freshness(&self) -> MarketFreshness {
        self.freshness
    }

    /// Returns provider sequence validation semantics when supported.
    pub const fn sequence_rule(&self) -> Option<SequenceValidationRule> {
        self.sequence_rule
    }

    /// Returns complete generation-bound sequence capability evidence.
    pub const fn sequence_evidence(&self) -> &SequenceEvidence {
        &self.sequence
    }

    /// Returns complete generation-bound checksum capability evidence.
    pub const fn checksum_evidence(&self) -> &ChecksumEvidence {
        &self.checksum
    }

    /// Returns the provider sequence, never a local diagnostic ordinal.
    pub const fn provider_sequence(&self) -> Option<SequenceNumber> {
        self.sequence.observed_sequence()
    }

    /// Returns a local connection-generation ordinal with no provider-sequence authority.
    pub const fn diagnostic_ordinal(&self) -> Option<u64> {
        self.diagnostic_ordinal
    }

    /// Returns the bounded transaction payload.
    pub const fn payload(&self) -> &OrderLevelBatchPayload {
        &self.payload
    }

    /// Returns checked bytes retained by this complete queued transaction.
    pub fn retained_bytes(&self) -> Result<usize, OrderLevelBatchError> {
        let sequence = self
            .sequence
            .rule()
            .map_or(Some(0), |rule| rule.dynamic_retained_bytes())
            .ok_or(OrderLevelBatchError::RetainedSizeOverflow)?;
        let checksum_rule = self
            .checksum
            .rule()
            .map_or(Some(0), |rule| rule.dynamic_retained_bytes())
            .ok_or(OrderLevelBatchError::RetainedSizeOverflow)?;
        let checksum_scope = self.checksum.target().map_or(0, |target| match target {
            ChecksumTarget::Book(scope) => scope.provider_scope().retained_bytes(),
            ChecksumTarget::Payload(scope) => scope.provider_scope().retained_bytes(),
        });
        size_of::<Self>()
            .checked_add(
                self.route
                    .dynamic_retained_bytes()
                    .ok_or(OrderLevelBatchError::RetainedSizeOverflow)?,
            )
            .and_then(|bytes| bytes.checked_add(self.batch_identifier.retained_bytes()))
            .and_then(|bytes| bytes.checked_add(sequence))
            .and_then(|bytes| bytes.checked_add(checksum_rule))
            .and_then(|bytes| bytes.checked_add(checksum_scope))
            .and_then(|bytes| bytes.checked_add(self.payload.dynamic_retained_bytes()?))
            .ok_or(OrderLevelBatchError::RetainedSizeOverflow)
    }

    /// Returns the bounded number of provider mutation operations in this transaction.
    pub fn operation_count(&self) -> usize {
        self.payload
            .events()
            .iter()
            .map(|event| event.operations().len())
            .sum()
    }
}

fn validate_quality(quality: DataQuality) -> Result<(), OrderLevelBatchError> {
    if quality == DataQuality::DirectUnverified {
        Ok(())
    } else {
        Err(OrderLevelBatchError::InvalidQuality)
    }
}

fn validate_freshness(
    freshness: MarketFreshness,
    received_at: Timestamp,
) -> Result<(), OrderLevelBatchError> {
    match freshness {
        MarketFreshness::Fresh { last_market_at } | MarketFreshness::Stale { last_market_at }
            if last_market_at == received_at =>
        {
            Ok(())
        }
        MarketFreshness::Uninitialized
        | MarketFreshness::Fresh { .. }
        | MarketFreshness::Stale { .. } => Err(OrderLevelBatchError::FreshnessMismatch),
    }
}

fn validate_availability(
    received_at: Timestamp,
    available_at: Timestamp,
) -> Result<(), OrderLevelBatchError> {
    if received_at <= available_at {
        Ok(())
    } else {
        Err(OrderLevelBatchError::AvailabilityBeforeReceipt)
    }
}

fn validate_payload(
    payload: &OrderLevelBatchPayload,
    source_timestamp: Timestamp,
    received_at: Timestamp,
) -> Result<(), OrderLevelBatchError> {
    let events = payload.events();
    if events.len() > max_batch_events()
        || matches!(payload, OrderLevelBatchPayload::Update { events } if events.is_empty())
    {
        return Err(OrderLevelBatchError::InvalidEventCount {
            observed: events.len(),
            maximum: max_batch_events(),
        });
    }
    if let OrderLevelBatchPayload::Snapshot { orders, .. } = payload
        && orders.len() > MAX_ORDER_LEVEL_ORDERS
    {
        return Err(OrderLevelBatchError::TooManyOrders {
            observed: orders.len(),
            maximum: MAX_ORDER_LEVEL_ORDERS,
        });
    }
    let operation_count = events.iter().try_fold(0_usize, |total, event| {
        total.checked_add(event.operations().len())
    });
    if operation_count.is_none_or(|count| count > MAX_DECODED_BOOK_ITEMS) {
        return Err(OrderLevelBatchError::TooManyOperations {
            maximum: MAX_DECODED_BOOK_ITEMS,
        });
    }
    let (mut previous_source, mut previous_received) = match payload {
        OrderLevelBatchPayload::Snapshot {
            snapshot_source_timestamp,
            snapshot_received_at,
            ..
        } => (
            Some(*snapshot_source_timestamp),
            Some(*snapshot_received_at),
        ),
        OrderLevelBatchPayload::Update { .. } => (None, None),
    };
    for event in events {
        if previous_source.is_some_and(|previous| event.source_timestamp() < previous)
            || previous_received.is_some_and(|previous| event.received_at() < previous)
        {
            return Err(OrderLevelBatchError::TimestampRegression);
        }
        previous_source = Some(event.source_timestamp());
        previous_received = Some(event.received_at());
    }
    if let Some(last) = events.last() {
        if last.source_timestamp() != source_timestamp || last.received_at() != received_at {
            return Err(OrderLevelBatchError::TerminalTimestampMismatch);
        }
    } else if let OrderLevelBatchPayload::Snapshot {
        snapshot_source_timestamp,
        snapshot_received_at,
        ..
    } = payload
        && (*snapshot_source_timestamp != source_timestamp || *snapshot_received_at != received_at)
    {
        return Err(OrderLevelBatchError::TerminalTimestampMismatch);
    }
    Ok(())
}

fn validate_sequence(
    route: &OrderLevelRoute,
    sequence_rule: Option<SequenceValidationRule>,
    evidence: &SequenceEvidence,
    diagnostic_ordinal: Option<u64>,
    payload: &OrderLevelBatchPayload,
) -> Result<(), OrderLevelBatchError> {
    if evidence.connection_generation() != route.generation() {
        return Err(OrderLevelBatchError::IntegrityGenerationMismatch);
    }
    let events = payload.events();
    match evidence.capability() {
        SequenceCapability::Provided => {
            let rule = sequence_rule.ok_or(OrderLevelBatchError::MissingSequenceRule)?;
            if diagnostic_ordinal.is_some()
                || events.iter().any(|event| {
                    event.provider_sequence().is_none() || event.diagnostic_ordinal().is_some()
                })
            {
                return Err(OrderLevelBatchError::SequenceDiagnosticContradiction);
            }
            let snapshot = evidence.snapshot_sequence();
            if snapshot.is_none() {
                return Err(OrderLevelBatchError::MissingSnapshotSequence);
            }
            let mut previous = match payload {
                OrderLevelBatchPayload::Snapshot { .. } => snapshot,
                OrderLevelBatchPayload::Update { .. } => None,
            };
            for event in events {
                let observed = event
                    .provider_sequence()
                    .ok_or(OrderLevelBatchError::MissingProviderSequence)?;
                if let Some(prior) = previous {
                    validate_progression(prior, observed, rule)?;
                }
                previous = Some(observed);
            }
            let expected_terminal = events
                .last()
                .and_then(OrderLevelEvent::provider_sequence)
                .or(snapshot.filter(|_| payload.kind() == OrderLevelBatchKind::Snapshot));
            if evidence.observed_sequence() != expected_terminal {
                return Err(OrderLevelBatchError::TerminalSequenceMismatch);
            }
            if events.len() > 1 {
                let expected_previous = events
                    .get(events.len() - 2)
                    .and_then(OrderLevelEvent::provider_sequence);
                if evidence.previous_sequence() != expected_previous {
                    return Err(OrderLevelBatchError::PreviousSequenceMismatch);
                }
            } else if payload.kind() == OrderLevelBatchKind::Snapshot && events.len() == 1 {
                if evidence.previous_sequence() != snapshot {
                    return Err(OrderLevelBatchError::PreviousSequenceMismatch);
                }
            } else if payload.kind() == OrderLevelBatchKind::Snapshot
                && evidence.previous_sequence().is_some()
            {
                return Err(OrderLevelBatchError::PreviousSequenceMismatch);
            }
        }
        SequenceCapability::Unsupported => {
            if sequence_rule.is_some()
                || evidence.integrity() != SequenceIntegrity::NotSupported
                || evidence.snapshot_sequence().is_some()
                || evidence.previous_sequence().is_some()
                || evidence.observed_sequence().is_some()
                || events.iter().any(|event| {
                    event.provider_sequence().is_some()
                        || event.diagnostic_ordinal().is_some_and(|ordinal| {
                            diagnostic_ordinal.is_none_or(|batch| batch != ordinal)
                        })
                })
            {
                return Err(OrderLevelBatchError::UnsupportedSequenceContradiction);
            }
        }
    }
    Ok(())
}

fn validate_progression(
    previous: SequenceNumber,
    observed: SequenceNumber,
    rule: SequenceValidationRule,
) -> Result<(), OrderLevelBatchError> {
    let valid = match rule {
        SequenceValidationRule::Consecutive => previous
            .checked_next()
            .is_ok_and(|expected| expected == observed),
        SequenceValidationRule::Monotonic => observed > previous,
    };
    if valid {
        Ok(())
    } else {
        Err(OrderLevelBatchError::IntraBatchSequenceFailure)
    }
}

fn validate_checksum(
    route: &OrderLevelRoute,
    evidence: &ChecksumEvidence,
) -> Result<(), OrderLevelBatchError> {
    if evidence.connection_generation() != route.generation() {
        return Err(OrderLevelBatchError::IntegrityGenerationMismatch);
    }
    match evidence.capability() {
        ChecksumCapability::Provided => match evidence.target() {
            Some(ChecksumTarget::Book(scope)) if scope.depth() == MarketDepth::OrderLevel => Ok(()),
            Some(ChecksumTarget::Book(_) | ChecksumTarget::Payload(_)) | None => {
                Err(OrderLevelBatchError::ChecksumScopeMismatch)
            }
        },
        ChecksumCapability::Unsupported => {
            if evidence.target().is_none()
                && evidence.expected().is_none()
                && evidence.computed().is_none()
            {
                Ok(())
            } else {
                Err(OrderLevelBatchError::UnsupportedChecksumContradiction)
            }
        }
    }
}

/// Invalid order-level transaction evidence.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum OrderLevelBatchError {
    /// Complete queued transaction retained-size arithmetic overflowed.
    #[error("order-level retained-size accounting overflowed")]
    RetainedSizeOverflow,
    /// Order-level adapters cannot promote observations to execution quality.
    #[error("order-level read batches must retain a DirectUnverified quality ceiling")]
    InvalidQuality,
    /// Market freshness did not identify this exact market-bearing receive time.
    #[error("order-level freshness does not match the terminal market receive time")]
    FreshnessMismatch,
    /// Local availability preceded receipt of the complete provider transaction.
    #[error("order-level availability precedes local receipt")]
    AvailabilityBeforeReceipt,
    /// Snapshot order count exceeded the shared hard limit.
    #[error("order-level snapshot contains {observed} orders; maximum is {maximum}")]
    TooManyOrders { observed: usize, maximum: usize },
    /// Event count was empty for an update or exceeded the shared hard limit.
    #[error("order-level transaction contains {observed} events; maximum is {maximum}")]
    InvalidEventCount { observed: usize, maximum: usize },
    /// Aggregate operation count overflowed or exceeded the shared hard limit.
    #[error("order-level transaction exceeds {maximum} operations")]
    TooManyOperations { maximum: usize },
    /// Provider or receive timestamps regressed inside one transaction.
    #[error("order-level transaction timestamps regress")]
    TimestampRegression,
    /// Batch-level terminal times did not match the final replay/update event.
    #[error("order-level terminal timestamps do not match the final event")]
    TerminalTimestampMismatch,
    /// Integrity evidence was issued for another connection generation.
    #[error("order-level integrity evidence belongs to another generation")]
    IntegrityGenerationMismatch,
    /// A supplied sequence capability omitted its validation semantics.
    #[error("provided sequence evidence requires a validation rule")]
    MissingSequenceRule,
    /// Provider sequence and local diagnostic evidence were conflated.
    #[error("provider sequence and local diagnostic evidence conflict")]
    SequenceDiagnosticContradiction,
    /// A supplied sequence protocol omitted its snapshot anchor.
    #[error("provided sequence evidence requires a snapshot sequence")]
    MissingSnapshotSequence,
    /// A sequenced transaction event omitted provider sequence.
    #[error("sequenced order-level event omitted provider sequence")]
    MissingProviderSequence,
    /// Events failed the declared provider progression rule.
    #[error("order-level events fail intra-batch sequence progression")]
    IntraBatchSequenceFailure,
    /// Final sequence evidence did not identify the last event or snapshot cursor.
    #[error("order-level terminal sequence does not match the payload")]
    TerminalSequenceMismatch,
    /// Final sequence evidence named the wrong immediate predecessor.
    #[error("order-level previous sequence does not match the payload")]
    PreviousSequenceMismatch,
    /// Unsupported sequence evidence carried sequence-only fields.
    #[error("unsupported order-level sequence evidence is contradictory")]
    UnsupportedSequenceContradiction,
    /// Supported checksum evidence did not cover an order-level book.
    #[error("order-level checksum scope is inconsistent")]
    ChecksumScopeMismatch,
    /// Unsupported checksum evidence carried checksum-only fields.
    #[error("unsupported order-level checksum evidence is contradictory")]
    UnsupportedChecksumContradiction,
}
