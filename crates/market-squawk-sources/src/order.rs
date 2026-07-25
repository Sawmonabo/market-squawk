//! Provider-neutral, sequence-bearing level-3 order contracts.

use market_squawk_domain::{
    InstrumentExecutionTerms, PriceTicks, ProviderProduct, QuantityLots, SequenceNumber,
    SourceIdentifier, Timestamp,
};
use thiserror::Error;

use crate::{DecoderEvidence, ProviderBookSide};

/// A complete order owned by a provider's level-3 book.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProviderOrderRecord {
    order_id: SourceIdentifier,
    side: ProviderBookSide,
    price: PriceTicks,
    quantity: QuantityLots,
    terms: InstrumentExecutionTerms,
}

impl ProviderOrderRecord {
    /// Constructs an adapter-normalized provider order.
    pub const fn new(
        order_id: SourceIdentifier,
        side: ProviderBookSide,
        price: PriceTicks,
        quantity: QuantityLots,
        terms: InstrumentExecutionTerms,
    ) -> Self {
        Self {
            order_id,
            side,
            price,
            quantity,
            terms,
        }
    }

    /// Returns the exact provider order identity.
    pub const fn order_id(&self) -> &SourceIdentifier {
        &self.order_id
    }

    /// Returns the order-book side.
    pub const fn side(&self) -> ProviderBookSide {
        self.side
    }

    /// Returns the instrument-scaled price.
    pub const fn price(&self) -> PriceTicks {
        self.price
    }

    /// Returns the instrument-scaled remaining quantity.
    pub const fn quantity(&self) -> QuantityLots {
        self.quantity
    }

    /// Returns the immutable terms used for exact adapter normalization.
    pub const fn execution_terms(&self) -> InstrumentExecutionTerms {
        self.terms
    }
}

/// Documented sequenced messages that advance a product cursor without changing public book state.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ProviderCursorOnlyReason {
    /// An order was received but has not yet become an open public-book order.
    Received,
    /// A pinned protocol message is proven not to mutate the maintained public book.
    DocumentedNoBookMutation,
}

/// Closed provider reason for a structurally validated order change.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ProviderOrderChangeReason {
    /// Self-trade prevention reduced the maintained remaining size.
    SelfTradePrevention,
    /// A public modify-order event replaced price and remaining size.
    ModifyOrder,
    /// A triggered take-profit/stop-loss order was publicly repriced.
    TpslTriggered,
}

/// One classified level-3 mutation or cursor-only advance.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ProviderOrderEventKind {
    /// The exact sequence advances without a public-book mutation.
    CursorOnly(ProviderCursorOnlyReason),
    /// Insert one newly open order.
    Open(ProviderOrderRecord),
    /// Decrement the known maker order by the matched quantity.
    Match {
        /// Exact maker order identity.
        maker_order_id: SourceIdentifier,
        /// Provider-reported maker side.
        maker_side: ProviderBookSide,
        /// Provider-reported scaled maker price.
        maker_price: PriceTicks,
        /// Instrument-scaled executed quantity.
        quantity: QuantityLots,
    },
    /// Remove a known order. A valid unknown received-only order is a cursor-only no-op.
    Done {
        /// Exact provider order identity.
        order_id: SourceIdentifier,
        /// Provider-reported side when present for a maintained limit order.
        side: Option<ProviderBookSide>,
        /// Provider-reported scaled price when present for a maintained limit order.
        price: Option<PriceTicks>,
        /// Provider-reported scaled remaining size when present.
        remaining_quantity: Option<QuantityLots>,
    },
    /// Change a known order under one documented closed reason.
    ///
    /// Unknown received-only orders remain cursor-only no-ops. Known orders require all evidence
    /// implied by `reason` to agree with maintained state before any mutation is applied.
    Change {
        /// Exact provider order identity.
        order_id: SourceIdentifier,
        /// Closed documented change reason.
        reason: ProviderOrderChangeReason,
        /// Provider-reported order-book side.
        side: ProviderBookSide,
        /// Provider-reported scaled price before a limit-order change.
        previous_price: Option<PriceTicks>,
        /// Provider-reported scaled remaining size before a size change.
        previous_quantity: Option<QuantityLots>,
        /// Scaled replacement price for a documented modify-order or TPSL repricing.
        new_price: Option<PriceTicks>,
        /// New remaining size when the message describes a maintained limit order. `None` is a
        /// documented non-book funds change.
        new_quantity: Option<QuantityLots>,
    },
}

/// One exact, product-scoped sequenced event plus its retained raw-frame byte charge.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProviderOrderEvent {
    product: ProviderProduct,
    sequence: SequenceNumber,
    timestamp: Timestamp,
    kind: ProviderOrderEventKind,
    terms: InstrumentExecutionTerms,
    evidence: DecoderEvidence,
}

impl ProviderOrderEvent {
    /// Constructs a bounded queue-admission value.
    ///
    /// # Errors
    ///
    /// Rejects zero byte charges and values larger than one capturable WebSocket frame.
    pub fn try_new(
        product: ProviderProduct,
        sequence: SequenceNumber,
        timestamp: Timestamp,
        kind: ProviderOrderEventKind,
        terms: InstrumentExecutionTerms,
        evidence: DecoderEvidence,
    ) -> Result<Self, ProviderOrderEventError> {
        if evidence.frame_bytes() == 0 || evidence.frame_bytes() > crate::MAX_RAW_FRAME_BYTES {
            return Err(ProviderOrderEventError::InvalidWireBytes);
        }
        if matches!(&kind, ProviderOrderEventKind::Open(order) if order.execution_terms() != terms)
        {
            return Err(ProviderOrderEventError::InstrumentTermsMismatch);
        }
        Ok(Self {
            product,
            sequence,
            timestamp,
            kind,
            terms,
            evidence,
        })
    }

    /// Returns the exact provider product sequence domain.
    pub const fn product(&self) -> &ProviderProduct {
        &self.product
    }

    /// Returns the exact product sequence.
    pub const fn sequence(&self) -> SequenceNumber {
        self.sequence
    }

    /// Returns the venue-supplied event time.
    pub const fn timestamp(&self) -> Timestamp {
        self.timestamp
    }

    /// Returns the classified order mutation.
    pub const fn kind(&self) -> &ProviderOrderEventKind {
        &self.kind
    }

    /// Returns the immutable instrument terms used for exact adapter normalization.
    pub const fn execution_terms(&self) -> InstrumentExecutionTerms {
        self.terms
    }

    /// Returns the validated raw-frame, source/session, receipt, digest, and decoder-rule proof.
    pub const fn evidence(&self) -> &DecoderEvidence {
        &self.evidence
    }

    /// Returns the exact raw-frame byte charge used by bounded replay admission.
    pub const fn wire_bytes(&self) -> usize {
        self.evidence.frame_bytes()
    }
}

/// Invalid provider order-event construction.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum ProviderOrderEventError {
    /// Queue byte accounting must bind one nonempty capturable raw frame.
    #[error("provider order event has an invalid raw-frame byte charge")]
    InvalidWireBytes,
    /// Nested normalized order state must use the event's exact instrument terms.
    #[error("provider order event has inconsistent instrument execution terms")]
    InstrumentTermsMismatch,
}
