//! Exact tick/lot normalization for validated Kraken order-level batches.

use market_squawk_domain::{
    InstrumentExecutionTerms, PriceTicks, QuantityLots, SourceIdentifier, Timestamp,
};
use market_squawk_sources::{
    NormalizationError, ProviderBookSide, ProviderDecimalLexeme, ProviderQuantity,
    normalize_delta_quantity, normalize_positive_quantity, normalize_price,
};
use thiserror::Error;

use super::{KrakenL3BookBatch, KrakenL3OrderEventKind, decoder::KrakenL3PriceLevel};

/// One Kraken order after exact instrument tick/lot normalization.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct KrakenL3ScaledOrder {
    order_id: SourceIdentifier,
    side: ProviderBookSide,
    price: PriceTicks,
    quantity: QuantityLots,
    provider_order_timestamp: Timestamp,
}

impl KrakenL3ScaledOrder {
    /// Returns the exact provider order identity.
    pub const fn order_id(&self) -> &SourceIdentifier {
        &self.order_id
    }

    /// Returns provider book side.
    pub const fn side(&self) -> ProviderBookSide {
        self.side
    }

    /// Returns the exact normalized tick price.
    pub const fn price(&self) -> PriceTicks {
        self.price
    }

    /// Returns remaining lots; zero is present only for a delete-on-zero event.
    pub const fn quantity(&self) -> QuantityLots {
        self.quantity
    }

    /// Returns the provider's order insertion or amendment timestamp.
    pub const fn provider_order_timestamp(&self) -> Timestamp {
        self.provider_order_timestamp
    }
}

/// One checksum-admitted Kraken event after exact financial normalization.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct KrakenL3ScaledOrderEvent {
    kind: KrakenL3OrderEventKind,
    order: KrakenL3ScaledOrder,
}

impl KrakenL3ScaledOrderEvent {
    /// Returns snapshot/add/modify/delete semantics.
    pub const fn kind(&self) -> KrakenL3OrderEventKind {
        self.kind
    }

    /// Returns exact scaled provider order evidence.
    pub const fn order(&self) -> &KrakenL3ScaledOrder {
        &self.order
    }
}

impl KrakenL3BookBatch {
    /// Converts every validated exact lexeme into configured instrument ticks and lots.
    ///
    /// The batch's local generation ordinal is deliberately absent from each scaled order. It
    /// remains available through [`KrakenL3BookBatch::local_generation_ordinal`] only as
    /// diagnostic/recovery ordering and cannot be promoted into provider sequence evidence.
    ///
    /// # Errors
    ///
    /// Rejects mismatched instrument terms, inexact values, nonzero delete quantities, or bounded
    /// allocation failure without returning a partial result.
    pub fn try_scaled_events(
        &self,
        terms: InstrumentExecutionTerms,
    ) -> Result<Vec<KrakenL3ScaledOrderEvent>, KrakenL3ScaleError> {
        if terms.instrument_id() != self.instrument() {
            return Err(KrakenL3ScaleError::InstrumentMismatch);
        }
        let mut scaled = Vec::new();
        scaled
            .try_reserve_exact(self.events().len())
            .map_err(|_| KrakenL3ScaleError::Allocation)?;
        for event in self.events() {
            let order = event.order();
            let price = normalize_price(order.price(), terms.price_tick())?;
            let quantity = match event.kind() {
                KrakenL3OrderEventKind::Delete => {
                    let quantity = normalize_delta_quantity(order.quantity(), terms.lot_size())?;
                    if quantity.get() != 0 {
                        return Err(KrakenL3ScaleError::DeleteQuantityNotZero);
                    }
                    quantity
                }
                KrakenL3OrderEventKind::Snapshot
                | KrakenL3OrderEventKind::Add
                | KrakenL3OrderEventKind::Modify => {
                    normalize_positive_quantity(order.quantity(), terms.lot_size())?
                }
            };
            scaled.push(KrakenL3ScaledOrderEvent {
                kind: event.kind(),
                order: KrakenL3ScaledOrder {
                    order_id: clone_identifier(order.order_id())?,
                    side: order.side(),
                    price,
                    quantity,
                    provider_order_timestamp: order.timestamp(),
                },
            });
        }
        Ok(scaled)
    }

    /// Validates that the checksum-admitted aggregate is exactly representable in instrument
    /// ticks and lots.
    ///
    /// # Errors
    ///
    /// Rejects mismatched terms or an inexact aggregate.
    pub fn validate_price_projection(
        &self,
        terms: InstrumentExecutionTerms,
    ) -> Result<(), KrakenL3ScaleError> {
        if terms.instrument_id() != self.instrument() {
            return Err(KrakenL3ScaleError::InstrumentMismatch);
        }
        validate_price_levels(self.price_projection().bids(), terms)?;
        validate_price_levels(self.price_projection().asks(), terms)
    }
}

fn validate_price_levels(
    levels: &[KrakenL3PriceLevel],
    terms: InstrumentExecutionTerms,
) -> Result<(), KrakenL3ScaleError> {
    for level in levels {
        let quantity = level.quantity().normalize().to_string();
        let quantity = ProviderDecimalLexeme::try_new(&quantity)
            .map(ProviderQuantity::new)
            .map_err(|_| KrakenL3ScaleError::ProjectionQuantity)?;
        let _price = normalize_price(level.price(), terms.price_tick())?;
        let _quantity = normalize_positive_quantity(&quantity, terms.lot_size())?;
    }
    Ok(())
}

fn clone_identifier(value: &SourceIdentifier) -> Result<SourceIdentifier, KrakenL3ScaleError> {
    let mut clone = String::new();
    clone
        .try_reserve_exact(value.as_str().len())
        .map_err(|_| KrakenL3ScaleError::Allocation)?;
    clone.push_str(value.as_str());
    SourceIdentifier::try_from(clone).map_err(|_| KrakenL3ScaleError::IdentifierInvariant)
}

/// Kraken order-level financial normalization failure.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum KrakenL3ScaleError {
    /// The supplied execution terms belong to another internal instrument.
    #[error("Kraken level-3 execution terms belong to another instrument")]
    InstrumentMismatch,
    /// Exact provider values were not representable in configured ticks/lots.
    #[error("Kraken level-3 exact normalization failed: {0}")]
    Normalization(#[from] NormalizationError),
    /// Kraken delete-on-zero evidence contained a nonzero normalized quantity.
    #[error("Kraken level-3 delete quantity must normalize to zero")]
    DeleteQuantityNotZero,
    /// A bounded output or identifier allocation failed.
    #[error("Kraken level-3 normalization allocation failed")]
    Allocation,
    /// A previously validated provider identity could not be reconstructed.
    #[error("Kraken level-3 order identity invariant failed")]
    IdentifierInvariant,
    /// A checked aggregate could not be represented as an exact provider decimal lexeme.
    #[error("Kraken level-3 price projection quantity is invalid")]
    ProjectionQuantity,
}
