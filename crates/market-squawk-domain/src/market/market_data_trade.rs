//! Source price and economic trade volume without executable tick or lot terms.

use crate::{
    AggressorSide, AssetClass, LiveEventClass, LiveProvenance, MarketDataEventError,
    MarketDataReference, Money, SourceIdentifier, TradeTakerOrderType,
};
use rust_decimal::Decimal;
use serde::{Deserialize, Deserializer, Serialize};

/// Economic size reported by an admitted provider contract, never executable lots.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MarketDataTradeQuantityUnit {
    /// Number of equity or ETF shares.
    Shares,
    /// Number of option contracts; this is independent of each contract's deliverable multiplier.
    Contracts,
    /// The provider supplied a size whose economic unit is not established.
    Unresolved,
}

/// A genuine executed-trade observation with independent canonical reference evidence.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct MarketDataTradeEvent {
    provenance: LiveProvenance,
    reference: MarketDataReference,
    provider_trade_id: SourceIdentifier,
    price: Money,
    quantity: Decimal,
    quantity_unit: MarketDataTradeQuantityUnit,
    aggressor_side: AggressorSide,
    taker_order_type: Option<TradeTakerOrderType>,
}
impl MarketDataTradeEvent {
    /// Validates the original price, volume, clocks and independent reference binding.
    #[allow(
        clippy::too_many_arguments,
        reason = "source and economic evidence remain explicit"
    )]
    pub fn try_new(
        provenance: LiveProvenance,
        reference: MarketDataReference,
        provider_trade_id: SourceIdentifier,
        price: Money,
        quantity: Decimal,
        quantity_unit: MarketDataTradeQuantityUnit,
        aggressor_side: AggressorSide,
        taker_order_type: Option<TradeTakerOrderType>,
    ) -> Result<Self, MarketDataEventError> {
        if provenance.instrument_id() != Some(reference.instrument_id())
            || provenance.venue_id().is_none()
            || provenance.binding().event_class() != LiveEventClass::Trade
            || provenance.source_identifier().as_str() != reference.source_symbol().as_str()
        {
            return Err(MarketDataEventError::Binding);
        }
        reference.validate_at(provenance.received_at())?;
        if matches!(quantity_unit, MarketDataTradeQuantityUnit::Shares)
            && !matches!(
                reference.asset_class(),
                AssetClass::Equity | AssetClass::Fund
            )
            || matches!(quantity_unit, MarketDataTradeQuantityUnit::Contracts)
                && reference.asset_class() != AssetClass::Option
        {
            return Err(MarketDataEventError::Binding);
        }
        if price.currency() != reference.currency() {
            return Err(MarketDataEventError::Currency);
        }
        if price.amount() <= Decimal::ZERO || quantity <= Decimal::ZERO {
            return Err(MarketDataEventError::Size);
        }
        Ok(Self {
            provenance,
            reference,
            provider_trade_id,
            price,
            quantity: quantity.normalize(),
            quantity_unit,
            aggressor_side,
            taker_order_type,
        })
    }
    /// Exact feed, source clock, capture and availability provenance.
    pub const fn provenance(&self) -> &LiveProvenance {
        &self.provenance
    }
    /// Exact canonical definition and accepted reference assertion.
    pub const fn reference(&self) -> &MarketDataReference {
        &self.reference
    }
    /// Opaque source trade identifier, separately retained from instrument reference identity.
    pub const fn provider_trade_id(&self) -> &SourceIdentifier {
        &self.provider_trade_id
    }
    /// Currency-qualified actual transaction price.
    pub const fn price(&self) -> Money {
        self.price
    }
    /// Exact source volume. Consumers must inspect its unit before aggregation.
    pub const fn quantity(&self) -> Decimal {
        self.quantity
    }
    /// Economic volume unit, never a trading lot scale.
    pub const fn quantity_unit(&self) -> MarketDataTradeQuantityUnit {
        self.quantity_unit
    }
    /// Source-established aggressor direction or explicit unknown.
    pub const fn aggressor_side(&self) -> AggressorSide {
        self.aggressor_side
    }
    /// Source-established taker order type, when supplied.
    pub const fn taker_order_type(&self) -> Option<TradeTakerOrderType> {
        self.taker_order_type
    }
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Wire {
    provenance: LiveProvenance,
    reference: MarketDataReference,
    provider_trade_id: SourceIdentifier,
    price: Money,
    quantity: Decimal,
    quantity_unit: MarketDataTradeQuantityUnit,
    aggressor_side: AggressorSide,
    taker_order_type: Option<TradeTakerOrderType>,
}
impl<'de> Deserialize<'de> for MarketDataTradeEvent {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let w = Wire::deserialize(deserializer)?;
        Self::try_new(
            w.provenance,
            w.reference,
            w.provider_trade_id,
            w.price,
            w.quantity,
            w.quantity_unit,
            w.aggressor_side,
            w.taker_order_type,
        )
        .map_err(serde::de::Error::custom)
    }
}
