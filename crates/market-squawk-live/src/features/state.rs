//! Bounded post-commit mutation for one route's preallocated feature slots.

use std::num::NonZeroUsize;

use market_squawk_analytics::{
    BookDepthView, FeatureScalar, FeatureValidity, FeatureValue, PriceLevelView,
    RequiredLiveFeature, TopOfBookView, TradeFeatureView, depth_weighted_price,
    order_flow_imbalance, top_of_book_features,
};
use market_squawk_domain::{
    BookLevel, ConnectionGeneration, DataQuality, MarketEvent, PriceTicks, QuantityLots, Timestamp,
};
use market_squawk_sources::CurrentStreamKey;

use super::{
    FeatureInvalidationReason, FeatureSetState, FeatureUpdateDisposition, RouteFeatureError,
    RouteFeatureState, feature_index,
};

pub(crate) struct CommittedFeatureInput<'a, I, J> {
    pub(crate) stream: &'a CurrentStreamKey,
    pub(crate) generation: ConnectionGeneration,
    pub(crate) event: &'a MarketEvent,
    pub(crate) observed_at: Timestamp,
    pub(crate) quality: DataQuality,
    pub(crate) bids: I,
    pub(crate) asks: J,
}

impl RouteFeatureState {
    pub(crate) fn apply_committed<I, J>(
        &mut self,
        input: CommittedFeatureInput<'_, I, J>,
    ) -> Result<FeatureUpdateDisposition, RouteFeatureError>
    where
        I: ExactSizeIterator<Item = (PriceTicks, QuantityLots)>,
        J: ExactSizeIterator<Item = (PriceTicks, QuantityLots)>,
    {
        let index = self.slot_index(input.stream, input.generation)?;
        let slot = self
            .slots
            .get_mut(index)
            .ok_or(RouteFeatureError::InternalStateInvariant)?;
        if slot
            .last_observed_at
            .is_some_and(|previous| input.observed_at < previous)
        {
            slot.reset(
                FeatureInvalidationReason::TimestampRegression,
                input.observed_at,
            )?;
            return Ok(FeatureUpdateDisposition::Unavailable);
        }
        slot.last_quality = input.quality;
        slot.last_observed_at = Some(input.observed_at);
        let result = match input.event {
            MarketEvent::BookSnapshot(_) => {
                slot.update_book(input.bids, input.asks, input.observed_at, true)
            }
            MarketEvent::BookDelta(_) | MarketEvent::Quote(_) => {
                slot.update_book(input.bids, input.asks, input.observed_at, false)
            }
            MarketEvent::Trade(trade) => slot.update_trade(TradeFeatureView::try_new(
                trade.price(),
                trade.quantity(),
                trade.aggressor_side(),
                input.observed_at,
            )?),
            MarketEvent::TradingHalt(_) | MarketEvent::CorporateAction(_) => {
                slot.reset(FeatureInvalidationReason::TradingHalt, input.observed_at)?;
                Ok(FeatureUpdateDisposition::Unavailable)
            }
            MarketEvent::InstrumentStatus(status)
                if status.status() != market_squawk_domain::TradingStatus::Active =>
            {
                slot.reset(FeatureInvalidationReason::TradingHalt, input.observed_at)?;
                Ok(FeatureUpdateDisposition::Unavailable)
            }
            MarketEvent::Auction(_) | MarketEvent::InstrumentStatus(_) => {
                Ok(FeatureUpdateDisposition::Updated)
            }
        };
        match result {
            Ok(disposition) => Ok(disposition),
            Err(error) if error.is_expected_feature_failure() => {
                slot.reset(FeatureInvalidationReason::Overflow, input.observed_at)?;
                Ok(FeatureUpdateDisposition::Overflow)
            }
            Err(error) => Err(error),
        }
    }
}

impl FeatureSetState {
    fn update_book<I, J>(
        &mut self,
        bids: I,
        asks: J,
        observed_at: Timestamp,
        is_snapshot: bool,
    ) -> Result<FeatureUpdateDisposition, RouteFeatureError>
    where
        I: ExactSizeIterator<Item = (PriceTicks, QuantityLots)>,
        J: ExactSizeIterator<Item = (PriceTicks, QuantityLots)>,
    {
        if (!is_snapshot && self.requires_snapshot)
            || bids.len() > self.bids.capacity()
            || asks.len() > self.asks.capacity()
        {
            self.invalidate_book(FeatureValidity::Unavailable, observed_at)?;
            return Ok(FeatureUpdateDisposition::Unavailable);
        }
        self.bids.clear();
        self.asks.clear();
        self.bid_views.clear();
        self.ask_views.clear();
        for (price, quantity) in bids {
            self.bids.push(BookLevel::new(price, quantity)?);
            self.bid_views
                .push(PriceLevelView::try_new(price, quantity)?);
        }
        for (price, quantity) in asks {
            self.asks.push(BookLevel::new(price, quantity)?);
            self.ask_views
                .push(PriceLevelView::try_new(price, quantity)?);
        }
        self.requires_snapshot = false;
        let maximum_levels = NonZeroUsize::new(self.bids.capacity().max(self.asks.capacity()))
            .ok_or(RouteFeatureError::InternalStateInvariant)?;
        let depth = BookDepthView::try_new(
            &self.bid_views,
            &self.ask_views,
            maximum_levels,
            observed_at,
        )?;
        self.set(
            RequiredLiveFeature::DepthWeightedPrice,
            map_value(depth_weighted_price(depth)?, FeatureScalar::ExactRatio)?,
        );
        let top = self
            .bid_views
            .first()
            .zip(self.ask_views.first())
            .map(|(bid, ask)| {
                TopOfBookView::try_new(
                    bid.price(),
                    bid.quantity(),
                    ask.price(),
                    ask.quantity(),
                    observed_at,
                )
            })
            .transpose()?;
        let Some(top) = top else {
            self.invalidate_top(FeatureValidity::Unavailable, observed_at)?;
            return Ok(FeatureUpdateDisposition::Unavailable);
        };
        let top_values = top_of_book_features(top)?;
        self.set(
            RequiredLiveFeature::Spread,
            map_value(top_values.spread().clone(), FeatureScalar::PriceTicks)?,
        );
        self.set(
            RequiredLiveFeature::Midpoint,
            map_value(top_values.midpoint().clone(), FeatureScalar::HalfTickPrice)?,
        );
        self.set(
            RequiredLiveFeature::Microprice,
            map_value(top_values.microprice().clone(), FeatureScalar::ExactRatio)?,
        );
        self.set(
            RequiredLiveFeature::BookImbalance,
            map_value(
                top_values.book_imbalance().clone(),
                FeatureScalar::ExactRatio,
            )?,
        );
        let flow = match self.previous_top {
            Some(previous) => map_value(
                order_flow_imbalance(previous, top)?,
                FeatureScalar::SignedInteger,
            )?,
            None => FeatureValue::invalid(FeatureValidity::WarmingUp, observed_at)?,
        };
        self.set(RequiredLiveFeature::OrderFlowImbalance, flow);
        self.previous_top = Some(top);
        Ok(FeatureUpdateDisposition::Updated)
    }

    fn update_trade(
        &mut self,
        trade: TradeFeatureView,
    ) -> Result<FeatureUpdateDisposition, RouteFeatureError> {
        let aggressor = map_value(self.trades.update(trade)?, FeatureScalar::ExactRatio)?;
        self.set(RequiredLiveFeature::AggressorImbalance, aggressor);
        let rolling = self.rolling.update(trade)?;
        self.set(
            RequiredLiveFeature::RollingVwap,
            map_value(rolling.vwap().clone(), FeatureScalar::ExactRatio)?,
        );
        self.set(
            RequiredLiveFeature::VolumeVelocity,
            map_value(rolling.volume_velocity().clone(), FeatureScalar::ExactRatio)?,
        );
        self.set(
            RequiredLiveFeature::Momentum,
            map_value(rolling.momentum().clone(), FeatureScalar::PriceTicks)?,
        );
        self.set(
            RequiredLiveFeature::RollingReturn,
            map_value(rolling.rolling_return().clone(), FeatureScalar::Statistical)?,
        );
        self.set(
            RequiredLiveFeature::RollingVolatility,
            map_value(
                rolling.rolling_volatility().clone(),
                FeatureScalar::Statistical,
            )?,
        );
        Ok(FeatureUpdateDisposition::Updated)
    }

    pub(super) fn set(&mut self, feature: RequiredLiveFeature, value: FeatureValue<FeatureScalar>) {
        self.values[feature_index(feature)] = value;
    }

    fn invalidate_book(
        &mut self,
        validity: FeatureValidity,
        observed_at: Timestamp,
    ) -> Result<(), RouteFeatureError> {
        for feature in [
            RequiredLiveFeature::Spread,
            RequiredLiveFeature::Midpoint,
            RequiredLiveFeature::Microprice,
            RequiredLiveFeature::BookImbalance,
            RequiredLiveFeature::OrderFlowImbalance,
            RequiredLiveFeature::DepthWeightedPrice,
            RequiredLiveFeature::AvailableLiquidity,
            RequiredLiveFeature::Slippage,
        ] {
            self.set(feature, FeatureValue::invalid(validity, observed_at)?);
        }
        Ok(())
    }

    fn invalidate_top(
        &mut self,
        validity: FeatureValidity,
        observed_at: Timestamp,
    ) -> Result<(), RouteFeatureError> {
        for feature in [
            RequiredLiveFeature::Spread,
            RequiredLiveFeature::Midpoint,
            RequiredLiveFeature::Microprice,
            RequiredLiveFeature::BookImbalance,
            RequiredLiveFeature::OrderFlowImbalance,
        ] {
            self.set(feature, FeatureValue::invalid(validity, observed_at)?);
        }
        Ok(())
    }
}

fn map_value<T: Copy>(
    value: FeatureValue<T>,
    mapper: fn(T) -> FeatureScalar,
) -> Result<FeatureValue<FeatureScalar>, market_squawk_analytics::FeatureError> {
    match value.ready_value() {
        Some(inner) => Ok(FeatureValue::ready(mapper(inner), value.observed_at())),
        None => FeatureValue::invalid(value.validity(), value.observed_at()),
    }
}
