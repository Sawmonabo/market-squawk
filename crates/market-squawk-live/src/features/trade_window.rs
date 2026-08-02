//! O(1)-amortized exact aggressor window for the live event path.

use std::mem::size_of;

use market_squawk_analytics::{ExactFeatureRatio, FeatureValidity, FeatureValue, TradeFeatureView};
use market_squawk_domain::AggressorSide;

use super::{ROLLING_DURATION_NANOS, RouteFeatureError};

#[derive(Debug)]
pub(super) struct AggressorWindow {
    observations: Box<[Option<TradeFeatureView>]>,
    head: usize,
    len: usize,
    buy_lots: i128,
    sell_lots: i128,
}

impl AggressorWindow {
    pub(super) fn try_new(capacity: usize) -> Result<Self, RouteFeatureError> {
        if capacity == 0 {
            return Err(RouteFeatureError::RollingCapacityTooSmall);
        }
        let mut observations = Vec::new();
        observations
            .try_reserve_exact(capacity)
            .map_err(|_| RouteFeatureError::Allocation)?;
        observations.resize(capacity, None);
        Ok(Self {
            observations: observations.into_boxed_slice(),
            head: 0,
            len: 0,
            buy_lots: 0,
            sell_lots: 0,
        })
    }

    pub(super) fn reset(&mut self) {
        self.observations.fill(None);
        self.head = 0;
        self.len = 0;
        self.buy_lots = 0;
        self.sell_lots = 0;
    }

    pub(super) fn update(
        &mut self,
        trade: TradeFeatureView,
    ) -> Result<FeatureValue<ExactFeatureRatio>, RouteFeatureError> {
        while self.oldest().is_some_and(|oldest| {
            i128::from(trade.observed_at().unix_nanos())
                - i128::from(oldest.observed_at().unix_nanos())
                > i128::from(ROLLING_DURATION_NANOS)
        }) {
            self.remove_oldest()?;
        }
        if self.len == self.observations.len() {
            self.remove_oldest()?;
        }
        let tail = self
            .head
            .checked_add(self.len)
            .ok_or(RouteFeatureError::CapacityOverflow)?
            % self.observations.len();
        if self.observations[tail].is_some() {
            return Err(RouteFeatureError::InternalStateInvariant);
        }
        self.add(trade)?;
        self.observations[tail] = Some(trade);
        self.len += 1;
        let classified = self
            .buy_lots
            .checked_add(self.sell_lots)
            .ok_or(RouteFeatureError::CapacityOverflow)?;
        if classified == 0 {
            return Ok(FeatureValue::invalid(
                FeatureValidity::Unavailable,
                trade.observed_at(),
            )?);
        }
        Ok(FeatureValue::ready(
            ExactFeatureRatio::try_new(
                self.buy_lots
                    .checked_sub(self.sell_lots)
                    .ok_or(RouteFeatureError::CapacityOverflow)?,
                u128::try_from(classified).map_err(|_| RouteFeatureError::CapacityOverflow)?,
            )?,
            trade.observed_at(),
        ))
    }

    pub(super) fn retained_bytes(&self) -> usize {
        self.observations.len() * size_of::<Option<TradeFeatureView>>()
    }

    fn oldest(&self) -> Option<TradeFeatureView> {
        if self.len == 0 {
            None
        } else {
            self.observations[self.head]
        }
    }

    fn remove_oldest(&mut self) -> Result<(), RouteFeatureError> {
        let oldest = self.observations[self.head]
            .take()
            .ok_or(RouteFeatureError::InternalStateInvariant)?;
        self.subtract(oldest)?;
        self.head = (self.head + 1) % self.observations.len();
        self.len = self
            .len
            .checked_sub(1)
            .ok_or(RouteFeatureError::InternalStateInvariant)?;
        Ok(())
    }

    fn add(&mut self, trade: TradeFeatureView) -> Result<(), RouteFeatureError> {
        let quantity = i128::from(trade.quantity().get());
        match trade.aggressor() {
            AggressorSide::Buy => {
                self.buy_lots = self
                    .buy_lots
                    .checked_add(quantity)
                    .ok_or(RouteFeatureError::CapacityOverflow)?;
            }
            AggressorSide::Sell => {
                self.sell_lots = self
                    .sell_lots
                    .checked_add(quantity)
                    .ok_or(RouteFeatureError::CapacityOverflow)?;
            }
            AggressorSide::Unknown => {}
        }
        Ok(())
    }

    fn subtract(&mut self, trade: TradeFeatureView) -> Result<(), RouteFeatureError> {
        let quantity = i128::from(trade.quantity().get());
        match trade.aggressor() {
            AggressorSide::Buy => {
                self.buy_lots = self
                    .buy_lots
                    .checked_sub(quantity)
                    .ok_or(RouteFeatureError::InternalStateInvariant)?;
            }
            AggressorSide::Sell => {
                self.sell_lots = self
                    .sell_lots
                    .checked_sub(quantity)
                    .ok_or(RouteFeatureError::InternalStateInvariant)?;
            }
            AggressorSide::Unknown => {}
        }
        Ok(())
    }
}
