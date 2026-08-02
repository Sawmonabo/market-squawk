//! One-update, one-liquidity-ledger deterministic matching.

use market_squawk_domain::{
    DataQuality, OrderSide, OrderType, PriceTicks, QuantityLots, TimeInForce,
};
use market_squawk_execution::{ExecutionMarketReference, ExecutionMarketUpdate};
use thiserror::Error;

use crate::LiquidityRole;
use crate::config::PaperExecutionConfig;
use crate::order::PaperOrder;
use crate::slippage::{adverse_bound, apply_level_impact};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct AvailableLevel {
    price: PriceTicks,
    remaining: i64,
}

#[derive(Debug)]
pub(crate) struct AvailableMarket {
    market: ExecutionMarketReference,
    bids: Vec<AvailableLevel>,
    asks: Vec<AvailableLevel>,
    bid_participation_remaining: i64,
    ask_participation_remaining: i64,
}

impl AvailableMarket {
    pub(crate) fn try_new(
        update: ExecutionMarketUpdate,
        config: &PaperExecutionConfig,
    ) -> Result<Self, MatchingError> {
        let market = update.market();
        if market.quality() != DataQuality::DirectVerified {
            return Err(MatchingError::InvalidMarket);
        }
        let bids = copy_side(market, OrderSide::Sell)?;
        let asks = copy_side(market, OrderSide::Buy)?;
        let participation = i64::from(config.input().maximum_participation_basis_points);
        let bid_total = sum_remaining(&bids)?;
        let ask_total = sum_remaining(&asks)?;
        Ok(Self {
            market,
            bids,
            asks,
            bid_participation_remaining: bid_total
                .checked_mul(participation)
                .ok_or(MatchingError::Overflow)?
                / 10_000,
            ask_participation_remaining: ask_total
                .checked_mul(participation)
                .ok_or(MatchingError::Overflow)?
                / 10_000,
        })
    }

    pub(crate) const fn market(&self) -> ExecutionMarketReference {
        self.market
    }

    pub(crate) fn plan(
        &self,
        order: &PaperOrder,
        update: ExecutionMarketUpdate,
        config: &PaperExecutionConfig,
    ) -> Result<MatchPlan, MatchingError> {
        if order.terms != self.market.execution_terms()
            || self.market.observed_at() < order.eligible_at
            || self.market.observed_at() > order.expires_at
        {
            return Ok(MatchPlan::none(false, false));
        }
        let trigger_price = update.trade_price().or_else(|| match order.side {
            OrderSide::Buy => self.market.best_ask().map(|level| level.price()),
            OrderSide::Sell => self.market.best_bid().map(|level| level.price()),
        });
        let triggered = order.triggered || stop_triggered(order, trigger_price);
        if !triggered {
            return Ok(MatchPlan::none(false, false));
        }
        let (levels, participation_remaining) = match order.side {
            OrderSide::Buy => (&self.asks, self.ask_participation_remaining),
            OrderSide::Sell => (&self.bids, self.bid_participation_remaining),
        };
        let remaining = order
            .remaining()
            .map_err(|_| MatchingError::InvalidOrder)?
            .get();
        let target = remaining.min(participation_remaining);
        let adverse = adverse_bound(order.reference_price, order.side, order.maximum_slippage)
            .map_err(|_| MatchingError::InvalidOrder)?;
        let mut needed = target;
        let mut legs = Vec::new();
        legs.try_reserve(levels.len())
            .map_err(|_| MatchingError::Allocation)?;
        for (index, level) in levels.iter().enumerate() {
            if needed == 0 {
                break;
            }
            let impacted = apply_level_impact(
                level.price,
                order.side,
                config.input().impact_basis_points_per_level,
                index,
            )
            .map_err(|_| MatchingError::Overflow)?;
            if !price_is_eligible(order, impacted, adverse) {
                break;
            }
            let fill = needed.min(level.remaining);
            if fill > 0 {
                legs.push(PlannedLeg {
                    index,
                    price: impacted,
                    quantity: QuantityLots::new(fill).map_err(|_| MatchingError::Overflow)?,
                });
                needed -= fill;
            }
        }
        let planned = target - needed;
        if order.time_in_force == TimeInForce::FillOrKill && planned != remaining {
            return Ok(MatchPlan::none(triggered, true));
        }
        let cancel_remainder = matches!(
            order.time_in_force,
            TimeInForce::ImmediateOrCancel | TimeInForce::FillOrKill
        ) && planned < remaining;
        let became_resting = planned < remaining
            && matches!(order.order_type, OrderType::Limit | OrderType::StopLimit)
            && !cancel_remainder;
        Ok(MatchPlan {
            legs,
            liquidity: if order.resting {
                LiquidityRole::Maker
            } else {
                LiquidityRole::Taker
            },
            triggered,
            cancel_remainder,
            became_resting,
        })
    }

    pub(crate) fn consume(
        &mut self,
        side: OrderSide,
        plan: &MatchPlan,
    ) -> Result<(), MatchingError> {
        let (levels, participation) = match side {
            OrderSide::Buy => (&mut self.asks, &mut self.ask_participation_remaining),
            OrderSide::Sell => (&mut self.bids, &mut self.bid_participation_remaining),
        };
        let mut consumed = 0_i64;
        for leg in &plan.legs {
            let level = levels
                .get_mut(leg.index)
                .ok_or(MatchingError::InvalidPlan)?;
            level.remaining = level
                .remaining
                .checked_sub(leg.quantity.get())
                .ok_or(MatchingError::Overflow)?;
            if level.remaining < 0 {
                return Err(MatchingError::InvalidPlan);
            }
            consumed = consumed
                .checked_add(leg.quantity.get())
                .ok_or(MatchingError::Overflow)?;
        }
        *participation = participation
            .checked_sub(consumed)
            .ok_or(MatchingError::Overflow)?;
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct PlannedLeg {
    pub(crate) index: usize,
    pub(crate) price: PriceTicks,
    pub(crate) quantity: QuantityLots,
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) struct MatchPlan {
    pub(crate) legs: Vec<PlannedLeg>,
    pub(crate) liquidity: LiquidityRole,
    pub(crate) triggered: bool,
    pub(crate) cancel_remainder: bool,
    pub(crate) became_resting: bool,
}

impl MatchPlan {
    fn none(triggered: bool, cancel_remainder: bool) -> Self {
        Self {
            legs: Vec::new(),
            liquidity: LiquidityRole::Taker,
            triggered,
            cancel_remainder,
            became_resting: false,
        }
    }

    pub(crate) fn fill_legs(&self) -> Vec<(PriceTicks, QuantityLots)> {
        self.legs
            .iter()
            .map(|leg| (leg.price, leg.quantity))
            .collect()
    }
}

fn copy_side(
    market: ExecutionMarketReference,
    order_side: OrderSide,
) -> Result<Vec<AvailableLevel>, MatchingError> {
    let count = match order_side {
        OrderSide::Buy => market.ask_count(),
        OrderSide::Sell => market.bid_count(),
    };
    let mut levels = Vec::new();
    levels
        .try_reserve(count)
        .map_err(|_| MatchingError::Allocation)?;
    for index in 0..count {
        let level = match order_side {
            OrderSide::Buy => market.ask(index),
            OrderSide::Sell => market.bid(index),
        }
        .ok_or(MatchingError::InvalidMarket)?;
        levels.push(AvailableLevel {
            price: level.price(),
            remaining: level.quantity().get(),
        });
    }
    Ok(levels)
}

fn sum_remaining(levels: &[AvailableLevel]) -> Result<i64, MatchingError> {
    levels.iter().try_fold(0_i64, |sum, level| {
        sum.checked_add(level.remaining)
            .ok_or(MatchingError::Overflow)
    })
}

fn stop_triggered(order: &PaperOrder, observed: Option<PriceTicks>) -> bool {
    let (Some(stop), Some(observed)) = (order.stop_price, observed) else {
        return false;
    };
    match order.side {
        OrderSide::Buy => observed >= stop,
        OrderSide::Sell => observed <= stop,
    }
}

fn price_is_eligible(order: &PaperOrder, price: PriceTicks, adverse: PriceTicks) -> bool {
    let within_slippage = match order.side {
        OrderSide::Buy => price <= adverse,
        OrderSide::Sell => price >= adverse,
    };
    let within_limit = match (order.side, order.limit_price) {
        (_, None) => true,
        (OrderSide::Buy, Some(limit)) => price <= limit,
        (OrderSide::Sell, Some(limit)) => price >= limit,
    };
    within_slippage && within_limit && order.execution_price_bound.permits(price)
}

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub(crate) enum MatchingError {
    #[error("paper matching received invalid market state")]
    InvalidMarket,
    #[error("paper matching received invalid order state")]
    InvalidOrder,
    #[error("paper matching produced an invalid consumption plan")]
    InvalidPlan,
    #[error("paper matching bounded allocation failed")]
    Allocation,
    #[error("paper matching arithmetic overflowed")]
    Overflow,
}
