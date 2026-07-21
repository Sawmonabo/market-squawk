//! Bounded strategy output and authority-free committed-market context.

use market_squawk_analytics::LiveFeatureView;
use market_squawk_domain::{MarketEvent, QualificationAssessmentId};
use market_squawk_live::ShardKey;
use thiserror::Error;

use crate::{ExecutionMarketReference, OrderIntent};

/// Hard output bound kept equal to live's per-observation authority ceiling.
pub const MAX_STRATEGY_ORDER_INTENTS: usize =
    market_squawk_live::MAX_ACTION_AUTHORITY_ISSUES_PER_OBSERVATION;

/// Borrowed, authority-free state presented to a strategy after market-update handoff.
#[derive(Debug)]
pub struct StrategyContext<'event> {
    route: &'event ShardKey,
    assessment_id: &'event QualificationAssessmentId,
    market: ExecutionMarketReference,
    features: &'event dyn LiveFeatureView,
}

impl<'event> StrategyContext<'event> {
    pub(crate) const fn from_committed(
        route: &'event ShardKey,
        assessment_id: &'event QualificationAssessmentId,
        market: ExecutionMarketReference,
        features: &'event dyn LiveFeatureView,
    ) -> Self {
        Self {
            route,
            assessment_id,
            market,
            features,
        }
    }

    pub const fn route(&self) -> &ShardKey {
        self.route
    }
    pub const fn assessment_id(&self) -> &QualificationAssessmentId {
        self.assessment_id
    }
    pub const fn market(&self) -> ExecutionMarketReference {
        self.market
    }
    pub const fn features(&self) -> &dyn LiveFeatureView {
        self.features
    }
}

/// Fixed-slot, non-cloneable strategy output with no unbounded queue or collection growth.
#[derive(Debug)]
pub struct BoundedOrderIntents {
    intents: [Option<OrderIntent>; MAX_STRATEGY_ORDER_INTENTS],
    len: u8,
}

impl BoundedOrderIntents {
    /// Creates an empty bounded output.
    pub fn new() -> Self {
        Self {
            intents: std::array::from_fn(|_| None),
            len: 0,
        }
    }

    /// Appends one validated authority-free intent.
    pub fn try_push(&mut self, intent: OrderIntent) -> Result<(), StrategyError> {
        let index = usize::from(self.len);
        let slot = self
            .intents
            .get_mut(index)
            .ok_or(StrategyError::IntentCapacity)?;
        *slot = Some(intent);
        self.len = self
            .len
            .checked_add(1)
            .ok_or(StrategyError::IntentCapacity)?;
        Ok(())
    }

    pub const fn len(&self) -> usize {
        self.len as usize
    }
    pub const fn is_empty(&self) -> bool {
        self.len == 0
    }
}

impl Default for BoundedOrderIntents {
    fn default() -> Self {
        Self::new()
    }
}

impl IntoIterator for BoundedOrderIntents {
    type Item = OrderIntent;
    type IntoIter = BoundedOrderIntentIterator;

    fn into_iter(self) -> Self::IntoIter {
        BoundedOrderIntentIterator {
            intents: self.intents.into_iter(),
            remaining: self.len,
        }
    }
}

/// Owning fixed-slot intent iterator.
#[derive(Debug)]
pub struct BoundedOrderIntentIterator {
    intents: std::array::IntoIter<Option<OrderIntent>, MAX_STRATEGY_ORDER_INTENTS>,
    remaining: u8,
}

impl Iterator for BoundedOrderIntentIterator {
    type Item = OrderIntent;

    fn next(&mut self) -> Option<Self::Item> {
        while self.remaining > 0 {
            let intent = self.intents.next()?;
            self.remaining -= 1;
            if intent.is_some() {
                return intent;
            }
        }
        None
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = usize::from(self.remaining);
        (remaining, Some(remaining))
    }
}

impl ExactSizeIterator for BoundedOrderIntentIterator {}

/// Route-owned bounded strategy contract.
pub trait Strategy: Send + std::fmt::Debug {
    fn on_market_event(
        &mut self,
        context: &StrategyContext<'_>,
        event: &MarketEvent,
    ) -> Result<BoundedOrderIntents, StrategyError>;

    fn retained_bytes(&self) -> Result<usize, StrategyError>;
}

/// Closed strategy-boundary failure.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum StrategyError {
    #[error("strategy order-intent capacity is exhausted")]
    IntentCapacity,
    #[error("strategy evaluation failed closed")]
    Evaluation,
    #[error("strategy retained-size accounting failed")]
    RetainedSize,
}
