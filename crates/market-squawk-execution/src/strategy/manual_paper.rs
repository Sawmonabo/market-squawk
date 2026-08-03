//! Nonblocking route-owned ingress for authority-free manual paper drafts.

use std::mem::size_of;

use market_squawk_domain::{
    AccountId, BasisPoints, ClientOrderId, DataQuality, MarketEvent, OrderId, OrderReasonCode,
    OrderSide, OrderType, PriceTicks, QuantityLots, StrategyId, TimeInForce, Timestamp,
};
use market_squawk_live::ShardKey;
use thiserror::Error;
use tokio::sync::mpsc;

use crate::{
    MAX_INTENT_SLIPPAGE_BASIS_POINTS, MAX_ORDER_TARGET_ID_BYTES, OrderIntent, OrderIntentInput,
    OrderTargetReference,
};

use super::{BoundedOrderIntents, Strategy, StrategyContext, StrategyError};

/// Authority-free user decision fields accepted before a committed market event supplies terms.
#[derive(Debug)]
pub struct ManualPaperDraftInput {
    pub order_id: OrderId,
    pub client_order_id: ClientOrderId,
    pub strategy_id: StrategyId,
    pub account_id: AccountId,
    pub side: OrderSide,
    pub order_type: OrderType,
    pub quantity: QuantityLots,
    pub limit_price: Option<PriceTicks>,
    pub stop_price: Option<PriceTicks>,
    pub time_in_force: TimeInForce,
    pub expires_at: Timestamp,
    pub reason_code: OrderReasonCode,
    pub maximum_slippage: BasisPoints,
    pub target_reference: OrderTargetReference,
}

/// Validated bounded manual paper draft carrying no market, risk, or adapter authority.
#[derive(Debug)]
pub struct ManualPaperDraft {
    input: ManualPaperDraftInput,
}

impl ManualPaperDraft {
    /// Validates every context-independent intent invariant before slot admission.
    pub fn try_new(input: ManualPaperDraftInput) -> Result<Self, ManualPaperDraftError> {
        let requires_limit = matches!(input.order_type, OrderType::Limit | OrderType::StopLimit);
        let requires_stop = matches!(input.order_type, OrderType::Stop | OrderType::StopLimit);
        let time_in_force_valid = match input.order_type {
            OrderType::Market => input.time_in_force != TimeInForce::GoodTilCancelled,
            OrderType::Limit => true,
            OrderType::Stop | OrderType::StopLimit => matches!(
                input.time_in_force,
                TimeInForce::Day | TimeInForce::GoodTilCancelled
            ),
        };
        if requires_limit != input.limit_price.is_some()
            || requires_stop != input.stop_price.is_some()
            || !time_in_force_valid
        {
            return Err(ManualPaperDraftError::InvalidOrderShape);
        }
        if input.quantity.get() == 0 {
            return Err(ManualPaperDraftError::ZeroQuantity);
        }
        if !(0..=MAX_INTENT_SLIPPAGE_BASIS_POINTS).contains(&input.maximum_slippage.get()) {
            return Err(ManualPaperDraftError::InvalidMaximumSlippage);
        }
        input
            .target_reference
            .validate()
            .map_err(|_| ManualPaperDraftError::InvalidTargetReference)?;
        Ok(Self { input })
    }
}

/// Context-independent manual draft validation failure.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum ManualPaperDraftError {
    #[error("manual paper draft order shape is invalid")]
    InvalidOrderShape,
    #[error("manual paper draft quantity must be positive")]
    ZeroQuantity,
    #[error("manual paper draft maximum slippage is invalid")]
    InvalidMaximumSlippage,
    #[error("manual paper draft target reference is invalid")]
    InvalidTargetReference,
}

/// Cloneable route-bound handle for nonblocking admission into one fixed draft slot.
#[derive(Clone, Debug)]
pub struct ManualPaperDraftIngress {
    route: ShardKey,
    sender: mpsc::Sender<ManualPaperDraft>,
}

impl ManualPaperDraftIngress {
    /// Attempts immediate submission and never waits for strategy consumption.
    pub fn try_submit(&self, draft: ManualPaperDraft) -> Result<(), ManualPaperIngressError> {
        self.sender.try_send(draft).map_err(|error| match error {
            mpsc::error::TrySendError::Full(_) => ManualPaperIngressError::Occupied,
            mpsc::error::TrySendError::Closed(_) => ManualPaperIngressError::Closed,
        })
    }

    /// Returns the exact route owning this single-slot ingress.
    pub const fn route(&self) -> &ShardKey {
        &self.route
    }
}

/// Immediate single-slot submission failure.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum ManualPaperIngressError {
    #[error("manual paper draft slot is occupied")]
    Occupied,
    #[error("manual paper draft ingress is closed")]
    Closed,
}

/// Strategy half of one route-owned manual paper draft slot.
#[derive(Debug)]
pub struct ManualPaperStrategy {
    route: ShardKey,
    drafts: mpsc::Receiver<ManualPaperDraft>,
    retained_bytes: usize,
}

impl ManualPaperStrategy {
    /// Creates one route-bound fixed single-slot ingress and its sole strategy consumer.
    pub fn try_new(route: ShardKey) -> Result<(ManualPaperDraftIngress, Self), StrategyError> {
        let retained_bytes = size_of::<Self>()
            .checked_add(route.venue().retained_bytes())
            .and_then(|value| value.checked_add(size_of::<Option<ManualPaperDraft>>()))
            .and_then(|value| value.checked_add(ClientOrderId::MAX_LENGTH))
            .and_then(|value| value.checked_add(OrderReasonCode::MAX_LENGTH))
            .and_then(|value| value.checked_add(MAX_ORDER_TARGET_ID_BYTES))
            .ok_or(StrategyError::RetainedSize)?;
        let (sender, drafts) = mpsc::channel(1);
        Ok((
            ManualPaperDraftIngress {
                route: route.clone(),
                sender,
            },
            Self {
                route,
                drafts,
                retained_bytes,
            },
        ))
    }
}

impl Strategy for ManualPaperStrategy {
    fn on_market_event(
        &mut self,
        context: &StrategyContext<'_>,
        _event: &MarketEvent,
    ) -> Result<BoundedOrderIntents, StrategyError> {
        if context.route() != &self.route
            || context.market().execution_terms().instrument_id() != self.route.instrument()
            || context.market().quality() != DataQuality::DirectVerified
        {
            return Err(StrategyError::Evaluation);
        }
        let draft = match self.drafts.try_recv() {
            Ok(draft) => draft,
            Err(mpsc::error::TryRecvError::Empty | mpsc::error::TryRecvError::Disconnected) => {
                return Ok(BoundedOrderIntents::new());
            }
        };
        let market = context.market();
        if draft.input.expires_at <= market.observed_at() {
            return Ok(BoundedOrderIntents::new());
        }
        let ManualPaperDraftInput {
            order_id,
            client_order_id,
            strategy_id,
            account_id,
            side,
            order_type,
            quantity,
            limit_price,
            stop_price,
            time_in_force,
            expires_at,
            reason_code,
            maximum_slippage,
            target_reference,
        } = draft.input;
        let intent = OrderIntent::try_new_with_target_reference(
            OrderIntentInput {
                order_id,
                client_order_id,
                strategy_id,
                model_id: None,
                account_id,
                execution_terms: market.execution_terms(),
                side,
                order_type,
                quantity,
                limit_price,
                stop_price,
                time_in_force,
                signal_at: market.observed_at(),
                expires_at,
                reason_codes: vec![reason_code],
                maximum_slippage,
                required_quality: market.quality(),
            },
            target_reference,
        )
        .map_err(|_| StrategyError::Evaluation)?;
        let mut output = BoundedOrderIntents::new();
        output.try_push(intent)?;
        Ok(output)
    }

    fn retained_bytes(&self) -> Result<usize, StrategyError> {
        Ok(self.retained_bytes)
    }
}
