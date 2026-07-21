//! Actor-owned paper order representation.

use market_squawk_domain::{
    AccountId, ApprovalId, BasisPoints, ClientOrderId, InstrumentExecutionTerms, ModelId, Money,
    OrderId, OrderSide, OrderType, PriceTicks, QuantityLots, RuleVersion, StrategyId, TimeInForce,
    Timestamp,
};
use market_squawk_execution::{
    DispatchOrder, ExecutionPriceBound, OrderIntentDigest, ReconciledOrder, ReconciledOrderStatus,
    RecoveredDispatchOrder, RiskPolicyIdentity,
};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::{PaperFill, PaperOrderLifecycle, PaperOrderState, PaperStateError};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PaperOrder {
    pub(crate) approval_id: ApprovalId,
    pub(crate) order_id: OrderId,
    pub(crate) client_order_id: ClientOrderId,
    pub(crate) account_id: AccountId,
    pub(crate) account_revision: u64,
    pub(crate) terms: InstrumentExecutionTerms,
    pub(crate) side: OrderSide,
    pub(crate) order_type: OrderType,
    pub(crate) quantity: QuantityLots,
    pub(crate) limit_price: Option<PriceTicks>,
    pub(crate) stop_price: Option<PriceTicks>,
    pub(crate) time_in_force: TimeInForce,
    pub(crate) maximum_slippage: BasisPoints,
    pub(crate) intent_digest: OrderIntentDigest,
    pub(crate) reference_price: PriceTicks,
    pub(crate) execution_price_bound: ExecutionPriceBound,
    pub(crate) accepted_at: Timestamp,
    pub(crate) eligible_at: Timestamp,
    pub(crate) expires_at: Timestamp,
    pub(crate) accepted_sequence: u64,
    pub(crate) cancel_effective_at: Option<Timestamp>,
    pub(crate) triggered: bool,
    pub(crate) resting: bool,
    pub(crate) lifecycle: PaperOrderLifecycle,
    pub(crate) cumulative_fee: Money,
    pub(crate) weighted_fill_ticks: i128,
    pub(crate) maximum_fill_price: Option<PriceTicks>,
    pub(crate) strategy_id: StrategyId,
    pub(crate) model_id: Option<ModelId>,
    pub(crate) assessment_digest: [u8; 32],
    pub(crate) evidence_binding_digest: [u8; 32],
    pub(crate) risk_policy: RiskPolicyIdentity,
    pub(crate) market_observed_at: Timestamp,
    pub(crate) valid_until: Timestamp,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PaperOrderRecoveryWire {
    approval_id: ApprovalId,
    order_id: OrderId,
    client_order_id: ClientOrderId,
    account_id: AccountId,
    account_revision: u64,
    terms: InstrumentExecutionTerms,
    side: OrderSide,
    order_type: OrderType,
    quantity: QuantityLots,
    limit_price: Option<PriceTicks>,
    stop_price: Option<PriceTicks>,
    time_in_force: TimeInForce,
    maximum_slippage: BasisPoints,
    intent_digest: [u8; 32],
    reference_price: PriceTicks,
    maximum_execution_price: PriceTicks,
    accepted_at: Timestamp,
    eligible_at: Timestamp,
    expires_at: Timestamp,
    accepted_sequence: u64,
    cancel_effective_at: Option<Timestamp>,
    triggered: bool,
    resting: bool,
    state: PaperOrderState,
    cumulative_filled: QuantityLots,
    revision: u64,
    last_sequence: u64,
    cumulative_fee: Money,
    weighted_fill_ticks: i128,
    maximum_fill_price: Option<PriceTicks>,
    strategy_id: StrategyId,
    model_id: Option<ModelId>,
    assessment_digest: [u8; 32],
    evidence_binding_digest: [u8; 32],
    risk_policy_digest: [u8; 32],
    risk_policy_version: RuleVersion,
    market_observed_at: Timestamp,
    valid_until: Timestamp,
}

impl PaperOrder {
    pub(crate) fn from_dispatch(
        dispatch: &DispatchOrder,
        accepted_sequence: u64,
        eligible_at: Timestamp,
        expires_at: Timestamp,
    ) -> Result<Self, PaperStateError> {
        let reference_price = dispatch
            .execution_price()
            .ok_or(PaperStateError::InvalidTransition)?;
        let execution_price_bound = dispatch.execution_price_bound();
        if !execution_price_bound.permits(reference_price) {
            return Err(PaperStateError::InvalidTransition);
        }
        let currency = dispatch.execution_terms().quote_currency();
        let lifecycle = PaperOrderLifecycle::try_new(dispatch.quantity())?;
        let intent_digest = dispatch.intent_digest();
        Ok(Self {
            approval_id: dispatch.approval_id(),
            order_id: dispatch.order_id(),
            client_order_id: dispatch.client_order_id().clone(),
            account_id: dispatch.account_id(),
            account_revision: dispatch.account_revision(),
            terms: dispatch.execution_terms(),
            side: dispatch.side(),
            order_type: dispatch.order_type(),
            quantity: dispatch.quantity(),
            limit_price: dispatch.limit_price(),
            stop_price: dispatch.stop_price(),
            time_in_force: dispatch.time_in_force(),
            maximum_slippage: dispatch.maximum_slippage(),
            intent_digest,
            reference_price,
            execution_price_bound,
            accepted_at: dispatch.submitted_at(),
            eligible_at,
            expires_at,
            accepted_sequence,
            cancel_effective_at: None,
            triggered: matches!(dispatch.order_type(), OrderType::Market | OrderType::Limit),
            resting: false,
            lifecycle,
            cumulative_fee: Money::new(Decimal::ZERO, currency),
            weighted_fill_ticks: 0,
            maximum_fill_price: None,
            strategy_id: dispatch.strategy_id(),
            model_id: dispatch.model_id(),
            assessment_digest: dispatch.assessment_digest(),
            evidence_binding_digest: dispatch.evidence_binding_digest(),
            risk_policy: dispatch.risk_policy(),
            market_observed_at: dispatch.market().observed_at(),
            valid_until: dispatch.valid_until(),
        })
    }

    pub(crate) fn remaining(&self) -> Result<QuantityLots, PaperStateError> {
        self.quantity
            .checked_sub(self.lifecycle.cumulative_filled())
            .map_err(|_| PaperStateError::QuantityOverflow)
    }

    pub(crate) fn apply_fill(
        &mut self,
        fill: PaperFill,
        sequence: u64,
    ) -> Result<(), PaperStateError> {
        if !self.execution_price_bound.permits(fill.maximum_price())
            || fill.maximum_price() < fill.average_price()
        {
            return Err(PaperStateError::InvalidTransition);
        }
        self.lifecycle.apply_fill(fill.quantity(), sequence)?;
        self.cumulative_fee = self
            .cumulative_fee
            .checked_add(fill.fee())
            .map_err(|_| PaperStateError::QuantityOverflow)?;
        let weighted = i128::from(fill.average_price().get())
            .checked_mul(i128::from(fill.quantity().get()))
            .ok_or(PaperStateError::QuantityOverflow)?;
        self.weighted_fill_ticks = self
            .weighted_fill_ticks
            .checked_add(weighted)
            .ok_or(PaperStateError::QuantityOverflow)?;
        self.maximum_fill_price = Some(
            self.maximum_fill_price
                .map_or(fill.maximum_price(), |current| {
                    current.max(fill.maximum_price())
                }),
        );
        Ok(())
    }

    pub(crate) fn average_fill_price(&self) -> Option<PriceTicks> {
        let filled = self.lifecycle.cumulative_filled().get();
        if filled == 0 {
            return None;
        }
        let ticks = self.weighted_fill_ticks.checked_div(i128::from(filled))?;
        i64::try_from(ticks).ok().map(PriceTicks::new)
    }

    pub(crate) fn reconciled_status(&self) -> ReconciledOrderStatus {
        match self.lifecycle.state() {
            PaperOrderState::New | PaperOrderState::Accepted | PaperOrderState::CancelPending => {
                if self.lifecycle.cumulative_filled().get() == 0 {
                    ReconciledOrderStatus::Open
                } else {
                    ReconciledOrderStatus::PartiallyFilled
                }
            }
            PaperOrderState::PartiallyFilled => ReconciledOrderStatus::PartiallyFilled,
            PaperOrderState::Filled => ReconciledOrderStatus::Filled,
            PaperOrderState::Canceled => ReconciledOrderStatus::Canceled,
            PaperOrderState::Rejected => ReconciledOrderStatus::Rejected,
            PaperOrderState::Expired => ReconciledOrderStatus::Expired,
        }
    }

    pub(crate) fn input_digest(&self) -> [u8; 32] {
        self.execution_price_bound
            .order_audit_digest(self.intent_digest)
    }

    pub(crate) fn recovery_wire(&self) -> PaperOrderRecoveryWire {
        PaperOrderRecoveryWire {
            approval_id: self.approval_id,
            order_id: self.order_id,
            client_order_id: self.client_order_id.clone(),
            account_id: self.account_id,
            account_revision: self.account_revision,
            terms: self.terms,
            side: self.side,
            order_type: self.order_type,
            quantity: self.quantity,
            limit_price: self.limit_price,
            stop_price: self.stop_price,
            time_in_force: self.time_in_force,
            maximum_slippage: self.maximum_slippage,
            intent_digest: self.intent_digest.as_bytes(),
            reference_price: self.reference_price,
            maximum_execution_price: self.execution_price_bound.maximum_price(),
            accepted_at: self.accepted_at,
            eligible_at: self.eligible_at,
            expires_at: self.expires_at,
            accepted_sequence: self.accepted_sequence,
            cancel_effective_at: self.cancel_effective_at,
            triggered: self.triggered,
            resting: self.resting,
            state: self.lifecycle.state(),
            cumulative_filled: self.lifecycle.cumulative_filled(),
            revision: self.lifecycle.revision(),
            last_sequence: self.lifecycle.last_sequence(),
            cumulative_fee: self.cumulative_fee,
            weighted_fill_ticks: self.weighted_fill_ticks,
            maximum_fill_price: self.maximum_fill_price,
            strategy_id: self.strategy_id,
            model_id: self.model_id,
            assessment_digest: self.assessment_digest,
            evidence_binding_digest: self.evidence_binding_digest,
            risk_policy_digest: self.risk_policy.digest(),
            risk_policy_version: self.risk_policy.ruleset_version(),
            market_observed_at: self.market_observed_at,
            valid_until: self.valid_until,
        }
    }

    pub(crate) fn try_from_recovery_wire(
        wire: PaperOrderRecoveryWire,
    ) -> Result<Self, PaperStateError> {
        let price_shape_valid = match wire.order_type {
            OrderType::Market => wire.limit_price.is_none() && wire.stop_price.is_none(),
            OrderType::Limit => wire.limit_price.is_some() && wire.stop_price.is_none(),
            OrderType::Stop => wire.limit_price.is_none() && wire.stop_price.is_some(),
            OrderType::StopLimit => wire.limit_price.is_some() && wire.stop_price.is_some(),
        };
        let fill_lots = wire.cumulative_filled.get();
        let average_fill_valid = if fill_lots == 0 {
            wire.weighted_fill_ticks == 0
        } else {
            wire.weighted_fill_ticks
                .checked_div(i128::from(fill_lots))
                .and_then(|ticks| i64::try_from(ticks).ok())
                .is_some_and(|ticks| ticks > 0 && ticks <= wire.maximum_execution_price.get())
        };
        let maximum_fill_valid = match (fill_lots, wire.maximum_fill_price) {
            (0, None) => true,
            (filled, Some(maximum)) if filled > 0 => {
                maximum.get() > 0
                    && maximum.get() <= wire.maximum_execution_price.get()
                    && wire
                        .weighted_fill_ticks
                        .checked_div(i128::from(filled))
                        .and_then(|ticks| i64::try_from(ticks).ok())
                        .is_some_and(|average| maximum.get() >= average)
            }
            _ => false,
        };
        let execution_price_bound = ExecutionPriceBound::try_new(wire.maximum_execution_price)
            .map_err(|_| PaperStateError::InvalidTransition)?;
        let risk_policy = RiskPolicyIdentity::try_from_recovery(
            wire.risk_policy_digest,
            wire.risk_policy_version,
        )
        .map_err(|_| PaperStateError::InvalidTransition)?;
        if !price_shape_valid
            || wire.reference_price.get() <= 0
            || !execution_price_bound.permits(wire.reference_price)
            || wire.limit_price.is_some_and(|price| price.get() <= 0)
            || wire.stop_price.is_some_and(|price| price.get() <= 0)
            || !(0..=10_000).contains(&wire.maximum_slippage.get())
            || wire.eligible_at < wire.accepted_at
            || wire.expires_at < wire.accepted_at
            || wire.accepted_sequence == 0
            || wire.cumulative_fee.currency() != wire.terms.quote_currency()
            || wire.cumulative_fee.amount().is_sign_negative()
            || !average_fill_valid
            || !maximum_fill_valid
            || wire.weighted_fill_ticks.is_negative()
            || wire.account_revision == 0
            || wire.assessment_digest == [0; 32]
            || wire.evidence_binding_digest == [0; 32]
            || wire.valid_until < wire.market_observed_at
            || wire.state == PaperOrderState::New
            || (matches!(wire.order_type, OrderType::Market | OrderType::Limit) && !wire.triggered)
        {
            return Err(PaperStateError::InvalidTransition);
        }
        let lifecycle = PaperOrderLifecycle::try_restore(
            wire.state,
            wire.quantity,
            wire.cumulative_filled,
            wire.revision,
            wire.last_sequence,
        )?;
        Ok(Self {
            approval_id: wire.approval_id,
            order_id: wire.order_id,
            client_order_id: wire.client_order_id,
            account_id: wire.account_id,
            account_revision: wire.account_revision,
            terms: wire.terms,
            side: wire.side,
            order_type: wire.order_type,
            quantity: wire.quantity,
            limit_price: wire.limit_price,
            stop_price: wire.stop_price,
            time_in_force: wire.time_in_force,
            maximum_slippage: wire.maximum_slippage,
            intent_digest: OrderIntentDigest::from_bytes(wire.intent_digest),
            reference_price: wire.reference_price,
            execution_price_bound,
            accepted_at: wire.accepted_at,
            eligible_at: wire.eligible_at,
            expires_at: wire.expires_at,
            accepted_sequence: wire.accepted_sequence,
            cancel_effective_at: wire.cancel_effective_at,
            triggered: wire.triggered,
            resting: wire.resting,
            lifecycle,
            cumulative_fee: wire.cumulative_fee,
            weighted_fill_ticks: wire.weighted_fill_ticks,
            maximum_fill_price: wire.maximum_fill_price,
            strategy_id: wire.strategy_id,
            model_id: wire.model_id,
            assessment_digest: wire.assessment_digest,
            evidence_binding_digest: wire.evidence_binding_digest,
            risk_policy,
            market_observed_at: wire.market_observed_at,
            valid_until: wire.valid_until,
        })
    }

    pub(crate) fn recovered_dispatch_order(
        &self,
    ) -> Result<RecoveredDispatchOrder, PaperStateError> {
        let lifecycle = ReconciledOrder::try_new(
            self.order_id,
            self.reconciled_status(),
            self.lifecycle.cumulative_filled(),
            self.average_fill_price(),
            self.maximum_fill_price,
            self.cumulative_fee,
        )
        .map_err(|_| PaperStateError::InvalidTransition)?;
        RecoveredDispatchOrder::try_new(
            self.approval_id,
            self.order_id,
            self.account_id,
            self.terms.instrument_id(),
            self.intent_digest,
            self.account_revision,
            self.quantity,
            self.execution_price_bound,
            self.terms.settlement_currency(),
            lifecycle,
            self.strategy_id,
            self.model_id,
            self.assessment_digest,
            self.evidence_binding_digest,
            self.risk_policy,
            self.market_observed_at,
            self.valid_until,
            self.accepted_at,
        )
        .map_err(|_| PaperStateError::InvalidTransition)
    }
}
