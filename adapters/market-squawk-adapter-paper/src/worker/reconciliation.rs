//! Reconciliation, checkpoint projection, audit admission, and deterministic ordering helpers.

use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::num::NonZeroU64;
use std::sync::atomic::Ordering as AtomicOrdering;

use market_squawk_domain::{Money, OrderId, OrderSide, PriceTicks, QuantityLots, Timestamp};
use market_squawk_execution::{
    ACCOUNT_REPLACEMENT_SCHEMA_VERSION, CancelReceipt, CancelStatus, ExecutionAdapterError,
    ExecutionState, ExecutionStateSourceBinding, ReconciledOrder, ReconciledOrderStatus,
};
use rust_decimal::Decimal;
use sha2::{Digest, Sha256};
use tokio::sync::mpsc;

use super::{PaperWorker, WorkerMarketUpdate};
use crate::audit::{PaperAuditKind, PaperAuditRecord};
use crate::order::PaperOrder;
use crate::slippage::adverse_bound;
use crate::snapshot::{PaperExecutionCheckpoint, PaperExecutionSnapshot};
use crate::{PaperExecutionConfig, PaperOrderState};

impl PaperWorker {
    pub(super) fn reconcile(
        &self,
        observed_at: Timestamp,
        order_ids: &[OrderId],
    ) -> Result<ExecutionState, ExecutionAdapterError> {
        let currency = self.config.input().reporting_currency;
        let mut orders = Vec::new();
        orders
            .try_reserve_exact(order_ids.len())
            .map_err(|_| ExecutionAdapterError::KnownFailure)?;
        let mut affected_accounts = Vec::new();
        affected_accounts
            .try_reserve_exact(order_ids.len())
            .map_err(|_| ExecutionAdapterError::KnownFailure)?;
        for order_id in order_ids {
            let reconciled = if let Some(order) = self.state.orders.get(order_id) {
                ReconciledOrder::try_new(
                    *order_id,
                    order.reconciled_status(),
                    order.lifecycle.cumulative_filled(),
                    order.average_fill_price(),
                    order.cumulative_fee,
                )
            } else {
                ReconciledOrder::try_new(
                    *order_id,
                    ReconciledOrderStatus::Unknown,
                    QuantityLots::new(0).map_err(|_| ExecutionAdapterError::KnownFailure)?,
                    None,
                    Money::new(Decimal::ZERO, currency),
                )
            }
            .map_err(|_| ExecutionAdapterError::KnownFailure)?;
            if reconciled.cumulative_filled().get() != 0
                || !reconciled.cumulative_fees().amount().is_zero()
            {
                let account_id = self
                    .state
                    .orders
                    .get(order_id)
                    .map(|order| order.account_id)
                    .ok_or(ExecutionAdapterError::KnownFailure)?;
                affected_accounts.push(account_id);
            }
            orders.push(reconciled);
        }
        affected_accounts.sort_unstable();
        affected_accounts.dedup();
        if affected_accounts.is_empty() {
            return ExecutionState::try_new(
                observed_at,
                orders,
                self.state.reconciliation_required,
            )
            .map_err(|_| ExecutionAdapterError::KnownFailure);
        }
        let mut accounts = Vec::new();
        accounts
            .try_reserve_exact(affected_accounts.len())
            .map_err(|_| ExecutionAdapterError::KnownFailure)?;
        for account_id in affected_accounts {
            accounts.push(
                self.state
                    .ledger
                    .reconciled_account_state(account_id)
                    .map_err(|_| ExecutionAdapterError::ReconciliationRequired)?,
            );
        }
        let checkpoint = self.checkpoint();
        let snapshot_digest = checkpoint
            .recovery_input_digest()
            .map_err(|_| ExecutionAdapterError::KnownFailure)?;
        let snapshot_sequence = NonZeroU64::new(self.state.sequence)
            .ok_or(ExecutionAdapterError::ReconciliationRequired)?;
        let source = ExecutionStateSourceBinding::try_new(
            ACCOUNT_REPLACEMENT_SCHEMA_VERSION,
            self.config.digest(),
            snapshot_sequence,
            snapshot_digest,
        )
        .map_err(|_| ExecutionAdapterError::KnownFailure)?;
        ExecutionState::try_new_complete(
            observed_at,
            orders,
            accounts,
            source,
            self.state.reconciliation_required,
        )
        .map_err(|_| ExecutionAdapterError::KnownFailure)
    }

    pub(super) fn snapshot(&self) -> PaperExecutionSnapshot {
        PaperExecutionSnapshot::from_state(
            self.config.digest(),
            self.state.sequence,
            self.state.reconciliation_required,
            &self.state.orders,
            &self.state.fills,
            &self.state.ledger,
        )
    }

    pub(super) fn checkpoint(&self) -> PaperExecutionCheckpoint {
        PaperExecutionCheckpoint {
            schema_version: PaperExecutionConfig::CHECKPOINT_SCHEMA_VERSION,
            configuration_digest: self.config.digest(),
            complete: true,
            sequence: self.state.sequence,
            reconciliation_required: self.state.reconciliation_required,
            orders: self.state.orders.clone(),
            fills: self.state.fills.clone(),
            ledger: self.state.ledger.clone(),
            idempotency: self.state.idempotency.clone(),
        }
    }

    pub(super) fn next_mutation_sequence(&self) -> Result<u64, ExecutionAdapterError> {
        self.state
            .sequence
            .checked_add(1)
            .ok_or(ExecutionAdapterError::ReconciliationRequired)
    }

    pub(super) fn refresh_audit_health(&mut self) {
        if self.audit_failed.load(AtomicOrdering::Acquire) {
            self.state.reconciliation_required = true;
        }
    }

    pub(super) fn admit_audit(
        &mut self,
        record: PaperAuditRecord,
    ) -> Result<(), ExecutionAdapterError> {
        self.refresh_audit_health();
        if self.state.reconciliation_required {
            return Err(ExecutionAdapterError::ReconciliationRequired);
        }
        match self.audit.try_send(record) {
            Ok(()) => Ok(()),
            Err(mpsc::error::TrySendError::Full(_)) => Err(ExecutionAdapterError::NotAttemptedBusy),
            Err(mpsc::error::TrySendError::Closed(_)) => {
                self.state.reconciliation_required = true;
                Err(ExecutionAdapterError::ReconciliationRequired)
            }
        }
    }

    pub(super) fn admit_committed_event_audit(&mut self, record: PaperAuditRecord) -> bool {
        if self.admit_audit(record).is_ok() {
            true
        } else {
            // The market or due event was already accepted by the worker. Without its mandatory
            // before-mutation audit record, retrying or silently dropping the event could produce
            // a different fill/cancel outcome, so the state must fail closed for reconciliation.
            self.state.reconciliation_required = true;
            false
        }
    }
}

pub(super) fn reservation_price(order: &PaperOrder) -> Result<PriceTicks, ExecutionAdapterError> {
    let adverse = adverse_bound(order.reference_price, order.side, order.maximum_slippage)
        .map_err(|_| ExecutionAdapterError::Rejected)?;
    Ok(match (order.side, order.limit_price) {
        (OrderSide::Buy, Some(limit)) => adverse.min(limit),
        (OrderSide::Sell, Some(limit)) => adverse.max(limit),
        (_, None) => adverse,
    })
}

pub(super) fn cancel_receipt(
    order: &PaperOrder,
    status: CancelStatus,
    observed_at: Timestamp,
) -> Result<CancelReceipt, ExecutionAdapterError> {
    CancelReceipt::try_new(
        order.order_id,
        status,
        observed_at,
        order.lifecycle.cumulative_filled(),
        order.average_fill_price(),
        order.cumulative_fee,
    )
    .map_err(|_| ExecutionAdapterError::KnownFailure)
}

pub(super) fn state_audit(
    config: &PaperExecutionConfig,
    sequence: u64,
    previous: &PaperOrder,
    new: &PaperOrder,
    kind: PaperAuditKind,
    event_at: Timestamp,
) -> PaperAuditRecord {
    PaperAuditRecord::new(
        sequence,
        Some(previous.order_id),
        kind,
        Some(previous.lifecycle.state()),
        Some(new.lifecycle.state()),
        event_at,
        None,
        config.digest(),
        new.input_digest(),
    )
}

pub(super) fn market_digest(order: &PaperOrder, event: WorkerMarketUpdate) -> [u8; 32] {
    let mut digest = Sha256::new();
    digest.update(b"market-squawk/paper-market-mutation/v1\0");
    digest.update(order.input_digest());
    digest.update(event.sequence.to_be_bytes());
    digest.update(event.update.assessment_digest());
    digest.update(
        event
            .update
            .market()
            .observed_at()
            .unix_nanos()
            .to_be_bytes(),
    );
    digest.finalize().into()
}

pub(super) fn order_priority(
    orders: &BTreeMap<OrderId, PaperOrder>,
    left: OrderId,
    right: OrderId,
) -> Ordering {
    let (Some(left_order), Some(right_order)) = (orders.get(&left), orders.get(&right)) else {
        return left.cmp(&right);
    };
    side_rank(left_order.side)
        .cmp(&side_rank(right_order.side))
        .then_with(|| price_priority(left_order, right_order))
        .then_with(|| left_order.eligible_at.cmp(&right_order.eligible_at))
        .then_with(|| {
            left_order
                .accepted_sequence
                .cmp(&right_order.accepted_sequence)
        })
        .then_with(|| left.cmp(&right))
}

const fn side_rank(side: OrderSide) -> u8 {
    match side {
        OrderSide::Buy => 0,
        OrderSide::Sell => 1,
    }
}

fn price_priority(left: &PaperOrder, right: &PaperOrder) -> Ordering {
    match (left.limit_price, right.limit_price, left.side) {
        (None, None, _) => Ordering::Equal,
        (None, Some(_), _) => Ordering::Less,
        (Some(_), None, _) => Ordering::Greater,
        (Some(left_price), Some(right_price), OrderSide::Buy) => right_price.cmp(&left_price),
        (Some(left_price), Some(right_price), OrderSide::Sell) => left_price.cmp(&right_price),
    }
}

pub(super) const fn is_terminal(state: PaperOrderState) -> bool {
    matches!(
        state,
        PaperOrderState::Filled
            | PaperOrderState::Canceled
            | PaperOrderState::Rejected
            | PaperOrderState::Expired
    )
}
