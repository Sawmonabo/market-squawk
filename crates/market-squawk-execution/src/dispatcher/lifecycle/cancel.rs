//! Accepted-order cancellation and response validation.

use std::sync::Arc;

use market_squawk_domain::{OrderId, Timestamp};

use super::LifecycleOutcomeFailSafe;
use crate::clock::system_now;
use crate::dispatcher::attempt::attempt_adapter_call;
use crate::dispatcher::{
    DispatchRecord, DispatchState, ExecutionDispatchError, ExecutionDispatcher, adapter_reason,
    commit_dispatch_audit, try_registry,
};
use crate::{
    CancelOrder, CancelReceipt, CancelStatus, ExecutionAdapterError, ExecutionAuditKind,
    ExecutionAuditReason, ReconciledOrder, ReconciledOrderStatus,
};

impl ExecutionDispatcher {
    /// Cancels a tracked accepted order without exposing the adapter.
    pub async fn cancel(&self, order_id: OrderId) -> Result<CancelReceipt, ExecutionDispatchError> {
        let task_permit = self.try_reserve_adapter_task()?;
        let audit = self
            .audit
            .try_reserve()
            .map_err(|_| ExecutionDispatchError::AuditUnavailable)?;
        let fail_safe = {
            let mut registry = try_registry(&self.registry)?;
            let record = registry
                .entries
                .values_mut()
                .find(|record| record.order_id == order_id)
                .ok_or(ExecutionDispatchError::OrderNotTracked)?;
            if record.state != DispatchState::Accepted {
                return Err(ExecutionDispatchError::OrderNotCancelable);
            }
            let invoked = match system_now() {
                Ok(invoked) if invoked.wall >= record.last_transition_at => invoked,
                _ => {
                    commit_dispatch_audit(
                        audit,
                        ExecutionAuditKind::DispatchRejected,
                        record.audit_context,
                        record.last_transition_at,
                        &[ExecutionAuditReason::ClockFailure],
                    );
                    return Err(ExecutionDispatchError::ClockUnavailable);
                }
            };
            let reservation = record
                .reservation
                .as_ref()
                .ok_or(ExecutionDispatchError::RegistryInvariant)?;
            let fail_safe = LifecycleOutcomeFailSafe::new(
                Arc::clone(&self.registry),
                order_id,
                reservation.outcome_fail_safe(),
                audit,
                record.audit_context,
                invoked.wall,
            );
            record.state = DispatchState::Canceling;
            record.last_transition_at = invoked.wall;
            (fail_safe, invoked)
        };
        let (fail_safe, invoked) = fail_safe;
        let operation = super::super::operation(
            self.operation_deadline,
            self.control_cancellation.child_token(),
        )?;
        let deadline = operation.deadline();
        let cancellation = operation.cancellation();
        let (result, deadline_exceeded) =
            attempt_adapter_call(
                &self.adapter,
                deadline,
                &cancellation,
                task_permit,
                move |adapter| async move {
                    adapter.cancel(CancelOrder::new(order_id, operation)).await
                },
            )
            .await;
        if deadline_exceeded {
            fail_safe.fail_uncertain(
                invoked.wall,
                &[ExecutionAuditReason::OperationDeadlineExceeded],
            );
            return Err(ExecutionDispatchError::OperationDeadlineExceeded);
        }
        let post_call = match system_now() {
            Ok(post_call)
                if post_call.wall >= invoked.wall && post_call.monotonic >= invoked.monotonic =>
            {
                post_call
            }
            _ => {
                fail_safe.fail_uncertain(invoked.wall, &[ExecutionAuditReason::ClockFailure]);
                return Err(ExecutionDispatchError::ClockUnavailable);
            }
        };
        if cancellation.is_cancelled() || tokio::time::Instant::now() >= deadline {
            cancellation.cancel();
            fail_safe.fail_uncertain(
                post_call.wall,
                &[ExecutionAuditReason::OperationDeadlineExceeded],
            );
            return Err(ExecutionDispatchError::OperationDeadlineExceeded);
        }
        let mut registry = match try_registry(&self.registry) {
            Ok(registry) => registry,
            Err(error) => {
                fail_safe
                    .fail_uncertain(post_call.wall, &[ExecutionAuditReason::RegistryUnavailable]);
                return Err(error);
            }
        };
        let Some(record) = registry
            .entries
            .values_mut()
            .find(|record| record.order_id == order_id)
        else {
            drop(registry);
            fail_safe.fail_uncertain(post_call.wall, &[ExecutionAuditReason::RegistryUnavailable]);
            return Err(ExecutionDispatchError::RegistryInvariant);
        };
        if record.state != DispatchState::Canceling {
            drop(registry);
            fail_safe.fail_uncertain(post_call.wall, &[ExecutionAuditReason::RegistryUnavailable]);
            return Err(ExecutionDispatchError::RegistryInvariant);
        }
        let invalid_reason = result.as_ref().ok().and_then(|receipt| {
            cancel_observation_invalid_reason(record, *receipt, post_call.wall)
        });
        match result {
            Ok(receipt) if invalid_reason.is_none() => {
                if retain_cancel_observation(record, receipt).is_err() {
                    if let Some(reservation) = record.reservation.as_ref() {
                        reservation.mark_reconciliation_required();
                    }
                    record.state = DispatchState::Reconciliation;
                    record.last_transition_at = post_call.wall;
                    fail_safe.complete_uncertain(
                        ExecutionAuditKind::DispatchUncertain,
                        post_call.wall,
                        &[ExecutionAuditReason::ReconciliationRequired],
                    );
                    return Err(ExecutionDispatchError::ReceiptMismatch);
                }
                let reservation = record
                    .reservation
                    .as_ref()
                    .ok_or(ExecutionDispatchError::RegistryInvariant)?;
                record.last_transition_at = receipt.observed_at();
                match receipt.status() {
                    CancelStatus::Pending => {
                        if cancel_has_financial_effect(receipt) {
                            reservation.mark_reconciliation_required();
                            record.state = DispatchState::Reconciliation;
                            fail_safe.complete_uncertain(
                                ExecutionAuditKind::DispatchUncertain,
                                receipt.observed_at(),
                                &[ExecutionAuditReason::ReconciliationRequired],
                            );
                        } else {
                            record.state = DispatchState::Accepted;
                            fail_safe.complete_known(
                                ExecutionAuditKind::CancelAccepted,
                                receipt.observed_at(),
                                &[],
                            );
                        }
                    }
                    CancelStatus::Canceled => {
                        if cancel_has_financial_effect(receipt) {
                            reservation.mark_reconciliation_required();
                            record.state = DispatchState::Reconciliation;
                            fail_safe.complete_uncertain(
                                ExecutionAuditKind::DispatchUncertain,
                                receipt.observed_at(),
                                &[ExecutionAuditReason::ReconciliationRequired],
                            );
                        } else {
                            reservation.mark_terminal_unfilled();
                            record.state = DispatchState::Terminal;
                            fail_safe.complete_known(
                                ExecutionAuditKind::CancelTerminal,
                                receipt.observed_at(),
                                &[],
                            );
                        }
                    }
                    CancelStatus::AlreadyTerminal => {
                        reservation.mark_reconciliation_required();
                        record.state = DispatchState::Reconciliation;
                        fail_safe.complete_uncertain(
                            ExecutionAuditKind::DispatchUncertain,
                            receipt.observed_at(),
                            &[ExecutionAuditReason::ReconciliationRequired],
                        );
                    }
                }
                Ok(receipt)
            }
            Ok(_) => {
                let reservation = record
                    .reservation
                    .as_ref()
                    .ok_or(ExecutionDispatchError::RegistryInvariant)?;
                reservation.mark_reconciliation_required();
                record.state = DispatchState::Reconciliation;
                record.last_transition_at = post_call.wall;
                fail_safe.complete_uncertain(
                    ExecutionAuditKind::DispatchUncertain,
                    post_call.wall,
                    &[invalid_reason.unwrap_or(ExecutionAuditReason::ReceiptMismatch)],
                );
                Err(ExecutionDispatchError::ReceiptMismatch)
            }
            Err(
                error @ (ExecutionAdapterError::Rejected
                | ExecutionAdapterError::KnownFailure
                | ExecutionAdapterError::NotAttemptedBusy),
            ) => {
                record.state = DispatchState::Accepted;
                record.last_transition_at = post_call.wall;
                fail_safe.complete_known(
                    ExecutionAuditKind::DispatchKnownFailure,
                    post_call.wall,
                    &[adapter_reason(error)],
                );
                Err(ExecutionDispatchError::Adapter(error))
            }
            Err(error) => {
                let reservation = record
                    .reservation
                    .as_ref()
                    .ok_or(ExecutionDispatchError::RegistryInvariant)?;
                reservation.mark_reconciliation_required();
                record.state = DispatchState::Reconciliation;
                record.last_transition_at = post_call.wall;
                fail_safe.complete_uncertain(
                    ExecutionAuditKind::DispatchUncertain,
                    post_call.wall,
                    &[adapter_reason(error)],
                );
                Err(ExecutionDispatchError::Adapter(error))
            }
        }
    }
}

fn cancel_observation_invalid_reason(
    record: &DispatchRecord,
    receipt: CancelReceipt,
    post_call_at: Timestamp,
) -> Option<ExecutionAuditReason> {
    if receipt.order_id() != record.order_id {
        return Some(ExecutionAuditReason::ReceiptMismatch);
    }
    if receipt.observed_at() < record.last_transition_at || receipt.observed_at() > post_call_at {
        return Some(ExecutionAuditReason::ObservationTimestampInvalid);
    }
    let cumulative_regression = record.lifecycle.is_some_and(|previous| {
        receipt.cumulative_filled().get() < previous.cumulative_filled().get()
            || receipt.cumulative_fees().currency() != previous.cumulative_fees().currency()
            || receipt.cumulative_fees().amount() < previous.cumulative_fees().amount()
            || receipt.maximum_fill_price() < previous.maximum_fill_price()
    });
    if cumulative_regression
        || receipt.cumulative_filled().get() < 0
        || receipt.cumulative_filled().get() > record.requested_quantity.get()
        || record.settlement_currency != Some(receipt.cumulative_fees().currency())
        || receipt
            .maximum_fill_price()
            .is_some_and(|price| !record.execution_price_bound.permits(price))
    {
        return Some(ExecutionAuditReason::ReconciliationRequired);
    }
    let status = match receipt.status() {
        CancelStatus::Pending if receipt.cumulative_filled().get() == 0 => {
            ReconciledOrderStatus::Open
        }
        CancelStatus::Pending => ReconciledOrderStatus::PartiallyFilled,
        CancelStatus::Canceled => ReconciledOrderStatus::Canceled,
        CancelStatus::AlreadyTerminal => ReconciledOrderStatus::Unknown,
    };
    ReconciledOrder::try_new(
        receipt.order_id(),
        status,
        receipt.cumulative_filled(),
        receipt.average_fill_price(),
        receipt.maximum_fill_price(),
        receipt.cumulative_fees(),
    )
    .err()
    .map(|_| ExecutionAuditReason::ReconciliationRequired)
}

fn retain_cancel_observation(
    record: &mut DispatchRecord,
    receipt: CancelReceipt,
) -> Result<(), ()> {
    let status = match receipt.status() {
        CancelStatus::Pending if receipt.cumulative_filled().get() == 0 => {
            ReconciledOrderStatus::Open
        }
        CancelStatus::Pending => ReconciledOrderStatus::PartiallyFilled,
        CancelStatus::Canceled => ReconciledOrderStatus::Canceled,
        CancelStatus::AlreadyTerminal => ReconciledOrderStatus::Unknown,
    };
    record.lifecycle = Some(
        ReconciledOrder::try_new(
            receipt.order_id(),
            status,
            receipt.cumulative_filled(),
            receipt.average_fill_price(),
            receipt.maximum_fill_price(),
            receipt.cumulative_fees(),
        )
        .map_err(|_| ())?,
    );
    Ok(())
}

fn cancel_has_financial_effect(receipt: CancelReceipt) -> bool {
    receipt.cumulative_filled().get() != 0 || !receipt.cumulative_fees().amount().is_zero()
}
