//! One-use submission worker and in-flight adapter ownership.

use std::sync::{Arc, Mutex};

use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use super::{
    DispatchCommand, DispatchRegistry, DispatchState, adapter_reason, commit_dispatch_audit,
    try_registry,
};
use crate::account::AccountSubmissionFailSafe;
use crate::adapter::dispatch_order_from_approval;
use crate::audit::{ExecutionAuditContext, ExecutionAuditPermit};
use crate::clock::{deadline_expired, system_now};
use crate::dispatcher::attempt::attempt_adapter_call;
use crate::{
    ExecutionAdapter, ExecutionAdapterError, ExecutionAuditKind, ExecutionAuditReason,
    ExecutionOperation, ExecutionReceipt, ExecutionTaskPermit, ExecutionTaskReaper,
};

#[derive(Debug)]
struct SubmissionOutcomeFailSafe {
    registry: Arc<Mutex<DispatchRegistry>>,
    approval_id: market_squawk_domain::ApprovalId,
    account: Option<AccountSubmissionFailSafe>,
    audit: Option<ExecutionAuditPermit>,
    context: ExecutionAuditContext,
    fallback_at: market_squawk_domain::Timestamp,
    armed: bool,
}

impl SubmissionOutcomeFailSafe {
    fn new(
        registry: Arc<Mutex<DispatchRegistry>>,
        approval_id: market_squawk_domain::ApprovalId,
        account: AccountSubmissionFailSafe,
        audit: ExecutionAuditPermit,
        context: ExecutionAuditContext,
        fallback_at: market_squawk_domain::Timestamp,
    ) -> Self {
        Self {
            registry,
            approval_id,
            account: Some(account),
            audit: Some(audit),
            context,
            fallback_at,
            armed: true,
        }
    }

    fn complete_known(
        mut self,
        kind: ExecutionAuditKind,
        observed_at: market_squawk_domain::Timestamp,
        reasons: &[ExecutionAuditReason],
    ) {
        self.armed = false;
        if let Some(account) = self.account.take() {
            account.disarm();
        }
        if let Some(audit) = self.audit.take() {
            commit_dispatch_audit(audit, kind, self.context, observed_at, reasons);
        }
    }

    fn complete_uncertain(
        mut self,
        observed_at: market_squawk_domain::Timestamp,
        reasons: &[ExecutionAuditReason],
    ) {
        self.armed = false;
        drop(self.account.take());
        if let Some(audit) = self.audit.take() {
            commit_dispatch_audit(
                audit,
                ExecutionAuditKind::DispatchUncertain,
                self.context,
                observed_at,
                reasons,
            );
        }
    }

    fn fail_uncertain(
        self,
        observed_at: market_squawk_domain::Timestamp,
        reasons: &[ExecutionAuditReason],
    ) {
        mark_reconciliation(&self.registry, self.approval_id, observed_at);
        self.complete_uncertain(observed_at, reasons);
    }
}

impl Drop for SubmissionOutcomeFailSafe {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        mark_reconciliation(&self.registry, self.approval_id, self.fallback_at);
        drop(self.account.take());
        if let Some(audit) = self.audit.take() {
            commit_dispatch_audit(
                audit,
                ExecutionAuditKind::DispatchUncertain,
                self.context,
                self.fallback_at,
                &[ExecutionAuditReason::ReconciliationRequired],
            );
        }
        self.armed = false;
    }
}

pub(super) async fn run_worker(
    adapter: Arc<dyn ExecutionAdapter>,
    registry: Arc<Mutex<DispatchRegistry>>,
    mut receiver: mpsc::Receiver<DispatchCommand>,
    cancellation: CancellationToken,
    task_reaper: ExecutionTaskReaper,
) {
    let mut draining = false;
    loop {
        let command = if draining {
            receiver.recv().await
        } else {
            tokio::select! {
                biased;
                () = cancellation.cancelled() => {
                    receiver.close();
                    draining = true;
                    continue;
                }
                command = receiver.recv() => command,
            }
        };
        let Some(command) = command else {
            break;
        };
        process_command(&adapter, &registry, &task_reaper, command).await;
    }
}

async fn process_command(
    adapter: &Arc<dyn ExecutionAdapter>,
    registry: &Arc<Mutex<DispatchRegistry>>,
    task_reaper: &ExecutionTaskReaper,
    command: DispatchCommand,
) {
    let DispatchCommand {
        approval,
        audit,
        context,
        operation_deadline,
        operation_cancellation,
        _bytes,
    } = command;
    let now = match system_now() {
        Ok(now) => now,
        Err(_) => {
            mark_terminal(
                registry,
                approval.approval_id(),
                context.market_observed_at(),
            );
            commit_dispatch_audit(
                audit,
                ExecutionAuditKind::DispatchRejected,
                context,
                context.market_observed_at(),
                &[ExecutionAuditReason::ClockFailure],
            );
            return;
        }
    };
    if approval.validate_current(now).is_err() {
        mark_terminal(registry, approval.approval_id(), now.wall);
        commit_dispatch_audit(
            audit,
            ExecutionAuditKind::DispatchRejected,
            context,
            now.wall,
            &[ExecutionAuditReason::ApprovalInvalid],
        );
        return;
    }
    let parts = approval.into_parts();
    let operation_deadline =
        effective_operation_deadline(parts.monotonic_deadline.into(), operation_deadline);
    let approval_id = parts.approval_id;
    let order_id = parts.intent.order_id();
    let account_revision = parts.reservation.expected_account_revision();
    if !parts
        .market
        .execution_price(parts.intent.side())
        .is_some_and(|price| parts.execution_price_bound.permits(price))
    {
        parts.reservation.mark_known_not_accepted();
        mark_terminal(registry, approval_id, now.wall);
        commit_dispatch_audit(
            audit,
            ExecutionAuditKind::DispatchRejected,
            context,
            now.wall,
            &[ExecutionAuditReason::ApprovalInvalid],
        );
        return;
    }
    if operation_cancellation.is_cancelled() || tokio::time::Instant::now() >= operation_deadline {
        parts.reservation.mark_known_not_accepted();
        mark_terminal(registry, approval_id, now.wall);
        commit_dispatch_audit(
            audit,
            ExecutionAuditKind::DispatchRejected,
            context,
            now.wall,
            &[ExecutionAuditReason::OperationDeadlineExceeded],
        );
        return;
    }
    let task_permit = if adapter.is_cooperative() {
        None
    } else {
        match task_reaper.try_reserve() {
            Ok(permit) => Some(permit),
            Err(_) => {
                parts.reservation.mark_known_not_accepted();
                mark_terminal(registry, approval_id, now.wall);
                commit_dispatch_audit(
                    audit,
                    ExecutionAuditKind::DispatchKnownFailure,
                    context,
                    now.wall,
                    &[ExecutionAuditReason::TaskOwnershipSaturated],
                );
                return;
            }
        }
    };
    let final_now = match system_now() {
        Ok(final_now) => final_now,
        Err(_) => {
            mark_terminal(registry, approval_id, now.wall);
            commit_dispatch_audit(
                audit,
                ExecutionAuditKind::DispatchRejected,
                context,
                now.wall,
                &[ExecutionAuditReason::ClockFailure],
            );
            return;
        }
    };
    if parts.authority.validate_current().is_err()
        || deadline_expired(final_now, parts.valid_until, parts.monotonic_deadline)
        || operation_cancellation.is_cancelled()
        || tokio::time::Instant::now() >= operation_deadline
    {
        mark_terminal(registry, approval_id, final_now.wall);
        commit_dispatch_audit(
            audit,
            ExecutionAuditKind::DispatchRejected,
            context,
            final_now.wall,
            &[ExecutionAuditReason::ApprovalInvalid],
        );
        return;
    }
    let fail_safe = match parts
        .reservation
        .begin_submission(parts.valid_until, parts.monotonic_deadline)
    {
        Ok(guard) => guard,
        Err(_) => {
            mark_terminal(registry, approval_id, final_now.wall);
            commit_dispatch_audit(
                audit,
                ExecutionAuditKind::DispatchRejected,
                context,
                final_now.wall,
                &[ExecutionAuditReason::ApprovalInvalid],
            );
            return;
        }
    };
    {
        let mut registry = match try_registry(registry) {
            Ok(registry) => registry,
            Err(_) => {
                parts.reservation.mark_known_not_accepted();
                fail_safe.disarm();
                mark_terminal(registry, approval_id, final_now.wall);
                commit_dispatch_audit(
                    audit,
                    ExecutionAuditKind::DispatchRejected,
                    context,
                    final_now.wall,
                    &[ExecutionAuditReason::RegistryUnavailable],
                );
                return;
            }
        };
        let Some(record) = registry.entries.get_mut(&approval_id) else {
            parts.reservation.mark_known_not_accepted();
            fail_safe.disarm();
            commit_dispatch_audit(
                audit,
                ExecutionAuditKind::DispatchRejected,
                context,
                final_now.wall,
                &[ExecutionAuditReason::RegistryUnavailable],
            );
            return;
        };
        if record.state != DispatchState::Queued || record.reservation.is_some() {
            parts.reservation.mark_known_not_accepted();
            fail_safe.disarm();
            record.state = DispatchState::Terminal;
            commit_dispatch_audit(
                audit,
                ExecutionAuditKind::DispatchRejected,
                context,
                final_now.wall,
                &[ExecutionAuditReason::RegistryUnavailable],
            );
            return;
        }
        record.state = DispatchState::Submitted;
        record.last_transition_at = final_now.wall;
        record.reservation = Some(super::DispatchReservation::Live(parts.reservation));
    }
    let outcome = SubmissionOutcomeFailSafe::new(
        Arc::clone(registry),
        approval_id,
        fail_safe,
        audit,
        context,
        final_now.wall,
    );
    let dispatch = dispatch_order_from_approval(
        approval_id,
        parts.intent,
        parts.market,
        parts.execution_price_bound,
        parts.authority.into_evidence(),
        parts.policy,
        parts.valid_until,
        final_now.wall,
        account_revision,
        ExecutionOperation::new(operation_deadline, operation_cancellation.clone()),
    );
    let (result, deadline_exceeded) = attempt_submit(
        adapter,
        dispatch,
        operation_deadline,
        &operation_cancellation,
        task_permit,
    )
    .await;
    let post_call = match system_now() {
        Ok(reading) => reading,
        Err(_) => {
            outcome.fail_uncertain(now.wall, &[ExecutionAuditReason::ClockFailure]);
            return;
        }
    };
    if post_call.wall < now.wall || post_call.monotonic < now.monotonic {
        outcome.fail_uncertain(now.wall, &[ExecutionAuditReason::ClockFailure]);
        return;
    }
    if operation_cancellation.is_cancelled() || tokio::time::Instant::now() >= operation_deadline {
        operation_cancellation.cancel();
        outcome.fail_uncertain(
            post_call.wall,
            &[ExecutionAuditReason::OperationDeadlineExceeded],
        );
        return;
    }
    let mut registry = match try_registry(registry) {
        Ok(registry) => registry,
        Err(_) => {
            outcome.fail_uncertain(post_call.wall, &[ExecutionAuditReason::RegistryUnavailable]);
            return;
        }
    };
    let Some(record) = registry.entries.get_mut(&approval_id) else {
        drop(registry);
        outcome.fail_uncertain(post_call.wall, &[ExecutionAuditReason::RegistryUnavailable]);
        return;
    };
    let Some(reservation) = record.reservation.as_ref() else {
        drop(registry);
        outcome.fail_uncertain(post_call.wall, &[ExecutionAuditReason::RegistryUnavailable]);
        return;
    };
    match result {
        Ok(receipt)
            if receipt.order_id() == order_id
                && receipt.accepted_at() >= record.last_transition_at
                && receipt.accepted_at() <= post_call.wall =>
        {
            if reservation.mark_accepted().is_err() {
                reservation.mark_reconciliation_required();
                record.state = DispatchState::Reconciliation;
                outcome.complete_uncertain(
                    receipt.accepted_at(),
                    &[ExecutionAuditReason::ReconciliationRequired],
                );
                return;
            }
            record.state = DispatchState::Accepted;
            record.last_transition_at = receipt.accepted_at();
            outcome.complete_known(
                ExecutionAuditKind::DispatchAccepted,
                receipt.accepted_at(),
                &[],
            );
        }
        Ok(receipt) => {
            reservation.mark_reconciliation_required();
            record.state = DispatchState::Reconciliation;
            record.last_transition_at = post_call.wall;
            outcome.complete_uncertain(
                post_call.wall,
                if receipt.order_id() == order_id {
                    &[ExecutionAuditReason::ObservationTimestampInvalid]
                } else {
                    &[ExecutionAuditReason::ReceiptMismatch]
                },
            );
        }
        Err(
            error @ (ExecutionAdapterError::Rejected
            | ExecutionAdapterError::KnownFailure
            | ExecutionAdapterError::NotAttemptedBusy),
        ) => {
            reservation.mark_known_not_accepted();
            record.state = DispatchState::Terminal;
            record.last_transition_at = post_call.wall;
            outcome.complete_known(
                ExecutionAuditKind::DispatchKnownFailure,
                post_call.wall,
                &[adapter_reason(error)],
            );
        }
        Err(error) => {
            reservation.mark_reconciliation_required();
            record.state = DispatchState::Reconciliation;
            record.last_transition_at = post_call.wall;
            outcome.complete_uncertain(
                post_call.wall,
                &[if deadline_exceeded {
                    ExecutionAuditReason::OperationDeadlineExceeded
                } else {
                    adapter_reason(error)
                }],
            );
        }
    }
}

async fn attempt_submit(
    adapter: &Arc<dyn ExecutionAdapter>,
    dispatch: crate::DispatchOrder,
    deadline: tokio::time::Instant,
    cancellation: &CancellationToken,
    task_permit: Option<ExecutionTaskPermit>,
) -> (Result<ExecutionReceipt, ExecutionAdapterError>, bool) {
    attempt_adapter_call(
        adapter,
        deadline,
        cancellation,
        task_permit,
        move |adapter| async move { adapter.submit(dispatch).await },
    )
    .await
}

fn effective_operation_deadline(
    approval_deadline: tokio::time::Instant,
    configured_deadline: tokio::time::Instant,
) -> tokio::time::Instant {
    if approval_deadline < configured_deadline {
        approval_deadline
    } else {
        configured_deadline
    }
}

fn mark_terminal(
    registry: &Arc<Mutex<DispatchRegistry>>,
    approval_id: market_squawk_domain::ApprovalId,
    observed_at: market_squawk_domain::Timestamp,
) {
    let mut registry = match registry.lock() {
        Ok(registry) => registry,
        Err(poisoned) => poisoned.into_inner(),
    };
    if let Some(record) = registry.entries.get_mut(&approval_id) {
        record.state = DispatchState::Terminal;
        record.last_transition_at = record.last_transition_at.max(observed_at);
    }
}

fn mark_reconciliation(
    registry: &Arc<Mutex<DispatchRegistry>>,
    approval_id: market_squawk_domain::ApprovalId,
    observed_at: market_squawk_domain::Timestamp,
) {
    let mut registry = match registry.lock() {
        Ok(registry) => registry,
        Err(poisoned) => poisoned.into_inner(),
    };
    if let Some(record) = registry.entries.get_mut(&approval_id)
        && record.state != DispatchState::Terminal
    {
        record.state = DispatchState::Reconciliation;
        record.last_transition_at = record.last_transition_at.max(observed_at);
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    #[test]
    fn approval_deadline_cannot_be_extended_by_dispatch_configuration() {
        let now = tokio::time::Instant::now();
        let approval = now + Duration::from_millis(1);
        let configured = now + Duration::from_secs(1);

        assert_eq!(
            super::effective_operation_deadline(approval, configured),
            approval
        );
        assert_eq!(
            super::effective_operation_deadline(configured, approval),
            approval
        );
    }
}
