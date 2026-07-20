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
use crate::clock::system_now;
use crate::{ExecutionAdapter, ExecutionAdapterError, ExecutionAuditKind, ExecutionAuditReason};

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
        process_command(&adapter, &registry, command).await;
    }
}

async fn process_command(
    adapter: &Arc<dyn ExecutionAdapter>,
    registry: &Arc<Mutex<DispatchRegistry>>,
    command: DispatchCommand,
) {
    let DispatchCommand {
        approval,
        audit,
        context,
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
    let approval_id = parts.approval_id;
    let order_id = parts.intent.order_id();
    let fail_safe = match parts.reservation.begin_submission() {
        Ok(guard) => guard,
        Err(_) => {
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
    };
    {
        let mut registry = match try_registry(registry) {
            Ok(registry) => registry,
            Err(_) => {
                parts.reservation.mark_known_not_accepted();
                fail_safe.disarm();
                mark_terminal(registry, approval_id, now.wall);
                commit_dispatch_audit(
                    audit,
                    ExecutionAuditKind::DispatchRejected,
                    context,
                    now.wall,
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
                now.wall,
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
                now.wall,
                &[ExecutionAuditReason::RegistryUnavailable],
            );
            return;
        }
        record.state = DispatchState::Submitted;
        record.last_transition_at = now.wall;
        record.reservation = Some(parts.reservation);
    }
    let outcome = SubmissionOutcomeFailSafe::new(
        Arc::clone(registry),
        approval_id,
        fail_safe,
        audit,
        context,
        now.wall,
    );
    let dispatch = dispatch_order_from_approval(
        approval_id,
        parts.intent,
        parts.market,
        parts.authority.into_evidence(),
        parts.policy,
        parts.valid_until,
        now.wall,
    );
    let result = adapter.submit(dispatch).await;
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
            outcome.complete_uncertain(post_call.wall, &[adapter_reason(error)]);
        }
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
