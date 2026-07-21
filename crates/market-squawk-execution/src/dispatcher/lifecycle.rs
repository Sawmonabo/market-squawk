//! Accepted-order cancellation, reconciliation, and fail-closed shutdown ownership.

mod cancel;

use std::sync::{Arc, Mutex};

use market_squawk_domain::{ApprovalId, OrderId, Timestamp};

use super::{
    DispatchRecord, DispatchRegistry, DispatchState, ExecutionDispatchError, ExecutionDispatcher,
    ExecutionDispatcherShutdown, PendingReconciliation, PendingReconciliationStatus,
    adapter_reason, commit_dispatch_audit, try_registry,
};
use crate::account::{AccountOutcomeFailSafe, AccountRiskReservation};
use crate::audit::{ExecutionAuditContext, ExecutionAuditPermit};
use crate::clock::system_now;
use crate::dispatcher::reconciliation::{
    ReconciliationRecordBinding, prepare_account_replacement, reconciliation_digest,
};
use crate::{
    ExecutionAdapterError, ExecutionAuditKind, ExecutionAuditReason, ExecutionState,
    ReconcileOrders, ReconciledOrder, ReconciledOrderStatus, ReconciliationAcknowledgement,
    ReconciliationBatchBinding,
};

#[derive(Debug)]
struct LifecycleOutcomeFailSafe {
    registry: Arc<Mutex<DispatchRegistry>>,
    order_id: OrderId,
    account: Option<AccountOutcomeFailSafe>,
    audit: Option<ExecutionAuditPermit>,
    context: ExecutionAuditContext,
    fallback_at: Timestamp,
    armed: bool,
}

impl LifecycleOutcomeFailSafe {
    fn new(
        registry: Arc<Mutex<DispatchRegistry>>,
        order_id: OrderId,
        account: AccountOutcomeFailSafe,
        audit: ExecutionAuditPermit,
        context: ExecutionAuditContext,
        fallback_at: Timestamp,
    ) -> Self {
        Self {
            registry,
            order_id,
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
        observed_at: Timestamp,
        reasons: &[ExecutionAuditReason],
    ) {
        self.armed = false;
        if let Some(account) = self.account.take() {
            account.disarm();
        }
        self.commit(kind, observed_at, reasons);
    }

    fn complete_uncertain(
        mut self,
        kind: ExecutionAuditKind,
        observed_at: Timestamp,
        reasons: &[ExecutionAuditReason],
    ) {
        self.armed = false;
        drop(self.account.take());
        self.commit(kind, observed_at, reasons);
    }

    fn fail_uncertain(mut self, observed_at: Timestamp, reasons: &[ExecutionAuditReason]) {
        mark_order_reconciliation(&self.registry, self.order_id, observed_at);
        self.armed = false;
        drop(self.account.take());
        self.commit(ExecutionAuditKind::DispatchUncertain, observed_at, reasons);
    }

    fn commit(
        &mut self,
        kind: ExecutionAuditKind,
        observed_at: Timestamp,
        reasons: &[ExecutionAuditReason],
    ) {
        if let Some(audit) = self.audit.take() {
            commit_dispatch_audit(audit, kind, self.context, observed_at, reasons);
        }
    }
}

impl Drop for LifecycleOutcomeFailSafe {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        mark_order_reconciliation(&self.registry, self.order_id, self.fallback_at);
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

impl ExecutionDispatcher {
    /// Obtains and applies a bounded backend state image for every accepted or uncertain order.
    /// Fill-bearing outcomes remain reconciliation-required until authoritative account state is
    /// replaced with matching balances, positions, fees, and revision.
    pub async fn reconcile(&self) -> Result<ExecutionState, ExecutionDispatchError> {
        let has_pending = {
            let registry = try_registry(&self.registry)?;
            registry.pending_reconciliation.is_some()
        };
        if has_pending {
            return self.retry_pending_reconciliation().await;
        }
        let (admissions, order_ids, invoked) = {
            let mut registry = try_registry(&self.registry)?;
            let count = registry
                .entries
                .values()
                .filter(|record| record.reconcilable())
                .count();
            if count == 0 {
                return Err(ExecutionDispatchError::OrderNotTracked);
            }
            let mut admissions = Vec::new();
            admissions
                .try_reserve_exact(count)
                .map_err(|_| ExecutionDispatchError::Allocation)?;
            for (approval_id, record) in registry
                .entries
                .iter()
                .filter(|(_, record)| record.reconcilable())
            {
                admissions.push(ReconcileAdmission {
                    approval_id: *approval_id,
                    order_id: record.order_id,
                    prior_state: record.state,
                    context: record.audit_context,
                    audit: Some(
                        self.audit
                            .try_reserve()
                            .map_err(|_| ExecutionDispatchError::AuditUnavailable)?,
                    ),
                    fail_safe: None,
                    invoked_at: record.last_transition_at,
                });
            }
            admissions.sort_by_key(|admission| admission.order_id);
            let mut order_ids = Vec::new();
            order_ids
                .try_reserve_exact(admissions.len())
                .map_err(|_| ExecutionDispatchError::Allocation)?;
            order_ids.extend(admissions.iter().map(|admission| admission.order_id));
            let invoked = match system_now() {
                Ok(invoked)
                    if admissions
                        .iter()
                        .all(|admission| invoked.wall >= admission.invoked_at) =>
                {
                    invoked
                }
                _ => {
                    reject_pre_dispatch(
                        &mut admissions,
                        Timestamp::from_unix_nanos(0),
                        ExecutionAuditReason::ClockFailure,
                    );
                    return Err(ExecutionDispatchError::ClockUnavailable);
                }
            };
            if admissions.iter().any(|admission| {
                registry
                    .entries
                    .get(&admission.approval_id)
                    .and_then(|record| record.reservation.as_ref())
                    .is_none()
            }) {
                reject_pre_dispatch(
                    &mut admissions,
                    invoked.wall,
                    ExecutionAuditReason::RegistryUnavailable,
                );
                return Err(ExecutionDispatchError::RegistryInvariant);
            }
            let mut setup_failed = false;
            for admission in &mut admissions {
                let Some(record) = registry.entries.get_mut(&admission.approval_id) else {
                    setup_failed = true;
                    break;
                };
                let Some(reservation) = record.reservation.as_ref() else {
                    setup_failed = true;
                    break;
                };
                let Some(audit) = admission.audit.take() else {
                    setup_failed = true;
                    break;
                };
                let account_fail_safe = reservation.outcome_fail_safe();
                admission.fail_safe = Some(LifecycleOutcomeFailSafe::new(
                    Arc::clone(&self.registry),
                    admission.order_id,
                    account_fail_safe,
                    audit,
                    admission.context,
                    invoked.wall,
                ));
                record.state = DispatchState::Reconciling;
                record.last_transition_at = invoked.wall;
            }
            if setup_failed {
                drop(registry);
                fail_all(
                    admissions,
                    invoked.wall,
                    ExecutionAuditReason::RegistryUnavailable,
                );
                return Err(ExecutionDispatchError::RegistryInvariant);
            }
            (admissions, order_ids, invoked)
        };

        let operation = super::operation(self.operation_deadline, self.cancellation.child_token())?;
        let deadline = operation.deadline();
        let cancellation = operation.cancellation();
        let request = ReconcileOrders::new(order_ids.clone().into_boxed_slice(), operation);
        let state = match tokio::time::timeout_at(deadline, self.adapter.reconcile(request)).await {
            Err(_) => {
                cancellation.cancel();
                fail_all(
                    admissions,
                    invoked.wall,
                    ExecutionAuditReason::OperationDeadlineExceeded,
                );
                return Err(ExecutionDispatchError::OperationDeadlineExceeded);
            }
            Ok(result) => match result {
                Ok(state) => state,
                Err(error) => {
                    let post_call = match system_now() {
                        Ok(post_call)
                            if post_call.wall >= invoked.wall
                                && post_call.monotonic >= invoked.monotonic =>
                        {
                            post_call
                        }
                        _ => {
                            fail_all(admissions, invoked.wall, ExecutionAuditReason::ClockFailure);
                            return Err(ExecutionDispatchError::ClockUnavailable);
                        }
                    };
                    let observed_at = post_call.wall;
                    let known = matches!(
                        error,
                        ExecutionAdapterError::Rejected
                            | ExecutionAdapterError::KnownFailure
                            | ExecutionAdapterError::NotAttemptedBusy
                    );
                    restore_or_reconcile(&self.registry, &admissions, known, observed_at);
                    for mut admission in admissions {
                        if let Some(fail_safe) = admission.fail_safe.take() {
                            if known {
                                fail_safe.complete_known(
                                    ExecutionAuditKind::DispatchKnownFailure,
                                    observed_at,
                                    &[adapter_reason(error)],
                                );
                            } else {
                                fail_safe.complete_uncertain(
                                    ExecutionAuditKind::DispatchUncertain,
                                    observed_at,
                                    &[adapter_reason(error)],
                                );
                            }
                        }
                    }
                    return Err(ExecutionDispatchError::Adapter(error));
                }
            },
        };
        let post_call = match system_now() {
            Ok(post_call)
                if post_call.wall >= invoked.wall && post_call.monotonic >= invoked.monotonic =>
            {
                post_call
            }
            _ => {
                fail_all(admissions, invoked.wall, ExecutionAuditReason::ClockFailure);
                return Err(ExecutionDispatchError::ClockUnavailable);
            }
        };
        if cancellation.is_cancelled() || tokio::time::Instant::now() >= deadline {
            cancellation.cancel();
            fail_all(
                admissions,
                post_call.wall,
                ExecutionAuditReason::OperationDeadlineExceeded,
            );
            return Err(ExecutionDispatchError::OperationDeadlineExceeded);
        }
        let timestamp_invalid =
            state.observed_at() < invoked.wall || state.observed_at() > post_call.wall;
        let unexpected_order = state
            .orders()
            .iter()
            .any(|order| order_ids.binary_search(&order.order_id()).is_err());
        if timestamp_invalid || unexpected_order {
            let reason = if timestamp_invalid {
                ExecutionAuditReason::ObservationTimestampInvalid
            } else {
                ExecutionAuditReason::UnexpectedReconciliationOrder
            };
            fail_all(admissions, post_call.wall, reason);
            return Err(ExecutionDispatchError::ReceiptMismatch);
        }

        {
            let mut registry = match try_registry(&self.registry) {
                Ok(registry) => registry,
                Err(error) => {
                    fail_all(
                        admissions,
                        post_call.wall,
                        ExecutionAuditReason::RegistryUnavailable,
                    );
                    return Err(error);
                }
            };
            let bindings = (|| {
                let mut bindings = Vec::new();
                bindings
                    .try_reserve_exact(admissions.len())
                    .map_err(|_| ExecutionDispatchError::Allocation)?;
                for admission in &admissions {
                    let record = registry
                        .entries
                        .get(&admission.approval_id)
                        .ok_or(ExecutionDispatchError::RegistryInvariant)?;
                    bindings.push(ReconciliationRecordBinding {
                        account_id: record.account_id,
                        order_id: record.order_id,
                        intent_digest: record.intent_digest,
                        account_revision: record.account_revision,
                        requested_quantity: record.requested_quantity,
                        execution_price_bound: record.execution_price_bound,
                        settlement_currency: record.settlement_currency,
                        previous: record.lifecycle,
                        was_reconciliation: admission.prior_state == DispatchState::Reconciliation,
                    });
                }
                Ok(bindings)
            })();
            let bindings = match bindings {
                Ok(bindings) => bindings,
                Err(error) => {
                    drop(registry);
                    fail_all(
                        admissions,
                        post_call.wall,
                        ExecutionAuditReason::RegistryUnavailable,
                    );
                    return Err(error);
                }
            };
            let prepared = match prepare_account_replacement(&state, &bindings, invoked.wall) {
                Ok(prepared) => prepared,
                Err(error) => {
                    drop(registry);
                    fail_all(
                        admissions,
                        post_call.wall,
                        ExecutionAuditReason::AccountReplacementRejected,
                    );
                    return Err(error);
                }
            };
            let batch = match ReconciliationBatchBinding::from_dispatcher_digest(
                reconciliation_digest(&state, &bindings, invoked.wall),
            ) {
                Ok(batch) => batch,
                Err(_) => {
                    drop(registry);
                    fail_all(
                        admissions,
                        post_call.wall,
                        ExecutionAuditReason::AccountReplacementRejected,
                    );
                    return Err(ExecutionDispatchError::AccountReplacementRejected);
                }
            };
            let pending =
                match PendingReconciliation::try_new(batch, order_ids.into_boxed_slice(), state) {
                    Ok(pending)
                        if pending.retained_bytes <= self.maximum_pending_reconciliation_bytes
                            && registry.finalized_reconciliations.len()
                                < registry.maximum_finalized_reconciliations =>
                    {
                        pending
                    }
                    _ => {
                        drop(registry);
                        fail_all(
                            admissions,
                            post_call.wall,
                            ExecutionAuditReason::PendingReconciliationCapacity,
                        );
                        return Err(ExecutionDispatchError::PendingReconciliationCapacity);
                    }
                };
            if cancellation.is_cancelled() || tokio::time::Instant::now() >= deadline {
                cancellation.cancel();
                drop(registry);
                fail_all(
                    admissions,
                    post_call.wall,
                    ExecutionAuditReason::OperationDeadlineExceeded,
                );
                return Err(ExecutionDispatchError::OperationDeadlineExceeded);
            }
            let affected_accounts = if let Some(prepared) = prepared {
                let (batch, affected_accounts) = prepared.into_parts();
                if self.accounts.replace_reconciled_accounts(batch).is_err() {
                    drop(registry);
                    fail_all(
                        admissions,
                        post_call.wall,
                        ExecutionAuditReason::AccountReplacementRejected,
                    );
                    return Err(ExecutionDispatchError::AccountReplacementRejected);
                }
                affected_accounts
            } else {
                Box::new([])
            };
            for mut admission in admissions {
                let observed = pending
                    .state
                    .orders()
                    .iter()
                    .copied()
                    .find(|order| order.order_id() == admission.order_id);
                let Some(record) = registry
                    .entries
                    .values_mut()
                    .find(|record| record.order_id == admission.order_id)
                else {
                    drop(registry);
                    if let Some(fail_safe) = admission.fail_safe.take() {
                        fail_safe.fail_uncertain(
                            post_call.wall,
                            &[ExecutionAuditReason::RegistryUnavailable],
                        );
                    }
                    return Err(ExecutionDispatchError::RegistryInvariant);
                };
                let reason = if affected_accounts.binary_search(&record.account_id).is_ok() {
                    record.lifecycle = observed;
                    record.state = DispatchState::Terminal;
                    None
                } else {
                    apply_reconciled_order(
                        record,
                        observed,
                        pending.state.reconciliation_required(),
                        admission.prior_state == DispatchState::Reconciliation,
                    )
                };
                record.last_transition_at = pending.state.observed_at();
                let fail_safe = admission
                    .fail_safe
                    .take()
                    .ok_or(ExecutionDispatchError::RegistryInvariant)?;
                match reason {
                    Some(reason) => fail_safe.complete_uncertain(
                        ExecutionAuditKind::ReconciliationObserved,
                        pending.state.observed_at(),
                        &[reason],
                    ),
                    None => fail_safe.complete_known(
                        ExecutionAuditKind::ReconciliationObserved,
                        pending.state.observed_at(),
                        &[],
                    ),
                }
            }
            registry
                .entries
                .retain(|_, record| record.state != DispatchState::Terminal);
            registry.pending_reconciliation = Some(pending);
        }
        self.retry_pending_reconciliation().await
    }

    async fn retry_pending_reconciliation(&self) -> Result<ExecutionState, ExecutionDispatchError> {
        let (batch, order_ids) = {
            let mut registry = try_registry(&self.registry)?;
            let pending = registry
                .pending_reconciliation
                .as_mut()
                .ok_or(ExecutionDispatchError::OrderNotTracked)?;
            match pending.status {
                PendingReconciliationStatus::InFlight => {
                    return Err(ExecutionDispatchError::ReconciliationAcknowledgementPending);
                }
                PendingReconciliationStatus::BackendAcknowledged => {
                    return finalize_pending_reconciliation(&mut registry);
                }
                PendingReconciliationStatus::Ready => {}
            }
            let mut order_ids = Vec::new();
            order_ids
                .try_reserve_exact(pending.order_ids.len())
                .map_err(|_| ExecutionDispatchError::Allocation)?;
            order_ids.extend_from_slice(&pending.order_ids);
            pending.status = PendingReconciliationStatus::InFlight;
            (pending.batch, order_ids.into_boxed_slice())
        };
        let operation =
            match super::operation(self.operation_deadline, self.cancellation.child_token()) {
                Ok(operation) => operation,
                Err(error) => {
                    restore_pending_acknowledgement(&self.registry, batch);
                    return Err(error);
                }
            };
        let deadline = operation.deadline();
        let cancellation = operation.cancellation();
        let acknowledgement = ReconciliationAcknowledgement::new(batch, order_ids, operation);
        let result = tokio::time::timeout_at(
            deadline,
            self.adapter.acknowledge_reconciliation(acknowledgement),
        )
        .await;
        match result {
            Err(_) => {
                cancellation.cancel();
                restore_pending_acknowledgement(&self.registry, batch);
                Err(ExecutionDispatchError::OperationDeadlineExceeded)
            }
            Ok(Err(error)) => {
                restore_pending_acknowledgement(&self.registry, batch);
                Err(ExecutionDispatchError::Adapter(error))
            }
            Ok(Ok(())) => {
                let mut registry = lock_registry(&self.registry);
                let pending = registry
                    .pending_reconciliation
                    .as_mut()
                    .ok_or(ExecutionDispatchError::RegistryInvariant)?;
                if pending.batch != batch || pending.status != PendingReconciliationStatus::InFlight
                {
                    return Err(ExecutionDispatchError::RegistryInvariant);
                }
                pending.status = PendingReconciliationStatus::BackendAcknowledged;
                finalize_pending_reconciliation(&mut registry)
            }
        }
    }

    /// Closes admission, drains accepted queue entries, and joins the worker within the deadline.
    pub async fn shutdown(mut self) -> ExecutionDispatcherShutdown {
        self.cancellation.cancel();
        let Some(mut worker) = self.worker.take() else {
            mark_shutdown_reconciliation(&self.registry);
            return ExecutionDispatcherShutdown::JoinError;
        };
        let outcome = match tokio::time::timeout(self.shutdown_deadline, &mut worker).await {
            Ok(Ok(())) => ExecutionDispatcherShutdown::Complete,
            Ok(Err(_)) => ExecutionDispatcherShutdown::JoinError,
            Err(_) => {
                worker.abort();
                match tokio::time::timeout(self.shutdown_deadline, &mut worker).await {
                    Ok(_) => ExecutionDispatcherShutdown::DeadlineAborted,
                    Err(_) => ExecutionDispatcherShutdown::Incomplete,
                }
            }
        };
        mark_shutdown_reconciliation(&self.registry);
        outcome
    }
}

impl Drop for ExecutionDispatcher {
    fn drop(&mut self) {
        self.cancellation.cancel();
        if let Some(worker) = self.worker.as_mut() {
            worker.abort();
        }
        mark_shutdown_reconciliation(&self.registry);
    }
}

#[derive(Debug)]
struct ReconcileAdmission {
    approval_id: ApprovalId,
    order_id: OrderId,
    prior_state: DispatchState,
    context: ExecutionAuditContext,
    audit: Option<ExecutionAuditPermit>,
    fail_safe: Option<LifecycleOutcomeFailSafe>,
    invoked_at: Timestamp,
}

fn reject_pre_dispatch(
    admissions: &mut [ReconcileAdmission],
    observed_at: Timestamp,
    reason: ExecutionAuditReason,
) {
    for admission in admissions {
        if let Some(audit) = admission.audit.take() {
            commit_dispatch_audit(
                audit,
                ExecutionAuditKind::DispatchRejected,
                admission.context,
                observed_at,
                &[reason],
            );
        }
    }
}

fn fail_all(
    admissions: Vec<ReconcileAdmission>,
    observed_at: Timestamp,
    reason: ExecutionAuditReason,
) {
    for mut admission in admissions {
        if let Some(fail_safe) = admission.fail_safe.take() {
            fail_safe.fail_uncertain(observed_at, &[reason]);
        } else if let Some(audit) = admission.audit.take() {
            commit_dispatch_audit(
                audit,
                ExecutionAuditKind::DispatchRejected,
                admission.context,
                observed_at,
                &[reason],
            );
        }
    }
}

fn restore_pending_acknowledgement(
    registry: &Arc<Mutex<DispatchRegistry>>,
    batch: ReconciliationBatchBinding,
) {
    let mut registry = lock_registry(registry);
    if let Some(pending) = registry.pending_reconciliation.as_mut()
        && pending.batch == batch
        && pending.status == PendingReconciliationStatus::InFlight
    {
        pending.status = PendingReconciliationStatus::Ready;
    }
}

fn finalize_pending_reconciliation(
    registry: &mut DispatchRegistry,
) -> Result<ExecutionState, ExecutionDispatchError> {
    let Some(pending) = registry.pending_reconciliation.as_ref() else {
        return Err(ExecutionDispatchError::OrderNotTracked);
    };
    if pending.status != PendingReconciliationStatus::BackendAcknowledged {
        return Err(ExecutionDispatchError::ReconciliationAcknowledgementPending);
    }
    if registry.finalized_reconciliations.len() >= registry.maximum_finalized_reconciliations {
        return Err(ExecutionDispatchError::PendingReconciliationCapacity);
    }
    let pending = registry
        .pending_reconciliation
        .take()
        .ok_or(ExecutionDispatchError::RegistryInvariant)?;
    registry.finalized_reconciliations.push(pending.batch);
    Ok(pending.state)
}

fn restore_or_reconcile(
    registry: &Arc<Mutex<DispatchRegistry>>,
    admissions: &[ReconcileAdmission],
    known_not_attempted: bool,
    observed_at: Timestamp,
) {
    let mut registry = lock_registry(registry);
    for admission in admissions {
        if let Some(record) = registry
            .entries
            .values_mut()
            .find(|record| record.order_id == admission.order_id)
        {
            record.state = if known_not_attempted {
                admission.prior_state
            } else {
                DispatchState::Reconciliation
            };
            record.last_transition_at = record.last_transition_at.max(observed_at);
            if !known_not_attempted && let Some(reservation) = record.reservation.as_ref() {
                reservation.mark_reconciliation_required();
            }
        }
    }
}

fn mark_order_reconciliation(
    registry: &Arc<Mutex<DispatchRegistry>>,
    order_id: OrderId,
    observed_at: Timestamp,
) {
    let mut registry = lock_registry(registry);
    if let Some(record) = registry
        .entries
        .values_mut()
        .find(|record| record.order_id == order_id)
        && record.state != DispatchState::Terminal
    {
        if let Some(reservation) = record.reservation.as_ref() {
            reservation.mark_reconciliation_required();
        }
        record.state = DispatchState::Reconciliation;
        record.last_transition_at = record.last_transition_at.max(observed_at);
    }
}

fn mark_shutdown_reconciliation(registry: &Arc<Mutex<DispatchRegistry>>) {
    let observed_at = system_now().ok().map(|reading| reading.wall);
    let mut registry = lock_registry(registry);
    for record in registry.entries.values_mut() {
        if mark_shutdown_reservation(&mut record.state, record.reservation.as_ref())
            && let Some(observed_at) = observed_at
        {
            record.last_transition_at = record.last_transition_at.max(observed_at);
        }
    }
}

fn mark_shutdown_reservation(
    state: &mut DispatchState,
    reservation: Option<&AccountRiskReservation>,
) -> bool {
    if !matches!(
        state,
        DispatchState::Submitted
            | DispatchState::Accepted
            | DispatchState::Canceling
            | DispatchState::Reconciling
    ) {
        return false;
    }
    let Some(reservation) = reservation else {
        return false;
    };
    reservation.mark_reconciliation_required();
    *state = DispatchState::Reconciliation;
    true
}

fn lock_registry(
    registry: &Arc<Mutex<DispatchRegistry>>,
) -> std::sync::MutexGuard<'_, DispatchRegistry> {
    match registry.lock() {
        Ok(registry) => registry,
        Err(poisoned) => poisoned.into_inner(),
    }
}

fn apply_reconciled_order(
    record: &mut DispatchRecord,
    observed: Option<ReconciledOrder>,
    backend_requires_reconciliation: bool,
    was_reconciliation: bool,
) -> Option<ExecutionAuditReason> {
    let Some(reservation) = record.reservation.as_ref() else {
        return Some(ExecutionAuditReason::RegistryUnavailable);
    };
    let Some(observed) = observed else {
        reservation.mark_reconciliation_required();
        record.state = DispatchState::Reconciliation;
        return Some(ExecutionAuditReason::ReconciliationRequired);
    };
    let cumulative_regression = record.lifecycle.is_some_and(|previous| {
        observed.cumulative_filled().get() < previous.cumulative_filled().get()
            || observed.cumulative_fees().currency() != previous.cumulative_fees().currency()
            || observed.cumulative_fees().amount() < previous.cumulative_fees().amount()
    });
    record.lifecycle = Some(observed);
    let filled = observed.cumulative_filled().get();
    let requested = record.requested_quantity.get();
    let fees = observed.cumulative_fees();
    let financial_effect = filled != 0 || !fees.amount().is_zero();
    let invalid_evidence = cumulative_regression
        || filled < 0
        || filled > requested
        || record.settlement_currency != Some(fees.currency())
        || observed
            .average_fill_price()
            .is_some_and(|price| !record.execution_price_bound.permits(price))
        || matches!(observed.status(), ReconciledOrderStatus::Filled) && filled != requested
        || matches!(observed.status(), ReconciledOrderStatus::PartiallyFilled)
            && (filled <= 0 || filled >= requested);
    if backend_requires_reconciliation
        || invalid_evidence
        || financial_effect
        || matches!(observed.status(), ReconciledOrderStatus::Unknown)
        || was_reconciliation
    {
        reservation.mark_reconciliation_required();
        record.state = DispatchState::Reconciliation;
        return Some(ExecutionAuditReason::ReconciliationRequired);
    }
    match observed.status() {
        ReconciledOrderStatus::Open => {
            record.state = DispatchState::Accepted;
            None
        }
        ReconciledOrderStatus::Canceled
        | ReconciledOrderStatus::Rejected
        | ReconciledOrderStatus::Expired => {
            reservation.mark_terminal_unfilled();
            record.state = DispatchState::Terminal;
            None
        }
        ReconciledOrderStatus::PartiallyFilled
        | ReconciledOrderStatus::Filled
        | ReconciledOrderStatus::Unknown => {
            reservation.mark_reconciliation_required();
            record.state = DispatchState::Reconciliation;
            Some(ExecutionAuditReason::ReconciliationRequired)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::Ordering;

    use super::{DispatchState, mark_shutdown_reservation};
    use crate::account::accepted_reservation_for_test;

    #[test]
    fn accepted_order_shutdown_requires_account_reconciliation()
    -> Result<(), Box<dyn std::error::Error>> {
        let (reservation, reconciliation_required) = accepted_reservation_for_test()?;
        let mut state = DispatchState::Accepted;

        assert!(mark_shutdown_reservation(&mut state, Some(&reservation)));
        assert_eq!(state, DispatchState::Reconciliation);
        assert!(reconciliation_required.load(Ordering::Acquire));
        Ok(())
    }
}
