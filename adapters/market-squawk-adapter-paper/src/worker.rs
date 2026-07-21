//! Single-writer paper state, matching, reconciliation, and shutdown.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use market_squawk_domain::{AccountId, ClientOrderId, OrderId, TimeInForce, Timestamp};
use market_squawk_execution::{
    CancelOrder, CancelReceipt, CancelStatus, DispatchOrder, ExecutionAdapterError,
    ExecutionMarketUpdate, ExecutionReceipt, ExecutionState, PersistenceAcknowledgement,
    ReconcileOrders, ReconciliationAcknowledgement, ReconciliationBatchBinding,
};
use tokio::sync::{OwnedSemaphorePermit, mpsc, oneshot};
use tokio_util::sync::CancellationToken;

use crate::audit::{PaperAuditKind, PaperAuditRecord};
use crate::latency::sample_latency;
use crate::matching::AvailableMarket;
use crate::order::PaperOrder;
use crate::snapshot::{
    PaperCheckpointPersistenceEvidence, PaperExecutionCheckpoint, PaperExecutionSnapshot,
    PaperFillSnapshot,
};
use crate::{
    PaperControlContext, PaperControlError, PaperExecutionConfig, PaperLedger, PaperOrderState,
};

#[path = "worker/reconciliation.rs"]
mod reconciliation;
use reconciliation::{
    cancel_receipt, is_terminal, market_digest, order_priority, reservation_price, state_audit,
};

#[derive(Debug)]
#[allow(
    clippy::large_enum_variant,
    reason = "the bounded inline submit command avoids an untracked fallible heap allocation in the execution path; queue count and bytes charge the full conservative order ceiling"
)]
pub(crate) enum WorkerCommand {
    Submit {
        order: DispatchOrder,
        reply: oneshot::Sender<Result<ExecutionReceipt, ExecutionAdapterError>>,
    },
    Cancel {
        order: CancelOrder,
        requested_at: Timestamp,
        reply: oneshot::Sender<Result<CancelReceipt, ExecutionAdapterError>>,
    },
    Reconcile {
        requested_at: Timestamp,
        request: ReconcileOrders,
        reply: oneshot::Sender<Result<ExecutionState, ExecutionAdapterError>>,
    },
    AcknowledgeReconciliation {
        acknowledgement: ReconciliationAcknowledgement,
        reply: oneshot::Sender<Result<(), ExecutionAdapterError>>,
    },
    AcknowledgePersistence {
        authority: PersistenceAcknowledgement,
        evidence: PaperCheckpointPersistenceEvidence,
        reply: oneshot::Sender<Result<(), PaperControlError>>,
    },
    Snapshot {
        control: PaperControlContext,
        reply: oneshot::Sender<Result<PaperExecutionSnapshot, PaperControlError>>,
    },
    Checkpoint {
        control: PaperControlContext,
        reply: oneshot::Sender<Result<PaperExecutionCheckpoint, PaperControlError>>,
    },
    Shutdown {
        control: PaperControlContext,
        reply: oneshot::Sender<Result<PaperExecutionSnapshot, PaperControlError>>,
    },
}

#[derive(Debug)]
#[allow(
    clippy::large_enum_variant,
    reason = "the unified bounded mailbox keeps both fixed-shape events inline so the live handoff performs no fallible allocation; each lane charges a conservative byte ceiling"
)]
pub(crate) enum WorkerEvent {
    Command(WorkerCommand),
    Market(ExecutionMarketUpdate),
}

#[derive(Debug)]
pub(crate) struct WorkerEnvelope {
    pub(crate) sequence: u64,
    pub(crate) event: WorkerEvent,
    pub(crate) _lane_slot: OwnedSemaphorePermit,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct WorkerMarketUpdate {
    pub(crate) sequence: u64,
    pub(crate) update: ExecutionMarketUpdate,
}

#[derive(Debug)]
pub(crate) struct PaperWorker {
    config: PaperExecutionConfig,
    state: WorkerState,
    events: mpsc::Receiver<WorkerEnvelope>,
    audit: mpsc::Sender<PaperAuditRecord>,
    audit_failed: Arc<AtomicBool>,
    cancellation: CancellationToken,
}

#[derive(Debug)]
struct WorkerState {
    sequence: u64,
    reconciliation_required: bool,
    orders: BTreeMap<OrderId, PaperOrder>,
    fills: Vec<PaperFillSnapshot>,
    archived_orders: BTreeMap<OrderId, PaperOrder>,
    archived_fills: Vec<PaperFillSnapshot>,
    durable_sequence: u64,
    reconciled_orders: BTreeSet<OrderId>,
    acknowledged_reconciliation_batches: Vec<ReconciliationBatchBinding>,
    issued_checkpoint: Option<IssuedCheckpoint>,
    ledger: PaperLedger,
    idempotency: BTreeMap<(AccountId, ClientOrderId), OrderId>,
}

#[derive(Debug)]
struct IssuedCheckpoint {
    evidence: PaperCheckpointPersistenceEvidence,
    acknowledged_reconciliation_batches: Box<[ReconciliationBatchBinding]>,
}

impl PaperWorker {
    #[allow(
        clippy::too_many_arguments,
        reason = "worker construction transfers each independently bounded owner"
    )]
    pub(crate) fn new(
        config: PaperExecutionConfig,
        ledger: PaperLedger,
        checkpoint: Option<PaperExecutionCheckpoint>,
        events: mpsc::Receiver<WorkerEnvelope>,
        audit: mpsc::Sender<PaperAuditRecord>,
        audit_failed: Arc<AtomicBool>,
        cancellation: CancellationToken,
    ) -> Self {
        let state = if let Some(checkpoint) = checkpoint {
            WorkerState {
                sequence: checkpoint.sequence,
                reconciliation_required: checkpoint.reconciliation_required,
                orders: checkpoint.orders,
                fills: checkpoint.fills,
                archived_orders: checkpoint.archived_orders,
                archived_fills: checkpoint.archived_fills,
                durable_sequence: checkpoint.durable_sequence,
                reconciled_orders: checkpoint.reconciled_orders,
                acknowledged_reconciliation_batches: checkpoint.acknowledged_reconciliation_batches,
                issued_checkpoint: None,
                ledger: checkpoint.ledger,
                idempotency: checkpoint.idempotency,
            }
        } else {
            WorkerState {
                sequence: 0,
                reconciliation_required: false,
                orders: BTreeMap::new(),
                fills: Vec::new(),
                archived_orders: BTreeMap::new(),
                archived_fills: Vec::new(),
                durable_sequence: 0,
                reconciled_orders: BTreeSet::new(),
                acknowledged_reconciliation_batches: Vec::new(),
                issued_checkpoint: None,
                ledger,
                idempotency: BTreeMap::new(),
            }
        };
        Self {
            config,
            state,
            events,
            audit,
            audit_failed,
            cancellation,
        }
    }

    pub(crate) async fn run(mut self) {
        loop {
            tokio::select! {
                _ = self.cancellation.cancelled() => break,
                envelope = self.events.recv() => {
                    let Some(envelope) = envelope else { break };
                    let WorkerEnvelope {
                        sequence,
                        event,
                        _lane_slot,
                    } = envelope;
                    match event {
                        WorkerEvent::Command(command) => {
                            if self.handle_command(sequence, command) {
                                break;
                            }
                        }
                        WorkerEvent::Market(update) => self.process_market(WorkerMarketUpdate {
                            sequence,
                            update,
                        }),
                    }
                }
            }
        }
    }

    fn handle_command(&mut self, event_sequence: u64, command: WorkerCommand) -> bool {
        match command {
            WorkerCommand::Submit { order, reply } => {
                let result = if order.operation().is_expired() {
                    Err(ExecutionAdapterError::KnownFailure)
                } else {
                    self.submit(event_sequence, order)
                };
                let _ = reply.send(result);
                false
            }
            WorkerCommand::Cancel {
                order,
                requested_at,
                reply,
            } => {
                let result = if order.operation().is_expired() {
                    Err(ExecutionAdapterError::KnownFailure)
                } else {
                    self.cancel(event_sequence, order.order_id(), requested_at)
                };
                let _ = reply.send(result);
                false
            }
            WorkerCommand::Reconcile {
                requested_at,
                request,
                reply,
            } => {
                let result = if request.operation().is_expired() {
                    Err(ExecutionAdapterError::KnownFailure)
                } else {
                    self.advance_due(requested_at);
                    self.reconcile(requested_at, request.order_ids())
                };
                let _ = reply.send(result);
                false
            }
            WorkerCommand::AcknowledgeReconciliation {
                acknowledgement,
                reply,
            } => {
                let result = self.acknowledge_reconciliation(acknowledgement);
                let _ = reply.send(result);
                false
            }
            WorkerCommand::AcknowledgePersistence {
                authority,
                evidence,
                reply,
            } => {
                let result = self.acknowledge_persistence(authority, evidence);
                let _ = reply.send(result);
                false
            }
            WorkerCommand::Snapshot { control, reply } => {
                let result = if control.is_expired() {
                    Err(PaperControlError::DeadlineExceeded)
                } else {
                    self.refresh_audit_health();
                    Ok(self.snapshot())
                };
                let _ = reply.send(result);
                false
            }
            WorkerCommand::Checkpoint { control, reply } => {
                let result = if control.is_expired() {
                    Err(PaperControlError::DeadlineExceeded)
                } else {
                    self.refresh_audit_health();
                    self.checkpoint().map_err(|_| PaperControlError::Closed)
                };
                let _ = reply.send(result);
                false
            }
            WorkerCommand::Shutdown { control, reply } => {
                if control.is_expired() {
                    let _ = reply.send(Err(PaperControlError::DeadlineExceeded));
                    false
                } else {
                    self.refresh_audit_health();
                    let _ = reply.send(Ok(self.snapshot()));
                    true
                }
            }
        }
    }

    fn acknowledge_reconciliation(
        &mut self,
        acknowledgement: ReconciliationAcknowledgement,
    ) -> Result<(), ExecutionAdapterError> {
        let maximum_batches = self
            .config
            .input()
            .maximum_orders
            .get()
            .checked_add(self.config.input().maximum_archived_orders.get())
            .ok_or(ExecutionAdapterError::KnownFailure)?;
        if acknowledgement.operation().is_expired()
            || acknowledgement.order_ids().len() > maximum_batches
        {
            return Err(ExecutionAdapterError::KnownFailure);
        }
        let batch = ReconciliationBatchBinding::try_new(
            acknowledgement.batch_id(),
            acknowledgement.binding_digest(),
        )
        .map_err(|_| ExecutionAdapterError::KnownFailure)?;
        if let Some(persisted) = self
            .state
            .acknowledged_reconciliation_batches
            .iter()
            .find(|persisted| persisted.batch_id() == batch.batch_id())
        {
            return if *persisted == batch {
                Ok(())
            } else {
                Err(ExecutionAdapterError::KnownFailure)
            };
        }
        if self.state.acknowledged_reconciliation_batches.len() >= maximum_batches {
            return Err(ExecutionAdapterError::NotAttemptedBusy);
        }
        self.state
            .acknowledged_reconciliation_batches
            .try_reserve(1)
            .map_err(|_| ExecutionAdapterError::NotAttemptedBusy)?;
        for order_id in acknowledgement.order_ids() {
            let order = self
                .state
                .orders
                .get(order_id)
                .or_else(|| self.state.archived_orders.get(order_id))
                .ok_or(ExecutionAdapterError::KnownFailure)?;
            if is_terminal(order.lifecycle.state()) {
                self.state.reconciled_orders.insert(*order_id);
            }
        }
        self.compact(
            crate::adapter::system_timestamp().map_err(|_| ExecutionAdapterError::KnownFailure)?,
        )?;
        self.state.acknowledged_reconciliation_batches.push(batch);
        Ok(())
    }

    fn acknowledge_persistence(
        &mut self,
        authority: PersistenceAcknowledgement,
        evidence: PaperCheckpointPersistenceEvidence,
    ) -> Result<(), PaperControlError> {
        let Some(issued) = self.state.issued_checkpoint.take() else {
            return Err(PaperControlError::Adapter(
                ExecutionAdapterError::KnownFailure,
            ));
        };
        if authority.operation().is_expired()
            || evidence.configuration_digest != self.config.digest()
            || evidence.sequence > self.state.sequence
            || issued.evidence != evidence
        {
            self.state.issued_checkpoint = Some(issued);
            return Err(PaperControlError::Adapter(
                ExecutionAdapterError::KnownFailure,
            ));
        }
        let mut prunable = Vec::new();
        if prunable
            .try_reserve_exact(issued.acknowledged_reconciliation_batches.len())
            .is_err()
        {
            self.state.issued_checkpoint = Some(issued);
            return Err(PaperControlError::Adapter(
                ExecutionAdapterError::NotAttemptedBusy,
            ));
        }
        prunable.extend(
            issued
                .acknowledged_reconciliation_batches
                .iter()
                .copied()
                .filter(|binding| {
                    authority
                        .finalized_reconciliations()
                        .iter()
                        .any(|finalized| finalized == binding)
                }),
        );
        let observed_at = match crate::adapter::system_timestamp() {
            Ok(observed_at) => observed_at,
            Err(_) => {
                self.state.issued_checkpoint = Some(issued);
                return Err(PaperControlError::Adapter(
                    ExecutionAdapterError::KnownFailure,
                ));
            }
        };
        let prior_durable_sequence = self.state.durable_sequence;
        self.state.durable_sequence = prior_durable_sequence.max(evidence.sequence);
        if let Err(error) = self.compact(observed_at) {
            self.state.durable_sequence = prior_durable_sequence;
            self.state.issued_checkpoint = Some(issued);
            return Err(PaperControlError::Adapter(error));
        }
        if let Err(error) = authority.commit_persisted(&prunable) {
            self.state.issued_checkpoint = Some(issued);
            return Err(PaperControlError::Adapter(error));
        }
        self.state
            .acknowledged_reconciliation_batches
            .retain(|binding| !prunable.iter().any(|persisted| persisted == binding));
        Ok(())
    }

    fn compact(&mut self, observed_at: Timestamp) -> Result<(), ExecutionAdapterError> {
        let durable_sequence = self.state.durable_sequence;
        let should_purge = |order_id: &OrderId, order: &PaperOrder| {
            observed_at > order.expires_at
                && order.lifecycle.last_sequence() <= durable_sequence
                && self.state.reconciled_orders.contains(order_id)
        };
        let purge_count = self
            .state
            .archived_orders
            .iter()
            .filter(|(order_id, order)| should_purge(order_id, order))
            .count();
        let retained_archive_count = self
            .state
            .archived_orders
            .len()
            .checked_sub(purge_count)
            .ok_or(ExecutionAdapterError::KnownFailure)?;
        let available_archive = self
            .config
            .input()
            .maximum_archived_orders
            .get()
            .checked_sub(retained_archive_count)
            .ok_or(ExecutionAdapterError::KnownFailure)?;
        let mut candidates = Vec::new();
        candidates
            .try_reserve_exact(available_archive.min(self.state.orders.len()))
            .map_err(|_| ExecutionAdapterError::KnownFailure)?;
        candidates.extend(
            self.state
                .orders
                .iter()
                .filter(|(order_id, order)| {
                    is_terminal(order.lifecycle.state())
                        && order.lifecycle.last_sequence() <= durable_sequence
                        && self.state.reconciled_orders.contains(order_id)
                })
                .map(|(order_id, _)| *order_id)
                .take(available_archive),
        );
        if candidates
            .iter()
            .any(|order_id| self.state.archived_orders.contains_key(order_id))
        {
            return Err(ExecutionAdapterError::KnownFailure);
        }
        let moving_fills = self
            .state
            .fills
            .iter()
            .filter(|fill| candidates.binary_search(&fill.order_id()).is_ok())
            .count();
        let maximum_archived_fills = self
            .config
            .input()
            .maximum_archived_orders
            .get()
            .checked_mul(self.config.input().maximum_fills.get())
            .ok_or(ExecutionAdapterError::KnownFailure)?;
        let retained_archived_fills = self
            .state
            .archived_fills
            .iter()
            .filter(|fill| {
                self.state
                    .archived_orders
                    .get(&fill.order_id())
                    .is_some_and(|order| !should_purge(&fill.order_id(), order))
            })
            .count();
        if retained_archived_fills
            .checked_add(moving_fills)
            .is_none_or(|total| total > maximum_archived_fills)
        {
            return Err(ExecutionAdapterError::KnownFailure);
        }
        self.state
            .archived_fills
            .try_reserve_exact(moving_fills)
            .map_err(|_| ExecutionAdapterError::KnownFailure)?;

        let reconciled_orders = &self.state.reconciled_orders;
        self.state.archived_orders.retain(|order_id, order| {
            !(observed_at > order.expires_at
                && order.lifecycle.last_sequence() <= durable_sequence
                && reconciled_orders.contains(order_id))
        });
        self.state
            .archived_fills
            .retain(|fill| self.state.archived_orders.contains_key(&fill.order_id()));
        for order_id in candidates {
            let order = self
                .state
                .orders
                .remove(&order_id)
                .ok_or(ExecutionAdapterError::KnownFailure)?;
            for fill in self
                .state
                .fills
                .iter()
                .filter(|fill| fill.order_id() == order_id)
            {
                self.state.archived_fills.push(*fill);
            }
            self.state.fills.retain(|fill| fill.order_id() != order_id);
            self.state
                .idempotency
                .retain(|_, retained_order_id| *retained_order_id != order_id);
            if self.state.archived_orders.insert(order_id, order).is_some() {
                return Err(ExecutionAdapterError::KnownFailure);
            }
        }
        self.state
            .archived_fills
            .sort_unstable_by_key(|fill| fill.sequence());
        let active_orders = &self.state.orders;
        let archived_orders = &self.state.archived_orders;
        self.state.reconciled_orders.retain(|order_id| {
            active_orders.contains_key(order_id) || archived_orders.contains_key(order_id)
        });
        Ok(())
    }

    fn submit(
        &mut self,
        event_sequence: u64,
        dispatch: DispatchOrder,
    ) -> Result<ExecutionReceipt, ExecutionAdapterError> {
        self.refresh_audit_health();
        if self.state.reconciliation_required {
            return Err(ExecutionAdapterError::ReconciliationRequired);
        }
        if dispatch.operation().is_expired() {
            return Err(ExecutionAdapterError::KnownFailure);
        }
        self.compact(dispatch.submitted_at())?;
        if dispatch.operation().is_expired() {
            return Err(ExecutionAdapterError::KnownFailure);
        }
        let key = (dispatch.account_id(), dispatch.client_order_id().clone());
        if let Some(existing_id) = self.state.idempotency.get(&key) {
            let existing = self
                .state
                .orders
                .get(existing_id)
                .ok_or(ExecutionAdapterError::ReconciliationRequired)?;
            if *existing_id == dispatch.order_id()
                && existing.lifecycle.state() != PaperOrderState::Rejected
            {
                return Ok(ExecutionReceipt::new(*existing_id, existing.accepted_at));
            }
            return Err(ExecutionAdapterError::Rejected);
        }
        if self.state.orders.contains_key(&dispatch.order_id())
            || self
                .state
                .archived_orders
                .contains_key(&dispatch.order_id())
            || self.state.archived_orders.values().any(|order| {
                order.account_id == dispatch.account_id()
                    && order.client_order_id == *dispatch.client_order_id()
            })
            || self.state.orders.len() >= self.config.input().maximum_orders.get()
            || self.state.idempotency.len() >= self.config.input().maximum_idempotency_keys.get()
            || dispatch.valid_until() < dispatch.submitted_at()
        {
            return Err(ExecutionAdapterError::Rejected);
        }
        let latency = sample_latency(
            self.config.input().deterministic_seed,
            self.config.input().configuration_version.get(),
            dispatch.order_id(),
            self.config.input().minimum_latency_nanos,
            self.config.input().maximum_latency_nanos,
            b"submit",
        );
        let latency = i64::try_from(latency).map_err(|_| ExecutionAdapterError::Rejected)?;
        let eligible_at = dispatch
            .submitted_at()
            .checked_add_nanos(latency)
            .map_err(|_| ExecutionAdapterError::Rejected)?;
        let expires_at = if dispatch.time_in_force() == TimeInForce::Day {
            let day_expiry = self
                .config
                .input()
                .day_session_calendar
                .day_expires_at(
                    dispatch.evidence_binding().venue_id(),
                    dispatch.submitted_at(),
                )
                .map_err(|_| ExecutionAdapterError::Rejected)?;
            dispatch.intent_expires_at().min(day_expiry)
        } else {
            dispatch.intent_expires_at()
        };
        if eligible_at > expires_at {
            return Err(ExecutionAdapterError::Rejected);
        }
        if dispatch.operation().is_expired() {
            return Err(ExecutionAdapterError::KnownFailure);
        }
        let mut order =
            PaperOrder::from_dispatch(dispatch, event_sequence, eligible_at, expires_at)
                .map_err(|_| ExecutionAdapterError::Rejected)?;
        let mutation_sequence = self.next_mutation_sequence()?;
        let reservation_price = reservation_price(&order)?;
        let mut candidate_ledger = self.state.ledger.clone();
        let reservation = candidate_ledger.reserve(
            order.order_id,
            order.account_id,
            order.terms,
            order.side,
            order.quantity,
            reservation_price,
        );
        let (kind, result) = if reservation.is_ok() {
            order
                .lifecycle
                .accept(mutation_sequence)
                .map_err(|_| ExecutionAdapterError::KnownFailure)?;
            (PaperAuditKind::Accepted, Ok(()))
        } else {
            order
                .lifecycle
                .reject(mutation_sequence)
                .map_err(|_| ExecutionAdapterError::KnownFailure)?;
            (
                PaperAuditKind::Rejected,
                Err(ExecutionAdapterError::Rejected),
            )
        };
        let audit = PaperAuditRecord::new(
            mutation_sequence,
            Some(order.order_id),
            kind,
            Some(PaperOrderState::New),
            Some(order.lifecycle.state()),
            order.accepted_at,
            None,
            self.config.digest(),
            order.input_digest(),
        );
        self.admit_audit(audit)?;
        let receipt = ExecutionReceipt::new(order.order_id, order.accepted_at);
        self.state.sequence = mutation_sequence;
        if result.is_ok() {
            self.state.ledger = candidate_ledger;
        } else {
            self.state.reconciled_orders.insert(order.order_id);
        }
        self.state.idempotency.insert(key, order.order_id);
        self.state.orders.insert(order.order_id, order);
        result.map(|()| receipt)
    }

    fn cancel(
        &mut self,
        _event_sequence: u64,
        order_id: OrderId,
        requested_at: Timestamp,
    ) -> Result<CancelReceipt, ExecutionAdapterError> {
        self.refresh_audit_health();
        if self.state.reconciliation_required {
            return Err(ExecutionAdapterError::ReconciliationRequired);
        }
        let existing = self
            .state
            .orders
            .get(&order_id)
            .cloned()
            .ok_or(ExecutionAdapterError::Rejected)?;
        if is_terminal(existing.lifecycle.state()) {
            return cancel_receipt(&existing, CancelStatus::AlreadyTerminal, requested_at);
        }
        if existing.lifecycle.state() == PaperOrderState::CancelPending {
            return cancel_receipt(&existing, CancelStatus::Pending, requested_at);
        }
        let sequence = self.next_mutation_sequence()?;
        let mut candidate = existing.clone();
        candidate
            .lifecycle
            .request_cancel(sequence)
            .map_err(|_| ExecutionAdapterError::KnownFailure)?;
        candidate.cancel_effective_at = Some(
            requested_at
                .checked_add_nanos(
                    i64::try_from(self.config.input().cancel_latency_nanos)
                        .map_err(|_| ExecutionAdapterError::KnownFailure)?,
                )
                .map_err(|_| ExecutionAdapterError::KnownFailure)?,
        );
        self.admit_audit(PaperAuditRecord::new(
            sequence,
            Some(order_id),
            PaperAuditKind::CancelRequested,
            Some(existing.lifecycle.state()),
            Some(PaperOrderState::CancelPending),
            requested_at,
            None,
            self.config.digest(),
            candidate.input_digest(),
        ))?;
        self.state.sequence = sequence;
        self.state.orders.insert(order_id, candidate.clone());
        cancel_receipt(&candidate, CancelStatus::Pending, requested_at)
    }

    fn process_market(&mut self, event: WorkerMarketUpdate) {
        self.refresh_audit_health();
        if self.state.reconciliation_required {
            return;
        }
        let Ok(mut available) = AvailableMarket::try_new(event.update, &self.config) else {
            self.state.reconciliation_required = true;
            return;
        };
        let instrument = available.market().execution_terms().instrument_id();
        let event_at = available.market().observed_at();
        let mut ids: Vec<_> = self
            .state
            .orders
            .values()
            .filter(|order| {
                order.terms.instrument_id() == instrument && !is_terminal(order.lifecycle.state())
            })
            .map(|order| order.order_id)
            .collect();
        ids.sort_by(|left, right| order_priority(&self.state.orders, *left, *right));
        for order_id in ids {
            if self.state.reconciliation_required {
                break;
            }
            let Some(order) = self.state.orders.get(&order_id).cloned() else {
                self.state.reconciliation_required = true;
                break;
            };
            if event_at > order.expires_at {
                self.expire_order(order, event_at);
                continue;
            }
            if order
                .cancel_effective_at
                .is_some_and(|deadline| event_at >= deadline)
            {
                self.confirm_cancel(order, event_at);
                continue;
            }
            let Ok(plan) = available.plan(&order, event.update, &self.config) else {
                self.state.reconciliation_required = true;
                break;
            };
            let has_fill = !plan.legs.is_empty();
            let mut cancel_remainder_handled = false;
            if has_fill {
                if self.state.fills.len() >= self.config.input().maximum_fills.get() {
                    self.state.reconciliation_required = true;
                    break;
                }
                let legs = plan.fill_legs();
                let mut candidate_ledger = self.state.ledger.clone();
                let Ok(fill) =
                    candidate_ledger.apply_fill(order_id, order.terms, &legs, plan.liquidity)
                else {
                    self.state.reconciliation_required = true;
                    break;
                };
                let Ok(fill_sequence) = self.next_mutation_sequence() else {
                    self.state.reconciliation_required = true;
                    break;
                };
                let mut candidate = order.clone();
                candidate.triggered = plan.triggered;
                candidate.resting |= plan.became_resting;
                if candidate.apply_fill(fill, fill_sequence).is_err() {
                    self.state.reconciliation_required = true;
                    break;
                }
                let mut mutation_sequence = fill_sequence;
                if plan.cancel_remainder && !is_terminal(candidate.lifecycle.state()) {
                    let Some(request_sequence) = mutation_sequence.checked_add(1) else {
                        self.state.reconciliation_required = true;
                        break;
                    };
                    let Some(cancel_sequence) = request_sequence.checked_add(1) else {
                        self.state.reconciliation_required = true;
                        break;
                    };
                    if candidate
                        .lifecycle
                        .request_cancel(request_sequence)
                        .is_err()
                        || candidate.lifecycle.confirm_cancel(cancel_sequence).is_err()
                        || candidate_ledger.release(order_id).is_err()
                    {
                        self.state.reconciliation_required = true;
                        break;
                    }
                    mutation_sequence = cancel_sequence;
                    cancel_remainder_handled = true;
                }
                let audit = PaperAuditRecord::new(
                    mutation_sequence,
                    Some(order_id),
                    PaperAuditKind::Filled,
                    Some(order.lifecycle.state()),
                    Some(candidate.lifecycle.state()),
                    event_at,
                    Some(fill.quantity()),
                    self.config.digest(),
                    market_digest(&candidate, event),
                );
                if available.consume(order.side, &plan).is_err() {
                    self.state.reconciliation_required = true;
                    break;
                }
                if !self.admit_committed_event_audit(audit) {
                    break;
                }
                self.state.sequence = mutation_sequence;
                self.state.ledger = candidate_ledger;
                self.state.fills.push(PaperFillSnapshot::new(
                    mutation_sequence,
                    order_id,
                    event_at,
                    fill.quantity(),
                    fill.average_price(),
                    fill.notional(),
                    fill.fee(),
                    fill.liquidity(),
                ));
                self.state.orders.insert(order_id, candidate);
            } else if plan.triggered != order.triggered || plan.became_resting {
                self.update_matching_state(order, &plan, event);
            }
            if plan.cancel_remainder && !cancel_remainder_handled {
                let Some(current) = self.state.orders.get(&order_id).cloned() else {
                    self.state.reconciliation_required = true;
                    break;
                };
                if !is_terminal(current.lifecycle.state()) {
                    self.cancel_remainder(current, event_at);
                }
            }
        }
    }

    fn update_matching_state(
        &mut self,
        order: PaperOrder,
        plan: &crate::matching::MatchPlan,
        event: WorkerMarketUpdate,
    ) {
        let Ok(sequence) = self.next_mutation_sequence() else {
            self.state.reconciliation_required = true;
            return;
        };
        let mut candidate = order.clone();
        candidate.triggered = plan.triggered;
        candidate.resting |= plan.became_resting;
        let audit = PaperAuditRecord::new(
            sequence,
            Some(order.order_id),
            PaperAuditKind::ActivatedOrResting,
            Some(order.lifecycle.state()),
            Some(candidate.lifecycle.state()),
            event.update.market().observed_at(),
            None,
            self.config.digest(),
            market_digest(&candidate, event),
        );
        if self.admit_committed_event_audit(audit) {
            self.state.sequence = sequence;
            self.state.orders.insert(order.order_id, candidate);
        }
    }

    fn advance_due(&mut self, now: Timestamp) {
        self.refresh_audit_health();
        if self.state.reconciliation_required {
            return;
        }
        let ids: Vec<_> = self
            .state
            .orders
            .values()
            .filter(|order| !is_terminal(order.lifecycle.state()))
            .map(|order| order.order_id)
            .collect();
        for order_id in ids {
            if self.state.reconciliation_required {
                break;
            }
            let Some(order) = self.state.orders.get(&order_id).cloned() else {
                self.state.reconciliation_required = true;
                break;
            };
            if now > order.expires_at {
                self.expire_order(order, now);
            } else if order
                .cancel_effective_at
                .is_some_and(|deadline| now >= deadline)
            {
                self.confirm_cancel(order, now);
            }
        }
    }

    fn confirm_cancel(&mut self, order: PaperOrder, event_at: Timestamp) {
        let Ok(sequence) = self.next_mutation_sequence() else {
            self.state.reconciliation_required = true;
            return;
        };
        let mut candidate = order.clone();
        if candidate.lifecycle.confirm_cancel(sequence).is_err() {
            self.state.reconciliation_required = true;
            return;
        }
        let mut ledger = self.state.ledger.clone();
        if candidate
            .remaining()
            .is_ok_and(|remaining| remaining.get() != 0)
            && ledger.release(order.order_id).is_err()
        {
            self.state.reconciliation_required = true;
            return;
        }
        let audit = state_audit(
            &self.config,
            sequence,
            &order,
            &candidate,
            PaperAuditKind::Canceled,
            event_at,
        );
        if self.admit_committed_event_audit(audit) {
            self.state.sequence = sequence;
            self.state.ledger = ledger;
            self.state.orders.insert(order.order_id, candidate);
        }
    }

    fn cancel_remainder(&mut self, order: PaperOrder, event_at: Timestamp) {
        let Some(request_sequence) = self.state.sequence.checked_add(1) else {
            self.state.reconciliation_required = true;
            return;
        };
        let Some(cancel_sequence) = request_sequence.checked_add(1) else {
            self.state.reconciliation_required = true;
            return;
        };
        let mut candidate = order.clone();
        if candidate
            .lifecycle
            .request_cancel(request_sequence)
            .is_err()
            || candidate.lifecycle.confirm_cancel(cancel_sequence).is_err()
        {
            self.state.reconciliation_required = true;
            return;
        }
        let mut ledger = self.state.ledger.clone();
        if ledger.release(order.order_id).is_err() {
            self.state.reconciliation_required = true;
            return;
        }
        let audit = state_audit(
            &self.config,
            cancel_sequence,
            &order,
            &candidate,
            PaperAuditKind::Canceled,
            event_at,
        );
        if self.admit_committed_event_audit(audit) {
            self.state.sequence = cancel_sequence;
            self.state.ledger = ledger;
            self.state.orders.insert(order.order_id, candidate);
        }
    }

    fn expire_order(&mut self, order: PaperOrder, event_at: Timestamp) {
        let Ok(sequence) = self.next_mutation_sequence() else {
            self.state.reconciliation_required = true;
            return;
        };
        let mut candidate = order.clone();
        if candidate.lifecycle.expire(sequence).is_err() {
            self.state.reconciliation_required = true;
            return;
        }
        let mut ledger = self.state.ledger.clone();
        if ledger.release(order.order_id).is_err() {
            self.state.reconciliation_required = true;
            return;
        }
        let audit = state_audit(
            &self.config,
            sequence,
            &order,
            &candidate,
            PaperAuditKind::Expired,
            event_at,
        );
        if self.admit_committed_event_audit(audit) {
            self.state.sequence = sequence;
            self.state.ledger = ledger;
            self.state.orders.insert(order.order_id, candidate);
        }
    }
}
