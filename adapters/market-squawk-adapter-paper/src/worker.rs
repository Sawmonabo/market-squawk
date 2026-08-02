//! Single-writer paper state, matching, reconciliation, and shutdown.

use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroU64;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};

use market_squawk_domain::{AccountId, ClientOrderId, OrderId, TimeInForce, Timestamp};
use market_squawk_execution::{
    AccountRiskReconciliationFence, CancelOrder, CancelReceipt, CancelStatus, DispatchOrder,
    ExecutionAdapterError, ExecutionMarketUpdate, ExecutionReceipt, ExecutionState,
    PersistenceAcknowledgement, ReconcileOrders, ReconciliationAcknowledgement,
    ReconciliationBatchBinding, RecoverExecutionState,
};
use tokio::sync::{Mutex, OwnedSemaphorePermit, mpsc, oneshot, watch};
use tokio_util::sync::CancellationToken;

use crate::audit::{PaperAuditKind, PaperAuditRecord};
use crate::latency::sample_latency;
use crate::ledger::PaperMarkDisposition;
use crate::matching::AvailableMarket;
use crate::order::PaperOrder;
use crate::snapshot::{
    PaperCheckpointPersistenceEvidence, PaperExecutionCheckpoint, PaperExecutionSnapshot,
    PaperFillSnapshot,
};
use crate::{
    PaperCheckpointReceipt, PaperControlContext, PaperControlError, PaperExecutionConfig,
    PaperLedger, PaperOrderState, PaperRecoveryInitialization,
};

#[path = "worker/reconciliation.rs"]
mod reconciliation;
use reconciliation::{
    cancel_receipt, is_terminal, mark_mutation_digest, market_digest, order_priority,
    reservation_price, state_audit,
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
    RecoverQuarantined {
        recovery: RecoverExecutionState,
        reply: oneshot::Sender<Result<(), ExecutionAdapterError>>,
    },
    AcknowledgePersistence {
        authority: PersistenceAcknowledgement,
        receipt: PaperCheckpointReceipt,
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
    InitializeRecovery {
        control: PaperControlContext,
        reply: oneshot::Sender<Result<PaperRecoveryInitialization, PaperControlError>>,
    },
    Shutdown {
        control: PaperControlContext,
        reply: oneshot::Sender<Result<PaperExecutionSnapshot, PaperControlError>>,
    },
}

impl WorkerCommand {
    pub(crate) fn retained_bytes(&self) -> Result<usize, ExecutionAdapterError> {
        let inline = WORKER_ENVELOPE_RETAINED_BYTES;
        let additional = match self {
            Self::Submit { .. } => return Ok(inline.max(64 * 1024)),
            Self::Reconcile { request, .. } => std::mem::size_of_val(request.order_ids()),
            Self::AcknowledgeReconciliation {
                acknowledgement, ..
            } => std::mem::size_of_val(acknowledgement.order_ids()),
            Self::RecoverQuarantined { .. } => 0,
            Self::AcknowledgePersistence {
                authority, receipt, ..
            } => authority
                .retained_bytes()
                .and_then(|retained| {
                    retained.checked_sub(std::mem::size_of::<PersistenceAcknowledgement>())
                })
                .and_then(|retained| retained.checked_add(receipt.retained_heap_bytes()))
                .ok_or(ExecutionAdapterError::KnownFailure)?,
            Self::Cancel { .. }
            | Self::Snapshot { .. }
            | Self::Checkpoint { .. }
            | Self::InitializeRecovery { .. }
            | Self::Shutdown { .. } => 0,
        };
        inline
            .checked_add(additional)
            .ok_or(ExecutionAdapterError::KnownFailure)
    }
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
    pub(crate) _retained_bytes: Option<OwnedSemaphorePermit>,
}

pub(crate) const WORKER_ENVELOPE_RETAINED_BYTES: usize = std::mem::size_of::<WorkerEnvelope>();

#[derive(Clone, Copy, Debug)]
pub(crate) struct WorkerMarketUpdate {
    pub(crate) sequence: u64,
    pub(crate) update: ExecutionMarketUpdate,
}

#[derive(Debug)]
pub(crate) struct PaperWorker {
    config: PaperExecutionConfig,
    repository_id: [u8; 32],
    state: WorkerState,
    events: mpsc::Receiver<WorkerEnvelope>,
    audit: mpsc::Sender<PaperAuditRecord>,
    audit_failed: Arc<AtomicBool>,
    cancellation: CancellationToken,
    reconciliation_fence: Option<AccountRiskReconciliationFence>,
    financial_changes: watch::Sender<u64>,
    event_sequence: Arc<Mutex<u64>>,
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
    accepted_repository_id: [u8; 32],
    accepted_repository_generation: u64,
    reconciled_orders: BTreeSet<OrderId>,
    acknowledged_reconciliation_batches: Vec<ReconciliationBatchBinding>,
    issued_checkpoint: Option<IssuedCheckpoint>,
    ledger: PaperLedger,
    idempotency: BTreeMap<(AccountId, ClientOrderId), OrderId>,
    recovery_pending: bool,
    recovery_input_digest: Option<[u8; 32]>,
}

#[derive(Debug)]
struct IssuedCheckpoint {
    evidence: PaperCheckpointPersistenceEvidence,
    acknowledged_reconciliation_batches: Box<[ReconciliationBatchBinding]>,
}

#[derive(Debug)]
struct CompactionPlan {
    orders: BTreeMap<OrderId, PaperOrder>,
    archived_orders: BTreeMap<OrderId, PaperOrder>,
    fills: Vec<PaperFillSnapshot>,
    archived_fills: Vec<PaperFillSnapshot>,
    idempotency: BTreeMap<(AccountId, ClientOrderId), OrderId>,
    reconciled_orders: BTreeSet<OrderId>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum QueuedMarketFreshness {
    Fresh,
    Stale,
}

fn queued_market_freshness(
    observed_at: Timestamp,
    processed_at: Timestamp,
    maximum_age_nanos: u64,
) -> Result<QueuedMarketFreshness, ()> {
    if maximum_age_nanos == 0 {
        return Err(());
    }
    let age = i128::from(processed_at.unix_nanos()) - i128::from(observed_at.unix_nanos());
    if age < 0 {
        Err(())
    } else if age >= i128::from(maximum_age_nanos) {
        Ok(QueuedMarketFreshness::Stale)
    } else {
        Ok(QueuedMarketFreshness::Fresh)
    }
}

fn ensure_compaction_active(
    operation: Option<&market_squawk_execution::ExecutionOperation>,
) -> Result<(), ExecutionAdapterError> {
    if operation.is_some_and(market_squawk_execution::ExecutionOperation::is_expired) {
        Err(ExecutionAdapterError::KnownFailure)
    } else {
        Ok(())
    }
}

pub(crate) fn receipt_authority_is_current(
    expected_repository_id: [u8; 32],
    accepted_repository_id: [u8; 32],
    accepted_repository_generation: u64,
    receipt: &PaperCheckpointReceipt,
) -> bool {
    let minimum_generation = if accepted_repository_id == expected_repository_id {
        accepted_repository_generation
    } else {
        0
    };
    receipt.authority_is_valid(expected_repository_id, minimum_generation)
}

impl PaperWorker {
    #[allow(
        clippy::too_many_arguments,
        reason = "worker construction transfers each independently bounded owner"
    )]
    pub(crate) fn new(
        config: PaperExecutionConfig,
        repository_id: [u8; 32],
        ledger: PaperLedger,
        checkpoint: Option<PaperExecutionCheckpoint>,
        events: mpsc::Receiver<WorkerEnvelope>,
        audit: mpsc::Sender<PaperAuditRecord>,
        audit_failed: Arc<AtomicBool>,
        cancellation: CancellationToken,
        reconciliation_fence: Option<AccountRiskReconciliationFence>,
        financial_changes: watch::Sender<u64>,
        event_sequence: Arc<Mutex<u64>>,
        recovery_input_digest: Option<[u8; 32]>,
    ) -> Self {
        let recovery_pending = checkpoint.is_some();
        let state = if let Some(checkpoint) = checkpoint {
            WorkerState {
                sequence: checkpoint.sequence,
                reconciliation_required: checkpoint.reconciliation_required,
                orders: checkpoint.orders,
                fills: checkpoint.fills,
                archived_orders: checkpoint.archived_orders,
                archived_fills: checkpoint.archived_fills,
                durable_sequence: checkpoint.durable_sequence,
                accepted_repository_id: checkpoint.accepted_repository_id,
                accepted_repository_generation: checkpoint.accepted_repository_generation,
                reconciled_orders: checkpoint.reconciled_orders,
                acknowledged_reconciliation_batches: checkpoint.acknowledged_reconciliation_batches,
                issued_checkpoint: None,
                ledger: checkpoint.ledger,
                idempotency: checkpoint.idempotency,
                recovery_pending,
                recovery_input_digest,
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
                accepted_repository_id: [0; 32],
                accepted_repository_generation: 0,
                reconciled_orders: BTreeSet::new(),
                acknowledged_reconciliation_batches: Vec::new(),
                issued_checkpoint: None,
                ledger,
                idempotency: BTreeMap::new(),
                recovery_pending,
                recovery_input_digest,
            }
        };
        Self {
            config,
            repository_id,
            state,
            events,
            audit,
            audit_failed,
            cancellation,
            reconciliation_fence,
            financial_changes,
            event_sequence,
        }
    }

    fn prepare_financial_audit(
        &mut self,
        sequence: u64,
        audit: PaperAuditRecord,
    ) -> Option<PreparedFinancialMutationAudit> {
        self.refresh_audit_health();
        if self.state.reconciliation_required {
            return None;
        }
        let Some(sequence) = NonZeroU64::new(sequence) else {
            self.state.reconciliation_required = true;
            return None;
        };
        let prepared = prepare_financial_mutation(
            &self.audit,
            self.reconciliation_fence.as_ref(),
            sequence,
            audit,
        );
        match prepared {
            Ok(prepared) => Some(prepared),
            Err(_) => {
                self.state.reconciliation_required = true;
                None
            }
        }
    }

    fn publish_financial_mutation(&self, sequence: u64) {
        self.financial_changes.send_replace(sequence);
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
                        _retained_bytes,
                    } = envelope;
                    match event {
                        WorkerEvent::Command(command) => {
                            if self.handle_command(sequence, command).await {
                                break;
                            }
                        }
                        WorkerEvent::Market(update) => {
                            self.process_market(WorkerMarketUpdate { sequence, update }).await;
                        }
                    }
                }
            }
        }
    }

    async fn handle_command(&mut self, event_sequence: u64, command: WorkerCommand) -> bool {
        match command {
            WorkerCommand::Submit { order, reply } => {
                let result = if self.state.recovery_pending {
                    Err(ExecutionAdapterError::ReconciliationRequired)
                } else if order.operation().is_expired() {
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
                let result = if self.state.recovery_pending {
                    Err(ExecutionAdapterError::ReconciliationRequired)
                } else if order.operation().is_expired() {
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
                let result = if self.state.recovery_pending {
                    Err(ExecutionAdapterError::ReconciliationRequired)
                } else if request.operation().is_expired() {
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
                let result = if self.state.recovery_pending {
                    Err(ExecutionAdapterError::ReconciliationRequired)
                } else {
                    self.acknowledge_reconciliation(acknowledgement)
                };
                let _ = reply.send(result);
                false
            }
            WorkerCommand::RecoverQuarantined { recovery, reply } => {
                let result = if self.state.recovery_pending {
                    Err(ExecutionAdapterError::ReconciliationRequired)
                } else {
                    self.recover_quarantined(recovery)
                };
                let _ = reply.send(result);
                false
            }
            WorkerCommand::AcknowledgePersistence {
                authority,
                receipt,
                reply,
            } => {
                let result = if self.state.recovery_pending {
                    Err(PaperControlError::RecoveryInitializationUnavailable)
                } else {
                    self.acknowledge_persistence(authority, receipt)
                };
                let _ = reply.send(result);
                false
            }
            WorkerCommand::Snapshot { control, reply } => {
                let result = if control.is_expired() {
                    Err(PaperControlError::DeadlineExceeded)
                } else if self.state.recovery_pending {
                    Err(PaperControlError::RecoveryInitializationUnavailable)
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
                } else if self.state.recovery_pending {
                    Err(PaperControlError::RecoveryInitializationUnavailable)
                } else {
                    self.refresh_audit_health();
                    self.checkpoint().map_err(|_| PaperControlError::Closed)
                };
                let _ = reply.send(result);
                false
            }
            WorkerCommand::InitializeRecovery { control, reply } => {
                let result = if control.is_expired() {
                    Err(PaperControlError::DeadlineExceeded)
                } else {
                    self.initialize_recovery(&control).await
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

    async fn initialize_recovery(
        &mut self,
        control: &PaperControlContext,
    ) -> Result<PaperRecoveryInitialization, PaperControlError> {
        self.refresh_audit_health();
        if !self.state.recovery_pending || self.audit_failed.load(AtomicOrdering::Acquire) {
            return Err(PaperControlError::RecoveryInitializationUnavailable);
        }
        let input_digest = self
            .state
            .recovery_input_digest
            .ok_or(PaperControlError::RecoveryInitializationUnavailable)?;
        let recovered_at =
            crate::adapter::system_timestamp().map_err(PaperControlError::Adapter)?;
        let recovery_sequence = self
            .state
            .sequence
            .checked_add(1)
            .ok_or(PaperControlError::RecoveryInitializationUnavailable)?;
        let final_sequence = if self.state.reconciliation_required {
            recovery_sequence
                .checked_add(1)
                .ok_or(PaperControlError::RecoveryInitializationUnavailable)?
        } else {
            recovery_sequence
        };
        let record_count = 1 + usize::from(self.state.reconciliation_required);
        let mut permits =
            self.audit
                .try_reserve_many(record_count)
                .map_err(|error| match error {
                    mpsc::error::TrySendError::Full(_) => {
                        PaperControlError::Adapter(ExecutionAdapterError::NotAttemptedBusy)
                    }
                    mpsc::error::TrySendError::Closed(_) => {
                        PaperControlError::RecoveryInitializationUnavailable
                    }
                })?;
        let cancellation = control.cancellation();
        let mut event_sequence = tokio::select! {
            biased;
            () = cancellation.cancelled() => return Err(PaperControlError::Cancelled),
            () = tokio::time::sleep_until(control.deadline()) => {
                return Err(PaperControlError::DeadlineExceeded);
            }
            sequence = self.event_sequence.lock() => sequence,
        };
        let recovery_permit = permits
            .next()
            .ok_or(PaperControlError::RecoveryInitializationUnavailable)?;
        let reconciliation_permit = if self.state.reconciliation_required {
            Some(
                permits
                    .next()
                    .ok_or(PaperControlError::RecoveryInitializationUnavailable)?,
            )
        } else {
            None
        };
        *event_sequence = (*event_sequence).max(final_sequence);
        let recovery = PaperAuditRecord::new(
            recovery_sequence,
            None,
            PaperAuditKind::RecoveryLoaded,
            None,
            None,
            recovered_at,
            None,
            self.config.digest(),
            input_digest,
        );
        recovery_permit.send(recovery);
        if let Some(reconciliation_permit) = reconciliation_permit {
            reconciliation_permit.send(PaperAuditRecord::new(
                final_sequence,
                None,
                PaperAuditKind::ReconciliationRequired,
                None,
                None,
                recovered_at,
                None,
                self.config.digest(),
                input_digest,
            ));
        }
        self.state.sequence = final_sequence;
        self.state.recovery_pending = false;
        self.state.recovery_input_digest = None;
        Ok(PaperRecoveryInitialization {
            sequence: NonZeroU64::new(final_sequence)
                .ok_or(PaperControlError::RecoveryInitializationUnavailable)?,
            quarantined: self.state.reconciliation_required,
        })
    }

    fn recover_quarantined(
        &mut self,
        recovery: RecoverExecutionState,
    ) -> Result<(), ExecutionAdapterError> {
        if recovery.operation().is_expired() {
            return Err(ExecutionAdapterError::KnownFailure);
        }
        self.refresh_audit_health();
        if self.audit_failed.load(AtomicOrdering::Acquire) {
            return Err(ExecutionAdapterError::ReconciliationRequired);
        }
        if !self.state.reconciliation_required {
            return Ok(());
        }
        let sequence = self.next_mutation_sequence()?;
        let sequence_authority =
            NonZeroU64::new(sequence).ok_or(ExecutionAdapterError::ReconciliationRequired)?;
        self.reconciliation_fence
            .as_ref()
            .ok_or(ExecutionAdapterError::ReconciliationRequired)?
            .require(sequence_authority)
            .map_err(|_| ExecutionAdapterError::ReconciliationRequired)?;
        let recovered_at = crate::adapter::system_timestamp()?;
        let input_digest = self
            .checkpoint()
            .and_then(|checkpoint| checkpoint.recovery_input_digest())
            .map_err(|_| ExecutionAdapterError::KnownFailure)?;
        if recovery.operation().is_expired() {
            return Err(ExecutionAdapterError::KnownFailure);
        }
        self.audit
            .try_send(PaperAuditRecord::new(
                sequence,
                None,
                PaperAuditKind::ReconciliationCleared,
                None,
                None,
                recovered_at,
                None,
                self.config.digest(),
                input_digest,
            ))
            .map_err(|error| match error {
                mpsc::error::TrySendError::Full(_) => ExecutionAdapterError::NotAttemptedBusy,
                mpsc::error::TrySendError::Closed(_) => {
                    ExecutionAdapterError::ReconciliationRequired
                }
            })?;
        self.state.sequence = sequence;
        self.state.reconciliation_required = false;
        Ok(())
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
        receipt: PaperCheckpointReceipt,
    ) -> Result<(), PaperControlError> {
        if !receipt_authority_is_current(
            self.repository_id,
            self.state.accepted_repository_id,
            self.state.accepted_repository_generation,
            &receipt,
        ) {
            return Err(PaperControlError::Adapter(
                ExecutionAdapterError::KnownFailure,
            ));
        }
        let evidence = receipt.persistence_evidence();
        let Some(issued) = self.state.issued_checkpoint.as_ref() else {
            return Err(PaperControlError::Adapter(
                ExecutionAdapterError::KnownFailure,
            ));
        };
        if authority.operation().is_expired()
            || evidence.configuration_digest != self.config.digest()
            || evidence.sequence > self.state.sequence
            || issued.evidence != evidence
        {
            return Err(PaperControlError::Adapter(
                ExecutionAdapterError::KnownFailure,
            ));
        }
        let mut prunable = Vec::new();
        if prunable
            .try_reserve_exact(issued.acknowledged_reconciliation_batches.len())
            .is_err()
        {
            return Err(PaperControlError::Adapter(
                ExecutionAdapterError::NotAttemptedBusy,
            ));
        }
        for binding in &issued.acknowledged_reconciliation_batches {
            if authority.operation().is_expired() {
                return Err(PaperControlError::DeadlineExceeded);
            }
            for finalized in authority.finalized_reconciliations() {
                if authority.operation().is_expired() {
                    return Err(PaperControlError::DeadlineExceeded);
                }
                if finalized == binding {
                    prunable.push(*binding);
                    break;
                }
            }
        }
        let mut retained_batches = Vec::new();
        retained_batches
            .try_reserve_exact(self.state.acknowledged_reconciliation_batches.len())
            .map_err(|_| PaperControlError::Adapter(ExecutionAdapterError::NotAttemptedBusy))?;
        for binding in &self.state.acknowledged_reconciliation_batches {
            if authority.operation().is_expired() {
                return Err(PaperControlError::DeadlineExceeded);
            }
            let mut is_prunable = false;
            for persisted in &prunable {
                if authority.operation().is_expired() {
                    return Err(PaperControlError::DeadlineExceeded);
                }
                if persisted == binding {
                    is_prunable = true;
                    break;
                }
            }
            if !is_prunable {
                retained_batches.push(*binding);
            }
        }
        let observed_at = match crate::adapter::system_timestamp() {
            Ok(observed_at) => observed_at,
            Err(_) => {
                return Err(PaperControlError::Adapter(
                    ExecutionAdapterError::KnownFailure,
                ));
            }
        };
        let durable_sequence = self.state.durable_sequence.max(evidence.sequence);
        let compaction = match self.prepare_compaction(
            observed_at,
            durable_sequence,
            Some(authority.operation()),
        ) {
            Ok(compaction) => compaction,
            Err(_) if authority.operation().is_expired() => {
                return Err(PaperControlError::DeadlineExceeded);
            }
            Err(error) => return Err(PaperControlError::Adapter(error)),
        };
        if authority.operation().is_expired() {
            return Err(PaperControlError::DeadlineExceeded);
        }
        authority
            .commit_persisted(&prunable)
            .map_err(PaperControlError::Adapter)?;
        self.state.durable_sequence = durable_sequence;
        self.state.accepted_repository_id = self.repository_id;
        self.state.accepted_repository_generation = receipt.generation().get();
        self.apply_compaction(compaction);
        self.state.acknowledged_reconciliation_batches = retained_batches;
        self.state.issued_checkpoint = None;
        Ok(())
    }

    fn compact(&mut self, observed_at: Timestamp) -> Result<(), ExecutionAdapterError> {
        let plan = self.prepare_compaction(observed_at, self.state.durable_sequence, None)?;
        self.apply_compaction(plan);
        Ok(())
    }

    fn prepare_compaction(
        &mut self,
        observed_at: Timestamp,
        durable_sequence: u64,
        operation: Option<&market_squawk_execution::ExecutionOperation>,
    ) -> Result<CompactionPlan, ExecutionAdapterError> {
        let should_purge = |order_id: &OrderId, order: &PaperOrder| {
            observed_at > order.expires_at
                && order.lifecycle.last_sequence() <= durable_sequence
                && self.state.reconciled_orders.contains(order_id)
        };
        let mut purged_archived_orders = Vec::new();
        purged_archived_orders
            .try_reserve_exact(self.state.archived_orders.len())
            .map_err(|_| ExecutionAdapterError::KnownFailure)?;
        for (order_id, order) in &self.state.archived_orders {
            ensure_compaction_active(operation)?;
            if should_purge(order_id, order) {
                purged_archived_orders.push(*order_id);
            }
        }
        let retained_archive_count = self
            .state
            .archived_orders
            .len()
            .checked_sub(purged_archived_orders.len())
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
        for (order_id, order) in &self.state.orders {
            ensure_compaction_active(operation)?;
            if candidates.len() >= available_archive {
                break;
            }
            if is_terminal(order.lifecycle.state())
                && order.lifecycle.last_sequence() <= durable_sequence
                && self.state.reconciled_orders.contains(order_id)
            {
                if self.state.archived_orders.contains_key(order_id) {
                    return Err(ExecutionAdapterError::KnownFailure);
                }
                candidates.push(*order_id);
            }
        }
        let mut moving_fills = 0_usize;
        for fill in &self.state.fills {
            ensure_compaction_active(operation)?;
            if candidates.binary_search(&fill.order_id()).is_ok() {
                moving_fills = moving_fills
                    .checked_add(1)
                    .ok_or(ExecutionAdapterError::KnownFailure)?;
            }
        }
        let maximum_archived_fills = self
            .config
            .input()
            .maximum_archived_orders
            .get()
            .checked_mul(self.config.input().maximum_fills.get())
            .ok_or(ExecutionAdapterError::KnownFailure)?;
        let mut retained_archived_fills = 0_usize;
        for fill in &self.state.archived_fills {
            ensure_compaction_active(operation)?;
            if self
                .state
                .archived_orders
                .get(&fill.order_id())
                .is_some_and(|order| !should_purge(&fill.order_id(), order))
            {
                retained_archived_fills = retained_archived_fills
                    .checked_add(1)
                    .ok_or(ExecutionAdapterError::KnownFailure)?;
            }
        }
        if retained_archived_fills
            .checked_add(moving_fills)
            .is_none_or(|total| total > maximum_archived_fills)
        {
            return Err(ExecutionAdapterError::KnownFailure);
        }

        let mut fills = Vec::new();
        fills
            .try_reserve_exact(
                self.state
                    .fills
                    .len()
                    .checked_sub(moving_fills)
                    .ok_or(ExecutionAdapterError::KnownFailure)?,
            )
            .map_err(|_| ExecutionAdapterError::KnownFailure)?;
        let final_archived_fill_count = retained_archived_fills
            .checked_add(moving_fills)
            .ok_or(ExecutionAdapterError::KnownFailure)?;
        let mut archived_fills = Vec::new();
        archived_fills
            .try_reserve_exact(final_archived_fill_count)
            .map_err(|_| ExecutionAdapterError::KnownFailure)?;
        for fill in &self.state.archived_fills {
            ensure_compaction_active(operation)?;
            if purged_archived_orders
                .binary_search(&fill.order_id())
                .is_err()
                && self.state.archived_orders.contains_key(&fill.order_id())
            {
                archived_fills.push(*fill);
            }
        }
        for fill in &self.state.fills {
            ensure_compaction_active(operation)?;
            if candidates.binary_search(&fill.order_id()).is_ok() {
                archived_fills.push(*fill);
            } else {
                fills.push(*fill);
            }
        }
        if archived_fills.len() != final_archived_fill_count {
            return Err(ExecutionAdapterError::KnownFailure);
        }
        ensure_compaction_active(operation)?;
        archived_fills.sort_unstable_by_key(|fill| fill.sequence());

        let expected_order_count = self
            .state
            .orders
            .len()
            .checked_sub(candidates.len())
            .ok_or(ExecutionAdapterError::KnownFailure)?;
        let mut orders = BTreeMap::new();
        for (order_id, order) in &self.state.orders {
            ensure_compaction_active(operation)?;
            if candidates.binary_search(order_id).is_err()
                && orders.insert(*order_id, order.clone()).is_some()
            {
                return Err(ExecutionAdapterError::KnownFailure);
            }
        }
        if orders.len() != expected_order_count {
            return Err(ExecutionAdapterError::KnownFailure);
        }

        let expected_archived_order_count = retained_archive_count
            .checked_add(candidates.len())
            .ok_or(ExecutionAdapterError::KnownFailure)?;
        let mut archived_orders = BTreeMap::new();
        for (order_id, order) in &self.state.archived_orders {
            ensure_compaction_active(operation)?;
            if purged_archived_orders.binary_search(order_id).is_err()
                && archived_orders.insert(*order_id, order.clone()).is_some()
            {
                return Err(ExecutionAdapterError::KnownFailure);
            }
        }
        for order_id in &candidates {
            ensure_compaction_active(operation)?;
            let order = self
                .state
                .orders
                .get(order_id)
                .ok_or(ExecutionAdapterError::KnownFailure)?;
            if archived_orders.insert(*order_id, order.clone()).is_some() {
                return Err(ExecutionAdapterError::KnownFailure);
            }
        }
        if archived_orders.len() != expected_archived_order_count {
            return Err(ExecutionAdapterError::KnownFailure);
        }

        for fill in &fills {
            ensure_compaction_active(operation)?;
            if !orders.contains_key(&fill.order_id()) {
                return Err(ExecutionAdapterError::KnownFailure);
            }
        }
        for fill in &archived_fills {
            ensure_compaction_active(operation)?;
            if !archived_orders.contains_key(&fill.order_id()) {
                return Err(ExecutionAdapterError::KnownFailure);
            }
        }

        let mut idempotency = BTreeMap::new();
        for (key, order_id) in &self.state.idempotency {
            ensure_compaction_active(operation)?;
            if candidates.binary_search(order_id).is_err()
                && (!orders.contains_key(order_id)
                    || idempotency.insert(key.clone(), *order_id).is_some())
            {
                return Err(ExecutionAdapterError::KnownFailure);
            }
        }

        let mut reconciled_orders = BTreeSet::new();
        for order_id in &self.state.reconciled_orders {
            ensure_compaction_active(operation)?;
            if (orders.contains_key(order_id) || archived_orders.contains_key(order_id))
                && !reconciled_orders.insert(*order_id)
            {
                return Err(ExecutionAdapterError::KnownFailure);
            }
        }
        ensure_compaction_active(operation)?;
        Ok(CompactionPlan {
            orders,
            archived_orders,
            fills,
            archived_fills,
            idempotency,
            reconciled_orders,
        })
    }

    fn apply_compaction(&mut self, plan: CompactionPlan) {
        let CompactionPlan {
            orders,
            archived_orders,
            fills,
            archived_fills,
            idempotency,
            reconciled_orders,
        } = plan;
        self.state.orders = orders;
        self.state.archived_orders = archived_orders;
        self.state.fills = fills;
        self.state.archived_fills = archived_fills;
        self.state.idempotency = idempotency;
        self.state.reconciled_orders = reconciled_orders;
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
            PaperOrder::from_dispatch(&dispatch, event_sequence, eligible_at, expires_at)
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
        if dispatch.operation().is_expired() {
            return Err(ExecutionAdapterError::KnownFailure);
        }
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

    async fn process_market(&mut self, event: WorkerMarketUpdate) {
        self.refresh_audit_health();
        if self.state.recovery_pending || self.state.reconciliation_required {
            return;
        }
        let Ok(mut available) = AvailableMarket::try_new(event.update, &self.config) else {
            self.state.reconciliation_required = true;
            return;
        };
        let instrument = available.market().execution_terms().instrument_id();
        let event_at = available.market().observed_at();
        if !matches!(
            event.update.event_class(),
            market_squawk_domain::LiveEventClass::Trade
                | market_squawk_domain::LiveEventClass::Quote
                | market_squawk_domain::LiveEventClass::BookSnapshot
                | market_squawk_domain::LiveEventClass::BookDelta
        ) {
            return;
        }
        let processed_at = match crate::adapter::system_timestamp() {
            Ok(processed_at) => processed_at,
            Err(_) => {
                self.state.reconciliation_required = true;
                return;
            }
        };
        match queued_market_freshness(
            event_at,
            processed_at,
            self.config.input().maximum_mark_age_nanos,
        ) {
            Ok(QueuedMarketFreshness::Fresh) => {}
            Ok(QueuedMarketFreshness::Stale) => return,
            Err(()) => {
                self.state.reconciliation_required = true;
                return;
            }
        }
        let mut marked_ledger = self.state.ledger.clone();
        match marked_ledger.apply_execution_market_update(
            event.update,
            processed_at,
            self.config.input().maximum_mark_age_nanos,
        ) {
            Ok(PaperMarkDisposition::Applied) => {}
            Ok(PaperMarkDisposition::Irrelevant) => return,
            Err(_) => {
                self.state.reconciliation_required = true;
                return;
            }
        }
        let account_risk_changed =
            marked_ledger.account_risk_snapshot() != self.state.ledger.account_risk_snapshot();
        let Ok(mark_sequence) = self.next_mutation_sequence() else {
            self.state.reconciliation_required = true;
            return;
        };
        let mark_audit = PaperAuditRecord::new(
            mark_sequence,
            None,
            PaperAuditKind::MarketMarked,
            None,
            None,
            event_at,
            None,
            self.config.digest(),
            mark_mutation_digest(event),
        );
        let financial_audit = if account_risk_changed {
            let Some(prepared) = self.prepare_financial_audit(mark_sequence, mark_audit) else {
                return;
            };
            Some(prepared)
        } else {
            if !self.admit_committed_event_audit(mark_audit) {
                return;
            }
            None
        };
        self.state.sequence = mark_sequence;
        self.state.ledger = marked_ledger;
        if let Some(financial_audit) = financial_audit {
            financial_audit.commit();
            self.publish_financial_mutation(mark_sequence);
        }
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
        for (processed, order_id) in ids.into_iter().enumerate() {
            if processed != 0
                && processed.is_multiple_of(self.config.input().matching_work_quantum.get())
            {
                tokio::task::yield_now().await;
            }
            if self.state.reconciliation_required {
                break;
            }
            let Some(order) = self.state.orders.get(&order_id).cloned() else {
                self.state.reconciliation_required = true;
                break;
            };
            if processed_at >= order.expires_at {
                self.expire_order(order, processed_at);
                continue;
            }
            if order
                .cancel_effective_at
                .is_some_and(|deadline| processed_at >= deadline)
            {
                self.confirm_cancel(order, processed_at);
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
                if candidate_ledger
                    .apply_execution_market_update(
                        event.update,
                        processed_at,
                        self.config.input().maximum_mark_age_nanos,
                    )
                    .is_err()
                {
                    self.state.reconciliation_required = true;
                    break;
                }
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
                let Some(financial_audit) = self.prepare_financial_audit(mutation_sequence, audit)
                else {
                    break;
                };
                self.state.sequence = mutation_sequence;
                self.state.ledger = candidate_ledger;
                self.state.fills.push(PaperFillSnapshot::new(
                    mutation_sequence,
                    order_id,
                    event_at,
                    fill.quantity(),
                    fill.average_price(),
                    fill.maximum_price(),
                    fill.notional(),
                    fill.fee(),
                    fill.liquidity(),
                ));
                self.state.orders.insert(order_id, candidate);
                financial_audit.commit();
                self.publish_financial_mutation(mutation_sequence);
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
        if let Some(financial_audit) = self.prepare_financial_audit(sequence, audit) {
            self.state.sequence = sequence;
            self.state.ledger = ledger;
            self.state.orders.insert(order.order_id, candidate);
            financial_audit.commit();
            self.publish_financial_mutation(sequence);
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
        if let Some(financial_audit) = self.prepare_financial_audit(cancel_sequence, audit) {
            self.state.sequence = cancel_sequence;
            self.state.ledger = ledger;
            self.state.orders.insert(order.order_id, candidate);
            financial_audit.commit();
            self.publish_financial_mutation(cancel_sequence);
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
        if let Some(financial_audit) = self.prepare_financial_audit(sequence, audit) {
            self.state.sequence = sequence;
            self.state.ledger = ledger;
            self.state.orders.insert(order.order_id, candidate);
            financial_audit.commit();
            self.publish_financial_mutation(sequence);
        }
    }
}

#[must_use = "a prepared financial audit must be committed after the state mutation"]
struct PreparedFinancialMutationAudit {
    permit: mpsc::OwnedPermit<PaperAuditRecord>,
    record: PaperAuditRecord,
}

impl PreparedFinancialMutationAudit {
    fn commit(self) {
        let _sender = self.permit.send(self.record);
    }
}

fn prepare_financial_mutation(
    audit: &mpsc::Sender<PaperAuditRecord>,
    reconciliation_fence: Option<&AccountRiskReconciliationFence>,
    sequence: NonZeroU64,
    record: PaperAuditRecord,
) -> Result<PreparedFinancialMutationAudit, ExecutionAdapterError> {
    let permit = audit
        .clone()
        .try_reserve_owned()
        .map_err(|error| match error {
            mpsc::error::TrySendError::Full(_) => ExecutionAdapterError::NotAttemptedBusy,
            mpsc::error::TrySendError::Closed(_) => ExecutionAdapterError::ReconciliationRequired,
        })?;
    if let Some(fence) = reconciliation_fence {
        fence
            .require(sequence)
            .map_err(|_| ExecutionAdapterError::ReconciliationRequired)?;
    }
    Ok(PreparedFinancialMutationAudit { permit, record })
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;
    use std::str::FromStr as _;

    use market_squawk_domain::{AccountId, Currency, Money, Timestamp};
    use market_squawk_execution::{
        AccountBootstrap, AccountCoordinatorConfig, AccountIdempotencyBootstrap,
        AccountRiskCoordinator, ExecutionAdapterError,
    };
    use rust_decimal::Decimal;
    use tokio::sync::mpsc;

    use super::{
        PaperAuditKind, PaperAuditRecord, QueuedMarketFreshness, WORKER_ENVELOPE_RETAINED_BYTES,
        WorkerEnvelope, WorkerMarketUpdate, prepare_financial_mutation, queued_market_freshness,
    };

    #[test]
    fn queued_market_freshness_is_exclusive_at_processing_time() {
        let observed_at = Timestamp::from_unix_nanos(100);
        assert_eq!(
            queued_market_freshness(observed_at, Timestamp::from_unix_nanos(109), 10),
            Ok(QueuedMarketFreshness::Fresh)
        );
        assert_eq!(
            queued_market_freshness(observed_at, Timestamp::from_unix_nanos(110), 10),
            Ok(QueuedMarketFreshness::Stale)
        );
        assert!(queued_market_freshness(observed_at, Timestamp::from_unix_nanos(99), 10).is_err());
    }

    #[test]
    fn market_ingress_charges_the_complete_channel_envelope() {
        assert_eq!(
            WORKER_ENVELOPE_RETAINED_BYTES,
            std::mem::size_of::<WorkerEnvelope>()
        );
        assert!(WORKER_ENVELOPE_RETAINED_BYTES > std::mem::size_of::<WorkerMarketUpdate>());
    }

    #[test]
    fn saturated_audit_admission_never_advances_the_financial_fence()
    -> Result<(), Box<dyn std::error::Error>> {
        let usd = Currency::try_from("USD")?;
        let account_id = AccountId::from_str("50000000-0000-0000-0000-000000000099")?;
        let accounts = AccountRiskCoordinator::try_new(
            AccountCoordinatorConfig::default(),
            [AccountBootstrap {
                account_id,
                revision: NonZeroU64::MIN,
                eligible: true,
                cash: Money::new(Decimal::TEN, usd),
                capital: Money::new(Decimal::TEN, usd),
                peak_capital: Money::new(Decimal::TEN, usd),
                gross_exposure: Money::new(Decimal::ZERO, usd),
                realized_pnl: Money::new(Decimal::ZERO, usd),
                realized_loss: Money::new(Decimal::ZERO, usd),
                positions: Vec::new(),
                position_cost_basis: Vec::new(),
                idempotency: AccountIdempotencyBootstrap::empty(),
            }],
        )?;
        let fence = accounts.reconciliation_fence();
        let (sender, mut receiver) = mpsc::channel(1);
        let record = PaperAuditRecord::new(
            1,
            None,
            PaperAuditKind::MarketMarked,
            None,
            None,
            Timestamp::from_unix_nanos(1),
            None,
            [1; 32],
            [2; 32],
        );
        sender.try_send(record)?;

        assert_eq!(
            prepare_financial_mutation(&sender, Some(&fence), NonZeroU64::MIN, record)
                .map(|prepared| prepared.commit()),
            Err(ExecutionAdapterError::NotAttemptedBusy)
        );
        assert!(fence.is_current());

        assert_eq!(receiver.try_recv(), Ok(record));
        let prepared = prepare_financial_mutation(&sender, Some(&fence), NonZeroU64::MIN, record)?;
        assert!(!fence.is_current());
        assert!(matches!(
            receiver.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
        prepared.commit();
        assert_eq!(receiver.try_recv(), Ok(record));
        Ok(())
    }
}
