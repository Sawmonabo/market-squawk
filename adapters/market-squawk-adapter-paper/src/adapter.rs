//! Bounded public adapter, market ingress, audit, and owned worker lifecycle.

use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex, TryLockError};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use market_squawk_domain::Timestamp;
use market_squawk_execution::{
    CancelOrder, CancelReceipt, DispatchOrder, ExecutionAdapter, ExecutionAdapterError,
    ExecutionAdapterFuture, ExecutionMarketSink, ExecutionMarketSinkError, ExecutionMarketUpdate,
    ExecutionReceipt, ExecutionState, ExecutionTask, ExecutionTaskReaper, ExecutionTaskReaperError,
    MAX_RECONCILED_ORDERS, PersistenceAcknowledgement, ReconcileOrders,
    ReconciliationAcknowledgement,
};
use thiserror::Error;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot};
use tokio_util::sync::CancellationToken;

use crate::PaperCheckpointRepository;
use crate::audit::{PaperAuditKind, PaperAuditReader, PaperAuditRecord};
use crate::snapshot::{PaperExecutionCheckpoint, PaperExecutionSnapshot};
use crate::worker::{PaperWorker, WorkerCommand, WorkerEnvelope, WorkerEvent};
use crate::{PaperAccountBootstrap, PaperCheckpointReceipt, PaperExecutionConfig, PaperLedger};

/// Caller-owned absolute deadline and cooperative cancellation for paper control operations.
#[derive(Debug)]
pub struct PaperControlContext {
    deadline: tokio::time::Instant,
    cancellation: CancellationToken,
}

impl PaperControlContext {
    /// Creates one bounded control lifetime starting now.
    pub fn try_new(
        timeout: Duration,
        cancellation: CancellationToken,
    ) -> Result<Self, PaperControlError> {
        if timeout.is_zero() {
            return Err(PaperControlError::InvalidDeadline);
        }
        let deadline = tokio::time::Instant::now()
            .checked_add(timeout)
            .ok_or(PaperControlError::InvalidDeadline)?;
        Ok(Self {
            deadline,
            cancellation,
        })
    }

    pub(crate) const fn deadline(&self) -> tokio::time::Instant {
        self.deadline
    }

    pub(crate) fn cancellation(&self) -> CancellationToken {
        self.cancellation.clone()
    }

    pub(crate) fn is_expired(&self) -> bool {
        self.cancellation.is_cancelled() || tokio::time::Instant::now() >= self.deadline
    }
}

/// Nonblocking dispatcher-facing paper adapter.
#[derive(Debug)]
pub struct PaperExecutionAdapter {
    events: mpsc::Sender<WorkerEnvelope>,
    command_slots: Arc<Semaphore>,
    command_bytes: Arc<Semaphore>,
    maximum_command_bytes: usize,
    event_sequence: Arc<Mutex<u64>>,
    retained_bytes: usize,
}

impl PaperExecutionAdapter {
    /// Returns the startup-fixed retained command-queue byte ceiling.
    pub const fn retained_bytes(&self) -> usize {
        self.retained_bytes
    }

    /// Requests a complete bounded state snapshot outside the live path.
    pub async fn snapshot(
        &self,
        control: PaperControlContext,
    ) -> Result<PaperExecutionSnapshot, PaperControlError> {
        let (reply, response) = oneshot::channel();
        let deadline = control.deadline();
        let cancellation = control.cancellation();
        self.send_control(
            WorkerCommand::Snapshot { control, reply },
            deadline,
            &cancellation,
        )
        .await?;
        control_response(response, deadline, cancellation).await
    }

    /// Exports a strict complete recovery checkpoint without performing filesystem I/O.
    pub async fn checkpoint(
        &self,
        control: PaperControlContext,
    ) -> Result<PaperExecutionCheckpoint, PaperControlError> {
        let (reply, response) = oneshot::channel();
        let deadline = control.deadline();
        let cancellation = control.cancellation();
        self.send_control(
            WorkerCommand::Checkpoint { control, reply },
            deadline,
            &cancellation,
        )
        .await?;
        control_response(response, deadline, cancellation).await
    }

    /// Advances the durable checkpoint fence only with dispatcher-minted one-use authority.
    pub async fn acknowledge_persistence(
        &self,
        authority: PersistenceAcknowledgement,
        receipt: PaperCheckpointReceipt,
    ) -> Result<(), PaperControlError> {
        let (reply, response) = oneshot::channel();
        self.try_send_command(WorkerCommand::AcknowledgePersistence {
            authority,
            receipt,
            reply,
        })
        .map_err(PaperControlError::Adapter)?;
        await_admitted_persistence_outcome(response).await
    }

    fn try_send_command(&self, command: WorkerCommand) -> Result<(), ExecutionAdapterError> {
        let retained_bytes = command.retained_bytes()?;
        if retained_bytes > self.maximum_command_bytes {
            return Err(ExecutionAdapterError::KnownFailure);
        }
        let slot = Arc::clone(&self.command_slots)
            .try_acquire_owned()
            .map_err(|_| {
                if self.events.is_closed() {
                    ExecutionAdapterError::KnownFailure
                } else {
                    ExecutionAdapterError::NotAttemptedBusy
                }
            })?;
        let retained_bytes = Arc::clone(&self.command_bytes)
            .try_acquire_many_owned(
                u32::try_from(retained_bytes).map_err(|_| ExecutionAdapterError::KnownFailure)?,
            )
            .map_err(|_| ExecutionAdapterError::NotAttemptedBusy)?;
        try_send_event(
            &self.events,
            &self.event_sequence,
            WorkerEvent::Command(command),
            slot,
            Some(retained_bytes),
        )
        .map_err(|error| match error {
            EnqueueError::Busy | EnqueueError::Full => ExecutionAdapterError::NotAttemptedBusy,
            EnqueueError::Closed | EnqueueError::Poisoned | EnqueueError::SequenceExhausted => {
                ExecutionAdapterError::KnownFailure
            }
        })
    }

    async fn send_control(
        &self,
        command: WorkerCommand,
        deadline: tokio::time::Instant,
        cancellation: &CancellationToken,
    ) -> Result<(), PaperControlError> {
        let retained_bytes = command
            .retained_bytes()
            .map_err(PaperControlError::Adapter)?;
        if retained_bytes > self.maximum_command_bytes {
            return Err(PaperControlError::Adapter(
                ExecutionAdapterError::KnownFailure,
            ));
        }
        let slot = tokio::select! {
            biased;
            () = cancellation.cancelled() => return Err(PaperControlError::Cancelled),
            result = tokio::time::timeout_at(deadline, Arc::clone(&self.command_slots).acquire_owned()) => {
                match result {
                    Ok(Ok(slot)) => slot,
                    Ok(Err(_)) => return Err(PaperControlError::Closed),
                    Err(_) => return Err(PaperControlError::DeadlineExceeded),
                }
            }
        };
        let retained_bytes = tokio::select! {
            biased;
            () = cancellation.cancelled() => return Err(PaperControlError::Cancelled),
            result = tokio::time::timeout_at(
                deadline,
                Arc::clone(&self.command_bytes).acquire_many_owned(
                    u32::try_from(retained_bytes).map_err(|_| {
                        PaperControlError::Adapter(ExecutionAdapterError::KnownFailure)
                    })?,
                ),
            ) => match result {
                Ok(Ok(bytes)) => bytes,
                Ok(Err(_)) => return Err(PaperControlError::Closed),
                Err(_) => return Err(PaperControlError::DeadlineExceeded),
            }
        };
        send_control_event(
            &self.events,
            &self.event_sequence,
            WorkerEvent::Command(command),
            slot,
            Some(retained_bytes),
            deadline,
            cancellation,
        )
        .await
    }
}

impl ExecutionAdapter for PaperExecutionAdapter {
    fn is_cooperative(&self) -> bool {
        true
    }

    fn submit(
        &self,
        order: DispatchOrder,
    ) -> ExecutionAdapterFuture<'_, Result<ExecutionReceipt, ExecutionAdapterError>> {
        Box::pin(async move {
            let (reply, response) = oneshot::channel();
            match self.try_send_command(WorkerCommand::Submit { order, reply }) {
                Ok(()) => response
                    .await
                    .unwrap_or(Err(ExecutionAdapterError::ReconciliationRequired)),
                Err(error) => Err(error),
            }
        })
    }

    fn cancel(
        &self,
        order: CancelOrder,
    ) -> ExecutionAdapterFuture<'_, Result<CancelReceipt, ExecutionAdapterError>> {
        Box::pin(async move {
            let requested_at = system_timestamp()?;
            let (reply, response) = oneshot::channel();
            self.try_send_command(WorkerCommand::Cancel {
                order,
                requested_at,
                reply,
            })?;
            response
                .await
                .unwrap_or(Err(ExecutionAdapterError::ReconciliationRequired))
        })
    }

    fn reconcile(
        &self,
        request: ReconcileOrders,
    ) -> ExecutionAdapterFuture<'_, Result<ExecutionState, ExecutionAdapterError>> {
        Box::pin(async move {
            if request.order_ids().len() > MAX_RECONCILED_ORDERS {
                return Err(ExecutionAdapterError::KnownFailure);
            }
            let requested_at = system_timestamp()?;
            let (reply, response) = oneshot::channel();
            self.try_send_command(WorkerCommand::Reconcile {
                requested_at,
                request,
                reply,
            })?;
            response
                .await
                .unwrap_or(Err(ExecutionAdapterError::ReconciliationRequired))
        })
    }

    fn acknowledge_reconciliation(
        &self,
        acknowledgement: ReconciliationAcknowledgement,
    ) -> ExecutionAdapterFuture<'_, Result<(), ExecutionAdapterError>> {
        Box::pin(async move {
            let (reply, response) = oneshot::channel();
            self.try_send_command(WorkerCommand::AcknowledgeReconciliation {
                acknowledgement,
                reply,
            })?;
            response
                .await
                .unwrap_or(Err(ExecutionAdapterError::ReconciliationRequired))
        })
    }
}

/// Bounded nonblocking market-update ingress for the paper worker.
#[derive(Debug)]
pub struct PaperMarketIngress {
    events: mpsc::Sender<WorkerEnvelope>,
    market_slots: Arc<Semaphore>,
    event_sequence: Arc<Mutex<u64>>,
    retained_bytes: usize,
}

impl ExecutionMarketSink for PaperMarketIngress {
    fn try_publish(&self, update: ExecutionMarketUpdate) -> Result<(), ExecutionMarketSinkError> {
        let slot = Arc::clone(&self.market_slots)
            .try_acquire_owned()
            .map_err(|_| {
                if self.events.is_closed() {
                    ExecutionMarketSinkError::Closed
                } else {
                    ExecutionMarketSinkError::Saturated
                }
            })?;
        try_send_event(
            &self.events,
            &self.event_sequence,
            WorkerEvent::Market(update),
            slot,
            None,
        )
        .map_err(|error| match error {
            EnqueueError::Busy | EnqueueError::Full => ExecutionMarketSinkError::Saturated,
            EnqueueError::Closed => ExecutionMarketSinkError::Closed,
            EnqueueError::Poisoned | EnqueueError::SequenceExhausted => {
                ExecutionMarketSinkError::RetainedSize
            }
        })
    }

    fn retained_bytes(&self) -> Result<usize, ExecutionMarketSinkError> {
        Ok(self.retained_bytes)
    }
}

/// Owns and reaps one paper state-writer. Dropping aborts rather than detaching it.
#[derive(Debug)]
pub struct PaperExecutionRuntime {
    adapter: Arc<PaperExecutionAdapter>,
    market_ingress: Arc<PaperMarketIngress>,
    audit_reader: Option<PaperAuditReader>,
    cancellation: CancellationToken,
    worker: Option<ExecutionTask<()>>,
    abort_join_deadline: Duration,
}

impl PaperExecutionRuntime {
    /// Starts a fresh worker from trusted local account bootstrap.
    pub fn try_start(
        config: PaperExecutionConfig,
        accounts: impl IntoIterator<Item = PaperAccountBootstrap>,
        checkpoint_repository: &PaperCheckpointRepository,
        task_reaper: ExecutionTaskReaper,
    ) -> Result<Self, PaperStartError> {
        if !checkpoint_repository.binds_config(&config) {
            return Err(PaperStartError::CheckpointRepositoryMismatch);
        }
        let ledger = PaperLedger::try_new(config.ledger_config(), accounts)?;
        Self::start_with_state(
            config,
            ledger,
            None,
            checkpoint_repository.binding_identity(),
            task_reaper,
        )
    }

    /// Restores only a complete, same-schema, same-configuration opaque checkpoint.
    pub fn try_start_from_checkpoint(
        config: PaperExecutionConfig,
        checkpoint: PaperExecutionCheckpoint,
        checkpoint_repository: &PaperCheckpointRepository,
        task_reaper: ExecutionTaskReaper,
    ) -> Result<Self, PaperStartError> {
        if !checkpoint_repository.binds_config(&config) {
            return Err(PaperStartError::CheckpointRepositoryMismatch);
        }
        if checkpoint.schema_version != PaperExecutionConfig::CHECKPOINT_SCHEMA_VERSION
            || checkpoint.configuration_digest != config.digest()
            || !checkpoint.complete
            || checkpoint.orders.len() > config.input().maximum_orders.get()
            || checkpoint.fills.len() > config.input().maximum_fills.get()
            || checkpoint.archived_orders.len() > config.input().maximum_archived_orders.get()
            || checkpoint.archived_fills.len()
                > config
                    .input()
                    .maximum_archived_orders
                    .get()
                    .checked_mul(config.input().maximum_fills.get())
                    .ok_or(PaperStartError::CapacityOverflow)?
            || checkpoint.acknowledged_reconciliation_batches.len()
                > config
                    .input()
                    .maximum_orders
                    .get()
                    .checked_add(config.input().maximum_archived_orders.get())
                    .ok_or(PaperStartError::CapacityOverflow)?
            || checkpoint.idempotency.len() > config.input().maximum_idempotency_keys.get()
        {
            return Err(PaperStartError::InvalidCheckpoint);
        }
        let ledger = checkpoint.ledger.clone();
        Self::start_with_state(
            config,
            ledger,
            Some(checkpoint),
            checkpoint_repository.binding_identity(),
            task_reaper,
        )
    }

    fn start_with_state(
        config: PaperExecutionConfig,
        ledger: PaperLedger,
        mut checkpoint: Option<PaperExecutionCheckpoint>,
        repository_id: [u8; 32],
        task_reaper: ExecutionTaskReaper,
    ) -> Result<Self, PaperStartError> {
        let input = config.input().clone();
        let abort_join_deadline = input.abort_join_deadline;
        let command_capacity = input.command_capacity.get();
        let command_bytes = usize::try_from(input.command_maximum_bytes.get())
            .map_err(|_| PaperStartError::CapacityOverflow)?;
        if command_bytes > Semaphore::MAX_PERMITS {
            return Err(PaperStartError::CapacityOverflow);
        }
        let market_capacity = config
            .market_event_capacity()
            .map_err(|error| match error {
                crate::PaperConfigError::InvalidValue => PaperStartError::InvalidCapacity,
                crate::PaperConfigError::CapacityOverflow => PaperStartError::CapacityOverflow,
            })?;
        let market_bytes = config
            .market_ingress_retained_bytes()
            .map_err(|error| match error {
                crate::PaperConfigError::InvalidValue => PaperStartError::InvalidCapacity,
                crate::PaperConfigError::CapacityOverflow => PaperStartError::CapacityOverflow,
            })?;
        let audit_capacity = byte_limited_capacity(
            input.audit_capacity.get(),
            input.audit_maximum_bytes.get() as usize,
            PaperAuditRecord::retained_bytes(),
        )?;
        let event_capacity = command_capacity
            .checked_add(market_capacity)
            .ok_or(PaperStartError::CapacityOverflow)?;
        if event_capacity > Semaphore::MAX_PERMITS {
            return Err(PaperStartError::CapacityOverflow);
        }
        let (event_tx, event_rx) = mpsc::channel(event_capacity);
        let (audit_tx, audit_rx) = mpsc::channel(audit_capacity);
        let recovery_audits = prepare_recovery_audits(&config, &mut checkpoint)?;
        for record in recovery_audits.into_iter().flatten() {
            audit_tx
                .try_send(record)
                .map_err(|_| PaperStartError::RecoveryAuditUnavailable)?;
        }
        let event_sequence = Arc::new(Mutex::new(
            checkpoint.as_ref().map_or(0, |state| state.sequence),
        ));
        let command_slots = Arc::new(Semaphore::new(command_capacity));
        let command_byte_slots = Arc::new(Semaphore::new(command_bytes));
        let market_slots = Arc::new(Semaphore::new(market_capacity));
        let audit_failed = Arc::new(AtomicBool::new(false));
        let cancellation = CancellationToken::new();
        let worker = PaperWorker::new(
            config,
            repository_id,
            ledger,
            checkpoint,
            event_rx,
            audit_tx,
            Arc::clone(&audit_failed),
            cancellation.clone(),
        );
        let join = task_reaper
            .try_reserve()
            .and_then(|permit| permit.spawn(worker.run()))
            .map_err(PaperStartError::TaskOwnership)?;
        Ok(Self {
            adapter: Arc::new(PaperExecutionAdapter {
                events: event_tx.clone(),
                command_slots,
                command_bytes: command_byte_slots,
                maximum_command_bytes: command_bytes,
                event_sequence: Arc::clone(&event_sequence),
                retained_bytes: command_bytes,
            }),
            market_ingress: Arc::new(PaperMarketIngress {
                events: event_tx,
                market_slots,
                event_sequence,
                retained_bytes: market_bytes,
            }),
            audit_reader: Some(PaperAuditReader::new(audit_rx, audit_failed)),
            cancellation,
            worker: Some(join),
            abort_join_deadline,
        })
    }

    pub fn adapter(&self) -> Arc<PaperExecutionAdapter> {
        Arc::clone(&self.adapter)
    }

    pub fn market_ingress(&self) -> Arc<PaperMarketIngress> {
        Arc::clone(&self.market_ingress)
    }

    /// Transfers the sole audit consumer to application persistence composition.
    pub fn take_audit_reader(&mut self) -> Option<PaperAuditReader> {
        self.audit_reader.take()
    }

    /// Requests deterministic shutdown, returns the final state, and reaps the writer.
    pub async fn shutdown(
        mut self,
        control: PaperControlContext,
    ) -> Result<PaperExecutionSnapshot, PaperControlError> {
        let (reply, response) = oneshot::channel();
        let deadline = control.deadline();
        let cancellation = control.cancellation();
        let sent = self
            .adapter
            .send_control(
                WorkerCommand::Shutdown { control, reply },
                deadline,
                &cancellation,
            )
            .await;
        let snapshot = match sent {
            Ok(()) => control_response(response, deadline, cancellation).await,
            Err(error) => Err(error),
        };
        let Some(mut worker) = self.worker.take() else {
            return Err(PaperControlError::WorkerFailed);
        };
        match snapshot {
            Ok(snapshot) => {
                match tokio::time::timeout(self.abort_join_deadline, worker.join()).await {
                    Ok(Ok(())) => Ok(snapshot),
                    Ok(Err(_)) => Err(PaperControlError::WorkerFailed),
                    Err(_) => {
                        worker.transfer();
                        Err(PaperControlError::ShutdownIncomplete)
                    }
                }
            }
            Err(error) => {
                self.cancellation.cancel();
                worker.abort();
                match tokio::time::timeout(self.abort_join_deadline, worker.join()).await {
                    Ok(_) => Err(error),
                    Err(_) => {
                        worker.transfer();
                        Err(PaperControlError::ShutdownIncomplete)
                    }
                }
            }
        }
    }
}

fn prepare_recovery_audits(
    config: &PaperExecutionConfig,
    checkpoint: &mut Option<PaperExecutionCheckpoint>,
) -> Result<[Option<PaperAuditRecord>; 2], PaperStartError> {
    let Some(checkpoint) = checkpoint.as_mut() else {
        return Ok([None, None]);
    };
    let input_digest = checkpoint
        .recovery_input_digest()
        .map_err(|_| PaperStartError::InvalidCheckpoint)?;
    let recovered_at = system_timestamp().map_err(|_| PaperStartError::Clock)?;
    let recovery_sequence = checkpoint
        .sequence
        .checked_add(1)
        .ok_or(PaperStartError::InvalidCheckpoint)?;
    let recovery = PaperAuditRecord::new(
        recovery_sequence,
        None,
        PaperAuditKind::RecoveryLoaded,
        None,
        None,
        recovered_at,
        None,
        config.digest(),
        input_digest,
    );
    let reconciliation = if checkpoint.reconciliation_required {
        let sequence = recovery_sequence
            .checked_add(1)
            .ok_or(PaperStartError::InvalidCheckpoint)?;
        checkpoint.sequence = sequence;
        Some(PaperAuditRecord::new(
            sequence,
            None,
            PaperAuditKind::ReconciliationRequired,
            None,
            None,
            recovered_at,
            None,
            config.digest(),
            input_digest,
        ))
    } else {
        checkpoint.sequence = recovery_sequence;
        None
    };
    Ok([Some(recovery), reconciliation])
}

impl Drop for PaperExecutionRuntime {
    fn drop(&mut self) {
        self.cancellation.cancel();
        if let Some(worker) = self.worker.take() {
            worker.transfer();
        }
    }
}

fn byte_limited_capacity(
    count: usize,
    maximum_bytes: usize,
    item_ceiling: usize,
) -> Result<usize, PaperStartError> {
    let by_bytes = maximum_bytes / item_ceiling;
    let capacity = count.min(by_bytes);
    if capacity == 0 {
        Err(PaperStartError::InvalidCapacity)
    } else {
        Ok(capacity)
    }
}

fn try_send_event(
    events: &mpsc::Sender<WorkerEnvelope>,
    sequence: &Mutex<u64>,
    event: WorkerEvent,
    slot: OwnedSemaphorePermit,
    retained_bytes: Option<OwnedSemaphorePermit>,
) -> Result<(), EnqueueError> {
    let mut sequence = match sequence.try_lock() {
        Ok(sequence) => sequence,
        Err(TryLockError::WouldBlock) => return Err(EnqueueError::Busy),
        Err(TryLockError::Poisoned(_)) => return Err(EnqueueError::Poisoned),
    };
    let next = sequence
        .checked_add(1)
        .ok_or(EnqueueError::SequenceExhausted)?;
    events
        .try_send(WorkerEnvelope {
            sequence: next,
            event,
            _lane_slot: slot,
            _retained_bytes: retained_bytes,
        })
        .map_err(|error| match error {
            mpsc::error::TrySendError::Full(_) => EnqueueError::Full,
            mpsc::error::TrySendError::Closed(_) => EnqueueError::Closed,
        })?;
    *sequence = next;
    Ok(())
}

async fn send_control_event(
    events: &mpsc::Sender<WorkerEnvelope>,
    sequence: &Mutex<u64>,
    event: WorkerEvent,
    slot: OwnedSemaphorePermit,
    retained_bytes: Option<OwnedSemaphorePermit>,
    deadline: tokio::time::Instant,
    cancellation: &CancellationToken,
) -> Result<(), PaperControlError> {
    let mut event = Some(event);
    let mut slot = Some(slot);
    let mut retained_bytes = retained_bytes;
    loop {
        match try_send_control_once(events, sequence, &mut event, &mut slot, &mut retained_bytes) {
            ControlEnqueueOutcome::Sent => return Ok(()),
            ControlEnqueueOutcome::Retry => {
                tokio::select! {
                    biased;
                    () = cancellation.cancelled() => return Err(PaperControlError::Cancelled),
                    () = tokio::time::sleep_until(deadline) => {
                        return Err(PaperControlError::DeadlineExceeded);
                    }
                    () = tokio::task::yield_now() => {}
                }
            }
            ControlEnqueueOutcome::Closed => return Err(PaperControlError::Closed),
        }
    }
}

async fn control_response<T>(
    response: oneshot::Receiver<Result<T, PaperControlError>>,
    deadline: tokio::time::Instant,
    cancellation: CancellationToken,
) -> Result<T, PaperControlError> {
    tokio::select! {
        biased;
        () = cancellation.cancelled() => Err(PaperControlError::Cancelled),
        result = tokio::time::timeout_at(deadline, response) => match result {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err(PaperControlError::Closed),
            Err(_) => Err(PaperControlError::DeadlineExceeded),
        }
    }
}

async fn await_admitted_persistence_outcome(
    response: oneshot::Receiver<Result<(), PaperControlError>>,
) -> Result<(), PaperControlError> {
    response.await.map_err(|_| PaperControlError::Closed)?
}

fn try_send_control_once(
    events: &mpsc::Sender<WorkerEnvelope>,
    sequence: &Mutex<u64>,
    event: &mut Option<WorkerEvent>,
    slot: &mut Option<OwnedSemaphorePermit>,
    retained_bytes: &mut Option<OwnedSemaphorePermit>,
) -> ControlEnqueueOutcome {
    let mut sequence = match sequence.try_lock() {
        Ok(sequence) => sequence,
        Err(TryLockError::WouldBlock) => return ControlEnqueueOutcome::Retry,
        Err(TryLockError::Poisoned(_)) => return ControlEnqueueOutcome::Closed,
    };
    let Some(next) = sequence.checked_add(1) else {
        return ControlEnqueueOutcome::Closed;
    };
    match events.try_send(WorkerEnvelope {
        sequence: next,
        event: match event.take() {
            Some(event) => event,
            None => return ControlEnqueueOutcome::Closed,
        },
        _lane_slot: match slot.take() {
            Some(slot) => slot,
            None => return ControlEnqueueOutcome::Closed,
        },
        _retained_bytes: retained_bytes.take(),
    }) {
        Ok(()) => {
            *sequence = next;
            ControlEnqueueOutcome::Sent
        }
        Err(mpsc::error::TrySendError::Full(envelope)) => {
            *event = Some(envelope.event);
            *slot = Some(envelope._lane_slot);
            *retained_bytes = envelope._retained_bytes;
            ControlEnqueueOutcome::Retry
        }
        Err(mpsc::error::TrySendError::Closed(_)) => ControlEnqueueOutcome::Closed,
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ControlEnqueueOutcome {
    Sent,
    Retry,
    Closed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum EnqueueError {
    Busy,
    Full,
    Closed,
    Poisoned,
    SequenceExhausted,
}

pub(crate) fn system_timestamp() -> Result<Timestamp, ExecutionAdapterError> {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| ExecutionAdapterError::KnownFailure)?;
    let nanos = i128::from(elapsed.as_secs())
        .checked_mul(1_000_000_000)
        .and_then(|value| value.checked_add(i128::from(elapsed.subsec_nanos())))
        .and_then(|value| i64::try_from(value).ok())
        .ok_or(ExecutionAdapterError::KnownFailure)?;
    Ok(Timestamp::from_unix_nanos(nanos))
}

/// Paper worker startup failure.
#[derive(Debug, Error)]
pub enum PaperStartError {
    #[error("paper execution requires an active Tokio runtime")]
    NoRuntime,
    #[error("paper queue count/byte capacity admits no item")]
    InvalidCapacity,
    #[error("paper queue retained-size arithmetic overflowed")]
    CapacityOverflow,
    #[error("paper recovery checkpoint is incomplete or incompatible")]
    InvalidCheckpoint,
    #[error("paper checkpoint repository configuration does not match the execution runtime")]
    CheckpointRepositoryMismatch,
    #[error("paper recovery audit capacity cannot admit mandatory startup evidence")]
    RecoveryAuditUnavailable,
    #[error("paper recovery could not obtain a valid wall-clock timestamp")]
    Clock,
    #[error(transparent)]
    TaskOwnership(#[from] ExecutionTaskReaperError),
    #[error(transparent)]
    Ledger(#[from] crate::PaperLedgerError),
}

/// Out-of-band lifecycle control failure.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum PaperControlError {
    #[error("paper control deadline is invalid")]
    InvalidDeadline,
    #[error("paper control operation was cancelled")]
    Cancelled,
    #[error("paper control operation exceeded its deadline")]
    DeadlineExceeded,
    #[error("paper execution worker is closed")]
    Closed,
    #[error("paper execution worker failed while being reaped")]
    WorkerFailed,
    #[error("paper execution worker could not be reaped within the abort deadline")]
    ShutdownIncomplete,
    #[error(transparent)]
    Adapter(ExecutionAdapterError),
}

#[cfg(test)]
mod tests {
    use std::future::{Future, poll_fn};
    use std::task::Poll;

    use super::*;

    #[tokio::test]
    async fn admitted_persistence_outcome_waits_for_definitive_worker_reply() {
        let (reply, response) = oneshot::channel::<Result<(), PaperControlError>>();
        let mut outcome = Box::pin(await_admitted_persistence_outcome(response));
        let first_poll = poll_fn(|context| Poll::Ready(outcome.as_mut().poll(context))).await;
        assert!(matches!(first_poll, Poll::Pending));

        tokio::time::sleep(Duration::from_millis(1)).await;
        let definitive = Err(PaperControlError::Adapter(
            ExecutionAdapterError::KnownFailure,
        ));
        assert!(reply.send(definitive).is_ok());
        assert_eq!(outcome.await, definitive);
    }
}
