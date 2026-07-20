//! Bounded public adapter, market ingress, audit, and owned worker lifecycle.

use std::mem::size_of;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex, TryLockError};
use std::time::{SystemTime, UNIX_EPOCH};

use market_squawk_domain::{OrderId, Timestamp};
use market_squawk_execution::{
    CancelReceipt, DispatchOrder, ExecutionAdapter, ExecutionAdapterError, ExecutionAdapterFuture,
    ExecutionMarketSink, ExecutionMarketSinkError, ExecutionMarketUpdate, ExecutionReceipt,
    ExecutionState, MAX_RECONCILED_ORDERS,
};
use thiserror::Error;
use tokio::runtime::Handle;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::audit::{PaperAuditKind, PaperAuditReader, PaperAuditRecord};
use crate::snapshot::{PaperExecutionCheckpoint, PaperExecutionSnapshot};
use crate::worker::{PaperWorker, WorkerCommand, WorkerEnvelope, WorkerEvent, WorkerMarketUpdate};
use crate::{PaperAccountBootstrap, PaperExecutionConfig, PaperLedger};

const SUBMIT_COMMAND_RETAINED_CEILING: usize = 64 * 1024;

/// Nonblocking dispatcher-facing paper adapter.
#[derive(Debug)]
pub struct PaperExecutionAdapter {
    events: mpsc::Sender<WorkerEnvelope>,
    command_slots: Arc<Semaphore>,
    event_sequence: Arc<Mutex<u64>>,
    retained_bytes: usize,
}

impl PaperExecutionAdapter {
    /// Returns the startup-fixed retained command-queue byte ceiling.
    pub const fn retained_bytes(&self) -> usize {
        self.retained_bytes
    }

    /// Requests a complete bounded state snapshot outside the live path.
    pub async fn snapshot(&self) -> Result<PaperExecutionSnapshot, PaperControlError> {
        let (reply, response) = oneshot::channel();
        self.send_control(WorkerCommand::Snapshot { reply }).await?;
        response.await.map_err(|_| PaperControlError::Closed)
    }

    /// Exports a strict complete recovery checkpoint without performing filesystem I/O.
    pub async fn checkpoint(&self) -> Result<PaperExecutionCheckpoint, PaperControlError> {
        let (reply, response) = oneshot::channel();
        self.send_control(WorkerCommand::Checkpoint { reply })
            .await?;
        response.await.map_err(|_| PaperControlError::Closed)
    }

    fn try_send_command(&self, command: WorkerCommand) -> Result<(), ExecutionAdapterError> {
        let slot = Arc::clone(&self.command_slots)
            .try_acquire_owned()
            .map_err(|_| {
                if self.events.is_closed() {
                    ExecutionAdapterError::KnownFailure
                } else {
                    ExecutionAdapterError::NotAttemptedBusy
                }
            })?;
        try_send_event(
            &self.events,
            &self.event_sequence,
            WorkerEvent::Command(command),
            slot,
        )
        .map_err(|error| match error {
            EnqueueError::Busy | EnqueueError::Full => ExecutionAdapterError::NotAttemptedBusy,
            EnqueueError::Closed | EnqueueError::Poisoned | EnqueueError::SequenceExhausted => {
                ExecutionAdapterError::KnownFailure
            }
        })
    }

    async fn send_control(&self, command: WorkerCommand) -> Result<(), PaperControlError> {
        let slot = Arc::clone(&self.command_slots)
            .acquire_owned()
            .await
            .map_err(|_| PaperControlError::Closed)?;
        send_control_event(
            &self.events,
            &self.event_sequence,
            WorkerEvent::Command(command),
            slot,
        )
        .await
    }
}

impl ExecutionAdapter for PaperExecutionAdapter {
    fn submit(
        &self,
        order: DispatchOrder,
    ) -> ExecutionAdapterFuture<'_, Result<ExecutionReceipt, ExecutionAdapterError>> {
        let (reply, response) = oneshot::channel();
        match self.try_send_command(WorkerCommand::Submit { order, reply }) {
            Ok(()) => Box::pin(async move {
                response
                    .await
                    .unwrap_or(Err(ExecutionAdapterError::ReconciliationRequired))
            }),
            Err(error) => Box::pin(async move { Err(error) }),
        }
    }

    fn cancel(
        &self,
        order_id: &OrderId,
    ) -> ExecutionAdapterFuture<'_, Result<CancelReceipt, ExecutionAdapterError>> {
        let requested_at = match system_timestamp() {
            Ok(timestamp) => timestamp,
            Err(error) => return Box::pin(async move { Err(error) }),
        };
        let (reply, response) = oneshot::channel();
        match self.try_send_command(WorkerCommand::Cancel {
            order_id: *order_id,
            requested_at,
            reply,
        }) {
            Ok(()) => Box::pin(async move {
                response
                    .await
                    .unwrap_or(Err(ExecutionAdapterError::ReconciliationRequired))
            }),
            Err(error) => Box::pin(async move { Err(error) }),
        }
    }

    fn reconcile<'adapter>(
        &'adapter self,
        order_ids: &'adapter [OrderId],
    ) -> ExecutionAdapterFuture<'adapter, Result<ExecutionState, ExecutionAdapterError>> {
        if order_ids.len() > MAX_RECONCILED_ORDERS {
            return Box::pin(async { Err(ExecutionAdapterError::KnownFailure) });
        }
        let requested_at = match system_timestamp() {
            Ok(timestamp) => timestamp,
            Err(error) => return Box::pin(async move { Err(error) }),
        };
        let mut requested = Vec::new();
        if requested.try_reserve_exact(order_ids.len()).is_err() {
            return Box::pin(async { Err(ExecutionAdapterError::KnownFailure) });
        }
        requested.extend_from_slice(order_ids);
        let (reply, response) = oneshot::channel();
        match self.try_send_command(WorkerCommand::Reconcile {
            requested_at,
            order_ids: requested,
            reply,
        }) {
            Ok(()) => Box::pin(async move {
                response
                    .await
                    .unwrap_or(Err(ExecutionAdapterError::ReconciliationRequired))
            }),
            Err(error) => Box::pin(async move { Err(error) }),
        }
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
    worker: Option<JoinHandle<()>>,
}

impl PaperExecutionRuntime {
    /// Starts a fresh worker from trusted local account bootstrap.
    pub fn try_start(
        config: PaperExecutionConfig,
        accounts: impl IntoIterator<Item = PaperAccountBootstrap>,
    ) -> Result<Self, PaperStartError> {
        let ledger = PaperLedger::try_new(config.ledger_config(), accounts)?;
        Self::start_with_state(config, ledger, None)
    }

    /// Restores only a complete, same-schema, same-configuration opaque checkpoint.
    pub fn try_start_from_checkpoint(
        config: PaperExecutionConfig,
        checkpoint: PaperExecutionCheckpoint,
    ) -> Result<Self, PaperStartError> {
        if checkpoint.schema_version != PaperExecutionConfig::CHECKPOINT_SCHEMA_VERSION
            || checkpoint.configuration_digest != config.digest()
            || !checkpoint.complete
            || checkpoint.orders.len() > config.input().maximum_orders.get()
            || checkpoint.fills.len() > config.input().maximum_fills.get()
            || checkpoint.idempotency.len() > config.input().maximum_idempotency_keys.get()
        {
            return Err(PaperStartError::InvalidCheckpoint);
        }
        let ledger = checkpoint.ledger.clone();
        Self::start_with_state(config, ledger, Some(checkpoint))
    }

    fn start_with_state(
        config: PaperExecutionConfig,
        ledger: PaperLedger,
        mut checkpoint: Option<PaperExecutionCheckpoint>,
    ) -> Result<Self, PaperStartError> {
        let handle = Handle::try_current().map_err(|_| PaperStartError::NoRuntime)?;
        let input = config.input().clone();
        let command_capacity = byte_limited_capacity(
            input.command_capacity.get(),
            input.command_maximum_bytes.get() as usize,
            SUBMIT_COMMAND_RETAINED_CEILING,
        )?;
        let market_capacity = byte_limited_capacity(
            input.market_capacity.get(),
            input.market_maximum_bytes.get() as usize,
            size_of::<WorkerMarketUpdate>(),
        )?;
        let audit_capacity = byte_limited_capacity(
            input.audit_capacity.get(),
            input.audit_maximum_bytes.get() as usize,
            PaperAuditRecord::retained_bytes(),
        )?;
        let event_capacity = command_capacity
            .checked_add(market_capacity)
            .ok_or(PaperStartError::CapacityOverflow)?;
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
        let market_slots = Arc::new(Semaphore::new(market_capacity));
        let audit_failed = Arc::new(AtomicBool::new(false));
        let cancellation = CancellationToken::new();
        let worker = PaperWorker::new(
            config,
            ledger,
            checkpoint,
            event_rx,
            audit_tx,
            Arc::clone(&audit_failed),
            cancellation.clone(),
        );
        let join = handle.spawn(worker.run());
        let command_bytes = command_capacity
            .checked_mul(SUBMIT_COMMAND_RETAINED_CEILING)
            .ok_or(PaperStartError::CapacityOverflow)?;
        let market_bytes = market_capacity
            .checked_mul(size_of::<WorkerMarketUpdate>())
            .ok_or(PaperStartError::CapacityOverflow)?;
        Ok(Self {
            adapter: Arc::new(PaperExecutionAdapter {
                events: event_tx.clone(),
                command_slots,
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
    pub async fn shutdown(mut self) -> Result<PaperExecutionSnapshot, PaperControlError> {
        let (reply, response) = oneshot::channel();
        self.adapter
            .send_control(WorkerCommand::Shutdown { reply })
            .await?;
        let snapshot = response.await.map_err(|_| PaperControlError::Closed)?;
        if let Some(worker) = self.worker.take() {
            worker.await.map_err(|_| PaperControlError::WorkerFailed)?;
        }
        Ok(snapshot)
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
            worker.abort();
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
) -> Result<(), PaperControlError> {
    let mut event = Some(event);
    let mut slot = Some(slot);
    loop {
        match try_send_control_once(events, sequence, &mut event, &mut slot) {
            ControlEnqueueOutcome::Sent => return Ok(()),
            ControlEnqueueOutcome::Retry => tokio::task::yield_now().await,
            ControlEnqueueOutcome::Closed => return Err(PaperControlError::Closed),
        }
    }
}

fn try_send_control_once(
    events: &mpsc::Sender<WorkerEnvelope>,
    sequence: &Mutex<u64>,
    event: &mut Option<WorkerEvent>,
    slot: &mut Option<OwnedSemaphorePermit>,
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
    }) {
        Ok(()) => {
            *sequence = next;
            ControlEnqueueOutcome::Sent
        }
        Err(mpsc::error::TrySendError::Full(envelope)) => {
            *event = Some(envelope.event);
            *slot = Some(envelope._lane_slot);
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
    #[error("paper recovery audit capacity cannot admit mandatory startup evidence")]
    RecoveryAuditUnavailable,
    #[error("paper recovery could not obtain a valid wall-clock timestamp")]
    Clock,
    #[error(transparent)]
    Ledger(#[from] crate::PaperLedgerError),
}

/// Out-of-band lifecycle control failure.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum PaperControlError {
    #[error("paper execution worker is closed")]
    Closed,
    #[error("paper execution worker failed while being reaped")]
    WorkerFailed,
}
