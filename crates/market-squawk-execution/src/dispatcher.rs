//! Bounded one-use approval dispatch and accepted-order ownership.

mod attempt;
mod lifecycle;
mod reconciliation;
mod worker;

use std::collections::HashMap;
use std::num::{NonZeroU32, NonZeroU64, NonZeroUsize};
use std::sync::{Arc, Mutex, TryLockError};
use std::time::Duration;

use market_squawk_domain::{AccountId, ApprovalId, Currency, OrderId, QuantityLots, Timestamp};
use thiserror::Error;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc};
use tokio_util::sync::CancellationToken;

use crate::account::{AccountOutcomeFailSafe, CompleteAccountReplacement};
use crate::approval::APPROVAL_COMMAND_RETAINED_BYTE_CEILING;
use crate::audit::{ExecutionAuditContext, ExecutionAuditPermit};
use crate::clock::system_now;
use crate::{
    AccountRiskCoordinator, AccountRiskReservation, ApprovedOrder, ExecutionAdapter,
    ExecutionAdapterError, ExecutionAuditEvent, ExecutionAuditKind, ExecutionAuditReason,
    ExecutionAuditWriter, ExecutionOperation, ExecutionPriceBound, ExecutionState, ExecutionTask,
    ExecutionTaskReaper, ExecutionTaskReaperError, OrderIntentDigest, PersistenceAcknowledgement,
    ReconciledOrder, ReconciliationBatchBinding, RecoveredDispatchOrder,
};
use worker::run_worker;

/// Startup-fixed dispatcher count, bytes, identity, and shutdown bounds.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExecutionDispatcherConfig {
    pub maximum_queued_commands: NonZeroUsize,
    pub maximum_queued_bytes: NonZeroU32,
    pub maximum_registry_entries: NonZeroUsize,
    pub maximum_pending_reconciliation_bytes: NonZeroU32,
    pub operation_deadline: Duration,
    pub shutdown_deadline: Duration,
}

impl ExecutionDispatcherConfig {
    /// Returns the exact shared dispatcher-handle charge used by route-hook composition.
    pub fn handle_retained_bytes(self) -> Result<usize, ExecutionDispatcherError> {
        retained_dispatcher_bytes(self)
    }
}

/// Owner of the execution worker, accepted reservations, and adapter lifecycle operations.
#[derive(Debug)]
pub struct ExecutionDispatcher {
    handle: ExecutionDispatcherHandle,
    adapter: Arc<dyn ExecutionAdapter>,
    accounts: Arc<AccountRiskCoordinator>,
    registry: Arc<Mutex<DispatchRegistry>>,
    audit: ExecutionAuditWriter,
    admission_cancellation: CancellationToken,
    control_cancellation: CancellationToken,
    worker: Option<ExecutionTask<()>>,
    task_reaper: ExecutionTaskReaper,
    operation_deadline: Duration,
    shutdown_deadline: Duration,
    maximum_pending_reconciliation_bytes: usize,
}

impl ExecutionDispatcher {
    /// Starts a bounded worker in the current Tokio runtime.
    pub fn try_start(
        adapter: Arc<dyn ExecutionAdapter>,
        accounts: Arc<AccountRiskCoordinator>,
        audit: ExecutionAuditWriter,
        config: ExecutionDispatcherConfig,
        task_reaper: ExecutionTaskReaper,
    ) -> Result<Self, ExecutionDispatcherError> {
        Self::try_start_inner(adapter, accounts, audit, config, task_reaper, None)
    }

    /// Starts with exact durable ownership for backend orders recovered before live admission.
    pub fn try_start_with_recovery(
        adapter: Arc<dyn ExecutionAdapter>,
        accounts: Arc<AccountRiskCoordinator>,
        audit: ExecutionAuditWriter,
        config: ExecutionDispatcherConfig,
        task_reaper: ExecutionTaskReaper,
        recovery_sequence: NonZeroU64,
        recovered: Vec<RecoveredDispatchOrder>,
    ) -> Result<Self, ExecutionDispatcherError> {
        Self::try_start_inner(
            adapter,
            accounts,
            audit,
            config,
            task_reaper,
            Some((recovery_sequence, recovered)),
        )
    }

    fn try_start_inner(
        adapter: Arc<dyn ExecutionAdapter>,
        accounts: Arc<AccountRiskCoordinator>,
        audit: ExecutionAuditWriter,
        config: ExecutionDispatcherConfig,
        task_reaper: ExecutionTaskReaper,
        recovery: Option<(NonZeroU64, Vec<RecoveredDispatchOrder>)>,
    ) -> Result<Self, ExecutionDispatcherError> {
        if config.operation_deadline.is_zero() || config.shutdown_deadline.is_zero() {
            return Err(ExecutionDispatcherError::ZeroShutdownDeadline);
        }
        let command_bytes = usize::try_from(config.maximum_queued_bytes.get())
            .map_err(|_| ExecutionDispatcherError::ByteCapacityUnsupported)?;
        if command_bytes > Semaphore::MAX_PERMITS {
            return Err(ExecutionDispatcherError::ByteCapacityUnsupported);
        }
        if command_bytes < APPROVAL_COMMAND_RETAINED_BYTE_CEILING {
            return Err(ExecutionDispatcherError::CommandExceedsByteCapacity);
        }
        let mut entries = HashMap::new();
        entries
            .try_reserve(config.maximum_registry_entries.get())
            .map_err(|_| ExecutionDispatcherError::Allocation)?;
        let mut recovery_audits = Vec::new();
        if let Some((_, recovered)) = recovery.as_ref() {
            if recovered.len() > config.maximum_registry_entries.get() {
                return Err(ExecutionDispatcherError::InvalidRecovery);
            }
            recovery_audits
                .try_reserve_exact(recovered.len())
                .map_err(|_| ExecutionDispatcherError::Allocation)?;
        }
        let recovery_sequence = recovery.as_ref().map(|(sequence, _)| *sequence);
        if let Some((_, recovered)) = recovery {
            for order in recovered {
                let parts = order.into_parts();
                if entries.contains_key(&parts.approval_id)
                    || entries
                        .values()
                        .any(|record: &DispatchRecord| record.order_id == parts.order_id)
                {
                    return Err(ExecutionDispatcherError::InvalidRecovery);
                }
                recovery_audits.push((
                    audit
                        .try_reserve()
                        .map_err(|_| ExecutionDispatcherError::RecoveryAuditUnavailable)?,
                    parts.audit_context,
                    parts.recovered_at,
                ));
                entries.insert(
                    parts.approval_id,
                    DispatchRecord {
                        order_id: parts.order_id,
                        account_id: parts.account_id,
                        intent_digest: parts.intent_digest,
                        account_revision: parts.account_revision,
                        state: DispatchState::Accepted,
                        reservation: Some(DispatchReservation::Recovered),
                        audit_context: parts.audit_context,
                        requested_quantity: parts.requested_quantity,
                        execution_price_bound: parts.execution_price_bound,
                        settlement_currency: parts.settlement_currency,
                        last_transition_at: parts.recovered_at,
                        lifecycle: Some(parts.lifecycle),
                        recovered: true,
                    },
                );
            }
        }
        let mut finalized_reconciliations = Vec::new();
        finalized_reconciliations
            .try_reserve_exact(config.maximum_registry_entries.get())
            .map_err(|_| ExecutionDispatcherError::Allocation)?;
        let registry = Arc::new(Mutex::new(DispatchRegistry {
            entries,
            maximum_entries: config.maximum_registry_entries.get(),
            pending_reconciliation: None,
            finalized_reconciliations,
            maximum_finalized_reconciliations: config.maximum_registry_entries.get(),
        }));
        let (sender, receiver) = mpsc::channel(config.maximum_queued_commands.get());
        let bytes = Arc::new(Semaphore::new(command_bytes));
        let admission_cancellation = CancellationToken::new();
        let control_cancellation = CancellationToken::new();
        let retained_bytes = retained_dispatcher_bytes(config)?;
        if let Some(sequence) = recovery_sequence {
            accounts
                .reconciliation_fence()
                .require(sequence)
                .map_err(|_| ExecutionDispatcherError::InvalidRecovery)?;
        }
        let worker = task_reaper
            .try_reserve()
            .and_then(|permit| {
                permit.spawn(run_worker(
                    Arc::clone(&adapter),
                    Arc::clone(&registry),
                    receiver,
                    admission_cancellation.child_token(),
                    task_reaper.clone(),
                ))
            })
            .map_err(ExecutionDispatcherError::TaskOwnership)?;
        for (permit, context, recovered_at) in recovery_audits {
            commit_dispatch_audit(
                permit,
                ExecutionAuditKind::DispatchUncertain,
                context,
                trusted_now_or(recovered_at),
                &[ExecutionAuditReason::ReconciliationRequired],
            );
        }
        let handle = ExecutionDispatcherHandle {
            sender,
            bytes,
            registry: Arc::clone(&registry),
            audit: audit.clone(),
            retained_bytes,
            operation_deadline: config.operation_deadline,
            cancellation: admission_cancellation.clone(),
        };
        Ok(Self {
            handle,
            adapter,
            accounts,
            registry,
            audit,
            admission_cancellation,
            control_cancellation,
            worker: Some(worker),
            task_reaper,
            operation_deadline: config.operation_deadline,
            shutdown_deadline: config.shutdown_deadline,
            maximum_pending_reconciliation_bytes: usize::try_from(
                config.maximum_pending_reconciliation_bytes.get(),
            )
            .map_err(|_| ExecutionDispatcherError::ByteCapacityUnsupported)?,
        })
    }

    /// Returns the cloneable nonblocking handoff used by live action hooks.
    pub fn handle(&self) -> ExecutionDispatcherHandle {
        self.handle.clone()
    }

    /// Mints one non-cloneable bounded capability after the caller durably persists a checkpoint.
    pub fn persistence_acknowledgement(
        &self,
    ) -> Result<PersistenceAcknowledgement, ExecutionDispatchError> {
        let operation = operation(
            self.operation_deadline,
            self.control_cancellation.child_token(),
        )?;
        let registry = try_registry(&self.registry)?;
        if registry.pending_reconciliation.is_some() {
            return Err(ExecutionDispatchError::ReconciliationAcknowledgementPending);
        }
        let mut finalized_reconciliations = Vec::new();
        finalized_reconciliations
            .try_reserve_exact(registry.finalized_reconciliations.len())
            .map_err(|_| ExecutionDispatchError::Allocation)?;
        finalized_reconciliations.extend_from_slice(&registry.finalized_reconciliations);
        drop(registry);
        Ok(PersistenceAcknowledgement::new(
            operation,
            finalized_reconciliations.into_boxed_slice(),
            PersistenceFinalization {
                registry: Arc::clone(&self.registry),
            },
        ))
    }

    /// Returns the process-lifetime owner used to drain transferred execution tasks.
    pub fn task_reaper(&self) -> ExecutionTaskReaper {
        self.task_reaper.clone()
    }
}

/// Cloneable, bounded, nonblocking approval handoff used by actor-owned hooks.
#[derive(Clone, Debug)]
pub struct ExecutionDispatcherHandle {
    sender: mpsc::Sender<DispatchCommand>,
    bytes: Arc<Semaphore>,
    registry: Arc<Mutex<DispatchRegistry>>,
    audit: ExecutionAuditWriter,
    retained_bytes: usize,
    operation_deadline: Duration,
    cancellation: CancellationToken,
}

impl ExecutionDispatcherHandle {
    /// Moves one approval into the one-use registry and bounded worker queue.
    pub fn try_submit(&self, approval: ApprovedOrder) -> Result<(), ExecutionDispatchError> {
        let audit = self
            .audit
            .try_reserve()
            .map_err(|_| ExecutionDispatchError::AuditUnavailable)?;
        let context = approval.audit_context();
        let approval_id = approval.approval_id();
        let order_id = approval.order_id();
        let account_id = approval.account_id();
        let intent_digest = approval.intent_digest();
        let account_revision = approval.account_revision();
        let observed_at = trusted_now_or(context.market_observed_at());
        let operation = operation(self.operation_deadline, self.cancellation.child_token())?;
        let operation_deadline = operation.deadline();
        let operation_cancellation = operation.cancellation();
        let slot = match self.sender.clone().try_reserve_owned() {
            Ok(slot) => slot,
            Err(mpsc::error::TrySendError::Full(_)) => {
                commit_dispatch_audit(
                    audit,
                    ExecutionAuditKind::DispatchRejected,
                    context,
                    observed_at,
                    &[ExecutionAuditReason::QueueCountSaturated],
                );
                return Err(ExecutionDispatchError::QueueCountSaturated);
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                commit_dispatch_audit(
                    audit,
                    ExecutionAuditKind::DispatchRejected,
                    context,
                    observed_at,
                    &[ExecutionAuditReason::RegistryUnavailable],
                );
                return Err(ExecutionDispatchError::Closed);
            }
        };
        let bytes = match Arc::clone(&self.bytes).try_acquire_many_owned(
            u32::try_from(approval.retained_byte_ceiling())
                .map_err(|_| ExecutionDispatchError::CommandSizeUnsupported)?,
        ) {
            Ok(bytes) => bytes,
            Err(_) => {
                commit_dispatch_audit(
                    audit,
                    ExecutionAuditKind::DispatchRejected,
                    context,
                    observed_at,
                    &[ExecutionAuditReason::QueueBytesSaturated],
                );
                return Err(ExecutionDispatchError::QueueBytesSaturated);
            }
        };
        {
            let mut registry = match try_registry(&self.registry) {
                Ok(registry) => registry,
                Err(error) => {
                    commit_dispatch_audit(
                        audit,
                        ExecutionAuditKind::DispatchRejected,
                        context,
                        observed_at,
                        &[ExecutionAuditReason::RegistryUnavailable],
                    );
                    return Err(error);
                }
            };
            registry
                .entries
                .retain(|_, record| record.state != DispatchState::Terminal);
            if registry.entries.contains_key(&approval_id) {
                commit_dispatch_audit(
                    audit,
                    ExecutionAuditKind::DispatchRejected,
                    context,
                    observed_at,
                    &[ExecutionAuditReason::DuplicateApproval],
                );
                return Err(ExecutionDispatchError::DuplicateApproval);
            }
            if registry.entries.len() >= registry.maximum_entries {
                commit_dispatch_audit(
                    audit,
                    ExecutionAuditKind::DispatchRejected,
                    context,
                    observed_at,
                    &[ExecutionAuditReason::RegistryCapacity],
                );
                return Err(ExecutionDispatchError::RegistryCapacity);
            }
            registry.entries.insert(
                approval_id,
                DispatchRecord {
                    order_id,
                    account_id,
                    intent_digest,
                    account_revision,
                    state: DispatchState::Queued,
                    reservation: None,
                    audit_context: context,
                    requested_quantity: approval.quantity(),
                    execution_price_bound: approval.execution_price_bound(),
                    settlement_currency: approval.execution_terms().settlement_currency(),
                    last_transition_at: context.market_observed_at(),
                    lifecycle: None,
                    recovered: false,
                },
            );
        }
        drop(slot.send(DispatchCommand {
            approval,
            audit,
            context,
            operation_deadline,
            operation_cancellation,
            _bytes: bytes,
        }));
        Ok(())
    }

    /// Returns the conservative startup-fixed shared graph charge used by route-hook accounting.
    pub const fn retained_bytes(&self) -> usize {
        self.retained_bytes
    }
}

#[derive(Debug)]
struct DispatchCommand {
    approval: ApprovedOrder,
    audit: ExecutionAuditPermit,
    context: ExecutionAuditContext,
    operation_deadline: tokio::time::Instant,
    operation_cancellation: CancellationToken,
    _bytes: OwnedSemaphorePermit,
}

#[derive(Debug)]
struct DispatchRegistry {
    entries: HashMap<ApprovalId, DispatchRecord>,
    maximum_entries: usize,
    pending_reconciliation: Option<PendingReconciliation>,
    finalized_reconciliations: Vec<ReconciliationBatchBinding>,
    maximum_finalized_reconciliations: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PendingReconciliationStatus {
    Ready,
    InFlight,
    BackendAcknowledged,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PendingReconciliationScope {
    TrackedOrders,
    CompleteAccounts,
}

const fn reconciliation_scope(order_ids: &[OrderId]) -> PendingReconciliationScope {
    if order_ids.is_empty() {
        PendingReconciliationScope::CompleteAccounts
    } else {
        PendingReconciliationScope::TrackedOrders
    }
}

#[derive(Debug)]
struct PendingReconciliation {
    batch: ReconciliationBatchBinding,
    order_ids: Box<[OrderId]>,
    state: ExecutionState,
    scope: PendingReconciliationScope,
    complete_accounts: Option<CompleteAccountReplacement>,
    status: PendingReconciliationStatus,
    retained_bytes: usize,
}

impl PendingReconciliation {
    fn retained_bytes_for(
        order_ids: &[OrderId],
        state: &ExecutionState,
    ) -> Result<usize, ExecutionDispatchError> {
        std::mem::size_of::<Self>()
            .checked_add(std::mem::size_of_val(order_ids))
            .and_then(|retained| retained.checked_add(state.retained_heap_bytes()?))
            .ok_or(ExecutionDispatchError::PendingReconciliationCapacity)
    }

    fn try_new(
        batch: ReconciliationBatchBinding,
        order_ids: Box<[OrderId]>,
        state: ExecutionState,
        complete_accounts: Option<CompleteAccountReplacement>,
    ) -> Result<Self, ExecutionDispatchError> {
        let retained_bytes = Self::retained_bytes_for(&order_ids, &state)?;
        let scope = reconciliation_scope(&order_ids);
        if (scope == PendingReconciliationScope::CompleteAccounts) != complete_accounts.is_some() {
            return Err(ExecutionDispatchError::AccountReplacementRejected);
        }
        Ok(Self {
            batch,
            scope,
            complete_accounts,
            order_ids,
            state,
            status: PendingReconciliationStatus::Ready,
            retained_bytes,
        })
    }
}

#[derive(Debug)]
pub(crate) struct PersistenceFinalization {
    registry: Arc<Mutex<DispatchRegistry>>,
}

impl PersistenceFinalization {
    pub(crate) fn commit(
        self,
        persisted: &[ReconciliationBatchBinding],
        operation: &ExecutionOperation,
    ) -> Result<(), ExecutionAdapterError> {
        let mut registry = self
            .registry
            .try_lock()
            .map_err(|_| ExecutionAdapterError::NotAttemptedBusy)?;
        for binding in persisted {
            let mut finalized = false;
            for candidate in &registry.finalized_reconciliations {
                if operation.is_expired() {
                    return Err(ExecutionAdapterError::KnownFailure);
                }
                if candidate == binding {
                    finalized = true;
                    break;
                }
            }
            if !finalized {
                return Err(ExecutionAdapterError::KnownFailure);
            }
        }
        let mut retained = Vec::new();
        retained
            .try_reserve_exact(registry.finalized_reconciliations.len())
            .map_err(|_| ExecutionAdapterError::KnownFailure)?;
        for binding in &registry.finalized_reconciliations {
            let mut is_persisted = false;
            for candidate in persisted {
                if operation.is_expired() {
                    return Err(ExecutionAdapterError::KnownFailure);
                }
                if candidate == binding {
                    is_persisted = true;
                    break;
                }
            }
            if !is_persisted {
                retained.push(*binding);
            }
        }
        if operation.is_expired() {
            return Err(ExecutionAdapterError::KnownFailure);
        }
        registry.finalized_reconciliations = retained;
        Ok(())
    }
}

#[derive(Debug)]
struct DispatchRecord {
    order_id: OrderId,
    account_id: AccountId,
    intent_digest: OrderIntentDigest,
    account_revision: u64,
    state: DispatchState,
    reservation: Option<DispatchReservation>,
    audit_context: ExecutionAuditContext,
    requested_quantity: QuantityLots,
    execution_price_bound: ExecutionPriceBound,
    settlement_currency: Option<Currency>,
    last_transition_at: Timestamp,
    lifecycle: Option<ReconciledOrder>,
    recovered: bool,
}

#[derive(Debug)]
enum DispatchReservation {
    Live(AccountRiskReservation),
    Recovered,
}

impl DispatchReservation {
    fn outcome_fail_safe(&self) -> DispatchOutcomeFailSafe {
        match self {
            Self::Live(reservation) => {
                DispatchOutcomeFailSafe::Live(reservation.outcome_fail_safe())
            }
            Self::Recovered => DispatchOutcomeFailSafe::Recovered,
        }
    }

    fn mark_accepted(&self) -> Result<(), crate::AccountReservationStateError> {
        match self {
            Self::Live(reservation) => reservation.mark_accepted(),
            Self::Recovered => Err(crate::AccountReservationStateError::NotSubmitted),
        }
    }

    fn mark_known_not_accepted(&self) {
        if let Self::Live(reservation) = self {
            reservation.mark_known_not_accepted();
        }
    }

    fn mark_reconciliation_required(&self) {
        if let Self::Live(reservation) = self {
            reservation.mark_reconciliation_required();
        }
    }

    fn mark_terminal_unfilled(&self) {
        if let Self::Live(reservation) = self {
            reservation.mark_terminal_unfilled();
        }
    }
}

#[derive(Debug)]
enum DispatchOutcomeFailSafe {
    Live(AccountOutcomeFailSafe),
    Recovered,
}

impl DispatchOutcomeFailSafe {
    fn disarm(self) {
        if let Self::Live(fail_safe) = self {
            fail_safe.disarm();
        }
    }
}

impl DispatchRecord {
    const fn reconcilable(&self) -> bool {
        matches!(
            self.state,
            DispatchState::Accepted | DispatchState::Reconciliation
        )
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DispatchState {
    Queued,
    Submitted,
    Accepted,
    Canceling,
    Reconciling,
    Terminal,
    Reconciliation,
}

fn try_registry(
    registry: &Arc<Mutex<DispatchRegistry>>,
) -> Result<std::sync::MutexGuard<'_, DispatchRegistry>, ExecutionDispatchError> {
    match registry.try_lock() {
        Ok(registry) => Ok(registry),
        Err(TryLockError::WouldBlock) => Err(ExecutionDispatchError::RegistryBusy),
        Err(TryLockError::Poisoned(_)) => Err(ExecutionDispatchError::RegistryPoisoned),
    }
}

fn commit_dispatch_audit(
    permit: ExecutionAuditPermit,
    kind: ExecutionAuditKind,
    context: ExecutionAuditContext,
    observed_at: Timestamp,
    reasons: &[ExecutionAuditReason],
) {
    let event = ExecutionAuditEvent::from_context(kind, context, observed_at, reasons);
    permit.commit(event);
}

const fn adapter_reason(error: ExecutionAdapterError) -> ExecutionAuditReason {
    match error {
        ExecutionAdapterError::Rejected => ExecutionAuditReason::AdapterRejected,
        ExecutionAdapterError::KnownFailure | ExecutionAdapterError::NotAttemptedBusy => {
            ExecutionAuditReason::AdapterKnownFailure
        }
        ExecutionAdapterError::UncertainOutcome => ExecutionAuditReason::AdapterUncertain,
        ExecutionAdapterError::ReconciliationRequired => {
            ExecutionAuditReason::ReconciliationRequired
        }
    }
}

fn trusted_now_or(fallback: Timestamp) -> Timestamp {
    system_now().map_or(fallback, |reading| reading.wall)
}

fn retained_dispatcher_bytes(
    config: ExecutionDispatcherConfig,
) -> Result<usize, ExecutionDispatcherError> {
    let command_count_bytes = config
        .maximum_queued_commands
        .get()
        .checked_mul(APPROVAL_COMMAND_RETAINED_BYTE_CEILING)
        .ok_or(ExecutionDispatcherError::RetainedSizeOverflow)?;
    let command_bytes = command_count_bytes.min(
        usize::try_from(config.maximum_queued_bytes.get())
            .map_err(|_| ExecutionDispatcherError::RetainedSizeOverflow)?,
    );
    std::mem::size_of::<ExecutionDispatcherHandle>()
        .checked_add(command_bytes)
        .and_then(|value| {
            value.checked_add(
                config
                    .maximum_registry_entries
                    .get()
                    .checked_mul(std::mem::size_of::<DispatchRecord>())?,
            )
        })
        .and_then(|value| {
            value.checked_add(
                usize::try_from(config.maximum_pending_reconciliation_bytes.get()).ok()?,
            )
        })
        .and_then(|value| {
            value.checked_add(
                config
                    .maximum_registry_entries
                    .get()
                    .checked_mul(std::mem::size_of::<ReconciliationBatchBinding>())?,
            )
        })
        .ok_or(ExecutionDispatcherError::RetainedSizeOverflow)
}

/// Nonblocking handoff, registry, adapter, or construction failure.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum ExecutionDispatchError {
    #[error("execution lifecycle bounded allocation failed")]
    Allocation,
    #[error("mandatory execution audit admission is unavailable")]
    AuditUnavailable,
    #[error("execution command count capacity is saturated")]
    QueueCountSaturated,
    #[error("execution command byte capacity is saturated")]
    QueueBytesSaturated,
    #[error("execution worker is closed")]
    Closed,
    #[error("execution command retained size is unsupported")]
    CommandSizeUnsupported,
    #[error("approval identity was already consumed")]
    DuplicateApproval,
    #[error("one-use approval registry capacity is exhausted")]
    RegistryCapacity,
    #[error("approval registry is busy")]
    RegistryBusy,
    #[error("approval registry is poisoned")]
    RegistryPoisoned,
    #[error("approval registry invariant failed")]
    RegistryInvariant,
    #[error("trusted execution lifecycle clock is unavailable or regressed")]
    ClockUnavailable,
    #[error("order is not tracked by this dispatcher")]
    OrderNotTracked,
    #[error("order is not in a cancelable accepted state")]
    OrderNotCancelable,
    #[error("execution backend receipt did not match the submitted order")]
    ReceiptMismatch,
    #[error("authoritative account replacement was rejected")]
    AccountReplacementRejected,
    #[error("a reconciliation acknowledgement remains pending")]
    ReconciliationAcknowledgementPending,
    #[error("pending reconciliation retained-state capacity is exhausted")]
    PendingReconciliationCapacity,
    #[error("execution operation exceeded its monotonic deadline")]
    OperationDeadlineExceeded,
    #[error("execution task ownership capacity is saturated")]
    TaskOwnershipUnavailable,
    #[error(transparent)]
    Adapter(ExecutionAdapterError),
}

/// Dispatcher startup failure.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum ExecutionDispatcherError {
    #[error("dispatcher shutdown deadline must be positive")]
    ZeroShutdownDeadline,
    #[error("dispatcher byte capacity is unsupported")]
    ByteCapacityUnsupported,
    #[error("one approval command exceeds the dispatcher byte capacity")]
    CommandExceedsByteCapacity,
    #[error("dispatcher bounded allocation failed")]
    Allocation,
    #[error("dispatcher requires a current Tokio runtime")]
    RuntimeUnavailable,
    #[error("dispatcher retained-size accounting overflowed")]
    RetainedSizeOverflow,
    #[error("durable dispatcher recovery ownership is invalid")]
    InvalidRecovery,
    #[error("mandatory dispatcher recovery audit admission is unavailable")]
    RecoveryAuditUnavailable,
    #[error(transparent)]
    TaskOwnership(#[from] ExecutionTaskReaperError),
}

fn operation(
    deadline: Duration,
    cancellation: CancellationToken,
) -> Result<ExecutionOperation, ExecutionDispatchError> {
    let deadline = tokio::time::Instant::now()
        .checked_add(deadline)
        .ok_or(ExecutionDispatchError::OperationDeadlineExceeded)?;
    Ok(ExecutionOperation::new(deadline, cancellation))
}

/// Bounded worker shutdown outcome.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExecutionDispatcherShutdown {
    Complete,
    JoinError,
    DeadlineAborted,
    Incomplete,
}

/// Admission-drain result that retains reconciliation and persistence authority.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExecutionDispatcherQuiesce {
    Complete,
    AlreadyQuiesced,
    JoinError,
    Incomplete,
}

#[cfg(test)]
mod tests {
    use std::str::FromStr as _;

    use market_squawk_domain::OrderId;

    use super::{PendingReconciliationScope, reconciliation_scope};

    #[test]
    fn only_complete_account_reconciliation_may_acknowledge_a_financial_sequence()
    -> Result<(), Box<dyn std::error::Error>> {
        let tracked = OrderId::from_str("20000000-0000-0000-0000-000000000099")?;

        assert_eq!(
            reconciliation_scope(&[tracked]),
            PendingReconciliationScope::TrackedOrders
        );
        assert_eq!(
            reconciliation_scope(&[]),
            PendingReconciliationScope::CompleteAccounts
        );
        Ok(())
    }
}
