//! Bounded one-use approval dispatch and accepted-order ownership.

mod lifecycle;
mod reconciliation;
mod worker;

use std::collections::HashMap;
use std::num::{NonZeroU32, NonZeroUsize};
use std::sync::{Arc, Mutex, TryLockError};
use std::time::Duration;

use market_squawk_domain::{AccountId, ApprovalId, Currency, OrderId, QuantityLots, Timestamp};
use thiserror::Error;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc};
use tokio_util::sync::CancellationToken;

use crate::approval::APPROVAL_COMMAND_RETAINED_BYTE_CEILING;
use crate::audit::{ExecutionAuditContext, ExecutionAuditPermit};
use crate::clock::system_now;
use crate::{
    AccountRiskCoordinator, AccountRiskReservation, ApprovedOrder, ExecutionAdapter,
    ExecutionAdapterError, ExecutionAuditEvent, ExecutionAuditKind, ExecutionAuditReason,
    ExecutionAuditWriter, OrderIntentDigest, ReconciledOrder,
};
use worker::run_worker;

/// Startup-fixed dispatcher count, bytes, identity, and shutdown bounds.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExecutionDispatcherConfig {
    pub maximum_queued_commands: NonZeroUsize,
    pub maximum_queued_bytes: NonZeroU32,
    pub maximum_registry_entries: NonZeroUsize,
    pub shutdown_deadline: Duration,
}

/// Owner of the execution worker, accepted reservations, and adapter lifecycle operations.
#[derive(Debug)]
pub struct ExecutionDispatcher {
    handle: ExecutionDispatcherHandle,
    adapter: Arc<dyn ExecutionAdapter>,
    accounts: Arc<AccountRiskCoordinator>,
    registry: Arc<Mutex<DispatchRegistry>>,
    audit: ExecutionAuditWriter,
    cancellation: CancellationToken,
    worker: Option<tokio::task::JoinHandle<()>>,
    shutdown_deadline: Duration,
}

impl ExecutionDispatcher {
    /// Starts a bounded worker in the current Tokio runtime.
    pub fn try_start(
        adapter: Arc<dyn ExecutionAdapter>,
        accounts: Arc<AccountRiskCoordinator>,
        audit: ExecutionAuditWriter,
        config: ExecutionDispatcherConfig,
    ) -> Result<Self, ExecutionDispatcherError> {
        if config.shutdown_deadline.is_zero() {
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
        let registry = Arc::new(Mutex::new(DispatchRegistry {
            entries,
            maximum_entries: config.maximum_registry_entries.get(),
        }));
        let (sender, receiver) = mpsc::channel(config.maximum_queued_commands.get());
        let bytes = Arc::new(Semaphore::new(command_bytes));
        let cancellation = CancellationToken::new();
        let retained_bytes = retained_dispatcher_bytes(config)?;
        let runtime = tokio::runtime::Handle::try_current()
            .map_err(|_| ExecutionDispatcherError::RuntimeUnavailable)?;
        let worker = runtime.spawn(run_worker(
            Arc::clone(&adapter),
            Arc::clone(&registry),
            receiver,
            cancellation.child_token(),
        ));
        let handle = ExecutionDispatcherHandle {
            sender,
            bytes,
            registry: Arc::clone(&registry),
            audit: audit.clone(),
            retained_bytes,
        };
        Ok(Self {
            handle,
            adapter,
            accounts,
            registry,
            audit,
            cancellation,
            worker: Some(worker),
            shutdown_deadline: config.shutdown_deadline,
        })
    }

    /// Returns the cloneable nonblocking handoff used by live action hooks.
    pub fn handle(&self) -> ExecutionDispatcherHandle {
        self.handle.clone()
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
                    settlement_currency: approval.execution_terms().settlement_currency(),
                    last_transition_at: context.market_observed_at(),
                    lifecycle: None,
                },
            );
        }
        drop(slot.send(DispatchCommand {
            approval,
            audit,
            context,
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
    _bytes: OwnedSemaphorePermit,
}

#[derive(Debug)]
struct DispatchRegistry {
    entries: HashMap<ApprovalId, DispatchRecord>,
    maximum_entries: usize,
}

#[derive(Debug)]
struct DispatchRecord {
    order_id: OrderId,
    account_id: AccountId,
    intent_digest: OrderIntentDigest,
    account_revision: u64,
    state: DispatchState,
    reservation: Option<AccountRiskReservation>,
    audit_context: ExecutionAuditContext,
    requested_quantity: QuantityLots,
    settlement_currency: Option<Currency>,
    last_transition_at: Timestamp,
    lifecycle: Option<ReconciledOrder>,
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
    std::mem::size_of::<ExecutionDispatcherHandle>()
        .checked_add(
            config
                .maximum_queued_commands
                .get()
                .checked_mul(APPROVAL_COMMAND_RETAINED_BYTE_CEILING)
                .ok_or(ExecutionDispatcherError::RetainedSizeOverflow)?,
        )
        .and_then(|value| {
            value.checked_add(
                config
                    .maximum_registry_entries
                    .get()
                    .checked_mul(std::mem::size_of::<DispatchRecord>())?,
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
}

/// Bounded worker shutdown outcome.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExecutionDispatcherShutdown {
    Complete,
    JoinError,
    DeadlineAborted,
}
