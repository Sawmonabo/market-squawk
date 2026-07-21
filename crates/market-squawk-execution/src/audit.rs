//! Bounded in-memory execution audit admission outside durable persistence.

use std::mem::size_of;
use std::num::{NonZeroU32, NonZeroUsize};
use std::sync::Arc;

use market_squawk_domain::{
    AccountId, ApprovalId, InstrumentId, ModelId, OrderId, StrategyId, Timestamp,
};
use market_squawk_live::ConsumedLiveAuthority;
use sha2::{Digest, Sha256};
use thiserror::Error;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc};

use crate::{
    ExecutionMarketReference, OrderIntent, OrderIntentDigest, RiskPolicyIdentity, RiskRejectionCode,
};

/// Maximum typed rejection reasons retained in a single fixed audit fact.
pub const MAX_EXECUTION_AUDIT_REASONS: usize = 64;

/// Startup-fixed count and byte capacities for mandatory execution audit admission.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExecutionAuditConfig {
    /// Maximum queued audit records.
    pub maximum_records: NonZeroUsize,
    /// Maximum retained record-envelope bytes across queued audit records.
    pub maximum_bytes: NonZeroU32,
}

/// Closed execution audit outcome.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExecutionAuditKind {
    RiskRejected,
    RiskApproved,
    DispatchRejected,
    DispatchKnownFailure,
    DispatchAccepted,
    DispatchUncertain,
    CancelAccepted,
    CancelTerminal,
    ReconciliationObserved,
}

/// Closed machine-readable reason retained with an audit outcome.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExecutionAuditReason {
    Risk(RiskRejectionCode),
    QueueCountSaturated,
    QueueBytesSaturated,
    DuplicateApproval,
    RegistryCapacity,
    RegistryUnavailable,
    ClockFailure,
    ApprovalInvalid,
    AdapterRejected,
    AdapterKnownFailure,
    AdapterUncertain,
    ReceiptMismatch,
    ObservationTimestampInvalid,
    UnexpectedReconciliationOrder,
    ReconciliationRequired,
    AccountReplacementRejected,
    PendingReconciliationCapacity,
    OperationDeadlineExceeded,
    AuditReasonOverflow,
}

/// Fixed, authority-free audit context retained through risk and dispatch.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ExecutionAuditContext {
    approval_id: ApprovalId,
    order_id: OrderId,
    intent_digest: OrderIntentDigest,
    strategy_id: StrategyId,
    model_id: Option<ModelId>,
    account_id: AccountId,
    instrument_id: InstrumentId,
    assessment_digest: [u8; 32],
    evidence_binding_digest: [u8; 32],
    evidence_present: bool,
    policy: RiskPolicyIdentity,
    market_observed_at: Timestamp,
    valid_until: Timestamp,
}

impl ExecutionAuditContext {
    pub(crate) fn from_risk(
        approval_id: ApprovalId,
        intent: &OrderIntent,
        market: ExecutionMarketReference,
        authority: Option<&ConsumedLiveAuthority>,
        policy: RiskPolicyIdentity,
        valid_until: Timestamp,
    ) -> Self {
        let (assessment_digest, evidence_binding_digest, evidence_present) =
            authority.map_or(([0; 32], [0; 32], false), |authority| {
                let mut assessment = Sha256::new();
                assessment.update(b"market-squawk/qualification-assessment\0");
                assessment.update(
                    authority
                        .assessment_id()
                        .as_source_identifier()
                        .as_str()
                        .as_bytes(),
                );
                (
                    assessment.finalize().into(),
                    authority.binding_digest(),
                    true,
                )
            });
        Self {
            approval_id,
            order_id: intent.order_id(),
            intent_digest: intent.digest(),
            strategy_id: intent.strategy_id(),
            model_id: intent.model_id(),
            account_id: intent.account_id(),
            instrument_id: intent.execution_terms().instrument_id(),
            assessment_digest,
            evidence_binding_digest,
            evidence_present,
            policy,
            market_observed_at: market.observed_at(),
            valid_until,
        }
    }

    pub(crate) const fn market_observed_at(self) -> Timestamp {
        self.market_observed_at
    }
}

/// Bounded audit fact containing no capability, approval, credential, or secret.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExecutionAuditEvent {
    kind: ExecutionAuditKind,
    context: ExecutionAuditContext,
    observed_at: Timestamp,
    reasons: [Option<ExecutionAuditReason>; MAX_EXECUTION_AUDIT_REASONS],
    reason_count: u8,
}

impl ExecutionAuditEvent {
    pub(crate) fn from_context(
        kind: ExecutionAuditKind,
        context: ExecutionAuditContext,
        observed_at: Timestamp,
        reasons: &[ExecutionAuditReason],
    ) -> Self {
        if reasons.len() > MAX_EXECUTION_AUDIT_REASONS {
            return Self {
                kind,
                context,
                observed_at,
                reasons: overflow_reasons(),
                reason_count: 1,
            };
        }
        let mut retained = [None; MAX_EXECUTION_AUDIT_REASONS];
        for (target, reason) in retained.iter_mut().zip(reasons) {
            *target = Some(*reason);
        }
        Self {
            kind,
            context,
            observed_at,
            reasons: retained,
            reason_count: u8::try_from(reasons.len()).unwrap_or(1),
        }
    }

    pub const fn kind(&self) -> ExecutionAuditKind {
        self.kind
    }
    pub const fn approval_id(&self) -> ApprovalId {
        self.context.approval_id
    }
    pub const fn order_id(&self) -> OrderId {
        self.context.order_id
    }
    pub const fn intent_digest(&self) -> OrderIntentDigest {
        self.context.intent_digest
    }
    pub const fn strategy_id(&self) -> StrategyId {
        self.context.strategy_id
    }
    pub const fn model_id(&self) -> Option<ModelId> {
        self.context.model_id
    }
    pub const fn account_id(&self) -> AccountId {
        self.context.account_id
    }
    pub const fn instrument_id(&self) -> InstrumentId {
        self.context.instrument_id
    }
    pub const fn assessment_digest(&self) -> Option<[u8; 32]> {
        if self.context.evidence_present {
            Some(self.context.assessment_digest)
        } else {
            None
        }
    }
    pub const fn evidence_binding_digest(&self) -> Option<[u8; 32]> {
        if self.context.evidence_present {
            Some(self.context.evidence_binding_digest)
        } else {
            None
        }
    }
    pub const fn risk_policy(&self) -> RiskPolicyIdentity {
        self.context.policy
    }
    pub const fn market_observed_at(&self) -> Timestamp {
        self.context.market_observed_at
    }
    pub const fn valid_until(&self) -> Timestamp {
        self.context.valid_until
    }
    pub const fn observed_at(&self) -> Timestamp {
        self.observed_at
    }
    pub fn reasons(&self) -> impl Iterator<Item = ExecutionAuditReason> + '_ {
        self.reasons[..usize::from(self.reason_count)]
            .iter()
            .filter_map(|reason| *reason)
    }

    pub(crate) fn from_risk_context(
        kind: ExecutionAuditKind,
        context: ExecutionAuditContext,
        observed_at: Timestamp,
        reasons: &[RiskRejectionCode],
    ) -> Self {
        if reasons.len() > MAX_EXECUTION_AUDIT_REASONS {
            return Self {
                kind,
                context,
                observed_at,
                reasons: overflow_reasons(),
                reason_count: 1,
            };
        }
        let mut retained = [None; MAX_EXECUTION_AUDIT_REASONS];
        for (target, reason) in retained.iter_mut().zip(reasons) {
            *target = Some(ExecutionAuditReason::Risk(*reason));
        }
        Self {
            kind,
            context,
            observed_at,
            reasons: retained,
            reason_count: u8::try_from(reasons.len()).unwrap_or(1),
        }
    }
}

/// Cloneable nonblocking audit admission handle shared by risk and dispatch.
#[derive(Clone, Debug)]
pub struct ExecutionAuditWriter {
    sender: mpsc::Sender<AuditEnvelope>,
    bytes: Arc<Semaphore>,
}

impl ExecutionAuditWriter {
    /// Creates one bounded audit channel and its sole reader.
    pub fn try_new(
        config: ExecutionAuditConfig,
    ) -> Result<(Self, ExecutionAuditReader), ExecutionAuditError> {
        let maximum_bytes = usize::try_from(config.maximum_bytes.get())
            .map_err(|_| ExecutionAuditError::ByteCapacityUnsupported)?;
        if maximum_bytes > Semaphore::MAX_PERMITS {
            return Err(ExecutionAuditError::ByteCapacityUnsupported);
        }
        if maximum_bytes < audit_envelope_bytes() {
            return Err(ExecutionAuditError::RecordExceedsCapacity);
        }
        let (sender, receiver) = mpsc::channel(config.maximum_records.get());
        Ok((
            Self {
                sender,
                bytes: Arc::new(Semaphore::new(maximum_bytes)),
            },
            ExecutionAuditReader { receiver },
        ))
    }

    pub(crate) fn try_reserve(&self) -> Result<ExecutionAuditPermit, ExecutionAuditError> {
        let byte_count = u32::try_from(audit_envelope_bytes())
            .map_err(|_| ExecutionAuditError::RecordSizeUnsupported)?;
        let slot = self
            .sender
            .clone()
            .try_reserve_owned()
            .map_err(|error| match error {
                mpsc::error::TrySendError::Full(_) => ExecutionAuditError::Saturated,
                mpsc::error::TrySendError::Closed(_) => ExecutionAuditError::Closed,
            })?;
        let bytes = Arc::clone(&self.bytes)
            .try_acquire_many_owned(byte_count)
            .map_err(|_| ExecutionAuditError::Saturated)?;
        Ok(ExecutionAuditPermit { slot, bytes })
    }

    /// Returns the fixed record-envelope charge used by the byte semaphore.
    pub const fn retained_bytes_per_record() -> usize {
        audit_envelope_bytes()
    }
}

/// Sole bounded reader transferred to an outside-hot-path audit consumer.
#[derive(Debug)]
pub struct ExecutionAuditReader {
    receiver: mpsc::Receiver<AuditEnvelope>,
}

impl ExecutionAuditReader {
    /// Reads one queued fact without waiting.
    pub fn try_next(&mut self) -> Result<Option<ExecutionAuditEvent>, ExecutionAuditError> {
        match self.receiver.try_recv() {
            Ok(envelope) => Ok(Some(envelope.event)),
            Err(mpsc::error::TryRecvError::Empty) => Ok(None),
            Err(mpsc::error::TryRecvError::Disconnected) => Err(ExecutionAuditError::Closed),
        }
    }
}

#[derive(Debug)]
struct AuditEnvelope {
    event: ExecutionAuditEvent,
    _bytes: OwnedSemaphorePermit,
}

#[derive(Debug)]
pub(crate) struct ExecutionAuditPermit {
    slot: mpsc::OwnedPermit<AuditEnvelope>,
    bytes: OwnedSemaphorePermit,
}

impl ExecutionAuditPermit {
    pub(crate) fn commit(self, event: ExecutionAuditEvent) {
        let Self { slot, bytes } = self;
        drop(slot.send(AuditEnvelope {
            event,
            _bytes: bytes,
        }));
    }
}

/// Bounded audit admission or event construction failure.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum ExecutionAuditError {
    #[error("execution audit queue is saturated")]
    Saturated,
    #[error("execution audit queue is closed")]
    Closed,
    #[error("execution audit byte capacity is unsupported")]
    ByteCapacityUnsupported,
    #[error("execution audit record size is unsupported")]
    RecordSizeUnsupported,
    #[error("execution audit record exceeds configured byte capacity")]
    RecordExceedsCapacity,
}

const fn overflow_reasons() -> [Option<ExecutionAuditReason>; MAX_EXECUTION_AUDIT_REASONS] {
    let mut reasons = [None; MAX_EXECUTION_AUDIT_REASONS];
    reasons[0] = Some(ExecutionAuditReason::AuditReasonOverflow);
    reasons
}

const fn audit_envelope_bytes() -> usize {
    size_of::<AuditEnvelope>()
}
