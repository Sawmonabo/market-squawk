//! Stable execution adapter and bounded market-update contracts.

mod account_state;

pub use account_state::{
    ACCOUNT_REPLACEMENT_SCHEMA_VERSION, ExecutionStateSourceBinding, MAX_RECONCILED_ACCOUNTS,
    MAX_RECONCILED_POSITIONS_PER_ACCOUNT, ReconciledAccountState, ReconciledAccountStateError,
};

use std::future::Future;
use std::pin::Pin;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use market_squawk_domain::{
    AccountId, AggressorSide, ApprovalId, BasisPoints, ClientOrderId, ConnectionGeneration,
    InstrumentExecutionTerms, LiveEventClass, LiveEvidenceBinding, MarketEvent, ModelId, Money,
    OrderId, OrderReasonCode, OrderSide, OrderType, PriceTicks, QualificationAssessmentId,
    QuantityLots, StrategyId, TimeInForce, Timestamp,
};
use market_squawk_live::{CommittedActionContext, ConsumedLiveEvidence};
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::dispatcher::PersistenceFinalization;
use crate::{
    ExecutionMarketReference, ExecutionPriceBound, OrderIntent, OrderIntentDigest,
    RiskPolicyIdentity,
};

/// Object-safe boxed future returned by execution adapters.
pub type ExecutionAdapterFuture<'adapter, Output> =
    Pin<Box<dyn Future<Output = Output> + Send + 'adapter>>;

/// Maximum reconciled orders an adapter may return in one bounded state image.
pub const MAX_RECONCILED_ORDERS: usize = 4_096;

/// Public adapter-consumable order with private dispatcher-only construction.
///
/// The type is intentionally non-cloneable and non-serializable. It owns the exact bounded market
/// reference and moved live evidence, but neither value can revalidate or mint execution authority.
#[derive(Debug)]
pub struct DispatchOrder {
    approval_id: ApprovalId,
    intent: OrderIntent,
    market: ExecutionMarketReference,
    execution_price_bound: ExecutionPriceBound,
    evidence: ConsumedLiveEvidence,
    policy: RiskPolicyIdentity,
    valid_until: Timestamp,
    submitted_at: Timestamp,
    account_revision: u64,
    operation: ExecutionOperation,
}

impl DispatchOrder {
    /// Returns the one-use approval identity consumed by dispatch.
    pub const fn approval_id(&self) -> ApprovalId {
        self.approval_id
    }

    /// Returns the stable internal order identity.
    pub const fn order_id(&self) -> OrderId {
        self.intent.order_id()
    }

    /// Returns the caller-selected idempotency identity.
    pub const fn client_order_id(&self) -> &ClientOrderId {
        self.intent.client_order_id()
    }

    /// Returns the strategy that originated the intent.
    pub const fn strategy_id(&self) -> StrategyId {
        self.intent.strategy_id()
    }

    /// Returns the contributing model, when present.
    pub const fn model_id(&self) -> Option<ModelId> {
        self.intent.model_id()
    }

    /// Returns the risk-coordinated account.
    pub const fn account_id(&self) -> AccountId {
        self.intent.account_id()
    }

    /// Returns exact instrument revision, precision, currencies, and multiplier.
    pub const fn execution_terms(&self) -> InstrumentExecutionTerms {
        self.market.execution_terms()
    }

    /// Returns the side.
    pub const fn side(&self) -> OrderSide {
        self.intent.side()
    }

    /// Returns the order type.
    pub const fn order_type(&self) -> OrderType {
        self.intent.order_type()
    }

    /// Returns the positive quantity in instrument lots.
    pub const fn quantity(&self) -> QuantityLots {
        self.intent.quantity()
    }

    /// Returns the optional limit price.
    pub const fn limit_price(&self) -> Option<PriceTicks> {
        self.intent.limit_price()
    }

    /// Returns the optional stop price.
    pub const fn stop_price(&self) -> Option<PriceTicks> {
        self.intent.stop_price()
    }

    /// Returns the requested time-in-force.
    pub const fn time_in_force(&self) -> TimeInForce {
        self.intent.time_in_force()
    }

    /// Returns when the strategy generated the signal.
    pub const fn signal_at(&self) -> Timestamp {
        self.intent.signal_at()
    }

    /// Returns the strategy-selected intent expiry.
    pub const fn intent_expires_at(&self) -> Timestamp {
        self.intent.expires_at()
    }

    /// Returns the bounded machine-readable rationale.
    pub const fn reason_codes(&self) -> &[OrderReasonCode] {
        self.intent.reason_codes()
    }

    /// Returns the versioned canonical digest over every execution-relevant intent field.
    pub const fn intent_digest(&self) -> OrderIntentDigest {
        self.intent.digest()
    }

    /// Returns the strategy-selected adverse slippage ceiling.
    pub const fn maximum_slippage(&self) -> BasisPoints {
        self.intent.maximum_slippage()
    }

    /// Returns the exact live qualification assessment.
    pub const fn assessment_id(&self) -> &QualificationAssessmentId {
        self.evidence.assessment_id()
    }

    /// Returns the exact source/venue/instrument/generation/state evidence binding.
    pub const fn evidence_binding(&self) -> &LiveEvidenceBinding {
        self.evidence.binding()
    }

    /// Returns the nonce-bound evidence digest.
    pub const fn evidence_binding_digest(&self) -> [u8; 32] {
        self.evidence.binding_digest()
    }

    /// Returns the bounded committed market image used by risk and paper matching.
    pub const fn market(&self) -> ExecutionMarketReference {
        self.market
    }

    /// Returns the side-aware top-of-book reference bound into approval and paper matching.
    pub fn execution_price(&self) -> Option<PriceTicks> {
        self.market.execution_price(self.side())
    }

    /// Returns the inclusive risk-reserved upper average execution-price bound.
    pub const fn execution_price_bound(&self) -> ExecutionPriceBound {
        self.execution_price_bound
    }

    /// Returns the fixed risk policy/ruleset identity.
    pub const fn risk_policy(&self) -> RiskPolicyIdentity {
        self.policy
    }

    /// Returns the inclusive minimum of all authority, account, policy, and intent deadlines.
    pub const fn valid_until(&self) -> Timestamp {
        self.valid_until
    }

    /// Returns the trusted final-dispatch time.
    pub const fn submitted_at(&self) -> Timestamp {
        self.submitted_at
    }

    /// Returns the exact account-state revision reserved before submission.
    pub const fn account_revision(&self) -> u64 {
        self.account_revision
    }

    /// Returns the stable digest used by execution audit for the qualification assessment.
    pub fn assessment_digest(&self) -> [u8; 32] {
        let mut assessment = Sha256::new();
        assessment.update(b"market-squawk/qualification-assessment\0");
        assessment.update(
            self.assessment_id()
                .as_source_identifier()
                .as_str()
                .as_bytes(),
        );
        assessment.finalize().into()
    }

    /// Returns the monotonic deadline and cooperative cancellation signal for this one attempt.
    pub const fn operation(&self) -> &ExecutionOperation {
        &self.operation
    }
}

#[allow(
    clippy::too_many_arguments,
    reason = "dispatcher construction transfers every immutable risk and evidence binding"
)]
pub(crate) const fn dispatch_order_from_approval(
    approval_id: ApprovalId,
    intent: OrderIntent,
    market: ExecutionMarketReference,
    execution_price_bound: ExecutionPriceBound,
    evidence: ConsumedLiveEvidence,
    policy: RiskPolicyIdentity,
    valid_until: Timestamp,
    submitted_at: Timestamp,
    account_revision: u64,
    operation: ExecutionOperation,
) -> DispatchOrder {
    DispatchOrder {
        approval_id,
        intent,
        market,
        execution_price_bound,
        evidence,
        policy,
        valid_until,
        submitted_at,
        account_revision,
        operation,
    }
}

/// Exact non-authoritative dispatcher ownership restored from a durable backend checkpoint.
#[derive(Debug)]
pub struct RecoveredDispatchOrder {
    approval_id: ApprovalId,
    order_id: OrderId,
    account_id: AccountId,
    intent_digest: OrderIntentDigest,
    account_revision: u64,
    requested_quantity: QuantityLots,
    execution_price_bound: ExecutionPriceBound,
    settlement_currency: Option<market_squawk_domain::Currency>,
    lifecycle: ReconciledOrder,
    audit_context: crate::audit::ExecutionAuditContext,
    recovered_at: Timestamp,
}

impl RecoveredDispatchOrder {
    #[allow(
        clippy::too_many_arguments,
        reason = "recovery validates every persisted dispatch and audit identity dimension"
    )]
    pub fn try_new(
        approval_id: ApprovalId,
        order_id: OrderId,
        account_id: AccountId,
        instrument_id: market_squawk_domain::InstrumentId,
        intent_digest: OrderIntentDigest,
        account_revision: u64,
        requested_quantity: QuantityLots,
        execution_price_bound: ExecutionPriceBound,
        settlement_currency: Option<market_squawk_domain::Currency>,
        lifecycle: ReconciledOrder,
        strategy_id: StrategyId,
        model_id: Option<ModelId>,
        assessment_digest: [u8; 32],
        evidence_binding_digest: [u8; 32],
        policy: RiskPolicyIdentity,
        market_observed_at: Timestamp,
        valid_until: Timestamp,
        recovered_at: Timestamp,
    ) -> Result<Self, RecoveredDispatchOrderError> {
        if account_revision == 0
            || requested_quantity.get() <= 0
            || lifecycle.order_id() != order_id
            || lifecycle.cumulative_filled().get() < 0
            || lifecycle.cumulative_filled().get() > requested_quantity.get()
            || lifecycle
                .maximum_fill_price()
                .is_some_and(|price| !execution_price_bound.permits(price))
            || settlement_currency != Some(lifecycle.cumulative_fees().currency())
            || assessment_digest == [0; 32]
            || evidence_binding_digest == [0; 32]
            || valid_until < market_observed_at
            || recovered_at < market_observed_at
        {
            return Err(RecoveredDispatchOrderError::InvalidIdentity);
        }
        Ok(Self {
            approval_id,
            order_id,
            account_id,
            intent_digest,
            account_revision,
            requested_quantity,
            execution_price_bound,
            settlement_currency,
            lifecycle,
            audit_context: crate::audit::ExecutionAuditContext::from_recovery(
                approval_id,
                order_id,
                intent_digest,
                strategy_id,
                model_id,
                account_id,
                instrument_id,
                assessment_digest,
                evidence_binding_digest,
                execution_price_bound,
                policy,
                market_observed_at,
                valid_until,
            ),
            recovered_at,
        })
    }

    pub(crate) fn into_parts(self) -> RecoveredDispatchOrderParts {
        RecoveredDispatchOrderParts {
            approval_id: self.approval_id,
            order_id: self.order_id,
            account_id: self.account_id,
            intent_digest: self.intent_digest,
            account_revision: self.account_revision,
            requested_quantity: self.requested_quantity,
            execution_price_bound: self.execution_price_bound,
            settlement_currency: self.settlement_currency,
            lifecycle: self.lifecycle,
            audit_context: self.audit_context,
            recovered_at: self.recovered_at,
        }
    }
}

#[derive(Debug)]
pub(crate) struct RecoveredDispatchOrderParts {
    pub(crate) approval_id: ApprovalId,
    pub(crate) order_id: OrderId,
    pub(crate) account_id: AccountId,
    pub(crate) intent_digest: OrderIntentDigest,
    pub(crate) account_revision: u64,
    pub(crate) requested_quantity: QuantityLots,
    pub(crate) execution_price_bound: ExecutionPriceBound,
    pub(crate) settlement_currency: Option<market_squawk_domain::Currency>,
    pub(crate) lifecycle: ReconciledOrder,
    pub(crate) audit_context: crate::audit::ExecutionAuditContext,
    pub(crate) recovered_at: Timestamp,
}

/// Invalid durable dispatcher recovery identity.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum RecoveredDispatchOrderError {
    #[error("persisted dispatcher recovery identity is inconsistent")]
    InvalidIdentity,
}

/// Dispatcher-minted monotonic operation lifetime supplied to an execution adapter.
#[derive(Debug)]
pub struct ExecutionOperation {
    deadline: Instant,
    cancellation: CancellationToken,
}

/// Private-construction authority to clear one backend recovery quarantine.
///
/// Only the dispatcher can mint this capability. Adapters must not clear durable quarantine state
/// through ordinary control-plane or adapter-specific APIs.
#[derive(Debug)]
pub struct RecoverExecutionState {
    operation: ExecutionOperation,
}

impl RecoverExecutionState {
    pub(crate) const fn new(operation: ExecutionOperation) -> Self {
        Self { operation }
    }

    /// Returns the dispatcher-owned monotonic operation lifetime.
    pub const fn operation(&self) -> &ExecutionOperation {
        &self.operation
    }
}

impl ExecutionOperation {
    pub(crate) fn new(deadline: Instant, cancellation: CancellationToken) -> Self {
        Self {
            deadline,
            cancellation,
        }
    }

    /// Returns the absolute monotonic deadline.
    pub const fn deadline(&self) -> Instant {
        self.deadline
    }

    /// Returns whether cancellation has already been requested.
    pub fn is_cancelled(&self) -> bool {
        self.cancellation.is_cancelled()
    }

    /// Waits for cooperative cancellation.
    pub async fn cancelled(&self) {
        self.cancellation.cancelled().await;
    }

    pub(crate) fn cancellation(&self) -> CancellationToken {
        self.cancellation.clone()
    }

    /// Returns whether cancellation or the monotonic deadline already forbids mutation.
    pub fn is_expired(&self) -> bool {
        self.is_cancelled() || Instant::now() >= self.deadline
    }
}

/// Private-construction cancellation authority for one dispatcher-owned order.
#[derive(Debug)]
pub struct CancelOrder {
    order_id: OrderId,
    operation: ExecutionOperation,
}

impl CancelOrder {
    pub(crate) fn new(order_id: OrderId, operation: ExecutionOperation) -> Self {
        Self {
            order_id,
            operation,
        }
    }

    pub const fn order_id(&self) -> OrderId {
        self.order_id
    }

    pub const fn operation(&self) -> &ExecutionOperation {
        &self.operation
    }
}

/// Private-construction bounded reconciliation authority.
#[derive(Debug)]
pub struct ReconcileOrders {
    order_ids: Box<[OrderId]>,
    operation: ExecutionOperation,
}

impl ReconcileOrders {
    pub(crate) fn new(order_ids: Box<[OrderId]>, operation: ExecutionOperation) -> Self {
        Self {
            order_ids,
            operation,
        }
    }

    pub const fn order_ids(&self) -> &[OrderId] {
        &self.order_ids
    }

    pub const fn operation(&self) -> &ExecutionOperation {
        &self.operation
    }
}

/// Private-construction proof that dispatcher-side reconciliation completed successfully.
#[derive(Debug)]
pub struct ReconciliationAcknowledgement {
    batch: ReconciliationBatchBinding,
    order_ids: Box<[OrderId]>,
    operation: ExecutionOperation,
}

impl ReconciliationAcknowledgement {
    pub(crate) fn new(
        batch: ReconciliationBatchBinding,
        order_ids: Box<[OrderId]>,
        operation: ExecutionOperation,
    ) -> Self {
        Self {
            batch,
            order_ids,
            operation,
        }
    }

    /// Returns the stable dispatcher-minted idempotency identity for this exact batch.
    pub const fn batch_id(&self) -> ReconciliationBatchId {
        self.batch.batch_id
    }

    /// Returns the digest binding the backend image, orders, accounts, and dispatcher invocation.
    pub const fn binding_digest(&self) -> [u8; 32] {
        self.batch.binding_digest
    }

    pub const fn order_ids(&self) -> &[OrderId] {
        &self.order_ids
    }

    pub const fn operation(&self) -> &ExecutionOperation {
        &self.operation
    }
}

/// Opaque nonzero dispatcher-minted reconciliation idempotency identity.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct ReconciliationBatchId([u8; 32]);

impl ReconciliationBatchId {
    /// Restores a bounded persisted identity while rejecting the reserved zero value.
    pub fn try_from_bytes(bytes: [u8; 32]) -> Result<Self, ReconciliationBatchBindingError> {
        if bytes == [0; 32] {
            return Err(ReconciliationBatchBindingError::ZeroIdentity);
        }
        Ok(Self(bytes))
    }

    /// Returns the fixed identity bytes.
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

/// Exact stable reconciliation identity and immutable state binding.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReconciliationBatchBinding {
    batch_id: ReconciliationBatchId,
    binding_digest: [u8; 32],
}

impl ReconciliationBatchBinding {
    /// Restores and validates one persisted replay-fence binding.
    pub fn try_new(
        batch_id: ReconciliationBatchId,
        binding_digest: [u8; 32],
    ) -> Result<Self, ReconciliationBatchBindingError> {
        if binding_digest == [0; 32] {
            return Err(ReconciliationBatchBindingError::ZeroDigest);
        }
        Ok(Self {
            batch_id,
            binding_digest,
        })
    }

    pub(crate) fn from_dispatcher_digest(
        binding_digest: [u8; 32],
    ) -> Result<Self, ReconciliationBatchBindingError> {
        if binding_digest == [0; 32] {
            return Err(ReconciliationBatchBindingError::ZeroDigest);
        }
        let mut digest = Sha256::new();
        digest.update(b"market-squawk/reconciliation-batch-id/v1\0");
        digest.update(binding_digest);
        let batch_id = ReconciliationBatchId::try_from_bytes(digest.finalize().into())?;
        Ok(Self {
            batch_id,
            binding_digest,
        })
    }

    /// Returns the opaque batch identity.
    pub const fn batch_id(self) -> ReconciliationBatchId {
        self.batch_id
    }

    /// Returns the immutable batch binding digest.
    pub const fn binding_digest(self) -> [u8; 32] {
        self.binding_digest
    }
}

/// Invalid persisted or dispatcher-created reconciliation binding.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum ReconciliationBatchBindingError {
    #[error("reconciliation batch identity must be nonzero")]
    ZeroIdentity,
    #[error("reconciliation batch binding digest must be nonzero")]
    ZeroDigest,
}

/// One-use dispatcher-minted authority to acknowledge externally durable checkpoint evidence.
#[derive(Debug)]
pub struct PersistenceAcknowledgement {
    operation: ExecutionOperation,
    finalized_reconciliations: Box<[ReconciliationBatchBinding]>,
    finalization: PersistenceFinalization,
}

impl PersistenceAcknowledgement {
    pub(crate) fn new(
        operation: ExecutionOperation,
        finalized_reconciliations: Box<[ReconciliationBatchBinding]>,
        finalization: PersistenceFinalization,
    ) -> Self {
        Self {
            operation,
            finalized_reconciliations,
            finalization,
        }
    }

    pub const fn operation(&self) -> &ExecutionOperation {
        &self.operation
    }

    /// Returns exact locally finalized reconciliation batches eligible for persisted-fence prune.
    pub const fn finalized_reconciliations(&self) -> &[ReconciliationBatchBinding] {
        &self.finalized_reconciliations
    }

    /// Returns the exact bytes exclusively retained by this sealed authority.
    pub fn retained_bytes(&self) -> Option<usize> {
        std::mem::size_of::<Self>().checked_add(std::mem::size_of_val(
            self.finalized_reconciliations.as_ref(),
        ))
    }

    /// Commits only the exact finalized proofs covered by durable adapter evidence.
    pub fn commit_persisted(
        self,
        persisted: &[ReconciliationBatchBinding],
    ) -> Result<(), ExecutionAdapterError> {
        let Self {
            operation,
            finalized_reconciliations,
            finalization,
        } = self;
        for binding in persisted {
            let mut finalized = false;
            for candidate in &finalized_reconciliations {
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
        if operation.is_expired() {
            return Err(ExecutionAdapterError::KnownFailure);
        }
        finalization.commit(persisted, &operation)
    }
}

/// Private-construction fixed-size market update published before strategy dispatch.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExecutionMarketUpdate {
    market: ExecutionMarketReference,
    assessment_digest: [u8; 32],
    venue_digest: [u8; 32],
    connection_generation: ConnectionGeneration,
    event_class: LiveEventClass,
    trade_price: Option<PriceTicks>,
    trade_quantity: Option<QuantityLots>,
    aggressor_side: Option<AggressorSide>,
}

impl ExecutionMarketUpdate {
    pub(crate) fn from_committed_context(
        context: &CommittedActionContext<'_>,
        market: ExecutionMarketReference,
    ) -> Self {
        let mut assessment = Sha256::new();
        assessment.update(b"market-squawk/qualification-assessment\0");
        assessment.update(
            context
                .assessment_id()
                .as_source_identifier()
                .as_str()
                .as_bytes(),
        );
        let binding = match context.event() {
            MarketEvent::Trade(event) => event.provenance().binding(),
            MarketEvent::Quote(event) => event.provenance().binding(),
            MarketEvent::BookSnapshot(event) => event.provenance().binding(),
            MarketEvent::BookDelta(event) => event.provenance().binding(),
            MarketEvent::Auction(event) => event.provenance().binding(),
            MarketEvent::TradingHalt(event) => event.provenance().binding(),
            MarketEvent::InstrumentStatus(event) => event.provenance().binding(),
            MarketEvent::CorporateAction(event) => event.provenance().binding(),
        };
        let mut venue = Sha256::new();
        venue.update(b"market-squawk/execution-market-venue/v1\0");
        venue.update(binding.venue_id().as_str().as_bytes());
        let (event_class, trade_price, trade_quantity, aggressor_side) = match context.event() {
            MarketEvent::Trade(trade) => (
                LiveEventClass::Trade,
                Some(trade.price()),
                Some(trade.quantity()),
                Some(trade.aggressor_side()),
            ),
            MarketEvent::Quote(_) => (LiveEventClass::Quote, None, None, None),
            MarketEvent::BookSnapshot(_) => (LiveEventClass::BookSnapshot, None, None, None),
            MarketEvent::BookDelta(_) => (LiveEventClass::BookDelta, None, None, None),
            MarketEvent::Auction(_) => (LiveEventClass::Auction, None, None, None),
            MarketEvent::TradingHalt(_) => (LiveEventClass::TradingHalt, None, None, None),
            MarketEvent::InstrumentStatus(_) => {
                (LiveEventClass::InstrumentStatus, None, None, None)
            }
            MarketEvent::CorporateAction(_) => (LiveEventClass::CorporateAction, None, None, None),
        };
        Self {
            market,
            assessment_digest: assessment.finalize().into(),
            venue_digest: venue.finalize().into(),
            connection_generation: binding.connection_generation(),
            event_class,
            trade_price,
            trade_quantity,
            aggressor_side,
        }
    }

    /// Returns the fixed market image after the canonical event committed.
    pub const fn market(self) -> ExecutionMarketReference {
        self.market
    }

    /// Returns a fixed digest of the retained qualification assessment identity.
    pub const fn assessment_digest(self) -> [u8; 32] {
        self.assessment_digest
    }

    /// Returns the fixed digest of the exact venue identity bound to the live assessment.
    pub const fn venue_digest(self) -> [u8; 32] {
        self.venue_digest
    }

    /// Returns the exact source connection generation bound to the live assessment.
    pub const fn connection_generation(self) -> ConnectionGeneration {
        self.connection_generation
    }

    /// Returns the canonical event class that produced the update.
    pub const fn event_class(self) -> LiveEventClass {
        self.event_class
    }

    /// Returns executed trade price when this update came from a trade.
    pub const fn trade_price(self) -> Option<PriceTicks> {
        self.trade_price
    }

    /// Returns executed trade quantity when this update came from a trade.
    pub const fn trade_quantity(self) -> Option<QuantityLots> {
        self.trade_quantity
    }

    /// Returns the aggressor side when supplied for a trade.
    pub const fn aggressor_side(self) -> Option<AggressorSide> {
        self.aggressor_side
    }
}

/// Bounded, nonblocking paper/live execution market publisher.
pub trait ExecutionMarketSink: Send + Sync + std::fmt::Debug + 'static {
    /// Publishes one fixed-size current market image without waiting or performing I/O.
    fn try_publish(&self, update: ExecutionMarketUpdate) -> Result<(), ExecutionMarketSinkError>;

    /// Returns the startup-fixed retained footprint of the sink-owned graph.
    fn retained_bytes(&self) -> Result<usize, ExecutionMarketSinkError>;
}

/// Execution-market handoff failure. Every variant suppresses strategy dispatch for that event.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum ExecutionMarketSinkError {
    /// The fixed count or byte budget is unavailable.
    #[error("execution market update capacity is saturated")]
    Saturated,
    /// The execution market consumer is not running.
    #[error("execution market update consumer is closed")]
    Closed,
    /// Retained-size accounting overflowed or changed after admission.
    #[error("execution market sink retained-size accounting failed")]
    RetainedSize,
}

/// Replaceable backend contract reachable only with a dispatcher-created order.
pub trait ExecutionAdapter: Send + Sync + std::fmt::Debug + 'static {
    /// Returns whether every attempt is cooperatively bounded by [`ExecutionOperation`].
    ///
    /// The default is fail-closed isolation in a reaper-owned task. An adapter may return `true`
    /// only when it performs no mutation after operation cancellation or deadline expiry and does
    /// not detach transport work from the returned future.
    fn is_cooperative(&self) -> bool {
        false
    }

    /// Submits one non-replayable dispatcher command.
    ///
    /// [`ExecutionAdapterError::Rejected`] is an authoritative known rejection.
    /// [`ExecutionAdapterError::KnownFailure`] and
    /// [`ExecutionAdapterError::NotAttemptedBusy`] guarantee that no backend or transport side
    /// effect was attempted. Every ambiguous attempt must return
    /// [`ExecutionAdapterError::UncertainOutcome`] or
    /// [`ExecutionAdapterError::ReconciliationRequired`]. Implementations must enforce
    /// [`DispatchOrder::execution_price_bound`] on every fill and reject the order before a side
    /// effect when they cannot guarantee that ceiling.
    fn submit(
        &self,
        order: DispatchOrder,
    ) -> ExecutionAdapterFuture<'_, Result<ExecutionReceipt, ExecutionAdapterError>>;

    /// Requests cancellation for an already accepted order under the same error guarantees as
    /// [`ExecutionAdapter::submit`].
    fn cancel(
        &self,
        order: CancelOrder,
    ) -> ExecutionAdapterFuture<'_, Result<CancelReceipt, ExecutionAdapterError>>;

    /// Returns a bounded current order-state image for only the requested order identities under
    /// the same error guarantees as [`ExecutionAdapter::submit`].
    fn reconcile(
        &self,
        request: ReconcileOrders,
    ) -> ExecutionAdapterFuture<'_, Result<ExecutionState, ExecutionAdapterError>>;

    /// Records that dispatcher-side validation and account replacement completed for the exact
    /// reconciliation set. This command may advance bounded backend compaction fences only.
    fn acknowledge_reconciliation(
        &self,
        acknowledgement: ReconciliationAcknowledgement,
    ) -> ExecutionAdapterFuture<'_, Result<(), ExecutionAdapterError>>;

    /// Clears a durably recovered backend quarantine after dispatcher ownership is established.
    ///
    /// The default rejects recovery so adapters cannot silently opt into clearing a fail-closed
    /// state. A successful implementation must durably audit the transition before returning.
    fn recover_quarantined(
        &self,
        recovery: RecoverExecutionState,
    ) -> ExecutionAdapterFuture<'_, Result<(), ExecutionAdapterError>> {
        Box::pin(async move {
            let _ = recovery;
            Err(ExecutionAdapterError::KnownFailure)
        })
    }
}

/// Successful backend acceptance of one risk-dispatched order.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExecutionReceipt {
    order_id: OrderId,
    accepted_at: Timestamp,
}

impl ExecutionReceipt {
    /// Creates an immutable accepted-order receipt.
    pub const fn new(order_id: OrderId, accepted_at: Timestamp) -> Self {
        Self {
            order_id,
            accepted_at,
        }
    }

    /// Returns the internal order identity.
    pub const fn order_id(self) -> OrderId {
        self.order_id
    }

    /// Returns the adapter-supplied acceptance timestamp. The dispatcher causally bounds it
    /// between trusted pre-call and post-call observations before accepting the receipt.
    pub const fn accepted_at(self) -> Timestamp {
        self.accepted_at
    }
}

/// Result of a cancellation request.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CancelStatus {
    /// Cancellation was accepted but may still race a fill.
    Pending,
    /// The order reached a terminal canceled state without a fill.
    Canceled,
    /// Backend state is terminal but the terminal cause must be reconciled.
    AlreadyTerminal,
}

/// Immutable typed cancellation receipt.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CancelReceipt {
    order_id: OrderId,
    status: CancelStatus,
    observed_at: Timestamp,
    cumulative_filled: QuantityLots,
    average_fill_price: Option<PriceTicks>,
    maximum_fill_price: Option<PriceTicks>,
    cumulative_fees: Money,
}

impl CancelReceipt {
    /// Validates a cancellation observation with cumulative fill, authoritative maximum-price,
    /// and fee evidence.
    pub fn try_new(
        order_id: OrderId,
        status: CancelStatus,
        observed_at: Timestamp,
        cumulative_filled: QuantityLots,
        average_fill_price: Option<PriceTicks>,
        maximum_fill_price: Option<PriceTicks>,
        cumulative_fees: Money,
    ) -> Result<Self, ExecutionStateError> {
        let fill_shape_valid = match (
            cumulative_filled.get(),
            average_fill_price,
            maximum_fill_price,
        ) {
            (0, None, None) => true,
            (filled, Some(average), Some(maximum)) => {
                filled > 0 && average.get() > 0 && maximum.get() > 0 && maximum >= average
            }
            _ => false,
        };
        if !fill_shape_valid || cumulative_fees.amount().is_sign_negative() {
            return Err(ExecutionStateError::InvalidOrderState);
        }
        Ok(Self {
            order_id,
            status,
            observed_at,
            cumulative_filled,
            average_fill_price,
            maximum_fill_price,
            cumulative_fees,
        })
    }

    pub const fn order_id(self) -> OrderId {
        self.order_id
    }
    pub const fn status(self) -> CancelStatus {
        self.status
    }
    pub const fn observed_at(self) -> Timestamp {
        self.observed_at
    }
    pub const fn cumulative_filled(self) -> QuantityLots {
        self.cumulative_filled
    }
    pub const fn average_fill_price(self) -> Option<PriceTicks> {
        self.average_fill_price
    }
    pub const fn maximum_fill_price(self) -> Option<PriceTicks> {
        self.maximum_fill_price
    }
    pub const fn cumulative_fees(self) -> Money {
        self.cumulative_fees
    }
}

/// Closed execution lifecycle returned by bounded reconciliation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReconciledOrderStatus {
    Open,
    PartiallyFilled,
    Filled,
    Canceled,
    Rejected,
    Expired,
    Unknown,
}

/// One bounded backend order observation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReconciledOrder {
    order_id: OrderId,
    status: ReconciledOrderStatus,
    cumulative_filled: QuantityLots,
    average_fill_price: Option<PriceTicks>,
    maximum_fill_price: Option<PriceTicks>,
    cumulative_fees: Money,
}

impl ReconciledOrder {
    /// Validates cumulative fill, average-price, authoritative maximum-price, and fee consistency
    /// for one backend order observation.
    pub fn try_new(
        order_id: OrderId,
        status: ReconciledOrderStatus,
        cumulative_filled: QuantityLots,
        average_fill_price: Option<PriceTicks>,
        maximum_fill_price: Option<PriceTicks>,
        cumulative_fees: Money,
    ) -> Result<Self, ExecutionStateError> {
        let has_fill = cumulative_filled.get() != 0;
        let fill_shape_valid = match (
            cumulative_filled.get(),
            average_fill_price,
            maximum_fill_price,
        ) {
            (0, None, None) => true,
            (filled, Some(average), Some(maximum)) => {
                filled > 0 && average.get() > 0 && maximum.get() > 0 && maximum >= average
            }
            _ => false,
        };
        if !fill_shape_valid
            || matches!(
                status,
                ReconciledOrderStatus::Open
                    | ReconciledOrderStatus::PartiallyFilled
                    | ReconciledOrderStatus::Filled
            ) != matches!(
                (status, has_fill),
                (ReconciledOrderStatus::Open, false)
                    | (ReconciledOrderStatus::PartiallyFilled, true)
                    | (ReconciledOrderStatus::Filled, true)
            )
            || cumulative_fees.amount().is_sign_negative()
        {
            return Err(ExecutionStateError::InvalidOrderState);
        }
        Ok(Self {
            order_id,
            status,
            cumulative_filled,
            average_fill_price,
            maximum_fill_price,
            cumulative_fees,
        })
    }
    pub const fn order_id(self) -> OrderId {
        self.order_id
    }
    pub const fn status(self) -> ReconciledOrderStatus {
        self.status
    }
    pub const fn cumulative_filled(self) -> QuantityLots {
        self.cumulative_filled
    }
    pub const fn average_fill_price(self) -> Option<PriceTicks> {
        self.average_fill_price
    }
    pub const fn maximum_fill_price(self) -> Option<PriceTicks> {
        self.maximum_fill_price
    }
    pub const fn cumulative_fees(self) -> Money {
        self.cumulative_fees
    }
}

/// Bounded account/order reconciliation state.
#[derive(Debug, Eq, PartialEq)]
pub struct ExecutionState {
    observed_at: Timestamp,
    orders: Box<[ReconciledOrder]>,
    accounts: Box<[ReconciledAccountState]>,
    source_binding: Option<ExecutionStateSourceBinding>,
    reconciliation_required: bool,
}

impl ExecutionState {
    /// Validates and owns a bounded reconciliation image.
    pub fn try_new(
        observed_at: Timestamp,
        orders: Vec<ReconciledOrder>,
        reconciliation_required: bool,
    ) -> Result<Self, ExecutionStateError> {
        if orders.len() > MAX_RECONCILED_ORDERS {
            return Err(ExecutionStateError::TooManyOrders);
        }
        let mut seen = std::collections::HashSet::new();
        seen.try_reserve(orders.len())
            .map_err(|_| ExecutionStateError::Allocation)?;
        if orders.iter().any(|order| !seen.insert(order.order_id())) {
            return Err(ExecutionStateError::DuplicateOrder);
        }
        Ok(Self {
            observed_at,
            orders: orders.into_boxed_slice(),
            accounts: Box::new([]),
            source_binding: None,
            reconciliation_required,
        })
    }

    /// Validates one complete backend-bound account replacement image.
    pub fn try_new_complete(
        observed_at: Timestamp,
        orders: Vec<ReconciledOrder>,
        accounts: Vec<ReconciledAccountState>,
        source_binding: ExecutionStateSourceBinding,
        reconciliation_required: bool,
    ) -> Result<Self, ExecutionStateError> {
        let mut state = Self::try_new(observed_at, orders, reconciliation_required)?;
        if accounts.len() > MAX_RECONCILED_ACCOUNTS {
            return Err(ExecutionStateError::TooManyAccounts);
        }
        let mut seen = std::collections::HashSet::new();
        seen.try_reserve(accounts.len())
            .map_err(|_| ExecutionStateError::Allocation)?;
        if accounts
            .iter()
            .any(|account| !seen.insert(account.account_id()))
        {
            return Err(ExecutionStateError::DuplicateAccount);
        }
        state.accounts = accounts.into_boxed_slice();
        state.source_binding = Some(source_binding);
        Ok(state)
    }

    pub const fn observed_at(&self) -> Timestamp {
        self.observed_at
    }
    pub const fn orders(&self) -> &[ReconciledOrder] {
        &self.orders
    }
    pub const fn accounts(&self) -> &[ReconciledAccountState] {
        &self.accounts
    }
    pub const fn source_binding(&self) -> Option<ExecutionStateSourceBinding> {
        self.source_binding
    }
    pub const fn reconciliation_required(&self) -> bool {
        self.reconciliation_required
    }

    pub(crate) fn retained_heap_bytes(&self) -> Option<usize> {
        let mut retained = std::mem::size_of_val(self.orders.as_ref())
            .checked_add(std::mem::size_of_val(self.accounts.as_ref()))?;
        for account in &self.accounts {
            retained = retained
                .checked_add(std::mem::size_of_val(account.positions()))?
                .checked_add(std::mem::size_of_val(account.position_cost_basis()))?;
        }
        Some(retained)
    }
}

/// Reconciliation image construction failure.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum ExecutionStateError {
    #[error("execution reconciliation exceeded the hard order bound")]
    TooManyOrders,
    #[error("execution reconciliation exceeded the hard account bound")]
    TooManyAccounts,
    #[error("execution reconciliation contains a duplicate order identity")]
    DuplicateOrder,
    #[error("execution reconciliation contains a duplicate account identity")]
    DuplicateAccount,
    #[error("execution reconciliation contains inconsistent fill, price, status, or fee data")]
    InvalidOrderState,
    #[error("execution reconciliation bounded allocation failed")]
    Allocation,
}

/// Execution-backend failure classification used by the one-use dispatcher.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum ExecutionAdapterError {
    #[error("execution backend rejected the command")]
    Rejected,
    /// The adapter guarantees no backend or transport side effect was attempted.
    #[error("execution backend failed before attempting the command")]
    KnownFailure,
    #[error("execution outcome is uncertain and requires reconciliation")]
    UncertainOutcome,
    #[error("execution adapter requires reconciliation")]
    ReconciliationRequired,
    /// The adapter refused the command before any backend or transport side effect was attempted.
    #[error("execution adapter was busy before attempting the command")]
    NotAttemptedBusy,
}
