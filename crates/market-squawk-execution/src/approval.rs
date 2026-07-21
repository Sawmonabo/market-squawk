//! Opaque one-use approval state produced only by current live risk evaluation.

use std::time::Instant;

use market_squawk_domain::{
    ApprovalId, BookLevel, DataQuality, InstrumentExecutionTerms, OrderId, OrderSide, PriceTicks,
    RuleVersion, SourceIdentifier, Timestamp,
};
use market_squawk_live::{CommittedActionContext, ConsumedLiveAuthority};
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::audit::ExecutionAuditContext;
use crate::clock::{ClockReading, deadline_expired};
use crate::{AccountRiskReservation, OrderIntent};

/// Hard inline depth retained with a single execution market observation.
pub const MAX_EXECUTION_MARKET_LEVELS_PER_SIDE: usize = 64;

// The dispatcher charges a conservative closed ceiling instead of runtime `String::capacity()`
// guesses. The graph is bounded by the canonical identity/reason ceilings and fixed live evidence
// schema; shared lease allocations already exist before queue admission and are not queue-owned.
pub(crate) const APPROVAL_COMMAND_RETAINED_BYTE_CEILING: usize = 64 * 1024;

/// Positive hard ceiling for every individual fill price of one approved order.
///
/// Risk derives this bound with checked fixed-point arithmetic and reserves account resources at
/// its maximum price. Carrying it as a distinct type prevents side-aware slippage semantics from
/// accidentally treating a higher sell price as harmless when it increases absolute exposure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExecutionPriceBound {
    maximum_price: PriceTicks,
}

impl ExecutionPriceBound {
    /// Validates a positive upper execution-price ceiling.
    ///
    /// # Errors
    ///
    /// Returns [`ExecutionPriceBoundError::NonPositive`] for zero or negative prices.
    pub fn try_new(maximum_price: PriceTicks) -> Result<Self, ExecutionPriceBoundError> {
        if maximum_price.get() <= 0 {
            return Err(ExecutionPriceBoundError::NonPositive);
        }
        Ok(Self { maximum_price })
    }

    /// Returns the inclusive maximum individual fill price.
    pub const fn maximum_price(self) -> PriceTicks {
        self.maximum_price
    }

    /// Returns whether a positive observed fill price is within this inclusive ceiling.
    pub const fn permits(self, price: PriceTicks) -> bool {
        price.get() > 0 && price.get() <= self.maximum_price.get()
    }

    /// Returns the versioned execution-audit identity binding this exact ceiling to an intent.
    pub fn order_audit_digest(self, intent_digest: crate::OrderIntentDigest) -> [u8; 32] {
        let mut digest = Sha256::new();
        digest.update(b"market-squawk/approved-execution-audit-identity/v1\0");
        digest.update(intent_digest.as_bytes());
        digest.update(self.maximum_price.get().to_be_bytes());
        digest.finalize().into()
    }
}

/// Invalid upper execution-price bound.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum ExecutionPriceBoundError {
    /// Execution prices must be strictly positive.
    #[error("maximum execution price must be positive")]
    NonPositive,
}

/// Fixed risk-policy identity retained through risk, audit, and adapter dispatch.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct RiskPolicyIdentity {
    digest: [u8; 32],
    ruleset_version: RuleVersion,
}

impl RiskPolicyIdentity {
    /// Hashes a bounded policy identifier with its explicit one-based ruleset revision.
    pub fn new(policy_id: &SourceIdentifier, ruleset_version: RuleVersion) -> Self {
        let mut digest = Sha256::new();
        digest.update(b"market-squawk/risk-policy\0");
        digest.update(policy_id.as_str().as_bytes());
        digest.update(ruleset_version.get().to_be_bytes());
        Self {
            digest: digest.finalize().into(),
            ruleset_version,
        }
    }

    /// Returns the stable policy-and-version digest.
    pub const fn digest(self) -> [u8; 32] {
        self.digest
    }

    /// Returns the one-based ruleset revision.
    pub const fn ruleset_version(self) -> RuleVersion {
        self.ruleset_version
    }

    /// Restores a persisted, non-authoritative policy identity.
    pub fn try_from_recovery(
        digest: [u8; 32],
        ruleset_version: RuleVersion,
    ) -> Result<Self, RiskPolicyIdentityError> {
        if digest == [0; 32] {
            return Err(RiskPolicyIdentityError::ZeroDigest);
        }
        Ok(Self {
            digest,
            ruleset_version,
        })
    }
}

/// Invalid persisted risk-policy audit identity.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum RiskPolicyIdentityError {
    #[error("persisted risk-policy digest is zero")]
    ZeroDigest,
}

/// Opaque bounded market reference derived only from a real committed live actor context.
///
/// This value carries no execution authority. It is copyable because its complete representation
/// is fixed-size; private construction prevents caller-authored market state from entering approval.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExecutionMarketReference {
    execution_terms: InstrumentExecutionTerms,
    observed_at: Timestamp,
    source_timestamp: Option<Timestamp>,
    quality: DataQuality,
    bids: [Option<BookLevel>; MAX_EXECUTION_MARKET_LEVELS_PER_SIDE],
    asks: [Option<BookLevel>; MAX_EXECUTION_MARKET_LEVELS_PER_SIDE],
    bid_count: u8,
    ask_count: u8,
    depth_complete: bool,
}

impl ExecutionMarketReference {
    pub(crate) fn from_committed_context(context: &CommittedActionContext<'_>) -> Self {
        let market = context.market();
        let (bids, bid_count) = copy_levels(market.bids());
        let (asks, ask_count) = copy_levels(market.asks());
        Self {
            execution_terms: market.execution_terms(),
            observed_at: market.observed_at(),
            source_timestamp: context.source_timestamp(),
            quality: DataQuality::DirectVerified,
            bids,
            asks,
            bid_count,
            ask_count,
            depth_complete: market.bids().len() <= MAX_EXECUTION_MARKET_LEVELS_PER_SIDE
                && market.asks().len() <= MAX_EXECUTION_MARKET_LEVELS_PER_SIDE,
        }
    }

    /// Returns the immutable, revision-bound execution terms.
    pub const fn execution_terms(self) -> InstrumentExecutionTerms {
        self.execution_terms
    }

    /// Returns the trusted local receive time of the committed observation.
    pub const fn observed_at(self) -> Timestamp {
        self.observed_at
    }

    /// Returns the provider event time when the source supplied one.
    pub const fn source_timestamp(self) -> Option<Timestamp> {
        self.source_timestamp
    }

    /// Returns the actor-qualified quality, always `DirectVerified` for a constructible value.
    pub const fn quality(self) -> DataQuality {
        self.quality
    }

    /// Returns the number of retained bid levels.
    pub const fn bid_count(self) -> usize {
        self.bid_count as usize
    }

    /// Returns the number of retained ask levels.
    pub const fn ask_count(self) -> usize {
        self.ask_count as usize
    }

    /// Returns whether the committed depth fit completely in the fixed execution view.
    pub const fn depth_complete(self) -> bool {
        self.depth_complete
    }

    /// Returns one retained bid by best-to-worst index.
    pub fn bid(self, index: usize) -> Option<BookLevel> {
        if index >= self.bid_count() {
            return None;
        }
        self.bids.get(index).copied().flatten()
    }

    /// Returns one retained ask by best-to-worst index.
    pub fn ask(self, index: usize) -> Option<BookLevel> {
        if index >= self.ask_count() {
            return None;
        }
        self.asks.get(index).copied().flatten()
    }

    /// Returns the current best bid, if any.
    pub fn best_bid(self) -> Option<BookLevel> {
        self.bid(0)
    }

    /// Returns the current best ask, if any.
    pub fn best_ask(self) -> Option<BookLevel> {
        self.ask(0)
    }

    pub(crate) fn execution_price(self, side: OrderSide) -> Option<PriceTicks> {
        match side {
            OrderSide::Buy => self.best_ask().map(BookLevel::price),
            OrderSide::Sell => self.best_bid().map(BookLevel::price),
        }
    }
}

fn copy_levels(
    levels: &[BookLevel],
) -> (
    [Option<BookLevel>; MAX_EXECUTION_MARKET_LEVELS_PER_SIDE],
    u8,
) {
    let mut copied = [None; MAX_EXECUTION_MARKET_LEVELS_PER_SIDE];
    let count = levels.len().min(MAX_EXECUTION_MARKET_LEVELS_PER_SIDE);
    for (target, level) in copied.iter_mut().zip(levels).take(count) {
        *target = Some(*level);
    }
    (copied, u8::try_from(count).unwrap_or(u8::MAX))
}

/// Non-cloneable approval produced only after current authority and account reservation succeed.
#[derive(Debug)]
pub struct ApprovedOrder {
    approval_id: ApprovalId,
    intent: OrderIntent,
    market: ExecutionMarketReference,
    execution_price_bound: ExecutionPriceBound,
    authority: ConsumedLiveAuthority,
    reservation: AccountRiskReservation,
    policy: RiskPolicyIdentity,
    valid_until: Timestamp,
    monotonic_deadline: Instant,
}

impl ApprovedOrder {
    /// Returns the one-use risk approval identity.
    pub const fn approval_id(&self) -> ApprovalId {
        self.approval_id
    }

    /// Returns the stable internal order identity without exposing approval construction.
    pub const fn order_id(&self) -> OrderId {
        self.intent.order_id()
    }

    pub(crate) const fn account_id(&self) -> market_squawk_domain::AccountId {
        self.intent.account_id()
    }

    pub(crate) const fn intent_digest(&self) -> crate::OrderIntentDigest {
        self.intent.digest()
    }

    pub(crate) fn account_revision(&self) -> u64 {
        self.reservation.expected_account_revision()
    }

    pub(crate) const fn retained_byte_ceiling(&self) -> usize {
        APPROVAL_COMMAND_RETAINED_BYTE_CEILING
    }

    pub(crate) fn audit_context(&self) -> ExecutionAuditContext {
        ExecutionAuditContext::from_risk(
            self.approval_id,
            &self.intent,
            self.market,
            Some(&self.authority),
            Some(self.execution_price_bound),
            self.policy,
            self.valid_until,
        )
    }

    pub(crate) const fn execution_terms(&self) -> InstrumentExecutionTerms {
        self.intent.execution_terms()
    }

    pub(crate) const fn quantity(&self) -> market_squawk_domain::QuantityLots {
        self.intent.quantity()
    }

    pub(crate) const fn execution_price_bound(&self) -> ExecutionPriceBound {
        self.execution_price_bound
    }

    pub(crate) fn validate_current(
        &self,
        now: ClockReading,
    ) -> Result<(), ApprovalValidationError> {
        self.authority.validate_current()?;
        self.reservation.validate_at(now)?;
        if deadline_expired(now, self.valid_until, self.monotonic_deadline) {
            return Err(ApprovalValidationError::Expired);
        }
        Ok(())
    }

    pub(crate) fn into_parts(self) -> ApprovedOrderParts {
        let Self {
            approval_id,
            intent,
            market,
            execution_price_bound,
            authority,
            reservation,
            policy,
            valid_until,
            monotonic_deadline,
        } = self;
        ApprovedOrderParts {
            approval_id,
            intent,
            market,
            execution_price_bound,
            authority,
            reservation,
            policy,
            valid_until,
            monotonic_deadline,
        }
    }
}

#[allow(
    clippy::too_many_arguments,
    reason = "the one-use approval atomically owns every independent authority binding"
)]
pub(crate) fn approved_order_from_risk(
    approval_id: ApprovalId,
    intent: OrderIntent,
    market: ExecutionMarketReference,
    execution_price_bound: ExecutionPriceBound,
    authority: ConsumedLiveAuthority,
    reservation: AccountRiskReservation,
    policy: RiskPolicyIdentity,
    valid_until: Timestamp,
    monotonic_deadline: Instant,
) -> ApprovedOrder {
    ApprovedOrder {
        approval_id,
        intent,
        market,
        execution_price_bound,
        authority,
        reservation,
        policy,
        valid_until,
        monotonic_deadline,
    }
}

#[derive(Debug)]
pub(crate) struct ApprovedOrderParts {
    pub(crate) approval_id: ApprovalId,
    pub(crate) intent: OrderIntent,
    pub(crate) market: ExecutionMarketReference,
    pub(crate) execution_price_bound: ExecutionPriceBound,
    pub(crate) authority: ConsumedLiveAuthority,
    pub(crate) reservation: AccountRiskReservation,
    pub(crate) policy: RiskPolicyIdentity,
    pub(crate) valid_until: Timestamp,
    pub(crate) monotonic_deadline: Instant,
}

/// Final approval revalidation failure before any adapter call.
#[derive(Debug, Error)]
pub(crate) enum ApprovalValidationError {
    #[error("live execution authority is no longer current")]
    Authority(#[from] market_squawk_live::AuthorityError),
    #[error("account reservation is no longer current")]
    Reservation(#[from] crate::AccountReservationStateError),
    #[error("approved order expired before dispatch")]
    Expired,
}
