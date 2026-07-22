//! Deterministic pre-authority risk assessment and atomic account reservation.

use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::Duration;

use market_squawk_domain::{
    ApprovalId, DataQuality, InstrumentExecutionTerms, OrderSide, OrderType, PriceTicks, Timestamp,
};
use thiserror::Error;

use market_squawk_live::{CurrentAuthorityGate, LiveExecutionCapability};
use serde::Serialize;

use crate::approval::approved_order_from_risk;
use crate::audit::{ExecutionAuditContext, ExecutionAuditEvidence, ExecutionAuditPermit};
use crate::clock::{monotonic_deadline, system_now};
use crate::{
    AccountReservationError, AccountRiskCoordinator, AccountRiskReservation, AccountRiskViolation,
    ApprovedOrder, ExecutionAuditEvent, ExecutionAuditKind, ExecutionAuditWriter,
    ExecutionMarketReference, ExecutionPriceBound, OrderIntent, RiskLimits, RiskPolicyIdentity,
};

/// Structurally validated but authority-free market input for pre-dispatch risk.
///
/// This value is deliberately not live execution authority. Task 11 binds it to the actor's
/// single-use current capability before any approval or adapter submission can exist.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MarketRiskInput {
    execution_terms: InstrumentExecutionTerms,
    quality: DataQuality,
    source_eligible: bool,
    instrument_trading: bool,
    observed_at: Timestamp,
    valid_until: Timestamp,
    reference_price: PriceTicks,
    estimated_execution_price: PriceTicks,
}

impl MarketRiskInput {
    /// Constructs bounded authority-free market risk input.
    ///
    /// # Errors
    ///
    /// Rejects a freshness deadline that is not strictly after the observation.
    #[allow(
        clippy::too_many_arguments,
        reason = "each independent market-risk invariant remains explicit at the boundary"
    )]
    pub fn try_new(
        execution_terms: InstrumentExecutionTerms,
        quality: DataQuality,
        source_eligible: bool,
        instrument_trading: bool,
        observed_at: Timestamp,
        valid_until: Timestamp,
        reference_price: PriceTicks,
        estimated_execution_price: PriceTicks,
    ) -> Result<Self, MarketRiskInputError> {
        if valid_until <= observed_at {
            return Err(MarketRiskInputError::InvalidFreshnessWindow);
        }
        Ok(Self {
            execution_terms,
            quality,
            source_eligible,
            instrument_trading,
            observed_at,
            valid_until,
            reference_price,
            estimated_execution_price,
        })
    }

    /// Returns the immutable instrument terms bound to the market observation.
    pub const fn execution_terms(self) -> InstrumentExecutionTerms {
        self.execution_terms
    }

    /// Returns the observation's evidence quality.
    pub const fn quality(self) -> DataQuality {
        self.quality
    }

    /// Returns whether source authorization, coverage, and health are currently eligible.
    pub const fn source_eligible(self) -> bool {
        self.source_eligible
    }

    /// Returns whether the instrument and venue are currently trading.
    pub const fn instrument_trading(self) -> bool {
        self.instrument_trading
    }

    /// Returns the trusted observation time supplied by the live boundary.
    pub const fn observed_at(self) -> Timestamp {
        self.observed_at
    }

    /// Returns the exclusive freshness deadline.
    pub const fn valid_until(self) -> Timestamp {
        self.valid_until
    }

    /// Returns the side-independent comparison price.
    pub const fn reference_price(self) -> PriceTicks {
        self.reference_price
    }

    /// Returns the side-aware estimated execution price.
    pub const fn estimated_execution_price(self) -> PriceTicks {
        self.estimated_execution_price
    }
}

/// Structural market-risk input failure.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum MarketRiskInputError {
    /// Freshness must remain valid for at least one nanosecond after observation.
    #[error("market freshness deadline must be later than observation time")]
    InvalidFreshnessWindow,
}

/// Stable complete pre-authority risk rejection reason.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RiskRejectionCode {
    /// Trusted decision clock failed.
    ClockFailure,
    /// Wall time regressed within this service instance.
    ClockRollback,
    /// The actor-owned live capability was stale, revoked, expired, or transplanted.
    Authority,
    /// A supposedly non-nil order identity could not form its one-use approval identity.
    ApprovalIdentity,
    /// Mandatory bounded audit capacity was unavailable before account mutation.
    AuditUnavailable,
    /// The exact risk policy deadline passed.
    PolicyExpired,
    /// The committed canonical book cannot supply an executable side price.
    MarketDepthUnavailable,
    /// Market data is not direct and verified.
    SourceQuality,
    /// Source authorization, coverage, or health is ineligible.
    SourceIneligible,
    /// The exclusive source freshness deadline was reached.
    SourceStale,
    /// Market observation time is later than decision time.
    MarketTimestampInFuture,
    /// Market state predates the intent signal.
    MarketPredatesSignal,
    /// Venue or instrument trading state is disabled.
    InstrumentNotTrading,
    /// Market terms differ from the intent's exact revision-bound terms.
    InstrumentDefinitionMismatch,
    /// Intent expiration was reached.
    IntentExpired,
    /// A zero reference cannot support a relative price bound.
    InvalidReferencePrice,
    /// Estimated execution violates the order's explicit limit.
    OrderPriceLimit,
    /// The current reference has not triggered a stop order.
    StopNotTriggered,
    /// Estimated adverse price movement exceeds the intent-selected bound.
    IntentSlippageLimit,
    /// Estimated adverse price movement exceeds the policy slippage bound.
    PolicySlippageLimit,
    /// Estimated price deviation exceeds the independent market-price bound.
    PriceDeviationLimit,
    /// Authoritative account state produced a typed violation.
    Account(AccountRiskViolation),
    /// Authoritative portfolio state or its execution binding is invalid.
    Portfolio(crate::PortfolioReadError),
}

/// Nonempty stable ordered risk rejection.
#[derive(Debug, Eq, PartialEq)]
pub struct RiskRejection {
    reasons: Box<[RiskRejectionCode]>,
}

impl RiskRejection {
    /// Returns all applicable reasons in stable order.
    pub const fn reasons(&self) -> &[RiskRejectionCode] {
        &self.reasons
    }

    fn new(mut reasons: Vec<RiskRejectionCode>) -> Self {
        reasons.sort_unstable();
        reasons.dedup();
        debug_assert!(!reasons.is_empty());
        Self {
            reasons: reasons.into_boxed_slice(),
        }
    }
}

/// Authority-free risk outcome.
///
/// A reservation protects account capacity but cannot be converted into an approved or dispatchable
/// order. No such public types or conversion exist in this stage.
#[derive(Debug)]
pub enum PreAuthorityRiskOutcome {
    /// One or more checks failed without retaining a new account reservation.
    Rejected(RiskRejection),
    /// Every current check passed and account capacity was atomically reserved.
    Reserved(AccountRiskReservation),
}

/// Full current-authority risk result. Only the approved variant can enter dispatch.
#[allow(
    clippy::large_enum_variant,
    reason = "approval keeps bounded depth and one-use authority inline to avoid a live-path allocation"
)]
#[derive(Debug)]
pub enum RiskOutcome {
    /// One or more checks failed without retaining a new account reservation.
    Rejected(RiskRejection),
    /// Current authority and account capacity were atomically bound into one opaque approval.
    Approved(ApprovedOrder),
}

/// Startup-fixed current risk policy chronology.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RiskServiceConfig {
    /// Fixed policy and ruleset identity retained through approval, dispatch, and audit.
    pub policy: RiskPolicyIdentity,
    /// Inclusive risk-policy deadline.
    pub policy_valid_until: Timestamp,
    /// Maximum additional wall/monotonic lifetime of one approval.
    pub maximum_approval_lifetime: Duration,
}

/// Risk service construction failure.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum RiskServiceError {
    /// Approval lifetime must make bounded positive progress.
    #[error("maximum approval lifetime must be positive")]
    ZeroApprovalLifetime,
    /// Complete fixed retained-size accounting overflowed.
    #[error("risk service retained-size calculation overflowed")]
    RetainedSizeOverflow,
}

/// Deterministic risk policy owner with authoritative account coordination and trusted time.
#[derive(Debug)]
pub struct RiskService {
    accounts: Arc<AccountRiskCoordinator>,
    portfolio: crate::PortfolioReadCapability,
    limits: RiskLimits,
    audit: ExecutionAuditWriter,
    config: RiskServiceConfig,
    last_wall_nanos: AtomicI64,
    retained_bytes: usize,
}

impl RiskService {
    /// Creates a risk service over authoritative account state and mandatory bounded audit.
    pub fn try_new(
        accounts: Arc<AccountRiskCoordinator>,
        portfolio: crate::PortfolioReadCapability,
        limits: RiskLimits,
        audit: ExecutionAuditWriter,
        config: RiskServiceConfig,
    ) -> Result<Self, RiskServiceError> {
        if config.maximum_approval_lifetime.is_zero() {
            return Err(RiskServiceError::ZeroApprovalLifetime);
        }
        let retained_bytes = Self::retained_bytes_for_limits(&limits)?;
        Ok(Self {
            accounts,
            portfolio,
            limits,
            audit,
            config,
            last_wall_nanos: AtomicI64::new(i64::MIN),
            retained_bytes,
        })
    }

    pub(crate) const fn retained_bytes(&self) -> usize {
        // The coordinator is an independently composed shared owner charged once by the
        // application memory model. A route hook retains only this Arc handle.
        self.retained_bytes
    }

    pub(crate) fn audit_writer(&self) -> ExecutionAuditWriter {
        self.audit.clone()
    }

    /// Returns the exact checked risk graph charge used before runtime ownership transfer.
    pub fn retained_bytes_for_limits(limits: &RiskLimits) -> Result<usize, RiskServiceError> {
        let limits_bytes = limits
            .checked_retained_byte_ceiling()
            .map_err(|_| RiskServiceError::RetainedSizeOverflow)?;
        let limits_heap_bytes = limits_bytes
            .checked_sub(std::mem::size_of::<RiskLimits>())
            .ok_or(RiskServiceError::RetainedSizeOverflow)?;
        std::mem::size_of::<Self>()
            .checked_add(limits_heap_bytes)
            .ok_or(RiskServiceError::RetainedSizeOverflow)
    }

    /// Consumes actor-owned live authority exactly once and approves only after mandatory audit
    /// admission and atomic account reservation.
    pub fn evaluate(
        &mut self,
        authority_gate: &mut CurrentAuthorityGate<'_>,
        capability: LiveExecutionCapability,
        intent: OrderIntent,
        market: &ExecutionMarketReference,
    ) -> RiskOutcome {
        let order_id = intent.order_id();
        let approval_id = match ApprovalId::try_from(order_id.as_uuid()) {
            Ok(approval_id) => approval_id,
            Err(_) => {
                return RiskOutcome::Rejected(RiskRejection::new(vec![
                    RiskRejectionCode::ApprovalIdentity,
                ]));
            }
        };
        let audit = match self.audit.try_reserve() {
            Ok(permit) => permit,
            Err(_) => {
                return RiskOutcome::Rejected(RiskRejection::new(vec![
                    RiskRejectionCode::AuditUnavailable,
                ]));
            }
        };
        let now = match system_now() {
            Ok(now) => now,
            Err(_) => {
                let context = ExecutionAuditContext::from_risk(
                    approval_id,
                    &intent,
                    *market,
                    ExecutionAuditEvidence::new(None, None, None),
                    self.config.policy,
                    intent.expires_at().min(self.config.policy_valid_until),
                );
                let reasons = [RiskRejectionCode::ClockFailure];
                commit_audit(
                    audit,
                    ExecutionAuditKind::RiskRejected,
                    context,
                    intent.signal_at(),
                    &reasons,
                );
                return RiskOutcome::Rejected(RiskRejection::new(reasons.to_vec()));
            }
        };
        let authority = match authority_gate.consume(capability) {
            Ok(authority) => authority,
            Err(_) => {
                let reasons = [RiskRejectionCode::Authority];
                let context = ExecutionAuditContext::from_risk(
                    approval_id,
                    &intent,
                    *market,
                    ExecutionAuditEvidence::new(None, None, None),
                    self.config.policy,
                    intent.expires_at().min(self.config.policy_valid_until),
                );
                commit_audit(
                    audit,
                    ExecutionAuditKind::RiskRejected,
                    context,
                    now.wall,
                    &reasons,
                );
                return RiskOutcome::Rejected(RiskRejection::new(reasons.to_vec()));
            }
        };
        let mut reasons = Vec::new();
        let mut portfolio_binding = None;
        let mut portfolio_snapshot = None;
        let previous = self
            .last_wall_nanos
            .fetch_max(now.wall.unix_nanos(), Ordering::AcqRel);
        if now.wall.unix_nanos() < previous {
            reasons.push(RiskRejectionCode::ClockRollback);
        }
        if authority.validate_current().is_err() {
            reasons.push(RiskRejectionCode::Authority);
        }
        if now.wall > self.config.policy_valid_until {
            reasons.push(RiskRejectionCode::PolicyExpired);
        }
        let execution_price = market.execution_price(intent.side());
        self.evaluate_current_market(&intent, market, execution_price, now.wall, &mut reasons);
        let execution_price_bound = execution_price.and_then(|execution_price| {
            execution_price_bound(&intent, execution_price, &self.limits)
        });
        if execution_price.is_some() && execution_price_bound.is_none() {
            reasons.push(RiskRejectionCode::Account(
                AccountRiskViolation::ArithmeticOverflow,
            ));
        }
        match self.portfolio.bind_current(
            intent.account_id(),
            intent.execution_terms().instrument_id(),
            intent.side(),
            self.limits.currency(),
        ) {
            Ok((binding, snapshot)) => {
                portfolio_binding = Some(binding);
                portfolio_snapshot = Some(snapshot);
            }
            Err(error) => reasons.push(RiskRejectionCode::Portfolio(error)),
        }
        if let (Some(execution_price_bound), Some(snapshot)) =
            (execution_price_bound, portfolio_snapshot.as_ref())
            && let Err(rejection) = self.accounts.assess_for_portfolio(
                &intent,
                execution_price_bound.maximum_price(),
                &self.limits,
                snapshot,
            )
        {
            extend_account_reasons(&mut reasons, &rejection);
        }
        if !reasons.is_empty() {
            let context = ExecutionAuditContext::from_risk(
                approval_id,
                &intent,
                *market,
                ExecutionAuditEvidence::new(
                    Some(&authority),
                    execution_price_bound,
                    portfolio_binding.as_ref(),
                ),
                self.config.policy,
                intent
                    .expires_at()
                    .min(authority.valid_until())
                    .min(self.config.policy_valid_until),
            );
            commit_audit(
                audit,
                ExecutionAuditKind::RiskRejected,
                context,
                now.wall,
                &reasons,
            );
            return RiskOutcome::Rejected(RiskRejection::new(reasons));
        }
        let Some(execution_price_bound) = execution_price_bound else {
            let reasons = [RiskRejectionCode::MarketDepthUnavailable];
            let context = ExecutionAuditContext::from_risk(
                approval_id,
                &intent,
                *market,
                ExecutionAuditEvidence::new(Some(&authority), None, portfolio_binding.as_ref()),
                self.config.policy,
                intent
                    .expires_at()
                    .min(authority.valid_until())
                    .min(self.config.policy_valid_until),
            );
            commit_audit(
                audit,
                ExecutionAuditKind::RiskRejected,
                context,
                now.wall,
                &reasons,
            );
            return RiskOutcome::Rejected(RiskRejection::new(reasons.to_vec()));
        };
        let Some(portfolio_binding) = portfolio_binding else {
            let reasons = [RiskRejectionCode::Portfolio(
                crate::PortfolioReadError::ContentMismatch,
            )];
            let context = ExecutionAuditContext::from_risk(
                approval_id,
                &intent,
                *market,
                ExecutionAuditEvidence::new(Some(&authority), Some(execution_price_bound), None),
                self.config.policy,
                intent
                    .expires_at()
                    .min(authority.valid_until())
                    .min(self.config.policy_valid_until),
            );
            commit_audit(
                audit,
                ExecutionAuditKind::RiskRejected,
                context,
                now.wall,
                &reasons,
            );
            return RiskOutcome::Rejected(RiskRejection::new(reasons.to_vec()));
        };
        let Some(portfolio_snapshot) = portfolio_snapshot else {
            let reasons = [RiskRejectionCode::Portfolio(
                crate::PortfolioReadError::ContentMismatch,
            )];
            let context = ExecutionAuditContext::from_risk(
                approval_id,
                &intent,
                *market,
                ExecutionAuditEvidence::new(
                    Some(&authority),
                    Some(execution_price_bound),
                    Some(&portfolio_binding),
                ),
                self.config.policy,
                intent
                    .expires_at()
                    .min(authority.valid_until())
                    .min(self.config.policy_valid_until),
            );
            commit_audit(
                audit,
                ExecutionAuditKind::RiskRejected,
                context,
                now.wall,
                &reasons,
            );
            return RiskOutcome::Rejected(RiskRejection::new(reasons.to_vec()));
        };
        if let Err(error) = self.portfolio.recheck(&portfolio_binding) {
            let reasons = [RiskRejectionCode::Portfolio(error)];
            let context = ExecutionAuditContext::from_risk(
                approval_id,
                &intent,
                *market,
                ExecutionAuditEvidence::new(
                    Some(&authority),
                    Some(execution_price_bound),
                    Some(&portfolio_binding),
                ),
                self.config.policy,
                intent
                    .expires_at()
                    .min(authority.valid_until())
                    .min(self.config.policy_valid_until),
            );
            commit_audit(
                audit,
                ExecutionAuditKind::RiskRejected,
                context,
                now.wall,
                &reasons,
            );
            return RiskOutcome::Rejected(RiskRejection::new(reasons.to_vec()));
        }
        let reservation = match self.accounts.try_reserve_for_portfolio(
            &intent,
            execution_price_bound.maximum_price(),
            &self.limits,
            &portfolio_snapshot,
        ) {
            Ok(reservation) => reservation,
            Err(rejection) => {
                extend_account_reasons(&mut reasons, &rejection);
                let context = ExecutionAuditContext::from_risk(
                    approval_id,
                    &intent,
                    *market,
                    ExecutionAuditEvidence::new(
                        Some(&authority),
                        Some(execution_price_bound),
                        Some(&portfolio_binding),
                    ),
                    self.config.policy,
                    intent
                        .expires_at()
                        .min(authority.valid_until())
                        .min(self.config.policy_valid_until),
                );
                commit_audit(
                    audit,
                    ExecutionAuditKind::RiskRejected,
                    context,
                    now.wall,
                    &reasons,
                );
                return RiskOutcome::Rejected(RiskRejection::new(reasons));
            }
        };
        let valid_until = intent
            .expires_at()
            .min(authority.valid_until())
            .min(reservation.valid_until())
            .min(self.config.policy_valid_until);
        let remaining = valid_until
            .unix_nanos()
            .checked_sub(now.wall.unix_nanos())
            .unwrap_or(-1);
        let maximum =
            i64::try_from(self.config.maximum_approval_lifetime.as_nanos()).unwrap_or(i64::MAX);
        let monotonic_deadline = match monotonic_deadline(now, remaining.min(maximum)) {
            Ok(deadline) if remaining >= 0 => deadline,
            _ => {
                let reasons = [RiskRejectionCode::ClockFailure];
                let context = ExecutionAuditContext::from_risk(
                    approval_id,
                    &intent,
                    *market,
                    ExecutionAuditEvidence::new(
                        Some(&authority),
                        Some(execution_price_bound),
                        Some(&portfolio_binding),
                    ),
                    self.config.policy,
                    valid_until,
                );
                commit_audit(
                    audit,
                    ExecutionAuditKind::RiskRejected,
                    context,
                    now.wall,
                    &reasons,
                );
                return RiskOutcome::Rejected(RiskRejection::new(reasons.to_vec()));
            }
        };
        let context = ExecutionAuditContext::from_risk(
            approval_id,
            &intent,
            *market,
            ExecutionAuditEvidence::new(
                Some(&authority),
                Some(execution_price_bound),
                Some(&portfolio_binding),
            ),
            self.config.policy,
            valid_until,
        );
        commit_audit(
            audit,
            ExecutionAuditKind::RiskApproved,
            context,
            now.wall,
            &[],
        );
        RiskOutcome::Approved(approved_order_from_risk(
            approval_id,
            intent,
            *market,
            execution_price_bound,
            authority,
            reservation,
            self.portfolio.clone(),
            portfolio_binding,
            self.config.policy,
            valid_until,
            monotonic_deadline,
        ))
    }

    /// Runs every deterministic pre-authority check and atomically reserves only on success.
    ///
    /// This method accepts no caller time or account snapshot and cannot approve or dispatch an
    /// order. Task 11 consumes actor-owned live authority before wrapping a successful reservation
    /// in a private approval candidate.
    pub fn evaluate_pre_authority(
        &self,
        intent: &OrderIntent,
        market: &MarketRiskInput,
    ) -> PreAuthorityRiskOutcome {
        let now = match system_now() {
            Ok(now) => now,
            Err(_) => {
                return PreAuthorityRiskOutcome::Rejected(RiskRejection::new(vec![
                    RiskRejectionCode::ClockFailure,
                ]));
            }
        };
        let mut reasons = Vec::new();
        let mut portfolio_binding = None;
        let mut portfolio_snapshot = None;
        let wall = now.wall.unix_nanos();
        let previous = self.last_wall_nanos.fetch_max(wall, Ordering::AcqRel);
        if wall < previous {
            reasons.push(RiskRejectionCode::ClockRollback);
        }
        self.evaluate_market(intent, market, now.wall, &mut reasons);
        let execution_price_bound =
            execution_price_bound(intent, market.estimated_execution_price, &self.limits);
        if execution_price_bound.is_none() {
            reasons.push(RiskRejectionCode::Account(
                AccountRiskViolation::ArithmeticOverflow,
            ));
        }
        match self.portfolio.bind_current(
            intent.account_id(),
            intent.execution_terms().instrument_id(),
            intent.side(),
            self.limits.currency(),
        ) {
            Ok((binding, snapshot)) => {
                portfolio_binding = Some(binding);
                portfolio_snapshot = Some(snapshot);
            }
            Err(error) => reasons.push(RiskRejectionCode::Portfolio(error)),
        }
        if let (Some(execution_price_bound), Some(snapshot)) =
            (execution_price_bound, portfolio_snapshot.as_ref())
            && let Err(rejection) = self.accounts.assess_for_portfolio(
                intent,
                execution_price_bound.maximum_price(),
                &self.limits,
                snapshot,
            )
        {
            extend_account_reasons(&mut reasons, &rejection);
        }
        if !reasons.is_empty() {
            return PreAuthorityRiskOutcome::Rejected(RiskRejection::new(reasons));
        }
        let Some(execution_price_bound) = execution_price_bound else {
            return PreAuthorityRiskOutcome::Rejected(RiskRejection::new(vec![
                RiskRejectionCode::Account(AccountRiskViolation::ArithmeticOverflow),
            ]));
        };

        let (Some(portfolio_binding), Some(portfolio_snapshot)) =
            (portfolio_binding, portfolio_snapshot)
        else {
            return PreAuthorityRiskOutcome::Rejected(RiskRejection::new(vec![
                RiskRejectionCode::Portfolio(crate::PortfolioReadError::ContentMismatch),
            ]));
        };
        if let Err(error) = self.portfolio.recheck(&portfolio_binding) {
            return PreAuthorityRiskOutcome::Rejected(RiskRejection::new(vec![
                RiskRejectionCode::Portfolio(error),
            ]));
        }

        match self.accounts.try_reserve_for_portfolio(
            intent,
            execution_price_bound.maximum_price(),
            &self.limits,
            &portfolio_snapshot,
        ) {
            Ok(reservation) => PreAuthorityRiskOutcome::Reserved(reservation),
            Err(rejection) => {
                extend_account_reasons(&mut reasons, &rejection);
                PreAuthorityRiskOutcome::Rejected(RiskRejection::new(reasons))
            }
        }
    }

    fn evaluate_market(
        &self,
        intent: &OrderIntent,
        market: &MarketRiskInput,
        now: Timestamp,
        reasons: &mut Vec<RiskRejectionCode>,
    ) {
        if intent.maximum_slippage().get() > self.limits.maximum_slippage().get() {
            reasons.push(RiskRejectionCode::PolicySlippageLimit);
        }
        if market.quality != DataQuality::DirectVerified
            || intent.required_quality() != DataQuality::DirectVerified
        {
            reasons.push(RiskRejectionCode::SourceQuality);
        }
        if !market.source_eligible {
            reasons.push(RiskRejectionCode::SourceIneligible);
        }
        if market_freshness_expired(now, market.valid_until) {
            reasons.push(RiskRejectionCode::SourceStale);
        }
        if market.observed_at > now {
            reasons.push(RiskRejectionCode::MarketTimestampInFuture);
        }
        if market.observed_at < intent.signal_at() {
            reasons.push(RiskRejectionCode::MarketPredatesSignal);
        }
        if !market.instrument_trading {
            reasons.push(RiskRejectionCode::InstrumentNotTrading);
        }
        if market.execution_terms != intent.execution_terms() {
            reasons.push(RiskRejectionCode::InstrumentDefinitionMismatch);
        }
        if now > intent.expires_at() {
            reasons.push(RiskRejectionCode::IntentExpired);
        }
        if market.reference_price.get() == 0 {
            reasons.push(RiskRejectionCode::InvalidReferencePrice);
        } else {
            if deviation_exceeds(
                market.reference_price,
                market.estimated_execution_price,
                intent.maximum_slippage().get(),
            ) {
                reasons.push(RiskRejectionCode::IntentSlippageLimit);
            }
            if deviation_exceeds(
                market.reference_price,
                market.estimated_execution_price,
                self.limits.maximum_slippage().get(),
            ) {
                reasons.push(RiskRejectionCode::PolicySlippageLimit);
            }
            if deviation_exceeds(
                market.reference_price,
                market.estimated_execution_price,
                self.limits.maximum_price_deviation().get(),
            ) {
                reasons.push(RiskRejectionCode::PriceDeviationLimit);
            }
        }
        if violates_limit(intent, market.estimated_execution_price) {
            reasons.push(RiskRejectionCode::OrderPriceLimit);
        }
        if !stop_triggered(intent, market.reference_price) {
            reasons.push(RiskRejectionCode::StopNotTriggered);
        }
    }

    fn evaluate_current_market(
        &self,
        intent: &OrderIntent,
        market: &ExecutionMarketReference,
        execution_price: Option<PriceTicks>,
        now: Timestamp,
        reasons: &mut Vec<RiskRejectionCode>,
    ) {
        if intent.maximum_slippage().get() > self.limits.maximum_slippage().get() {
            reasons.push(RiskRejectionCode::PolicySlippageLimit);
        }
        if market.execution_terms() != intent.execution_terms() {
            reasons.push(RiskRejectionCode::InstrumentDefinitionMismatch);
        }
        if market.observed_at() > now {
            reasons.push(RiskRejectionCode::MarketTimestampInFuture);
        }
        if market.observed_at() < intent.signal_at() {
            reasons.push(RiskRejectionCode::MarketPredatesSignal);
        }
        if now > intent.expires_at() {
            reasons.push(RiskRejectionCode::IntentExpired);
        }
        let Some(execution_price) = execution_price else {
            reasons.push(RiskRejectionCode::MarketDepthUnavailable);
            return;
        };
        if violates_limit(intent, execution_price) {
            reasons.push(RiskRejectionCode::OrderPriceLimit);
        }
        if !stop_triggered(intent, execution_price) {
            reasons.push(RiskRejectionCode::StopNotTriggered);
        }
    }
}

fn extend_account_reasons(
    reasons: &mut Vec<RiskRejectionCode>,
    rejection: &AccountReservationError,
) {
    reasons.extend(
        rejection
            .reasons()
            .iter()
            .copied()
            .map(RiskRejectionCode::Account),
    );
}

fn commit_audit(
    permit: ExecutionAuditPermit,
    kind: ExecutionAuditKind,
    context: ExecutionAuditContext,
    observed_at: Timestamp,
    reasons: &[RiskRejectionCode],
) {
    let event = ExecutionAuditEvent::from_risk_context(kind, context, observed_at, reasons);
    permit.commit(event);
}

fn violates_limit(intent: &OrderIntent, execution_price: PriceTicks) -> bool {
    let Some(limit) = intent.limit_price() else {
        return false;
    };
    match intent.side() {
        OrderSide::Buy => execution_price > limit,
        OrderSide::Sell => execution_price < limit,
    }
}

fn stop_triggered(intent: &OrderIntent, reference_price: PriceTicks) -> bool {
    if !matches!(intent.order_type(), OrderType::Stop | OrderType::StopLimit) {
        return true;
    }
    let Some(stop) = intent.stop_price() else {
        return false;
    };
    match intent.side() {
        OrderSide::Buy => reference_price >= stop,
        OrderSide::Sell => reference_price <= stop,
    }
}

/// Derives the hard upper average execution-price ceiling used for account reservation.
///
/// Buy intent slippage and limit prices can tighten the policy ceiling. Sell limits remain price
/// floors, so the symmetric policy-deviation ceiling is retained to bound growing short exposure.
fn execution_price_bound(
    intent: &OrderIntent,
    execution_price: PriceTicks,
    limits: &RiskLimits,
) -> Option<ExecutionPriceBound> {
    if execution_price.get() <= 0
        || !(0..=10_000).contains(&intent.maximum_slippage().get())
        || !(0..=10_000).contains(&limits.maximum_price_deviation().get())
    {
        return None;
    }
    let policy_ceiling =
        checked_upper_price(execution_price, limits.maximum_price_deviation().get())?;
    let maximum_price = match intent.side() {
        OrderSide::Sell => policy_ceiling,
        OrderSide::Buy => {
            let intent_ceiling =
                checked_upper_price(execution_price, intent.maximum_slippage().get())?;
            let ceiling = policy_ceiling.min(intent_ceiling);
            intent
                .limit_price()
                .map_or(ceiling, |limit| ceiling.min(limit))
        }
    };
    ExecutionPriceBound::try_new(maximum_price).ok()
}

fn checked_upper_price(price: PriceTicks, basis_points: i32) -> Option<PriceTicks> {
    if price.get() <= 0 || !(0..=10_000).contains(&basis_points) {
        return None;
    }
    let factor = 10_000_i128.checked_add(i128::from(basis_points))?;
    let numerator = i128::from(price.get()).checked_mul(factor)?;
    let quotient = numerator / 10_000_i128;
    let remainder = numerator % 10_000_i128;
    let ceiling = if remainder == 0 {
        quotient
    } else {
        quotient.checked_add(1)?
    };
    Some(PriceTicks::new(i64::try_from(ceiling).ok()?))
}

fn deviation_exceeds(reference: PriceTicks, candidate: PriceTicks, maximum_bps: i32) -> bool {
    let reference = i128::from(reference.get());
    let candidate = i128::from(candidate.get());
    let difference = (candidate - reference).unsigned_abs();
    let reference = reference.unsigned_abs();
    difference * 10_000 > reference * maximum_bps.unsigned_abs() as u128
}

const fn market_freshness_expired(now: Timestamp, valid_until: Timestamp) -> bool {
    now.unix_nanos() >= valid_until.unix_nanos()
}

#[cfg(test)]
mod tests {
    use super::market_freshness_expired;
    use market_squawk_domain::Timestamp;

    #[test]
    fn market_freshness_deadline_is_exclusive() {
        let deadline = Timestamp::from_unix_nanos(100);
        assert!(!market_freshness_expired(
            Timestamp::from_unix_nanos(99),
            deadline,
        ));
        assert!(market_freshness_expired(deadline, deadline));
    }
}
