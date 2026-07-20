//! Deterministic pre-authority risk assessment and atomic account reservation.

use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};

use market_squawk_domain::{
    DataQuality, InstrumentExecutionTerms, OrderSide, OrderType, PriceTicks, Timestamp,
};
use thiserror::Error;

use crate::clock::system_now;
use crate::{
    AccountReservationError, AccountRiskCoordinator, AccountRiskReservation, AccountRiskViolation,
    OrderIntent, RiskLimits,
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
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum RiskRejectionCode {
    /// Trusted decision clock failed.
    ClockFailure,
    /// Wall time regressed within this service instance.
    ClockRollback,
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

/// Deterministic risk policy owner with authoritative account coordination and trusted time.
#[derive(Debug)]
pub struct RiskService {
    accounts: Arc<AccountRiskCoordinator>,
    limits: RiskLimits,
    last_wall_nanos: AtomicI64,
}

impl RiskService {
    /// Creates a risk service over an authoritative account coordinator.
    pub fn new(accounts: Arc<AccountRiskCoordinator>, limits: RiskLimits) -> Self {
        Self {
            accounts,
            limits,
            last_wall_nanos: AtomicI64::new(i64::MIN),
        }
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
        let wall = now.wall.unix_nanos();
        let previous = self.last_wall_nanos.fetch_max(wall, Ordering::AcqRel);
        if wall < previous {
            reasons.push(RiskRejectionCode::ClockRollback);
        }
        self.evaluate_market(intent, market, now.wall, &mut reasons);
        if let Err(rejection) =
            self.accounts
                .assess(intent, market.estimated_execution_price, &self.limits)
        {
            extend_account_reasons(&mut reasons, &rejection);
        }
        if !reasons.is_empty() {
            return PreAuthorityRiskOutcome::Rejected(RiskRejection::new(reasons));
        }

        match self
            .accounts
            .try_reserve(intent, market.estimated_execution_price, &self.limits)
        {
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
        if market.quality != DataQuality::DirectVerified
            || intent.required_quality() != DataQuality::DirectVerified
        {
            reasons.push(RiskRejectionCode::SourceQuality);
        }
        if !market.source_eligible {
            reasons.push(RiskRejectionCode::SourceIneligible);
        }
        if now >= market.valid_until {
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
        if now >= intent.expires_at() {
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

fn deviation_exceeds(reference: PriceTicks, candidate: PriceTicks, maximum_bps: i32) -> bool {
    let reference = i128::from(reference.get());
    let candidate = i128::from(candidate.get());
    let difference = (candidate - reference).unsigned_abs();
    let reference = reference.unsigned_abs();
    difference * 10_000 > reference * maximum_bps.unsigned_abs() as u128
}
