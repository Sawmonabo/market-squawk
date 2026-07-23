//! Validated, exact risk policy limits.

use std::collections::BTreeSet;
use std::fmt;
use std::num::NonZeroU32;

use market_squawk_domain::{
    BasisPoints, Currency, InstrumentId, Money, OrderSide, PriceTicks, RoundingPolicy,
};
use rust_decimal::Decimal;
use serde::Serialize;
use thiserror::Error;

use crate::OrderIntent;

/// Deterministic reason that account state cannot reserve an intent.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AccountRiskViolation {
    /// Global risk policy is fail-closed.
    KillSwitch,
    /// The configured account does not exist.
    AccountNotFound,
    /// The account is not eligible to trade.
    AccountIneligible,
    /// An uncertain submission blocks the account until explicit reconciliation.
    ReconciliationRequired,
    /// The instrument is not in the policy allowlist.
    InstrumentIneligible,
    /// The account, limits, and instrument terms do not use one currency.
    CurrencyMismatch,
    /// Portfolio accounting and execution account economics disagree.
    PortfolioStateMismatch,
    /// Asset settlement is unsupported by this cash risk coordinator.
    UnsupportedSettlement,
    /// The intent expired before reservation.
    IntentExpired,
    /// Signal-to-expiration duration exceeds the configured replay-protection horizon.
    IntentLifetimeExceeded,
    /// The client-order identity was previously consumed.
    DuplicateClientOrder,
    /// The stable internal order identity was previously consumed.
    DuplicateOrder,
    /// The fixed idempotency registry is full.
    IdempotencyCapacity,
    /// The persisted idempotency revision cannot advance without wrapping.
    IdempotencyRevisionExhausted,
    /// The fixed reservation registry is full.
    ReservationCapacity,
    /// The rolling order-rate limit is reached.
    OrderRateLimit,
    /// The order exceeds the maximum single-order notional.
    OrderNotionalLimit,
    /// The projected absolute position exceeds the configured bound.
    PositionLimit,
    /// A non-shortable sell exceeds the available position.
    InsufficientPosition,
    /// A buy exceeds cash remaining after active reservations.
    InsufficientCash,
    /// Projected gross exposure exceeds its bound.
    ExposureLimit,
    /// Projected exposure exceeds maximum leverage.
    LeverageLimit,
    /// Current capital is below the required floor.
    CapitalLimit,
    /// Current realized loss exceeds its bound.
    LossLimit,
    /// Current peak-to-capital drawdown exceeds its bound.
    DrawdownLimit,
    /// Exact financial or integer arithmetic overflowed.
    ArithmeticOverflow,
    /// Another shard currently owns this account partition.
    AccountCoordinatorBusy,
    /// The account partition lock was poisoned and is fail-closed.
    AccountCoordinatorPoisoned,
    /// The platform clock cannot produce a valid bounded reading.
    ClockFailure,
}

impl fmt::Display for AccountRiskViolation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{self:?}")
    }
}

/// Maximum configured eligible-instrument universe retained by one risk policy.
pub const MAX_RISK_INSTRUMENTS: usize = 4_096;

/// Canonical upper bound shared by paper configuration, fee calculation, and central risk.
pub const MAX_PAPER_FEE_BASIS_POINTS: u64 = 10_000;

/// Complete untrusted input for constructing [`RiskLimits`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RiskLimitsInput {
    /// Accounting currency shared by every monetary limit.
    pub currency: Currency,
    /// Closed instrument allowlist.
    pub eligible_instruments: BTreeSet<InstrumentId>,
    /// Maximum absolute post-order position in lots.
    pub maximum_position_lots: i64,
    /// Maximum exact notional for one order.
    pub maximum_order_notional: Money,
    /// Maximum account gross exposure including active reservations.
    pub maximum_gross_exposure: Money,
    /// Maximum gross-exposure-to-capital ratio in basis points.
    pub maximum_leverage: BasisPoints,
    /// Minimum capital required to trade.
    pub minimum_capital: Money,
    /// Maximum retained realized loss.
    pub maximum_loss: Money,
    /// Maximum peak-to-current-capital drawdown.
    pub maximum_drawdown: Money,
    /// Worst-case fee reserve applied to new orders.
    pub maximum_fee: BasisPoints,
    /// Maximum reference-to-market deviation accepted by policy.
    pub maximum_price_deviation: BasisPoints,
    /// Maximum policy slippage even when an intent asks for more.
    pub maximum_slippage: BasisPoints,
    /// Maximum accepted reservations in one rolling window.
    pub maximum_orders_per_window: NonZeroU32,
    /// Positive rolling order-rate window in nanoseconds.
    pub order_rate_window_nanos: i64,
    /// Positive maximum reservation life in nanoseconds.
    pub reservation_ttl_nanos: i64,
    /// Whether positions may cross below zero.
    pub allow_short: bool,
    /// Immediate fail-closed policy switch.
    pub kill_switch: bool,
}

/// Immutable validated account risk policy.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RiskLimits {
    input: RiskLimitsInput,
}

impl RiskLimits {
    /// Validates currency, sign, bound, and duration invariants.
    ///
    /// # Errors
    ///
    /// Returns a typed error before any policy can be used for a reservation.
    pub fn try_new(input: RiskLimitsInput) -> Result<Self, RiskLimitsError> {
        if input.eligible_instruments.is_empty() {
            return Err(RiskLimitsError::EmptyInstrumentUniverse);
        }
        if input.eligible_instruments.len() > MAX_RISK_INSTRUMENTS {
            return Err(RiskLimitsError::InstrumentUniverseTooLarge {
                max: MAX_RISK_INSTRUMENTS,
            });
        }
        if input.maximum_position_lots <= 0 {
            return Err(RiskLimitsError::NonPositivePositionLimit);
        }
        for money in [
            input.maximum_order_notional,
            input.maximum_gross_exposure,
            input.minimum_capital,
            input.maximum_loss,
            input.maximum_drawdown,
        ] {
            if money.currency() != input.currency {
                return Err(RiskLimitsError::CurrencyMismatch);
            }
            if money.amount().is_sign_negative() {
                return Err(RiskLimitsError::NegativeMoneyLimit);
            }
        }
        if input.maximum_order_notional.amount().is_zero()
            || input.maximum_gross_exposure.amount().is_zero()
            || input.minimum_capital.amount().is_zero()
        {
            return Err(RiskLimitsError::ZeroMandatoryMoneyLimit);
        }
        for basis_points in [
            input.maximum_leverage,
            input.maximum_fee,
            input.maximum_price_deviation,
            input.maximum_slippage,
        ] {
            if basis_points.get() < 0 {
                return Err(RiskLimitsError::NegativeBasisPoints);
            }
        }
        let maximum_fee = u64::try_from(input.maximum_fee.get())
            .map_err(|_| RiskLimitsError::NegativeBasisPoints)?;
        if maximum_fee > MAX_PAPER_FEE_BASIS_POINTS
            || input.maximum_price_deviation.get() > 10_000
            || input.maximum_slippage.get() > 10_000
            || input.maximum_leverage.get() > 1_000_000
        {
            return Err(RiskLimitsError::ExcessiveBasisPoints);
        }
        if input.order_rate_window_nanos <= 0 || input.reservation_ttl_nanos <= 0 {
            return Err(RiskLimitsError::NonPositiveDuration);
        }
        Ok(Self { input })
    }

    pub(crate) const fn currency(&self) -> Currency {
        self.input.currency
    }

    pub(crate) const fn maximum_position_lots(&self) -> i64 {
        self.input.maximum_position_lots
    }

    pub(crate) const fn maximum_order_notional(&self) -> Money {
        self.input.maximum_order_notional
    }

    pub(crate) const fn maximum_gross_exposure(&self) -> Money {
        self.input.maximum_gross_exposure
    }

    pub(crate) const fn maximum_leverage(&self) -> BasisPoints {
        self.input.maximum_leverage
    }

    pub(crate) const fn minimum_capital(&self) -> Money {
        self.input.minimum_capital
    }

    pub(crate) const fn maximum_loss(&self) -> Money {
        self.input.maximum_loss
    }

    pub(crate) const fn maximum_drawdown(&self) -> Money {
        self.input.maximum_drawdown
    }

    pub(crate) const fn maximum_fee(&self) -> BasisPoints {
        self.input.maximum_fee
    }

    pub(crate) const fn maximum_orders_per_window(&self) -> NonZeroU32 {
        self.input.maximum_orders_per_window
    }

    pub(crate) const fn order_rate_window_nanos(&self) -> i64 {
        self.input.order_rate_window_nanos
    }

    pub(crate) const fn reservation_ttl_nanos(&self) -> i64 {
        self.input.reservation_ttl_nanos
    }

    pub(crate) const fn allow_short(&self) -> bool {
        self.input.allow_short
    }

    pub(crate) const fn kill_switch(&self) -> bool {
        self.input.kill_switch
    }

    pub(crate) fn instrument_is_eligible(&self, instrument: InstrumentId) -> bool {
        self.input.eligible_instruments.contains(&instrument)
    }

    pub(crate) const fn maximum_price_deviation(&self) -> BasisPoints {
        self.input.maximum_price_deviation
    }

    pub(crate) const fn maximum_slippage(&self) -> BasisPoints {
        self.input.maximum_slippage
    }

    pub(crate) fn leverage_exceeded(&self, exposure: Money, capital: Money) -> bool {
        if capital.amount() <= Decimal::ZERO {
            return true;
        }
        let left = exposure.checked_mul_decimal(Decimal::from(10_000_u32));
        let right = capital.checked_mul_decimal(Decimal::from(self.maximum_leverage().get()));
        match (left, right) {
            (Ok(left), Ok(right)) => left.amount() > right.amount(),
            _ => true,
        }
    }

    /// Returns the complete conservative retained-byte ceiling using checked arithmetic.
    pub fn checked_retained_byte_ceiling(&self) -> Result<usize, RiskLimitsError> {
        std::mem::size_of::<Self>()
            .checked_add(
                self.input
                    .eligible_instruments
                    .len()
                    .checked_mul(
                        std::mem::size_of::<InstrumentId>()
                            .checked_mul(4)
                            .ok_or(RiskLimitsError::RetainedSizeOverflow)?,
                    )
                    .ok_or(RiskLimitsError::RetainedSizeOverflow)?,
            )
            .ok_or(RiskLimitsError::RetainedSizeOverflow)
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct ReservationCalculation {
    pub(crate) cash: Money,
    pub(crate) exposure: Money,
    pub(crate) signed_quantity: i64,
}

impl ReservationCalculation {
    pub(crate) fn for_intent(
        intent: &OrderIntent,
        price: PriceTicks,
        limits: &RiskLimits,
    ) -> Result<Self, ()> {
        let terms = intent.execution_terms();
        let base = price
            .checked_mul_quantity(
                intent.quantity(),
                terms.price_tick(),
                terms.lot_size(),
                terms.quote_currency(),
            )
            .map_err(|_| ())?;
        let exposure = base
            .checked_mul_decimal(terms.contract_multiplier())
            .map_err(|_| ())?;
        let exposure = Money::new(exposure.amount().abs(), terms.quote_currency());
        let fee = exposure
            .checked_basis_points(
                limits.maximum_fee(),
                Decimal::MAX_SCALE,
                RoundingPolicy::Ceiling,
            )
            .map_err(|_| ())?;
        let cash = match intent.side() {
            OrderSide::Buy => exposure.checked_add(fee).map_err(|_| ())?,
            OrderSide::Sell => Money::new(Decimal::ZERO, terms.quote_currency()),
        };
        let quantity = intent.quantity().get();
        let signed_quantity = match intent.side() {
            OrderSide::Buy => quantity,
            OrderSide::Sell => quantity.checked_neg().ok_or(())?,
        };
        Ok(Self {
            cash,
            exposure,
            signed_quantity,
        })
    }
}

/// Risk-limit configuration failure.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum RiskLimitsError {
    /// At least one instrument must be explicitly eligible.
    #[error("risk policy instrument universe must not be empty")]
    EmptyInstrumentUniverse,
    /// The instrument allowlist exceeded its startup bound.
    #[error("risk policy instrument universe exceeds {max} entries")]
    InstrumentUniverseTooLarge {
        /// Maximum accepted entries.
        max: usize,
    },
    /// Position limit must be positive.
    #[error("maximum position lots must be positive")]
    NonPositivePositionLimit,
    /// Every monetary limit must use the configured accounting currency.
    #[error("risk policy monetary limit currency mismatch")]
    CurrencyMismatch,
    /// Monetary limits cannot be negative.
    #[error("risk policy monetary limits cannot be negative")]
    NegativeMoneyLimit,
    /// Order notional, gross exposure, and minimum capital must be positive.
    #[error("mandatory risk policy monetary limits must be positive")]
    ZeroMandatoryMoneyLimit,
    /// Ratio and fee limits cannot be negative.
    #[error("risk policy basis-point limits cannot be negative")]
    NegativeBasisPoints,
    /// A ratio exceeded its closed safety ceiling.
    #[error("risk policy basis-point limit exceeds its safety ceiling")]
    ExcessiveBasisPoints,
    /// The complete retained-size ceiling cannot be represented on this target.
    #[error("risk policy retained-size calculation overflowed")]
    RetainedSizeOverflow,
    /// Rate and reservation durations must be positive.
    #[error("risk policy durations must be positive")]
    NonPositiveDuration,
}
