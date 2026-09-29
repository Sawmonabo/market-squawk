//! Versioned research-only execution assumptions and deterministic fill simulation.

mod forward;

use std::num::NonZeroU32;

use market_squawk_data::Sha256Digest;
use market_squawk_domain::{
    BasisPoints, FinancialError, InstrumentId, LotSize, Money, OrderId, OrderSide, OrderType,
    PriceTicks, QuantityLots, TimeInForce, Timestamp,
};
use market_squawk_execution::{OrderIntent, OrderIntentDigest};
use rand_chacha::ChaCha20Rng;
use rand_core::{RngCore as _, SeedableRng as _};
use rust_decimal::{Decimal, prelude::ToPrimitive as _};
use sha2::{Digest as _, Sha256};
use thiserror::Error;

/// Current complete semantic version of the research execution policy.
pub const RESEARCH_EXECUTION_POLICY_VERSION: u32 = 3;

/// Deterministic precedence applied when multiple eligible intents compete for one snapshot.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ResearchLiquidityPriority {
    /// Earlier signals win, with canonical order identity breaking exact-time ties.
    SignalTimeThenOrderId,
}

/// Untrusted research-fill policy input.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ResearchExecutionAssumptionsInput {
    /// Nonzero semantic version of the complete fill model.
    pub version: u32,
    /// Fee applied to exact filled notional.
    pub fee_basis_points: BasisPoints,
    /// Deterministic adverse slippage beyond half spread.
    pub slippage_basis_points: BasisPoints,
    /// Seeded additional adverse slippage sampled inclusively from zero to this bound.
    pub maximum_random_slippage_basis_points: BasisPoints,
    /// Maximum share of evidenced executable depth consumed by one order.
    pub maximum_participation_basis_points: BasisPoints,
    /// Deterministic precedence for the shared aggregate snapshot-depth budget.
    pub liquidity_priority: ResearchLiquidityPriority,
    /// Minimum event-time delay between signal and execution.
    pub latency_nanos: i64,
    /// Whether an order may terminate with less than its requested quantity filled.
    pub allow_partial_fills: bool,
    /// Explicit decimal scale and nearest-even rounding for fees.
    pub fee_decimal_scale: u32,
}

/// Validated, immutable, content-identified research fill semantics.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ResearchExecutionAssumptions {
    version: NonZeroU32,
    fee_basis_points: BasisPoints,
    slippage_basis_points: BasisPoints,
    maximum_random_slippage_basis_points: BasisPoints,
    maximum_participation_basis_points: BasisPoints,
    liquidity_priority: ResearchLiquidityPriority,
    latency_nanos: i64,
    allow_partial_fills: bool,
    fee_decimal_scale: u32,
    digest: Sha256Digest,
}

impl ResearchExecutionAssumptions {
    /// Validates bounded nonnegative execution parameters and computes their semantic digest.
    pub fn try_new(input: ResearchExecutionAssumptionsInput) -> Result<Self, ResearchFillError> {
        let version = NonZeroU32::new(input.version).ok_or(ResearchFillError::InvalidPolicy)?;
        let rates = [
            input.fee_basis_points,
            input.slippage_basis_points,
            input.maximum_random_slippage_basis_points,
            input.maximum_participation_basis_points,
        ];
        if version.get() != RESEARCH_EXECUTION_POLICY_VERSION
            || rates
                .into_iter()
                .any(|rate| !(0..=10_000).contains(&rate.get()))
            || input.maximum_participation_basis_points.get() == 0
            || input.latency_nanos <= 0
            || input.fee_decimal_scale > 28
        {
            return Err(ResearchFillError::InvalidPolicy);
        }
        let mut hash = Sha256::new();
        hash.update(b"market-squawk/research-execution-assumptions/v3");
        hash.update(version.get().to_be_bytes());
        hash.update(input.fee_basis_points.get().to_be_bytes());
        hash.update(input.slippage_basis_points.get().to_be_bytes());
        hash.update(
            input
                .maximum_random_slippage_basis_points
                .get()
                .to_be_bytes(),
        );
        hash.update(input.maximum_participation_basis_points.get().to_be_bytes());
        hash.update([match input.liquidity_priority {
            ResearchLiquidityPriority::SignalTimeThenOrderId => 1,
        }]);
        hash.update(input.latency_nanos.to_be_bytes());
        hash.update([u8::from(input.allow_partial_fills)]);
        hash.update(input.fee_decimal_scale.to_be_bytes());
        Ok(Self {
            version,
            fee_basis_points: input.fee_basis_points,
            slippage_basis_points: input.slippage_basis_points,
            maximum_random_slippage_basis_points: input.maximum_random_slippage_basis_points,
            maximum_participation_basis_points: input.maximum_participation_basis_points,
            liquidity_priority: input.liquidity_priority,
            latency_nanos: input.latency_nanos,
            allow_partial_fills: input.allow_partial_fills,
            fee_decimal_scale: input.fee_decimal_scale,
            digest: Sha256Digest::new(hash.finalize().into()),
        })
    }

    /// Returns the semantic policy version.
    #[must_use]
    pub const fn version(self) -> NonZeroU32 {
        self.version
    }

    /// Returns the complete fill-semantics identity.
    #[must_use]
    pub const fn digest(self) -> Sha256Digest {
        self.digest
    }

    /// Returns the complete deterministic research-fill policy for evidence presentation.
    #[must_use]
    pub const fn fee_basis_points(self) -> BasisPoints {
        self.fee_basis_points
    }

    /// Returns the deterministic adverse slippage beyond the observed half spread.
    #[must_use]
    pub const fn slippage_basis_points(self) -> BasisPoints {
        self.slippage_basis_points
    }

    /// Returns the seeded additional adverse-slippage ceiling.
    #[must_use]
    pub const fn maximum_random_slippage_basis_points(self) -> BasisPoints {
        self.maximum_random_slippage_basis_points
    }

    /// Returns the maximum observed executable-depth participation rate.
    #[must_use]
    pub const fn maximum_participation_basis_points(self) -> BasisPoints {
        self.maximum_participation_basis_points
    }

    /// Returns the deterministic shared-depth allocation rule.
    #[must_use]
    pub const fn liquidity_priority(self) -> ResearchLiquidityPriority {
        self.liquidity_priority
    }

    /// Returns the minimum event-time delay between a signal and execution.
    #[must_use]
    pub const fn latency_nanos(self) -> i64 {
        self.latency_nanos
    }

    /// Returns whether an order can complete below its requested quantity.
    #[must_use]
    pub const fn allow_partial_fills(self) -> bool {
        self.allow_partial_fills
    }

    /// Returns the exact fee decimal scale and rounding contract.
    #[must_use]
    pub const fn fee_decimal_scale(self) -> u32 {
        self.fee_decimal_scale
    }
}

/// One deterministic research fill carrying no broker or live-execution authority.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResearchFill {
    order_id: OrderId,
    intent_digest: OrderIntentDigest,
    instrument_id: InstrumentId,
    signal_at: Timestamp,
    executed_at: Timestamp,
    side: OrderSide,
    quantity: QuantityLots,
    price: PriceTicks,
    fee: Money,
    partial: bool,
    assumption_digest: Sha256Digest,
}

impl ResearchFill {
    /// Returns the strategy order identity.
    #[must_use]
    pub const fn order_id(&self) -> OrderId {
        self.order_id
    }

    /// Returns the filled canonical instrument.
    #[must_use]
    pub const fn instrument_id(&self) -> InstrumentId {
        self.instrument_id
    }

    /// Returns the original signal time.
    #[must_use]
    pub const fn signal_at(&self) -> Timestamp {
        self.signal_at
    }

    /// Returns the later event-time execution timestamp.
    #[must_use]
    pub const fn executed_at(&self) -> Timestamp {
        self.executed_at
    }

    /// Returns the filled side.
    #[must_use]
    pub const fn side(&self) -> OrderSide {
        self.side
    }

    /// Returns filled instrument lots.
    #[must_use]
    pub const fn quantity(&self) -> QuantityLots {
        self.quantity
    }

    /// Returns the exact research execution price in instrument ticks.
    #[must_use]
    pub const fn price(&self) -> PriceTicks {
        self.price
    }

    /// Returns the exact rounded fee.
    #[must_use]
    pub const fn fee(&self) -> Money {
        self.fee
    }

    /// Returns whether less than the requested quantity filled.
    #[must_use]
    pub const fn partial(&self) -> bool {
        self.partial
    }

    /// Returns the complete assumption identity applied to this fill.
    #[must_use]
    pub const fn assumption_digest(&self) -> Sha256Digest {
        self.assumption_digest
    }

    pub(crate) const fn intent_digest(&self) -> OrderIntentDigest {
        self.intent_digest
    }
}

#[derive(Debug)]
pub(crate) struct ResearchFillSimulator {
    assumptions: ResearchExecutionAssumptions,
    random: ChaCha20Rng,
}

impl ResearchFillSimulator {
    pub(crate) fn new(assumptions: ResearchExecutionAssumptions, seed: u64) -> Self {
        Self {
            assumptions,
            random: ChaCha20Rng::seed_from_u64(seed),
        }
    }

    pub(crate) fn simulate(
        &mut self,
        intent: &OrderIntent,
        requested: QuantityLots,
        executed_at: Timestamp,
        mid_price: PriceTicks,
        spread: BasisPoints,
        available_capacity: QuantityLots,
    ) -> Result<Option<ResearchFill>, ResearchFillError> {
        self.simulate_reference(
            intent,
            requested,
            executed_at,
            Decimal::from(mid_price.get()),
            Decimal::ONE,
            spread,
            available_capacity,
        )
    }

    /// Uses the native monetary close; only the final adverse execution price is quantized.
    pub(crate) fn simulate_money(
        &mut self,
        intent: &OrderIntent,
        requested: QuantityLots,
        executed_at: Timestamp,
        reference: Money,
        spread: BasisPoints,
        available_capacity: QuantityLots,
    ) -> Result<Option<ResearchFill>, ResearchFillError> {
        if reference.currency() != intent.execution_terms().quote_currency()
            || reference.amount() <= Decimal::ZERO
        {
            return Err(ResearchFillError::Arithmetic);
        }
        self.simulate_reference(
            intent,
            requested,
            executed_at,
            reference.amount(),
            intent.execution_terms().price_tick().as_decimal(),
            spread,
            available_capacity,
        )
    }

    fn simulate_reference(
        &mut self,
        intent: &OrderIntent,
        requested: QuantityLots,
        executed_at: Timestamp,
        reference_amount: Decimal,
        tick_size: Decimal,
        spread: BasisPoints,
        available_capacity: QuantityLots,
    ) -> Result<Option<ResearchFill>, ResearchFillError> {
        if available_capacity.get() == 0 {
            return Ok(None);
        }
        if intent.time_in_force() == TimeInForce::FillOrKill
            && available_capacity.get() < requested.get()
        {
            return Ok(None);
        }
        let fill_lots = requested.get().min(available_capacity.get());
        if fill_lots < requested.get() && !self.assumptions.allow_partial_fills {
            return Ok(None);
        }
        let quantity = QuantityLots::new(fill_lots).map_err(|_| ResearchFillError::Arithmetic)?;
        let jitter_bound =
            u32::try_from(self.assumptions.maximum_random_slippage_basis_points.get())
                .map_err(|_| ResearchFillError::Arithmetic)?;
        let jitter = if jitter_bound == 0 {
            0
        } else {
            self.random.next_u32() % (jitter_bound + 1)
        };
        let half_spread = spread
            .get()
            .checked_add(1)
            .ok_or(ResearchFillError::Arithmetic)?
            / 2;
        let adverse = half_spread
            .checked_add(self.assumptions.slippage_basis_points.get())
            .and_then(|value| value.checked_add(i32::try_from(jitter).ok()?))
            .ok_or(ResearchFillError::Arithmetic)?;
        let price = adverse_price(reference_amount, tick_size, intent.side(), adverse)?;
        if price.get() <= 0 || !order_permits(intent, reference_amount, tick_size, price) {
            return Ok(None);
        }
        let terms = intent.execution_terms();
        let notional = self.assumptions.modeled_notional(terms, price, quantity)?;
        let fee = self.assumptions.modeled_fee(notional)?;
        Ok(Some(ResearchFill {
            order_id: intent.order_id(),
            intent_digest: intent.digest(),
            instrument_id: terms.instrument_id(),
            signal_at: intent.signal_at(),
            executed_at,
            side: intent.side(),
            quantity,
            price,
            fee,
            partial: fill_lots < requested.get(),
            assumption_digest: self.assumptions.digest,
        }))
    }

    pub(crate) fn observation_capacity(
        &self,
        depth: QuantityLots,
    ) -> Result<QuantityLots, ResearchFillError> {
        self.assumptions.modeled_participation_capacity(depth)
    }

    /// Declared daily research participation in realized traded volume. This calculation grants
    /// no observed quote-depth or instantaneous execution-liquidity claim.
    pub(crate) fn daily_bar_capacity(
        &self,
        traded_volume: Decimal,
        lot_size: LotSize,
    ) -> Result<QuantityLots, ResearchFillError> {
        if traded_volume < Decimal::ZERO {
            return Err(ResearchFillError::Arithmetic);
        }
        let capacity = traded_volume
            .checked_mul(Decimal::from(
                self.assumptions.maximum_participation_basis_points.get(),
            ))
            .and_then(|value| value.checked_div(Decimal::from(10_000_u32)))
            .and_then(|value| value.checked_div(lot_size.as_decimal()))
            .and_then(|value| value.floor().to_i64())
            .ok_or(ResearchFillError::Arithmetic)?;
        QuantityLots::new(capacity).map_err(|_| ResearchFillError::Arithmetic)
    }
}

fn participation_capacity(
    depth: QuantityLots,
    participation: BasisPoints,
) -> Result<QuantityLots, ResearchFillError> {
    let capacity = i128::from(depth.get())
        .checked_mul(i128::from(participation.get()))
        .ok_or(ResearchFillError::Arithmetic)?
        / 10_000;
    let capacity = i64::try_from(capacity).map_err(|_| ResearchFillError::Arithmetic)?;
    QuantityLots::new(capacity).map_err(|_| ResearchFillError::Arithmetic)
}

fn adverse_price(
    reference_amount: Decimal,
    tick_size: Decimal,
    side: OrderSide,
    adverse_basis_points: i32,
) -> Result<PriceTicks, ResearchFillError> {
    let factor = match side {
        OrderSide::Buy => 10_000_i32.checked_add(adverse_basis_points),
        OrderSide::Sell => 10_000_i32.checked_sub(adverse_basis_points),
    }
    .filter(|value| *value > 0)
    .ok_or(ResearchFillError::InvalidPolicy)?;
    let denominator = tick_size
        .checked_mul(Decimal::from(10_000))
        .ok_or(ResearchFillError::Arithmetic)?;
    let adjusted = reference_amount
        .checked_mul(Decimal::from(factor))
        .and_then(|value| value.checked_div(denominator))
        .ok_or(ResearchFillError::Arithmetic)?;
    let ticks = match side {
        OrderSide::Buy => adjusted.ceil(),
        OrderSide::Sell => adjusted.floor(),
    };
    Ok(PriceTicks::new(
        ticks.to_i64().ok_or(ResearchFillError::Arithmetic)?,
    ))
}

fn order_permits(
    intent: &OrderIntent,
    reference: Decimal,
    tick_size: Decimal,
    execution: PriceTicks,
) -> bool {
    let limit_ok = match (intent.side(), intent.limit_price()) {
        (OrderSide::Buy, Some(limit)) => execution <= limit,
        (OrderSide::Sell, Some(limit)) => execution >= limit,
        (_, None) => true,
    };
    let stop_ok = match (intent.side(), intent.stop_price()) {
        (OrderSide::Buy, Some(stop)) => Decimal::from(stop.get())
            .checked_mul(tick_size)
            .is_some_and(|stop| reference >= stop),
        (OrderSide::Sell, Some(stop)) => Decimal::from(stop.get())
            .checked_mul(tick_size)
            .is_some_and(|stop| reference <= stop),
        (_, None) => true,
    };
    match intent.order_type() {
        OrderType::Market => true,
        OrderType::Limit => limit_ok,
        OrderType::Stop => stop_ok,
        OrderType::StopLimit => stop_ok && limit_ok,
    }
}

/// Research fill-policy or exact arithmetic failure.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum ResearchFillError {
    /// Policy fields were zero, negative, or beyond fixed economic ceilings.
    #[error("research execution assumptions are invalid")]
    InvalidPolicy,
    /// Exact tick, lot, notional, or fee arithmetic failed.
    #[error("research fill arithmetic failed")]
    Arithmetic,
    /// Exact domain financial arithmetic failed.
    #[error("research fill financial arithmetic failed: {0}")]
    Financial(#[from] FinancialError),
}

// Private persistence wire: deserializing it cannot mint an execution intent or approval.
#[derive(serde::Serialize, serde::Deserialize)]
struct StoredResearchFill {
    order_id: OrderId,
    intent_digest: [u8; 32],
    instrument_id: InstrumentId,
    signal_at: Timestamp,
    executed_at: Timestamp,
    side: OrderSide,
    quantity: QuantityLots,
    price: PriceTicks,
    fee: Money,
    partial: bool,
    assumption_digest: [u8; 32],
}
impl ResearchFill {
    pub(crate) fn storage_bytes(&self) -> Result<Vec<u8>, crate::BacktestError> {
        serde_json::to_vec(&StoredResearchFill {
            order_id: self.order_id,
            intent_digest: self.intent_digest.as_bytes(),
            instrument_id: self.instrument_id,
            signal_at: self.signal_at,
            executed_at: self.executed_at,
            side: self.side,
            quantity: self.quantity,
            price: self.price,
            fee: self.fee,
            partial: self.partial,
            assumption_digest: self.assumption_digest.bytes(),
        })
        .map_err(|_| crate::BacktestError::InvalidDataset)
    }
    pub(crate) fn from_storage_bytes(bytes: &[u8]) -> Result<Self, crate::BacktestError> {
        let value: StoredResearchFill =
            serde_json::from_slice(bytes).map_err(|_| crate::BacktestError::InvalidDataset)?;
        Ok(Self {
            order_id: value.order_id,
            intent_digest: OrderIntentDigest::from_bytes(value.intent_digest),
            instrument_id: value.instrument_id,
            signal_at: value.signal_at,
            executed_at: value.executed_at,
            side: value.side,
            quantity: value.quantity,
            price: value.price,
            fee: value.fee,
            partial: value.partial,
            assumption_digest: Sha256Digest::new(value.assumption_digest),
        })
    }
}
