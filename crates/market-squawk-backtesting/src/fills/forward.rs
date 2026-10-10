//! Pure current-snapshot stress calculations under the same immutable research fill policy.
//! These methods calculate assumptions; they supply no market, cash, order or execution authority.

use market_squawk_domain::{
    InstrumentExecutionTerms, Money, OrderSide, PriceTicks, QuantityLots, RoundingPolicy,
};
use rust_decimal::Decimal;

use super::{
    ResearchExecutionAssumptions, ResearchFillError, adverse_price, participation_capacity,
};

impl ResearchExecutionAssumptions {
    /// Applies the declared adverse slippage and maximal jitter to an actual source side price.
    /// The side price already includes spread; no assumed half spread is added again.
    ///
    /// # Errors
    /// Rejects invalid currency/price, a nonpositive adverse sell price, or checked overflow.
    pub fn modeled_adverse_side_price(
        self,
        terms: InstrumentExecutionTerms,
        side: OrderSide,
        source_side_price: Money,
    ) -> Result<PriceTicks, ResearchFillError> {
        if source_side_price.currency() != terms.quote_currency()
            || source_side_price.amount() <= Decimal::ZERO
        {
            return Err(ResearchFillError::Arithmetic);
        }
        let adverse = self
            .slippage_basis_points
            .get()
            .checked_add(self.maximum_random_slippage_basis_points.get())
            .ok_or(ResearchFillError::Arithmetic)?;
        adverse_price(
            source_side_price.amount(),
            terms.price_tick().as_decimal(),
            side,
            adverse,
        )
    }

    /// Computes the original checked fill notional at exact ticks, lots and multiplier.
    ///
    /// # Errors
    /// Rejects invalid prices or checked financial overflow.
    pub fn modeled_notional(
        self,
        terms: InstrumentExecutionTerms,
        price: PriceTicks,
        quantity: QuantityLots,
    ) -> Result<Money, ResearchFillError> {
        if price.get() <= 0 {
            return Err(ResearchFillError::Arithmetic);
        }
        Ok(price
            .checked_mul_quantity(
                quantity,
                terms.price_tick(),
                terms.lot_size(),
                terms.quote_currency(),
            )?
            .checked_mul_decimal(terms.contract_multiplier())?)
    }

    /// Computes the same exact order-level modeled fee used by historical fills.
    ///
    /// # Errors
    /// Rejects negative notional or checked financial overflow. It is not an actual broker fee.
    pub fn modeled_fee(self, notional: Money) -> Result<Money, ResearchFillError> {
        if notional.amount() < Decimal::ZERO {
            return Err(ResearchFillError::Arithmetic);
        }
        Ok(notional.checked_basis_points(
            self.fee_basis_points,
            self.fee_decimal_scale,
            RoundingPolicy::NearestEven,
        )?)
    }

    /// Applies the declared participation fraction to actual supplied displayed depth in lots.
    ///
    /// # Errors
    /// Returns checked capacity arithmetic failures; never substitutes historical volume for depth.
    pub fn modeled_participation_capacity(
        self,
        depth: QuantityLots,
    ) -> Result<QuantityLots, ResearchFillError> {
        participation_capacity(depth, self.maximum_participation_basis_points)
    }
}
