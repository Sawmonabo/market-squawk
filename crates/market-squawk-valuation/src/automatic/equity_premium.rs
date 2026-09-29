//! Historical equity-premium arithmetic; source admission remains application-owned.

use super::{AutomaticValuationError, discount};
use market_squawk_analytics::{
    Annualization, MissingValuePolicy, ReturnSeries, StatisticalDispersion, StatisticalInput,
    StatisticalScale, StatisticalUnit, VarianceConvention, volatility,
};
use rust_decimal::{Decimal, RoundingStrategy};
use std::num::NonZeroU32;

/// Fixed trailing complete calendar-year sample; eleven annual closing endpoints are required.
pub const EQUITY_PREMIUM_SAMPLE_YEARS: usize = 10;
/// Entire estimator convention; changing a policy requires a new identity and recomputation.
pub const EQUITY_PREMIUM_ESTIMATOR: &str =
    "spy-cash-retained-geometric-excess-hypothetical-10y-par-semiannual-flat-yield-10years/v1";

/// One hypothetical government bond holding return, never an observed security return.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ModeledGovernmentAnnualReturn {
    opening_yield_percent: Decimal,
    closing_yield_percent: Decimal,
    cash_coupons: Decimal,
    closing_price: Decimal,
    holding_return: Decimal,
}

impl ModeledGovernmentAnnualReturn {
    /// Starts a ten-year par bond at one, holds it for one financial year and retains its two
    /// semiannual coupons in cash. The closing ten-year yield prices the remaining eighteen
    /// semiannual coupons and principal: a disclosed flat-yield proxy for the nine-year bond.
    /// No observed nine-year yield, interpolation, coupon reinvestment or bond transaction exists.
    ///
    /// # Errors
    ///
    /// Rejects negative yields or checked arithmetic failure.
    pub fn calculate(
        opening_yield_percent: Decimal,
        closing_yield_percent: Decimal,
    ) -> Result<Self, AutomaticValuationError> {
        if opening_yield_percent < Decimal::ZERO || closing_yield_percent < Decimal::ZERO {
            return Err(AutomaticValuationError::InvalidContract);
        }
        let arithmetic = AutomaticValuationError::Arithmetic;
        let coupon = opening_yield_percent
            .checked_div(Decimal::from(200))
            .ok_or(arithmetic)?;
        let discount_base = closing_yield_percent
            .checked_div(Decimal::from(200))
            .and_then(|rate| Decimal::ONE.checked_add(rate))
            .ok_or(arithmetic)?;
        let mut closing_price = Decimal::ZERO;
        for period in 1..=18 {
            let amount = if period == 18 {
                coupon.checked_add(Decimal::ONE).ok_or(arithmetic)?
            } else {
                coupon
            };
            let (_, discounted) = discount(
                amount,
                discount_base,
                NonZeroU32::new(period).ok_or(AutomaticValuationError::InvalidContract)?,
            )?;
            closing_price = closing_price.checked_add(discounted).ok_or(arithmetic)?;
        }
        let cash_coupons = coupon.checked_mul(Decimal::TWO).ok_or(arithmetic)?;
        let holding_return = closing_price
            .checked_add(cash_coupons)
            .and_then(|wealth| wealth.checked_sub(Decimal::ONE))
            .ok_or(arithmetic)?;
        Ok(Self {
            opening_yield_percent: opening_yield_percent.normalize(),
            closing_yield_percent: closing_yield_percent.normalize(),
            cash_coupons: cash_coupons.normalize(),
            closing_price: closing_price.normalize(),
            holding_return: holding_return.normalize(),
        })
    }

    /// Actual source annual yields in percent, supplied in chronological order.
    pub const fn source_yields_percent(self) -> (Decimal, Decimal) {
        (self.opening_yield_percent, self.closing_yield_percent)
    }
    /// Two retained hypothetical coupons per unit of initial par.
    pub const fn cash_coupons(self) -> Decimal {
        self.cash_coupons
    }
    /// Modeled remaining bond value per unit of initial par.
    pub const fn closing_price(self) -> Decimal {
        self.closing_price
    }
    /// Modeled annual return as a decimal fraction.
    pub const fn holding_return(self) -> Decimal {
        self.holding_return
    }
}

/// Numeric estimate with explicit diagnostic uncertainty, not source or valuation authority.
#[derive(Clone, Debug, PartialEq)]
pub struct AnnualEquityPremiumArithmetic {
    geometric_premium: Decimal,
    arithmetic_premium: Decimal,
    geometric_equity_return: Decimal,
    geometric_government_return: Decimal,
    arithmetic_standard_error: StatisticalDispersion,
    geometric_delta_standard_error: StatisticalDispersion,
    government_returns: [ModeledGovernmentAnnualReturn; EQUITY_PREMIUM_SAMPLE_YEARS],
}

impl AnnualEquityPremiumArithmetic {
    /// Computes the difference of geometric annual returns, rather than compounding excess
    /// returns. Exact annual equity cash-retained returns and eleven matched source yields are
    /// required. The source producer must prove calendar years, currency, actions and coverage.
    /// Both legs reset all terminal wealth at annual boundaries under the declared model.
    /// Sampling standard errors assume independent years and omit model/revision uncertainty;
    /// they are diagnostics, never a calibrated future-return interval or confidence claim.
    ///
    /// # Errors
    ///
    /// Rejects nonpositive gross returns, invalid government inputs or nonfinite arithmetic.
    pub fn calculate(
        equity_returns: &[Decimal; EQUITY_PREMIUM_SAMPLE_YEARS],
        government_yields_percent: &[Decimal; EQUITY_PREMIUM_SAMPLE_YEARS + 1],
    ) -> Result<Self, AutomaticValuationError> {
        let arithmetic = AutomaticValuationError::Arithmetic;
        let mut government = Vec::new();
        government
            .try_reserve_exact(EQUITY_PREMIUM_SAMPLE_YEARS)
            .map_err(|_| arithmetic)?;
        let mut equity_logs = [0.0; EQUITY_PREMIUM_SAMPLE_YEARS];
        let mut government_logs = [0.0; EQUITY_PREMIUM_SAMPLE_YEARS];
        let mut excess = Vec::new();
        excess
            .try_reserve_exact(EQUITY_PREMIUM_SAMPLE_YEARS)
            .map_err(|_| arithmetic)?;
        let mut arithmetic_sum = Decimal::ZERO;
        for index in 0..EQUITY_PREMIUM_SAMPLE_YEARS {
            let modeled = ModeledGovernmentAnnualReturn::calculate(
                government_yields_percent[index],
                government_yields_percent[index + 1],
            )?;
            let equity = statistical_return(equity_returns[index])?;
            let bond = statistical_return(modeled.holding_return())?;
            if equity.value() <= -1.0 || bond.value() <= -1.0 {
                return Err(AutomaticValuationError::InvalidContract);
            }
            equity_logs[index] = equity.value().ln_1p();
            government_logs[index] = bond.value().ln_1p();
            let difference = equity_returns[index]
                .checked_sub(modeled.holding_return())
                .ok_or(arithmetic)?;
            arithmetic_sum = arithmetic_sum.checked_add(difference).ok_or(arithmetic)?;
            excess.push(statistical_return(difference)?);
            government.push(modeled);
        }
        let count = EQUITY_PREMIUM_SAMPLE_YEARS as f64;
        let equity_mean_log = equity_logs.iter().sum::<f64>() / count;
        let government_mean_log = government_logs.iter().sum::<f64>() / count;
        let equity_growth = equity_mean_log.exp();
        let government_growth = government_mean_log.exp();
        let mut linearized = Vec::new();
        linearized
            .try_reserve_exact(EQUITY_PREMIUM_SAMPLE_YEARS)
            .map_err(|_| arithmetic)?;
        for (equity, government) in equity_logs.iter().zip(government_logs) {
            linearized.push(
                StatisticalInput::try_new(
                    equity_growth * (equity - equity_mean_log)
                        - government_growth * (government - government_mean_log),
                    StatisticalUnit::Return,
                    StatisticalScale::Unit,
                )
                .map_err(|_| arithmetic)?,
            );
        }
        Ok(Self {
            geometric_premium: statistical_decimal(equity_growth - government_growth)?,
            arithmetic_premium: arithmetic_sum
                .checked_div(Decimal::from(EQUITY_PREMIUM_SAMPLE_YEARS as u32))
                .ok_or(arithmetic)?
                .normalize(),
            geometric_equity_return: statistical_decimal(equity_growth - 1.0)?,
            geometric_government_return: statistical_decimal(government_growth - 1.0)?,
            arithmetic_standard_error: standard_error(excess)?,
            geometric_delta_standard_error: standard_error(linearized)?,
            government_returns: government.try_into().map_err(|_| arithmetic)?,
        })
    }

    /// Annual premium fraction, statistically estimated and explicitly quantized to twelve places.
    pub const fn geometric_premium(&self) -> Decimal {
        self.geometric_premium
    }
    /// Paired arithmetic mean excess annual return, retained as a separate diagnostic.
    pub const fn arithmetic_premium(&self) -> Decimal {
        self.arithmetic_premium
    }
    /// Geometric equity and modeled government annual returns, in that order.
    pub const fn geometric_returns(&self) -> (Decimal, Decimal) {
        (
            self.geometric_equity_return,
            self.geometric_government_return,
        )
    }
    /// Conventional paired arithmetic standard error under independent annual observations.
    pub const fn arithmetic_standard_error(&self) -> StatisticalDispersion {
        self.arithmetic_standard_error
    }
    /// First-order paired delta-method standard error of the difference of geometric returns.
    pub const fn geometric_delta_standard_error(&self) -> StatisticalDispersion {
        self.geometric_delta_standard_error
    }
    /// Exact modeled coupon and price calculations for every paired annual period.
    pub const fn government_returns(
        &self,
    ) -> &[ModeledGovernmentAnnualReturn; EQUITY_PREMIUM_SAMPLE_YEARS] {
        &self.government_returns
    }
}

fn statistical_return(value: Decimal) -> Result<StatisticalInput, AutomaticValuationError> {
    StatisticalInput::try_from_decimal(value, StatisticalUnit::Return, StatisticalScale::Unit)
        .map_err(|_| AutomaticValuationError::Arithmetic)
}

fn statistical_decimal(value: f64) -> Result<Decimal, AutomaticValuationError> {
    Decimal::from_f64_retain(value)
        .map(|value| {
            value
                .round_dp_with_strategy(12, RoundingStrategy::MidpointNearestEven)
                .normalize()
        })
        .ok_or(AutomaticValuationError::Arithmetic)
}

fn standard_error(
    values: Vec<StatisticalInput>,
) -> Result<StatisticalDispersion, AutomaticValuationError> {
    let arithmetic = AutomaticValuationError::Arithmetic;
    let annualization = Annualization::PeriodsPerYear(NonZeroU32::MIN);
    let series = ReturnSeries::try_new(values, annualization).map_err(|_| arithmetic)?;
    let deviation = volatility(
        &series,
        VarianceConvention::Sample,
        MissingValuePolicy::Reject,
    )
    .map_err(|_| arithmetic)?;
    StatisticalDispersion::try_new(
        deviation.value() / (EQUITY_PREMIUM_SAMPLE_YEARS as f64).sqrt(),
        StatisticalScale::Unit,
        StatisticalUnit::Return,
        EQUITY_PREMIUM_SAMPLE_YEARS,
        VarianceConvention::Sample,
        annualization,
    )
    .map_err(|_| arithmetic)
}
