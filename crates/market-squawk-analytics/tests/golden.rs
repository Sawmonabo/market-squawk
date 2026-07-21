use std::num::NonZeroU32;

use market_squawk_analytics::{
    AnalyticsError, Annualization, DatedMoney, DatedStatisticalInput, DecimalPolicy,
    FactorObservation, FundamentalPeriod, MissingValuePolicy, PortfolioAllocation, Quantile,
    RatePoint, ScenarioShock, ShockComposition, StatisticalInput, StatisticalScale,
    StatisticalUnit, VarianceConvention, WeightPolicy, WeightedStatisticalInput, alpha_beta,
    correlation, cumulative_return, discrete_expected_shortfall, earnings_surprise,
    factor_regression, free_cash_flow_yield, fundamental_growth, historical_var, information_ratio,
    macro_surprise, margin, maximum_drawdown, parametric_var, portfolio_attribution,
    portfolio_exposure, resolve_optional_inputs, scenario_impact, sharpe_ratio, simple_returns,
    sortino_ratio, total_returns, tracking_error, valuation_multiple, volatility,
    weighted_expected_shortfall, yield_curve_change, yield_curve_features,
};
use market_squawk_domain::{Currency, Money, RoundingPolicy, Timestamp};
use rust_decimal::Decimal;

type TestResult = Result<(), Box<dyn std::error::Error>>;

fn input(value: f64, unit: StatisticalUnit) -> Result<StatisticalInput, AnalyticsError> {
    StatisticalInput::try_new(value, unit, StatisticalScale::Unit)
}

fn dated(
    at: i64,
    value: f64,
    unit: StatisticalUnit,
) -> Result<DatedStatisticalInput, AnalyticsError> {
    Ok(DatedStatisticalInput::new(
        Timestamp::from_unix_nanos(at),
        input(value, unit)?,
    ))
}

#[test]
fn return_kernels_enforce_time_units_and_exact_distributions() -> TestResult {
    let usd = Currency::try_from("USD")?;
    let prices = [
        DatedMoney::new(
            Timestamp::from_unix_nanos(1),
            Money::new(Decimal::new(100, 0), usd),
        ),
        DatedMoney::new(
            Timestamp::from_unix_nanos(2),
            Money::new(Decimal::new(110, 0), usd),
        ),
        DatedMoney::new(
            Timestamp::from_unix_nanos(4),
            Money::new(Decimal::new(99, 0), usd),
        ),
    ];
    let distributions = [
        Money::new(Decimal::ZERO, usd),
        Money::new(Decimal::new(1, 0), usd),
    ];
    let returns = total_returns(&prices, &distributions)?;
    assert_eq!(returns.observations(), 2);
    assert!((returns.values()[0].value() - 0.1).abs() < 1e-12);
    assert!((returns.values()[1].value() + 10.0 / 110.0).abs() < 1e-12);
    assert!(cumulative_return(returns.values())?.value().abs() < 1e-12);

    let statistical_prices = [
        dated(1, 100.0, StatisticalUnit::Currency(usd))?,
        dated(2, 110.0, StatisticalUnit::Currency(usd))?,
    ];
    assert!((simple_returns(&statistical_prices)?.values()[0].value() - 0.1).abs() < 1e-12);
    assert_eq!(
        simple_returns(&[
            dated(2, 100.0, StatisticalUnit::Currency(usd))?,
            dated(1, 101.0, StatisticalUnit::Currency(usd))?,
        ]),
        Err(AnalyticsError::TimestampNotStrictlyIncreasing)
    );
    assert_eq!(
        simple_returns(&[
            dated(1, -1.0, StatisticalUnit::Currency(usd))?,
            dated(2, 1.0, StatisticalUnit::Currency(usd))?,
        ]),
        Err(AnalyticsError::NonPositivePrice)
    );
    assert!(
        StatisticalInput::try_new(f64::NAN, StatisticalUnit::Return, StatisticalScale::Unit)
            .is_err()
    );
    assert_eq!(
        StatisticalInput::try_new(
            100.0,
            StatisticalUnit::Currency(usd),
            StatisticalScale::Percent,
        ),
        Err(AnalyticsError::IncompatibleScale)
    );
    let optional = [Some(input(0.1, StatisticalUnit::Return)?), None];
    assert_eq!(
        resolve_optional_inputs(&optional, MissingValuePolicy::Reject),
        Err(AnalyticsError::MissingObservation)
    );
    assert_eq!(
        resolve_optional_inputs(&optional, MissingValuePolicy::Drop)?.len(),
        1
    );
    Ok(())
}

#[test]
fn risk_statistics_disclose_sampling_annualization_and_singularities() -> TestResult {
    let observations = [
        input(0.01, StatisticalUnit::Return)?,
        input(0.02, StatisticalUnit::Return)?,
        input(0.03, StatisticalUnit::Return)?,
    ];
    let annualization = Annualization::PeriodsPerYear(NonZeroU32::new(4).ok_or("periods")?);
    let sigma = volatility(
        &observations,
        VarianceConvention::Sample,
        annualization,
        MissingValuePolicy::Reject,
    )?;
    assert!((sigma.value() - 0.02).abs() < 1e-12);
    assert_eq!(sigma.observations(), 3);

    let benchmark = [
        input(0.01, StatisticalUnit::Return)?,
        input(0.02, StatisticalUnit::Return)?,
        input(0.03, StatisticalUnit::Return)?,
    ];
    let asset = [
        input(0.03, StatisticalUnit::Return)?,
        input(0.05, StatisticalUnit::Return)?,
        input(0.07, StatisticalUnit::Return)?,
    ];
    let fit = alpha_beta(&asset, &benchmark, VarianceConvention::Sample)?;
    assert!((fit.beta().value() - 2.0).abs() < 1e-12);
    assert!((fit.alpha().value() - 0.01).abs() < 1e-12);
    assert_eq!(
        correlation(&asset, &[input(1.0, StatisticalUnit::Return)?; 3]),
        Err(AnalyticsError::ZeroVariance)
    );

    let ratios = [
        input(-0.02, StatisticalUnit::Return)?,
        input(0.01, StatisticalUnit::Return)?,
        input(0.04, StatisticalUnit::Return)?,
    ];
    assert!(
        sharpe_ratio(&ratios, input(0.0, StatisticalUnit::Return)?, annualization,)?
            .value()
            .is_finite()
    );
    assert!(
        sortino_ratio(&ratios, input(0.0, StatisticalUnit::Return)?, annualization,)?
            .value()
            .is_finite()
    );
    assert!(tracking_error(&asset, &benchmark, annualization)?.value() > 0.0);
    assert!(information_ratio(&asset, &benchmark, annualization)?.value() > 0.0);
    Ok(())
}

#[test]
fn drawdown_tracks_recovery_and_discrete_tail_mass_is_coherent() -> TestResult {
    let usd = Currency::try_from("USD")?;
    let prices = [0_i64, 1, 2, 3]
        .into_iter()
        .zip([100.0, 120.0, 90.0, 120.0])
        .map(|(index, value)| dated(index, value, StatisticalUnit::Currency(usd)))
        .collect::<Result<Vec<_>, _>>()?;
    let drawdown = maximum_drawdown(&prices)?;
    assert!((drawdown.magnitude().value() - 0.25).abs() < 1e-12);
    assert_eq!(drawdown.peak_index(), 1);
    assert_eq!(drawdown.trough_index(), 2);
    assert_eq!(drawdown.recovery_index(), Some(3));

    let losses = [
        input(10.0, StatisticalUnit::Currency(usd))?,
        input(10.0, StatisticalUnit::Currency(usd))?,
        input(0.0, StatisticalUnit::Currency(usd))?,
        input(0.0, StatisticalUnit::Currency(usd))?,
    ];
    let confidence = Quantile::try_new(0.625)?;
    assert_eq!(historical_var(&losses, confidence)?.value(), 10.0);
    assert_eq!(
        discrete_expected_shortfall(&losses, confidence)?.value(),
        10.0
    );
    let point_mass = [input(7.0, StatisticalUnit::Currency(usd))?; 4];
    assert_eq!(
        discrete_expected_shortfall(&point_mass, confidence)?.value(),
        7.0
    );
    let weighted = [
        WeightedStatisticalInput::try_new(input(10.0, StatisticalUnit::Currency(usd))?, 0.25)?,
        WeightedStatisticalInput::try_new(input(0.0, StatisticalUnit::Currency(usd))?, 0.75)?,
    ];
    assert_eq!(
        weighted_expected_shortfall(&weighted, confidence, WeightPolicy::PositiveNormalized)?
            .value(),
        20.0 / 3.0
    );
    assert!(
        parametric_var(
            input(0.0, StatisticalUnit::Currency(usd))?,
            input(1.0, StatisticalUnit::Currency(usd))?,
            Quantile::try_new(0.975)?,
        )?
        .value()
            > 1.95
    );
    assert_eq!(
        parametric_var(
            input(0.0, StatisticalUnit::Currency(usd))?,
            input(-1.0, StatisticalUnit::Currency(usd))?,
            Quantile::try_new(0.975)?,
        ),
        Err(AnalyticsError::NegativeStandardDeviation)
    );
    Ok(())
}

#[test]
fn factor_regression_recovers_exposure_and_rejects_rank_deficiency() -> TestResult {
    let observations = [
        factor_observation(0.07, &[0.01, 0.02])?,
        factor_observation(0.08, &[0.02, 0.02])?,
        factor_observation(0.09, &[0.03, 0.02])?,
        factor_observation(0.10, &[0.04, 0.02])?,
    ];
    assert_eq!(
        factor_regression(&observations),
        Err(AnalyticsError::RankDeficient)
    );

    let full_rank = [
        factor_observation(0.08, &[0.01, 0.02])?,
        factor_observation(0.09, &[0.02, 0.02])?,
        factor_observation(0.12, &[0.03, 0.03])?,
        factor_observation(0.13, &[0.04, 0.03])?,
    ];
    let fit = factor_regression(&full_rank)?;
    assert!((fit.intercept().value() - 0.03).abs() < 1e-10);
    assert!((fit.exposures()[0].value() - 1.0).abs() < 1e-10);
    assert!((fit.exposures()[1].value() - 2.0).abs() < 1e-10);
    Ok(())
}

fn factor_observation(response: f64, factors: &[f64]) -> Result<FactorObservation, AnalyticsError> {
    FactorObservation::try_new(
        input(response, StatisticalUnit::Return)?,
        factors
            .iter()
            .map(|factor| input(*factor, StatisticalUnit::Return))
            .collect::<Result<Vec<_>, _>>()?,
    )
}

#[test]
fn exact_fundamental_kernels_preserve_currency_and_signs() -> TestResult {
    let usd = Currency::try_from("USD")?;
    let eur = Currency::try_from("EUR")?;
    let prior = FundamentalPeriod::try_new(
        Money::new(Decimal::new(100, 0), usd),
        Money::new(Decimal::new(20, 0), usd),
        Money::new(Decimal::new(15, 0), usd),
        Money::new(Decimal::new(5, 0), usd),
    )?;
    let current = FundamentalPeriod::try_new(
        Money::new(Decimal::new(125, 0), usd),
        Money::new(Decimal::new(25, 0), usd),
        Money::new(Decimal::new(18, 0), usd),
        Money::new(Decimal::new(6, 0), usd),
    )?;
    let policy = DecimalPolicy::try_new(6, RoundingPolicy::NearestEven)?;
    assert_eq!(
        fundamental_growth(current.revenue(), prior.revenue(), policy)?,
        Decimal::new(25, 2)
    );
    assert_eq!(
        margin(current.operating_income(), current.revenue(), policy)?,
        Decimal::new(2, 1)
    );
    assert_eq!(
        current.free_cash_flow()?,
        Money::new(Decimal::new(12, 0), usd)
    );
    assert_eq!(
        valuation_multiple(
            Money::new(Decimal::new(250, 0), usd),
            current.operating_income(),
            policy,
        )?,
        Decimal::new(10, 0)
    );
    assert_eq!(
        free_cash_flow_yield(
            current.free_cash_flow()?,
            Money::new(Decimal::new(250, 0), usd),
            policy,
        )?,
        Decimal::new(48, 3)
    );
    assert_eq!(
        earnings_surprise(
            Money::new(Decimal::new(110, 0), usd),
            Money::new(Decimal::new(100, 0), usd),
            policy,
        )?,
        Decimal::new(1, 1)
    );
    assert!(
        valuation_multiple(
            Money::new(Decimal::new(250, 0), eur),
            current.operating_income(),
            policy,
        )
        .is_err()
    );
    assert_eq!(
        FundamentalPeriod::try_new(
            Money::new(Decimal::new(125, 0), usd),
            Money::new(Decimal::new(25, 0), usd),
            Money::new(Decimal::new(18, 0), usd),
            Money::new(Decimal::new(-6, 0), usd),
        ),
        Err(AnalyticsError::NegativeCapitalExpenditure)
    );
    Ok(())
}

#[test]
fn yield_curve_requires_ordering_and_exposes_rate_shape() -> TestResult {
    let curve = [
        RatePoint::try_new(30, Decimal::new(4, 2))?,
        RatePoint::try_new(365, Decimal::new(45, 3))?,
        RatePoint::try_new(3_650, Decimal::new(5, 2))?,
    ];
    let features = yield_curve_features(&curve)?;
    assert_eq!(features.slope(), Decimal::new(1, 2));
    assert_eq!(features.curvature(), Decimal::ZERO);
    assert_eq!(
        yield_curve_features(&[curve[1], curve[0], curve[2]]),
        Err(AnalyticsError::MaturityNotStrictlyIncreasing)
    );
    let shifted = [
        RatePoint::try_new(30, Decimal::new(5, 2))?,
        RatePoint::try_new(365, Decimal::new(55, 3))?,
        RatePoint::try_new(3_650, Decimal::new(6, 2))?,
    ];
    let policy = DecimalPolicy::try_new(6, RoundingPolicy::NearestEven)?;
    assert_eq!(
        yield_curve_change(&curve, &shifted, policy)?.average_parallel_shift(),
        Decimal::new(1, 2)
    );
    assert_eq!(
        macro_surprise(
            Decimal::new(105, 0),
            Decimal::new(100, 0),
            Decimal::new(2, 0),
            policy,
        )?,
        Decimal::new(25, 1)
    );
    Ok(())
}

#[test]
fn portfolio_attribution_and_composed_scenarios_remain_exact() -> TestResult {
    let usd = Currency::try_from("USD")?;
    let allocations = [
        PortfolioAllocation::try_new(
            "equity",
            Money::new(Decimal::new(600, 0), usd),
            Decimal::new(1, 1),
        )?,
        PortfolioAllocation::try_new(
            "rates",
            Money::new(Decimal::new(400, 0), usd),
            Decimal::new(-5, 2),
        )?,
    ];
    let attribution = portfolio_attribution(&allocations)?;
    assert_eq!(attribution.total(), Money::new(Decimal::new(40, 0), usd));
    assert_eq!(attribution.contributions().len(), 2);
    let exposure = portfolio_exposure(&allocations)?;
    assert_eq!(exposure.net(), Money::new(Decimal::new(1_000, 0), usd));
    assert_eq!(exposure.gross(), Money::new(Decimal::new(1_000, 0), usd));

    let shocks = [
        ScenarioShock::try_new("equity", Decimal::new(-1, 1))?,
        ScenarioShock::try_new("equity", Decimal::new(-5, 2))?,
        ScenarioShock::try_new("rates", Decimal::new(2, 2))?,
    ];
    let impact = scenario_impact(&allocations, &shocks, ShockComposition::Additive)?;
    assert_eq!(impact.total(), Money::new(Decimal::new(-82, 0), usd));
    let compounded = scenario_impact(&allocations, &shocks, ShockComposition::Compounded)?;
    assert_eq!(compounded.total(), Money::new(Decimal::new(-79, 0), usd));
    Ok(())
}
