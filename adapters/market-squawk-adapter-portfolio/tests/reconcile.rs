use std::error::Error;

use market_squawk_adapter_portfolio::{
    CalculatedTotals, PortfolioImportError, ReconciliationField, ReconciliationLimits,
    ReconciliationTolerance, SuppliedTotals, reconcile_totals,
};
use market_squawk_domain::{AccountId, Currency, Money, SourceIdentifier};
use rust_decimal::Decimal;

type TestResult = Result<(), Box<dyn Error>>;

#[test]
fn broker_rounding_is_retained_but_never_replaces_calculated_money() -> TestResult {
    let account_id = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa".parse::<AccountId>()?;
    let currency = Currency::try_from("USD")?;
    let source = SourceIdentifier::try_from("totals-source-record")?;
    let supplied = SuppliedTotals::try_new(
        account_id,
        currency,
        Some(money("1000.00", currency)?),
        Some(money("200.01", currency)?),
        Some(money("240.00", currency)?),
        ReconciliationTolerance::try_absolute(money("0.01", currency)?)?,
        source.clone(),
    )?;
    let calculated = CalculatedTotals::try_new(
        account_id,
        currency,
        Some(money("1000.00", currency)?),
        Some(money("200.005", currency)?),
        Some(money("239.50", currency)?),
    )?;

    let discrepancies =
        reconcile_totals(&supplied, &calculated, ReconciliationLimits::try_new(4)?)?;
    assert_eq!(discrepancies.len(), 1);
    let discrepancy = &discrepancies[0];
    assert_eq!(discrepancy.field(), ReconciliationField::CostBasis);
    assert_eq!(discrepancy.supplied(), money("240.00", currency)?);
    assert_eq!(discrepancy.calculated(), money("239.50", currency)?);
    assert_eq!(discrepancy.currency(), currency);
    assert_eq!(discrepancy.tolerance_policy(), supplied.tolerance_policy());
    assert_eq!(discrepancy.source_reference(), &source);
    assert_eq!(calculated.market_value(), Some(money("200.005", currency)?));
    assert_eq!(supplied.market_value(), Some(money("200.01", currency)?));
    Ok(())
}

#[test]
fn discrepancy_output_fails_closed_at_its_bound() -> TestResult {
    let account_id = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa".parse::<AccountId>()?;
    let currency = Currency::try_from("USD")?;
    let supplied = SuppliedTotals::try_new(
        account_id,
        currency,
        Some(money("10", currency)?),
        Some(money("20", currency)?),
        None,
        ReconciliationTolerance::try_absolute(money("0", currency)?)?,
        SourceIdentifier::try_from("bounded-totals")?,
    )?;
    let calculated = CalculatedTotals::try_new(
        account_id,
        currency,
        Some(money("11", currency)?),
        Some(money("21", currency)?),
        None,
    )?;

    assert_eq!(
        reconcile_totals(&supplied, &calculated, ReconciliationLimits::try_new(1)?,),
        Err(PortfolioImportError::DiscrepancyLimitExceeded { max: 1 })
    );
    Ok(())
}

fn money(value: &str, currency: Currency) -> Result<Money, Box<dyn Error>> {
    Ok(Money::new(value.parse::<Decimal>()?, currency))
}
