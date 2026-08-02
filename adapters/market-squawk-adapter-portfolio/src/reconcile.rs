//! Exact, currency-aware reconciliation of supplied and calculated portfolio totals.

use market_squawk_domain::{AccountId, Currency, Money, SourceIdentifier};
use serde::Serialize;

use crate::PortfolioImportError;

/// A portfolio total that may be reconciled.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ReconciliationField {
    /// Account cash balance.
    Cash,
    /// Sum of source holding market values.
    MarketValue,
    /// Sum of fully resolved source cost bases.
    CostBasis,
}

/// Explicit source-total comparison policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ReconciliationTolerance {
    /// An inclusive absolute difference in the account currency.
    Absolute {
        /// Maximum accepted absolute difference.
        amount: Money,
    },
}

impl ReconciliationTolerance {
    /// Constructs a nonnegative absolute tolerance.
    ///
    /// # Errors
    ///
    /// Rejects a negative tolerance.
    pub fn try_absolute(amount: Money) -> Result<Self, PortfolioImportError> {
        if amount.amount().is_sign_negative() {
            return Err(PortfolioImportError::InvalidReconciliationTolerance);
        }
        Ok(Self::Absolute { amount })
    }

    /// Returns the tolerance currency.
    pub const fn currency(self) -> Currency {
        match self {
            Self::Absolute { amount } => amount.currency(),
        }
    }

    const fn absolute_amount(self) -> Money {
        match self {
            Self::Absolute { amount } => amount,
        }
    }
}

/// Fail-closed bound for generated discrepancy records.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReconciliationLimits {
    max_discrepancies: usize,
}

impl ReconciliationLimits {
    /// Constructs a positive discrepancy bound.
    ///
    /// # Errors
    ///
    /// Rejects zero and values above the adapter-wide hard ceiling.
    pub fn try_new(max_discrepancies: usize) -> Result<Self, PortfolioImportError> {
        if max_discrepancies == 0
            || max_discrepancies > PortfolioImportLimitsCeiling::MAX_DISCREPANCIES
        {
            return Err(PortfolioImportError::InvalidLimits);
        }
        Ok(Self { max_discrepancies })
    }

    /// Returns the maximum number of discrepancy records.
    pub const fn max_discrepancies(self) -> usize {
        self.max_discrepancies
    }
}

pub(crate) struct PortfolioImportLimitsCeiling;

impl PortfolioImportLimitsCeiling {
    pub(crate) const MAX_DISCREPANCIES: usize = 4_096;
}

/// Source-supplied portfolio totals retained exactly as supplied.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct SuppliedTotals {
    account_id: AccountId,
    currency: Currency,
    cash: Option<Money>,
    market_value: Option<Money>,
    cost_basis: Option<Money>,
    tolerance_policy: ReconciliationTolerance,
    source_reference: SourceIdentifier,
}

impl SuppliedTotals {
    /// Constructs source totals after validating every currency and nonnegative cost basis.
    ///
    /// # Errors
    ///
    /// Rejects mixed currencies, a negative cost basis, or a tolerance in another currency.
    #[allow(
        clippy::too_many_arguments,
        reason = "source totals and policy stay explicit"
    )]
    pub fn try_new(
        account_id: AccountId,
        currency: Currency,
        cash: Option<Money>,
        market_value: Option<Money>,
        cost_basis: Option<Money>,
        tolerance_policy: ReconciliationTolerance,
        source_reference: SourceIdentifier,
    ) -> Result<Self, PortfolioImportError> {
        validate_money_currency(currency, cash)?;
        validate_money_currency(currency, market_value)?;
        validate_money_currency(currency, cost_basis)?;
        if cost_basis.is_some_and(|value| value.amount().is_sign_negative()) {
            return Err(PortfolioImportError::InvalidCostBasis);
        }
        if tolerance_policy.currency() != currency {
            return Err(PortfolioImportError::CurrencyMismatch);
        }
        Ok(Self {
            account_id,
            currency,
            cash,
            market_value,
            cost_basis,
            tolerance_policy,
            source_reference,
        })
    }

    /// Returns the checked account identity.
    pub const fn account_id(&self) -> AccountId {
        self.account_id
    }

    /// Returns the normalized account currency.
    pub const fn currency(&self) -> Currency {
        self.currency
    }

    /// Returns the exact supplied cash total when present.
    pub const fn cash(&self) -> Option<Money> {
        self.cash
    }

    /// Returns the exact supplied market-value total when present.
    pub const fn market_value(&self) -> Option<Money> {
        self.market_value
    }

    /// Returns the exact supplied cost-basis total when present.
    pub const fn cost_basis(&self) -> Option<Money> {
        self.cost_basis
    }

    /// Returns the explicit source comparison policy.
    pub const fn tolerance_policy(&self) -> ReconciliationTolerance {
        self.tolerance_policy
    }

    /// Returns the immutable raw source reference.
    pub const fn source_reference(&self) -> &SourceIdentifier {
        &self.source_reference
    }
}

/// Independently calculated exact portfolio totals.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct CalculatedTotals {
    account_id: AccountId,
    currency: Currency,
    cash: Option<Money>,
    market_value: Option<Money>,
    cost_basis: Option<Money>,
}

impl CalculatedTotals {
    /// Constructs calculated totals after validating currencies and nonnegative cost basis.
    ///
    /// # Errors
    ///
    /// Rejects mixed currencies or a negative cost basis.
    pub fn try_new(
        account_id: AccountId,
        currency: Currency,
        cash: Option<Money>,
        market_value: Option<Money>,
        cost_basis: Option<Money>,
    ) -> Result<Self, PortfolioImportError> {
        validate_money_currency(currency, cash)?;
        validate_money_currency(currency, market_value)?;
        validate_money_currency(currency, cost_basis)?;
        if cost_basis.is_some_and(|value| value.amount().is_sign_negative()) {
            return Err(PortfolioImportError::InvalidCostBasis);
        }
        Ok(Self {
            account_id,
            currency,
            cash,
            market_value,
            cost_basis,
        })
    }

    /// Returns the checked account identity.
    pub const fn account_id(&self) -> AccountId {
        self.account_id
    }

    /// Returns the normalized account currency.
    pub const fn currency(&self) -> Currency {
        self.currency
    }

    /// Returns the exact calculated cash total when available.
    pub const fn cash(&self) -> Option<Money> {
        self.cash
    }

    /// Returns the exact calculated market-value total when available.
    pub const fn market_value(&self) -> Option<Money> {
        self.market_value
    }

    /// Returns the exact calculated cost-basis total when every basis is resolved.
    pub const fn cost_basis(&self) -> Option<Money> {
        self.cost_basis
    }
}

/// One exact source-versus-calculated mismatch outside the declared tolerance.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ReconciliationDiscrepancy {
    field: ReconciliationField,
    supplied: Money,
    calculated: Money,
    currency: Currency,
    tolerance_policy: ReconciliationTolerance,
    source_reference: SourceIdentifier,
}

impl ReconciliationDiscrepancy {
    /// Returns the mismatching total.
    pub const fn field(&self) -> ReconciliationField {
        self.field
    }

    /// Returns the untouched source-supplied value.
    pub const fn supplied(&self) -> Money {
        self.supplied
    }

    /// Returns the untouched independently calculated value.
    pub const fn calculated(&self) -> Money {
        self.calculated
    }

    /// Returns the common comparison currency.
    pub const fn currency(&self) -> Currency {
        self.currency
    }

    /// Returns the policy that classified this difference.
    pub const fn tolerance_policy(&self) -> ReconciliationTolerance {
        self.tolerance_policy
    }

    /// Returns the immutable raw source reference.
    pub const fn source_reference(&self) -> &SourceIdentifier {
        &self.source_reference
    }
}

/// Compares all source-supplied totals without modifying either input.
///
/// # Errors
///
/// Rejects mismatched account or currency bindings, unavailable calculated counterparts,
/// arithmetic overflow, and output beyond `limits`.
pub fn reconcile_totals(
    supplied: &SuppliedTotals,
    calculated: &CalculatedTotals,
    limits: ReconciliationLimits,
) -> Result<Vec<ReconciliationDiscrepancy>, PortfolioImportError> {
    if supplied.account_id != calculated.account_id {
        return Err(PortfolioImportError::AccountMismatch);
    }
    if supplied.currency != calculated.currency
        || supplied.tolerance_policy.currency() != supplied.currency
    {
        return Err(PortfolioImportError::CurrencyMismatch);
    }

    let mut discrepancies = Vec::new();
    compare(
        ReconciliationField::Cash,
        supplied.cash,
        calculated.cash,
        supplied,
        limits,
        &mut discrepancies,
    )?;
    compare(
        ReconciliationField::MarketValue,
        supplied.market_value,
        calculated.market_value,
        supplied,
        limits,
        &mut discrepancies,
    )?;
    compare(
        ReconciliationField::CostBasis,
        supplied.cost_basis,
        calculated.cost_basis,
        supplied,
        limits,
        &mut discrepancies,
    )?;
    Ok(discrepancies)
}

fn compare(
    field: ReconciliationField,
    supplied_value: Option<Money>,
    calculated_value: Option<Money>,
    supplied: &SuppliedTotals,
    limits: ReconciliationLimits,
    discrepancies: &mut Vec<ReconciliationDiscrepancy>,
) -> Result<(), PortfolioImportError> {
    let Some(supplied_value) = supplied_value else {
        return Ok(());
    };
    let calculated_value =
        calculated_value.ok_or(PortfolioImportError::CalculatedTotalUnavailable { field })?;
    validate_money_currency(supplied.currency, Some(calculated_value))?;
    let difference = supplied_value
        .checked_sub(calculated_value)
        .map_err(|_| PortfolioImportError::Arithmetic)?
        .amount()
        .abs();
    if difference <= supplied.tolerance_policy.absolute_amount().amount() {
        return Ok(());
    }
    if discrepancies.len() >= limits.max_discrepancies {
        return Err(PortfolioImportError::DiscrepancyLimitExceeded {
            max: limits.max_discrepancies,
        });
    }
    discrepancies.push(ReconciliationDiscrepancy {
        field,
        supplied: supplied_value,
        calculated: calculated_value,
        currency: supplied.currency,
        tolerance_policy: supplied.tolerance_policy,
        source_reference: supplied.source_reference.clone(),
    });
    Ok(())
}

fn validate_money_currency(
    currency: Currency,
    money: Option<Money>,
) -> Result<(), PortfolioImportError> {
    if money.is_some_and(|value| value.currency() != currency) {
        Err(PortfolioImportError::CurrencyMismatch)
    } else {
        Ok(())
    }
}
