//! Non-destructive source-total reconciliation.

use market_squawk_domain::{AccountId, Currency, Money, SourceIdentifier};

use crate::{PortfolioError, PortfolioLimits, PortfolioRevision};

/// Closed portfolio total field calculated independently from source-supplied evidence.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ReconciliationField {
    /// Account cash balance.
    Cash,
    /// Position market value.
    MarketValue,
    /// Resolved position cost basis.
    CostBasis,
}

/// Explicit comparison policy attached to source-total evidence.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReconciliationTolerance {
    amount: Money,
}

impl ReconciliationTolerance {
    /// Constructs an inclusive nonnegative absolute tolerance.
    ///
    /// # Errors
    ///
    /// Rejects negative tolerance amounts.
    pub fn try_absolute(amount: Money) -> Result<Self, PortfolioError> {
        if amount.amount().is_sign_negative() {
            return Err(PortfolioError::Reconciliation);
        }
        Ok(Self { amount })
    }

    /// Returns the maximum accepted absolute difference.
    pub const fn amount(self) -> Money {
        self.amount
    }
}

/// Source-authored totals retained as immutable reconciliation evidence.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SourcePortfolioTotals {
    account_id: AccountId,
    currency: Currency,
    cash: Option<Money>,
    market_value: Option<Money>,
    cost_basis: Option<Money>,
    tolerance: ReconciliationTolerance,
    source_reference: SourceIdentifier,
}

impl SourcePortfolioTotals {
    /// Constructs currency-consistent source totals and comparison policy.
    ///
    /// # Errors
    ///
    /// Rejects mixed currencies, negative cost basis, or a mixed-currency tolerance.
    #[allow(clippy::too_many_arguments, reason = "source evidence stays explicit")]
    pub fn try_new(
        account_id: AccountId,
        currency: Currency,
        cash: Option<Money>,
        market_value: Option<Money>,
        cost_basis: Option<Money>,
        tolerance: ReconciliationTolerance,
        source_reference: SourceIdentifier,
    ) -> Result<Self, PortfolioError> {
        if [cash, market_value, cost_basis]
            .into_iter()
            .flatten()
            .any(|value| value.currency() != currency)
            || cost_basis.is_some_and(|value| value.amount().is_sign_negative())
            || tolerance.amount().currency() != currency
        {
            return Err(PortfolioError::Reconciliation);
        }
        Ok(Self {
            account_id,
            currency,
            cash,
            market_value,
            cost_basis,
            tolerance,
            source_reference,
        })
    }
}

/// One immutable source-supplied versus ledger-calculated mismatch.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReconciliationDiscrepancy {
    field: ReconciliationField,
    supplied: Money,
    calculated: Money,
    currency: Currency,
    tolerance: ReconciliationTolerance,
    source_reference: SourceIdentifier,
}

impl ReconciliationDiscrepancy {
    /// Returns the mismatching canonical total field.
    pub const fn field(&self) -> ReconciliationField {
        self.field
    }

    /// Returns the source value exactly as supplied.
    pub const fn supplied(&self) -> Money {
        self.supplied
    }

    /// Returns the independently calculated ledger value.
    pub const fn calculated(&self) -> Money {
        self.calculated
    }

    /// Returns comparison currency.
    pub const fn currency(&self) -> Currency {
        self.currency
    }

    /// Returns the explicit comparison policy.
    pub const fn tolerance(&self) -> ReconciliationTolerance {
        self.tolerance
    }

    /// Returns immutable source-total identity.
    pub const fn source_reference(&self) -> &SourceIdentifier {
        &self.source_reference
    }
}

impl PortfolioRevision {
    /// Reconciles source totals without changing either supplied or calculated state.
    ///
    /// # Errors
    ///
    /// Rejects another account/currency, excessive output, or checked arithmetic failure.
    pub fn reconcile_supplied(
        &self,
        supplied: &[SourcePortfolioTotals],
        limits: PortfolioLimits,
    ) -> Result<Vec<ReconciliationDiscrepancy>, PortfolioError> {
        if supplied.len() > limits.max_results {
            return Err(PortfolioError::LimitExceeded {
                resource: "supplied totals",
                observed: supplied.len(),
                limit: limits.max_results,
            });
        }
        let mut discrepancies = Vec::new();
        for totals in supplied {
            if totals.account_id != self.account_id() || totals.currency != self.base_currency() {
                return Err(PortfolioError::Reconciliation);
            }
            append(
                &mut discrepancies,
                totals,
                ReconciliationField::Cash,
                totals.cash,
                self.cash(),
                limits,
            )?;
            append(
                &mut discrepancies,
                totals,
                ReconciliationField::MarketValue,
                totals.market_value,
                self.market_value(),
                limits,
            )?;
            append(
                &mut discrepancies,
                totals,
                ReconciliationField::CostBasis,
                totals.cost_basis,
                self.cost_basis(),
                limits,
            )?;
        }
        Ok(discrepancies)
    }
}

fn append(
    discrepancies: &mut Vec<ReconciliationDiscrepancy>,
    totals: &SourcePortfolioTotals,
    field: ReconciliationField,
    supplied: Option<Money>,
    calculated: Money,
    limits: PortfolioLimits,
) -> Result<(), PortfolioError> {
    let Some(supplied) = supplied else {
        return Ok(());
    };
    let difference = supplied
        .checked_sub(calculated)
        .map_err(|_| PortfolioError::Arithmetic)?
        .amount()
        .abs();
    let tolerance = totals.tolerance;
    let accepted = difference <= tolerance.amount().amount();
    if accepted {
        return Ok(());
    }
    if discrepancies.len() >= limits.max_results {
        return Err(PortfolioError::LimitExceeded {
            resource: "reconciliation discrepancies",
            observed: discrepancies.len().saturating_add(1),
            limit: limits.max_results,
        });
    }
    discrepancies.push(ReconciliationDiscrepancy {
        field,
        supplied,
        calculated,
        currency: totals.currency,
        tolerance,
        source_reference: totals.source_reference.clone(),
    });
    Ok(())
}
