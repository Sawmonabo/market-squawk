//! Exact contribution-based performance attribution.

use std::collections::BTreeSet;

use market_squawk_analytics::{
    ExactRate, MonetaryBasis, MonetaryValue, PortfolioAllocation, portfolio_attribution,
};
use market_squawk_domain::{InstrumentId, Money};

use crate::exposure::instrument_dimension;
use crate::{PortfolioError, PortfolioLimits, PortfolioRevision, PortfolioRevisionId};

/// One instrument's exact opening allocation and realized return.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AttributionInput {
    instrument_id: InstrumentId,
    opening_market_value: Money,
    return_rate: ExactRate,
}

impl AttributionInput {
    /// Constructs one attribution input.
    ///
    /// # Errors
    ///
    /// Rejects no already-valid exact values; the result preserves constructor symmetry.
    pub const fn try_new(
        instrument_id: InstrumentId,
        opening_market_value: Money,
        return_rate: ExactRate,
    ) -> Result<Self, PortfolioError> {
        Ok(Self {
            instrument_id,
            opening_market_value,
            return_rate,
        })
    }
}

/// One exact instrument contribution.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AttributionLine {
    instrument_id: InstrumentId,
    amount: Money,
}

impl AttributionLine {
    /// Returns canonical instrument identity.
    pub const fn instrument_id(self) -> InstrumentId {
        self.instrument_id
    }

    /// Returns exact contribution amount.
    pub const fn amount(self) -> Money {
        self.amount
    }
}

/// Revision-bound contribution attribution.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AttributionReport {
    revision_id: PortfolioRevisionId,
    lines: Vec<AttributionLine>,
    total: Money,
}

impl AttributionReport {
    /// Uses the Task 12 exact contribution kernel and binds its result to a portfolio revision.
    ///
    /// # Errors
    ///
    /// Rejects empty/excessive, duplicate, mixed-currency, or unknown instrument input.
    pub fn try_calculate(
        revision: &PortfolioRevision,
        inputs: &[AttributionInput],
        limits: PortfolioLimits,
    ) -> Result<Self, PortfolioError> {
        if inputs.is_empty() || inputs.len() > limits.max_results {
            return Err(PortfolioError::LimitExceeded {
                resource: "attribution inputs",
                observed: inputs.len(),
                limit: limits.max_results,
            });
        }
        let unique = inputs
            .iter()
            .map(|input| input.instrument_id)
            .collect::<BTreeSet<_>>();
        if unique.len() != inputs.len()
            || inputs.iter().any(|input| {
                revision.position(input.instrument_id).is_none()
                    || input.opening_market_value.currency() != revision.base_currency()
            })
        {
            return Err(PortfolioError::InvalidDimension);
        }
        let allocations = inputs
            .iter()
            .map(|input| {
                PortfolioAllocation::try_new(
                    &instrument_dimension(input.instrument_id),
                    MonetaryValue::new(input.opening_market_value, MonetaryBasis::Total),
                    input.return_rate,
                )
                .map_err(|_| PortfolioError::Analytics)
            })
            .collect::<Result<Vec<_>, _>>()?;
        let result = portfolio_attribution(&allocations).map_err(|_| PortfolioError::Analytics)?;
        let lines = inputs
            .iter()
            .zip(result.contributions())
            .map(|(input, contribution)| AttributionLine {
                instrument_id: input.instrument_id,
                amount: contribution.amount().money(),
            })
            .collect();
        Ok(Self {
            revision_id: revision.id(),
            lines,
            total: result.total().money(),
        })
    }

    /// Returns bound immutable revision identity.
    pub const fn revision_id(&self) -> PortfolioRevisionId {
        self.revision_id
    }

    /// Returns contributions in caller instrument order.
    pub fn lines(&self) -> &[AttributionLine] {
        &self.lines
    }

    /// Returns exact total contribution.
    pub const fn total(&self) -> Money {
        self.total
    }
}
