//! Exact contribution-based performance attribution.

use market_squawk_analytics::{
    ExactRate, MonetaryBasis, MonetaryValue, PortfolioAllocation, portfolio_attribution,
};
use market_squawk_data::Sha256Digest;
use market_squawk_domain::{InstrumentId, Money};

use crate::exposure::try_instrument_dimension;
use crate::{
    PortfolioAnalyticsEvidence, PortfolioError, PortfolioLimits, PortfolioRevision,
    PortfolioRevisionId, admit_retained_bytes, checked_usize_add, checked_usize_mul,
};

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
    analytics_evidence_digest: Sha256Digest,
    lines: Vec<AttributionLine>,
    total: Money,
    retained_bytes: usize,
}

impl AttributionReport {
    /// Uses the Task 12 exact contribution kernel and binds its result to a portfolio revision.
    ///
    /// # Errors
    ///
    /// Rejects empty/excessive, duplicate, mixed-currency, or unknown instrument input.
    pub fn try_calculate(
        revision: &PortfolioRevision,
        analytics_evidence: &PortfolioAnalyticsEvidence,
        inputs: &[AttributionInput],
        limits: PortfolioLimits,
    ) -> Result<Self, PortfolioError> {
        let report_through = revision.evidence().as_of();
        analytics_evidence.validate_report(revision, report_through, report_through)?;
        if inputs.is_empty() || inputs.len() > limits.max_results {
            return Err(PortfolioError::LimitExceeded {
                resource: "attribution inputs",
                observed: inputs.len(),
                limit: limits.max_results,
            });
        }
        let retained_preflight = checked_usize_add(
            std::mem::size_of::<Self>(),
            checked_usize_mul(inputs.len(), std::mem::size_of::<AttributionLine>())?,
        )?;
        admit_retained_bytes(retained_preflight, limits)?;
        let mut unique = Vec::new();
        unique
            .try_reserve_exact(inputs.len())
            .map_err(|_| PortfolioError::AllocationFailed)?;
        unique.extend(inputs.iter().map(|input| input.instrument_id));
        unique.sort_unstable();
        if unique.windows(2).any(|pair| pair[0] == pair[1])
            || inputs.iter().any(|input| {
                revision.position(input.instrument_id).is_none()
                    || input.opening_market_value.currency() != revision.base_currency()
            })
        {
            return Err(PortfolioError::InvalidDimension);
        }
        let mut allocations = Vec::new();
        allocations
            .try_reserve_exact(inputs.len())
            .map_err(|_| PortfolioError::AllocationFailed)?;
        for input in inputs {
            let dimension = try_instrument_dimension(input.instrument_id)?;
            allocations.push(
                PortfolioAllocation::try_new(
                    &dimension,
                    MonetaryValue::new(input.opening_market_value, MonetaryBasis::Total),
                    input.return_rate,
                )
                .map_err(|_| PortfolioError::Analytics)?,
            );
        }
        let result = portfolio_attribution(&allocations).map_err(|_| PortfolioError::Analytics)?;
        let mut lines = Vec::new();
        lines
            .try_reserve_exact(inputs.len())
            .map_err(|_| PortfolioError::AllocationFailed)?;
        lines.extend(
            inputs
                .iter()
                .zip(result.contributions())
                .map(|(input, contribution)| AttributionLine {
                    instrument_id: input.instrument_id,
                    amount: contribution.amount().money(),
                }),
        );
        let retained_bytes = checked_usize_add(
            std::mem::size_of::<Self>(),
            checked_usize_mul(lines.capacity(), std::mem::size_of::<AttributionLine>())?,
        )?;
        admit_retained_bytes(retained_bytes, limits)?;
        Ok(Self {
            revision_id: revision.id(),
            analytics_evidence_digest: analytics_evidence.semantic_digest(),
            lines,
            total: result.total().money(),
            retained_bytes,
        })
    }

    /// Returns bound immutable revision identity.
    pub const fn revision_id(&self) -> PortfolioRevisionId {
        self.revision_id
    }

    /// Returns the exact point-in-time analytics authority digest.
    pub const fn analytics_evidence_digest(&self) -> Sha256Digest {
        self.analytics_evidence_digest
    }

    /// Returns contributions in caller instrument order.
    pub fn lines(&self) -> &[AttributionLine] {
        &self.lines
    }

    /// Returns exact total contribution.
    pub const fn total(&self) -> Money {
        self.total
    }

    /// Returns exact Rust-visible bytes retained by this report.
    pub const fn retained_bytes(&self) -> usize {
        self.retained_bytes
    }
}
