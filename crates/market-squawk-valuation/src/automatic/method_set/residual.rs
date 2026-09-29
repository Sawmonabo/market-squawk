//! Saved residual terminal disclosure. This projection never grants source authority.

use super::*;

/// Read-only mirror of the actual conditional finite residual-income receipt.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResidualIncomeTerminalAudit {
    projection: ResidualIncomeTerminalReceipt,
}
impl ResidualIncomeTerminalAudit {
    pub(super) fn from_receipt(receipt: &ResidualIncomeTerminalReceipt) -> Self {
        Self {
            projection: receipt.clone(),
        }
    }
    /// Reconstructs a saved mirror only. Active use must reopen the actual calculation.
    #[allow(
        clippy::too_many_arguments,
        reason = "exact saved model operands remain distinct"
    )]
    pub fn try_recover(
        convention: ResidualIncomeTerminalConvention,
        terminal_period: NonZeroU32,
        current_book_input: InputId,
        final_income_input: InputId,
        final_opening_book_input: InputId,
        annual_rate_identity: EvidenceDigest,
        annual_cost_of_equity: Decimal,
        continuing_value_sensitivity: Decimal,
        expected_identity: EvidenceDigest,
    ) -> Result<Self, AutomaticValuationError> {
        Ok(Self {
            projection: ResidualIncomeTerminalReceipt::try_recover(
                convention,
                terminal_period,
                current_book_input,
                final_income_input,
                final_opening_book_input,
                annual_rate_identity,
                annual_cost_of_equity,
                continuing_value_sensitivity,
                expected_identity,
            )?,
        })
    }
    pub const fn convention(&self) -> ResidualIncomeTerminalConvention {
        self.projection.convention()
    }
    pub const fn terminal_period(&self) -> NonZeroU32 {
        self.projection.terminal_period()
    }
    pub const fn current_book_input(&self) -> InputId {
        self.projection.current_book_input()
    }
    pub const fn final_income_input(&self) -> InputId {
        self.projection.final_income_input()
    }
    pub const fn final_opening_book_input(&self) -> InputId {
        self.projection.final_opening_book_input()
    }
    pub const fn annual_rate_identity(&self) -> EvidenceDigest {
        self.projection.annual_rate_identity()
    }
    pub const fn annual_cost_of_equity(&self) -> Decimal {
        self.projection.annual_cost_of_equity()
    }
    pub const fn continuing_value_sensitivity(&self) -> Decimal {
        self.projection.continuing_value_sensitivity()
    }
    pub const fn identity(&self) -> EvidenceDigest {
        self.projection.identity()
    }
    pub const fn condition_description(&self) -> &'static str {
        self.projection.condition_description()
    }
}
