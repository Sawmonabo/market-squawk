//! The explicit finite residual-income condition; no continuing value is fabricated.

use super::*;

/// Closed terminal condition of the existing finite residual-income calculation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ResidualIncomeTerminalConvention {
    /// Future abnormal earnings are conditionally zero after the explicit horizon.
    ZeroAbnormalEarningsAfterExplicitHorizon,
}

impl ResidualIncomeTerminalConvention {
    pub const fn identifier(self) -> &'static str {
        match self {
            Self::ZeroAbnormalEarningsAfterExplicitHorizon => {
                "zero_abnormal_earnings_after_explicit_horizon"
            }
        }
    }
    pub const fn condition_description(self) -> &'static str {
        match self {
            Self::ZeroAbnormalEarningsAfterExplicitHorizon => RESIDUAL_TERMINAL_CONDITION,
        }
    }
}

pub(super) const RESIDUAL_TERMINAL_CONDITION: &str = "Conditional residual-income model with a clean-surplus-compatible earnings and book relation and zero abnormal earnings after the explicit horizon. Neither clean surplus nor future convergence is observed by these inputs. The conditional input-support range excludes unknown continuing-value uncertainty and is not a universal lower bound.";

/// Exact inputs and analytic continuing-value sensitivity of the conditional finite model.
/// Only the calculator and authenticated receipt recovery construct this type.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResidualIncomeTerminalReceipt {
    convention: ResidualIncomeTerminalConvention,
    terminal_period: NonZeroU32,
    current_book_input: InputId,
    final_income_input: InputId,
    final_opening_book_input: InputId,
    annual_rate_identity: EvidenceDigest,
    annual_cost_of_equity: Decimal,
    continuing_value_sensitivity: Decimal,
    identity: EvidenceDigest,
}

impl ResidualIncomeTerminalReceipt {
    pub const fn convention(&self) -> ResidualIncomeTerminalConvention {
        self.convention
    }
    pub const fn terminal_period(&self) -> NonZeroU32 {
        self.terminal_period
    }
    pub const fn current_book_input(&self) -> InputId {
        self.current_book_input
    }
    pub const fn final_income_input(&self) -> InputId {
        self.final_income_input
    }
    pub const fn final_opening_book_input(&self) -> InputId {
        self.final_opening_book_input
    }
    pub const fn annual_rate_identity(&self) -> EvidenceDigest {
        self.annual_rate_identity
    }
    pub const fn annual_cost_of_equity(&self) -> Decimal {
        self.annual_cost_of_equity
    }
    /// dV0/dCV_N = (1 + Ke)^(-N); this coefficient is not a forecast of continuing value.
    pub const fn continuing_value_sensitivity(&self) -> Decimal {
        self.continuing_value_sensitivity
    }
    pub const fn identity(&self) -> EvidenceDigest {
        self.identity
    }
    pub const fn condition_description(&self) -> &'static str {
        RESIDUAL_TERMINAL_CONDITION
    }

    pub(super) fn from_inputs(
        current_book: &PointInTimeValuationInput,
        final_income: &PointInTimeValuationInput,
        final_opening_book: &PointInTimeValuationInput,
        terminal_period: NonZeroU32,
        rate: &AutomaticValuationAssumption,
    ) -> Result<Self, AutomaticValuationError> {
        let invalid = AutomaticValuationError::InvalidContract;
        if terminal_period.get() > MAX_RESIDUAL_PERIODS as u32 {
            return Err(invalid);
        }
        let anchor =
            native_financial_input(current_book, FinancialAmountRole::CommonBookEquity, 0)?;
        if rate.kind() != AutomaticValuationAssumptionKind::CostOfEquity
            || native_financial_input(
                final_income,
                FinancialAmountRole::CommonNetIncome,
                terminal_period.get(),
            )? != anchor
            || native_financial_input(
                final_opening_book,
                FinancialAmountRole::CommonBookEquity,
                terminal_period.get() - 1,
            )? != anchor
        {
            return Err(invalid);
        }
        let sensitivity =
            AnnualEquityArithmetic::discounted_amount(Decimal::ONE, rate.value(), terminal_period)?;
        Self::projection(
            ResidualIncomeTerminalConvention::ZeroAbnormalEarningsAfterExplicitHorizon,
            terminal_period,
            current_book.input().id(),
            final_income.input().id(),
            final_opening_book.input().id(),
            rate.evidence(),
            rate.value(),
            sensitivity,
        )
    }

    // This internal scalar reconstruction is used only by strict persistence and saved mirrors.
    // Active receipt recovery additionally rederives the entire condition from authentic inputs.
    #[allow(
        clippy::too_many_arguments,
        reason = "exact persisted model operands remain distinct"
    )]
    pub(crate) fn try_recover(
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
        let value = Self::projection(
            convention,
            terminal_period,
            current_book_input,
            final_income_input,
            final_opening_book_input,
            annual_rate_identity,
            annual_cost_of_equity,
            continuing_value_sensitivity,
        )?;
        if value.identity != expected_identity {
            return Err(AutomaticValuationError::InvalidContract);
        }
        Ok(value)
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "exact model operands remain distinct"
    )]
    fn projection(
        convention: ResidualIncomeTerminalConvention,
        terminal_period: NonZeroU32,
        current_book_input: InputId,
        final_income_input: InputId,
        final_opening_book_input: InputId,
        annual_rate_identity: EvidenceDigest,
        annual_cost_of_equity: Decimal,
        continuing_value_sensitivity: Decimal,
    ) -> Result<Self, AutomaticValuationError> {
        let invalid = AutomaticValuationError::InvalidContract;
        if terminal_period.get() > MAX_RESIDUAL_PERIODS as u32
            || [
                current_book_input,
                final_income_input,
                final_opening_book_input,
            ]
            .iter()
            .any(|id| id.bytes() == [0; 32])
            || current_book_input == final_income_input
            || final_income_input == final_opening_book_input
            || (current_book_input == final_opening_book_input) != (terminal_period.get() == 1)
            || !valid_sha256(annual_rate_identity)
            || annual_cost_of_equity <= -Decimal::ONE
            || continuing_value_sensitivity <= Decimal::ZERO
            || AnnualEquityArithmetic::discounted_amount(
                Decimal::ONE,
                annual_cost_of_equity,
                terminal_period,
            )? != continuing_value_sensitivity
        {
            return Err(invalid);
        }
        let mut value = Self {
            convention,
            terminal_period,
            current_book_input,
            final_income_input,
            final_opening_book_input,
            annual_rate_identity,
            annual_cost_of_equity: annual_cost_of_equity.normalize(),
            continuing_value_sensitivity: continuing_value_sensitivity.normalize(),
            identity: EvidenceDigest::new(DigestAlgorithm::Sha256, [0; 32]),
        };
        let mut hash = CanonicalHasher::new(b"market-squawk/conditional-finite-residual-income/v1");
        hash.u8(match convention {
            ResidualIncomeTerminalConvention::ZeroAbnormalEarningsAfterExplicitHorizon => 1,
        });
        hash.u32(terminal_period.get());
        for id in [
            current_book_input,
            final_income_input,
            final_opening_book_input,
        ] {
            hash.fixed(id.bytes());
        }
        hash.fixed(annual_rate_identity.bytes());
        for amount in [
            value.annual_cost_of_equity,
            value.continuing_value_sensitivity,
        ] {
            hash.bytes(&amount.mantissa().to_be_bytes());
            hash.u32(amount.scale());
        }
        // This domain commits the conditional interpretation and excludes unmodeled tail uncertainty.
        value.identity = EvidenceDigest::new(DigestAlgorithm::Sha256, hash.finish());
        Ok(value)
    }
}

pub(super) fn verify_residual_terminal(
    receipt: &AutomaticValuationMethodReceipt,
) -> Result<(), AutomaticValuationError> {
    let invalid = AutomaticValuationError::InvalidContract;
    if receipt.method != AutomaticValuationMethod::ResidualIncome {
        return if receipt.residual_terminal.is_none() {
            Ok(())
        } else {
            Err(invalid)
        };
    }
    let final_step = receipt.intermediates.last().ok_or(invalid)?;
    if final_step.kind != AutomaticValuationIntermediateKind::DiscountedResidualIncome {
        return Err(invalid);
    }
    let reconstructed = ResidualIncomeTerminalReceipt::from_inputs(
        recovered_input(receipt, receipt.method_base_input.ok_or(invalid)?)?,
        recovered_input(receipt, final_step.primary_input)?,
        recovered_input(receipt, final_step.secondary_input.ok_or(invalid)?)?,
        NonZeroU32::new(final_step.sequence).ok_or(invalid)?,
        recovered_assumption(receipt, AutomaticValuationAssumptionKind::CostOfEquity)?,
    )?;
    if receipt.residual_terminal.as_ref() != Some(&reconstructed) {
        return Err(invalid);
    }
    Ok(())
}
