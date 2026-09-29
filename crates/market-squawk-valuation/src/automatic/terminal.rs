//! Conditional stable-financing/reinvestment continuation over authentic annual common FCFE.

use super::*;
use crate::MacroRateMaturity;

pub(super) const CONTINUATION_ASSUMPTION: &str = "Conditional stable common-equity financing and reinvestment; nominal USD growth is the final forecast FCFE ratio capped at the same 10-year risk-free component used in the annual equity discount rate. The cap does not establish steady-state suitability.";

/// A derived model assumption, not evidence that the issuer has reached a stable state.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DcfTerminalGrowthPolicy {
    terminal_period: NonZeroU32,
    final_explicit_input: InputId,
    next_period_input: InputId,
    annual_reference_identity: EvidenceDigest,
    annual_rate_identity: EvidenceDigest,
    uncapped_growth: Decimal,
    nominal_risk_free_cap: Decimal,
    assumption: AutomaticValuationAssumption,
}

impl DcfTerminalGrowthPolicy {
    /// Explicit conditional scenario: stable financing and reinvestment are assumed, not observed.
    pub const fn assumption_description(&self) -> &'static str {
        CONTINUATION_ASSUMPTION
    }
    pub const fn terminal_period(&self) -> NonZeroU32 {
        self.terminal_period
    }
    pub const fn final_explicit_input(&self) -> InputId {
        self.final_explicit_input
    }
    pub const fn next_period_input(&self) -> InputId {
        self.next_period_input
    }
    pub const fn annual_reference_identity(&self) -> EvidenceDigest {
        self.annual_reference_identity
    }
    pub const fn annual_rate_identity(&self) -> EvidenceDigest {
        self.annual_rate_identity
    }
    pub const fn uncapped_growth(&self) -> Decimal {
        self.uncapped_growth
    }
    pub const fn nominal_risk_free_cap(&self) -> Decimal {
        self.nominal_risk_free_cap
    }
    pub fn was_capped(&self) -> bool {
        self.uncapped_growth > self.nominal_risk_free_cap
    }
    pub const fn assumption(&self) -> &AutomaticValuationAssumption {
        &self.assumption
    }

    pub(super) fn from_inputs(
        final_explicit: &PointInTimeValuationInput,
        next_period: &PointInTimeValuationInput,
        period: NonZeroU32,
        macro_assumptions: &FinancialModelMacroAssumptions,
        produced_at: Timestamp,
        expires_at: Timestamp,
    ) -> Result<Self, AutomaticValuationError> {
        let invalid = AutomaticValuationError::InvalidContract;
        if macro_assumptions.reference().maturity() != MacroRateMaturity::TenYear
            || final_explicit.input().amount().money().currency().as_str() != "USD"
            || final_explicit.input().amount().money().currency()
                != next_period.input().amount().money().currency()
            || native_financial_input(
                final_explicit,
                FinancialAmountRole::CommonEquityCashFlow,
                period.get(),
            )? != native_financial_input(
                next_period,
                FinancialAmountRole::CommonEquityCashFlow,
                period.get().checked_add(1).ok_or(invalid)?,
            )?
        {
            return Err(invalid);
        }
        let reference = macro_assumptions.reference();
        let cap = reference
            .annual_yield_percent()
            .checked_div(Decimal::from(100_u32))
            .ok_or(AutomaticValuationError::Arithmetic)?;
        if cap.checked_mul(Decimal::from(100_u32)) != Some(reference.annual_yield_percent()) {
            return Err(AutomaticValuationError::Arithmetic);
        }
        let (uncapped_growth, growth) = conditional_terminal_growth(
            input_decimal(final_explicit),
            input_decimal(next_period),
            cap,
            macro_assumptions.assumption().value(),
        )?;
        let source_available_at = macro_assumptions
            .assumption()
            .available_at()
            .max(
                final_explicit
                    .input()
                    .evidence()
                    .available_at()
                    .ok_or(invalid)?,
            )
            .max(
                next_period
                    .input()
                    .evidence()
                    .available_at()
                    .ok_or(invalid)?,
            );
        if produced_at < source_available_at
            || expires_at <= produced_at
            || expires_at > final_explicit.expires_at()
            || expires_at > next_period.expires_at()
            || expires_at > macro_assumptions.assumption().expires_at()
        {
            return Err(invalid);
        }
        let mut hash =
            CanonicalHasher::new(b"market-squawk/conditional-nominal-fcfe-continuation/v1");
        hash.u32(period.get());
        hash.fixed(final_explicit.input().id().bytes());
        hash.fixed(next_period.input().id().bytes());
        hash.fixed(reference.evidence_identity().bytes());
        hash.fixed(macro_assumptions.assumption().evidence().bytes());
        for value in [uncapped_growth, cap, growth] {
            hash.bytes(&value.mantissa().to_be_bytes());
            hash.u32(value.scale());
        }
        hash.u8(u8::from(uncapped_growth > cap));
        // The domain fixes conditional stable-financing/reinvestment and nominal USD/10-year conventions.
        let assumption = AutomaticValuationAssumption::try_new(
            AutomaticValuationAssumptionKind::TerminalGrowth,
            "conditional_stable_financing_reinvestment_nominal_fcfe_growth",
            growth,
            EvidenceDigest::new(DigestAlgorithm::Sha256, hash.finish()),
            produced_at,
            expires_at,
        )?;
        Ok(Self {
            terminal_period: period,
            final_explicit_input: final_explicit.input().id(),
            next_period_input: next_period.input().id(),
            annual_reference_identity: reference.evidence_identity(),
            annual_rate_identity: macro_assumptions.assumption().evidence(),
            uncapped_growth,
            nominal_risk_free_cap: cap,
            assumption,
        })
    }
}

impl AutomaticValuationMethodReceipt {
    /// Recreates the exact disclosed conditional policy from the receipt's genuine retained inputs.
    pub fn terminal_growth_policy(
        &self,
    ) -> Result<Option<DcfTerminalGrowthPolicy>, AutomaticValuationError> {
        if self.method != AutomaticValuationMethod::DiscountedCashFlow {
            return Ok(None);
        }
        let invalid = AutomaticValuationError::InvalidContract;
        let terminal = self.intermediates.last().ok_or(invalid)?;
        let previous = self.intermediates.iter().rev().nth(1).ok_or(invalid)?;
        if previous.kind != AutomaticValuationIntermediateKind::DiscountedCashFlow
            || terminal.kind != AutomaticValuationIntermediateKind::DiscountedTerminalValue
            || previous.sequence != terminal.sequence
            || terminal.secondary_input != Some(previous.primary_input)
        {
            return Err(invalid);
        }
        let policy = DcfTerminalGrowthPolicy::from_inputs(
            recovered_input(self, previous.primary_input)?,
            recovered_input(self, terminal.primary_input)?,
            NonZeroU32::new(terminal.sequence).ok_or(invalid)?,
            self.macro_assumptions.as_ref().ok_or(invalid)?,
            recovered_assumption(self, AutomaticValuationAssumptionKind::TerminalGrowth)?
                .available_at(),
            self.expires_at,
        )?;
        if policy.assumption()
            != recovered_assumption(self, AutomaticValuationAssumptionKind::TerminalGrowth)?
        {
            return Err(invalid);
        }
        Ok(Some(policy))
    }
}

/// Pure checked arithmetic shared with sensitivity. It mints no source or model authority.
pub(super) fn conditional_terminal_growth(
    final_fcfe: Decimal,
    next_fcfe: Decimal,
    nominal_risk_free_cap: Decimal,
    annual_cost_of_equity: Decimal,
) -> Result<(Decimal, Decimal), AutomaticValuationError> {
    if final_fcfe <= Decimal::ZERO || next_fcfe <= Decimal::ZERO {
        return Err(AutomaticValuationError::InvalidContract);
    }
    let uncapped = next_fcfe
        .checked_div(final_fcfe)
        .and_then(|ratio| ratio.checked_sub(Decimal::ONE))
        .ok_or(AutomaticValuationError::Arithmetic)?
        .normalize();
    let growth = uncapped.min(nominal_risk_free_cap).normalize();
    if growth <= -Decimal::ONE || growth >= annual_cost_of_equity {
        return Err(AutomaticValuationError::InvalidContract);
    }
    Ok((uncapped, growth))
}
