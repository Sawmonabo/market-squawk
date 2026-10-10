//! One financial projection policy shared by preparation, profile commitments and valuation.
use market_squawk_data::{FinancialAmountBasis, FinancialAmountRole, FinancialAmountSelection};
use market_squawk_domain::FundamentalCadence;
use serde::Serialize;
use serde_json::{Value, json};
use std::num::NonZeroU16;

pub(crate) const FISCAL_PROJECTION_POLICY_ID: &str =
    "market-squawk.annual-common-equity-projection.v1";
pub(crate) const FISCAL_PROJECTION_EXPLICIT_PERIODS: u16 = 3;

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct FiscalProjectionTarget {
    pub(crate) target_id: String,
    pub(crate) role: FinancialAmountRole,
    pub(crate) basis: FinancialAmountBasis,
    pub(crate) share_convention: Option<market_squawk_data::FinancialShareConvention>,
    pub(crate) cadence: FundamentalCadence,
    pub(crate) periods_ahead: NonZeroU16,
}
impl FiscalProjectionTarget {
    pub(crate) fn measurement(&self) -> FinancialAmountSelection {
        FinancialAmountSelection {
            role: self.role,
            basis: self.basis,
            share_convention: self.share_convention,
        }
    }
}
/// Predeclared financial requirements; no data availability or financial amount is asserted.
pub(crate) fn fiscal_projection_targets() -> Vec<FiscalProjectionTarget> {
    let mut targets = Vec::with_capacity(9);
    for (prefix, role, last) in [
        (
            "common-cash-flow",
            FinancialAmountRole::CommonEquityCashFlow,
            FISCAL_PROJECTION_EXPLICIT_PERIODS + 1,
        ),
        (
            "common-income",
            FinancialAmountRole::CommonNetIncome,
            FISCAL_PROJECTION_EXPLICIT_PERIODS,
        ),
        (
            "common-book",
            FinancialAmountRole::CommonBookEquity,
            FISCAL_PROJECTION_EXPLICIT_PERIODS - 1,
        ),
    ] {
        for offset in 1..=last {
            if let Some(periods_ahead) = NonZeroU16::new(offset) {
                targets.push(FiscalProjectionTarget {
                    target_id: format!("{prefix}-annual-{offset}"),
                    role,
                    basis: FinancialAmountBasis::TotalCommonEquity,
                    share_convention: None,
                    cadence: FundamentalCadence::Annual,
                    periods_ahead,
                });
            }
        }
    }
    targets
}
pub(crate) fn fiscal_projection_policy_value() -> Value {
    json!({"identity":FISCAL_PROJECTION_POLICY_ID,"explicitPeriods":FISCAL_PROJECTION_EXPLICIT_PERIODS,
        "cadence":"annual","targets":fiscal_projection_targets(),
        "openingBook":"actual_source_period_zero",
        "terminalCashFlow":"actual_common_equity_cash_flow_period_n_plus_one",
        "terminalGrowth":"separately_evidenced_annual_rate",
        "basis":"retrospective_frozen_snapshot","population":"present_day_fixed_cohort"})
}
