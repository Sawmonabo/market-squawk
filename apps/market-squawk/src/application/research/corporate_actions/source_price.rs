//! One source-issued split price kernel shared by feature and measured-outcome consumers.
use market_squawk_data::{AdjustmentStep, CorporateActionAdjustment, CorporateActionPlan};
use market_squawk_domain::{MarketBarAdjustment, MarketBarObservation, Timestamp};
use rust_decimal::Decimal;
use super::ApplicableActionPlanError;

pub(crate) fn source_split_adjusted_close(
    plan: &CorporateActionPlan, bar: &MarketBarObservation, effective: Timestamp,
) -> Result<Decimal, ApplicableActionPlanError> {
    let coverage = plan.source_split_admission().ok_or(ApplicableActionPlanError::InvalidEvidence)?;
    let instrument = bar.context().provenance().instrument_id().ok_or(ApplicableActionPlanError::InvalidEvidence)?;
    if !coverage.instruments().contains(&instrument)
        || effective > plan.valuation_cutoff()
        || plan.policy().adjustment() != CorporateActionAdjustment::SplitAdjusted
        || bar.adjustment() != MarketBarAdjustment::Raw {
        return Err(ApplicableActionPlanError::InvalidEvidence);
    }
    let mut adjusted = bar.close().amount();
    for step in plan.steps() {
        let AdjustmentStep::Split { admitted_index, price_factor, .. } = step else {
            return Err(ApplicableActionPlanError::InvalidEvidence);
        };
        let action = plan.admitted().get(*admitted_index)
            .ok_or(ApplicableActionPlanError::InvalidEvidence)?;
        if action.observation().context().provenance().instrument_id() != Some(instrument) {
            return Err(ApplicableActionPlanError::InvalidEvidence);
        }
        let application = action.application().ok_or(ApplicableActionPlanError::InvalidEvidence)?;
        // A split at an exclusive close changes subsequent units: adjust the preceding close.
        if effective <= application.application_at() {
            adjusted = adjusted.checked_mul(Decimal::from(price_factor.numerator().get()))
                .and_then(|value| value.checked_div(Decimal::from(price_factor.denominator().get())))
                .ok_or(ApplicableActionPlanError::InvalidEvidence)?;
        }
    }
    Ok(adjusted.normalize())
}
