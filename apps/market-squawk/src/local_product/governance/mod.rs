//! Concrete adapters binding shared governance to installed product authorities.

mod decision;
mod fair_value;

pub(super) use decision::DecisionGovernanceAdapter;
pub(super) use fair_value::ProductionFairValueGovernanceActionFactory;
