//! Bounded point-in-time corporate-action adjustment plans.

mod canonical;
mod model;
mod plan;
mod retained;

pub use model::{
    AdjustmentConflict, AdjustmentRatio, AdjustmentStep, CorporateActionAdjustment,
    CorporateActionError, CorporateActionExclusion, CorporateActionExclusionReason,
    CorporateActionLimits, CorporateActionPlan, CorporateActionPolicy, CorporateActionRecord,
    MAX_CORPORATE_ACTION_RETAINED_BYTES, MAX_CORPORATE_ACTIONS,
};
