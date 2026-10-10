//! Bounded point-in-time corporate-action adjustment plans.

mod application;
mod current_ordinary;
pub use current_ordinary::{CompletedOrdinaryHistoryEvidence, CurrentOrdinaryActionFamily, CurrentOrdinaryActionSourceRead, CurrentOrdinaryActionRow, CurrentOrdinaryActionDisposition};
mod canonical;
mod model;
mod plan;
mod query_identity;
mod retained;
mod retained_calendar;
mod source_coverage;
mod source_plan;
mod retained_ordinary;
pub use retained_ordinary::RetainedTiingoEodActionHistory;
pub(crate) mod source_capture;

pub use application::{
    CorporateActionApplication, CorporateActionPaymentPolicy, CorporateActionSessionValues,
};
pub use query_identity::{
    CorporateActionQueryIdentityError, CorporateActionQueryIdentityPrecommitAuthority,
    CorporateActionQueryIdentitySelection,
};

pub use model::{
    AdjustmentConflict, AdjustmentRatio, AdjustmentStep, CorporateActionAdjustment,
    CorporateActionError, CorporateActionExclusion, CorporateActionExclusionReason,
    CorporateActionLimits, CorporateActionPlan, CorporateActionPolicy, CorporateActionRecord,
    MAX_CORPORATE_ACTION_RETAINED_BYTES, MAX_CORPORATE_ACTIONS,
};

pub use retained_calendar::RetainedCorporateActionCalendar;
pub use source_coverage::CorporateActionSourceCoverage;
pub use source_plan::{ApplicableActionGap, OrdinaryActionCoverageGap, OrdinaryActionField, ReconciledOrdinaryAction};
