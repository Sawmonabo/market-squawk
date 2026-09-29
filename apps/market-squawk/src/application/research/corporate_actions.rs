//! Retained source-query, calendar application and existing corporate-action plan composition.

mod applicable;
mod preflight;
pub(crate) use preflight::PreparedCurrentPaperSources;
mod source_errors;
mod query;
pub(crate) use preflight::{PreparedHistorySourceActions, PendingCurrentPriceActions, SourceActionPreparationCapability};

pub use applicable::SourceAppliedCorporateActionPlanReference;
pub(crate) use applicable::SourceForecastOutcomeEvidence;
pub(crate) use preflight::{ForecastOutcomeSourcePreparation, PreparedForecastOutcomeMeasurement, PreparedForecastOutcomeSource};
pub(crate) use applicable::{
    ApplicableActionGap, ApplicableActionPlanError, OrdinaryActionCoverage,
    OrdinaryActionCoverageGap, OrdinaryActionField, ReconciledOrdinaryAction,
    SourceAppliedCorporateActionPlan, SourceAppliedCorporateActionReadCapability, SourcePlanCalendar,
    SourceForecastUnitContinuity, SourceForecastUnitContinuityError,
    SourceForecastUnitContinuityReference,
};
pub(crate) use query::{
    PreparedSourceActionQuery, SourceActionQueryError, SourceActionQueryPublication,
};

mod source_price;
pub(crate) use source_price::source_split_adjusted_close;

pub(crate) use source_errors::{map_analytical_error, map_research_error};
