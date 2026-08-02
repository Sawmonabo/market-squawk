#![forbid(unsafe_code)]
//! Immutable investment-decision contracts.
//!
//! This crate owns bounded values for saved-screen results, candidate records, evidence dossiers,
//! investment target sets, reviews, and invalidations. It deliberately owns no repository,
//! transport, filesystem, database, network, job, execution, or mutable orchestration authority.

mod contracts;
mod identity;
mod target;

pub use contracts::{
    CandidateRecord, Dossier, DossierEvidence, MAX_SCREEN_FEATURE_BINDINGS, ScreenFeatureBinding,
    ScreenRevision, ScreenRun,
};
pub use identity::{
    CandidateId, DecisionActorId, DecisionContentDigest, DecisionContractError, DossierId,
    InvestmentTargetSetId, MAX_DECISION_ID_BYTES, ScreenId, ScreenRunId, TargetInvalidationId,
    TargetReviewId,
};
pub use target::{
    InvalidationKind, InvestmentTargetSet, ReferenceMark, TargetInvalidation, TargetPriceCases,
    TargetPriceRange, TargetReview, TargetReviewDisposition,
};

#[cfg(test)]
mod tests;
