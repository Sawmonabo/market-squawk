#![forbid(unsafe_code)]
//! Immutable investment-decision contracts.
//!
//! This crate owns bounded values for saved-screen results, candidate records, evidence dossiers,
//! investment target sets, reviews, invalidations, and their single-writer typed append journal. It
//! deliberately owns no transport, filesystem, database, network, job, or execution authority.

mod authority;
mod candidate;
mod contracts;
mod dossier;
mod identity;
mod repository;
mod screen;
mod target;

pub use authority::DecisionAuthority;
pub use candidate::{
    CandidateAssessment, CandidateFlag, CandidateInput, CandidateScoreContribution,
    MAX_CANDIDATE_FLAGS, MAX_SCREEN_INPUT_ROWS, ScreenExecution, ScreenFeatureObservation,
};
pub use contracts::{
    CandidateRecord, Dossier, DossierEvidence, MAX_SCREEN_FEATURE_BINDINGS, ScreenFeatureBinding,
    ScreenRevision, ScreenRun,
};
pub use dossier::{DecisionDossier, DossierReference, DossierSection, MAX_DOSSIER_REFERENCES};
pub use identity::{
    CandidateId, DecisionActorId, DecisionContentDigest, DecisionContractError, DossierId,
    InvestmentTargetSetId, MAX_DECISION_ID_BYTES, ScreenId, ScreenRunId, TargetInvalidationId,
    TargetReviewId,
};
pub use repository::{
    AppendOutcome, DecisionJournalSnapshot, DecisionRecord, DecisionRepository,
    DecisionRepositoryError, DecisionRepositoryLimits, ScreenRunIndexEntry, TargetIndexEntry,
};
pub use screen::{
    AsOfSemantics, ComparisonOperator, MAX_SCREEN_DATA_QUALITIES, MAX_SCREEN_RESULTS, NullPolicy,
    RankingDirection, SavedScreen, ScreenConstraints, ScreenPredicate, ScreenRanking,
};
pub use target::{
    DecisionText, GovernedTargetSet, InvalidationKind, InvestmentTargetSet,
    MAX_DECISION_TEXT_BYTES, MAX_TARGET_NARRATIVE_ITEMS, ReferenceMark, TargetAssumption,
    TargetDecisionContext, TargetEvidence, TargetGovernanceInput, TargetInvalidation, TargetMethod,
    TargetPriceCases, TargetPriceRange, TargetReview, TargetReviewDisposition, TargetState,
    TargetStatus,
};

#[cfg(test)]
mod tests;
