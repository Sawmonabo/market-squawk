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
mod investment_projection;
mod investment_proposal;
mod recommendation_outcome;
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
pub use investment_projection::{
    AfterTaxPnlAvailability, BenchmarkReturnAvailability, CandidatePortfolioSizingState,
    CandidateSizingConstraints, CapacityRange, ExactFinancialRatio, ExactFinancialRatioRange,
    ExactPositionScale, ExpectedGrossPricePnlAvailability, ExpectedReturnAvailability,
    FeasibleLotRangeAvailability, FeasibleNotionalRangeAvailability, GrossMarkRelativeRange,
    GrossPricePnlAvailability, INVESTMENT_OUTCOME_PROJECTION_SCHEMA_VERSION,
    INVESTMENT_SIZING_PROJECTION_SCHEMA_VERSION, InvestmentOutcomeProjection,
    InvestmentProjectionAuthority, InvestmentProjectionBinding, InvestmentProjectionDigest,
    InvestmentProjectionError, InvestmentSizingInputs, InvestmentSizingProjection, LotRange,
    MarkToZoneDistance, NetPnlAvailability, NonnegativeMoneyRange,
    PreferredWeightRoundingRemainder, SignedMoneyRange, SizingCapacityAvailability,
    SizingCapacityEvidence, SizingConstraintCap, SizingConstraintKind, SizingUnavailableReason,
};
pub use investment_proposal::{
    ActionSpecificCostAvailability, CONFIDENCE_PARTS_PER_MILLION, CostAdjustedPitBacktestEvidence,
    ForecastCalibrationSummary, ForecastPriceRanges, GeneratedInvestmentProposal,
    GeneratedPriceLadder, InvestmentAnalysisEvidence, InvestmentAnalysisEvidenceInput,
    InvestmentAnalysisId, InvestmentProposalAuthority, InvestmentProposalDecision,
    InvestmentProposalError, InvestmentProposalId, LiquidityEvidence, MAX_PROPOSAL_INVALIDATORS,
    MarketReferenceAdjustmentBasis, MarketReferenceEvidence, MarketReferencePriceKind,
    NoActionInvestmentProposal, NoActionReason, PortfolioPositionState, PortfolioRiskEvidence,
    PriceForecastEvidence, ProposalEvidenceWindow, ProposalExecutionEligibility,
    ProposalForecastVintageId, ProposalInvalidator, ProposalTimeBenchmarkAvailability,
    ProposalUnavailableReason, RECOMMENDATION_ASSUMPTION_COUNT,
    RECOMMENDATION_CONFIDENCE_COMPONENT_COUNT, RECOMMENDATION_INVALIDATION_COUNT,
    RECOMMENDATION_LIMITATION_COUNT, RecommendationAction, RecommendationConfidence,
    RecommendationConfidenceComponent, RecommendationConfidenceComponentKind,
    RecommendationConfidenceMeaning, RecommendationDerivationDigest, RecommendationEvidenceDigest,
    RecommendationEvidenceKind, RecommendationPolicy, RecommendationPolicyDigest,
    UnavailableInvestmentAnalysis, ValuationEvidence,
};
pub use recommendation_outcome::{
    AnalyticalProfileBindingReference, INVESTMENT_ANALYSIS_PUBLICATION_SCHEMA_VERSION,
    InvestmentAnalysisPublicationId, InvestmentAnalysisWorkflowReference,
    PublishedInvestmentAnalysis, RECOMMENDATION_OUTCOME_STATUS_SCHEMA_VERSION,
    RECOMMENDATION_TRACK_RECORD_MINIMUM_COMPLETED,
    RECOMMENDATION_TRACK_RECORD_MINIMUM_COVERAGE_PPM, RecommendationAfterTaxReturnAvailability,
    RecommendationBenchmarkReturnAvailability, RecommendationNetReturnAvailability,
    RecommendationOutcomeCohort, RecommendationOutcomeCurrentIndexEntry,
    RecommendationOutcomeError, RecommendationOutcomeObservation,
    RecommendationOutcomePendingReason, RecommendationOutcomeSeriesId, RecommendationOutcomeStatus,
    RecommendationOutcomeStatusDigest, RecommendationOutcomeStatusRecord,
    RecommendationOutcomeUnavailableReason, RecommendationRealizedOutcome,
    RecommendationSettlementAvailability, RecommendationTrackRecord,
    RecommendationTrackRecordGroup, RecommendationTrackRecordPerformance,
};
pub use repository::{
    AppendOutcome, DecisionJournalSnapshot, DecisionRecord, DecisionRepository,
    DecisionRepositoryError, DecisionRepositoryLimits, InvestmentAnalysisCurrentIndexEntry,
    InvestmentProposalIndexEntry, InvestmentProposalIndexOutcome, ScreenRunIndexEntry,
    TargetIndexEntry,
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
