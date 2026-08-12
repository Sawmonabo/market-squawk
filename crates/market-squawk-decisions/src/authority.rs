//! Typed decision workflow authority over the sole bounded repository writer.

use market_squawk_domain::{RevisionNumber, Timestamp};

use crate::{
    AnalyticalProfileBindingReference, AppendOutcome, CandidateAssessment, CandidateInput,
    DecisionDossier, DecisionRepository, DecisionRepositoryError, GovernedTargetSet,
    InvestmentAnalysisCurrentIndexEntry, InvestmentAnalysisId, InvestmentOutcomeProjection,
    InvestmentProposalDecision, InvestmentProposalId, InvestmentProposalIndexEntry,
    InvestmentSizingProjection, InvestmentTargetSetId, PublishedInvestmentAnalysis,
    RecommendationOutcomeCurrentIndexEntry, RecommendationOutcomeSeriesId,
    RecommendationOutcomeStatusRecord, RecommendationTrackRecord, SavedScreen, ScreenExecution,
    ScreenId, ScreenRun, ScreenRunId, TargetIndexEntry, TargetInvalidation, TargetReview,
    TargetState, TargetStatus, candidate::execute,
};

/// One transport-neutral workflow authority. Mutation requires exclusive access to the sole writer.
#[derive(Debug)]
pub struct DecisionAuthority {
    repository: DecisionRepository,
}

impl DecisionAuthority {
    /// Installs a previously created or recovered sole repository writer.
    #[must_use]
    pub const fn new(repository: DecisionRepository) -> Self {
        Self { repository }
    }

    /// Read-only repository access for bounded presentation and persistence snapshots.
    #[must_use]
    pub const fn repository(&self) -> &DecisionRepository {
        &self.repository
    }

    /// Saves one compare-and-append screen revision.
    pub fn save_screen(
        &mut self,
        expected: Option<RevisionNumber>,
        screen: SavedScreen,
    ) -> Result<AppendOutcome, DecisionRepositoryError> {
        self.repository.append_screen(expected, screen)
    }

    /// Executes closed screen semantics and atomically retains the exact run and candidates.
    pub fn run_screen(
        &mut self,
        run: ScreenRun,
        inputs: Vec<CandidateInput>,
        selected_at: Timestamp,
    ) -> Result<ScreenExecution, DecisionRepositoryError> {
        let screen = self
            .repository
            .screen(run.screen().id(), run.screen().revision())
            .cloned()
            .ok_or(DecisionRepositoryError::NotFound)?;
        let execution = execute(&screen, run, inputs, selected_at)
            .map_err(|_error| DecisionRepositoryError::EvidenceMismatch)?;
        self.repository.append_screen_execution(execution.clone())?;
        Ok(execution)
    }

    /// Lists immutable saved-screen revisions without allocation.
    pub fn list_screens(&self) -> impl Iterator<Item = &SavedScreen> {
        self.repository.screens()
    }

    /// Returns the ranked candidates for one exact run.
    pub fn get_candidates(
        &self,
        run_id: &ScreenRunId,
    ) -> Result<&[CandidateAssessment], DecisionRepositoryError> {
        self.repository
            .screen_execution(run_id)
            .map(ScreenExecution::candidates)
            .ok_or(DecisionRepositoryError::NotFound)
    }

    /// Returns one globally unique retained candidate and its immutable parent run.
    pub fn get_candidate(
        &self,
        candidate_id: &crate::CandidateId,
    ) -> Result<(&ScreenRun, &CandidateAssessment), DecisionRepositoryError> {
        self.repository
            .candidate(candidate_id)
            .ok_or(DecisionRepositoryError::NotFound)
    }

    /// Lists bounded retained saved-screen runs for discovery before exact candidate lookup.
    pub fn list_screen_runs(
        &self,
        maximum: usize,
    ) -> Result<Vec<crate::ScreenRunIndexEntry>, DecisionRepositoryError> {
        self.repository.list_screen_runs(maximum)
    }

    /// Continues bounded screen-run discovery after one exact retained run identity.
    pub fn list_screen_runs_after(
        &self,
        after: Option<&ScreenRunId>,
        maximum: usize,
    ) -> Result<Vec<crate::ScreenRunIndexEntry>, DecisionRepositoryError> {
        self.repository.list_screen_runs_after(after, maximum)
    }

    /// Returns one immutable reference-only dossier.
    pub fn get_dossier(
        &self,
        id: &crate::DossierId,
    ) -> Result<&DecisionDossier, DecisionRepositoryError> {
        self.repository
            .dossier(id)
            .ok_or(DecisionRepositoryError::NotFound)
    }

    /// Appends one reference-only dossier.
    pub fn append_dossier(
        &mut self,
        dossier: DecisionDossier,
    ) -> Result<AppendOutcome, DecisionRepositoryError> {
        self.repository.append_dossier(dossier)
    }

    /// Lists bounded immutable dossiers assembled for one exact candidate.
    pub fn list_candidate_dossiers(
        &self,
        candidate_id: &crate::CandidateId,
        maximum: usize,
    ) -> Result<Vec<DecisionDossier>, DecisionRepositoryError> {
        self.repository
            .dossiers_for_candidate(candidate_id, maximum)
    }

    /// Continues dossier discovery for one candidate after one exact retained dossier identity.
    pub fn list_candidate_dossiers_after(
        &self,
        candidate_id: &crate::CandidateId,
        after: Option<&crate::DossierId>,
        maximum: usize,
    ) -> Result<Vec<DecisionDossier>, DecisionRepositoryError> {
        self.repository
            .dossiers_for_candidate_after(candidate_id, after, maximum)
    }

    /// Creates revision one of an investment target series.
    pub fn create_target(
        &mut self,
        target: GovernedTargetSet,
    ) -> Result<AppendOutcome, DecisionRepositoryError> {
        self.repository.append_target(None, target)
    }

    /// Returns one exact immutable target revision.
    pub fn get_target(
        &self,
        id: &InvestmentTargetSetId,
        revision: RevisionNumber,
    ) -> Result<TargetState, DecisionRepositoryError> {
        self.repository.target_state(id, revision)
    }

    /// Lists immutable revisions for a target series.
    pub fn list_targets<'a>(
        &'a self,
        id: &'a InvestmentTargetSetId,
    ) -> impl Iterator<Item = &'a GovernedTargetSet> + 'a {
        self.repository.target_revisions(id)
    }

    /// Lists one locator for each discovered target series at its current immutable revision.
    pub fn list_target_index(
        &self,
        maximum: usize,
    ) -> Result<Vec<TargetIndexEntry>, DecisionRepositoryError> {
        self.repository.list_target_index(maximum)
    }

    /// Continues target-series discovery after one exact retained target-series identity.
    pub fn list_target_index_after(
        &self,
        after: Option<&InvestmentTargetSetId>,
        maximum: usize,
    ) -> Result<Vec<TargetIndexEntry>, DecisionRepositoryError> {
        self.repository.list_target_index_after(after, maximum)
    }

    /// Appends explicit review evidence; activation is impossible without this operation.
    pub fn review_target(
        &mut self,
        review: TargetReview,
    ) -> Result<AppendOutcome, DecisionRepositoryError> {
        self.repository.append_review(review)
    }

    /// Compare-and-appends one immutable successor revision.
    pub fn reevaluate_target(
        &mut self,
        expected: RevisionNumber,
        successor: GovernedTargetSet,
    ) -> Result<AppendOutcome, DecisionRepositoryError> {
        self.repository.append_target(Some(expected), successor)
    }

    /// Appends idempotent invalidation evidence without changing target or review bytes.
    pub fn invalidate_target(
        &mut self,
        invalidation: TargetInvalidation,
    ) -> Result<AppendOutcome, DecisionRepositoryError> {
        self.repository.append_invalidation(invalidation)
    }

    /// Appends one immutable proposal result after pure recommendation-authority recomputation.
    pub fn append_investment_proposal(
        &mut self,
        decision: InvestmentProposalDecision,
    ) -> Result<AppendOutcome, DecisionRepositoryError> {
        self.repository.append_investment_proposal(decision)
    }

    /// Appends one immutable analytical-profile/workflow publication binding.
    pub fn append_investment_analysis_publication(
        &mut self,
        publication: PublishedInvestmentAnalysis,
    ) -> Result<AppendOutcome, DecisionRepositoryError> {
        self.repository
            .append_investment_analysis_publication(publication)
    }

    /// Appends one immutable proposal-bound outcome projection.
    pub fn append_investment_outcome_projection(
        &mut self,
        projection: InvestmentOutcomeProjection,
    ) -> Result<AppendOutcome, DecisionRepositoryError> {
        self.repository
            .append_investment_outcome_projection(projection)
    }

    /// Appends one immutable proposal-bound sizing projection.
    pub fn append_investment_sizing_projection(
        &mut self,
        projection: InvestmentSizingProjection,
    ) -> Result<AppendOutcome, DecisionRepositoryError> {
        self.repository
            .append_investment_sizing_projection(projection)
    }

    /// Appends one contiguous recommendation-outcome status revision.
    pub fn append_recommendation_outcome_status(
        &mut self,
        status: RecommendationOutcomeStatusRecord,
    ) -> Result<AppendOutcome, DecisionRepositoryError> {
        self.repository.append_recommendation_outcome_status(status)
    }

    /// Returns one exact generated, no-action, or unavailable investment-analysis result.
    pub fn get_investment_proposal(
        &self,
        analysis_id: InvestmentAnalysisId,
    ) -> Result<&InvestmentProposalDecision, DecisionRepositoryError> {
        self.repository
            .investment_proposal(analysis_id)
            .ok_or(DecisionRepositoryError::NotFound)
    }

    /// Returns the immutable profile/workflow publication for one analysis.
    pub fn get_investment_analysis_publication(
        &self,
        analysis_id: InvestmentAnalysisId,
    ) -> Result<&PublishedInvestmentAnalysis, DecisionRepositoryError> {
        self.repository
            .investment_analysis_publication(analysis_id)
            .ok_or(DecisionRepositoryError::NotFound)
    }

    /// Returns the exact persisted outcome projection for one generated proposal.
    pub fn get_investment_outcome_projection(
        &self,
        proposal_id: InvestmentProposalId,
    ) -> Result<&InvestmentOutcomeProjection, DecisionRepositoryError> {
        self.repository
            .investment_outcome_projection(proposal_id)
            .ok_or(DecisionRepositoryError::NotFound)
    }

    /// Returns the exact persisted sizing projection for one generated proposal.
    pub fn get_investment_sizing_projection(
        &self,
        proposal_id: InvestmentProposalId,
    ) -> Result<&InvestmentSizingProjection, DecisionRepositoryError> {
        self.repository
            .investment_sizing_projection(proposal_id)
            .ok_or(DecisionRepositoryError::NotFound)
    }

    /// Returns the latest status for one recommendation-outcome series.
    pub fn get_recommendation_outcome_current(
        &self,
        series_id: RecommendationOutcomeSeriesId,
    ) -> Result<&RecommendationOutcomeCurrentIndexEntry, DecisionRepositoryError> {
        self.repository
            .recommendation_outcome_current(series_id)
            .ok_or(DecisionRepositoryError::NotFound)
    }

    /// Returns one analysis currentness entry without append-order ranking.
    pub fn get_investment_analysis_current(
        &self,
        analysis_id: InvestmentAnalysisId,
    ) -> Result<&InvestmentAnalysisCurrentIndexEntry, DecisionRepositoryError> {
        self.repository
            .investment_analysis_current(analysis_id)
            .ok_or(DecisionRepositoryError::NotFound)
    }

    /// Computes action-separated current-status performance for one profile and horizon.
    pub fn recommendation_track_record(
        &self,
        profile: &AnalyticalProfileBindingReference,
        horizon_nanos: i64,
        evaluated_at: Timestamp,
    ) -> Result<RecommendationTrackRecord, DecisionRepositoryError> {
        self.repository
            .recommendation_track_record(profile, horizon_nanos, evaluated_at)
    }

    /// Lists bounded immutable investment-analysis locators in durable append order.
    pub fn list_investment_proposal_index(
        &self,
        maximum: usize,
    ) -> Result<Vec<InvestmentProposalIndexEntry>, DecisionRepositoryError> {
        self.repository.list_investment_proposal_index(maximum)
    }

    /// Continues investment-analysis discovery after one exact retained analysis identity.
    pub fn list_investment_proposal_index_after(
        &self,
        after: Option<InvestmentAnalysisId>,
        maximum: usize,
    ) -> Result<Vec<InvestmentProposalIndexEntry>, DecisionRepositoryError> {
        self.repository
            .list_investment_proposal_index_after(after, maximum)
    }

    /// Derives the current status of one exact revision from append-only lifecycle evidence.
    pub fn target_status(
        &self,
        id: &InvestmentTargetSetId,
        revision: RevisionNumber,
    ) -> Result<TargetStatus, DecisionRepositoryError> {
        self.repository.target_status(id, revision)
    }

    /// Consumes this authority and returns the sole repository writer for controlled shutdown.
    #[must_use]
    pub fn into_repository(self) -> DecisionRepository {
        self.repository
    }

    /// Returns one exact saved screen.
    pub fn get_screen(
        &self,
        id: &ScreenId,
        revision: RevisionNumber,
    ) -> Result<&SavedScreen, DecisionRepositoryError> {
        self.repository
            .screen(id, revision)
            .ok_or(DecisionRepositoryError::NotFound)
    }
}
