//! Typed decision workflow authority over the sole bounded repository writer.

use market_squawk_domain::{RevisionNumber, Timestamp};

use crate::{
    AppendOutcome, CandidateAssessment, CandidateInput, DecisionDossier, DecisionRepository,
    DecisionRepositoryError, GovernedTargetSet, InvestmentTargetSetId, SavedScreen,
    ScreenExecution, ScreenId, ScreenRun, ScreenRunId, TargetIndexEntry, TargetInvalidation,
    TargetReview, TargetState, TargetStatus, candidate::execute,
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
