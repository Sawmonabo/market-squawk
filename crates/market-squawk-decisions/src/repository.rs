//! Bounded single-writer append journal and deterministic restart recovery.

use std::fmt;

use crate::{
    CandidateId, DecisionDossier, GovernedTargetSet, InvestmentTargetSetId, SavedScreen,
    ScreenExecution, ScreenId, ScreenRun, ScreenRunId, TargetInvalidation, TargetReview,
    TargetReviewDisposition, TargetState, TargetStatus,
};
use market_squawk_domain::{InstrumentId, RevisionNumber};

const MAX_REPOSITORY_LIMIT: usize = 65_536;
const MAX_REPOSITORY_CANDIDATES: usize = 1_000_000;

/// Fixed resource ceilings for the in-process decision index.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DecisionRepositoryLimits {
    maximum_screen_revisions: usize,
    maximum_screen_runs: usize,
    maximum_candidates_per_run: usize,
    maximum_dossiers: usize,
    maximum_target_revisions: usize,
    maximum_reviews: usize,
    maximum_invalidations: usize,
    maximum_records: usize,
}

impl DecisionRepositoryLimits {
    /// Constructs positive limits below the process-wide hard ceiling.
    #[allow(
        clippy::too_many_arguments,
        reason = "each independently bounded persisted record family remains explicit"
    )]
    pub fn try_new(
        maximum_screen_revisions: usize,
        maximum_screen_runs: usize,
        maximum_candidates_per_run: usize,
        maximum_dossiers: usize,
        maximum_target_revisions: usize,
        maximum_reviews: usize,
        maximum_invalidations: usize,
    ) -> Result<Self, DecisionRepositoryError> {
        let values = [
            maximum_screen_revisions,
            maximum_screen_runs,
            maximum_candidates_per_run,
            maximum_dossiers,
            maximum_target_revisions,
            maximum_reviews,
            maximum_invalidations,
        ];
        if values
            .into_iter()
            .any(|value| value == 0 || value > MAX_REPOSITORY_LIMIT)
        {
            return Err(DecisionRepositoryError::InvalidLimits);
        }
        let maximum_records = maximum_screen_revisions
            .checked_add(maximum_screen_runs)
            .and_then(|value| value.checked_add(maximum_dossiers))
            .and_then(|value| value.checked_add(maximum_target_revisions))
            .and_then(|value| value.checked_add(maximum_reviews))
            .and_then(|value| value.checked_add(maximum_invalidations))
            .ok_or(DecisionRepositoryError::InvalidLimits)?;
        let maximum_candidates = maximum_screen_runs
            .checked_mul(maximum_candidates_per_run)
            .ok_or(DecisionRepositoryError::InvalidLimits)?;
        if maximum_records > MAX_REPOSITORY_LIMIT || maximum_candidates > MAX_REPOSITORY_CANDIDATES
        {
            return Err(DecisionRepositoryError::InvalidLimits);
        }
        Ok(Self {
            maximum_screen_revisions,
            maximum_screen_runs,
            maximum_candidates_per_run,
            maximum_dossiers,
            maximum_target_revisions,
            maximum_reviews,
            maximum_invalidations,
            maximum_records,
        })
    }
}

/// Result of an idempotent append.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AppendOutcome {
    /// A new immutable record was appended.
    Appended,
    /// The exact same stable identity and immutable content was already present.
    AlreadyPresent,
}

/// Bounded discovery projection for one immutable saved-screen run.
///
/// It provides the exact retained run identity and candidate count, but never republishes candidate
/// inputs, formulas, queries, or execution authority.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ScreenRunIndexEntry {
    run: ScreenRun,
    candidate_count: usize,
}

impl ScreenRunIndexEntry {
    fn new(run: ScreenRun, candidate_count: usize) -> Self {
        Self {
            run,
            candidate_count,
        }
    }

    /// Exact immutable point-in-time screen run.
    #[must_use]
    pub const fn run(&self) -> &ScreenRun {
        &self.run
    }

    /// Number of retained selected candidates, not an unbounded source-row count.
    #[must_use]
    pub const fn candidate_count(&self) -> usize {
        self.candidate_count
    }
}

/// Bounded discovery projection for the current immutable head of one target series.
///
/// The entry is a research/read-side locator only. It is neither a rebalance target nor an order.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TargetIndexEntry {
    id: InvestmentTargetSetId,
    revision: RevisionNumber,
    instrument_id: InstrumentId,
    status: TargetStatus,
}

impl TargetIndexEntry {
    fn new(
        id: InvestmentTargetSetId,
        revision: RevisionNumber,
        instrument_id: InstrumentId,
        status: TargetStatus,
    ) -> Self {
        Self {
            id,
            revision,
            instrument_id,
            status,
        }
    }

    /// Stable target-series identity.
    #[must_use]
    pub const fn id(&self) -> &InvestmentTargetSetId {
        &self.id
    }

    /// Latest immutable revision discovered for the series.
    #[must_use]
    pub const fn revision(&self) -> RevisionNumber {
        self.revision
    }

    /// Instrument supported by this research judgment.
    #[must_use]
    pub const fn instrument_id(&self) -> InstrumentId {
        self.instrument_id
    }

    /// Append-derived current review state for the exact indexed revision.
    #[must_use]
    pub const fn status(&self) -> TargetStatus {
        self.status
    }
}

/// One typed persisted record. No variant contains a path, query, formula, credential, or order.
#[derive(Clone, Debug, PartialEq)]
#[allow(
    clippy::large_enum_variant,
    reason = "journal records retain owned typed values without an extra infallible box allocation per append"
)]
pub enum DecisionRecord {
    /// Immutable saved-screen revision.
    Screen(SavedScreen),
    /// Exact point-in-time run and bounded ranked candidates.
    ScreenExecution(ScreenExecution),
    /// Immutable reference-only dossier.
    Dossier(DecisionDossier),
    /// Immutable governed target revision.
    Target(GovernedTargetSet),
    /// Immutable explicit review.
    Review(TargetReview),
    /// Immutable invalidation evidence.
    Invalidation(TargetInvalidation),
}

/// Fully typed restart payload. A durable application adapter controls byte persistence.
#[derive(Clone, Debug, PartialEq)]
pub struct DecisionJournalSnapshot {
    records: Box<[DecisionRecord]>,
}

impl DecisionJournalSnapshot {
    /// Admits a bounded decoded journal for ordinary invariant-checked recovery.
    pub fn try_from_records(records: Vec<DecisionRecord>) -> Result<Self, DecisionRepositoryError> {
        if records.len() > MAX_REPOSITORY_LIMIT {
            return Err(DecisionRepositoryError::Capacity);
        }
        Ok(Self {
            records: records.into_boxed_slice(),
        })
    }

    /// Ordered immutable records.
    #[must_use]
    pub fn records(&self) -> &[DecisionRecord] {
        &self.records
    }
}

/// Repository validation or compare-and-append failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DecisionRepositoryError {
    /// A configured count limit was zero, excessive, or overflowed.
    InvalidLimits,
    /// A fixed record-family capacity would be exceeded.
    Capacity,
    /// A stable identity already names different immutable content.
    Conflict,
    /// The expected head revision does not equal the current head.
    StaleRevision,
    /// A referenced saved screen, run, candidate, dossier, target, or revision does not exist.
    NotFound,
    /// A supplied identity or semantic binding does not match its authoritative parent.
    EvidenceMismatch,
    /// Fallible retained-memory allocation failed before mutation.
    Allocation,
}

impl fmt::Display for DecisionRepositoryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidLimits => "decision repository limits are invalid",
            Self::Capacity => "decision repository capacity is exhausted",
            Self::Conflict => "decision record identity conflicts with retained content",
            Self::StaleRevision => "decision append expected a different head revision",
            Self::NotFound => "referenced decision record was not found",
            Self::EvidenceMismatch => "decision evidence does not match its authoritative parent",
            Self::Allocation => "decision repository allocation failed",
        })
    }
}

impl std::error::Error for DecisionRepositoryError {}

/// One-writer, compare-and-append repository with scan-only bounded indexes.
#[derive(Debug)]
pub struct DecisionRepository {
    limits: DecisionRepositoryLimits,
    records: Vec<DecisionRecord>,
    target_index: Vec<TargetIndexEntry>,
}

impl DecisionRepository {
    /// Preallocates the complete journal capacity before accepting writes.
    pub fn try_new(limits: DecisionRepositoryLimits) -> Result<Self, DecisionRepositoryError> {
        let mut records = Vec::new();
        records
            .try_reserve_exact(limits.maximum_records)
            .map_err(|_error| DecisionRepositoryError::Allocation)?;
        let mut target_index = Vec::new();
        target_index
            .try_reserve_exact(limits.maximum_target_revisions)
            .map_err(|_error| DecisionRepositoryError::Allocation)?;
        Ok(Self {
            limits,
            records,
            target_index,
        })
    }

    /// Replays a typed snapshot through every ordinary append invariant.
    pub fn recover(
        limits: DecisionRepositoryLimits,
        snapshot: DecisionJournalSnapshot,
    ) -> Result<Self, DecisionRepositoryError> {
        let mut repository = Self::try_new(limits)?;
        for record in snapshot.records.into_vec() {
            match record {
                DecisionRecord::Screen(value) => {
                    let expected = repository.screen_head(value.revision().id());
                    repository.append_screen(expected, value)?;
                }
                DecisionRecord::ScreenExecution(value) => {
                    repository.append_screen_execution(value)?;
                }
                DecisionRecord::Dossier(value) => {
                    repository.append_dossier(value)?;
                }
                DecisionRecord::Target(value) => {
                    let expected = repository.target_head(value.target().id());
                    repository.append_target(expected, value)?;
                }
                DecisionRecord::Review(value) => {
                    repository.append_review(value)?;
                }
                DecisionRecord::Invalidation(value) => {
                    repository.append_invalidation(value)?;
                }
            }
        }
        Ok(repository)
    }

    /// Clones a bounded restart snapshot with fallible preallocation.
    pub fn try_snapshot(&self) -> Result<DecisionJournalSnapshot, DecisionRepositoryError> {
        let mut records = Vec::new();
        records
            .try_reserve_exact(self.records.len())
            .map_err(|_error| DecisionRepositoryError::Allocation)?;
        records.extend(self.records.iter().cloned());
        Ok(DecisionJournalSnapshot {
            records: records.into_boxed_slice(),
        })
    }

    /// Consumes the one-writer repository into its restart payload without copying.
    #[must_use]
    pub fn into_snapshot(self) -> DecisionJournalSnapshot {
        DecisionJournalSnapshot {
            records: self.records.into_boxed_slice(),
        }
    }

    /// Compare-and-appends an immutable screen revision.
    pub fn append_screen(
        &mut self,
        expected: Option<RevisionNumber>,
        screen: SavedScreen,
    ) -> Result<AppendOutcome, DecisionRepositoryError> {
        if let Some(existing) = self.screen(screen.revision().id(), screen.revision().revision()) {
            return if existing == &screen {
                Ok(AppendOutcome::AlreadyPresent)
            } else {
                Err(DecisionRepositoryError::Conflict)
            };
        }
        let current = self.screen_head(screen.revision().id());
        if current != expected {
            return Err(DecisionRepositoryError::StaleRevision);
        }
        let required = current
            .map_or(Some(1), |revision| revision.get().checked_add(1))
            .ok_or(DecisionRepositoryError::StaleRevision)?;
        if screen.revision().revision().get() != required
            || self.screen_count() >= self.limits.maximum_screen_revisions
        {
            return Err(
                if self.screen_count() >= self.limits.maximum_screen_revisions {
                    DecisionRepositoryError::Capacity
                } else {
                    DecisionRepositoryError::StaleRevision
                },
            );
        }
        self.push(DecisionRecord::Screen(screen))?;
        Ok(AppendOutcome::Appended)
    }

    /// Appends one exact run and its complete candidate batch atomically.
    pub fn append_screen_execution(
        &mut self,
        execution: ScreenExecution,
    ) -> Result<AppendOutcome, DecisionRepositoryError> {
        if let Some(existing) = self.screen_execution(execution.run().id()) {
            return if existing == &execution {
                Ok(AppendOutcome::AlreadyPresent)
            } else {
                Err(DecisionRepositoryError::Conflict)
            };
        }
        let Some(screen) = self.screen(
            execution.run().screen().id(),
            execution.run().screen().revision(),
        ) else {
            return Err(DecisionRepositoryError::NotFound);
        };
        if execution.run().universe_identity() != screen.universe_identity()
            || execution.run().feature_bindings() != screen.feature_bindings()
            || execution.candidates().len() > self.limits.maximum_candidates_per_run
            || execution.candidates().iter().any(|candidate| {
                candidate.record().screen_run_id() != execution.run().id()
                    || candidate.record().screen() != execution.run().screen()
            })
        {
            return Err(DecisionRepositoryError::EvidenceMismatch);
        }
        if execution
            .candidates()
            .iter()
            .any(|candidate| self.candidate(candidate.record().id()).is_some())
        {
            return Err(DecisionRepositoryError::Conflict);
        }
        if self.screen_run_count() >= self.limits.maximum_screen_runs {
            return Err(DecisionRepositoryError::Capacity);
        }
        self.push(DecisionRecord::ScreenExecution(execution))?;
        Ok(AppendOutcome::Appended)
    }

    /// Appends one dossier only when its candidate and instrument match an exact retained run.
    pub fn append_dossier(
        &mut self,
        dossier: DecisionDossier,
    ) -> Result<AppendOutcome, DecisionRepositoryError> {
        if let Some(existing) = self.dossier(dossier.dossier().id()) {
            return if existing == &dossier {
                Ok(AppendOutcome::AlreadyPresent)
            } else {
                Err(DecisionRepositoryError::Conflict)
            };
        }
        let candidate = self
            .records
            .iter()
            .filter_map(|record| match record {
                DecisionRecord::ScreenExecution(execution) => Some(execution.candidates()),
                _ => None,
            })
            .flatten()
            .find(|candidate| candidate.record().id() == dossier.dossier().candidate_id())
            .ok_or(DecisionRepositoryError::NotFound)?;
        if candidate.record().instrument_id() != dossier.dossier().instrument_id() {
            return Err(DecisionRepositoryError::EvidenceMismatch);
        }
        if self.dossier_count() >= self.limits.maximum_dossiers {
            return Err(DecisionRepositoryError::Capacity);
        }
        self.push(DecisionRecord::Dossier(dossier))?;
        Ok(AppendOutcome::Appended)
    }

    /// Compare-and-appends an immutable governed target revision.
    pub fn append_target(
        &mut self,
        expected: Option<RevisionNumber>,
        target: GovernedTargetSet,
    ) -> Result<AppendOutcome, DecisionRepositoryError> {
        if let Some(existing) =
            self.target_revision(target.target().id(), target.target().revision())
        {
            return if existing == &target {
                Ok(AppendOutcome::AlreadyPresent)
            } else {
                Err(DecisionRepositoryError::Conflict)
            };
        }
        let current = self.target_head(target.target().id());
        if current != expected {
            return Err(DecisionRepositoryError::StaleRevision);
        }
        let required = current
            .map_or(Some(1), |revision| revision.get().checked_add(1))
            .ok_or(DecisionRepositoryError::StaleRevision)?;
        if target.target().revision().get() != required {
            return Err(DecisionRepositoryError::StaleRevision);
        }
        if let Some(prior) = current {
            let Some(previous) = self.target_revision(target.target().id(), prior) else {
                return Err(DecisionRepositoryError::NotFound);
            };
            if previous.target().instrument_id() != target.target().instrument_id()
                || target.supersedes().map(|value| value.0) != Some(prior)
            {
                return Err(DecisionRepositoryError::EvidenceMismatch);
            }
        }
        if self.target_count() >= self.limits.maximum_target_revisions {
            return Err(DecisionRepositoryError::Capacity);
        }
        let indexed = TargetIndexEntry::new(
            target.target().id().clone(),
            target.target().revision(),
            target.target().instrument_id(),
            TargetStatus::PendingReview,
        );
        self.push(DecisionRecord::Target(target))?;
        self.replace_target_index(indexed);
        Ok(AppendOutcome::Appended)
    }

    /// Appends explicit review evidence for the current exact revision.
    pub fn append_review(
        &mut self,
        review: TargetReview,
    ) -> Result<AppendOutcome, DecisionRepositoryError> {
        if let Some(existing) = self.records.iter().find_map(|record| match record {
            DecisionRecord::Review(existing) if existing.id() == review.id() => Some(existing),
            _ => None,
        }) {
            return if existing == &review {
                Ok(AppendOutcome::AlreadyPresent)
            } else {
                Err(DecisionRepositoryError::Conflict)
            };
        }
        let target = self
            .target_revision(review.target_id(), review.target_revision())
            .ok_or(DecisionRepositoryError::NotFound)?;
        let latest_invalidation = self
            .invalidations(review.target_id(), review.target_revision())
            .map(TargetInvalidation::observed_at)
            .max();
        if self.target_head(review.target_id()) != Some(review.target_revision())
            || (matches!(review.disposition(), TargetReviewDisposition::Activate)
                && (review.reviewed_at() < target.effective_at()
                    || latest_invalidation
                        .is_some_and(|observed_at| review.reviewed_at() < observed_at)))
        {
            return Err(DecisionRepositoryError::EvidenceMismatch);
        }
        if self.review_count() >= self.limits.maximum_reviews {
            return Err(DecisionRepositoryError::Capacity);
        }
        let status = match review.disposition() {
            TargetReviewDisposition::Activate => TargetStatus::Active,
            TargetReviewDisposition::Reject => TargetStatus::Rejected,
            TargetReviewDisposition::NeedsChanges => TargetStatus::NeedsChanges,
        };
        let target_id = review.target_id().clone();
        let revision = review.target_revision();
        self.push(DecisionRecord::Review(review))?;
        self.set_index_status(&target_id, revision, status);
        Ok(AppendOutcome::Appended)
    }

    /// Appends invalidation evidence idempotently without changing a target or approval record.
    pub fn append_invalidation(
        &mut self,
        invalidation: TargetInvalidation,
    ) -> Result<AppendOutcome, DecisionRepositoryError> {
        if let Some(existing) = self.records.iter().find_map(|record| match record {
            DecisionRecord::Invalidation(existing) if existing.id() == invalidation.id() => {
                Some(existing)
            }
            _ => None,
        }) {
            return if existing == &invalidation {
                Ok(AppendOutcome::AlreadyPresent)
            } else {
                Err(DecisionRepositoryError::Conflict)
            };
        }
        if self
            .target_revision(invalidation.target_id(), invalidation.target_revision())
            .is_none()
        {
            return Err(DecisionRepositoryError::NotFound);
        }
        if self.invalidation_count() >= self.limits.maximum_invalidations {
            return Err(DecisionRepositoryError::Capacity);
        }
        let target_id = invalidation.target_id().clone();
        let revision = invalidation.target_revision();
        self.push(DecisionRecord::Invalidation(invalidation))?;
        self.set_index_status(&target_id, revision, TargetStatus::NeedsReview);
        Ok(AppendOutcome::Appended)
    }

    /// Lists screen revisions in append order without allocating.
    pub fn screens(&self) -> impl Iterator<Item = &SavedScreen> {
        self.records.iter().filter_map(|record| match record {
            DecisionRecord::Screen(screen) => Some(screen),
            _ => None,
        })
    }

    /// Finds one exact saved-screen revision.
    pub fn screen(&self, id: &ScreenId, revision: RevisionNumber) -> Option<&SavedScreen> {
        self.screens()
            .find(|screen| screen.revision().id() == id && screen.revision().revision() == revision)
    }

    /// Finds one exact screen execution.
    pub fn screen_execution(&self, id: &ScreenRunId) -> Option<&ScreenExecution> {
        self.records.iter().find_map(|record| match record {
            DecisionRecord::ScreenExecution(execution) if execution.run().id() == id => {
                Some(execution)
            }
            _ => None,
        })
    }

    /// Finds one globally unique candidate together with its immutable parent run.
    ///
    /// Dossiers identify candidates independently of a run coordinate, so accepting the same
    /// candidate identity in two runs would make downstream evidence resolution ambiguous.
    pub fn candidate(&self, id: &CandidateId) -> Option<(&ScreenRun, &crate::CandidateAssessment)> {
        self.records.iter().find_map(|record| match record {
            DecisionRecord::ScreenExecution(execution) => execution
                .candidates()
                .iter()
                .find(|candidate| candidate.record().id() == id)
                .map(|candidate| (execution.run(), candidate)),
            _ => None,
        })
    }

    /// Lists bounded saved-screen run locators in durable append order.
    ///
    /// The caller must select a positive presentation bound. This exposes only retained run
    /// identities and selected-candidate counts; exact candidates remain a separate lookup.
    pub fn list_screen_runs(
        &self,
        maximum: usize,
    ) -> Result<Vec<ScreenRunIndexEntry>, DecisionRepositoryError> {
        self.list_screen_runs_after(None, maximum)
    }

    /// Continues bounded saved-screen run discovery strictly after one exact retained run identity.
    ///
    /// A supplied cursor must name a retained execution. Unknown cursors fail closed rather than
    /// restarting at the beginning and presenting a misleading duplicate page.
    pub fn list_screen_runs_after(
        &self,
        after: Option<&ScreenRunId>,
        maximum: usize,
    ) -> Result<Vec<ScreenRunIndexEntry>, DecisionRepositoryError> {
        if maximum == 0 {
            return Err(DecisionRepositoryError::InvalidLimits);
        }
        let count = self.screen_run_count().min(maximum);
        let mut result = Vec::new();
        result
            .try_reserve_exact(count)
            .map_err(|_error| DecisionRepositoryError::Allocation)?;
        let mut executions = self.records.iter().filter_map(|record| match record {
            DecisionRecord::ScreenExecution(execution) => Some(execution),
            _ => None,
        });
        if let Some(after) = after
            && executions
                .position(|execution| execution.run().id() == after)
                .is_none()
        {
            return Err(DecisionRepositoryError::NotFound);
        }
        result.extend(executions.take(maximum).map(|execution| {
            ScreenRunIndexEntry::new(execution.run().clone(), execution.candidates().len())
        }));
        Ok(result)
    }

    /// Finds one immutable dossier.
    pub fn dossier(&self, id: &crate::DossierId) -> Option<&DecisionDossier> {
        self.records.iter().find_map(|record| match record {
            DecisionRecord::Dossier(dossier) if dossier.dossier().id() == id => Some(dossier),
            _ => None,
        })
    }

    /// Lists bounded dossiers assembled for one exact retained candidate.
    ///
    /// Candidate-to-dossier discovery is derived from immutable retained identities rather than a
    /// caller-supplied relationship or a second mutable index.
    pub fn dossiers_for_candidate(
        &self,
        candidate_id: &CandidateId,
        maximum: usize,
    ) -> Result<Vec<DecisionDossier>, DecisionRepositoryError> {
        self.dossiers_for_candidate_after(candidate_id, None, maximum)
    }

    /// Continues dossier discovery for one candidate strictly after an exact retained dossier.
    ///
    /// The optional cursor is validated against the candidate relationship itself, preventing a
    /// caller from paging one candidate through another candidate's evidence chain.
    pub fn dossiers_for_candidate_after(
        &self,
        candidate_id: &CandidateId,
        after: Option<&crate::DossierId>,
        maximum: usize,
    ) -> Result<Vec<DecisionDossier>, DecisionRepositoryError> {
        if maximum == 0 {
            return Err(DecisionRepositoryError::InvalidLimits);
        }
        let count = self
            .records
            .iter()
            .filter(|record| {
                matches!(record, DecisionRecord::Dossier(dossier)
                    if dossier.dossier().candidate_id() == candidate_id)
            })
            .take(maximum)
            .count();
        let mut result = Vec::new();
        result
            .try_reserve_exact(count)
            .map_err(|_error| DecisionRepositoryError::Allocation)?;
        let mut dossiers = self.records.iter().filter_map(|record| match record {
            DecisionRecord::Dossier(dossier)
                if dossier.dossier().candidate_id() == candidate_id =>
            {
                Some(dossier)
            }
            _ => None,
        });
        if let Some(after) = after
            && dossiers
                .position(|dossier| dossier.dossier().id() == after)
                .is_none()
        {
            return Err(DecisionRepositoryError::NotFound);
        }
        result.extend(dossiers.take(maximum).cloned());
        Ok(result)
    }

    /// Lists immutable revisions for one target series.
    pub fn target_revisions<'a>(
        &'a self,
        id: &'a InvestmentTargetSetId,
    ) -> impl Iterator<Item = &'a GovernedTargetSet> + 'a {
        self.records.iter().filter_map(move |record| match record {
            DecisionRecord::Target(target) if target.target().id() == id => Some(target),
            _ => None,
        })
    }

    /// Finds one exact target revision.
    pub fn target_revision(
        &self,
        id: &InvestmentTargetSetId,
        revision: RevisionNumber,
    ) -> Option<&GovernedTargetSet> {
        self.records.iter().find_map(|record| match record {
            DecisionRecord::Target(target)
                if target.target().id() == id && target.target().revision() == revision =>
            {
                Some(target)
            }
            _ => None,
        })
    }

    /// Lists reviews for one exact target revision without allocating.
    pub fn reviews<'a>(
        &'a self,
        id: &'a InvestmentTargetSetId,
        revision: RevisionNumber,
    ) -> impl Iterator<Item = &'a TargetReview> + 'a {
        self.records.iter().filter_map(move |record| match record {
            DecisionRecord::Review(review)
                if review.target_id() == id && review.target_revision() == revision =>
            {
                Some(review)
            }
            _ => None,
        })
    }

    /// Lists invalidations for one exact target revision without allocating.
    pub fn invalidations<'a>(
        &'a self,
        id: &'a InvestmentTargetSetId,
        revision: RevisionNumber,
    ) -> impl Iterator<Item = &'a TargetInvalidation> + 'a {
        self.records.iter().filter_map(move |record| match record {
            DecisionRecord::Invalidation(invalidation)
                if invalidation.target_id() == id && invalidation.target_revision() == revision =>
            {
                Some(invalidation)
            }
            _ => None,
        })
    }

    /// Derives target status from immutable history; it never rewrites approval or target bytes.
    pub fn target_status(
        &self,
        id: &InvestmentTargetSetId,
        revision: RevisionNumber,
    ) -> Result<TargetStatus, DecisionRepositoryError> {
        if self.target_revision(id, revision).is_none() {
            return Err(DecisionRepositoryError::NotFound);
        }
        if self
            .target_revisions(id)
            .any(|target| target.target().revision().get() > revision.get())
        {
            return Ok(TargetStatus::Superseded);
        }
        let mut status = TargetStatus::PendingReview;
        for record in &self.records {
            match record {
                DecisionRecord::Review(review)
                    if review.target_id() == id && review.target_revision() == revision =>
                {
                    status = match review.disposition() {
                        TargetReviewDisposition::Activate => TargetStatus::Active,
                        TargetReviewDisposition::Reject => TargetStatus::Rejected,
                        TargetReviewDisposition::NeedsChanges => TargetStatus::NeedsChanges,
                    };
                }
                DecisionRecord::Invalidation(invalidation)
                    if invalidation.target_id() == id
                        && invalidation.target_revision() == revision =>
                {
                    status = TargetStatus::NeedsReview;
                }
                _ => {}
            }
        }
        Ok(status)
    }

    /// Builds an owned target read model with reviewer, approval, status, and invalidation facts.
    pub fn target_state(
        &self,
        id: &InvestmentTargetSetId,
        revision: RevisionNumber,
    ) -> Result<TargetState, DecisionRepositoryError> {
        let target = self
            .target_revision(id, revision)
            .cloned()
            .ok_or(DecisionRepositoryError::NotFound)?;
        let latest_review = self.reviews(id, revision).last().cloned();
        let latest_invalidation = self.invalidations(id, revision).last().cloned();
        Ok(TargetState::new(
            target,
            self.target_status(id, revision)?,
            latest_review,
            latest_invalidation,
        ))
    }

    /// Lists at most one bounded locator for each target series, always at its latest revision.
    ///
    /// The index is rebuilt only through the same checked append paths as its immutable source
    /// records, so it cannot become an independently mutable source of target authority.
    pub fn list_target_index(
        &self,
        maximum: usize,
    ) -> Result<Vec<TargetIndexEntry>, DecisionRepositoryError> {
        self.list_target_index_after(None, maximum)
    }

    /// Continues target-series discovery strictly after one exact current index identity.
    ///
    /// The cursor is checked against the in-memory index maintained by immutable append paths. A
    /// target that no longer appears as a current series head therefore cannot be used to restart
    /// discovery ambiguously.
    pub fn list_target_index_after(
        &self,
        after: Option<&InvestmentTargetSetId>,
        maximum: usize,
    ) -> Result<Vec<TargetIndexEntry>, DecisionRepositoryError> {
        if maximum == 0 {
            return Err(DecisionRepositoryError::InvalidLimits);
        }
        let count = self.target_index.len().min(maximum);
        let mut result = Vec::new();
        result
            .try_reserve_exact(count)
            .map_err(|_error| DecisionRepositoryError::Allocation)?;
        let mut entries = self.target_index.iter();
        if let Some(after) = after
            && entries.position(|entry| entry.id() == after).is_none()
        {
            return Err(DecisionRepositoryError::NotFound);
        }
        result.extend(entries.take(maximum).cloned());
        Ok(result)
    }

    fn screen_head(&self, id: &ScreenId) -> Option<RevisionNumber> {
        self.screens()
            .filter(|screen| screen.revision().id() == id)
            .map(|screen| screen.revision().revision())
            .max_by_key(|revision| revision.get())
    }

    fn target_head(&self, id: &InvestmentTargetSetId) -> Option<RevisionNumber> {
        self.target_revisions(id)
            .map(|target| target.target().revision())
            .max_by_key(|revision| revision.get())
    }

    fn replace_target_index(&mut self, entry: TargetIndexEntry) {
        if let Some(existing) = self
            .target_index
            .iter_mut()
            .find(|existing| existing.id == entry.id)
        {
            *existing = entry;
        } else {
            // The vector is reserved to the checked target-revision capacity before any append.
            // One target series cannot outnumber retained target revisions.
            self.target_index.push(entry);
        }
    }

    fn set_index_status(
        &mut self,
        id: &InvestmentTargetSetId,
        revision: RevisionNumber,
        status: TargetStatus,
    ) {
        if let Some(entry) = self
            .target_index
            .iter_mut()
            .find(|entry| entry.id == *id && entry.revision == revision)
        {
            entry.status = status;
        }
    }

    fn push(&mut self, record: DecisionRecord) -> Result<(), DecisionRepositoryError> {
        if self.records.len() >= self.limits.maximum_records {
            return Err(DecisionRepositoryError::Capacity);
        }
        self.records.push(record);
        Ok(())
    }

    fn screen_count(&self) -> usize {
        self.records
            .iter()
            .filter(|record| matches!(record, DecisionRecord::Screen(_)))
            .count()
    }

    fn screen_run_count(&self) -> usize {
        self.records
            .iter()
            .filter(|record| matches!(record, DecisionRecord::ScreenExecution(_)))
            .count()
    }

    fn dossier_count(&self) -> usize {
        self.records
            .iter()
            .filter(|record| matches!(record, DecisionRecord::Dossier(_)))
            .count()
    }

    fn target_count(&self) -> usize {
        self.records
            .iter()
            .filter(|record| matches!(record, DecisionRecord::Target(_)))
            .count()
    }

    fn review_count(&self) -> usize {
        self.records
            .iter()
            .filter(|record| matches!(record, DecisionRecord::Review(_)))
            .count()
    }

    fn invalidation_count(&self) -> usize {
        self.records
            .iter()
            .filter(|record| matches!(record, DecisionRecord::Invalidation(_)))
            .count()
    }
}
