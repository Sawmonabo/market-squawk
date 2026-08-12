//! Bounded single-writer append journal and deterministic restart recovery.

use std::fmt;

use crate::{
    AnalyticalProfileBindingReference, CandidateId, DecisionDossier, GovernedTargetSet,
    InvestmentAnalysisId, InvestmentOutcomeProjection, InvestmentProjectionDigest,
    InvestmentProposalAuthority, InvestmentProposalDecision, InvestmentProposalId,
    InvestmentSizingProjection, InvestmentTargetSetId, NoActionReason, ProposalUnavailableReason,
    PublishedInvestmentAnalysis, RecommendationAction, RecommendationDerivationDigest,
    RecommendationEvidenceDigest, RecommendationOutcomeCohort,
    RecommendationOutcomeCurrentIndexEntry, RecommendationOutcomeStatus,
    RecommendationOutcomeStatusRecord, RecommendationPolicyDigest, RecommendationTrackRecord,
    RecommendationTrackRecordGroup, SavedScreen, ScreenExecution, ScreenId, ScreenRun, ScreenRunId,
    TargetInvalidation, TargetReview, TargetReviewDisposition, TargetState, TargetStatus,
};
use market_squawk_domain::{AccountId, Currency, InstrumentId, RevisionNumber, Timestamp};

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
    maximum_investment_proposals: usize,
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
        maximum_investment_proposals: usize,
    ) -> Result<Self, DecisionRepositoryError> {
        let values = [
            maximum_screen_revisions,
            maximum_screen_runs,
            maximum_candidates_per_run,
            maximum_dossiers,
            maximum_target_revisions,
            maximum_reviews,
            maximum_invalidations,
            maximum_investment_proposals,
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
            .and_then(|value| value.checked_add(maximum_investment_proposals))
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
            maximum_investment_proposals,
            maximum_records,
        })
    }

    /// Returns the maximum durable saved-screen executions.
    #[must_use]
    pub const fn maximum_screen_runs(self) -> usize {
        self.maximum_screen_runs
    }

    /// Returns the shared maximum for proposal, publication, projection, and outcome records.
    #[must_use]
    pub const fn maximum_investment_proposals(self) -> usize {
        self.maximum_investment_proposals
    }

    /// Returns the complete domain-record ceiling, excluding application-owned journal rows.
    #[must_use]
    pub const fn maximum_records(self) -> usize {
        self.maximum_records
    }
}

/// Current publication and sidecar locator for one immutable investment analysis.
///
/// Entries are keyed by analysis identity and updated only from validated immutable records. They
/// are not an append-order or profitability ranking.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InvestmentAnalysisCurrentIndexEntry {
    publication: PublishedInvestmentAnalysis,
    outcome_projection_digest: Option<InvestmentProjectionDigest>,
    sizing_projection_digest: Option<InvestmentProjectionDigest>,
    current_outcome: Option<RecommendationOutcomeCurrentIndexEntry>,
}

impl InvestmentAnalysisCurrentIndexEntry {
    fn new(publication: PublishedInvestmentAnalysis) -> Self {
        Self {
            publication,
            outcome_projection_digest: None,
            sizing_projection_digest: None,
            current_outcome: None,
        }
    }

    /// Returns the canonical immutable profile/workflow publication.
    #[must_use]
    pub const fn publication(&self) -> &PublishedInvestmentAnalysis {
        &self.publication
    }

    /// Returns the current exact outcome-projection identity when persisted.
    #[must_use]
    pub const fn outcome_projection_digest(&self) -> Option<InvestmentProjectionDigest> {
        self.outcome_projection_digest
    }

    /// Returns the current exact sizing-projection identity when persisted.
    #[must_use]
    pub const fn sizing_projection_digest(&self) -> Option<InvestmentProjectionDigest> {
        self.sizing_projection_digest
    }

    /// Returns the latest contiguous realized-outcome status when one exists.
    #[must_use]
    pub const fn current_outcome(&self) -> Option<&RecommendationOutcomeCurrentIndexEntry> {
        self.current_outcome.as_ref()
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

/// Closed outcome summary for one immutable investment-analysis locator.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InvestmentProposalIndexOutcome {
    /// Complete evidence produced a position-aware recommendation.
    Generated(RecommendationAction),
    /// Complete evidence required an explicit abstention.
    NoAction(NoActionReason),
    /// Mandatory evidence was missing or inadmissible.
    Unavailable(ProposalUnavailableReason),
}

/// Bounded append-derived locator for one immutable investment-analysis result.
///
/// This projection grants no generation, ranking, current-profile, order, or execution authority.
/// Consumers use its exact analysis identity to retrieve the complete recomputed decision.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InvestmentProposalIndexEntry {
    analysis_id: InvestmentAnalysisId,
    proposal_id: Option<InvestmentProposalId>,
    derivation_digest: Option<RecommendationDerivationDigest>,
    instrument_id: InstrumentId,
    account_id: AccountId,
    currency: Currency,
    as_of: Timestamp,
    horizon_at: Timestamp,
    expires_at: Timestamp,
    policy_digest: RecommendationPolicyDigest,
    evidence_digest: RecommendationEvidenceDigest,
    outcome: InvestmentProposalIndexOutcome,
}

impl InvestmentProposalIndexEntry {
    fn from_decision(decision: &InvestmentProposalDecision) -> Self {
        let evidence = decision.evidence();
        let outcome = match decision {
            InvestmentProposalDecision::Generated(value) => {
                InvestmentProposalIndexOutcome::Generated(value.action())
            }
            InvestmentProposalDecision::NoAction(value) => {
                InvestmentProposalIndexOutcome::NoAction(value.reason())
            }
            InvestmentProposalDecision::Unavailable(value) => {
                InvestmentProposalIndexOutcome::Unavailable(value.reason())
            }
        };
        Self {
            analysis_id: decision.analysis_id(),
            proposal_id: decision.proposal_id(),
            derivation_digest: decision.derivation_digest(),
            instrument_id: evidence.instrument_id(),
            account_id: evidence.account_id(),
            currency: evidence.currency(),
            as_of: evidence.as_of(),
            horizon_at: decision.horizon_at(),
            expires_at: decision.expires_at(),
            policy_digest: decision.policy_digest(),
            evidence_digest: decision.evidence_digest(),
            outcome,
        }
    }

    /// Returns the stable identity shared by generated, no-action, and unavailable results.
    #[must_use]
    pub const fn analysis_id(&self) -> InvestmentAnalysisId {
        self.analysis_id
    }

    /// Returns the proposal identity for generated and no-action results.
    #[must_use]
    pub const fn proposal_id(&self) -> Option<InvestmentProposalId> {
        self.proposal_id
    }

    /// Returns the exact derivation commitment for generated and no-action results.
    #[must_use]
    pub const fn derivation_digest(&self) -> Option<RecommendationDerivationDigest> {
        self.derivation_digest
    }

    /// Returns the stable instrument analyzed by this result.
    #[must_use]
    pub const fn instrument_id(&self) -> InstrumentId {
        self.instrument_id
    }

    /// Returns the exact portfolio account bound into this analysis.
    #[must_use]
    pub const fn account_id(&self) -> AccountId {
        self.account_id
    }

    /// Returns the analysis denomination.
    #[must_use]
    pub const fn currency(&self) -> Currency {
        self.currency
    }

    /// Returns the point-in-time evidence cutoff.
    #[must_use]
    pub const fn as_of(&self) -> Timestamp {
        self.as_of
    }

    /// Returns the exact code-owned analysis horizon.
    #[must_use]
    pub const fn horizon_at(&self) -> Timestamp {
        self.horizon_at
    }

    /// Returns the exclusive result expiry.
    #[must_use]
    pub const fn expires_at(&self) -> Timestamp {
        self.expires_at
    }

    /// Returns the exact code-owned recommendation-policy identity.
    #[must_use]
    pub const fn policy_digest(&self) -> RecommendationPolicyDigest {
        self.policy_digest
    }

    /// Returns the commitment to every supplied or absent evidence field.
    #[must_use]
    pub const fn evidence_digest(&self) -> RecommendationEvidenceDigest {
        self.evidence_digest
    }

    /// Returns the compact typed result family and conclusion.
    #[must_use]
    pub const fn outcome(&self) -> InvestmentProposalIndexOutcome {
        self.outcome
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
    /// Immutable evidence-bound investment-analysis result.
    InvestmentProposal(InvestmentProposalDecision),
    /// Immutable analytical-profile/workflow publication binding.
    InvestmentAnalysisPublication(PublishedInvestmentAnalysis),
    /// Immutable exact mark-relative projection sidecar.
    InvestmentOutcomeProjection(InvestmentOutcomeProjection),
    /// Immutable exact sizing-feasibility sidecar.
    InvestmentSizingProjection(InvestmentSizingProjection),
    /// Immutable pending, unavailable, or completed realized-outcome revision.
    RecommendationOutcomeStatus(RecommendationOutcomeStatusRecord),
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
    /// A referenced screen, run, candidate, dossier, target, revision, or analysis does not exist.
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
    investment_analysis_index: Vec<InvestmentAnalysisCurrentIndexEntry>,
    recommendation_outcome_index: Vec<RecommendationOutcomeCurrentIndexEntry>,
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
        let mut investment_analysis_index = Vec::new();
        investment_analysis_index
            .try_reserve_exact(limits.maximum_investment_proposals)
            .map_err(|_error| DecisionRepositoryError::Allocation)?;
        let mut recommendation_outcome_index = Vec::new();
        recommendation_outcome_index
            .try_reserve_exact(limits.maximum_investment_proposals)
            .map_err(|_error| DecisionRepositoryError::Allocation)?;
        Ok(Self {
            limits,
            records,
            target_index,
            investment_analysis_index,
            recommendation_outcome_index,
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
                DecisionRecord::InvestmentProposal(value) => {
                    repository.append_investment_proposal(value)?;
                }
                DecisionRecord::InvestmentAnalysisPublication(value) => {
                    repository.append_investment_analysis_publication(value)?;
                }
                DecisionRecord::InvestmentOutcomeProjection(value) => {
                    repository.append_investment_outcome_projection(value)?;
                }
                DecisionRecord::InvestmentSizingProjection(value) => {
                    repository.append_investment_sizing_projection(value)?;
                }
                DecisionRecord::RecommendationOutcomeStatus(value) => {
                    repository.append_recommendation_outcome_status(value)?;
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
        let dossier = self
            .dossier(target.target().dossier_id())
            .ok_or(DecisionRepositoryError::NotFound)?;
        if dossier.dossier().instrument_id() != target.target().instrument_id() {
            return Err(DecisionRepositoryError::EvidenceMismatch);
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

    /// Appends one immutable investment-analysis result only after exact authority recomputation.
    ///
    /// The stable analysis identity is unique across generated, no-action, and unavailable results.
    /// Generated and no-action proposal identities are also globally unique. The repository accepts
    /// neither caller-selected output fields nor a typed value that the pure authority cannot
    /// reproduce exactly from its retained policy and evidence.
    pub fn append_investment_proposal(
        &mut self,
        decision: InvestmentProposalDecision,
    ) -> Result<AppendOutcome, DecisionRepositoryError> {
        if let Some(existing) = self.investment_proposal(decision.analysis_id()) {
            return if existing == &decision {
                Ok(AppendOutcome::AlreadyPresent)
            } else {
                Err(DecisionRepositoryError::Conflict)
            };
        }
        if decision.proposal_id().is_some_and(|proposal_id| {
            self.investment_proposals()
                .any(|existing| existing.proposal_id() == Some(proposal_id))
        }) {
            return Err(DecisionRepositoryError::Conflict);
        }
        let regenerated = InvestmentProposalAuthority::generate(
            decision.evidence().clone(),
            decision.policy().clone(),
        )
        .map_err(|_error| DecisionRepositoryError::EvidenceMismatch)?;
        if regenerated != decision {
            return Err(DecisionRepositoryError::EvidenceMismatch);
        }
        if self.investment_record_count() >= self.limits.maximum_investment_proposals {
            return Err(DecisionRepositoryError::Capacity);
        }
        self.push(DecisionRecord::InvestmentProposal(decision))?;
        Ok(AppendOutcome::Appended)
    }

    /// Appends the sole immutable profile/workflow publication for one analysis.
    pub fn append_investment_analysis_publication(
        &mut self,
        publication: PublishedInvestmentAnalysis,
    ) -> Result<AppendOutcome, DecisionRepositoryError> {
        if let Some(existing) = self.investment_analysis_publication(publication.analysis_id()) {
            return if existing == &publication {
                Ok(AppendOutcome::AlreadyPresent)
            } else {
                Err(DecisionRepositoryError::Conflict)
            };
        }
        let decision = self
            .investment_proposal(publication.analysis_id())
            .ok_or(DecisionRepositoryError::NotFound)?;
        let regenerated = PublishedInvestmentAnalysis::try_new(
            decision,
            publication.analytical_profile().clone(),
            publication.workflow().clone(),
            publication.published_at(),
        )
        .map_err(|_error| DecisionRepositoryError::EvidenceMismatch)?;
        if regenerated != publication {
            return Err(DecisionRepositoryError::EvidenceMismatch);
        }
        self.ensure_investment_record_capacity()?;
        self.push(DecisionRecord::InvestmentAnalysisPublication(
            publication.clone(),
        ))?;
        self.investment_analysis_index
            .push(InvestmentAnalysisCurrentIndexEntry::new(publication));
        Ok(AppendOutcome::Appended)
    }

    /// Appends one deterministic generated-proposal outcome projection sidecar.
    pub fn append_investment_outcome_projection(
        &mut self,
        projection: InvestmentOutcomeProjection,
    ) -> Result<AppendOutcome, DecisionRepositoryError> {
        let proposal_id = projection.binding().proposal_id();
        if let Some(existing) = self.investment_outcome_projection(proposal_id) {
            return if existing == &projection {
                Ok(AppendOutcome::AlreadyPresent)
            } else {
                Err(DecisionRepositoryError::Conflict)
            };
        }
        let (analysis_id, generated) = self
            .generated_investment_proposal(proposal_id)
            .ok_or(DecisionRepositoryError::NotFound)?;
        if self.investment_analysis_publication(analysis_id).is_none() {
            return Err(DecisionRepositoryError::NotFound);
        }
        let regenerated =
            InvestmentOutcomeProjection::try_from_proposal(generated, projection.position_scale())
                .map_err(|_error| DecisionRepositoryError::EvidenceMismatch)?;
        if regenerated != projection {
            return Err(DecisionRepositoryError::EvidenceMismatch);
        }
        self.ensure_investment_record_capacity()?;
        self.push(DecisionRecord::InvestmentOutcomeProjection(
            projection.clone(),
        ))?;
        self.investment_analysis_index_entry_mut(analysis_id)?
            .outcome_projection_digest = Some(projection.result_digest());
        Ok(AppendOutcome::Appended)
    }

    /// Appends one deterministic generated-proposal sizing-feasibility sidecar.
    pub fn append_investment_sizing_projection(
        &mut self,
        projection: InvestmentSizingProjection,
    ) -> Result<AppendOutcome, DecisionRepositoryError> {
        let proposal_id = projection.binding().proposal_id();
        if let Some(existing) = self.investment_sizing_projection(proposal_id) {
            return if existing == &projection {
                Ok(AppendOutcome::AlreadyPresent)
            } else {
                Err(DecisionRepositoryError::Conflict)
            };
        }
        let (analysis_id, generated) = self
            .generated_investment_proposal(proposal_id)
            .ok_or(DecisionRepositoryError::NotFound)?;
        if self.investment_analysis_publication(analysis_id).is_none() {
            return Err(DecisionRepositoryError::NotFound);
        }
        let regenerated =
            InvestmentSizingProjection::try_from_proposal(generated, projection.inputs().clone())
                .map_err(|_error| DecisionRepositoryError::EvidenceMismatch)?;
        if regenerated != projection {
            return Err(DecisionRepositoryError::EvidenceMismatch);
        }
        self.ensure_investment_record_capacity()?;
        self.push(DecisionRecord::InvestmentSizingProjection(
            projection.clone(),
        ))?;
        self.investment_analysis_index_entry_mut(analysis_id)?
            .sizing_projection_digest = Some(projection.result_digest());
        Ok(AppendOutcome::Appended)
    }

    /// Appends one contiguous pending, unavailable, or completed outcome status revision.
    pub fn append_recommendation_outcome_status(
        &mut self,
        status: RecommendationOutcomeStatusRecord,
    ) -> Result<AppendOutcome, DecisionRepositoryError> {
        if let Some(existing) =
            self.recommendation_outcome_status(status.series_id(), status.revision())
        {
            return if existing == &status {
                Ok(AppendOutcome::AlreadyPresent)
            } else {
                Err(DecisionRepositoryError::Conflict)
            };
        }
        let decision = self
            .investment_proposal(status.analysis_id())
            .ok_or(DecisionRepositoryError::NotFound)?;
        let publication = self
            .investment_analysis_publication(status.analysis_id())
            .ok_or(DecisionRepositoryError::NotFound)?
            .clone();
        let regenerated = match status.status() {
            RecommendationOutcomeStatus::Pending(reason) => {
                RecommendationOutcomeStatusRecord::try_pending(
                    decision,
                    &publication,
                    status.revision(),
                    status.previous_status_digest(),
                    status.evaluated_at(),
                    reason,
                )
            }
            RecommendationOutcomeStatus::Unavailable(reason) => {
                RecommendationOutcomeStatusRecord::try_unavailable(
                    decision,
                    &publication,
                    status.revision(),
                    status.previous_status_digest(),
                    status.evaluated_at(),
                    reason,
                )
            }
            RecommendationOutcomeStatus::Completed(outcome) => {
                RecommendationOutcomeStatusRecord::try_completed(
                    decision,
                    &publication,
                    status.revision(),
                    status.previous_status_digest(),
                    status.evaluated_at(),
                    outcome.observation(),
                )
            }
        }
        .map_err(|_error| DecisionRepositoryError::EvidenceMismatch)?;
        if regenerated != status {
            return Err(DecisionRepositoryError::EvidenceMismatch);
        }
        let current = self.recommendation_outcome_current(status.series_id());
        match current {
            None if status.revision().get() == 1 && status.previous_status_digest().is_none() => {}
            Some(current)
                if matches!(current.status(), RecommendationOutcomeStatus::Pending(_))
                    && status.revision().get() == current.revision().get().saturating_add(1)
                    && status.previous_status_digest() == Some(current.status_digest()) => {}
            None | Some(_) => return Err(DecisionRepositoryError::StaleRevision),
        }
        self.ensure_investment_record_capacity()?;
        self.push(DecisionRecord::RecommendationOutcomeStatus(status.clone()))?;
        let current_entry = RecommendationOutcomeCurrentIndexEntry::new(&publication, &status);
        if let Some(existing) = self
            .recommendation_outcome_index
            .iter_mut()
            .find(|entry| entry.series_id() == status.series_id())
        {
            *existing = current_entry.clone();
        } else {
            self.recommendation_outcome_index
                .push(current_entry.clone());
        }
        self.investment_analysis_index_entry_mut(status.analysis_id())?
            .current_outcome = Some(current_entry);
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

    /// Lists immutable investment-analysis results in durable append order without allocating.
    pub fn investment_proposals(&self) -> impl Iterator<Item = &InvestmentProposalDecision> {
        self.records.iter().filter_map(|record| match record {
            DecisionRecord::InvestmentProposal(decision) => Some(decision),
            _ => None,
        })
    }

    /// Finds one exact generated, no-action, or unavailable investment-analysis result.
    pub fn investment_proposal(
        &self,
        analysis_id: InvestmentAnalysisId,
    ) -> Option<&InvestmentProposalDecision> {
        self.investment_proposals()
            .find(|decision| decision.analysis_id() == analysis_id)
    }

    /// Returns the sole immutable profile/workflow publication for one analysis.
    pub fn investment_analysis_publication(
        &self,
        analysis_id: InvestmentAnalysisId,
    ) -> Option<&PublishedInvestmentAnalysis> {
        self.records.iter().find_map(|record| match record {
            DecisionRecord::InvestmentAnalysisPublication(publication)
                if publication.analysis_id() == analysis_id =>
            {
                Some(publication)
            }
            _ => None,
        })
    }

    /// Returns one durable outcome projection by generated proposal identity.
    pub fn investment_outcome_projection(
        &self,
        proposal_id: InvestmentProposalId,
    ) -> Option<&InvestmentOutcomeProjection> {
        self.records.iter().find_map(|record| match record {
            DecisionRecord::InvestmentOutcomeProjection(projection)
                if projection.binding().proposal_id() == proposal_id =>
            {
                Some(projection)
            }
            _ => None,
        })
    }

    /// Returns one durable sizing projection by generated proposal identity.
    pub fn investment_sizing_projection(
        &self,
        proposal_id: InvestmentProposalId,
    ) -> Option<&InvestmentSizingProjection> {
        self.records.iter().find_map(|record| match record {
            DecisionRecord::InvestmentSizingProjection(projection)
                if projection.binding().proposal_id() == proposal_id =>
            {
                Some(projection)
            }
            _ => None,
        })
    }

    /// Returns one exact immutable recommendation-outcome status revision.
    pub fn recommendation_outcome_status(
        &self,
        series_id: crate::RecommendationOutcomeSeriesId,
        revision: RevisionNumber,
    ) -> Option<&RecommendationOutcomeStatusRecord> {
        self.records.iter().find_map(|record| match record {
            DecisionRecord::RecommendationOutcomeStatus(status)
                if status.series_id() == series_id && status.revision() == revision =>
            {
                Some(status)
            }
            _ => None,
        })
    }

    /// Returns the latest contiguous status for one recommendation-outcome series.
    pub fn recommendation_outcome_current(
        &self,
        series_id: crate::RecommendationOutcomeSeriesId,
    ) -> Option<&RecommendationOutcomeCurrentIndexEntry> {
        self.recommendation_outcome_index
            .iter()
            .find(|entry| entry.series_id() == series_id)
    }

    /// Returns the profile-bound current analysis locator without append-order ranking.
    pub fn investment_analysis_current(
        &self,
        analysis_id: InvestmentAnalysisId,
    ) -> Option<&InvestmentAnalysisCurrentIndexEntry> {
        self.investment_analysis_index
            .iter()
            .find(|entry| entry.publication().analysis_id() == analysis_id)
    }

    /// Lists canonical analysis currentness locators ordered by stable analysis identity.
    pub fn list_investment_analysis_current_index(
        &self,
        maximum: usize,
    ) -> Result<Vec<InvestmentAnalysisCurrentIndexEntry>, DecisionRepositoryError> {
        if maximum == 0 {
            return Err(DecisionRepositoryError::InvalidLimits);
        }
        let mut entries = self.investment_analysis_index.clone();
        entries.sort_by_key(|entry| entry.publication().analysis_id());
        entries.truncate(maximum);
        Ok(entries)
    }

    /// Computes a current-status track record for one exact profile and horizon duration.
    ///
    /// Each action and the no-action control remain separate. Missing status records count against
    /// due-outcome coverage, and no append ordering or survivorship filter affects the denominator.
    pub fn recommendation_track_record(
        &self,
        analytical_profile: &AnalyticalProfileBindingReference,
        horizon_nanos: i64,
        evaluated_at: Timestamp,
    ) -> Result<RecommendationTrackRecord, DecisionRepositoryError> {
        if horizon_nanos <= 0 {
            return Err(DecisionRepositoryError::InvalidLimits);
        }
        let cohorts = [
            RecommendationOutcomeCohort::Generated(RecommendationAction::Buy),
            RecommendationOutcomeCohort::Generated(RecommendationAction::Add),
            RecommendationOutcomeCohort::Generated(RecommendationAction::Hold),
            RecommendationOutcomeCohort::Generated(RecommendationAction::Trim),
            RecommendationOutcomeCohort::Generated(RecommendationAction::Sell),
            RecommendationOutcomeCohort::NoActionControl,
        ];
        let mut groups = cohorts
            .into_iter()
            .map(RecommendationTrackRecordGroup::empty)
            .collect::<Vec<_>>();
        let mut analysis_unavailable_count = 0_u32;
        for entry in &self.investment_analysis_index {
            let publication = entry.publication();
            if publication.analytical_profile() != analytical_profile
                || publication
                    .horizon_at()
                    .unix_nanos()
                    .checked_sub(publication.as_of().unix_nanos())
                    != Some(horizon_nanos)
            {
                continue;
            }
            let decision = self
                .investment_proposal(publication.analysis_id())
                .ok_or(DecisionRepositoryError::NotFound)?;
            let cohort = match decision {
                InvestmentProposalDecision::Generated(value) => {
                    RecommendationOutcomeCohort::Generated(value.action())
                }
                InvestmentProposalDecision::NoAction(_) => {
                    RecommendationOutcomeCohort::NoActionControl
                }
                InvestmentProposalDecision::Unavailable(_) => {
                    analysis_unavailable_count = analysis_unavailable_count
                        .checked_add(1)
                        .ok_or(DecisionRepositoryError::Capacity)?;
                    continue;
                }
            };
            let group = groups
                .iter_mut()
                .find(|group| group.cohort() == cohort)
                .ok_or(DecisionRepositoryError::EvidenceMismatch)?;
            group
                .observe(
                    publication.horizon_at() <= evaluated_at,
                    entry.current_outcome().map(|current| current.status()),
                )
                .map_err(|_error| DecisionRepositoryError::EvidenceMismatch)?;
        }
        for group in &mut groups {
            group
                .finalize()
                .map_err(|_error| DecisionRepositoryError::EvidenceMismatch)?;
        }
        Ok(RecommendationTrackRecord::new(
            analytical_profile.clone(),
            horizon_nanos,
            evaluated_at,
            analysis_unavailable_count,
            groups,
        ))
    }

    /// Lists bounded immutable investment-analysis locators in durable append order.
    pub fn list_investment_proposal_index(
        &self,
        maximum: usize,
    ) -> Result<Vec<InvestmentProposalIndexEntry>, DecisionRepositoryError> {
        self.list_investment_proposal_index_after(None, maximum)
    }

    /// Continues investment-analysis discovery strictly after an exact retained analysis identity.
    ///
    /// An unknown cursor fails closed rather than restarting at the beginning and presenting a
    /// misleading duplicate page. The projection is derived from immutable source records and owns
    /// no independent recommendation or current-profile authority.
    pub fn list_investment_proposal_index_after(
        &self,
        after: Option<InvestmentAnalysisId>,
        maximum: usize,
    ) -> Result<Vec<InvestmentProposalIndexEntry>, DecisionRepositoryError> {
        if maximum == 0 {
            return Err(DecisionRepositoryError::InvalidLimits);
        }
        let count = self.investment_proposal_count().min(maximum);
        let mut result = Vec::new();
        result
            .try_reserve_exact(count)
            .map_err(|_error| DecisionRepositoryError::Allocation)?;
        let mut decisions = self.investment_proposals();
        if let Some(after) = after
            && decisions
                .position(|decision| decision.analysis_id() == after)
                .is_none()
        {
            return Err(DecisionRepositoryError::NotFound);
        }
        result.extend(
            decisions
                .take(maximum)
                .map(InvestmentProposalIndexEntry::from_decision),
        );
        Ok(result)
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

    fn investment_proposal_count(&self) -> usize {
        self.records
            .iter()
            .filter(|record| matches!(record, DecisionRecord::InvestmentProposal(_)))
            .count()
    }

    fn investment_record_count(&self) -> usize {
        self.records
            .iter()
            .filter(|record| {
                matches!(
                    record,
                    DecisionRecord::InvestmentProposal(_)
                        | DecisionRecord::InvestmentAnalysisPublication(_)
                        | DecisionRecord::InvestmentOutcomeProjection(_)
                        | DecisionRecord::InvestmentSizingProjection(_)
                        | DecisionRecord::RecommendationOutcomeStatus(_)
                )
            })
            .count()
    }

    fn ensure_investment_record_capacity(&self) -> Result<(), DecisionRepositoryError> {
        if self.investment_record_count() >= self.limits.maximum_investment_proposals {
            Err(DecisionRepositoryError::Capacity)
        } else {
            Ok(())
        }
    }

    fn generated_investment_proposal(
        &self,
        proposal_id: InvestmentProposalId,
    ) -> Option<(InvestmentAnalysisId, &crate::GeneratedInvestmentProposal)> {
        self.investment_proposals()
            .find_map(|decision| match decision {
                InvestmentProposalDecision::Generated(value)
                    if value.proposal_id() == proposal_id =>
                {
                    Some((value.analysis_id(), value))
                }
                InvestmentProposalDecision::Generated(_)
                | InvestmentProposalDecision::NoAction(_)
                | InvestmentProposalDecision::Unavailable(_) => None,
            })
    }

    fn investment_analysis_index_entry_mut(
        &mut self,
        analysis_id: InvestmentAnalysisId,
    ) -> Result<&mut InvestmentAnalysisCurrentIndexEntry, DecisionRepositoryError> {
        self.investment_analysis_index
            .iter_mut()
            .find(|entry| entry.publication().analysis_id() == analysis_id)
            .ok_or(DecisionRepositoryError::NotFound)
    }
}
