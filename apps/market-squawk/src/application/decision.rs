//! Transport-neutral access to the sole durable decision workflow authority.

mod backup;
mod codec;
pub(crate) mod dossier_preparation;
mod persistence;
pub(crate) mod recommendation;
pub(crate) mod screen_workflow;
pub(crate) mod target_preparation;
mod workspace;

use std::{collections::BTreeMap, fmt, sync::Mutex, time::Instant};

use market_squawk_decisions::{
    AnalyticalProfileBindingReference, AppendOutcome, CandidateAssessment, CandidateInput,
    CurrentScreenPage, DecisionAuthority, DecisionDossier, DecisionRepository,
    DecisionRepositoryError, DecisionRepositoryLimits, InvestmentAnalysisCurrentIndexEntry,
    InvestmentAnalysisId, InvestmentOutcomeProjection, InvestmentProposalDecision,
    InvestmentProposalId, InvestmentProposalIndexEntry, InvestmentSizingProjection,
    InvestmentTargetSetId, PreparedPublishedInvestmentAnalysis, PublishedInvestmentAnalysis,
    RecommendationOutcomeStatusRecord, RecommendationTrackRecord, SavedScreen, ScreenExecution,
    ScreenId, ScreenRun, ScreenRunId, TargetIndexEntry, TargetInvalidation, TargetReview,
    TargetState, TargetStatus,
};
use market_squawk_domain::{EvidenceDigest, RevisionNumber, SourceIdentifier, Timestamp};
use market_squawk_platform::DecisionDatabaseLocation;
use uuid::Uuid;

use super::opaque_product_token;

use self::codec::{EncodedRecord, RecoveryContext};
use self::persistence::DecisionJournal;

pub(crate) use self::backup::RetainedDecisionBackupSnapshot;
pub(crate) use self::dossier_preparation::{
    DossierEvidenceInventory, DossierEvidenceSelection, DossierFairValueEvidence,
    DossierForecastEvidence, DossierPreparationDraft, DossierPreparationError,
    DossierPreparationFence, DossierPreparationReceipt, PreparedDossierPreview,
};
pub(crate) use self::screen_workflow::{AdmittedScreenJob, ScreenJobRequest, ScreenWorkflowError};
use self::target_preparation::{
    PreparedTargetPreview, TargetEvidenceInventory, TargetPreparationCommitKind,
    TargetPreparationDraft, TargetPreparationError, TargetPreparationFence,
    TargetPreparationReceipt, TargetReferenceMarkSelector,
};
pub(crate) use self::workspace::{
    DecisionWorkspaceCandidate, DecisionWorkspaceCandidateRun, DecisionWorkspaceCompleteness,
    DecisionWorkspaceQuery, DecisionWorkspaceRead, DecisionWorkspaceReadError,
    DecisionWorkspaceReadLimits, DecisionWorkspaceSelectionCounts,
    DecisionWorkspaceSelectionReceipt, DecisionWorkspaceSnapshot, DecisionWorkspaceTargetHead,
    DecisionWorkspaceTruncationReason,
};

/// Typed application failure that does not leak SQLite, lock, or filesystem internals.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DecisionApplicationError {
    /// The single-writer state is poisoned, retained by backup, or its lock is unavailable.
    Unavailable,
    /// A bounded result allocation failed.
    Allocation,
    /// Domain repository validation rejected a live operation.
    Repository(DecisionRepositoryError),
    /// Durable storage or its retained capability could not be used safely.
    Persistence,
    /// Committed rows were corrupt, partial, divergent, or from an unsupported schema.
    InvalidPersistentState,
    /// The bounded durable journal is full.
    Capacity,
}

impl fmt::Display for DecisionApplicationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unavailable => formatter.write_str("decision authority is unavailable"),
            Self::Allocation => formatter.write_str("decision result allocation failed"),
            Self::Repository(error) => fmt::Display::fmt(error, formatter),
            Self::Persistence => formatter.write_str("decision persistence is unavailable"),
            Self::InvalidPersistentState => {
                formatter.write_str("decision journal state is invalid")
            }
            Self::Capacity => formatter.write_str("decision journal capacity is exhausted"),
        }
    }
}

impl std::error::Error for DecisionApplicationError {}

impl From<DecisionRepositoryError> for DecisionApplicationError {
    fn from(error: DecisionRepositoryError) -> Self {
        Self::Repository(error)
    }
}

#[derive(Debug)]
struct DecisionState {
    authority: DecisionAuthority,
    journal: DecisionJournal,
    preparation: target_preparation::TargetPreparationAuthority,
    dossier_preparation: dossier_preparation::DossierPreparationAuthority,
    screen_job_inputs: BTreeMap<String, screen_workflow::ScreenJobPlan>,
    limits: DecisionRepositoryLimits,
    backup_retained: bool,
    poisoned: bool,
}

/// Sole application adapter over one recovered decision writer and one durable append journal.
#[derive(Debug)]
pub struct DecisionApplication {
    state: Mutex<DecisionState>,
}

/// One immutable proposal-index snapshot with its exact remaining locator count.
#[derive(Debug)]
pub(crate) struct InvestmentProposalIndexReadPage {
    entries: Vec<InvestmentProposalIndexEntry>,
    available: usize,
}

/// One-lock current read of a proposal, its publication, and generated-proposal sidecars.
#[derive(Debug)]
pub(crate) struct InvestmentAnalysisRead {
    pub(crate) decision: InvestmentProposalDecision,
    pub(crate) current: Option<InvestmentAnalysisCurrentIndexEntry>,
    pub(crate) outcome_projection: Option<InvestmentOutcomeProjection>,
    pub(crate) sizing_projection: Option<InvestmentSizingProjection>,
}

const INVESTMENT_ANALYSIS_PRODUCT_TOKEN_DOMAIN: &[u8] =
    b"market-squawk/investment-analysis-product-action/v1\0";

fn investment_analysis_product_token(analysis_id: InvestmentAnalysisId) -> Uuid {
    let identity = analysis_id.bytes();
    opaque_product_token(INVESTMENT_ANALYSIS_PRODUCT_TOKEN_DOMAIN, &[&identity])
}

impl InvestmentProposalIndexReadPage {
    /// Separates the bounded locator page from its exact same-snapshot availability count.
    pub(crate) fn into_parts(self) -> (Vec<InvestmentProposalIndexEntry>, usize) {
        (self.entries, self.available)
    }
}

impl DecisionApplication {
    /// Opens the fixed decision database, acquires its sole writer lease, and canonically replays
    /// every committed row before making the authority available.
    pub fn open(
        location: DecisionDatabaseLocation,
        limits: DecisionRepositoryLimits,
    ) -> Result<Self, DecisionApplicationError> {
        let journal = DecisionJournal::open(location, limits)?;
        let repository = DecisionRepository::try_new(limits)?;
        let mut authority = DecisionAuthority::new(repository);
        let mut recovery = RecoveryContext::try_new(limits.maximum_screen_runs())?;
        let _semantic_sha256 = journal.recover(&mut authority, &mut recovery)?;
        let screen_job_inputs = recovery.into_screen_job_inputs();
        Ok(Self {
            state: Mutex::new(DecisionState {
                authority,
                journal,
                preparation: target_preparation::TargetPreparationAuthority::default(),
                dossier_preparation: dossier_preparation::DossierPreparationAuthority::default(),
                screen_job_inputs,
                limits,
                backup_retained: false,
                poisoned: false,
            }),
        })
    }

    /// `Decision.SaveScreen` typed implementation. The call returns only after WAL commit.
    pub fn save_screen(
        &self,
        expected: Option<RevisionNumber>,
        screen: SavedScreen,
    ) -> Result<AppendOutcome, DecisionApplicationError> {
        let encoded = codec::screen(&screen)?;
        let mut state = self.writer()?;
        let outcome = state.authority.save_screen(expected, screen)?;
        persist_outcome(&mut state, &encoded, outcome)
    }

    /// `Decision.RunScreen` implementation with the exact ranked result committed before return.
    pub fn run_screen(
        &self,
        run: ScreenRun,
        candidates: Vec<CandidateInput>,
        selected_at: Timestamp,
    ) -> Result<ScreenExecution, DecisionApplicationError> {
        let mut state = self.writer()?;
        let execution = state.authority.run_screen(run, candidates, selected_at)?;
        let encoded = match codec::execution(&execution, selected_at) {
            Ok(encoded) => encoded,
            Err(error) => {
                state.poisoned = true;
                return Err(error);
            }
        };
        if let Err(error) = state.journal.append(&encoded) {
            state.poisoned = true;
            return Err(error);
        }
        Ok(execution)
    }

    /// Resolves one retained screen and exact feature generation, derives all candidate inputs,
    /// and commits the immutable job input before returning an admission locator.
    pub async fn prepare_screen_job(
        &self,
        request: ScreenJobRequest,
        reader: &market_squawk_data::AnalyticalReadCapability,
        selected_at: Timestamp,
        deadline: Instant,
        cancellation: tokio_util::sync::CancellationToken,
    ) -> Result<AdmittedScreenJob, ScreenWorkflowError> {
        let screen = self
            .get_screen(request.screen_id(), request.screen_revision())
            .map_err(|error| match error {
                DecisionApplicationError::Repository(DecisionRepositoryError::NotFound) => {
                    ScreenWorkflowError::NotFound
                }
                other => ScreenWorkflowError::Application(other),
            })?;
        let expected_run_id = screen_workflow::expected_request_run_id(&screen, &request)?;
        {
            let state = self.reader()?;
            if let Some(existing) = state.screen_job_inputs.get(expected_run_id.as_str()) {
                return if existing.matches_request(&screen, &request) {
                    existing.admitted()
                } else {
                    Err(ScreenWorkflowError::Conflict)
                };
            }
            if state.screen_job_inputs.len() >= state.limits.maximum_screen_runs() {
                return Err(ScreenWorkflowError::Application(
                    DecisionApplicationError::Capacity,
                ));
            }
            if state
                .authority
                .repository()
                .screen_execution(&expected_run_id)
                .is_some()
            {
                return Err(ScreenWorkflowError::Application(
                    DecisionApplicationError::InvalidPersistentState,
                ));
            }
        }
        let plan = screen_workflow::prepare(
            &screen,
            &request,
            reader,
            selected_at,
            deadline,
            cancellation,
        )
        .await?;
        let encoded = codec::screen_job_input(&plan)?;
        let admitted = plan.admitted()?;
        let mut state = self.writer()?;
        let authoritative_screen = state
            .authority
            .get_screen(plan.run().screen().id(), plan.run().screen().revision())
            .map_err(DecisionApplicationError::from)?;
        screen_workflow::validate_fence(&plan, authoritative_screen)?;
        if let Some(existing) = state.screen_job_inputs.get(plan.run().id().as_str()) {
            return if existing.matches_request(authoritative_screen, &request) {
                existing.admitted()
            } else {
                Err(ScreenWorkflowError::Conflict)
            };
        }
        if state.screen_job_inputs.len() >= state.limits.maximum_screen_runs() {
            return Err(ScreenWorkflowError::Application(
                DecisionApplicationError::Capacity,
            ));
        }
        if state
            .authority
            .repository()
            .screen_execution(plan.run().id())
            .is_some()
        {
            return Err(ScreenWorkflowError::Conflict);
        }
        match state.journal.append(&encoded) {
            Ok(AppendOutcome::Appended) => {
                state
                    .screen_job_inputs
                    .insert(plan.run().id().as_str().to_owned(), plan);
                Ok(admitted)
            }
            Ok(AppendOutcome::AlreadyPresent) => {
                state.poisoned = true;
                Err(ScreenWorkflowError::Application(
                    DecisionApplicationError::InvalidPersistentState,
                ))
            }
            Err(DecisionApplicationError::Capacity) => Err(ScreenWorkflowError::Capacity),
            Err(error) => {
                state.poisoned = true;
                Err(ScreenWorkflowError::Application(error))
            }
        }
    }

    /// Executes one previously committed screen-job input. No caller-supplied candidate or run
    /// record crosses this boundary.
    pub fn run_prepared_screen_job(
        &self,
        input_identity: &SourceIdentifier,
        input_digest: EvidenceDigest,
    ) -> Result<ScreenExecution, ScreenWorkflowError> {
        let plan = self.screen_job_plan(input_identity, input_digest)?;
        let (run, candidates, selected_at) = plan.into_execution();
        self.run_screen(run, candidates, selected_at)
            .map_err(ScreenWorkflowError::Application)
    }

    /// Resolves the immutable run associated with one exact prepared input.
    pub fn prepared_screen_run_id(
        &self,
        input_identity: &SourceIdentifier,
        input_digest: EvidenceDigest,
    ) -> Result<ScreenRunId, ScreenWorkflowError> {
        Ok(self
            .screen_job_plan(input_identity, input_digest)?
            .run()
            .id()
            .clone())
    }

    /// Returns whether the decision journal already contains the exact prepared run result.
    pub fn prepared_screen_result(
        &self,
        input_identity: &SourceIdentifier,
        input_digest: EvidenceDigest,
    ) -> Result<Option<ScreenExecution>, ScreenWorkflowError> {
        let state = self.reader()?;
        let plan = resolve_screen_job_plan(&state, input_identity, input_digest)?;
        Ok(state
            .authority
            .repository()
            .screen_execution(plan.run().id())
            .cloned())
    }

    fn screen_job_plan(
        &self,
        input_identity: &SourceIdentifier,
        input_digest: EvidenceDigest,
    ) -> Result<screen_workflow::ScreenJobPlan, ScreenWorkflowError> {
        let state = self.reader()?;
        Ok(resolve_screen_job_plan(&state, input_identity, input_digest)?.clone())
    }

    /// `Decision.ListScreens` typed implementation with one current revision per stable screen.
    pub fn list_screens(
        &self,
        maximum: usize,
    ) -> Result<Vec<SavedScreen>, DecisionApplicationError> {
        Ok(self
            .list_current_screens_after(None, maximum)?
            .into_screens())
    }

    /// Reads a deterministic page of current saved-screen heads after one stable identity.
    pub fn list_current_screens_after(
        &self,
        after: Option<&ScreenId>,
        maximum: usize,
    ) -> Result<CurrentScreenPage, DecisionApplicationError> {
        self.reader()?
            .authority
            .repository()
            .list_current_screens_after(after, maximum)
            .map_err(Into::into)
    }

    /// `Decision.GetCandidates` typed implementation.
    pub fn get_candidates(
        &self,
        run_id: &ScreenRunId,
    ) -> Result<Vec<CandidateAssessment>, DecisionApplicationError> {
        let state = self.reader()?;
        let candidates = state.authority.get_candidates(run_id)?;
        let mut result = Vec::new();
        result
            .try_reserve_exact(candidates.len())
            .map_err(|_error| DecisionApplicationError::Allocation)?;
        result.extend_from_slice(candidates);
        Ok(result)
    }

    /// `Decision.ListScreenRuns` discovery implementation. Each locator remains bounded and exact;
    /// consumers must use `Decision.GetCandidates` for the selected immutable run.
    pub fn list_screen_runs(
        &self,
        maximum: usize,
    ) -> Result<Vec<market_squawk_decisions::ScreenRunIndexEntry>, DecisionApplicationError> {
        self.list_screen_runs_after(None, maximum)
    }

    /// Continues `Decision.ListScreenRuns` strictly after an exact retained cursor identity.
    pub fn list_screen_runs_after(
        &self,
        after: Option<&ScreenRunId>,
        maximum: usize,
    ) -> Result<Vec<market_squawk_decisions::ScreenRunIndexEntry>, DecisionApplicationError> {
        self.reader()?
            .authority
            .list_screen_runs_after(after, maximum)
            .map_err(Into::into)
    }

    /// Appends one immutable reference-only dossier after durable commit.
    pub fn append_dossier(
        &self,
        dossier: DecisionDossier,
    ) -> Result<AppendOutcome, DecisionApplicationError> {
        let encoded = codec::dossier(&dossier)?;
        let mut state = self.writer()?;
        let outcome = state.authority.append_dossier(dossier)?;
        persist_outcome(&mut state, &encoded, outcome)
    }

    /// Enumerates application-owned evidence options for one retained selected candidate.
    pub fn dossier_evidence_inventory(
        &self,
        candidate_id: &market_squawk_decisions::CandidateId,
    ) -> Result<DossierEvidenceInventory, DossierPreparationError> {
        let state = self.reader()?;
        state
            .dossier_preparation
            .inventory(&state.authority, candidate_id)
    }

    /// Assembles an immutable dossier from retained candidate evidence behind a one-use receipt.
    pub fn prepare_dossier(
        &self,
        fence: DossierPreparationFence,
        draft: DossierPreparationDraft,
        now: Timestamp,
    ) -> Result<PreparedDossierPreview, DossierPreparationError> {
        let mut state = self.writer()?;
        let DecisionState {
            authority,
            dossier_preparation,
            ..
        } = &mut *state;
        dossier_preparation.prepare(authority, fence, draft, now)
    }

    /// Consumes one dossier receipt after revalidating its complete installed authority fence.
    pub fn consume_dossier_preparation(
        &self,
        receipt: DossierPreparationReceipt,
        fence: DossierPreparationFence,
        now: Timestamp,
    ) -> Result<AppendOutcome, DossierPreparationError> {
        let mut state = self.writer()?;
        dossier_preparation::consume_prepared(&mut state, receipt, fence, now)
    }

    /// `Decision.GetDossier` typed implementation.
    pub fn get_dossier(
        &self,
        id: &market_squawk_decisions::DossierId,
    ) -> Result<DecisionDossier, DecisionApplicationError> {
        Ok(self.reader()?.authority.get_dossier(id)?.clone())
    }

    /// `Decision.ListCandidateDossiers` discovery implementation.
    ///
    /// The relationship is derived solely from retained immutable candidate and dossier records;
    /// callers cannot invent a link or create a dossier by reading this index.
    pub fn list_candidate_dossiers(
        &self,
        candidate_id: &market_squawk_decisions::CandidateId,
        maximum: usize,
    ) -> Result<Vec<DecisionDossier>, DecisionApplicationError> {
        self.list_candidate_dossiers_after(candidate_id, None, maximum)
    }

    /// Continues `Decision.ListCandidateDossiers` strictly after an exact dossier cursor.
    pub fn list_candidate_dossiers_after(
        &self,
        candidate_id: &market_squawk_decisions::CandidateId,
        after: Option<&market_squawk_decisions::DossierId>,
        maximum: usize,
    ) -> Result<Vec<DecisionDossier>, DecisionApplicationError> {
        self.reader()?
            .authority
            .list_candidate_dossiers_after(candidate_id, after, maximum)
            .map_err(Into::into)
    }

    /// Admits one exact producer-owned portfolio price as selectable target reference evidence.
    ///
    /// The application derives the evidence identity and opaque selector; presentation callers do
    /// not submit either value.
    pub fn admit_target_reference_mark(
        &self,
        evidence: &market_squawk_portfolio::PriceEvidence,
        quality: market_squawk_domain::DataQuality,
        admitted_at: Timestamp,
    ) -> Result<TargetReferenceMarkSelector, TargetPreparationError> {
        self.writer()?
            .preparation
            .admit_reference_mark(evidence, quality, admitted_at)
    }

    /// Enumerates bounded authoritative target evidence for one retained dossier.
    pub fn target_evidence_inventory(
        &self,
        dossier_id: &market_squawk_decisions::DossierId,
        now: Timestamp,
    ) -> Result<TargetEvidenceInventory, TargetPreparationError> {
        let state = self.reader()?;
        let dossier = state
            .authority
            .get_dossier(dossier_id)
            .map_err(|_error| TargetPreparationError::NotFound)?;
        state.preparation.inventory(dossier, now)
    }

    /// Validates a human target judgment and retains its exact typed target behind a receipt.
    pub fn prepare_target(
        &self,
        fence: TargetPreparationFence,
        draft: TargetPreparationDraft,
        now: Timestamp,
    ) -> Result<PreparedTargetPreview, TargetPreparationError> {
        let mut state = self.writer()?;
        let DecisionState {
            authority,
            preparation,
            ..
        } = &mut *state;
        preparation.prepare(
            &target_preparation::DecisionStateView { authority },
            fence,
            draft,
            now,
        )
    }

    /// Consumes one preparation receipt and revalidates its complete authority fence before append.
    pub fn consume_target_preparation(
        &self,
        receipt: TargetPreparationReceipt,
        fence: TargetPreparationFence,
        expected: TargetPreparationCommitKind,
        now: Timestamp,
    ) -> Result<AppendOutcome, TargetPreparationError> {
        let mut state = self.writer()?;
        target_preparation::consume_prepared(&mut state, receipt, fence, expected, now)
    }

    /// Immutable `GetTargetSet` implementation.
    pub fn get_target(
        &self,
        id: &InvestmentTargetSetId,
        revision: RevisionNumber,
    ) -> Result<TargetState, DecisionApplicationError> {
        self.reader()?
            .authority
            .get_target(id, revision)
            .map_err(Into::into)
    }

    /// Immutable `ListTargetSets` implementation.
    pub fn list_targets(
        &self,
        id: &InvestmentTargetSetId,
    ) -> Result<Vec<TargetState>, DecisionApplicationError> {
        let state = self.reader()?;
        let count = state.authority.list_targets(id).count();
        let mut result = Vec::new();
        result
            .try_reserve_exact(count)
            .map_err(|_error| DecisionApplicationError::Allocation)?;
        for target in state.authority.list_targets(id) {
            result.push(state.authority.get_target(id, target.target().revision())?);
        }
        Ok(result)
    }

    /// `Decision.ListTargetIndex` discovery implementation.
    ///
    /// It returns exactly one append-derived locator per target series and does not grant any
    /// rebalance, order, valuation, or execution authority.
    pub fn list_target_index(
        &self,
        maximum: usize,
    ) -> Result<Vec<TargetIndexEntry>, DecisionApplicationError> {
        self.list_target_index_after(None, maximum)
    }

    /// Continues `Decision.ListTargetIndex` strictly after an exact target-series cursor.
    pub fn list_target_index_after(
        &self,
        after: Option<&InvestmentTargetSetId>,
        maximum: usize,
    ) -> Result<Vec<TargetIndexEntry>, DecisionApplicationError> {
        self.reader()?
            .authority
            .list_target_index_after(after, maximum)
            .map_err(Into::into)
    }

    /// Explicit immutable `ReviewTargetSet` implementation committed before acknowledgment.
    pub fn review_target(
        &self,
        review: TargetReview,
    ) -> Result<AppendOutcome, DecisionApplicationError> {
        let encoded = codec::review(&review)?;
        let mut state = self.writer()?;
        let outcome = state.authority.review_target(review)?;
        persist_outcome(&mut state, &encoded, outcome)
    }

    /// Idempotent invalidation append used by evidence scanners.
    pub fn invalidate_target(
        &self,
        invalidation: TargetInvalidation,
    ) -> Result<AppendOutcome, DecisionApplicationError> {
        let encoded = codec::invalidation(&invalidation)?;
        let mut state = self.writer()?;
        let outcome = state.authority.invalidate_target(invalidation)?;
        persist_outcome(&mut state, &encoded, outcome)
    }

    /// Persists one constructor-governed proposal after independent authority recomputation.
    ///
    /// This typed composition boundary accepts neither raw financial scalars nor caller-selected
    /// recommendation fields. It grants no generation, workflow, sizing, order, or execution
    /// authority and is deliberately not registered as a service, MCP, or Desktop operation.
    pub fn append_investment_proposal(
        &self,
        decision: InvestmentProposalDecision,
    ) -> Result<AppendOutcome, DecisionApplicationError> {
        let encoded = codec::investment_proposal(&decision)?;
        let mut state = self.writer()?;
        let outcome = state.authority.append_investment_proposal(decision)?;
        persist_outcome(&mut state, &encoded, outcome)
    }

    /// Persists the sole immutable analytical-profile/workflow publication for one analysis.
    pub fn append_investment_analysis_publication(
        &self,
        publication: PublishedInvestmentAnalysis,
    ) -> Result<AppendOutcome, DecisionApplicationError> {
        let encoded = codec::investment_analysis_publication(&publication)?;
        let mut state = self.writer()?;
        let outcome = state
            .authority
            .append_investment_analysis_publication(publication)?;
        persist_outcome(&mut state, &encoded, outcome)
    }

    /// Atomically persists one selected-candidate decision, explanation, and publication.
    ///
    /// Domain validation is staged before SQLite commit and installs into memory only after the
    /// single immutable bundle row is durable. A post-commit staging divergence poisons the live
    /// writer; canonical restart recovery then replays the complete row or none of it.
    pub fn append_prepared_published_investment_analysis(
        &self,
        bundle: PreparedPublishedInvestmentAnalysis,
    ) -> Result<AppendOutcome, DecisionApplicationError> {
        let encoded = codec::prepared_published_investment_analysis(&bundle)?;
        let mut state = self.writer()?;
        let staged = state
            .authority
            .stage_prepared_published_investment_analysis(bundle)?;
        let expected = staged.outcome();
        let persisted = match state.journal.append(&encoded) {
            Ok(outcome) => outcome,
            Err(error) => {
                state.poisoned = true;
                return Err(error);
            }
        };
        if persisted != expected {
            state.poisoned = true;
            return Err(DecisionApplicationError::InvalidPersistentState);
        }
        let committed = match state
            .authority
            .commit_staged_published_investment_analysis(staged)
        {
            Ok(outcome) => outcome,
            Err(_error) => {
                state.poisoned = true;
                return Err(DecisionApplicationError::InvalidPersistentState);
            }
        };
        if committed != persisted {
            state.poisoned = true;
            return Err(DecisionApplicationError::InvalidPersistentState);
        }
        Ok(committed)
    }

    /// Persists one deterministic proposal-bound outcome projection sidecar.
    pub fn append_investment_outcome_projection(
        &self,
        projection: InvestmentOutcomeProjection,
    ) -> Result<AppendOutcome, DecisionApplicationError> {
        let encoded = codec::investment_outcome_projection(&projection)?;
        let mut state = self.writer()?;
        let outcome = state
            .authority
            .append_investment_outcome_projection(projection)?;
        persist_outcome(&mut state, &encoded, outcome)
    }

    /// Persists one deterministic proposal-bound sizing projection sidecar.
    pub fn append_investment_sizing_projection(
        &self,
        projection: InvestmentSizingProjection,
    ) -> Result<AppendOutcome, DecisionApplicationError> {
        let encoded = codec::investment_sizing_projection(&projection)?;
        let mut state = self.writer()?;
        let outcome = state
            .authority
            .append_investment_sizing_projection(projection)?;
        persist_outcome(&mut state, &encoded, outcome)
    }

    /// Persists one contiguous pending, unavailable, or completed outcome status revision.
    pub fn append_recommendation_outcome_status(
        &self,
        status: RecommendationOutcomeStatusRecord,
    ) -> Result<AppendOutcome, DecisionApplicationError> {
        let encoded = codec::recommendation_outcome_status(&status)?;
        let mut state = self.writer()?;
        let outcome = state
            .authority
            .append_recommendation_outcome_status(status)?;
        persist_outcome(&mut state, &encoded, outcome)
    }

    /// Returns one exact complete generated, no-action, or unavailable investment analysis.
    pub fn get_investment_proposal(
        &self,
        analysis_id: InvestmentAnalysisId,
    ) -> Result<InvestmentProposalDecision, DecisionApplicationError> {
        Ok(self
            .reader()?
            .authority
            .get_investment_proposal(analysis_id)?
            .clone())
    }

    /// Returns the profile/workflow publication bound to one analysis.
    pub fn get_investment_analysis_publication(
        &self,
        analysis_id: InvestmentAnalysisId,
    ) -> Result<PublishedInvestmentAnalysis, DecisionApplicationError> {
        Ok(self
            .reader()?
            .authority
            .get_investment_analysis_publication(analysis_id)?
            .clone())
    }

    /// Returns one complete selected-candidate analysis bundle under a single reader lock.
    pub fn get_prepared_published_investment_analysis(
        &self,
        analysis_id: InvestmentAnalysisId,
    ) -> Result<PreparedPublishedInvestmentAnalysis, DecisionApplicationError> {
        Ok(self
            .reader()?
            .authority
            .get_prepared_published_investment_analysis(analysis_id)?
            .clone())
    }

    /// Returns the exact durable outcome projection for one generated proposal.
    pub fn get_investment_outcome_projection(
        &self,
        proposal_id: InvestmentProposalId,
    ) -> Result<InvestmentOutcomeProjection, DecisionApplicationError> {
        Ok(self
            .reader()?
            .authority
            .get_investment_outcome_projection(proposal_id)?
            .clone())
    }

    /// Returns the exact durable sizing projection for one generated proposal.
    pub fn get_investment_sizing_projection(
        &self,
        proposal_id: InvestmentProposalId,
    ) -> Result<InvestmentSizingProjection, DecisionApplicationError> {
        Ok(self
            .reader()?
            .authority
            .get_investment_sizing_projection(proposal_id)?
            .clone())
    }

    /// Returns the current profile, projection, sizing, and outcome locator for one analysis.
    pub fn get_investment_analysis_current(
        &self,
        analysis_id: InvestmentAnalysisId,
    ) -> Result<InvestmentAnalysisCurrentIndexEntry, DecisionApplicationError> {
        Ok(self
            .reader()?
            .authority
            .get_investment_analysis_current(analysis_id)?
            .clone())
    }

    /// Atomically reads one proposal and every presently published analysis sidecar.
    pub(crate) fn read_investment_analysis(
        &self,
        analysis_id: InvestmentAnalysisId,
    ) -> Result<InvestmentAnalysisRead, DecisionApplicationError> {
        let state = self.reader()?;
        let repository = state.authority.repository();
        let decision = repository
            .investment_proposal(analysis_id)
            .ok_or(DecisionRepositoryError::NotFound)?
            .clone();
        let current = repository.investment_analysis_current(analysis_id).cloned();
        let (outcome_projection, sizing_projection) =
            decision.proposal_id().map_or((None, None), |proposal_id| {
                (
                    repository
                        .investment_outcome_projection(proposal_id)
                        .cloned(),
                    repository
                        .investment_sizing_projection(proposal_id)
                        .cloned(),
                )
            });
        Ok(InvestmentAnalysisRead {
            decision,
            current,
            outcome_projection,
            sizing_projection,
        })
    }

    /// Returns the restart-stable opaque product action token for one retained analysis.
    ///
    /// The full bounded retained set is checked before issuing the token so a truncated UUID
    /// collision fails closed rather than resolving to the wrong durable decision.
    pub(crate) fn investment_analysis_product_token(
        &self,
        analysis_id: InvestmentAnalysisId,
    ) -> Result<Uuid, DecisionApplicationError> {
        let state = self.reader()?;
        let repository = state.authority.repository();
        if repository.investment_proposal(analysis_id).is_none() {
            return Err(DecisionRepositoryError::NotFound.into());
        }
        let token = investment_analysis_product_token(analysis_id);
        for decision in repository.investment_proposals() {
            if decision.analysis_id() != analysis_id
                && investment_analysis_product_token(decision.analysis_id()) == token
            {
                return Err(DecisionApplicationError::InvalidPersistentState);
            }
        }
        Ok(token)
    }

    /// Resolves one opaque product action token against the complete bounded retained set.
    ///
    /// Deterministic derivation makes resolution restart-stable. Multiple matches are treated as
    /// corrupt state even though a UUID collision is cryptographically improbable.
    pub(crate) fn resolve_investment_analysis_product_token(
        &self,
        token: Uuid,
    ) -> Result<InvestmentAnalysisId, DecisionApplicationError> {
        let state = self.reader()?;
        let mut resolved = None;
        for decision in state.authority.repository().investment_proposals() {
            if investment_analysis_product_token(decision.analysis_id()) != token {
                continue;
            }
            if resolved.replace(decision.analysis_id()).is_some() {
                return Err(DecisionApplicationError::InvalidPersistentState);
            }
        }
        resolved.ok_or_else(|| DecisionRepositoryError::NotFound.into())
    }

    /// Computes current-status performance grouped by exact profile, action, and horizon.
    pub fn recommendation_track_record(
        &self,
        profile: &AnalyticalProfileBindingReference,
        horizon_nanos: i64,
        evaluated_at: Timestamp,
    ) -> Result<RecommendationTrackRecord, DecisionApplicationError> {
        self.reader()?
            .authority
            .recommendation_track_record(profile, horizon_nanos, evaluated_at)
            .map_err(Into::into)
    }

    /// Lists bounded immutable investment-analysis locators in durable append order.
    pub fn list_investment_proposal_index(
        &self,
        maximum: usize,
    ) -> Result<Vec<InvestmentProposalIndexEntry>, DecisionApplicationError> {
        self.reader()?
            .authority
            .list_investment_proposal_index(maximum)
            .map_err(Into::into)
    }

    /// Continues proposal discovery strictly after one exact retained analysis identity.
    pub fn list_investment_proposal_index_after(
        &self,
        after: Option<InvestmentAnalysisId>,
        maximum: usize,
    ) -> Result<Vec<InvestmentProposalIndexEntry>, DecisionApplicationError> {
        self.reader()?
            .authority
            .list_investment_proposal_index_after(after, maximum)
            .map_err(Into::into)
    }

    /// Atomically reads a bounded append-order proposal index and its exact remaining count.
    ///
    /// The count and locators are derived while holding one reader guard, so a concurrent append
    /// cannot make completeness metadata disagree with the returned page.
    pub(crate) fn read_investment_proposal_index_page_after(
        &self,
        after: Option<InvestmentAnalysisId>,
        maximum: usize,
    ) -> Result<InvestmentProposalIndexReadPage, DecisionApplicationError> {
        if maximum == 0 {
            return Err(DecisionRepositoryError::InvalidLimits.into());
        }
        let state = self.reader()?;
        let repository = state.authority.repository();
        let available = if let Some(cursor) = after {
            let mut proposals = repository.investment_proposals();
            if proposals
                .position(|proposal| proposal.analysis_id() == cursor)
                .is_none()
            {
                return Err(DecisionRepositoryError::NotFound.into());
            }
            proposals.count()
        } else {
            repository.investment_proposals().count()
        };
        if available > state.limits.maximum_investment_proposals()
            || available > state.limits.maximum_records()
        {
            return Err(DecisionApplicationError::InvalidPersistentState);
        }
        let entries = state
            .authority
            .list_investment_proposal_index_after(after, maximum)?;
        if entries.len() > maximum || entries.len() > available {
            return Err(DecisionApplicationError::InvalidPersistentState);
        }
        Ok(InvestmentProposalIndexReadPage { entries, available })
    }

    /// Effective append-derived status.
    pub fn target_status(
        &self,
        id: &InvestmentTargetSetId,
        revision: RevisionNumber,
    ) -> Result<TargetStatus, DecisionApplicationError> {
        self.reader()?
            .authority
            .target_status(id, revision)
            .map_err(Into::into)
    }

    /// Returns one exact screen for transport serialization.
    pub fn get_screen(
        &self,
        id: &ScreenId,
        revision: RevisionNumber,
    ) -> Result<SavedScreen, DecisionApplicationError> {
        Ok(self.reader()?.authority.get_screen(id, revision)?.clone())
    }

    /// Returns the current immutable revision for one stable saved-screen identity.
    pub fn get_current_screen(
        &self,
        id: &ScreenId,
    ) -> Result<SavedScreen, DecisionApplicationError> {
        self.reader()?
            .authority
            .repository()
            .current_screen(id)
            .cloned()
            .ok_or_else(|| DecisionRepositoryError::NotFound.into())
    }

    fn writer(&self) -> Result<std::sync::MutexGuard<'_, DecisionState>, DecisionApplicationError> {
        let state = self
            .state
            .lock()
            .map_err(|_error| DecisionApplicationError::Unavailable)?;
        if state.poisoned || state.backup_retained {
            Err(DecisionApplicationError::Unavailable)
        } else {
            Ok(state)
        }
    }

    fn reader(&self) -> Result<std::sync::MutexGuard<'_, DecisionState>, DecisionApplicationError> {
        let state = self
            .state
            .lock()
            .map_err(|_error| DecisionApplicationError::Unavailable)?;
        if state.poisoned {
            Err(DecisionApplicationError::Unavailable)
        } else {
            Ok(state)
        }
    }
}

fn resolve_screen_job_plan<'a>(
    state: &'a DecisionState,
    input_identity: &SourceIdentifier,
    input_digest: EvidenceDigest,
) -> Result<&'a screen_workflow::ScreenJobPlan, ScreenWorkflowError> {
    let plan = state
        .screen_job_inputs
        .get(input_identity.as_str())
        .ok_or(ScreenWorkflowError::NotFound)?;
    if plan.input_digest() != input_digest {
        return Err(ScreenWorkflowError::Conflict);
    }
    Ok(plan)
}

fn persist_outcome(
    state: &mut DecisionState,
    encoded: &EncodedRecord,
    domain_outcome: AppendOutcome,
) -> Result<AppendOutcome, DecisionApplicationError> {
    match state.journal.append(encoded) {
        Ok(persistent_outcome) if persistent_outcome == domain_outcome => Ok(domain_outcome),
        Ok(_mismatched) => {
            state.poisoned = true;
            Err(DecisionApplicationError::InvalidPersistentState)
        }
        Err(error) => {
            state.poisoned = true;
            Err(error)
        }
    }
}
