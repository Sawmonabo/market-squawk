//! Transport-neutral access to the sole durable decision workflow authority.

mod backup;
mod codec;
pub(crate) mod dossier_preparation;
mod persistence;
pub(crate) mod screen_workflow;
pub(crate) mod target_preparation;

use std::{collections::BTreeMap, fmt, sync::Mutex, time::Instant};

use market_squawk_decisions::{
    AppendOutcome, CandidateAssessment, CandidateInput, DecisionAuthority, DecisionDossier,
    DecisionRepository, DecisionRepositoryError, DecisionRepositoryLimits, InvestmentTargetSetId,
    SavedScreen, ScreenExecution, ScreenId, ScreenRun, ScreenRunId, TargetIndexEntry,
    TargetInvalidation, TargetReview, TargetState, TargetStatus,
};
use market_squawk_domain::{EvidenceDigest, RevisionNumber, SourceIdentifier, Timestamp};
use market_squawk_platform::DecisionDatabaseLocation;

use self::codec::{EncodedRecord, RecoveryContext};
use self::persistence::DecisionJournal;

pub(crate) use self::backup::RetainedDecisionBackupSnapshot;
pub(crate) use self::dossier_preparation::{
    DossierEvidenceInventory, DossierPreparationDraft, DossierPreparationError,
    DossierPreparationFence, DossierPreparationReceipt, PreparedDossierPreview,
};
pub(crate) use self::screen_workflow::{AdmittedScreenJob, ScreenJobRequest, ScreenWorkflowError};
use self::target_preparation::{
    PreparedTargetPreview, TargetEvidenceInventory, TargetPreparationCommitKind,
    TargetPreparationDraft, TargetPreparationError, TargetPreparationFence,
    TargetPreparationReceipt, TargetReferenceMarkSelector,
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

impl DecisionApplication {
    /// Opens the fixed decision database, acquires its sole writer lease, and canonically replays
    /// every committed row before making the authority available.
    pub fn open(
        location: DecisionDatabaseLocation,
        limits: DecisionRepositoryLimits,
    ) -> Result<Self, DecisionApplicationError> {
        let journal = DecisionJournal::open(location)?;
        let repository = DecisionRepository::try_new(limits)?;
        let mut authority = DecisionAuthority::new(repository);
        let mut recovery = RecoveryContext::try_new()?;
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
            return if existing == &plan {
                existing.admitted()
            } else {
                Err(ScreenWorkflowError::Conflict)
            };
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

    /// `Decision.ListScreens` typed implementation with caller-selected bounded output.
    pub fn list_screens(
        &self,
        maximum: usize,
    ) -> Result<Vec<SavedScreen>, DecisionApplicationError> {
        if maximum == 0 {
            return Err(DecisionRepositoryError::InvalidLimits.into());
        }
        let state = self.reader()?;
        let count = state.authority.list_screens().count().min(maximum);
        let mut result = Vec::new();
        result
            .try_reserve_exact(count)
            .map_err(|_error| DecisionApplicationError::Allocation)?;
        result.extend(state.authority.list_screens().take(maximum).cloned());
        Ok(result)
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
