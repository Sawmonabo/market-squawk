//! Transport-neutral access to the sole durable decision workflow authority.

mod backup;
mod codec;
mod persistence;

use std::{fmt, sync::Mutex};

use market_squawk_decisions::{
    AppendOutcome, CandidateAssessment, CandidateInput, DecisionAuthority, DecisionDossier,
    DecisionRepository, DecisionRepositoryError, DecisionRepositoryLimits, GovernedTargetSet,
    InvestmentTargetSetId, SavedScreen, ScreenExecution, ScreenId, ScreenRun, ScreenRunId,
    TargetIndexEntry, TargetInvalidation, TargetReview, TargetState, TargetStatus,
};
use market_squawk_domain::{RevisionNumber, Timestamp};
use market_squawk_platform::DecisionDatabaseLocation;

use self::codec::{EncodedRecord, RecoveryContext};
use self::persistence::DecisionJournal;

pub(crate) use self::backup::RetainedDecisionBackupSnapshot;

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
        Ok(Self {
            state: Mutex::new(DecisionState {
                authority,
                journal,
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

    /// Immutable `CreateTargetSet` implementation committed before acknowledgment.
    pub fn create_target(
        &self,
        target: GovernedTargetSet,
    ) -> Result<AppendOutcome, DecisionApplicationError> {
        let encoded = codec::target(&target)?;
        let mut state = self.writer()?;
        let outcome = state.authority.create_target(target)?;
        persist_outcome(&mut state, &encoded, outcome)
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

    /// Compare-and-append `ReevaluateTargetSet` implementation.
    pub fn reevaluate_target(
        &self,
        expected: RevisionNumber,
        successor: GovernedTargetSet,
    ) -> Result<AppendOutcome, DecisionApplicationError> {
        let encoded = codec::target(&successor)?;
        let mut state = self.writer()?;
        let outcome = state.authority.reevaluate_target(expected, successor)?;
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
