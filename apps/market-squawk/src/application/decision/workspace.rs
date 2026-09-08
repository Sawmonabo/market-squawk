//! Complete, bounded retained-decision evidence for one instrument workspace.

use std::{fmt, num::NonZeroUsize, sync::TryLockError, time::Instant};

use market_squawk_decisions::{
    CandidateAssessment, DecisionAuthority, DecisionContentDigest, DecisionDossier,
    DecisionRepositoryError, DossierSection, InvalidationKind, ScreenRun, TargetIndexEntry,
    TargetInvalidation, TargetReview, TargetReviewDisposition, TargetState, TargetStatus,
};
use market_squawk_domain::{DataQuality, DigestAlgorithm, EvidenceDigest, InstrumentId, Timestamp};
use sha2::{Digest as _, Sha256};
use tokio_util::sync::CancellationToken;

use super::DecisionApplication;

const POLICY_REVISION: u32 = 1;
const MAXIMUM_SCREEN_RUN_SCAN: usize = 65_536;
const MAXIMUM_CANDIDATE_SCAN: usize = 1_000_000;
const MAXIMUM_DOSSIER_SCAN: usize = 65_536;
const MAXIMUM_TARGET_SERIES_SCAN: usize = 65_536;
const MAXIMUM_TARGET_HEADS: usize = 65_536;
const POLICY_CANONICAL_BYTES: &[u8] = concat!(
    "market-squawk/decision-workspace-policy/v1\n",
    "screen-runs=durable-append-order;candidate-order=rank-then-id;",
    "dossiers=id-order;targets=series-id-order\n",
    "candidate-cutoff=run-as-of-and-selected-at-at-or-before-query-as-of\n",
    "dossier-cutoff=assembled-at-at-or-before-query-as-of\n",
    "target-head=current-append-derived-head-and-status\n",
    "active-target=current-active-with-activation-at-or-before-as-of-and-",
    "effective-at<=as-of<review-due-at-and-expires-at\n",
    "truncation=fail-closed-no-partial-records"
)
.as_bytes();

/// Exact retained-decision selector for one instrument and one evidence cutoff.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct DecisionWorkspaceQuery {
    instrument_id: InstrumentId,
    as_of: Timestamp,
}

impl DecisionWorkspaceQuery {
    #[must_use]
    pub(crate) const fn new(instrument_id: InstrumentId, as_of: Timestamp) -> Self {
        Self {
            instrument_id,
            as_of,
        }
    }

    #[must_use]
    pub(crate) const fn instrument_id(self) -> InstrumentId {
        self.instrument_id
    }

    #[must_use]
    pub(crate) const fn as_of(self) -> Timestamp {
        self.as_of
    }
}

/// Independent scan and returned-record ceilings for one coherent decision read.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct DecisionWorkspaceReadLimits {
    maximum_screen_runs_scanned: NonZeroUsize,
    maximum_candidates_scanned: NonZeroUsize,
    maximum_dossiers_scanned: NonZeroUsize,
    maximum_target_series_scanned: NonZeroUsize,
    maximum_target_heads: NonZeroUsize,
}

impl DecisionWorkspaceReadLimits {
    pub(crate) fn try_new(
        maximum_screen_runs_scanned: NonZeroUsize,
        maximum_candidates_scanned: NonZeroUsize,
        maximum_dossiers_scanned: NonZeroUsize,
        maximum_target_series_scanned: NonZeroUsize,
        maximum_target_heads: NonZeroUsize,
    ) -> Result<Self, DecisionWorkspaceReadError> {
        if maximum_screen_runs_scanned.get() > MAXIMUM_SCREEN_RUN_SCAN
            || maximum_candidates_scanned.get() > MAXIMUM_CANDIDATE_SCAN
            || maximum_dossiers_scanned.get() > MAXIMUM_DOSSIER_SCAN
            || maximum_target_series_scanned.get() > MAXIMUM_TARGET_SERIES_SCAN
            || maximum_target_heads.get() > MAXIMUM_TARGET_HEADS
            || maximum_target_heads > maximum_target_series_scanned
        {
            return Err(DecisionWorkspaceReadError::InvalidLimits);
        }
        Ok(Self {
            maximum_screen_runs_scanned,
            maximum_candidates_scanned,
            maximum_dossiers_scanned,
            maximum_target_series_scanned,
            maximum_target_heads,
        })
    }
}

/// Whether the receipt represents every retained record admitted by the exact query.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DecisionWorkspaceCompleteness {
    Complete,
    Truncated,
}

/// Exact bound that prevented a complete decision-evidence selection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DecisionWorkspaceTruncationReason {
    ScreenRuns,
    Candidates,
    Dossiers,
    TargetSeries,
    TargetHeads,
}

/// Count evidence bound into a selection receipt.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct DecisionWorkspaceSelectionCounts {
    scanned_screen_runs: usize,
    scanned_candidates: usize,
    scanned_dossiers: usize,
    selected_screen_runs: usize,
    selected_candidates: usize,
    selected_dossiers: usize,
    scanned_target_series: usize,
    selected_target_heads: usize,
    active_targets: usize,
}

impl DecisionWorkspaceSelectionCounts {
    #[must_use]
    pub(crate) const fn scanned_screen_runs(self) -> usize {
        self.scanned_screen_runs
    }

    #[must_use]
    pub(crate) const fn scanned_candidates(self) -> usize {
        self.scanned_candidates
    }

    #[must_use]
    pub(crate) const fn scanned_dossiers(self) -> usize {
        self.scanned_dossiers
    }

    #[must_use]
    pub(crate) const fn selected_screen_runs(self) -> usize {
        self.selected_screen_runs
    }

    #[must_use]
    pub(crate) const fn selected_candidates(self) -> usize {
        self.selected_candidates
    }

    #[must_use]
    pub(crate) const fn selected_dossiers(self) -> usize {
        self.selected_dossiers
    }

    #[must_use]
    pub(crate) const fn scanned_target_series(self) -> usize {
        self.scanned_target_series
    }

    #[must_use]
    pub(crate) const fn selected_target_heads(self) -> usize {
        self.selected_target_heads
    }

    #[must_use]
    pub(crate) const fn active_targets(self) -> usize {
        self.active_targets
    }
}

/// Versioned proof of the exact query, policy, ordered records, and completeness state.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct DecisionWorkspaceSelectionReceipt {
    instrument_id: InstrumentId,
    as_of: Timestamp,
    policy_revision: u32,
    policy_digest: EvidenceDigest,
    query_digest: EvidenceDigest,
    record_set_digest: EvidenceDigest,
    selection_digest: EvidenceDigest,
    completeness: DecisionWorkspaceCompleteness,
    truncation_reason: Option<DecisionWorkspaceTruncationReason>,
    counts: DecisionWorkspaceSelectionCounts,
}

impl DecisionWorkspaceSelectionReceipt {
    #[must_use]
    pub(crate) const fn instrument_id(&self) -> InstrumentId {
        self.instrument_id
    }

    #[must_use]
    pub(crate) const fn as_of(&self) -> Timestamp {
        self.as_of
    }

    #[must_use]
    pub(crate) const fn policy_revision(&self) -> u32 {
        self.policy_revision
    }

    #[must_use]
    pub(crate) const fn policy_digest(&self) -> EvidenceDigest {
        self.policy_digest
    }

    #[must_use]
    pub(crate) const fn query_digest(&self) -> EvidenceDigest {
        self.query_digest
    }

    #[must_use]
    pub(crate) const fn record_set_digest(&self) -> EvidenceDigest {
        self.record_set_digest
    }

    #[must_use]
    pub(crate) const fn selection_digest(&self) -> EvidenceDigest {
        self.selection_digest
    }

    #[must_use]
    pub(crate) const fn completeness(&self) -> DecisionWorkspaceCompleteness {
        self.completeness
    }

    #[must_use]
    pub(crate) const fn truncation_reason(&self) -> Option<DecisionWorkspaceTruncationReason> {
        self.truncation_reason
    }

    #[must_use]
    pub(crate) const fn counts(&self) -> DecisionWorkspaceSelectionCounts {
        self.counts
    }
}

/// One exact candidate and only the dossiers that name that candidate and instrument.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct DecisionWorkspaceCandidate {
    assessment: CandidateAssessment,
    dossiers: Box<[DecisionDossier]>,
}

impl DecisionWorkspaceCandidate {
    #[must_use]
    pub(crate) const fn assessment(&self) -> &CandidateAssessment {
        &self.assessment
    }

    #[must_use]
    pub(crate) fn dossiers(&self) -> &[DecisionDossier] {
        &self.dossiers
    }
}

/// Candidate assessments kept under their exact immutable screen-run coordinate.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct DecisionWorkspaceCandidateRun {
    run: ScreenRun,
    candidates: Box<[DecisionWorkspaceCandidate]>,
}

impl DecisionWorkspaceCandidateRun {
    #[must_use]
    pub(crate) const fn run(&self) -> &ScreenRun {
        &self.run
    }

    #[must_use]
    pub(crate) fn candidates(&self) -> &[DecisionWorkspaceCandidate] {
        &self.candidates
    }
}

/// One exact current target-series head and its append-derived lifecycle evidence.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct DecisionWorkspaceTargetHead {
    index: TargetIndexEntry,
    content_identity: DecisionContentDigest,
    latest_review: Option<TargetReview>,
    latest_invalidation: Option<TargetInvalidation>,
    active_target_at_as_of: Option<TargetState>,
}

impl DecisionWorkspaceTargetHead {
    #[must_use]
    pub(crate) const fn index(&self) -> &TargetIndexEntry {
        &self.index
    }

    #[must_use]
    pub(crate) const fn content_identity(&self) -> DecisionContentDigest {
        self.content_identity
    }

    #[must_use]
    pub(crate) const fn latest_review(&self) -> Option<&TargetReview> {
        self.latest_review.as_ref()
    }

    #[must_use]
    pub(crate) const fn latest_invalidation(&self) -> Option<&TargetInvalidation> {
        self.latest_invalidation.as_ref()
    }

    #[must_use]
    pub(crate) const fn active_target_at_as_of(&self) -> Option<&TargetState> {
        self.active_target_at_as_of.as_ref()
    }
}

/// Complete retained decision evidence selected for one instrument workspace.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct DecisionWorkspaceSnapshot {
    instrument_id: InstrumentId,
    as_of: Timestamp,
    candidate_runs: Box<[DecisionWorkspaceCandidateRun]>,
    target_heads: Box<[DecisionWorkspaceTargetHead]>,
    selection_receipt: DecisionWorkspaceSelectionReceipt,
}

impl DecisionWorkspaceSnapshot {
    #[must_use]
    pub(crate) const fn instrument_id(&self) -> InstrumentId {
        self.instrument_id
    }

    #[must_use]
    pub(crate) const fn as_of(&self) -> Timestamp {
        self.as_of
    }

    #[must_use]
    pub(crate) fn candidate_runs(&self) -> &[DecisionWorkspaceCandidateRun] {
        &self.candidate_runs
    }

    #[must_use]
    pub(crate) fn target_heads(&self) -> &[DecisionWorkspaceTargetHead] {
        &self.target_heads
    }

    #[must_use]
    pub(crate) const fn selection_receipt(&self) -> &DecisionWorkspaceSelectionReceipt {
        &self.selection_receipt
    }
}

/// Complete data or a receipt proving why no partial decision view was returned.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum DecisionWorkspaceRead {
    Complete(DecisionWorkspaceSnapshot),
    Truncated(DecisionWorkspaceSelectionReceipt),
}

impl DecisionWorkspaceRead {
    #[must_use]
    pub(crate) const fn receipt(&self) -> &DecisionWorkspaceSelectionReceipt {
        match self {
            Self::Complete(snapshot) => snapshot.selection_receipt(),
            Self::Truncated(receipt) => receipt,
        }
    }
}

/// Read-side failures kept distinct from a successful complete-empty result or bounded truncation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DecisionWorkspaceReadError {
    InvalidLimits,
    Cancelled,
    DeadlineExceeded,
    Unavailable,
    Allocation,
    InvalidRetainedState,
}

impl fmt::Display for DecisionWorkspaceReadError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidLimits => "decision workspace limits are invalid",
            Self::Cancelled => "decision workspace read was cancelled",
            Self::DeadlineExceeded => "decision workspace read deadline was exceeded",
            Self::Unavailable => "decision workspace authority is unavailable",
            Self::Allocation => "decision workspace allocation failed",
            Self::InvalidRetainedState => "retained decision evidence is inconsistent",
        })
    }
}

impl std::error::Error for DecisionWorkspaceReadError {}

impl DecisionApplication {
    /// Reads one coherent retained-decision snapshot without invoking preparation or mutation.
    pub(crate) fn read_instrument_workspace(
        &self,
        query: DecisionWorkspaceQuery,
        limits: DecisionWorkspaceReadLimits,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<DecisionWorkspaceRead, DecisionWorkspaceReadError> {
        ensure_live(deadline, cancellation)?;
        let state = self.state.try_lock().map_err(|error| match error {
            TryLockError::WouldBlock | TryLockError::Poisoned(_) => {
                DecisionWorkspaceReadError::Unavailable
            }
        })?;
        if state.poisoned {
            return Err(DecisionWorkspaceReadError::Unavailable);
        }
        ensure_live(deadline, cancellation)?;
        read_authority(&state.authority, query, limits, deadline, cancellation)
    }
}

fn read_authority(
    authority: &DecisionAuthority,
    query: DecisionWorkspaceQuery,
    limits: DecisionWorkspaceReadLimits,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<DecisionWorkspaceRead, DecisionWorkspaceReadError> {
    let mut counts = DecisionWorkspaceSelectionCounts::default();
    let mut record_set_hash = Sha256::new();
    record_set_hash.update(b"market-squawk/decision-workspace-record-set/v1");
    let run_request = plus_one(limits.maximum_screen_runs_scanned)?;
    let mut run_entries = authority
        .list_screen_runs(run_request)
        .map_err(map_repository_error)?;
    let screen_runs_truncated = run_entries.len() > limits.maximum_screen_runs_scanned.get();
    run_entries.truncate(limits.maximum_screen_runs_scanned.get());
    counts.scanned_screen_runs = run_entries.len();
    let mut candidate_runs = Vec::new();
    candidate_runs
        .try_reserve_exact(run_entries.len())
        .map_err(|_error| DecisionWorkspaceReadError::Allocation)?;

    let mut truncation = None;
    'runs: for entry in run_entries {
        ensure_live(deadline, cancellation)?;
        let run = entry.run();
        record_set_hash.update([1]);
        hash_text(&mut record_set_hash, run.id().as_str())?;
        hash_text(&mut record_set_hash, run.screen().id().as_str())?;
        record_set_hash.update(run.screen().revision().get().to_be_bytes());
        record_set_hash.update(run.as_of().unix_nanos().to_be_bytes());
        hash_decision_content(&mut record_set_hash, run.dataset_identity());
        hash_decision_content(&mut record_set_hash, run.universe_identity());
        if run.as_of() > query.as_of {
            continue;
        }
        let assessments = authority
            .get_candidates(run.id())
            .map_err(map_repository_error)?;
        if assessments.len() != entry.candidate_count() {
            return Err(DecisionWorkspaceReadError::InvalidRetainedState);
        }
        let remaining_candidates = limits
            .maximum_candidates_scanned
            .get()
            .saturating_sub(counts.scanned_candidates);
        let candidates_truncated = assessments.len() > remaining_candidates;
        let scanned = &assessments[..assessments.len().min(remaining_candidates)];
        counts.scanned_candidates = counts
            .scanned_candidates
            .checked_add(scanned.len())
            .ok_or(DecisionWorkspaceReadError::InvalidRetainedState)?;

        let mut selected = Vec::new();
        selected
            .try_reserve_exact(scanned.len())
            .map_err(|_error| DecisionWorkspaceReadError::Allocation)?;
        for assessment in scanned {
            ensure_live(deadline, cancellation)?;
            let record = assessment.record();
            record_set_hash.update([2]);
            hash_text(&mut record_set_hash, record.id().as_str())?;
            record_set_hash.update(record.instrument_id().as_uuid().as_bytes());
            record_set_hash.update(record.rank().get().to_be_bytes());
            record_set_hash.update(record.selected_at().unix_nanos().to_be_bytes());
            hash_decision_content(&mut record_set_hash, assessment.evidence_identity());
            if record.screen_run_id() != run.id() || record.screen() != run.screen() {
                return Err(DecisionWorkspaceReadError::InvalidRetainedState);
            }
            if record.selected_at() > query.as_of || record.instrument_id() != query.instrument_id {
                continue;
            }

            let remaining_dossiers = limits
                .maximum_dossiers_scanned
                .get()
                .saturating_sub(counts.scanned_dossiers);
            let dossier_request = remaining_dossiers
                .checked_add(1)
                .ok_or(DecisionWorkspaceReadError::InvalidLimits)?;
            let mut dossiers = authority
                .list_candidate_dossiers(record.id(), dossier_request)
                .map_err(map_repository_error)?;
            let dossiers_truncated = dossiers.len() > remaining_dossiers;
            dossiers.truncate(remaining_dossiers);
            counts.scanned_dossiers = counts
                .scanned_dossiers
                .checked_add(dossiers.len())
                .ok_or(DecisionWorkspaceReadError::InvalidRetainedState)?;
            for dossier in &dossiers {
                record_set_hash.update([3]);
                hash_text(&mut record_set_hash, dossier.dossier().id().as_str())?;
                hash_decision_content(
                    &mut record_set_hash,
                    dossier.dossier().evidence().content_identity(),
                );
                if dossier.dossier().candidate_id() != record.id()
                    || dossier.dossier().instrument_id() != query.instrument_id
                {
                    return Err(DecisionWorkspaceReadError::InvalidRetainedState);
                }
            }
            dossiers.retain(|dossier| dossier.dossier().assembled_at() <= query.as_of);
            dossiers.sort_unstable_by(|left, right| left.dossier().id().cmp(right.dossier().id()));
            counts.selected_dossiers = counts
                .selected_dossiers
                .checked_add(dossiers.len())
                .ok_or(DecisionWorkspaceReadError::InvalidRetainedState)?;
            selected.push(DecisionWorkspaceCandidate {
                assessment: assessment.clone(),
                dossiers: dossiers.into_boxed_slice(),
            });
            counts.selected_candidates = counts
                .selected_candidates
                .checked_add(1)
                .ok_or(DecisionWorkspaceReadError::InvalidRetainedState)?;
            if dossiers_truncated {
                truncation = Some(DecisionWorkspaceTruncationReason::Dossiers);
                break;
            }
        }
        selected.sort_unstable_by(|left, right| {
            left.assessment
                .record()
                .rank()
                .cmp(&right.assessment.record().rank())
                .then_with(|| {
                    left.assessment
                        .record()
                        .id()
                        .cmp(right.assessment.record().id())
                })
        });
        if !selected.is_empty() {
            counts.selected_screen_runs = counts
                .selected_screen_runs
                .checked_add(1)
                .ok_or(DecisionWorkspaceReadError::InvalidRetainedState)?;
            candidate_runs.push(DecisionWorkspaceCandidateRun {
                run: run.clone(),
                candidates: selected.into_boxed_slice(),
            });
        }
        if truncation.is_some() {
            break 'runs;
        }
        if candidates_truncated {
            truncation = Some(DecisionWorkspaceTruncationReason::Candidates);
            break 'runs;
        }
    }
    candidate_runs.sort_unstable_by(|left, right| left.run.id().cmp(right.run.id()));
    if truncation.is_none() && screen_runs_truncated {
        truncation = Some(DecisionWorkspaceTruncationReason::ScreenRuns);
    }
    if let Some(reason) = truncation {
        ensure_live(deadline, cancellation)?;
        let receipt = selection_receipt(
            query,
            limits,
            DecisionWorkspaceCompleteness::Truncated,
            Some(reason),
            counts,
            record_set_digest(&record_set_hash),
            &candidate_runs,
            &[],
            deadline,
            cancellation,
        )?;
        return Ok(DecisionWorkspaceRead::Truncated(receipt));
    }

    let target_request = plus_one(limits.maximum_target_series_scanned)?;
    let mut target_entries = authority
        .list_target_index(target_request)
        .map_err(map_repository_error)?;
    let target_series_truncated = target_entries.len() > limits.maximum_target_series_scanned.get();
    target_entries.truncate(limits.maximum_target_series_scanned.get());
    counts.scanned_target_series = target_entries.len();
    let mut target_heads = Vec::new();
    target_heads
        .try_reserve_exact(target_entries.len().min(limits.maximum_target_heads.get()))
        .map_err(|_error| DecisionWorkspaceReadError::Allocation)?;
    for index in target_entries {
        ensure_live(deadline, cancellation)?;
        record_set_hash.update([4]);
        hash_text(&mut record_set_hash, index.id().as_str())?;
        record_set_hash.update(index.revision().get().to_be_bytes());
        record_set_hash.update(index.instrument_id().as_uuid().as_bytes());
        record_set_hash.update([target_status_tag(index.status())]);
        if index.instrument_id() != query.instrument_id {
            continue;
        }
        if target_heads.len() == limits.maximum_target_heads.get() {
            truncation = Some(DecisionWorkspaceTruncationReason::TargetHeads);
            break;
        }
        let state = authority
            .get_target(index.id(), index.revision())
            .map_err(map_repository_error)?;
        let governed = state.target();
        let target = governed.target();
        let dossier = authority
            .get_dossier(target.dossier_id())
            .map_err(map_repository_error)?;
        if target.id() != index.id()
            || target.revision() != index.revision()
            || target.instrument_id() != query.instrument_id
            || state.status() != index.status()
            || dossier.dossier().id() != target.dossier_id()
            || dossier.dossier().instrument_id() != query.instrument_id
        {
            return Err(DecisionWorkspaceReadError::InvalidRetainedState);
        }
        let active_target_at_as_of = is_active_at_as_of(&state, query.as_of).then(|| state.clone());
        if active_target_at_as_of.is_some() {
            counts.active_targets = counts
                .active_targets
                .checked_add(1)
                .ok_or(DecisionWorkspaceReadError::InvalidRetainedState)?;
        }
        target_heads.push(DecisionWorkspaceTargetHead {
            index,
            content_identity: target.content_identity(),
            latest_review: state.latest_review().cloned(),
            latest_invalidation: state.latest_invalidation().cloned(),
            active_target_at_as_of,
        });
        counts.selected_target_heads = counts
            .selected_target_heads
            .checked_add(1)
            .ok_or(DecisionWorkspaceReadError::InvalidRetainedState)?;
    }
    target_heads.sort_unstable_by(|left, right| left.index.id().cmp(right.index.id()));
    if truncation.is_none() && target_series_truncated {
        truncation = Some(DecisionWorkspaceTruncationReason::TargetSeries);
    }
    ensure_live(deadline, cancellation)?;
    if let Some(reason) = truncation {
        let receipt = selection_receipt(
            query,
            limits,
            DecisionWorkspaceCompleteness::Truncated,
            Some(reason),
            counts,
            record_set_digest(&record_set_hash),
            &candidate_runs,
            &target_heads,
            deadline,
            cancellation,
        )?;
        return Ok(DecisionWorkspaceRead::Truncated(receipt));
    }

    let selection_receipt = selection_receipt(
        query,
        limits,
        DecisionWorkspaceCompleteness::Complete,
        None,
        counts,
        record_set_digest(&record_set_hash),
        &candidate_runs,
        &target_heads,
        deadline,
        cancellation,
    )?;
    Ok(DecisionWorkspaceRead::Complete(DecisionWorkspaceSnapshot {
        instrument_id: query.instrument_id,
        as_of: query.as_of,
        candidate_runs: candidate_runs.into_boxed_slice(),
        target_heads: target_heads.into_boxed_slice(),
        selection_receipt,
    }))
}

fn is_active_at_as_of(state: &TargetState, as_of: Timestamp) -> bool {
    let governed = state.target();
    let target = governed.target();
    state.status() == TargetStatus::Active
        && state
            .approval()
            .is_some_and(|review| review.reviewed_at() <= as_of)
        && governed.effective_at() <= as_of
        && as_of < governed.review_due_at()
        && as_of < target.expires_at()
}

#[allow(
    clippy::too_many_arguments,
    reason = "query, policy outcome, counts, and the two ordered evidence families are independent receipt inputs"
)]
fn selection_receipt(
    query: DecisionWorkspaceQuery,
    limits: DecisionWorkspaceReadLimits,
    completeness: DecisionWorkspaceCompleteness,
    truncation_reason: Option<DecisionWorkspaceTruncationReason>,
    counts: DecisionWorkspaceSelectionCounts,
    record_set_digest: EvidenceDigest,
    candidate_runs: &[DecisionWorkspaceCandidateRun],
    target_heads: &[DecisionWorkspaceTargetHead],
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<DecisionWorkspaceSelectionReceipt, DecisionWorkspaceReadError> {
    if (completeness == DecisionWorkspaceCompleteness::Complete && truncation_reason.is_some())
        || (completeness == DecisionWorkspaceCompleteness::Truncated && truncation_reason.is_none())
    {
        return Err(DecisionWorkspaceReadError::InvalidRetainedState);
    }
    let policy_digest = EvidenceDigest::new(
        DigestAlgorithm::Sha256,
        Sha256::digest(POLICY_CANONICAL_BYTES).into(),
    );
    let query_digest = query_digest(query, limits)?;
    let selection_digest = selection_digest(
        query_digest,
        policy_digest,
        completeness,
        truncation_reason,
        counts,
        record_set_digest,
        candidate_runs,
        target_heads,
        deadline,
        cancellation,
    )?;
    Ok(DecisionWorkspaceSelectionReceipt {
        instrument_id: query.instrument_id,
        as_of: query.as_of,
        policy_revision: POLICY_REVISION,
        policy_digest,
        query_digest,
        record_set_digest,
        selection_digest,
        completeness,
        truncation_reason,
        counts,
    })
}

fn query_digest(
    query: DecisionWorkspaceQuery,
    limits: DecisionWorkspaceReadLimits,
) -> Result<EvidenceDigest, DecisionWorkspaceReadError> {
    let mut hash = Sha256::new();
    hash.update(b"market-squawk/decision-workspace-query/v1");
    hash.update(query.instrument_id.as_uuid().as_bytes());
    hash.update(query.as_of.unix_nanos().to_be_bytes());
    for value in [
        limits.maximum_screen_runs_scanned.get(),
        limits.maximum_candidates_scanned.get(),
        limits.maximum_dossiers_scanned.get(),
        limits.maximum_target_series_scanned.get(),
        limits.maximum_target_heads.get(),
    ] {
        hash_count(&mut hash, value)?;
    }
    Ok(EvidenceDigest::new(
        DigestAlgorithm::Sha256,
        hash.finalize().into(),
    ))
}

#[allow(
    clippy::too_many_arguments,
    reason = "every independently authenticated selection dimension remains explicit"
)]
fn selection_digest(
    query_digest: EvidenceDigest,
    policy_digest: EvidenceDigest,
    completeness: DecisionWorkspaceCompleteness,
    truncation_reason: Option<DecisionWorkspaceTruncationReason>,
    counts: DecisionWorkspaceSelectionCounts,
    record_set_digest: EvidenceDigest,
    candidate_runs: &[DecisionWorkspaceCandidateRun],
    target_heads: &[DecisionWorkspaceTargetHead],
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<EvidenceDigest, DecisionWorkspaceReadError> {
    let mut hash = Sha256::new();
    hash.update(b"market-squawk/decision-workspace-selection/v1");
    hash.update(POLICY_REVISION.to_be_bytes());
    hash_evidence(&mut hash, policy_digest);
    hash_evidence(&mut hash, query_digest);
    hash_evidence(&mut hash, record_set_digest);
    hash.update([match completeness {
        DecisionWorkspaceCompleteness::Complete => 0,
        DecisionWorkspaceCompleteness::Truncated => 1,
    }]);
    hash.update([truncation_reason.map_or(0, truncation_tag)]);
    for value in [
        counts.scanned_screen_runs,
        counts.scanned_candidates,
        counts.scanned_dossiers,
        counts.selected_screen_runs,
        counts.selected_candidates,
        counts.selected_dossiers,
        counts.scanned_target_series,
        counts.selected_target_heads,
        counts.active_targets,
    ] {
        hash_count(&mut hash, value)?;
    }
    hash_count(&mut hash, candidate_runs.len())?;
    for group in candidate_runs {
        ensure_live(deadline, cancellation)?;
        let run = &group.run;
        hash_text(&mut hash, run.id().as_str())?;
        hash_text(&mut hash, run.screen().id().as_str())?;
        hash.update(run.screen().revision().get().to_be_bytes());
        hash.update(run.as_of().unix_nanos().to_be_bytes());
        hash_decision_content(&mut hash, run.dataset_identity());
        hash_decision_content(&mut hash, run.universe_identity());
        hash_count(&mut hash, run.feature_bindings().len())?;
        for binding in run.feature_bindings() {
            hash_text(&mut hash, binding.key().name())?;
            hash.update(binding.key().version().get().to_be_bytes());
            hash.update(binding.semantic_digest().as_bytes());
        }
        hash_count(&mut hash, group.candidates.len())?;
        for selected in &group.candidates {
            ensure_live(deadline, cancellation)?;
            let assessment = &selected.assessment;
            let record = assessment.record();
            hash_text(&mut hash, record.id().as_str())?;
            hash.update(record.rank().get().to_be_bytes());
            hash.update(record.score().get().to_bits().to_be_bytes());
            hash.update(record.selected_at().unix_nanos().to_be_bytes());
            hash.update(assessment.coverage().get().to_bits().to_be_bytes());
            hash.update(assessment.liquidity().get().to_bits().to_be_bytes());
            hash.update([data_quality_tag(assessment.data_quality())]);
            hash_decision_content(&mut hash, assessment.evidence_identity());
            hash_count(&mut hash, selected.dossiers.len())?;
            for dossier in &selected.dossiers {
                ensure_live(deadline, cancellation)?;
                hash_text(&mut hash, dossier.dossier().id().as_str())?;
                hash.update(dossier.dossier().assembled_at().unix_nanos().to_be_bytes());
                hash_decision_content(&mut hash, dossier.dossier().evidence().content_identity());
                hash_count(&mut hash, dossier.references().len())?;
                for reference in dossier.references() {
                    hash.update([dossier_section_tag(reference.section())]);
                    hash_decision_content(&mut hash, reference.content_identity());
                }
            }
        }
    }
    hash_count(&mut hash, target_heads.len())?;
    for head in target_heads {
        ensure_live(deadline, cancellation)?;
        hash_text(&mut hash, head.index.id().as_str())?;
        hash.update(head.index.revision().get().to_be_bytes());
        hash.update([target_status_tag(head.index.status())]);
        hash_decision_content(&mut hash, head.content_identity);
        match &head.latest_review {
            Some(review) => {
                hash.update([1]);
                hash_text(&mut hash, review.id().as_str())?;
                hash.update(review.reviewed_at().unix_nanos().to_be_bytes());
                hash.update([review_disposition_tag(review.disposition())]);
                hash_decision_content(&mut hash, review.content_identity());
            }
            None => hash.update([0]),
        }
        match &head.latest_invalidation {
            Some(invalidation) => {
                hash.update([1]);
                hash_text(&mut hash, invalidation.id().as_str())?;
                hash.update(invalidation.observed_at().unix_nanos().to_be_bytes());
                hash.update([invalidation_kind_tag(invalidation.kind())]);
                hash_decision_content(&mut hash, invalidation.content_identity());
            }
            None => hash.update([0]),
        }
        hash.update([u8::from(head.active_target_at_as_of.is_some())]);
    }
    Ok(EvidenceDigest::new(
        DigestAlgorithm::Sha256,
        hash.finalize().into(),
    ))
}

fn ensure_live(
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<(), DecisionWorkspaceReadError> {
    if cancellation.is_cancelled() {
        Err(DecisionWorkspaceReadError::Cancelled)
    } else if Instant::now() >= deadline {
        Err(DecisionWorkspaceReadError::DeadlineExceeded)
    } else {
        Ok(())
    }
}

fn plus_one(value: NonZeroUsize) -> Result<usize, DecisionWorkspaceReadError> {
    value
        .get()
        .checked_add(1)
        .ok_or(DecisionWorkspaceReadError::InvalidLimits)
}

fn map_repository_error(error: DecisionRepositoryError) -> DecisionWorkspaceReadError {
    match error {
        DecisionRepositoryError::Allocation => DecisionWorkspaceReadError::Allocation,
        DecisionRepositoryError::InvalidLimits
        | DecisionRepositoryError::Capacity
        | DecisionRepositoryError::Conflict
        | DecisionRepositoryError::StaleRevision
        | DecisionRepositoryError::NotFound
        | DecisionRepositoryError::EvidenceMismatch => {
            DecisionWorkspaceReadError::InvalidRetainedState
        }
    }
}

fn hash_count(hash: &mut Sha256, value: usize) -> Result<(), DecisionWorkspaceReadError> {
    hash.update(
        u64::try_from(value)
            .map_err(|_error| DecisionWorkspaceReadError::InvalidRetainedState)?
            .to_be_bytes(),
    );
    Ok(())
}

fn hash_text(hash: &mut Sha256, value: &str) -> Result<(), DecisionWorkspaceReadError> {
    hash_count(hash, value.len())?;
    hash.update(value.as_bytes());
    Ok(())
}

fn hash_evidence(hash: &mut Sha256, digest: EvidenceDigest) {
    hash.update([match digest.algorithm() {
        DigestAlgorithm::Sha256 => 0,
        DigestAlgorithm::Blake3 => 1,
    }]);
    hash.update(digest.bytes());
}

fn hash_decision_content(hash: &mut Sha256, digest: DecisionContentDigest) {
    hash_evidence(hash, digest.evidence_digest());
}

fn record_set_digest(hash: &Sha256) -> EvidenceDigest {
    EvidenceDigest::new(DigestAlgorithm::Sha256, hash.clone().finalize().into())
}

const fn truncation_tag(reason: DecisionWorkspaceTruncationReason) -> u8 {
    match reason {
        DecisionWorkspaceTruncationReason::ScreenRuns => 1,
        DecisionWorkspaceTruncationReason::Candidates => 2,
        DecisionWorkspaceTruncationReason::Dossiers => 3,
        DecisionWorkspaceTruncationReason::TargetSeries => 4,
        DecisionWorkspaceTruncationReason::TargetHeads => 5,
    }
}

const fn target_status_tag(status: TargetStatus) -> u8 {
    match status {
        TargetStatus::PendingReview => 0,
        TargetStatus::Active => 1,
        TargetStatus::Rejected => 2,
        TargetStatus::NeedsChanges => 3,
        TargetStatus::NeedsReview => 4,
        TargetStatus::Superseded => 5,
    }
}

const fn review_disposition_tag(disposition: TargetReviewDisposition) -> u8 {
    match disposition {
        TargetReviewDisposition::Activate => 0,
        TargetReviewDisposition::Reject => 1,
        TargetReviewDisposition::NeedsChanges => 2,
    }
}

const fn invalidation_kind_tag(kind: InvalidationKind) -> u8 {
    match kind {
        InvalidationKind::CorporateAction => 0,
        InvalidationKind::Model => 1,
        InvalidationKind::Data => 2,
        InvalidationKind::ReferenceMark => 3,
        InvalidationKind::Assumption => 4,
    }
}

const fn dossier_section_tag(section: DossierSection) -> u8 {
    match section {
        DossierSection::Data => 0,
        DossierSection::CorporateActions => 1,
        DossierSection::Fundamentals => 2,
        DossierSection::Forecast => 3,
        DossierSection::PortfolioImpact => 4,
        DossierSection::FairValue => 5,
        DossierSection::DecisionContext => 6,
    }
}

const fn data_quality_tag(quality: DataQuality) -> u8 {
    match quality {
        DataQuality::DirectVerified => 0,
        DataQuality::DirectUnverified => 1,
        DataQuality::OfficialDelayed => 2,
        DataQuality::Aggregated => 3,
        DataQuality::Indicative => 4,
        DataQuality::Modeled => 5,
        DataQuality::Estimated => 6,
        DataQuality::Stale => 7,
        DataQuality::Quarantined => 8,
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use market_squawk_analytics::{FeatureOutputType, StatisticalF64};
    use market_squawk_decisions::{
        AsOfSemantics, ComparisonOperator, DecisionContractError, DecisionRepository,
        DecisionRepositoryLimits, NullPolicy, RankingDirection, SavedScreen, ScreenConstraints,
        ScreenFeatureBinding, ScreenId, ScreenPredicate, ScreenRanking, ScreenRevision, ScreenRun,
        ScreenRunId,
    };
    use market_squawk_domain::{DataQuality, RevisionNumber};
    use market_squawk_modeling::ProductionFeatureRegistry;

    use super::*;

    #[test]
    fn complete_empty_and_bounded_truncation_are_distinct_and_read_only()
    -> Result<(), Box<dyn std::error::Error>> {
        let query = DecisionWorkspaceQuery::new(
            "018f8f6a-9d6f-7b43-9f38-55db5f4b0e01".parse()?,
            Timestamp::from_unix_nanos(100),
        );
        let one = NonZeroUsize::MIN;
        let limits = DecisionWorkspaceReadLimits::try_new(one, one, one, one, one)?;
        let cancellation = CancellationToken::new();
        let deadline = Instant::now() + Duration::from_secs(2);
        let empty = DecisionAuthority::new(DecisionRepository::try_new(repository_limits()?)?);
        let complete = read_authority(&empty, query, limits, deadline, &cancellation)?;
        let DecisionWorkspaceRead::Complete(snapshot) = complete else {
            return Err("empty authority was not complete".into());
        };
        assert!(snapshot.candidate_runs().is_empty());
        assert!(snapshot.target_heads().is_empty());

        let populated = authority_with_two_runs()?;
        let before = populated.repository().try_snapshot()?;
        let truncated = read_authority(
            &populated,
            query,
            limits,
            Instant::now() + Duration::from_secs(2),
            &cancellation,
        )?;
        let DecisionWorkspaceRead::Truncated(receipt) = truncated else {
            return Err("bounded scan did not fail closed".into());
        };
        assert_eq!(
            receipt.truncation_reason(),
            Some(DecisionWorkspaceTruncationReason::ScreenRuns)
        );
        assert_ne!(
            receipt.selection_digest(),
            snapshot.selection_receipt().selection_digest()
        );
        assert_eq!(populated.repository().try_snapshot()?, before);
        Ok(())
    }

    fn repository_limits() -> Result<DecisionRepositoryLimits, DecisionRepositoryError> {
        DecisionRepositoryLimits::try_new(4, 4, 4, 4, 4, 4, 4, 4)
    }

    fn authority_with_two_runs() -> Result<DecisionAuthority, Box<dyn std::error::Error>> {
        let registry = ProductionFeatureRegistry::try_new()?;
        let metadata = registry
            .feature_registry()
            .entries()
            .find(|metadata| {
                metadata.is_point_in_time_compatible()
                    && metadata.output_type() == FeatureOutputType::StatisticalF64
            })
            .ok_or(DecisionContractError::UnknownScreenFeature)?;
        let binding = ScreenFeatureBinding::new(metadata.key().clone(), metadata.semantic_digest());
        let saved = SavedScreen::try_new(
            ScreenRevision::new(
                ScreenId::try_new("screen.workspace")?,
                RevisionNumber::new(1)?,
            ),
            content_digest(1)?,
            AsOfSemantics::AvailableAtOrBeforeCutoff,
            vec![ScreenPredicate::new(
                binding.clone(),
                ComparisonOperator::GreaterThanOrEqual,
                StatisticalF64::try_new(0.0)?,
                NullPolicy::Exclude,
            )],
            ScreenRanking::new(binding.clone(), RankingDirection::Descending),
            NonZeroUsize::MIN,
            ScreenConstraints::try_new(
                StatisticalF64::try_new(0.0)?,
                StatisticalF64::try_new(0.0)?,
                vec![DataQuality::DirectVerified],
            )?,
            registry.feature_registry(),
        )?;
        let mut authority =
            DecisionAuthority::new(DecisionRepository::try_new(repository_limits()?)?);
        authority.save_screen(None, saved.clone())?;
        for (id, at, digest) in [("run.workspace.one", 10, 2), ("run.workspace.two", 20, 3)] {
            authority.run_screen(
                ScreenRun::try_new(
                    ScreenRunId::try_new(id)?,
                    saved.revision().clone(),
                    Timestamp::from_unix_nanos(at),
                    content_digest(digest)?,
                    saved.universe_identity(),
                    vec![binding.clone()],
                )?,
                Vec::new(),
                Timestamp::from_unix_nanos(at),
            )?;
        }
        Ok(authority)
    }

    fn content_digest(byte: u8) -> Result<DecisionContentDigest, DecisionContractError> {
        DecisionContentDigest::try_new(EvidenceDigest::new(DigestAlgorithm::Sha256, [byte; 32]))
    }
}
