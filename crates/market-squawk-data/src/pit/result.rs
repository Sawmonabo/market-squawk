//! Point-in-time decisions, complete counts, conflict evidence, and typed failures.

use thiserror::Error;

use super::PointInTimeCandidate;
use crate::Sha256Digest;

/// One typed reason bit in a complete candidate exclusion.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[repr(u8)]
pub enum PointInTimeExclusionReason {
    AvailabilityAfterAsOf = 0,
    InferredAvailability = 1,
    UnknownAvailability = 2,
    PublicationAfterAsOf = 3,
    PublicationAfterCutoff = 4,
    PublicationIncomparable = 5,
    EffectiveAfterCutoff = 6,
    EffectiveNotAfterCutoff = 7,
    EffectiveAfterLabelCutoff = 8,
    EffectiveIncomparable = 9,
    SupersededByKnowledgeTime = 10,
    SupersessionIncomparable = 11,
    LowerRevision = 12,
    DuplicateRevision = 13,
}

/// Nonempty set of every applicable reason for one excluded candidate.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct PointInTimeExclusionReasons(u16);

impl PointInTimeExclusionReasons {
    /// Returns whether the complete set contains `reason`.
    pub const fn contains(self, reason: PointInTimeExclusionReason) -> bool {
        self.0 & reason_bit(reason) != 0
    }

    /// Returns whether no reason has been recorded.
    pub const fn is_empty(self) -> bool {
        self.0 == 0
    }

    pub(super) fn insert(&mut self, reason: PointInTimeExclusionReason) {
        self.0 |= reason_bit(reason);
    }

    pub(super) const fn bits(self) -> u16 {
        self.0
    }
}

const fn reason_bit(reason: PointInTimeExclusionReason) -> u16 {
    1_u16 << reason as u8
}

/// Complete per-reason exclusion counts; reason counts may exceed excluded candidates.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct PointInTimeExclusionCounts {
    excluded_candidates: usize,
    by_reason: [usize; 14],
}

impl PointInTimeExclusionCounts {
    pub const fn excluded_candidates(self) -> usize {
        self.excluded_candidates
    }

    pub const fn availability_after_as_of(self) -> usize {
        self.count(PointInTimeExclusionReason::AvailabilityAfterAsOf)
    }

    pub const fn inferred_availability(self) -> usize {
        self.count(PointInTimeExclusionReason::InferredAvailability)
    }

    pub const fn unknown_availability(self) -> usize {
        self.count(PointInTimeExclusionReason::UnknownAvailability)
    }

    pub const fn publication_after_as_of(self) -> usize {
        self.count(PointInTimeExclusionReason::PublicationAfterAsOf)
    }

    pub const fn publication_after_cutoff(self) -> usize {
        self.count(PointInTimeExclusionReason::PublicationAfterCutoff)
    }

    pub const fn publication_incomparable(self) -> usize {
        self.count(PointInTimeExclusionReason::PublicationIncomparable)
    }

    pub const fn effective_after_cutoff(self) -> usize {
        self.count(PointInTimeExclusionReason::EffectiveAfterCutoff)
    }

    pub const fn effective_not_after_cutoff(self) -> usize {
        self.count(PointInTimeExclusionReason::EffectiveNotAfterCutoff)
    }

    pub const fn effective_after_label_cutoff(self) -> usize {
        self.count(PointInTimeExclusionReason::EffectiveAfterLabelCutoff)
    }

    pub const fn effective_incomparable(self) -> usize {
        self.count(PointInTimeExclusionReason::EffectiveIncomparable)
    }

    pub const fn superseded_by_knowledge_time(self) -> usize {
        self.count(PointInTimeExclusionReason::SupersededByKnowledgeTime)
    }

    pub const fn supersession_incomparable(self) -> usize {
        self.count(PointInTimeExclusionReason::SupersessionIncomparable)
    }

    pub const fn lower_revision(self) -> usize {
        self.count(PointInTimeExclusionReason::LowerRevision)
    }

    pub const fn duplicate_revision(self) -> usize {
        self.count(PointInTimeExclusionReason::DuplicateRevision)
    }

    const fn count(self, reason: PointInTimeExclusionReason) -> usize {
        self.by_reason[reason as usize]
    }

    pub(super) fn record(&mut self, reasons: PointInTimeExclusionReasons) {
        self.excluded_candidates += 1;
        for reason in ALL_EXCLUSION_REASONS {
            if reasons.contains(reason) {
                self.by_reason[reason as usize] += 1;
            }
        }
    }

    pub(super) const fn counts(self) -> [usize; 14] {
        self.by_reason
    }
}

const ALL_EXCLUSION_REASONS: [PointInTimeExclusionReason; 14] = [
    PointInTimeExclusionReason::AvailabilityAfterAsOf,
    PointInTimeExclusionReason::InferredAvailability,
    PointInTimeExclusionReason::UnknownAvailability,
    PointInTimeExclusionReason::PublicationAfterAsOf,
    PointInTimeExclusionReason::PublicationAfterCutoff,
    PointInTimeExclusionReason::PublicationIncomparable,
    PointInTimeExclusionReason::EffectiveAfterCutoff,
    PointInTimeExclusionReason::EffectiveNotAfterCutoff,
    PointInTimeExclusionReason::EffectiveAfterLabelCutoff,
    PointInTimeExclusionReason::EffectiveIncomparable,
    PointInTimeExclusionReason::SupersededByKnowledgeTime,
    PointInTimeExclusionReason::SupersessionIncomparable,
    PointInTimeExclusionReason::LowerRevision,
    PointInTimeExclusionReason::DuplicateRevision,
];

/// Explicit currentness state for a selected revision.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum PointInTimeRevisionState {
    Current,
    Superseded,
    SupersessionIncomparable,
}

/// Counts for explicit selected revision states.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct PointInTimeRevisionCounts {
    current: usize,
    superseded_history: usize,
    incomparable_history: usize,
}

impl PointInTimeRevisionCounts {
    pub const fn current(self) -> usize {
        self.current
    }

    pub const fn superseded_history(self) -> usize {
        self.superseded_history
    }

    pub const fn incomparable_history(self) -> usize {
        self.incomparable_history
    }

    pub(super) fn record(&mut self, state: PointInTimeRevisionState) {
        match state {
            PointInTimeRevisionState::Current => self.current += 1,
            PointInTimeRevisionState::Superseded => self.superseded_history += 1,
            PointInTimeRevisionState::SupersessionIncomparable => {
                self.incomparable_history += 1;
            }
        }
    }

    pub(super) const fn values(self) -> [usize; 3] {
        [
            self.current,
            self.superseded_history,
            self.incomparable_history,
        ]
    }
}

/// Selected immutable evidence and its canonical identities.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PointInTimeRecord<'a> {
    pub(super) candidate: &'a PointInTimeCandidate,
    pub(super) family_identity: Sha256Digest,
    pub(super) payload_identity: Sha256Digest,
    pub(super) provenance_identity: Sha256Digest,
    pub(super) evidence_identity: Sha256Digest,
    pub(super) revision_state: PointInTimeRevisionState,
}

impl<'a> PointInTimeRecord<'a> {
    pub const fn candidate(&self) -> &'a PointInTimeCandidate {
        self.candidate
    }

    pub const fn family_identity(&self) -> Sha256Digest {
        self.family_identity
    }

    pub const fn payload_identity(&self) -> Sha256Digest {
        self.payload_identity
    }

    pub const fn provenance_identity(&self) -> Sha256Digest {
        self.provenance_identity
    }

    pub const fn evidence_identity(&self) -> Sha256Digest {
        self.evidence_identity
    }

    pub const fn revision_state(&self) -> PointInTimeRevisionState {
        self.revision_state
    }
}

/// Complete excluded candidate and every applicable typed reason.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PointInTimeExclusion<'a> {
    pub(super) record: PointInTimeRecord<'a>,
    pub(super) reasons: PointInTimeExclusionReasons,
}

impl<'a> PointInTimeExclusion<'a> {
    pub const fn candidate(&self) -> &'a PointInTimeCandidate {
        self.record.candidate
    }

    pub const fn reasons(&self) -> PointInTimeExclusionReasons {
        self.reasons
    }

    pub const fn record(&self) -> PointInTimeRecord<'a> {
        self.record
    }
}

/// Complete counts for divergent same-family/same-revision payload groups.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct PointInTimeConflictCounts {
    pub(super) conflicting_groups: usize,
    pub(super) conflicting_candidates: usize,
    pub(super) payload_variants: usize,
}

impl PointInTimeConflictCounts {
    pub const fn conflicting_groups(self) -> usize {
        self.conflicting_groups
    }

    pub const fn conflicting_candidates(self) -> usize {
        self.conflicting_candidates
    }

    pub const fn payload_variants(self) -> usize {
        self.payload_variants
    }
}

/// All exact evidence for one divergent family/revision group.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PointInTimeConflict<'a> {
    pub(super) family_identity: Sha256Digest,
    pub(super) revision: market_squawk_domain::RevisionNumber,
    pub(super) records: Vec<PointInTimeRecord<'a>>,
}

impl<'a> PointInTimeConflict<'a> {
    pub const fn family_identity(&self) -> Sha256Digest {
        self.family_identity
    }

    pub const fn revision(&self) -> market_squawk_domain::RevisionNumber {
        self.revision
    }

    pub fn records(&self) -> &[PointInTimeRecord<'a>] {
        &self.records
    }
}

/// Fail-closed conflict result with complete exclusions and deterministic audit identity.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PointInTimeConflictReport<'a> {
    pub(super) conflicts: Vec<PointInTimeConflict<'a>>,
    pub(super) conflict_counts: PointInTimeConflictCounts,
    pub(super) exclusions: Vec<PointInTimeExclusion<'a>>,
    pub(super) exclusion_counts: PointInTimeExclusionCounts,
    pub(super) audit_identity: Sha256Digest,
    pub(super) retained_bytes: usize,
}

impl<'a> PointInTimeConflictReport<'a> {
    pub fn conflicts(&self) -> &[PointInTimeConflict<'a>] {
        &self.conflicts
    }

    pub const fn conflict_counts(&self) -> PointInTimeConflictCounts {
        self.conflict_counts
    }

    pub fn exclusions(&self) -> &[PointInTimeExclusion<'a>] {
        &self.exclusions
    }

    pub const fn exclusion_counts(&self) -> PointInTimeExclusionCounts {
        self.exclusion_counts
    }

    pub const fn audit_identity(&self) -> Sha256Digest {
        self.audit_identity
    }

    pub const fn retained_bytes(&self) -> usize {
        self.retained_bytes
    }
}

/// Deterministically ordered usable point-in-time observations and complete exclusions.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PointInTimeSelection<'a> {
    pub(super) records: Vec<PointInTimeRecord<'a>>,
    pub(super) exclusions: Vec<PointInTimeExclusion<'a>>,
    pub(super) exclusion_counts: PointInTimeExclusionCounts,
    pub(super) revision_counts: PointInTimeRevisionCounts,
    pub(super) content_identity: Sha256Digest,
    pub(super) audit_identity: Sha256Digest,
    pub(super) retained_bytes: usize,
}

impl<'a> PointInTimeSelection<'a> {
    pub fn records(&self) -> &[PointInTimeRecord<'a>] {
        &self.records
    }

    pub fn exclusions(&self) -> &[PointInTimeExclusion<'a>] {
        &self.exclusions
    }

    pub const fn exclusion_counts(&self) -> PointInTimeExclusionCounts {
        self.exclusion_counts
    }

    pub const fn revision_counts(&self) -> PointInTimeRevisionCounts {
        self.revision_counts
    }

    pub const fn content_identity(&self) -> Sha256Digest {
        self.content_identity
    }

    pub const fn audit_identity(&self) -> Sha256Digest {
        self.audit_identity
    }

    pub const fn retained_bytes(&self) -> usize {
        self.retained_bytes
    }
}

/// Point-in-time construction or fail-closed conflict failure.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum PointInTimeError<'a> {
    #[error("point-in-time policy version {found} is unsupported")]
    UnsupportedPolicyVersion { found: u32 },
    #[error("point-in-time limits must be nonzero and within fixed process ceilings")]
    InvalidLimits,
    #[error("label cutoff must be comparable to and strictly after the effective cutoff")]
    InvalidLabelWindow,
    #[error("point-in-time input has {observed} candidates; caller limit is {limit}")]
    CandidateLimitExceeded { limit: usize, observed: usize },
    #[error("point-in-time input has {observed} families; caller limit is {limit}")]
    FamilyLimitExceeded { limit: usize, observed: usize },
    #[error("point-in-time input has {observed} conflicts; caller limit is {limit}")]
    ConflictLimitExceeded { limit: usize, observed: usize },
    #[error("point-in-time selection has {observed} rows; caller limit is {limit}")]
    ResultRowLimitExceeded { limit: usize, observed: usize },
    #[error("point-in-time selection retains {observed} bytes; caller limit is {limit}")]
    RetainedBytesExceeded { limit: usize, observed: usize },
    #[error("point-in-time allocation reservation failed")]
    AllocationFailure,
    #[error("point-in-time checked accounting overflow")]
    AccountingOverflow,
    #[error("point-in-time canonical identity encoding failed")]
    CanonicalEncoding,
    #[error("point-in-time selection cancelled")]
    Cancelled,
    #[error("point-in-time selection deadline exceeded")]
    DeadlineExceeded,
    #[error("point-in-time selection rejected divergent same-revision payloads")]
    RevisionConflicts {
        report: Box<PointInTimeConflictReport<'a>>,
    },
}
