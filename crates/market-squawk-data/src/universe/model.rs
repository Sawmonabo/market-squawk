//! Public historical-universe contracts and bounded result types.

use std::fmt;
use std::str::FromStr;

use market_squawk_domain::{
    AvailabilityEvidence, EffectiveInterval, EvidenceDigest, InstrumentId, Timestamp,
};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use thiserror::Error;

use crate::{DatasetManifestRef, Sha256Digest};

/// Fixed process ceiling for caller-bounded universe candidates.
pub const MAX_UNIVERSE_CANDIDATES: usize = 1_000_000;
/// Fixed process ceiling for Rust-visible memory retained by one universe result.
pub const MAX_UNIVERSE_RETAINED_BYTES: usize = 512 * 1024 * 1024;

/// Stable canonical identity of a historical instrument universe.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct UniverseId(Box<str>);

impl UniverseId {
    /// Maximum canonical identity length in ASCII bytes.
    pub const MAX_LENGTH: usize = 128;

    /// Returns the canonical universe identity.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<&str> for UniverseId {
    type Error = UniverseError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        validate_universe_id(value)?;
        Ok(Self(value.into()))
    }
}

impl TryFrom<String> for UniverseId {
    type Error = UniverseError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        validate_universe_id(&value)?;
        Ok(Self(value.into_boxed_str()))
    }
}

impl FromStr for UniverseId {
    type Err = UniverseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::try_from(value)
    }
}

impl fmt::Display for UniverseId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl Serialize for UniverseId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for UniverseId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::try_from(value).map_err(serde::de::Error::custom)
    }
}

fn validate_universe_id(value: &str) -> Result<(), UniverseError> {
    if value.is_empty() {
        return Err(UniverseError::InvalidUniverseId);
    }
    if value.len() > UniverseId::MAX_LENGTH {
        return Err(UniverseError::UniverseIdTooLong {
            max: UniverseId::MAX_LENGTH,
            observed: value.len(),
        });
    }
    let bytes = value.as_bytes();
    let canonical_edge = |byte: &u8| byte.is_ascii_lowercase() || byte.is_ascii_digit();
    if !bytes.first().is_some_and(canonical_edge)
        || !bytes.last().is_some_and(canonical_edge)
        || bytes.iter().any(|byte| {
            !(byte.is_ascii_lowercase()
                || byte.is_ascii_digit()
                || matches!(byte, b'-' | b'_' | b'.'))
        })
    {
        return Err(UniverseError::InvalidUniverseId);
    }
    Ok(())
}

/// Immutable evidence that an instrument belonged to a universe over a half-open interval.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UniverseMembership {
    pub(super) instrument_id: InstrumentId,
    pub(super) effective_interval: EffectiveInterval,
    pub(super) availability: AvailabilityEvidence,
    pub(super) source_manifest: DatasetManifestRef,
    pub(super) evidence_digest: EvidenceDigest,
}

impl UniverseMembership {
    /// Binds membership semantics to immutable source and exact evidence identities.
    pub const fn new(
        instrument_id: InstrumentId,
        effective_interval: EffectiveInterval,
        availability: AvailabilityEvidence,
        source_manifest: DatasetManifestRef,
        evidence_digest: EvidenceDigest,
    ) -> Self {
        Self {
            instrument_id,
            effective_interval,
            availability,
            source_manifest,
            evidence_digest,
        }
    }

    /// Returns the stable internal instrument identity.
    pub const fn instrument_id(&self) -> InstrumentId {
        self.instrument_id
    }

    /// Returns the source-supplied half-open membership interval.
    pub const fn effective_interval(&self) -> EffectiveInterval {
        self.effective_interval
    }

    /// Returns the retained availability evidence used by point-in-time admission.
    pub const fn availability(&self) -> &AvailabilityEvidence {
        &self.availability
    }

    /// Returns the immutable source dataset generation.
    pub const fn source_manifest(&self) -> &DatasetManifestRef {
        &self.source_manifest
    }

    /// Returns the exact source membership-evidence digest.
    pub const fn evidence_digest(&self) -> EvidenceDigest {
        self.evidence_digest
    }
}

/// Fail-closed reason a candidate was not admitted to a point-in-time snapshot.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum UniverseExclusionReason {
    /// The half-open effective interval does not contain the snapshot instant.
    NotEffective,
    /// Conservative evidence establishes availability only after the snapshot instant.
    FutureAvailability,
    /// Availability was inferred and therefore is not admitted by the default policy.
    InferredAvailability,
    /// Historical availability cannot be established.
    UnknownAvailability,
}

/// Retained candidate and typed reason for a point-in-time exclusion.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UniverseExclusion {
    pub(super) membership: UniverseMembership,
    pub(super) reason: UniverseExclusionReason,
}

impl UniverseExclusion {
    /// Returns the complete excluded membership evidence.
    pub const fn membership(&self) -> &UniverseMembership {
        &self.membership
    }

    /// Returns the fail-closed exclusion reason.
    pub const fn reason(&self) -> UniverseExclusionReason {
        self.reason
    }
}

/// Complete counts for all candidates excluded from a build.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct UniverseExclusionCounts {
    pub(super) total: usize,
    pub(super) not_effective: usize,
    pub(super) future_availability: usize,
    pub(super) inferred_availability: usize,
    pub(super) unknown_availability: usize,
}

impl UniverseExclusionCounts {
    /// Returns the number of all excluded candidates.
    pub const fn total(self) -> usize {
        self.total
    }

    /// Returns candidates outside their effective interval at the snapshot instant.
    pub const fn not_effective(self) -> usize {
        self.not_effective
    }

    /// Returns candidates whose conservative availability is after the snapshot instant.
    pub const fn future_availability(self) -> usize {
        self.future_availability
    }

    /// Returns candidates excluded because availability was inferred.
    pub const fn inferred_availability(self) -> usize {
        self.inferred_availability
    }

    /// Returns candidates excluded because availability is unknown.
    pub const fn unknown_availability(self) -> usize {
        self.unknown_availability
    }

    pub(super) fn record(&mut self, reason: UniverseExclusionReason) {
        self.total += 1;
        match reason {
            UniverseExclusionReason::NotEffective => self.not_effective += 1,
            UniverseExclusionReason::FutureAvailability => self.future_availability += 1,
            UniverseExclusionReason::InferredAvailability => self.inferred_availability += 1,
            UniverseExclusionReason::UnknownAvailability => self.unknown_availability += 1,
        }
    }
}

/// Complete overlap counts for admitted candidates sharing an instrument.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct UniverseConflictCounts {
    pub(super) conflicting_instruments: usize,
    pub(super) conflicting_memberships: usize,
    pub(super) overlap_pairs: u64,
}

impl UniverseConflictCounts {
    /// Returns the number of stable instruments with conflicting admitted memberships.
    pub const fn conflicting_instruments(self) -> usize {
        self.conflicting_instruments
    }

    /// Returns all admitted memberships participating in conflicts.
    pub const fn conflicting_memberships(self) -> usize {
        self.conflicting_memberships
    }

    /// Returns the number of overlapping membership pairs across all conflicting instruments.
    pub const fn overlap_pairs(self) -> u64 {
        self.overlap_pairs
    }
}

/// Deterministically ordered competing memberships retained when a build rejects overlap.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UniverseConflictEvidence {
    pub(super) memberships: Vec<UniverseMembership>,
    pub(super) retained_bytes: usize,
}

impl UniverseConflictEvidence {
    /// Returns every competing membership, including immutable source manifest and digest terms.
    pub fn memberships(&self) -> &[UniverseMembership] {
        &self.memberships
    }

    /// Returns checked Rust-visible bytes retained by the evidence vector and its owned strings.
    pub const fn retained_bytes(&self) -> usize {
        self.retained_bytes
    }
}

/// Explicit caller bounds for one historical-universe build.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct UniverseLimits {
    pub(super) max_candidates: usize,
    pub(super) max_retained_bytes: usize,
}

impl UniverseLimits {
    /// Constructs nonzero work and retained-memory bounds within fixed process ceilings.
    ///
    /// # Errors
    ///
    /// Returns [`UniverseError::InvalidLimits`] for zero or excessive limits.
    pub fn try_new(
        max_candidates: usize,
        max_retained_bytes: usize,
    ) -> Result<Self, UniverseError> {
        if max_candidates == 0
            || max_candidates > MAX_UNIVERSE_CANDIDATES
            || max_retained_bytes == 0
            || max_retained_bytes > MAX_UNIVERSE_RETAINED_BYTES
        {
            Err(UniverseError::InvalidLimits)
        } else {
            Ok(Self {
                max_candidates,
                max_retained_bytes,
            })
        }
    }

    /// Returns the maximum candidates examined and retained by one build.
    pub const fn max_candidates(self) -> usize {
        self.max_candidates
    }

    /// Returns the maximum Rust-visible bytes retained by a snapshot or conflict report.
    pub const fn max_retained_bytes(self) -> usize {
        self.max_retained_bytes
    }
}

/// Deterministic point-in-time universe and its complete fail-closed audit result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UniverseSnapshot {
    pub(super) universe_id: UniverseId,
    pub(super) as_of: Timestamp,
    pub(super) memberships: Vec<UniverseMembership>,
    pub(super) exclusions: Vec<UniverseExclusion>,
    pub(super) exclusion_counts: UniverseExclusionCounts,
    pub(super) conflict_counts: UniverseConflictCounts,
    pub(super) content_hash: Sha256Digest,
    pub(super) audit_hash: Sha256Digest,
    pub(super) retained_bytes: usize,
}

impl UniverseSnapshot {
    /// Returns the stable universe identity.
    pub const fn universe_id(&self) -> &UniverseId {
        &self.universe_id
    }

    /// Returns the sole point-in-time instant governing the snapshot.
    pub const fn as_of(&self) -> Timestamp {
        self.as_of
    }

    /// Returns admitted memberships sorted by stable instrument identity and evidence terms.
    pub fn memberships(&self) -> &[UniverseMembership] {
        &self.memberships
    }

    /// Returns all excluded candidates in deterministic evidence order.
    pub fn exclusions(&self) -> &[UniverseExclusion] {
        &self.exclusions
    }

    /// Returns complete exclusion counts.
    pub const fn exclusion_counts(&self) -> UniverseExclusionCounts {
        self.exclusion_counts
    }

    /// Returns complete conflict counts; successful snapshots always report zero conflicts.
    pub const fn conflict_counts(&self) -> UniverseConflictCounts {
        self.conflict_counts
    }

    /// Returns the canonical SHA-256 identity of the admitted point-in-time membership set.
    pub const fn content_hash(&self) -> Sha256Digest {
        self.content_hash
    }

    /// Returns the canonical SHA-256 identity of admissions, exclusions, and decision counts.
    pub const fn audit_hash(&self) -> Sha256Digest {
        self.audit_hash
    }

    /// Returns checked Rust-visible bytes retained by this result and its owned strings.
    pub const fn retained_bytes(&self) -> usize {
        self.retained_bytes
    }

    /// Returns whether the stable instrument is admitted.
    pub fn contains(&self, instrument_id: InstrumentId) -> bool {
        self.membership(instrument_id).is_some()
    }

    /// Returns the unique admitted membership for a stable instrument.
    pub fn membership(&self, instrument_id: InstrumentId) -> Option<&UniverseMembership> {
        self.memberships
            .binary_search_by_key(&instrument_id, UniverseMembership::instrument_id)
            .ok()
            .map(|index| &self.memberships[index])
    }
}

/// Historical-universe construction failure.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum UniverseError {
    /// Universe identity is empty or not canonical lowercase ASCII.
    #[error("universe identity must be lowercase ASCII with canonical separators")]
    InvalidUniverseId,
    /// Universe identity exceeds its fixed encoded bound.
    #[error("universe identity is {observed} bytes; maximum is {max}")]
    UniverseIdTooLong {
        /// Maximum accepted bytes.
        max: usize,
        /// Observed bytes.
        observed: usize,
    },
    /// Caller limits are zero or exceed fixed process ceilings.
    #[error("universe limits must be nonzero and within process ceilings")]
    InvalidLimits,
    /// Candidate work exceeds the caller-selected bound.
    #[error("universe has {observed} candidates; caller limit is {limit}")]
    CandidateLimitExceeded {
        /// Caller-selected candidate limit.
        limit: usize,
        /// Candidates presented to the build.
        observed: usize,
    },
    /// Multiple point-in-time-admissible memberships overlap for a stable instrument.
    #[error("overlapping admitted memberships begin with instrument {first_instrument}")]
    OverlappingAdmittedMemberships {
        /// First conflicting stable instrument in deterministic order.
        first_instrument: InstrumentId,
        /// Complete conflict counts across the rejected build.
        conflicts: UniverseConflictCounts,
        /// Bounded competing membership records with immutable source and digest terms.
        conflict_evidence: UniverseConflictEvidence,
        /// Complete exclusion counts computed before rejecting conflicts.
        exclusions: UniverseExclusionCounts,
    },
    /// Snapshot or conflict evidence would exceed the caller-selected retained-memory bound.
    #[error("universe requires {required} retained bytes; caller limit is {limit}")]
    RetainedByteLimitExceeded {
        /// Caller-selected retained-byte limit.
        limit: usize,
        /// Checked Rust-visible bytes required by the result.
        required: usize,
    },
    /// A retained-size calculation exceeded the platform representation.
    #[error("universe retained-size calculation overflow")]
    RetainedSizeOverflow,
    /// A fallible bounded allocation could not be reserved.
    #[error("universe bounded allocation failed")]
    AllocationFailed,
    /// A platform-size value cannot be represented by the canonical encoding.
    #[error("universe canonical encoding overflow")]
    CanonicalEncodingOverflow,
}
