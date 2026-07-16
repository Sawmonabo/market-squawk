//! Generation-bound sequence, snapshot, and checksum evidence.

use std::fmt;
use std::num::NonZeroU32;

use serde::Serialize;

use super::{AssessmentValidity, SequenceIntegrity, SnapshotConsistency};
use crate::{ConnectionGeneration, SequenceNumber, SourceIdentifier};

#[path = "integrity/checksum.rs"]
mod checksum;

pub use checksum::{
    ChecksumEvidence, ChecksumScope, ChecksumTarget, ChecksumValue, PayloadChecksumScope,
};

/// Provider metadata declaration for sequence support.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SequenceCapability {
    /// The selected protocol/channel supplies sequence information.
    Provided,
    /// Authoritative metadata declares that it supplies no sequence information.
    Unsupported,
}

/// Provider metadata declaration for checksum support.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ChecksumCapability {
    /// The selected protocol/channel supplies a verifiable checksum.
    Provided,
    /// Authoritative metadata declares that it supplies no checksum.
    Unsupported,
}

/// Sequence and checksum capabilities declared by source metadata.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub struct IntegrityCapabilities {
    sequence: SequenceCapability,
    checksum: ChecksumCapability,
}

impl IntegrityCapabilities {
    /// Constructs an immutable source capability declaration.
    pub const fn new(sequence: SequenceCapability, checksum: ChecksumCapability) -> Self {
        Self { sequence, checksum }
    }

    /// Returns the declared sequence capability.
    pub const fn sequence(self) -> SequenceCapability {
        self.sequence
    }

    /// Returns the declared checksum capability.
    pub const fn checksum(self) -> ChecksumCapability {
        self.checksum
    }
}

/// One-based provider validation-rule version.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct RuleVersion(NonZeroU32);

impl RuleVersion {
    /// Constructs a one-based rule version.
    ///
    /// # Errors
    ///
    /// Returns [`IntegrityEvidenceError::ZeroRuleVersion`] for zero.
    pub fn new(value: u32) -> Result<Self, IntegrityEvidenceError> {
        NonZeroU32::new(value)
            .map(Self)
            .ok_or(IntegrityEvidenceError::ZeroRuleVersion)
    }

    /// Returns the primitive rule version.
    pub const fn get(self) -> u32 {
        self.0.get()
    }
}

/// Provider-owned rule identity and version retained with validation evidence.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct IntegrityRule {
    provider_rule: SourceIdentifier,
    version: RuleVersion,
}

impl IntegrityRule {
    /// Constructs a provider validation rule reference.
    pub const fn new(provider_rule: SourceIdentifier, version: RuleVersion) -> Self {
        Self {
            provider_rule,
            version,
        }
    }

    /// Returns the provider rule identity.
    pub const fn provider_rule(&self) -> &SourceIdentifier {
        &self.provider_rule
    }

    /// Returns the provider rule version.
    pub const fn version(&self) -> RuleVersion {
        self.version
    }
}

/// Sequence progression semantics implemented by the selected provider validator.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SequenceValidationRule {
    /// Each update must be exactly one greater than the prior update.
    Consecutive,
    /// Each update must be strictly greater than the prior update.
    Monotonic,
}

/// A generation-bound, auditable sequence validation result.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct SequenceEvidence {
    capability: SequenceCapability,
    rule: Option<IntegrityRule>,
    validation_rule: Option<SequenceValidationRule>,
    connection_generation: ConnectionGeneration,
    snapshot_sequence: Option<SequenceNumber>,
    previous_sequence: Option<SequenceNumber>,
    observed_sequence: Option<SequenceNumber>,
    integrity: SequenceIntegrity,
}

impl SequenceEvidence {
    /// Validates a supplied sequence under an explicit provider rule.
    ///
    /// # Errors
    ///
    /// Rejects capability contradictions, missing rules/observations, and sequence overflow.
    pub fn validate(
        capability: SequenceCapability,
        rule: Option<IntegrityRule>,
        validation_rule: SequenceValidationRule,
        connection_generation: ConnectionGeneration,
        snapshot_sequence: Option<SequenceNumber>,
        previous_sequence: Option<SequenceNumber>,
        observed_sequence: Option<SequenceNumber>,
    ) -> Result<Self, IntegrityEvidenceError> {
        if capability != SequenceCapability::Provided {
            return Err(IntegrityEvidenceError::CapabilityContradiction {
                evidence: IntegrityEvidenceKind::Sequence,
            });
        }
        let rule = rule.ok_or(IntegrityEvidenceError::MissingRule {
            evidence: IntegrityEvidenceKind::Sequence,
        })?;
        let observed = observed_sequence.ok_or(IntegrityEvidenceError::MissingObservation {
            evidence: IntegrityEvidenceKind::Sequence,
        })?;
        let integrity = match previous_sequence {
            Some(previous) => match validation_rule {
                SequenceValidationRule::Consecutive => {
                    let expected = previous
                        .checked_next()
                        .map_err(|_| IntegrityEvidenceError::SequenceOverflow)?;
                    if observed == expected {
                        SequenceIntegrity::Valid
                    } else {
                        SequenceIntegrity::Invalid
                    }
                }
                SequenceValidationRule::Monotonic => {
                    if observed > previous {
                        SequenceIntegrity::Valid
                    } else {
                        SequenceIntegrity::Invalid
                    }
                }
            },
            None => match snapshot_sequence {
                Some(snapshot) if observed == snapshot => SequenceIntegrity::Valid,
                Some(_) => SequenceIntegrity::Invalid,
                None => SequenceIntegrity::Uninitialized,
            },
        };
        Ok(Self {
            capability,
            rule: Some(rule),
            validation_rule: Some(validation_rule),
            connection_generation,
            snapshot_sequence,
            previous_sequence,
            observed_sequence: Some(observed),
            integrity,
        })
    }

    /// Retains a supported sequence capability before an observation is available.
    pub fn uninitialized(
        rule: IntegrityRule,
        validation_rule: SequenceValidationRule,
        connection_generation: ConnectionGeneration,
        snapshot_sequence: Option<SequenceNumber>,
    ) -> Self {
        Self {
            capability: SequenceCapability::Provided,
            rule: Some(rule),
            validation_rule: Some(validation_rule),
            connection_generation,
            snapshot_sequence,
            previous_sequence: None,
            observed_sequence: None,
            integrity: SequenceIntegrity::Uninitialized,
        }
    }

    /// Retains an authoritative absence of sequence capability.
    pub const fn unsupported(connection_generation: ConnectionGeneration) -> Self {
        Self {
            capability: SequenceCapability::Unsupported,
            rule: None,
            validation_rule: None,
            connection_generation,
            snapshot_sequence: None,
            previous_sequence: None,
            observed_sequence: None,
            integrity: SequenceIntegrity::NotSupported,
        }
    }

    /// Returns the declared capability embedded in this evidence.
    pub const fn capability(&self) -> SequenceCapability {
        self.capability
    }

    /// Returns the provider rule reference when supported.
    pub const fn rule(&self) -> Option<&IntegrityRule> {
        self.rule.as_ref()
    }

    /// Returns the assessed connection generation.
    pub const fn connection_generation(&self) -> ConnectionGeneration {
        self.connection_generation
    }

    /// Returns the snapshot anchor retained by the validator.
    pub const fn snapshot_sequence(&self) -> Option<SequenceNumber> {
        self.snapshot_sequence
    }

    /// Returns the sequence immediately preceding the observation.
    pub const fn previous_sequence(&self) -> Option<SequenceNumber> {
        self.previous_sequence
    }

    /// Returns the observed provider sequence.
    pub const fn observed_sequence(&self) -> Option<SequenceNumber> {
        self.observed_sequence
    }

    /// Returns the derived validation result.
    pub const fn integrity(&self) -> SequenceIntegrity {
        self.integrity
    }
}

impl AssessmentValidity for SequenceEvidence {}

/// Exact initialized snapshot state, independent of provider sequence capability.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct InitializedSnapshot {
    connection_generation: ConnectionGeneration,
    snapshot_identity: SourceIdentifier,
    state_digest: super::EvidenceDigest,
    initialized_at: crate::Timestamp,
    sequence: Option<SequenceNumber>,
}

impl InitializedSnapshot {
    /// Constructs an explicit initialized snapshot state.
    pub const fn new(
        connection_generation: ConnectionGeneration,
        snapshot_identity: SourceIdentifier,
        state_digest: super::EvidenceDigest,
        initialized_at: crate::Timestamp,
        sequence: Option<SequenceNumber>,
    ) -> Self {
        Self {
            connection_generation,
            snapshot_identity,
            state_digest,
            initialized_at,
            sequence,
        }
    }

    /// Returns the connection generation in which initialization occurred.
    pub const fn connection_generation(&self) -> ConnectionGeneration {
        self.connection_generation
    }
    /// Returns the snapshot identity.
    pub const fn snapshot_identity(&self) -> &SourceIdentifier {
        &self.snapshot_identity
    }
    /// Returns the canonical snapshot digest.
    pub const fn state_digest(&self) -> super::EvidenceDigest {
        self.state_digest
    }
    /// Returns when initialization completed.
    pub const fn initialized_at(&self) -> crate::Timestamp {
        self.initialized_at
    }
    /// Returns the optional provider sequence without conflating absence with initialization.
    pub const fn sequence(&self) -> Option<SequenceNumber> {
        self.sequence
    }
}

/// Explicit snapshot presence for one connection generation.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case", tag = "state")]
pub enum SnapshotState {
    /// A complete snapshot identity, digest, time, and optional sequence are retained.
    Initialized(InitializedSnapshot),
    /// No qualifying snapshot has initialized this generation.
    Uninitialized {
        /// Generation that remains uninitialized.
        connection_generation: ConnectionGeneration,
    },
}

/// Provider metadata declaration for snapshot applicability to an event class.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum SnapshotApplicability {
    /// Event processing requires an initialized snapshot.
    Required,
    /// Provider metadata explicitly declares snapshots inapplicable for this event class.
    NotApplicable {
        /// Metadata rule supporting non-applicability.
        metadata_rule: IntegrityRule,
    },
}

/// A generation-bound snapshot/update consistency assessment.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct SnapshotEvidence {
    state: SnapshotState,
    observed_generation: ConnectionGeneration,
    observed_sequence: Option<SequenceNumber>,
    consistency: SnapshotConsistency,
}

impl SnapshotEvidence {
    /// Assesses an explicit initialized snapshot against an observation.
    ///
    /// # Errors
    ///
    /// Returns a typed error if the snapshot and observation generations differ.
    pub fn assess_initialized(
        initialized: InitializedSnapshot,
        observed_generation: ConnectionGeneration,
        observed_sequence: Option<SequenceNumber>,
    ) -> Result<Self, IntegrityEvidenceError> {
        if initialized.connection_generation != observed_generation {
            return Err(IntegrityEvidenceError::GenerationMismatch {
                expected: initialized.connection_generation,
                found: observed_generation,
            });
        }
        let consistency = match (initialized.sequence, observed_sequence) {
            (Some(snapshot), Some(observed)) if observed < snapshot => {
                SnapshotConsistency::Inconsistent
            }
            _ => SnapshotConsistency::Consistent,
        };
        Ok(Self {
            state: SnapshotState::Initialized(initialized),
            observed_generation,
            observed_sequence,
            consistency,
        })
    }

    /// Represents a generation for which no qualifying snapshot exists.
    pub const fn uninitialized(connection_generation: ConnectionGeneration) -> Self {
        Self {
            state: SnapshotState::Uninitialized {
                connection_generation,
            },
            observed_generation: connection_generation,
            observed_sequence: None,
            consistency: SnapshotConsistency::Uninitialized,
        }
    }

    /// Returns the assessed generation.
    pub const fn connection_generation(&self) -> ConnectionGeneration {
        self.observed_generation
    }

    /// Returns the retained snapshot sequence.
    pub const fn snapshot_sequence(&self) -> Option<SequenceNumber> {
        match &self.state {
            SnapshotState::Initialized(initialized) => initialized.sequence,
            SnapshotState::Uninitialized { .. } => None,
        }
    }

    /// Returns the retained observed sequence.
    pub const fn observed_sequence(&self) -> Option<SequenceNumber> {
        self.observed_sequence
    }

    /// Returns the derived snapshot consistency.
    pub const fn consistency(&self) -> SnapshotConsistency {
        self.consistency
    }

    /// Returns the explicit snapshot state.
    pub const fn state(&self) -> &SnapshotState {
        &self.state
    }

    /// Returns true only for a complete initialized snapshot state.
    pub const fn is_initialized(&self) -> bool {
        matches!(&self.state, SnapshotState::Initialized(_))
    }
}

impl AssessmentValidity for SnapshotEvidence {}

/// Integrity evidence family used in typed construction failures.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IntegrityEvidenceKind {
    /// Sequence evidence.
    Sequence,
    /// Checksum evidence.
    Checksum,
}

/// A contradiction or invalid boundary in integrity evidence construction.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IntegrityEvidenceError {
    /// Rule versions are one-based.
    ZeroRuleVersion,
    /// A checksum cannot cover zero levels.
    ZeroChecksumLevels,
    /// Supplied details contradict an unsupported capability declaration.
    CapabilityContradiction {
        /// The contradictory evidence family.
        evidence: IntegrityEvidenceKind,
    },
    /// A supported capability lacks a provider rule reference.
    MissingRule {
        /// The incomplete evidence family.
        evidence: IntegrityEvidenceKind,
    },
    /// A supported capability lacks its provider observation.
    MissingObservation {
        /// The incomplete evidence family.
        evidence: IntegrityEvidenceKind,
    },
    /// Supported checksum evidence lacks the locally computed value.
    MissingComputation,
    /// Supported checksum evidence lacks a declared scope.
    MissingChecksumScope,
    /// Supported non-book checksum evidence lacks a declared payload scope.
    MissingPayloadChecksumScope,
    /// A consecutive sequence cannot advance past `u64::MAX`.
    SequenceOverflow,
    /// Snapshot and observation belong to different connection generations.
    GenerationMismatch {
        /// Snapshot generation.
        expected: ConnectionGeneration,
        /// Observation generation.
        found: ConnectionGeneration,
    },
}

impl fmt::Display for IntegrityEvidenceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ZeroRuleVersion => formatter.write_str("integrity rule version must be nonzero"),
            Self::ZeroChecksumLevels => formatter.write_str("checksum scope must include levels"),
            Self::CapabilityContradiction { evidence } => {
                write!(
                    formatter,
                    "{evidence:?} evidence contradicts declared capability"
                )
            }
            Self::MissingRule { evidence } => {
                write!(formatter, "{evidence:?} evidence requires a provider rule")
            }
            Self::MissingObservation { evidence } => {
                write!(formatter, "{evidence:?} evidence requires an observation")
            }
            Self::MissingComputation => {
                formatter.write_str("checksum evidence requires a computed checksum")
            }
            Self::MissingChecksumScope => {
                formatter.write_str("checksum evidence requires an exact scope")
            }
            Self::MissingPayloadChecksumScope => {
                formatter.write_str("payload checksum evidence requires an exact payload scope")
            }
            Self::SequenceOverflow => formatter.write_str("consecutive sequence overflow"),
            Self::GenerationMismatch { expected, found } => write!(
                formatter,
                "integrity generation {found} does not match snapshot generation {expected}"
            ),
        }
    }
}

impl std::error::Error for IntegrityEvidenceError {}
