//! Generation-bound sequence, snapshot, and checksum evidence.

use std::fmt;
use std::num::NonZeroU32;

use serde::Serialize;

use super::{ChecksumIntegrity, MarketDepth, SequenceIntegrity, SnapshotConsistency};
use crate::{ConnectionGeneration, SequenceNumber, SourceIdentifier};

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
    #[allow(clippy::too_many_arguments)]
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

/// A generation-bound snapshot/update consistency assessment.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub struct SnapshotEvidence {
    connection_generation: ConnectionGeneration,
    snapshot_sequence: Option<SequenceNumber>,
    observed_sequence: Option<SequenceNumber>,
    consistency: SnapshotConsistency,
}

impl SnapshotEvidence {
    /// Assesses generation and sequence ordering against an initialized snapshot.
    ///
    /// # Errors
    ///
    /// Returns a typed error if the snapshot and observation generations differ.
    pub fn assess(
        snapshot_generation: ConnectionGeneration,
        observed_generation: ConnectionGeneration,
        snapshot_sequence: Option<SequenceNumber>,
        observed_sequence: Option<SequenceNumber>,
    ) -> Result<Self, IntegrityEvidenceError> {
        if snapshot_generation != observed_generation {
            return Err(IntegrityEvidenceError::GenerationMismatch {
                expected: snapshot_generation,
                found: observed_generation,
            });
        }
        let consistency = match (snapshot_sequence, observed_sequence) {
            (Some(snapshot), Some(observed)) if observed < snapshot => {
                SnapshotConsistency::Inconsistent
            }
            _ => SnapshotConsistency::Consistent,
        };
        Ok(Self {
            connection_generation: snapshot_generation,
            snapshot_sequence,
            observed_sequence,
            consistency,
        })
    }

    /// Represents a generation for which no qualifying snapshot exists.
    pub const fn uninitialized(connection_generation: ConnectionGeneration) -> Self {
        Self {
            connection_generation,
            snapshot_sequence: None,
            observed_sequence: None,
            consistency: SnapshotConsistency::Uninitialized,
        }
    }

    /// Returns the assessed generation.
    pub const fn connection_generation(self) -> ConnectionGeneration {
        self.connection_generation
    }

    /// Returns the retained snapshot sequence.
    pub const fn snapshot_sequence(self) -> Option<SequenceNumber> {
        self.snapshot_sequence
    }

    /// Returns the retained observed sequence.
    pub const fn observed_sequence(self) -> Option<SequenceNumber> {
        self.observed_sequence
    }

    /// Returns the derived snapshot consistency.
    pub const fn consistency(self) -> SnapshotConsistency {
        self.consistency
    }
}

/// Provider-defined checksum value widened for protocol-specific integer sizes.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize)]
#[serde(transparent)]
pub struct ChecksumValue(u64);

impl ChecksumValue {
    /// Constructs a checksum value.
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// Returns the checksum scalar.
    pub const fn get(self) -> u64 {
        self.0
    }
}

/// Exact order-book scope covered by a checksum rule.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ChecksumScope {
    depth: MarketDepth,
    level_count: NonZeroU32,
    provider_scope: SourceIdentifier,
}

impl ChecksumScope {
    /// Constructs a nonempty provider checksum scope.
    ///
    /// # Errors
    ///
    /// Returns [`IntegrityEvidenceError::ZeroChecksumLevels`] for zero levels.
    pub fn new(
        depth: MarketDepth,
        level_count: u32,
        provider_scope: SourceIdentifier,
    ) -> Result<Self, IntegrityEvidenceError> {
        let level_count =
            NonZeroU32::new(level_count).ok_or(IntegrityEvidenceError::ZeroChecksumLevels)?;
        Ok(Self {
            depth,
            level_count,
            provider_scope,
        })
    }

    /// Returns the market-depth class covered by this checksum.
    pub const fn depth(&self) -> MarketDepth {
        self.depth
    }

    /// Returns the number of price/order levels included by the provider rule.
    pub const fn level_count(&self) -> u32 {
        self.level_count.get()
    }

    /// Returns the provider's scope identity.
    pub const fn provider_scope(&self) -> &SourceIdentifier {
        &self.provider_scope
    }
}

/// A generation-bound, auditable checksum validation result.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ChecksumEvidence {
    capability: ChecksumCapability,
    rule: Option<IntegrityRule>,
    connection_generation: ConnectionGeneration,
    scope: Option<ChecksumScope>,
    expected: Option<ChecksumValue>,
    computed: Option<ChecksumValue>,
    integrity: ChecksumIntegrity,
}

impl ChecksumEvidence {
    /// Compares expected and computed checksums under an explicit provider rule and scope.
    ///
    /// # Errors
    ///
    /// Rejects capability contradictions and incomplete supported-checksum evidence.
    pub fn validate(
        capability: ChecksumCapability,
        rule: Option<IntegrityRule>,
        connection_generation: ConnectionGeneration,
        scope: Option<ChecksumScope>,
        expected: Option<ChecksumValue>,
        computed: Option<ChecksumValue>,
    ) -> Result<Self, IntegrityEvidenceError> {
        if capability != ChecksumCapability::Provided {
            return Err(IntegrityEvidenceError::CapabilityContradiction {
                evidence: IntegrityEvidenceKind::Checksum,
            });
        }
        let rule = rule.ok_or(IntegrityEvidenceError::MissingRule {
            evidence: IntegrityEvidenceKind::Checksum,
        })?;
        let scope = scope.ok_or(IntegrityEvidenceError::MissingChecksumScope)?;
        let expected = expected.ok_or(IntegrityEvidenceError::MissingObservation {
            evidence: IntegrityEvidenceKind::Checksum,
        })?;
        let computed = computed.ok_or(IntegrityEvidenceError::MissingComputation)?;
        let integrity = if expected == computed {
            ChecksumIntegrity::Valid
        } else {
            ChecksumIntegrity::Failed
        };
        Ok(Self {
            capability,
            rule: Some(rule),
            connection_generation,
            scope: Some(scope),
            expected: Some(expected),
            computed: Some(computed),
            integrity,
        })
    }

    /// Retains a supported checksum capability before comparison completes.
    pub fn unchecked(
        rule: IntegrityRule,
        connection_generation: ConnectionGeneration,
        scope: ChecksumScope,
    ) -> Self {
        Self {
            capability: ChecksumCapability::Provided,
            rule: Some(rule),
            connection_generation,
            scope: Some(scope),
            expected: None,
            computed: None,
            integrity: ChecksumIntegrity::Unchecked,
        }
    }

    /// Retains an authoritative absence of checksum capability.
    pub const fn unsupported(connection_generation: ConnectionGeneration) -> Self {
        Self {
            capability: ChecksumCapability::Unsupported,
            rule: None,
            connection_generation,
            scope: None,
            expected: None,
            computed: None,
            integrity: ChecksumIntegrity::NotSupported,
        }
    }

    /// Returns the declared capability embedded in this evidence.
    pub const fn capability(&self) -> ChecksumCapability {
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

    /// Returns the exact provider checksum scope when supported.
    pub const fn scope(&self) -> Option<&ChecksumScope> {
        self.scope.as_ref()
    }

    /// Returns the provider-supplied checksum.
    pub const fn expected(&self) -> Option<ChecksumValue> {
        self.expected
    }

    /// Returns the locally computed checksum.
    pub const fn computed(&self) -> Option<ChecksumValue> {
        self.computed
    }

    /// Returns the derived checksum result.
    pub const fn integrity(&self) -> ChecksumIntegrity {
        self.integrity
    }
}

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
            Self::SequenceOverflow => formatter.write_str("consecutive sequence overflow"),
            Self::GenerationMismatch { expected, found } => write!(
                formatter,
                "integrity generation {found} does not match snapshot generation {expected}"
            ),
        }
    }
}

impl std::error::Error for IntegrityEvidenceError {}
