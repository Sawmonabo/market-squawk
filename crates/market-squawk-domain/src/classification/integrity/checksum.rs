//! Event-shaped checksum validation evidence.

use std::num::NonZeroU32;

use serde::Serialize;

use super::super::{AssessmentValidity, ChecksumIntegrity, MarketDepth};
use super::{ChecksumCapability, IntegrityEvidenceError, IntegrityEvidenceKind, IntegrityRule};
use crate::{ConnectionGeneration, SourceIdentifier};

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

/// Provider-defined checksum scope for a non-book canonical event payload.
///
/// This deliberately carries no market depth or level count. A payload checksum cannot be
/// confused with evidence over an order-book image.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct PayloadChecksumScope {
    provider_scope: SourceIdentifier,
}

impl PayloadChecksumScope {
    /// Constructs an explicit provider payload-checksum scope.
    pub const fn new(provider_scope: SourceIdentifier) -> Self {
        Self { provider_scope }
    }

    /// Returns the provider's canonical payload-scope identity.
    pub const fn provider_scope(&self) -> &SourceIdentifier {
        &self.provider_scope
    }
}

/// Event-shaped target covered by one supported checksum result.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case", tag = "kind", content = "scope")]
pub enum ChecksumTarget {
    /// Checksum covers an exact order-book depth and level count.
    Book(ChecksumScope),
    /// Checksum covers a provider-defined non-book event payload.
    Payload(PayloadChecksumScope),
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
    target: Option<ChecksumTarget>,
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
    pub fn validate_book(
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
        Self::validate_target(
            capability,
            rule,
            connection_generation,
            ChecksumTarget::Book(scope),
            expected,
            computed,
        )
    }

    /// Compares checksums over a provider-defined non-book canonical payload.
    ///
    /// # Errors
    ///
    /// Rejects capability contradictions and incomplete supported-checksum evidence.
    pub fn validate_payload(
        capability: ChecksumCapability,
        rule: Option<IntegrityRule>,
        connection_generation: ConnectionGeneration,
        scope: Option<PayloadChecksumScope>,
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
        let scope = scope.ok_or(IntegrityEvidenceError::MissingPayloadChecksumScope)?;
        Self::validate_target(
            capability,
            rule,
            connection_generation,
            ChecksumTarget::Payload(scope),
            expected,
            computed,
        )
    }

    fn validate_target(
        capability: ChecksumCapability,
        rule: IntegrityRule,
        connection_generation: ConnectionGeneration,
        target: ChecksumTarget,
        expected: Option<ChecksumValue>,
        computed: Option<ChecksumValue>,
    ) -> Result<Self, IntegrityEvidenceError> {
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
            target: Some(target),
            expected: Some(expected),
            computed: Some(computed),
            integrity,
        })
    }

    /// Retains a supported checksum capability before comparison completes.
    pub fn unchecked_book(
        rule: IntegrityRule,
        connection_generation: ConnectionGeneration,
        scope: ChecksumScope,
    ) -> Self {
        Self {
            capability: ChecksumCapability::Provided,
            rule: Some(rule),
            connection_generation,
            target: Some(ChecksumTarget::Book(scope)),
            expected: None,
            computed: None,
            integrity: ChecksumIntegrity::Unchecked,
        }
    }

    /// Retains an incomplete payload checksum comparison under an explicit provider rule.
    pub fn unchecked_payload(
        rule: IntegrityRule,
        connection_generation: ConnectionGeneration,
        scope: PayloadChecksumScope,
    ) -> Self {
        Self {
            capability: ChecksumCapability::Provided,
            rule: Some(rule),
            connection_generation,
            target: Some(ChecksumTarget::Payload(scope)),
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
            target: None,
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

    /// Returns the event-shaped checksum target when supported.
    pub const fn target(&self) -> Option<&ChecksumTarget> {
        self.target.as_ref()
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

impl AssessmentValidity for ChecksumEvidence {}
