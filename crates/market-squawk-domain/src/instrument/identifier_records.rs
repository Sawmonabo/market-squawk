//! External identifier evidence attached to stable internal instruments.

use std::fmt;

use serde::{Deserialize, Deserializer, Serialize};

use super::EffectiveInterval;
use crate::{
    ChainAddress, CryptoPair, Cusip, Figi, FuturesContractIdentity, Isin, OccOptionIdentity,
    PayloadReference, Sedol, SourceId, SourceIdentifier, Ticker, Timestamp,
};

/// A syntactically validated external identifier.
///
/// Every variant remains syntax/checksum-only. Assignment, existence, lifecycle, source, and
/// licensed-data rights are retained by [`ExternalIdentifierRecord`].
#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum ExternalIdentifier {
    /// Ticker alias.
    Ticker(Ticker),
    /// CUSIP syntax/checksum value.
    Cusip(Cusip),
    /// ISIN syntax/checksum value.
    Isin(Isin),
    /// SEDOL syntax/checksum value.
    Sedol(Sedol),
    /// FIGI syntax/checksum value.
    Figi(Figi),
    /// OCC fixed-width option identity.
    OccOption(OccOptionIdentity),
    /// Structured venue futures identity.
    Futures(FuturesContractIdentity),
    /// Structured venue crypto pair.
    CryptoPair(CryptoPair),
    /// Chain-qualified protocol-specific address.
    ChainAddress(ChainAddress),
}

impl ExternalIdentifier {
    const fn syntax_verification(&self) -> IdentifierSyntaxVerification {
        match self {
            Self::Cusip(_) | Self::Isin(_) | Self::Sedol(_) | Self::Figi(_) => {
                IdentifierSyntaxVerification::ChecksumValidated
            }
            Self::Ticker(_)
            | Self::OccOption(_)
            | Self::Futures(_)
            | Self::CryptoPair(_)
            | Self::ChainAddress(_) => IdentifierSyntaxVerification::SyntaxValidated,
        }
    }
}

impl fmt::Display for ExternalIdentifier {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Ticker(value) => value.fmt(formatter),
            Self::Cusip(value) => value.fmt(formatter),
            Self::Isin(value) => value.fmt(formatter),
            Self::Sedol(value) => value.fmt(formatter),
            Self::Figi(value) => value.fmt(formatter),
            Self::OccOption(value) => value.fmt(formatter),
            Self::Futures(value) => value.fmt(formatter),
            Self::CryptoPair(value) => value.fmt(formatter),
            Self::ChainAddress(value) => value.fmt(formatter),
        }
    }
}

/// The strongest syntactic property established by the identifier-specific constructor.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum IdentifierSyntaxVerification {
    /// Field grammar and relational syntax were validated.
    SyntaxValidated,
    /// Field grammar and a type-specific check digit were validated.
    ChecksumValidated,
}

/// Assignment verification against a responsible registry, venue, or reference master.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AssignmentVerification {
    /// No authoritative assignment determination has been made.
    Unverified,
    /// The referenced source verified that the identifier was assigned.
    VerifiedAssigned,
    /// The referenced source verified that the identifier was not assigned.
    VerifiedUnassigned,
}

/// Entitlement asserted by a referenced, versioned rights policy.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum IdentifierEntitlement {
    /// Rights are unknown or restricted; safe handling must not assume use or redistribution.
    UnknownOrRestricted,
    /// Identifier data is dedicated to the public domain by the rights holder.
    PublicDomain,
    /// User-owned data is authorized for the user's local use.
    UserOwned,
    /// A license authorizes local/internal use but not redistribution.
    LicensedInternalUse,
    /// A license explicitly authorizes redistribution for the covered fields and scope.
    LicensedRedistribution,
}

/// Typed reference to the policy used for identifier data-rights decisions.
#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub struct IdentifierRightsPolicyReference {
    policy_id: SourceIdentifier,
    entitlement: IdentifierEntitlement,
    terms_reference: SourceIdentifier,
}

impl IdentifierRightsPolicyReference {
    /// Constructs a rights-policy reference from already bounded identities.
    pub const fn new(
        policy_id: SourceIdentifier,
        entitlement: IdentifierEntitlement,
        terms_reference: SourceIdentifier,
    ) -> Self {
        Self {
            policy_id,
            entitlement,
            terms_reference,
        }
    }

    /// Returns the local versioned policy identity.
    pub const fn policy_id(&self) -> &SourceIdentifier {
        &self.policy_id
    }

    /// Returns the policy's explicit entitlement decision.
    pub const fn entitlement(&self) -> IdentifierEntitlement {
        self.entitlement
    }

    /// Returns the retained source terms or license reference.
    pub const fn terms_reference(&self) -> &SourceIdentifier {
        &self.terms_reference
    }
}

/// A validated identifier attachment with verification, provenance, time, and rights evidence.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct ExternalIdentifierRecord {
    identifier: ExternalIdentifier,
    syntax_verification: IdentifierSyntaxVerification,
    assignment_verification: AssignmentVerification,
    source_id: SourceId,
    source_reference: PayloadReference,
    source_timestamp: Option<Timestamp>,
    observed_at: Timestamp,
    validity: EffectiveInterval,
    rights_policy: IdentifierRightsPolicyReference,
}

/// Complete evidence input for constructing [`ExternalIdentifierRecord`].
///
/// Syntax verification is intentionally absent because it is derived from `identifier` and
/// cannot be asserted by callers.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct ExternalIdentifierRecordInput {
    /// Validated typed external identifier.
    pub identifier: ExternalIdentifier,
    /// Authoritative assignment verification result.
    pub assignment_verification: AssignmentVerification,
    /// Evidence source namespace.
    pub source_id: SourceId,
    /// Immutable source evidence.
    pub source_reference: PayloadReference,
    /// Source timestamp when supplied.
    pub source_timestamp: Option<Timestamp>,
    /// Local first-observation time.
    pub observed_at: Timestamp,
    /// Half-open interval during which this attachment applies.
    pub validity: EffectiveInterval,
    /// Versioned data-rights decision.
    pub rights_policy: IdentifierRightsPolicyReference,
}

impl ExternalIdentifierRecord {
    /// Constructs an evidence-bearing attachment from a validated identifier.
    pub fn new(input: ExternalIdentifierRecordInput) -> Self {
        let ExternalIdentifierRecordInput {
            identifier,
            assignment_verification,
            source_id,
            source_reference,
            source_timestamp,
            observed_at,
            validity,
            rights_policy,
        } = input;
        let syntax_verification = identifier.syntax_verification();
        Self {
            identifier,
            syntax_verification,
            assignment_verification,
            source_id,
            source_reference,
            source_timestamp,
            observed_at,
            validity,
            rights_policy,
        }
    }

    /// Returns the typed external identifier.
    pub const fn identifier(&self) -> &ExternalIdentifier {
        &self.identifier
    }

    /// Returns the strongest validation performed by the identifier constructor.
    pub const fn syntax_verification(&self) -> IdentifierSyntaxVerification {
        self.syntax_verification
    }

    /// Returns authoritative assignment verification state.
    pub const fn assignment_verification(&self) -> AssignmentVerification {
        self.assignment_verification
    }

    /// Returns the evidence source namespace.
    pub const fn source_id(&self) -> &SourceId {
        &self.source_id
    }

    /// Returns immutable source evidence.
    pub const fn source_reference(&self) -> &PayloadReference {
        &self.source_reference
    }

    /// Returns the source timestamp when supplied.
    pub const fn source_timestamp(&self) -> Option<Timestamp> {
        self.source_timestamp
    }

    /// Returns local first-observation time.
    pub const fn observed_at(&self) -> Timestamp {
        self.observed_at
    }

    /// Returns the identifier attachment's half-open effective interval.
    pub const fn validity(&self) -> EffectiveInterval {
        self.validity
    }

    /// Returns the typed data-rights policy reference.
    pub const fn rights_policy(&self) -> &IdentifierRightsPolicyReference {
        &self.rights_policy
    }
}

impl fmt::Display for ExternalIdentifierRecord {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.identifier.fmt(formatter)
    }
}

#[derive(Serialize)]
struct ExternalIdentifierRecordWireRef<'a> {
    identifier: &'a ExternalIdentifier,
    assignment_verification: AssignmentVerification,
    source_id: &'a SourceId,
    source_reference: &'a PayloadReference,
    source_timestamp: Option<Timestamp>,
    observed_at: Timestamp,
    validity: EffectiveInterval,
    rights_policy: &'a IdentifierRightsPolicyReference,
}

impl Serialize for ExternalIdentifierRecord {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        ExternalIdentifierRecordWireRef {
            identifier: &self.identifier,
            assignment_verification: self.assignment_verification,
            source_id: &self.source_id,
            source_reference: &self.source_reference,
            source_timestamp: self.source_timestamp,
            observed_at: self.observed_at,
            validity: self.validity,
            rights_policy: &self.rights_policy,
        }
        .serialize(serializer)
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ExternalIdentifierRecordWire {
    identifier: ExternalIdentifier,
    assignment_verification: AssignmentVerification,
    source_id: SourceId,
    source_reference: PayloadReference,
    source_timestamp: Option<Timestamp>,
    observed_at: Timestamp,
    validity: EffectiveInterval,
    rights_policy: IdentifierRightsPolicyReference,
}

impl<'de> Deserialize<'de> for ExternalIdentifierRecord {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = ExternalIdentifierRecordWire::deserialize(deserializer)?;
        Ok(Self::new(ExternalIdentifierRecordInput {
            identifier: wire.identifier,
            assignment_verification: wire.assignment_verification,
            source_id: wire.source_id,
            source_reference: wire.source_reference,
            source_timestamp: wire.source_timestamp,
            observed_at: wire.observed_at,
            validity: wire.validity,
            rights_policy: wire.rights_policy,
        }))
    }
}
