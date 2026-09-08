//! Domain-facing governance preparation, commit ports, and immutable safe receipt DTOs.

use std::fmt;

use async_trait::async_trait;
use market_squawk_domain::Timestamp;
use serde::Serialize;
use thiserror::Error;

use super::{
    GovernanceActionDigest, GovernanceActionKind, GovernanceCommitReceipt, GovernanceTimestamp,
};

const MAXIMUM_IDENTIFIER_BYTES: usize = 256;
const MAXIMUM_NOTE_BYTES: usize = 4_096;

/// Bounded decision-review disposition selected before the service creates a canonical action.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DecisionReviewDisposition {
    /// Activate the reviewed target revision.
    Activate,
    /// Record that the target needs changes before it can be active.
    NeedsChanges,
    /// Reject the reviewed target revision.
    Reject,
}

/// Bounded invalidation evidence family selected before the service creates a canonical action.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DecisionInvalidationKind {
    /// A relevant corporate action invalidated the retained target.
    CorporateAction,
    /// A model or forecast change invalidated the retained target.
    Model,
    /// A point-in-time data correction invalidated the retained target.
    Data,
    /// A reference-mark change invalidated the retained target.
    ReferenceMark,
    /// A stated target assumption no longer holds.
    Assumption,
}

/// Bounded decision-review proposal. It contains no actor, role, action timestamp, or digest.
#[derive(Clone, Eq, PartialEq)]
pub struct DecisionReviewProposal {
    target_id: Box<str>,
    target_revision: u64,
    disposition: DecisionReviewDisposition,
    note: Box<str>,
}

impl DecisionReviewProposal {
    /// Validates one presentation proposal before its domain owns canonicalization.
    pub fn try_new(
        target_id: impl Into<String>,
        target_revision: u64,
        disposition: DecisionReviewDisposition,
        note: impl Into<String>,
    ) -> Result<Self, GovernanceDomainAdapterError> {
        Ok(Self {
            target_id: bounded_identifier(target_id.into())?,
            target_revision: nonzero_revision(target_revision)?,
            disposition,
            note: bounded_note(note.into())?,
        })
    }

    /// Exact target-series locator selected by the caller.
    #[must_use]
    pub fn target_id(&self) -> &str {
        &self.target_id
    }

    /// Exact immutable target revision selected by the caller.
    #[must_use]
    pub const fn target_revision(&self) -> u64 {
        self.target_revision
    }

    /// Requested review disposition, subject to the domain's current validation.
    #[must_use]
    pub const fn disposition(&self) -> DecisionReviewDisposition {
        self.disposition
    }

    /// Bounded reviewer note that remains inside domain canonical action storage.
    #[must_use]
    pub fn note(&self) -> &str {
        &self.note
    }
}

impl fmt::Debug for DecisionReviewProposal {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DecisionReviewProposal")
            .field("target_id", &self.target_id)
            .field("target_revision", &self.target_revision)
            .field("disposition", &self.disposition)
            .field("note", &"[REDACTED GOVERNANCE CONTENT]")
            .finish()
    }
}

/// Bounded decision-invalidation proposal. It contains no actor, action time, or digest.
#[derive(Clone, Eq, PartialEq)]
pub struct DecisionInvalidationProposal {
    target_id: Box<str>,
    target_revision: u64,
    kind: DecisionInvalidationKind,
    note: Box<str>,
}

impl DecisionInvalidationProposal {
    /// Validates one presentation proposal before its domain owns canonicalization.
    pub fn try_new(
        target_id: impl Into<String>,
        target_revision: u64,
        kind: DecisionInvalidationKind,
        note: impl Into<String>,
    ) -> Result<Self, GovernanceDomainAdapterError> {
        Ok(Self {
            target_id: bounded_identifier(target_id.into())?,
            target_revision: nonzero_revision(target_revision)?,
            kind,
            note: bounded_note(note.into())?,
        })
    }

    /// Exact target-series locator selected by the caller.
    #[must_use]
    pub fn target_id(&self) -> &str {
        &self.target_id
    }

    /// Exact immutable target revision selected by the caller.
    #[must_use]
    pub const fn target_revision(&self) -> u64 {
        self.target_revision
    }

    /// Requested bounded invalidation evidence family.
    #[must_use]
    pub const fn kind(&self) -> DecisionInvalidationKind {
        self.kind
    }

    /// Bounded evidence note that remains inside domain canonical action storage.
    #[must_use]
    pub fn note(&self) -> &str {
        &self.note
    }
}

impl fmt::Debug for DecisionInvalidationProposal {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DecisionInvalidationProposal")
            .field("target_id", &self.target_id)
            .field("target_revision", &self.target_revision)
            .field("kind", &self.kind)
            .field("note", &"[REDACTED GOVERNANCE CONTENT]")
            .finish()
    }
}

/// Closed hierarchy override requested before the service creates a canonical fair-value action.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FairValueRequestedHierarchy {
    /// The domain may propose a Level 2 hierarchy override.
    Level2,
    /// The domain may propose a Level 3 hierarchy override.
    Level3,
}

/// Bounded fair-value approval proposal with no actor, approval time, role, or digest.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FairValueApprovalProposal {
    measurement_token: Box<str>,
    classification_token: Box<str>,
    requested_expires_at: Box<str>,
}

impl FairValueApprovalProposal {
    /// Validates presentation identifiers and a domain-validated policy expiry selector.
    pub fn try_new(
        measurement_token: impl Into<String>,
        classification_token: impl Into<String>,
        requested_expires_at: impl Into<String>,
    ) -> Result<Self, GovernanceDomainAdapterError> {
        Ok(Self {
            measurement_token: bounded_identifier(measurement_token.into())?,
            classification_token: bounded_identifier(classification_token.into())?,
            requested_expires_at: bounded_identifier(requested_expires_at.into())?,
        })
    }

    /// Retained measurement selector.
    #[must_use]
    pub fn measurement_token(&self) -> &str {
        &self.measurement_token
    }

    /// Retained classification decision selector.
    #[must_use]
    pub fn classification_token(&self) -> &str {
        &self.classification_token
    }

    /// Requested policy expiry; the fair-value domain parses and admits it before preview.
    #[must_use]
    pub fn requested_expires_at(&self) -> &str {
        &self.requested_expires_at
    }
}

/// Bounded fair-value override proposal with no actor, approval time, role, or digest.
#[derive(Clone, Eq, PartialEq)]
pub struct FairValueOverrideProposal {
    measurement_token: Box<str>,
    classification_token: Box<str>,
    requested_hierarchy: FairValueRequestedHierarchy,
    justification: Box<str>,
    requested_expires_at: Box<str>,
}

impl FairValueOverrideProposal {
    /// Validates one proposal before fair-value canonicalization.
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        measurement_token: impl Into<String>,
        classification_token: impl Into<String>,
        requested_hierarchy: FairValueRequestedHierarchy,
        justification: impl Into<String>,
        requested_expires_at: impl Into<String>,
    ) -> Result<Self, GovernanceDomainAdapterError> {
        Ok(Self {
            measurement_token: bounded_identifier(measurement_token.into())?,
            classification_token: bounded_identifier(classification_token.into())?,
            requested_hierarchy,
            justification: bounded_note(justification.into())?,
            requested_expires_at: bounded_identifier(requested_expires_at.into())?,
        })
    }

    /// Retained measurement selector.
    #[must_use]
    pub fn measurement_token(&self) -> &str {
        &self.measurement_token
    }

    /// Retained classification decision selector.
    #[must_use]
    pub fn classification_token(&self) -> &str {
        &self.classification_token
    }

    /// Requested hierarchy, subject to current fair-value eligibility rules.
    #[must_use]
    pub const fn requested_hierarchy(&self) -> FairValueRequestedHierarchy {
        self.requested_hierarchy
    }

    /// Bounded justification retained only by the canonical domain action.
    #[must_use]
    pub fn justification(&self) -> &str {
        &self.justification
    }

    /// Requested policy expiry; the fair-value domain parses and admits it before preview.
    #[must_use]
    pub fn requested_expires_at(&self) -> &str {
        &self.requested_expires_at
    }
}

impl fmt::Debug for FairValueOverrideProposal {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("FairValueOverrideProposal")
            .field("measurement_token", &self.measurement_token)
            .field("classification_token", &self.classification_token)
            .field("requested_hierarchy", &self.requested_hierarchy)
            .field("justification", &"[REDACTED GOVERNANCE CONTENT]")
            .field("requested_expires_at", &self.requested_expires_at)
            .finish()
    }
}

/// Bounded fair-value approval-revocation proposal with no actor, revocation time, or digest.
#[derive(Clone, Eq, PartialEq)]
pub struct FairValueRevocationProposal {
    approval_token: Box<str>,
    reason: Box<str>,
}

impl FairValueRevocationProposal {
    /// Validates one revocation proposal before fair-value canonicalization.
    pub fn try_new(
        approval_token: impl Into<String>,
        reason: impl Into<String>,
    ) -> Result<Self, GovernanceDomainAdapterError> {
        Ok(Self {
            approval_token: bounded_identifier(approval_token.into())?,
            reason: bounded_note(reason.into())?,
        })
    }

    /// Retained approval selector.
    #[must_use]
    pub fn approval_token(&self) -> &str {
        &self.approval_token
    }

    /// Bounded revocation reason retained only by the canonical domain action.
    #[must_use]
    pub fn reason(&self) -> &str {
        &self.reason
    }
}

impl fmt::Debug for FairValueRevocationProposal {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("FairValueRevocationProposal")
            .field("approval_token", &self.approval_token)
            .field("reason", &"[REDACTED GOVERNANCE CONTENT]")
            .finish()
    }
}

/// Bounded fair-value market-access conclusion selected before canonicalization.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FairValueMarketAccessConclusion {
    /// The retained evidence supports accessible-market treatment.
    Accessible,
    /// The retained evidence does not support accessible-market treatment.
    Inaccessible,
}

/// Bounded market-access proposal with no preparer, approver, approval time, role, or digest.
#[derive(Clone, Eq, PartialEq)]
pub struct FairValueMarketAccessProposal {
    market_input_token: Box<str>,
    conclusion: FairValueMarketAccessConclusion,
    effective_from: Box<str>,
    effective_until: Box<str>,
    rationale: Box<str>,
}

impl FairValueMarketAccessProposal {
    /// Validates one market-access proposal before fair-value canonicalization.
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        market_input_token: impl Into<String>,
        conclusion: FairValueMarketAccessConclusion,
        effective_from: impl Into<String>,
        effective_until: impl Into<String>,
        rationale: impl Into<String>,
    ) -> Result<Self, GovernanceDomainAdapterError> {
        Ok(Self {
            market_input_token: bounded_identifier(market_input_token.into())?,
            conclusion,
            effective_from: bounded_identifier(effective_from.into())?,
            effective_until: bounded_identifier(effective_until.into())?,
            rationale: bounded_note(rationale.into())?,
        })
    }

    /// Retained account selector.
    #[must_use]
    pub fn market_input_token(&self) -> &str {
        &self.market_input_token
    }

    /// Proposed conclusion, subject to retained evidence validation in the domain.
    #[must_use]
    pub const fn conclusion(&self) -> FairValueMarketAccessConclusion {
        self.conclusion
    }

    /// Requested business-effective start, parsed and admitted by the fair-value domain.
    #[must_use]
    pub fn effective_from(&self) -> &str {
        &self.effective_from
    }

    /// Requested business-effective end, parsed and admitted by the fair-value domain.
    #[must_use]
    pub fn effective_until(&self) -> &str {
        &self.effective_until
    }

    /// Bounded rationale retained only by the canonical domain action.
    #[must_use]
    pub fn rationale(&self) -> &str {
        &self.rationale
    }
}

impl fmt::Debug for FairValueMarketAccessProposal {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("FairValueMarketAccessProposal")
            .field("market_input_token", &self.market_input_token)
            .field("conclusion", &self.conclusion)
            .field("effective_from", &self.effective_from)
            .field("effective_until", &self.effective_until)
            .field("rationale", &"[REDACTED GOVERNANCE CONTENT]")
            .finish()
    }
}

/// Immutable durable domain-local commit reference, never a caller-provided receipt or actor.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct GovernanceDomainReceipt {
    action_kind: GovernanceActionKind,
    domain_receipt_id: Box<str>,
    committed_at: GovernanceTimestamp,
}

impl GovernanceDomainReceipt {
    /// Creates a safe reference returned only after the corresponding domain mutation is durable.
    pub fn try_new(
        action_kind: GovernanceActionKind,
        domain_receipt_id: impl Into<String>,
        committed_at: Timestamp,
    ) -> Result<Self, GovernanceDomainAdapterError> {
        Ok(Self {
            action_kind,
            domain_receipt_id: bounded_identifier(domain_receipt_id.into())?,
            committed_at: GovernanceTimestamp::from_timestamp(committed_at),
        })
    }

    /// Closed governed domain action family.
    #[must_use]
    pub const fn action_kind(&self) -> GovernanceActionKind {
        self.action_kind
    }

    /// Opaque durable domain record or receipt identity.
    #[must_use]
    pub fn domain_receipt_id(&self) -> &str {
        &self.domain_receipt_id
    }

    /// Server-derived durable domain-commit time.
    #[must_use]
    pub const fn committed_at(&self) -> GovernanceTimestamp {
        self.committed_at
    }
}

/// Immutable joined generic-authorization and domain-mutation receipt safe for presentation.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct GovernedActionCommitReceipt {
    authorization: GovernanceCommitReceipt,
    domain: GovernanceDomainReceipt,
}

impl GovernedActionCommitReceipt {
    /// Joins only matching immutable receipts after the domain has durably committed.
    pub fn try_new(
        authorization: GovernanceCommitReceipt,
        domain: GovernanceDomainReceipt,
    ) -> Result<Self, GovernanceDomainAdapterError> {
        if authorization.effects().len() != 1
            || authorization.effects()[0].kind() != domain.action_kind()
            || authorization.committed_at() != domain.committed_at()
        {
            return Err(GovernanceDomainAdapterError::ReceiptMismatch);
        }
        Ok(Self {
            authorization,
            domain,
        })
    }

    /// Generic one-use authorization receipt generated before the domain commit.
    #[must_use]
    pub const fn authorization(&self) -> &GovernanceCommitReceipt {
        &self.authorization
    }

    /// Durable domain-local mutation receipt.
    #[must_use]
    pub const fn domain(&self) -> &GovernanceDomainReceipt {
        &self.domain
    }
}

/// A fully server-held canonical action that can commit only with consumed generic authority.
#[async_trait]
pub trait CanonicalGovernanceAction: fmt::Debug + Send + Sync {
    /// Exact closed action kind selected by the trusted preparer.
    fn kind(&self) -> GovernanceActionKind;

    /// SHA-256 of the exact server-canonical action bytes retained by this object.
    fn digest(&self) -> GovernanceActionDigest;

    /// Commits with server-derived principals, roles, time, and digest only after ticket consumption.
    async fn commit(
        &self,
        authorization: &GovernanceCommitReceipt,
    ) -> Result<GovernanceDomainReceipt, GovernanceDomainAdapterError>;
}

/// Trusted decision-domain canonicalizer and committer. It owns target validation and persistence.
#[async_trait]
pub trait DecisionGovernanceActionFactory: fmt::Debug + Send + Sync {
    /// Resolves the exact immutable target and returns its server-canonical review action.
    async fn prepare_review(
        &self,
        proposal: DecisionReviewProposal,
        prepared_at: Timestamp,
    ) -> Result<std::sync::Arc<dyn CanonicalGovernanceAction>, GovernanceDomainAdapterError>;

    /// Resolves the exact immutable target and returns its server-canonical invalidation action.
    async fn prepare_invalidation(
        &self,
        proposal: DecisionInvalidationProposal,
        prepared_at: Timestamp,
    ) -> Result<std::sync::Arc<dyn CanonicalGovernanceAction>, GovernanceDomainAdapterError>;
}

/// Trusted fair-value canonicalizer and committer. It owns evidence, status, and audit validation.
#[async_trait]
pub trait FairValueGovernanceActionFactory: fmt::Debug + Send + Sync {
    /// Returns a server-canonical approval action for one retained measurement and decision.
    async fn prepare_approval(
        &self,
        proposal: FairValueApprovalProposal,
        prepared_at: Timestamp,
    ) -> Result<std::sync::Arc<dyn CanonicalGovernanceAction>, GovernanceDomainAdapterError>;

    /// Returns a server-canonical hierarchy-override action for retained fair-value evidence.
    async fn prepare_override(
        &self,
        proposal: FairValueOverrideProposal,
        prepared_at: Timestamp,
    ) -> Result<std::sync::Arc<dyn CanonicalGovernanceAction>, GovernanceDomainAdapterError>;

    /// Returns a server-canonical approval-revocation action for an active retained approval.
    async fn prepare_revocation(
        &self,
        proposal: FairValueRevocationProposal,
        prepared_at: Timestamp,
    ) -> Result<std::sync::Arc<dyn CanonicalGovernanceAction>, GovernanceDomainAdapterError>;

    /// Returns a server-canonical dual-principal market-access action for retained evidence.
    async fn prepare_market_access(
        &self,
        proposal: FairValueMarketAccessProposal,
        prepared_at: Timestamp,
    ) -> Result<std::sync::Arc<dyn CanonicalGovernanceAction>, GovernanceDomainAdapterError>;
}

/// Closed non-sensitive domain preparation, persistence, or receipt validation failure.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum GovernanceDomainAdapterError {
    /// A bounded proposal selector or content field was invalid.
    #[error("governance proposal is invalid")]
    InvalidProposal,
    /// The selected retained target, measurement, decision, or approval was not found.
    #[error("governance domain record was not found")]
    NotFound,
    /// Current durable domain state does not admit the canonical action.
    #[error("governance domain action conflicts with current state")]
    Conflict,
    /// The domain's bounded action or persistence capacity is exhausted.
    #[error("governance domain capacity is exhausted")]
    CapacityExceeded,
    /// Durable target or fair-value authority is unavailable.
    #[error("governance domain authority is unavailable")]
    Unavailable,
    /// A durable domain commit or receipt could not complete.
    #[error("governance domain persistence is unavailable")]
    PersistenceUnavailable,
    /// A trusted adapter attempted to join mismatched immutable receipts.
    #[error("governance domain receipt does not match the authorization")]
    ReceiptMismatch,
    /// A domain adapter failed without caller-safe implementation detail.
    #[error("governance domain action failed")]
    Internal,
}

fn bounded_identifier(value: String) -> Result<Box<str>, GovernanceDomainAdapterError> {
    if value.is_empty()
        || value.len() > MAXIMUM_IDENTIFIER_BYTES
        || value.trim() != value
        || value.chars().any(char::is_control)
    {
        return Err(GovernanceDomainAdapterError::InvalidProposal);
    }
    Ok(value.into_boxed_str())
}

fn bounded_note(value: String) -> Result<Box<str>, GovernanceDomainAdapterError> {
    if value.is_empty()
        || value.len() > MAXIMUM_NOTE_BYTES
        || value.trim() != value
        || value.chars().any(char::is_control)
    {
        return Err(GovernanceDomainAdapterError::InvalidProposal);
    }
    Ok(value.into_boxed_str())
}

fn nonzero_revision(value: u64) -> Result<u64, GovernanceDomainAdapterError> {
    if value == 0 {
        Err(GovernanceDomainAdapterError::InvalidProposal)
    } else {
        Ok(value)
    }
}
