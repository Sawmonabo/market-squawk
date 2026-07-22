//! Immutable override, approval, expiry, and revocation records.

use std::mem::size_of;

use market_squawk_domain::{FairValueHierarchy, Timestamp};

use crate::measurement::{ActorId, MeasurementId};
use crate::rules::{ClassificationDecision, DecisionId};
use crate::{CanonicalHasher, FairValueError, checked_add};

const MAX_EXPLANATION_BYTES: usize = 4_096;

digest_id!(
    /// SHA-256 content identity of an immutable valuation override.
    OverrideId
);
digest_id!(
    /// SHA-256 content identity of an immutable valuation approval.
    ValuationApprovalId
);
digest_id!(
    /// SHA-256 content identity of an immutable approval revocation.
    ApprovalRevocationId
);

#[derive(Clone, Debug, Eq, PartialEq)]
struct Explanation(Box<str>);

impl Explanation {
    fn try_new(value: &str) -> Result<Self, FairValueError> {
        if value.is_empty()
            || value.len() > MAX_EXPLANATION_BYTES
            || value.bytes().any(|byte| byte.is_ascii_control())
        {
            Err(FairValueError::InvalidText)
        } else {
            Ok(Self(value.into()))
        }
    }

    fn as_str(&self) -> &str {
        &self.0
    }

    fn retained_bytes(&self) -> usize {
        self.0.len()
    }
}

/// Immutable explicit judgment that proposes a new hierarchy without rewriting evidence.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ValuationOverride {
    id: OverrideId,
    base_decision_id: DecisionId,
    requested_hierarchy: FairValueHierarchy,
    justification: Explanation,
    prepared_by: ActorId,
    prepared_at: Timestamp,
    expires_at: Timestamp,
    retained_bytes: usize,
}

impl ValuationOverride {
    pub(crate) fn try_new(
        base: &ClassificationDecision,
        requested_hierarchy: FairValueHierarchy,
        justification: &str,
        prepared_by: ActorId,
        prepared_at: Timestamp,
        expires_at: Timestamp,
    ) -> Result<Self, FairValueError> {
        if requested_hierarchy == FairValueHierarchy::Level1
            || requested_hierarchy == FairValueHierarchy::Unclassified
            || base.hierarchy() == FairValueHierarchy::Unclassified
            || requested_hierarchy == base.hierarchy()
            || expires_at <= prepared_at
        {
            return Err(FairValueError::InvalidOverride);
        }
        let justification = Explanation::try_new(justification)?;
        let retained_bytes = checked_add(
            size_of::<Self>(),
            checked_add(justification.retained_bytes(), prepared_by.retained_bytes())?,
        )?;
        let mut hash = CanonicalHasher::new(b"market-squawk/valuation-override/v1");
        hash.fixed(base.id().bytes());
        hash.u8(hierarchy_tag(requested_hierarchy));
        hash.bytes(justification.as_str().as_bytes());
        hash.bytes(prepared_by.as_str().as_bytes());
        hash.i64(prepared_at.unix_nanos());
        hash.i64(expires_at.unix_nanos());
        Ok(Self {
            id: OverrideId(hash.finish()),
            base_decision_id: base.id(),
            requested_hierarchy,
            justification,
            prepared_by,
            prepared_at,
            expires_at,
            retained_bytes,
        })
    }

    /// Returns immutable override identity.
    pub const fn id(&self) -> OverrideId {
        self.id
    }

    /// Returns original rules-decision identity.
    pub const fn base_decision_id(&self) -> DecisionId {
        self.base_decision_id
    }

    /// Returns explicitly requested hierarchy.
    pub const fn requested_hierarchy(&self) -> FairValueHierarchy {
        self.requested_hierarchy
    }

    /// Returns bounded justification.
    pub fn justification(&self) -> &str {
        self.justification.as_str()
    }

    /// Returns override preparer.
    pub const fn prepared_by(&self) -> &ActorId {
        &self.prepared_by
    }

    /// Returns preparation time.
    pub const fn prepared_at(&self) -> Timestamp {
        self.prepared_at
    }

    /// Returns immutable expiry.
    pub const fn expires_at(&self) -> Timestamp {
        self.expires_at
    }

    pub(crate) const fn retained_bytes(&self) -> usize {
        self.retained_bytes
    }
}

/// Atomic result of creating an override record and its new immutable decision.
#[derive(Clone, Debug)]
pub struct OverrideProposal {
    valuation_override: std::sync::Arc<ValuationOverride>,
    decision: std::sync::Arc<ClassificationDecision>,
}

impl OverrideProposal {
    pub(crate) const fn new(
        valuation_override: std::sync::Arc<ValuationOverride>,
        decision: std::sync::Arc<ClassificationDecision>,
    ) -> Self {
        Self {
            valuation_override,
            decision,
        }
    }

    /// Returns immutable override record.
    pub fn valuation_override(&self) -> &ValuationOverride {
        &self.valuation_override
    }

    /// Returns the new content-bound decision.
    pub fn decision(&self) -> &ClassificationDecision {
        &self.decision
    }
}

/// Immutable independent approval of one exact decision.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ValuationApproval {
    id: ValuationApprovalId,
    decision_id: DecisionId,
    measurement_id: MeasurementId,
    override_id: Option<OverrideId>,
    approved_by: ActorId,
    approved_at: Timestamp,
    expires_at: Timestamp,
    retained_bytes: usize,
}

impl ValuationApproval {
    pub(crate) fn try_new(
        decision: &ClassificationDecision,
        override_id: Option<OverrideId>,
        approved_by: ActorId,
        approved_at: Timestamp,
        expires_at: Timestamp,
    ) -> Result<Self, FairValueError> {
        if expires_at <= approved_at {
            return Err(FairValueError::InvalidApprovalWindow);
        }
        let mut hash = CanonicalHasher::new(b"market-squawk/valuation-approval/v1");
        hash.fixed(decision.id().bytes());
        hash.fixed(decision.measurement_id().bytes());
        match override_id {
            Some(value) => {
                hash.u8(1);
                hash.fixed(value.bytes());
            }
            None => hash.u8(0),
        }
        hash.bytes(approved_by.as_str().as_bytes());
        hash.i64(approved_at.unix_nanos());
        hash.i64(expires_at.unix_nanos());
        let retained_bytes = checked_add(size_of::<Self>(), approved_by.retained_bytes())?;
        Ok(Self {
            id: ValuationApprovalId(hash.finish()),
            decision_id: decision.id(),
            measurement_id: decision.measurement_id(),
            override_id,
            approved_by,
            approved_at,
            expires_at,
            retained_bytes,
        })
    }

    /// Returns immutable approval identity.
    pub const fn id(&self) -> ValuationApprovalId {
        self.id
    }

    /// Returns exact approved decision.
    pub const fn decision_id(&self) -> DecisionId {
        self.decision_id
    }

    /// Returns exact measurement.
    pub const fn measurement_id(&self) -> MeasurementId {
        self.measurement_id
    }

    /// Returns optional override identity.
    pub const fn override_id(&self) -> Option<OverrideId> {
        self.override_id
    }

    /// Returns independent approver.
    pub const fn approved_by(&self) -> &ActorId {
        &self.approved_by
    }

    /// Returns approval instant.
    pub const fn approved_at(&self) -> Timestamp {
        self.approved_at
    }

    /// Returns expiry instant.
    pub const fn expires_at(&self) -> Timestamp {
        self.expires_at
    }

    pub(crate) const fn retained_bytes(&self) -> usize {
        self.retained_bytes
    }
}

/// Immutable revocation of one exact approval.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ApprovalRevocation {
    id: ApprovalRevocationId,
    approval_id: ValuationApprovalId,
    revoked_by: ActorId,
    revoked_at: Timestamp,
    reason: Explanation,
    retained_bytes: usize,
}

impl ApprovalRevocation {
    pub(crate) fn try_new(
        approval: &ValuationApproval,
        revoked_by: ActorId,
        revoked_at: Timestamp,
        reason: &str,
    ) -> Result<Self, FairValueError> {
        if revoked_at < approval.approved_at() {
            return Err(FairValueError::InvalidRevocationTime);
        }
        let reason = Explanation::try_new(reason)?;
        let retained_bytes = checked_add(
            size_of::<Self>(),
            checked_add(revoked_by.retained_bytes(), reason.retained_bytes())?,
        )?;
        let mut hash = CanonicalHasher::new(b"market-squawk/approval-revocation/v1");
        hash.fixed(approval.id().bytes());
        hash.bytes(revoked_by.as_str().as_bytes());
        hash.i64(revoked_at.unix_nanos());
        hash.bytes(reason.as_str().as_bytes());
        Ok(Self {
            id: ApprovalRevocationId(hash.finish()),
            approval_id: approval.id(),
            revoked_by,
            revoked_at,
            reason,
            retained_bytes,
        })
    }

    /// Returns immutable revocation identity.
    pub const fn id(&self) -> ApprovalRevocationId {
        self.id
    }

    /// Returns exact revoked approval.
    pub const fn approval_id(&self) -> ValuationApprovalId {
        self.approval_id
    }

    /// Returns revoking actor.
    pub const fn revoked_by(&self) -> &ActorId {
        &self.revoked_by
    }

    /// Returns revocation instant.
    pub const fn revoked_at(&self) -> Timestamp {
        self.revoked_at
    }

    /// Returns bounded revocation reason.
    pub fn reason(&self) -> &str {
        self.reason.as_str()
    }

    pub(crate) const fn retained_bytes(&self) -> usize {
        self.retained_bytes
    }
}

/// Time-relative approval state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ApprovalStatus {
    /// Query time precedes the approval instant.
    NotYetEffective,
    /// Approval is within its immutable lifetime and has not been revoked.
    Active,
    /// Approval lifetime ended before the query time.
    Expired,
    /// An immutable revocation applies at the query time.
    Revoked,
}

const fn hierarchy_tag(value: FairValueHierarchy) -> u8 {
    match value {
        FairValueHierarchy::Level1 => 1,
        FairValueHierarchy::Level2 => 2,
        FairValueHierarchy::Level3 => 3,
        FairValueHierarchy::Unclassified => 4,
    }
}
