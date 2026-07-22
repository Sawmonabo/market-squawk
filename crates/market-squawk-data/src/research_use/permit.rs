//! Opaque, process-local, single-use research authority.

use std::fmt;

use market_squawk_domain::Timestamp;
use uuid::Uuid;

use super::{
    ResearchUse, ResearchUseDecisionDigest, ResearchUseDecisionInput, ResearchUseDecisionOutcome,
    ResearchUseError, ResearchUseGraphDigest,
};

/// Opaque single-use authority for one bounded downstream research operation.
///
/// This type intentionally implements neither `Clone` nor serialization. Consumers take it by
/// value, and a process restart changes the bound catalog-session identity so retained bytes can
/// never resume authority.
pub struct ResearchUsePermit {
    session_id: Uuid,
    decision_digest: ResearchUseDecisionDigest,
    graph_digest: ResearchUseGraphDigest,
    research_use: ResearchUse,
    expires_at: Timestamp,
}

impl ResearchUsePermit {
    /// Returns the durable decision this ephemeral capability was issued from.
    pub const fn decision_digest(&self) -> ResearchUseDecisionDigest {
        self.decision_digest
    }

    /// Returns the exact transitive graph authorized by the decision.
    pub const fn graph_digest(&self) -> ResearchUseGraphDigest {
        self.graph_digest
    }

    /// Returns the independently authorized downstream use.
    pub const fn research_use(&self) -> ResearchUse {
        self.research_use
    }

    /// Returns the exclusive capability expiry.
    pub const fn expires_at(&self) -> Timestamp {
        self.expires_at
    }

    pub(crate) const fn session_id(&self) -> Uuid {
        self.session_id
    }
}

impl fmt::Debug for ResearchUsePermit {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ResearchUsePermit([SEALED AUTHORITY])")
    }
}

/// The ResearchUse authorizer's only constructor surface; it is not publicly reachable.
#[allow(
    dead_code,
    reason = "the Wave 2 authorizer is the sole production caller of this sealed constructor"
)]
pub(super) fn issue_permit(
    session_id: Uuid,
    decision: &ResearchUseDecisionInput,
) -> Result<ResearchUsePermit, ResearchUseError> {
    if session_id.is_nil() {
        return Err(ResearchUseError::InvalidPermitSession);
    }
    if decision.outcome() != ResearchUseDecisionOutcome::Allowed {
        return Err(ResearchUseError::InvalidDecision);
    }
    let expires_at = decision
        .expires_at()
        .ok_or(ResearchUseError::InvalidDecision)?;
    Ok(ResearchUsePermit {
        session_id,
        decision_digest: decision.digest(),
        graph_digest: decision.graph_digest(),
        research_use: decision.requested_use(),
        expires_at,
    })
}
