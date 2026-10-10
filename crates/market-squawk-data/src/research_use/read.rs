//! Snapshot-only display and local-analysis authority over the immutable manifest graph.

use std::time::{Duration, Instant};

use market_squawk_domain::Timestamp;
use rusqlite::params;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::market_event::checked_read_clock;
use super::persistence::{self, SourceAuthority};
use super::traversal::{check_control, load_graph};
use super::{
    ResearchUse, ResearchUseCatalogError, ResearchUseDecisionInput, ResearchUseDecisionOutcome,
    ResearchUseDenialReason, ResearchUseGraph, ResearchUseRequest, ResearchUseSet,
};
use crate::catalog::CatalogReadSnapshot;

/// Process-local read authority for an exact transitive manifest graph.
///
/// Display and transient local calculations use independently scoped grants. This receipt
/// grants no durable publication or training permit and records no durable decision.
/// Recheck through the owning analytical service before returning data or calculated values.
/// It cannot be cloned, serialized, or reused after the catalog session changes.
#[derive(Debug)]
pub struct AuthorizedResearchRead {
    session_id: Uuid,
    graph: ResearchUseGraph,
    decision: ResearchUseDecisionInput,
    expires_at: Timestamp,
    monotonic_expiry: Instant,
}

impl AuthorizedResearchRead {
    /// Returns the exact transitive graph admitted for this read.
    pub const fn graph(&self) -> &ResearchUseGraph {
        &self.graph
    }

    /// Returns the independently authorized display or local-analysis use.
    pub const fn research_use(&self) -> ResearchUse {
        self.decision.requested_use()
    }

    /// Returns the earliest source, grant, or local receipt expiry.
    pub const fn expires_at(&self) -> Timestamp {
        self.expires_at
    }
}

/// Returns no receipt only when valid exact lineage needs grant admission or renewal.
/// The complete graph and every source are evaluated before deferring a renewable denial.
pub(crate) fn authorize_current_research_use_in_snapshot(
    snapshot: &CatalogReadSnapshot,
    session_id: Uuid,
    request: ResearchUseRequest,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<Option<AuthorizedResearchRead>, ResearchUseCatalogError> {
    check_control(cancellation, deadline)?;
    if session_id.is_nil() {
        return Err(ResearchUseCatalogError::InvalidPermitSession);
    }
    if !matches!(
        request.requested_use,
        ResearchUse::Display | ResearchUse::LocalAnalysis
    ) {
        return Err(ResearchUseCatalogError::InvalidGrant);
    }
    let started = Instant::now();
    let deadline = deadline.min(
        started
            .checked_add(request.limits.traversal_deadline())
            .ok_or(ResearchUseCatalogError::DeadlineExceeded)?,
    );
    let connection = snapshot.connection();
    let now = checked_read_clock(connection, None)?;
    let graph = load_graph(connection, &request, cancellation, deadline)?;
    let (authorities, _, _, denial) = persistence::select_authorities(
        connection,
        &graph,
        request.requested_use,
        now,
        cancellation,
        deadline,
    )?;
    if let Some(reason) = denial {
        if !matches!(
            reason,
            ResearchUseDenialReason::MissingGrant | ResearchUseDenialReason::Expired
        ) {
            return Err(denial_error(reason));
        }
        // Renewal cannot extend the original source operation scope. Check every source,
        // including one whose missing scope was masked by another source's expired grant.
        let required = i64::from(
            ResearchUseSet::try_new(vec![request.requested_use])?.required_source_operation_mask(),
        );
        for source in graph.sources() {
            check_control(cancellation, deadline)?;
            let permitted: bool = connection.query_row(
                "SELECT (operation_mask & ?2)=?2 FROM source_rights WHERE rights_id=?1",
                params![source.rights_id(), required],
                |row| row.get(0),
            )?;
            if !permitted {
                return Err(ResearchUseCatalogError::InvalidGrant);
            }
        }
        check_control(cancellation, deadline)?;
        checked_read_clock(connection, Some(now))?;
        return Ok(None);
    }
    let expires_at = persistence::decision_expiry(now, request.limits, &authorities)?;
    let decision = ResearchUseDecisionInput::try_new(
        &graph,
        request.requested_use,
        1,
        now,
        Some(expires_at),
        ResearchUseDecisionOutcome::Allowed,
        authorities,
    )?;
    let remaining = u64::try_from(
        expires_at
            .unix_nanos()
            .checked_sub(now.unix_nanos())
            .ok_or(ResearchUseCatalogError::LimitExceeded)?,
    )
    .map_err(|_| ResearchUseCatalogError::LimitExceeded)?;
    let monotonic_expiry = started
        .checked_add(Duration::from_nanos(remaining))
        .ok_or(ResearchUseCatalogError::LimitExceeded)?;
    check_control(cancellation, deadline)?;
    if checked_read_clock(connection, Some(now))? >= expires_at
        || Instant::now() >= monotonic_expiry
    {
        return Err(ResearchUseCatalogError::Expired);
    }
    Ok(Some(AuthorizedResearchRead {
        session_id,
        graph,
        decision,
        expires_at,
        monotonic_expiry,
    }))
}

/// Rechecks exact lineage and the originally selected grants in a fresh catalog snapshot.
pub(crate) fn recheck_research_use_in_snapshot(
    snapshot: &CatalogReadSnapshot,
    session_id: Uuid,
    authorization: &AuthorizedResearchRead,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<(), ResearchUseCatalogError> {
    check_control(cancellation, deadline)?;
    if session_id != authorization.session_id {
        return Err(ResearchUseCatalogError::InvalidPermitSession);
    }
    let deadline = deadline.min(
        Instant::now()
            .checked_add(authorization.graph.limits().traversal_deadline())
            .ok_or(ResearchUseCatalogError::DeadlineExceeded)?,
    );
    let connection = snapshot.connection();
    let now = checked_read_clock(connection, Some(authorization.decision.evaluated_at()))?;
    if now >= authorization.expires_at || Instant::now() >= authorization.monotonic_expiry {
        return Err(ResearchUseCatalogError::Expired);
    }
    let request = ResearchUseRequest::try_new(
        authorization.graph.roots().to_vec(),
        authorization.research_use(),
        authorization.graph.limits(),
    )?;
    let graph = load_graph(connection, &request, cancellation, deadline)?;
    if graph != authorization.graph {
        return Err(ResearchUseCatalogError::CorruptCatalog);
    }
    let frontier = persistence::source_use_frontier(connection, now)?;
    for authority in &authorization.decision.authorities {
        check_control(cancellation, deadline)?;
        match persistence::select_source_authority(
            connection,
            &authority.source,
            authorization.research_use(),
            now,
            frontier,
            Some(authority.research_grant_id()),
            cancellation,
            deadline,
        )? {
            SourceAuthority::Selected(selected) => {
                let mut expected = authority.clone();
                expected.revocation_frontier = frontier;
                if selected.as_ref() != &expected {
                    return Err(ResearchUseCatalogError::InvalidGrant);
                }
            }
            SourceAuthority::Denied(reason) => return Err(denial_error(reason)),
        }
    }
    check_control(cancellation, deadline)?;
    if checked_read_clock(connection, Some(authorization.decision.evaluated_at()))?
        >= authorization.expires_at
        || Instant::now() >= authorization.monotonic_expiry
    {
        return Err(ResearchUseCatalogError::Expired);
    }
    Ok(())
}

fn denial_error(reason: ResearchUseDenialReason) -> ResearchUseCatalogError {
    match reason {
        ResearchUseDenialReason::Revoked => ResearchUseCatalogError::Revoked,
        ResearchUseDenialReason::Expired => ResearchUseCatalogError::Expired,
        ResearchUseDenialReason::MissingGrant => ResearchUseCatalogError::InvalidGrant,
        ResearchUseDenialReason::CorruptAuthority => ResearchUseCatalogError::CorruptCatalog,
        ResearchUseDenialReason::LimitExceeded => ResearchUseCatalogError::LimitExceeded,
        ResearchUseDenialReason::Cancelled => ResearchUseCatalogError::Cancelled,
        ResearchUseDenialReason::DeadlineExceeded => ResearchUseCatalogError::DeadlineExceeded,
    }
}
