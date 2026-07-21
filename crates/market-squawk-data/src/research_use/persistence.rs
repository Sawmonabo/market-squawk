//! Immutable grants, revocations, decisions, and output-reservation bindings.

use std::collections::BTreeMap;
use std::time::Instant;

use market_squawk_domain::Timestamp;
use rusqlite::{OptionalExtension as _, Transaction, params};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::catalog::{
    AuthorizationTransactionOutcome, AuthorizedResearchUse, DerivedOutputObjectInput,
    RegisteredResearchUseGrant, ResearchUseCatalogError, ResearchUseGrantInput, ResearchUseRequest,
    ResearchUseRevocationInput, ResearchUseRevocationReceipt,
};
use super::decision::{
    ResearchUseDecisionInput, ResearchUseDecisionOutcome, ResearchUseDenialReason,
};
use super::identity::{
    append_audit, digest_algorithm, duration_nanos, encode_hex, grant_digest,
    output_reservation_digest, parse_digest, parse_evidence, positive_u64, research_use_mask,
    revocation_digest, to_i64, to_i64_usize,
};
use super::permit::issue_permit;
use super::{
    DerivedPublicationInput, DerivedPublicationObject, DerivedRetentionOperation,
    ResearchUseAuthorityEvidence, ResearchUseGraph, ResearchUseSourceInput,
};
use crate::IngestReservation;

pub(super) fn admit_grant(
    transaction: &Transaction<'_>,
    session_id: Uuid,
    now: Timestamp,
    input: ResearchUseGrantInput,
) -> Result<RegisteredResearchUseGrant, ResearchUseCatalogError> {
    let rights = transaction
        .query_row(
            "SELECT source_id, authorization_expires_at_ns, operation_mask
             FROM source_rights WHERE rights_id=?1",
            [input.rights_id],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, Option<i64>>(1)?,
                    row.get::<_, i64>(2)?,
                ))
            },
        )
        .optional()?
        .ok_or(ResearchUseCatalogError::InvalidGrant)?;
    let required_operations = i64::from(input.permitted_uses.required_source_operation_mask());
    if rights.2 & required_operations != required_operations
        || rights.1.is_some_and(|expiry| now.unix_nanos() >= expiry)
        || input
            .authorization_expires_at
            .is_some_and(|expiry| expiry <= now)
        || rights.1.is_some_and(|rights_expiry| {
            input
                .authorization_expires_at
                .is_none_or(|grant_expiry| grant_expiry.unix_nanos() > rights_expiry)
        })
    {
        return Err(ResearchUseCatalogError::InvalidGrant);
    }
    let grant_id = grant_digest(&input);
    let evidence_algorithm = digest_algorithm(input.evidence.algorithm());
    let use_mask = i64::from(input.permitted_uses.mask());
    let expiry = input.authorization_expires_at.map(Timestamp::unix_nanos);
    let existing = transaction
        .query_row(
            "SELECT rights_id, source_id, use_mask, evidence_algorithm, evidence_digest,
                    authorization_expires_at_ns
             FROM source_research_use_grants WHERE research_grant_id=?1",
            [grant_id],
            |row| {
                Ok((
                    row.get::<_, Vec<u8>>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, i64>(3)?,
                    row.get::<_, Vec<u8>>(4)?,
                    row.get::<_, Option<i64>>(5)?,
                ))
            },
        )
        .optional()?;
    if let Some(existing) = existing {
        if existing.0.as_slice() != input.rights_id
            || existing.1 != rights.0
            || existing.2 != use_mask
            || existing.3 != evidence_algorithm
            || existing.4.as_slice() != input.evidence.bytes()
            || existing.5 != expiry
        {
            return Err(ResearchUseCatalogError::CorruptCatalog);
        }
    } else {
        transaction.execute(
            "INSERT INTO source_research_use_grants
             (research_grant_id, rights_id, source_id, use_mask, evidence_algorithm,
              evidence_digest, authorization_expires_at_ns, admitted_at_ns)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            params![
                grant_id,
                input.rights_id,
                rights.0,
                use_mask,
                evidence_algorithm,
                input.evidence.bytes(),
                expiry,
                now.unix_nanos(),
            ],
        )?;
        append_audit(
            transaction,
            "research-use.grant-admitted",
            &encode_hex(grant_id),
            grant_id,
            now,
        )?;
    }
    Ok(RegisteredResearchUseGrant {
        catalog_id: session_id,
        grant_id,
    })
}

pub(super) fn revoke_grant(
    transaction: &Transaction<'_>,
    session_id: Uuid,
    now: Timestamp,
    input: ResearchUseRevocationInput,
) -> Result<ResearchUseRevocationReceipt, ResearchUseCatalogError> {
    if input.catalog_id != session_id || input.effective_at > now {
        return Err(ResearchUseCatalogError::InvalidRevocation);
    }
    let grant_mask = transaction
        .query_row(
            "SELECT use_mask FROM source_research_use_grants WHERE research_grant_id=?1",
            [input.grant_id],
            |row| row.get::<_, i64>(0),
        )
        .optional()?
        .ok_or(ResearchUseCatalogError::InvalidRevocation)?;
    let revoked_mask = i64::from(input.revoked_uses.mask());
    if grant_mask & revoked_mask != revoked_mask {
        return Err(ResearchUseCatalogError::InvalidRevocation);
    }
    let revocation_id = revocation_digest(&input);
    let existing = transaction
        .query_row(
            "SELECT revocation_sequence, research_grant_id, use_mask, reason_code,
                    evidence_algorithm, evidence_digest, effective_at_ns, recorded_at_ns
             FROM source_research_use_revocations WHERE revocation_id=?1",
            [revocation_id],
            |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, Vec<u8>>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, i64>(4)?,
                    row.get::<_, Vec<u8>>(5)?,
                    row.get::<_, i64>(6)?,
                    row.get::<_, i64>(7)?,
                ))
            },
        )
        .optional()?;
    if let Some(existing) = existing {
        if existing.1.as_slice() != input.grant_id
            || existing.2 != revoked_mask
            || existing.3 != input.reason.database_name()
            || existing.4 != digest_algorithm(input.evidence.algorithm())
            || existing.5.as_slice() != input.evidence.bytes()
            || existing.6 != input.effective_at.unix_nanos()
        {
            return Err(ResearchUseCatalogError::CorruptCatalog);
        }
        return Ok(ResearchUseRevocationReceipt::new(
            revocation_id,
            positive_u64(existing.0)?,
            Timestamp::from_unix_nanos(existing.7),
        ));
    }
    let previous: i64 = transaction.query_row(
        "SELECT COALESCE(MAX(revocation_sequence), 0)
         FROM source_research_use_revocations",
        [],
        |row| row.get(0),
    )?;
    let sequence = previous
        .checked_add(1)
        .ok_or(ResearchUseCatalogError::LimitExceeded)?;
    transaction.execute(
        "INSERT INTO source_research_use_revocations
         (revocation_sequence, revocation_id, research_grant_id, use_mask, reason_code,
          evidence_algorithm, evidence_digest, effective_at_ns, recorded_at_ns)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
        params![
            sequence,
            revocation_id,
            input.grant_id,
            revoked_mask,
            input.reason.database_name(),
            digest_algorithm(input.evidence.algorithm()),
            input.evidence.bytes(),
            input.effective_at.unix_nanos(),
            now.unix_nanos(),
        ],
    )?;
    append_audit(
        transaction,
        "research-use.grant-revoked",
        &encode_hex(revocation_id),
        revocation_id,
        now,
    )?;
    Ok(ResearchUseRevocationReceipt::new(
        revocation_id,
        positive_u64(sequence)?,
        now,
    ))
}

pub(super) fn authorize(
    transaction: &Transaction<'_>,
    session_id: Uuid,
    now: Timestamp,
    request: ResearchUseRequest,
    cancellation: &CancellationToken,
) -> Result<AuthorizationTransactionOutcome, ResearchUseCatalogError> {
    let deadline = Instant::now()
        .checked_add(request.limits.traversal_deadline())
        .ok_or(ResearchUseCatalogError::DeadlineExceeded)?;
    let graph = super::traversal::load_graph(transaction, &request, cancellation, deadline)?;
    super::traversal::check_control(cancellation, deadline)?;
    let (authorities, selections, frontier, denial) = select_authorities(
        transaction,
        &graph,
        request.requested_use,
        now,
        cancellation,
        deadline,
    )?;
    let outcome = denial.map_or(
        ResearchUseDecisionOutcome::Allowed,
        ResearchUseDecisionOutcome::Denied,
    );
    let expires_at = if denial.is_none() {
        Some(decision_expiry(now, request.limits, &authorities)?)
    } else {
        None
    };
    let decision = ResearchUseDecisionInput::try_new(
        &graph,
        request.requested_use,
        1,
        now,
        expires_at,
        outcome,
        authorities,
    )?;
    persist_decision(
        transaction,
        &graph,
        &decision,
        now,
        &selections,
        frontier,
        cancellation,
        deadline,
    )?;
    append_audit(
        transaction,
        "research-use.decision-recorded",
        &encode_hex(decision.digest().bytes()),
        decision.digest().bytes(),
        now,
    )?;
    super::traversal::check_control(cancellation, deadline)?;
    if let Some(reason) = denial {
        return Ok(AuthorizationTransactionOutcome::Denied {
            decision_digest: decision.digest(),
            reason,
        });
    }
    let permit = issue_permit(session_id, &decision)?;
    Ok(AuthorizationTransactionOutcome::Allowed(
        AuthorizedResearchUse::new(graph, permit),
    ))
}

pub(super) fn bind_derived_output(
    transaction: &Transaction<'_>,
    session_id: Uuid,
    reservation: &IngestReservation,
    input: DerivedOutputObjectInput,
) -> Result<DerivedPublicationObject, ResearchUseCatalogError> {
    let stored = transaction
        .query_row(
            "SELECT run.operation, run.rights_id, run.requested_at_ns, run.state,
                    run.payload_algorithm, run.payload_digest, artifact.content_algorithm,
                    artifact.content_digest, artifact.size_bytes
             FROM ingest_runs AS run
             JOIN artifacts AS artifact ON artifact.run_id=run.run_id
             WHERE run.run_id=?1 AND artifact.artifact_id=?2",
            params![
                reservation.run_id().to_string(),
                input.artifact_id.to_string()
            ],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, Vec<u8>>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, i64>(4)?,
                    row.get::<_, Vec<u8>>(5)?,
                    row.get::<_, i64>(6)?,
                    row.get::<_, Vec<u8>>(7)?,
                    row.get::<_, i64>(8)?,
                ))
            },
        )
        .optional()?
        .ok_or(ResearchUseCatalogError::InvalidPublication)?;
    let operation = match stored.0.as_str() {
        "persist" => DerivedRetentionOperation::Persist,
        "cache" => DerivedRetentionOperation::Cache,
        _ => return Err(ResearchUseCatalogError::InvalidPublication),
    };
    let rights_id = parse_digest(stored.1)?;
    if stored.2 != reservation.requested_at().unix_nanos()
        || !matches!(stored.3.as_str(), "reserved" | "succeeded")
        || stored.4 != 1
        || stored.5.as_slice() != input.content_hash.bytes()
        || stored.6 != 1
        || stored.7.as_slice() != input.content_hash.bytes()
        || u64::try_from(stored.8).ok() != Some(input.size_bytes)
    {
        return Err(ResearchUseCatalogError::InvalidPublication);
    }
    let reservation_digest =
        output_reservation_digest(session_id, reservation, operation, rights_id, &input);
    DerivedPublicationObject::try_new(
        reservation.run_id(),
        reservation_digest,
        operation,
        rights_id,
        input.artifact_id,
        input.content_hash,
        input.row_count,
        input.size_bytes,
        input.lineage_digest,
    )
    .map_err(Into::into)
}

pub(super) fn publish_derived(
    transaction: &Transaction<'_>,
    session_id: Uuid,
    now: Timestamp,
    input: DerivedPublicationInput,
) -> Result<super::catalog::PublishedDerivedGeneration, ResearchUseCatalogError> {
    super::derived::publish(transaction, session_id, now, input)
}

#[derive(Clone)]
enum SourceSelection {
    Selected(ResearchUseAuthorityEvidence),
    Missing,
}

fn select_authorities(
    transaction: &Transaction<'_>,
    graph: &ResearchUseGraph,
    requested_use: super::ResearchUse,
    now: Timestamp,
    cancellation: &CancellationToken,
    deadline: Instant,
) -> Result<
    (
        Vec<ResearchUseAuthorityEvidence>,
        BTreeMap<u64, SourceSelection>,
        u64,
        Option<ResearchUseDenialReason>,
    ),
    ResearchUseCatalogError,
> {
    let frontier: i64 = transaction.query_row(
        "SELECT COALESCE(MAX(revocation_sequence), 0)
         FROM source_research_use_revocations WHERE recorded_at_ns<=?1",
        [now.unix_nanos()],
        |row| row.get(0),
    )?;
    let frontier = u64::try_from(frontier).map_err(|_| ResearchUseCatalogError::CorruptCatalog)?;
    let mut authorities = Vec::new();
    authorities
        .try_reserve_exact(graph.sources().len())
        .map_err(|_| ResearchUseCatalogError::LimitExceeded)?;
    let mut selections = BTreeMap::new();
    let mut denial = None;
    for source in graph.sources() {
        super::traversal::check_control(cancellation, deadline)?;
        match select_source_authority(transaction, source, requested_use, now, frontier)? {
            SourceAuthority::Selected(authority) => {
                authorities.push(authority.clone());
                selections.insert(
                    source.generation_sequence(),
                    SourceSelection::Selected(authority),
                );
            }
            SourceAuthority::Denied(reason) => {
                denial = Some(preferred_denial(denial, reason));
                selections.insert(source.generation_sequence(), SourceSelection::Missing);
            }
        }
    }
    Ok((authorities, selections, frontier, denial))
}

enum SourceAuthority {
    Selected(ResearchUseAuthorityEvidence),
    Denied(ResearchUseDenialReason),
}

fn select_source_authority(
    transaction: &Transaction<'_>,
    source: &ResearchUseSourceInput,
    requested_use: super::ResearchUse,
    now: Timestamp,
    frontier: u64,
) -> Result<SourceAuthority, ResearchUseCatalogError> {
    let rights = transaction
        .query_row(
            "SELECT basis_digest, authorization_algorithm, authorization_digest,
                    authorization_expires_at_ns
             FROM source_rights WHERE rights_id=?1 AND source_id=?2",
            params![source.rights_id(), source.source_id().as_str()],
            |row| {
                Ok((
                    row.get::<_, Vec<u8>>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, Vec<u8>>(2)?,
                    row.get::<_, Option<i64>>(3)?,
                ))
            },
        )
        .optional()?
        .ok_or(ResearchUseCatalogError::CorruptCatalog)?;
    if rights.3.is_some_and(|expiry| now.unix_nanos() >= expiry) {
        return Ok(SourceAuthority::Denied(ResearchUseDenialReason::Expired));
    }
    let mut statement = transaction.prepare(
        "SELECT research_grant_id, evidence_algorithm, evidence_digest,
                authorization_expires_at_ns
         FROM source_research_use_grants
         WHERE rights_id=?1 AND source_id=?2 AND admitted_at_ns<=?3
           AND (use_mask & ?4)<>0
         ORDER BY authorization_expires_at_ns IS NOT NULL,
                  authorization_expires_at_ns DESC, research_grant_id",
    )?;
    let rows = statement.query_map(
        params![
            source.rights_id(),
            source.source_id().as_str(),
            now.unix_nanos(),
            research_use_mask(requested_use),
        ],
        |row| {
            Ok((
                row.get::<_, Vec<u8>>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, Vec<u8>>(2)?,
                row.get::<_, Option<i64>>(3)?,
            ))
        },
    )?;
    let mut saw_expired = false;
    let mut saw_revoked = false;
    for row in rows {
        let grant = row?;
        let grant_id = parse_digest(grant.0)?;
        if grant.3.is_some_and(|expiry| now.unix_nanos() >= expiry) {
            saw_expired = true;
            continue;
        }
        let revoked: bool = transaction.query_row(
            "SELECT EXISTS(
                SELECT 1 FROM source_research_use_revocations
                WHERE research_grant_id=?1 AND revocation_sequence<=?2
                  AND effective_at_ns<=?3 AND recorded_at_ns<=?3
                  AND (use_mask & ?4)<>0
             )",
            params![
                grant_id,
                to_i64(frontier)?,
                now.unix_nanos(),
                research_use_mask(requested_use),
            ],
            |row| row.get(0),
        )?;
        if revoked {
            saw_revoked = true;
            continue;
        }
        let authority = ResearchUseAuthorityEvidence::try_new(
            source.clone(),
            source.rights_id(),
            parse_digest(rights.0)?,
            parse_evidence(rights.1, rights.2)?,
            rights.3.map(Timestamp::from_unix_nanos),
            grant_id,
            parse_evidence(grant.1, grant.2)?,
            grant.3.map(Timestamp::from_unix_nanos),
            frontier,
        )
        .map_err(|_| ResearchUseCatalogError::CorruptCatalog)?;
        return Ok(SourceAuthority::Selected(authority));
    }
    let reason = if saw_revoked {
        ResearchUseDenialReason::Revoked
    } else if saw_expired {
        ResearchUseDenialReason::Expired
    } else {
        ResearchUseDenialReason::MissingGrant
    };
    Ok(SourceAuthority::Denied(reason))
}

fn decision_expiry(
    now: Timestamp,
    limits: super::ResearchUseLimits,
    authorities: &[ResearchUseAuthorityEvidence],
) -> Result<Timestamp, ResearchUseCatalogError> {
    let lifetime = i64::try_from(limits.permit_lifetime().as_nanos())
        .map_err(|_| ResearchUseCatalogError::LimitExceeded)?;
    let mut expiry = now
        .checked_add_nanos(lifetime)
        .map_err(|_| ResearchUseCatalogError::LimitExceeded)?;
    for authority in authorities {
        if let Some(value) = authority.rights_expires_at() {
            expiry = expiry.min(value);
        }
        if let Some(value) = authority.grant_expires_at() {
            expiry = expiry.min(value);
        }
    }
    if expiry <= now {
        Err(ResearchUseCatalogError::Expired)
    } else {
        Ok(expiry)
    }
}

fn persist_decision(
    transaction: &Transaction<'_>,
    graph: &ResearchUseGraph,
    decision: &ResearchUseDecisionInput,
    requested_at: Timestamp,
    selections: &BTreeMap<u64, SourceSelection>,
    frontier: u64,
    cancellation: &CancellationToken,
    deadline: Instant,
) -> Result<(), ResearchUseCatalogError> {
    let decision_id = decision.digest().bytes();
    for (ordinal, node) in graph.nodes().iter().enumerate() {
        super::traversal::check_control(cancellation, deadline)?;
        transaction.execute(
            "INSERT INTO research_use_decision_nodes
             (decision_id, ordinal, generation_sequence) VALUES (?1, ?2, ?3)",
            params![
                decision_id,
                to_i64_usize(ordinal)?,
                to_i64(node.sequence())?
            ],
        )?;
    }
    for (ordinal, root) in graph.roots().iter().enumerate() {
        super::traversal::check_control(cancellation, deadline)?;
        let sequence = graph
            .nodes()
            .iter()
            .find(|node| node.manifest() == root)
            .map(|node| node.sequence())
            .ok_or(ResearchUseCatalogError::CorruptCatalog)?;
        transaction.execute(
            "INSERT INTO research_use_decision_roots
             (decision_id, ordinal, generation_sequence, dataset_id, manifest_version,
              schema_name, schema_version, schema_fingerprint, content_hash)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            params![
                decision_id,
                to_i64_usize(ordinal)?,
                to_i64(sequence)?,
                root.dataset_id().as_str(),
                to_i64(root.manifest_version())?,
                root.schema().name(),
                i64::from(root.schema().version().get()),
                root.schema().fingerprint(),
                root.content_hash().bytes(),
            ],
        )?;
    }
    for (ordinal, source) in graph.sources().iter().enumerate() {
        super::traversal::check_control(cancellation, deadline)?;
        let selection = selections
            .get(&source.generation_sequence())
            .ok_or(ResearchUseCatalogError::CorruptCatalog)?;
        let (outcome, grant_id, selected_expiry, frontier) = match selection {
            SourceSelection::Selected(authority) => (
                "selected",
                Some(authority.research_grant_id()),
                authority.grant_expires_at().map(Timestamp::unix_nanos),
                authority.revocation_frontier(),
            ),
            SourceSelection::Missing => ("missing", None, None, frontier),
        };
        transaction.execute(
            "INSERT INTO research_use_decision_sources
             (decision_id, ordinal, source_generation_sequence, run_id, source_id, rights_id,
              requested_use, selection_outcome, selected_research_grant_id,
              selected_expires_at_ns, observed_revocation_sequence)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
            params![
                decision_id,
                to_i64_usize(ordinal)?,
                to_i64(source.generation_sequence())?,
                source.ingest_run_id().to_string(),
                source.source_id().as_str(),
                source.rights_id(),
                decision.requested_use().database_name(),
                outcome,
                grant_id,
                selected_expiry,
                to_i64(frontier)?,
            ],
        )?;
    }
    let (outcome, reason) = match decision.outcome() {
        ResearchUseDecisionOutcome::Allowed => ("allowed", None),
        ResearchUseDecisionOutcome::Denied(reason) => ("denied", Some(denial_name(reason))),
    };
    let limits = decision.limits();
    super::traversal::check_control(cancellation, deadline)?;
    transaction.execute(
        "INSERT INTO research_use_decisions
         (decision_id, policy_version, requested_use, graph_digest, requested_at_ns,
          decided_at_ns, expires_at_ns, outcome, reason_code, max_root_count, max_node_count,
          max_edge_count, max_source_count, max_retained_bytes, traversal_deadline_ns,
          permit_lifetime_ns, root_count, node_count, edge_count, source_count, retained_bytes)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15,
                 ?16, ?17, ?18, ?19, ?20, ?21)",
        params![
            decision_id,
            i64::from(decision.policy_version()),
            decision.requested_use().database_name(),
            decision.graph_digest().bytes(),
            requested_at.unix_nanos(),
            decision.evaluated_at().unix_nanos(),
            decision.expires_at().map(Timestamp::unix_nanos),
            outcome,
            reason,
            to_i64_usize(limits.max_roots())?,
            to_i64_usize(limits.max_nodes())?,
            to_i64_usize(limits.max_edges())?,
            to_i64_usize(limits.max_sources())?,
            to_i64_usize(limits.max_retained_bytes())?,
            duration_nanos(limits.traversal_deadline())?,
            duration_nanos(limits.permit_lifetime())?,
            to_i64_usize(graph.roots().len())?,
            to_i64_usize(graph.nodes().len())?,
            to_i64_usize(graph.edges().len())?,
            to_i64_usize(graph.sources().len())?,
            to_i64_usize(graph.retained_bytes())?,
        ],
    )?;
    super::traversal::check_control(cancellation, deadline)?;
    Ok(())
}

fn preferred_denial(
    current: Option<ResearchUseDenialReason>,
    candidate: ResearchUseDenialReason,
) -> ResearchUseDenialReason {
    match (current, candidate) {
        (_, ResearchUseDenialReason::Revoked) => ResearchUseDenialReason::Revoked,
        (Some(ResearchUseDenialReason::Revoked), _) => ResearchUseDenialReason::Revoked,
        (_, ResearchUseDenialReason::Expired) => ResearchUseDenialReason::Expired,
        (Some(ResearchUseDenialReason::Expired), _) => ResearchUseDenialReason::Expired,
        (_, value) => value,
    }
}

fn denial_name(reason: ResearchUseDenialReason) -> &'static str {
    match reason {
        ResearchUseDenialReason::MissingGrant => "missing_grant",
        ResearchUseDenialReason::Expired => "expired",
        ResearchUseDenialReason::Revoked => "revoked",
        ResearchUseDenialReason::CorruptAuthority => "corrupt_authority",
        ResearchUseDenialReason::LimitExceeded => "limit_exceeded",
        ResearchUseDenialReason::Cancelled => "cancelled",
        ResearchUseDenialReason::DeadlineExceeded => "deadline_exceeded",
    }
}
