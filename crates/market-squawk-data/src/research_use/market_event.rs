//! Source-use authority for exact selected event rows at a logical collection horizon.
//!
//! This receipt is neither a physical-manifest permit nor authority over a horizon's ancestors.
//! A hybrid query must authorize its selected coordinates before exposing their rows.

use std::time::Instant;

use market_squawk_domain::{DigestAlgorithm, EvidenceDigest, SourceId, Timestamp};
use rusqlite::{Connection, OptionalExtension as _, params};
use sha2::{Digest as _, Sha256};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::persistence::{RetainedSourceUseGrant, SourceGrantSelection};
use super::traversal::check_control;
use super::{
    ResearchUse, ResearchUseCatalogError, ResearchUseDecisionDigest, ResearchUseDenialReason,
    ResearchUseLimits, select_source_use_grant, source_use_frontier,
};
use crate::catalog::market_event_store::{
    load_market_event_commit, load_market_event_commit_for_publication,
};
use crate::catalog::provider_event::provider_market_event_selection_for_publication;
use crate::catalog::{CatalogReadSnapshot, now_timestamp};
use crate::provider_event_selection::publication_kind_name;
use crate::{
    CatalogError, MarketEventCommitRef, ProviderMarketEventExactPublication,
    ProviderMarketEventPublicationKind, ProviderMarketEventSelectionCoordinate,
};

#[derive(Clone, Debug, Eq, PartialEq)]
/// Exact retained row coordinates; reconstruction alone conveys no authority.
pub struct MarketEventUseInput {
    publication: ProviderMarketEventExactPublication,
    row: u32,
    coordinate_digest: EvidenceDigest,
    canonical_event_digest: EvidenceDigest,
    source: SourceId,
    origin_committed_at: Timestamp,
}

impl MarketEventUseInput {
    /// Decodes checked retained coordinates. Only catalog authorization can admit their use.
    #[allow(
        clippy::too_many_arguments,
        reason = "all exact retained row coordinates are required"
    )]
    pub fn try_new(
        publication_digest: EvidenceDigest,
        publication_kind: ProviderMarketEventPublicationKind,
        row_ordinal: u32,
        coordinate_digest: EvidenceDigest,
        canonical_event_digest: EvidenceDigest,
        source_id: SourceId,
        origin_committed_at: Timestamp,
    ) -> Result<Self, ResearchUseCatalogError> {
        if [
            publication_digest,
            coordinate_digest,
            canonical_event_digest,
        ]
        .iter()
        .any(|digest| digest.algorithm() != DigestAlgorithm::Sha256 || digest.bytes() == [0; 32])
        {
            return Err(ResearchUseCatalogError::InvalidPublication);
        }
        Ok(Self {
            publication: ProviderMarketEventExactPublication::from_catalog(
                publication_digest,
                publication_kind,
            ),
            row: row_ordinal,
            coordinate_digest,
            canonical_event_digest,
            source: source_id,
            origin_committed_at,
        })
    }
    /// Returns the exact source publication digest.
    pub const fn publication_digest(&self) -> EvidenceDigest {
        self.publication.digest()
    }
    /// Returns the closed source publication kind.
    pub const fn publication_kind(&self) -> ProviderMarketEventPublicationKind {
        self.publication.kind()
    }
    /// Returns the row ordinal within the publication.
    pub const fn row_ordinal(&self) -> u32 {
        self.row
    }
    /// Returns the canonical source-coordinate digest.
    pub const fn coordinate_digest(&self) -> EvidenceDigest {
        self.coordinate_digest
    }
    /// Returns the canonical typed-event digest.
    pub const fn canonical_event_digest(&self) -> EvidenceDigest {
        self.canonical_event_digest
    }
    /// Returns the exact source surface.
    pub const fn source_id(&self) -> &SourceId {
        &self.source
    }
    /// Returns the original logical publication clock.
    pub const fn origin_committed_at(&self) -> Timestamp {
        self.origin_committed_at
    }
}

/// A bounded exact row selection; a horizon alone never grants source-use authority.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MarketEventUseRequest {
    commit: MarketEventCommitRef,
    inputs: Box<[MarketEventUseInput]>,
    requested_use: ResearchUse,
    limits: ResearchUseLimits,
    input_digest: EvidenceDigest,
}

impl MarketEventUseRequest {
    /// Binds opaque reader-validated coordinates without resolving mutable latest state.
    pub fn try_new(
        commit: MarketEventCommitRef,
        coordinates: Vec<ProviderMarketEventSelectionCoordinate>,
        requested_use: ResearchUse,
        limits: ResearchUseLimits,
    ) -> Result<Self, ResearchUseCatalogError> {
        if coordinates.is_empty() || coordinates.len() > limits.max_sources() {
            return Err(ResearchUseCatalogError::LimitExceeded);
        }
        let mut inputs = Vec::new();
        inputs
            .try_reserve_exact(coordinates.len())
            .map_err(|_| ResearchUseCatalogError::LimitExceeded)?;
        for coordinate in coordinates {
            inputs.push(MarketEventUseInput {
                publication: coordinate.publication(),
                row: coordinate.publication_row_ordinal(),
                coordinate_digest: coordinate.coordinate_digest(),
                canonical_event_digest: coordinate.canonical_event_digest(),
                source: coordinate.source_surface().clone(),
                origin_committed_at: coordinate.origin_committed_at(),
            });
        }
        Self::try_from_retained(commit, inputs, requested_use, limits)
    }

    /// Reconstructs an untrusted exact request; catalog authorization revalidates every field.
    pub fn try_from_retained(
        commit: MarketEventCommitRef,
        mut inputs: Vec<MarketEventUseInput>,
        requested_use: ResearchUse,
        limits: ResearchUseLimits,
    ) -> Result<Self, ResearchUseCatalogError> {
        if inputs.is_empty() || inputs.len() > limits.max_sources() {
            return Err(ResearchUseCatalogError::LimitExceeded);
        }
        let mut bytes = std::mem::size_of::<AuthorizedMarketEventUse>()
            .checked_add(commit.dataset_id().as_str().len())
            .and_then(|value| value.checked_add(commit.schema().name().len()))
            .ok_or(ResearchUseCatalogError::LimitExceeded)?;
        for input in &inputs {
            bytes = bytes
                .checked_add(std::mem::size_of::<MarketEventUseInput>())
                .and_then(|value| value.checked_add(std::mem::size_of::<SelectedGrant>()))
                .and_then(|value| value.checked_add(input.source.as_str().len()))
                .ok_or(ResearchUseCatalogError::LimitExceeded)?;
            if bytes > limits.max_retained_bytes() {
                return Err(ResearchUseCatalogError::LimitExceeded);
            }
        }
        inputs.sort_unstable_by_key(|input| (input.publication.digest().bytes(), input.row));
        let mut publications = 0usize;
        for (index, input) in inputs.iter().enumerate() {
            if index == 0 || inputs[index - 1].publication.digest() != input.publication.digest() {
                publications += 1;
            } else if inputs[index - 1].row == input.row {
                return Err(ResearchUseCatalogError::InvalidPublication);
            }
        }
        if publications > limits.max_roots() || publications > limits.max_nodes() {
            return Err(ResearchUseCatalogError::LimitExceeded);
        }
        let input_digest = input_digest(&commit, &inputs, requested_use);
        Ok(Self {
            commit,
            inputs: inputs.into_boxed_slice(),
            requested_use,
            limits,
            input_digest,
        })
    }

    /// Returns exact retained inputs, without granting access to their source data.
    pub fn inputs(&self) -> &[MarketEventUseInput] {
        &self.inputs
    }

    /// Returns the stable exact logical-input commitment.
    pub const fn rights_input_digest(&self) -> EvidenceDigest {
        self.input_digest
    }

    /// Checks input membership only; this untrusted request is not source-use authority.
    pub fn contains_event(
        &self,
        commit: &MarketEventCommitRef,
        publication: EvidenceDigest,
        row: u32,
        canonical_event_digest: EvidenceDigest,
    ) -> bool {
        &self.commit == commit
            && self.inputs.iter().any(|input| {
                input.publication.digest() == publication
                    && input.row == row
                    && input.canonical_event_digest == canonical_event_digest
            })
    }

    /// Returns the bounded operation policy.
    pub const fn limits(&self) -> ResearchUseLimits {
        self.limits
    }

    /// Returns the exact logical horizon, not a physical manifest.
    pub const fn commit(&self) -> &MarketEventCommitRef {
        &self.commit
    }

    /// Returns the independently requested downstream use.
    pub const fn requested_use(&self) -> ResearchUse {
        self.requested_use
    }
}

#[derive(Debug, Eq, PartialEq)]
struct SelectedGrant {
    run: Uuid,
    rights: [u8; 32],
    grant: RetainedSourceUseGrant,
}

/// Process-bound authority for exactly the retained selected rows and downstream use.
///
/// Recheck through the owning analytical service before subsequent consumption. Reopening after
/// restart requires fresh authorization; this handle is deliberately not serializable or cloneable.
#[derive(Debug)]
pub struct AuthorizedMarketEventUse {
    session_id: Uuid,
    request: MarketEventUseRequest,
    grants: Box<[SelectedGrant]>,
    evaluated_at: Timestamp,
    expires_at: Timestamp,
    monotonic_expiry: Instant,
    decision_digest: ResearchUseDecisionDigest,
}

impl AuthorizedMarketEventUse {
    /// Returns exact admitted row coordinates, for retained audit and fresh restart authorization.
    pub fn inputs(&self) -> &[MarketEventUseInput] {
        &self.request.inputs
    }

    /// Admits only an exact selected event while this local receipt remains live.
    /// Catalog revocation still requires the owning service's explicit recheck before consumption.
    pub fn admits_event(
        &self,
        commit: &MarketEventCommitRef,
        publication: EvidenceDigest,
        row: u32,
        canonical_event_digest: EvidenceDigest,
    ) -> bool {
        Instant::now() < self.monotonic_expiry
            && now_timestamp().is_ok_and(|now| now >= self.evaluated_at && now < self.expires_at)
            && self
                .request
                .contains_event(commit, publication, row, canonical_event_digest)
    }

    /// Returns the exact logical horizon.
    pub const fn commit(&self) -> &MarketEventCommitRef {
        &self.request.commit
    }
    /// Returns the horizon, exact coordinate and downstream-use identity.
    pub const fn rights_input_digest(&self) -> EvidenceDigest {
        self.request.input_digest
    }
    /// Returns the selected grants and evaluation decision identity.
    pub const fn decision_digest(&self) -> ResearchUseDecisionDigest {
        self.decision_digest
    }
    /// Returns the earliest source-rights, grant or local receipt expiry.
    pub const fn expires_at(&self) -> Timestamp {
        self.expires_at
    }
    /// Returns the independently admitted downstream use.
    pub const fn research_use(&self) -> ResearchUse {
        self.request.requested_use
    }
    /// Returns the wall clock checked against the catalog's durable authority floor.
    pub const fn evaluated_at(&self) -> Timestamp {
        self.evaluated_at
    }
}

/// Evaluates within the caller's single endpoint-bound read transaction.
pub(crate) fn authorize_market_event_use_in_snapshot(
    snapshot: &CatalogReadSnapshot,
    session_id: Uuid,
    request: MarketEventUseRequest,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<AuthorizedMarketEventUse, ResearchUseCatalogError> {
    check_control(cancellation, deadline)?;
    if session_id.is_nil() {
        return Err(ResearchUseCatalogError::InvalidPermitSession);
    }
    let started = Instant::now();
    let connection = snapshot.connection();
    let now = checked_read_clock(connection, None)?;
    validate_horizon(connection, &request.commit)?;
    let frontier = source_use_frontier(connection, now)?;
    let mut grants = Vec::new();
    grants
        .try_reserve_exact(request.inputs.len())
        .map_err(|_| ResearchUseCatalogError::LimitExceeded)?;
    let lifetime = i64::try_from(request.limits.permit_lifetime().as_nanos())
        .map_err(|_| ResearchUseCatalogError::LimitExceeded)?;
    let mut expires_at = now
        .checked_add_nanos(lifetime)
        .map_err(|_| ResearchUseCatalogError::LimitExceeded)?;
    for input in &request.inputs {
        check_control(cancellation, deadline)?;
        let (run, rights) = validate_input(connection, &request.commit, input)?;
        let grant = selected_grant(select_source_use_grant(
            connection,
            rights,
            &input.source,
            request.requested_use,
            now,
            frontier,
            None,
            cancellation,
            deadline,
        )?)?;
        for expiry in [grant.rights_expires_at, grant.grant_expires_at]
            .into_iter()
            .flatten()
        {
            expires_at = expires_at.min(expiry);
        }
        grants.push(SelectedGrant { run, rights, grant });
    }
    if expires_at <= now {
        return Err(ResearchUseCatalogError::Expired);
    }
    let remaining = u64::try_from(
        expires_at
            .unix_nanos()
            .checked_sub(now.unix_nanos())
            .ok_or(ResearchUseCatalogError::LimitExceeded)?,
    )
    .map_err(|_| ResearchUseCatalogError::LimitExceeded)?;
    let monotonic_expiry = started
        .checked_add(std::time::Duration::from_nanos(remaining))
        .ok_or(ResearchUseCatalogError::LimitExceeded)?;
    let decision_digest = decision_digest(&request, &grants, now, expires_at, frontier);
    check_control(cancellation, deadline)?;
    if checked_read_clock(connection, Some(now))? >= expires_at
        || Instant::now() >= monotonic_expiry
    {
        return Err(ResearchUseCatalogError::Expired);
    }
    Ok(AuthorizedMarketEventUse {
        session_id,
        request,
        grants: grants.into_boxed_slice(),
        evaluated_at: now,
        expires_at,
        monotonic_expiry,
        decision_digest,
    })
}

/// Revalidates original capture/run bindings and the exact previously selected grants.
pub(crate) fn recheck_market_event_use_in_snapshot(
    snapshot: &CatalogReadSnapshot,
    session_id: Uuid,
    authorization: &AuthorizedMarketEventUse,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<(), ResearchUseCatalogError> {
    check_control(cancellation, deadline)?;
    if session_id != authorization.session_id {
        return Err(ResearchUseCatalogError::InvalidPermitSession);
    }
    let connection = snapshot.connection();
    let now = checked_read_clock(connection, Some(authorization.evaluated_at))?;
    if now >= authorization.expires_at || Instant::now() >= authorization.monotonic_expiry {
        return Err(ResearchUseCatalogError::Expired);
    }
    validate_horizon(connection, &authorization.request.commit)?;
    let frontier = source_use_frontier(connection, now)?;
    for (input, selected) in authorization
        .request
        .inputs
        .iter()
        .zip(&authorization.grants)
    {
        check_control(cancellation, deadline)?;
        let (run, rights) = validate_input(connection, &authorization.request.commit, input)?;
        if run != selected.run || rights != selected.rights {
            return Err(ResearchUseCatalogError::CorruptCatalog);
        }
        let grant = selected_grant(select_source_use_grant(
            connection,
            rights,
            &input.source,
            authorization.research_use(),
            now,
            frontier,
            Some(selected.grant.research_grant_id),
            cancellation,
            deadline,
        )?)?;
        if grant != selected.grant {
            return Err(ResearchUseCatalogError::InvalidGrant);
        }
    }
    check_control(cancellation, deadline)?;
    if checked_read_clock(connection, Some(authorization.evaluated_at))? >= authorization.expires_at
        || Instant::now() >= authorization.monotonic_expiry
    {
        return Err(ResearchUseCatalogError::Expired);
    }
    Ok(())
}

fn validate_horizon(
    connection: &Connection,
    commit: &MarketEventCommitRef,
) -> Result<(), ResearchUseCatalogError> {
    if load_market_event_commit(connection, commit.dataset_id(), commit.sequence())?.as_ref()
        != Some(commit)
    {
        return Err(ResearchUseCatalogError::UnknownGeneration);
    }
    Ok(())
}

fn validate_input(
    connection: &Connection,
    horizon: &MarketEventCommitRef,
    input: &MarketEventUseInput,
) -> Result<(Uuid, [u8; 32]), ResearchUseCatalogError> {
    let origin = load_market_event_commit_for_publication(
        connection,
        horizon.dataset_id(),
        input.publication.digest(),
    )?
    .ok_or(ResearchUseCatalogError::UnknownGeneration)?;
    if origin.sequence() > horizon.sequence() || origin.available_at() != input.origin_committed_at
    {
        return Err(ResearchUseCatalogError::InvalidPublication);
    }
    // Shared loader verifies capture bindings and canonical coordinate digests. No hot-row or
    // archive placement enters the identity, and no unselected ancestor is traversed.
    let rows =
        provider_market_event_selection_for_publication(connection, input.publication.digest())?;
    let row = rows
        .get(input.row as usize)
        .ok_or(ResearchUseCatalogError::InvalidPublication)?;
    if row.canonical_event_digest() != input.canonical_event_digest
        || row.coordinate_digest() != input.coordinate_digest
        || row.source_id() != input.source.as_str()
    {
        return Err(ResearchUseCatalogError::InvalidPublication);
    }
    let binding: Option<(String, Vec<u8>)> = connection
        .query_row(
            "SELECT run.run_id,run.rights_id FROM market_event_complete_commits AS committed
         JOIN ingest_runs AS run ON run.run_id=committed.run_id
         JOIN ingest_run_provider_publication_bindings AS binding
           ON binding.run_id=run.run_id AND binding.publication_digest=committed.publication_digest
          AND binding.active_dataset_id=committed.dataset_id
          AND binding.active_commit_sequence=committed.commit_sequence
         WHERE committed.dataset_id=?1 AND committed.commit_sequence=?2
           AND committed.publication_digest=?3 AND committed.publication_kind=?4
           AND binding.publication_kind=committed.publication_kind
           AND binding.source_id=?5 AND run.source_id=binding.source_id
           AND run.operation='persist' AND run.payload_algorithm=1
           AND run.payload_digest=committed.publication_digest
           AND run.state='succeeded' AND run.completed_at_ns=committed.available_at_ns",
            params![
                horizon.dataset_id().as_str(),
                i64::try_from(origin.sequence())
                    .map_err(|_| ResearchUseCatalogError::CorruptCatalog)?,
                input.publication.digest().bytes(),
                publication_kind_name(input.publication.kind()),
                input.source.as_str()
            ],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    let (run, rights) = binding.ok_or(ResearchUseCatalogError::CorruptCatalog)?;
    Ok((
        Uuid::parse_str(&run).map_err(|_| ResearchUseCatalogError::CorruptCatalog)?,
        rights
            .try_into()
            .map_err(|_| ResearchUseCatalogError::CorruptCatalog)?,
    ))
}

fn selected_grant(
    selection: SourceGrantSelection,
) -> Result<RetainedSourceUseGrant, ResearchUseCatalogError> {
    match selection {
        SourceGrantSelection::Selected(grant) => Ok(grant),
        SourceGrantSelection::Denied(ResearchUseDenialReason::Expired) => {
            Err(ResearchUseCatalogError::Expired)
        }
        SourceGrantSelection::Denied(ResearchUseDenialReason::Revoked) => {
            Err(ResearchUseCatalogError::Revoked)
        }
        SourceGrantSelection::Denied(_) => Err(ResearchUseCatalogError::InvalidGrant),
    }
}

// Immutable reads do not advance the durable clock. A detected rollback fails closed; recovering
// requires the wall clock to reach the retained floor, not resetting it or reviving this handle.
fn checked_read_clock(
    connection: &Connection,
    evaluated_at: Option<Timestamp>,
) -> Result<Timestamp, ResearchUseCatalogError> {
    let now = now_timestamp()?;
    let floor: i64 = connection.query_row(
        "SELECT last_timestamp_ns FROM catalog_authority_clock WHERE singleton=1",
        [],
        |row| row.get(0),
    )?;
    if now.unix_nanos() < floor || evaluated_at.is_some_and(|value| now < value) {
        return Err(CatalogError::AuthorityClockRollback.into());
    }
    Ok(now)
}

fn input_digest(
    commit: &MarketEventCommitRef,
    inputs: &[MarketEventUseInput],
    requested_use: ResearchUse,
) -> EvidenceDigest {
    let mut hash = Sha256::new();
    hash.update(b"market-squawk/market-event-use-input/v1");
    hash_bytes(&mut hash, commit.dataset_id().as_str().as_bytes());
    hash.update(commit.sequence().to_be_bytes());
    hash.update(commit.content_hash().bytes());
    hash.update(commit.publication_digest().bytes());
    hash.update(commit.available_at().unix_nanos().to_be_bytes());
    hash.update(commit.schema().fingerprint());
    hash.update(commit.row_count().to_be_bytes());
    hash.update([requested_use.tag()]);
    hash.update((inputs.len() as u64).to_be_bytes());
    for input in inputs {
        hash.update(input.publication.digest().bytes());
        hash_bytes(
            &mut hash,
            publication_kind_name(input.publication.kind()).as_bytes(),
        );
        hash.update(input.row.to_be_bytes());
        hash.update(input.coordinate_digest.bytes());
        hash.update(input.canonical_event_digest.bytes());
        hash_bytes(&mut hash, input.source.as_str().as_bytes());
        hash.update(input.origin_committed_at.unix_nanos().to_be_bytes());
    }
    EvidenceDigest::new(DigestAlgorithm::Sha256, hash.finalize().into())
}

fn decision_digest(
    request: &MarketEventUseRequest,
    grants: &[SelectedGrant],
    now: Timestamp,
    expires_at: Timestamp,
    frontier: u64,
) -> ResearchUseDecisionDigest {
    let mut hash = Sha256::new();
    hash.update(b"market-squawk/market-event-use-decision/v1");
    hash.update(request.input_digest.bytes());
    hash.update(now.unix_nanos().to_be_bytes());
    hash.update(expires_at.unix_nanos().to_be_bytes());
    hash.update(frontier.to_be_bytes());
    for limit in [
        request.limits.max_roots(),
        request.limits.max_nodes(),
        request.limits.max_edges(),
        request.limits.max_sources(),
        request.limits.max_retained_bytes(),
    ] {
        hash.update((limit as u64).to_be_bytes());
    }
    hash.update(request.limits.traversal_deadline().as_nanos().to_be_bytes());
    hash.update(request.limits.permit_lifetime().as_nanos().to_be_bytes());
    hash.update((grants.len() as u64).to_be_bytes());
    for selected in grants {
        hash.update(selected.run.as_bytes());
        hash.update(selected.rights);
        hash.update(selected.grant.rights_basis_digest);
        hash_evidence(&mut hash, selected.grant.authorization_evidence);
        hash.update(selected.grant.research_grant_id);
        hash_evidence(&mut hash, selected.grant.grant_evidence);
        for expiry in [
            selected.grant.rights_expires_at,
            selected.grant.grant_expires_at,
        ] {
            hash.update([u8::from(expiry.is_some())]);
            if let Some(expiry) = expiry {
                hash.update(expiry.unix_nanos().to_be_bytes());
            }
        }
    }
    ResearchUseDecisionDigest::from_canonical(hash.finalize().into())
}

fn hash_bytes(hash: &mut Sha256, bytes: &[u8]) {
    hash.update((bytes.len() as u64).to_be_bytes());
    hash.update(bytes);
}

fn hash_evidence(hash: &mut Sha256, digest: EvidenceDigest) {
    hash.update([match digest.algorithm() {
        DigestAlgorithm::Sha256 => 1,
        DigestAlgorithm::Blake3 => 2,
    }]);
    hash.update(digest.bytes());
}
