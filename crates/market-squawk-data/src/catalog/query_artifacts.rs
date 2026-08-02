//! Durable least-authority reservations and reachability for analytical query artifacts.

use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use market_squawk_domain::{DigestAlgorithm, EvidenceDigest, SourceIdentifier, Timestamp};
use rusqlite::{OptionalExtension as _, params};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::runs::CatalogAuthority;
use super::storage::{append_audit, digest_columns, trusted_catalog_now};
use super::types::{ArtifactRecord, Catalog, CatalogError};

const MAX_QUERY_ARTIFACT_BYTES: u64 = 256 * 1024 * 1024;
const MAX_QUERY_ARTIFACT_TTL_NS: i64 = 24 * 60 * 60 * 1_000_000_000;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum QueryArtifactBindCheckpoint {
    BeforeCommit,
    AfterCommit,
}

struct StoredQueryArtifactReservation {
    owner: String,
    algorithm: i64,
    digest: Vec<u8>,
    max_bytes: i64,
    requested_at: i64,
    expires_at: i64,
    state: String,
}

/// Bounded ownership, request identity, and lifetime for a query-artifact reservation.
#[derive(Debug)]
pub struct QueryArtifactReservationInput {
    owner: SourceIdentifier,
    request_identity: EvidenceDigest,
    max_bytes: u64,
    expires_at: Timestamp,
}

impl QueryArtifactReservationInput {
    /// Constructs one exact SHA-256 request binding with a finite durable lifetime.
    pub fn try_new(
        owner: SourceIdentifier,
        request_identity: EvidenceDigest,
        max_bytes: u64,
        expires_at: Timestamp,
    ) -> Result<Self, CatalogError> {
        if request_identity.algorithm() != DigestAlgorithm::Sha256
            || max_bytes == 0
            || max_bytes > MAX_QUERY_ARTIFACT_BYTES
            || i64::try_from(max_bytes).is_err()
        {
            return Err(CatalogError::InvalidRecord);
        }
        Ok(Self {
            owner,
            request_identity,
            max_bytes,
            expires_at,
        })
    }
}

/// Non-cloneable authority receipt proving a reservation was durable before publication.
pub struct QueryArtifactReservation {
    reservation_id: Uuid,
    owner: SourceIdentifier,
    request_identity: EvidenceDigest,
    max_bytes: u64,
    requested_at: Timestamp,
    expires_at: Timestamp,
    catalog_id: Uuid,
}

impl QueryArtifactReservation {
    /// Returns the opaque durable reservation identity.
    pub const fn reservation_id(&self) -> Uuid {
        self.reservation_id
    }

    /// Returns the explicit result owner.
    pub const fn owner(&self) -> &SourceIdentifier {
        &self.owner
    }

    /// Returns the trusted reservation time.
    pub const fn requested_at(&self) -> Timestamp {
        self.requested_at
    }

    /// Returns the exclusive durable reachability expiry.
    pub const fn expires_at(&self) -> Timestamp {
        self.expires_at
    }

    pub(crate) const fn request_identity(&self) -> EvidenceDigest {
        self.request_identity
    }

    pub(crate) const fn max_bytes(&self) -> u64 {
        self.max_bytes
    }

    pub(crate) const fn catalog_id(&self) -> Uuid {
        self.catalog_id
    }
}

impl fmt::Debug for QueryArtifactReservation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("QueryArtifactReservation")
            .field("reservation_id", &self.reservation_id)
            .field("owner", &self.owner)
            .field("request_identity", &"[SEALED]")
            .field("max_bytes", &self.max_bytes)
            .field("requested_at", &self.requested_at)
            .field("expires_at", &self.expires_at)
            .field("catalog_capability", &"[SEALED]")
            .finish()
    }
}

/// Durable ownership and lifecycle receipt returned with an authorized query artifact.
#[derive(Debug, Eq, PartialEq)]
pub struct QueryArtifactResult {
    reservation_id: Uuid,
    owner: SourceIdentifier,
    artifact_id: Uuid,
    expires_at: Timestamp,
}

impl QueryArtifactResult {
    /// Returns the durable reservation that owns this result.
    pub const fn reservation_id(&self) -> Uuid {
        self.reservation_id
    }

    /// Returns the explicit result owner.
    pub const fn owner(&self) -> &SourceIdentifier {
        &self.owner
    }

    /// Returns the exact controlled artifact identity.
    pub const fn artifact_id(&self) -> Uuid {
        self.artifact_id
    }

    /// Returns the exclusive durable reachability expiry.
    pub const fn expires_at(&self) -> Timestamp {
        self.expires_at
    }
}

/// Least-authority binder shared by ingestion and query composition over one catalog writer.
pub(crate) struct QueryArtifactPublisher {
    authority: Arc<Mutex<CatalogAuthority>>,
}

impl QueryArtifactPublisher {
    pub(crate) fn new(authority: Arc<Mutex<CatalogAuthority>>) -> Self {
        Self { authority }
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "binding keeps authority, cancellation, deadline, and durability capabilities explicit"
    )]
    pub(crate) fn bind(
        &self,
        reservation: &QueryArtifactReservation,
        artifact: &ArtifactRecord,
        cancellation: &CancellationToken,
        deadline: Instant,
        #[cfg(test)] precommit_deadline: Option<Instant>,
        durable_bound: &AtomicBool,
        checkpoint: &mut impl FnMut(QueryArtifactBindCheckpoint),
    ) -> Result<QueryArtifactResult, CatalogError> {
        self.authority
            .lock()
            .map_err(|_| CatalogError::AuthorityLockPoisoned)?
            .bind_query_artifact(
                reservation,
                artifact,
                cancellation,
                deadline,
                #[cfg(test)]
                precommit_deadline,
                durable_bound,
                checkpoint,
            )
    }
}

impl fmt::Debug for QueryArtifactPublisher {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("QueryArtifactPublisher")
            .field("authority", &"[SHARED LEAST AUTHORITY]")
            .finish()
    }
}

impl CatalogAuthority {
    /// Persists a query-artifact reservation before any final object publication can begin.
    pub fn reserve_query_artifact(
        &self,
        input: QueryArtifactReservationInput,
    ) -> Result<QueryArtifactReservation, CatalogError> {
        self.catalog().reserve_query_artifact(input)
    }
}

impl Catalog {
    fn reserve_query_artifact(
        &self,
        input: QueryArtifactReservationInput,
    ) -> Result<QueryArtifactReservation, CatalogError> {
        let transaction = self.connection.unchecked_transaction()?;
        let requested_at = trusted_catalog_now(&transaction)?;
        let ttl = input
            .expires_at
            .unix_nanos()
            .checked_sub(requested_at.unix_nanos())
            .filter(|ttl| (1..=MAX_QUERY_ARTIFACT_TTL_NS).contains(ttl))
            .ok_or(CatalogError::QueryArtifactExpired)?;
        if ttl == 0 {
            return Err(CatalogError::QueryArtifactExpired);
        }
        let reservation = QueryArtifactReservation {
            reservation_id: Uuid::new_v4(),
            owner: input.owner,
            request_identity: input.request_identity,
            max_bytes: input.max_bytes,
            requested_at,
            expires_at: input.expires_at,
            catalog_id: self.catalog_id,
        };
        let (algorithm, digest) = digest_columns(reservation.request_identity);
        transaction.execute(
            "INSERT INTO query_artifact_reservations
             (reservation_id, owner, request_algorithm, request_digest, max_bytes,
              requested_at_ns, expires_at_ns, state, bound_at_ns)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, 'reserved', NULL)",
            params![
                reservation.reservation_id.to_string(),
                reservation.owner.as_str(),
                algorithm,
                digest,
                i64::try_from(reservation.max_bytes).map_err(|_| CatalogError::InvalidRecord)?,
                reservation.requested_at.unix_nanos(),
                reservation.expires_at.unix_nanos(),
            ],
        )?;
        append_audit(
            &transaction,
            "query-artifact.reserved",
            &reservation.reservation_id.to_string(),
            reservation.request_identity.bytes(),
            requested_at,
        )?;
        transaction.commit()?;
        Ok(reservation)
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "binding keeps authority, cancellation, deadline, and durability capabilities explicit"
    )]
    fn bind_query_artifact(
        &self,
        reservation: &QueryArtifactReservation,
        artifact: &ArtifactRecord,
        cancellation: &CancellationToken,
        deadline: Instant,
        #[cfg(test)] precommit_deadline: Option<Instant>,
        durable_bound: &AtomicBool,
        checkpoint: &mut impl FnMut(QueryArtifactBindCheckpoint),
    ) -> Result<QueryArtifactResult, CatalogError> {
        check_query_artifact_boundary(cancellation, deadline)?;
        if reservation.catalog_id != self.catalog_id {
            return Err(CatalogError::InvalidReservationCapability);
        }
        let transaction = self.connection.unchecked_transaction()?;
        let bound_at = trusted_catalog_now(&transaction)?;
        let stored: Option<StoredQueryArtifactReservation> = transaction
            .query_row(
                "SELECT owner, request_algorithm, request_digest, max_bytes,
                        requested_at_ns, expires_at_ns, state
                 FROM query_artifact_reservations WHERE reservation_id=?1",
                [reservation.reservation_id.to_string()],
                |row| {
                    Ok(StoredQueryArtifactReservation {
                        owner: row.get(0)?,
                        algorithm: row.get(1)?,
                        digest: row.get(2)?,
                        max_bytes: row.get(3)?,
                        requested_at: row.get(4)?,
                        expires_at: row.get(5)?,
                        state: row.get(6)?,
                    })
                },
            )
            .optional()?;
        let stored = stored.ok_or(CatalogError::InvalidReservationCapability)?;
        if stored.owner != reservation.owner.as_str()
            || stored.algorithm != 1
            || stored.digest.as_slice() != reservation.request_identity.bytes()
            || stored.max_bytes != i64::try_from(reservation.max_bytes).unwrap_or(i64::MAX)
            || stored.requested_at != reservation.requested_at.unix_nanos()
            || stored.expires_at != reservation.expires_at.unix_nanos()
            || stored.state != "reserved"
        {
            return Err(CatalogError::QueryArtifactReservationMismatch);
        }
        if bound_at >= reservation.expires_at {
            return Err(CatalogError::QueryArtifactExpired);
        }
        if artifact.content_digest().algorithm() != DigestAlgorithm::Sha256
            || artifact.size_bytes() == 0
            || artifact.size_bytes() > reservation.max_bytes
            || artifact.created_at() < reservation.requested_at
            || artifact.created_at() > bound_at
        {
            return Err(CatalogError::PublicationTimeConflict);
        }
        let (content_algorithm, content_digest) = digest_columns(artifact.content_digest());
        transaction.execute(
            "INSERT INTO query_artifact_results
             (reservation_id, artifact_id, relative_reference, content_algorithm,
              content_digest, size_bytes, created_at_ns, bound_at_ns)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            params![
                reservation.reservation_id.to_string(),
                artifact.artifact_id().to_string(),
                artifact.relative_reference(),
                content_algorithm,
                content_digest,
                i64::try_from(artifact.size_bytes()).map_err(|_| CatalogError::InvalidRecord)?,
                artifact.created_at().unix_nanos(),
                bound_at.unix_nanos(),
            ],
        )?;
        let changed = transaction.execute(
            "UPDATE query_artifact_reservations
             SET state='published', bound_at_ns=?1
             WHERE reservation_id=?2 AND state='reserved' AND bound_at_ns IS NULL",
            params![
                bound_at.unix_nanos(),
                reservation.reservation_id.to_string()
            ],
        )?;
        if changed != 1 {
            return Err(CatalogError::QueryArtifactReservationMismatch);
        }
        append_audit(
            &transaction,
            "query-artifact.published",
            &reservation.reservation_id.to_string(),
            artifact.content_digest().bytes(),
            bound_at,
        )?;
        checkpoint(QueryArtifactBindCheckpoint::BeforeCommit);
        #[cfg(test)]
        let deadline = precommit_deadline.unwrap_or(deadline);
        check_query_artifact_boundary(cancellation, deadline)?;
        transaction.commit()?;
        durable_bound.store(true, Ordering::Release);
        checkpoint(QueryArtifactBindCheckpoint::AfterCommit);
        Ok(QueryArtifactResult {
            reservation_id: reservation.reservation_id,
            owner: reservation.owner.clone(),
            artifact_id: artifact.artifact_id(),
            expires_at: reservation.expires_at,
        })
    }
}

fn check_query_artifact_boundary(
    cancellation: &CancellationToken,
    deadline: Instant,
) -> Result<(), CatalogError> {
    if cancellation.is_cancelled() {
        Err(CatalogError::QueryArtifactCancelled)
    } else if Instant::now() >= deadline {
        Err(CatalogError::QueryArtifactDeadlineExceeded)
    } else {
        Ok(())
    }
}
