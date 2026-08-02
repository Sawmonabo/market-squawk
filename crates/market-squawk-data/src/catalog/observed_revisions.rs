//! Durable, collision-checked observed-revision assignment.

use std::cmp::Ordering;
use std::time::Instant;

use market_squawk_domain::{RevisionNumber, Timestamp};
use market_squawk_sources::{
    CanonicalObservationFamily, ObservedProviderOrder, ObservedRevisionAssignments,
    ObservedRevisionBatch, ObservedRevisionError, ObservedRevisionRecord, ObservedSemanticPayload,
    ObservedVersionEvidence, ObservedVersionKind,
};
use rusqlite::{Transaction, params};
use tokio_util::sync::CancellationToken;

use self::canonical::{canonical_batch_digest, exact_record_match, provider_order_cmp};
use self::persistence::{
    insert_version, load_frontier, load_version_identity, persist_batch, persist_batch_member,
    persist_family, require_source, validate_retained_family,
};
use self::stored::StoredVersionRow;
use super::storage::{ResultBudget, digest_columns, trusted_catalog_now};
use super::{Catalog, CatalogLimit};

mod async_authority;
mod canonical;
mod persistence;
mod stored;

pub(crate) use async_authority::CatalogObservedRevisionAuthority;

pub(super) const FAMILY_ENCODING_VERSION: i64 = 1;
pub(super) const VERSION_EVIDENCE_VERSION: i64 = 1;
pub(super) const PAYLOAD_EVIDENCE_VERSION: i64 = 1;
pub(super) const PROVIDER_ORDER_EVIDENCE_VERSION: i64 = 1;
pub(super) const BATCH_CANONICAL_VERSION: i64 = 1;
pub(super) const BATCH_CANONICAL_DOMAIN: &[u8] = b"market-squawk/observed-revision-batch/v1";

/// One validated immutable revision loaded from the local observed-revision authority.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StoredObservedRevision {
    pub(super) revision: RevisionNumber,
    pub(super) version: ObservedVersionEvidence,
    pub(super) semantic_payload: ObservedSemanticPayload,
    pub(super) provider_order: Option<ObservedProviderOrder>,
    pub(super) assigned_at: Timestamp,
}

impl StoredObservedRevision {
    /// Returns the replay-stable one-based revision.
    pub const fn revision(&self) -> RevisionNumber {
        self.revision
    }

    /// Returns the exact provider or locally observed version evidence.
    pub const fn version(&self) -> &ObservedVersionEvidence {
        &self.version
    }

    /// Returns the exact canonical semantic payload evidence.
    pub const fn semantic_payload(&self) -> &ObservedSemanticPayload {
        &self.semantic_payload
    }

    /// Returns explicit provider ordering without manufacturing missing evidence.
    pub const fn provider_order(&self) -> Option<&ObservedProviderOrder> {
        self.provider_order.as_ref()
    }

    /// Returns when this revision was first committed by the catalog authority.
    pub const fn assigned_at(&self) -> Timestamp {
        self.assigned_at
    }
}

impl Catalog {
    /// Atomically assigns durable revisions to one bounded, validated single-source batch.
    ///
    /// The operation exact-compares retained evidence on every digest hit. An exact retry returns
    /// the prior revisions, while divergent evidence, an unordered append, cancellation, or an
    /// elapsed deadline leaves the entire batch uncommitted. This is blocking control-plane I/O
    /// and must not run in the live event-to-action path or directly on an asynchronous executor.
    pub fn assign_observed_revisions(
        &self,
        batch: ObservedRevisionBatch,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<ObservedRevisionAssignments, ObservedRevisionError> {
        check_operation(deadline, cancellation)?;
        let batch_digest = canonical_batch_digest(&batch)?;
        let transaction = self
            .connection
            .unchecked_transaction()
            .map_err(map_persistence_error)?;
        require_source(&transaction, batch.source_id().as_str())?;
        let assigned_at = trusted_catalog_now(&transaction).map_err(map_catalog_error)?;
        let mut revisions = Vec::new();
        revisions
            .try_reserve_exact(batch.unique_records().len())
            .map_err(|_| ObservedRevisionError::AllocationFailure)?;

        for (ordinal, record) in batch.unique_records().iter().enumerate() {
            check_operation(deadline, cancellation)?;
            persist_family(&transaction, record.family())?;
            let revision = assign_record(&transaction, record, assigned_at)?;
            persist_batch_member(
                &transaction,
                batch.source_id().as_str(),
                batch_digest,
                ordinal,
                record,
                revision,
            )?;
            revisions.push(revision);
        }
        persist_batch(&transaction, &batch, batch_digest, assigned_at)?;
        check_operation(deadline, cancellation)?;
        transaction.commit().map_err(map_persistence_error)?;
        batch.align_assignments(revisions)
    }

    /// Loads oldest-first immutable history for one exact canonical observation family.
    ///
    /// Every row is reconstructed through the current bounded source contracts and all retained
    /// digests and canonical encodings are revalidated before it is returned.
    pub fn observed_revision_history(
        &self,
        family: &CanonicalObservationFamily,
        limit: CatalogLimit,
    ) -> Result<Vec<StoredObservedRevision>, ObservedRevisionError> {
        self.enforce_limit(limit).map_err(map_catalog_error)?;
        validate_retained_family(&self.connection, family)?;
        let row_limit = i64::try_from(limit.get())
            .map_err(|_| ObservedRevisionError::PersistenceUnavailable)?;
        let (family_algorithm, family_digest) = digest_columns(family.identity());
        let mut statement = self
            .connection
            .prepare(
                "SELECT revision, version_kind, version_algorithm, version_digest,
                        version_evidence_version, version_evidence, payload_algorithm,
                        payload_digest, payload_evidence_version, payload_evidence,
                        provider_order_evidence_version, provider_coordinate_json,
                        provider_tie_breaker, assigned_at_ns
                 FROM observed_revision_versions
                 WHERE source_id=?1 AND family_algorithm=?2 AND family_digest=?3
                 ORDER BY revision LIMIT ?4",
            )
            .map_err(map_persistence_error)?;
        let mut rows = statement
            .query(params![
                family.source_id().as_str(),
                family_algorithm,
                family_digest,
                row_limit
            ])
            .map_err(map_persistence_error)?;
        let mut budget = ResultBudget::new(self.result_bytes);
        let mut history = Vec::new();
        history
            .try_reserve_exact(budget.bounded_row_capacity(limit.get()))
            .map_err(|_| ObservedRevisionError::AllocationFailure)?;
        while let Some(row) = rows.next().map_err(map_persistence_error)? {
            let stored = StoredVersionRow::read(row).map_err(map_persistence_error)?;
            budget
                .charge(stored.retained_components())
                .map_err(map_catalog_error)?;
            history.push(stored.decode()?);
        }
        Ok(history)
    }
}

fn assign_record(
    transaction: &Transaction<'_>,
    record: &ObservedRevisionRecord,
    assigned_at: Timestamp,
) -> Result<RevisionNumber, ObservedRevisionError> {
    if record.version().kind() == ObservedVersionKind::LocallyObservedContent
        && record.provider_order().is_some()
    {
        return Err(ObservedRevisionError::Conflict);
    }
    if let Some(existing) = load_version_identity(transaction, record)? {
        let existing = existing.decode()?;
        if exact_record_match(&existing, record) {
            return Ok(existing.revision);
        }
        return Err(ObservedRevisionError::Conflict);
    }

    let frontier = load_frontier(transaction, record.family())?
        .map(StoredVersionRow::decode)
        .transpose()?;
    if let Some(frontier) = &frontier {
        match (record.version().kind(), frontier.version.kind()) {
            (
                ObservedVersionKind::LocallyObservedContent,
                ObservedVersionKind::LocallyObservedContent,
            ) => {}
            (ObservedVersionKind::ProviderSupplied, ObservedVersionKind::ProviderSupplied) => {
                let incoming = record
                    .provider_order()
                    .ok_or(ObservedRevisionError::BackfillRejected)?;
                let retained = frontier
                    .provider_order
                    .as_ref()
                    .ok_or(ObservedRevisionError::BackfillRejected)?;
                if provider_order_cmp(incoming, retained) != Some(Ordering::Greater) {
                    return Err(ObservedRevisionError::BackfillRejected);
                }
            }
            _ => return Err(ObservedRevisionError::Conflict),
        }
    }
    let next = match frontier {
        None => 1,
        Some(retained) => retained
            .revision
            .get()
            .checked_add(1)
            .ok_or(ObservedRevisionError::RevisionOverflow)?,
    };
    let revision =
        RevisionNumber::new(next).map_err(|_| ObservedRevisionError::RevisionOverflow)?;
    insert_version(transaction, record, revision, assigned_at)?;
    Ok(revision)
}

pub(super) fn check_operation(
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<(), ObservedRevisionError> {
    if cancellation.is_cancelled() {
        Err(ObservedRevisionError::Cancelled)
    } else if Instant::now() >= deadline {
        Err(ObservedRevisionError::DeadlineExceeded)
    } else {
        Ok(())
    }
}

fn map_catalog_error(error: super::CatalogError) -> ObservedRevisionError {
    match error {
        super::CatalogError::Allocation => ObservedRevisionError::AllocationFailure,
        super::CatalogError::CorruptCatalog => ObservedRevisionError::CorruptAuthorityState,
        _ => ObservedRevisionError::PersistenceUnavailable,
    }
}

pub(super) fn map_persistence_error(_error: rusqlite::Error) -> ObservedRevisionError {
    ObservedRevisionError::PersistenceUnavailable
}

#[cfg(test)]
#[path = "observed_revisions/tests.rs"]
mod tests;
