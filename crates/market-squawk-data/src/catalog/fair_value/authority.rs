//! Transactional append authority and coherent bounded snapshot reads.

use super::hash::{audit_digest, count_exceeds, fixed_digest};
use super::recovery::{
    last_audit, read_and_validate_audit, read_payload, validate_stored_operation,
};
use super::*;

type StoredAuditRow = (
    i64,
    Vec<u8>,
    Option<Vec<u8>>,
    i64,
    String,
    i64,
    i64,
    Vec<u8>,
);

impl CatalogAuthority {
    /// Atomically appends one exact fair-value operation under catalog-trusted time.
    pub fn append_fair_value_operation(
        &self,
        operation: &FairValueCatalogOperation,
        limits: FairValueCatalogSnapshotLimits,
        expected_position: FairValueCatalogPosition,
    ) -> Result<FairValueCatalogCommit, CatalogError> {
        let transaction =
            Transaction::new_unchecked(&self.catalog().connection, TransactionBehavior::Immediate)?;
        let current_position = catalog_position(&transaction)?;
        if current_position != expected_position {
            return Err(CatalogError::FairValuePositionConflict);
        }
        let appended_at = trusted_catalog_now(&transaction)?;
        if let Some(commit) = replay_commit(&transaction, operation, limits)? {
            transaction.commit()?;
            return Ok(commit);
        }
        enforce_append_capacity(&transaction, operation, limits)?;
        for record in &operation.records {
            append_record(&transaction, record, appended_at)?;
        }
        transaction.execute(
            "INSERT INTO fair_value_operations
             (operation_id, operation_kind, actor, business_at_ns, operation_sha256,
              appended_at_ns) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                operation.id.as_slice(),
                operation.kind.tag(),
                operation.actor.as_ref(),
                operation.business_at.unix_nanos(),
                operation.id.as_slice(),
                appended_at.unix_nanos(),
            ],
        )?;
        for (ordinal, record) in operation.records.iter().enumerate() {
            transaction.execute(
                "INSERT INTO fair_value_operation_records
                 (operation_id, ordinal, record_kind, record_id) VALUES (?1, ?2, ?3, ?4)",
                params![
                    operation.id.as_slice(),
                    i64::try_from(ordinal).map_err(|_| CatalogError::InvalidRecord)?,
                    record.kind.tag(),
                    record.id.as_slice(),
                ],
            )?;
        }
        for (ordinal, link) in operation.links.iter().enumerate() {
            transaction.execute(
                "INSERT INTO fair_value_record_links
                 (operation_id, ordinal, source_kind, source_id, relation_kind, target_kind,
                  target_id) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                params![
                    operation.id.as_slice(),
                    i64::try_from(ordinal).map_err(|_| CatalogError::InvalidRecord)?,
                    link.source_kind.tag(),
                    link.source_id.as_slice(),
                    link.relation.tag(),
                    link.target_kind.tag(),
                    link.target_id.as_slice(),
                ],
            )?;
        }
        let previous = last_audit(&transaction)?;
        let sequence = previous.as_ref().map_or(Ok(1_u64), |value| {
            value.0.checked_add(1).ok_or(CatalogError::InvalidRecord)
        })?;
        let previous_id = previous.map(|value| value.1);
        let audit_id = audit_digest(
            sequence,
            previous_id,
            operation.id,
            operation.kind,
            &operation.actor,
            operation.business_at,
            appended_at,
        );
        transaction.execute(
            "INSERT INTO fair_value_audit
             (sequence, audit_id, previous_audit_id, operation_id, operation_kind, actor,
              business_at_ns, appended_at_ns, audit_sha256)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            params![
                i64::try_from(sequence).map_err(|_| CatalogError::InvalidRecord)?,
                audit_id.as_slice(),
                previous_id.as_ref().map(<[u8; 32]>::as_slice),
                operation.id.as_slice(),
                operation.kind.tag(),
                operation.actor.as_ref(),
                operation.business_at.unix_nanos(),
                appended_at.unix_nanos(),
                audit_id.as_slice(),
            ],
        )?;
        let position = catalog_position(&transaction)?;
        let counts = position.counts();
        transaction.commit()?;
        Ok(FairValueCatalogCommit {
            disposition: FairValueCommitDisposition::Inserted,
            audit_sequence: sequence,
            audit_id,
            appended_at,
            record_count: counts.records,
            operation_count: counts.operations,
            membership_count: counts.memberships,
            link_count: counts.links,
            position,
        })
    }

    /// Reads and validates a complete bounded fair-value recovery snapshot.
    pub fn fair_value_snapshot(
        &self,
        limits: FairValueCatalogSnapshotLimits,
    ) -> Result<FairValueCatalogSnapshot, CatalogError> {
        let transaction =
            Transaction::new_unchecked(&self.catalog().connection, TransactionBehavior::Deferred)?;
        let record_count: i64 = transaction.query_row(
            "SELECT COUNT(*) FROM fair_value_record_identities",
            [],
            |row| row.get(0),
        )?;
        let audit_count: i64 =
            transaction.query_row("SELECT COUNT(*) FROM fair_value_audit", [], |row| {
                row.get(0)
            })?;
        let operation_count: i64 =
            transaction.query_row("SELECT COUNT(*) FROM fair_value_operations", [], |row| {
                row.get(0)
            })?;
        let membership_count: i64 = transaction.query_row(
            "SELECT COUNT(*) FROM fair_value_operation_records",
            [],
            |row| row.get(0),
        )?;
        let link_count: i64 =
            transaction.query_row("SELECT COUNT(*) FROM fair_value_record_links", [], |row| {
                row.get(0)
            })?;
        if count_exceeds(record_count, limits.max_records)
            || count_exceeds(operation_count, limits.max_operations)
            || count_exceeds(membership_count, limits.max_memberships)
            || count_exceeds(link_count, limits.max_links)
            || operation_count != audit_count
        {
            return Err(CatalogError::AnalyticalEvidenceLimitExceeded);
        }
        let mut budget = ResultBudget::new(self.catalog().result_bytes);
        budget.charge_many(
            usize::try_from(membership_count).map_err(|_| CatalogError::CorruptCatalog)?,
            MEMBERSHIP_DECODED_BYTES,
        )?;
        budget.charge_many(
            usize::try_from(link_count).map_err(|_| CatalogError::CorruptCatalog)?,
            LINK_DECODED_BYTES,
        )?;
        let mut records = Vec::new();
        records
            .try_reserve_exact(usize::try_from(record_count).map_err(|_| CatalogError::Allocation)?)
            .map_err(|_| CatalogError::Allocation)?;
        let mut identities = transaction.prepare(
            "SELECT record_kind, record_id, payload_sha256
             FROM fair_value_record_identities ORDER BY record_kind, record_id",
        )?;
        let rows = identities.query_map([], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, Vec<u8>>(1)?,
                row.get::<_, Vec<u8>>(2)?,
            ))
        })?;
        for row in rows {
            let (kind, id, digest) = row?;
            let kind = FairValueRecordKind::from_tag(kind).ok_or(CatalogError::CorruptCatalog)?;
            let id = fixed_digest(id)?;
            let expected_digest = fixed_digest(digest)?;
            let payload = read_payload(&transaction, kind, id)?;
            budget.charge([payload.len(), 96])?;
            let record = FairValueCatalogRecord::try_new(kind, id, payload)?;
            if record.payload_sha256 != expected_digest {
                return Err(CatalogError::CorruptCatalog);
            }
            records.push(record);
        }
        drop(identities);
        let audit = read_and_validate_audit(
            &transaction,
            usize::try_from(audit_count).map_err(|_| CatalogError::CorruptCatalog)?,
            &mut budget,
        )?;
        let position = FairValueCatalogPosition {
            records: usize::try_from(record_count).map_err(|_| CatalogError::CorruptCatalog)?,
            operations: usize::try_from(operation_count)
                .map_err(|_| CatalogError::CorruptCatalog)?,
            memberships: usize::try_from(membership_count)
                .map_err(|_| CatalogError::CorruptCatalog)?,
            links: usize::try_from(link_count).map_err(|_| CatalogError::CorruptCatalog)?,
            last_audit_sequence: audit.last().map_or(0, FairValueCatalogAuditEvent::sequence),
            last_audit_id: audit.last().map(FairValueCatalogAuditEvent::id),
        };
        let snapshot = FairValueCatalogSnapshot {
            records: records.into_boxed_slice(),
            audit: audit.into_boxed_slice(),
            membership_count: usize::try_from(membership_count)
                .map_err(|_| CatalogError::CorruptCatalog)?,
            link_count: usize::try_from(link_count).map_err(|_| CatalogError::CorruptCatalog)?,
            position,
        };
        transaction.commit()?;
        Ok(snapshot)
    }
}

fn append_record(
    transaction: &Transaction<'_>,
    record: &FairValueCatalogRecord,
    appended_at: Timestamp,
) -> Result<(), CatalogError> {
    transaction.execute(
        "INSERT OR IGNORE INTO fair_value_record_identities
         (record_kind, record_id, payload_sha256, appended_at_ns) VALUES (?1, ?2, ?3, ?4)",
        params![
            record.kind.tag(),
            record.id.as_slice(),
            record.payload_sha256.as_slice(),
            appended_at.unix_nanos(),
        ],
    )?;
    let retained: Vec<u8> = transaction.query_row(
        "SELECT payload_sha256 FROM fair_value_record_identities
         WHERE record_kind=?1 AND record_id=?2",
        params![record.kind.tag(), record.id.as_slice()],
        |row| row.get(0),
    )?;
    if retained.as_slice() != record.payload_sha256 {
        return Err(CatalogError::EvidenceConflict);
    }
    let sql = format!(
        "INSERT OR IGNORE INTO {} (record_kind, record_id, payload) VALUES (?1, ?2, ?3)",
        record.kind.table()
    );
    transaction.execute(
        &sql,
        params![
            record.kind.tag(),
            record.id.as_slice(),
            record.payload.as_ref()
        ],
    )?;
    let retained = read_payload(transaction, record.kind, record.id)?;
    if retained.as_slice() != record.payload.as_ref() || sha256(&retained) != record.payload_sha256
    {
        return Err(CatalogError::EvidenceConflict);
    }
    Ok(())
}

fn replay_commit(
    transaction: &Transaction<'_>,
    operation: &FairValueCatalogOperation,
    limits: FairValueCatalogSnapshotLimits,
) -> Result<Option<FairValueCatalogCommit>, CatalogError> {
    let existing = transaction
        .query_row(
            "SELECT operation_kind, actor, business_at_ns, operation_sha256, appended_at_ns
             FROM fair_value_operations WHERE operation_id=?1",
            [operation.id.as_slice()],
            |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, Vec<u8>>(3)?,
                    row.get::<_, i64>(4)?,
                ))
            },
        )
        .optional()?;
    let Some((kind, actor, business_at, digest, appended_at)) = existing else {
        return Ok(None);
    };
    if FairValueOperationKind::from_tag(kind) != Some(operation.kind)
        || actor != operation.actor.as_ref()
        || business_at != operation.business_at.unix_nanos()
        || digest.as_slice() != operation.id
    {
        return Err(CatalogError::IdempotencyConflict);
    }
    verify_operation_members(transaction, operation)?;
    let (
        sequence,
        audit_id,
        previous_id,
        audit_kind,
        audit_actor,
        audit_business,
        audit_appended,
        audit_sha256,
    ): StoredAuditRow = transaction.query_row(
        "SELECT sequence, audit_id, previous_audit_id, operation_kind, actor, business_at_ns,
                appended_at_ns, audit_sha256
         FROM fair_value_audit WHERE operation_id=?1",
        [operation.id.as_slice()],
        |row| {
            Ok((
                row.get(0)?,
                row.get(1)?,
                row.get(2)?,
                row.get(3)?,
                row.get(4)?,
                row.get(5)?,
                row.get(6)?,
                row.get(7)?,
            ))
        },
    )?;
    let sequence_u64 = u64::try_from(sequence).map_err(|_| CatalogError::CorruptCatalog)?;
    let audit_id = fixed_digest(audit_id)?;
    let previous_id = previous_id.map(fixed_digest).transpose()?;
    let expected_previous = if sequence_u64 == 1 {
        None
    } else {
        transaction
            .query_row(
                "SELECT audit_id FROM fair_value_audit WHERE sequence=?1",
                [sequence
                    .checked_sub(1)
                    .ok_or(CatalogError::CorruptCatalog)?],
                |row| row.get::<_, Vec<u8>>(0),
            )
            .optional()?
            .map(fixed_digest)
            .transpose()?
    };
    let audit_appended = Timestamp::from_unix_nanos(audit_appended);
    if previous_id != expected_previous
        || FairValueOperationKind::from_tag(audit_kind) != Some(operation.kind)
        || audit_actor != operation.actor.as_ref()
        || audit_business != operation.business_at.unix_nanos()
        || audit_appended != Timestamp::from_unix_nanos(appended_at)
        || audit_id != fixed_digest(audit_sha256)?
        || audit_id
            != audit_digest(
                sequence_u64,
                previous_id,
                operation.id,
                operation.kind,
                &operation.actor,
                operation.business_at,
                audit_appended,
            )
        || validate_stored_operation(
            transaction,
            operation.id,
            operation.kind,
            &operation.actor,
            operation.business_at,
        )?
        .0 != audit_appended
    {
        return Err(CatalogError::CorruptCatalog);
    }
    let position = catalog_position(transaction)?;
    let counts = position.counts();
    ensure_counts_within_limits(counts, limits)?;
    Ok(Some(FairValueCatalogCommit {
        disposition: FairValueCommitDisposition::Replay,
        audit_sequence: sequence_u64,
        audit_id,
        appended_at: audit_appended,
        record_count: counts.records,
        operation_count: counts.operations,
        membership_count: counts.memberships,
        link_count: counts.links,
        position,
    }))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct CatalogCounts {
    records: usize,
    operations: usize,
    memberships: usize,
    links: usize,
}

impl FairValueCatalogPosition {
    const fn counts(self) -> CatalogCounts {
        CatalogCounts {
            records: self.records,
            operations: self.operations,
            memberships: self.memberships,
            links: self.links,
        }
    }
}

fn catalog_counts(transaction: &Transaction<'_>) -> Result<CatalogCounts, CatalogError> {
    Ok(CatalogCounts {
        records: table_count(transaction, "fair_value_record_identities")?,
        operations: table_count(transaction, "fair_value_operations")?,
        memberships: table_count(transaction, "fair_value_operation_records")?,
        links: table_count(transaction, "fair_value_record_links")?,
    })
}

fn catalog_position(
    transaction: &Transaction<'_>,
) -> Result<FairValueCatalogPosition, CatalogError> {
    let counts = catalog_counts(transaction)?;
    let latest = last_audit(transaction)?;
    Ok(FairValueCatalogPosition {
        records: counts.records,
        operations: counts.operations,
        memberships: counts.memberships,
        links: counts.links,
        last_audit_sequence: latest.map_or(0, |value| value.0),
        last_audit_id: latest.map(|value| value.1),
    })
}

fn table_count(transaction: &Transaction<'_>, table: &str) -> Result<usize, CatalogError> {
    let sql = format!("SELECT COUNT(*) FROM {table}");
    let count: i64 = transaction.query_row(&sql, [], |row| row.get(0))?;
    usize::try_from(count).map_err(|_| CatalogError::CorruptCatalog)
}

fn enforce_append_capacity(
    transaction: &Transaction<'_>,
    operation: &FairValueCatalogOperation,
    limits: FairValueCatalogSnapshotLimits,
) -> Result<(), CatalogError> {
    let counts = catalog_counts(transaction)?;
    let mut new_records = 0_usize;
    for record in &operation.records {
        let exists = transaction
            .query_row(
                "SELECT 1 FROM fair_value_record_identities
                 WHERE record_kind=?1 AND record_id=?2",
                params![record.kind.tag(), record.id.as_slice()],
                |_| Ok(()),
            )
            .optional()?
            .is_some();
        if !exists {
            new_records = new_records
                .checked_add(1)
                .ok_or(CatalogError::InvalidRecord)?;
        }
    }
    let projected = CatalogCounts {
        records: counts
            .records
            .checked_add(new_records)
            .ok_or(CatalogError::InvalidRecord)?,
        operations: counts
            .operations
            .checked_add(1)
            .ok_or(CatalogError::InvalidRecord)?,
        memberships: counts
            .memberships
            .checked_add(operation.records.len())
            .ok_or(CatalogError::InvalidRecord)?,
        links: counts
            .links
            .checked_add(operation.links.len())
            .ok_or(CatalogError::InvalidRecord)?,
    };
    ensure_counts_within_limits(projected, limits)
}

fn ensure_counts_within_limits(
    counts: CatalogCounts,
    limits: FairValueCatalogSnapshotLimits,
) -> Result<(), CatalogError> {
    if counts.records > limits.max_records
        || counts.operations > limits.max_operations
        || counts.memberships > limits.max_memberships
        || counts.links > limits.max_links
    {
        Err(CatalogError::AnalyticalEvidenceLimitExceeded)
    } else {
        Ok(())
    }
}

fn verify_operation_members(
    transaction: &Transaction<'_>,
    operation: &FairValueCatalogOperation,
) -> Result<(), CatalogError> {
    let mut records = transaction.prepare(
        "SELECT r.ordinal, r.record_kind, r.record_id, i.payload_sha256
         FROM fair_value_operation_records r
         JOIN fair_value_record_identities i
           ON i.record_kind=r.record_kind AND i.record_id=r.record_id
         WHERE r.operation_id=?1 ORDER BY r.ordinal",
    )?;
    let mut rows = records.query([operation.id.as_slice()])?;
    for (ordinal, expected) in operation.records.iter().enumerate() {
        let row = rows.next()?.ok_or(CatalogError::CorruptCatalog)?;
        if row.get::<_, i64>(0)?
            != i64::try_from(ordinal).map_err(|_| CatalogError::CorruptCatalog)?
            || FairValueRecordKind::from_tag(row.get(1)?) != Some(expected.kind)
            || fixed_digest(row.get(2)?)? != expected.id
            || fixed_digest(row.get(3)?)? != expected.payload_sha256
        {
            return Err(CatalogError::CorruptCatalog);
        }
    }
    if rows.next()?.is_some() {
        return Err(CatalogError::CorruptCatalog);
    }
    drop(rows);
    drop(records);

    let mut links = transaction.prepare(
        "SELECT ordinal, source_kind, source_id, relation_kind, target_kind, target_id
         FROM fair_value_record_links WHERE operation_id=?1 ORDER BY ordinal",
    )?;
    let mut rows = links.query([operation.id.as_slice()])?;
    for (ordinal, expected) in operation.links.iter().enumerate() {
        let row = rows.next()?.ok_or(CatalogError::CorruptCatalog)?;
        if row.get::<_, i64>(0)?
            != i64::try_from(ordinal).map_err(|_| CatalogError::CorruptCatalog)?
            || FairValueRecordKind::from_tag(row.get(1)?) != Some(expected.source_kind)
            || fixed_digest(row.get(2)?)? != expected.source_id
            || FairValueLinkRelation::from_tag(row.get(3)?) != Some(expected.relation)
            || FairValueRecordKind::from_tag(row.get(4)?) != Some(expected.target_kind)
            || fixed_digest(row.get(5)?)? != expected.target_id
        {
            return Err(CatalogError::CorruptCatalog);
        }
    }
    if rows.next()?.is_some() {
        return Err(CatalogError::CorruptCatalog);
    }
    Ok(())
}
