//! Bounded recovery decoding and immutable catalog validation.

use super::hash::{audit_digest, fixed_digest, hash_bytes};
use super::*;

pub(super) fn read_payload(
    connection: &rusqlite::Connection,
    kind: FairValueRecordKind,
    id: [u8; 32],
) -> Result<Vec<u8>, CatalogError> {
    let sql = format!("SELECT payload FROM {} WHERE record_id=?1", kind.table());
    connection
        .query_row(&sql, [id.as_slice()], |row| row.get(0))
        .map_err(Into::into)
}

pub(super) fn read_and_validate_audit(
    connection: &rusqlite::Connection,
    capacity: usize,
    budget: &mut ResultBudget,
) -> Result<Vec<FairValueCatalogAuditEvent>, CatalogError> {
    let mut events = Vec::new();
    events
        .try_reserve_exact(capacity)
        .map_err(|_| CatalogError::Allocation)?;
    let mut statement = connection.prepare(
        "SELECT sequence, audit_id, previous_audit_id, operation_id, operation_kind, actor,
                business_at_ns, appended_at_ns, audit_sha256
         FROM fair_value_audit ORDER BY sequence",
    )?;
    let rows = statement.query_map([], |row| {
        Ok((
            row.get::<_, i64>(0)?,
            row.get::<_, Vec<u8>>(1)?,
            row.get::<_, Option<Vec<u8>>>(2)?,
            row.get::<_, Vec<u8>>(3)?,
            row.get::<_, i64>(4)?,
            row.get::<_, String>(5)?,
            row.get::<_, i64>(6)?,
            row.get::<_, i64>(7)?,
            row.get::<_, Vec<u8>>(8)?,
        ))
    })?;
    for row in rows {
        let (sequence, id, previous, operation_id, kind, actor, business, appended, digest) = row?;
        let sequence = u64::try_from(sequence).map_err(|_| CatalogError::CorruptCatalog)?;
        let id = fixed_digest(id)?;
        let previous_id = previous.map(fixed_digest).transpose()?;
        let operation_id = fixed_digest(operation_id)?;
        let kind = FairValueOperationKind::from_tag(kind).ok_or(CatalogError::CorruptCatalog)?;
        let business_at = Timestamp::from_unix_nanos(business);
        let appended_at = Timestamp::from_unix_nanos(appended);
        budget.charge([AUDIT_DECODED_FIXED_BYTES, actor.len()])?;
        if sequence != u64::try_from(events.len()).map_err(|_| CatalogError::CorruptCatalog)? + 1
            || previous_id != events.last().map(FairValueCatalogAuditEvent::id)
            || events
                .last()
                .is_some_and(|previous| previous.appended_at() > appended_at)
            || id != fixed_digest(digest)?
            || id
                != audit_digest(
                    sequence,
                    previous_id,
                    operation_id,
                    kind,
                    &actor,
                    business_at,
                    appended_at,
                )
        {
            return Err(CatalogError::CorruptCatalog);
        }
        let (operation_appended, operation_record_count, operation_link_count) =
            validate_stored_operation(connection, operation_id, kind, &actor, business_at)?;
        if operation_appended != appended_at {
            return Err(CatalogError::CorruptCatalog);
        }
        let records =
            read_operation_record_identities(connection, operation_id, operation_record_count)?;
        let links = read_operation_links(connection, operation_id, operation_link_count)?;
        events.push(FairValueCatalogAuditEvent {
            sequence,
            id,
            previous_id,
            operation_id,
            kind,
            actor: actor.into(),
            business_at,
            appended_at,
            records: records.into_boxed_slice(),
            links: links.into_boxed_slice(),
        });
    }
    Ok(events)
}

fn read_operation_record_identities(
    connection: &rusqlite::Connection,
    operation_id: [u8; 32],
    expected_count: usize,
) -> Result<Vec<(FairValueRecordKind, [u8; 32])>, CatalogError> {
    let mut statement = connection.prepare(
        "SELECT ordinal, record_kind, record_id FROM fair_value_operation_records
         WHERE operation_id=?1 ORDER BY ordinal",
    )?;
    let mut rows = statement.query([operation_id.as_slice()])?;
    let mut values = Vec::new();
    values
        .try_reserve_exact(expected_count)
        .map_err(|_| CatalogError::Allocation)?;
    while let Some(row) = rows.next()? {
        let ordinal =
            usize::try_from(row.get::<_, i64>(0)?).map_err(|_| CatalogError::CorruptCatalog)?;
        if ordinal != values.len() || values.len() >= MAX_OPERATION_RECORDS {
            return Err(CatalogError::CorruptCatalog);
        }
        values.push((
            FairValueRecordKind::from_tag(row.get(1)?).ok_or(CatalogError::CorruptCatalog)?,
            fixed_digest(row.get(2)?)?,
        ));
    }
    if values.is_empty() || values.len() != expected_count {
        return Err(CatalogError::CorruptCatalog);
    }
    Ok(values)
}

fn read_operation_links(
    connection: &rusqlite::Connection,
    operation_id: [u8; 32],
    expected_count: usize,
) -> Result<Vec<FairValueCatalogLink>, CatalogError> {
    let mut statement = connection.prepare(
        "SELECT ordinal, source_kind, source_id, relation_kind, target_kind, target_id
         FROM fair_value_record_links WHERE operation_id=?1 ORDER BY ordinal",
    )?;
    let mut rows = statement.query([operation_id.as_slice()])?;
    let mut values = Vec::new();
    values
        .try_reserve_exact(expected_count)
        .map_err(|_| CatalogError::Allocation)?;
    while let Some(row) = rows.next()? {
        let ordinal =
            usize::try_from(row.get::<_, i64>(0)?).map_err(|_| CatalogError::CorruptCatalog)?;
        if ordinal != values.len() || values.len() >= MAX_OPERATION_LINKS {
            return Err(CatalogError::CorruptCatalog);
        }
        values.push(FairValueCatalogLink::try_new(
            FairValueRecordKind::from_tag(row.get(1)?).ok_or(CatalogError::CorruptCatalog)?,
            fixed_digest(row.get(2)?)?,
            FairValueLinkRelation::from_tag(row.get(3)?).ok_or(CatalogError::CorruptCatalog)?,
            FairValueRecordKind::from_tag(row.get(4)?).ok_or(CatalogError::CorruptCatalog)?,
            fixed_digest(row.get(5)?)?,
        )?);
    }
    if values.len() != expected_count {
        return Err(CatalogError::CorruptCatalog);
    }
    Ok(values)
}

pub(super) fn validate_stored_operation(
    connection: &rusqlite::Connection,
    operation_id: [u8; 32],
    expected_kind: FairValueOperationKind,
    expected_actor: &str,
    expected_business_at: Timestamp,
) -> Result<(Timestamp, usize, usize), CatalogError> {
    let (kind, actor, business, operation_digest, appended): (i64, String, i64, Vec<u8>, i64) =
        connection.query_row(
            "SELECT operation_kind, actor, business_at_ns, operation_sha256, appended_at_ns
         FROM fair_value_operations
         WHERE operation_id=?1",
            [operation_id.as_slice()],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                ))
            },
        )?;
    if FairValueOperationKind::from_tag(kind) != Some(expected_kind)
        || actor != expected_actor
        || business != expected_business_at.unix_nanos()
        || fixed_digest(operation_digest)? != operation_id
    {
        return Err(CatalogError::CorruptCatalog);
    }
    let record_count: i64 = connection.query_row(
        "SELECT COUNT(*) FROM fair_value_operation_records WHERE operation_id=?1",
        [operation_id.as_slice()],
        |row| row.get(0),
    )?;
    let link_count: i64 = connection.query_row(
        "SELECT COUNT(*) FROM fair_value_record_links WHERE operation_id=?1",
        [operation_id.as_slice()],
        |row| row.get(0),
    )?;
    let record_count = usize::try_from(record_count).map_err(|_| CatalogError::CorruptCatalog)?;
    let link_count = usize::try_from(link_count).map_err(|_| CatalogError::CorruptCatalog)?;
    if record_count == 0 || record_count > MAX_OPERATION_RECORDS || link_count > MAX_OPERATION_LINKS
    {
        return Err(CatalogError::CorruptCatalog);
    }
    let mut hash = Sha256::new();
    hash.update(b"market-squawk/fair-value-catalog-operation/v1");
    hash.update(expected_kind.tag().to_be_bytes());
    hash_bytes(&mut hash, expected_actor.as_bytes())?;
    hash.update(expected_business_at.unix_nanos().to_be_bytes());
    hash.update(
        u64::try_from(record_count)
            .map_err(|_| CatalogError::CorruptCatalog)?
            .to_be_bytes(),
    );
    let mut statement = connection.prepare(
        "SELECT r.ordinal, r.record_kind, r.record_id, i.payload_sha256
         FROM fair_value_operation_records r
         JOIN fair_value_record_identities i
           ON i.record_kind=r.record_kind AND i.record_id=r.record_id
         WHERE r.operation_id=?1 ORDER BY r.ordinal",
    )?;
    let mut rows = statement.query([operation_id.as_slice()])?;
    for ordinal in 0..record_count {
        let row = rows.next()?.ok_or(CatalogError::CorruptCatalog)?;
        if row.get::<_, i64>(0)?
            != i64::try_from(ordinal).map_err(|_| CatalogError::CorruptCatalog)?
        {
            return Err(CatalogError::CorruptCatalog);
        }
        let kind =
            FairValueRecordKind::from_tag(row.get(1)?).ok_or(CatalogError::CorruptCatalog)?;
        hash.update(kind.tag().to_be_bytes());
        hash.update(fixed_digest(row.get(2)?)?);
        hash.update(fixed_digest(row.get(3)?)?);
    }
    if rows.next()?.is_some() {
        return Err(CatalogError::CorruptCatalog);
    }
    drop(rows);
    drop(statement);
    hash.update(
        u64::try_from(link_count)
            .map_err(|_| CatalogError::CorruptCatalog)?
            .to_be_bytes(),
    );
    let mut statement = connection.prepare(
        "SELECT ordinal, source_kind, source_id, relation_kind, target_kind, target_id
         FROM fair_value_record_links WHERE operation_id=?1 ORDER BY ordinal",
    )?;
    let mut rows = statement.query([operation_id.as_slice()])?;
    for ordinal in 0..link_count {
        let row = rows.next()?.ok_or(CatalogError::CorruptCatalog)?;
        if row.get::<_, i64>(0)?
            != i64::try_from(ordinal).map_err(|_| CatalogError::CorruptCatalog)?
        {
            return Err(CatalogError::CorruptCatalog);
        }
        let source_kind =
            FairValueRecordKind::from_tag(row.get(1)?).ok_or(CatalogError::CorruptCatalog)?;
        let relation =
            FairValueLinkRelation::from_tag(row.get(3)?).ok_or(CatalogError::CorruptCatalog)?;
        let target_kind =
            FairValueRecordKind::from_tag(row.get(4)?).ok_or(CatalogError::CorruptCatalog)?;
        if relation.expected() != (source_kind, target_kind) {
            return Err(CatalogError::CorruptCatalog);
        }
        hash.update(source_kind.tag().to_be_bytes());
        hash.update(fixed_digest(row.get(2)?)?);
        hash.update(relation.tag().to_be_bytes());
        hash.update(target_kind.tag().to_be_bytes());
        hash.update(fixed_digest(row.get(5)?)?);
    }
    if rows.next()?.is_some() || <[u8; 32]>::from(hash.finalize()) != operation_id {
        return Err(CatalogError::CorruptCatalog);
    }
    Ok((
        Timestamp::from_unix_nanos(appended),
        record_count,
        link_count,
    ))
}

pub(super) fn last_audit(
    transaction: &Transaction<'_>,
) -> Result<Option<(u64, [u8; 32])>, CatalogError> {
    transaction
        .query_row(
            "SELECT sequence, audit_id FROM fair_value_audit ORDER BY sequence DESC LIMIT 1",
            [],
            |row| Ok((row.get::<_, i64>(0)?, row.get::<_, Vec<u8>>(1)?)),
        )
        .optional()?
        .map(|(sequence, id)| {
            Ok((
                u64::try_from(sequence).map_err(|_| CatalogError::CorruptCatalog)?,
                fixed_digest(id)?,
            ))
        })
        .transpose()
}
