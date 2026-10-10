//! Whole-publication placement and bounded commit-order archive progress.

use super::*;
use crate::{DatasetSchemaRegistry, ParquetObjectStore, ParquetStoreError, PublishedObject};
use arrow::array::Array as _;

pub(crate) struct MarketEventArchivePlan {
    pub(crate) dataset: DatasetId,
    pub(crate) previous_sequence: u64,
    pub(crate) commits: Vec<MarketEventCommitRef>,
    pub(crate) rows: u64,
}

/// Visits one dataset in keyset order; a below-target prefix returns an empty work plan.
pub(crate) fn plan_market_event_archive(
    connection: &Connection,
    after: Option<&DatasetId>,
    maximum_publications: usize,
    target_bytes: usize,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<Option<MarketEventArchivePlan>, CatalogError> {
    check_read(deadline, cancellation)?;
    let selected: Option<(String, i64)> = connection
        .query_row(
            "SELECT head.dataset_id,COALESCE(progress.archived_sequence,0)
         FROM market_event_storage_heads AS head
         LEFT JOIN market_event_archive_progress AS progress USING(dataset_id)
         WHERE (?1 IS NULL OR head.dataset_id>?1)
           AND head.committed_sequence>COALESCE(progress.archived_sequence,0)
         ORDER BY head.dataset_id LIMIT 1",
            [after.map(DatasetId::as_str)],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    let Some((dataset, previous)) = selected else {
        return Ok(None);
    };
    let dataset =
        DatasetId::try_from(dataset.as_str()).map_err(|_| CatalogError::CorruptCatalog)?;
    let previous_sequence = u64::try_from(previous).map_err(|_| CatalogError::CorruptCatalog)?;
    let mut statement = connection.prepare(
        "SELECT committed.commit_sequence,
           (SELECT SUM(length(active.event_json)) FROM market_event_active_rows AS active
            WHERE active.publication_digest=committed.publication_digest)
         FROM market_event_commits AS committed
         WHERE committed.dataset_id=?1 AND committed.commit_sequence>?2
         ORDER BY committed.commit_sequence LIMIT ?3",
    )?;
    let mut candidates = statement.query(params![
        dataset.as_str(),
        previous,
        i64::try_from(maximum_publications).map_err(|_| CatalogError::InvalidConfiguration)?
    ])?;
    let mut commits = Vec::new();
    let mut bytes = 0usize;
    let mut total_rows = 0u64;
    while let Some(row) = candidates.next()? {
        check_read(deadline, cancellation)?;
        let sequence =
            u64::try_from(row.get::<_, i64>(0)?).map_err(|_| CatalogError::CorruptCatalog)?;
        if sequence
            != previous_sequence
                .checked_add(commits.len() as u64 + 1)
                .ok_or(CatalogError::CorruptCatalog)?
        {
            return Err(CatalogError::CorruptCatalog);
        }
        let size =
            usize::try_from(row.get::<_, i64>(1)?).map_err(|_| CatalogError::CorruptCatalog)?;
        if size == 0 {
            return Err(CatalogError::CorruptCatalog);
        }
        let commit = load_market_event_commit(connection, &dataset, sequence)?
            .ok_or(CatalogError::CorruptCatalog)?;
        bytes = bytes
            .checked_add(size)
            .ok_or(CatalogError::ResultByteLimitExceeded)?;
        total_rows = total_rows
            .checked_add(commit.row_count())
            .ok_or(CatalogError::CorruptCatalog)?;
        commits
            .try_reserve(1)
            .map_err(|_| CatalogError::Allocation)?;
        commits.push(commit);
        // The last whole publication may cross the preferred size. It is never split/rejected.
        if bytes >= target_bytes {
            break;
        }
    }
    if bytes < target_bytes && commits.len() < maximum_publications {
        commits.clear();
        total_rows = 0;
    }
    Ok(Some(MarketEventArchivePlan {
        dataset,
        previous_sequence,
        commits,
        rows: total_rows,
    }))
}

impl Catalog {
    /// Publishes exact durable placement, advances progress and reclaims hot payloads atomically.
    pub(crate) fn commit_market_event_archive(
        &self,
        plan: &MarketEventArchivePlan,
        object: &PublishedObject,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<(), CatalogError> {
        check_read(deadline, cancellation)?;
        let first = plan.commits.first().ok_or(CatalogError::InvalidRecord)?;
        let last = plan.commits.last().ok_or(CatalogError::InvalidRecord)?;
        if object.row_count() != plan.rows || first.dataset_id() != &plan.dataset {
            return Err(CatalogError::ProviderEventMismatch);
        }
        let transaction = self.connection.unchecked_transaction()?;
        transaction.execute(
            "INSERT INTO market_event_archive_progress(dataset_id,archived_sequence)
            VALUES (?1,0) ON CONFLICT(dataset_id) DO NOTHING",
            [plan.dataset.as_str()],
        )?;
        let previous: i64 = transaction.query_row(
            "SELECT archived_sequence FROM market_event_archive_progress WHERE dataset_id=?1",
            [plan.dataset.as_str()],
            |row| row.get(0),
        )?;
        if u64::try_from(previous).ok() != Some(plan.previous_sequence) {
            return Err(CatalogError::ProviderEventConflict);
        }
        let published_at = trusted_catalog_now(&transaction)?;
        if published_at < object.created_at() {
            return Err(CatalogError::InvalidRecord);
        }
        transaction.execute("INSERT INTO market_event_archive_objects
            (content_digest,relative_reference,schema_name,schema_version,schema_fingerprint,
             size_bytes,row_count,created_at_ns,published_at_ns) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9)",
            params![object.content_hash().bytes().as_slice(),object.relative_reference(),first.schema().name(),
                i64::from(first.schema().version().get()),first.schema().fingerprint().as_slice(),
                i64::try_from(object.size_bytes()).map_err(|_|CatalogError::InvalidRecord)?,
                i64::try_from(object.row_count()).map_err(|_|CatalogError::InvalidRecord)?,
                object.created_at().unix_nanos(),published_at.unix_nanos()])?;
        let mut offset = 0u64;
        for (ordinal, commit) in plan.commits.iter().enumerate() {
            check_read(deadline, cancellation)?;
            if commit.dataset_id() != &plan.dataset
                || commit.schema() != first.schema()
                || commit.sequence()
                    != plan
                        .previous_sequence
                        .checked_add(ordinal as u64 + 1)
                        .ok_or(CatalogError::CorruptCatalog)?
                || load_market_event_commit(&transaction, &plan.dataset, commit.sequence())?
                    .as_ref()
                    != Some(commit)
            {
                return Err(CatalogError::ProviderEventConflict);
            }
            transaction.execute("INSERT INTO market_event_archive_memberships
                (publication_digest,dataset_id,commit_sequence,object_content_digest,first_row,row_count)
                VALUES (?1,?2,?3,?4,?5,?6)",params![commit.publication_digest().bytes().as_slice(),
                    plan.dataset.as_str(),i64::try_from(commit.sequence()).map_err(|_|CatalogError::InvalidRecord)?,
                    object.content_hash().bytes().as_slice(),i64::try_from(offset).map_err(|_|CatalogError::InvalidRecord)?,
                    i64::try_from(commit.row_count()).map_err(|_|CatalogError::InvalidRecord)?])?;
            offset = offset
                .checked_add(commit.row_count())
                .ok_or(CatalogError::CorruptCatalog)?;
        }
        if offset != object.row_count() {
            return Err(CatalogError::ProviderEventMismatch);
        }
        if transaction.execute(
            "UPDATE market_event_archive_progress SET archived_sequence=?1
            WHERE dataset_id=?2 AND archived_sequence=?3",
            params![
                i64::try_from(last.sequence()).map_err(|_| CatalogError::InvalidRecord)?,
                plan.dataset.as_str(),
                previous
            ],
        )? != 1
        {
            return Err(CatalogError::ProviderEventConflict);
        }
        for commit in &plan.commits {
            check_read(deadline, cancellation)?;
            let removed = transaction.execute(
                "DELETE FROM market_event_active_rows WHERE publication_digest=?1",
                [commit.publication_digest().bytes().as_slice()],
            )?;
            if removed as u64 != commit.row_count() {
                return Err(CatalogError::ProviderEventConflict);
            }
        }
        append_audit(
            &transaction,
            "market-event.archived",
            plan.dataset.as_str(),
            object.content_hash().bytes(),
            published_at,
        )?;
        check_read(deadline, cancellation)?;
        transaction.commit()?;
        Ok(())
    }
}

/// Returns None only when this exact snapshot has no cold placement.
pub(super) fn load_archived_payloads(
    connection: &Connection,
    commit: &MarketEventCommitRef,
    objects: &ParquetObjectStore,
    limits: CatalogResultLimits,
    deadline: Option<Instant>,
    cancellation: &CancellationToken,
) -> Result<Option<Vec<Vec<u8>>>, CatalogError> {
    let placement = connection.query_row(
        "SELECT object.relative_reference,object.content_digest,object.size_bytes,object.row_count,
                object.created_at_ns,member.first_row,member.row_count,
                object.schema_name,object.schema_version,object.schema_fingerprint
         FROM market_event_archive_memberships AS member
         JOIN market_event_archive_objects AS object ON object.content_digest=member.object_content_digest
         WHERE member.publication_digest=?1 AND member.dataset_id=?2 AND member.commit_sequence=?3",
        params![commit.publication_digest().bytes().as_slice(),commit.dataset_id().as_str(),
            i64::try_from(commit.sequence()).map_err(|_|CatalogError::CorruptCatalog)?],
        |row|Ok((row.get::<_,String>(0)?,row.get::<_,Vec<u8>>(1)?,row.get::<_,i64>(2)?,row.get::<_,i64>(3)?,
            row.get::<_,i64>(4)?,row.get::<_,i64>(5)?,row.get::<_,i64>(6)?,row.get::<_,String>(7)?,
            row.get::<_,i64>(8)?,row.get::<_,Vec<u8>>(9)?)),
    ).optional()?;
    let Some((reference, hash, bytes, rows, created, first, count, name, version, fingerprint)) =
        placement
    else {
        return Ok(None);
    };
    if u64::try_from(count).ok() != Some(commit.row_count())
        || name != commit.schema().name()
        || version != i64::from(commit.schema().version().get())
        || fingerprint != commit.schema().fingerprint()
    {
        return Err(CatalogError::CorruptCatalog);
    }
    let object = PublishedObject::try_from_catalog_parts(
        reference,
        parse_sha256(&hash)?,
        u64::try_from(bytes).map_err(|_| CatalogError::CorruptCatalog)?,
        u64::try_from(rows).map_err(|_| CatalogError::CorruptCatalog)?,
        Timestamp::from_unix_nanos(created),
    )
    .map_err(map_archive_error)?;
    let first = u64::try_from(first).map_err(|_| CatalogError::CorruptCatalog)?;
    let schema = DatasetSchemaRegistry::local()
        .resolve(commit.schema())
        .map_err(|_| CatalogError::CorruptCatalog)?;
    let column = schema
        .index_of("event_json")
        .map_err(|_| CatalogError::CorruptCatalog)?;
    let count = usize::try_from(count).map_err(|_| CatalogError::ResultRowLimitExceeded)?;
    let mut budget = ResultBudget::new(limits);
    budget.charge_many(count, std::mem::size_of::<Vec<u8>>())?;
    let mut output = Vec::new();
    output
        .try_reserve_exact(count)
        .map_err(|_| CatalogError::Allocation)?;
    let mut statement = connection.prepare(
        "SELECT publication_row_ordinal,canonical_event_digest
        FROM provider_market_event_selection_index WHERE publication_digest=?1 AND dataset_id=?2
          AND commit_sequence=?3 ORDER BY publication_row_ordinal",
    )?;
    let mut indexed = statement.query(params![
        commit.publication_digest().bytes().as_slice(),
        commit.dataset_id().as_str(),
        i64::try_from(commit.sequence()).map_err(|_| CatalogError::CorruptCatalog)?
    ])?;
    let mut callback_error = None;
    let result = objects.read_published_row_range(
        &object,
        schema,
        first,
        commit.row_count(),
        vec![column],
        crate::ingest::MAX_EVENT_PUBLICATION_READ_BYTES,
        deadline,
        cancellation,
        |batch| {
            let checked = (|| -> Result<(), CatalogError> {
                let payloads = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<BinaryArray>()
                    .ok_or(CatalogError::CorruptCatalog)?;
                for index in 0..payloads.len() {
                    check_cancelled(cancellation)?;
                    if let Some(deadline) = deadline {
                        check_read(deadline, cancellation)?;
                    }
                    if payloads.is_null(index) || output.len() >= count {
                        return Err(CatalogError::CorruptCatalog);
                    }
                    let row = indexed.next()?.ok_or(CatalogError::CorruptCatalog)?;
                    let digest: Vec<u8> = row.get(1)?;
                    let payload = payloads.value(index);
                    if row.get::<_, i64>(0)? != output.len() as i64
                        || sha256(payload).as_slice() != digest
                    {
                        return Err(CatalogError::CorruptCatalog);
                    }
                    budget.charge([payload.len()])?;
                    let mut copied = Vec::new();
                    copied
                        .try_reserve_exact(payload.len())
                        .map_err(|_| CatalogError::Allocation)?;
                    copied.extend_from_slice(payload);
                    output.push(copied);
                }
                Ok(())
            })();
            match checked {
                Ok(()) => Ok(()),
                Err(error) => {
                    callback_error = Some(error);
                    Err(ParquetStoreError::ObjectMetadataMismatch)
                }
            }
        },
    );
    if let Some(error) = callback_error {
        return Err(error);
    }
    result.map_err(map_archive_error)?;
    if output.len() != count || indexed.next()?.is_some() {
        return Err(CatalogError::CorruptCatalog);
    }
    Ok(Some(output))
}

fn map_archive_error(error: ParquetStoreError) -> CatalogError {
    match error {
        ParquetStoreError::Cancelled => CatalogError::MarketRecoveryReadCancelled,
        ParquetStoreError::RecoveryDeadlineExceeded => {
            CatalogError::MarketRecoveryReadDeadlineExceeded
        }
        ParquetStoreError::ReadLimitExceeded => CatalogError::ResultByteLimitExceeded,
        _ => CatalogError::CorruptCatalog,
    }
}
