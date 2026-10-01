//! Atomic canonical event microbatches and immutable logical publication horizons.

pub(crate) mod archive;

use std::time::Instant;

use arrow::array::{Array as _, BinaryArray};
use market_squawk_domain::{DigestAlgorithm, EvidenceDigest, SchemaVersion, Timestamp};
use rusqlite::{Connection, OptionalExtension as _, params};
use sha2::{Digest as _, Sha256};
use tokio_util::sync::CancellationToken;

use super::provider_event::{
    PreparedProviderPublicationBinding, load_provider_publication_for_run,
    provider_market_event_selection_for_publication, retain_prepared_provider_publication_binding,
};
use super::storage::{
    ResultBudget, append_audit, require_reserved_run, sha256, trusted_catalog_now,
};
use super::{Catalog, CatalogError, CatalogResultLimits, ContractCompletion, IngestReservation};
use crate::{
    DatasetId, DatasetSchemaRef, MarketEventCommitRef, ProviderMarketEventArrowBatch, Sha256Digest,
};

impl Catalog {
    /// Commits one admitted sealed event publication, or reopens its exact successful retry.
    pub(crate) fn commit_market_event_publication(
        &self,
        reservation: &IngestReservation,
        dataset: &DatasetId,
        prepared: &PreparedProviderPublicationBinding,
        batch: &ProviderMarketEventArrowBatch,
        objects: &crate::ParquetObjectStore,
        cancellation: &CancellationToken,
    ) -> Result<MarketEventCommitRef, CatalogError> {
        check_cancelled(cancellation)?;
        if reservation.catalog_id() != self.catalog_id {
            return Err(CatalogError::InvalidReservationCapability);
        }
        if batch.publication_digest() != prepared.publication_digest()
            || batch.publication_kind() != prepared.publication_kind_name()
        {
            return Err(CatalogError::ProviderEventMismatch);
        }
        let record_batch = batch.dataset_batch().record_batch();
        let payloads = record_batch
            .column_by_name("event_json")
            .and_then(|column| column.as_any().downcast_ref::<BinaryArray>())
            .ok_or(CatalogError::ProviderEventMismatch)?;
        let lineage = batch
            .lineage_digest()
            .map_err(|_| CatalogError::ProviderEventMismatch)?;
        let row_count =
            u64::try_from(batch.events().len()).map_err(|_| CatalogError::InvalidRecord)?;
        let transaction = self.connection.unchecked_transaction()?;
        let run = self
            .ingest_run(reservation.run_id())?
            .ok_or(CatalogError::RunStateConflict)?;
        if run.payload_digest() != prepared.publication_digest()
            || run.source_id().as_str() != prepared.source_id()
            || run.operation() != crate::SourceOperation::Persist
        {
            return Err(CatalogError::ProviderEventMismatch);
        }
        if run.state() == super::IngestRunState::Succeeded {
            let commit = load_market_event_commit_for_publication(
                &transaction,
                dataset,
                prepared.publication_digest(),
            )?
            .ok_or(CatalogError::ProviderEventConflict)?;
            let retained = load_provider_publication_for_run(&transaction, reservation.run_id())?
                .ok_or(CatalogError::ProviderEventConflict)?;
            let stored_lineage: Vec<u8> = transaction.query_row(
                "SELECT lineage_digest FROM market_event_commits WHERE run_id=?1",
                [reservation.run_id().to_string()],
                |row| row.get(0),
            )?;
            if !prepared.matches_persisted(&retained)
                || commit.schema() != batch.schema_ref()
                || commit.row_count() != row_count
                || stored_lineage != lineage.bytes()
            {
                return Err(CatalogError::ProviderEventConflict);
            }
            if let Some(rows) = archive::load_archived_payloads(
                &transaction,
                &commit,
                objects,
                self.result_bytes,
                None,
                cancellation,
            )? {
                if rows.len() != payloads.len()
                    || rows
                        .iter()
                        .enumerate()
                        .any(|(ordinal, row)| row.as_slice() != payloads.value(ordinal))
                {
                    return Err(CatalogError::ProviderEventConflict);
                }
            } else {
                verify_active_payloads(
                    &transaction,
                    &commit,
                    self.result_bytes,
                    cancellation,
                    |ordinal, payload| {
                        if payload == payloads.value(ordinal) {
                            Ok(())
                        } else {
                            Err(CatalogError::ProviderEventConflict)
                        }
                    },
                )?;
            }
            check_cancelled(cancellation)?;
            transaction.commit()?;
            return Ok(commit);
        }
        require_reserved_run(&transaction, reservation.run_id())?;
        let available_at = trusted_catalog_now(&transaction)?;
        transaction.execute(
            "INSERT INTO market_event_storage_heads(dataset_id,committed_sequence,content_digest)
             VALUES (?1,0,NULL) ON CONFLICT(dataset_id) DO NOTHING",
            [dataset.as_str()],
        )?;
        let (previous_sequence, previous_bytes): (i64, Option<Vec<u8>>) = transaction.query_row(
            "SELECT committed_sequence,content_digest FROM market_event_storage_heads WHERE dataset_id=?1",
            [dataset.as_str()], |row| Ok((row.get(0)?,row.get(1)?)),
        )?;
        let previous = previous_bytes.as_deref().map(parse_sha256).transpose()?;
        if previous_sequence < 0 || (previous_sequence == 0) != previous.is_none() {
            return Err(CatalogError::CorruptCatalog);
        }
        if previous_sequence > 0 {
            let prior = load_market_event_commit(&transaction, dataset, previous_sequence as u64)?
                .ok_or(CatalogError::CorruptCatalog)?;
            if Some(prior.content_hash()) != previous || prior.schema() != batch.schema_ref() {
                return Err(CatalogError::CorruptCatalog);
            }
        }
        let sequence = previous_sequence
            .checked_add(1)
            .ok_or(CatalogError::InvalidRecord)?;
        let content_hash = commit_content_hash(
            dataset,
            sequence as u64,
            batch.schema_ref(),
            previous,
            prepared.publication_digest(),
            Sha256Digest::new(lineage.bytes()),
            row_count,
            available_at,
        );
        let commit = MarketEventCommitRef::try_new(
            dataset.clone(),
            sequence as u64,
            batch.schema_ref().clone(),
            content_hash,
            available_at,
            prepared.publication_digest(),
            row_count,
        )?;
        transaction.execute(
            "INSERT INTO market_event_commits
             (dataset_id,commit_sequence,run_id,publication_digest,publication_kind,schema_name,
              schema_version,schema_fingerprint,previous_content_digest,content_digest,lineage_digest,
              row_count,available_at_ns)
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13)",
            params![dataset.as_str(), sequence, reservation.run_id().to_string(),
                prepared.publication_digest().bytes().as_slice(), prepared.publication_kind_name(),
                batch.schema_ref().name(), i64::from(batch.schema_ref().version().get()),
                batch.schema_ref().fingerprint().as_slice(), previous_bytes.as_deref(),
                content_hash.bytes().as_slice(), lineage.bytes().as_slice(),
                i64::try_from(row_count).map_err(|_| CatalogError::InvalidRecord)?, available_at.unix_nanos()],
        )?;
        retain_prepared_provider_publication_binding(
            &transaction,
            reservation.run_id(),
            prepared,
            &commit,
            available_at,
        )?;
        let coordinates = provider_market_event_selection_for_publication(
            &transaction,
            prepared.publication_digest(),
        )?;
        if coordinates.len() != batch.events().len() {
            return Err(CatalogError::ProviderEventMismatch);
        }
        let mut budget = ResultBudget::new(self.result_bytes);
        for (ordinal, coordinate) in coordinates.iter().enumerate() {
            check_cancelled(cancellation)?;
            let payload = payloads.value(ordinal);
            budget.charge([payload.len()])?;
            if sha256(payload) != coordinate.canonical_event_digest().bytes() {
                return Err(CatalogError::ProviderEventMismatch);
            }
            coordinate.revalidate_reconstructed_event(&batch.events()[ordinal])?;
            transaction.execute(
                "INSERT INTO market_event_active_rows(publication_digest,publication_row_ordinal,event_json)
                 VALUES (?1,?2,?3)",
                params![prepared.publication_digest().bytes().as_slice(),
                    i64::try_from(ordinal).map_err(|_| CatalogError::InvalidRecord)?, payload],
            )?;
        }
        let changed = transaction.execute(
            "UPDATE market_event_storage_heads SET committed_sequence=?1,content_digest=?2
             WHERE dataset_id=?3 AND committed_sequence=?4 AND content_digest IS ?5",
            params![
                sequence,
                content_hash.bytes().as_slice(),
                dataset.as_str(),
                previous_sequence,
                previous_bytes
            ],
        )?;
        if changed != 1 {
            return Err(CatalogError::ProviderEventConflict);
        }
        super::complete_ingest_in_transaction(
            &transaction,
            reservation,
            ContractCompletion::Succeeded,
            None,
            None,
            available_at,
        )?;
        append_audit(
            &transaction,
            "market-event.committed",
            &reservation.run_id().to_string(),
            content_hash.bytes(),
            available_at,
        )?;
        check_cancelled(cancellation)?;
        transaction.commit()?;
        Ok(commit)
    }
}

/// Resolves only a complete, successful publication at an exact logical horizon.
pub(crate) fn load_market_event_commit(
    connection: &Connection,
    dataset: &DatasetId,
    sequence: u64,
) -> Result<Option<MarketEventCommitRef>, CatalogError> {
    if sequence == 0 {
        return Err(CatalogError::InvalidRecord);
    }
    let sequence = i64::try_from(sequence).map_err(|_| CatalogError::InvalidRecord)?;
    let mut statement = connection.prepare(
        "SELECT committed.schema_name,committed.schema_version,committed.schema_fingerprint,
                committed.previous_content_digest,committed.content_digest,committed.lineage_digest,
                committed.publication_digest,committed.row_count,committed.available_at_ns
         FROM market_event_complete_commits AS committed
         JOIN ingest_runs AS run ON run.run_id=committed.run_id
         WHERE committed.dataset_id=?1 AND committed.commit_sequence=?2
           AND run.state='succeeded' AND run.completed_at_ns=committed.available_at_ns",
    )?;
    let mut rows = statement.query(params![dataset.as_str(), sequence])?;
    let Some(row) = rows.next()? else {
        return Ok(None);
    };
    let name: String = row.get(0)?;
    let version: i64 = row.get(1)?;
    let fingerprint: Vec<u8> = row.get(2)?;
    let schema = DatasetSchemaRef::try_new(
        name,
        SchemaVersion::new(u16::try_from(version).map_err(|_| CatalogError::CorruptCatalog)?)
            .map_err(|_| CatalogError::CorruptCatalog)?,
        fingerprint
            .try_into()
            .map_err(|_| CatalogError::CorruptCatalog)?,
    )
    .map_err(|_| CatalogError::CorruptCatalog)?;
    let previous_bytes: Option<Vec<u8>> = row.get(3)?;
    let previous = previous_bytes.as_deref().map(parse_sha256).transpose()?;
    let content = parse_sha256(
        row.get_ref(4)?
            .as_blob()
            .map_err(|_| CatalogError::CorruptCatalog)?,
    )?;
    let lineage = parse_sha256(
        row.get_ref(5)?
            .as_blob()
            .map_err(|_| CatalogError::CorruptCatalog)?,
    )?;
    let publication = EvidenceDigest::new(
        DigestAlgorithm::Sha256,
        parse_sha256(
            row.get_ref(6)?
                .as_blob()
                .map_err(|_| CatalogError::CorruptCatalog)?,
        )?
        .bytes(),
    );
    let row_count =
        u64::try_from(row.get::<_, i64>(7)?).map_err(|_| CatalogError::CorruptCatalog)?;
    let available = Timestamp::from_unix_nanos(row.get(8)?);
    let sequence = u64::try_from(sequence).map_err(|_| CatalogError::CorruptCatalog)?;
    if sequence == 0
        || (sequence == 1) != previous.is_none()
        || content
            != commit_content_hash(
                dataset,
                sequence,
                &schema,
                previous,
                publication,
                lineage,
                row_count,
                available,
            )
    {
        return Err(CatalogError::CorruptCatalog);
    }
    if let Some(previous) = previous {
        let exact_parent: bool = connection.query_row(
            "SELECT EXISTS(SELECT 1 FROM market_event_commits AS prior
             JOIN ingest_runs AS run ON run.run_id=prior.run_id
             WHERE prior.dataset_id=?1 AND prior.commit_sequence=?2 AND prior.content_digest=?3
               AND prior.available_at_ns<=?4 AND run.state='succeeded'
               AND run.completed_at_ns=prior.available_at_ns)",
            params![
                dataset.as_str(),
                i64::try_from(sequence - 1).map_err(|_| CatalogError::CorruptCatalog)?,
                previous.bytes().as_slice(),
                available.unix_nanos()
            ],
            |row| row.get(0),
        )?;
        if !exact_parent {
            return Err(CatalogError::CorruptCatalog);
        }
    }
    MarketEventCommitRef::try_new(
        dataset.clone(),
        sequence,
        schema,
        content,
        available,
        publication,
        row_count,
    )
    .map(Some)
}

/// Resolves an exact source publication without scanning earlier logical commits.
pub(crate) fn load_market_event_commit_for_publication(
    connection: &Connection,
    dataset: &DatasetId,
    publication: EvidenceDigest,
) -> Result<Option<MarketEventCommitRef>, CatalogError> {
    if publication.algorithm() != DigestAlgorithm::Sha256 || publication.bytes() == [0; 32] {
        return Err(CatalogError::InvalidRecord);
    }
    let sequence: Option<i64> = connection.query_row(
        "SELECT commit_sequence FROM market_event_commits WHERE dataset_id=?1 AND publication_digest=?2",
        params![dataset.as_str(),publication.bytes().as_slice()], |row| row.get(0),
    ).optional()?;
    sequence
        .map(|sequence| {
            load_market_event_commit(
                connection,
                dataset,
                u64::try_from(sequence).map_err(|_| CatalogError::CorruptCatalog)?,
            )
        })
        .transpose()
        .map(Option::flatten)
}

/// Resolves one authoritative placement from the same snapshot as candidate selection.
pub(crate) fn load_market_event_rows(
    connection: &Connection,
    commit: &MarketEventCommitRef,
    objects: &crate::ParquetObjectStore,
    limits: CatalogResultLimits,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<Vec<Vec<u8>>, CatalogError> {
    check_read(deadline, cancellation)?;
    if load_market_event_commit(connection, commit.dataset_id(), commit.sequence())?.as_ref()
        != Some(commit)
    {
        return Err(CatalogError::ProviderEventConflict);
    }
    if let Some(rows) = archive::load_archived_payloads(
        connection,
        commit,
        objects,
        limits,
        Some(deadline),
        cancellation,
    )? {
        return Ok(rows);
    }
    load_market_event_active_rows(connection, commit, limits, deadline, cancellation)
}

/// Loads one bounded publication in exact ordinal order on the caller's read snapshot.
fn load_market_event_active_rows(
    connection: &Connection,
    commit: &MarketEventCommitRef,
    limits: CatalogResultLimits,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<Vec<Vec<u8>>, CatalogError> {
    check_read(deadline, cancellation)?;
    let count =
        usize::try_from(commit.row_count()).map_err(|_| CatalogError::ResultRowLimitExceeded)?;
    let mut budget = ResultBudget::new(limits);
    budget.charge_many(count, std::mem::size_of::<Vec<u8>>())?;
    let mut result = Vec::new();
    result
        .try_reserve_exact(count)
        .map_err(|_| CatalogError::Allocation)?;
    verify_active_payloads(connection, commit, limits, cancellation, |_, payload| {
        check_read(deadline, cancellation)?;
        budget.charge([payload.len()])?;
        let mut copied = Vec::new();
        copied
            .try_reserve_exact(payload.len())
            .map_err(|_| CatalogError::Allocation)?;
        copied.extend_from_slice(payload);
        result.push(copied);
        Ok(())
    })?;
    check_read(deadline, cancellation)?;
    Ok(result)
}

fn verify_active_payloads(
    connection: &Connection,
    commit: &MarketEventCommitRef,
    limits: CatalogResultLimits,
    cancellation: &CancellationToken,
    mut consume: impl FnMut(usize, &[u8]) -> Result<(), CatalogError>,
) -> Result<(), CatalogError> {
    let mut statement = connection.prepare(
        "SELECT active.publication_row_ordinal,active.event_json,indexed.canonical_event_digest
         FROM market_event_active_rows AS active
         JOIN provider_market_event_selection_index AS indexed
           ON indexed.publication_digest=active.publication_digest
          AND indexed.publication_row_ordinal=active.publication_row_ordinal
         WHERE active.publication_digest=?1 AND indexed.dataset_id=?2 AND indexed.commit_sequence=?3
         ORDER BY active.publication_row_ordinal LIMIT ?4",
    )?;
    let count = i64::try_from(commit.row_count()).map_err(|_| CatalogError::CorruptCatalog)?;
    let mut rows = statement.query(params![
        commit.publication_digest().bytes().as_slice(),
        commit.dataset_id().as_str(),
        i64::try_from(commit.sequence()).map_err(|_| CatalogError::CorruptCatalog)?,
        count.checked_add(1).ok_or(CatalogError::CorruptCatalog)?
    ])?;
    let mut budget = ResultBudget::new(limits);
    let mut ordinal = 0_i64;
    while let Some(row) = rows.next()? {
        check_cancelled(cancellation)?;
        if ordinal >= count || row.get::<_, i64>(0)? != ordinal {
            return Err(CatalogError::CorruptCatalog);
        }
        let payload = row
            .get_ref(1)?
            .as_blob()
            .map_err(|_| CatalogError::CorruptCatalog)?;
        budget.charge([payload.len()])?;
        let digest = row
            .get_ref(2)?
            .as_blob()
            .map_err(|_| CatalogError::CorruptCatalog)?;
        if sha256(payload).as_slice() != digest {
            return Err(CatalogError::CorruptCatalog);
        }
        consume(
            usize::try_from(ordinal).map_err(|_| CatalogError::CorruptCatalog)?,
            payload,
        )?;
        ordinal += 1;
    }
    if ordinal != count {
        return Err(CatalogError::CorruptCatalog);
    }
    Ok(())
}

#[allow(
    clippy::too_many_arguments,
    reason = "the immutable logical chain binds every typed coordinate"
)]
fn commit_content_hash(
    dataset: &DatasetId,
    sequence: u64,
    schema: &DatasetSchemaRef,
    previous: Option<Sha256Digest>,
    publication: EvidenceDigest,
    lineage: Sha256Digest,
    row_count: u64,
    available: Timestamp,
) -> Sha256Digest {
    let mut hash = Sha256::new();
    hash.update(b"market-squawk/market-event-commit/v1");
    for text in [dataset.as_str(), schema.name()] {
        hash.update((text.len() as u64).to_be_bytes());
        hash.update(text.as_bytes());
    }
    hash.update(schema.version().get().to_be_bytes());
    hash.update(schema.fingerprint());
    hash.update(sequence.to_be_bytes());
    match previous {
        Some(previous) => {
            hash.update([1]);
            hash.update(previous.bytes());
        }
        None => hash.update([0]),
    }
    hash.update(publication.bytes());
    hash.update(lineage.bytes());
    hash.update(row_count.to_be_bytes());
    hash.update(available.unix_nanos().to_be_bytes());
    Sha256Digest::new(hash.finalize().into())
}

fn parse_sha256(bytes: &[u8]) -> Result<Sha256Digest, CatalogError> {
    let bytes: [u8; 32] = bytes.try_into().map_err(|_| CatalogError::CorruptCatalog)?;
    if bytes == [0; 32] {
        return Err(CatalogError::CorruptCatalog);
    }
    Ok(Sha256Digest::new(bytes))
}

fn check_cancelled(cancellation: &CancellationToken) -> Result<(), CatalogError> {
    if cancellation.is_cancelled() {
        Err(CatalogError::MarketRecoveryReadCancelled)
    } else {
        Ok(())
    }
}

fn check_read(deadline: Instant, cancellation: &CancellationToken) -> Result<(), CatalogError> {
    check_cancelled(cancellation)?;
    if Instant::now() >= deadline {
        Err(CatalogError::MarketRecoveryReadDeadlineExceeded)
    } else {
        Ok(())
    }
}
