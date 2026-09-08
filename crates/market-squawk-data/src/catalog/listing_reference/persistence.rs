use std::time::Instant;

use market_squawk_domain::{
    DigestAlgorithm, EvidenceDigest, ExactPayloadEvidence, SourceId, SourceIdentifier, Timestamp,
    VersionPinnedSourceLocator,
};
use rusqlite::{Connection, OptionalExtension as _, Transaction, params};
use tokio_util::sync::CancellationToken;

use super::canonical;
use super::{
    CatalogAuthority, ListingReferenceError, ListingReferenceFileKind,
    ListingReferenceGenerationInput, ListingReferenceGenerationReceipt,
    ListingReferencePublicationReceipt, ListingReferenceRightsState,
    ListingReferenceSourceFileInput,
};
use crate::RegisteredRightsGrant;
use crate::catalog::storage::{append_audit, sha256, trusted_catalog_now};

/// Outcome of reconciling one content-addressed current-directory generation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ListingReferencePublicationDisposition {
    /// A new immutable successor was committed.
    Inserted,
    /// The exact still-current content was already durable.
    Replay,
}

impl CatalogAuthority {
    pub(super) fn publish_listing_reference_generation(
        &self,
        dataset: &SourceIdentifier,
        source_id: &SourceId,
        rights: &RegisteredRightsGrant,
        input: ListingReferenceGenerationInput,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<ListingReferencePublicationReceipt, ListingReferenceError> {
        canonical::check_operation(deadline, cancellation)?;
        if rights.catalog_id != self.session_id() || input.source.source_id() != source_id {
            return Err(ListingReferenceError::InvalidRightsCapability);
        }
        if rights.payload_digest != input.source_payload_set_digest() {
            return Err(ListingReferenceError::RightsUnavailable);
        }
        let source_json = serde_json::to_string(&input.source)?;
        let source_revision_digest = sha256(source_json.as_bytes());
        let records_digest = canonical::records_digest(&input.records);
        let generation_digest = canonical::generation_digest(
            dataset,
            &input,
            source_revision_digest,
            rights.rights_id,
            records_digest,
        );

        let transaction = self.catalog().connection.unchecked_transaction()?;
        let published_at =
            trusted_catalog_now(&transaction).map_err(|_| ListingReferenceError::CorruptCatalog)?;
        canonical::check_operation(deadline, cancellation)?;
        if !input.source.is_effective_at(published_at)
            || input
                .files
                .iter()
                .any(|file| file.received_at > published_at || file.available_at > published_at)
        {
            return Err(ListingReferenceError::InvalidSourceContract);
        }
        require_exact_source_revision(
            &transaction,
            source_id,
            source_revision_digest,
            &source_json,
        )?;
        require_current_rights(&transaction, source_id, rights.rights_id, published_at)?;

        let current = current_position(&transaction, dataset, source_id)?;
        let existing = load_generation_receipt(&transaction, generation_digest)?;
        if let Some(existing) = existing {
            if current.as_ref().map(|position| position.0) == Some(generation_digest) {
                return Ok(ListingReferencePublicationReceipt {
                    disposition: ListingReferencePublicationDisposition::Replay,
                    generation: existing,
                });
            }
            return Err(ListingReferenceError::SupersededGeneration);
        }

        let expected = input
            .expected_previous_generation
            .map(EvidenceDigest::bytes);
        if current.as_ref().map(|position| position.0) != expected {
            return Err(ListingReferenceError::PositionConflict);
        }
        let generation_sequence = current.as_ref().map_or(Ok(1_u32), |position| {
            position
                .1
                .checked_add(1)
                .ok_or(ListingReferenceError::PositionConflict)
        })?;
        if generation_sequence > 16_384 {
            return Err(ListingReferenceError::PositionConflict);
        }

        insert_generation(
            &transaction,
            dataset,
            source_id,
            rights.rights_id,
            &input,
            source_revision_digest,
            generation_digest,
            generation_sequence,
            records_digest,
            published_at,
            deadline,
            cancellation,
        )?;
        append_audit(
            &transaction,
            "listing-reference.generation-published",
            dataset.as_str(),
            generation_digest,
            published_at,
        )
        .map_err(|_| ListingReferenceError::CorruptCatalog)?;
        transaction.commit()?;
        let generation = ListingReferenceGenerationReceipt {
            dataset: dataset.clone(),
            generation_digest: canonical::digest(generation_digest),
            generation_sequence,
            previous_generation_digest: current
                .as_ref()
                .map(|position| canonical::digest(position.0)),
            source_id: source_id.clone(),
            source_revision: input.source.revision().as_source_identifier().clone(),
            source_revision_digest: canonical::digest(source_revision_digest),
            rights_id: rights.rights_id,
            rights_state: ListingReferenceRightsState::AdmittedScoped,
            record_count: input.records.len(),
            published_at,
        };
        Ok(ListingReferencePublicationReceipt {
            disposition: ListingReferencePublicationDisposition::Inserted,
            generation,
        })
    }
}

fn require_exact_source_revision(
    transaction: &Transaction<'_>,
    source_id: &SourceId,
    revision_digest: [u8; 32],
    expected_json: &str,
) -> Result<(), ListingReferenceError> {
    let retained: Option<String> = transaction
        .query_row(
            "SELECT metadata_json FROM source_revisions
             WHERE source_id=?1 AND revision_digest=?2",
            params![source_id.as_str(), revision_digest],
            |row| row.get(0),
        )
        .optional()?;
    match retained {
        Some(retained) if retained == expected_json => Ok(()),
        _ => Err(ListingReferenceError::SourceRevisionUnavailable),
    }
}

fn require_current_rights(
    transaction: &Transaction<'_>,
    source_id: &SourceId,
    rights_id: [u8; 32],
    at: Timestamp,
) -> Result<(), ListingReferenceError> {
    let authorized: bool = transaction.query_row(
        "SELECT EXISTS(
             SELECT 1 FROM source_rights
             WHERE rights_id=?1 AND source_id=?2
               AND (operation_mask & 6)=6
               AND admitted_at_ns<=?3
               AND (authorization_expires_at_ns IS NULL OR authorization_expires_at_ns>?3)
         )",
        params![rights_id, source_id.as_str(), at.unix_nanos()],
        |row| row.get(0),
    )?;
    if authorized {
        Ok(())
    } else {
        Err(ListingReferenceError::RightsUnavailable)
    }
}

fn current_position(
    connection: &Connection,
    dataset: &SourceIdentifier,
    expected_source: &SourceId,
) -> Result<Option<([u8; 32], u32)>, ListingReferenceError> {
    let position: Option<(Vec<u8>, i64, String)> = connection
        .query_row(
            "SELECT generation_digest, generation_sequence, source_id
             FROM listing_reference_generations
             WHERE dataset_id=?1
             ORDER BY generation_sequence DESC LIMIT 1",
            [dataset.as_str()],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional()?;
    position
        .map(|(digest, sequence, source)| {
            if source != expected_source.as_str() {
                return Err(ListingReferenceError::CorruptCatalog);
            }
            Ok((
                digest
                    .try_into()
                    .map_err(|_| ListingReferenceError::CorruptCatalog)?,
                u32::try_from(sequence).map_err(|_| ListingReferenceError::CorruptCatalog)?,
            ))
        })
        .transpose()
}

#[allow(
    clippy::too_many_arguments,
    reason = "atomic generation coordinates stay explicit"
)]
fn insert_generation(
    transaction: &Transaction<'_>,
    dataset: &SourceIdentifier,
    source_id: &SourceId,
    rights_id: [u8; 32],
    input: &ListingReferenceGenerationInput,
    source_revision_digest: [u8; 32],
    generation_digest: [u8; 32],
    generation_sequence: u32,
    records_digest: [u8; 32],
    published_at: Timestamp,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<(), ListingReferenceError> {
    transaction.execute(
        "INSERT INTO listing_reference_generations
         (generation_digest, dataset_id, generation_sequence, previous_generation_digest,
          source_id, source_revision, source_revision_digest, rights_id, rights_state,
          file_count, record_count, records_digest, published_at_ns)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, 'admitted_scoped', 2, ?9, ?10, ?11)",
        params![
            generation_digest,
            dataset.as_str(),
            i64::from(generation_sequence),
            input
                .expected_previous_generation
                .map(EvidenceDigest::bytes),
            source_id.as_str(),
            input.source.revision().as_source_identifier().as_str(),
            source_revision_digest,
            rights_id,
            i64::try_from(input.records.len()).map_err(|_| ListingReferenceError::InvalidInput)?,
            records_digest,
            published_at.unix_nanos(),
        ],
    )?;

    let mut insert_file = transaction.prepare(
        "INSERT INTO listing_reference_files
         (generation_digest, file_kind, source_object_id, source_reference,
          file_creation_time, payload_algorithm, payload_digest, payload_locator_reference,
          payload_locator_version, source_last_modified_at_ns, received_at_ns, available_at_ns,
          ingested_at_ns, record_count)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)",
    )?;
    for file in &input.files {
        canonical::check_operation(deadline, cancellation)?;
        let count = input
            .records
            .iter()
            .filter(|(kind, _)| *kind == file.kind)
            .count();
        let (algorithm, digest, locator_reference, locator_version) =
            canonical::evidence_columns(&file.payload_evidence);
        insert_file.execute(params![
            generation_digest,
            file.kind.database_name(),
            file.source_object_id.as_str(),
            file.source_reference.as_str(),
            file.file_creation_time,
            algorithm,
            digest,
            locator_reference,
            locator_version,
            file.source_last_modified_at.unix_nanos(),
            file.received_at.unix_nanos(),
            file.available_at.unix_nanos(),
            published_at.unix_nanos(),
            i64::try_from(count).map_err(|_| ListingReferenceError::InvalidInput)?,
        ])?;
    }
    drop(insert_file);

    let mut insert_value = transaction.prepare(
        "INSERT OR IGNORE INTO listing_reference_values
         (value_digest, file_kind, provider_symbol, normalized_provider_symbol, security_name,
          normalized_security_name, listing_venue, exchange_code, cqs_symbol, nasdaq_symbol,
          market_category, financial_status, is_etf, is_test_issue, round_lot_size,
          is_next_shares, directory_presence, data_quality, authority_class)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14,
                 ?15, ?16, 'current_directory', 'official_delayed', 'reference_only')",
    )?;
    let mut insert_membership = transaction.prepare(
        "INSERT INTO listing_reference_memberships
         (generation_digest, file_kind, provider_row_number, provider_symbol, record_revision,
          record_algorithm, record_payload_digest, record_locator_reference,
          record_locator_version, value_digest, record_digest)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
    )?;
    for (index, (kind, record)) in input.records.iter().enumerate() {
        if index.is_multiple_of(256) {
            canonical::check_operation(deadline, cancellation)?;
        }
        let value_digest = canonical::value_digest(record);
        insert_value.execute(params![
            value_digest,
            kind.database_name(),
            record.provider_symbol,
            canonical::normalize_symbol(&record.provider_symbol),
            record.security_name,
            canonical::normalize_name(&record.security_name),
            record.listing_venue.as_str(),
            record.exchange_code.map(|value| value.database_name()),
            record.cqs_symbol,
            record.nasdaq_symbol,
            record.market_category.map(|value| value.database_name()),
            record.financial_status.map(|value| value.database_name()),
            i64::from(record.is_etf),
            i64::from(record.is_test_issue),
            i64::from(record.round_lot_size),
            record.is_next_shares.map(i64::from),
        ])?;
        let (algorithm, digest, locator_reference, locator_version) =
            canonical::evidence_columns(&record.record_payload_evidence);
        insert_membership.execute(params![
            generation_digest,
            kind.database_name(),
            i64::from(record.provider_row_number),
            record.provider_symbol,
            record.record_revision.as_str(),
            algorithm,
            digest,
            locator_reference,
            locator_version,
            value_digest,
            canonical::record_digest(*kind, record, value_digest),
        ])?;
    }
    canonical::check_operation(deadline, cancellation)
}

pub(super) fn load_generation_receipt(
    connection: &Connection,
    generation_digest: [u8; 32],
) -> Result<Option<ListingReferenceGenerationReceipt>, ListingReferenceError> {
    type GenerationRow = (
        String,
        i64,
        Option<Vec<u8>>,
        String,
        String,
        Vec<u8>,
        Vec<u8>,
        String,
        i64,
        Vec<u8>,
        i64,
    );
    let row: Option<GenerationRow> = connection
        .query_row(
            "SELECT dataset_id, generation_sequence, previous_generation_digest, source_id,
                    source_revision, source_revision_digest, rights_id, rights_state,
                    record_count, records_digest, published_at_ns
             FROM listing_reference_generations WHERE generation_digest=?1",
            [generation_digest],
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
                    row.get(8)?,
                    row.get(9)?,
                    row.get(10)?,
                ))
            },
        )
        .optional()?;
    let Some((
        dataset,
        generation_sequence,
        previous,
        source_id,
        source_revision,
        source_revision_digest,
        rights_id,
        rights_state,
        record_count,
        records_digest,
        published_at,
    )) = row
    else {
        return Ok(None);
    };
    if rights_state != "admitted_scoped" {
        return Err(ListingReferenceError::CorruptCatalog);
    }
    let dataset =
        SourceIdentifier::try_from(dataset).map_err(|_| ListingReferenceError::CorruptCatalog)?;
    let source_id =
        SourceId::try_from(source_id).map_err(|_| ListingReferenceError::CorruptCatalog)?;
    let source_revision = SourceIdentifier::try_from(source_revision)
        .map_err(|_| ListingReferenceError::CorruptCatalog)?;
    let source_revision_digest: [u8; 32] = source_revision_digest
        .try_into()
        .map_err(|_| ListingReferenceError::CorruptCatalog)?;
    let rights_id: [u8; 32] = rights_id
        .try_into()
        .map_err(|_| ListingReferenceError::CorruptCatalog)?;
    let records_digest: [u8; 32] = records_digest
        .try_into()
        .map_err(|_| ListingReferenceError::CorruptCatalog)?;
    let files = load_source_files(connection, generation_digest)?;
    if files.len() != 2
        || canonical::generation_digest_parts(
            &dataset,
            source_id.as_str(),
            source_revision.as_str(),
            source_revision_digest,
            rights_id,
            &files,
            records_digest,
        ) != generation_digest
    {
        return Err(ListingReferenceError::CorruptCatalog);
    }
    let retained_count: i64 = connection.query_row(
        "SELECT COUNT(*) FROM listing_reference_memberships WHERE generation_digest=?1",
        [generation_digest],
        |row| row.get(0),
    )?;
    if retained_count != record_count {
        return Err(ListingReferenceError::CorruptCatalog);
    }
    Ok(Some(ListingReferenceGenerationReceipt {
        dataset,
        generation_digest: canonical::digest(generation_digest),
        generation_sequence: u32::try_from(generation_sequence)
            .map_err(|_| ListingReferenceError::CorruptCatalog)?,
        previous_generation_digest: previous
            .map(|digest| {
                digest
                    .try_into()
                    .map(canonical::digest)
                    .map_err(|_| ListingReferenceError::CorruptCatalog)
            })
            .transpose()?,
        source_id,
        source_revision,
        source_revision_digest: canonical::digest(source_revision_digest),
        rights_id,
        rights_state: ListingReferenceRightsState::AdmittedScoped,
        record_count: usize::try_from(record_count)
            .map_err(|_| ListingReferenceError::CorruptCatalog)?,
        published_at: Timestamp::from_unix_nanos(published_at),
    }))
}

pub(super) fn load_source_files(
    connection: &Connection,
    generation_digest: [u8; 32],
) -> Result<Vec<ListingReferenceSourceFileInput>, ListingReferenceError> {
    let mut statement = connection.prepare(
        "SELECT file_kind, source_object_id, source_reference, file_creation_time,
                payload_algorithm, payload_digest, payload_locator_reference,
                payload_locator_version, source_last_modified_at_ns, received_at_ns,
                available_at_ns
         FROM listing_reference_files WHERE generation_digest=?1 ORDER BY file_kind",
    )?;
    let rows = statement.query_map([generation_digest], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, String>(2)?,
            row.get::<_, String>(3)?,
            row.get::<_, i64>(4)?,
            row.get::<_, Vec<u8>>(5)?,
            row.get::<_, Option<String>>(6)?,
            row.get::<_, Option<String>>(7)?,
            row.get::<_, i64>(8)?,
            row.get::<_, i64>(9)?,
            row.get::<_, i64>(10)?,
        ))
    })?;
    let mut files = Vec::with_capacity(2);
    for row in rows {
        let (
            kind,
            object,
            reference,
            creation,
            algorithm,
            digest,
            locator_reference,
            locator_version,
            last_modified,
            received,
            available,
        ) = row?;
        files.push(ListingReferenceSourceFileInput::try_new(
            ListingReferenceFileKind::from_database(&kind)?,
            SourceIdentifier::try_from(object)
                .map_err(|_| ListingReferenceError::CorruptCatalog)?,
            SourceIdentifier::try_from(reference)
                .map_err(|_| ListingReferenceError::CorruptCatalog)?,
            creation,
            exact_evidence(algorithm, digest, locator_reference, locator_version)?,
            Timestamp::from_unix_nanos(last_modified),
            Timestamp::from_unix_nanos(received),
            Timestamp::from_unix_nanos(available),
        )?);
    }
    Ok(files)
}

pub(super) fn exact_evidence(
    algorithm: i64,
    digest: Vec<u8>,
    locator_reference: Option<String>,
    locator_version: Option<String>,
) -> Result<ExactPayloadEvidence, ListingReferenceError> {
    let algorithm = match algorithm {
        1 => DigestAlgorithm::Sha256,
        2 => DigestAlgorithm::Blake3,
        _ => return Err(ListingReferenceError::CorruptCatalog),
    };
    let digest = EvidenceDigest::new(
        algorithm,
        digest
            .try_into()
            .map_err(|_| ListingReferenceError::CorruptCatalog)?,
    );
    match (locator_reference, locator_version) {
        (None, None) => Ok(ExactPayloadEvidence::from_content_digest(digest)),
        (Some(reference), Some(version)) => Ok(ExactPayloadEvidence::with_version_pinned_locator(
            digest,
            VersionPinnedSourceLocator::new(
                SourceIdentifier::try_from(reference)
                    .map_err(|_| ListingReferenceError::CorruptCatalog)?,
                SourceIdentifier::try_from(version)
                    .map_err(|_| ListingReferenceError::CorruptCatalog)?,
            ),
        )),
        _ => Err(ListingReferenceError::CorruptCatalog),
    }
}
