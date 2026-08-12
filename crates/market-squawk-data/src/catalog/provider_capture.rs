//! Immutable provider-response capture receipts and ingest-run bindings.

use market_squawk_domain::{DigestAlgorithm, EvidenceDigest};
use market_squawk_platform::SealedResearchJournalSegmentClaim;
use market_squawk_sources::{
    ExtractionBatch, ProviderCapturePageReceipt, ProviderCaptureSetReceipt,
    ProviderCaptureTerminalDisposition, SealedProviderCaptureSetReceipt, SourceMetadata,
    SourceObjectCaptureIdentity,
};
use rusqlite::{Connection, OptionalExtension as _, params};
use uuid::Uuid;

use super::storage::{append_audit, require_reserved_run, sha256, trusted_catalog_now};
use super::{Catalog, CatalogError, IngestReservation};

const MAX_PROVIDER_CAPTURE_SETS: usize = 100_000;

#[derive(Clone, Debug)]
pub(crate) struct PersistedProviderCapture {
    capture: ProviderCaptureSetReceipt,
    claim: SealedResearchJournalSegmentClaim,
    receipt_digest: EvidenceDigest,
}

impl PersistedProviderCapture {
    pub(crate) const fn capture(&self) -> &ProviderCaptureSetReceipt {
        &self.capture
    }

    pub(crate) const fn claim(&self) -> &SealedResearchJournalSegmentClaim {
        &self.claim
    }

    pub(crate) const fn receipt_digest(&self) -> EvidenceDigest {
        self.receipt_digest
    }
}

impl Catalog {
    pub(crate) fn retain_provider_capture(
        &self,
        reservation: &IngestReservation,
        batch: &ExtractionBatch,
        sealed: &SealedProviderCaptureSetReceipt,
    ) -> Result<(), CatalogError> {
        if !provider_capture_matches_batch(sealed, batch) {
            return Err(CatalogError::ProviderCaptureMismatch);
        }
        let capture = sealed.capture();
        let segment = sealed.segment();
        let capture_json = serde_json::to_string(capture)?;
        let claim_json = serde_json::to_string(segment.claim())?;
        let transaction = self.connection.unchecked_transaction()?;
        let recorded_at = trusted_catalog_now(&transaction)?;
        require_reserved_run(&transaction, reservation.run_id())?;
        let run_source: String = transaction.query_row(
            "SELECT source_id FROM ingest_runs WHERE run_id=?1",
            [reservation.run_id().to_string()],
            |row| row.get(0),
        )?;
        if run_source != capture.source_id().as_str() {
            return Err(CatalogError::ProviderCaptureMismatch);
        }
        let (source_revision_digest, metadata_json): (Vec<u8>, String) = transaction.query_row(
            "SELECT sources.current_revision_digest, revisions.metadata_json
             FROM sources
             JOIN source_revisions AS revisions
               ON revisions.source_id=sources.source_id
              AND revisions.revision_digest=sources.current_revision_digest
             WHERE sources.source_id=?1",
            [capture.source_id().as_str()],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        if source_revision_digest.as_slice() != sha256(metadata_json.as_bytes()) {
            return Err(CatalogError::CorruptCatalog);
        }
        let source: SourceMetadata = serde_json::from_str(&metadata_json)?;
        if source.source_id() != capture.source_id()
            || source.revision() != capture.metadata_revision()
        {
            return Err(CatalogError::ProviderCaptureMismatch);
        }
        let terminal = terminal_name(capture.terminal());
        let inserted_set = transaction.execute(
            "INSERT OR IGNORE INTO provider_capture_sets
             (capture_receipt_digest, capture_content_digest, capture_observation_digest,
              source_id, source_revision_digest, metadata_revision, provider_dataset,
              request_set_identity, terminal_disposition, page_count, total_body_bytes,
              capture_json, sealed_relative_reference, sealed_content_digest,
              sealed_size_bytes, sealed_physical_receipt_digest, segment_claim_json,
              recorded_at_ns)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13,
                     ?14, ?15, ?16, ?17, ?18)",
            params![
                sealed.receipt_digest().bytes().as_slice(),
                capture.content_digest().bytes().as_slice(),
                capture.observation_digest().bytes().as_slice(),
                capture.source_id().as_str(),
                source_revision_digest,
                capture.metadata_revision().as_source_identifier().as_str(),
                capture.dataset().as_str(),
                capture.request_set_identity().bytes().as_slice(),
                terminal,
                i64::try_from(capture.pages().len())
                    .map_err(|_| CatalogError::ProviderCaptureMismatch)?,
                i64::try_from(capture.total_body_bytes())
                    .map_err(|_| CatalogError::ProviderCaptureMismatch)?,
                capture_json,
                segment.relative_reference(),
                segment.content_digest().bytes().as_slice(),
                i64::try_from(segment.size_bytes())
                    .map_err(|_| CatalogError::ProviderCaptureMismatch)?,
                segment.physical_receipt_digest().bytes().as_slice(),
                claim_json,
                recorded_at.unix_nanos(),
            ],
        )?;
        for page in capture.pages() {
            insert_page(&transaction, sealed.receipt_digest(), page)?;
        }
        for frame in segment.frames() {
            transaction.execute(
                "INSERT OR IGNORE INTO provider_capture_frames
                 (capture_receipt_digest, frame_ordinal, frame_offset, framed_bytes,
                  provider_payload_bytes, provider_payload_digest, received_at_ns,
                  source_sequence)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                params![
                    sealed.receipt_digest().bytes().as_slice(),
                    i64::from(frame.ordinal()),
                    i64::try_from(frame.offset())
                        .map_err(|_| CatalogError::ProviderCaptureMismatch)?,
                    i64::try_from(frame.framed_bytes())
                        .map_err(|_| CatalogError::ProviderCaptureMismatch)?,
                    i64::try_from(frame.provider_payload_bytes())
                        .map_err(|_| CatalogError::ProviderCaptureMismatch)?,
                    frame.provider_payload_digest().bytes().as_slice(),
                    frame.received_at().unix_nanos(),
                    frame
                        .source_sequence()
                        .map(i64::try_from)
                        .transpose()
                        .map_err(|_| CatalogError::ProviderCaptureMismatch)?,
                ],
            )?;
        }
        let inserted_input = transaction.execute(
            "INSERT OR IGNORE INTO ingest_run_capture_inputs
             (run_id, input_ordinal, capture_receipt_digest, source_id)
             VALUES (?1, 0, ?2, ?3)",
            params![
                reservation.run_id().to_string(),
                sealed.receipt_digest().bytes().as_slice(),
                capture.source_id().as_str(),
            ],
        )?;
        let retained = load_provider_capture_for_run(&transaction, reservation.run_id())?
            .ok_or(CatalogError::ProviderCaptureConflict)?;
        if retained.receipt_digest != sealed.receipt_digest()
            || retained.capture != *capture
            || retained.claim != *segment.claim()
        {
            return Err(CatalogError::ProviderCaptureConflict);
        }
        if inserted_set != 0 || inserted_input != 0 {
            append_audit(
                &transaction,
                "provider-capture.retained",
                &reservation.run_id().to_string(),
                sealed.receipt_digest().bytes(),
                recorded_at,
            )?;
        }
        transaction.commit()?;
        Ok(())
    }

    pub(crate) fn provider_capture_for_run(
        &self,
        run_id: Uuid,
    ) -> Result<Option<PersistedProviderCapture>, CatalogError> {
        load_provider_capture_for_run(&self.connection, run_id)
    }

    pub(crate) fn authoritative_provider_capture_claims(
        &self,
    ) -> Result<Vec<SealedResearchJournalSegmentClaim>, CatalogError> {
        let (set_count, input_count): (i64, i64) = self.connection.query_row(
            "SELECT (SELECT COUNT(*) FROM provider_capture_sets),
                    (SELECT COUNT(*) FROM ingest_run_capture_inputs)",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        let count = usize::try_from(set_count).map_err(|_| CatalogError::CorruptCatalog)?;
        if set_count != input_count || count > MAX_PROVIDER_CAPTURE_SETS {
            return Err(CatalogError::CorruptCatalog);
        }
        let retrieval_limit = count.checked_add(1).ok_or(CatalogError::CorruptCatalog)?;
        let mut statement = self.connection.prepare(
            "SELECT run_id FROM ingest_run_capture_inputs
             ORDER BY capture_receipt_digest, run_id LIMIT ?1",
        )?;
        let rows = statement.query_map(
            [i64::try_from(retrieval_limit).map_err(|_| CatalogError::CorruptCatalog)?],
            |row| row.get::<_, String>(0),
        )?;
        let mut claims = Vec::new();
        claims
            .try_reserve_exact(count)
            .map_err(|_| CatalogError::Allocation)?;
        for row in rows {
            if claims.len() == count {
                return Err(CatalogError::CorruptCatalog);
            }
            let run_id = Uuid::parse_str(&row?).map_err(|_| CatalogError::CorruptCatalog)?;
            let capture = load_provider_capture_for_run(&self.connection, run_id)?
                .ok_or(CatalogError::CorruptCatalog)?;
            claims.push(capture.claim);
        }
        if claims.len() != count {
            return Err(CatalogError::CorruptCatalog);
        }
        Ok(claims)
    }
}

pub(crate) fn provider_capture_matches_batch(
    sealed: &SealedProviderCaptureSetReceipt,
    batch: &ExtractionBatch,
) -> bool {
    let capture = sealed.capture();
    let object = batch.request().object();
    capture.source_id() == object.source_id()
        && capture.metadata_revision() == object.metadata_revision()
        && capture.dataset() == object.dataset()
        && SourceObjectCaptureIdentity::try_from_capture(capture)
            .is_ok_and(|identity| identity == object.capture_identity())
}

fn insert_page(
    transaction: &rusqlite::Transaction<'_>,
    receipt_digest: EvidenceDigest,
    page: &ProviderCapturePageReceipt,
) -> Result<(), CatalogError> {
    transaction.execute(
        "INSERT OR IGNORE INTO provider_capture_pages
         (capture_receipt_digest, page_ordinal, request_identity,
          request_page_token_digest, response_next_page_token_digest, http_status,
          body_bytes, body_digest, received_at_ns)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
        params![
            receipt_digest.bytes().as_slice(),
            i64::from(page.ordinal()),
            page.request_identity().bytes().as_slice(),
            page.request_page_token_digest().map(|value| value.bytes()),
            page.response_next_page_token_digest()
                .map(|value| value.bytes()),
            i64::from(page.http_status()),
            i64::try_from(page.body_bytes()).map_err(|_| CatalogError::ProviderCaptureMismatch)?,
            page.body_digest().bytes().as_slice(),
            page.received_at().unix_nanos(),
        ],
    )?;
    Ok(())
}

fn load_provider_capture_for_run(
    connection: &Connection,
    run_id: Uuid,
) -> Result<Option<PersistedProviderCapture>, CatalogError> {
    type StoredCapture = (
        Vec<u8>,
        Vec<u8>,
        Vec<u8>,
        String,
        Vec<u8>,
        String,
        String,
        Vec<u8>,
        String,
        i64,
        i64,
        String,
        String,
        Vec<u8>,
        i64,
        Vec<u8>,
        String,
    );
    let stored: Option<StoredCapture> = connection
        .query_row(
            "SELECT capture.capture_receipt_digest, capture.capture_content_digest,
                    capture.capture_observation_digest, capture.source_id,
                    capture.source_revision_digest, capture.metadata_revision,
                    capture.provider_dataset, capture.request_set_identity,
                    capture.terminal_disposition, capture.page_count,
                    capture.total_body_bytes, capture.capture_json,
                    capture.sealed_relative_reference, capture.sealed_content_digest,
                    capture.sealed_size_bytes, capture.sealed_physical_receipt_digest,
                    capture.segment_claim_json
             FROM ingest_run_capture_inputs AS input
             JOIN provider_capture_sets AS capture USING (capture_receipt_digest)
             WHERE input.run_id=?1 AND input.input_ordinal=0",
            [run_id.to_string()],
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
                    row.get(11)?,
                    row.get(12)?,
                    row.get(13)?,
                    row.get(14)?,
                    row.get(15)?,
                    row.get(16)?,
                ))
            },
        )
        .optional()?;
    let Some(stored) = stored else {
        return Ok(None);
    };
    let receipt_digest = required_digest(&stored.0)?;
    let capture: ProviderCaptureSetReceipt = serde_json::from_str(&stored.11)?;
    let claim: SealedResearchJournalSegmentClaim = serde_json::from_str(&stored.16)?;
    if required_digest(&stored.1)? != capture.content_digest()
        || required_digest(&stored.2)? != capture.observation_digest()
        || stored.3 != capture.source_id().as_str()
        || stored.5 != capture.metadata_revision().as_source_identifier().as_str()
        || stored.6 != capture.dataset().as_str()
        || required_digest(&stored.7)? != capture.request_set_identity()
        || stored.8 != terminal_name(capture.terminal())
        || usize::try_from(stored.9).ok() != Some(capture.pages().len())
        || u64::try_from(stored.10).ok() != Some(capture.total_body_bytes())
        || stored.12 != claim.relative_reference()
        || required_digest(&stored.13)? != claim.content_digest()
        || u64::try_from(stored.14).ok() != Some(claim.size_bytes())
        || required_digest(&stored.15)? != claim.physical_receipt_digest()
    {
        return Err(CatalogError::CorruptCatalog);
    }
    validate_source_revision(connection, &capture, &stored.4)?;
    validate_pages(connection, receipt_digest, &capture)?;
    validate_frames(connection, receipt_digest, &claim)?;
    Ok(Some(PersistedProviderCapture {
        capture,
        claim,
        receipt_digest,
    }))
}

fn validate_source_revision(
    connection: &Connection,
    capture: &ProviderCaptureSetReceipt,
    source_revision_digest: &[u8],
) -> Result<(), CatalogError> {
    let metadata_json: String = connection.query_row(
        "SELECT metadata_json FROM source_revisions
         WHERE source_id=?1 AND revision_digest=?2",
        params![capture.source_id().as_str(), source_revision_digest],
        |row| row.get(0),
    )?;
    if source_revision_digest != sha256(metadata_json.as_bytes()) {
        return Err(CatalogError::CorruptCatalog);
    }
    let source: SourceMetadata = serde_json::from_str(&metadata_json)?;
    if source.source_id() != capture.source_id() || source.revision() != capture.metadata_revision()
    {
        return Err(CatalogError::CorruptCatalog);
    }
    Ok(())
}

fn validate_pages(
    connection: &Connection,
    receipt_digest: EvidenceDigest,
    capture: &ProviderCaptureSetReceipt,
) -> Result<(), CatalogError> {
    let mut statement = connection.prepare(
        "SELECT page_ordinal, request_identity, request_page_token_digest,
                response_next_page_token_digest, http_status, body_bytes, body_digest,
                received_at_ns
         FROM provider_capture_pages
         WHERE capture_receipt_digest=?1 ORDER BY page_ordinal",
    )?;
    let rows = statement.query_map([receipt_digest.bytes().as_slice()], |row| {
        Ok((
            row.get::<_, i64>(0)?,
            row.get::<_, Vec<u8>>(1)?,
            row.get::<_, Option<Vec<u8>>>(2)?,
            row.get::<_, Option<Vec<u8>>>(3)?,
            row.get::<_, i64>(4)?,
            row.get::<_, i64>(5)?,
            row.get::<_, Vec<u8>>(6)?,
            row.get::<_, i64>(7)?,
        ))
    })?;
    let mut count = 0_usize;
    for row in rows {
        let row = row?;
        let page = capture
            .pages()
            .get(count)
            .ok_or(CatalogError::CorruptCatalog)?;
        if u16::try_from(row.0).ok() != Some(page.ordinal())
            || required_digest(&row.1)? != page.request_identity()
            || optional_digest(row.2.as_deref())? != page.request_page_token_digest()
            || optional_digest(row.3.as_deref())? != page.response_next_page_token_digest()
            || u16::try_from(row.4).ok() != Some(page.http_status())
            || u64::try_from(row.5).ok() != Some(page.body_bytes())
            || required_digest(&row.6)? != page.body_digest()
            || row.7 != page.received_at().unix_nanos()
        {
            return Err(CatalogError::CorruptCatalog);
        }
        count = count.checked_add(1).ok_or(CatalogError::CorruptCatalog)?;
    }
    if count != capture.pages().len() {
        return Err(CatalogError::CorruptCatalog);
    }
    Ok(())
}

fn validate_frames(
    connection: &Connection,
    receipt_digest: EvidenceDigest,
    claim: &SealedResearchJournalSegmentClaim,
) -> Result<(), CatalogError> {
    let mut statement = connection.prepare(
        "SELECT frame_ordinal, frame_offset, framed_bytes, provider_payload_bytes,
                provider_payload_digest, received_at_ns, source_sequence
         FROM provider_capture_frames
         WHERE capture_receipt_digest=?1 ORDER BY frame_ordinal",
    )?;
    let rows = statement.query_map([receipt_digest.bytes().as_slice()], |row| {
        Ok((
            row.get::<_, i64>(0)?,
            row.get::<_, i64>(1)?,
            row.get::<_, i64>(2)?,
            row.get::<_, i64>(3)?,
            row.get::<_, Vec<u8>>(4)?,
            row.get::<_, i64>(5)?,
            row.get::<_, Option<i64>>(6)?,
        ))
    })?;
    let mut count = 0_usize;
    for row in rows {
        let row = row?;
        let frame = claim
            .frames()
            .get(count)
            .ok_or(CatalogError::CorruptCatalog)?;
        if u32::try_from(row.0).ok() != Some(frame.ordinal())
            || u64::try_from(row.1).ok() != Some(frame.offset())
            || u64::try_from(row.2).ok() != Some(frame.framed_bytes())
            || u64::try_from(row.3).ok() != Some(frame.provider_payload_bytes())
            || required_digest(&row.4)? != frame.provider_payload_digest()
            || row.5 != frame.received_at().unix_nanos()
            || row
                .6
                .map(u64::try_from)
                .transpose()
                .map_err(|_| CatalogError::CorruptCatalog)?
                != frame.source_sequence()
        {
            return Err(CatalogError::CorruptCatalog);
        }
        count = count.checked_add(1).ok_or(CatalogError::CorruptCatalog)?;
    }
    if count != claim.frames().len() {
        return Err(CatalogError::CorruptCatalog);
    }
    Ok(())
}

fn required_digest(bytes: &[u8]) -> Result<EvidenceDigest, CatalogError> {
    let bytes: [u8; 32] = bytes.try_into().map_err(|_| CatalogError::CorruptCatalog)?;
    if bytes == [0; 32] {
        return Err(CatalogError::CorruptCatalog);
    }
    Ok(EvidenceDigest::new(DigestAlgorithm::Sha256, bytes))
}

fn optional_digest(bytes: Option<&[u8]>) -> Result<Option<EvidenceDigest>, CatalogError> {
    bytes.map(required_digest).transpose()
}

const fn terminal_name(terminal: ProviderCaptureTerminalDisposition) -> &'static str {
    match terminal {
        ProviderCaptureTerminalDisposition::StandaloneResponse => "standalone_response",
        ProviderCaptureTerminalDisposition::ExhaustedWithoutNextPage => {
            "exhausted_without_next_page"
        }
        ProviderCaptureTerminalDisposition::CompleteRequestGraph => "complete_request_graph",
    }
}
