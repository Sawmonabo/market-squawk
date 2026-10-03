//! Pending original standalone responses in the existing capture catalog and raw store.
use super::*;
use crate::{DatasetId, RegisteredRightsGrant, SourceOperation};
use std::time::Instant;
use tokio_util::sync::CancellationToken;

pub(crate) const MAX_ORIGINAL_CONTEXT_BYTES: usize = 128 * 1024;

/// Constructor-private custody locator. It carries original facts, never live seal authority.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProviderCaptureOriginalReceipt {
    session: EvidenceDigest,
    ordinal: u16,
    expected_count: u16,
    dataset: DatasetId,
    context: Box<[u8]>,
    decoded_at: Timestamp,
    capture: ProviderCaptureSetReceipt,
    physical: PersistedProviderCapturePhysicalClaim,
    predecessor: Option<EvidenceDigest>,
    digest: EvidenceDigest,
    published: Option<EvidenceDigest>,
}
impl ProviderCaptureOriginalReceipt {
    /// Returns the exact original source-session identity.
    pub const fn session(&self) -> EvidenceDigest {
        self.session
    }
    /// Returns the immutable metadata or page ordinal.
    pub const fn ordinal(&self) -> u16 {
        self.ordinal
    }
    /// Returns the admitted complete capture count, including metadata.
    pub const fn expected_count(&self) -> u16 {
        self.expected_count
    }
    /// Returns the original analytical target.
    pub const fn dataset(&self) -> &DatasetId {
        &self.dataset
    }
    /// Returns source-owned original native and calendar coordinates.
    pub fn context(&self) -> &[u8] {
        &self.context
    }
    /// Returns the original strict-decoding clock.
    pub const fn decoded_at(&self) -> Timestamp {
        self.decoded_at
    }
    /// Returns the original source capture receipt.
    pub const fn capture(&self) -> &ProviderCaptureSetReceipt {
        &self.capture
    }
    /// Returns the original sole-store physical claim.
    pub const fn physical(&self) -> &PersistedProviderCapturePhysicalClaim {
        &self.physical
    }
    /// Returns the full immutable custody coordinate digest.
    pub const fn digest(&self) -> EvidenceDigest {
        self.digest
    }
    /// Returns the final binding after atomic publication or verified macro reobservation.
    pub const fn published_binding(&self) -> Option<EvidenceDigest> {
        self.published
    }
}

/// Discovers the sole unpublished source session on the caller's existing catalog connection.
pub(in crate::catalog) fn pending_session(
    connection: &Connection,
    source: &SourceId,
) -> Result<Option<EvidenceDigest>, CatalogError> {
    let mut statement=connection.prepare("SELECT DISTINCT original.session_digest FROM provider_capture_originals AS original JOIN provider_raw_observations AS observation ON observation.capture_observation_digest=original.capture_observation_digest WHERE original.ordinal=0 AND original.published_binding IS NULL AND original.published_option_binding IS NULL AND original.published_logical_binding IS NULL AND observation.source_id=?1 LIMIT 2")?;
    let mut rows = statement.query([source.as_str()])?;
    let Some(row) = rows.next()? else {
        return Ok(None);
    };
    let session = parse_digest(1, &row.get::<_, Vec<u8>>(0)?)?;
    if rows.next()?.is_some() {
        return Err(CatalogError::ProviderCaptureConflict);
    }
    Ok(Some(session))
}

impl Catalog {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn retain_provider_capture_original(
        &self,
        session: EvidenceDigest,
        ordinal: u16,
        expected_count: u16,
        dataset: &DatasetId,
        context: &[u8],
        decoded_at: Timestamp,
        token: ProviderWholeCaptureToken,
        grant: &RegisteredRightsGrant,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<ProviderCaptureOriginalReceipt, CatalogError> {
        self.market_recovery_read(deadline, cancellation, || {
            let transaction = self.connection.unchecked_transaction()?;
            let value = self.retain_provider_capture_original_in_transaction(
                &transaction,
                session,
                ordinal,
                expected_count,
                dataset,
                context,
                decoded_at,
                token,
                grant,
            )?;
            if cancellation.is_cancelled() {
                return Err(CatalogError::MarketRecoveryReadCancelled);
            }
            if Instant::now() >= deadline {
                return Err(CatalogError::MarketRecoveryReadDeadlineExceeded);
            }
            transaction.commit()?;
            Ok(value)
        })
    }

    /// Commits an entire bounded option-reference original graph or none of its custody rows.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn retain_option_contract_reference_originals(
        &self,
        session: EvidenceDigest,
        dataset: &DatasetId,
        context: &[u8],
        pages: Vec<(Timestamp, ProviderWholeCaptureToken, RegisteredRightsGrant)>,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<Vec<ProviderCaptureOriginalReceipt>, CatalogError> {
        self.market_recovery_read(deadline, cancellation, || {
            if pages.is_empty() || pages.len() > 32 {
                return Err(CatalogError::ProviderCaptureMismatch);
            }
            let mut bytes = 0_u64;
            for (_, token, _) in &pages {
                bytes = bytes
                    .checked_add(token.persisted_receipt().capture().total_body_bytes())
                    .filter(|bytes| *bytes <= 32 * 1024 * 1024)
                    .ok_or(CatalogError::ProviderCaptureMismatch)?;
            }
            let expected_count =
                u16::try_from(pages.len()).map_err(|_| CatalogError::ProviderCaptureMismatch)?;
            let transaction = self.connection.unchecked_transaction()?;
            let mut retained = Vec::with_capacity(pages.len());
            for (index, (decoded_at, token, grant)) in pages.into_iter().enumerate() {
                if cancellation.is_cancelled() {
                    return Err(CatalogError::MarketRecoveryReadCancelled);
                }
                if Instant::now() >= deadline {
                    return Err(CatalogError::MarketRecoveryReadDeadlineExceeded);
                }
                let ordinal =
                    u16::try_from(index).map_err(|_| CatalogError::ProviderCaptureMismatch)?;
                retained.push(self.retain_provider_capture_original_in_transaction(
                    &transaction,
                    session,
                    ordinal,
                    expected_count,
                    dataset,
                    if index == 0 { context } else { &[] },
                    decoded_at,
                    token,
                    &grant,
                )?);
            }
            if cancellation.is_cancelled() {
                return Err(CatalogError::MarketRecoveryReadCancelled);
            }
            if Instant::now() >= deadline {
                return Err(CatalogError::MarketRecoveryReadDeadlineExceeded);
            }
            transaction.commit()?;
            Ok(retained)
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn retain_provider_capture_original_in_transaction(
        &self,
        transaction: &rusqlite::Transaction<'_>,
        session: EvidenceDigest,
        ordinal: u16,
        expected_count: u16,
        dataset: &DatasetId,
        context: &[u8],
        decoded_at: Timestamp,
        token: ProviderWholeCaptureToken,
        grant: &RegisteredRightsGrant,
    ) -> Result<ProviderCaptureOriginalReceipt, CatalogError> {
        if session.bytes() == [0; 32]
            || expected_count == 0
            || ordinal >= expected_count
            || context.len() > MAX_ORIGINAL_CONTEXT_BYTES
            || (ordinal == 0) != !context.is_empty()
            || grant.catalog_id != self.catalog_id
        {
            return Err(CatalogError::ProviderCaptureMismatch);
        }
        let prepared = ProviderMacroPlanCompletionCapture::try_from_live(token)?;
        let capture = prepared.capture;
        if grant.payload_digest() != capture.observation_digest()
            || capture.pages()[0].received_at() > decoded_at
        {
            return Err(CatalogError::InvalidRightsCapability);
        }
        let now = super::super::storage::trusted_catalog_now(transaction)?;
        if decoded_at > now {
            return Err(CatalogError::PublicationTimeConflict);
        }
        let admitted: bool = transaction.query_row(
                "SELECT EXISTS(SELECT 1 FROM source_rights WHERE rights_id=?1 AND source_id=?2
                 AND payload_algorithm=1 AND payload_digest=?3 AND (operation_mask & ?4)<>0
                 AND admitted_at_ns<=?5 AND (authorization_expires_at_ns IS NULL OR authorization_expires_at_ns>?5))",
                params![grant.rights_id(),capture.source_id().as_str(),capture.observation_digest().bytes(),
                    i64::from(SourceOperation::Persist.mask()),now.unix_nanos()],|row| row.get(0))?;
        if !admitted {
            return Err(CatalogError::InvalidRightsCapability);
        }
        let previous = if ordinal == 0 {
            None
        } else {
            let previous = load(transaction, session, ordinal - 1)?
                .ok_or(CatalogError::ProviderCaptureConflict)?;
            if previous.expected_count != expected_count
                || previous.dataset != *dataset
                || previous.capture.source_id() != capture.source_id()
                || previous.capture.metadata_revision() != capture.metadata_revision()
                || previous.published.is_some()
                || previous.decoded_at > decoded_at
            {
                return Err(CatalogError::ProviderCaptureConflict);
            }
            Some(previous.digest)
        };
        let mut value = ProviderCaptureOriginalReceipt {
            session,
            ordinal,
            expected_count,
            dataset: dataset.clone(),
            context: context.into(),
            decoded_at,
            capture,
            physical: prepared.physical_claim,
            predecessor: previous,
            digest: EvidenceDigest::new(DigestAlgorithm::Sha256, [0; 32]),
            published: None,
        };
        value.digest = original_digest(&value)?;
        if let Some(existing) = load(transaction, session, ordinal)? {
            if existing != value {
                return Err(CatalogError::ProviderCaptureConflict);
            }
            return Ok(existing);
        }
        validate_source_revision(transaction, &value.capture)?;
        require_physical_claim_capacity(transaction, std::slice::from_ref(&value.physical))?;
        insert_raw_observation(
            transaction,
            &value.capture,
            std::slice::from_ref(&value.physical),
            "whole_single_segment",
            now,
        )?;
        transaction.execute(
            "INSERT INTO provider_capture_originals
                (session_digest,ordinal,expected_count,dataset_id,context_bytes,decoded_at_ns,
                 capture_observation_digest,raw_claim_digest,physical_receipt_digest,
                 predecessor_digest,original_digest,rights_id,retained_at_ns,published_binding)
                VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,NULL)",
            params![
                session.bytes(),
                i64::from(ordinal),
                i64::from(expected_count),
                dataset.as_str(),
                context,
                decoded_at.unix_nanos(),
                value.capture.observation_digest().bytes(),
                value.physical.raw_claim_digest.bytes(),
                value.physical.claim.physical_receipt_digest().bytes(),
                previous.map(EvidenceDigest::bytes),
                value.digest.bytes(),
                grant.rights_id(),
                now.unix_nanos()
            ],
        )?;
        let exact =
            load(transaction, session, ordinal)?.ok_or(CatalogError::ProviderCaptureConflict)?;
        if exact != value {
            return Err(CatalogError::ProviderCaptureConflict);
        }
        append_audit(
            transaction,
            "provider-capture-original.retained",
            dataset.as_str(),
            value.digest.bytes(),
            now,
        )?;
        Ok(value)
    }
}

fn original_digest(value: &ProviderCaptureOriginalReceipt) -> Result<EvidenceDigest, CatalogError> {
    let mut hash = Sha256::new();
    for bytes in [
        b"market-squawk/provider-capture-original/v1".as_slice(),
        &value.session.bytes(),
        &value.ordinal.to_be_bytes(),
        &value.expected_count.to_be_bytes(),
        value.dataset.as_str().as_bytes(),
        value.context.as_ref(),
        &value.decoded_at.unix_nanos().to_be_bytes(),
        &value.capture.observation_digest().bytes(),
        &value.physical.raw_claim_digest.bytes(),
        &value.physical.sealed_capture_receipt_digest.bytes(),
        &value.predecessor.map_or([0; 32], EvidenceDigest::bytes),
    ] {
        hash_field(&mut hash, bytes)?;
    }
    Ok(EvidenceDigest::new(
        DigestAlgorithm::Sha256,
        hash.finalize().into(),
    ))
}
pub(in crate::catalog) fn load(
    connection: &Connection,
    session: EvidenceDigest,
    ordinal: u16,
) -> Result<Option<ProviderCaptureOriginalReceipt>, CatalogError> {
    load_original(connection, session, ordinal, true)
}

// Whole-session consumers verify the ordered pack once after reading every original.
fn load_original(
    connection: &Connection,
    session: EvidenceDigest,
    ordinal: u16,
    verify_pack: bool,
) -> Result<Option<ProviderCaptureOriginalReceipt>, CatalogError> {
    let exists: bool = connection.query_row("SELECT EXISTS(SELECT 1 FROM provider_capture_originals WHERE session_digest=?1 AND ordinal=?2)",params![session.bytes(),i64::from(ordinal)],|row|row.get(0))?;
    let raw = connection.query_row("SELECT original.expected_count,original.dataset_id,original.context_bytes,original.decoded_at_ns,
        observation.capture_json,object.raw_claim_digest,object.object_capture_content_digest,object.object_capture_observation_digest,
        object.capture_receipt_digest,raw.raw_claim_json,original.predecessor_digest,original.original_digest,COALESCE(original.published_binding,original.published_option_binding,original.published_logical_binding)
        FROM provider_capture_originals AS original JOIN provider_raw_observations AS observation
          ON observation.capture_observation_digest=original.capture_observation_digest
        JOIN provider_raw_observation_objects AS object ON object.capture_observation_digest=observation.capture_observation_digest AND object.input_ordinal=0
          AND object.raw_claim_digest=original.raw_claim_digest AND object.physical_receipt_digest=original.physical_receipt_digest
        JOIN sealed_raw_objects AS raw ON raw.raw_claim_digest=object.raw_claim_digest AND raw.raw_claim_kind='journal_segment'
        WHERE original.session_digest=?1 AND original.ordinal=?2",
        params![session.bytes(),i64::from(ordinal)],|row| Ok((row.get::<_,u16>(0)?,row.get::<_,String>(1)?,row.get::<_,Vec<u8>>(2)?,row.get::<_,i64>(3)?,row.get::<_,String>(4)?,row.get::<_,Vec<u8>>(5)?,row.get::<_,Vec<u8>>(6)?,row.get::<_,Vec<u8>>(7)?,row.get::<_,Vec<u8>>(8)?,row.get::<_,String>(9)?,row.get::<_,Option<Vec<u8>>>(10)?,row.get::<_,Vec<u8>>(11)?,row.get::<_,Option<Vec<u8>>>(12)?))).optional()?;
    let Some((
        count,
        dataset,
        context,
        decoded,
        capture,
        claim_digest,
        content,
        observation,
        sealed,
        claim,
        predecessor,
        digest,
        published,
    )) = raw
    else {
        if exists {
            return Err(CatalogError::CorruptCatalog);
        }
        return Ok(None);
    };
    if context.len() > MAX_ORIGINAL_CONTEXT_BYTES
        || capture.len() > MAX_PROVIDER_CLAIM_JSON_BYTES
        || claim.len() > MAX_PROVIDER_CLAIM_JSON_BYTES
        || count == 0
        || ordinal >= count
        || (ordinal == 0) != !context.is_empty()
    {
        return Err(CatalogError::CorruptCatalog);
    }
    let capture: ProviderCaptureSetReceipt = serde_json::from_str(&capture)?;
    if capture.pages().len() != 1 {
        return Err(CatalogError::CorruptCatalog);
    }
    let claim = parse_journal_claim(&claim)?;
    let physical = PersistedProviderCapturePhysicalClaim {
        raw_claim_digest: parse_digest(1, &claim_digest)?,
        capture_content_digest: parse_digest(1, &content)?,
        capture_observation_digest: parse_digest(1, &observation)?,
        sealed_capture_receipt_digest: parse_digest(1, &sealed)?,
        claim,
    };
    let value = ProviderCaptureOriginalReceipt {
        session,
        ordinal,
        expected_count: count,
        dataset: DatasetId::try_from(dataset.as_str()).map_err(|_| CatalogError::CorruptCatalog)?,
        context: context.into_boxed_slice(),
        decoded_at: Timestamp::from_unix_nanos(decoded),
        capture,
        physical,
        predecessor: predecessor
            .map(|bytes| parse_digest(1, &bytes))
            .transpose()?,
        digest: parse_digest(1, &digest)?,
        published: published.map(|bytes| parse_digest(1, &bytes)).transpose()?,
    };
    if original_digest(&value)? != value.digest
        || raw_claim_digest(journal_claim_json(&value.physical.claim)?.as_bytes())
            != value.physical.raw_claim_digest
        || value.capture.content_digest() != value.physical.capture_content_digest
        || value.capture.observation_digest() != value.physical.capture_observation_digest
        || value.capture.pages().len() != 1
        || value.capture.terminal() != ProviderCaptureTerminalDisposition::StandaloneResponse
        || value.capture.pages()[0].received_at() > value.decoded_at
        || (ordinal == 0) != value.predecessor.is_none()
    {
        return Err(CatalogError::CorruptCatalog);
    }
    // Validate the retained relational custody as well as each content digest. This also runs
    // during backup evidence enumeration, where malformed joins must never disappear as absence.
    // A reobservation retains a separate fresh physical binding. Its committed audit relationship
    // must resolve to the same source/content and original run/target; it is never inserted into
    // that immutable generation's creating inputs.
    let custody: bool = connection.query_row("SELECT EXISTS(
        SELECT 1 FROM provider_capture_originals AS original
        JOIN provider_raw_observations AS observation ON observation.capture_observation_digest=original.capture_observation_digest
        JOIN source_rights AS rights ON rights.rights_id=original.rights_id
        WHERE original.session_digest=?1 AND original.ordinal=?2
         AND rights.source_id=observation.source_id AND rights.payload_algorithm=1
         AND rights.payload_digest=original.capture_observation_digest AND (rights.operation_mask & 4)<>0
         AND rights.admitted_at_ns<=original.retained_at_ns
         AND (rights.authorization_expires_at_ns IS NULL OR rights.authorization_expires_at_ns>original.retained_at_ns)
         AND original.decoded_at_ns<=original.retained_at_ns
         AND (original.ordinal=0 OR EXISTS(SELECT 1 FROM provider_capture_originals AS previous
          JOIN provider_raw_observations AS previous_observation ON previous_observation.capture_observation_digest=previous.capture_observation_digest
          WHERE previous.session_digest=original.session_digest AND previous.ordinal=original.ordinal-1
           AND previous.original_digest=original.predecessor_digest AND previous.expected_count=original.expected_count
           AND previous.dataset_id=original.dataset_id AND previous.decoded_at_ns<=original.decoded_at_ns
           AND COALESCE(previous.published_binding,previous.published_option_binding,previous.published_logical_binding) IS COALESCE(original.published_binding,original.published_option_binding,original.published_logical_binding)
           AND previous_observation.source_id=observation.source_id
           AND previous_observation.metadata_revision=observation.metadata_revision
           AND previous_observation.source_revision_digest=observation.source_revision_digest))
         AND (COALESCE(original.published_binding,original.published_option_binding,original.published_logical_binding) IS NULL OR original.expected_count=(SELECT COUNT(*) FROM provider_capture_originals AS member WHERE member.session_digest=original.session_digest AND COALESCE(member.published_binding,member.published_option_binding,member.published_logical_binding)=COALESCE(original.published_binding,original.published_option_binding,original.published_logical_binding)))
         AND (COALESCE(original.published_binding,original.published_option_binding,original.published_logical_binding) IS NULL OR EXISTS(
          SELECT 1 FROM ingest_run_provider_capture_bindings AS binding
          JOIN dataset_manifests AS manifest ON manifest.run_id=binding.run_id
          JOIN provider_capture_binding_objects AS object ON object.binding_digest=binding.binding_digest
          WHERE binding.binding_digest=COALESCE(original.published_binding,original.published_option_binding,original.published_logical_binding) AND manifest.dataset_name=original.dataset_id
           AND object.raw_claim_digest=original.raw_claim_digest AND object.physical_receipt_digest=original.physical_receipt_digest)
          OR EXISTS(
           SELECT 1 FROM provider_option_market_bindings AS option_binding
           JOIN ingest_run_provider_publication_bindings AS input ON input.option_binding_digest=option_binding.option_binding_digest
           JOIN dataset_manifests AS manifest ON manifest.run_id=input.run_id
           JOIN json_each(option_binding.reference_dependencies_json) AS reference
           JOIN provider_capture_metadata_dependencies AS dependency
             ON lower(hex(dependency.dependency_digest))=json_extract(reference.value,'$.dependency_digest')
           WHERE option_binding.option_binding_digest=COALESCE(original.published_binding,original.published_option_binding,original.published_logical_binding)
             AND manifest.dataset_name=original.dataset_id
             AND dependency.capture_observation_digest=original.capture_observation_digest
             AND dependency.raw_claim_digest=original.raw_claim_digest
             AND dependency.physical_receipt_digest=original.physical_receipt_digest
             AND option_binding.recorded_at_ns>=original.retained_at_ns)
          OR EXISTS(
           SELECT 1 FROM provider_logical_publication_bindings AS logical
           JOIN ingest_run_provider_publication_bindings AS input ON input.logical_binding_digest=logical.binding_digest
           JOIN dataset_manifests AS manifest ON manifest.run_id=input.run_id
           WHERE original.published_logical_binding=logical.binding_digest
             AND input.publication_kind='provider_logical' AND input.source_id=observation.source_id
             AND logical.source_id=observation.source_id AND manifest.dataset_name=original.dataset_id
             AND logical.recorded_at_ns>=original.retained_at_ns)
          OR EXISTS(
           SELECT 1 FROM audit_events AS replay
           JOIN ingest_runs AS run ON run.run_id=replay.subject_id AND run.state='succeeded' AND run.operation='persist'
           JOIN ingest_run_provider_capture_bindings AS input ON input.run_id=run.run_id AND input.input_ordinal=0
           JOIN dataset_manifests AS manifest ON manifest.run_id=run.run_id
           JOIN provider_capture_bindings AS previous_binding ON previous_binding.binding_digest=input.binding_digest
           JOIN provider_raw_observations AS previous_capture ON previous_capture.capture_observation_digest=previous_binding.capture_observation_digest
           JOIN provider_capture_bindings AS fresh_binding ON fresh_binding.binding_digest=COALESCE(original.published_binding,original.published_option_binding,original.published_logical_binding)
           JOIN provider_raw_observations AS fresh_capture ON fresh_capture.capture_observation_digest=fresh_binding.capture_observation_digest
           JOIN provider_capture_binding_objects AS object ON object.binding_digest=fresh_binding.binding_digest
           WHERE replay.event_type='provider-capture-binding.reobserved' AND replay.details_digest=fresh_binding.binding_digest
            AND replay.occurred_at_ns=fresh_binding.recorded_at_ns AND replay.occurred_at_ns>=original.retained_at_ns
            AND manifest.dataset_name=original.dataset_id
            AND (SELECT COUNT(*) FROM ingest_run_provider_capture_bindings AS member WHERE member.run_id=run.run_id)=1
            AND run.source_id=fresh_capture.source_id AND fresh_capture.source_id=previous_capture.source_id
            AND fresh_capture.source_id=observation.source_id AND fresh_capture.metadata_revision=observation.metadata_revision
            AND fresh_capture.metadata_revision=previous_capture.metadata_revision
            AND fresh_capture.source_revision_digest=previous_capture.source_revision_digest
            AND fresh_capture.provider_dataset=previous_capture.provider_dataset
            AND fresh_capture.capture_content_digest=previous_capture.capture_content_digest
            AND run.payload_algorithm=1 AND run.payload_digest=fresh_capture.capture_content_digest
            AND fresh_binding.canonical_record_count=previous_binding.canonical_record_count
            AND object.raw_claim_digest=original.raw_claim_digest AND object.physical_receipt_digest=original.physical_receipt_digest)))",
        params![session.bytes(),i64::from(ordinal)],|row|row.get(0))?;
    if !custody {
        return Err(CatalogError::CorruptCatalog);
    }
    let logical: Option<Vec<u8>> = connection.query_row(
        "SELECT published_logical_binding FROM provider_capture_originals WHERE session_digest=?1 AND ordinal=?2",
        params![session.bytes(), i64::from(ordinal)], |row| row.get(0))?;
    if let Some(logical) = logical.filter(|_| verify_pack) {
        validate_persisted_pack_identity(connection, session, count, parse_digest(1, &logical)?)?;
    }
    Ok(Some(value))
}

/// Called inside the existing run/artifact/manifest transaction after its capture binding insert.
/// Every original must appear exactly once and in order. The compare-and-set permits one owner.
pub(super) fn consume_for_publication(
    connection: &Connection,
    evidence: &PersistedProviderCaptureBindingEvidence,
) -> Result<(), CatalogError> {
    let mut sessions = BTreeSet::new();
    for physical in &evidence.physical_claims {
        let mut statement=connection.prepare("SELECT DISTINCT session_digest FROM provider_capture_originals WHERE raw_claim_digest=?1 LIMIT 2")?;
        let rows = statement.query_map([physical.raw_claim_digest.bytes()], |row| {
            row.get::<_, Vec<u8>>(0)
        })?;
        for row in rows {
            sessions.insert(parse_digest(1, &row?)?.bytes());
        }
    }
    if sessions.is_empty() {
        return Ok(());
    }
    if sessions.len() != 1 {
        return Err(CatalogError::ProviderCaptureConflict);
    }
    let session = EvidenceDigest::new(
        DigestAlgorithm::Sha256,
        *sessions
            .first()
            .ok_or(CatalogError::ProviderCaptureConflict)?,
    );
    let first = load(connection, session, 0)?.ok_or(CatalogError::ProviderCaptureConflict)?;
    if usize::from(first.expected_count) != evidence.physical_claims.len() {
        return Err(CatalogError::ProviderCaptureConflict);
    }
    let mut predecessor = None;
    for (ordinal, physical) in evidence.physical_claims.iter().enumerate() {
        let original = load(
            connection,
            session,
            u16::try_from(ordinal).map_err(|_| CatalogError::ProviderCaptureConflict)?,
        )?
        .ok_or(CatalogError::ProviderCaptureConflict)?;
        if original.published.is_some()
            || original.physical != *physical
            || original.predecessor != predecessor
            || original.capture.source_id() != evidence.capture.source_id()
            || original.expected_count != first.expected_count
            || original.dataset != first.dataset
            || original.capture.metadata_revision() != first.capture.metadata_revision()
        {
            return Err(CatalogError::ProviderCaptureConflict);
        }
        predecessor = Some(original.digest);
    }
    let changed=connection.execute("UPDATE provider_capture_originals SET published_binding=?1 WHERE session_digest=?2 AND published_binding IS NULL",
        params![evidence.binding_digest.bytes(),session.bytes()])?;
    if changed != usize::from(first.expected_count) {
        return Err(CatalogError::ProviderCaptureConflict);
    }
    Ok(())
}

/// Option-specific dependency admission. Macro's same-dataset metadata relationship is unchanged.
/// The ordered complete original custody session must be consumed by this precise option target.
pub(in crate::catalog) fn validate_option_dependencies(
    connection: &Connection,
    evidence: &super::super::provider_option::PersistedProviderOptionMarketBindingEvidence,
    admitted_at: Timestamp,
    analytical_dataset: Option<&str>,
    published: bool,
) -> Result<(), CatalogError> {
    let dependencies = evidence.reference_dependencies();
    if dependencies.is_empty() {
        return Ok(());
    }
    let dataset = if published {
        let mut statement = connection.prepare("SELECT DISTINCT manifest.dataset_name
            FROM ingest_run_provider_publication_bindings AS input JOIN dataset_manifests AS manifest ON manifest.run_id=input.run_id
            WHERE input.option_binding_digest=?1 LIMIT 2")?;
        let mut rows = statement.query([evidence.binding_digest().bytes()])?;
        let value: String = rows
            .next()?
            .ok_or(CatalogError::ProviderCaptureConflict)?
            .get(0)?;
        if rows.next()?.is_some() {
            return Err(CatalogError::ProviderCaptureConflict);
        }
        value
    } else {
        analytical_dataset
            .ok_or(CatalogError::ProviderCaptureConflict)?
            .to_owned()
    };
    let admitted_at = if published {
        Timestamp::from_unix_nanos(connection.query_row(
            "SELECT recorded_at_ns FROM provider_option_market_bindings WHERE option_binding_digest=?1",
            [evidence.binding_digest().bytes()], |row| row.get(0))?)
    } else {
        admitted_at
    };
    let mut session = None;
    let mut predecessor = None;
    for (ordinal, dependency) in dependencies.iter().enumerate() {
        let mut statement = connection.prepare(
            "SELECT session_digest, ordinal FROM provider_capture_originals
            WHERE raw_claim_digest=?1 AND physical_receipt_digest=?2 LIMIT 2",
        )?;
        let mut rows = statement.query(params![
            dependency.physical().raw_claim_digest().bytes(),
            dependency
                .physical()
                .claim()
                .physical_receipt_digest()
                .bytes()
        ])?;
        let row = rows.next()?.ok_or(CatalogError::ProviderCaptureConflict)?;
        let selected_session = parse_digest(1, &row.get::<_, Vec<u8>>(0)?)?;
        let selected_ordinal: u16 = row.get(1)?;
        if rows.next()?.is_some()
            || usize::from(selected_ordinal) != ordinal
            || session.is_some_and(|session| session != selected_session)
        {
            return Err(CatalogError::ProviderCaptureConflict);
        }
        session = Some(selected_session);
        let original = load(connection, selected_session, selected_ordinal)?
            .ok_or(CatalogError::ProviderCaptureConflict)?;
        if usize::from(original.expected_count) != dependencies.len()
            || original.dataset.as_str() != dataset
            || original.capture != *dependency.capture()
            || original.physical != *dependency.physical()
            || original.predecessor != predecessor
            || original.capture.source_id() != evidence.capture().source_id()
            || original.capture.metadata_revision() != evidence.capture().metadata_revision()
            || original.decoded_at > evidence.capture().pages()[0].received_at()
            || (published && original.published != Some(evidence.binding_digest()))
            || (!published && original.published.is_some())
        {
            return Err(CatalogError::ProviderCaptureConflict);
        }
        {
            let rights: bool = connection.query_row("SELECT EXISTS(SELECT 1 FROM provider_capture_originals AS original
                JOIN source_rights AS rights ON rights.rights_id=original.rights_id
                WHERE original.session_digest=?1 AND original.ordinal=?2 AND rights.source_id=?3
                 AND rights.payload_algorithm=1 AND rights.payload_digest=?4 AND (rights.operation_mask & 4)<>0
                 AND rights.admitted_at_ns<=?5 AND (rights.authorization_expires_at_ns IS NULL OR rights.authorization_expires_at_ns>?5))",
                params![selected_session.bytes(),i64::from(selected_ordinal),original.capture.source_id().as_str(),
                    original.capture.observation_digest().bytes(),admitted_at.unix_nanos()],|row| row.get(0))?;
            if !rights {
                return Err(CatalogError::InvalidRightsCapability);
            }
        }
        predecessor = Some(original.digest);
    }
    Ok(())
}

/// Compare-and-set the original reference session inside the option run's publication transaction.
pub(in crate::catalog) fn consume_option_dependencies(
    connection: &Connection,
    evidence: &super::super::provider_option::PersistedProviderOptionMarketBindingEvidence,
) -> Result<(), CatalogError> {
    let Some(first) = evidence.reference_dependencies().first() else {
        return Ok(());
    };
    let session: Vec<u8> = connection.query_row("SELECT session_digest FROM provider_capture_originals
        WHERE raw_claim_digest=?1 AND physical_receipt_digest=?2 AND ordinal=0 AND published_binding IS NULL AND published_option_binding IS NULL AND published_logical_binding IS NULL",
        params![first.physical().raw_claim_digest().bytes(),first.physical().claim().physical_receipt_digest().bytes()],|row| row.get(0))?;
    let changed = connection.execute(
        "UPDATE provider_capture_originals SET published_option_binding=?1
        WHERE session_digest=?2 AND published_binding IS NULL AND published_option_binding IS NULL AND published_logical_binding IS NULL",
        params![evidence.binding_digest().bytes(), session],
    )?;
    if changed != evidence.reference_dependencies().len() {
        return Err(CatalogError::ProviderCaptureConflict);
    }
    Ok(())
}

/// Consumes the exact independently sealed original session in the logical publication transaction.
pub(in crate::catalog) fn consume_for_logical_publication(
    connection: &Connection,
    dataset: &DatasetId,
    binding: &market_squawk_sources::SealedProviderLogicalPublicationBinding,
    pack: &market_squawk_sources::ProviderCapturePackSeal,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<(), CatalogError> {
    let session = validate_live_pack_session(
        connection,
        dataset,
        binding,
        pack,
        false,
        deadline,
        cancellation,
    )?;
    let changed = connection.execute(
        "UPDATE provider_capture_originals SET published_logical_binding=?1 WHERE session_digest=?2
         AND published_binding IS NULL AND published_option_binding IS NULL AND published_logical_binding IS NULL",
        params![binding.binding_digest().bytes(), session.bytes()])?;
    if changed as u64 != pack.capture_count() {
        return Err(CatalogError::ProviderCaptureConflict);
    }
    Ok(())
}

pub(in crate::catalog) fn logical_publication_matches(
    connection: &Connection,
    dataset: &DatasetId,
    binding: &market_squawk_sources::SealedProviderLogicalPublicationBinding,
    pack: &market_squawk_sources::ProviderCapturePackSeal,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<bool, CatalogError> {
    validate_live_pack_session(
        connection,
        dataset,
        binding,
        pack,
        true,
        deadline,
        cancellation,
    )
    .map(|_| true)
}

#[allow(clippy::too_many_arguments)]
fn validate_live_pack_session(
    connection: &Connection,
    dataset: &DatasetId,
    binding: &market_squawk_sources::SealedProviderLogicalPublicationBinding,
    pack: &market_squawk_sources::ProviderCapturePackSeal,
    published: bool,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<EvidenceDigest, CatalogError> {
    let first_capture = pack.first_capture();
    let mut lookup = connection.prepare(
        "SELECT session_digest FROM provider_capture_originals WHERE ordinal=0
         AND capture_observation_digest=?1 AND physical_receipt_digest=?2 LIMIT 2",
    )?;
    let mut rows = lookup.query(params![
        first_capture.capture().observation_digest().bytes(),
        first_capture.segment().physical_receipt_digest().bytes()
    ])?;
    let first = rows.next()?.ok_or(CatalogError::ProviderCaptureConflict)?;
    let session = parse_digest(1, &first.get::<_, Vec<u8>>(0)?)?;
    if rows.next()?.is_some() {
        return Err(CatalogError::ProviderCaptureConflict);
    }
    drop(rows);
    drop(lookup);
    let expected =
        u16::try_from(pack.capture_count()).map_err(|_| CatalogError::ProviderCaptureConflict)?;
    if expected == 0
        || pack.source_id() != binding.terminal().source_id()
        || binding
            .objects()
            .get(pack.logical_ordinal() as usize)
            .is_none_or(|object| {
                object.role() != market_squawk_sources::LogicalObjectRole::ProviderPayload
                    || object.object() != pack.object()
                    || object.semantic_identity() != pack.captures_digest()
            })
    {
        return Err(CatalogError::ProviderCaptureConflict);
    }
    let mut identity = market_squawk_sources::ProviderCapturePackAccumulator::new();
    let mut predecessor = None;
    for ordinal in 0..expected {
        if cancellation.is_cancelled() {
            return Err(CatalogError::MarketRecoveryReadCancelled);
        }
        if Instant::now() >= deadline {
            return Err(CatalogError::MarketRecoveryReadDeadlineExceeded);
        }
        let original = load_original(connection, session, ordinal, false)?
            .ok_or(CatalogError::ProviderCaptureConflict)?;
        if original.expected_count != expected
            || &original.dataset != dataset
            || original.capture.source_id() != pack.source_id()
            || original.predecessor != predecessor
            || original.published != published.then_some(binding.binding_digest())
        {
            return Err(CatalogError::ProviderCaptureConflict);
        }
        identity
            .push_evidence(
                original.physical.sealed_capture_receipt_digest,
                original.capture.pages().len() as u64,
                original.capture.total_body_bytes(),
            )
            .map_err(|_| CatalogError::ProviderCaptureConflict)?;
        predecessor = Some(original.digest);
    }
    if identity.finish() != pack.captures_digest() {
        return Err(CatalogError::ProviderCaptureConflict);
    }
    Ok(session)
}

// A direct indexed custody read must also reject a transplanted logical target. Hash only compact
// scalar receipt fields, never decode or retain the complete session's contexts or response bodies.
fn validate_persisted_pack_identity(
    connection: &Connection,
    session: EvidenceDigest,
    expected: u16,
    binding: EvidenceDigest,
) -> Result<(), CatalogError> {
    let mut statement = connection.prepare(
        "SELECT original.ordinal,object.capture_receipt_digest,observation.page_count,observation.total_body_bytes
         FROM provider_capture_originals AS original
         JOIN provider_raw_observations AS observation ON observation.capture_observation_digest=original.capture_observation_digest
         JOIN provider_raw_observation_objects AS object ON object.capture_observation_digest=original.capture_observation_digest
           AND object.raw_claim_digest=original.raw_claim_digest AND object.physical_receipt_digest=original.physical_receipt_digest
           AND object.input_ordinal=0
         WHERE original.session_digest=?1 AND original.published_logical_binding=?2 ORDER BY original.ordinal")?;
    let mut rows = statement.query(params![session.bytes(), binding.bytes()])?;
    let mut identity = market_squawk_sources::ProviderCapturePackAccumulator::new();
    while let Some(row) = rows.next()? {
        let ordinal =
            u64::try_from(row.get::<_, i64>(0)?).map_err(|_| CatalogError::CorruptCatalog)?;
        let body_count =
            u64::try_from(row.get::<_, i64>(2)?).map_err(|_| CatalogError::CorruptCatalog)?;
        let body_bytes =
            u64::try_from(row.get::<_, i64>(3)?).map_err(|_| CatalogError::CorruptCatalog)?;
        if ordinal != identity.capture_count() || identity.capture_count() >= u64::from(expected) {
            return Err(CatalogError::CorruptCatalog);
        }
        identity
            .push_evidence(
                parse_digest(1, &row.get::<_, Vec<u8>>(1)?)?,
                body_count,
                body_bytes,
            )
            .map_err(|_| CatalogError::CorruptCatalog)?;
    }
    if identity.capture_count() != u64::from(expected) {
        return Err(CatalogError::CorruptCatalog);
    }
    let packed_identity = identity.finish();
    let retained = super::super::provider_logical::load_provider_logical_publication_binding(
        connection, binding,
    )?
    .ok_or(CatalogError::CorruptCatalog)?;
    if retained.objects().first().is_none_or(|object| {
        object.ordinal() != 0
            || object.role() != market_squawk_sources::LogicalObjectRole::ProviderPayload
            || object.semantic_identity() != packed_identity
    }) {
        return Err(CatalogError::CorruptCatalog);
    }
    Ok(())
}
