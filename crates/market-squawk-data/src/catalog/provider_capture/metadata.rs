//! Original metadata capture dependencies of canonical publication inputs.
use super::*;
use market_squawk_sources::SealedProviderCaptureSetReceipt;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ProviderMetadataCaptureEvidence {
    pub(crate) digest: EvidenceDigest,
    pub(crate) capture: ProviderCaptureSetReceipt,
    pub(crate) physical: PersistedProviderCapturePhysicalClaim,
}

impl ProviderMetadataCaptureEvidence {
    pub(crate) fn from_receipt(
        receipt: &SealedProviderCaptureSetReceipt,
    ) -> Result<Self, CatalogError> {
        let capture = receipt.capture().clone();
        let claim = receipt.segment().claim().clone();
        let json = journal_claim_json(&claim)?;
        if json.len() > MAX_PROVIDER_CLAIM_JSON_BYTES {
            return Err(CatalogError::ResultByteLimitExceeded);
        }
        let mut value = Self {
            digest: EvidenceDigest::new(DigestAlgorithm::Sha256, [0; 32]),
            physical: PersistedProviderCapturePhysicalClaim {
                raw_claim_digest: raw_claim_digest(json.as_bytes()),
                capture_content_digest: capture.content_digest(),
                capture_observation_digest: capture.observation_digest(),
                sealed_capture_receipt_digest: receipt.receipt_digest(),
                claim,
            },
            capture,
        };
        value.digest = value.expected_digest();
        value.validate()?;
        Ok(value)
    }

    pub(crate) fn retained_bytes(&self) -> Result<usize, CatalogError> {
        let capture_bytes = serde_json::to_vec(&self.capture)?.len();
        let claim_bytes = journal_claim_json(&self.physical.claim)?.len();
        if capture_bytes > MAX_PROVIDER_CLAIM_JSON_BYTES
            || claim_bytes > MAX_PROVIDER_CLAIM_JSON_BYTES
        {
            return Err(CatalogError::ResultByteLimitExceeded);
        }
        capture_bytes
            .checked_add(claim_bytes)
            .and_then(|bytes| bytes.checked_add(std::mem::size_of::<Self>()))
            .ok_or(CatalogError::ResultByteLimitExceeded)
    }

    fn expected_digest(&self) -> EvidenceDigest {
        let mut hash = Sha256::new();
        hash.update(b"market-squawk/provider-capture-metadata-dependency/v1\0");
        hash.update(self.capture.observation_digest().bytes());
        hash.update(self.physical.raw_claim_digest.bytes());
        hash.update(self.physical.sealed_capture_receipt_digest.bytes());
        hash.update(self.physical.claim.physical_receipt_digest().bytes());
        EvidenceDigest::new(DigestAlgorithm::Sha256, hash.finalize().into())
    }

    pub(crate) fn validate(&self) -> Result<(), CatalogError> {
        self.retained_bytes()?;
        let json = journal_claim_json(&self.physical.claim)?;
        if json.len() > MAX_PROVIDER_CLAIM_JSON_BYTES
            || self.digest != self.expected_digest()
            || raw_claim_digest(json.as_bytes()) != self.physical.raw_claim_digest
            || self.capture.content_digest() != self.physical.capture_content_digest
            || self.capture.observation_digest() != self.physical.capture_observation_digest
            || self.capture.pages().len() != self.physical.claim.frames().len()
        {
            return Err(CatalogError::ProviderCaptureMismatch);
        }
        ProviderCapturePhysicalClaimEvidenceRef::try_new(
            self.capture.content_digest(),
            self.capture.observation_digest(),
            self.physical.sealed_capture_receipt_digest,
            &self.physical.claim,
        )
        .map_err(|_| CatalogError::ProviderCaptureMismatch)?;
        for (page, frame) in self
            .capture
            .pages()
            .iter()
            .zip(self.physical.claim.frames())
        {
            if frame.ordinal() != u32::from(page.ordinal())
                || frame.source_sequence() != Some(u64::from(page.ordinal()))
                || frame.received_at() != page.received_at()
                || frame.provider_payload_digest() != page.body_digest()
                || frame.provider_payload_bytes() != page.body_bytes()
            {
                return Err(CatalogError::ProviderCaptureMismatch);
            }
        }
        Ok(())
    }

    pub(crate) fn validate_data(
        &self,
        data: &ProviderCaptureSetReceipt,
    ) -> Result<(), CatalogError> {
        self.validate()?;
        if self.capture.source_id() != data.source_id()
            || self.capture.metadata_revision() != data.metadata_revision()
            || self.capture.dataset() != data.dataset()
            || self.capture.observation_digest() == data.observation_digest()
            || self
                .capture
                .pages()
                .iter()
                .map(ProviderCapturePageReceipt::received_at)
                .max()
                > data
                    .pages()
                    .iter()
                    .map(ProviderCapturePageReceipt::received_at)
                    .min()
        {
            return Err(CatalogError::ProviderCaptureMismatch);
        }
        Ok(())
    }
}

pub(in crate::catalog) fn retain(
    connection: &Connection,
    value: &ProviderMetadataCaptureEvidence,
    recorded_at: Timestamp,
) -> Result<(), CatalogError> {
    value.validate()?;
    validate_source_revision(connection, &value.capture)?;
    require_physical_claim_capacity(connection, std::slice::from_ref(&value.physical))?;
    insert_raw_observation(
        connection,
        &value.capture,
        std::slice::from_ref(&value.physical),
        "whole_single_segment",
        recorded_at,
    )?;
    retain_dependency(connection, value)
}

/// Links an exact pending option original after the caller validates the full dependency
/// session, target and renewal origin in this same publication transaction. Historical custody
/// is already durable; neither its raw observation nor its source revision is inserted again.
pub(in crate::catalog) fn retain_option_original(
    connection: &rusqlite::Transaction<'_>,
    value: &ProviderMetadataCaptureEvidence,
) -> Result<(), CatalogError> {
    value.validate()?;
    validate_retained_source_revisions(connection, &value.capture)?;
    let mut statement = connection.prepare(
        "SELECT session_digest, ordinal FROM provider_capture_originals
         WHERE capture_observation_digest=?1 AND raw_claim_digest=?2
          AND physical_receipt_digest=?3 LIMIT 2",
    )?;
    let mut rows = statement.query(params![
        value.capture.observation_digest().bytes(),
        value.physical.raw_claim_digest.bytes(),
        value.physical.claim.physical_receipt_digest().bytes(),
    ])?;
    let row = rows.next()?.ok_or(CatalogError::ProviderCaptureMismatch)?;
    let session = parse_digest(1, &row.get::<_, Vec<u8>>(0)?)?;
    let ordinal: u16 = row.get(1)?;
    if rows.next()?.is_some() {
        return Err(CatalogError::ProviderCaptureConflict);
    }
    drop(rows);
    drop(statement);
    let original = original::load(connection, session, ordinal)?
        .ok_or(CatalogError::ProviderCaptureMismatch)?;
    if original.published_binding().is_some()
        || original.capture() != &value.capture
        || original.physical() != &value.physical
    {
        return Err(CatalogError::ProviderCaptureMismatch);
    }
    retain_dependency(connection, value)
}

fn retain_dependency(
    connection: &Connection,
    value: &ProviderMetadataCaptureEvidence,
) -> Result<(), CatalogError> {
    connection.execute(
        "INSERT OR IGNORE INTO provider_capture_metadata_dependencies
        (dependency_digest,capture_observation_digest,raw_object_input_ordinal,raw_claim_digest,
         physical_receipt_digest,sealed_capture_receipt_digest)
        VALUES (?1,?2,0,?3,?4,?5)",
        params![
            value.digest.bytes(),
            value.capture.observation_digest().bytes(),
            value.physical.raw_claim_digest.bytes(),
            value.physical.claim.physical_receipt_digest().bytes(),
            value.physical.sealed_capture_receipt_digest.bytes()
        ],
    )?;
    if load(connection, value.digest)?.as_ref() != Some(value) {
        return Err(CatalogError::ProviderCaptureConflict);
    }
    Ok(())
}

pub(in crate::catalog) fn load(
    connection: &Connection,
    digest: EvidenceDigest,
) -> Result<Option<ProviderMetadataCaptureEvidence>, CatalogError> {
    let exists: bool = connection.query_row("SELECT EXISTS(SELECT 1 FROM provider_capture_metadata_dependencies WHERE dependency_digest=?1)",
        [digest.bytes()], |row| row.get(0))?;
    let row = connection.query_row("SELECT observation.capture_json, raw.raw_claim_json,
        dependency.raw_claim_digest,dependency.sealed_capture_receipt_digest,
        object.object_capture_content_digest,object.object_capture_observation_digest,
        object.capture_receipt_digest,dependency.physical_receipt_digest,dependency.capture_observation_digest
        FROM provider_capture_metadata_dependencies AS dependency
        JOIN provider_raw_observations AS observation ON observation.capture_observation_digest=dependency.capture_observation_digest
        JOIN provider_raw_observation_objects AS object
          ON object.capture_observation_digest=dependency.capture_observation_digest
         AND object.input_ordinal=dependency.raw_object_input_ordinal
         AND object.raw_claim_digest=dependency.raw_claim_digest
         AND object.physical_receipt_digest=dependency.physical_receipt_digest
        JOIN sealed_raw_objects AS raw ON raw.raw_claim_digest=dependency.raw_claim_digest
         AND raw.physical_receipt_digest=dependency.physical_receipt_digest AND raw.raw_claim_kind='journal_segment'
        WHERE dependency.dependency_digest=?1", [digest.bytes()], |row| Ok((
            row.get::<_,String>(0)?,row.get::<_,String>(1)?,row.get::<_,Vec<u8>>(2)?,row.get::<_,Vec<u8>>(3)?,
            row.get::<_,Vec<u8>>(4)?,row.get::<_,Vec<u8>>(5)?,row.get::<_,Vec<u8>>(6)?,row.get::<_,Vec<u8>>(7)?,row.get::<_,Vec<u8>>(8)?)))
        .optional()?;
    let Some((
        capture,
        claim,
        raw,
        sealed,
        content,
        observation,
        object_sealed,
        physical,
        dependency_observation,
    )) = row
    else {
        return if exists {
            Err(CatalogError::CorruptCatalog)
        } else {
            Ok(None)
        };
    };
    if capture.len() > MAX_PROVIDER_CLAIM_JSON_BYTES || claim.len() > MAX_PROVIDER_CLAIM_JSON_BYTES
    {
        return Err(CatalogError::ResultByteLimitExceeded);
    }
    let value = ProviderMetadataCaptureEvidence {
        digest,
        capture: serde_json::from_str(&capture)?,
        physical: PersistedProviderCapturePhysicalClaim {
            raw_claim_digest: parse_digest(1, &raw)?,
            capture_content_digest: parse_digest(1, &content)?,
            capture_observation_digest: parse_digest(1, &observation)?,
            sealed_capture_receipt_digest: parse_digest(1, &sealed)?,
            claim: parse_journal_claim(&claim)?,
        },
    };
    value.validate()?;
    if value.capture.observation_digest() != parse_digest(1, &dependency_observation)?
        || value.physical.sealed_capture_receipt_digest != parse_digest(1, &object_sealed)?
        || value.physical.claim.physical_receipt_digest() != parse_digest(1, &physical)?
    {
        return Err(CatalogError::CorruptCatalog);
    }
    Ok(Some(value))
}

impl Catalog {
    pub(crate) fn metadata_for_provider_binding(
        &self,
        binding: EvidenceDigest,
    ) -> Result<Option<ProviderMetadataCaptureEvidence>, CatalogError> {
        let digest: Option<Vec<u8>> = self.connection.query_row(
            "SELECT metadata_dependency_digest FROM ingest_run_provider_capture_bindings WHERE binding_digest=?1",
            [binding.bytes()], |row| row.get(0),
        ).optional()?.flatten();
        digest
            .as_deref()
            .map(|digest| {
                load(&self.connection, parse_digest(1, digest)?)?
                    .ok_or(CatalogError::ProviderCaptureMismatch)
            })
            .transpose()
    }

    pub(crate) fn provider_metadata_capture_unbounded(
        &self,
        digest: EvidenceDigest,
    ) -> Result<ProviderMetadataCaptureEvidence, CatalogError> {
        load(&self.connection, digest)?.ok_or(CatalogError::ProviderCaptureMismatch)
    }
}
