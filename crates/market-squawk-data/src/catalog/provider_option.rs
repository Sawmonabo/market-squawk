//! Durable value-only evidence for sealed provider option-market publications.

use market_squawk_domain::{DigestAlgorithm, EvidenceDigest, InstrumentId, Timestamp};
use market_squawk_platform::{SealedResearchJournalSegmentClaim, SealedResearchRawClaim};
use market_squawk_sources::{
    MAX_PROVIDER_NATIVE_LINEAGE_BATCH_BYTES, MAX_PROVIDER_NATIVE_LINEAGE_ROW_BYTES,
    MAX_PROVIDER_NATIVE_LINEAGE_SIDECAR_BYTES, MAX_PROVIDER_OPTION_MARKET_BATCH_ROWS,
    OptionMarketBatchDisposition, OptionMarketBatchKind, PROVIDER_OPTION_MARKET_SCHEMA_VERSION,
    ProviderCaptureSetReceipt, SealedProviderOptionMarketBinding,
};
use rusqlite::{Connection, OptionalExtension as _, Transaction, params};
use sha2::{Digest as _, Sha256};
use uuid::Uuid;

use super::provider_capture::{
    ProviderArtifactInputCoordinate, native_implementation_name, parse_source_sequence,
    raw_claim_digest, source_sequence_blob,
};
use super::provider_event::{
    insert_response_capture, require_raw_claim_capacity, validate_response_capture_source_revision,
};
use super::storage::{append_audit, parse_digest};
use super::{Catalog, CatalogError};

const OPTION_BINDING_FORMAT_VERSION: i64 = 1;
const MAX_OPTION_CLAIM_JSON_BYTES: usize = 2 * 1024 * 1024;
const OPTION_MARKET_SCHEMA_DOMAIN: &[u8] = b"market-squawk/provider-option-market/schema/v1";
const OPTION_MARKET_NATIVE_DOMAIN: &[u8] =
    b"market-squawk/provider-option-market/native-lineage/v1";
const OPTION_MARKET_BINDING_DOMAIN: &[u8] =
    b"market-squawk/provider-option-market/sealed-binding/v1";
const OPTION_ROW_MAPPING_DOMAIN: &[u8] = b"market-squawk/provider-option-market/catalog-row-map/v1";

/// One exact persisted canonical/native/HTTP-page/physical-frame coordinate.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PersistedProviderOptionMarketBindingRow {
    canonical_row_ordinal: u32,
    canonical_row_digest: EvidenceDigest,
    native_semantic_payload: Vec<u8>,
    native_semantic_digest: EvidenceDigest,
    capture_page_ordinal: u16,
    physical_frame_ordinal: u32,
    payload_digest: EvidenceDigest,
    received_at: Timestamp,
    source_sequence: Option<u64>,
}

impl PersistedProviderOptionMarketBindingRow {
    pub const fn canonical_row_ordinal(&self) -> u32 {
        self.canonical_row_ordinal
    }
    pub const fn canonical_row_digest(&self) -> EvidenceDigest {
        self.canonical_row_digest
    }
    pub fn native_semantic_payload(&self) -> &[u8] {
        &self.native_semantic_payload
    }
    pub const fn native_semantic_digest(&self) -> EvidenceDigest {
        self.native_semantic_digest
    }
    pub const fn capture_page_ordinal(&self) -> u16 {
        self.capture_page_ordinal
    }
    pub const fn physical_frame_ordinal(&self) -> u32 {
        self.physical_frame_ordinal
    }
    pub const fn payload_digest(&self) -> EvidenceDigest {
        self.payload_digest
    }
    pub const fn received_at(&self) -> Timestamp {
        self.received_at
    }
    pub const fn source_sequence(&self) -> Option<u64> {
        self.source_sequence
    }
}

/// Restart-safe provider-native evidence aligned to one option batch.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PersistedProviderOptionMarketNativeLineage {
    schema_version: u16,
    implementation: String,
    schema_fingerprint: EvidenceDigest,
    row_count: usize,
    batch_digest: EvidenceDigest,
    batch_sidecar: Vec<u8>,
    batch_sidecar_digest: EvidenceDigest,
}

impl PersistedProviderOptionMarketNativeLineage {
    pub const fn schema_version(&self) -> u16 {
        self.schema_version
    }
    pub fn implementation(&self) -> &str {
        &self.implementation
    }
    pub const fn schema_fingerprint(&self) -> EvidenceDigest {
        self.schema_fingerprint
    }
    pub const fn row_count(&self) -> usize {
        self.row_count
    }
    pub const fn batch_digest(&self) -> EvidenceDigest {
        self.batch_digest
    }
    pub fn batch_sidecar(&self) -> &[u8] {
        &self.batch_sidecar
    }
    pub const fn batch_sidecar_digest(&self) -> EvidenceDigest {
        self.batch_sidecar_digest
    }
}

/// Historical option publication evidence that cannot recreate live publication authority.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PersistedProviderOptionMarketBindingEvidence {
    binding_digest: EvidenceDigest,
    capture: ProviderCaptureSetReceipt,
    sealed_capture_receipt_digest: EvidenceDigest,
    publication_kind: OptionMarketBatchKind,
    canonical_schema_fingerprint: EvidenceDigest,
    canonical_content_digest: EvidenceDigest,
    canonical_row_count: usize,
    scope_json: Vec<u8>,
    scope_digest: EvidenceDigest,
    completeness_json: Vec<u8>,
    completeness_digest: EvidenceDigest,
    filter_json: Vec<u8>,
    filter_digest: EvidenceDigest,
    underlying_instrument_id: InstrumentId,
    available_at: Timestamp,
    received_at: Timestamp,
    ingested_at: Timestamp,
    disposition: OptionMarketBatchDisposition,
    native_lineage: PersistedProviderOptionMarketNativeLineage,
    row_mapping_digest: EvidenceDigest,
    rows: Vec<PersistedProviderOptionMarketBindingRow>,
    raw_claim_digest: EvidenceDigest,
    physical_claim: SealedResearchJournalSegmentClaim,
}

impl PersistedProviderOptionMarketBindingEvidence {
    pub const fn binding_digest(&self) -> EvidenceDigest {
        self.binding_digest
    }
    pub const fn capture(&self) -> &ProviderCaptureSetReceipt {
        &self.capture
    }
    pub const fn sealed_capture_receipt_digest(&self) -> EvidenceDigest {
        self.sealed_capture_receipt_digest
    }
    pub const fn publication_kind(&self) -> OptionMarketBatchKind {
        self.publication_kind
    }
    pub const fn publication_kind_name(&self) -> &'static str {
        option_publication_kind(self.publication_kind)
    }
    pub const fn canonical_schema_fingerprint(&self) -> EvidenceDigest {
        self.canonical_schema_fingerprint
    }
    pub const fn canonical_content_digest(&self) -> EvidenceDigest {
        self.canonical_content_digest
    }
    pub const fn canonical_row_count(&self) -> usize {
        self.canonical_row_count
    }
    pub fn scope_json(&self) -> &[u8] {
        &self.scope_json
    }
    pub const fn scope_digest(&self) -> EvidenceDigest {
        self.scope_digest
    }
    pub fn completeness_json(&self) -> &[u8] {
        &self.completeness_json
    }
    pub const fn completeness_digest(&self) -> EvidenceDigest {
        self.completeness_digest
    }
    pub const fn filter_digest(&self) -> EvidenceDigest {
        self.filter_digest
    }
    pub fn filter_json(&self) -> &[u8] {
        &self.filter_json
    }
    pub const fn underlying_instrument_id(&self) -> InstrumentId {
        self.underlying_instrument_id
    }
    pub const fn knowledge_clocks(&self) -> (Timestamp, Timestamp, Timestamp) {
        (self.available_at, self.received_at, self.ingested_at)
    }
    pub const fn disposition(&self) -> OptionMarketBatchDisposition {
        self.disposition
    }
    pub const fn native_lineage(&self) -> &PersistedProviderOptionMarketNativeLineage {
        &self.native_lineage
    }
    pub const fn row_mapping_digest(&self) -> EvidenceDigest {
        self.row_mapping_digest
    }
    pub fn rows(&self) -> &[PersistedProviderOptionMarketBindingRow] {
        &self.rows
    }
    pub const fn raw_claim_digest(&self) -> EvidenceDigest {
        self.raw_claim_digest
    }
    pub const fn physical_claim(&self) -> &SealedResearchJournalSegmentClaim {
        &self.physical_claim
    }

    pub(crate) fn verify_integrity(&self) -> Result<(), CatalogError> {
        if self.canonical_row_count > MAX_PROVIDER_OPTION_MARKET_BATCH_ROWS
            || self.canonical_row_count != self.rows.len()
            || self.native_lineage.row_count != self.rows.len()
            || self.capture.pages().is_empty()
            || self.capture.pages().len() != self.physical_claim.frames().len()
            || self.received_at > self.ingested_at
            || self.available_at > self.ingested_at
            || sha256_evidence(&self.scope_json) != self.scope_digest
            || sha256_evidence(&self.completeness_json) != self.completeness_digest
            || sha256_evidence(&self.filter_json) != self.filter_digest
        {
            return Err(CatalogError::ProviderEventMismatch);
        }
        let claim_json = journal_claim_json(&self.physical_claim)?;
        if claim_json.len() > MAX_OPTION_CLAIM_JSON_BYTES
            || raw_claim_digest(claim_json.as_bytes()) != self.raw_claim_digest
        {
            return Err(CatalogError::ProviderEventMismatch);
        }
        let expected_schema = option_schema_fingerprint();
        if self.canonical_schema_fingerprint != expected_schema
            || self.native_lineage.schema_version == 0
            || self.native_lineage.batch_sidecar.is_empty()
            || self.native_lineage.batch_sidecar.len() > MAX_PROVIDER_NATIVE_LINEAGE_SIDECAR_BYTES
            || sha256_evidence(&self.native_lineage.batch_sidecar)
                != self.native_lineage.batch_sidecar_digest
        {
            return Err(CatalogError::ProviderEventMismatch);
        }
        let mut native_bytes = self.native_lineage.batch_sidecar.len();
        for (ordinal, row) in self.rows.iter().enumerate() {
            native_bytes = native_bytes
                .checked_add(row.native_semantic_payload.len())
                .ok_or(CatalogError::ProviderEventMismatch)?;
            let page = self
                .capture
                .pages()
                .get(usize::from(row.capture_page_ordinal))
                .ok_or(CatalogError::ProviderEventMismatch)?;
            let frame = self
                .physical_claim
                .frames()
                .get(
                    usize::try_from(row.physical_frame_ordinal)
                        .map_err(|_| CatalogError::ProviderEventMismatch)?,
                )
                .ok_or(CatalogError::ProviderEventMismatch)?;
            if row.canonical_row_ordinal
                != u32::try_from(ordinal).map_err(|_| CatalogError::ProviderEventMismatch)?
                || row.native_semantic_payload.is_empty()
                || row.native_semantic_payload.len() > MAX_PROVIDER_NATIVE_LINEAGE_ROW_BYTES
                || sha256_evidence(&row.native_semantic_payload) != row.native_semantic_digest
                || page.body_digest() != row.payload_digest
                || page.received_at() != row.received_at
                || frame.ordinal() != row.physical_frame_ordinal
                || frame.provider_payload_digest() != row.payload_digest
                || frame.received_at() != row.received_at
                || frame.source_sequence() != row.source_sequence
            {
                return Err(CatalogError::ProviderEventMismatch);
            }
        }
        if native_bytes > MAX_PROVIDER_NATIVE_LINEAGE_BATCH_BYTES
            || option_native_digest(self)? != self.native_lineage.batch_digest
            || option_binding_digest(self)? != self.binding_digest
            || option_row_mapping_digest(&self.rows)? != self.row_mapping_digest
        {
            return Err(CatalogError::ProviderEventMismatch);
        }
        Ok(())
    }
}

#[derive(Debug)]
pub(crate) struct PreparedProviderOptionMarketBinding {
    evidence: PersistedProviderOptionMarketBindingEvidence,
}

impl PreparedProviderOptionMarketBinding {
    pub(crate) fn try_from_live(
        binding: &SealedProviderOptionMarketBinding,
    ) -> Result<Self, CatalogError> {
        binding
            .validate()
            .map_err(|_| CatalogError::ProviderEventMismatch)?;
        let batch = binding.batch();
        let scope_json = serde_json::to_vec(batch.scope())?;
        let completeness_json = serde_json::to_vec(&batch.completeness())?;
        let filter_json = serde_json::to_vec(batch.scope().filter())?;
        let row_count = batch.row_count();
        if row_count > MAX_PROVIDER_OPTION_MARKET_BATCH_ROWS
            || row_count != binding.native_lineage().rows().len()
            || row_count != binding.row_frames().len()
        {
            return Err(CatalogError::ProviderEventMismatch);
        }
        let native = binding.native_lineage();
        let mut rows = Vec::new();
        rows.try_reserve_exact(row_count)
            .map_err(|_| CatalogError::Allocation)?;
        for (ordinal, (native_row, frame)) in
            native.rows().iter().zip(binding.row_frames()).enumerate()
        {
            rows.push(PersistedProviderOptionMarketBindingRow {
                canonical_row_ordinal: u32::try_from(ordinal)
                    .map_err(|_| CatalogError::ProviderEventMismatch)?,
                canonical_row_digest: batch
                    .canonical_row_digest(ordinal)
                    .ok_or(CatalogError::ProviderEventMismatch)?,
                native_semantic_payload: native_row.to_vec(),
                native_semantic_digest: native
                    .row_digest(ordinal)
                    .ok_or(CatalogError::ProviderEventMismatch)?,
                capture_page_ordinal: frame.capture_page_ordinal(),
                physical_frame_ordinal: frame.physical_frame_ordinal(),
                payload_digest: frame.page_body_digest(),
                received_at: frame.received_at(),
                source_sequence: frame.source_sequence(),
            });
        }
        let claim = binding.persisted_receipt().segment().claim().clone();
        let claim_json = journal_claim_json(&claim)?;
        if claim_json.len() > MAX_OPTION_CLAIM_JSON_BYTES {
            return Err(CatalogError::ResultByteLimitExceeded);
        }
        let content = batch.content_identity();
        let native_schema = native.schema();
        let evidence = PersistedProviderOptionMarketBindingEvidence {
            binding_digest: binding.evidence_digest().evidence(),
            capture: binding.persisted_receipt().capture().clone(),
            sealed_capture_receipt_digest: binding.persisted_receipt().receipt_digest(),
            publication_kind: batch.kind(),
            canonical_schema_fingerprint: content.schema_fingerprint(),
            canonical_content_digest: content.content_digest(),
            canonical_row_count: content.row_count(),
            scope_digest: sha256_evidence(&scope_json),
            scope_json,
            completeness_digest: sha256_evidence(&completeness_json),
            completeness_json,
            filter_json: filter_json.clone(),
            filter_digest: sha256_evidence(&filter_json),
            underlying_instrument_id: batch.scope().underlying_instrument_id(),
            available_at: batch.scope().available_at(),
            received_at: batch.scope().received_at(),
            ingested_at: batch.scope().ingested_at(),
            disposition: batch.completeness().disposition(),
            native_lineage: PersistedProviderOptionMarketNativeLineage {
                schema_version: native_schema.version(),
                implementation: native_implementation_name(native_schema.implementation())
                    .to_owned(),
                schema_fingerprint: native_schema.fingerprint(),
                row_count,
                batch_digest: native.batch_digest(),
                batch_sidecar: native.batch_sidecar().to_vec(),
                batch_sidecar_digest: native.batch_sidecar_digest(),
            },
            row_mapping_digest: option_row_mapping_digest(&rows)?,
            rows,
            raw_claim_digest: raw_claim_digest(claim_json.as_bytes()),
            physical_claim: claim,
        };
        evidence.verify_integrity()?;
        Ok(Self { evidence })
    }

    pub(crate) const fn publication_digest(&self) -> EvidenceDigest {
        self.evidence.binding_digest
    }
    pub(crate) const fn source_id(&self) -> &market_squawk_domain::SourceId {
        self.evidence.capture.source_id()
    }
    pub(crate) const fn publication_kind_name(&self) -> &'static str {
        self.evidence.publication_kind_name()
    }
    pub(crate) fn matches_persisted(
        &self,
        persisted: &PersistedProviderOptionMarketBindingEvidence,
    ) -> bool {
        self.evidence == *persisted
    }
}

impl Catalog {
    /// Reopens one exact option-market binding after restart.
    pub fn provider_option_market_binding_evidence(
        &self,
        binding_digest: EvidenceDigest,
    ) -> Result<Option<PersistedProviderOptionMarketBindingEvidence>, CatalogError> {
        load_provider_option_market_binding_evidence(&self.connection, binding_digest)
    }

    pub(crate) fn provider_option_market_for_run(
        &self,
        run_id: Uuid,
    ) -> Result<Option<PersistedProviderOptionMarketBindingEvidence>, CatalogError> {
        let digest = self
            .connection
            .query_row(
                "SELECT option_binding_digest FROM ingest_run_provider_publication_bindings
                 WHERE run_id=?1 AND publication_kind IN ('option_snapshots','option_expirations')",
                [run_id.to_string()],
                |row| row.get::<_, Vec<u8>>(0),
            )
            .optional()?
            .map(|bytes| parse_digest(1, &bytes))
            .transpose()?;
        digest
            .map(|value| load_provider_option_market_binding_evidence(&self.connection, value))
            .transpose()
            .map(Option::flatten)
    }
}

pub(crate) fn retain_prepared_provider_option_market_binding(
    connection: &Transaction<'_>,
    run_id: Uuid,
    prepared: &PreparedProviderOptionMarketBinding,
    coordinate: ProviderArtifactInputCoordinate,
    recorded_at: Timestamp,
) -> Result<(), CatalogError> {
    let evidence = &prepared.evidence;
    evidence.verify_integrity()?;
    validate_response_capture_source_revision(connection, run_id, &evidence.capture)?;
    require_raw_claim_capacity(
        connection,
        evidence.raw_claim_digest,
        &evidence.physical_claim,
    )?;
    insert_response_capture(
        connection,
        &evidence.capture,
        evidence.sealed_capture_receipt_digest,
        evidence.raw_claim_digest,
        &evidence.physical_claim,
        recorded_at,
    )?;
    insert_option_binding(connection, evidence, recorded_at)?;
    let used: bool = connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM ingest_run_provider_publication_bindings
         WHERE publication_digest=?1)",
        [evidence.binding_digest.bytes().as_slice()],
        |row| row.get(0),
    )?;
    if used {
        return Err(CatalogError::ProviderEventConflict);
    }
    let inserted = connection.execute(
        "INSERT INTO ingest_run_provider_publication_bindings
         (run_id, input_ordinal, output_artifact_ordinal, object_input_ordinal,
          publication_digest, publication_kind, source_id, response_binding_digest,
          event_binding_digest, composite_binding_digest, option_binding_digest)
         VALUES (?1, 0, ?2, ?3, ?4, ?5, ?6, NULL, NULL, NULL, ?4)",
        params![
            run_id.to_string(),
            i64::try_from(coordinate.output_artifact_ordinal())
                .map_err(|_| CatalogError::ProviderEventConflict)?,
            i64::try_from(coordinate.object_input_ordinal())
                .map_err(|_| CatalogError::ProviderEventConflict)?,
            evidence.binding_digest.bytes().as_slice(),
            evidence.publication_kind_name(),
            evidence.capture.source_id().as_str(),
        ],
    )?;
    if inserted != 1 {
        return Err(CatalogError::ProviderEventConflict);
    }
    append_audit(
        connection,
        "provider-option-market-publication.retained",
        &run_id.to_string(),
        evidence.binding_digest.bytes(),
        recorded_at,
    )?;
    let retained =
        load_provider_option_market_binding_evidence(connection, evidence.binding_digest)?
            .ok_or(CatalogError::ProviderEventConflict)?;
    if retained != *evidence {
        return Err(CatalogError::ProviderEventConflict);
    }
    Ok(())
}

fn insert_option_binding(
    connection: &Connection,
    evidence: &PersistedProviderOptionMarketBindingEvidence,
    recorded_at: Timestamp,
) -> Result<(), CatalogError> {
    connection.execute(
        "INSERT OR IGNORE INTO provider_option_market_bindings
         (option_binding_digest, binding_format_version, capture_observation_digest,
          sealed_capture_receipt_digest, publication_kind, canonical_schema_fingerprint,
          canonical_content_digest, canonical_row_count, scope_json, scope_digest,
          completeness_json, completeness_digest, filter_json, filter_digest,
          underlying_instrument_id,
          available_at_ns, received_at_ns, ingested_at_ns, disposition, row_mapping_digest,
          recorded_at_ns)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14,
                 ?15, ?16, ?17, ?18, ?19, ?20, ?21)",
        params![
            evidence.binding_digest.bytes().as_slice(),
            OPTION_BINDING_FORMAT_VERSION,
            evidence.capture.observation_digest().bytes().as_slice(),
            evidence.sealed_capture_receipt_digest.bytes().as_slice(),
            evidence.publication_kind_name(),
            evidence.canonical_schema_fingerprint.bytes().as_slice(),
            evidence.canonical_content_digest.bytes().as_slice(),
            to_i64(evidence.canonical_row_count)?,
            &evidence.scope_json,
            evidence.scope_digest.bytes().as_slice(),
            &evidence.completeness_json,
            evidence.completeness_digest.bytes().as_slice(),
            &evidence.filter_json,
            evidence.filter_digest.bytes().as_slice(),
            evidence
                .underlying_instrument_id
                .as_uuid()
                .as_bytes()
                .as_slice(),
            evidence.available_at.unix_nanos(),
            evidence.received_at.unix_nanos(),
            evidence.ingested_at.unix_nanos(),
            disposition_name(evidence.disposition),
            evidence.row_mapping_digest.bytes().as_slice(),
            recorded_at.unix_nanos(),
        ],
    )?;
    let native = &evidence.native_lineage;
    connection.execute(
        "INSERT OR IGNORE INTO provider_option_market_binding_native_lineage
         (option_binding_digest, schema_version, implementation, schema_fingerprint, row_count,
          batch_digest, batch_sidecar_payload, batch_sidecar_digest)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
        params![
            evidence.binding_digest.bytes().as_slice(),
            i64::from(native.schema_version),
            native.implementation,
            native.schema_fingerprint.bytes().as_slice(),
            to_i64(native.row_count)?,
            native.batch_digest.bytes().as_slice(),
            &native.batch_sidecar,
            native.batch_sidecar_digest.bytes().as_slice(),
        ],
    )?;
    for row in &evidence.rows {
        connection.execute(
            "INSERT OR IGNORE INTO provider_option_market_binding_rows
             (option_binding_digest, capture_observation_digest, canonical_row_ordinal,
              canonical_row_digest, native_semantic_payload, native_semantic_digest,
              capture_page_ordinal, physical_frame_ordinal, payload_digest, received_at_ns,
              source_sequence)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
            params![
                evidence.binding_digest.bytes().as_slice(),
                evidence.capture.observation_digest().bytes().as_slice(),
                i64::from(row.canonical_row_ordinal),
                row.canonical_row_digest.bytes().as_slice(),
                &row.native_semantic_payload,
                row.native_semantic_digest.bytes().as_slice(),
                i64::from(row.capture_page_ordinal),
                i64::from(row.physical_frame_ordinal),
                row.payload_digest.bytes().as_slice(),
                row.received_at.unix_nanos(),
                source_sequence_blob(row.source_sequence),
            ],
        )?;
    }
    Ok(())
}

fn load_provider_option_market_binding_evidence(
    connection: &Connection,
    binding_digest: EvidenceDigest,
) -> Result<Option<PersistedProviderOptionMarketBindingEvidence>, CatalogError> {
    #[allow(clippy::type_complexity)]
    let header: Option<(
        Vec<u8>,
        Vec<u8>,
        String,
        Vec<u8>,
        Vec<u8>,
        i64,
        Vec<u8>,
        Vec<u8>,
        Vec<u8>,
        Vec<u8>,
        Vec<u8>,
        Vec<u8>,
        Vec<u8>,
        i64,
        i64,
        i64,
        String,
        Vec<u8>,
        String,
        i64,
        String,
        Vec<u8>,
        i64,
        Vec<u8>,
        Vec<u8>,
        Vec<u8>,
        Vec<u8>,
        String,
        Vec<u8>,
        String,
        Vec<u8>,
        i64,
        Option<i64>,
        i64,
        String,
    )> = connection
        .query_row(
            "SELECT binding.capture_observation_digest, binding.sealed_capture_receipt_digest,
                    binding.publication_kind, binding.canonical_schema_fingerprint,
                    binding.canonical_content_digest, binding.canonical_row_count,
                    binding.scope_json, binding.scope_digest, binding.completeness_json,
                    binding.completeness_digest, binding.filter_json, binding.filter_digest,
                    binding.underlying_instrument_id, binding.available_at_ns,
                    binding.received_at_ns, binding.ingested_at_ns, binding.disposition,
                    binding.row_mapping_digest, native.implementation, native.schema_version,
                    capture.capture_json, native.schema_fingerprint, native.row_count,
                    native.batch_digest, native.batch_sidecar_payload,
                    native.batch_sidecar_digest, object.raw_claim_digest,
                    object.raw_claim_kind, object.physical_receipt_digest,
                    object.relative_reference, object.content_digest, object.size_bytes,
                    object.integrity_chunk_bytes, object.unit_count, object.raw_claim_json
             FROM provider_option_market_bindings AS binding
             JOIN provider_option_market_binding_native_lineage AS native
               USING (option_binding_digest)
             JOIN provider_raw_observations AS capture
               USING (capture_observation_digest)
             JOIN provider_raw_observation_objects AS response_object
               ON response_object.capture_observation_digest=binding.capture_observation_digest
              AND response_object.capture_receipt_digest=binding.sealed_capture_receipt_digest
             JOIN sealed_raw_objects AS object
               ON object.raw_claim_digest=response_object.raw_claim_digest
              AND object.physical_receipt_digest=response_object.physical_receipt_digest
             WHERE binding.option_binding_digest=?1",
            [binding_digest.bytes().as_slice()],
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
                    row.get(17)?,
                    row.get(18)?,
                    row.get(19)?,
                    row.get(20)?,
                    row.get(21)?,
                    row.get(22)?,
                    row.get(23)?,
                    row.get(24)?,
                    row.get(25)?,
                    row.get(26)?,
                    row.get(27)?,
                    row.get(28)?,
                    row.get(29)?,
                    row.get(30)?,
                    row.get(31)?,
                    row.get(32)?,
                    row.get(33)?,
                    row.get(34)?,
                ))
            },
        )
        .optional()?;
    let Some((
        capture_digest,
        sealed_receipt,
        kind,
        schema_fingerprint,
        content_digest,
        row_count,
        scope_json,
        scope_digest,
        completeness_json,
        completeness_digest,
        filter_json,
        filter_digest,
        underlying,
        available,
        received,
        ingested,
        disposition,
        row_mapping,
        implementation,
        native_version,
        capture_json,
        native_fingerprint,
        native_count,
        native_digest,
        sidecar,
        sidecar_digest,
        raw_claim_digest_bytes,
        raw_claim_kind,
        physical_receipt_digest,
        relative_reference,
        raw_content_digest,
        raw_size_bytes,
        integrity_chunk_bytes,
        unit_count,
        claim_json,
    )) = header
    else {
        return Ok(None);
    };
    let capture: ProviderCaptureSetReceipt = serde_json::from_str(&capture_json)?;
    if parse_digest(1, &capture_digest)? != capture.observation_digest() {
        return Err(CatalogError::CorruptCatalog);
    }
    if claim_json.len() > MAX_OPTION_CLAIM_JSON_BYTES
        || raw_claim_digest(claim_json.as_bytes()) != parse_digest(1, &raw_claim_digest_bytes)?
    {
        return Err(CatalogError::CorruptCatalog);
    }
    let physical_claim = parse_journal_claim(&claim_json)?;
    if raw_claim_kind != "journal_segment"
        || journal_claim_json(&physical_claim)? != claim_json
        || parse_digest(1, &physical_receipt_digest)? != physical_claim.physical_receipt_digest()
        || relative_reference != physical_claim.relative_reference()
        || parse_digest(1, &raw_content_digest)? != physical_claim.content_digest()
        || u64::try_from(raw_size_bytes).map_err(|_| CatalogError::CorruptCatalog)?
            != physical_claim.size_bytes()
        || integrity_chunk_bytes.is_some()
        || usize::try_from(unit_count).map_err(|_| CatalogError::CorruptCatalog)?
            != physical_claim.frames().len()
    {
        return Err(CatalogError::CorruptCatalog);
    }
    let mut statement = connection.prepare(
        "SELECT canonical_row_ordinal, canonical_row_digest, native_semantic_payload,
                native_semantic_digest, capture_page_ordinal, physical_frame_ordinal,
                payload_digest, received_at_ns, source_sequence
         FROM provider_option_market_binding_rows WHERE option_binding_digest=?1
         ORDER BY canonical_row_ordinal LIMIT 100001",
    )?;
    let mapped = statement.query_map([binding_digest.bytes().as_slice()], |row| {
        Ok((
            row.get::<_, i64>(0)?,
            row.get::<_, Vec<u8>>(1)?,
            row.get::<_, Vec<u8>>(2)?,
            row.get::<_, Vec<u8>>(3)?,
            row.get::<_, i64>(4)?,
            row.get::<_, i64>(5)?,
            row.get::<_, Vec<u8>>(6)?,
            row.get::<_, i64>(7)?,
            row.get::<_, Option<Vec<u8>>>(8)?,
        ))
    })?;
    let mut rows = Vec::new();
    for row in mapped {
        let (ordinal, canonical, native_payload, native, page, frame, payload, time, sequence) =
            row?;
        let source_sequence = sequence
            .map(|bytes| bytes.try_into().map_err(|_| CatalogError::CorruptCatalog))
            .transpose()?;
        rows.push(PersistedProviderOptionMarketBindingRow {
            canonical_row_ordinal: u32::try_from(ordinal)
                .map_err(|_| CatalogError::CorruptCatalog)?,
            canonical_row_digest: parse_digest(1, &canonical)?,
            native_semantic_payload: native_payload,
            native_semantic_digest: parse_digest(1, &native)?,
            capture_page_ordinal: u16::try_from(page).map_err(|_| CatalogError::CorruptCatalog)?,
            physical_frame_ordinal: u32::try_from(frame)
                .map_err(|_| CatalogError::CorruptCatalog)?,
            payload_digest: parse_digest(1, &payload)?,
            received_at: Timestamp::from_unix_nanos(time),
            source_sequence: parse_source_sequence(source_sequence),
        });
    }
    let underlying_uuid =
        uuid::Uuid::from_slice(&underlying).map_err(|_| CatalogError::CorruptCatalog)?;
    let evidence = PersistedProviderOptionMarketBindingEvidence {
        binding_digest,
        capture,
        sealed_capture_receipt_digest: parse_digest(1, &sealed_receipt)?,
        publication_kind: parse_option_kind(&kind)?,
        canonical_schema_fingerprint: parse_digest(1, &schema_fingerprint)?,
        canonical_content_digest: parse_digest(1, &content_digest)?,
        canonical_row_count: usize::try_from(row_count)
            .map_err(|_| CatalogError::CorruptCatalog)?,
        scope_json,
        scope_digest: parse_digest(1, &scope_digest)?,
        completeness_json,
        completeness_digest: parse_digest(1, &completeness_digest)?,
        filter_json,
        filter_digest: parse_digest(1, &filter_digest)?,
        underlying_instrument_id: InstrumentId::try_from(underlying_uuid)
            .map_err(|_| CatalogError::CorruptCatalog)?,
        available_at: Timestamp::from_unix_nanos(available),
        received_at: Timestamp::from_unix_nanos(received),
        ingested_at: Timestamp::from_unix_nanos(ingested),
        disposition: parse_disposition(&disposition)?,
        native_lineage: PersistedProviderOptionMarketNativeLineage {
            schema_version: u16::try_from(native_version)
                .map_err(|_| CatalogError::CorruptCatalog)?,
            implementation,
            schema_fingerprint: parse_digest(1, &native_fingerprint)?,
            row_count: usize::try_from(native_count).map_err(|_| CatalogError::CorruptCatalog)?,
            batch_digest: parse_digest(1, &native_digest)?,
            batch_sidecar: sidecar,
            batch_sidecar_digest: parse_digest(1, &sidecar_digest)?,
        },
        row_mapping_digest: parse_digest(1, &row_mapping)?,
        rows,
        raw_claim_digest: parse_digest(1, &raw_claim_digest_bytes)?,
        physical_claim,
    };
    evidence.verify_integrity()?;
    Ok(Some(evidence))
}

fn option_native_digest(
    evidence: &PersistedProviderOptionMarketBindingEvidence,
) -> Result<EvidenceDigest, CatalogError> {
    let native = &evidence.native_lineage;
    let mut digest = Sha256::new();
    hash_field(&mut digest, OPTION_MARKET_NATIVE_DOMAIN)?;
    digest.update(native.schema_version.to_be_bytes());
    hash_digest(&mut digest, native.schema_fingerprint);
    hash_digest(&mut digest, evidence.canonical_content_digest);
    hash_length(&mut digest, evidence.rows.len())?;
    for row in &evidence.rows {
        digest.update(row.canonical_row_ordinal.to_be_bytes());
        hash_digest(&mut digest, row.canonical_row_digest);
        hash_length(&mut digest, row.native_semantic_payload.len())?;
        hash_digest(&mut digest, row.native_semantic_digest);
    }
    hash_length(&mut digest, native.batch_sidecar.len())?;
    hash_digest(&mut digest, native.batch_sidecar_digest);
    Ok(finalize(digest))
}

fn option_binding_digest(
    evidence: &PersistedProviderOptionMarketBindingEvidence,
) -> Result<EvidenceDigest, CatalogError> {
    let mut digest = Sha256::new();
    hash_field(&mut digest, OPTION_MARKET_BINDING_DOMAIN)?;
    hash_digest(&mut digest, evidence.sealed_capture_receipt_digest);
    hash_digest(&mut digest, evidence.capture.content_digest());
    hash_digest(&mut digest, evidence.capture.observation_digest());
    hash_digest(&mut digest, evidence.physical_claim.content_digest());
    hash_digest(
        &mut digest,
        evidence.physical_claim.physical_receipt_digest(),
    );
    hash_digest(&mut digest, evidence.canonical_schema_fingerprint);
    hash_digest(&mut digest, evidence.canonical_content_digest);
    hash_field(&mut digest, option_kind_tag(evidence.publication_kind))?;
    hash_length(&mut digest, evidence.canonical_row_count)?;
    hash_digest(&mut digest, evidence.native_lineage.schema_fingerprint);
    hash_digest(&mut digest, evidence.native_lineage.batch_digest);
    hash_length(&mut digest, evidence.rows.len())?;
    for row in &evidence.rows {
        digest.update(row.canonical_row_ordinal.to_be_bytes());
        digest.update(row.capture_page_ordinal.to_be_bytes());
        digest.update(row.physical_frame_ordinal.to_be_bytes());
        hash_digest(&mut digest, row.payload_digest);
        digest.update(row.received_at.unix_nanos().to_be_bytes());
        match row.source_sequence {
            Some(sequence) => {
                digest.update([1]);
                digest.update(sequence.to_be_bytes());
            }
            None => digest.update([0]),
        }
    }
    Ok(finalize(digest))
}

fn option_row_mapping_digest(
    rows: &[PersistedProviderOptionMarketBindingRow],
) -> Result<EvidenceDigest, CatalogError> {
    let mut digest = Sha256::new();
    hash_field(&mut digest, OPTION_ROW_MAPPING_DOMAIN)?;
    hash_length(&mut digest, rows.len())?;
    for row in rows {
        digest.update(row.canonical_row_ordinal.to_be_bytes());
        hash_digest(&mut digest, row.canonical_row_digest);
        hash_digest(&mut digest, row.native_semantic_digest);
        digest.update(row.capture_page_ordinal.to_be_bytes());
        digest.update(row.physical_frame_ordinal.to_be_bytes());
        hash_digest(&mut digest, row.payload_digest);
        digest.update(row.received_at.unix_nanos().to_be_bytes());
        match row.source_sequence {
            Some(sequence) => {
                digest.update([1]);
                digest.update(sequence.to_be_bytes());
            }
            None => digest.update([0]),
        }
    }
    Ok(finalize(digest))
}

fn option_schema_fingerprint() -> EvidenceDigest {
    let mut digest = Sha256::new();
    digest.update(OPTION_MARKET_SCHEMA_DOMAIN);
    digest.update(PROVIDER_OPTION_MARKET_SCHEMA_VERSION.to_be_bytes());
    finalize(digest)
}

pub(crate) const fn option_publication_kind(kind: OptionMarketBatchKind) -> &'static str {
    match kind {
        OptionMarketBatchKind::Snapshots => "option_snapshots",
        OptionMarketBatchKind::Expirations => "option_expirations",
    }
}

const fn option_kind_tag(kind: OptionMarketBatchKind) -> &'static [u8] {
    match kind {
        OptionMarketBatchKind::Snapshots => b"snapshots",
        OptionMarketBatchKind::Expirations => b"expirations",
    }
}

fn parse_option_kind(value: &str) -> Result<OptionMarketBatchKind, CatalogError> {
    match value {
        "option_snapshots" => Ok(OptionMarketBatchKind::Snapshots),
        "option_expirations" => Ok(OptionMarketBatchKind::Expirations),
        _ => Err(CatalogError::CorruptCatalog),
    }
}

const fn disposition_name(value: OptionMarketBatchDisposition) -> &'static str {
    match value {
        OptionMarketBatchDisposition::Complete => "complete",
        OptionMarketBatchDisposition::Unavailable => "unavailable",
    }
}

fn parse_disposition(value: &str) -> Result<OptionMarketBatchDisposition, CatalogError> {
    match value {
        "complete" => Ok(OptionMarketBatchDisposition::Complete),
        "unavailable" => Ok(OptionMarketBatchDisposition::Unavailable),
        _ => Err(CatalogError::CorruptCatalog),
    }
}

fn sha256_evidence(bytes: &[u8]) -> EvidenceDigest {
    EvidenceDigest::new(DigestAlgorithm::Sha256, Sha256::digest(bytes).into())
}

fn journal_claim_json(claim: &SealedResearchJournalSegmentClaim) -> Result<String, CatalogError> {
    serde_json::to_string(&SealedResearchRawClaim::JournalSegment(claim.clone()))
        .map_err(Into::into)
}

fn parse_journal_claim(json: &str) -> Result<SealedResearchJournalSegmentClaim, CatalogError> {
    match serde_json::from_str(json)? {
        SealedResearchRawClaim::JournalSegment(claim) => Ok(claim),
        SealedResearchRawClaim::LogicalObject(_) => Err(CatalogError::CorruptCatalog),
    }
}

fn finalize(digest: Sha256) -> EvidenceDigest {
    EvidenceDigest::new(DigestAlgorithm::Sha256, digest.finalize().into())
}

fn hash_digest(digest: &mut Sha256, evidence: EvidenceDigest) {
    digest.update(match evidence.algorithm() {
        DigestAlgorithm::Sha256 => [1],
        DigestAlgorithm::Blake3 => [2],
    });
    digest.update(evidence.bytes());
}

fn hash_field(digest: &mut Sha256, value: &[u8]) -> Result<(), CatalogError> {
    hash_length(digest, value.len())?;
    digest.update(value);
    Ok(())
}

fn hash_length(digest: &mut Sha256, value: usize) -> Result<(), CatalogError> {
    digest.update(
        u64::try_from(value)
            .map_err(|_| CatalogError::ProviderEventMismatch)?
            .to_be_bytes(),
    );
    Ok(())
}

fn to_i64(value: usize) -> Result<i64, CatalogError> {
    i64::try_from(value).map_err(|_| CatalogError::InvalidRecord)
}
