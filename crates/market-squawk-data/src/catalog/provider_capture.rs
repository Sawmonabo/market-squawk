//! Immutable provider raw observations and their canonical publication bindings.

use market_squawk_domain::{DigestAlgorithm, EvidenceDigest, Timestamp};
use market_squawk_platform::{SealedResearchJournalSegmentClaim, SealedResearchRawClaim};
use market_squawk_sources::{
    MAX_IN_MEMORY_EXTRACTION_BATCH_BYTES, MAX_PROVIDER_NATIVE_LINEAGE_SIDECAR_BYTES,
    ProviderCaptureBindingDigest, ProviderCaptureBindingLayout, ProviderCapturePageReceipt,
    ProviderCapturePhysicalClaimEvidenceRef, ProviderCaptureRowFrameEvidence, ProviderCaptureScope,
    ProviderCaptureSetReceipt, ProviderNativeLineageBatchSidecarEvidenceRef,
    ProviderNativeLineageImplementation, ProviderNativeLineageRowEvidenceRef,
    SealedProviderCaptureBinding, SourceMetadata, verify_provider_native_lineage_batch_evidence,
};
use rusqlite::{Connection, OptionalExtension as _, params};
use sha2::{Digest as _, Sha256};
use uuid::Uuid;

use super::storage::{append_audit, parse_digest, sha256};
use super::{Catalog, CatalogError};

const BINDING_FORMAT_VERSION: i64 = 1;
const ROW_MAPPING_DIGEST_DOMAIN: &[u8] = b"market-squawk/provider-capture-binding/row-map/v1";
const RAW_CLAIM_DIGEST_DOMAIN: &[u8] = b"market-squawk/sealed-raw-object/claim-json/v1";
/// Maximum physical provider raw objects retained by one installed catalog.
///
/// Recovery shares a fixed 100,000-entry budget between authoritative claims and the staging,
/// object-shard, object, and quarantine entries that must be inspected. Keeping claims at one
/// quarter of that ceiling leaves deterministic headroom for the physical tree.
pub(crate) const MAX_PROVIDER_CAPTURE_PHYSICAL_CLAIMS: usize = 25_000;
pub(crate) const MAX_PROVIDER_CAPTURE_PHYSICAL_BYTES: u64 = 512 * 1024 * 1024 * 1024;
pub(crate) const PROVIDER_CAPTURE_RECOVERY_ENTRY_BUDGET: usize = 75_000;
const PROVIDER_CAPTURE_CLAIM_PAGE_ROWS: usize = 128;
const MAX_PROVIDER_CAPTURE_ROWS: usize = 100_000;
const MAX_PROVIDER_CAPTURE_SEGMENTS: usize = 64;
pub(crate) const MAX_PROVIDER_CAPTURE_INPUTS: usize = 4_096;
const MAX_PROVIDER_NATIVE_BYTES: usize = 64 * 1024 * 1024;
const MAX_PROVIDER_CLAIM_JSON_BYTES: usize = 2 * 1024 * 1024;
const MAX_PROVIDER_PUBLICATION_PEAK_BYTES: usize = 768 * 1024 * 1024;

/// Value-only provider-native schema retained for historical verification.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PersistedProviderNativeLineageSchema {
    version: u16,
    implementation: String,
    fingerprint: EvidenceDigest,
    row_count: usize,
    batch_digest: EvidenceDigest,
    batch_sidecar: Option<Vec<u8>>,
    batch_sidecar_digest: Option<EvidenceDigest>,
}

impl PersistedProviderNativeLineageSchema {
    /// Returns the retained code-owned schema version.
    pub const fn version(&self) -> u16 {
        self.version
    }
    /// Returns the exact closed implementation identifier.
    pub fn implementation(&self) -> &str {
        &self.implementation
    }
    /// Returns the retained schema fingerprint.
    pub const fn fingerprint(&self) -> EvidenceDigest {
        self.fingerprint
    }
    /// Returns the native row count.
    pub const fn row_count(&self) -> usize {
        self.row_count
    }
    /// Returns the common-owned native-lineage batch digest.
    pub const fn batch_digest(&self) -> EvidenceDigest {
        self.batch_digest
    }
    /// Returns optional exact batch-level provider-native semantic bytes.
    pub fn batch_sidecar_semantic_payload(&self) -> Option<&[u8]> {
        self.batch_sidecar.as_deref()
    }
    /// Returns SHA-256 of the optional batch-level provider-native semantics.
    pub const fn batch_sidecar_semantic_payload_digest(&self) -> Option<EvidenceDigest> {
        self.batch_sidecar_digest
    }
}

/// Exact persisted canonical/native/raw coordinate for one provider publication row.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PersistedProviderCaptureBindingRow {
    canonical_row_ordinal: u32,
    canonical_row_digest: EvidenceDigest,
    native_semantic_payload: Vec<u8>,
    native_semantic_digest: EvidenceDigest,
    capture_page_ordinal: u16,
    segment_ordinal: u16,
    raw_claim_digest: EvidenceDigest,
    physical_receipt_digest: EvidenceDigest,
    physical_frame_ordinal: u32,
    page_body_digest: EvidenceDigest,
    received_at: Timestamp,
    source_sequence: Option<u64>,
}

/// Exact value-only logical/physical evidence for one ordered sealed raw object.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PersistedProviderCapturePhysicalClaim {
    raw_claim_digest: EvidenceDigest,
    capture_content_digest: EvidenceDigest,
    capture_observation_digest: EvidenceDigest,
    sealed_capture_receipt_digest: EvidenceDigest,
    claim: SealedResearchJournalSegmentClaim,
}

impl PersistedProviderCapturePhysicalClaim {
    /// Returns the exact canonical serialized-claim identity.
    pub const fn raw_claim_digest(&self) -> EvidenceDigest {
        self.raw_claim_digest
    }
    /// Returns the logical content identity for this physical object.
    pub const fn capture_content_digest(&self) -> EvidenceDigest {
        self.capture_content_digest
    }
    /// Returns the logical observation identity for this physical object.
    pub const fn capture_observation_digest(&self) -> EvidenceDigest {
        self.capture_observation_digest
    }
    /// Returns the exact common-owned logical-to-physical receipt digest.
    pub const fn sealed_capture_receipt_digest(&self) -> EvidenceDigest {
        self.sealed_capture_receipt_digest
    }
    /// Returns the immutable platform claim.
    pub const fn claim(&self) -> &SealedResearchJournalSegmentClaim {
        &self.claim
    }
}

impl PersistedProviderCaptureBindingRow {
    /// Returns the contiguous canonical row ordinal.
    pub const fn canonical_row_ordinal(&self) -> u32 {
        self.canonical_row_ordinal
    }
    /// Returns the canonical record digest bound by common.
    pub const fn canonical_row_digest(&self) -> EvidenceDigest {
        self.canonical_row_digest
    }
    /// Returns the exact bounded provider-native semantic bytes.
    pub fn native_semantic_payload(&self) -> &[u8] {
        &self.native_semantic_payload
    }
    /// Returns SHA-256 of the exact semantic bytes.
    pub const fn native_semantic_digest(&self) -> EvidenceDigest {
        self.native_semantic_digest
    }
    /// Returns the logical capture page ordinal.
    pub const fn capture_page_ordinal(&self) -> u16 {
        self.capture_page_ordinal
    }
    /// Returns the ordered physical segment ordinal.
    pub const fn segment_ordinal(&self) -> u16 {
        self.segment_ordinal
    }
    /// Returns the frame ordinal within the segment.
    pub const fn physical_frame_ordinal(&self) -> u32 {
        self.physical_frame_ordinal
    }
    /// Returns the exact provider body digest.
    pub const fn page_body_digest(&self) -> EvidenceDigest {
        self.page_body_digest
    }
    /// Returns the raw receipt clock.
    pub const fn received_at(&self) -> Timestamp {
        self.received_at
    }
    /// Returns the source sequence when present.
    pub const fn source_sequence(&self) -> Option<u64> {
        self.source_sequence
    }
}

/// Historical evidence for one already-published provider binding.
///
/// This value verifies existing lineage but cannot reconstruct live publication authority.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PersistedProviderCaptureBindingEvidence {
    binding_digest: EvidenceDigest,
    capture: ProviderCaptureSetReceipt,
    sealed_capture_receipt_digest: EvidenceDigest,
    scope: String,
    layout: String,
    component_ordinal: Option<u16>,
    extraction_content_identity: EvidenceDigest,
    record_count: usize,
    native_lineage: PersistedProviderNativeLineageSchema,
    row_mapping_digest: EvidenceDigest,
    rows: Vec<PersistedProviderCaptureBindingRow>,
    physical_claims: Vec<PersistedProviderCapturePhysicalClaim>,
}

impl PersistedProviderCaptureBindingEvidence {
    /// Returns the common-owned value identity of the original live binding.
    pub const fn binding_digest(&self) -> EvidenceDigest {
        self.binding_digest
    }
    /// Returns the exact logical provider capture receipt.
    pub const fn capture(&self) -> &ProviderCaptureSetReceipt {
        &self.capture
    }
    /// Returns the logical-to-physical sealed receipt digest.
    pub const fn sealed_capture_receipt_digest(&self) -> EvidenceDigest {
        self.sealed_capture_receipt_digest
    }
    /// Returns the provider-native lineage summary.
    pub const fn native_lineage(&self) -> &PersistedProviderNativeLineageSchema {
        &self.native_lineage
    }
    /// Returns the extraction content identity.
    pub const fn extraction_content_identity(&self) -> EvidenceDigest {
        self.extraction_content_identity
    }
    /// Returns the canonical record count.
    pub const fn record_count(&self) -> usize {
        self.record_count
    }
    /// Returns the compact row-coordinate digest.
    pub const fn row_mapping_digest(&self) -> EvidenceDigest {
        self.row_mapping_digest
    }
    /// Returns exact ordered row evidence.
    pub fn rows(&self) -> &[PersistedProviderCaptureBindingRow] {
        &self.rows
    }
    /// Returns exact ordered physical claims.
    pub fn physical_claims(&self) -> &[PersistedProviderCapturePhysicalClaim] {
        &self.physical_claims
    }
    /// Returns whether the binding consumed the whole capture or one graph component.
    pub fn scope(&self) -> &str {
        &self.scope
    }
    /// Returns the exact physical selection layout.
    pub fn layout(&self) -> &str {
        &self.layout
    }
    /// Returns the exact selected request-graph component, when applicable.
    pub const fn component_ordinal(&self) -> Option<u16> {
        self.component_ordinal
    }

    pub(crate) fn verify_integrity(&self) -> Result<(), CatalogError> {
        if self.record_count == 0
            || self.record_count != self.rows.len()
            || self.native_lineage.row_count != self.record_count
            || self.physical_claims.is_empty()
            || self.physical_claims.len() > MAX_PROVIDER_CAPTURE_SEGMENTS
        {
            return Err(CatalogError::CorruptCatalog);
        }
        let mut native_bytes = self
            .native_lineage
            .batch_sidecar
            .as_ref()
            .map_or(0, Vec::len);
        if native_bytes > MAX_PROVIDER_NATIVE_LINEAGE_SIDECAR_BYTES {
            return Err(CatalogError::CorruptCatalog);
        }
        for (ordinal, row) in self.rows.iter().enumerate() {
            native_bytes = native_bytes
                .checked_add(row.native_semantic_payload.len())
                .ok_or(CatalogError::CorruptCatalog)?;
            if native_bytes > MAX_PROVIDER_NATIVE_BYTES
                || row.canonical_row_ordinal
                    != u32::try_from(ordinal).map_err(|_| CatalogError::CorruptCatalog)?
                || row.native_semantic_payload.is_empty()
                || sha256_evidence(&row.native_semantic_payload) != row.native_semantic_digest
                || usize::from(row.segment_ordinal) >= self.physical_claims.len()
            {
                return Err(CatalogError::CorruptCatalog);
            }
            let claim = self.physical_claims[usize::from(row.segment_ordinal)].claim();
            let physical = &self.physical_claims[usize::from(row.segment_ordinal)];
            let frame = claim
                .frames()
                .get(
                    usize::try_from(row.physical_frame_ordinal)
                        .map_err(|_| CatalogError::CorruptCatalog)?,
                )
                .ok_or(CatalogError::CorruptCatalog)?;
            if frame.ordinal() != row.physical_frame_ordinal
                || physical.raw_claim_digest != row.raw_claim_digest
                || claim.physical_receipt_digest() != row.physical_receipt_digest
                || frame.provider_payload_digest() != row.page_body_digest
                || frame.received_at() != row.received_at
                || frame.source_sequence() != row.source_sequence
            {
                return Err(CatalogError::CorruptCatalog);
            }
        }
        if row_mapping_digest(&self.rows)? != self.row_mapping_digest {
            return Err(CatalogError::CorruptCatalog);
        }
        let implementation = parse_native_implementation(&self.native_lineage.implementation)?;
        let mut native_rows = Vec::new();
        native_rows
            .try_reserve_exact(self.rows.len())
            .map_err(|_| CatalogError::Allocation)?;
        let mut row_frames = Vec::new();
        row_frames
            .try_reserve_exact(self.rows.len())
            .map_err(|_| CatalogError::Allocation)?;
        for row in &self.rows {
            native_rows.push(
                ProviderNativeLineageRowEvidenceRef::try_new(
                    row.canonical_row_ordinal,
                    row.canonical_row_digest,
                    &row.native_semantic_payload,
                    row.native_semantic_digest,
                )
                .map_err(|_| CatalogError::CorruptCatalog)?,
            );
            row_frames.push(
                ProviderCaptureRowFrameEvidence::try_new(
                    row.canonical_row_ordinal,
                    row.capture_page_ordinal,
                    row.segment_ordinal,
                    row.physical_frame_ordinal,
                    row.page_body_digest,
                    row.received_at,
                    row.source_sequence,
                )
                .map_err(|_| CatalogError::CorruptCatalog)?,
            );
        }
        let batch_sidecar = match (
            self.native_lineage.batch_sidecar.as_deref(),
            self.native_lineage.batch_sidecar_digest,
        ) {
            (Some(payload), Some(digest)) => Some(
                ProviderNativeLineageBatchSidecarEvidenceRef::try_new(payload, digest)
                    .map_err(|_| CatalogError::CorruptCatalog)?,
            ),
            (None, None) => None,
            _ => return Err(CatalogError::CorruptCatalog),
        };
        verify_provider_native_lineage_batch_evidence(
            self.native_lineage.batch_digest,
            self.native_lineage.version,
            implementation,
            self.native_lineage.fingerprint,
            self.extraction_content_identity,
            self.record_count,
            &native_rows,
            batch_sidecar.as_ref(),
        )
        .map_err(|_| CatalogError::CorruptCatalog)?;
        let mut physical = Vec::new();
        physical
            .try_reserve_exact(self.physical_claims.len())
            .map_err(|_| CatalogError::Allocation)?;
        let mut claim_bytes = 0usize;
        for claim in &self.physical_claims {
            let claim_json = journal_claim_json(&claim.claim)?;
            claim_bytes = claim_bytes
                .checked_add(claim_json.len())
                .ok_or(CatalogError::CorruptCatalog)?;
            if claim_json.len() > MAX_PROVIDER_CLAIM_JSON_BYTES
                || claim_bytes > MAX_PROVIDER_CLAIM_JSON_BYTES * MAX_PROVIDER_CAPTURE_SEGMENTS
                || raw_claim_digest(claim_json.as_bytes()) != claim.raw_claim_digest
            {
                return Err(CatalogError::CorruptCatalog);
            }
            physical.push(
                ProviderCapturePhysicalClaimEvidenceRef::try_new(
                    claim.capture_content_digest,
                    claim.capture_observation_digest,
                    claim.sealed_capture_receipt_digest,
                    &claim.claim,
                )
                .map_err(|_| CatalogError::CorruptCatalog)?,
            );
        }
        ProviderCaptureBindingDigest::verify_evidence(
            self.binding_digest,
            &self.capture,
            self.sealed_capture_receipt_digest,
            parse_scope(&self.scope, self.component_ordinal)?,
            parse_layout(&self.layout)?,
            self.extraction_content_identity,
            self.record_count,
            self.record_count,
            self.native_lineage.version,
            implementation,
            self.native_lineage.fingerprint,
            self.native_lineage.batch_digest,
            self.native_lineage.row_count,
            &row_frames,
            &physical,
        )
        .map_err(|_| CatalogError::CorruptCatalog)?;
        Ok(())
    }
}

#[derive(Debug)]
pub(crate) struct PreparedProviderCaptureBinding {
    pub(crate) evidence: PersistedProviderCaptureBindingEvidence,
}

impl PreparedProviderCaptureBinding {
    pub(crate) fn try_from_live(
        binding: &SealedProviderCaptureBinding,
    ) -> Result<Self, CatalogError> {
        binding
            .validate()
            .map_err(|_| CatalogError::ProviderCaptureMismatch)?;
        let capture = binding.capture_evidence();
        let record_count = binding.record_count();
        if record_count == 0
            || record_count > MAX_PROVIDER_CAPTURE_ROWS
            || record_count != binding.native_lineage().rows().len()
            || record_count != binding.row_frames().len()
        {
            return Err(CatalogError::ProviderCaptureMismatch);
        }
        let segment_count = match binding.layout() {
            ProviderCaptureBindingLayout::WholeSingleSegment
            | ProviderCaptureBindingLayout::RequestGraphComponent => 1,
            ProviderCaptureBindingLayout::OrderedSegments => capture.pages().len(),
        };
        if segment_count == 0 || segment_count > MAX_PROVIDER_CAPTURE_SEGMENTS {
            return Err(CatalogError::ProviderCaptureMismatch);
        }
        let mut raw_claim_digests = Vec::new();
        raw_claim_digests
            .try_reserve_exact(segment_count)
            .map_err(|_| CatalogError::Allocation)?;
        let mut claim_bytes = 0usize;
        for ordinal in 0..segment_count {
            let receipt = binding
                .persisted_segment_receipt(ordinal)
                .ok_or(CatalogError::ProviderCaptureMismatch)?;
            let claim_json = journal_claim_json(receipt.segment().claim())?;
            claim_bytes = claim_bytes
                .checked_add(claim_json.len())
                .ok_or(CatalogError::ProviderCaptureMismatch)?;
            if claim_json.len() > MAX_PROVIDER_CLAIM_JSON_BYTES
                || claim_bytes > MAX_PROVIDER_CLAIM_JSON_BYTES * MAX_PROVIDER_CAPTURE_SEGMENTS
            {
                return Err(CatalogError::ResultByteLimitExceeded);
            }
            raw_claim_digests.push(raw_claim_digest(claim_json.as_bytes()));
        }
        if binding.persisted_segment_receipt(segment_count).is_some() {
            return Err(CatalogError::ProviderCaptureMismatch);
        }
        let row_native_bytes = binding
            .native_lineage()
            .rows()
            .iter()
            .try_fold(0usize, |total, row| {
                total.checked_add(row.semantic_payload().len())
            })
            .ok_or(CatalogError::ProviderCaptureMismatch)?;
        let sidecar_bytes = binding
            .native_lineage()
            .batch_sidecar()
            .map_or(0, |sidecar| sidecar.semantic_payload().len());
        if sidecar_bytes > MAX_PROVIDER_NATIVE_LINEAGE_SIDECAR_BYTES {
            return Err(CatalogError::ProviderCaptureMismatch);
        }
        let native_bytes = row_native_bytes
            .checked_add(sidecar_bytes)
            .ok_or(CatalogError::ProviderCaptureMismatch)?;
        let structural_bytes = record_count
            .checked_mul(std::mem::size_of::<PersistedProviderCaptureBindingRow>())
            .and_then(|bytes| {
                segment_count
                    .checked_mul(std::mem::size_of::<PersistedProviderCapturePhysicalClaim>())
                    .and_then(|claims| bytes.checked_add(claims))
            })
            .ok_or(CatalogError::ProviderCaptureMismatch)?;
        let peak_bytes = usize::try_from(MAX_IN_MEMORY_EXTRACTION_BATCH_BYTES)
            .ok()
            .and_then(|bytes| bytes.checked_mul(4))
            .and_then(|bytes| bytes.checked_add(native_bytes.checked_mul(2)?))
            .and_then(|bytes| bytes.checked_add(claim_bytes.checked_mul(2)?))
            .and_then(|bytes| bytes.checked_add(structural_bytes))
            .ok_or(CatalogError::ProviderCaptureMismatch)?;
        if native_bytes > MAX_PROVIDER_NATIVE_BYTES
            || peak_bytes > MAX_PROVIDER_PUBLICATION_PEAK_BYTES
        {
            return Err(CatalogError::ResultByteLimitExceeded);
        }
        let capture = capture.clone();
        let mut physical_claims = Vec::new();
        physical_claims
            .try_reserve_exact(segment_count)
            .map_err(|_| CatalogError::Allocation)?;
        for ordinal in 0..segment_count {
            let receipt = binding
                .persisted_segment_receipt(ordinal)
                .ok_or(CatalogError::ProviderCaptureMismatch)?;
            let raw_claim_digest = raw_claim_digests
                .get(ordinal)
                .copied()
                .ok_or(CatalogError::ProviderCaptureMismatch)?;
            physical_claims.push(PersistedProviderCapturePhysicalClaim {
                raw_claim_digest,
                capture_content_digest: receipt.capture().content_digest(),
                capture_observation_digest: receipt.capture().observation_digest(),
                sealed_capture_receipt_digest: receipt.receipt_digest(),
                claim: receipt.segment().claim().clone(),
            });
        }
        let native = binding.native_lineage();
        let mut rows = Vec::new();
        rows.try_reserve_exact(record_count)
            .map_err(|_| CatalogError::Allocation)?;
        for (native_row, frame) in native.rows().iter().zip(binding.row_frames()) {
            let mut semantic = Vec::new();
            semantic
                .try_reserve_exact(native_row.semantic_payload().len())
                .map_err(|_| CatalogError::Allocation)?;
            semantic.extend_from_slice(native_row.semantic_payload());
            let physical = physical_claims
                .get(usize::from(frame.segment_ordinal()))
                .ok_or(CatalogError::ProviderCaptureMismatch)?;
            rows.push(PersistedProviderCaptureBindingRow {
                canonical_row_ordinal: native_row.ordinal(),
                canonical_row_digest: native_row.canonical_record_digest(),
                native_semantic_payload: semantic,
                native_semantic_digest: native_row.semantic_payload_digest(),
                capture_page_ordinal: frame.capture_page_ordinal(),
                segment_ordinal: frame.segment_ordinal(),
                raw_claim_digest: physical.raw_claim_digest,
                physical_receipt_digest: physical.claim.physical_receipt_digest(),
                physical_frame_ordinal: frame.physical_frame_ordinal(),
                page_body_digest: frame.page_body_digest(),
                received_at: frame.received_at(),
                source_sequence: frame.source_sequence(),
            });
        }
        let schema = native.schema();
        let batch_sidecar = if let Some(sidecar) = native.batch_sidecar() {
            let mut payload = Vec::new();
            payload
                .try_reserve_exact(sidecar.semantic_payload().len())
                .map_err(|_| CatalogError::Allocation)?;
            payload.extend_from_slice(sidecar.semantic_payload());
            Some(payload)
        } else {
            None
        };
        let (scope, component_ordinal) = scope_columns(binding.scope());
        let evidence = PersistedProviderCaptureBindingEvidence {
            binding_digest: binding.evidence_digest().evidence(),
            capture,
            sealed_capture_receipt_digest: binding.sealed_capture_receipt_digest(),
            scope: scope.to_owned(),
            layout: layout_name(binding.layout()).to_owned(),
            component_ordinal,
            extraction_content_identity: binding.content_identity().digest(),
            record_count,
            native_lineage: PersistedProviderNativeLineageSchema {
                version: schema.version(),
                implementation: native_implementation_name(schema.implementation()).to_owned(),
                fingerprint: schema.fingerprint(),
                row_count: native.rows().len(),
                batch_digest: native.batch_digest(),
                batch_sidecar,
                batch_sidecar_digest: native
                    .batch_sidecar()
                    .map(|sidecar| sidecar.semantic_payload_digest()),
            },
            row_mapping_digest: row_mapping_digest(&rows)?,
            rows,
            physical_claims,
        };
        evidence.verify_integrity()?;
        Ok(Self { evidence })
    }

    pub(crate) const fn binding_digest(&self) -> EvidenceDigest {
        self.evidence.binding_digest
    }
    pub(crate) const fn source_id(&self) -> &market_squawk_domain::SourceId {
        self.evidence.capture.source_id()
    }
    pub(crate) const fn record_count(&self) -> usize {
        self.evidence.record_count
    }
    pub(crate) const fn capture_observation_digest(&self) -> EvidenceDigest {
        self.evidence.capture.observation_digest()
    }
    pub(crate) fn rows(&self) -> &[PersistedProviderCaptureBindingRow] {
        &self.evidence.rows
    }
}

impl Catalog {
    /// Loads bounded historical evidence for one exact published binding.
    pub fn provider_capture_binding_evidence(
        &self,
        binding_digest: EvidenceDigest,
    ) -> Result<Option<PersistedProviderCaptureBindingEvidence>, CatalogError> {
        load_provider_capture_binding_evidence(&self.connection, binding_digest)
    }

    pub(crate) fn provider_capture_for_run(
        &self,
        run_id: Uuid,
    ) -> Result<Option<PersistedProviderCaptureBindingEvidence>, CatalogError> {
        load_provider_capture_for_run(&self.connection, run_id)
    }

    pub(crate) fn authoritative_provider_capture_claim_page(
        &self,
        after: Option<EvidenceDigest>,
    ) -> Result<Vec<(EvidenceDigest, SealedResearchJournalSegmentClaim)>, CatalogError> {
        let mut claims = Vec::new();
        claims
            .try_reserve_exact(PROVIDER_CAPTURE_CLAIM_PAGE_ROWS)
            .map_err(|_| CatalogError::Allocation)?;
        let mut scan_after = after;
        while claims.len() < PROVIDER_CAPTURE_CLAIM_PAGE_ROWS {
            let page = self.authoritative_provider_raw_claim_page(scan_after)?;
            if page.is_empty() {
                break;
            }
            let page_was_full = page.len() == PROVIDER_CAPTURE_CLAIM_PAGE_ROWS;
            for (digest, claim) in page {
                scan_after = Some(digest);
                if let SealedResearchRawClaim::JournalSegment(claim) = claim {
                    claims.push((digest, claim));
                    if claims.len() == PROVIDER_CAPTURE_CLAIM_PAGE_ROWS {
                        break;
                    }
                }
            }
            if !page_was_full {
                break;
            }
        }
        Ok(claims)
    }

    /// Pages every authoritative sealed raw claim across journal and logical-object formats.
    pub(crate) fn authoritative_provider_raw_claim_page(
        &self,
        after: Option<EvidenceDigest>,
    ) -> Result<Vec<(EvidenceDigest, SealedResearchRawClaim)>, CatalogError> {
        let mut claims = Vec::new();
        claims
            .try_reserve_exact(PROVIDER_CAPTURE_CLAIM_PAGE_ROWS)
            .map_err(|_| CatalogError::Allocation)?;
        let limit = to_i64(PROVIDER_CAPTURE_CLAIM_PAGE_ROWS)?;
        if let Some(after) = after {
            let mut statement = self.connection.prepare(
                "SELECT raw_claim_digest, raw_claim_kind, raw_claim_json
                 FROM sealed_raw_objects
                 WHERE raw_claim_digest > ?1 AND (
                    EXISTS (
                      SELECT 1 FROM provider_capture_binding_objects AS object
                      WHERE object.raw_claim_digest=sealed_raw_objects.raw_claim_digest
                        AND object.physical_receipt_digest=sealed_raw_objects.physical_receipt_digest)
                    OR EXISTS (
                      SELECT 1 FROM provider_event_microbatch_objects AS object
                      WHERE object.raw_claim_digest=sealed_raw_objects.raw_claim_digest
                        AND object.physical_receipt_digest=sealed_raw_objects.physical_receipt_digest)
                    OR EXISTS (
                      SELECT 1
                      FROM provider_raw_observation_objects AS object
                      JOIN provider_response_market_event_bindings AS binding
                        ON binding.capture_observation_digest=object.capture_observation_digest
                      WHERE object.raw_claim_digest=sealed_raw_objects.raw_claim_digest
                        AND object.physical_receipt_digest=sealed_raw_objects.physical_receipt_digest)
                    OR EXISTS (
                      SELECT 1
                      FROM provider_raw_observation_objects AS object
                      JOIN provider_option_market_bindings AS binding
                        ON binding.capture_observation_digest=object.capture_observation_digest
                      WHERE object.raw_claim_digest=sealed_raw_objects.raw_claim_digest
                        AND object.physical_receipt_digest=sealed_raw_objects.physical_receipt_digest)
                    OR EXISTS (
                      SELECT 1 FROM provider_logical_publication_objects AS object
                      WHERE object.raw_claim_digest=sealed_raw_objects.raw_claim_digest
                        AND object.physical_receipt_digest=sealed_raw_objects.physical_receipt_digest)
                    OR EXISTS (
                      SELECT 1 FROM provider_logical_publication_partitions AS partition
                      WHERE partition.raw_claim_digest=sealed_raw_objects.raw_claim_digest
                        AND partition.physical_receipt_digest=sealed_raw_objects.physical_receipt_digest)
                    OR EXISTS (
                      SELECT 1 FROM official_options_reference_objects AS object
                      WHERE object.raw_claim_digest=sealed_raw_objects.raw_claim_digest
                        AND object.physical_receipt_digest=sealed_raw_objects.physical_receipt_digest)
                 ) ORDER BY raw_claim_digest LIMIT ?2",
            )?;
            let mut rows = statement.query(params![after.bytes().as_slice(), limit])?;
            append_authoritative_raw_claim_rows(&mut rows, &mut claims)?;
        } else {
            let mut statement = self.connection.prepare(
                "SELECT raw_claim_digest, raw_claim_kind, raw_claim_json
                 FROM sealed_raw_objects
                 WHERE EXISTS (
                      SELECT 1 FROM provider_capture_binding_objects AS object
                      WHERE object.raw_claim_digest=sealed_raw_objects.raw_claim_digest
                        AND object.physical_receipt_digest=sealed_raw_objects.physical_receipt_digest)
                    OR EXISTS (
                      SELECT 1 FROM provider_event_microbatch_objects AS object
                      WHERE object.raw_claim_digest=sealed_raw_objects.raw_claim_digest
                        AND object.physical_receipt_digest=sealed_raw_objects.physical_receipt_digest)
                    OR EXISTS (
                      SELECT 1
                      FROM provider_raw_observation_objects AS object
                      JOIN provider_response_market_event_bindings AS binding
                        ON binding.capture_observation_digest=object.capture_observation_digest
                      WHERE object.raw_claim_digest=sealed_raw_objects.raw_claim_digest
                        AND object.physical_receipt_digest=sealed_raw_objects.physical_receipt_digest)
                    OR EXISTS (
                      SELECT 1
                      FROM provider_raw_observation_objects AS object
                      JOIN provider_option_market_bindings AS binding
                        ON binding.capture_observation_digest=object.capture_observation_digest
                      WHERE object.raw_claim_digest=sealed_raw_objects.raw_claim_digest
                        AND object.physical_receipt_digest=sealed_raw_objects.physical_receipt_digest)
                    OR EXISTS (
                      SELECT 1 FROM provider_logical_publication_objects AS object
                      WHERE object.raw_claim_digest=sealed_raw_objects.raw_claim_digest
                        AND object.physical_receipt_digest=sealed_raw_objects.physical_receipt_digest)
                    OR EXISTS (
                      SELECT 1 FROM provider_logical_publication_partitions AS partition
                      WHERE partition.raw_claim_digest=sealed_raw_objects.raw_claim_digest
                        AND partition.physical_receipt_digest=sealed_raw_objects.physical_receipt_digest)
                    OR EXISTS (
                      SELECT 1 FROM official_options_reference_objects AS object
                      WHERE object.raw_claim_digest=sealed_raw_objects.raw_claim_digest
                        AND object.physical_receipt_digest=sealed_raw_objects.physical_receipt_digest)
                 ORDER BY raw_claim_digest LIMIT ?1",
            )?;
            let mut rows = statement.query([limit])?;
            append_authoritative_raw_claim_rows(&mut rows, &mut claims)?;
        }
        Ok(claims)
    }
}

pub(crate) fn load_provider_capture_for_run(
    connection: &Connection,
    run_id: Uuid,
) -> Result<Option<PersistedProviderCaptureBindingEvidence>, CatalogError> {
    let count: i64 = connection.query_row(
        "SELECT COUNT(*) FROM ingest_run_provider_capture_bindings WHERE run_id=?1",
        [run_id.to_string()],
        |row| row.get(0),
    )?;
    match count {
        0 => Ok(None),
        1 => load_ordered_provider_captures_for_run(connection, run_id)
            .map(|mut values| values.pop()),
        _ => Err(CatalogError::ProviderCaptureConflict),
    }
}

/// Loads every direct provider capture for one run in its exact retained input order.
pub(crate) fn load_ordered_provider_captures_for_run(
    connection: &Connection,
    run_id: Uuid,
) -> Result<Vec<PersistedProviderCaptureBindingEvidence>, CatalogError> {
    let count: i64 = connection.query_row(
        "SELECT COUNT(*) FROM ingest_run_provider_capture_bindings WHERE run_id=?1",
        [run_id.to_string()],
        |row| row.get(0),
    )?;
    let count = usize::try_from(count)
        .ok()
        .filter(|count| *count <= MAX_PROVIDER_CAPTURE_INPUTS)
        .ok_or(CatalogError::ProviderCaptureConflict)?;
    let limit = i64::try_from(MAX_PROVIDER_CAPTURE_INPUTS + 1)
        .map_err(|_| CatalogError::ProviderCaptureConflict)?;
    let mut statement = connection.prepare(
        "SELECT input_ordinal, binding_digest, source_id
         FROM ingest_run_provider_capture_bindings
         WHERE run_id=?1 ORDER BY input_ordinal LIMIT ?2",
    )?;
    let rows = statement.query_map(params![run_id.to_string(), limit], |row| {
        Ok((
            row.get::<_, i64>(0)?,
            row.get::<_, Vec<u8>>(1)?,
            row.get::<_, String>(2)?,
        ))
    })?;
    let mut retained = Vec::new();
    retained
        .try_reserve_exact(count)
        .map_err(|_| CatalogError::Allocation)?;
    for row in rows {
        if retained.len() == MAX_PROVIDER_CAPTURE_INPUTS {
            return Err(CatalogError::ProviderCaptureConflict);
        }
        let (ordinal, digest, source_id) = row?;
        if ordinal != i64::try_from(retained.len()).map_err(|_| CatalogError::CorruptCatalog)? {
            return Err(CatalogError::CorruptCatalog);
        }
        let digest = parse_digest(1, &digest)?;
        let evidence = load_provider_capture_binding_evidence(connection, digest)?
            .ok_or(CatalogError::CorruptCatalog)?;
        if evidence.binding_digest != digest || evidence.capture.source_id().as_str() != source_id {
            return Err(CatalogError::CorruptCatalog);
        }
        retained.push(evidence);
    }
    if retained.len() != count {
        return Err(CatalogError::CorruptCatalog);
    }
    retained.shrink_to_fit();
    Ok(retained)
}

pub(crate) fn retain_prepared_provider_capture_binding(
    connection: &rusqlite::Transaction<'_>,
    run_id: Uuid,
    prepared: &PreparedProviderCaptureBinding,
    recorded_at: Timestamp,
) -> Result<(), CatalogError> {
    retain_ordered_prepared_provider_capture_bindings(
        connection,
        run_id,
        std::slice::from_ref(prepared),
        recorded_at,
    )
}

/// Retains one complete ordered provider plan under contiguous input ordinals in the caller's
/// transaction. Any failed binding rolls the whole plan back with the surrounding publication.
pub(crate) fn retain_ordered_prepared_provider_capture_bindings(
    connection: &rusqlite::Transaction<'_>,
    run_id: Uuid,
    prepared: &[PreparedProviderCaptureBinding],
    recorded_at: Timestamp,
) -> Result<(), CatalogError> {
    if prepared.is_empty() || prepared.len() > MAX_PROVIDER_CAPTURE_INPUTS {
        return Err(CatalogError::ProviderCaptureConflict);
    }
    let source_id = prepared[0].source_id();
    if prepared.iter().enumerate().any(|(ordinal, binding)| {
        binding.source_id() != source_id
            || prepared[..ordinal]
                .iter()
                .any(|prior| prior.binding_digest() == binding.binding_digest())
    }) {
        return Err(CatalogError::ProviderCaptureConflict);
    }
    let existing = connection.query_row(
        "SELECT COUNT(*) FROM ingest_run_provider_capture_bindings WHERE run_id=?1",
        [run_id.to_string()],
        |row| row.get::<_, i64>(0),
    )?;
    if existing != 0 {
        return Err(CatalogError::ProviderCaptureConflict);
    }
    for (input_ordinal, binding) in prepared.iter().enumerate() {
        retain_prepared_provider_capture_binding_evidence(
            connection,
            run_id,
            binding,
            recorded_at,
        )?;
        let evidence = &binding.evidence;
        let associated: bool = connection.query_row(
            "SELECT EXISTS(
                SELECT 1 FROM ingest_run_provider_capture_bindings WHERE binding_digest=?1
             )",
            [digest_bytes(evidence.binding_digest)],
            |row| row.get(0),
        )?;
        if associated {
            return Err(CatalogError::ProviderCaptureConflict);
        }
        let input_ordinal =
            i64::try_from(input_ordinal).map_err(|_| CatalogError::ProviderCaptureConflict)?;
        let inserted = connection.execute(
            "INSERT INTO ingest_run_provider_capture_bindings
             (run_id, input_ordinal, binding_digest, source_id) VALUES (?1, ?2, ?3, ?4)",
            params![
                run_id.to_string(),
                input_ordinal,
                digest_bytes(evidence.binding_digest),
                evidence.capture.source_id().as_str()
            ],
        )?;
        if inserted != 1 {
            return Err(CatalogError::ProviderCaptureConflict);
        }
        let exact_association: bool = connection.query_row(
            "SELECT EXISTS(
                SELECT 1 FROM ingest_run_provider_capture_bindings
                WHERE run_id=?1 AND input_ordinal=?2 AND binding_digest=?3 AND source_id=?4
             )",
            params![
                run_id.to_string(),
                input_ordinal,
                digest_bytes(evidence.binding_digest),
                evidence.capture.source_id().as_str()
            ],
            |row| row.get(0),
        )?;
        if !exact_association {
            return Err(CatalogError::ProviderCaptureConflict);
        }
        append_audit(
            connection,
            "provider-capture-binding.retained",
            &run_id.to_string(),
            evidence.binding_digest.bytes(),
            recorded_at,
        )?;
    }
    let retained = load_ordered_provider_captures_for_run(connection, run_id)?;
    if retained.len() != prepared.len()
        || retained
            .iter()
            .zip(prepared)
            .any(|(persisted, expected)| persisted != &expected.evidence)
    {
        return Err(CatalogError::ProviderCaptureConflict);
    }
    Ok(())
}

pub(super) fn retain_prepared_provider_capture_binding_evidence(
    connection: &rusqlite::Transaction<'_>,
    run_id: Uuid,
    prepared: &PreparedProviderCaptureBinding,
    recorded_at: Timestamp,
) -> Result<(), CatalogError> {
    let evidence = &prepared.evidence;
    evidence.verify_integrity()?;
    let run_source: String = connection.query_row(
        "SELECT source_id FROM ingest_runs WHERE run_id=?1 AND state='reserved'",
        [run_id.to_string()],
        |row| row.get(0),
    )?;
    if run_source != evidence.capture.source_id().as_str() {
        return Err(CatalogError::ProviderCaptureMismatch);
    }
    validate_source_revision(connection, &evidence.capture)?;
    require_physical_claim_capacity(connection, evidence)?;
    insert_raw_observation(connection, evidence, recorded_at)?;
    insert_binding(connection, evidence, recorded_at)?;
    let retained = load_provider_capture_binding_evidence(connection, evidence.binding_digest)?
        .ok_or(CatalogError::ProviderCaptureConflict)?;
    if retained != *evidence {
        return Err(CatalogError::ProviderCaptureConflict);
    }
    Ok(())
}

fn require_physical_claim_capacity(
    connection: &Connection,
    evidence: &PersistedProviderCaptureBindingEvidence,
) -> Result<(), CatalogError> {
    let (retained, retained_bytes): (i64, i64) = connection.query_row(
        "SELECT physical_claims, physical_bytes
         FROM provider_capture_recovery_capacity WHERE singleton=1",
        [],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    let retained = usize::try_from(retained).map_err(|_| CatalogError::CorruptCatalog)?;
    let retained_bytes = u64::try_from(retained_bytes).map_err(|_| CatalogError::CorruptCatalog)?;
    if retained > MAX_PROVIDER_CAPTURE_PHYSICAL_CLAIMS
        || retained_bytes > MAX_PROVIDER_CAPTURE_PHYSICAL_BYTES
    {
        return Err(CatalogError::CorruptCatalog);
    }
    let mut new_digests = Vec::new();
    new_digests
        .try_reserve_exact(evidence.physical_claims.len())
        .map_err(|_| CatalogError::Allocation)?;
    let mut new_bytes = 0u64;
    for physical in &evidence.physical_claims {
        if new_digests.contains(&physical.raw_claim_digest.bytes()) {
            continue;
        }
        let exists: bool = connection.query_row(
            "SELECT EXISTS(SELECT 1 FROM sealed_raw_objects WHERE raw_claim_digest=?1)",
            [physical.raw_claim_digest.bytes().as_slice()],
            |row| row.get(0),
        )?;
        if !exists {
            new_digests.push(physical.raw_claim_digest.bytes());
            new_bytes = new_bytes.checked_add(physical.claim.size_bytes()).ok_or(
                CatalogError::ProviderCaptureCapacityExceeded {
                    max_claims: MAX_PROVIDER_CAPTURE_PHYSICAL_CLAIMS,
                    max_bytes: MAX_PROVIDER_CAPTURE_PHYSICAL_BYTES,
                },
            )?;
        }
    }
    if retained
        .checked_add(new_digests.len())
        .is_none_or(|total| total > MAX_PROVIDER_CAPTURE_PHYSICAL_CLAIMS)
        || retained_bytes
            .checked_add(new_bytes)
            .is_none_or(|total| total > MAX_PROVIDER_CAPTURE_PHYSICAL_BYTES)
    {
        return Err(CatalogError::ProviderCaptureCapacityExceeded {
            max_claims: MAX_PROVIDER_CAPTURE_PHYSICAL_CLAIMS,
            max_bytes: MAX_PROVIDER_CAPTURE_PHYSICAL_BYTES,
        });
    }
    Ok(())
}

fn append_authoritative_raw_claim_rows(
    rows: &mut rusqlite::Rows<'_>,
    claims: &mut Vec<(EvidenceDigest, SealedResearchRawClaim)>,
) -> Result<(), CatalogError> {
    while let Some(row) = rows.next()? {
        let digest = parse_digest(1, &row.get::<_, Vec<u8>>(0)?)?;
        let kind: String = row.get(1)?;
        let json: String = row.get(2)?;
        if json.len() > MAX_PROVIDER_CLAIM_JSON_BYTES {
            return Err(CatalogError::ResultByteLimitExceeded);
        }
        if raw_claim_digest(json.as_bytes()) != digest {
            return Err(CatalogError::CorruptCatalog);
        }
        let claim = parse_raw_claim(&kind, &json)?;
        claims.push((digest, claim));
    }
    Ok(())
}

fn insert_raw_observation(
    connection: &Connection,
    evidence: &PersistedProviderCaptureBindingEvidence,
    recorded_at: Timestamp,
) -> Result<(), CatalogError> {
    let capture = &evidence.capture;
    let capture_json = serde_json::to_string(capture)?;
    if capture_json.len() > MAX_PROVIDER_CLAIM_JSON_BYTES {
        return Err(CatalogError::ProviderCaptureMismatch);
    }
    let source_revision_digest: Vec<u8> = connection.query_row(
        "SELECT current_revision_digest FROM sources WHERE source_id=?1",
        [capture.source_id().as_str()],
        |row| row.get(0),
    )?;
    connection.execute(
        "INSERT OR IGNORE INTO provider_raw_observations
         (capture_observation_digest, capture_content_digest, source_id,
          source_revision_digest, metadata_revision, provider_dataset,
          request_set_identity, terminal_disposition, page_count, total_body_bytes,
          capture_json, recorded_at_ns)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
        params![
            digest_bytes(capture.observation_digest()),
            digest_bytes(capture.content_digest()),
            capture.source_id().as_str(),
            source_revision_digest,
            capture.metadata_revision().as_source_identifier().as_str(),
            capture.dataset().as_str(),
            digest_bytes(capture.request_set_identity()),
            terminal_name(capture.terminal()),
            to_i64(capture.pages().len())?,
            to_i64(capture.total_body_bytes())?,
            capture_json,
            recorded_at.unix_nanos()
        ],
    )?;
    for page in capture.pages() {
        insert_page(connection, capture.observation_digest(), page)?;
    }
    for (segment_ordinal, physical) in evidence.physical_claims.iter().enumerate() {
        let claim = &physical.claim;
        let claim_json = journal_claim_json(claim)?;
        if claim_json.len() > MAX_PROVIDER_CLAIM_JSON_BYTES {
            return Err(CatalogError::ProviderCaptureMismatch);
        }
        let claim_digest = raw_claim_digest(claim_json.as_bytes());
        if claim_digest != physical.raw_claim_digest {
            return Err(CatalogError::ProviderCaptureMismatch);
        }
        connection.execute(
            "INSERT OR IGNORE INTO sealed_raw_objects
             (raw_claim_digest, raw_claim_kind, physical_receipt_digest, relative_reference,
              content_digest, size_bytes, integrity_chunk_bytes, unit_count, raw_claim_json,
              recorded_at_ns)
             VALUES (?1, 'journal_segment', ?2, ?3, ?4, ?5, NULL, ?6, ?7, ?8)",
            params![
                digest_bytes(claim_digest),
                digest_bytes(claim.physical_receipt_digest()),
                claim.relative_reference(),
                digest_bytes(claim.content_digest()),
                to_i64(claim.size_bytes())?,
                to_i64(claim.frames().len())?,
                claim_json,
                recorded_at.unix_nanos()
            ],
        )?;
        connection.execute(
            "INSERT OR IGNORE INTO provider_raw_observation_objects
             (capture_observation_digest, input_ordinal, raw_claim_digest,
              physical_receipt_digest, object_capture_content_digest,
              object_capture_observation_digest, capture_receipt_digest)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![
                digest_bytes(capture.observation_digest()),
                to_i64(segment_ordinal)?,
                digest_bytes(claim_digest),
                digest_bytes(claim.physical_receipt_digest()),
                digest_bytes(physical.capture_content_digest),
                digest_bytes(physical.capture_observation_digest),
                digest_bytes(physical.sealed_capture_receipt_digest)
            ],
        )?;
        for frame in claim.frames() {
            let observation_ordinal = if evidence.layout == "ordered_segments" {
                segment_ordinal
            } else {
                usize::try_from(frame.ordinal())
                    .map_err(|_| CatalogError::ProviderCaptureMismatch)?
            };
            connection.execute(
                "INSERT OR IGNORE INTO provider_raw_observation_frames
                 (capture_observation_digest, observation_unit_ordinal,
                  raw_object_input_ordinal, raw_claim_digest, physical_receipt_digest,
                  raw_unit_ordinal, frame_offset, framed_bytes, provider_payload_bytes,
                  provider_payload_digest, received_at_ns, source_sequence)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
                params![
                    digest_bytes(capture.observation_digest()),
                    to_i64(observation_ordinal)?,
                    to_i64(segment_ordinal)?,
                    digest_bytes(physical.raw_claim_digest),
                    digest_bytes(claim.physical_receipt_digest()),
                    i64::from(frame.ordinal()),
                    to_i64(frame.offset())?,
                    to_i64(frame.framed_bytes())?,
                    to_i64(frame.provider_payload_bytes())?,
                    digest_bytes(frame.provider_payload_digest()),
                    frame.received_at().unix_nanos(),
                    source_sequence_blob(frame.source_sequence())
                ],
            )?;
        }
    }
    Ok(())
}

fn insert_binding(
    connection: &Connection,
    evidence: &PersistedProviderCaptureBindingEvidence,
    recorded_at: Timestamp,
) -> Result<(), CatalogError> {
    connection.execute(
        "INSERT OR IGNORE INTO provider_capture_bindings
         (binding_digest, binding_format_version, capture_observation_digest,
          sealed_capture_receipt_digest, capture_scope, binding_layout,
          request_graph_component_ordinal, extraction_content_digest,
          canonical_record_count, row_mapping_digest, recorded_at_ns)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
        params![
            digest_bytes(evidence.binding_digest),
            BINDING_FORMAT_VERSION,
            digest_bytes(evidence.capture.observation_digest()),
            digest_bytes(evidence.sealed_capture_receipt_digest),
            evidence.scope,
            evidence.layout,
            evidence.component_ordinal.map(i64::from),
            digest_bytes(evidence.extraction_content_identity),
            to_i64(evidence.record_count)?,
            digest_bytes(evidence.row_mapping_digest),
            recorded_at.unix_nanos()
        ],
    )?;
    for (input_ordinal, physical) in evidence.physical_claims.iter().enumerate() {
        connection.execute(
            "INSERT OR IGNORE INTO provider_capture_binding_objects
             (binding_digest, input_ordinal, capture_observation_digest,
              raw_claim_digest, physical_receipt_digest)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                digest_bytes(evidence.binding_digest),
                to_i64(input_ordinal)?,
                digest_bytes(evidence.capture.observation_digest()),
                digest_bytes(physical.raw_claim_digest),
                digest_bytes(physical.claim.physical_receipt_digest())
            ],
        )?;
    }
    let native = &evidence.native_lineage;
    connection.execute(
        "INSERT OR IGNORE INTO provider_capture_binding_native_lineage
         (binding_digest, schema_version, implementation, schema_fingerprint,
          row_count, batch_digest, batch_sidecar_payload, batch_sidecar_digest)
          VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
        params![
            digest_bytes(evidence.binding_digest),
            i64::from(native.version),
            native.implementation,
            digest_bytes(native.fingerprint),
            to_i64(native.row_count)?,
            digest_bytes(native.batch_digest),
            native.batch_sidecar.as_deref(),
            native.batch_sidecar_digest.map(digest_bytes)
        ],
    )?;
    for row in &evidence.rows {
        connection.execute(
            "INSERT OR IGNORE INTO provider_capture_binding_rows
             (binding_digest, capture_observation_digest, canonical_row_ordinal,
              canonical_record_digest, native_semantic_payload, native_semantic_digest,
              capture_page_ordinal, segment_ordinal, raw_claim_digest,
              physical_receipt_digest, physical_frame_ordinal, page_body_digest,
              received_at_ns, source_sequence)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)",
            params![
                digest_bytes(evidence.binding_digest),
                digest_bytes(evidence.capture.observation_digest()),
                i64::from(row.canonical_row_ordinal),
                digest_bytes(row.canonical_row_digest),
                row.native_semantic_payload,
                digest_bytes(row.native_semantic_digest),
                i64::from(row.capture_page_ordinal),
                i64::from(row.segment_ordinal),
                digest_bytes(row.raw_claim_digest),
                digest_bytes(row.physical_receipt_digest),
                i64::from(row.physical_frame_ordinal),
                digest_bytes(row.page_body_digest),
                row.received_at.unix_nanos(),
                source_sequence_blob(row.source_sequence)
            ],
        )?;
    }
    Ok(())
}

fn load_provider_capture_binding_evidence(
    connection: &Connection,
    binding_digest: EvidenceDigest,
) -> Result<Option<PersistedProviderCaptureBindingEvidence>, CatalogError> {
    type Header = (
        Vec<u8>,
        Vec<u8>,
        String,
        String,
        Option<i64>,
        Vec<u8>,
        i64,
        Vec<u8>,
    );
    let header: Option<Header> = connection
        .query_row(
            "SELECT capture_observation_digest, sealed_capture_receipt_digest, capture_scope,
                binding_layout, request_graph_component_ordinal, extraction_content_digest,
                canonical_record_count, row_mapping_digest
         FROM provider_capture_bindings WHERE binding_digest=?1",
            [digest_bytes(binding_digest)],
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
        )
        .optional()?;
    let Some((observation, sealed, scope, layout, component, content, count, row_map)) = header
    else {
        return Ok(None);
    };
    let observation_digest = parse_digest(1, &observation)?;
    let capture_json: String = connection.query_row(
        "SELECT capture_json FROM provider_raw_observations WHERE capture_observation_digest=?1",
        [digest_bytes(observation_digest)],
        |row| row.get(0),
    )?;
    if capture_json.len() > MAX_PROVIDER_CLAIM_JSON_BYTES {
        return Err(CatalogError::CorruptCatalog);
    }
    let capture: ProviderCaptureSetReceipt = serde_json::from_str(&capture_json)?;
    if capture.observation_digest() != observation_digest {
        return Err(CatalogError::CorruptCatalog);
    }
    type NativeHeader = (
        i64,
        String,
        Vec<u8>,
        i64,
        Vec<u8>,
        Option<Vec<u8>>,
        Option<Vec<u8>>,
    );
    let native: NativeHeader = connection.query_row(
        "SELECT schema_version, implementation, schema_fingerprint, row_count, batch_digest,
                batch_sidecar_payload, batch_sidecar_digest
         FROM provider_capture_binding_native_lineage WHERE binding_digest=?1",
        [digest_bytes(binding_digest)],
        |row| {
            Ok((
                row.get(0)?,
                row.get(1)?,
                row.get(2)?,
                row.get(3)?,
                row.get(4)?,
                row.get(5)?,
                row.get(6)?,
            ))
        },
    )?;
    if native
        .5
        .as_ref()
        .is_some_and(|payload| payload.len() > MAX_PROVIDER_NATIVE_LINEAGE_SIDECAR_BYTES)
        || native.5.is_some() != native.6.is_some()
    {
        return Err(CatalogError::CorruptCatalog);
    }
    let record_count = usize::try_from(count).map_err(|_| CatalogError::CorruptCatalog)?;
    let mut out = Vec::new();
    out.try_reserve_exact(record_count)
        .map_err(|_| CatalogError::Allocation)?;
    let mut statement = connection.prepare(
        "SELECT canonical_row_ordinal, canonical_record_digest, native_semantic_payload,
                native_semantic_digest, capture_page_ordinal, segment_ordinal,
                raw_claim_digest, physical_receipt_digest, physical_frame_ordinal,
                page_body_digest, received_at_ns, source_sequence
         FROM provider_capture_binding_rows WHERE binding_digest=?1 ORDER BY canonical_row_ordinal",
    )?;
    let mut sqlite_rows = statement.query([digest_bytes(binding_digest)])?;
    let mut native_bytes = native.5.as_ref().map_or(0, Vec::len);
    while let Some(row) = sqlite_rows.next()? {
        let semantic: Vec<u8> = row.get(2)?;
        let canonical_digest: Vec<u8> = row.get(1)?;
        let native_digest: Vec<u8> = row.get(3)?;
        let raw_claim: Vec<u8> = row.get(6)?;
        let physical_receipt: Vec<u8> = row.get(7)?;
        let page_digest: Vec<u8> = row.get(9)?;
        native_bytes = native_bytes
            .checked_add(semantic.len())
            .ok_or(CatalogError::CorruptCatalog)?;
        if native_bytes > MAX_PROVIDER_NATIVE_BYTES {
            return Err(CatalogError::ResultByteLimitExceeded);
        }
        out.push(PersistedProviderCaptureBindingRow {
            canonical_row_ordinal: u32::try_from(row.get::<_, i64>(0)?)
                .map_err(|_| CatalogError::CorruptCatalog)?,
            canonical_row_digest: parse_digest(1, &canonical_digest)?,
            native_semantic_payload: semantic,
            native_semantic_digest: parse_digest(1, &native_digest)?,
            capture_page_ordinal: u16::try_from(row.get::<_, i64>(4)?)
                .map_err(|_| CatalogError::CorruptCatalog)?,
            segment_ordinal: u16::try_from(row.get::<_, i64>(5)?)
                .map_err(|_| CatalogError::CorruptCatalog)?,
            raw_claim_digest: parse_digest(1, &raw_claim)?,
            physical_receipt_digest: parse_digest(1, &physical_receipt)?,
            physical_frame_ordinal: u32::try_from(row.get::<_, i64>(8)?)
                .map_err(|_| CatalogError::CorruptCatalog)?,
            page_body_digest: parse_digest(1, &page_digest)?,
            received_at: Timestamp::from_unix_nanos(row.get(10)?),
            source_sequence: parse_source_sequence(row.get(11)?),
        });
        if out.len() > MAX_PROVIDER_CAPTURE_ROWS {
            return Err(CatalogError::ResultRowLimitExceeded);
        }
    }
    let mut physical_claims = Vec::new();
    let mut statement = connection.prepare(
        "SELECT selected.raw_claim_digest, edge.object_capture_content_digest,
                edge.object_capture_observation_digest, edge.capture_receipt_digest,
                object.raw_claim_json
         FROM provider_capture_binding_objects AS selected
         JOIN provider_raw_observation_objects AS edge
           ON edge.capture_observation_digest=selected.capture_observation_digest
          AND edge.input_ordinal=selected.input_ordinal
          AND edge.raw_claim_digest=selected.raw_claim_digest
          AND edge.physical_receipt_digest=selected.physical_receipt_digest
         JOIN sealed_raw_objects AS object
           ON object.raw_claim_digest=edge.raw_claim_digest
          AND object.physical_receipt_digest=edge.physical_receipt_digest
          AND object.raw_claim_kind='journal_segment'
         WHERE selected.binding_digest=?1 ORDER BY selected.input_ordinal",
    )?;
    let mut claim_rows = statement.query([digest_bytes(binding_digest)])?;
    let mut claim_bytes = 0usize;
    while let Some(row) = claim_rows.next()? {
        let raw_claim: Vec<u8> = row.get(0)?;
        let content: Vec<u8> = row.get(1)?;
        let observation: Vec<u8> = row.get(2)?;
        let receipt: Vec<u8> = row.get(3)?;
        let json: String = row.get(4)?;
        claim_bytes = claim_bytes
            .checked_add(json.len())
            .ok_or(CatalogError::CorruptCatalog)?;
        if json.len() > MAX_PROVIDER_CLAIM_JSON_BYTES
            || claim_bytes > MAX_PROVIDER_CLAIM_JSON_BYTES * MAX_PROVIDER_CAPTURE_SEGMENTS
            || physical_claims.len() == MAX_PROVIDER_CAPTURE_SEGMENTS
        {
            return Err(CatalogError::ResultByteLimitExceeded);
        }
        physical_claims.push(PersistedProviderCapturePhysicalClaim {
            raw_claim_digest: parse_digest(1, &raw_claim)?,
            capture_content_digest: parse_digest(1, &content)?,
            capture_observation_digest: parse_digest(1, &observation)?,
            sealed_capture_receipt_digest: parse_digest(1, &receipt)?,
            claim: parse_journal_claim(&json)?,
        });
    }
    let evidence = PersistedProviderCaptureBindingEvidence {
        binding_digest,
        capture,
        sealed_capture_receipt_digest: parse_digest(1, &sealed)?,
        scope,
        layout,
        component_ordinal: component
            .map(u16::try_from)
            .transpose()
            .map_err(|_| CatalogError::CorruptCatalog)?,
        extraction_content_identity: parse_digest(1, &content)?,
        record_count,
        native_lineage: PersistedProviderNativeLineageSchema {
            version: u16::try_from(native.0).map_err(|_| CatalogError::CorruptCatalog)?,
            implementation: native.1,
            fingerprint: parse_digest(1, &native.2)?,
            row_count: usize::try_from(native.3).map_err(|_| CatalogError::CorruptCatalog)?,
            batch_digest: parse_digest(1, &native.4)?,
            batch_sidecar: native.5,
            batch_sidecar_digest: native
                .6
                .map(|digest| parse_digest(1, &digest))
                .transpose()?,
        },
        row_mapping_digest: parse_digest(1, &row_map)?,
        rows: out,
        physical_claims,
    };
    evidence.verify_integrity()?;
    Ok(Some(evidence))
}

fn insert_page(
    connection: &Connection,
    observation: EvidenceDigest,
    page: &ProviderCapturePageReceipt,
) -> Result<(), CatalogError> {
    connection.execute(
        "INSERT OR IGNORE INTO provider_raw_observation_pages
         (capture_observation_digest, page_ordinal, request_identity,
          request_page_token_digest, response_next_page_token_digest, http_status, body_bytes,
          body_digest, received_at_ns) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
        params![
            digest_bytes(observation),
            i64::from(page.ordinal()),
            digest_bytes(page.request_identity()),
            page.request_page_token_digest().map(digest_bytes),
            page.response_next_page_token_digest().map(digest_bytes),
            i64::from(page.http_status()),
            to_i64(page.body_bytes())?,
            digest_bytes(page.body_digest()),
            page.received_at().unix_nanos()
        ],
    )?;
    Ok(())
}

fn validate_source_revision(
    connection: &Connection,
    capture: &ProviderCaptureSetReceipt,
) -> Result<(), CatalogError> {
    let (digest, json): (Vec<u8>, String) = connection.query_row(
        "SELECT sources.current_revision_digest, revisions.metadata_json
         FROM sources JOIN source_revisions AS revisions
           ON revisions.source_id=sources.source_id
          AND revisions.revision_digest=sources.current_revision_digest
         WHERE sources.source_id=?1",
        [capture.source_id().as_str()],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    if digest.as_slice() != sha256(json.as_bytes()) {
        return Err(CatalogError::CorruptCatalog);
    }
    let source: SourceMetadata = serde_json::from_str(&json)?;
    if source.source_id() != capture.source_id() || source.revision() != capture.metadata_revision()
    {
        return Err(CatalogError::ProviderCaptureMismatch);
    }
    Ok(())
}

fn row_mapping_digest(
    rows: &[PersistedProviderCaptureBindingRow],
) -> Result<EvidenceDigest, CatalogError> {
    let mut hash = Sha256::new();
    hash_field(&mut hash, ROW_MAPPING_DIGEST_DOMAIN)?;
    hash.update(
        u64::try_from(rows.len())
            .map_err(|_| CatalogError::InvalidRecord)?
            .to_be_bytes(),
    );
    for row in rows {
        hash.update(row.canonical_row_ordinal.to_be_bytes());
        hash.update(row.canonical_row_digest.bytes());
        hash.update(row.native_semantic_digest.bytes());
        hash.update(row.capture_page_ordinal.to_be_bytes());
        hash.update(row.segment_ordinal.to_be_bytes());
        hash.update(row.physical_frame_ordinal.to_be_bytes());
        hash.update(row.page_body_digest.bytes());
        hash.update(row.received_at.unix_nanos().to_be_bytes());
        match row.source_sequence {
            Some(sequence) => {
                hash.update([1]);
                hash.update(sequence.to_be_bytes());
            }
            None => hash.update([0]),
        }
    }
    Ok(EvidenceDigest::new(
        DigestAlgorithm::Sha256,
        hash.finalize().into(),
    ))
}

pub(super) fn source_sequence_blob(sequence: Option<u64>) -> Option<[u8; 8]> {
    sequence.map(u64::to_be_bytes)
}

pub(super) fn parse_source_sequence(bytes: Option<[u8; 8]>) -> Option<u64> {
    bytes.map(u64::from_be_bytes)
}

pub(super) fn raw_claim_digest(json: &[u8]) -> EvidenceDigest {
    let mut hash = Sha256::new();
    hash.update(RAW_CLAIM_DIGEST_DOMAIN);
    hash.update((json.len() as u64).to_be_bytes());
    hash.update(json);
    EvidenceDigest::new(DigestAlgorithm::Sha256, hash.finalize().into())
}

fn journal_claim_json(claim: &SealedResearchJournalSegmentClaim) -> Result<String, CatalogError> {
    serde_json::to_string(&SealedResearchRawClaim::JournalSegment(claim.clone()))
        .map_err(Into::into)
}

fn parse_journal_claim(json: &str) -> Result<SealedResearchJournalSegmentClaim, CatalogError> {
    match parse_raw_claim("journal_segment", json)? {
        SealedResearchRawClaim::JournalSegment(claim) => Ok(claim),
        SealedResearchRawClaim::LogicalObject(_) => Err(CatalogError::CorruptCatalog),
    }
}

fn parse_raw_claim(kind: &str, json: &str) -> Result<SealedResearchRawClaim, CatalogError> {
    let claim: SealedResearchRawClaim = serde_json::from_str(json)?;
    if serde_json::to_string(&claim)? == json
        && matches!(
            (kind, &claim),
            ("journal_segment", SealedResearchRawClaim::JournalSegment(_))
                | ("logical_object", SealedResearchRawClaim::LogicalObject(_))
        )
    {
        Ok(claim)
    } else {
        Err(CatalogError::CorruptCatalog)
    }
}

fn hash_field(hash: &mut Sha256, value: &[u8]) -> Result<(), CatalogError> {
    hash.update(
        u64::try_from(value.len())
            .map_err(|_| CatalogError::InvalidRecord)?
            .to_be_bytes(),
    );
    hash.update(value);
    Ok(())
}

fn sha256_evidence(bytes: &[u8]) -> EvidenceDigest {
    EvidenceDigest::new(DigestAlgorithm::Sha256, Sha256::digest(bytes).into())
}

fn digest_bytes(digest: EvidenceDigest) -> [u8; 32] {
    digest.bytes()
}

fn to_i64<T>(value: T) -> Result<i64, CatalogError>
where
    i64: TryFrom<T>,
{
    i64::try_from(value).map_err(|_| CatalogError::InvalidRecord)
}

fn scope_columns(scope: ProviderCaptureScope) -> (&'static str, Option<u16>) {
    match scope {
        ProviderCaptureScope::Whole => ("whole", None),
        ProviderCaptureScope::RequestGraphComponent { ordinal } => ("component", Some(ordinal)),
    }
}

const fn layout_name(layout: ProviderCaptureBindingLayout) -> &'static str {
    match layout {
        ProviderCaptureBindingLayout::WholeSingleSegment => "whole_single_segment",
        ProviderCaptureBindingLayout::RequestGraphComponent => "request_graph_component",
        ProviderCaptureBindingLayout::OrderedSegments => "ordered_segments",
    }
}

pub(super) const fn native_implementation_name(
    implementation: ProviderNativeLineageImplementation,
) -> &'static str {
    match implementation {
        ProviderNativeLineageImplementation::BeaRegionalV1 => "bea_regional_v1",
        ProviderNativeLineageImplementation::BlsTimeseriesV1 => "bls_timeseries_v1",
        ProviderNativeLineageImplementation::CensusTabularV1 => "census_tabular_v1",
        ProviderNativeLineageImplementation::CoinbaseAdvancedTradeV1 => {
            "coinbase_advanced_trade_v1"
        }
        ProviderNativeLineageImplementation::CoinbaseExchangeDirectV1 => {
            "coinbase_exchange_direct_v1"
        }
        ProviderNativeLineageImplementation::EiaSeriesV1 => "eia_series_v1",
        ProviderNativeLineageImplementation::FredAlfredSeriesObservationsV1 => {
            "fred_alfred_series_observations_v1"
        }
        ProviderNativeLineageImplementation::KrakenSpotV1 => "kraken_spot_v1",
        ProviderNativeLineageImplementation::SecEdgarV1 => "sec_edgar_v1",
        ProviderNativeLineageImplementation::AlpacaHistoricalBarV1 => "alpaca_historical_bar_v1",
        ProviderNativeLineageImplementation::SchwabRestMarketDataV1 => "schwab_rest_market_data_v1",
        ProviderNativeLineageImplementation::SchwabStreamerMarketDataV1 => {
            "schwab_streamer_market_data_v1"
        }
        ProviderNativeLineageImplementation::TiingoFundNavV1 => "tiingo_fund_nav_v1",
        ProviderNativeLineageImplementation::TiingoEodMarketBarV1 => "tiingo_eod_market_bar_v1",
        ProviderNativeLineageImplementation::UsTreasuryMacroV1 => "us_treasury_macro_v1",
        ProviderNativeLineageImplementation::YahooEnrichmentV1 => "yahoo_enrichment_v1",
    }
}

pub(super) fn parse_native_implementation(
    value: &str,
) -> Result<ProviderNativeLineageImplementation, CatalogError> {
    match value {
        "bea_regional_v1" => Ok(ProviderNativeLineageImplementation::BeaRegionalV1),
        "bls_timeseries_v1" => Ok(ProviderNativeLineageImplementation::BlsTimeseriesV1),
        "census_tabular_v1" => Ok(ProviderNativeLineageImplementation::CensusTabularV1),
        "coinbase_advanced_trade_v1" => {
            Ok(ProviderNativeLineageImplementation::CoinbaseAdvancedTradeV1)
        }
        "coinbase_exchange_direct_v1" => {
            Ok(ProviderNativeLineageImplementation::CoinbaseExchangeDirectV1)
        }
        "eia_series_v1" => Ok(ProviderNativeLineageImplementation::EiaSeriesV1),
        "fred_alfred_series_observations_v1" => {
            Ok(ProviderNativeLineageImplementation::FredAlfredSeriesObservationsV1)
        }
        "kraken_spot_v1" => Ok(ProviderNativeLineageImplementation::KrakenSpotV1),
        "sec_edgar_v1" => Ok(ProviderNativeLineageImplementation::SecEdgarV1),
        "alpaca_historical_bar_v1" => {
            Ok(ProviderNativeLineageImplementation::AlpacaHistoricalBarV1)
        }
        "schwab_rest_market_data_v1" => {
            Ok(ProviderNativeLineageImplementation::SchwabRestMarketDataV1)
        }
        "schwab_streamer_market_data_v1" => {
            Ok(ProviderNativeLineageImplementation::SchwabStreamerMarketDataV1)
        }
        "tiingo_fund_nav_v1" => Ok(ProviderNativeLineageImplementation::TiingoFundNavV1),
        "tiingo_eod_market_bar_v1" => Ok(ProviderNativeLineageImplementation::TiingoEodMarketBarV1),
        "us_treasury_macro_v1" => Ok(ProviderNativeLineageImplementation::UsTreasuryMacroV1),
        "yahoo_enrichment_v1" => Ok(ProviderNativeLineageImplementation::YahooEnrichmentV1),
        _ => Err(CatalogError::CorruptCatalog),
    }
}

#[cfg(test)]
mod tests {
    use super::{native_implementation_name, parse_native_implementation};
    use market_squawk_sources::ProviderNativeLineageImplementation;

    #[test]
    fn fred_and_treasury_native_implementations_round_trip_through_catalog_names() {
        for implementation in [
            ProviderNativeLineageImplementation::FredAlfredSeriesObservationsV1,
            ProviderNativeLineageImplementation::UsTreasuryMacroV1,
        ] {
            assert!(matches!(
                parse_native_implementation(native_implementation_name(implementation)),
                Ok(parsed) if parsed == implementation
            ));
        }
    }
}

fn parse_scope(
    value: &str,
    component_ordinal: Option<u16>,
) -> Result<ProviderCaptureScope, CatalogError> {
    match (value, component_ordinal) {
        ("whole", None) => Ok(ProviderCaptureScope::Whole),
        ("component", Some(ordinal)) => Ok(ProviderCaptureScope::RequestGraphComponent { ordinal }),
        _ => Err(CatalogError::CorruptCatalog),
    }
}

fn parse_layout(value: &str) -> Result<ProviderCaptureBindingLayout, CatalogError> {
    match value {
        "whole_single_segment" => Ok(ProviderCaptureBindingLayout::WholeSingleSegment),
        "request_graph_component" => Ok(ProviderCaptureBindingLayout::RequestGraphComponent),
        "ordered_segments" => Ok(ProviderCaptureBindingLayout::OrderedSegments),
        _ => Err(CatalogError::CorruptCatalog),
    }
}

const fn terminal_name(
    terminal: market_squawk_sources::ProviderCaptureTerminalDisposition,
) -> &'static str {
    use market_squawk_sources::ProviderCaptureTerminalDisposition as Terminal;
    match terminal {
        Terminal::StandaloneResponse => "standalone_response",
        Terminal::ExhaustedWithoutNextPage => "exhausted_without_next_page",
        Terminal::CompleteRequestGraph => "complete_request_graph",
    }
}
