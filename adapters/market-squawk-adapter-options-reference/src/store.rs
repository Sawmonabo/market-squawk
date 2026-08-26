//! Content-addressed raw capture, atomic generation publication, and bounded typed reads.

use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::num::{NonZeroU32, NonZeroU64};

#[cfg(unix)]
use std::os::unix::fs::MetadataExt as _;

use bytes::Bytes;
use cap_fs_ext::{FollowSymlinks, OpenOptionsFollowExt as _};
use cap_std::fs::{Dir, OpenOptions};
use market_squawk_domain::{
    AvailabilityEvidence, CalendarDate, DigestAlgorithm, EvidenceDigest, ProviderInstrumentId,
    ResearchTemporalCoordinate, SourceIdentifier, Timestamp,
};
use rusqlite::{Connection, OpenFlags, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;
use uuid::Uuid;

use crate::{
    CanonicalReferenceIdentityState, CatalogCounts, CboeContractReferenceView, CboeSymbolId,
    CboeVenue, CboeVenuePresenceView, HttpLastModifiedEvidence, ObjectClockEvidence,
    OccExchangeCode, OccExchangeListingEvidence, OccPositionLimit, OccProductReferenceView,
    OccProductType, OptionContractIdentity, PublicationCatalog, PublicationCompleteness,
    PublicationLimits, PublicationRequest, ReferenceCancellation, ReferenceFetchControl,
    ReferenceHttpReceipt, ReferenceObjectContext, ReferencePublicationSpool, ReferenceSpoolLimits,
    ReferenceSurface, ReferenceTransportEvidence, RejectedReferenceGeneration,
    StagedReferenceGeneration,
};

#[cfg(test)]
use crate::RetrievedReferenceObject;

const RAW_DIRECTORY: &str = "raw";
const GENERATION_DIRECTORY: &str = "generations";
const QUARANTINE_DIRECTORY: &str = "quarantine";
const MANIFEST_DIRECTORY: &str = "manifests";
const STAGING_DIRECTORY: &str = "staging";
const CURRENT_ACTIVATION_POINTER: &str = "CURRENT";
const ACTIVATION_LOCK_FILE: &str = "LOCK";
const STORE_LAYOUT_VERSION: u16 = 3;
const SPOOL_SCHEMA_VERSION: i64 = 6;
const MAX_RECOVERY_RECEIPTS: usize = 128;
const MAX_RECOVERY_STAGING_ENTRIES: usize = 256;
const MAX_RECOVERY_NAMESPACE_ENTRIES: usize = 100_000;
const MAX_RAW_OBJECTS_PER_GENERATION: usize = 64;
const STAGED_RAW_PREFIX: &str = ".options-reference-raw-";
const STAGED_GENERATION_PREFIX: &str = ".options-reference-generation-";
const STAGED_SPOOL_PREFIX: &str = ".options-reference-spool-";
const DOCTOR_PROBE_PREFIX: &str = ".options-reference-doctor-";
const STAGED_MANIFEST_PREFIX: &str = ".options-reference-manifest-";
const RAW_STREAM_CHANNEL_CAPACITY: usize = 8;
const RAW_STREAM_WRITE_CHUNK_BYTES: usize = 64 * 1024;
const MAX_MANIFEST_BYTES: u64 = 8 * 1024;
const MAX_ACTIVATION_BYTES: u64 = 64 * 1024;
const MAX_MANIFEST_NAMESPACE_ENTRIES: usize = 512;
const MIN_STORE_FREE_RESERVE_BYTES: u64 = 64 * 1024 * 1024;
const RECEIPT_MANIFEST_MAGIC: &str = "market-squawk-options-reference-receipt-v3";
const ACTIVATION_INDEX_MAGIC: &str = "market-squawk-options-reference-activation-v3";
const CURRENT_POINTER_MAGIC: &str = "market-squawk-options-reference-current-v3";
const MAX_CANONICAL_EXPORT_PAGE_ROWS: u32 = 10_000;
const MAX_CANONICAL_EXPORT_PAGE_EVIDENCE_BYTES: u64 = 64 * 1024 * 1024;
const MAX_EXACT_QUERY_EVIDENCE_BYTES: u64 = 4 * 1024 * 1024;

pub(crate) enum RawStreamFrame {
    Chunk(Bytes),
    Complete,
}

pub(crate) struct StreamedRawCaptureReceipt {
    directory: Dir,
    staged_name: String,
    _operation_lock: OwnedManifestLockGuard,
    pub(crate) storage_name: SourceIdentifier,
    pub(crate) digest: EvidenceDigest,
    pub(crate) bytes: u64,
}

impl Drop for StreamedRawCaptureReceipt {
    fn drop(&mut self) {
        let _ = self.directory.remove_file(&self.staged_name);
    }
}

pub(crate) struct ReferenceRawStreamSink {
    directory: Dir,
    operation_lock: File,
    control: ReferenceFetchControl,
    worker_cancellation: ReferenceCancellation,
}

impl ReferenceRawStreamSink {
    pub(crate) fn persist_receiver(
        self,
        mut receiver: tokio::sync::mpsc::Receiver<RawStreamFrame>,
        expected_bytes: Option<u64>,
        maximum_bytes: u64,
    ) -> Result<StreamedRawCaptureReceipt, ReferenceStoreError> {
        let Self {
            directory,
            operation_lock,
            control,
            worker_cancellation,
        } = self;
        let operation_lock = OwnedManifestLockGuard::try_shared(operation_lock)?;
        if maximum_bytes == 0 || expected_bytes.is_some_and(|bytes| bytes > maximum_bytes) {
            return Err(ReferenceStoreError::RawEvidenceMismatch);
        }
        let (staged_name, mut staged) = create_capability_staging(&directory, STAGED_RAW_PREFIX)?;
        let mut cleanup = CapabilityStagingCleanup {
            directory: &directory,
            name: &staged_name,
            armed: true,
        };
        let mut digest = Sha256::new();
        let mut observed = 0_u64;
        let mut complete = false;
        while let Some(frame) = receiver.blocking_recv() {
            ensure_raw_stream_worker_open(&control, &worker_cancellation)?;
            match frame {
                RawStreamFrame::Chunk(chunk) if !complete => {
                    observed = observed
                        .checked_add(
                            u64::try_from(chunk.len())
                                .map_err(|_| ReferenceStoreError::RawEvidenceMismatch)?,
                        )
                        .ok_or(ReferenceStoreError::RawEvidenceMismatch)?;
                    if observed > maximum_bytes
                        || expected_bytes.is_some_and(|expected| observed > expected)
                    {
                        return Err(ReferenceStoreError::RawEvidenceMismatch);
                    }
                    for part in chunk.chunks(RAW_STREAM_WRITE_CHUNK_BYTES) {
                        ensure_raw_stream_worker_open(&control, &worker_cancellation)?;
                        staged
                            .write_all(part)
                            .map_err(|_| ReferenceStoreError::StoreIo)?;
                        digest.update(part);
                    }
                }
                RawStreamFrame::Complete if !complete => complete = true,
                RawStreamFrame::Chunk(_) | RawStreamFrame::Complete => {
                    return Err(ReferenceStoreError::RawEvidenceMismatch);
                }
            }
        }
        if !complete || observed == 0 || expected_bytes.is_some_and(|expected| expected != observed)
        {
            return Err(ReferenceStoreError::RawEvidenceMismatch);
        }
        ensure_raw_stream_worker_open(&control, &worker_cancellation)?;
        staged
            .sync_all()
            .map_err(|_| ReferenceStoreError::StoreIo)?;
        ensure_raw_stream_worker_open(&control, &worker_cancellation)?;
        let digest: [u8; 32] = digest.finalize().into();
        let storage_name = format!("{}.raw", hex_digest(digest));
        cleanup.disarm();
        drop(cleanup);
        Ok(StreamedRawCaptureReceipt {
            directory,
            staged_name,
            _operation_lock: operation_lock,
            storage_name: SourceIdentifier::try_from(storage_name)
                .map_err(|_| ReferenceStoreError::InvalidReceipt)?,
            digest: EvidenceDigest::new(DigestAlgorithm::Sha256, digest),
            bytes: observed,
        })
    }
}

fn ensure_raw_stream_worker_open(
    control: &ReferenceFetchControl,
    worker_cancellation: &ReferenceCancellation,
) -> Result<(), ReferenceStoreError> {
    if worker_cancellation.is_cancelled() {
        return Err(ReferenceStoreError::PublicationCancelled);
    }
    match control.ensure_open() {
        Ok(()) => Ok(()),
        Err(crate::ReferenceTransportError::Cancelled) => {
            Err(ReferenceStoreError::PublicationCancelled)
        }
        Err(crate::ReferenceTransportError::DeadlineExceeded) => {
            Err(ReferenceStoreError::PublicationDeadlineExceeded)
        }
        Err(_) => Err(ReferenceStoreError::PublicationControlUnavailable),
    }
}

struct CapabilityStagingCleanup<'a> {
    directory: &'a Dir,
    name: &'a str,
    armed: bool,
}

struct ManifestLockGuard<'a> {
    file: &'a File,
}

struct OwnedManifestLockGuard {
    file: File,
}

impl OwnedManifestLockGuard {
    fn try_shared(file: File) -> Result<Self, ReferenceStoreError> {
        fs2::FileExt::try_lock_shared(&file).map_err(map_manifest_lock_error)?;
        Ok(Self { file })
    }
}

impl Drop for OwnedManifestLockGuard {
    fn drop(&mut self) {
        let _ = fs2::FileExt::unlock(&self.file);
    }
}

impl<'a> ManifestLockGuard<'a> {
    fn try_shared(file: &'a File) -> Result<Self, ReferenceStoreError> {
        fs2::FileExt::try_lock_shared(file).map_err(map_manifest_lock_error)?;
        Ok(Self { file })
    }

    fn try_exclusive(file: &'a File) -> Result<Self, ReferenceStoreError> {
        fs2::FileExt::try_lock_exclusive(file).map_err(map_manifest_lock_error)?;
        Ok(Self { file })
    }
}

impl Drop for ManifestLockGuard<'_> {
    fn drop(&mut self) {
        let _ = fs2::FileExt::unlock(self.file);
    }
}

fn map_manifest_lock_error(error: std::io::Error) -> ReferenceStoreError {
    if error.kind() == std::io::ErrorKind::WouldBlock {
        ReferenceStoreError::ActivationBusy
    } else {
        ReferenceStoreError::StoreIo
    }
}

impl CapabilityStagingCleanup<'_> {
    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for CapabilityStagingCleanup<'_> {
    fn drop(&mut self) {
        if self.armed {
            let _ = self.directory.remove_file(self.name);
        }
    }
}

/// Exact durable raw-object receipt after content-addressed no-replace publication.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SealedReferenceRawObject {
    layout_version: u16,
    storage_name: SourceIdentifier,
    context: ReferenceObjectContext,
    transport: ReferenceHttpReceipt,
}

impl SealedReferenceRawObject {
    /// Returns the content-addressed provider-local object name.
    pub const fn storage_name(&self) -> &SourceIdentifier {
        &self.storage_name
    }

    /// Returns exact provider, schema, object, and clock evidence.
    pub const fn context(&self) -> &ReferenceObjectContext {
        &self.context
    }

    /// Returns exact admitted HTTP evidence.
    pub const fn transport(&self) -> &ReferenceHttpReceipt {
        &self.transport
    }
}

/// Receipt for one atomically published, conflict-free SQLite reference generation.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ReferenceGenerationReceipt {
    layout_version: u16,
    generation_id: SourceIdentifier,
    storage_name: SourceIdentifier,
    database_digest: EvidenceDigest,
    database_bytes: u64,
    limits: ReferenceSpoolLimits,
    raw_object_ids: Vec<SourceIdentifier>,
    catalog: PublicationCatalog,
}

/// Evidence that the explicit mutating activation probe completed for every store namespace.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ReferenceStoreActivationReceipt {
    layout_version: u16,
    verified_namespaces: u8,
}

impl ReferenceStoreActivationReceipt {
    /// Returns the provider-local store layout proven by the activation probe.
    pub const fn layout_version(&self) -> u16 {
        self.layout_version
    }

    /// Returns the exact number of capability namespaces proven writable and durable.
    pub const fn verified_namespaces(&self) -> u8 {
        self.verified_namespaces
    }
}

/// Exact generation evidence attached to every bounded read result.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ReferenceQueryEvidence {
    generation_id: SourceIdentifier,
    database_digest: EvidenceDigest,
    coordinate: ReferenceQueryCoordinate,
    coordinate_digest: EvidenceDigest,
    result_digest: EvidenceDigest,
    result_item_count: u32,
    native_row_count: u64,
}

/// Closed provider-native coordinate answered by one authenticated exact read or export page.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case", tag = "family")]
pub enum ReferenceQueryCoordinate {
    /// One exact case-sensitive Cboe Symbol ID.
    CboeSymbol {
        /// Exact provider-native key.
        symbol: SourceIdentifier,
    },
    /// One exact 21-character OCC/OSI contract identity.
    CboeOsi {
        /// Exact provider-native OSI key.
        osi: SourceIdentifier,
    },
    /// One exact OCC option-root and product-type tuple.
    OccProduct {
        /// Exact OCC option-root symbol.
        options_symbol: SourceIdentifier,
        /// Exact OCC two-character provider product code.
        product_type: SourceIdentifier,
    },
    /// One deterministic page of every Cboe contract in provider-key order.
    CboeContractPage {
        /// Exclusive prior Cboe Symbol ID, absent for the first page.
        after_symbol: Option<SourceIdentifier>,
        /// Caller-selected page row ceiling.
        maximum_rows: NonZeroU32,
        /// Exact sealed-generation contract count.
        total_rows: NonZeroU64,
        /// Number of earlier rows covered by the sealed ordinal index and this cursor.
        rows_emitted_before: u64,
        /// One-based deterministic page ordinal.
        page_ordinal: NonZeroU32,
        /// Exact export ordering/result-schema contract.
        export_contract_digest: EvidenceDigest,
    },
    /// One deterministic page of every OCC product in provider-key order.
    OccProductPage {
        /// Exclusive prior OCC option-root symbol, absent for the first page.
        after_options_symbol: Option<SourceIdentifier>,
        /// Exclusive prior OCC product code paired with `after_options_symbol`.
        after_product_type: Option<SourceIdentifier>,
        /// Caller-selected page row ceiling.
        maximum_rows: NonZeroU32,
        /// Exact sealed-generation product count.
        total_rows: NonZeroU64,
        /// Number of earlier rows covered by the sealed ordinal index and this cursor.
        rows_emitted_before: u64,
        /// One-based deterministic page ordinal.
        page_ordinal: NonZeroU32,
        /// Exact export ordering/result-schema contract.
        export_contract_digest: EvidenceDigest,
    },
}

/// Closed complete-generation export family carried by an opaque continuation cursor.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ReferenceCanonicalExportFamily {
    /// Complete Cboe contract mappings with their venue-specific row evidence.
    CboeContracts,
    /// Complete OCC product/root rows.
    OccProducts,
}

/// Generation-bound continuation for deterministic complete reference export.
///
/// Deserialization is intentionally supported for Desktop/MCP continuation round trips, but every
/// query revalidates the binding digest and exact active-generation coordinates before use.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ReferenceCanonicalExportCursor {
    layout_version: u16,
    generation_id: SourceIdentifier,
    database_digest: EvidenceDigest,
    family: ReferenceCanonicalExportFamily,
    after_primary: SourceIdentifier,
    after_secondary: Option<SourceIdentifier>,
    total_rows: NonZeroU64,
    rows_emitted: u64,
    next_page_ordinal: NonZeroU32,
    page_size: NonZeroU32,
    export_contract_digest: EvidenceDigest,
    binding_digest: EvidenceDigest,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ReferenceCanonicalExportCursorWire {
    layout_version: u16,
    generation_id: SourceIdentifier,
    database_digest: EvidenceDigest,
    family: ReferenceCanonicalExportFamily,
    after_primary: SourceIdentifier,
    after_secondary: Option<SourceIdentifier>,
    total_rows: NonZeroU64,
    rows_emitted: u64,
    next_page_ordinal: NonZeroU32,
    page_size: NonZeroU32,
    export_contract_digest: EvidenceDigest,
    binding_digest: EvidenceDigest,
}

/// One deterministic bounded complete-generation page with raw-object and query evidence.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AuthenticatedReferencePage<T> {
    rows: Vec<T>,
    object_evidence: Vec<ReferenceGenerationObjectEvidence>,
    next_cursor: Option<ReferenceCanonicalExportCursor>,
    total_rows: NonZeroU64,
    rows_emitted: u64,
    complete: bool,
    evidence: ReferenceQueryEvidence,
}

/// Complete durable lineage for one independently clocked raw object in a sealed generation.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ReferenceGenerationObjectEvidence {
    surface: ReferenceSurface,
    object_id: SourceIdentifier,
    configured_locator: SourceIdentifier,
    final_locator: SourceIdentifier,
    canonical_media_type: SourceIdentifier,
    native_schema: SourceIdentifier,
    native_schema_digest: EvidenceDigest,
    transport: ReferenceTransportEvidence,
    payload_digest: EvidenceDigest,
    payload_bytes: u64,
    source_filename: Option<SourceIdentifier>,
    source_publication_date: Option<CalendarDate>,
    http_last_modified: Option<HttpLastModifiedEvidence>,
    clocks: ObjectClockEvidence,
}

impl ReferenceGenerationObjectEvidence {
    /// Returns the exact provider surface represented by this independently clocked object.
    pub const fn surface(&self) -> &ReferenceSurface {
        &self.surface
    }

    /// Returns the content-derived object identity referenced by provider-native rows.
    pub const fn object_id(&self) -> &SourceIdentifier {
        &self.object_id
    }

    /// Returns the exact code-owned official request locator before redirects.
    pub const fn configured_locator(&self) -> &SourceIdentifier {
        &self.configured_locator
    }

    /// Returns the final admitted response locator.
    pub const fn final_locator(&self) -> &SourceIdentifier {
        &self.final_locator
    }

    /// Returns the canonical media type selected by strict response admission.
    pub const fn canonical_media_type(&self) -> &SourceIdentifier {
        &self.canonical_media_type
    }

    /// Returns the exact provider-native decoder/schema revision.
    pub const fn native_schema(&self) -> &SourceIdentifier {
        &self.native_schema
    }

    /// Returns a domain-separated digest of the exact provider-native schema revision.
    pub const fn native_schema_digest(&self) -> EvidenceDigest {
        self.native_schema_digest
    }

    /// Returns complete secret-free official-request and HTTP-response evidence.
    pub const fn transport(&self) -> &ReferenceTransportEvidence {
        &self.transport
    }

    /// Returns SHA-256 of the exact retained provider bytes.
    pub const fn payload_digest(&self) -> EvidenceDigest {
        self.payload_digest
    }

    /// Returns the exact retained provider-object byte count.
    pub const fn payload_bytes(&self) -> u64 {
        self.payload_bytes
    }

    /// Returns the provider filename when the admitted response supplied one.
    pub const fn source_filename(&self) -> Option<&SourceIdentifier> {
        self.source_filename.as_ref()
    }

    /// Returns the independent provider filename/report date without inventing an instant.
    pub const fn source_publication_date(&self) -> Option<CalendarDate> {
        self.source_publication_date
    }

    /// Returns the independent HTTP Last-Modified evidence when supplied.
    pub const fn http_last_modified(&self) -> Option<&HttpLastModifiedEvidence> {
        self.http_last_modified.as_ref()
    }

    /// Returns provider-native, local availability, receipt, and transport clocks.
    pub const fn clocks(&self) -> &ObjectClockEvidence {
        &self.clocks
    }
}

impl ReferenceQueryEvidence {
    /// Returns the identity of the exact immutable SQLite generation that answered the query.
    pub const fn generation_id(&self) -> &SourceIdentifier {
        &self.generation_id
    }

    /// Returns the digest of the exact immutable SQLite generation that answered the query.
    pub const fn database_digest(&self) -> EvidenceDigest {
        self.database_digest
    }

    /// Returns the complete provider-native query or page coordinate.
    pub const fn coordinate(&self) -> &ReferenceQueryCoordinate {
        &self.coordinate
    }

    /// Returns the domain-separated SHA-256 digest of the exact query coordinate.
    pub const fn coordinate_digest(&self) -> EvidenceDigest {
        self.coordinate_digest
    }

    /// Returns the domain-separated SHA-256 digest of the exact result, including an exact miss.
    pub const fn result_digest(&self) -> EvidenceDigest {
        self.result_digest
    }

    /// Returns the number of top-level values represented by the result.
    pub const fn result_item_count(&self) -> u32 {
        self.result_item_count
    }

    /// Returns the exact number of provider-native rows represented by those values.
    pub const fn native_row_count(&self) -> u64 {
        self.native_row_count
    }
}

/// One exact bounded query result with mandatory generation evidence, including misses.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AuthenticatedReferenceQuery<T> {
    value: Option<T>,
    object_evidence: Vec<ReferenceGenerationObjectEvidence>,
    evidence: ReferenceQueryEvidence,
}

impl<T> AuthenticatedReferenceQuery<T> {
    /// Returns the exact value when the provider generation contained the requested key.
    pub const fn value(&self) -> Option<&T> {
        self.value.as_ref()
    }

    /// Returns complete raw-object request, schema, and clock lineage for the result.
    pub fn object_evidence(&self) -> &[ReferenceGenerationObjectEvidence] {
        &self.object_evidence
    }

    /// Returns the generation evidence for this read or exact miss.
    pub const fn evidence(&self) -> &ReferenceQueryEvidence {
        &self.evidence
    }
}

impl ReferenceCanonicalExportCursor {
    fn try_new(
        receipt: &ReferenceGenerationReceipt,
        family: ReferenceCanonicalExportFamily,
        after_primary: SourceIdentifier,
        after_secondary: Option<SourceIdentifier>,
        total_rows: NonZeroU64,
        rows_emitted: u64,
        next_page_ordinal: NonZeroU32,
        page_size: NonZeroU32,
    ) -> Result<Self, ReferenceStoreError> {
        let export_contract_digest = export_contract_digest(family);
        let mut cursor = Self {
            layout_version: STORE_LAYOUT_VERSION,
            generation_id: receipt.generation_id.clone(),
            database_digest: receipt.database_digest,
            family,
            after_primary,
            after_secondary,
            total_rows,
            rows_emitted,
            next_page_ordinal,
            page_size,
            export_contract_digest,
            binding_digest: EvidenceDigest::new(DigestAlgorithm::Sha256, [0_u8; 32]),
        };
        cursor.binding_digest = cursor.expected_binding_digest()?;
        if cursor.binding_digest.bytes().iter().all(|byte| *byte == 0) {
            return Err(ReferenceStoreError::InvalidQuery);
        }
        cursor.validate_shape()?;
        Ok(cursor)
    }

    fn validate_for(
        &self,
        receipt: &ReferenceGenerationReceipt,
        expected_family: ReferenceCanonicalExportFamily,
    ) -> Result<(), ReferenceStoreError> {
        self.validate_shape()?;
        if self.layout_version != STORE_LAYOUT_VERSION
            || self.generation_id != receipt.generation_id
            || self.database_digest != receipt.database_digest
            || self.family != expected_family
            || self.export_contract_digest != export_contract_digest(expected_family)
            || self.database_digest.algorithm() != DigestAlgorithm::Sha256
            || self.binding_digest.algorithm() != DigestAlgorithm::Sha256
            || self.database_digest.bytes().iter().all(|byte| *byte == 0)
            || self.binding_digest.bytes().iter().all(|byte| *byte == 0)
            || self.expected_binding_digest()? != self.binding_digest
        {
            return Err(ReferenceStoreError::InvalidQuery);
        }
        match self.family {
            ReferenceCanonicalExportFamily::CboeContracts => {
                CboeSymbolId::try_from_provider(self.after_primary.as_str())?;
            }
            ReferenceCanonicalExportFamily::OccProducts => {
                ProviderInstrumentId::try_from(self.after_primary.as_str())
                    .map_err(|_| ReferenceStoreError::InvalidQuery)?;
                OccProductType::try_from_provider(
                    self.after_secondary
                        .as_ref()
                        .ok_or(ReferenceStoreError::InvalidQuery)?
                        .as_str(),
                )?;
            }
        }
        Ok(())
    }

    fn expected_binding_digest(&self) -> Result<EvidenceDigest, ReferenceStoreError> {
        let wire = serde_json::to_vec(&(
            self.layout_version,
            &self.generation_id,
            self.database_digest,
            self.family,
            &self.after_primary,
            &self.after_secondary,
            self.total_rows,
            self.rows_emitted,
            self.next_page_ordinal,
            self.page_size,
            self.export_contract_digest,
        ))
        .map_err(|_| ReferenceStoreError::InvalidQuery)?;
        Ok(domain_digest(
            b"market-squawk:options-reference-export-cursor:v1\0",
            &wire,
        ))
    }

    /// Returns the complete-generation family continued by this cursor.
    pub const fn family(&self) -> ReferenceCanonicalExportFamily {
        self.family
    }

    /// Encodes a bounded continuation token for Desktop/MCP transport.
    pub fn to_json(&self) -> Result<String, ReferenceStoreError> {
        serde_json::to_string(self).map_err(|_| ReferenceStoreError::InvalidQuery)
    }

    /// Decodes an untrusted continuation token; the query still validates its generation binding.
    pub fn from_json(value: &str) -> Result<Self, ReferenceStoreError> {
        if value.is_empty() || value.len() > 4 * 1024 {
            return Err(ReferenceStoreError::InvalidQuery);
        }
        let wire: ReferenceCanonicalExportCursorWire =
            serde_json::from_str(value).map_err(|_| ReferenceStoreError::InvalidQuery)?;
        let cursor = Self {
            layout_version: wire.layout_version,
            generation_id: wire.generation_id,
            database_digest: wire.database_digest,
            family: wire.family,
            after_primary: wire.after_primary,
            after_secondary: wire.after_secondary,
            total_rows: wire.total_rows,
            rows_emitted: wire.rows_emitted,
            next_page_ordinal: wire.next_page_ordinal,
            page_size: wire.page_size,
            export_contract_digest: wire.export_contract_digest,
            binding_digest: wire.binding_digest,
        };
        cursor.validate_shape()?;
        if cursor.expected_binding_digest()? != cursor.binding_digest {
            return Err(ReferenceStoreError::InvalidQuery);
        }
        Ok(cursor)
    }

    fn validate_shape(&self) -> Result<(), ReferenceStoreError> {
        let expected_next_page = self
            .rows_emitted
            .checked_div(u64::from(self.page_size.get()))
            .and_then(|completed_pages| completed_pages.checked_add(1))
            .and_then(|page| u32::try_from(page).ok())
            .and_then(NonZeroU32::new);
        if self.layout_version != STORE_LAYOUT_VERSION
            || self.database_digest.algorithm() != DigestAlgorithm::Sha256
            || self.binding_digest.algorithm() != DigestAlgorithm::Sha256
            || self.database_digest.bytes().iter().all(|byte| *byte == 0)
            || self.export_contract_digest != export_contract_digest(self.family)
            || self.page_size.get() > MAX_CANONICAL_EXPORT_PAGE_ROWS
            || self.rows_emitted % u64::from(self.page_size.get()) != 0
            || expected_next_page != Some(self.next_page_ordinal)
            || self.after_primary.as_str().is_empty()
            || self.rows_emitted == 0
            || self.rows_emitted >= self.total_rows.get()
            || matches!(self.family, ReferenceCanonicalExportFamily::CboeContracts)
                && self.after_secondary.is_some()
            || matches!(self.family, ReferenceCanonicalExportFamily::OccProducts)
                && self.after_secondary.is_none()
        {
            Err(ReferenceStoreError::InvalidQuery)
        } else {
            Ok(())
        }
    }
}

impl<T> AuthenticatedReferencePage<T> {
    /// Returns the complete deterministic provider-native rows in this page.
    pub fn rows(&self) -> &[T] {
        &self.rows
    }

    /// Returns the bounded complete raw-object clock/schema lineage joined by row `object_id`.
    pub fn object_evidence(&self) -> &[ReferenceGenerationObjectEvidence] {
        &self.object_evidence
    }

    /// Returns the next generation-bound continuation, or `None` at exact completion.
    pub const fn next_cursor(&self) -> Option<&ReferenceCanonicalExportCursor> {
        self.next_cursor.as_ref()
    }

    /// Returns the exact total row count in the sealed generation for this family.
    pub const fn total_rows(&self) -> NonZeroU64 {
        self.total_rows
    }

    /// Returns the sealed ordinal immediately after this page.
    ///
    /// This positional value does not prove a caller consumed earlier pages. Canonical composition
    /// must begin without a cursor and retain every exact server-returned continuation in order.
    pub const fn rows_emitted(&self) -> u64 {
        self.rows_emitted
    }

    /// Returns true only when this page ends at the sealed generation's final ordinal.
    ///
    /// It does not independently prove traversal from the first page.
    pub const fn complete(&self) -> bool {
        self.complete
    }

    /// Returns page coordinate, result digest/count, and generation evidence.
    pub const fn evidence(&self) -> &ReferenceQueryEvidence {
        &self.evidence
    }
}

/// Durable bounded doctor evidence for a complete source closure rejected on exact conflicts.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RejectedReferenceGenerationReceipt {
    layout_version: u16,
    storage_name: SourceIdentifier,
    database_digest: EvidenceDigest,
    database_bytes: u64,
    catalog: PublicationCatalog,
}

impl RejectedReferenceGenerationReceipt {
    /// Returns the immutable quarantine object name.
    pub const fn storage_name(&self) -> &SourceIdentifier {
        &self.storage_name
    }

    /// Returns the exact conflict-bearing catalog.
    pub const fn catalog(&self) -> &PublicationCatalog {
        &self.catalog
    }

    /// Returns the rejected evidence database digest.
    pub const fn database_digest(&self) -> EvidenceDigest {
        self.database_digest
    }

    /// Returns exact rejected evidence bytes.
    pub const fn database_bytes(&self) -> u64 {
        self.database_bytes
    }
}

impl ReferenceGenerationReceipt {
    /// Returns the immutable generation identity.
    pub const fn generation_id(&self) -> &SourceIdentifier {
        &self.generation_id
    }

    /// Returns the content-addressed provider-local SQLite object name.
    pub const fn storage_name(&self) -> &SourceIdentifier {
        &self.storage_name
    }

    /// Returns SHA-256 of the exact sealed SQLite file.
    pub const fn database_digest(&self) -> EvidenceDigest {
        self.database_digest
    }

    /// Returns exact sealed database bytes.
    pub const fn database_bytes(&self) -> u64 {
        self.database_bytes
    }

    /// Returns exact code-owned disk/cache/query limits for this generation.
    pub const fn limits(&self) -> ReferenceSpoolLimits {
        self.limits
    }

    /// Returns every exact raw object bound into this generation.
    pub fn raw_object_ids(&self) -> &[SourceIdentifier] {
        &self.raw_object_ids
    }

    /// Returns complete publication evidence represented by the database.
    pub const fn catalog(&self) -> &PublicationCatalog {
        &self.catalog
    }
}

/// Provider-local direct artifact root with separate staging, raw, generation, and quarantine
/// namespaces.
#[derive(Debug)]
pub struct ReferenceArtifactStore {
    root: Dir,
    staging: Dir,
    raw: Dir,
    generations: Dir,
    quarantine: Dir,
    manifests: Dir,
}

impl ReferenceArtifactStore {
    /// Opens an application-minted directory capability and creates only frozen subdirectories.
    ///
    /// # Errors
    ///
    /// Rejects a non-directory capability or unsafe pre-existing child entry. Ambient path
    /// opening remains the root composition layer's responsibility.
    pub fn open(root_capability: Dir) -> Result<Self, ReferenceStoreError> {
        validate_capability_directory(&root_capability)?;
        let staging = prepare_capability_child(&root_capability, STAGING_DIRECTORY)?;
        let raw = prepare_capability_child(&root_capability, RAW_DIRECTORY)?;
        let generations = prepare_capability_child(&root_capability, GENERATION_DIRECTORY)?;
        let quarantine = prepare_capability_child(&root_capability, QUARANTINE_DIRECTORY)?;
        let manifests = prepare_capability_child(&root_capability, MANIFEST_DIRECTORY)?;
        validate_single_filesystem_layout([&staging, &raw, &generations, &quarantine, &manifests])?;
        let manifest_lock = open_capability_lock_file(&manifests)?;
        drop(manifest_lock);
        Ok(Self {
            root: root_capability,
            staging,
            raw,
            generations,
            quarantine,
            manifests,
        })
    }

    /// Begins one bounded publication spool inside this store's capability and filesystem.
    ///
    /// Keeping staging store-owned makes the two-copy disk reservation and cross-process spool
    /// lock authoritative for the exact generation namespace that will receive publication.
    pub fn begin_publication(
        &self,
        request: PublicationRequest,
        control: ReferenceFetchControl,
        limits: ReferenceSpoolLimits,
    ) -> Result<ReferencePublicationSpool, ReferenceStoreError> {
        ReferencePublicationSpool::create(
            request,
            control,
            self.staging
                .try_clone()
                .map_err(|_| ReferenceStoreError::StoreIo)?,
            limits,
        )
        .map_err(ReferenceStoreError::from)
    }

    pub(crate) fn begin_stream_capture(
        &self,
        expected_bytes: Option<u64>,
        maximum_bytes: u64,
        control: &ReferenceFetchControl,
    ) -> Result<
        (
            tokio::sync::mpsc::Sender<RawStreamFrame>,
            tokio::task::JoinHandle<Result<StreamedRawCaptureReceipt, ReferenceStoreError>>,
            ReferenceCancellation,
        ),
        ReferenceStoreError,
    > {
        let required = maximum_bytes
            .checked_add(MIN_STORE_FREE_RESERVE_BYTES)
            .ok_or(ReferenceStoreError::CapacityUnavailable)?;
        ensure_capability_disk_capacity(&self.staging, required)?;
        ensure_capability_disk_capacity(&self.raw, required)?;
        let worker_cancellation = ReferenceCancellation::new();
        let sink = ReferenceRawStreamSink {
            directory: self
                .staging
                .try_clone()
                .map_err(|_| ReferenceStoreError::StoreIo)?,
            operation_lock: open_capability_lock_file(&self.manifests)?,
            control: control.clone(),
            worker_cancellation: worker_cancellation.clone(),
        };
        let (sender, receiver) = tokio::sync::mpsc::channel(RAW_STREAM_CHANNEL_CAPACITY);
        let worker = tokio::task::spawn_blocking(move || {
            sink.persist_receiver(receiver, expected_bytes, maximum_bytes)
        });
        Ok((sender, worker, worker_cancellation))
    }

    pub(crate) fn bind_streamed_raw_object(
        &self,
        capture: StreamedRawCaptureReceipt,
        context: ReferenceObjectContext,
        transport: ReferenceHttpReceipt,
    ) -> Result<SealedReferenceRawObject, ReferenceStoreError> {
        let retained_transport = context.transport_evidence();
        let transport_digest = transport
            .evidence_digest()
            .map_err(|_| ReferenceStoreError::RawEvidenceMismatch)?;
        if capture.digest != context.payload_digest()
            || capture.digest != transport.payload_digest()
            || capture.bytes != context.payload_bytes()
            || capture.bytes != transport.payload_bytes()
            || context.clocks().transport_elapsed_nanos() != transport.transport_elapsed_nanos()
            || retained_transport.request_digest() != transport.request_digest()
            || retained_transport.receipt_digest() != transport_digest
            || capture.storage_name.as_str()
                != format!("{}.raw", hex_digest(capture.digest.bytes()))
        {
            return Err(ReferenceStoreError::RawEvidenceMismatch);
        }
        publish_capability_no_replace(
            &capture.directory,
            &capture.staged_name,
            &self.raw,
            capture.storage_name.as_str(),
            capture.digest.bytes(),
            capture.bytes,
        )?;
        let receipt = SealedReferenceRawObject {
            layout_version: STORE_LAYOUT_VERSION,
            storage_name: capture.storage_name.clone(),
            context,
            transport,
        };
        self.verify_raw_object(&receipt)?;
        Ok(receipt)
    }

    /// Reads one exact raw object after full receipt/digest validation and within an explicit bound.
    ///
    /// This is the parser handoff: acquisition itself streams to disk, and only one sealed object
    /// is materialized at a time for the existing strict row decoder.
    pub(crate) fn read_raw_object(
        &self,
        receipt: &SealedReferenceRawObject,
        maximum_bytes: u64,
    ) -> Result<Vec<u8>, ReferenceStoreError> {
        self.verify_raw_object(receipt)?;
        if receipt.context.payload_bytes() > maximum_bytes {
            return Err(ReferenceStoreError::RawEvidenceMismatch);
        }
        let mut file = validate_capability_content_file(
            &self.raw,
            receipt.storage_name.as_str(),
            receipt.context.payload_digest().bytes(),
            receipt.context.payload_bytes(),
            maximum_bytes,
        )?
        .file;
        let capacity = usize::try_from(receipt.context.payload_bytes())
            .map_err(|_| ReferenceStoreError::RawEvidenceMismatch)?;
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(capacity)
            .map_err(|_| ReferenceStoreError::CapacityUnavailable)?;
        file.read_to_end(&mut bytes)
            .map_err(|_| ReferenceStoreError::StoreIo)?;
        if bytes.len() != capacity {
            return Err(ReferenceStoreError::ObjectCorrupt);
        }
        Ok(bytes)
    }

    /// Seals one already-admitted bounded object into the SHA-256 raw namespace.
    ///
    /// Publication uses a fully written and fsynced capability-scoped staging inode followed by
    /// an atomic same-filesystem no-replace hard link. An existing content name is accepted only
    /// after full digest/length verification.
    #[cfg(test)]
    pub(crate) fn seal_raw_object(
        &self,
        object: &RetrievedReferenceObject,
    ) -> Result<SealedReferenceRawObject, ReferenceStoreError> {
        let operation_lock_file = open_capability_lock_file(&self.manifests)?;
        let _operation_lock = ManifestLockGuard::try_shared(&operation_lock_file)?;
        let retained_transport = object.context().transport_evidence();
        let transport_digest = object
            .receipt()
            .evidence_digest()
            .map_err(|_| ReferenceStoreError::RawEvidenceMismatch)?;
        if object.context().payload_digest().algorithm() != DigestAlgorithm::Sha256
            || object.context().payload_digest() != object.receipt().payload_digest()
            || object.context().payload_bytes() != object.receipt().payload_bytes()
            || object.context().clocks().transport_elapsed_nanos()
                != object.receipt().transport_elapsed_nanos()
            || usize::try_from(object.context().payload_bytes()).ok() != Some(object.bytes().len())
            || retained_transport.request_digest() != object.receipt().request_digest()
            || retained_transport.receipt_digest() != transport_digest
        {
            return Err(ReferenceStoreError::RawEvidenceMismatch);
        }
        let digest = hash_bytes(object.bytes());
        if digest != object.context().payload_digest().bytes() {
            return Err(ReferenceStoreError::RawEvidenceMismatch);
        }
        let storage_name = format!("{}.raw", hex_digest(digest));
        let (staged_name, mut staged) =
            create_capability_staging(&self.staging, STAGED_RAW_PREFIX)?;
        staged
            .write_all(object.bytes())
            .map_err(|_| ReferenceStoreError::StoreIo)?;
        staged
            .sync_all()
            .map_err(|_| ReferenceStoreError::StoreIo)?;
        publish_capability_no_replace(
            &self.staging,
            &staged_name,
            &self.raw,
            &storage_name,
            digest,
            object.context().payload_bytes(),
        )?;
        Ok(SealedReferenceRawObject {
            layout_version: STORE_LAYOUT_VERSION,
            storage_name: SourceIdentifier::try_from(storage_name)
                .map_err(|_| ReferenceStoreError::InvalidReceipt)?,
            context: object.context().clone(),
            transport: object.receipt().clone(),
        })
    }

    /// Verifies a durable raw receipt and its exact content-addressed bytes.
    pub fn verify_raw_object(
        &self,
        receipt: &SealedReferenceRawObject,
    ) -> Result<(), ReferenceStoreError> {
        let retained_transport = receipt.context.transport_evidence();
        let transport_digest = receipt
            .transport
            .evidence_digest()
            .map_err(|_| ReferenceStoreError::InvalidReceipt)?;
        if receipt.layout_version != STORE_LAYOUT_VERSION
            || receipt.context.payload_digest() != receipt.transport.payload_digest()
            || receipt.context.payload_bytes() != receipt.transport.payload_bytes()
            || receipt.context.clocks().transport_elapsed_nanos()
                != receipt.transport.transport_elapsed_nanos()
            || retained_transport.request_digest() != receipt.transport.request_digest()
            || retained_transport.receipt_digest() != transport_digest
        {
            return Err(ReferenceStoreError::InvalidReceipt);
        }
        let expected_name = format!(
            "{}.raw",
            hex_digest(receipt.context.payload_digest().bytes())
        );
        if receipt.storage_name.as_str() != expected_name {
            return Err(ReferenceStoreError::InvalidReceipt);
        }
        let _verified = validate_capability_content_file(
            &self.raw,
            &expected_name,
            receipt.context.payload_digest().bytes(),
            receipt.context.payload_bytes(),
            receipt.context.payload_bytes(),
        )?;
        Ok(())
    }

    /// Publishes one complete staged SQLite generation after binding its exact raw-object closure.
    ///
    /// # Errors
    ///
    /// Rejects missing/corrupt raw objects, database/object-set divergence, an ineligible catalog,
    /// or any fsync/no-replace/reopen failure.
    pub fn publish_generation(
        &self,
        staged: StagedReferenceGeneration,
        raw_objects: &[SealedReferenceRawObject],
    ) -> Result<ReferenceGenerationReceipt, ReferenceStoreError> {
        staged.ensure_publication_open()?;
        let operation_lock_file = open_capability_lock_file(&self.manifests)?;
        let operation_lock = ManifestLockGuard::try_shared(&operation_lock_file)?;
        if raw_objects.is_empty()
            || raw_objects.len() > MAX_RAW_OBJECTS_PER_GENERATION
            || !staged.catalog().publication_eligible()
        {
            return Err(ReferenceStoreError::InvalidGeneration);
        }
        for raw in raw_objects {
            staged.ensure_publication_open()?;
            self.verify_raw_object(raw)?;
        }
        let expected_objects = exact_raw_evidence(raw_objects)?;
        let database_file = staged.try_clone_database_file()?;
        let actual_objects = read_database_raw_evidence(&database_file)?;
        if expected_objects != actual_objects {
            return Err(ReferenceStoreError::RawGenerationDivergence);
        }

        let digest = staged.database_digest();
        let database_bytes = staged.database_bytes();
        let storage_name = format!("{}.sqlite", hex_digest(digest));
        let required = database_bytes
            .checked_add(MIN_STORE_FREE_RESERVE_BYTES)
            .ok_or(ReferenceStoreError::CapacityUnavailable)?;
        ensure_capability_disk_capacity(&self.staging, required)?;
        ensure_capability_disk_capacity(&self.generations, required)?;
        let (staged_name, mut destination) =
            create_capability_staging(&self.staging, STAGED_GENERATION_PREFIX)?;
        copy_bounded_file(
            &database_file,
            &mut destination,
            database_bytes,
            staged.limits().max_database_bytes(),
            || {
                staged
                    .ensure_publication_open()
                    .map_err(ReferenceStoreError::from)
            },
        )?;
        staged.ensure_publication_open()?;
        destination
            .sync_all()
            .map_err(|_| ReferenceStoreError::StoreIo)?;
        staged.ensure_publication_open()?;
        publish_capability_no_replace(
            &self.staging,
            &staged_name,
            &self.generations,
            &storage_name,
            digest,
            database_bytes,
        )?;

        let raw_object_ids = actual_objects
            .into_iter()
            .map(|object| SourceIdentifier::try_from(object.object_id))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| ReferenceStoreError::InvalidGeneration)?;
        let generation_id = SourceIdentifier::try_from(format!(
            "options-reference-generation:sha256:{}",
            hex_digest(digest)
        ))
        .map_err(|_| ReferenceStoreError::InvalidGeneration)?;
        let receipt = ReferenceGenerationReceipt {
            layout_version: STORE_LAYOUT_VERSION,
            generation_id,
            storage_name: SourceIdentifier::try_from(storage_name)
                .map_err(|_| ReferenceStoreError::InvalidGeneration)?,
            database_digest: EvidenceDigest::new(DigestAlgorithm::Sha256, digest),
            database_bytes,
            limits: staged.limits(),
            raw_object_ids,
            catalog: staged.catalog().clone(),
        };
        let _verified = self.open_generation(&receipt)?;
        staged.ensure_publication_open()?;
        drop(operation_lock);
        self.activate_generation(&receipt, &staged)?;
        let recovered = self.repair_active()?;
        if recovered
            .generation()
            .map(|generation| generation.receipt().generation_id())
            != Some(receipt.generation_id())
        {
            return Err(ReferenceStoreError::InvalidActivationManifest);
        }
        Ok(receipt)
    }

    /// Persists a complete conflict-bearing generation in quarantine for bounded doctor evidence.
    ///
    /// This receipt has no API that can open or promote it as the current query generation.
    pub fn quarantine_rejected_generation(
        &self,
        rejected: &RejectedReferenceGeneration,
    ) -> Result<RejectedReferenceGenerationReceipt, ReferenceStoreError> {
        let operation_lock_file = open_capability_lock_file(&self.manifests)?;
        let _operation_lock = ManifestLockGuard::try_shared(&operation_lock_file)?;
        if rejected.catalog().conflicts().is_empty() {
            return Err(ReferenceStoreError::InvalidGeneration);
        }
        let digest = rejected.database_digest();
        let bytes = rejected.database_bytes();
        let storage_name = format!("{}.rejected.sqlite", hex_digest(digest));
        let required = bytes
            .checked_add(MIN_STORE_FREE_RESERVE_BYTES)
            .ok_or(ReferenceStoreError::CapacityUnavailable)?;
        ensure_capability_disk_capacity(&self.staging, required)?;
        ensure_capability_disk_capacity(&self.quarantine, required)?;
        let (staged_name, mut destination) =
            create_capability_staging(&self.staging, STAGED_GENERATION_PREFIX)?;
        let database_file = rejected.try_clone_database_file()?;
        copy_bounded_file(
            &database_file,
            &mut destination,
            bytes,
            rejected.limits().max_database_bytes(),
            || Ok(()),
        )?;
        destination
            .sync_all()
            .map_err(|_| ReferenceStoreError::StoreIo)?;
        publish_capability_no_replace(
            &self.staging,
            &staged_name,
            &self.quarantine,
            &storage_name,
            digest,
            bytes,
        )?;
        Ok(RejectedReferenceGenerationReceipt {
            layout_version: STORE_LAYOUT_VERSION,
            storage_name: SourceIdentifier::try_from(storage_name)
                .map_err(|_| ReferenceStoreError::InvalidReceipt)?,
            database_digest: EvidenceDigest::new(DigestAlgorithm::Sha256, digest),
            database_bytes: bytes,
            catalog: rejected.catalog().clone(),
        })
    }

    /// Opens an exact generation read-only after digest, size, schema, integrity, and receipt checks.
    pub fn open_generation(
        &self,
        receipt: &ReferenceGenerationReceipt,
    ) -> Result<ReferenceGeneration, ReferenceStoreError> {
        validate_generation_receipt(receipt)?;
        let database_file = validate_capability_content_file(
            &self.generations,
            receipt.storage_name.as_str(),
            receipt.database_digest.bytes(),
            receipt.database_bytes,
            receipt.limits.max_database_bytes(),
        )?;
        reject_capability_sqlite_sidecars(&self.generations, receipt.storage_name.as_str())?;
        let connection = open_sqlite_from_descriptor(&database_file.file)?;
        let raw_objects = configure_and_validate_read_only(&connection, receipt)?;
        let mut raw_files = Vec::new();
        raw_files
            .try_reserve_exact(raw_objects.len())
            .map_err(|_| ReferenceStoreError::CapacityUnavailable)?;
        for raw in raw_objects {
            let verified = validate_capability_content_file(
                &self.raw,
                &format!("{}.raw", hex_digest(raw.digest)),
                raw.digest,
                raw.bytes,
                raw.bytes,
            )?;
            raw_files.push(verified);
        }
        Ok(ReferenceGeneration {
            connection,
            database_file,
            raw_files,
            receipt: receipt.clone(),
        })
    }

    /// Explicitly repairs interrupted staging and recovers the newest independently verified
    /// generation from the durable provider-local activation manifest.
    ///
    /// The fixed current pointer names one content-addressed activation index. That bounded index
    /// retains oldest-to-newest immutable receipt-manifest names, allowing a corrupt/missing newest
    /// database to fall back to the last independently valid complete generation. This operation
    /// takes the exclusive activation lock and can quarantine staging; it is not a query-only
    /// doctor/status API and must run under the application blocking-work supervisor.
    pub fn repair_active(&self) -> Result<ReferenceRecoveryOutcome, ReferenceStoreError> {
        let manifest_lock_file = open_capability_lock_file(&self.manifests)?;
        let _manifest_lock = ManifestLockGuard::try_exclusive(&manifest_lock_file)?;
        let quarantined_staging = self.quarantine_interrupted_staging()?;
        let history = self.load_activation_history()?;
        let mut rejected = Vec::new();
        for manifest_name in history.iter().rev() {
            let fallback_id = manifest_recovery_identifier(manifest_name)?;
            match self.load_generation_receipt(manifest_name) {
                Ok(receipt) => match self.open_generation(&receipt) {
                    Ok(generation) => {
                        return Ok(ReferenceRecoveryOutcome {
                            generation: Some(generation),
                            rejected,
                            quarantined_staging,
                        });
                    }
                    Err(ReferenceStoreError::ObjectMissing) => {
                        rejected.push(ReferenceRecoveryRejection::new(
                            receipt.generation_id.clone(),
                            ReferenceRecoveryFailure::Missing,
                        ))
                    }
                    Err(_) => rejected.push(ReferenceRecoveryRejection::new(
                        receipt.generation_id.clone(),
                        ReferenceRecoveryFailure::CorruptOrIncompatible,
                    )),
                },
                Err(ReferenceStoreError::ObjectMissing) => rejected.push(
                    ReferenceRecoveryRejection::new(fallback_id, ReferenceRecoveryFailure::Missing),
                ),
                Err(_) => rejected.push(ReferenceRecoveryRejection::new(
                    fallback_id,
                    ReferenceRecoveryFailure::CorruptOrIncompatible,
                )),
            }
        }
        Ok(ReferenceRecoveryOutcome {
            generation: None,
            rejected,
            quarantined_staging,
        })
    }

    /// Explicitly proves each capability-scoped namespace can create, write, fsync, read, unlink,
    /// and fsync a private probe without following ambient paths.
    ///
    /// # Errors
    ///
    /// Returns a closed I/O or unsafe-entry failure. A successful probe does not replace
    /// generation integrity/query validation; it is the mutating local-storage portion of
    /// activation and must never be composed as query-only doctor/status.
    pub fn activation_storage_probe(
        &self,
    ) -> Result<ReferenceStoreActivationReceipt, ReferenceStoreError> {
        let operation_lock_file = open_capability_lock_file(&self.manifests)?;
        let _operation_lock = ManifestLockGuard::try_shared(&operation_lock_file)?;
        for directory in [
            &self.root,
            &self.staging,
            &self.raw,
            &self.generations,
            &self.quarantine,
            &self.manifests,
        ] {
            probe_capability_directory(directory)?;
        }
        Ok(ReferenceStoreActivationReceipt {
            layout_version: STORE_LAYOUT_VERSION,
            verified_namespaces: 6,
        })
    }

    fn activate_generation(
        &self,
        receipt: &ReferenceGenerationReceipt,
        staged: &StagedReferenceGeneration,
    ) -> Result<(), ReferenceStoreError> {
        let manifest_lock_file = open_capability_lock_file(&self.manifests)?;
        let _manifest_lock = ManifestLockGuard::try_exclusive(&manifest_lock_file)?;
        validate_generation_receipt(receipt)?;
        let manifest_bytes = encode_receipt_manifest(receipt)?;
        let manifest_digest = hash_bytes(&manifest_bytes);
        let manifest_name = format!(
            "{}-{}.receipt",
            hex_digest(receipt.database_digest.bytes()),
            hex_digest(manifest_digest)
        );
        persist_immutable_capability_bytes(
            &self.manifests,
            &manifest_name,
            &manifest_bytes,
            MAX_MANIFEST_BYTES,
        )?;

        let mut history = self.load_activation_history()?;
        history.retain(|name| name != &manifest_name);
        history.push(manifest_name);
        if history.len() > MAX_RECOVERY_RECEIPTS {
            let remove = history.len() - MAX_RECOVERY_RECEIPTS;
            history.drain(..remove);
        }
        let activation_bytes = encode_activation_index(&history)?;
        let activation_digest = hash_bytes(&activation_bytes);
        let activation_name = format!("{}.activation", hex_digest(activation_digest));
        persist_immutable_capability_bytes(
            &self.manifests,
            &activation_name,
            &activation_bytes,
            MAX_ACTIVATION_BYTES,
        )?;
        let pointer_bytes = encode_current_pointer(
            &activation_name,
            activation_digest,
            u64::try_from(activation_bytes.len())
                .map_err(|_| ReferenceStoreError::InvalidActivationManifest)?,
        );
        atomic_replace_current_pointer(&self.manifests, &pointer_bytes, || {
            staged
                .ensure_publication_open()
                .map_err(ReferenceStoreError::from)
        })?;
        let loaded = self.load_activation_history()?;
        if loaded != history {
            return Err(ReferenceStoreError::InvalidActivationManifest);
        }
        prune_manifest_namespace(&self.manifests, &history, &activation_name)?;
        Ok(())
    }

    fn load_activation_history(&self) -> Result<Vec<String>, ReferenceStoreError> {
        let pointer = match read_capability_file_bounded(
            &self.manifests,
            CURRENT_ACTIVATION_POINTER,
            MAX_MANIFEST_BYTES,
        ) {
            Ok(bytes) => bytes,
            Err(ReferenceStoreError::ObjectMissing) => return Ok(Vec::new()),
            Err(error) => return Err(error),
        };
        let current = parse_current_pointer(&pointer)?;
        let activation = read_capability_digest_bounded(
            &self.manifests,
            &current.activation_name,
            current.activation_digest,
            current.activation_bytes,
            MAX_ACTIVATION_BYTES,
        )?;
        let history = parse_activation_index(&activation)?;
        if encode_activation_index(&history)? != activation {
            return Err(ReferenceStoreError::InvalidActivationManifest);
        }
        Ok(history)
    }

    fn load_generation_receipt(
        &self,
        manifest_name: &str,
    ) -> Result<ReferenceGenerationReceipt, ReferenceStoreError> {
        let (database_digest, manifest_digest) = parse_receipt_manifest_name(manifest_name)?;
        let manifest_bytes = read_capability_digest_with_unknown_size(
            &self.manifests,
            manifest_name,
            manifest_digest,
            MAX_MANIFEST_BYTES,
        )?;
        let record = parse_receipt_manifest(&manifest_bytes)?;
        if record.database_digest != database_digest
            || encode_receipt_manifest_record(&record)? != manifest_bytes
        {
            return Err(ReferenceStoreError::InvalidActivationManifest);
        }
        self.reconstruct_generation_receipt(record)
    }

    fn reconstruct_generation_receipt(
        &self,
        manifest: ReceiptManifestRecord,
    ) -> Result<ReferenceGenerationReceipt, ReferenceStoreError> {
        let storage_name = format!("{}.sqlite", hex_digest(manifest.database_digest));
        let database_file = validate_capability_content_file(
            &self.generations,
            &storage_name,
            manifest.database_digest,
            manifest.database_bytes,
            manifest.limits.max_database_bytes(),
        )?;
        reject_capability_sqlite_sidecars(&self.generations, &storage_name)?;
        let connection = open_sqlite_from_descriptor(&database_file.file)?;
        let (request, counts, raw_object_ids) =
            reconstruct_catalog_evidence(&connection, &manifest)?;
        let catalog = PublicationCatalog::from_spool(
            request,
            PublicationCompleteness::Complete,
            counts,
            Vec::new(),
        );
        let digest_hex = hex_digest(manifest.database_digest);
        Ok(ReferenceGenerationReceipt {
            layout_version: STORE_LAYOUT_VERSION,
            generation_id: SourceIdentifier::try_from(format!(
                "options-reference-generation:sha256:{digest_hex}"
            ))
            .map_err(|_| ReferenceStoreError::InvalidActivationManifest)?,
            storage_name: SourceIdentifier::try_from(storage_name)
                .map_err(|_| ReferenceStoreError::InvalidActivationManifest)?,
            database_digest: EvidenceDigest::new(DigestAlgorithm::Sha256, manifest.database_digest),
            database_bytes: manifest.database_bytes,
            limits: manifest.limits,
            raw_object_ids,
            catalog,
        })
    }

    fn quarantine_interrupted_staging(&self) -> Result<u32, ReferenceStoreError> {
        let mut visited = 0_usize;
        let mut examined = 0_usize;
        let mut quarantined = 0_u32;
        for (namespace, directory) in [
            ("root", &self.root),
            ("staging", &self.staging),
            ("raw", &self.raw),
            ("generations", &self.generations),
            ("quarantine", &self.quarantine),
            ("manifests", &self.manifests),
        ] {
            for entry in directory
                .entries()
                .map_err(|_| ReferenceStoreError::StoreIo)?
            {
                visited = visited
                    .checked_add(1)
                    .ok_or(ReferenceStoreError::RecoveryLimitExceeded)?;
                if visited > MAX_RECOVERY_NAMESPACE_ENTRIES {
                    return Err(ReferenceStoreError::RecoveryLimitExceeded);
                }
                let entry = entry.map_err(|_| ReferenceStoreError::StoreIo)?;
                let name = entry
                    .file_name()
                    .into_string()
                    .map_err(|_| ReferenceStoreError::UnsafeStoreEntry)?;
                if !name.starts_with(STAGED_RAW_PREFIX)
                    && !name.starts_with(STAGED_GENERATION_PREFIX)
                    && !name.starts_with(STAGED_SPOOL_PREFIX)
                    && !name.starts_with(STAGED_MANIFEST_PREFIX)
                    && !name.starts_with(DOCTOR_PROBE_PREFIX)
                {
                    continue;
                }
                examined = examined
                    .checked_add(1)
                    .ok_or(ReferenceStoreError::RecoveryLimitExceeded)?;
                if examined > MAX_RECOVERY_STAGING_ENTRIES {
                    return Err(ReferenceStoreError::RecoveryLimitExceeded);
                }
                if !entry
                    .file_type()
                    .map_err(|_| ReferenceStoreError::StoreIo)?
                    .is_file()
                {
                    return Err(ReferenceStoreError::UnsafeStoreEntry);
                }
                let quarantine_name = interrupted_quarantine_name(namespace, &name)?;
                match directory.hard_link(&name, &self.quarantine, &quarantine_name) {
                    Ok(()) => sync_capability_directory(&self.quarantine)?,
                    Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                        if !capability_files_share_identity(
                            directory,
                            &name,
                            &self.quarantine,
                            &quarantine_name,
                        )? {
                            return Err(ReferenceStoreError::UnsafeStoreEntry);
                        }
                    }
                    Err(_) => return Err(ReferenceStoreError::StoreIo),
                }
                directory
                    .remove_file(&name)
                    .map_err(|_| ReferenceStoreError::StoreIo)?;
                sync_capability_directory(directory)?;
                quarantined = quarantined
                    .checked_add(1)
                    .ok_or(ReferenceStoreError::RecoveryLimitExceeded)?;
            }
        }
        Ok(quarantined)
    }
}

fn interrupted_quarantine_name(
    namespace: &str,
    source_name: &str,
) -> Result<String, ReferenceStoreError> {
    let mut digest = Sha256::new();
    digest.update(b"market-squawk:options-reference-interrupted-stage:v1\0");
    digest.update(
        u64::try_from(namespace.len())
            .map_err(|_| ReferenceStoreError::CapacityUnavailable)?
            .to_be_bytes(),
    );
    digest.update(namespace.as_bytes());
    digest.update(
        u64::try_from(source_name.len())
            .map_err(|_| ReferenceStoreError::CapacityUnavailable)?
            .to_be_bytes(),
    );
    digest.update(source_name.as_bytes());
    Ok(format!(
        "interrupted-{namespace}-{}.staged",
        hex_digest(digest.finalize().into())
    ))
}

#[cfg(unix)]
fn capability_files_share_identity(
    source_directory: &Dir,
    source_name: &str,
    target_directory: &Dir,
    target_name: &str,
) -> Result<bool, ReferenceStoreError> {
    let open = |directory: &Dir, name: &str| {
        let mut options = OpenOptions::new();
        options.read(true);
        options.follow(FollowSymlinks::No);
        directory
            .open_with(name, &options)
            .map_err(|_| ReferenceStoreError::StoreIo)
    };
    let source = open(source_directory, source_name)?;
    let target = open(target_directory, target_name)?;
    let source_metadata = source
        .metadata()
        .map_err(|_| ReferenceStoreError::StoreIo)?;
    let target_metadata = target
        .metadata()
        .map_err(|_| ReferenceStoreError::StoreIo)?;
    Ok(source_metadata.is_file()
        && target_metadata.is_file()
        && cap_fs_ext::MetadataExt::dev(&source_metadata)
            == cap_fs_ext::MetadataExt::dev(&target_metadata)
        && cap_fs_ext::MetadataExt::ino(&source_metadata)
            == cap_fs_ext::MetadataExt::ino(&target_metadata))
}

#[cfg(not(unix))]
fn capability_files_share_identity(
    _source_directory: &Dir,
    _source_name: &str,
    _target_directory: &Dir,
    _target_name: &str,
) -> Result<bool, ReferenceStoreError> {
    Err(ReferenceStoreError::CapabilityDatabaseUnavailable)
}

/// Open exact generation with read-only, bounded provider-reference queries.
pub struct ReferenceGeneration {
    connection: Connection,
    database_file: ImmutableFileEvidence,
    raw_files: Vec<ImmutableFileEvidence>,
    receipt: ReferenceGenerationReceipt,
}

struct ImmutableFileEvidence {
    file: File,
    fingerprint: CapabilityFileFingerprint,
}

impl ImmutableFileEvidence {
    fn verify_unchanged(&self) -> Result<(), ReferenceStoreError> {
        if CapabilityFileFingerprint::capture(&self.file)? == self.fingerprint {
            Ok(())
        } else {
            Err(ReferenceStoreError::ObjectCorrupt)
        }
    }
}

#[cfg(unix)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct CapabilityFileFingerprint {
    device: u64,
    inode: u64,
    bytes: u64,
    mode: u32,
    links: u64,
    modified_seconds: i64,
    modified_nanos: i64,
    changed_seconds: i64,
    changed_nanos: i64,
}

#[cfg(unix)]
impl CapabilityFileFingerprint {
    fn capture(file: &File) -> Result<Self, ReferenceStoreError> {
        let metadata = file.metadata().map_err(|_| ReferenceStoreError::StoreIo)?;
        if !metadata.is_file() {
            return Err(ReferenceStoreError::ObjectCorrupt);
        }
        Ok(Self {
            device: metadata.dev(),
            inode: metadata.ino(),
            bytes: metadata.size(),
            mode: metadata.mode(),
            links: metadata.nlink(),
            modified_seconds: metadata.mtime(),
            modified_nanos: metadata.mtime_nsec(),
            changed_seconds: metadata.ctime(),
            changed_nanos: metadata.ctime_nsec(),
        })
    }
}

#[cfg(not(unix))]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct CapabilityFileFingerprint {
    bytes: u64,
    read_only: bool,
    modified: Option<std::time::SystemTime>,
    created: Option<std::time::SystemTime>,
}

#[cfg(not(unix))]
impl CapabilityFileFingerprint {
    fn capture(file: &File) -> Result<Self, ReferenceStoreError> {
        let metadata = file.metadata().map_err(|_| ReferenceStoreError::StoreIo)?;
        if !metadata.is_file() {
            return Err(ReferenceStoreError::ObjectCorrupt);
        }
        Ok(Self {
            bytes: metadata.len(),
            read_only: metadata.permissions().readonly(),
            modified: metadata.modified().ok(),
            created: metadata.created().ok(),
        })
    }
}

impl std::fmt::Debug for ReferenceGeneration {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ReferenceGeneration")
            .field("generation_id", &self.receipt.generation_id)
            .finish_non_exhaustive()
    }
}

impl ReferenceGeneration {
    /// Returns exact immutable generation evidence.
    pub const fn receipt(&self) -> &ReferenceGenerationReceipt {
        &self.receipt
    }

    /// Returns the complete, independently clocked raw-object closure for canonical mapping.
    ///
    /// The vector is bounded by the generation's closed surface count. Every object is revalidated
    /// before and after reconstruction; consumers join rows to clocks only by exact `object_id`.
    pub fn object_evidence(
        &self,
    ) -> Result<Vec<ReferenceGenerationObjectEvidence>, ReferenceStoreError> {
        self.verify_active_evidence()?;
        let mut statement = self
            .connection
            .prepare(
                "SELECT surface,object_id,provider,configured_locator,final_locator,media_type,native_schema,request_digest,receipt_digest,http_status,redirect_chain_json,observed_content_type,observed_content_disposition,declared_content_length,cache_etag,cache_last_modified,body_complete,payload_digest,payload_bytes,received_at,transport_elapsed_nanos,clocks_json,source_publication_date,source_filename,http_last_modified,transport_json FROM objects ORDER BY surface COLLATE BINARY",
            )
            .map_err(ReferenceStoreError::sqlite)?;
        let mut rows = statement.query([]).map_err(ReferenceStoreError::sqlite)?;
        let mut evidence = Vec::new();
        evidence
            .try_reserve_exact(self.receipt.raw_object_ids.len())
            .map_err(|_| ReferenceStoreError::CapacityUnavailable)?;
        while let Some(row) = rows.next().map_err(ReferenceStoreError::sqlite)? {
            if evidence.len() >= self.receipt.raw_object_ids.len() {
                return Err(ReferenceStoreError::RawGenerationDivergence);
            }
            let surface_key_value = row
                .get::<_, String>(0)
                .map_err(ReferenceStoreError::sqlite)?;
            let surface = self
                .receipt
                .catalog
                .request()
                .surfaces()
                .iter()
                .find(|surface| {
                    surface_key(surface)
                        .ok()
                        .as_deref()
                        .is_some_and(|key| key == surface_key_value)
                })
                .cloned()
                .ok_or(ReferenceStoreError::InvalidGeneration)?;
            let object_id = SourceIdentifier::try_from(
                row.get::<_, String>(1)
                    .map_err(ReferenceStoreError::sqlite)?,
            )
            .map_err(|_| ReferenceStoreError::InvalidGeneration)?;
            let provider = row
                .get::<_, String>(2)
                .map_err(ReferenceStoreError::sqlite)?;
            if !matches!(
                (surface.provider(), provider.as_str()),
                (crate::ReferenceProvider::Cboe, "cboe") | (crate::ReferenceProvider::Occ, "occ")
            ) {
                return Err(ReferenceStoreError::InvalidGeneration);
            }
            let configured_locator = SourceIdentifier::try_from(
                row.get::<_, String>(3)
                    .map_err(ReferenceStoreError::sqlite)?,
            )
            .map_err(|_| ReferenceStoreError::InvalidGeneration)?;
            let final_locator = SourceIdentifier::try_from(
                row.get::<_, String>(4)
                    .map_err(ReferenceStoreError::sqlite)?,
            )
            .map_err(|_| ReferenceStoreError::InvalidGeneration)?;
            let canonical_media_type = SourceIdentifier::try_from(
                row.get::<_, String>(5)
                    .map_err(ReferenceStoreError::sqlite)?,
            )
            .map_err(|_| ReferenceStoreError::InvalidGeneration)?;
            let native_schema = SourceIdentifier::try_from(
                row.get::<_, String>(6)
                    .map_err(ReferenceStoreError::sqlite)?,
            )
            .map_err(|_| ReferenceStoreError::InvalidGeneration)?;
            let request_digest: [u8; 32] = row
                .get::<_, Vec<u8>>(7)
                .map_err(ReferenceStoreError::sqlite)?
                .try_into()
                .map_err(|_| ReferenceStoreError::InvalidGeneration)?;
            let receipt_digest: [u8; 32] = row
                .get::<_, Vec<u8>>(8)
                .map_err(ReferenceStoreError::sqlite)?
                .try_into()
                .map_err(|_| ReferenceStoreError::InvalidGeneration)?;
            let status = u16::try_from(row.get::<_, i64>(9).map_err(ReferenceStoreError::sqlite)?)
                .map_err(|_| ReferenceStoreError::InvalidGeneration)?;
            let redirect_chain_json = row
                .get::<_, String>(10)
                .map_err(ReferenceStoreError::sqlite)?;
            if redirect_chain_json.len() > 8 * 1024 {
                return Err(ReferenceStoreError::InvalidGeneration);
            }
            let redirect_chain: Vec<SourceIdentifier> = serde_json::from_str(&redirect_chain_json)
                .map_err(|_| ReferenceStoreError::InvalidGeneration)?;
            if serde_json::to_string(&redirect_chain)
                .map_err(|_| ReferenceStoreError::InvalidGeneration)?
                != redirect_chain_json
            {
                return Err(ReferenceStoreError::InvalidGeneration);
            }
            let observed_content_type = row
                .get::<_, String>(11)
                .map_err(ReferenceStoreError::sqlite)?;
            let observed_content_disposition = row
                .get::<_, Option<String>>(12)
                .map_err(ReferenceStoreError::sqlite)?;
            let declared_content_length = row
                .get::<_, Option<i64>>(13)
                .map_err(ReferenceStoreError::sqlite)?
                .map(u64::try_from)
                .transpose()
                .map_err(|_| ReferenceStoreError::InvalidGeneration)?;
            let cache_etag = row
                .get::<_, Option<String>>(14)
                .map_err(ReferenceStoreError::sqlite)?;
            let cache_last_modified = row
                .get::<_, Option<String>>(15)
                .map_err(ReferenceStoreError::sqlite)?;
            let body_complete = match row.get::<_, i64>(16).map_err(ReferenceStoreError::sqlite)? {
                1 => true,
                _ => return Err(ReferenceStoreError::InvalidGeneration),
            };
            let payload_digest: [u8; 32] = row
                .get::<_, Vec<u8>>(17)
                .map_err(ReferenceStoreError::sqlite)?
                .try_into()
                .map_err(|_| ReferenceStoreError::InvalidGeneration)?;
            let payload_bytes =
                u64::try_from(row.get::<_, i64>(18).map_err(ReferenceStoreError::sqlite)?)
                    .ok()
                    .filter(|bytes| *bytes > 0)
                    .ok_or(ReferenceStoreError::InvalidGeneration)?;
            let received_at = Timestamp::from_unix_nanos(
                row.get::<_, i64>(19).map_err(ReferenceStoreError::sqlite)?,
            );
            let transport_elapsed_nanos =
                u64::try_from(row.get::<_, i64>(20).map_err(ReferenceStoreError::sqlite)?)
                    .ok()
                    .filter(|elapsed| *elapsed > 0)
                    .ok_or(ReferenceStoreError::InvalidGeneration)?;
            let clocks_json = row
                .get::<_, String>(21)
                .map_err(ReferenceStoreError::sqlite)?;
            let source_publication_date = row
                .get::<_, Option<String>>(22)
                .map_err(ReferenceStoreError::sqlite)?
                .as_deref()
                .map(parse_iso_date)
                .transpose()?;
            let source_filename = row
                .get::<_, Option<String>>(23)
                .map_err(ReferenceStoreError::sqlite)?
                .map(SourceIdentifier::try_from)
                .transpose()
                .map_err(|_| ReferenceStoreError::InvalidGeneration)?;
            let http_last_modified = row
                .get::<_, Option<String>>(24)
                .map_err(ReferenceStoreError::sqlite)?
                .as_deref()
                .map(HttpLastModifiedEvidence::try_from_header)
                .transpose()
                .map_err(|_| ReferenceStoreError::InvalidGeneration)?;
            let clocks = ObjectClockEvidence::try_new(
                source_publication_date.map(ResearchTemporalCoordinate::calendar_date),
                None,
                AvailabilityEvidence::local_first_observed(received_at),
                received_at,
                transport_elapsed_nanos,
            )
            .map_err(|_| ReferenceStoreError::InvalidGeneration)?;
            let transport_json = row
                .get::<_, String>(25)
                .map_err(ReferenceStoreError::sqlite)?;
            if transport_json.is_empty() || transport_json.len() > 64 * 1024 {
                return Err(ReferenceStoreError::InvalidGeneration);
            }
            let parsed_transport: ReferenceTransportEvidence =
                serde_json::from_str(&transport_json)
                    .map_err(|_| ReferenceStoreError::InvalidGeneration)?;
            let transport = parsed_transport
                .try_revalidated()
                .map_err(|_| ReferenceStoreError::InvalidGeneration)?;
            if serde_json::to_string(&clocks).map_err(|_| ReferenceStoreError::InvalidGeneration)?
                != clocks_json
                || serde_json::to_string(&transport)
                    .map_err(|_| ReferenceStoreError::InvalidGeneration)?
                    != transport_json
                || payload_digest.iter().all(|byte| *byte == 0)
                || transport.request_digest()
                    != EvidenceDigest::new(DigestAlgorithm::Sha256, request_digest)
                || transport.receipt_digest()
                    != EvidenceDigest::new(DigestAlgorithm::Sha256, receipt_digest)
                || transport.status() != status
                || transport.redirect_chain() != redirect_chain
                || transport.observed_content_type() != Some(observed_content_type.as_str())
                || transport.observed_content_disposition()
                    != observed_content_disposition.as_deref()
                || transport.declared_content_length() != declared_content_length
                || transport.etag() != cache_etag.as_deref()
                || transport.cache_last_modified() != cache_last_modified.as_deref()
                || transport.body_complete() != body_complete
                || transport.response_body_digest()
                    != EvidenceDigest::new(DigestAlgorithm::Sha256, payload_digest)
                || transport.response_body_bytes() != payload_bytes
                || transport.body_completed_at() != received_at
                || transport.transport_elapsed_nanos() != transport_elapsed_nanos
                || transport.request().configured_locator() != &configured_locator
                || transport.final_locator() != &final_locator
                || transport.canonical_media_type() != &canonical_media_type
                || transport.native_schema().name() != &native_schema
                || cache_last_modified.as_deref()
                    != http_last_modified
                        .as_ref()
                        .map(HttpLastModifiedEvidence::as_str)
                || self
                    .receipt
                    .raw_object_ids
                    .binary_search(&object_id)
                    .is_err()
            {
                return Err(ReferenceStoreError::RawGenerationDivergence);
            }
            evidence.push(ReferenceGenerationObjectEvidence {
                surface,
                object_id,
                configured_locator,
                final_locator,
                canonical_media_type,
                native_schema_digest: domain_digest(
                    b"market-squawk:options-reference-native-schema:v1\0",
                    native_schema.as_str().as_bytes(),
                ),
                native_schema,
                transport,
                payload_digest: EvidenceDigest::new(DigestAlgorithm::Sha256, payload_digest),
                payload_bytes,
                source_filename,
                source_publication_date,
                http_last_modified,
                clocks,
            });
        }
        evidence.sort_by(|left, right| left.surface.cmp(&right.surface));
        if evidence.len() != self.receipt.raw_object_ids.len()
            || evidence
                .windows(2)
                .any(|pair| pair[0].surface >= pair[1].surface)
        {
            return Err(ReferenceStoreError::RawGenerationDivergence);
        }
        self.verify_active_evidence()?;
        Ok(evidence)
    }

    pub(crate) fn durable_query_keys(
        &self,
    ) -> Result<(CboeSymbolId, ProviderInstrumentId, OccProductType), ReferenceStoreError> {
        self.verify_active_evidence()?;
        let cboe_symbol = self
            .connection
            .query_row(
                "SELECT cboe_symbol FROM cboe_contracts ORDER BY cboe_symbol LIMIT 1",
                [],
                |row| row.get::<_, String>(0),
            )
            .map_err(ReferenceStoreError::sqlite)?;
        let (occ_symbol, product_type) = self
            .connection
            .query_row(
                "SELECT options_symbol,product_type FROM occ_products ORDER BY options_symbol,product_type LIMIT 1",
                [],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
            )
            .map_err(ReferenceStoreError::sqlite)?;
        let keys = (
            CboeSymbolId::try_from_provider(&cboe_symbol)?,
            ProviderInstrumentId::try_from(occ_symbol.as_str())
                .map_err(|_| ReferenceStoreError::InvalidGeneration)?,
            OccProductType::try_from_provider(&product_type)?,
        );
        self.verify_active_evidence()?;
        Ok(keys)
    }

    /// Queries one exact case-sensitive Cboe Symbol ID. No ticker/name inference is performed.
    pub fn cboe_by_symbol(
        &self,
        symbol: &CboeSymbolId,
    ) -> Result<AuthenticatedReferenceQuery<CboeContractReferenceView>, ReferenceStoreError> {
        let coordinate = ReferenceQueryCoordinate::CboeSymbol {
            symbol: SourceIdentifier::try_from(symbol.as_str())
                .map_err(|_| ReferenceStoreError::InvalidQuery)?,
        };
        self.verify_active_evidence()?;
        let value = self.query_cboe("cboe_symbol", symbol.as_str())?;
        let native_row_count = match value.as_ref() {
            Some(value) => u64::try_from(value.venues().len())
                .map_err(|_| ReferenceStoreError::QueryLimitExceeded)?,
            None => 0,
        };
        self.finish_query(value, native_row_count, coordinate)
    }

    /// Queries one exact 21-character OSI identity. No root/underlying equality is assumed.
    pub fn cboe_by_osi(
        &self,
        osi: &str,
    ) -> Result<AuthenticatedReferenceQuery<CboeContractReferenceView>, ReferenceStoreError> {
        let _validated = OptionContractIdentity::try_from_osi(osi)
            .map_err(|_| ReferenceStoreError::InvalidQuery)?;
        let coordinate = ReferenceQueryCoordinate::CboeOsi {
            osi: SourceIdentifier::try_from(osi).map_err(|_| ReferenceStoreError::InvalidQuery)?,
        };
        self.verify_active_evidence()?;
        let value = self.query_cboe("osi", osi)?;
        let native_row_count = match value.as_ref() {
            Some(value) => u64::try_from(value.venues().len())
                .map_err(|_| ReferenceStoreError::QueryLimitExceeded)?,
            None => 0,
        };
        self.finish_query(value, native_row_count, coordinate)
    }

    fn query_cboe(
        &self,
        column: &'static str,
        value: &str,
    ) -> Result<Option<CboeContractReferenceView>, ReferenceStoreError> {
        let sql = match column {
            "cboe_symbol" => {
                "SELECT cboe_symbol,osi,underlying FROM cboe_contracts WHERE cboe_symbol=?1"
            }
            "osi" => "SELECT cboe_symbol,osi,underlying FROM cboe_contracts WHERE osi=?1",
            _ => return Err(ReferenceStoreError::InvalidQuery),
        };
        let mapping: Option<(String, String, String)> = self
            .connection
            .query_row(sql, [value], |row| {
                Ok((row.get(0)?, row.get(1)?, row.get(2)?))
            })
            .optional()
            .map_err(ReferenceStoreError::sqlite)?;
        let Some((symbol, osi, underlying)) = mapping else {
            return Ok(None);
        };
        let mut statement = self
            .connection
            .prepare(
                "SELECT venue,matching_unit,status,object_id,row_number,evidence FROM cboe_presence WHERE cboe_symbol=?1 ORDER BY CASE venue WHEN 'c1' THEN 0 WHEN 'bzx' THEN 1 WHEN 'c2' THEN 2 WHEN 'edgx' THEN 3 ELSE 4 END",
            )
            .map_err(ReferenceStoreError::sqlite)?;
        let mut rows = statement
            .query([symbol.as_str()])
            .map_err(ReferenceStoreError::sqlite)?;
        let mut venues = Vec::new();
        while let Some(row) = rows.next().map_err(ReferenceStoreError::sqlite)? {
            if venues.len() >= 4
                || venues.len()
                    >= usize::try_from(self.receipt.limits.max_query_rows().get())
                        .map_err(|_| ReferenceStoreError::QueryLimitExceeded)?
            {
                return Err(ReferenceStoreError::QueryLimitExceeded);
            }
            venues.push(CboeVenuePresenceView::try_from_spool(
                row.get::<_, String>(0)
                    .map_err(ReferenceStoreError::sqlite)?,
                row.get::<_, i64>(1).map_err(ReferenceStoreError::sqlite)?,
                row.get::<_, String>(2)
                    .map_err(ReferenceStoreError::sqlite)?,
                row.get::<_, String>(3)
                    .map_err(ReferenceStoreError::sqlite)?,
                row.get::<_, i64>(4).map_err(ReferenceStoreError::sqlite)?,
                row.get::<_, String>(5)
                    .map_err(ReferenceStoreError::sqlite)?,
            )?);
        }
        if venues.is_empty() {
            return Err(ReferenceStoreError::InvalidGeneration);
        }
        Ok(Some(CboeContractReferenceView::try_from_spool(
            symbol,
            osi,
            underlying,
            CanonicalReferenceIdentityState::Unresolved,
            venues,
        )?))
    }

    /// Queries one exact OCC product/root and product-type key.
    pub fn occ_product(
        &self,
        options_symbol: &ProviderInstrumentId,
        product_type: OccProductType,
    ) -> Result<AuthenticatedReferenceQuery<OccProductReferenceView>, ReferenceStoreError> {
        let coordinate = ReferenceQueryCoordinate::OccProduct {
            options_symbol: SourceIdentifier::try_from(options_symbol.as_str())
                .map_err(|_| ReferenceStoreError::InvalidQuery)?,
            product_type: SourceIdentifier::try_from(product_type.provider_code())
                .map_err(|_| ReferenceStoreError::InvalidQuery)?,
        };
        self.verify_active_evidence()?;
        let value = self.query_occ_product(options_symbol, product_type)?;
        let native_row_count = u64::from(value.is_some());
        self.finish_query(value, native_row_count, coordinate)
    }

    fn query_occ_product(
        &self,
        options_symbol: &ProviderInstrumentId,
        product_type: OccProductType,
    ) -> Result<Option<OccProductReferenceView>, ReferenceStoreError> {
        let row: Option<(
            String,
            String,
            String,
            String,
            String,
            String,
            String,
            String,
            i64,
        )> = self
            .connection
            .query_row(
                "SELECT underlying_symbol,symbol_name,exchanges,exchange_state,position_state,position_value,object_id,evidence,row_number FROM occ_products WHERE options_symbol=?1 AND product_type=?2",
                params![options_symbol.as_str(), product_type.provider_code()],
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
                    ))
                },
            )
            .optional()
            .map_err(ReferenceStoreError::sqlite)?;
        let Some((
            underlying,
            symbol_name,
            exchanges,
            exchange_state,
            position_state,
            position_value,
            object_id,
            evidence,
            row_number,
        )) = row
        else {
            return Ok(None);
        };
        let exchange_listing_evidence =
            OccExchangeListingEvidence::try_from_stable_label(&exchange_state)?;
        let trading_exchanges = decode_exchange_codes(&exchanges, exchange_listing_evidence)?;
        let position_limit = decode_position_limit(&position_state, &position_value)?;
        Ok(Some(OccProductReferenceView::try_from_spool(
            options_symbol.as_str().to_owned(),
            product_type,
            underlying,
            symbol_name,
            trading_exchanges,
            exchange_listing_evidence,
            position_limit,
            CanonicalReferenceIdentityState::Unresolved,
            object_id,
            row_number,
            evidence,
        )?))
    }

    fn query_cboe_export_range(
        &self,
        first_ordinal: u64,
        last_ordinal: u64,
    ) -> Result<Vec<CboeContractReferenceView>, ReferenceStoreError> {
        let mut statement = self
            .connection
            .prepare(
                "SELECT e.export_ordinal,c.cboe_symbol,c.osi,c.underlying,p.venue,p.matching_unit,p.status,p.object_id,p.row_number,p.evidence FROM cboe_export e JOIN cboe_contracts c ON c.cboe_symbol=e.cboe_symbol JOIN cboe_presence p ON p.cboe_symbol=c.cboe_symbol WHERE e.export_ordinal BETWEEN ?1 AND ?2 ORDER BY e.export_ordinal,CASE p.venue WHEN 'c1' THEN 0 WHEN 'bzx' THEN 1 WHEN 'c2' THEN 2 WHEN 'edgx' THEN 3 ELSE 4 END",
            )
            .map_err(ReferenceStoreError::sqlite)?;
        let mut rows = statement
            .query(params![
                i64::try_from(first_ordinal)
                    .map_err(|_| ReferenceStoreError::QueryLimitExceeded)?,
                i64::try_from(last_ordinal).map_err(|_| ReferenceStoreError::QueryLimitExceeded)?
            ])
            .map_err(ReferenceStoreError::sqlite)?;
        let expected = last_ordinal
            .checked_sub(first_ordinal)
            .and_then(|count| count.checked_add(1))
            .ok_or(ReferenceStoreError::QueryLimitExceeded)?;
        let mut values = Vec::new();
        values
            .try_reserve_exact(
                usize::try_from(expected).map_err(|_| ReferenceStoreError::CapacityUnavailable)?,
            )
            .map_err(|_| ReferenceStoreError::CapacityUnavailable)?;
        let mut current: Option<(u64, String, String, String, Vec<CboeVenuePresenceView>)> = None;
        while let Some(row) = rows.next().map_err(ReferenceStoreError::sqlite)? {
            let ordinal = u64::try_from(row.get::<_, i64>(0).map_err(ReferenceStoreError::sqlite)?)
                .map_err(|_| ReferenceStoreError::InvalidGeneration)?;
            let symbol = row
                .get::<_, String>(1)
                .map_err(ReferenceStoreError::sqlite)?;
            let osi = row
                .get::<_, String>(2)
                .map_err(ReferenceStoreError::sqlite)?;
            let underlying = row
                .get::<_, String>(3)
                .map_err(ReferenceStoreError::sqlite)?;
            if current
                .as_ref()
                .is_some_and(|(prior, ..)| *prior != ordinal)
            {
                let (_, prior_symbol, prior_osi, prior_underlying, venues) = current
                    .take()
                    .ok_or(ReferenceStoreError::InvalidGeneration)?;
                values.push(CboeContractReferenceView::try_from_spool(
                    prior_symbol,
                    prior_osi,
                    prior_underlying,
                    CanonicalReferenceIdentityState::Unresolved,
                    venues,
                )?);
            }
            let current = current.get_or_insert_with(|| {
                (
                    ordinal,
                    symbol.clone(),
                    osi.clone(),
                    underlying.clone(),
                    Vec::new(),
                )
            });
            if current.0 != ordinal
                || current.1 != symbol
                || current.2 != osi
                || current.3 != underlying
                || current.4.len() >= 4
            {
                return Err(ReferenceStoreError::InvalidGeneration);
            }
            current.4.push(CboeVenuePresenceView::try_from_spool(
                row.get::<_, String>(4)
                    .map_err(ReferenceStoreError::sqlite)?,
                row.get::<_, i64>(5).map_err(ReferenceStoreError::sqlite)?,
                row.get::<_, String>(6)
                    .map_err(ReferenceStoreError::sqlite)?,
                row.get::<_, String>(7)
                    .map_err(ReferenceStoreError::sqlite)?,
                row.get::<_, i64>(8).map_err(ReferenceStoreError::sqlite)?,
                row.get::<_, String>(9)
                    .map_err(ReferenceStoreError::sqlite)?,
            )?);
        }
        let (_, symbol, osi, underlying, venues) =
            current.ok_or(ReferenceStoreError::InvalidGeneration)?;
        values.push(CboeContractReferenceView::try_from_spool(
            symbol,
            osi,
            underlying,
            CanonicalReferenceIdentityState::Unresolved,
            venues,
        )?);
        if u64::try_from(values.len()).ok() != Some(expected) {
            return Err(ReferenceStoreError::InvalidGeneration);
        }
        Ok(values)
    }

    fn query_occ_export_range(
        &self,
        first_ordinal: u64,
        last_ordinal: u64,
    ) -> Result<Vec<OccProductReferenceView>, ReferenceStoreError> {
        let mut statement = self
            .connection
            .prepare(
                "SELECT e.export_ordinal,o.options_symbol,o.product_type,o.underlying_symbol,o.symbol_name,o.exchanges,o.exchange_state,o.position_state,o.position_value,o.object_id,o.evidence,o.row_number FROM occ_export e JOIN occ_products o ON o.options_symbol=e.options_symbol AND o.product_type=e.product_type WHERE e.export_ordinal BETWEEN ?1 AND ?2 ORDER BY e.export_ordinal",
            )
            .map_err(ReferenceStoreError::sqlite)?;
        let rows = statement
            .query_map(
                params![
                    i64::try_from(first_ordinal)
                        .map_err(|_| ReferenceStoreError::QueryLimitExceeded)?,
                    i64::try_from(last_ordinal)
                        .map_err(|_| ReferenceStoreError::QueryLimitExceeded)?
                ],
                |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, String>(4)?,
                        row.get::<_, String>(5)?,
                        row.get::<_, String>(6)?,
                        row.get::<_, String>(7)?,
                        row.get::<_, String>(8)?,
                        row.get::<_, String>(9)?,
                        row.get::<_, String>(10)?,
                        row.get::<_, i64>(11)?,
                    ))
                },
            )
            .map_err(ReferenceStoreError::sqlite)?;
        let expected = last_ordinal
            .checked_sub(first_ordinal)
            .and_then(|count| count.checked_add(1))
            .ok_or(ReferenceStoreError::QueryLimitExceeded)?;
        let mut values = Vec::new();
        values
            .try_reserve_exact(
                usize::try_from(expected).map_err(|_| ReferenceStoreError::CapacityUnavailable)?,
            )
            .map_err(|_| ReferenceStoreError::CapacityUnavailable)?;
        let mut expected_ordinal = first_ordinal;
        for row in rows {
            let (
                ordinal,
                options_symbol,
                product_type,
                underlying,
                symbol_name,
                exchanges,
                exchange_state,
                position_state,
                position_value,
                object_id,
                evidence,
                row_number,
            ) = row.map_err(ReferenceStoreError::sqlite)?;
            if u64::try_from(ordinal).ok() != Some(expected_ordinal) {
                return Err(ReferenceStoreError::InvalidGeneration);
            }
            let product_type = OccProductType::try_from_provider(&product_type)?;
            let exchange_listing_evidence =
                OccExchangeListingEvidence::try_from_stable_label(&exchange_state)?;
            let trading_exchanges = decode_exchange_codes(&exchanges, exchange_listing_evidence)?;
            let position_limit = decode_position_limit(&position_state, &position_value)?;
            values.push(OccProductReferenceView::try_from_spool(
                options_symbol,
                product_type,
                underlying,
                symbol_name,
                trading_exchanges,
                exchange_listing_evidence,
                position_limit,
                CanonicalReferenceIdentityState::Unresolved,
                object_id,
                row_number,
                evidence,
            )?);
            expected_ordinal = expected_ordinal
                .checked_add(1)
                .ok_or(ReferenceStoreError::QueryLimitExceeded)?;
        }
        if u64::try_from(values.len()).ok() != Some(expected) {
            return Err(ReferenceStoreError::InvalidGeneration);
        }
        Ok(values)
    }

    /// Exports every Cboe contract mapping in deterministic provider-key pages after restart.
    ///
    /// Each row retains exact venue/object/row evidence. The page additionally carries the
    /// complete bounded raw-object clock, request, and schema closure, so canonical composition
    /// joins solely by exact `object_id` and never by ticker or name.
    pub fn cboe_contract_page_for_transform(
        &self,
        cursor: Option<&ReferenceCanonicalExportCursor>,
        maximum_rows: NonZeroU32,
    ) -> Result<AuthenticatedReferencePage<CboeContractReferenceView>, ReferenceStoreError> {
        self.validate_export_page_limit(maximum_rows)?;
        self.verify_active_evidence()?;
        let total_rows = self
            .connection
            .query_row("SELECT MAX(export_ordinal) FROM cboe_export", [], |row| {
                sqlite_optional_u64(row, 0)
            })
            .map_err(ReferenceStoreError::sqlite)?
            .and_then(NonZeroU64::new)
            .ok_or(ReferenceStoreError::InvalidGeneration)?;
        let (after_symbol, rows_emitted_before, page_ordinal) = match cursor {
            Some(cursor) => {
                cursor
                    .validate_for(&self.receipt, ReferenceCanonicalExportFamily::CboeContracts)?;
                if cursor.total_rows != total_rows {
                    return Err(ReferenceStoreError::InvalidQuery);
                }
                if cursor.page_size != maximum_rows {
                    return Err(ReferenceStoreError::InvalidQuery);
                }
                let indexed_symbol: String = self
                    .connection
                    .query_row(
                        "SELECT cboe_symbol FROM cboe_export WHERE export_ordinal=?1",
                        [i64::try_from(cursor.rows_emitted)
                            .map_err(|_| ReferenceStoreError::InvalidQuery)?],
                        |row| row.get(0),
                    )
                    .map_err(ReferenceStoreError::sqlite)?;
                if indexed_symbol != cursor.after_primary.as_str() {
                    return Err(ReferenceStoreError::InvalidQuery);
                }
                (
                    Some(cursor.after_primary.clone()),
                    cursor.rows_emitted,
                    cursor.next_page_ordinal,
                )
            }
            None => (None, 0, NonZeroU32::MIN),
        };
        let first_ordinal = rows_emitted_before
            .checked_add(1)
            .ok_or(ReferenceStoreError::QueryLimitExceeded)?;
        let rows_emitted = rows_emitted_before
            .checked_add(u64::from(maximum_rows.get()))
            .map(|last| last.min(total_rows.get()))
            .ok_or(ReferenceStoreError::QueryLimitExceeded)?;
        let values = self.query_cboe_export_range(first_ordinal, rows_emitted)?;
        let complete = rows_emitted == total_rows.get();
        let next_cursor = if !complete {
            let last = values
                .last()
                .ok_or(ReferenceStoreError::InvalidGeneration)?;
            Some(ReferenceCanonicalExportCursor::try_new(
                &self.receipt,
                ReferenceCanonicalExportFamily::CboeContracts,
                SourceIdentifier::try_from(last.cboe_symbol_id().as_str())
                    .map_err(|_| ReferenceStoreError::InvalidGeneration)?,
                None,
                total_rows,
                rows_emitted,
                NonZeroU32::new(
                    page_ordinal
                        .get()
                        .checked_add(1)
                        .ok_or(ReferenceStoreError::QueryLimitExceeded)?,
                )
                .ok_or(ReferenceStoreError::QueryLimitExceeded)?,
                maximum_rows,
            )?)
        } else {
            None
        };
        let coordinate = ReferenceQueryCoordinate::CboeContractPage {
            after_symbol,
            maximum_rows,
            total_rows,
            rows_emitted_before,
            page_ordinal,
            export_contract_digest: export_contract_digest(
                ReferenceCanonicalExportFamily::CboeContracts,
            ),
        };
        let native_row_count = values.iter().try_fold(0_u64, |count, value| {
            count
                .checked_add(
                    u64::try_from(value.venues().len())
                        .map_err(|_| ReferenceStoreError::QueryLimitExceeded)?,
                )
                .ok_or(ReferenceStoreError::QueryLimitExceeded)
        })?;
        self.finish_page(
            values,
            next_cursor,
            total_rows,
            rows_emitted,
            complete,
            native_row_count,
            coordinate,
        )
    }

    /// Exports every OCC product/root row in deterministic provider-key pages after restart.
    pub fn occ_product_page_for_transform(
        &self,
        cursor: Option<&ReferenceCanonicalExportCursor>,
        maximum_rows: NonZeroU32,
    ) -> Result<AuthenticatedReferencePage<OccProductReferenceView>, ReferenceStoreError> {
        self.validate_export_page_limit(maximum_rows)?;
        self.verify_active_evidence()?;
        let total_rows = self
            .connection
            .query_row("SELECT MAX(export_ordinal) FROM occ_export", [], |row| {
                sqlite_optional_u64(row, 0)
            })
            .map_err(ReferenceStoreError::sqlite)?
            .and_then(NonZeroU64::new)
            .ok_or(ReferenceStoreError::InvalidGeneration)?;
        let (after_options_symbol, after_product_type, rows_emitted_before, page_ordinal) =
            match cursor {
                Some(cursor) => {
                    cursor
                        .validate_for(&self.receipt, ReferenceCanonicalExportFamily::OccProducts)?;
                    if cursor.total_rows != total_rows {
                        return Err(ReferenceStoreError::InvalidQuery);
                    }
                    if cursor.page_size != maximum_rows {
                        return Err(ReferenceStoreError::InvalidQuery);
                    }
                    let indexed_key: (String, String) = self
                        .connection
                        .query_row(
                            "SELECT options_symbol,product_type FROM occ_export WHERE export_ordinal=?1",
                            [i64::try_from(cursor.rows_emitted)
                                .map_err(|_| ReferenceStoreError::InvalidQuery)?],
                            |row| Ok((row.get(0)?, row.get(1)?)),
                        )
                        .map_err(ReferenceStoreError::sqlite)?;
                    if indexed_key.0 != cursor.after_primary.as_str()
                        || cursor
                            .after_secondary
                            .as_ref()
                            .is_none_or(|value| value.as_str() != indexed_key.1)
                    {
                        return Err(ReferenceStoreError::InvalidQuery);
                    }
                    (
                        Some(cursor.after_primary.clone()),
                        cursor.after_secondary.clone(),
                        cursor.rows_emitted,
                        cursor.next_page_ordinal,
                    )
                }
                None => (None, None, 0, NonZeroU32::MIN),
            };
        let first_ordinal = rows_emitted_before
            .checked_add(1)
            .ok_or(ReferenceStoreError::QueryLimitExceeded)?;
        let rows_emitted = rows_emitted_before
            .checked_add(u64::from(maximum_rows.get()))
            .map(|last| last.min(total_rows.get()))
            .ok_or(ReferenceStoreError::QueryLimitExceeded)?;
        let values = self.query_occ_export_range(first_ordinal, rows_emitted)?;
        let complete = rows_emitted == total_rows.get();
        let next_cursor = if !complete {
            let last = values
                .last()
                .ok_or(ReferenceStoreError::InvalidGeneration)?;
            Some(ReferenceCanonicalExportCursor::try_new(
                &self.receipt,
                ReferenceCanonicalExportFamily::OccProducts,
                SourceIdentifier::try_from(last.options_symbol().as_str())
                    .map_err(|_| ReferenceStoreError::InvalidGeneration)?,
                Some(
                    SourceIdentifier::try_from(last.product_type().provider_code())
                        .map_err(|_| ReferenceStoreError::InvalidGeneration)?,
                ),
                total_rows,
                rows_emitted,
                NonZeroU32::new(
                    page_ordinal
                        .get()
                        .checked_add(1)
                        .ok_or(ReferenceStoreError::QueryLimitExceeded)?,
                )
                .ok_or(ReferenceStoreError::QueryLimitExceeded)?,
                maximum_rows,
            )?)
        } else {
            None
        };
        let coordinate = ReferenceQueryCoordinate::OccProductPage {
            after_options_symbol,
            after_product_type,
            maximum_rows,
            total_rows,
            rows_emitted_before,
            page_ordinal,
            export_contract_digest: export_contract_digest(
                ReferenceCanonicalExportFamily::OccProducts,
            ),
        };
        let native_row_count =
            u64::try_from(values.len()).map_err(|_| ReferenceStoreError::QueryLimitExceeded)?;
        self.finish_page(
            values,
            next_cursor,
            total_rows,
            rows_emitted,
            complete,
            native_row_count,
            coordinate,
        )
    }

    fn validate_export_page_limit(
        &self,
        maximum_rows: NonZeroU32,
    ) -> Result<(), ReferenceStoreError> {
        if maximum_rows.get() > MAX_CANONICAL_EXPORT_PAGE_ROWS
            || maximum_rows > self.receipt.limits.max_query_rows()
        {
            Err(ReferenceStoreError::QueryLimitExceeded)
        } else {
            Ok(())
        }
    }

    fn finish_page<T>(
        &self,
        rows: Vec<T>,
        next_cursor: Option<ReferenceCanonicalExportCursor>,
        total_rows: NonZeroU64,
        rows_emitted: u64,
        complete: bool,
        native_row_count: u64,
        coordinate: ReferenceQueryCoordinate,
    ) -> Result<AuthenticatedReferencePage<T>, ReferenceStoreError>
    where
        T: Serialize,
    {
        let object_evidence = self.object_evidence()?;
        let result_item_count =
            u32::try_from(rows.len()).map_err(|_| ReferenceStoreError::QueryLimitExceeded)?;
        let evidence = authenticated_query_evidence(
            &self.receipt,
            coordinate,
            &(
                &rows,
                &object_evidence,
                &next_cursor,
                total_rows,
                rows_emitted,
                complete,
            ),
            result_item_count,
            native_row_count,
        )?;
        self.verify_active_evidence()?;
        Ok(AuthenticatedReferencePage {
            rows,
            object_evidence,
            next_cursor,
            total_rows,
            rows_emitted,
            complete,
            evidence,
        })
    }

    fn finish_query<T>(
        &self,
        value: Option<T>,
        native_row_count: u64,
        coordinate: ReferenceQueryCoordinate,
    ) -> Result<AuthenticatedReferenceQuery<T>, ReferenceStoreError>
    where
        T: Serialize,
    {
        let object_evidence = self.object_evidence()?;
        let result_item_count = u32::from(value.is_some());
        let evidence = authenticated_query_evidence(
            &self.receipt,
            coordinate,
            &(&value, &object_evidence),
            result_item_count,
            native_row_count,
        )?;
        self.verify_active_evidence()?;
        Ok(AuthenticatedReferenceQuery {
            value,
            object_evidence,
            evidence,
        })
    }

    fn verify_active_evidence(&self) -> Result<(), ReferenceStoreError> {
        self.database_file.verify_unchanged()?;
        for raw in &self.raw_files {
            raw.verify_unchanged()?;
        }
        Ok(())
    }
}

fn authenticated_query_evidence<T: Serialize>(
    receipt: &ReferenceGenerationReceipt,
    coordinate: ReferenceQueryCoordinate,
    result: &T,
    result_item_count: u32,
    native_row_count: u64,
) -> Result<ReferenceQueryEvidence, ReferenceStoreError> {
    let coordinate_digest = digest_serializable(
        b"market-squawk:options-reference-query-coordinate:v1\0",
        &coordinate,
        16 * 1024,
    )?;
    let maximum_result_bytes = if matches!(
        &coordinate,
        ReferenceQueryCoordinate::CboeContractPage { .. }
            | ReferenceQueryCoordinate::OccProductPage { .. }
    ) {
        MAX_CANONICAL_EXPORT_PAGE_EVIDENCE_BYTES
    } else {
        MAX_EXACT_QUERY_EVIDENCE_BYTES
    };
    let result_digest = digest_serializable(
        b"market-squawk:options-reference-query-result:v1\0",
        &(
            &receipt.generation_id,
            receipt.database_digest,
            coordinate_digest,
            result_item_count,
            native_row_count,
            result,
        ),
        maximum_result_bytes,
    )?;
    Ok(ReferenceQueryEvidence {
        generation_id: receipt.generation_id.clone(),
        database_digest: receipt.database_digest,
        coordinate,
        coordinate_digest,
        result_digest,
        result_item_count,
        native_row_count,
    })
}

fn export_contract_digest(family: ReferenceCanonicalExportFamily) -> EvidenceDigest {
    let contract: &[u8] = match family {
        ReferenceCanonicalExportFamily::CboeContracts => {
            b"cboe-contracts;order=cboe_symbol-binary;schema=cboe-contract-reference-view-v1"
        }
        ReferenceCanonicalExportFamily::OccProducts => {
            b"occ-products;order=options_symbol-binary,product_type-binary;schema=occ-product-reference-view-v1"
        }
    };
    domain_digest(
        b"market-squawk:options-reference-export-contract:v1\0",
        contract,
    )
}

struct BoundedDigestWriter {
    digest: Sha256,
    observed: u64,
    maximum: u64,
}

impl Write for BoundedDigestWriter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let next = self
            .observed
            .checked_add(u64::try_from(bytes.len()).unwrap_or(u64::MAX))
            .ok_or_else(|| std::io::Error::other("serialized evidence exceeded its bound"))?;
        if next > self.maximum {
            return Err(std::io::Error::other(
                "serialized evidence exceeded its bound",
            ));
        }
        self.digest.update(bytes);
        self.observed = next;
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn digest_serializable<T: Serialize>(
    domain: &'static [u8],
    value: &T,
    maximum_bytes: u64,
) -> Result<EvidenceDigest, ReferenceStoreError> {
    let mut writer = BoundedDigestWriter {
        digest: Sha256::new(),
        observed: 0,
        maximum: maximum_bytes,
    };
    writer.digest.update(domain);
    serde_json::to_writer(&mut writer, value)
        .map_err(|_| ReferenceStoreError::QueryLimitExceeded)?;
    let digest: [u8; 32] = writer.digest.finalize().into();
    if writer.observed == 0 || digest.iter().all(|byte| *byte == 0) {
        return Err(ReferenceStoreError::InvalidQuery);
    }
    Ok(EvidenceDigest::new(DigestAlgorithm::Sha256, digest))
}

fn domain_digest(domain: &'static [u8], bytes: &[u8]) -> EvidenceDigest {
    let mut digest = Sha256::new();
    digest.update(domain);
    digest.update(u64::try_from(bytes.len()).unwrap_or(u64::MAX).to_be_bytes());
    digest.update(bytes);
    EvidenceDigest::new(DigestAlgorithm::Sha256, digest.finalize().into())
}

/// Recovery failure classification that never promotes an unverified candidate.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ReferenceRecoveryFailure {
    /// The receipted immutable database object is absent.
    Missing,
    /// Bytes, schema, integrity, limits, or raw-object closure are incompatible.
    CorruptOrIncompatible,
}

/// One skipped generation during newest-to-oldest recovery.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ReferenceRecoveryRejection {
    generation_id: SourceIdentifier,
    failure: ReferenceRecoveryFailure,
}

impl ReferenceRecoveryRejection {
    fn new(generation_id: SourceIdentifier, failure: ReferenceRecoveryFailure) -> Self {
        Self {
            generation_id,
            failure,
        }
    }

    /// Returns the skipped generation identity.
    pub const fn generation_id(&self) -> &SourceIdentifier {
        &self.generation_id
    }

    /// Returns the fail-closed reason.
    pub const fn failure(&self) -> ReferenceRecoveryFailure {
        self.failure
    }
}

/// Recovery result containing at most one verified last-complete generation.
pub struct ReferenceRecoveryOutcome {
    generation: Option<ReferenceGeneration>,
    rejected: Vec<ReferenceRecoveryRejection>,
    quarantined_staging: u32,
}

impl std::fmt::Debug for ReferenceRecoveryOutcome {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ReferenceRecoveryOutcome")
            .field("generation", &self.generation)
            .field("rejected", &self.rejected)
            .field("quarantined_staging", &self.quarantined_staging)
            .finish()
    }
}

impl ReferenceRecoveryOutcome {
    /// Returns the newest independently verified complete generation, if one exists.
    pub const fn generation(&self) -> Option<&ReferenceGeneration> {
        self.generation.as_ref()
    }

    /// Consumes the recovery result into its verified generation.
    pub fn into_generation(self) -> Option<ReferenceGeneration> {
        self.generation
    }

    /// Returns skipped newer candidates in newest-to-older order.
    pub fn rejected(&self) -> &[ReferenceRecoveryRejection] {
        &self.rejected
    }

    /// Returns interrupted provider-local staging files moved out of publication namespaces.
    pub const fn quarantined_staging(&self) -> u32 {
        self.quarantined_staging
    }
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct RawDatabaseEvidence {
    object_id: String,
    digest: [u8; 32],
    bytes: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ReceiptManifestRecord {
    database_digest: [u8; 32],
    database_bytes: u64,
    limits: ReferenceSpoolLimits,
}

struct CurrentActivationPointer {
    activation_name: String,
    activation_digest: [u8; 32],
    activation_bytes: u64,
}

fn encode_receipt_manifest(
    receipt: &ReferenceGenerationReceipt,
) -> Result<Vec<u8>, ReferenceStoreError> {
    encode_receipt_manifest_record(&ReceiptManifestRecord {
        database_digest: receipt.database_digest.bytes(),
        database_bytes: receipt.database_bytes,
        limits: receipt.limits,
    })
}

fn encode_receipt_manifest_record(
    record: &ReceiptManifestRecord,
) -> Result<Vec<u8>, ReferenceStoreError> {
    let limits = record.limits;
    let value = format!(
        "{RECEIPT_MANIFEST_MAGIC}\n{}\n{}\n{}\n{}\n{}\n{}\n",
        hex_digest(record.database_digest),
        record.database_bytes,
        limits.max_database_bytes(),
        limits.sqlite_cache_bytes(),
        limits.max_conflicts(),
        limits.max_query_rows().get(),
    );
    if value.len() > usize::try_from(MAX_MANIFEST_BYTES).unwrap_or(usize::MAX) {
        return Err(ReferenceStoreError::InvalidActivationManifest);
    }
    Ok(value.into_bytes())
}

fn parse_receipt_manifest(bytes: &[u8]) -> Result<ReceiptManifestRecord, ReferenceStoreError> {
    let lines = canonical_manifest_lines(bytes, 7, MAX_MANIFEST_BYTES)?;
    if lines[0] != RECEIPT_MANIFEST_MAGIC {
        return Err(ReferenceStoreError::InvalidActivationManifest);
    }
    let database_digest = parse_hex_digest(lines[1])?;
    let database_bytes = parse_canonical_u64(lines[2])?;
    let limits = ReferenceSpoolLimits::try_new(
        parse_canonical_u64(lines[3])?,
        parse_canonical_u64(lines[4])?,
        parse_canonical_u32(lines[5])?,
        parse_canonical_u32(lines[6])?,
    )
    .map_err(|_| ReferenceStoreError::InvalidActivationManifest)?;
    if database_bytes == 0 || database_bytes > limits.max_database_bytes() {
        return Err(ReferenceStoreError::InvalidActivationManifest);
    }
    Ok(ReceiptManifestRecord {
        database_digest,
        database_bytes,
        limits,
    })
}

fn parse_receipt_manifest_name(name: &str) -> Result<([u8; 32], [u8; 32]), ReferenceStoreError> {
    let stem = name
        .strip_suffix(".receipt")
        .ok_or(ReferenceStoreError::InvalidActivationManifest)?;
    if stem.len() != 129 || stem.as_bytes().get(64) != Some(&b'-') {
        return Err(ReferenceStoreError::InvalidActivationManifest);
    }
    Ok((
        parse_hex_digest(&stem[..64])?,
        parse_hex_digest(&stem[65..])?,
    ))
}

fn encode_activation_index(history: &[String]) -> Result<Vec<u8>, ReferenceStoreError> {
    if history.len() > MAX_RECOVERY_RECEIPTS {
        return Err(ReferenceStoreError::RecoveryLimitExceeded);
    }
    let mut value = format!("{ACTIVATION_INDEX_MAGIC}\n{}\n", history.len());
    for name in history {
        let _ = parse_receipt_manifest_name(name)?;
        value.push_str(name);
        value.push('\n');
    }
    if value.len() > usize::try_from(MAX_ACTIVATION_BYTES).unwrap_or(usize::MAX) {
        return Err(ReferenceStoreError::RecoveryLimitExceeded);
    }
    Ok(value.into_bytes())
}

fn parse_activation_index(bytes: &[u8]) -> Result<Vec<String>, ReferenceStoreError> {
    if bytes.is_empty()
        || u64::try_from(bytes.len()).unwrap_or(u64::MAX) > MAX_ACTIVATION_BYTES
        || bytes.last() != Some(&b'\n')
        || bytes.contains(&b'\r')
    {
        return Err(ReferenceStoreError::InvalidActivationManifest);
    }
    let text =
        std::str::from_utf8(bytes).map_err(|_| ReferenceStoreError::InvalidActivationManifest)?;
    let lines = text
        .strip_suffix('\n')
        .unwrap_or(text)
        .split('\n')
        .collect::<Vec<_>>();
    if lines.len() < 2 || lines[0] != ACTIVATION_INDEX_MAGIC {
        return Err(ReferenceStoreError::InvalidActivationManifest);
    }
    let count = usize::try_from(parse_canonical_u64(lines[1])?)
        .map_err(|_| ReferenceStoreError::InvalidActivationManifest)?;
    if count > MAX_RECOVERY_RECEIPTS || lines.len() != count.saturating_add(2) {
        return Err(ReferenceStoreError::InvalidActivationManifest);
    }
    let mut history = Vec::new();
    history
        .try_reserve_exact(count)
        .map_err(|_| ReferenceStoreError::CapacityUnavailable)?;
    for name in &lines[2..] {
        let _ = parse_receipt_manifest_name(name)?;
        if history.iter().any(|existing| existing == name) {
            return Err(ReferenceStoreError::InvalidActivationManifest);
        }
        history.push((*name).to_owned());
    }
    Ok(history)
}

fn encode_current_pointer(
    activation_name: &str,
    activation_digest: [u8; 32],
    activation_bytes: u64,
) -> Vec<u8> {
    format!(
        "{CURRENT_POINTER_MAGIC}\n{activation_name}\n{}\n{activation_bytes}\n",
        hex_digest(activation_digest)
    )
    .into_bytes()
}

fn parse_current_pointer(bytes: &[u8]) -> Result<CurrentActivationPointer, ReferenceStoreError> {
    let lines = canonical_manifest_lines(bytes, 4, MAX_MANIFEST_BYTES)?;
    if lines[0] != CURRENT_POINTER_MAGIC {
        return Err(ReferenceStoreError::InvalidActivationManifest);
    }
    let stem = lines[1]
        .strip_suffix(".activation")
        .ok_or(ReferenceStoreError::InvalidActivationManifest)?;
    let activation_digest = parse_hex_digest(stem)?;
    if parse_hex_digest(lines[2])? != activation_digest {
        return Err(ReferenceStoreError::InvalidActivationManifest);
    }
    let activation_bytes = parse_canonical_u64(lines[3])?;
    if activation_bytes == 0 || activation_bytes > MAX_ACTIVATION_BYTES {
        return Err(ReferenceStoreError::InvalidActivationManifest);
    }
    Ok(CurrentActivationPointer {
        activation_name: lines[1].to_owned(),
        activation_digest,
        activation_bytes,
    })
}

fn canonical_manifest_lines<'a>(
    bytes: &'a [u8],
    expected: usize,
    maximum: u64,
) -> Result<Vec<&'a str>, ReferenceStoreError> {
    if bytes.is_empty()
        || u64::try_from(bytes.len()).unwrap_or(u64::MAX) > maximum
        || bytes.last() != Some(&b'\n')
        || bytes.contains(&b'\r')
    {
        return Err(ReferenceStoreError::InvalidActivationManifest);
    }
    let text =
        std::str::from_utf8(bytes).map_err(|_| ReferenceStoreError::InvalidActivationManifest)?;
    let lines = text
        .strip_suffix('\n')
        .unwrap_or(text)
        .split('\n')
        .collect::<Vec<_>>();
    if lines.len() != expected || lines.iter().any(|line| line.is_empty()) {
        return Err(ReferenceStoreError::InvalidActivationManifest);
    }
    Ok(lines)
}

fn parse_canonical_u64(value: &str) -> Result<u64, ReferenceStoreError> {
    if value.is_empty()
        || value.len() > 20
        || (value.len() > 1 && value.starts_with('0'))
        || !value.bytes().all(|byte| byte.is_ascii_digit())
    {
        return Err(ReferenceStoreError::InvalidActivationManifest);
    }
    value
        .parse()
        .map_err(|_| ReferenceStoreError::InvalidActivationManifest)
}

fn parse_canonical_u32(value: &str) -> Result<u32, ReferenceStoreError> {
    u32::try_from(parse_canonical_u64(value)?)
        .map_err(|_| ReferenceStoreError::InvalidActivationManifest)
}

fn parse_hex_digest(value: &str) -> Result<[u8; 32], ReferenceStoreError> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(ReferenceStoreError::InvalidActivationManifest);
    }
    let mut output = [0_u8; 32];
    for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
        output[index] = (hex_nibble(pair[0])? << 4) | hex_nibble(pair[1])?;
    }
    Ok(output)
}

fn hex_nibble(value: u8) -> Result<u8, ReferenceStoreError> {
    match value {
        b'0'..=b'9' => Ok(value - b'0'),
        b'a'..=b'f' => Ok(value - b'a' + 10),
        _ => Err(ReferenceStoreError::InvalidActivationManifest),
    }
}

fn persist_immutable_capability_bytes(
    directory: &Dir,
    name: &str,
    bytes: &[u8],
    maximum: u64,
) -> Result<(), ReferenceStoreError> {
    let length =
        u64::try_from(bytes.len()).map_err(|_| ReferenceStoreError::InvalidActivationManifest)?;
    if length == 0 || length > maximum {
        return Err(ReferenceStoreError::InvalidActivationManifest);
    }
    let (staged_name, mut staged) = create_capability_staging(directory, STAGED_MANIFEST_PREFIX)?;
    staged
        .write_all(bytes)
        .map_err(|_| ReferenceStoreError::StoreIo)?;
    staged
        .sync_all()
        .map_err(|_| ReferenceStoreError::StoreIo)?;
    publish_capability_no_replace(
        directory,
        &staged_name,
        directory,
        name,
        hash_bytes(bytes),
        length,
    )
}

fn atomic_replace_current_pointer(
    directory: &Dir,
    bytes: &[u8],
    mut checkpoint: impl FnMut() -> Result<(), ReferenceStoreError>,
) -> Result<(), ReferenceStoreError> {
    checkpoint()?;
    let length =
        u64::try_from(bytes.len()).map_err(|_| ReferenceStoreError::InvalidActivationManifest)?;
    if length == 0 || length > MAX_MANIFEST_BYTES {
        return Err(ReferenceStoreError::InvalidActivationManifest);
    }
    let (staged_name, mut staged) = create_capability_staging(directory, STAGED_MANIFEST_PREFIX)?;
    let cleanup = CapabilityStagingCleanup {
        directory,
        name: &staged_name,
        armed: true,
    };
    staged
        .write_all(bytes)
        .map_err(|_| ReferenceStoreError::StoreIo)?;
    staged
        .sync_all()
        .map_err(|_| ReferenceStoreError::StoreIo)?;
    drop(staged);
    checkpoint()?;
    directory
        .rename(&staged_name, directory, CURRENT_ACTIVATION_POINTER)
        .map_err(|_| ReferenceStoreError::StoreIo)?;
    sync_capability_directory(directory)?;
    drop(cleanup);
    let observed =
        read_capability_file_bounded(directory, CURRENT_ACTIVATION_POINTER, MAX_MANIFEST_BYTES)?;
    if observed != bytes {
        return Err(ReferenceStoreError::InvalidActivationManifest);
    }
    Ok(())
}

fn prune_manifest_namespace(
    directory: &Dir,
    history: &[String],
    active_index: &str,
) -> Result<(), ReferenceStoreError> {
    let mut examined = 0_usize;
    let mut removed = false;
    for entry in directory
        .entries()
        .map_err(|_| ReferenceStoreError::StoreIo)?
    {
        examined = examined
            .checked_add(1)
            .ok_or(ReferenceStoreError::RecoveryLimitExceeded)?;
        if examined > MAX_MANIFEST_NAMESPACE_ENTRIES {
            return Err(ReferenceStoreError::RecoveryLimitExceeded);
        }
        let entry = entry.map_err(|_| ReferenceStoreError::StoreIo)?;
        let name = entry
            .file_name()
            .into_string()
            .map_err(|_| ReferenceStoreError::UnsafeStoreEntry)?;
        if !entry
            .file_type()
            .map_err(|_| ReferenceStoreError::StoreIo)?
            .is_file()
        {
            return Err(ReferenceStoreError::UnsafeStoreEntry);
        }
        if name == CURRENT_ACTIVATION_POINTER
            || name == ACTIVATION_LOCK_FILE
            || name == active_index
            || history.iter().any(|retained| retained == &name)
            || name.starts_with(STAGED_MANIFEST_PREFIX)
        {
            continue;
        }
        if name.ends_with(".activation") {
            let stem = name
                .strip_suffix(".activation")
                .ok_or(ReferenceStoreError::InvalidActivationManifest)?;
            let _ = parse_hex_digest(stem)?;
        } else if name.ends_with(".receipt") {
            let _ = parse_receipt_manifest_name(&name)?;
        } else {
            return Err(ReferenceStoreError::UnsafeStoreEntry);
        }
        directory
            .remove_file(&name)
            .map_err(|_| ReferenceStoreError::StoreIo)?;
        removed = true;
    }
    if removed {
        sync_capability_directory(directory)?;
    }
    Ok(())
}

fn read_capability_file_bounded(
    directory: &Dir,
    name: &str,
    maximum: u64,
) -> Result<Vec<u8>, ReferenceStoreError> {
    let mut options = OpenOptions::new();
    options.read(true);
    options.follow(FollowSymlinks::No);
    let capability_file = match directory.open_with(name, &options) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Err(ReferenceStoreError::ObjectMissing);
        }
        Err(_) => return Err(ReferenceStoreError::StoreIo),
    };
    let metadata = capability_file
        .metadata()
        .map_err(|_| ReferenceStoreError::StoreIo)?;
    let direct = directory
        .symlink_metadata(name)
        .map_err(|_| ReferenceStoreError::StoreIo)?;
    if !metadata.is_file() || !direct.is_file() || metadata.len() == 0 || metadata.len() > maximum {
        return Err(ReferenceStoreError::ObjectCorrupt);
    }
    let capacity =
        usize::try_from(metadata.len()).map_err(|_| ReferenceStoreError::CapacityUnavailable)?;
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(capacity)
        .map_err(|_| ReferenceStoreError::CapacityUnavailable)?;
    capability_file
        .into_std()
        .take(maximum.saturating_add(1))
        .read_to_end(&mut bytes)
        .map_err(|_| ReferenceStoreError::StoreIo)?;
    if bytes.len() != capacity {
        return Err(ReferenceStoreError::ObjectCorrupt);
    }
    Ok(bytes)
}

fn read_capability_digest_bounded(
    directory: &Dir,
    name: &str,
    digest: [u8; 32],
    expected_bytes: u64,
    maximum: u64,
) -> Result<Vec<u8>, ReferenceStoreError> {
    let mut file =
        validate_capability_content_file(directory, name, digest, expected_bytes, maximum)?.file;
    let capacity =
        usize::try_from(expected_bytes).map_err(|_| ReferenceStoreError::CapacityUnavailable)?;
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(capacity)
        .map_err(|_| ReferenceStoreError::CapacityUnavailable)?;
    file.read_to_end(&mut bytes)
        .map_err(|_| ReferenceStoreError::StoreIo)?;
    if bytes.len() != capacity {
        return Err(ReferenceStoreError::ObjectCorrupt);
    }
    Ok(bytes)
}

fn read_capability_digest_with_unknown_size(
    directory: &Dir,
    name: &str,
    digest: [u8; 32],
    maximum: u64,
) -> Result<Vec<u8>, ReferenceStoreError> {
    let bytes = read_capability_file_bounded(directory, name, maximum)?;
    if hash_bytes(&bytes) != digest {
        return Err(ReferenceStoreError::ObjectCorrupt);
    }
    Ok(bytes)
}

fn reconstruct_catalog_evidence(
    connection: &Connection,
    manifest: &ReceiptManifestRecord,
) -> Result<(PublicationRequest, CatalogCounts, Vec<SourceIdentifier>), ReferenceStoreError> {
    let cache_kib = manifest.limits.sqlite_cache_bytes() / 1_024;
    connection
        .execute_batch(&format!(
            "PRAGMA query_only=ON; PRAGMA trusted_schema=OFF; PRAGMA foreign_keys=ON; PRAGMA temp_store=MEMORY; PRAGMA mmap_size=0; PRAGMA cache_size=-{cache_kib};"
        ))
        .map_err(ReferenceStoreError::sqlite)?;
    let metadata: (
        String,
        i64,
        i64,
        String,
        i64,
        i64,
        i64,
        i64,
        i64,
        i64,
        i64,
        i64,
        i64,
    ) = connection
        .query_row(
            "SELECT request_id,requested_at,deadline,surfaces_json,max_surfaces,max_pages,max_total_bytes,max_total_records,max_conflicts,max_database_bytes,sqlite_cache_bytes,max_retained_conflicts,max_query_rows FROM generation_metadata WHERE singleton=1 AND schema_identity='market-squawk-options-reference-v6'",
            [],
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
                ))
            },
        )
        .map_err(ReferenceStoreError::sqlite)?;
    if metadata.3.len() > 8 * 1024
        || u64::try_from(metadata.9).ok() != Some(manifest.limits.max_database_bytes())
        || u64::try_from(metadata.10).ok() != Some(manifest.limits.sqlite_cache_bytes())
        || u32::try_from(metadata.11).ok() != Some(manifest.limits.max_conflicts())
        || u32::try_from(metadata.12).ok() != Some(manifest.limits.max_query_rows().get())
    {
        return Err(ReferenceStoreError::InvalidActivationManifest);
    }
    let max_surfaces =
        usize::try_from(metadata.4).map_err(|_| ReferenceStoreError::InvalidActivationManifest)?;
    let publication_limits = PublicationLimits::try_new(
        max_surfaces,
        u32::try_from(metadata.5).map_err(|_| ReferenceStoreError::InvalidActivationManifest)?,
        u64::try_from(metadata.6).map_err(|_| ReferenceStoreError::InvalidActivationManifest)?,
        u64::try_from(metadata.7).map_err(|_| ReferenceStoreError::InvalidActivationManifest)?,
        usize::try_from(metadata.8).map_err(|_| ReferenceStoreError::InvalidActivationManifest)?,
    )
    .map_err(|_| ReferenceStoreError::InvalidActivationManifest)?;
    let surfaces = parse_surface_closure(&metadata.3)?;
    let request = PublicationRequest::try_new(
        SourceIdentifier::try_from(metadata.0.as_str())
            .map_err(|_| ReferenceStoreError::InvalidActivationManifest)?,
        Timestamp::from_unix_nanos(metadata.1),
        Timestamp::from_unix_nanos(metadata.2),
        surfaces,
        publication_limits,
    )
    .map_err(|_| ReferenceStoreError::InvalidActivationManifest)?;

    let counts: (u64, u64, u64, u64, u64, u64, u64) = connection
        .query_row(
            "SELECT (SELECT COUNT(*) FROM pages),(SELECT COALESCE(SUM(payload_bytes),0) FROM objects),(SELECT COALESCE(SUM(returned_records),0) FROM pages),(SELECT COUNT(*) FROM cboe_presence),(SELECT COUNT(*) FROM occ_products),(SELECT COUNT(*) FROM conflicts),(SELECT COUNT(*) FROM objects)",
            [],
            |row| {
                Ok((
                    sqlite_u64(row, 0)?,
                    sqlite_u64(row, 1)?,
                    sqlite_u64(row, 2)?,
                    sqlite_u64(row, 3)?,
                    sqlite_u64(row, 4)?,
                    sqlite_u64(row, 5)?,
                    sqlite_u64(row, 6)?,
                ))
            },
        )
        .map_err(ReferenceStoreError::sqlite)?;
    if counts.5 != 0
        || counts.0 != u64::try_from(request.surfaces().len()).unwrap_or(u64::MAX)
        || counts.6 != counts.0
    {
        return Err(ReferenceStoreError::InvalidActivationManifest);
    }
    let catalog_counts =
        CatalogCounts::from_spool(counts.0, counts.1, counts.2, counts.3, counts.4);
    let raw = read_database_raw_evidence_from_connection(connection)?;
    let mut raw_object_ids = Vec::new();
    raw_object_ids
        .try_reserve_exact(raw.len())
        .map_err(|_| ReferenceStoreError::CapacityUnavailable)?;
    for object in raw {
        raw_object_ids.push(
            SourceIdentifier::try_from(object.object_id)
                .map_err(|_| ReferenceStoreError::InvalidActivationManifest)?,
        );
    }
    if raw_object_ids.is_empty()
        || raw_object_ids
            .windows(2)
            .any(|pair| pair[0].as_str() >= pair[1].as_str())
    {
        return Err(ReferenceStoreError::InvalidActivationManifest);
    }
    Ok((request, catalog_counts, raw_object_ids))
}

fn parse_surface_closure(value: &str) -> Result<Vec<ReferenceSurface>, ReferenceStoreError> {
    let json: serde_json::Value =
        serde_json::from_str(value).map_err(|_| ReferenceStoreError::InvalidActivationManifest)?;
    let entries = json
        .as_array()
        .ok_or(ReferenceStoreError::InvalidActivationManifest)?;
    if entries.is_empty() || entries.len() > 16 {
        return Err(ReferenceStoreError::InvalidActivationManifest);
    }
    let mut surfaces = Vec::new();
    surfaces
        .try_reserve_exact(entries.len())
        .map_err(|_| ReferenceStoreError::CapacityUnavailable)?;
    for entry in entries {
        let object = entry
            .as_object()
            .ok_or(ReferenceStoreError::InvalidActivationManifest)?;
        let kind = object
            .get("kind")
            .and_then(serde_json::Value::as_str)
            .ok_or(ReferenceStoreError::InvalidActivationManifest)?;
        let surface = match kind {
            "cboe_all_series" if object.len() == 2 => {
                let venue = object
                    .get("venue")
                    .and_then(serde_json::Value::as_str)
                    .ok_or(ReferenceStoreError::InvalidActivationManifest)?;
                ReferenceSurface::CboeAllSeries {
                    venue: CboeVenue::try_from_stable_label(venue)?,
                }
            }
            "occ_dlp_selected_text" if object.len() == 1 => ReferenceSurface::OccDlpSelectedText,
            "occ_dlp_daily_text" if object.len() == 1 => ReferenceSurface::OccDlpDailyText,
            "occ_dlp_daily_xml" if object.len() == 1 => ReferenceSurface::OccDlpDailyXml,
            _ => return Err(ReferenceStoreError::InvalidActivationManifest),
        };
        surfaces.push(surface);
    }
    Ok(surfaces)
}

fn manifest_recovery_identifier(name: &str) -> Result<SourceIdentifier, ReferenceStoreError> {
    let _ = parse_receipt_manifest_name(name)?;
    SourceIdentifier::try_from(format!("options-reference-manifest:{name}"))
        .map_err(|_| ReferenceStoreError::InvalidActivationManifest)
}

fn exact_raw_evidence(
    raw_objects: &[SealedReferenceRawObject],
) -> Result<Vec<RawDatabaseEvidence>, ReferenceStoreError> {
    let mut values = raw_objects
        .iter()
        .map(|raw| RawDatabaseEvidence {
            object_id: raw.context.object_id().as_str().to_owned(),
            digest: raw.context.payload_digest().bytes(),
            bytes: raw.context.payload_bytes(),
        })
        .collect::<Vec<_>>();
    values.sort();
    if values
        .windows(2)
        .any(|pair| pair[0].object_id == pair[1].object_id)
    {
        return Err(ReferenceStoreError::RawGenerationDivergence);
    }
    Ok(values)
}

fn read_database_raw_evidence(
    file: &File,
) -> Result<Vec<RawDatabaseEvidence>, ReferenceStoreError> {
    let connection = open_sqlite_from_descriptor(file)?;
    let mut statement = connection
        .prepare("SELECT object_id,payload_digest,payload_bytes FROM objects ORDER BY object_id")
        .map_err(ReferenceStoreError::sqlite)?;
    let rows = statement
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, Vec<u8>>(1)?,
                row.get::<_, i64>(2)?,
            ))
        })
        .map_err(ReferenceStoreError::sqlite)?;
    let mut values = Vec::new();
    for row in rows {
        if values.len() >= MAX_RAW_OBJECTS_PER_GENERATION {
            return Err(ReferenceStoreError::RawGenerationDivergence);
        }
        let (object_id, digest, bytes) = row.map_err(ReferenceStoreError::sqlite)?;
        values.push(RawDatabaseEvidence {
            object_id,
            digest: digest
                .try_into()
                .map_err(|_| ReferenceStoreError::RawGenerationDivergence)?,
            bytes: u64::try_from(bytes)
                .map_err(|_| ReferenceStoreError::RawGenerationDivergence)?,
        });
    }
    Ok(values)
}

fn validate_generation_receipt(
    receipt: &ReferenceGenerationReceipt,
) -> Result<(), ReferenceStoreError> {
    if receipt.layout_version != STORE_LAYOUT_VERSION
        || receipt.database_digest.algorithm() != DigestAlgorithm::Sha256
        || receipt.database_bytes == 0
        || receipt.database_bytes > receipt.limits.max_database_bytes()
        || !receipt.catalog.publication_eligible()
        || receipt.raw_object_ids.is_empty()
        || receipt.raw_object_ids.len() > MAX_RAW_OBJECTS_PER_GENERATION
    {
        return Err(ReferenceStoreError::InvalidReceipt);
    }
    let digest_hex = hex_digest(receipt.database_digest.bytes());
    if receipt.storage_name.as_str() != format!("{digest_hex}.sqlite")
        || receipt.generation_id.as_str()
            != format!("options-reference-generation:sha256:{digest_hex}")
        || receipt
            .raw_object_ids
            .windows(2)
            .any(|pair| pair[0].as_str() >= pair[1].as_str())
    {
        return Err(ReferenceStoreError::InvalidReceipt);
    }
    Ok(())
}

fn configure_and_validate_read_only(
    connection: &Connection,
    receipt: &ReferenceGenerationReceipt,
) -> Result<Vec<RawDatabaseEvidence>, ReferenceStoreError> {
    let cache_kib = receipt.limits.sqlite_cache_bytes() / 1_024;
    connection
        .execute_batch(&format!(
            "PRAGMA query_only=ON; PRAGMA trusted_schema=OFF; PRAGMA foreign_keys=ON; PRAGMA temp_store=MEMORY; PRAGMA mmap_size=0; PRAGMA cache_size=-{cache_kib};"
        ))
        .map_err(ReferenceStoreError::sqlite)?;
    let user_version: i64 = connection
        .pragma_query_value(None, "user_version", |row| row.get(0))
        .map_err(ReferenceStoreError::sqlite)?;
    let page_size: u64 = connection
        .pragma_query_value(None, "page_size", |row| sqlite_u64(row, 0))
        .map_err(ReferenceStoreError::sqlite)?;
    let page_count: u64 = connection
        .pragma_query_value(None, "page_count", |row| sqlite_u64(row, 0))
        .map_err(ReferenceStoreError::sqlite)?;
    let integrity: String = connection
        .pragma_query_value(None, "integrity_check", |row| row.get(0))
        .map_err(ReferenceStoreError::sqlite)?;
    let foreign_keys: u32 = connection
        .query_row("SELECT COUNT(*) FROM pragma_foreign_key_check", [], |row| {
            row.get(0)
        })
        .map_err(ReferenceStoreError::sqlite)?;
    let mmap_size: u64 = connection
        .pragma_query_value(None, "mmap_size", |row| sqlite_u64(row, 0))
        .map_err(ReferenceStoreError::sqlite)?;
    let cache_size: i64 = connection
        .pragma_query_value(None, "cache_size", |row| row.get(0))
        .map_err(ReferenceStoreError::sqlite)?;
    let expected_cache =
        -i64::try_from(cache_kib).map_err(|_| ReferenceStoreError::InvalidGeneration)?;
    if user_version != SPOOL_SCHEMA_VERSION
        || page_size != 4_096
        || page_count.checked_mul(page_size) != Some(receipt.database_bytes)
        || receipt.database_bytes > receipt.limits.max_database_bytes()
        || integrity != "ok"
        || foreign_keys != 0
        || mmap_size != 0
        || cache_size != expected_cache
    {
        return Err(ReferenceStoreError::InvalidGeneration);
    }
    let mut statement = connection
        .prepare("SELECT type,name FROM sqlite_schema WHERE sql IS NOT NULL ORDER BY type,name")
        .map_err(ReferenceStoreError::sqlite)?;
    let observed = statement
        .query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })
        .map_err(ReferenceStoreError::sqlite)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(ReferenceStoreError::sqlite)?;
    let expected = vec![
        ("index".to_owned(), "cboe_presence_symbol".to_owned()),
        ("table".to_owned(), "cboe_contracts".to_owned()),
        ("table".to_owned(), "cboe_export".to_owned()),
        ("table".to_owned(), "cboe_presence".to_owned()),
        ("table".to_owned(), "conflicts".to_owned()),
        ("table".to_owned(), "generation_metadata".to_owned()),
        ("table".to_owned(), "objects".to_owned()),
        ("table".to_owned(), "occ_export".to_owned()),
        ("table".to_owned(), "occ_products".to_owned()),
        ("table".to_owned(), "pages".to_owned()),
    ];
    if observed != expected {
        return Err(ReferenceStoreError::InvalidGeneration);
    }
    validate_read_only_metadata(connection, receipt)?;
    let objects = read_database_raw_evidence_from_connection(connection)?;
    let object_ids = objects
        .into_iter()
        .map(|object| object.object_id)
        .collect::<Vec<_>>();
    if object_ids
        != receipt
            .raw_object_ids
            .iter()
            .map(|value| value.as_str().to_owned())
            .collect::<Vec<_>>()
    {
        return Err(ReferenceStoreError::RawGenerationDivergence);
    }
    read_database_raw_evidence_from_connection(connection)
}

fn validate_read_only_metadata(
    connection: &Connection,
    receipt: &ReferenceGenerationReceipt,
) -> Result<(), ReferenceStoreError> {
    let request = receipt.catalog.request();
    let surfaces_json = serde_json::to_string(request.surfaces())
        .map_err(|_| ReferenceStoreError::InvalidGeneration)?;
    let schema_digest = sqlite_schema_digest(connection)?;
    let matched: u32 = connection
        .query_row(
            "SELECT COUNT(*) FROM generation_metadata WHERE singleton=1 AND schema_identity='market-squawk-options-reference-v6' AND schema_digest=?1 AND request_id=?2 AND requested_at=?3 AND deadline=?4 AND surfaces_json=?5 AND max_surfaces=?6 AND max_pages=?7 AND max_total_bytes=?8 AND max_total_records=?9 AND max_conflicts=?10 AND max_database_bytes=?11 AND sqlite_cache_bytes=?12 AND max_retained_conflicts=?13 AND max_query_rows=?14",
            params![
                schema_digest.as_slice(),
                request.request_id().as_str(),
                request.requested_at().unix_nanos(),
                request.deadline().unix_nanos(),
                surfaces_json,
                i64::try_from(request.limits().max_surfaces())
                    .map_err(|_| ReferenceStoreError::InvalidGeneration)?,
                i64::from(request.limits().max_pages()),
                i64::try_from(request.limits().max_total_bytes())
                    .map_err(|_| ReferenceStoreError::InvalidGeneration)?,
                i64::try_from(request.limits().max_total_records())
                    .map_err(|_| ReferenceStoreError::InvalidGeneration)?,
                i64::try_from(request.limits().max_conflicts())
                    .map_err(|_| ReferenceStoreError::InvalidGeneration)?,
                i64::try_from(receipt.limits.max_database_bytes())
                    .map_err(|_| ReferenceStoreError::InvalidGeneration)?,
                i64::try_from(receipt.limits.sqlite_cache_bytes())
                    .map_err(|_| ReferenceStoreError::InvalidGeneration)?,
                i64::from(receipt.limits.max_conflicts()),
                i64::from(receipt.limits.max_query_rows().get()),
            ],
            |row| row.get(0),
        )
        .map_err(ReferenceStoreError::sqlite)?;
    if matched != 1 {
        return Err(ReferenceStoreError::InvalidGeneration);
    }

    let counts: (u64, u64, u64, u64, u64, u64) = connection
        .query_row(
            "SELECT (SELECT COUNT(*) FROM pages),(SELECT COUNT(*) FROM objects),(SELECT COALESCE(SUM(payload_bytes),0) FROM objects),(SELECT COALESCE(SUM(returned_records),0) FROM pages),(SELECT COUNT(*) FROM cboe_presence),(SELECT COUNT(*) FROM occ_products)",
            [],
            |row| {
                Ok((
                    sqlite_u64(row, 0)?,
                    sqlite_u64(row, 1)?,
                    sqlite_u64(row, 2)?,
                    sqlite_u64(row, 3)?,
                    sqlite_u64(row, 4)?,
                    sqlite_u64(row, 5)?,
                ))
            },
        )
        .map_err(ReferenceStoreError::sqlite)?;
    let catalog_counts = receipt.catalog.counts();
    let expected_surfaces = u64::try_from(request.surfaces().len())
        .map_err(|_| ReferenceStoreError::InvalidGeneration)?;
    if counts.0 != expected_surfaces
        || counts.1 != expected_surfaces
        || counts.0 != catalog_counts.pages()
        || counts.2 != catalog_counts.bytes()
        || counts.3 != catalog_counts.returned_records()
        || counts.4 != catalog_counts.cboe_series()
        || counts.5 != catalog_counts.occ_dlp_products()
        || catalog_counts.rejected_records() != 0
        || catalog_counts.duplicate_records() != 0
    {
        return Err(ReferenceStoreError::InvalidGeneration);
    }
    let conflict_count: u64 = connection
        .query_row("SELECT COUNT(*) FROM conflicts", [], |row| {
            sqlite_u64(row, 0)
        })
        .map_err(ReferenceStoreError::sqlite)?;
    if conflict_count != 0 {
        return Err(ReferenceStoreError::InvalidGeneration);
    }
    let (cboe_contracts, cboe_export, occ_products, occ_export): (u64, u64, u64, u64) = connection
        .query_row(
            "SELECT (SELECT COUNT(*) FROM cboe_contracts),(SELECT COUNT(*) FROM cboe_export),(SELECT COUNT(*) FROM occ_products),(SELECT COUNT(*) FROM occ_export)",
            [],
            |row| {
                Ok((
                    sqlite_u64(row, 0)?,
                    sqlite_u64(row, 1)?,
                    sqlite_u64(row, 2)?,
                    sqlite_u64(row, 3)?,
                ))
            },
        )
        .map_err(ReferenceStoreError::sqlite)?;
    if cboe_contracts == 0
        || occ_products == 0
        || cboe_contracts != cboe_export
        || occ_products != occ_export
    {
        return Err(ReferenceStoreError::InvalidGeneration);
    }
    validate_export_ordinals(connection, cboe_export, occ_export)?;
    for surface in request.surfaces() {
        let count: u32 = connection
            .query_row(
                "SELECT COUNT(*) FROM pages WHERE surface=?1",
                [surface_key(surface)?],
                |row| row.get(0),
            )
            .map_err(ReferenceStoreError::sqlite)?;
        if count != 1 {
            return Err(ReferenceStoreError::InvalidGeneration);
        }
    }
    Ok(())
}

fn validate_export_ordinals(
    connection: &Connection,
    cboe_count: u64,
    occ_count: u64,
) -> Result<(), ReferenceStoreError> {
    let (cboe_min, cboe_max, cboe_misordered, occ_min, occ_max, occ_misordered): (
        Option<u64>,
        Option<u64>,
        u64,
        Option<u64>,
        Option<u64>,
        u64,
    ) = connection
        .query_row(
            "SELECT (SELECT MIN(export_ordinal) FROM cboe_export),(SELECT MAX(export_ordinal) FROM cboe_export),(SELECT COUNT(*) FROM (SELECT export_ordinal,ROW_NUMBER() OVER (ORDER BY cboe_symbol COLLATE BINARY) expected_ordinal FROM cboe_export) WHERE export_ordinal<>expected_ordinal),(SELECT MIN(export_ordinal) FROM occ_export),(SELECT MAX(export_ordinal) FROM occ_export),(SELECT COUNT(*) FROM (SELECT export_ordinal,ROW_NUMBER() OVER (ORDER BY options_symbol COLLATE BINARY,product_type COLLATE BINARY) expected_ordinal FROM occ_export) WHERE export_ordinal<>expected_ordinal)",
            [],
            |row| {
                Ok((
                    sqlite_optional_u64(row, 0)?,
                    sqlite_optional_u64(row, 1)?,
                    sqlite_u64(row, 2)?,
                    sqlite_optional_u64(row, 3)?,
                    sqlite_optional_u64(row, 4)?,
                    sqlite_u64(row, 5)?,
                ))
            },
        )
        .map_err(ReferenceStoreError::sqlite)?;
    if cboe_min != Some(1)
        || cboe_max != Some(cboe_count)
        || cboe_misordered != 0
        || occ_min != Some(1)
        || occ_max != Some(occ_count)
        || occ_misordered != 0
    {
        return Err(ReferenceStoreError::InvalidGeneration);
    }
    Ok(())
}

fn sqlite_schema_digest(connection: &Connection) -> Result<[u8; 32], ReferenceStoreError> {
    let mut statement = connection
        .prepare("SELECT type,name,sql FROM sqlite_schema WHERE sql IS NOT NULL ORDER BY type,name")
        .map_err(ReferenceStoreError::sqlite)?;
    let mut rows = statement.query([]).map_err(ReferenceStoreError::sqlite)?;
    let mut digest = Sha256::new();
    digest.update(b"market-squawk:options-reference-sqlite-schema:v4\0");
    let mut count = 0_u32;
    while let Some(row) = rows.next().map_err(ReferenceStoreError::sqlite)? {
        count = count
            .checked_add(1)
            .ok_or(ReferenceStoreError::InvalidGeneration)?;
        if count > 10 {
            return Err(ReferenceStoreError::InvalidGeneration);
        }
        for value in [
            row.get::<_, String>(0)
                .map_err(ReferenceStoreError::sqlite)?,
            row.get::<_, String>(1)
                .map_err(ReferenceStoreError::sqlite)?,
            row.get::<_, String>(2)
                .map_err(ReferenceStoreError::sqlite)?,
        ] {
            digest.update(
                u64::try_from(value.len())
                    .map_err(|_| ReferenceStoreError::InvalidGeneration)?
                    .to_be_bytes(),
            );
            digest.update(value.as_bytes());
        }
    }
    if count != 10 {
        return Err(ReferenceStoreError::InvalidGeneration);
    }
    Ok(digest.finalize().into())
}

fn surface_key(surface: &crate::ReferenceSurface) -> Result<String, ReferenceStoreError> {
    let value = match surface {
        crate::ReferenceSurface::CboeAllSeries { venue } => {
            format!("cboe_all_series:{}", venue.stable_label())
        }
        crate::ReferenceSurface::OccDlpSelectedText => "occ_dlp_selected_text".to_owned(),
        crate::ReferenceSurface::OccDlpDailyText => "occ_dlp_daily_text".to_owned(),
        crate::ReferenceSurface::OccDlpDailyXml => "occ_dlp_daily_xml".to_owned(),
        crate::ReferenceSurface::OccMemoIndexCsv => "occ_memo_index_csv".to_owned(),
        crate::ReferenceSurface::OccMemoIndexJson => "occ_memo_index_json".to_owned(),
        crate::ReferenceSurface::OccMemoDocument { memo_number } => {
            format!("occ_memo_document:{memo_number}")
        }
        crate::ReferenceSurface::OccMemoAttachment {
            memo_number,
            ordinal,
        } => format!("occ_memo_attachment:{memo_number}:{}", ordinal.get()),
    };
    if value.len() > 128 {
        Err(ReferenceStoreError::InvalidGeneration)
    } else {
        Ok(value)
    }
}

fn read_database_raw_evidence_from_connection(
    connection: &Connection,
) -> Result<Vec<RawDatabaseEvidence>, ReferenceStoreError> {
    let mut statement = connection
        .prepare("SELECT object_id,payload_digest,payload_bytes FROM objects ORDER BY object_id")
        .map_err(ReferenceStoreError::sqlite)?;
    let rows = statement
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, Vec<u8>>(1)?,
                row.get::<_, i64>(2)?,
            ))
        })
        .map_err(ReferenceStoreError::sqlite)?;
    let mut values = Vec::new();
    for row in rows {
        if values.len() >= MAX_RAW_OBJECTS_PER_GENERATION {
            return Err(ReferenceStoreError::RawGenerationDivergence);
        }
        let (object_id, digest, bytes) = row.map_err(ReferenceStoreError::sqlite)?;
        values.push(RawDatabaseEvidence {
            object_id,
            digest: digest
                .try_into()
                .map_err(|_| ReferenceStoreError::RawGenerationDivergence)?,
            bytes: u64::try_from(bytes)
                .map_err(|_| ReferenceStoreError::RawGenerationDivergence)?,
        });
    }
    Ok(values)
}

fn decode_exchange_codes(
    value: &str,
    state: OccExchangeListingEvidence,
) -> Result<Vec<OccExchangeCode>, ReferenceStoreError> {
    if matches!(
        state,
        OccExchangeListingEvidence::NotReportedInSelectedDirectory
    ) {
        return if value.is_empty() {
            Ok(Vec::new())
        } else {
            Err(ReferenceStoreError::InvalidGeneration)
        };
    }
    if value.is_empty() {
        return Err(ReferenceStoreError::InvalidGeneration);
    }
    let mut result = Vec::new();
    let mut previous = None;
    for byte in value.bytes() {
        if previous.is_some_and(|prior| prior >= byte) {
            return Err(ReferenceStoreError::InvalidGeneration);
        }
        result.push(OccExchangeCode::try_from_byte(byte)?);
        previous = Some(byte);
    }
    Ok(result)
}

fn decode_position_limit(
    state: &str,
    value: &str,
) -> Result<OccPositionLimit, ReferenceStoreError> {
    if value.is_empty()
        || value.len() > 20
        || (value != "0" && value.starts_with('0'))
        || !value.bytes().all(|byte| byte.is_ascii_digit())
    {
        return Err(ReferenceStoreError::InvalidGeneration);
    }
    let parsed = value
        .parse::<u64>()
        .map_err(|_| ReferenceStoreError::InvalidGeneration)?;
    match (state, parsed) {
        ("equity_reported", value) => Ok(OccPositionLimit::EquityReported(
            NonZeroU64::new(value).ok_or(ReferenceStoreError::InvalidGeneration)?,
        )),
        ("non_equity_unavailable_zero", 0) => Ok(OccPositionLimit::NonEquityUnavailableZero),
        ("non_equity_provider_value_outside_documented_scope", value) => Ok(
            OccPositionLimit::NonEquityProviderValueOutsideDocumentedScope {
                raw_value: NonZeroU64::new(value).ok_or(ReferenceStoreError::InvalidGeneration)?,
            },
        ),
        _ => Err(ReferenceStoreError::InvalidGeneration),
    }
}

fn parse_iso_date(value: &str) -> Result<CalendarDate, ReferenceStoreError> {
    if value.len() != 10
        || value.as_bytes().get(4) != Some(&b'-')
        || value.as_bytes().get(7) != Some(&b'-')
        || !value
            .bytes()
            .enumerate()
            .all(|(index, byte)| matches!(index, 4 | 7) || byte.is_ascii_digit())
    {
        return Err(ReferenceStoreError::InvalidGeneration);
    }
    let year = value[0..4]
        .parse::<u16>()
        .map_err(|_| ReferenceStoreError::InvalidGeneration)?;
    let month = value[5..7]
        .parse::<u8>()
        .map_err(|_| ReferenceStoreError::InvalidGeneration)?;
    let day = value[8..10]
        .parse::<u8>()
        .map_err(|_| ReferenceStoreError::InvalidGeneration)?;
    CalendarDate::new(year, month, day).map_err(|_| ReferenceStoreError::InvalidGeneration)
}

fn prepare_capability_child(root: &Dir, name: &str) -> Result<Dir, ReferenceStoreError> {
    match root.create_dir(name) {
        Ok(()) => sync_capability_directory(root)?,
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
        Err(_) => return Err(ReferenceStoreError::StoreIo),
    }
    let child = root
        .open_dir(name)
        .map_err(|_| ReferenceStoreError::UnsafeStoreEntry)?;
    validate_capability_directory(&child)?;
    Ok(child)
}

fn open_capability_lock_file(directory: &Dir) -> Result<File, ReferenceStoreError> {
    let mut options = OpenOptions::new();
    options.read(true).write(true).create(true);
    options.follow(FollowSymlinks::No);
    configure_private_creation(&mut options);
    let file = directory
        .open_with(ACTIVATION_LOCK_FILE, &options)
        .map_err(|_| ReferenceStoreError::StoreIo)?
        .into_std();
    let metadata = file.metadata().map_err(|_| ReferenceStoreError::StoreIo)?;
    let direct = directory
        .symlink_metadata(ACTIVATION_LOCK_FILE)
        .map_err(|_| ReferenceStoreError::StoreIo)?;
    if !metadata.is_file() || !direct.is_file() || metadata.len() != 0 {
        return Err(ReferenceStoreError::UnsafeStoreEntry);
    }
    sync_capability_directory(directory)?;
    Ok(file)
}

fn validate_capability_directory(directory: &Dir) -> Result<(), ReferenceStoreError> {
    let metadata = directory
        .metadata(".")
        .map_err(|_| ReferenceStoreError::StoreIo)?;
    let symlink_metadata = directory
        .symlink_metadata(".")
        .map_err(|_| ReferenceStoreError::StoreIo)?;
    if !metadata.is_dir() || !symlink_metadata.is_dir() {
        Err(ReferenceStoreError::UnsafeStoreEntry)
    } else {
        Ok(())
    }
}

#[cfg(unix)]
fn validate_single_filesystem_layout(directories: [&Dir; 5]) -> Result<(), ReferenceStoreError> {
    let mut devices = directories.iter().map(|directory| {
        directory
            .metadata(".")
            .map(|metadata| cap_fs_ext::MetadataExt::dev(&metadata))
            .map_err(|_| ReferenceStoreError::StoreIo)
    });
    let expected = devices
        .next()
        .ok_or(ReferenceStoreError::UnsafeStoreEntry)??;
    if devices.any(|device| !matches!(device, Ok(observed) if observed == expected)) {
        Err(ReferenceStoreError::UnsafeStoreEntry)
    } else {
        Ok(())
    }
}

#[cfg(not(unix))]
fn validate_single_filesystem_layout(_directories: [&Dir; 5]) -> Result<(), ReferenceStoreError> {
    Ok(())
}

#[cfg(unix)]
fn ensure_capability_disk_capacity(
    directory: &Dir,
    required: u64,
) -> Result<(), ReferenceStoreError> {
    let filesystem = rustix::fs::fstatvfs(directory).map_err(|_| ReferenceStoreError::StoreIo)?;
    let available = filesystem
        .f_frsize
        .checked_mul(filesystem.f_bavail)
        .ok_or(ReferenceStoreError::StoreIo)?;
    if available < required {
        Err(ReferenceStoreError::InsufficientDisk {
            required,
            available,
        })
    } else {
        Ok(())
    }
}

#[cfg(not(unix))]
fn ensure_capability_disk_capacity(
    _directory: &Dir,
    _required: u64,
) -> Result<(), ReferenceStoreError> {
    Err(ReferenceStoreError::CapabilityDatabaseUnavailable)
}

fn create_capability_staging(
    directory: &Dir,
    prefix: &str,
) -> Result<(String, File), ReferenceStoreError> {
    let name = format!("{prefix}{}.tmp", Uuid::new_v4());
    let mut options = OpenOptions::new();
    options.read(true).write(true).create_new(true);
    options.follow(FollowSymlinks::No);
    configure_private_creation(&mut options);
    let file = directory
        .open_with(&name, &options)
        .map_err(|_| ReferenceStoreError::StoreIo)?
        .into_std();
    Ok((name, file))
}

fn probe_capability_directory(directory: &Dir) -> Result<(), ReferenceStoreError> {
    const PROBE: &[u8] = b"market-squawk-options-reference-doctor-v1";
    let (name, mut file) = create_capability_staging(directory, DOCTOR_PROBE_PREFIX)?;
    let cleanup = CapabilityStagingCleanup {
        directory,
        name: &name,
        armed: true,
    };
    file.write_all(PROBE)
        .map_err(|_| ReferenceStoreError::StoreIo)?;
    file.sync_all().map_err(|_| ReferenceStoreError::StoreIo)?;
    file.seek(SeekFrom::Start(0))
        .map_err(|_| ReferenceStoreError::StoreIo)?;
    let mut observed = [0_u8; PROBE.len()];
    file.read_exact(&mut observed)
        .map_err(|_| ReferenceStoreError::StoreIo)?;
    if observed.as_slice() != PROBE {
        return Err(ReferenceStoreError::ObjectCorrupt);
    }
    drop(file);
    directory
        .remove_file(&name)
        .map_err(|_| ReferenceStoreError::StoreIo)?;
    sync_capability_directory(directory)?;
    drop(cleanup);
    Ok(())
}

fn publish_capability_no_replace(
    staging_directory: &Dir,
    staged_name: &str,
    target_directory: &Dir,
    target_name: &str,
    expected_digest: [u8; 32],
    expected_bytes: u64,
) -> Result<(), ReferenceStoreError> {
    seal_capability_file_read_only(staging_directory, staged_name)?;
    let published = match staging_directory.hard_link(staged_name, target_directory, target_name) {
        Ok(()) => {
            sync_capability_directory(target_directory)?;
            true
        }
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            let _existing = validate_capability_content_file(
                target_directory,
                target_name,
                expected_digest,
                expected_bytes,
                expected_bytes,
            )?;
            false
        }
        Err(_) => return Err(ReferenceStoreError::StoreIo),
    };
    staging_directory
        .remove_file(staged_name)
        .map_err(|_| ReferenceStoreError::StoreIo)?;
    sync_capability_directory(staging_directory)?;
    if published {
        let _published = validate_capability_content_file(
            target_directory,
            target_name,
            expected_digest,
            expected_bytes,
            expected_bytes,
        )?;
    }
    Ok(())
}

fn copy_bounded_file(
    source: &File,
    destination: &mut File,
    expected_bytes: u64,
    maximum_bytes: u64,
    mut checkpoint: impl FnMut() -> Result<(), ReferenceStoreError>,
) -> Result<(), ReferenceStoreError> {
    checkpoint()?;
    let metadata = source
        .metadata()
        .map_err(|_| ReferenceStoreError::StoreIo)?;
    if !metadata.is_file()
        || expected_bytes == 0
        || expected_bytes > maximum_bytes
        || metadata.len() != expected_bytes
    {
        return Err(ReferenceStoreError::InvalidGeneration);
    }
    let mut input = source
        .try_clone()
        .map_err(|_| ReferenceStoreError::StoreIo)?;
    input
        .seek(SeekFrom::Start(0))
        .map_err(|_| ReferenceStoreError::StoreIo)?;
    let mut copied = 0_u64;
    let mut buffer = [0_u8; 128 * 1024];
    loop {
        checkpoint()?;
        let read = input
            .read(&mut buffer)
            .map_err(|_| ReferenceStoreError::StoreIo)?;
        if read == 0 {
            break;
        }
        copied = copied
            .checked_add(u64::try_from(read).map_err(|_| ReferenceStoreError::InvalidGeneration)?)
            .ok_or(ReferenceStoreError::InvalidGeneration)?;
        if copied > maximum_bytes {
            return Err(ReferenceStoreError::InvalidGeneration);
        }
        destination
            .write_all(&buffer[..read])
            .map_err(|_| ReferenceStoreError::StoreIo)?;
    }
    if copied != expected_bytes {
        return Err(ReferenceStoreError::InvalidGeneration);
    }
    checkpoint()?;
    destination
        .seek(SeekFrom::Start(0))
        .map_err(|_| ReferenceStoreError::StoreIo)?;
    Ok(())
}

fn validate_capability_content_file(
    directory: &Dir,
    name: &str,
    expected_digest: [u8; 32],
    expected_bytes: u64,
    maximum_bytes: u64,
) -> Result<ImmutableFileEvidence, ReferenceStoreError> {
    let mut options = OpenOptions::new();
    options.read(true);
    options.follow(FollowSymlinks::No);
    let capability_file = match directory.open_with(name, &options) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Err(ReferenceStoreError::ObjectMissing);
        }
        Err(_) => return Err(ReferenceStoreError::StoreIo),
    };
    let metadata = capability_file
        .metadata()
        .map_err(|_| ReferenceStoreError::StoreIo)?;
    let path_metadata = directory
        .symlink_metadata(name)
        .map_err(|_| ReferenceStoreError::StoreIo)?;
    if !metadata.is_file()
        || !path_metadata.is_file()
        || !metadata.permissions().readonly()
        || !path_metadata.permissions().readonly()
        || metadata.len() != expected_bytes
        || expected_bytes == 0
        || expected_bytes > maximum_bytes
    {
        return Err(ReferenceStoreError::ObjectCorrupt);
    }
    let mut file = capability_file.into_std();
    let fingerprint = CapabilityFileFingerprint::capture(&file)?;
    #[cfg(unix)]
    if fingerprint.links != 1 {
        return Err(ReferenceStoreError::ObjectCorrupt);
    }
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 128 * 1024];
    let mut total = 0_u64;
    loop {
        let read = file
            .read(&mut buffer)
            .map_err(|_| ReferenceStoreError::StoreIo)?;
        if read == 0 {
            break;
        }
        total = total
            .checked_add(u64::try_from(read).map_err(|_| ReferenceStoreError::ObjectCorrupt)?)
            .ok_or(ReferenceStoreError::ObjectCorrupt)?;
        if total > maximum_bytes {
            return Err(ReferenceStoreError::ObjectCorrupt);
        }
        hasher.update(&buffer[..read]);
    }
    if total != expected_bytes
        || <[u8; 32]>::from(hasher.finalize()) != expected_digest
        || CapabilityFileFingerprint::capture(&file)? != fingerprint
    {
        return Err(ReferenceStoreError::ObjectCorrupt);
    }
    file.seek(SeekFrom::Start(0))
        .map_err(|_| ReferenceStoreError::StoreIo)?;
    Ok(ImmutableFileEvidence { file, fingerprint })
}

fn seal_capability_file_read_only(directory: &Dir, name: &str) -> Result<(), ReferenceStoreError> {
    let mut options = OpenOptions::new();
    options.read(true).write(true);
    options.follow(FollowSymlinks::No);
    let file = directory
        .open_with(name, &options)
        .map_err(|_| ReferenceStoreError::StoreIo)?
        .into_std();
    let metadata = file.metadata().map_err(|_| ReferenceStoreError::StoreIo)?;
    let direct = directory
        .symlink_metadata(name)
        .map_err(|_| ReferenceStoreError::StoreIo)?;
    if !metadata.is_file() || !direct.is_file() {
        return Err(ReferenceStoreError::UnsafeStoreEntry);
    }
    let mut permissions = metadata.permissions();
    permissions.set_readonly(true);
    file.set_permissions(permissions)
        .map_err(|_| ReferenceStoreError::StoreIo)?;
    file.sync_all().map_err(|_| ReferenceStoreError::StoreIo)?;
    if !file
        .metadata()
        .map_err(|_| ReferenceStoreError::StoreIo)?
        .permissions()
        .readonly()
    {
        return Err(ReferenceStoreError::ObjectCorrupt);
    }
    Ok(())
}

fn reject_capability_sqlite_sidecars(
    directory: &Dir,
    name: &str,
) -> Result<(), ReferenceStoreError> {
    for suffix in ["-journal", "-wal", "-shm"] {
        match directory.symlink_metadata(format!("{name}{suffix}")) {
            Ok(_) => return Err(ReferenceStoreError::InvalidGeneration),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(_) => return Err(ReferenceStoreError::StoreIo),
        }
    }
    Ok(())
}

#[cfg(unix)]
fn sync_capability_directory(directory: &Dir) -> Result<(), ReferenceStoreError> {
    use cap_std::fs::OpenOptionsExt as _;

    let mut options = OpenOptions::new();
    options.read(true);
    options.follow(FollowSymlinks::No);
    options.custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC);
    directory
        .open_with(".", &options)
        .map(cap_std::fs::File::into_std)
        .and_then(|file| file.sync_all())
        .map_err(|_| ReferenceStoreError::StoreIo)
}

#[cfg(not(unix))]
fn sync_capability_directory(directory: &Dir) -> Result<(), ReferenceStoreError> {
    directory
        .try_clone()
        .and_then(|directory| directory.into_std_file().sync_all())
        .map_err(|_| ReferenceStoreError::StoreIo)
}

#[cfg(unix)]
fn open_sqlite_from_descriptor(file: &File) -> Result<Connection, ReferenceStoreError> {
    use std::os::fd::AsRawFd as _;

    let locator = format!("file:/dev/fd/{}?mode=ro&immutable=1", file.as_raw_fd());
    Connection::open_with_flags(
        locator,
        OpenFlags::SQLITE_OPEN_READ_ONLY
            | OpenFlags::SQLITE_OPEN_NO_MUTEX
            | OpenFlags::SQLITE_OPEN_URI,
    )
    .map_err(ReferenceStoreError::sqlite)
}

#[cfg(not(unix))]
fn open_sqlite_from_descriptor(_file: &File) -> Result<Connection, ReferenceStoreError> {
    Err(ReferenceStoreError::CapabilityDatabaseUnavailable)
}

#[cfg(unix)]
fn configure_private_creation(options: &mut OpenOptions) {
    use cap_std::fs::OpenOptionsExt as _;
    options.mode(0o600);
}

#[cfg(not(unix))]
fn configure_private_creation(_options: &mut OpenOptions) {}

fn hash_bytes(bytes: &[u8]) -> [u8; 32] {
    Sha256::digest(bytes).into()
}

fn hex_digest(digest: [u8; 32]) -> String {
    let mut output = String::with_capacity(64);
    for byte in digest {
        use std::fmt::Write as _;
        let _ = write!(&mut output, "{byte:02x}");
    }
    output
}

fn sqlite_u64(row: &rusqlite::Row<'_>, index: usize) -> rusqlite::Result<u64> {
    let value = row.get::<_, i64>(index)?;
    u64::try_from(value).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(
            index,
            rusqlite::types::Type::Integer,
            Box::new(error),
        )
    })
}

fn sqlite_optional_u64(row: &rusqlite::Row<'_>, index: usize) -> rusqlite::Result<Option<u64>> {
    row.get::<_, Option<i64>>(index)?
        .map(u64::try_from)
        .transpose()
        .map_err(|error| {
            rusqlite::Error::FromSqlConversionFailure(
                index,
                rusqlite::types::Type::Integer,
                Box::new(error),
            )
        })
}

/// Durable raw, generation, recovery, or typed-query refusal.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum ReferenceStoreError {
    /// The caller cancelled generation publication before atomic activation.
    #[error("option-reference generation publication was cancelled")]
    PublicationCancelled,
    /// The publication deadline elapsed before atomic activation.
    #[error("option-reference generation publication deadline elapsed")]
    PublicationDeadlineExceeded,
    /// Trusted publication-control evidence was unavailable.
    #[error("option-reference generation publication control is unavailable")]
    PublicationControlUnavailable,
    /// Store root or a child entry was a symlink, wrong type, or otherwise unsafe.
    #[error("unsafe option-reference store entry")]
    UnsafeStoreEntry,
    /// Bounded local file or directory I/O failed.
    #[error("option-reference store I/O failed")]
    StoreIo,
    /// Raw bytes and transport/context evidence diverged.
    #[error("option-reference raw-object evidence mismatch")]
    RawEvidenceMismatch,
    /// One explicitly bounded parser handoff allocation was unavailable.
    #[error("option-reference bounded allocation unavailable")]
    CapacityUnavailable,
    /// The store filesystem cannot retain the exact admitted object plus safety reserve.
    #[error("option-reference store needs {required} bytes but only {available} are available")]
    InsufficientDisk {
        /// Required free bytes at the operation boundary.
        required: u64,
        /// Measured filesystem-available bytes.
        available: u64,
    },
    /// A durable receipt was malformed or internally inconsistent.
    #[error("invalid option-reference artifact receipt")]
    InvalidReceipt,
    /// A receipted content-addressed object was absent.
    #[error("option-reference artifact is missing")]
    ObjectMissing,
    /// A receipted object had wrong type, size, or digest.
    #[error("option-reference artifact is corrupt")]
    ObjectCorrupt,
    /// Staged database and exact raw-object closure differed.
    #[error("option-reference generation raw-object closure diverged")]
    RawGenerationDivergence,
    /// Generation schema, integrity, catalog, or object content was invalid.
    #[error("invalid option-reference generation")]
    InvalidGeneration,
    /// SQLite refused an operation without exposing local/provider details.
    #[error("option-reference generation database operation failed")]
    Sqlite,
    /// Exact typed query input was invalid.
    #[error("invalid exact option-reference query")]
    InvalidQuery,
    /// Query results exceeded the frozen bounded seam.
    #[error("option-reference query row limit exceeded")]
    QueryLimitExceeded,
    /// Recovery scan/receipt bounds were exceeded.
    #[error("option-reference recovery bound exceeded")]
    RecoveryLimitExceeded,
    /// Durable receipt, activation index, or current-pointer evidence was malformed or divergent.
    #[error("invalid option-reference activation manifest")]
    InvalidActivationManifest,
    /// Another process currently owns the provider-local activation transaction.
    #[error("option-reference activation is busy")]
    ActivationBusy,
    /// Another process currently owns the provider-local bounded publication spool.
    #[error("option-reference publication staging is busy")]
    StagingBusy,
    /// This platform lacks the descriptor-backed immutable SQLite reopen used by the capability
    /// store; ambient-path fallback is deliberately forbidden.
    #[error("capability-backed option-reference database reopen is unavailable")]
    CapabilityDatabaseUnavailable,
    /// Cboe provider reference could not be reconstructed.
    #[error("invalid Cboe data in sealed option-reference generation")]
    Cboe,
    /// OCC provider reference could not be reconstructed.
    #[error("invalid OCC data in sealed option-reference generation")]
    Occ,
}

impl ReferenceStoreError {
    fn sqlite(_: rusqlite::Error) -> Self {
        Self::Sqlite
    }
}

impl From<crate::CboeParseError> for ReferenceStoreError {
    fn from(_: crate::CboeParseError) -> Self {
        Self::Cboe
    }
}

impl From<crate::OccParseError> for ReferenceStoreError {
    fn from(_: crate::OccParseError) -> Self {
        Self::Occ
    }
}

impl From<crate::ReferenceSpoolError> for ReferenceStoreError {
    fn from(error: crate::ReferenceSpoolError) -> Self {
        match error {
            crate::ReferenceSpoolError::PublicationCancelled => Self::PublicationCancelled,
            crate::ReferenceSpoolError::PublicationDeadlineExceeded => {
                Self::PublicationDeadlineExceeded
            }
            crate::ReferenceSpoolError::PublicationControlUnavailable => {
                Self::PublicationControlUnavailable
            }
            crate::ReferenceSpoolError::StagingBusy => Self::StagingBusy,
            crate::ReferenceSpoolError::UnsafeStagingDirectory
            | crate::ReferenceSpoolError::UnsafeStagingInventory => Self::UnsafeStoreEntry,
            crate::ReferenceSpoolError::DiskProbeFailed | crate::ReferenceSpoolError::StagingIo => {
                Self::StoreIo
            }
            crate::ReferenceSpoolError::InsufficientDisk {
                required,
                available,
            } => Self::InsufficientDisk {
                required,
                available,
            },
            crate::ReferenceSpoolError::CapacityUnavailable => Self::CapacityUnavailable,
            crate::ReferenceSpoolError::CapabilityDatabaseUnavailable => {
                Self::CapabilityDatabaseUnavailable
            }
            crate::ReferenceSpoolError::Sqlite => Self::Sqlite,
            crate::ReferenceSpoolError::Cboe(_) => Self::Cboe,
            crate::ReferenceSpoolError::Occ(_) => Self::Occ,
            crate::ReferenceSpoolError::InvalidLimits
            | crate::ReferenceSpoolError::InvalidSurfaceState
            | crate::ReferenceSpoolError::PageReceiptMismatch
            | crate::ReferenceSpoolError::IncompletePublication
            | crate::ReferenceSpoolError::UnsupportedPublicationSurface
            | crate::ReferenceSpoolError::InvalidPublicationCycle
            | crate::ReferenceSpoolError::PublicationLimitExceeded
            | crate::ReferenceSpoolError::ConflictedPublication
            | crate::ReferenceSpoolError::ConflictLimitExceeded
            | crate::ReferenceSpoolError::DatabaseLimitExceeded
            | crate::ReferenceSpoolError::InvalidSealedDatabase
            | crate::ReferenceSpoolError::CountOverflow
            | crate::ReferenceSpoolError::EncodingFailed
            | crate::ReferenceSpoolError::AlreadySealed => Self::InvalidGeneration,
        }
    }
}
