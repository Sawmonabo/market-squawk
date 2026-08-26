//! Durable lossless publication and typed queries for every official SEC bulk table family.

use std::collections::BTreeMap;
use std::fmt;
use std::io::{Read, Seek, SeekFrom, Write};

use market_squawk_domain::{DigestAlgorithm, EvidenceDigest, SourceIdentifier, Timestamp};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use tokio_util::sync::CancellationToken;

use crate::evidence_store::{RawEvidenceContentWriter, RawEvidenceReceipt, RawEvidenceScratch};
use crate::{RawEvidenceStore, SecHttpValidators, SecObjectLocator};

use super::model::{
    SEC_BULK_CATALOG_SNAPSHOT_DATE, SecBulkCapture, SecBulkCatalogSnapshot, SecBulkCoverage,
    SecBulkFamily, SecBulkJoinCoordinate, SecBulkJoinDomain, SecBulkKeyField,
    SecBulkLayoutManifest, SecBulkMediaKind, SecBulkNativeRow, SecBulkNativeRowMembership,
    SecBulkNumericAttribute, SecBulkRelatedTableRows, SecBulkSchemaIdentity, SecBulkTableKind,
    SecBulkTablePresence, SecBulkTransportEvidence, SecBulkTypedField, SecBulkTypedValue,
    SecNportHoldingSupplementSet, SecQuarter, nport_holding_supplement_tables,
};
use super::{
    SecBulkError, SecBulkQueryCompleteness, SecBulkQueryLimits, SecBulkRowSink, SecBulkScanReport,
};

const NATIVE_DATA_MAGIC: &[u8] = b"MSSEC-NATIVE-v2\n";
const NATIVE_INDEX_MAGIC: &[u8] = b"MSSEC-NIDX-v1\n";
const NATIVE_LOOKUP_MAGIC: &[u8] = b"MSSEC-NLOOK-v1\n";
const NATIVE_ROOT_VERSION: u8 = 4;
const NATIVE_KEY_BYTES: usize = 42;
const NATIVE_INDEX_ENTRY_BYTES: usize = NATIVE_KEY_BYTES + 8 + 4 + 32;
const NATIVE_INDEX_PAGE_RECORDS: u32 = 4_096;
const NATIVE_LOOKUP_PAGE_RECORDS: u32 = 4_096;
const NATIVE_LOOKUP_BUCKETS: u16 = 4_096;
const MAX_OPEN_LOOKUP_SCRATCH: usize = 16;
const NATIVE_LOOKUP_ENTRY_BYTES: usize = 2 + 1 + 32 + 8 + 4 + 32;
const MAX_NATIVE_LOOKUP_SCRATCH_BYTES: u64 = 256 * 1024 * 1024 * 1024;
const MAX_NATIVE_RECORD_BYTES: usize = 4 * 1024 * 1024;
const MAX_NATIVE_PUBLICATION_ROWS: u64 = 250_000_000;
const MAX_NATIVE_DATA_BYTES: u64 = 1024 * 1024 * 1024 * 1024;
const MAX_NATIVE_ROOT_BYTES: u64 = 128 * 1024 * 1024;
const MAX_NATIVE_QUERY_RESPONSE_BYTES: u64 = 64 * 1024 * 1024;
const MAX_HOLDING_SUPPLEMENT_ROWS: u64 = 100_000;
const MAX_HOLDING_SUPPLEMENT_RESPONSE_BYTES: u64 = 64 * 1024 * 1024;
const MAX_BOUND_ARCHIVE_BYTES: u64 = 1024 * 1024 * 1024;
const MAX_BOUND_README_BYTES: u64 = 16 * 1024 * 1024;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct NativeIndexPageDescriptor {
    evidence: EvidenceDigest,
    size_bytes: u64,
    record_count: u32,
    first_key: Vec<u8>,
    last_key: Vec<u8>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct NativeLookupPageDescriptor {
    evidence: EvidenceDigest,
    size_bytes: u64,
    record_count: u32,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct NativeLookupBucketDescriptor {
    bucket: u16,
    record_count: u64,
    records_evidence: EvidenceDigest,
    pages: Vec<NativeLookupPageDescriptor>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct NativeTableDescriptor {
    table: SecBulkTableKind,
    evidence: Option<EvidenceDigest>,
    row_count: u64,
    declared_absent: bool,
    primary_key: Vec<String>,
    columns: Vec<NativeColumnDescriptor>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct NativeColumnDescriptor {
    name: String,
    datatype_base: String,
    max_length: Option<u64>,
    data_precision: Option<SecBulkNumericAttribute>,
    data_scale: Option<SecBulkNumericAttribute>,
    required: bool,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct NativeTransportLineage {
    locator: String,
    http_status: u16,
    media_type: Option<String>,
    etag: Option<String>,
    last_modified_header: Option<String>,
    last_modified_at_unix_nanos: Option<i64>,
    body_received_at_unix_nanos: i64,
    first_observed_at_unix_nanos: i64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct NativeGenerationManifest {
    version: u8,
    family: SecBulkFamily,
    quarter_year: u16,
    quarter_number: u8,
    catalog_snapshot_date: String,
    manifest_evidence: EvidenceDigest,
    archive_evidence: EvidenceDigest,
    archive_size_bytes: u64,
    archive_retrieval_revision: u64,
    archive_transport: NativeTransportLineage,
    readme_evidence: EvidenceDigest,
    readme_size_bytes: u64,
    readme_retrieval_revision: u64,
    readme_transport: NativeTransportLineage,
    metadata_evidence: EvidenceDigest,
    archive_readme_evidence: EvidenceDigest,
    accepted_schema_version: String,
    accepted_schema_effective_date: String,
    accepted_schema_locator: String,
    declared_coverage_gap: bool,
    tables: Vec<NativeTableDescriptor>,
    source_rows: u64,
    emitted_rows: u64,
    data_evidence: EvidenceDigest,
    data_size_bytes: u64,
    index_pages: Vec<NativeIndexPageDescriptor>,
    lookup_records: u64,
    lookup_buckets: Vec<NativeLookupBucketDescriptor>,
    records_evidence: EvidenceDigest,
    published_at_unix_nanos: i64,
}

/// Immutable all-table native generation returned only after full restart verification.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SecBulkNativePublishedGeneration {
    root_evidence: EvidenceDigest,
    root_size_bytes: u64,
    manifest: NativeGenerationManifest,
}

/// Serializable cold-restart coordinate for one immutable native generation root.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SecBulkNativeGenerationReceipt {
    root_evidence: EvidenceDigest,
    root_size_bytes: u64,
}

impl SecBulkNativeGenerationReceipt {
    /// Returns the exact content-addressed root identity.
    pub const fn root_evidence(&self) -> EvidenceDigest {
        self.root_evidence
    }

    /// Returns the exact serialized root byte length.
    pub const fn root_size_bytes(&self) -> u64 {
        self.root_size_bytes
    }
}

impl SecBulkNativePublishedGeneration {
    /// Returns exact family.
    pub const fn family(&self) -> SecBulkFamily {
        self.manifest.family
    }

    /// Returns exact layout identity.
    pub const fn manifest_evidence(&self) -> EvidenceDigest {
        self.manifest.manifest_evidence
    }

    /// Returns atomic root identity.
    pub const fn evidence(&self) -> EvidenceDigest {
        self.root_evidence
    }

    /// Returns the minimal serializable receipt required for a process-cold recovery.
    pub const fn receipt(&self) -> SecBulkNativeGenerationReceipt {
        SecBulkNativeGenerationReceipt {
            root_evidence: self.root_evidence,
            root_size_bytes: self.root_size_bytes,
        }
    }

    /// Returns exact published row count.
    pub const fn row_count(&self) -> u64 {
        self.manifest.emitted_rows
    }

    /// Returns trusted local publication clock.
    pub const fn published_at(&self) -> Timestamp {
        Timestamp::from_unix_nanos(self.manifest.published_at_unix_nanos)
    }

    pub(crate) const fn archive_evidence(&self) -> EvidenceDigest {
        self.manifest.archive_evidence
    }

    pub(crate) const fn archive_size_bytes(&self) -> u64 {
        self.manifest.archive_size_bytes
    }

    pub(crate) const fn archive_retrieval_revision(&self) -> u64 {
        self.manifest.archive_retrieval_revision
    }

    pub(crate) const fn readme_evidence(&self) -> EvidenceDigest {
        self.manifest.readme_evidence
    }

    pub(crate) const fn readme_size_bytes(&self) -> u64 {
        self.manifest.readme_size_bytes
    }

    pub(crate) const fn readme_retrieval_revision(&self) -> u64 {
        self.manifest.readme_retrieval_revision
    }

    pub(crate) const fn metadata_evidence(&self) -> EvidenceDigest {
        self.manifest.metadata_evidence
    }

    pub(crate) const fn archive_readme_evidence(&self) -> EvidenceDigest {
        self.manifest.archive_readme_evidence
    }
}

/// Continuation bound to one native generation, table, and exact primary-key filter.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SecBulkNativeQueryCursor {
    generation_evidence: EvidenceDigest,
    query_evidence: EvidenceDigest,
    rows_to_skip: u64,
}

/// One exact provider-native join filter used to traverse related SEC tables.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SecBulkNativeJoinFilter {
    domain: SecBulkJoinDomain,
    value: String,
}

impl SecBulkNativeJoinFilter {
    /// Constructs a bounded non-empty provider-native join coordinate.
    pub fn try_new(domain: SecBulkJoinDomain, value: &str) -> Result<Self, SecBulkError> {
        if value.is_empty() || value.len() > 1024 * 1024 || value.contains('\0') {
            return Err(SecBulkError::InvalidCanonicalMapping);
        }
        Ok(Self {
            domain,
            value: value.to_owned(),
        })
    }

    /// Returns the closed provider-native relationship domain.
    pub const fn domain(&self) -> SecBulkJoinDomain {
        self.domain
    }

    /// Returns the exact source lexical relationship value.
    pub fn value(&self) -> &str {
        &self.value
    }
}

#[derive(Clone, Copy)]
enum NativeQueryPredicate<'a> {
    All,
    PrimaryKey(&'a [SecBulkKeyField]),
    Joins(&'a [SecBulkNativeJoinFilter]),
}

/// Bounded typed all-table query page.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SecBulkNativeQueryPage {
    generation_evidence: EvidenceDigest,
    query_evidence: EvidenceDigest,
    table_presence: SecBulkTablePresence,
    completeness: SecBulkQueryCompleteness,
    rows: Vec<SecBulkNativeRow>,
    next_cursor: Option<SecBulkNativeQueryCursor>,
    scanned_rows: u64,
}

impl SecBulkNativeQueryPage {
    /// Returns exact immutable generation identity.
    pub const fn generation_evidence(&self) -> EvidenceDigest {
        self.generation_evidence
    }

    /// Returns exact query identity.
    pub const fn query_evidence(&self) -> EvidenceDigest {
        self.query_evidence
    }

    /// Returns present-rows/present-empty/declared-absent state.
    pub const fn table_presence(&self) -> SecBulkTablePresence {
        self.table_presence
    }

    /// Returns exact/unavailable coverage state.
    pub const fn completeness(&self) -> SecBulkQueryCompleteness {
        self.completeness
    }

    /// Returns lossless metadata-typed provider rows.
    pub fn rows(&self) -> &[SecBulkNativeRow] {
        &self.rows
    }

    /// Returns a generation/query-bound continuation.
    pub const fn next_cursor(&self) -> Option<SecBulkNativeQueryCursor> {
        self.next_cursor
    }

    /// Returns index rows examined.
    pub const fn scanned_rows(&self) -> u64 {
        self.scanned_rows
    }
}

struct LookupDigestState {
    digest: Sha256,
    records: u64,
}

impl LookupDigestState {
    fn new(bucket: u16) -> Self {
        let mut digest = Sha256::new();
        digest.update(b"market-squawk/sec-bulk-native-lookup-bucket/v1");
        hash_field(&mut digest, &bucket.to_be_bytes());
        Self { digest, records: 0 }
    }

    fn observe(&mut self, entry: &NativeLookupEntry) -> Result<(), SecBulkError> {
        hash_field(&mut self.digest, &entry.encode());
        self.records = self
            .records
            .checked_add(1)
            .ok_or(SecBulkError::QueryLimitExceeded)?;
        Ok(())
    }

    fn evidence(&self) -> EvidenceDigest {
        EvidenceDigest::new(
            DigestAlgorithm::Sha256,
            self.digest.clone().finalize().into(),
        )
    }
}

struct NativeLookupScratch<'a> {
    scratch: RawEvidenceScratch<'a>,
    state: LookupDigestState,
    bytes: u64,
    descriptor_open: bool,
    last_used: u64,
}

/// Atomic sink that turns one exact all-table archive scan into a durable native generation.
pub struct SecBulkNativePublicationSession<'a> {
    store: &'a RawEvidenceStore,
    layout: SecBulkLayoutManifest,
    published_at: Timestamp,
    deadline: Timestamp,
    cancellation: CancellationToken,
    data: Option<RawEvidenceContentWriter<'a>>,
    index_page: Vec<u8>,
    index_page_records: u32,
    index_pages: Vec<NativeIndexPageDescriptor>,
    lookup_scratch: BTreeMap<u16, NativeLookupScratch<'a>>,
    lookup_buckets: Vec<NativeLookupBucketDescriptor>,
    lookup_scratch_bytes: u64,
    lookup_records: u64,
    lookup_open_descriptors: usize,
    lookup_clock: u64,
    previous_key: Option<[u8; NATIVE_KEY_BYTES]>,
    records_digest: Sha256,
    staged_rows: u64,
    begun: bool,
    generation: Option<SecBulkNativePublishedGeneration>,
}

impl fmt::Debug for SecBulkNativePublicationSession<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SecBulkNativePublicationSession")
            .field("manifest_evidence", &self.layout.evidence())
            .field("published_at", &self.published_at)
            .field("staged_rows", &self.staged_rows)
            .field("begun", &self.begun)
            .field("sealed", &self.generation.is_some())
            .finish_non_exhaustive()
    }
}

impl<'a> SecBulkNativePublicationSession<'a> {
    /// Creates a non-visible sink for one exact layout and absolute deadline.
    pub fn new(
        store: &'a RawEvidenceStore,
        layout: SecBulkLayoutManifest,
        published_at: Timestamp,
        deadline: Timestamp,
        cancellation: CancellationToken,
    ) -> Result<Self, SecBulkError> {
        check_operation(deadline, &cancellation)?;
        let now = crate::client::system_timestamp()?;
        if published_at < layout.capture().first_observed_at()
            || published_at > now
            || published_at > deadline
        {
            return Err(SecBulkError::PublicationNotReady);
        }
        if layout
            .tables()
            .iter()
            .try_fold(0_u64, |total, table| total.checked_add(table.row_count()))
            .is_none_or(|rows| rows > MAX_NATIVE_PUBLICATION_ROWS)
        {
            return Err(SecBulkError::QueryLimitExceeded);
        }
        if projected_lookup_scratch_bytes(&layout)? > MAX_NATIVE_LOOKUP_SCRATCH_BYTES {
            return Err(SecBulkError::ScratchLimitExceeded);
        }
        let mut data =
            store.create_content_writer(MAX_NATIVE_DATA_BYTES, deadline, &cancellation)?;
        data.write_bytes(NATIVE_DATA_MAGIC, &cancellation)?;
        let mut index_page = Vec::new();
        index_page
            .try_reserve_exact(
                NATIVE_INDEX_MAGIC.len()
                    + 4
                    + NATIVE_INDEX_ENTRY_BYTES * NATIVE_INDEX_PAGE_RECORDS as usize,
            )
            .map_err(|_| SecBulkError::AllocationFailed)?;
        start_index_page(&mut index_page);
        let records_digest = records_digest_prefix(layout.evidence());
        Ok(Self {
            store,
            layout,
            published_at,
            deadline,
            cancellation,
            data: Some(data),
            index_page,
            index_page_records: 0,
            index_pages: Vec::new(),
            lookup_scratch: BTreeMap::new(),
            lookup_buckets: Vec::new(),
            lookup_scratch_bytes: 0,
            lookup_records: 0,
            lookup_open_descriptors: 0,
            lookup_clock: 0,
            previous_key: None,
            records_digest,
            staged_rows: 0,
            begun: false,
            generation: None,
        })
    }

    /// Returns the fully recovered generation only after the scanner committed successfully.
    pub const fn published_generation(&self) -> Option<&SecBulkNativePublishedGeneration> {
        self.generation.as_ref()
    }

    fn stage_row(&mut self, row: SecBulkNativeRow) -> Result<(), SecBulkError> {
        check_operation(self.deadline, &self.cancellation)?;
        if !self.begun || self.generation.is_some() {
            return Err(SecBulkError::PublicationNotReady);
        }
        if self.staged_rows >= MAX_NATIVE_PUBLICATION_ROWS {
            return Err(SecBulkError::QueryLimitExceeded);
        }
        validate_native_row(&self.layout, &row)?;
        let key = native_key(&row)?;
        if self.previous_key.is_some_and(|previous| previous >= key) {
            return Err(SecBulkError::RecoveryMismatch);
        }
        let payload = serde_json::to_vec(&row).map_err(|_| SecBulkError::PublicationNotReady)?;
        if payload.is_empty() || payload.len() > MAX_NATIVE_RECORD_BYTES {
            return Err(SecBulkError::QueryLimitExceeded);
        }
        let length = u32::try_from(payload.len()).map_err(|_| SecBulkError::QueryLimitExceeded)?;
        let data = self
            .data
            .as_mut()
            .ok_or(SecBulkError::PublicationNotReady)?;
        let offset = data
            .observed_bytes()
            .checked_add(4)
            .ok_or(SecBulkError::QueryLimitExceeded)?;
        data.write_bytes(&length.to_be_bytes(), &self.cancellation)?;
        data.write_bytes(&payload, &self.cancellation)?;
        let payload_digest: [u8; 32] = Sha256::digest(&payload).into();
        let data_entry = NativeIndexEntry {
            key,
            payload_offset: offset,
            payload_length: length,
            payload_digest,
        };
        self.index_page.extend_from_slice(&key);
        self.index_page.extend_from_slice(&offset.to_be_bytes());
        self.index_page.extend_from_slice(&length.to_be_bytes());
        self.index_page.extend_from_slice(&payload_digest);
        self.stage_lookup_entries(&row, data_entry)?;
        hash_field(&mut self.records_digest, &payload_digest);
        self.index_page_records = self
            .index_page_records
            .checked_add(1)
            .ok_or(SecBulkError::QueryLimitExceeded)?;
        self.staged_rows = self
            .staged_rows
            .checked_add(1)
            .ok_or(SecBulkError::QueryLimitExceeded)?;
        self.previous_key = Some(key);
        if self.index_page_records == NATIVE_INDEX_PAGE_RECORDS {
            self.flush_index_page()?;
        }
        Ok(())
    }

    fn stage_lookup_entries(
        &mut self,
        row: &SecBulkNativeRow,
        data_entry: NativeIndexEntry,
    ) -> Result<(), SecBulkError> {
        visit_native_lookup_entries(row, data_entry, |bucket, entry| {
            self.append_lookup_entry(bucket, entry)
        })
    }

    fn append_lookup_entry(
        &mut self,
        bucket: u16,
        entry: NativeLookupEntry,
    ) -> Result<(), SecBulkError> {
        check_operation(self.deadline, &self.cancellation)?;
        let increment =
            u64::try_from(NATIVE_LOOKUP_ENTRY_BYTES).map_err(|_| SecBulkError::AllocationFailed)?;
        let next_bytes = self
            .lookup_scratch_bytes
            .checked_add(increment)
            .ok_or(SecBulkError::ScratchLimitExceeded)?;
        if next_bytes > MAX_NATIVE_LOOKUP_SCRATCH_BYTES {
            return Err(SecBulkError::ScratchLimitExceeded);
        }
        match self
            .lookup_scratch
            .get(&bucket)
            .map(|scratch| scratch.descriptor_open)
        {
            None => {
                self.ensure_lookup_descriptor_slot(None)?;
                self.lookup_clock = self
                    .lookup_clock
                    .checked_add(1)
                    .ok_or(SecBulkError::QueryLimitExceeded)?;
                let replaced = self.lookup_scratch.insert(
                    bucket,
                    NativeLookupScratch {
                        scratch: self.store.create_scratch()?,
                        state: LookupDigestState::new(bucket),
                        bytes: 0,
                        descriptor_open: true,
                        last_used: self.lookup_clock,
                    },
                );
                if replaced.is_some() {
                    return Err(SecBulkError::RecoveryMismatch);
                }
                self.lookup_open_descriptors += 1;
            }
            Some(false) => {
                self.ensure_lookup_descriptor_slot(Some(bucket))?;
                let scratch = self
                    .lookup_scratch
                    .get_mut(&bucket)
                    .ok_or(SecBulkError::PublicationNotReady)?;
                scratch.scratch.file_mut()?.seek(SeekFrom::End(0))?;
                scratch.descriptor_open = true;
                self.lookup_open_descriptors += 1;
            }
            Some(true) => {}
        }
        self.lookup_clock = self
            .lookup_clock
            .checked_add(1)
            .ok_or(SecBulkError::QueryLimitExceeded)?;
        let scratch = self
            .lookup_scratch
            .get_mut(&bucket)
            .ok_or(SecBulkError::PublicationNotReady)?;
        scratch.scratch.file_mut()?.write_all(&entry.encode())?;
        scratch.state.observe(&entry)?;
        scratch.bytes = scratch
            .bytes
            .checked_add(increment)
            .ok_or(SecBulkError::ScratchLimitExceeded)?;
        scratch.last_used = self.lookup_clock;
        self.lookup_scratch_bytes = next_bytes;
        self.lookup_records = self
            .lookup_records
            .checked_add(1)
            .ok_or(SecBulkError::QueryLimitExceeded)?;
        Ok(())
    }

    fn ensure_lookup_descriptor_slot(&mut self, except: Option<u16>) -> Result<(), SecBulkError> {
        if self.lookup_open_descriptors < MAX_OPEN_LOOKUP_SCRATCH {
            return Ok(());
        }
        let candidate = self
            .lookup_scratch
            .iter()
            .filter(|(bucket, scratch)| {
                scratch.descriptor_open && except.is_none_or(|except| **bucket != except)
            })
            .min_by_key(|(_, scratch)| scratch.last_used)
            .map(|(bucket, _)| *bucket)
            .ok_or(SecBulkError::ScratchLimitExceeded)?;
        let scratch = self
            .lookup_scratch
            .get_mut(&candidate)
            .ok_or(SecBulkError::ScratchLimitExceeded)?;
        scratch.scratch.close_descriptor();
        scratch.descriptor_open = false;
        self.lookup_open_descriptors = self
            .lookup_open_descriptors
            .checked_sub(1)
            .ok_or(SecBulkError::RecoveryMismatch)?;
        Ok(())
    }

    fn flush_index_page(&mut self) -> Result<(), SecBulkError> {
        if self.index_page_records == 0 {
            return Ok(());
        }
        check_operation(self.deadline, &self.cancellation)?;
        let count_offset = NATIVE_INDEX_MAGIC.len();
        self.index_page[count_offset..count_offset + 4]
            .copy_from_slice(&self.index_page_records.to_be_bytes());
        let first_offset = NATIVE_INDEX_MAGIC.len() + 4;
        let last_offset =
            first_offset + (self.index_page_records as usize - 1) * NATIVE_INDEX_ENTRY_BYTES;
        let receipt = persist_bounded_content(
            self.store,
            &self.index_page,
            u64::try_from(self.index_page.len()).map_err(|_| SecBulkError::AllocationFailed)?,
            self.deadline,
            &self.cancellation,
        )?;
        self.index_pages
            .try_reserve(1)
            .map_err(|_| SecBulkError::AllocationFailed)?;
        self.index_pages.push(NativeIndexPageDescriptor {
            evidence: receipt.evidence(),
            size_bytes: receipt.size_bytes(),
            record_count: self.index_page_records,
            first_key: self.index_page[first_offset..first_offset + NATIVE_KEY_BYTES].to_vec(),
            last_key: self.index_page[last_offset..last_offset + NATIVE_KEY_BYTES].to_vec(),
        });
        self.index_page.clear();
        start_index_page(&mut self.index_page);
        self.index_page_records = 0;
        check_operation(self.deadline, &self.cancellation)
    }

    fn seal_lookup_buckets(&mut self) -> Result<(), SecBulkError> {
        check_operation(self.deadline, &self.cancellation)?;
        for scratch in self.lookup_scratch.values_mut() {
            scratch.scratch.close_descriptor();
            scratch.descriptor_open = false;
        }
        self.lookup_open_descriptors = 0;
        let buckets = std::mem::take(&mut self.lookup_scratch);
        let mut descriptors = Vec::new();
        descriptors
            .try_reserve_exact(buckets.len())
            .map_err(|_| SecBulkError::AllocationFailed)?;
        let mut sealed_records = 0_u64;
        for (bucket, mut scratch) in buckets {
            check_operation(self.deadline, &self.cancellation)?;
            let expected_bytes = scratch
                .state
                .records
                .checked_mul(
                    u64::try_from(NATIVE_LOOKUP_ENTRY_BYTES)
                        .map_err(|_| SecBulkError::AllocationFailed)?,
                )
                .ok_or(SecBulkError::ScratchLimitExceeded)?;
            if scratch.bytes == 0 || scratch.bytes != expected_bytes {
                return Err(SecBulkError::RecoveryMismatch);
            }
            scratch.scratch.sync_and_rewind()?;
            let mut remaining = scratch.state.records;
            let mut pages = Vec::new();
            pages
                .try_reserve_exact(
                    usize::try_from(remaining.div_ceil(u64::from(NATIVE_LOOKUP_PAGE_RECORDS)))
                        .map_err(|_| SecBulkError::AllocationFailed)?,
                )
                .map_err(|_| SecBulkError::AllocationFailed)?;
            let mut observed = LookupDigestState::new(bucket);
            while remaining != 0 {
                check_operation(self.deadline, &self.cancellation)?;
                let count = remaining.min(u64::from(NATIVE_LOOKUP_PAGE_RECORDS));
                let count_u32 = u32::try_from(count).map_err(|_| SecBulkError::AllocationFailed)?;
                let page_size = NATIVE_LOOKUP_ENTRY_BYTES
                    .checked_mul(
                        usize::try_from(count).map_err(|_| SecBulkError::AllocationFailed)?,
                    )
                    .and_then(|body| body.checked_add(NATIVE_LOOKUP_MAGIC.len() + 2 + 4))
                    .ok_or(SecBulkError::AllocationFailed)?;
                let mut page = Vec::new();
                page.try_reserve_exact(page_size)
                    .map_err(|_| SecBulkError::AllocationFailed)?;
                page.extend_from_slice(NATIVE_LOOKUP_MAGIC);
                page.extend_from_slice(&bucket.to_be_bytes());
                page.extend_from_slice(&count_u32.to_be_bytes());
                for _ in 0..count {
                    check_operation(self.deadline, &self.cancellation)?;
                    let mut encoded = [0_u8; NATIVE_LOOKUP_ENTRY_BYTES];
                    scratch.scratch.file_mut()?.read_exact(&mut encoded)?;
                    let entry = NativeLookupEntry::decode(&encoded)?;
                    if lookup_bucket(entry.query_digest) != bucket {
                        return Err(SecBulkError::RecoveryMismatch);
                    }
                    observed.observe(&entry)?;
                    page.extend_from_slice(&encoded);
                }
                let receipt = persist_bounded_content(
                    self.store,
                    &page,
                    u64::try_from(page_size).map_err(|_| SecBulkError::AllocationFailed)?,
                    self.deadline,
                    &self.cancellation,
                )?;
                pages.push(NativeLookupPageDescriptor {
                    evidence: receipt.evidence(),
                    size_bytes: receipt.size_bytes(),
                    record_count: count_u32,
                });
                remaining = remaining
                    .checked_sub(count)
                    .ok_or(SecBulkError::RecoveryMismatch)?;
            }
            let mut extra = [0_u8; 1];
            if scratch.scratch.file_mut()?.read(&mut extra)? != 0
                || observed.records != scratch.state.records
                || observed.evidence() != scratch.state.evidence()
            {
                return Err(SecBulkError::RecoveryMismatch);
            }
            sealed_records = sealed_records
                .checked_add(observed.records)
                .ok_or(SecBulkError::QueryLimitExceeded)?;
            self.lookup_scratch_bytes = self
                .lookup_scratch_bytes
                .checked_sub(scratch.bytes)
                .ok_or(SecBulkError::RecoveryMismatch)?;
            descriptors.push(NativeLookupBucketDescriptor {
                bucket,
                record_count: observed.records,
                records_evidence: observed.evidence(),
                pages,
            });
        }
        self.lookup_open_descriptors = 0;
        if sealed_records != self.lookup_records || self.lookup_scratch_bytes != 0 {
            return Err(SecBulkError::RecoveryMismatch);
        }
        self.lookup_buckets = descriptors;
        Ok(())
    }

    fn seal(&mut self, report: SecBulkScanReport) -> Result<(), SecBulkError> {
        check_operation(self.deadline, &self.cancellation)?;
        if report.manifest_evidence() != self.layout.evidence()
            || report.source_rows() != self.staged_rows
            || report.emitted_typed_rows() != self.staged_rows
        {
            return Err(SecBulkError::PublicationNotReady);
        }
        self.flush_index_page()?;
        self.seal_lookup_buckets()?;
        let data = self.data.take().ok_or(SecBulkError::PublicationNotReady)?;
        let data_receipt = data.seal(&self.cancellation)?;
        let tables = generation_tables(&self.layout)?;
        let records_evidence = EvidenceDigest::new(
            DigestAlgorithm::Sha256,
            self.records_digest.clone().finalize().into(),
        );
        let manifest = NativeGenerationManifest {
            version: NATIVE_ROOT_VERSION,
            family: self.layout.capture().selection().family(),
            quarter_year: self.layout.capture().selection().quarter().year(),
            quarter_number: self.layout.capture().selection().quarter().quarter(),
            catalog_snapshot_date: SEC_BULK_CATALOG_SNAPSHOT_DATE.to_owned(),
            manifest_evidence: self.layout.evidence(),
            archive_evidence: self.layout.capture().evidence(),
            archive_size_bytes: self.layout.capture().size_bytes(),
            archive_retrieval_revision: self.layout.capture().retrieval_revision(),
            archive_transport: transport_lineage(self.layout.capture()),
            readme_evidence: self.layout.official_readme_capture().evidence(),
            readme_size_bytes: self.layout.official_readme_capture().size_bytes(),
            readme_retrieval_revision: self.layout.official_readme_capture().retrieval_revision(),
            readme_transport: transport_lineage(self.layout.official_readme_capture()),
            metadata_evidence: self.layout.metadata_evidence(),
            archive_readme_evidence: self.layout.readme_evidence(),
            accepted_schema_version: self
                .layout
                .capture()
                .selection()
                .accepted_schema()
                .version()
                .as_str()
                .to_owned(),
            accepted_schema_effective_date: self
                .layout
                .capture()
                .selection()
                .accepted_schema()
                .effective_date()
                .to_string(),
            accepted_schema_locator: self
                .layout
                .capture()
                .selection()
                .accepted_schema()
                .technical_spec_locator()
                .as_str()
                .to_owned(),
            declared_coverage_gap: matches!(
                self.layout.capture().selection().coverage(),
                super::model::SecBulkCoverage::AcceptedSchemaExcluded { .. }
            ),
            tables,
            source_rows: report.source_rows(),
            emitted_rows: report.emitted_typed_rows(),
            data_evidence: data_receipt.evidence(),
            data_size_bytes: data_receipt.size_bytes(),
            index_pages: std::mem::take(&mut self.index_pages),
            lookup_records: self.lookup_records,
            lookup_buckets: std::mem::take(&mut self.lookup_buckets),
            records_evidence,
            published_at_unix_nanos: self.published_at.unix_nanos(),
        };
        let root_bytes =
            serde_json::to_vec(&manifest).map_err(|_| SecBulkError::PublicationNotReady)?;
        if root_bytes.is_empty()
            || u64::try_from(root_bytes.len()).map_or(true, |size| size > MAX_NATIVE_ROOT_BYTES)
        {
            return Err(SecBulkError::QueryLimitExceeded);
        }
        let root = persist_bounded_content(
            self.store,
            &root_bytes,
            MAX_NATIVE_ROOT_BYTES,
            self.deadline,
            &self.cancellation,
        )?;
        self.generation = Some(recover_from_receipt(
            self.store,
            root,
            self.deadline,
            &self.cancellation,
        )?);
        Ok(())
    }
}

impl SecBulkRowSink for SecBulkNativePublicationSession<'_> {
    fn begin(&mut self, manifest_evidence: EvidenceDigest) -> Result<(), SecBulkError> {
        if self.begun || self.generation.is_some() || manifest_evidence != self.layout.evidence() {
            return Err(SecBulkError::PublicationNotReady);
        }
        self.begun = true;
        Ok(())
    }

    fn stage(&mut self, row: SecBulkNativeRow) -> Result<(), SecBulkError> {
        self.stage_row(row)
    }

    fn commit(&mut self, report: SecBulkScanReport) -> Result<(), SecBulkError> {
        self.seal(report)
    }

    fn abort(&mut self) {
        drop(self.data.take());
        self.index_page.clear();
        self.index_pages.clear();
        self.lookup_scratch.clear();
        self.lookup_buckets.clear();
        self.lookup_scratch_bytes = 0;
        self.lookup_records = 0;
        self.lookup_open_descriptors = 0;
        self.begun = false;
        self.generation = None;
    }
}

/// Fully re-verifies a native generation after restart, without replaying caller-owned rows.
pub fn recover_native_generation(
    store: &RawEvidenceStore,
    expected: &SecBulkNativePublishedGeneration,
    deadline: Timestamp,
    cancellation: &CancellationToken,
) -> Result<SecBulkNativePublishedGeneration, SecBulkError> {
    let recovered =
        recover_native_generation_from_receipt(store, expected.receipt(), deadline, cancellation)?;
    if recovered != *expected {
        return Err(SecBulkError::RecoveryMismatch);
    }
    Ok(recovered)
}

/// Reconstructs native query authority after a process-cold restart from only the sealed receipt.
pub fn recover_native_generation_from_receipt(
    store: &RawEvidenceStore,
    receipt: SecBulkNativeGenerationReceipt,
    deadline: Timestamp,
    cancellation: &CancellationToken,
) -> Result<SecBulkNativePublishedGeneration, SecBulkError> {
    if !valid_sha256(receipt.root_evidence)
        || receipt.root_size_bytes == 0
        || receipt.root_size_bytes > MAX_NATIVE_ROOT_BYTES
    {
        return Err(SecBulkError::RecoveryMismatch);
    }
    recover_from_receipt(
        store,
        RawEvidenceReceipt::new(receipt.root_evidence, receipt.root_size_bytes),
        deadline,
        cancellation,
    )
}

/// Executes a bounded typed query against any one of the 30 N-PORT or 53 N-CEN families.
#[allow(clippy::too_many_arguments)]
pub fn query_native_rows(
    store: &RawEvidenceStore,
    generation: &SecBulkNativePublishedGeneration,
    table: SecBulkTableKind,
    exact_primary_key: Option<&[SecBulkKeyField]>,
    limits: SecBulkQueryLimits,
    cursor: Option<SecBulkNativeQueryCursor>,
    deadline: Timestamp,
    cancellation: &CancellationToken,
) -> Result<SecBulkNativeQueryPage, SecBulkError> {
    let predicate =
        exact_primary_key.map_or(NativeQueryPredicate::All, NativeQueryPredicate::PrimaryKey);
    query_native_rows_inner(
        store,
        generation,
        table,
        predicate,
        limits,
        cursor,
        deadline,
        cancellation,
    )
}

/// Executes a bounded relationship query, using the durable generation-bound secondary index.
#[allow(clippy::too_many_arguments)]
pub fn query_native_rows_by_joins(
    store: &RawEvidenceStore,
    generation: &SecBulkNativePublishedGeneration,
    table: SecBulkTableKind,
    joins: &[SecBulkNativeJoinFilter],
    limits: SecBulkQueryLimits,
    cursor: Option<SecBulkNativeQueryCursor>,
    deadline: Timestamp,
    cancellation: &CancellationToken,
) -> Result<SecBulkNativeQueryPage, SecBulkError> {
    query_native_rows_inner(
        store,
        generation,
        table,
        NativeQueryPredicate::Joins(joins),
        limits,
        cursor,
        deadline,
        cancellation,
    )
}

/// Materializes the complete typed C.9-C.12 supplement set for one provider-native holding.
#[allow(clippy::too_many_arguments)]
pub fn query_nport_holding_supplements(
    store: &RawEvidenceStore,
    generation: &SecBulkNativePublishedGeneration,
    accession: &SourceIdentifier,
    holding_id: &SourceIdentifier,
    limits: SecBulkQueryLimits,
    deadline: Timestamp,
    cancellation: &CancellationToken,
) -> Result<SecNportHoldingSupplementSet, SecBulkError> {
    if generation.family() != SecBulkFamily::Nport {
        return Err(SecBulkError::InvalidCanonicalMapping);
    }
    let filters = [
        SecBulkNativeJoinFilter::try_new(SecBulkJoinDomain::Accession, accession.as_str())?,
        SecBulkNativeJoinFilter::try_new(SecBulkJoinDomain::Holding, holding_id.as_str())?,
    ];
    let holding_page = query_native_rows_by_joins(
        store,
        generation,
        SecBulkTableKind::NportFundReportedHolding,
        &filters,
        limits,
        None,
        deadline,
        cancellation,
    )?;
    if holding_page.rows().is_empty() {
        return Err(SecBulkError::NativeRowUnavailable);
    }
    if holding_page.rows().len() != 1 || holding_page.next_cursor().is_some() {
        return Err(SecBulkError::NativeRowAmbiguous);
    }
    let holding = holding_page
        .rows()
        .first()
        .cloned()
        .ok_or(SecBulkError::NativeRowUnavailable)?;
    let mut groups = Vec::new();
    groups
        .try_reserve_exact(nport_holding_supplement_tables().len())
        .map_err(|_| SecBulkError::AllocationFailed)?;
    let mut total_rows = 0_u64;
    let mut total_bytes = 0_u64;
    for table in nport_holding_supplement_tables() {
        check_operation(deadline, cancellation)?;
        let page = query_native_rows_by_joins(
            store,
            generation,
            *table,
            &filters,
            limits,
            None,
            deadline,
            cancellation,
        )?;
        if page.next_cursor().is_some() {
            return Err(SecBulkError::QueryLimitExceeded);
        }
        total_rows = total_rows
            .checked_add(
                u64::try_from(page.rows.len()).map_err(|_| SecBulkError::AllocationFailed)?,
            )
            .ok_or(SecBulkError::QueryLimitExceeded)?;
        if total_rows > MAX_HOLDING_SUPPLEMENT_ROWS {
            return Err(SecBulkError::QueryLimitExceeded);
        }
        for row in &page.rows {
            let encoded = serde_json::to_vec(row).map_err(|_| SecBulkError::RecoveryMismatch)?;
            total_bytes = total_bytes
                .checked_add(
                    u64::try_from(encoded.len()).map_err(|_| SecBulkError::AllocationFailed)?,
                )
                .ok_or(SecBulkError::QueryLimitExceeded)?;
            if total_bytes > MAX_HOLDING_SUPPLEMENT_RESPONSE_BYTES {
                return Err(SecBulkError::QueryLimitExceeded);
            }
        }
        groups.push(SecBulkRelatedTableRows::new(
            *table,
            page.table_presence,
            page.rows,
        ));
    }
    SecNportHoldingSupplementSet::try_new(
        generation.evidence(),
        generation.manifest_evidence(),
        accession.clone(),
        holding_id.clone(),
        holding,
        groups,
    )
}

#[allow(clippy::too_many_arguments)]
fn query_native_rows_inner(
    store: &RawEvidenceStore,
    generation: &SecBulkNativePublishedGeneration,
    table: SecBulkTableKind,
    predicate: NativeQueryPredicate<'_>,
    limits: SecBulkQueryLimits,
    cursor: Option<SecBulkNativeQueryCursor>,
    deadline: Timestamp,
    cancellation: &CancellationToken,
) -> Result<SecBulkNativeQueryPage, SecBulkError> {
    check_operation(deadline, cancellation)?;
    if table.family() != generation.family() {
        return Err(SecBulkError::PublicationNotReady);
    }
    let table_descriptor = generation
        .manifest
        .tables
        .iter()
        .find(|candidate| candidate.table == table)
        .ok_or(SecBulkError::RecoveryMismatch)?;
    match predicate {
        NativeQueryPredicate::All => {}
        NativeQueryPredicate::PrimaryKey(primary_key) => {
            validate_primary_key_filter(table_descriptor, primary_key)?;
            if cursor.is_some() {
                return Err(SecBulkError::InvalidCanonicalMapping);
            }
        }
        NativeQueryPredicate::Joins(joins) => validate_join_filters(table_descriptor, joins)?,
    }
    let presence = table_presence(table_descriptor)?;
    let query_evidence = native_query_digest(generation.evidence(), table, &predicate);
    let skip = match cursor {
        Some(cursor)
            if cursor.generation_evidence == generation.evidence()
                && cursor.query_evidence == query_evidence
                && cursor.rows_to_skip <= table_descriptor.row_count =>
        {
            cursor.rows_to_skip
        }
        Some(_) => return Err(SecBulkError::PublicationNotReady),
        None => 0,
    };
    let mut data = store.open_sealed_readonly(
        &generation.manifest.data_evidence,
        generation.manifest.data_size_bytes,
        MAX_NATIVE_DATA_BYTES,
    )?;
    let ordinal = table.ordinal().to_be_bytes();
    let mut scanned = 0_u64;
    let mut matched = 0_u64;
    let mut materialized_bytes = 0_u64;
    let mut rows = Vec::new();
    let mut has_more = false;
    let indexed_lookup = predicate_lookup(table, &predicate);
    if let Some((selector, digest)) = indexed_lookup {
        let bucket = lookup_bucket(digest);
        if let Ok(index) = generation
            .manifest
            .lookup_buckets
            .binary_search_by_key(&bucket, |descriptor| descriptor.bucket)
        {
            let descriptor = &generation.manifest.lookup_buckets[index];
            'lookup_pages: for page in &descriptor.pages {
                let bytes = read_verified_lookup_page(store, page, deadline, cancellation)?;
                for entry in parse_lookup_page(&bytes, bucket, page.record_count)? {
                    check_operation(deadline, cancellation)?;
                    scanned = scanned
                        .checked_add(1)
                        .ok_or(SecBulkError::QueryLimitExceeded)?;
                    if scanned > limits.max_scanned_records() {
                        return Err(SecBulkError::QueryLimitExceeded);
                    }
                    if entry.table_ordinal != table.ordinal()
                        || entry.selector != selector
                        || entry.query_digest != digest
                    {
                        continue;
                    }
                    let mut row = read_native_row(
                        &mut data,
                        generation.manifest.data_size_bytes,
                        entry.data_entry(),
                    )?;
                    if !row_matches_predicate(&row, &predicate) {
                        return Err(SecBulkError::RecoveryMismatch);
                    }
                    bind_query_membership(&mut row, generation, query_evidence)?;
                    if matched < skip {
                        matched += 1;
                        continue;
                    }
                    if rows.len() == limits.max_results() {
                        has_more = true;
                        break 'lookup_pages;
                    }
                    materialized_bytes = materialized_bytes
                        .checked_add(u64::from(entry.payload_length))
                        .ok_or(SecBulkError::QueryLimitExceeded)?;
                    if materialized_bytes > MAX_NATIVE_QUERY_RESPONSE_BYTES {
                        return Err(SecBulkError::QueryLimitExceeded);
                    }
                    rows.push(row);
                    matched += 1;
                }
            }
        }
    } else {
        'pages: for descriptor in &generation.manifest.index_pages {
            check_operation(deadline, cancellation)?;
            if descriptor.last_key[..2] < ordinal[..] {
                continue;
            }
            if descriptor.first_key[..2] > ordinal[..] {
                break;
            }
            if descriptor.first_key[..2] == ordinal[..]
                && descriptor.last_key[..2] == ordinal[..]
                && matched < skip
            {
                let page_rows = u64::from(descriptor.record_count);
                if matched
                    .checked_add(page_rows)
                    .is_some_and(|after_page| after_page <= skip)
                {
                    matched += page_rows;
                    continue;
                }
            }
            let bytes = read_verified_page(store, descriptor, deadline, cancellation)?;
            for entry in parse_index_page(&bytes, descriptor.record_count)? {
                check_operation(deadline, cancellation)?;
                if entry.key[..2] < ordinal[..] {
                    continue;
                }
                if entry.key[..2] > ordinal[..] {
                    break;
                }
                scanned = scanned
                    .checked_add(1)
                    .ok_or(SecBulkError::QueryLimitExceeded)?;
                if scanned > limits.max_scanned_records() {
                    return Err(SecBulkError::QueryLimitExceeded);
                }
                let mut row =
                    read_native_row(&mut data, generation.manifest.data_size_bytes, entry)?;
                if !row_matches_predicate(&row, &predicate) {
                    continue;
                }
                bind_query_membership(&mut row, generation, query_evidence)?;
                if matched < skip {
                    matched += 1;
                    continue;
                }
                if rows.len() == limits.max_results() {
                    has_more = true;
                    break 'pages;
                }
                materialized_bytes = materialized_bytes
                    .checked_add(u64::from(entry.payload_length))
                    .ok_or(SecBulkError::QueryLimitExceeded)?;
                if materialized_bytes > MAX_NATIVE_QUERY_RESPONSE_BYTES {
                    return Err(SecBulkError::QueryLimitExceeded);
                }
                rows.push(row);
                matched += 1;
            }
        }
    }
    let returned = u64::try_from(rows.len()).map_err(|_| SecBulkError::AllocationFailed)?;
    let next_cursor = (has_more && !matches!(predicate, NativeQueryPredicate::PrimaryKey(_)))
        .then_some(SecBulkNativeQueryCursor {
            generation_evidence: generation.evidence(),
            query_evidence,
            rows_to_skip: skip
                .checked_add(returned)
                .ok_or(SecBulkError::QueryLimitExceeded)?,
        });
    let filtered = !matches!(predicate, NativeQueryPredicate::All);
    let completeness = if matches!(presence, SecBulkTablePresence::DeclaredAbsent)
        || generation.manifest.declared_coverage_gap
        || (filtered && rows.is_empty())
    {
        SecBulkQueryCompleteness::Unavailable
    } else if matches!(predicate, NativeQueryPredicate::PrimaryKey(_))
        && (rows.len() > 1 || has_more)
    {
        SecBulkQueryCompleteness::Ambiguous
    } else {
        SecBulkQueryCompleteness::Exact
    };
    Ok(SecBulkNativeQueryPage {
        generation_evidence: generation.evidence(),
        query_evidence,
        table_presence: presence,
        completeness,
        rows,
        next_cursor,
        scanned_rows: scanned,
    })
}

fn validate_primary_key_filter(
    table: &NativeTableDescriptor,
    primary_key: &[SecBulkKeyField],
) -> Result<(), SecBulkError> {
    if table.primary_key.is_empty() || primary_key.len() != table.primary_key.len() {
        return Err(SecBulkError::InvalidCanonicalMapping);
    }
    for (field, expected) in primary_key.iter().zip(&table.primary_key) {
        let column = table
            .columns
            .iter()
            .find(|column| column.name == *expected)
            .ok_or(SecBulkError::RecoveryMismatch)?;
        if field.name().as_str() != expected || !valid_lexical_filter(field.value(), column) {
            return Err(SecBulkError::InvalidCanonicalMapping);
        }
    }
    Ok(())
}

fn validate_join_filters(
    table: &NativeTableDescriptor,
    filters: &[SecBulkNativeJoinFilter],
) -> Result<(), SecBulkError> {
    if filters.is_empty() || filters.len() > 8 {
        return Err(SecBulkError::InvalidCanonicalMapping);
    }
    let mut previous_selector = 0_u8;
    for filter in filters {
        let selector = join_selector_tag(filter.domain);
        if selector <= previous_selector {
            return Err(SecBulkError::InvalidCanonicalMapping);
        }
        previous_selector = selector;
        let column_name = join_column_for_domain(filter.domain);
        let column = table
            .columns
            .iter()
            .find(|column| column.name == column_name)
            .ok_or(SecBulkError::InvalidCanonicalMapping)?;
        if !valid_lexical_filter(filter.value(), column) {
            return Err(SecBulkError::InvalidCanonicalMapping);
        }
    }
    let has = |domain| filters.iter().any(|filter| filter.domain == domain);
    if (has(SecBulkJoinDomain::NcenDirectorSequence)
        || has(SecBulkJoinDomain::NcenComplianceOfficerSequence)
        || has(SecBulkJoinDomain::NcenValuationChangeSequence))
        && !has(SecBulkJoinDomain::Accession)
        || (has(SecBulkJoinDomain::NcenSecurityLendingSequence)
            || has(SecBulkJoinDomain::NcenLineOfCreditSequence))
            && !has(SecBulkJoinDomain::Fund)
    {
        return Err(SecBulkError::InvalidCanonicalMapping);
    }
    Ok(())
}

const fn join_column_for_domain(domain: SecBulkJoinDomain) -> &'static str {
    match domain {
        SecBulkJoinDomain::Accession => "ACCESSION_NUMBER",
        SecBulkJoinDomain::Holding => "HOLDING_ID",
        SecBulkJoinDomain::Fund => "FUND_ID",
        SecBulkJoinDomain::Series => "SERIES_ID",
        SecBulkJoinDomain::RegistrantCik => "CIK",
        SecBulkJoinDomain::ShareClass => "CLASS_ID",
        SecBulkJoinDomain::NcenDirectorSequence => "DIRECTOR_SEQNUM",
        SecBulkJoinDomain::NcenComplianceOfficerSequence => "CCO_SEQNUM",
        SecBulkJoinDomain::NcenValuationChangeSequence => "VALUATION_METHOD_CHANGE_SEQNUM",
        SecBulkJoinDomain::NcenSecurityLendingSequence => "SECURITY_LENDING_SEQNUM",
        SecBulkJoinDomain::NcenLineOfCreditSequence => "LINE_OF_CREDIT_SEQNUM",
    }
}

fn predicate_lookup(
    table: SecBulkTableKind,
    predicate: &NativeQueryPredicate<'_>,
) -> Option<(u8, [u8; 32])> {
    match predicate {
        NativeQueryPredicate::All => None,
        NativeQueryPredicate::PrimaryKey(primary_key) if primary_key.is_empty() => None,
        NativeQueryPredicate::PrimaryKey(primary_key) => {
            Some((0, lookup_digest_primary_key(table, primary_key)))
        }
        NativeQueryPredicate::Joins(filters) => filters.last().map(|filter| {
            let selector = join_selector_tag(filter.domain);
            (
                selector,
                lookup_digest_join(table, filter.domain, filter.value()),
            )
        }),
    }
}

fn row_matches_predicate(row: &SecBulkNativeRow, predicate: &NativeQueryPredicate<'_>) -> bool {
    match predicate {
        NativeQueryPredicate::All => true,
        NativeQueryPredicate::PrimaryKey(primary_key) => row.primary_key() == *primary_key,
        NativeQueryPredicate::Joins(filters) => filters.iter().all(|filter| {
            row.joins()
                .iter()
                .any(|join| join.domain() == filter.domain && join.value() == filter.value())
        }),
    }
}

fn valid_lexical_filter(value: &str, column: &NativeColumnDescriptor) -> bool {
    if value.is_empty() {
        return !column.required;
    }
    if column.max_length.is_some_and(|maximum| {
        u64::try_from(value.chars().count()).map_or(true, |observed| observed > maximum)
    }) {
        return false;
    }
    match column.datatype_base.as_str() {
        "string" => true,
        "date (DD-MON-YYYY)" => valid_sec_date_lexical(value),
        "NUMBER" => valid_fixed_number(value, column.data_precision, column.data_scale),
        _ => false,
    }
}

fn valid_sec_date_lexical(value: &str) -> bool {
    let bytes = value.as_bytes();
    const MONTHS: [&[u8; 3]; 12] = [
        b"JAN", b"FEB", b"MAR", b"APR", b"MAY", b"JUN", b"JUL", b"AUG", b"SEP", b"OCT", b"NOV",
        b"DEC",
    ];
    bytes.len() == 11
        && bytes[2] == b'-'
        && bytes[6] == b'-'
        && bytes[..2].iter().all(u8::is_ascii_digit)
        && MONTHS.iter().any(|month| bytes[3..6] == month[..])
        && bytes[7..].iter().all(u8::is_ascii_digit)
        && chrono::NaiveDate::parse_from_str(value, "%d-%b-%Y").is_ok()
}

fn recover_from_receipt(
    store: &RawEvidenceStore,
    root: RawEvidenceReceipt,
    deadline: Timestamp,
    cancellation: &CancellationToken,
) -> Result<SecBulkNativePublishedGeneration, SecBulkError> {
    check_operation(deadline, cancellation)?;
    let mut root_file = store.open_verified_before(
        &root.evidence(),
        root.size_bytes(),
        MAX_NATIVE_ROOT_BYTES,
        deadline,
        cancellation,
    )?;
    let mut root_bytes = Vec::new();
    root_bytes
        .try_reserve_exact(
            usize::try_from(root.size_bytes()).map_err(|_| SecBulkError::AllocationFailed)?,
        )
        .map_err(|_| SecBulkError::AllocationFailed)?;
    root_file.read_to_end(&mut root_bytes)?;
    check_operation(deadline, cancellation)?;
    let manifest: NativeGenerationManifest =
        serde_json::from_slice(&root_bytes).map_err(|_| SecBulkError::RecoveryMismatch)?;
    validate_manifest(&manifest)?;
    drop(store.open_verified_before(
        &manifest.archive_evidence,
        manifest.archive_size_bytes,
        MAX_BOUND_ARCHIVE_BYTES,
        deadline,
        cancellation,
    )?);
    drop(store.open_verified_before(
        &manifest.readme_evidence,
        manifest.readme_size_bytes,
        MAX_BOUND_README_BYTES,
        deadline,
        cancellation,
    )?);
    let mut data = store.open_verified_before(
        &manifest.data_evidence,
        manifest.data_size_bytes,
        MAX_NATIVE_DATA_BYTES,
        deadline,
        cancellation,
    )?;
    let mut magic = vec![0_u8; NATIVE_DATA_MAGIC.len()];
    data.read_exact(&mut magic)?;
    if magic != NATIVE_DATA_MAGIC {
        return Err(SecBulkError::RecoveryMismatch);
    }
    let mut previous_key = None;
    let mut next_offset = u64::try_from(NATIVE_DATA_MAGIC.len())
        .map_err(|_| SecBulkError::AllocationFailed)?
        .checked_add(4)
        .ok_or(SecBulkError::RecoveryMismatch)?;
    let mut observed = 0_u64;
    let mut rows_by_table = BTreeMap::new();
    let mut expected_lookups = BTreeMap::<u16, LookupDigestState>::new();
    let mut digest = records_digest_prefix(manifest.manifest_evidence);
    for descriptor in &manifest.index_pages {
        check_operation(deadline, cancellation)?;
        let bytes = read_verified_page(store, descriptor, deadline, cancellation)?;
        let entries = parse_index_page(&bytes, descriptor.record_count)?;
        if entries.first().map(|entry| entry.key.as_slice())
            != Some(descriptor.first_key.as_slice())
            || entries.last().map(|entry| entry.key.as_slice())
                != Some(descriptor.last_key.as_slice())
        {
            return Err(SecBulkError::RecoveryMismatch);
        }
        for entry in entries {
            check_operation(deadline, cancellation)?;
            if previous_key.is_some_and(|previous| previous >= entry.key)
                || entry.payload_offset != next_offset
                || !entry_fits_data(entry, manifest.data_size_bytes)
            {
                return Err(SecBulkError::RecoveryMismatch);
            }
            let row = read_native_row(&mut data, manifest.data_size_bytes, entry)?;
            if native_key(&row)? != entry.key {
                return Err(SecBulkError::RecoveryMismatch);
            }
            let table = manifest
                .tables
                .iter()
                .find(|table| table.table == row.table())
                .ok_or(SecBulkError::RecoveryMismatch)?;
            validate_recovered_row(table, &row)?;
            visit_native_lookup_entries(&row, entry, |bucket, lookup| {
                expected_lookups
                    .entry(bucket)
                    .or_insert_with(|| LookupDigestState::new(bucket))
                    .observe(&lookup)
            })?;
            *rows_by_table.entry(row.table()).or_insert(0_u64) += 1;
            hash_field(&mut digest, &entry.payload_digest);
            observed = observed
                .checked_add(1)
                .ok_or(SecBulkError::QueryLimitExceeded)?;
            next_offset = entry
                .payload_offset
                .checked_add(u64::from(entry.payload_length))
                .and_then(|offset| offset.checked_add(4))
                .ok_or(SecBulkError::RecoveryMismatch)?;
            previous_key = Some(entry.key);
        }
    }
    let expected_data_size = if observed == 0 {
        u64::try_from(NATIVE_DATA_MAGIC.len()).map_err(|_| SecBulkError::AllocationFailed)?
    } else {
        next_offset
            .checked_sub(4)
            .ok_or(SecBulkError::RecoveryMismatch)?
    };
    if observed != manifest.emitted_rows
        || observed != manifest.source_rows
        || expected_data_size != manifest.data_size_bytes
        || EvidenceDigest::new(DigestAlgorithm::Sha256, digest.finalize().into())
            != manifest.records_evidence
        || manifest
            .tables
            .iter()
            .any(|table| rows_by_table.get(&table.table).copied().unwrap_or(0) != table.row_count)
    {
        return Err(SecBulkError::RecoveryMismatch);
    }
    verify_lookup_buckets(
        store,
        manifest.family,
        manifest.data_size_bytes,
        &manifest.lookup_buckets,
        manifest.lookup_records,
        &expected_lookups,
        deadline,
        cancellation,
    )?;
    Ok(SecBulkNativePublishedGeneration {
        root_evidence: root.evidence(),
        root_size_bytes: root.size_bytes(),
        manifest,
    })
}

fn validate_manifest(manifest: &NativeGenerationManifest) -> Result<(), SecBulkError> {
    let quarter = SecQuarter::try_new(manifest.quarter_year, manifest.quarter_number)?;
    let catalog = SecBulkCatalogSnapshot::official_2026_08_14()?;
    let schema = SecBulkSchemaIdentity::current(manifest.family)?;
    let expected_coverage_gap = matches!(
        SecBulkCoverage::current(manifest.family, quarter)?,
        SecBulkCoverage::AcceptedSchemaExcluded { .. }
    );
    let now = crate::client::system_timestamp()?;
    let published_at = Timestamp::from_unix_nanos(manifest.published_at_unix_nanos);
    if manifest.version != NATIVE_ROOT_VERSION
        || manifest.catalog_snapshot_date != SEC_BULK_CATALOG_SNAPSHOT_DATE
        || !quarter.is_catalogued(manifest.family, catalog)
        || manifest.accepted_schema_version != schema.version().as_str()
        || manifest.accepted_schema_effective_date != schema.effective_date().to_string()
        || manifest.accepted_schema_locator != schema.technical_spec_locator().as_str()
        || manifest.declared_coverage_gap != expected_coverage_gap
        || !valid_sha256(manifest.manifest_evidence)
        || !valid_sha256(manifest.archive_evidence)
        || !valid_sha256(manifest.readme_evidence)
        || !valid_sha256(manifest.metadata_evidence)
        || !valid_sha256(manifest.archive_readme_evidence)
        || !valid_sha256(manifest.data_evidence)
        || !valid_sha256(manifest.records_evidence)
        || manifest.archive_size_bytes == 0
        || manifest.archive_size_bytes > MAX_BOUND_ARCHIVE_BYTES
        || manifest.readme_size_bytes == 0
        || manifest.readme_size_bytes > MAX_BOUND_README_BYTES
        || manifest.archive_retrieval_revision == 0
        || manifest.readme_retrieval_revision == 0
        || manifest.data_size_bytes < NATIVE_DATA_MAGIC.len() as u64
        || manifest.data_size_bytes > MAX_NATIVE_DATA_BYTES
        || manifest.source_rows != manifest.emitted_rows
        || manifest.emitted_rows > MAX_NATIVE_PUBLICATION_ROWS
        || published_at > now
        || manifest.tables.len()
            != match manifest.family {
                SecBulkFamily::Nport => 30,
                SecBulkFamily::Ncen => 53,
            }
    {
        return Err(SecBulkError::PublicationNotReady);
    }
    validate_transport_lineage(
        &manifest.archive_transport,
        SecBulkMediaKind::Zip,
        SecObjectLocator::quarterly_bulk_archive(manifest.family, quarter)?.url(),
        published_at,
    )?;
    validate_transport_lineage(
        &manifest.readme_transport,
        SecBulkMediaKind::Pdf,
        SecObjectLocator::quarterly_bulk_readme(manifest.family)?.url(),
        published_at,
    )?;

    let mut previous = None;
    let mut table_rows = 0_u64;
    for table in &manifest.tables {
        if table.table.family() != manifest.family
            || previous.is_some_and(|previous| previous >= table.table)
            || table.declared_absent != table.evidence.is_none()
            || (table.declared_absent && table.row_count != 0)
            || table.columns.is_empty()
            || table.evidence.is_some_and(|evidence| {
                evidence.algorithm() != DigestAlgorithm::Sha256
                    || evidence.bytes().iter().all(|byte| *byte == 0)
            })
            || !valid_table_contract(table)
        {
            return Err(SecBulkError::RecoveryMismatch);
        }
        table_rows = table_rows
            .checked_add(table.row_count)
            .ok_or(SecBulkError::QueryLimitExceeded)?;
        previous = Some(table.table);
    }
    let expected_pages = manifest
        .emitted_rows
        .div_ceil(u64::from(NATIVE_INDEX_PAGE_RECORDS));
    if table_rows != manifest.source_rows
        || u64::try_from(manifest.index_pages.len()).map_or(true, |pages| pages != expected_pages)
    {
        return Err(SecBulkError::RecoveryMismatch);
    }
    let mut indexed_rows = 0_u64;
    let mut previous_last: Option<&[u8]> = None;
    for page in &manifest.index_pages {
        if page.evidence.algorithm() != DigestAlgorithm::Sha256
            || page.evidence.bytes().iter().all(|byte| *byte == 0)
            || page.record_count == 0
            || page.record_count > NATIVE_INDEX_PAGE_RECORDS
            || page.first_key.len() != NATIVE_KEY_BYTES
            || page.last_key.len() != NATIVE_KEY_BYTES
            || page.first_key > page.last_key
            || previous_last.is_some_and(|previous| previous >= page.first_key.as_slice())
        {
            return Err(SecBulkError::RecoveryMismatch);
        }
        indexed_rows = indexed_rows
            .checked_add(u64::from(page.record_count))
            .ok_or(SecBulkError::QueryLimitExceeded)?;
        previous_last = Some(&page.last_key);
    }
    if indexed_rows != manifest.emitted_rows {
        return Err(SecBulkError::RecoveryMismatch);
    }
    let mut lookup_records = 0_u64;
    let mut previous_bucket = None;
    for bucket in &manifest.lookup_buckets {
        if bucket.bucket >= NATIVE_LOOKUP_BUCKETS
            || bucket.record_count == 0
            || !valid_sha256(bucket.records_evidence)
            || previous_bucket.is_some_and(|previous| previous >= bucket.bucket)
            || u64::try_from(bucket.pages.len()).map_or(true, |pages| {
                pages
                    != bucket
                        .record_count
                        .div_ceil(u64::from(NATIVE_LOOKUP_PAGE_RECORDS))
            })
        {
            return Err(SecBulkError::RecoveryMismatch);
        }
        let mut page_records = 0_u64;
        for page in &bucket.pages {
            if !valid_lookup_page_descriptor(page) {
                return Err(SecBulkError::RecoveryMismatch);
            }
            page_records = page_records
                .checked_add(u64::from(page.record_count))
                .ok_or(SecBulkError::QueryLimitExceeded)?;
        }
        if page_records != bucket.record_count {
            return Err(SecBulkError::RecoveryMismatch);
        }
        lookup_records = lookup_records
            .checked_add(bucket.record_count)
            .ok_or(SecBulkError::QueryLimitExceeded)?;
        previous_bucket = Some(bucket.bucket);
    }
    if lookup_records != manifest.lookup_records
        || (manifest.lookup_records == 0) != manifest.lookup_buckets.is_empty()
        || manifest
            .lookup_records
            .checked_mul(NATIVE_LOOKUP_ENTRY_BYTES as u64)
            .is_none_or(|bytes| bytes > MAX_NATIVE_LOOKUP_SCRATCH_BYTES)
    {
        return Err(SecBulkError::RecoveryMismatch);
    }
    Ok(())
}

fn valid_table_contract(table: &NativeTableDescriptor) -> bool {
    let mut names = std::collections::BTreeSet::new();
    for column in &table.columns {
        if column.name.is_empty()
            || !names.insert(column.name.as_str())
            || column.max_length == Some(0)
        {
            return false;
        }
        let valid_datatype = match column.datatype_base.as_str() {
            "string" => {
                column.max_length.is_some()
                    && column.data_precision.is_none()
                    && column.data_scale.is_none()
            }
            "date (DD-MON-YYYY)" => {
                column.max_length.is_none()
                    && column.data_precision.is_none()
                    && column.data_scale.is_none()
            }
            "NUMBER" => match (column.data_precision, column.data_scale) {
                (
                    Some(SecBulkNumericAttribute::Value(precision)),
                    Some(SecBulkNumericAttribute::Value(scale)),
                ) => precision > 0 && precision <= 38 && scale <= precision,
                (
                    Some(SecBulkNumericAttribute::ProviderNull),
                    Some(SecBulkNumericAttribute::ProviderNull),
                ) => true,
                _ => false,
            },
            _ => false,
        };
        if !valid_datatype {
            return false;
        }
    }
    let mut primary_names = std::collections::BTreeSet::new();
    table.primary_key.iter().all(|key| {
        !key.is_empty() && primary_names.insert(key.as_str()) && names.contains(key.as_str())
    })
}

fn validate_transport_lineage(
    lineage: &NativeTransportLineage,
    media_kind: SecBulkMediaKind,
    expected_locator: &str,
    published_at: Timestamp,
) -> Result<(), SecBulkError> {
    if lineage.locator != expected_locator {
        return Err(SecBulkError::RecoveryMismatch);
    }
    let validators = SecHttpValidators::try_new(
        lineage.etag.as_deref(),
        lineage.last_modified_header.as_deref(),
    )
    .map_err(|_| SecBulkError::RecoveryMismatch)?;
    let transport = SecBulkTransportEvidence::try_new(
        lineage.http_status,
        media_kind,
        lineage.media_type.as_deref(),
        validators,
        Timestamp::from_unix_nanos(lineage.body_received_at_unix_nanos),
    )?;
    if transport.last_modified_at().map(Timestamp::unix_nanos)
        != lineage.last_modified_at_unix_nanos
        || transport.body_received_at()
            > Timestamp::from_unix_nanos(lineage.first_observed_at_unix_nanos)
        || Timestamp::from_unix_nanos(lineage.first_observed_at_unix_nanos) > published_at
    {
        return Err(SecBulkError::RecoveryMismatch);
    }
    Ok(())
}

fn generation_tables(
    layout: &SecBulkLayoutManifest,
) -> Result<Vec<NativeTableDescriptor>, SecBulkError> {
    let family = layout.capture().selection().family();
    let mut tables = Vec::new();
    tables
        .try_reserve_exact(layout.declared_table_contracts().len())
        .map_err(|_| SecBulkError::AllocationFailed)?;
    for contract in layout.declared_table_contracts() {
        let receipt = layout.table(contract.name().as_str());
        let declared_absent = receipt.is_none();
        if declared_absent
            != layout
                .absent_declared_tables()
                .iter()
                .any(|name| name == contract.name())
        {
            return Err(SecBulkError::InvalidLayout);
        }
        tables.push(NativeTableDescriptor {
            table: SecBulkTableKind::from_member(family, contract.name().as_str())?,
            evidence: receipt.map(|receipt| receipt.evidence()),
            row_count: receipt.map_or(0, |receipt| receipt.row_count()),
            declared_absent,
            primary_key: contract
                .primary_key()
                .iter()
                .map(|field| field.as_str().to_owned())
                .collect(),
            columns: contract
                .columns()
                .iter()
                .map(|column| NativeColumnDescriptor {
                    name: column.name().as_str().to_owned(),
                    datatype_base: column.datatype_base().to_owned(),
                    max_length: column.max_length(),
                    data_precision: column.data_precision(),
                    data_scale: column.data_scale(),
                    required: column.required(),
                })
                .collect(),
        });
    }
    Ok(tables)
}

fn validate_native_row(
    layout: &SecBulkLayoutManifest,
    row: &SecBulkNativeRow,
) -> Result<(), SecBulkError> {
    if row.table().family() != layout.capture().selection().family()
        || row.row_number() == 0
        || row.row_evidence().algorithm() != DigestAlgorithm::Sha256
        || row.row_evidence().bytes().iter().all(|byte| *byte == 0)
    {
        return Err(SecBulkError::InvalidCanonicalMapping);
    }
    let receipt = layout
        .table(row.table().member_name())
        .ok_or(SecBulkError::InvalidCanonicalMapping)?;
    if row.row_number() > receipt.row_count()
        || row.fields().len() != receipt.columns().len()
        || row.primary_key().len() != receipt.primary_key().len()
    {
        return Err(SecBulkError::InvalidCanonicalMapping);
    }
    for (field, column) in row.fields().iter().zip(receipt.columns()) {
        if field.name() != column.name()
            || !valid_typed_value(
                field.value(),
                column.datatype_base(),
                column.max_length(),
                column.data_precision(),
                column.data_scale(),
                column.required(),
            )
        {
            return Err(SecBulkError::InvalidCanonicalMapping);
        }
    }
    for (key, expected) in row.primary_key().iter().zip(receipt.primary_key()) {
        let field = row
            .fields()
            .iter()
            .find(|field| field.name() == expected)
            .ok_or(SecBulkError::InvalidCanonicalMapping)?;
        if key.name() != expected || !key_matches_value(key.value(), field.value()) {
            return Err(SecBulkError::InvalidCanonicalMapping);
        }
    }
    if row.membership().is_some()
        || row.joins() != expected_joins(row.fields())?.as_slice()
        || row.projection_disposition()
            != &super::archive::provider_projection_disposition_from_native(row)?
        || row.row_evidence() != native_row_evidence(row)
    {
        return Err(SecBulkError::InvalidCanonicalMapping);
    }
    Ok(())
}

fn validate_recovered_row(
    table: &NativeTableDescriptor,
    row: &SecBulkNativeRow,
) -> Result<(), SecBulkError> {
    if table.declared_absent
        || table.table != row.table()
        || row.row_number() == 0
        || row.row_number() > table.row_count
        || row.row_evidence().bytes().iter().all(|byte| *byte == 0)
    {
        return Err(SecBulkError::RecoveryMismatch);
    }
    if row.fields().len() != table.columns.len()
        || row.primary_key().len() != table.primary_key.len()
    {
        return Err(SecBulkError::RecoveryMismatch);
    }
    for (field, column) in row.fields().iter().zip(&table.columns) {
        if field.name().as_str() != column.name
            || !valid_typed_value(
                field.value(),
                &column.datatype_base,
                column.max_length,
                column.data_precision,
                column.data_scale,
                column.required,
            )
        {
            return Err(SecBulkError::RecoveryMismatch);
        }
    }
    for (key, expected) in row.primary_key().iter().zip(&table.primary_key) {
        let field = row
            .fields()
            .iter()
            .find(|field| field.name().as_str() == expected)
            .ok_or(SecBulkError::RecoveryMismatch)?;
        if key.name().as_str() != expected || !key_matches_value(key.value(), field.value()) {
            return Err(SecBulkError::RecoveryMismatch);
        }
    }
    if row.membership().is_some()
        || row.joins() != expected_joins(row.fields())?.as_slice()
        || row.projection_disposition()
            != &super::archive::provider_projection_disposition_from_native(row)?
        || row.row_evidence() != native_row_evidence(row)
    {
        return Err(SecBulkError::RecoveryMismatch);
    }
    Ok(())
}

fn valid_typed_value(
    value: &SecBulkTypedValue,
    datatype: &str,
    max_length: Option<u64>,
    precision: Option<SecBulkNumericAttribute>,
    scale: Option<SecBulkNumericAttribute>,
    required: bool,
) -> bool {
    let Some(lexical) = typed_lexical(value) else {
        return !required;
    };
    if max_length.is_some_and(|maximum| {
        u64::try_from(lexical.chars().count()).map_or(true, |observed| observed > maximum)
    }) {
        return false;
    }
    match value {
        SecBulkTypedValue::Missing => false,
        SecBulkTypedValue::Text(_) => datatype == "string",
        SecBulkTypedValue::Date(_) => datatype == "date (DD-MON-YYYY)",
        SecBulkTypedValue::Number(number) => {
            datatype == "NUMBER" && valid_fixed_number(number.as_str(), precision, scale)
        }
    }
}

fn key_matches_value(key: &str, value: &SecBulkTypedValue) -> bool {
    typed_lexical(value).map_or_else(|| key.is_empty(), |lexical| lexical == key)
}

fn valid_fixed_number(
    value: &str,
    precision: Option<SecBulkNumericAttribute>,
    scale: Option<SecBulkNumericAttribute>,
) -> bool {
    let unsigned = value.strip_prefix('-').unwrap_or(value);
    if unsigned.is_empty() || value.starts_with('+') {
        return false;
    }
    let mut parts = unsigned.split('.');
    let integer = parts.next().unwrap_or_default();
    let fraction = parts.next();
    if parts.next().is_some()
        || (integer.is_empty() && fraction.is_none_or(str::is_empty))
        || !integer.bytes().all(|byte| byte.is_ascii_digit())
        || fraction.is_some_and(|digits| {
            digits.is_empty() || !digits.bytes().all(|byte| byte.is_ascii_digit())
        })
    {
        return false;
    }
    match (precision, scale) {
        (
            Some(SecBulkNumericAttribute::Value(maximum_digits)),
            Some(SecBulkNumericAttribute::Value(maximum_scale)),
        ) => {
            let fractional_digits = fraction.map_or(0, str::len);
            let Some(total_digits) = integer.len().checked_add(fractional_digits) else {
                return false;
            };
            u64::try_from(total_digits).is_ok_and(|digits| digits <= maximum_digits)
                && u64::try_from(fractional_digits).is_ok_and(|digits| digits <= maximum_scale)
        }
        (
            Some(SecBulkNumericAttribute::ProviderNull),
            Some(SecBulkNumericAttribute::ProviderNull),
        ) => true,
        _ => false,
    }
}

fn expected_joins(
    fields: &[SecBulkTypedField],
) -> Result<Vec<SecBulkJoinCoordinate>, SecBulkError> {
    let mut joins = Vec::new();
    joins
        .try_reserve_exact(11)
        .map_err(|_| SecBulkError::AllocationFailed)?;
    for (column_name, domain) in [
        ("ACCESSION_NUMBER", SecBulkJoinDomain::Accession),
        ("HOLDING_ID", SecBulkJoinDomain::Holding),
        ("FUND_ID", SecBulkJoinDomain::Fund),
        ("SERIES_ID", SecBulkJoinDomain::Series),
        ("CIK", SecBulkJoinDomain::RegistrantCik),
        ("CLASS_ID", SecBulkJoinDomain::ShareClass),
        ("DIRECTOR_SEQNUM", SecBulkJoinDomain::NcenDirectorSequence),
        (
            "CCO_SEQNUM",
            SecBulkJoinDomain::NcenComplianceOfficerSequence,
        ),
        (
            "VALUATION_METHOD_CHANGE_SEQNUM",
            SecBulkJoinDomain::NcenValuationChangeSequence,
        ),
        (
            "SECURITY_LENDING_SEQNUM",
            SecBulkJoinDomain::NcenSecurityLendingSequence,
        ),
        (
            "LINE_OF_CREDIT_SEQNUM",
            SecBulkJoinDomain::NcenLineOfCreditSequence,
        ),
    ] {
        let Some(field) = fields
            .iter()
            .find(|field| field.name().as_str() == column_name)
        else {
            continue;
        };
        let Some(value) = typed_lexical(field.value()) else {
            continue;
        };
        joins.push(SecBulkJoinCoordinate {
            domain,
            column: SourceIdentifier::try_from(column_name)?,
            value,
        });
    }
    Ok(joins)
}

fn join_domain_for_column(name: &str) -> Option<SecBulkJoinDomain> {
    match name {
        "ACCESSION_NUMBER" => Some(SecBulkJoinDomain::Accession),
        "HOLDING_ID" => Some(SecBulkJoinDomain::Holding),
        "FUND_ID" => Some(SecBulkJoinDomain::Fund),
        "SERIES_ID" => Some(SecBulkJoinDomain::Series),
        "CIK" => Some(SecBulkJoinDomain::RegistrantCik),
        "CLASS_ID" => Some(SecBulkJoinDomain::ShareClass),
        "DIRECTOR_SEQNUM" => Some(SecBulkJoinDomain::NcenDirectorSequence),
        "CCO_SEQNUM" => Some(SecBulkJoinDomain::NcenComplianceOfficerSequence),
        "VALUATION_METHOD_CHANGE_SEQNUM" => Some(SecBulkJoinDomain::NcenValuationChangeSequence),
        "SECURITY_LENDING_SEQNUM" => Some(SecBulkJoinDomain::NcenSecurityLendingSequence),
        "LINE_OF_CREDIT_SEQNUM" => Some(SecBulkJoinDomain::NcenLineOfCreditSequence),
        _ => None,
    }
}

fn native_row_evidence(row: &SecBulkNativeRow) -> EvidenceDigest {
    let mut digest = Sha256::new();
    digest.update(b"market-squawk/sec-bulk-decoded-row/v1");
    hash_field(&mut digest, row.table().member_name().as_bytes());
    hash_field(&mut digest, &row.row_number().to_be_bytes());
    for field in row.fields() {
        hash_field(
            &mut digest,
            typed_lexical(field.value())
                .as_deref()
                .unwrap_or_default()
                .as_bytes(),
        );
    }
    EvidenceDigest::new(DigestAlgorithm::Sha256, digest.finalize().into())
}

fn typed_lexical(value: &SecBulkTypedValue) -> Option<String> {
    match value {
        SecBulkTypedValue::Missing => None,
        SecBulkTypedValue::Text(value) => Some(value.clone()),
        SecBulkTypedValue::Date(value) => Some(value.format("%d-%b-%Y").to_string().to_uppercase()),
        SecBulkTypedValue::Number(value) => Some(value.as_str().to_owned()),
    }
}

fn table_presence(table: &NativeTableDescriptor) -> Result<SecBulkTablePresence, SecBulkError> {
    match (table.declared_absent, table.evidence, table.row_count) {
        (true, None, 0) => Ok(SecBulkTablePresence::DeclaredAbsent),
        (false, Some(evidence), 0) => Ok(SecBulkTablePresence::PresentEmpty { evidence }),
        (false, Some(evidence), row_count) => Ok(SecBulkTablePresence::PresentRows {
            evidence,
            row_count,
        }),
        _ => Err(SecBulkError::RecoveryMismatch),
    }
}

#[derive(Clone, Copy)]
struct NativeIndexEntry {
    key: [u8; NATIVE_KEY_BYTES],
    payload_offset: u64,
    payload_length: u32,
    payload_digest: [u8; 32],
}

#[derive(Clone, Copy)]
struct NativeLookupEntry {
    table_ordinal: u16,
    selector: u8,
    query_digest: [u8; 32],
    payload_offset: u64,
    payload_length: u32,
    payload_digest: [u8; 32],
}

impl NativeLookupEntry {
    fn encode(self) -> [u8; NATIVE_LOOKUP_ENTRY_BYTES] {
        let mut encoded = [0_u8; NATIVE_LOOKUP_ENTRY_BYTES];
        encoded[..2].copy_from_slice(&self.table_ordinal.to_be_bytes());
        encoded[2] = self.selector;
        encoded[3..35].copy_from_slice(&self.query_digest);
        encoded[35..43].copy_from_slice(&self.payload_offset.to_be_bytes());
        encoded[43..47].copy_from_slice(&self.payload_length.to_be_bytes());
        encoded[47..].copy_from_slice(&self.payload_digest);
        encoded
    }

    fn decode(encoded: &[u8; NATIVE_LOOKUP_ENTRY_BYTES]) -> Result<Self, SecBulkError> {
        let selector = encoded[2];
        if !valid_lookup_selector(selector) {
            return Err(SecBulkError::RecoveryMismatch);
        }
        Ok(Self {
            table_ordinal: u16::from_be_bytes(
                encoded[..2]
                    .try_into()
                    .map_err(|_| SecBulkError::RecoveryMismatch)?,
            ),
            selector,
            query_digest: encoded[3..35]
                .try_into()
                .map_err(|_| SecBulkError::RecoveryMismatch)?,
            payload_offset: u64::from_be_bytes(
                encoded[35..43]
                    .try_into()
                    .map_err(|_| SecBulkError::RecoveryMismatch)?,
            ),
            payload_length: u32::from_be_bytes(
                encoded[43..47]
                    .try_into()
                    .map_err(|_| SecBulkError::RecoveryMismatch)?,
            ),
            payload_digest: encoded[47..]
                .try_into()
                .map_err(|_| SecBulkError::RecoveryMismatch)?,
        })
    }

    const fn data_entry(self) -> NativeIndexEntry {
        NativeIndexEntry {
            key: [0_u8; NATIVE_KEY_BYTES],
            payload_offset: self.payload_offset,
            payload_length: self.payload_length,
            payload_digest: self.payload_digest,
        }
    }
}

fn visit_native_lookup_entries(
    row: &SecBulkNativeRow,
    data_entry: NativeIndexEntry,
    mut emit: impl FnMut(u16, NativeLookupEntry) -> Result<(), SecBulkError>,
) -> Result<(), SecBulkError> {
    if !row.primary_key().is_empty() {
        let selector = 0;
        let query_digest = lookup_digest_primary_key(row.table(), row.primary_key());
        emit(
            lookup_bucket(query_digest),
            NativeLookupEntry {
                table_ordinal: row.table().ordinal(),
                selector,
                query_digest,
                payload_offset: data_entry.payload_offset,
                payload_length: data_entry.payload_length,
                payload_digest: data_entry.payload_digest,
            },
        )?;
    }
    for join in row.joins() {
        let selector = join_selector_tag(join.domain());
        let query_digest = lookup_digest_join(row.table(), join.domain(), join.value());
        emit(
            lookup_bucket(query_digest),
            NativeLookupEntry {
                table_ordinal: row.table().ordinal(),
                selector,
                query_digest,
                payload_offset: data_entry.payload_offset,
                payload_length: data_entry.payload_length,
                payload_digest: data_entry.payload_digest,
            },
        )?;
    }
    Ok(())
}

fn lookup_digest_primary_key(table: SecBulkTableKind, primary_key: &[SecBulkKeyField]) -> [u8; 32] {
    let mut digest = lookup_digest_prefix(table, 0);
    for field in primary_key {
        hash_field(&mut digest, field.name().as_str().as_bytes());
        hash_field(&mut digest, field.value().as_bytes());
    }
    digest.finalize().into()
}

fn lookup_digest_join(table: SecBulkTableKind, domain: SecBulkJoinDomain, value: &str) -> [u8; 32] {
    let selector = join_selector_tag(domain);
    let mut digest = lookup_digest_prefix(table, selector);
    hash_field(&mut digest, value.as_bytes());
    digest.finalize().into()
}

fn lookup_digest_prefix(table: SecBulkTableKind, selector: u8) -> Sha256 {
    let mut digest = Sha256::new();
    digest.update(b"market-squawk/sec-bulk-native-lookup-key/v1");
    hash_field(&mut digest, &table.ordinal().to_be_bytes());
    hash_field(&mut digest, &[selector]);
    digest
}

const fn lookup_bucket(digest: [u8; 32]) -> u16 {
    (u16::from_be_bytes([digest[0], digest[1]]) >> 4) & (NATIVE_LOOKUP_BUCKETS - 1)
}

const fn join_selector_tag(domain: SecBulkJoinDomain) -> u8 {
    match domain {
        SecBulkJoinDomain::Accession => 1,
        SecBulkJoinDomain::Holding => 2,
        SecBulkJoinDomain::Fund => 3,
        SecBulkJoinDomain::Series => 4,
        SecBulkJoinDomain::RegistrantCik => 5,
        SecBulkJoinDomain::ShareClass => 6,
        SecBulkJoinDomain::NcenDirectorSequence => 7,
        SecBulkJoinDomain::NcenComplianceOfficerSequence => 8,
        SecBulkJoinDomain::NcenValuationChangeSequence => 9,
        SecBulkJoinDomain::NcenSecurityLendingSequence => 10,
        SecBulkJoinDomain::NcenLineOfCreditSequence => 11,
    }
}

const fn valid_lookup_selector(selector: u8) -> bool {
    selector <= 11
}

fn projected_lookup_scratch_bytes(layout: &SecBulkLayoutManifest) -> Result<u64, SecBulkError> {
    let mut records = 0_u64;
    for contract in layout.declared_table_contracts() {
        let rows = layout
            .table(contract.name().as_str())
            .map_or(0, |receipt| receipt.row_count());
        let primary_key_count = if contract.primary_key().is_empty() {
            0_u64
        } else {
            1_u64
        };
        let lookup_keys = primary_key_count
            .checked_add(
                u64::try_from(
                    contract
                        .columns()
                        .iter()
                        .filter(|column| join_domain_for_column(column.name().as_str()).is_some())
                        .count(),
                )
                .map_err(|_| SecBulkError::AllocationFailed)?,
            )
            .ok_or(SecBulkError::ScratchLimitExceeded)?;
        records = records
            .checked_add(
                rows.checked_mul(lookup_keys)
                    .ok_or(SecBulkError::ScratchLimitExceeded)?,
            )
            .ok_or(SecBulkError::ScratchLimitExceeded)?;
    }
    records
        .checked_mul(
            u64::try_from(NATIVE_LOOKUP_ENTRY_BYTES).map_err(|_| SecBulkError::AllocationFailed)?,
        )
        .ok_or(SecBulkError::ScratchLimitExceeded)
}

fn native_key(row: &SecBulkNativeRow) -> Result<[u8; NATIVE_KEY_BYTES], SecBulkError> {
    let mut key = [0_u8; NATIVE_KEY_BYTES];
    key[..2].copy_from_slice(&row.table().ordinal().to_be_bytes());
    key[2..10].copy_from_slice(&row.row_number().to_be_bytes());
    key[10..].copy_from_slice(&row.row_evidence().bytes());
    Ok(key)
}

fn start_index_page(page: &mut Vec<u8>) {
    page.extend_from_slice(NATIVE_INDEX_MAGIC);
    page.extend_from_slice(&0_u32.to_be_bytes());
}

fn parse_index_page(bytes: &[u8], expected: u32) -> Result<Vec<NativeIndexEntry>, SecBulkError> {
    let header = NATIVE_INDEX_MAGIC.len() + 4;
    if bytes.len() < header || &bytes[..NATIVE_INDEX_MAGIC.len()] != NATIVE_INDEX_MAGIC {
        return Err(SecBulkError::RecoveryMismatch);
    }
    let observed = u32::from_be_bytes(
        bytes[NATIVE_INDEX_MAGIC.len()..header]
            .try_into()
            .map_err(|_| SecBulkError::RecoveryMismatch)?,
    );
    if observed != expected
        || observed == 0
        || bytes.len() != header + observed as usize * NATIVE_INDEX_ENTRY_BYTES
    {
        return Err(SecBulkError::RecoveryMismatch);
    }
    let mut entries = Vec::new();
    entries
        .try_reserve_exact(observed as usize)
        .map_err(|_| SecBulkError::AllocationFailed)?;
    for chunk in bytes[header..].chunks_exact(NATIVE_INDEX_ENTRY_BYTES) {
        let key = chunk[..NATIVE_KEY_BYTES]
            .try_into()
            .map_err(|_| SecBulkError::RecoveryMismatch)?;
        let offset = NATIVE_KEY_BYTES;
        entries.push(NativeIndexEntry {
            key,
            payload_offset: u64::from_be_bytes(
                chunk[offset..offset + 8]
                    .try_into()
                    .map_err(|_| SecBulkError::RecoveryMismatch)?,
            ),
            payload_length: u32::from_be_bytes(
                chunk[offset + 8..offset + 12]
                    .try_into()
                    .map_err(|_| SecBulkError::RecoveryMismatch)?,
            ),
            payload_digest: chunk[offset + 12..]
                .try_into()
                .map_err(|_| SecBulkError::RecoveryMismatch)?,
        });
    }
    Ok(entries)
}

fn read_verified_page(
    store: &RawEvidenceStore,
    descriptor: &NativeIndexPageDescriptor,
    deadline: Timestamp,
    cancellation: &CancellationToken,
) -> Result<Vec<u8>, SecBulkError> {
    let record_count =
        usize::try_from(descriptor.record_count).map_err(|_| SecBulkError::AllocationFailed)?;
    let expected_size = NATIVE_INDEX_ENTRY_BYTES
        .checked_mul(record_count)
        .and_then(|body| body.checked_add(NATIVE_INDEX_MAGIC.len() + 4))
        .and_then(|size| u64::try_from(size).ok())
        .ok_or(SecBulkError::AllocationFailed)?;
    if descriptor.evidence.algorithm() != DigestAlgorithm::Sha256
        || descriptor.record_count == 0
        || descriptor.record_count > NATIVE_INDEX_PAGE_RECORDS
        || descriptor.size_bytes != expected_size
        || descriptor.first_key.len() != NATIVE_KEY_BYTES
        || descriptor.last_key.len() != NATIVE_KEY_BYTES
        || descriptor.first_key > descriptor.last_key
    {
        return Err(SecBulkError::RecoveryMismatch);
    }
    let maximum = (NATIVE_INDEX_MAGIC.len()
        + 4
        + NATIVE_INDEX_ENTRY_BYTES * NATIVE_INDEX_PAGE_RECORDS as usize) as u64;
    let mut file = store.open_verified_before(
        &descriptor.evidence,
        descriptor.size_bytes,
        maximum,
        deadline,
        cancellation,
    )?;
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(
            usize::try_from(descriptor.size_bytes).map_err(|_| SecBulkError::AllocationFailed)?,
        )
        .map_err(|_| SecBulkError::AllocationFailed)?;
    file.read_to_end(&mut bytes)?;
    Ok(bytes)
}

fn valid_lookup_page_descriptor(descriptor: &NativeLookupPageDescriptor) -> bool {
    let Some(expected_size) = NATIVE_LOOKUP_ENTRY_BYTES
        .checked_mul(descriptor.record_count as usize)
        .and_then(|body| body.checked_add(NATIVE_LOOKUP_MAGIC.len() + 2 + 4))
        .and_then(|size| u64::try_from(size).ok())
    else {
        return false;
    };
    valid_sha256(descriptor.evidence)
        && descriptor.record_count != 0
        && descriptor.record_count <= NATIVE_LOOKUP_PAGE_RECORDS
        && descriptor.size_bytes == expected_size
}

fn read_verified_lookup_page(
    store: &RawEvidenceStore,
    descriptor: &NativeLookupPageDescriptor,
    deadline: Timestamp,
    cancellation: &CancellationToken,
) -> Result<Vec<u8>, SecBulkError> {
    if !valid_lookup_page_descriptor(descriptor) {
        return Err(SecBulkError::RecoveryMismatch);
    }
    let maximum = NATIVE_LOOKUP_ENTRY_BYTES
        .checked_mul(NATIVE_LOOKUP_PAGE_RECORDS as usize)
        .and_then(|body| body.checked_add(NATIVE_LOOKUP_MAGIC.len() + 2 + 4))
        .and_then(|size| u64::try_from(size).ok())
        .ok_or(SecBulkError::AllocationFailed)?;
    let mut file = store.open_verified_before(
        &descriptor.evidence,
        descriptor.size_bytes,
        maximum,
        deadline,
        cancellation,
    )?;
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(
            usize::try_from(descriptor.size_bytes).map_err(|_| SecBulkError::AllocationFailed)?,
        )
        .map_err(|_| SecBulkError::AllocationFailed)?;
    file.read_to_end(&mut bytes)?;
    check_operation(deadline, cancellation)?;
    Ok(bytes)
}

fn parse_lookup_page(
    bytes: &[u8],
    expected_bucket: u16,
    expected_records: u32,
) -> Result<Vec<NativeLookupEntry>, SecBulkError> {
    let header = NATIVE_LOOKUP_MAGIC.len() + 2 + 4;
    if bytes.len() < header || &bytes[..NATIVE_LOOKUP_MAGIC.len()] != NATIVE_LOOKUP_MAGIC {
        return Err(SecBulkError::RecoveryMismatch);
    }
    let bucket_offset = NATIVE_LOOKUP_MAGIC.len();
    let bucket = u16::from_be_bytes(
        bytes[bucket_offset..bucket_offset + 2]
            .try_into()
            .map_err(|_| SecBulkError::RecoveryMismatch)?,
    );
    let count_offset = bucket_offset + 2;
    let count = u32::from_be_bytes(
        bytes[count_offset..header]
            .try_into()
            .map_err(|_| SecBulkError::RecoveryMismatch)?,
    );
    if bucket != expected_bucket
        || count != expected_records
        || count == 0
        || bytes.len()
            != NATIVE_LOOKUP_ENTRY_BYTES
                .checked_mul(count as usize)
                .and_then(|body| body.checked_add(header))
                .ok_or(SecBulkError::RecoveryMismatch)?
    {
        return Err(SecBulkError::RecoveryMismatch);
    }
    let mut entries = Vec::new();
    entries
        .try_reserve_exact(count as usize)
        .map_err(|_| SecBulkError::AllocationFailed)?;
    for chunk in bytes[header..].chunks_exact(NATIVE_LOOKUP_ENTRY_BYTES) {
        let encoded: &[u8; NATIVE_LOOKUP_ENTRY_BYTES] = chunk
            .try_into()
            .map_err(|_| SecBulkError::RecoveryMismatch)?;
        entries.push(NativeLookupEntry::decode(encoded)?);
    }
    Ok(entries)
}

#[allow(clippy::too_many_arguments)]
fn verify_lookup_buckets(
    store: &RawEvidenceStore,
    family: SecBulkFamily,
    data_size_bytes: u64,
    descriptors: &[NativeLookupBucketDescriptor],
    expected_records: u64,
    expected: &BTreeMap<u16, LookupDigestState>,
    deadline: Timestamp,
    cancellation: &CancellationToken,
) -> Result<(), SecBulkError> {
    if descriptors.len() != expected.len() {
        return Err(SecBulkError::RecoveryMismatch);
    }
    let mut observed_records = 0_u64;
    for descriptor in descriptors {
        check_operation(deadline, cancellation)?;
        let expected_bucket = expected
            .get(&descriptor.bucket)
            .ok_or(SecBulkError::RecoveryMismatch)?;
        if expected_bucket.records != descriptor.record_count
            || expected_bucket.evidence() != descriptor.records_evidence
        {
            return Err(SecBulkError::RecoveryMismatch);
        }
        let mut observed = LookupDigestState::new(descriptor.bucket);
        for page in &descriptor.pages {
            let bytes = read_verified_lookup_page(store, page, deadline, cancellation)?;
            for entry in parse_lookup_page(&bytes, descriptor.bucket, page.record_count)? {
                check_operation(deadline, cancellation)?;
                let family_matches = match family {
                    SecBulkFamily::Nport => entry.table_ordinal < 30,
                    SecBulkFamily::Ncen => (30..83).contains(&entry.table_ordinal),
                };
                if !family_matches
                    || lookup_bucket(entry.query_digest) != descriptor.bucket
                    || !entry_fits_data(entry.data_entry(), data_size_bytes)
                {
                    return Err(SecBulkError::RecoveryMismatch);
                }
                observed.observe(&entry)?;
            }
        }
        if observed.records != descriptor.record_count
            || observed.evidence() != descriptor.records_evidence
        {
            return Err(SecBulkError::RecoveryMismatch);
        }
        observed_records = observed_records
            .checked_add(observed.records)
            .ok_or(SecBulkError::QueryLimitExceeded)?;
    }
    if observed_records != expected_records {
        return Err(SecBulkError::RecoveryMismatch);
    }
    Ok(())
}

fn read_native_row(
    data: &mut std::fs::File,
    data_size_bytes: u64,
    entry: NativeIndexEntry,
) -> Result<SecBulkNativeRow, SecBulkError> {
    let length =
        usize::try_from(entry.payload_length).map_err(|_| SecBulkError::QueryLimitExceeded)?;
    if length == 0 || length > MAX_NATIVE_RECORD_BYTES || !entry_fits_data(entry, data_size_bytes) {
        return Err(SecBulkError::RecoveryMismatch);
    }
    data.seek(SeekFrom::Start(
        entry
            .payload_offset
            .checked_sub(4)
            .ok_or(SecBulkError::RecoveryMismatch)?,
    ))?;
    let mut encoded_length = [0_u8; 4];
    data.read_exact(&mut encoded_length)?;
    if u32::from_be_bytes(encoded_length) != entry.payload_length {
        return Err(SecBulkError::RecoveryMismatch);
    }
    let mut payload = vec![0_u8; length];
    data.read_exact(&mut payload)?;
    if Sha256::digest(&payload).as_slice() != entry.payload_digest {
        return Err(SecBulkError::RecoveryMismatch);
    }
    serde_json::from_slice(&payload).map_err(|_| SecBulkError::RecoveryMismatch)
}

fn bind_query_membership(
    row: &mut SecBulkNativeRow,
    generation: &SecBulkNativePublishedGeneration,
    query_evidence: EvidenceDigest,
) -> Result<(), SecBulkError> {
    row.bind_membership(SecBulkNativeRowMembership {
        generation_evidence: generation.evidence(),
        manifest_evidence: generation.manifest_evidence(),
        query_evidence,
        provider_published_at: generation
            .manifest
            .archive_transport
            .last_modified_at_unix_nanos
            .map(Timestamp::from_unix_nanos),
        first_observed_at: Timestamp::from_unix_nanos(
            generation
                .manifest
                .archive_transport
                .first_observed_at_unix_nanos,
        ),
        generation_published_at: generation.published_at(),
        table: row.table(),
        row_number: row.row_number(),
        row_evidence: row.row_evidence(),
    })
}

fn entry_fits_data(entry: NativeIndexEntry, data_size_bytes: u64) -> bool {
    entry.payload_offset >= NATIVE_DATA_MAGIC.len() as u64 + 4
        && entry
            .payload_offset
            .checked_add(u64::from(entry.payload_length))
            .is_some_and(|end| end <= data_size_bytes)
}

fn native_query_digest(
    generation: EvidenceDigest,
    table: SecBulkTableKind,
    predicate: &NativeQueryPredicate<'_>,
) -> EvidenceDigest {
    let mut digest = Sha256::new();
    digest.update(b"market-squawk/sec-bulk-native-query/v3");
    hash_field(&mut digest, &generation.bytes());
    hash_field(&mut digest, &table.ordinal().to_be_bytes());
    match predicate {
        NativeQueryPredicate::All => hash_field(&mut digest, b"all"),
        NativeQueryPredicate::PrimaryKey(primary_key) => {
            hash_field(&mut digest, b"primary-key");
            for key in *primary_key {
                hash_field(&mut digest, key.name().as_str().as_bytes());
                hash_field(&mut digest, key.value().as_bytes());
            }
        }
        NativeQueryPredicate::Joins(joins) => {
            hash_field(&mut digest, b"joins");
            for join in *joins {
                hash_field(&mut digest, &[join_selector_tag(join.domain)]);
                hash_field(&mut digest, join.value().as_bytes());
            }
        }
    }
    EvidenceDigest::new(DigestAlgorithm::Sha256, digest.finalize().into())
}

fn records_digest_prefix(manifest: EvidenceDigest) -> Sha256 {
    let mut digest = Sha256::new();
    digest.update(b"market-squawk/sec-bulk-native-records/v3");
    hash_field(&mut digest, &manifest.bytes());
    digest
}

fn transport_lineage(capture: &SecBulkCapture) -> NativeTransportLineage {
    let transport = capture.transport();
    NativeTransportLineage {
        locator: capture.locator().as_str().to_owned(),
        http_status: transport.http_status(),
        media_type: transport.media_type().map(str::to_owned),
        etag: transport.validators().etag().map(str::to_owned),
        last_modified_header: transport.validators().last_modified().map(str::to_owned),
        last_modified_at_unix_nanos: transport.last_modified_at().map(Timestamp::unix_nanos),
        body_received_at_unix_nanos: transport.body_received_at().unix_nanos(),
        first_observed_at_unix_nanos: capture.first_observed_at().unix_nanos(),
    }
}

fn persist_bounded_content(
    store: &RawEvidenceStore,
    bytes: &[u8],
    maximum: u64,
    deadline: Timestamp,
    cancellation: &CancellationToken,
) -> Result<RawEvidenceReceipt, SecBulkError> {
    check_operation(deadline, cancellation)?;
    let mut writer = store.create_content_writer(maximum, deadline, cancellation)?;
    writer.write_bytes(bytes, cancellation)?;
    let receipt = writer.seal(cancellation)?;
    check_operation(deadline, cancellation)?;
    Ok(receipt)
}

fn check_operation(
    deadline: Timestamp,
    cancellation: &CancellationToken,
) -> Result<(), SecBulkError> {
    if cancellation.is_cancelled() {
        return Err(SecBulkError::Cancelled);
    }
    if crate::client::system_timestamp()? >= deadline {
        return Err(SecBulkError::DeadlineExceeded);
    }
    Ok(())
}

fn hash_field(digest: &mut Sha256, value: &[u8]) {
    digest.update(u64::try_from(value.len()).unwrap_or(u64::MAX).to_be_bytes());
    digest.update(value);
}

fn valid_sha256(evidence: EvidenceDigest) -> bool {
    evidence.algorithm() == DigestAlgorithm::Sha256
        && evidence.bytes().iter().any(|byte| *byte != 0)
}
