//! Durable content-addressed publication, restart verification, and indexed point-in-time queries.

use std::cmp::Ordering;
use std::fmt;
use std::io::{Read, Seek, SeekFrom};
use std::str::FromStr as _;

use chrono::{Datelike as _, NaiveDate};
use market_squawk_domain::{
    DigestAlgorithm, EvidenceDigest, InstrumentId, SourceIdentifier, Timestamp,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest as _, Sha256};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::RawEvidenceStore;
use crate::evidence_store::{RawEvidenceContentWriter, RawEvidenceReceipt};

use super::model::{
    SecBulkCandidatePublicationPermit, SecBulkCoverage, SecBulkFamily, SecBulkLayoutManifest,
    SecFilingChronology, SecFundHoldingCandidate, SecFundHoldingCandidatesQuery,
    SecHoldingResolutionState, SecNcenFundMetadataCandidate, SecNcenFundMetadataQuery,
};
use super::native_query::{SecBulkNativeGenerationReceipt, recover_native_generation_from_receipt};
use super::{SecBulkError, SecBulkScanReport};

const MAX_QUERY_SCAN_RECORDS: u64 = 5_000_000;
const MAX_QUERY_RESULTS: usize = 100_000;
const MAX_PUBLICATION_RECORDS: u64 = 250_000_000;
const MAX_RECORD_BYTES: usize = 4 * 1024 * 1024;
const MAX_GENERATION_DATA_BYTES: u64 = 1024 * 1024 * 1024 * 1024;
const MAX_GENERATION_MANIFEST_BYTES: u64 = 64 * 1024 * 1024;
const MAX_BOUND_ARCHIVE_BYTES: u64 = 1024 * 1024 * 1024;
const MAX_BOUND_README_BYTES: u64 = 16 * 1024 * 1024;
const INDEX_PAGE_RECORDS: u32 = 4_096;
const INDEX_KEY_BYTES: usize = 80;
const INDEX_ENTRY_BYTES: usize = INDEX_KEY_BYTES + 8 + 4 + 32;
const DATA_MAGIC: &[u8] = b"MSSEC-DATA-v2\n";
const INDEX_MAGIC: &[u8] = b"MSSEC-IDX-v2\n";
const GENERATION_VERSION: u8 = 3;

/// Hard bounds applied to one provider-local point-in-time query.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SecBulkQueryLimits {
    max_scanned_records: u64,
    max_results: usize,
}

impl SecBulkQueryLimits {
    /// Production defaults keep Desktop/MCP pages finite without scanning an entire generation.
    pub const fn production_defaults() -> Self {
        Self {
            max_scanned_records: MAX_QUERY_SCAN_RECORDS,
            max_results: MAX_QUERY_RESULTS,
        }
    }

    /// Constructs explicit finite limits.
    pub const fn try_new(
        max_scanned_records: u64,
        max_results: usize,
    ) -> Result<Self, SecBulkError> {
        if max_scanned_records == 0
            || max_scanned_records > MAX_QUERY_SCAN_RECORDS
            || max_results == 0
            || max_results > MAX_QUERY_RESULTS
        {
            Err(SecBulkError::QueryLimitExceeded)
        } else {
            Ok(Self {
                max_scanned_records,
                max_results,
            })
        }
    }

    /// Returns the finite maximum number of authenticated index records examined.
    pub const fn max_scanned_records(self) -> u64 {
        self.max_scanned_records
    }

    /// Returns the finite maximum number of rows materialized in one response page.
    pub const fn max_results(self) -> usize {
        self.max_results
    }
}

/// Completeness of one generation-bound point-in-time page.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SecBulkQueryCompleteness {
    /// The indexed generation determines one closed answer under its declared coverage.
    Exact,
    /// More than one latest filing revision remains equally knowable.
    Ambiguous,
    /// No represented record exists, or declared provider coverage prevents a complete answer.
    Unavailable,
}

/// One fully authenticated canonical-candidate payload returned from an indexed SEC generation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SecBulkPublishedRecord {
    family: SecBulkFamily,
    fund_instrument_id: InstrumentId,
    accession: SourceIdentifier,
    report_date: Option<NaiveDate>,
    knowledge_time: Timestamp,
    amendment: bool,
    holding_resolution: Option<SecHoldingResolutionState>,
    held_instrument_id: Option<InstrumentId>,
    payload_evidence: EvidenceDigest,
    canonical_payload_json: Box<[u8]>,
}

impl SecBulkPublishedRecord {
    /// Returns the exact provider family.
    pub const fn family(&self) -> SecBulkFamily {
        self.family
    }

    /// Returns the governed fund/share-class identity.
    pub const fn fund_instrument_id(&self) -> InstrumentId {
        self.fund_instrument_id
    }

    /// Returns exact EDGAR accession.
    pub const fn accession(&self) -> &SourceIdentifier {
        &self.accession
    }

    /// Returns the source reporting date used by the PIT index.
    pub const fn report_date(&self) -> Option<NaiveDate> {
        self.report_date
    }

    /// Returns the conservative knowledge clock.
    pub const fn knowledge_time(&self) -> Timestamp {
        self.knowledge_time
    }

    /// Returns whether the exact source filing is an amendment.
    pub const fn amendment(&self) -> bool {
        self.amendment
    }

    /// Returns exact/ambiguous/unresolved held-security identity for N-PORT records.
    pub const fn holding_resolution(&self) -> Option<SecHoldingResolutionState> {
        self.holding_resolution
    }

    /// Returns held-instrument identity only when the governed mapping is exact.
    pub const fn held_instrument_id(&self) -> Option<InstrumentId> {
        self.held_instrument_id
    }

    /// Returns the exact serialized canonical-candidate digest.
    pub const fn payload_evidence(&self) -> EvidenceDigest {
        self.payload_evidence
    }

    /// Returns the complete canonical candidate payload in the adapter's versioned JSON wire.
    ///
    /// Provider-native rows inside this payload originate from the typed model; ticker/name values
    /// remain associations and cannot be promoted to identity by this query surface.
    pub fn canonical_payload_json(&self) -> &[u8] {
        &self.canonical_payload_json
    }
}

/// Opaque continuation bound to one exact generation and query digest.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SecBulkQueryCursor {
    generation_evidence: EvidenceDigest,
    query_evidence: EvidenceDigest,
    matched_records_to_skip: u64,
}

/// One bounded PIT page.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SecBulkQueryPage {
    generation_evidence: EvidenceDigest,
    query_evidence: EvidenceDigest,
    completeness: SecBulkQueryCompleteness,
    conflicting_revisions: bool,
    records: Vec<SecBulkPublishedRecord>,
    next_cursor: Option<SecBulkQueryCursor>,
    scanned_records: u64,
}

impl SecBulkQueryPage {
    /// Returns exact immutable generation identity.
    pub const fn generation_evidence(&self) -> EvidenceDigest {
        self.generation_evidence
    }

    /// Returns exact query identity.
    pub const fn query_evidence(&self) -> EvidenceDigest {
        self.query_evidence
    }

    /// Returns exact/ambiguous/unavailable state.
    pub const fn completeness(&self) -> SecBulkQueryCompleteness {
        self.completeness
    }

    /// Returns whether equally knowable filing revisions conflict at this cutoff.
    pub const fn conflicting_revisions(&self) -> bool {
        self.conflicting_revisions
    }

    /// Returns bounded authenticated records.
    pub fn records(&self) -> &[SecBulkPublishedRecord] {
        &self.records
    }

    /// Returns a generation/query-bound continuation when more records remain.
    pub const fn next_cursor(&self) -> Option<SecBulkQueryCursor> {
        self.next_cursor
    }

    /// Returns index entries examined by this query execution.
    pub const fn scanned_records(&self) -> u64 {
        self.scanned_records
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct IndexPageDescriptor {
    evidence: EvidenceDigest,
    size_bytes: u64,
    record_count: u32,
    first_key: Vec<u8>,
    last_key: Vec<u8>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct GenerationManifestWire {
    version: u8,
    family: String,
    quarter_year: u16,
    quarter: u8,
    catalog_snapshot_date: String,
    manifest_evidence: EvidenceDigest,
    source_generation: SecBulkNativeGenerationReceipt,
    archive_evidence: EvidenceDigest,
    archive_size_bytes: u64,
    archive_retrieval_revision: u64,
    readme_evidence: EvidenceDigest,
    readme_size_bytes: u64,
    readme_retrieval_revision: u64,
    metadata_evidence: EvidenceDigest,
    archive_readme_evidence: EvidenceDigest,
    accepted_schema_version: String,
    accepted_schema_effective_date: String,
    accepted_schema_locator: String,
    declared_coverage_gap: bool,
    source_rows: u64,
    emitted_typed_rows: u64,
    data_evidence: EvidenceDigest,
    data_size_bytes: u64,
    index_pages: Vec<IndexPageDescriptor>,
    records_evidence: EvidenceDigest,
    holding_records: u64,
    ncen_records: u64,
    amendment_records: u64,
    exact_holding_identity_records: u64,
    ambiguous_holding_identity_records: u64,
    unresolved_holding_identity_records: u64,
    published_at_unix_nanos: i64,
}

/// Serializable cold-restart coordinate for one sealed canonical-candidate handoff.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SecBulkCandidateGenerationReceipt {
    root_evidence: EvidenceDigest,
    root_size_bytes: u64,
}

impl SecBulkCandidateGenerationReceipt {
    /// Returns the exact content-addressed handoff root identity.
    pub const fn root_evidence(self) -> EvidenceDigest {
        self.root_evidence
    }

    /// Returns the exact serialized handoff root byte length.
    pub const fn root_size_bytes(self) -> u64 {
        self.root_size_bytes
    }
}

/// Immutable provider-local canonical-candidate handoff returned after durable recovery.
///
/// This type is not a registered shared `market_squawk.fund_holdings` family and confers no
/// application-publication authority. Root integration must validate and publish that family.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SecBulkPublishedGeneration {
    root_evidence: EvidenceDigest,
    root_size_bytes: u64,
    family: SecBulkFamily,
    manifest: GenerationManifestWire,
}

impl SecBulkPublishedGeneration {
    /// Returns the selected provider family.
    pub const fn family(&self) -> SecBulkFamily {
        self.family
    }

    /// Returns immutable source layout identity.
    pub const fn manifest_evidence(&self) -> EvidenceDigest {
        self.manifest.manifest_evidence
    }

    /// Returns the exact immutable provider-native generation supplying every candidate row.
    pub const fn source_generation_evidence(&self) -> EvidenceDigest {
        self.manifest.source_generation.root_evidence()
    }

    /// Returns the exact process-cold receipt for the provider-native source generation.
    pub const fn source_generation_receipt(&self) -> SecBulkNativeGenerationReceipt {
        self.manifest.source_generation
    }

    /// Returns the exact captured quarterly archive content identity.
    pub const fn archive_evidence(&self) -> EvidenceDigest {
        self.manifest.archive_evidence
    }

    /// Returns the bounded exact captured quarterly archive length.
    pub const fn archive_size_bytes(&self) -> u64 {
        self.manifest.archive_size_bytes
    }

    /// Returns the durable provider representation revision for the archive locator.
    pub const fn archive_retrieval_revision(&self) -> u64 {
        self.manifest.archive_retrieval_revision
    }

    /// Returns the exact official readme content identity.
    pub const fn readme_evidence(&self) -> EvidenceDigest {
        self.manifest.readme_evidence
    }

    /// Returns the exact accepted provider schema version sealed into this handoff.
    pub fn accepted_schema_version(&self) -> &str {
        &self.manifest.accepted_schema_version
    }

    /// Returns the exact accepted provider schema effective date sealed into this handoff.
    pub fn accepted_schema_effective_date(&self) -> &str {
        &self.manifest.accepted_schema_effective_date
    }

    /// Returns the exact official accepted-schema locator sealed into this handoff.
    pub fn accepted_schema_locator(&self) -> &str {
        &self.manifest.accepted_schema_locator
    }

    /// Returns ordered record-set evidence.
    pub const fn records_evidence(&self) -> EvidenceDigest {
        self.manifest.records_evidence
    }

    /// Returns published N-PORT holding candidate count.
    pub const fn holding_records(&self) -> u64 {
        self.manifest.holding_records
    }

    /// Returns published N-CEN candidate count.
    pub const fn ncen_records(&self) -> u64 {
        self.manifest.ncen_records
    }

    /// Returns the number of sealed amendment candidates.
    pub const fn amendment_records(&self) -> u64 {
        self.manifest.amendment_records
    }

    /// Returns the number of N-PORT candidates with exact governed held-security identity.
    pub const fn exact_holding_identity_records(&self) -> u64 {
        self.manifest.exact_holding_identity_records
    }

    /// Returns the number of N-PORT candidates preserving an identity conflict.
    pub const fn ambiguous_holding_identity_records(&self) -> u64 {
        self.manifest.ambiguous_holding_identity_records
    }

    /// Returns the number of N-PORT candidates abstaining from held-security identity.
    pub const fn unresolved_holding_identity_records(&self) -> u64 {
        self.manifest.unresolved_holding_identity_records
    }

    /// Returns whether accepted provider schema coverage is known to be incomplete.
    pub const fn declared_coverage_gap(&self) -> bool {
        self.manifest.declared_coverage_gap
    }

    /// Returns trusted local publication clock.
    pub const fn published_at(&self) -> Timestamp {
        Timestamp::from_unix_nanos(self.manifest.published_at_unix_nanos)
    }

    /// Returns atomic root-manifest content identity.
    pub const fn evidence(&self) -> EvidenceDigest {
        self.root_evidence
    }

    /// Returns the minimal serializable receipt for process-cold handoff recovery.
    pub const fn receipt(&self) -> SecBulkCandidateGenerationReceipt {
        SecBulkCandidateGenerationReceipt {
            root_evidence: self.root_evidence,
            root_size_bytes: self.root_size_bytes,
        }
    }
}

/// Streaming, monotonic, non-visible generation writer.
///
/// Data and bounded index pages are sealed content objects first. The root manifest is published
/// last, so a crash can leave only unreachable immutable objects, never a partial generation.
pub struct SecBulkPublicationSession<'a> {
    store: &'a RawEvidenceStore,
    permit: SecBulkCandidatePublicationPermit,
    scan_report: SecBulkScanReport,
    layout: SecBulkLayoutManifest,
    data: RawEvidenceContentWriter<'a>,
    index_page: Vec<u8>,
    index_page_records: u32,
    index_pages: Vec<IndexPageDescriptor>,
    previous_key: Option<[u8; INDEX_KEY_BYTES]>,
    records_digest: Sha256,
    holding_records: u64,
    ncen_records: u64,
    amendment_records: u64,
    exact_holding_identity_records: u64,
    ambiguous_holding_identity_records: u64,
    unresolved_holding_identity_records: u64,
    max_records: u64,
    deadline: Timestamp,
}

impl fmt::Debug for SecBulkPublicationSession<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SecBulkPublicationSession")
            .field("manifest_evidence", &self.layout.evidence())
            .field(
                "source_generation",
                &self.permit.source_generation().root_evidence(),
            )
            .field("holding_records", &self.holding_records)
            .field("ncen_records", &self.ncen_records)
            .finish_non_exhaustive()
    }
}

impl<'a> SecBulkPublicationSession<'a> {
    /// Begins a non-visible generation for one exact scanned layout.
    pub fn begin(
        store: &'a RawEvidenceStore,
        permit: SecBulkCandidatePublicationPermit,
        scan_report: SecBulkScanReport,
        layout: SecBulkLayoutManifest,
        deadline: Timestamp,
        cancellation: &CancellationToken,
    ) -> Result<Self, SecBulkError> {
        Self::with_record_limit(
            store,
            permit,
            scan_report,
            layout,
            MAX_PUBLICATION_RECORDS,
            deadline,
            cancellation,
        )
    }

    /// Begins with an explicit finite candidate ceiling.
    #[allow(clippy::too_many_arguments)]
    pub fn with_record_limit(
        store: &'a RawEvidenceStore,
        permit: SecBulkCandidatePublicationPermit,
        scan_report: SecBulkScanReport,
        layout: SecBulkLayoutManifest,
        max_records: u64,
        deadline: Timestamp,
        cancellation: &CancellationToken,
    ) -> Result<Self, SecBulkError> {
        check_operation(deadline, cancellation)?;
        if max_records == 0
            || permit.manifest_evidence() != scan_report.manifest_evidence()
            || permit.manifest_evidence() != layout.evidence()
            || permit.family() != layout.capture().selection().family()
            || permit.quarter() != layout.capture().selection().quarter()
        {
            return Err(SecBulkError::PublicationNotReady);
        }
        let mut data =
            store.create_content_writer(MAX_GENERATION_DATA_BYTES, deadline, cancellation)?;
        data.write_bytes(DATA_MAGIC, cancellation)?;
        let mut index_page = Vec::new();
        index_page
            .try_reserve_exact(
                INDEX_MAGIC.len().saturating_add(4).saturating_add(
                    usize::try_from(INDEX_PAGE_RECORDS)
                        .unwrap_or(usize::MAX)
                        .saturating_mul(INDEX_ENTRY_BYTES),
                ),
            )
            .map_err(|_| SecBulkError::AllocationFailed)?;
        start_index_page(&mut index_page);
        let records_digest = generation_records_digest_prefix(
            permit.manifest_evidence(),
            scan_report.source_rows(),
            scan_report.emitted_typed_rows(),
        );
        Ok(Self {
            store,
            permit,
            scan_report,
            layout,
            data,
            index_page,
            index_page_records: 0,
            index_pages: Vec::new(),
            previous_key: None,
            records_digest,
            holding_records: 0,
            ncen_records: 0,
            amendment_records: 0,
            exact_holding_identity_records: 0,
            ambiguous_holding_identity_records: 0,
            unresolved_holding_identity_records: 0,
            max_records,
            deadline,
        })
    }

    /// Streams one N-PORT canonical-candidate payload into the non-visible generation.
    pub fn stage_fund_holding(
        &mut self,
        candidate: &SecFundHoldingCandidate,
        cancellation: &CancellationToken,
    ) -> Result<(), SecBulkError> {
        if self.permit.family() != SecBulkFamily::Nport
            || candidate.manifest_evidence() != self.permit.manifest_evidence()
            || candidate.generation_evidence() != self.permit.source_generation().root_evidence()
        {
            return Err(SecBulkError::InvalidCanonicalMapping);
        }
        let key = index_key(
            *candidate.fund_identity().fund_instrument_id(),
            candidate.chronology().knowledge_time(),
            candidate.chronology().report_date(),
            candidate.accession(),
            candidate.holding().row_evidence,
        )?;
        let payload =
            serde_json::to_vec(candidate).map_err(|_| SecBulkError::PublicationNotReady)?;
        self.stage_payload(key, &payload, cancellation)?;
        self.holding_records = self
            .holding_records
            .checked_add(1)
            .ok_or(SecBulkError::QueryLimitExceeded)?;
        if candidate.amendment() {
            self.amendment_records = self
                .amendment_records
                .checked_add(1)
                .ok_or(SecBulkError::QueryLimitExceeded)?;
        }
        let resolution_count = match candidate.instrument_resolution().state() {
            SecHoldingResolutionState::Exact => &mut self.exact_holding_identity_records,
            SecHoldingResolutionState::Ambiguous => &mut self.ambiguous_holding_identity_records,
            SecHoldingResolutionState::Unresolved => &mut self.unresolved_holding_identity_records,
        };
        *resolution_count = resolution_count
            .checked_add(1)
            .ok_or(SecBulkError::QueryLimitExceeded)?;
        Ok(())
    }

    /// Streams one N-CEN annual/fund/ETF/exchange canonical-candidate payload.
    pub fn stage_ncen_fund_metadata(
        &mut self,
        candidate: &SecNcenFundMetadataCandidate,
        cancellation: &CancellationToken,
    ) -> Result<(), SecBulkError> {
        if self.permit.family() != SecBulkFamily::Ncen
            || candidate.manifest_evidence() != self.permit.manifest_evidence()
            || candidate.generation_evidence() != self.permit.source_generation().root_evidence()
        {
            return Err(SecBulkError::InvalidCanonicalMapping);
        }
        let key = index_key(
            *candidate.fund_identity().fund_instrument_id(),
            candidate.chronology().knowledge_time(),
            candidate.submission().report_ending_period,
            &candidate.submission().accession,
            candidate.fund().row_evidence,
        )?;
        let payload =
            serde_json::to_vec(candidate).map_err(|_| SecBulkError::PublicationNotReady)?;
        self.stage_payload(key, &payload, cancellation)?;
        self.ncen_records = self
            .ncen_records
            .checked_add(1)
            .ok_or(SecBulkError::QueryLimitExceeded)?;
        if candidate.amendment() {
            self.amendment_records = self
                .amendment_records
                .checked_add(1)
                .ok_or(SecBulkError::QueryLimitExceeded)?;
        }
        Ok(())
    }

    /// Atomically publishes a root only after every data/index object is immutable, then performs
    /// a full read-only recovery before returning query authority.
    pub fn commit(
        mut self,
        published_at: Timestamp,
        cancellation: &CancellationToken,
    ) -> Result<SecBulkPublishedGeneration, SecBulkError> {
        check_operation(self.deadline, cancellation)?;
        if published_at < self.permit.issued_at() || published_at > self.deadline {
            return Err(SecBulkError::PublicationNotReady);
        }
        self.flush_index_page(cancellation)?;
        let data_receipt = self.data.seal(cancellation)?;
        let records_evidence = EvidenceDigest::new(
            DigestAlgorithm::Sha256,
            self.records_digest.finalize().into(),
        );
        let manifest = GenerationManifestWire {
            version: GENERATION_VERSION,
            family: family_tag(self.permit.family()).to_owned(),
            quarter_year: self.permit.quarter().year(),
            quarter: self.permit.quarter().quarter(),
            catalog_snapshot_date: super::model::SEC_BULK_CATALOG_SNAPSHOT_DATE.to_owned(),
            manifest_evidence: self.permit.manifest_evidence(),
            source_generation: self.permit.source_generation(),
            archive_evidence: self.layout.capture().evidence(),
            archive_size_bytes: self.layout.capture().size_bytes(),
            archive_retrieval_revision: self.layout.capture().retrieval_revision(),
            readme_evidence: self.layout.official_readme_capture().evidence(),
            readme_size_bytes: self.layout.official_readme_capture().size_bytes(),
            readme_retrieval_revision: self.layout.official_readme_capture().retrieval_revision(),
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
            source_rows: self.scan_report.source_rows(),
            emitted_typed_rows: self.scan_report.emitted_typed_rows(),
            data_evidence: data_receipt.evidence(),
            data_size_bytes: data_receipt.size_bytes(),
            index_pages: self.index_pages,
            records_evidence,
            holding_records: self.holding_records,
            ncen_records: self.ncen_records,
            amendment_records: self.amendment_records,
            exact_holding_identity_records: self.exact_holding_identity_records,
            ambiguous_holding_identity_records: self.ambiguous_holding_identity_records,
            unresolved_holding_identity_records: self.unresolved_holding_identity_records,
            published_at_unix_nanos: published_at.unix_nanos(),
        };
        let root_bytes =
            serde_json::to_vec(&manifest).map_err(|_| SecBulkError::PublicationNotReady)?;
        if u64::try_from(root_bytes.len()).map_or(true, |size| {
            size == 0 || size > MAX_GENERATION_MANIFEST_BYTES
        }) {
            return Err(SecBulkError::QueryLimitExceeded);
        }
        check_operation(self.deadline, cancellation)?;
        let root_evidence = self.store.persist_cancellable(&root_bytes, cancellation)?;
        check_operation(self.deadline, cancellation)?;
        let root_receipt = RawEvidenceReceipt::new(
            root_evidence,
            u64::try_from(root_bytes.len()).map_err(|_| SecBulkError::AllocationFailed)?,
        );
        recover_from_receipt(self.store, root_receipt, self.deadline, cancellation)
    }

    fn stage_payload(
        &mut self,
        key: [u8; INDEX_KEY_BYTES],
        payload: &[u8],
        cancellation: &CancellationToken,
    ) -> Result<(), SecBulkError> {
        check_operation(self.deadline, cancellation)?;
        let total = self
            .holding_records
            .checked_add(self.ncen_records)
            .and_then(|value| value.checked_add(1))
            .ok_or(SecBulkError::QueryLimitExceeded)?;
        if total > self.max_records
            || total > self.scan_report.emitted_typed_rows()
            || payload.is_empty()
            || payload.len() > MAX_RECORD_BYTES
            || self.previous_key.is_some_and(|previous| previous >= key)
        {
            return Err(SecBulkError::QueryLimitExceeded);
        }
        let payload_length =
            u32::try_from(payload.len()).map_err(|_| SecBulkError::QueryLimitExceeded)?;
        let payload_offset = self
            .data
            .observed_bytes()
            .checked_add(4)
            .ok_or(SecBulkError::QueryLimitExceeded)?;
        self.data
            .write_bytes(&payload_length.to_be_bytes(), cancellation)?;
        self.data.write_bytes(payload, cancellation)?;
        let payload_digest: [u8; 32] = Sha256::digest(payload).into();
        hash_field(&mut self.records_digest, &payload_digest);
        self.index_page.extend_from_slice(&key);
        self.index_page
            .extend_from_slice(&payload_offset.to_be_bytes());
        self.index_page
            .extend_from_slice(&payload_length.to_be_bytes());
        self.index_page.extend_from_slice(&payload_digest);
        self.index_page_records = self
            .index_page_records
            .checked_add(1)
            .ok_or(SecBulkError::QueryLimitExceeded)?;
        self.previous_key = Some(key);
        if self.index_page_records == INDEX_PAGE_RECORDS {
            self.flush_index_page(cancellation)?;
        }
        Ok(())
    }

    fn flush_index_page(&mut self, cancellation: &CancellationToken) -> Result<(), SecBulkError> {
        if self.index_page_records == 0 {
            return Ok(());
        }
        check_operation(self.deadline, cancellation)?;
        let count_offset = INDEX_MAGIC.len();
        self.index_page[count_offset..count_offset + 4]
            .copy_from_slice(&self.index_page_records.to_be_bytes());
        let first_offset = INDEX_MAGIC.len() + 4;
        let last_offset = first_offset
            + usize::try_from(self.index_page_records - 1)
                .map_err(|_| SecBulkError::AllocationFailed)?
                * INDEX_ENTRY_BYTES;
        let first_key = self.index_page[first_offset..first_offset + INDEX_KEY_BYTES].to_vec();
        let last_key = self.index_page[last_offset..last_offset + INDEX_KEY_BYTES].to_vec();
        let evidence = self
            .store
            .persist_cancellable(&self.index_page, cancellation)?;
        self.index_pages
            .try_reserve(1)
            .map_err(|_| SecBulkError::AllocationFailed)?;
        self.index_pages.push(IndexPageDescriptor {
            evidence,
            size_bytes: u64::try_from(self.index_page.len())
                .map_err(|_| SecBulkError::AllocationFailed)?,
            record_count: self.index_page_records,
            first_key,
            last_key,
        });
        self.index_page.clear();
        start_index_page(&mut self.index_page);
        self.index_page_records = 0;
        check_operation(self.deadline, cancellation)
    }
}

/// Reopens and fully verifies an expected N-PORT generation without caller-owned record replay.
pub fn recover_fund_holding_candidate_generation(
    store: &RawEvidenceStore,
    expected: &SecBulkPublishedGeneration,
    deadline: Timestamp,
    cancellation: &CancellationToken,
) -> Result<SecBulkPublishedGeneration, SecBulkError> {
    if expected.family() != SecBulkFamily::Nport {
        return Err(SecBulkError::RecoveryMismatch);
    }
    recover_expected(store, expected, deadline, cancellation)
}

/// Reconstructs canonical-candidate handoff authority after a process-cold restart.
///
/// Recovery reopens the exact raw archive/readme evidence, the provider-native source generation,
/// every candidate payload, and every bounded index page before returning this adapter-local
/// handoff. It does not publish a shared canonical family.
pub fn recover_bulk_candidate_generation_from_receipt(
    store: &RawEvidenceStore,
    receipt: SecBulkCandidateGenerationReceipt,
    deadline: Timestamp,
    cancellation: &CancellationToken,
) -> Result<SecBulkPublishedGeneration, SecBulkError> {
    recover_from_receipt(
        store,
        RawEvidenceReceipt::new(receipt.root_evidence, receipt.root_size_bytes),
        deadline,
        cancellation,
    )
}

/// Reopens and fully verifies an expected N-CEN generation without caller-owned record replay.
pub fn recover_ncen_generation(
    store: &RawEvidenceStore,
    expected: &SecBulkPublishedGeneration,
    deadline: Timestamp,
    cancellation: &CancellationToken,
) -> Result<SecBulkPublishedGeneration, SecBulkError> {
    if expected.family() != SecBulkFamily::Ncen {
        return Err(SecBulkError::RecoveryMismatch);
    }
    recover_expected(store, expected, deadline, cancellation)
}

/// Executes an indexed N-PORT point-in-time page.
#[allow(clippy::too_many_arguments)]
pub fn query_fund_holding_candidates(
    store: &RawEvidenceStore,
    generation: &SecBulkPublishedGeneration,
    query: &SecFundHoldingCandidatesQuery,
    limits: SecBulkQueryLimits,
    cursor: Option<SecBulkQueryCursor>,
    deadline: Timestamp,
    cancellation: &CancellationToken,
) -> Result<SecBulkQueryPage, SecBulkError> {
    let filter = QueryFilter {
        family: SecBulkFamily::Nport,
        fund_instrument_id: *query.fund_instrument_id(),
        cutoff: query.cutoff(),
        report_date: query.report_date(),
        include_all_known_revisions: query.include_all_known_revisions(),
    };
    query_generation(
        store,
        generation,
        filter,
        limits,
        cursor,
        deadline,
        cancellation,
    )
}

/// Executes an indexed N-CEN annual/fund/ETF/exchange point-in-time page.
#[allow(clippy::too_many_arguments)]
pub fn query_ncen_fund_metadata(
    store: &RawEvidenceStore,
    generation: &SecBulkPublishedGeneration,
    query: &SecNcenFundMetadataQuery,
    limits: SecBulkQueryLimits,
    cursor: Option<SecBulkQueryCursor>,
    deadline: Timestamp,
    cancellation: &CancellationToken,
) -> Result<SecBulkQueryPage, SecBulkError> {
    let filter = QueryFilter {
        family: SecBulkFamily::Ncen,
        fund_instrument_id: *query.fund_instrument_id(),
        cutoff: query.cutoff(),
        report_date: query.report_ending_period(),
        include_all_known_revisions: query.include_all_known_revisions(),
    };
    query_generation(
        store,
        generation,
        filter,
        limits,
        cursor,
        deadline,
        cancellation,
    )
}

#[derive(Clone, Copy)]
struct QueryFilter {
    family: SecBulkFamily,
    fund_instrument_id: InstrumentId,
    cutoff: Timestamp,
    report_date: Option<NaiveDate>,
    include_all_known_revisions: bool,
}

#[derive(Clone, Copy, Eq, PartialEq)]
struct VersionCoordinate {
    report_days: i32,
    knowledge_nanos: i64,
    accession: [u8; 20],
}

impl Ord for VersionCoordinate {
    fn cmp(&self, other: &Self) -> Ordering {
        self.report_days
            .cmp(&other.report_days)
            .then_with(|| self.knowledge_nanos.cmp(&other.knowledge_nanos))
            .then_with(|| self.accession.cmp(&other.accession))
    }
}

impl PartialOrd for VersionCoordinate {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

fn query_generation(
    store: &RawEvidenceStore,
    generation: &SecBulkPublishedGeneration,
    filter: QueryFilter,
    limits: SecBulkQueryLimits,
    cursor: Option<SecBulkQueryCursor>,
    deadline: Timestamp,
    cancellation: &CancellationToken,
) -> Result<SecBulkQueryPage, SecBulkError> {
    check_operation(deadline, cancellation)?;
    if generation.family() != filter.family {
        return Err(SecBulkError::PublicationNotReady);
    }
    let query_evidence = query_digest(generation.evidence(), filter);
    let skip = match cursor {
        Some(cursor)
            if cursor.generation_evidence == generation.evidence()
                && cursor.query_evidence == query_evidence =>
        {
            cursor.matched_records_to_skip
        }
        Some(_) => return Err(SecBulkError::PublicationNotReady),
        None => 0,
    };

    let mut scanned = 0_u64;
    let mut selected_version = None;
    let mut equally_latest = false;
    if !filter.include_all_known_revisions {
        visit_matching_entries(store, generation, filter, deadline, cancellation, |entry| {
            scanned = scanned
                .checked_add(1)
                .ok_or(SecBulkError::QueryLimitExceeded)?;
            if scanned > limits.max_scanned_records {
                return Err(SecBulkError::QueryLimitExceeded);
            }
            let coordinate = entry.version_coordinate()?;
            match selected_version {
                None => selected_version = Some(coordinate),
                Some(current) if coordinate > current => {
                    equally_latest = coordinate.report_days == current.report_days
                        && coordinate.knowledge_nanos == current.knowledge_nanos
                        && coordinate.accession != current.accession;
                    selected_version = Some(coordinate);
                }
                Some(current)
                    if coordinate.report_days == current.report_days
                        && coordinate.knowledge_nanos == current.knowledge_nanos
                        && coordinate.accession != current.accession =>
                {
                    equally_latest = true;
                }
                Some(_) => {}
            }
            Ok(())
        })?;
    }

    let mut data = store.open_sealed_readonly(
        &generation.manifest.data_evidence,
        generation.manifest.data_size_bytes,
        MAX_GENERATION_DATA_BYTES,
    )?;
    let mut records = Vec::new();
    records
        .try_reserve_exact(limits.max_results.min(1_024))
        .map_err(|_| SecBulkError::AllocationFailed)?;
    let mut matched = 0_u64;
    let mut has_more = false;
    visit_matching_entries(store, generation, filter, deadline, cancellation, |entry| {
        scanned = scanned
            .checked_add(1)
            .ok_or(SecBulkError::QueryLimitExceeded)?;
        let scan_ceiling = if filter.include_all_known_revisions {
            limits.max_scanned_records
        } else {
            limits.max_scanned_records.saturating_mul(2)
        };
        if scanned > scan_ceiling {
            return Err(SecBulkError::QueryLimitExceeded);
        }
        if let Some(selected) = selected_version {
            let observed = entry.version_coordinate()?;
            let selected_match = if equally_latest {
                observed.report_days == selected.report_days
                    && observed.knowledge_nanos == selected.knowledge_nanos
            } else {
                observed == selected
            };
            if !selected_match {
                return Ok(());
            }
        }
        if matched < skip {
            matched += 1;
            return Ok(());
        }
        if records.len() == limits.max_results {
            has_more = true;
            return Ok(());
        }
        let record = read_published_record(&mut data, entry, &generation.manifest)?;
        records.push(record);
        matched += 1;
        Ok(())
    })?;
    let returned = u64::try_from(records.len()).map_err(|_| SecBulkError::AllocationFailed)?;
    let next_cursor = if has_more {
        Some(SecBulkQueryCursor {
            generation_evidence: generation.evidence(),
            query_evidence,
            matched_records_to_skip: skip
                .checked_add(returned)
                .ok_or(SecBulkError::QueryLimitExceeded)?,
        })
    } else {
        None
    };
    let completeness = if records.is_empty() {
        SecBulkQueryCompleteness::Unavailable
    } else if generation.manifest.declared_coverage_gap {
        // Current derived N-CEN sets explicitly omit accepted schema 3.1; represented rows remain
        // useful, but the page cannot claim complete filing coverage.
        SecBulkQueryCompleteness::Unavailable
    } else if equally_latest {
        SecBulkQueryCompleteness::Ambiguous
    } else {
        SecBulkQueryCompleteness::Exact
    };
    Ok(SecBulkQueryPage {
        generation_evidence: generation.evidence(),
        query_evidence,
        completeness,
        conflicting_revisions: equally_latest,
        records,
        next_cursor,
        scanned_records: scanned,
    })
}

fn recover_expected(
    store: &RawEvidenceStore,
    expected: &SecBulkPublishedGeneration,
    deadline: Timestamp,
    cancellation: &CancellationToken,
) -> Result<SecBulkPublishedGeneration, SecBulkError> {
    let recovered = recover_bulk_candidate_generation_from_receipt(
        store,
        expected.receipt(),
        deadline,
        cancellation,
    )?;
    if recovered != *expected {
        return Err(SecBulkError::RecoveryMismatch);
    }
    Ok(recovered)
}

fn recover_from_receipt(
    store: &RawEvidenceStore,
    root: RawEvidenceReceipt,
    deadline: Timestamp,
    cancellation: &CancellationToken,
) -> Result<SecBulkPublishedGeneration, SecBulkError> {
    check_operation(deadline, cancellation)?;
    let mut root_file = store.open_verified_before(
        &root.evidence(),
        root.size_bytes(),
        MAX_GENERATION_MANIFEST_BYTES,
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
    let manifest: GenerationManifestWire =
        serde_json::from_slice(&root_bytes).map_err(|_| SecBulkError::RecoveryMismatch)?;
    let family = validate_generation_manifest(&manifest)?;
    let source_generation = recover_native_generation_from_receipt(
        store,
        manifest.source_generation,
        deadline,
        cancellation,
    )?;
    if source_generation.family() != family
        || source_generation.manifest_evidence() != manifest.manifest_evidence
        || source_generation.row_count() != manifest.emitted_typed_rows
        || source_generation.published_at().unix_nanos() > manifest.published_at_unix_nanos
        || source_generation.archive_evidence() != manifest.archive_evidence
        || source_generation.archive_size_bytes() != manifest.archive_size_bytes
        || source_generation.archive_retrieval_revision() != manifest.archive_retrieval_revision
        || source_generation.readme_evidence() != manifest.readme_evidence
        || source_generation.readme_size_bytes() != manifest.readme_size_bytes
        || source_generation.readme_retrieval_revision() != manifest.readme_retrieval_revision
        || source_generation.metadata_evidence() != manifest.metadata_evidence
        || source_generation.archive_readme_evidence() != manifest.archive_readme_evidence
    {
        return Err(SecBulkError::RecoveryMismatch);
    }
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
        MAX_GENERATION_DATA_BYTES,
        deadline,
        cancellation,
    )?;
    let mut magic = vec![0_u8; DATA_MAGIC.len()];
    data.read_exact(&mut magic)?;
    if magic != DATA_MAGIC {
        return Err(SecBulkError::RecoveryMismatch);
    }
    let mut previous_key = None;
    let mut observed_records = 0_u64;
    let mut observed_amendments = 0_u64;
    let mut observed_exact_holding_identities = 0_u64;
    let mut observed_ambiguous_holding_identities = 0_u64;
    let mut observed_unresolved_holding_identities = 0_u64;
    let mut next_payload_offset = u64::try_from(DATA_MAGIC.len())
        .map_err(|_| SecBulkError::AllocationFailed)?
        .checked_add(4)
        .ok_or(SecBulkError::RecoveryMismatch)?;
    let mut records_digest = generation_records_digest_prefix(
        manifest.manifest_evidence,
        manifest.source_rows,
        manifest.emitted_typed_rows,
    );
    for descriptor in &manifest.index_pages {
        check_operation(deadline, cancellation)?;
        validate_descriptor(descriptor, previous_key.as_ref())?;
        let bytes = read_verified_page(store, descriptor, deadline, cancellation)?;
        for entry in parse_index_page(&bytes, descriptor.record_count)? {
            if previous_key.is_some_and(|previous| previous >= entry.key)
                || entry.payload_offset != next_payload_offset
            {
                return Err(SecBulkError::RecoveryMismatch);
            }
            let disposition =
                validate_data_entry(&mut data, &entry, manifest.data_size_bytes, &manifest)?;
            if disposition.amendment {
                observed_amendments = observed_amendments
                    .checked_add(1)
                    .ok_or(SecBulkError::QueryLimitExceeded)?;
            }
            match disposition.holding_resolution {
                Some(SecHoldingResolutionState::Exact) => {
                    observed_exact_holding_identities = observed_exact_holding_identities
                        .checked_add(1)
                        .ok_or(SecBulkError::QueryLimitExceeded)?;
                }
                Some(SecHoldingResolutionState::Ambiguous) => {
                    observed_ambiguous_holding_identities = observed_ambiguous_holding_identities
                        .checked_add(1)
                        .ok_or(SecBulkError::QueryLimitExceeded)?;
                }
                Some(SecHoldingResolutionState::Unresolved) => {
                    observed_unresolved_holding_identities = observed_unresolved_holding_identities
                        .checked_add(1)
                        .ok_or(SecBulkError::QueryLimitExceeded)?;
                }
                None => {}
            }
            next_payload_offset = entry
                .payload_offset
                .checked_add(u64::from(entry.payload_length))
                .and_then(|offset| offset.checked_add(4))
                .ok_or(SecBulkError::RecoveryMismatch)?;
            hash_field(&mut records_digest, &entry.payload_digest);
            previous_key = Some(entry.key);
            observed_records = observed_records
                .checked_add(1)
                .ok_or(SecBulkError::QueryLimitExceeded)?;
        }
    }
    let expected_records = manifest
        .holding_records
        .checked_add(manifest.ncen_records)
        .ok_or(SecBulkError::QueryLimitExceeded)?;
    let expected_pages = expected_records.div_ceil(u64::from(INDEX_PAGE_RECORDS));
    if observed_records != expected_records
        || u64::try_from(manifest.index_pages.len()).map_or(true, |pages| pages != expected_pages)
        || (expected_records != 0
            && next_payload_offset.saturating_sub(4) != manifest.data_size_bytes)
        || (family == SecBulkFamily::Nport && manifest.ncen_records != 0)
        || (family == SecBulkFamily::Ncen && manifest.holding_records != 0)
        || observed_amendments != manifest.amendment_records
        || observed_exact_holding_identities != manifest.exact_holding_identity_records
        || observed_ambiguous_holding_identities != manifest.ambiguous_holding_identity_records
        || observed_unresolved_holding_identities != manifest.unresolved_holding_identity_records
        || EvidenceDigest::new(DigestAlgorithm::Sha256, records_digest.finalize().into())
            != manifest.records_evidence
    {
        return Err(SecBulkError::RecoveryMismatch);
    }
    Ok(SecBulkPublishedGeneration {
        root_evidence: root.evidence(),
        root_size_bytes: root.size_bytes(),
        family,
        manifest,
    })
}

fn validate_generation_manifest(
    manifest: &GenerationManifestWire,
) -> Result<SecBulkFamily, SecBulkError> {
    let family = parse_family(&manifest.family)?;
    let quarter = super::model::SecQuarter::try_new(manifest.quarter_year, manifest.quarter)?;
    let expected_schema = super::model::SecBulkSchemaIdentity::current(family)?;
    let expected_coverage = super::model::SecBulkCoverage::current(family, quarter)?;
    let candidate_records = manifest
        .holding_records
        .checked_add(manifest.ncen_records)
        .ok_or(SecBulkError::PublicationNotReady)?;
    let holding_identity_records = manifest
        .exact_holding_identity_records
        .checked_add(manifest.ambiguous_holding_identity_records)
        .and_then(|count| count.checked_add(manifest.unresolved_holding_identity_records))
        .ok_or(SecBulkError::PublicationNotReady)?;
    if manifest.version != GENERATION_VERSION
        || manifest.catalog_snapshot_date != super::model::SEC_BULK_CATALOG_SNAPSHOT_DATE
        || manifest
            .manifest_evidence
            .bytes()
            .iter()
            .all(|byte| *byte == 0)
        || manifest
            .source_generation
            .root_evidence()
            .bytes()
            .iter()
            .all(|byte| *byte == 0)
        || manifest.source_generation.root_size_bytes() == 0
        || manifest
            .archive_evidence
            .bytes()
            .iter()
            .all(|byte| *byte == 0)
        || manifest
            .readme_evidence
            .bytes()
            .iter()
            .all(|byte| *byte == 0)
        || manifest
            .metadata_evidence
            .bytes()
            .iter()
            .all(|byte| *byte == 0)
        || manifest
            .archive_readme_evidence
            .bytes()
            .iter()
            .all(|byte| *byte == 0)
        || manifest.archive_size_bytes == 0
        || manifest.readme_size_bytes == 0
        || manifest.archive_retrieval_revision == 0
        || manifest.readme_retrieval_revision == 0
        || expected_schema.version().as_str() != manifest.accepted_schema_version
        || expected_schema.effective_date().to_string() != manifest.accepted_schema_effective_date
        || expected_schema.technical_spec_locator().as_str() != manifest.accepted_schema_locator
        || manifest.declared_coverage_gap
            != matches!(
                expected_coverage,
                super::model::SecBulkCoverage::AcceptedSchemaExcluded { .. }
            )
        || manifest.data_size_bytes <= u64::try_from(DATA_MAGIC.len()).unwrap_or(u64::MAX)
        || manifest.records_evidence.algorithm() != DigestAlgorithm::Sha256
        || manifest
            .records_evidence
            .bytes()
            .iter()
            .all(|byte| *byte == 0)
        || candidate_records == 0
        || candidate_records > manifest.emitted_typed_rows
        || manifest.amendment_records > candidate_records
        || holding_identity_records != manifest.holding_records
        || (family == SecBulkFamily::Ncen && holding_identity_records != 0)
    {
        return Err(SecBulkError::PublicationNotReady);
    }
    Ok(family)
}

fn validate_descriptor(
    descriptor: &IndexPageDescriptor,
    previous_key: Option<&[u8; INDEX_KEY_BYTES]>,
) -> Result<(), SecBulkError> {
    if descriptor.evidence.algorithm() != DigestAlgorithm::Sha256
        || descriptor.size_bytes == 0
        || descriptor.record_count == 0
        || descriptor.record_count > INDEX_PAGE_RECORDS
        || descriptor.first_key.len() != INDEX_KEY_BYTES
        || descriptor.last_key.len() != INDEX_KEY_BYTES
        || descriptor.first_key > descriptor.last_key
        || previous_key
            .is_some_and(|previous| previous.as_slice() >= descriptor.first_key.as_slice())
    {
        return Err(SecBulkError::RecoveryMismatch);
    }
    Ok(())
}

fn read_verified_page(
    store: &RawEvidenceStore,
    descriptor: &IndexPageDescriptor,
    deadline: Timestamp,
    cancellation: &CancellationToken,
) -> Result<Vec<u8>, SecBulkError> {
    let maximum =
        u64::try_from(INDEX_MAGIC.len() + 4 + INDEX_ENTRY_BYTES * INDEX_PAGE_RECORDS as usize)
            .map_err(|_| SecBulkError::AllocationFailed)?;
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

#[derive(Clone, Copy)]
struct IndexEntry {
    key: [u8; INDEX_KEY_BYTES],
    payload_offset: u64,
    payload_length: u32,
    payload_digest: [u8; 32],
}

impl IndexEntry {
    fn fund_instrument_id(self) -> Result<InstrumentId, SecBulkError> {
        InstrumentId::try_from(Uuid::from_bytes(
            self.key[..16]
                .try_into()
                .map_err(|_| SecBulkError::RecoveryMismatch)?,
        ))
        .map_err(Into::into)
    }

    fn knowledge_time(self) -> Timestamp {
        let encoded = u64::from_be_bytes(self.key[16..24].try_into().unwrap_or([0; 8]));
        Timestamp::from_unix_nanos(i64::from_be_bytes((encoded ^ (1_u64 << 63)).to_be_bytes()))
    }

    fn report_date(self) -> Result<Option<NaiveDate>, SecBulkError> {
        let days = i32::from_be_bytes(
            self.key[24..28]
                .try_into()
                .map_err(|_| SecBulkError::RecoveryMismatch)?,
        );
        if days == 0 {
            Ok(None)
        } else {
            NaiveDate::from_num_days_from_ce_opt(days)
                .map(Some)
                .ok_or(SecBulkError::RecoveryMismatch)
        }
    }

    fn accession(self) -> Result<SourceIdentifier, SecBulkError> {
        let value =
            std::str::from_utf8(&self.key[28..48]).map_err(|_| SecBulkError::RecoveryMismatch)?;
        SourceIdentifier::try_from(value).map_err(Into::into)
    }

    fn version_coordinate(self) -> Result<VersionCoordinate, SecBulkError> {
        Ok(VersionCoordinate {
            report_days: i32::from_be_bytes(
                self.key[24..28]
                    .try_into()
                    .map_err(|_| SecBulkError::RecoveryMismatch)?,
            ),
            knowledge_nanos: self.knowledge_time().unix_nanos(),
            accession: self.key[28..48]
                .try_into()
                .map_err(|_| SecBulkError::RecoveryMismatch)?,
        })
    }
}

fn parse_index_page(bytes: &[u8], expected: u32) -> Result<Vec<IndexEntry>, SecBulkError> {
    let header = INDEX_MAGIC.len() + 4;
    if bytes.len() < header || &bytes[..INDEX_MAGIC.len()] != INDEX_MAGIC {
        return Err(SecBulkError::RecoveryMismatch);
    }
    let observed = u32::from_be_bytes(
        bytes[INDEX_MAGIC.len()..header]
            .try_into()
            .map_err(|_| SecBulkError::RecoveryMismatch)?,
    );
    let expected_size = header
        .checked_add(
            usize::try_from(observed)
                .map_err(|_| SecBulkError::AllocationFailed)?
                .checked_mul(INDEX_ENTRY_BYTES)
                .ok_or(SecBulkError::AllocationFailed)?,
        )
        .ok_or(SecBulkError::AllocationFailed)?;
    if observed != expected || observed == 0 || bytes.len() != expected_size {
        return Err(SecBulkError::RecoveryMismatch);
    }
    let mut entries = Vec::new();
    entries
        .try_reserve_exact(usize::try_from(observed).map_err(|_| SecBulkError::AllocationFailed)?)
        .map_err(|_| SecBulkError::AllocationFailed)?;
    for chunk in bytes[header..].chunks_exact(INDEX_ENTRY_BYTES) {
        let key = chunk[..INDEX_KEY_BYTES]
            .try_into()
            .map_err(|_| SecBulkError::RecoveryMismatch)?;
        let offset_start = INDEX_KEY_BYTES;
        let payload_offset = u64::from_be_bytes(
            chunk[offset_start..offset_start + 8]
                .try_into()
                .map_err(|_| SecBulkError::RecoveryMismatch)?,
        );
        let length_start = offset_start + 8;
        let payload_length = u32::from_be_bytes(
            chunk[length_start..length_start + 4]
                .try_into()
                .map_err(|_| SecBulkError::RecoveryMismatch)?,
        );
        let payload_digest = chunk[length_start + 4..]
            .try_into()
            .map_err(|_| SecBulkError::RecoveryMismatch)?;
        entries.push(IndexEntry {
            key,
            payload_offset,
            payload_length,
            payload_digest,
        });
    }
    Ok(entries)
}

#[derive(Clone, Copy)]
struct CandidateDisposition {
    amendment: bool,
    holding_resolution: Option<SecHoldingResolutionState>,
}

fn validate_data_entry(
    data: &mut std::fs::File,
    entry: &IndexEntry,
    data_size: u64,
    manifest: &GenerationManifestWire,
) -> Result<CandidateDisposition, SecBulkError> {
    let length =
        usize::try_from(entry.payload_length).map_err(|_| SecBulkError::QueryLimitExceeded)?;
    let end = entry
        .payload_offset
        .checked_add(u64::from(entry.payload_length))
        .ok_or(SecBulkError::RecoveryMismatch)?;
    if length == 0
        || length > MAX_RECORD_BYTES
        || entry.payload_offset < u64::try_from(DATA_MAGIC.len() + 4).unwrap_or(u64::MAX)
        || end > data_size
    {
        return Err(SecBulkError::RecoveryMismatch);
    }
    data.seek(SeekFrom::Start(entry.payload_offset - 4))?;
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
    let value: Value =
        serde_json::from_slice(&payload).map_err(|_| SecBulkError::RecoveryMismatch)?;
    validate_payload_coordinates(&value, *entry, manifest)
}

fn read_published_record(
    data: &mut std::fs::File,
    entry: IndexEntry,
    manifest: &GenerationManifestWire,
) -> Result<SecBulkPublishedRecord, SecBulkError> {
    let length =
        usize::try_from(entry.payload_length).map_err(|_| SecBulkError::QueryLimitExceeded)?;
    if length == 0 || length > MAX_RECORD_BYTES {
        return Err(SecBulkError::RecoveryMismatch);
    }
    data.seek(SeekFrom::Start(entry.payload_offset - 4))?;
    let mut encoded_length = [0_u8; 4];
    data.read_exact(&mut encoded_length)?;
    if u32::from_be_bytes(encoded_length) != entry.payload_length {
        return Err(SecBulkError::RecoveryMismatch);
    }
    let mut payload = vec![0_u8; length];
    data.read_exact(&mut payload)?;
    let digest: [u8; 32] = Sha256::digest(&payload).into();
    if digest != entry.payload_digest {
        return Err(SecBulkError::RecoveryMismatch);
    }
    let value: Value =
        serde_json::from_slice(&payload).map_err(|_| SecBulkError::RecoveryMismatch)?;
    let disposition = validate_payload_coordinates(&value, entry, manifest)?;
    let family = parse_family(&manifest.family)?;
    let (holding_resolution, held_instrument_id) = if family == SecBulkFamily::Nport {
        let state = json_string(&value, "/instrument_resolution/state")?;
        let state = match state {
            "Exact" => SecHoldingResolutionState::Exact,
            "Ambiguous" => SecHoldingResolutionState::Ambiguous,
            "Unresolved" => SecHoldingResolutionState::Unresolved,
            _ => return Err(SecBulkError::RecoveryMismatch),
        };
        let instrument = value
            .pointer("/instrument_resolution/instrument_id")
            .and_then(Value::as_str)
            .map(InstrumentId::from_str)
            .transpose()
            .map_err(|_| SecBulkError::RecoveryMismatch)?;
        if (state == SecHoldingResolutionState::Exact) != instrument.is_some() {
            return Err(SecBulkError::RecoveryMismatch);
        }
        (Some(state), instrument)
    } else {
        (None, None)
    };
    Ok(SecBulkPublishedRecord {
        family,
        fund_instrument_id: entry.fund_instrument_id()?,
        accession: entry.accession()?,
        report_date: entry.report_date()?,
        knowledge_time: entry.knowledge_time(),
        amendment: disposition.amendment,
        holding_resolution,
        held_instrument_id,
        payload_evidence: EvidenceDigest::new(DigestAlgorithm::Sha256, digest),
        canonical_payload_json: payload.into_boxed_slice(),
    })
}

fn validate_payload_coordinates(
    value: &Value,
    entry: IndexEntry,
    manifest: &GenerationManifestWire,
) -> Result<CandidateDisposition, SecBulkError> {
    let family = parse_family(&manifest.family)?;
    let fund = InstrumentId::from_str(json_string(value, "/fund_identity/fund_instrument_id")?)
        .map_err(|_| SecBulkError::RecoveryMismatch)?;
    let accession_path = if family == SecBulkFamily::Nport {
        "/accession"
    } else {
        "/submission/accession"
    };
    let report_path = if family == SecBulkFamily::Nport {
        "/chronology/report_date"
    } else {
        "/submission/report_ending_period"
    };
    let accession = json_string(value, accession_path)?;
    let report = value
        .pointer(report_path)
        .and_then(Value::as_str)
        .map(|date| NaiveDate::parse_from_str(date, "%Y-%m-%d"))
        .transpose()
        .map_err(|_| SecBulkError::RecoveryMismatch)?;
    let chronology = json_chronology(
        value
            .pointer("/chronology")
            .ok_or(SecBulkError::RecoveryMismatch)?,
    )?;
    let generation_evidence = json_evidence_digest(value, "/generation_evidence")?;
    let manifest_evidence = json_evidence_digest(value, "/manifest_evidence")?;
    let amendment = value
        .pointer("/amendment")
        .and_then(Value::as_bool)
        .ok_or(SecBulkError::RecoveryMismatch)?;
    let form_path = if family == SecBulkFamily::Nport {
        "/form"
    } else {
        "/submission/form"
    };
    let holding_resolution = if family == SecBulkFamily::Nport {
        Some(match json_string(value, "/instrument_resolution/state")? {
            "Exact" => SecHoldingResolutionState::Exact,
            "Ambiguous" => SecHoldingResolutionState::Ambiguous,
            "Unresolved" => SecHoldingResolutionState::Unresolved,
            _ => return Err(SecBulkError::RecoveryMismatch),
        })
    } else {
        let expected_coverage = SecBulkCoverage::current(
            family,
            super::model::SecQuarter::try_new(manifest.quarter_year, manifest.quarter)?,
        )?;
        let expected_coverage =
            serde_json::to_value(expected_coverage).map_err(|_| SecBulkError::RecoveryMismatch)?;
        if value.pointer("/coverage") != Some(&expected_coverage) {
            return Err(SecBulkError::RecoveryMismatch);
        }
        None
    };
    if fund != entry.fund_instrument_id()?
        || accession != entry.accession()?.as_str()
        || report != entry.report_date()?
        || chronology.knowledge_time() != entry.knowledge_time()
        || generation_evidence != manifest.source_generation.root_evidence()
        || manifest_evidence != manifest.manifest_evidence
        || amendment != json_string(value, form_path)?.ends_with("/A")
    {
        return Err(SecBulkError::RecoveryMismatch);
    }
    Ok(CandidateDisposition {
        amendment,
        holding_resolution,
    })
}

fn json_evidence_digest(value: &Value, path: &str) -> Result<EvidenceDigest, SecBulkError> {
    serde_json::from_value(
        value
            .pointer(path)
            .cloned()
            .ok_or(SecBulkError::RecoveryMismatch)?,
    )
    .map_err(|_| SecBulkError::RecoveryMismatch)
}

fn json_chronology(value: &Value) -> Result<SecFilingChronology, SecBulkError> {
    let date = |name: &str| -> Result<Option<NaiveDate>, SecBulkError> {
        value
            .get(name)
            .and_then(Value::as_str)
            .map(|date| NaiveDate::parse_from_str(date, "%Y-%m-%d"))
            .transpose()
            .map_err(|_| SecBulkError::RecoveryMismatch)
    };
    let timestamp = |name: &str| -> Result<Option<Timestamp>, SecBulkError> {
        match value.get(name) {
            Some(Value::Number(number)) => number
                .as_i64()
                .map(Timestamp::from_unix_nanos)
                .map(Some)
                .ok_or(SecBulkError::RecoveryMismatch),
            Some(Value::Null) | None => Ok(None),
            _ => Err(SecBulkError::RecoveryMismatch),
        }
    };
    let observed = value
        .get("first_observed_at")
        .and_then(Value::as_i64)
        .map(Timestamp::from_unix_nanos)
        .ok_or(SecBulkError::RecoveryMismatch)?;
    SecFilingChronology::try_new(
        date("report_date")?,
        date("filing_date")?,
        timestamp("accepted_at")?,
        timestamp("provider_published_at")?,
        observed,
    )
}

fn visit_matching_entries(
    store: &RawEvidenceStore,
    generation: &SecBulkPublishedGeneration,
    filter: QueryFilter,
    deadline: Timestamp,
    cancellation: &CancellationToken,
    mut visit: impl FnMut(IndexEntry) -> Result<(), SecBulkError>,
) -> Result<(), SecBulkError> {
    let fund_uuid = filter.fund_instrument_id.as_uuid();
    let fund_prefix: &[u8] = fund_uuid.as_bytes();
    for descriptor in &generation.manifest.index_pages {
        check_operation(deadline, cancellation)?;
        if descriptor.last_key[..16] < fund_prefix[..] {
            continue;
        }
        if descriptor.first_key[..16] > fund_prefix[..] {
            break;
        }
        let bytes = read_verified_page(store, descriptor, deadline, cancellation)?;
        for entry in parse_index_page(&bytes, descriptor.record_count)? {
            let entry_fund = &entry.key[..16];
            if entry_fund < fund_prefix {
                continue;
            }
            if entry_fund > fund_prefix {
                break;
            }
            if entry.knowledge_time() > filter.cutoff
                || filter
                    .report_date
                    .is_some_and(|date| entry.report_date().ok() != Some(Some(date)))
            {
                continue;
            }
            visit(entry)?;
        }
    }
    Ok(())
}

fn index_key(
    fund_instrument_id: InstrumentId,
    knowledge_time: Timestamp,
    report_date: Option<NaiveDate>,
    accession: &SourceIdentifier,
    row_evidence: EvidenceDigest,
) -> Result<[u8; INDEX_KEY_BYTES], SecBulkError> {
    if accession.as_str().len() != 20 {
        return Err(SecBulkError::InvalidCanonicalMapping);
    }
    let mut key = [0_u8; INDEX_KEY_BYTES];
    key[..16].copy_from_slice(fund_instrument_id.as_uuid().as_bytes());
    let ordered_time =
        u64::from_be_bytes(knowledge_time.unix_nanos().to_be_bytes()) ^ (1_u64 << 63);
    key[16..24].copy_from_slice(&ordered_time.to_be_bytes());
    let report_days = report_date.map_or(0, |date| date.num_days_from_ce());
    if report_days < 0 {
        return Err(SecBulkError::InvalidChronology);
    }
    key[24..28].copy_from_slice(&report_days.to_be_bytes());
    key[28..48].copy_from_slice(accession.as_str().as_bytes());
    key[48..].copy_from_slice(&row_evidence.bytes());
    Ok(key)
}

fn start_index_page(page: &mut Vec<u8>) {
    page.extend_from_slice(INDEX_MAGIC);
    page.extend_from_slice(&0_u32.to_be_bytes());
}

fn query_digest(generation: EvidenceDigest, filter: QueryFilter) -> EvidenceDigest {
    let mut digest = Sha256::new();
    digest.update(b"market-squawk/sec-bulk-query/v2");
    hash_field(&mut digest, &generation.bytes());
    hash_field(&mut digest, family_tag(filter.family).as_bytes());
    hash_field(&mut digest, filter.fund_instrument_id.as_uuid().as_bytes());
    hash_field(&mut digest, &filter.cutoff.unix_nanos().to_be_bytes());
    match filter.report_date {
        Some(date) => hash_field(&mut digest, &date.num_days_from_ce().to_be_bytes()),
        None => hash_field(&mut digest, b"all-report-dates"),
    }
    hash_field(&mut digest, &[u8::from(filter.include_all_known_revisions)]);
    EvidenceDigest::new(DigestAlgorithm::Sha256, digest.finalize().into())
}

fn family_tag(family: SecBulkFamily) -> &'static str {
    match family {
        SecBulkFamily::Nport => "nport",
        SecBulkFamily::Ncen => "ncen",
    }
}

fn parse_family(value: &str) -> Result<SecBulkFamily, SecBulkError> {
    match value {
        "nport" => Ok(SecBulkFamily::Nport),
        "ncen" => Ok(SecBulkFamily::Ncen),
        _ => Err(SecBulkError::RecoveryMismatch),
    }
}

fn json_string<'a>(value: &'a Value, pointer: &str) -> Result<&'a str, SecBulkError> {
    value
        .pointer(pointer)
        .and_then(Value::as_str)
        .ok_or(SecBulkError::RecoveryMismatch)
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

fn generation_records_digest_prefix(
    manifest_evidence: EvidenceDigest,
    source_rows: u64,
    emitted_typed_rows: u64,
) -> Sha256 {
    let mut digest = Sha256::new();
    digest.update(b"market-squawk/sec-bulk-published-records/v3");
    hash_field(&mut digest, &manifest_evidence.bytes());
    hash_field(&mut digest, &source_rows.to_be_bytes());
    hash_field(&mut digest, &emitted_typed_rows.to_be_bytes());
    digest
}

fn hash_field(digest: &mut Sha256, value: &[u8]) {
    digest.update(u64::try_from(value.len()).unwrap_or(u64::MAX).to_be_bytes());
    digest.update(value);
}
