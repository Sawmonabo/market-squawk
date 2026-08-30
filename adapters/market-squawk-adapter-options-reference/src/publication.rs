use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroU32;
use std::time::UNIX_EPOCH;

use market_squawk_domain::{
    AvailabilityEvidence, CalendarDate, DigestAlgorithm, EvidenceDigest,
    ResearchTemporalCoordinate, SourceIdentifier, Timestamp,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::{CboeVenue, export::ReferenceAliasAssertionSetEvidence};

const MAX_PUBLICATION_SURFACES: usize = 64;
const MAX_PUBLICATION_PAGES: u32 = 10_000;
const MAX_PUBLICATION_BYTES: u64 = 2 * 1024 * 1024 * 1024;
const MAX_PUBLICATION_RECORDS: u64 = 12_000_000;
const MAX_PUBLICATION_CONFLICTS: usize = 100_000;
const MAX_HTTP_DATE_EVIDENCE_BYTES: usize = 128;
const MAX_TRANSPORT_HEADER_EVIDENCE_BYTES: usize = 1_024;
const MAX_TRANSPORT_REDIRECTS: usize = 4;
const MAX_REFERENCE_TRANSPORT_ELAPSED_NANOS: u64 = 10 * 60 * 1_000_000_000;
const REFERENCE_EVIDENCE_DIGEST_BYTES: usize = 32;
const MAX_STRICT_REFERENCE_ROW_EVIDENCE_BYTES: usize = 64 * 1024;
const STRICT_REFERENCE_ROW_SET_DIGEST_DOMAIN: &[u8] =
    b"market-squawk/options-reference-strict-row-set/v1";
const STRICT_REFERENCE_REQUEST_ROW_SET_DIGEST_DOMAIN: &[u8] =
    b"market-squawk/options-reference-strict-request-row-set/v1";

/// Incremental exact digest of the ordered typed rows accepted by one strict parser.
///
/// The caller-owned sink may stage rows outside adapter memory. This digest binds the exact
/// provider-native serialized rows and their order without retaining the production-sized file.
pub(crate) struct StrictReferenceRowSetDigestBuilder {
    digest: Sha256,
    rows: u32,
}
impl StrictReferenceRowSetDigestBuilder {
    pub(crate) fn new() -> Self {
        let mut digest = Sha256::new();
        digest.update(STRICT_REFERENCE_ROW_SET_DIGEST_DOMAIN);
        Self { digest, rows: 0 }
    }

    pub(crate) fn try_observe<T: Serialize>(&mut self, row: &T) -> Result<(), PublicationError> {
        let encoded =
            serde_json::to_vec(row).map_err(|_| PublicationError::InvalidObjectContext)?;
        if encoded.is_empty() || encoded.len() > MAX_STRICT_REFERENCE_ROW_EVIDENCE_BYTES {
            return Err(PublicationError::InvalidObjectContext);
        }
        let ordinal = self
            .rows
            .checked_add(1)
            .ok_or(PublicationError::LimitsExceeded)?;
        self.digest.update(ordinal.to_be_bytes());
        self.digest.update(
            u64::try_from(encoded.len())
                .map_err(|_| PublicationError::LimitsExceeded)?
                .to_be_bytes(),
        );
        self.digest.update(encoded);
        self.rows = ordinal;
        Ok(())
    }

    pub(crate) fn finish(mut self, expected_rows: u32) -> Result<EvidenceDigest, PublicationError> {
        if self.rows != expected_rows {
            return Err(PublicationError::InvalidObjectContext);
        }
        self.digest.update(self.rows.to_be_bytes());
        Ok(EvidenceDigest::new(
            DigestAlgorithm::Sha256,
            self.digest.finalize().into(),
        ))
    }
}

fn strict_empty_row_set_digest() -> Result<EvidenceDigest, PublicationError> {
    StrictReferenceRowSetDigestBuilder::new().finish(0)
}

/// Exact bounded HTTP date field retained separately from provider-native and local clocks.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct HttpLastModifiedEvidence {
    raw: String,
    instant: Timestamp,
}

impl HttpLastModifiedEvidence {
    pub(crate) fn try_from_header(value: &str) -> Result<Self, PublicationError> {
        if value.is_empty()
            || value.len() > MAX_HTTP_DATE_EVIDENCE_BYTES
            || !value.is_ascii()
            || value.bytes().any(|byte| byte.is_ascii_control())
        {
            return Err(PublicationError::InvalidObjectContext);
        }
        let system_time =
            httpdate::parse_http_date(value).map_err(|_| PublicationError::InvalidObjectContext)?;
        if httpdate::fmt_http_date(system_time) != value {
            return Err(PublicationError::InvalidObjectContext);
        }
        let nanos = system_time
            .duration_since(UNIX_EPOCH)
            .ok()
            .and_then(|duration| i64::try_from(duration.as_nanos()).ok())
            .ok_or(PublicationError::InvalidObjectContext)?;
        Ok(Self {
            raw: value.to_owned(),
            instant: Timestamp::from_unix_nanos(nanos),
        })
    }

    /// Returns the exact retained HTTP field value.
    pub fn as_str(&self) -> &str {
        &self.raw
    }

    /// Returns the strictly parsed IMF-fixdate instant.
    pub const fn instant(&self) -> Timestamp {
        self.instant
    }
}

/// Provider namespace retained by every reference object and record.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ReferenceProvider {
    /// The Options Clearing Corporation.
    Occ,
    /// Cboe U.S. Options exchanges.
    Cboe,
}

/// Exact selected reference surface represented by one requested publication component.
#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum ReferenceSurface {
    /// One venue-specific Cboe `All Series` CSV file.
    CboeAllSeries {
        /// Exchange whose presence the file represents.
        venue: CboeVenue,
    },
    /// OCC DLP `delo-download` text with the exact six selected fields.
    OccDlpSelectedText,
    /// OCC dated DLP HTTP download text.
    OccDlpDailyText,
    /// OCC dated DLP HTTP download XML.
    OccDlpDailyXml,
    /// OCC Information Memo export/index CSV.
    OccMemoIndexCsv,
    /// Closed OCC Information Memo index JSON page.
    OccMemoIndexJson,
    /// One complete OCC memo document.
    OccMemoDocument {
        /// OCC memo number.
        memo_number: u64,
    },
    /// One memo attachment, retained separately from the memo body.
    OccMemoAttachment {
        /// OCC memo number.
        memo_number: u64,
        /// One-based attachment ordinal from the retained document index.
        ordinal: NonZeroU32,
    },
}

impl ReferenceSurface {
    /// Returns the provider owning this surface.
    pub const fn provider(&self) -> ReferenceProvider {
        match self {
            Self::CboeAllSeries { .. } => ReferenceProvider::Cboe,
            Self::OccDlpSelectedText
            | Self::OccDlpDailyText
            | Self::OccDlpDailyXml
            | Self::OccMemoIndexCsv
            | Self::OccMemoIndexJson
            | Self::OccMemoDocument { .. }
            | Self::OccMemoAttachment { .. } => ReferenceProvider::Occ,
        }
    }
}

/// Code-owned bounds for one publication assembly.
///
/// These are application safety ceilings, not provider request or publication limits.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PublicationLimits {
    max_surfaces: usize,
    max_pages: u32,
    max_total_bytes: u64,
    max_total_records: u64,
    max_conflicts: usize,
}

impl PublicationLimits {
    /// Constructs caller-selected limits at or below the code-owned safety ceilings.
    ///
    /// # Errors
    ///
    /// Rejects zero or excessive limits.
    pub fn try_new(
        max_surfaces: usize,
        max_pages: u32,
        max_total_bytes: u64,
        max_total_records: u64,
        max_conflicts: usize,
    ) -> Result<Self, PublicationError> {
        if max_surfaces == 0
            || max_surfaces > MAX_PUBLICATION_SURFACES
            || max_pages == 0
            || max_pages > MAX_PUBLICATION_PAGES
            || max_total_bytes == 0
            || max_total_bytes > MAX_PUBLICATION_BYTES
            || max_total_records == 0
            || max_total_records > MAX_PUBLICATION_RECORDS
            || max_conflicts == 0
            || max_conflicts > MAX_PUBLICATION_CONFLICTS
        {
            return Err(PublicationError::InvalidLimits);
        }
        Ok(Self {
            max_surfaces,
            max_pages,
            max_total_bytes,
            max_total_records,
            max_conflicts,
        })
    }

    /// Returns the maximum requested surfaces.
    pub const fn max_surfaces(self) -> usize {
        self.max_surfaces
    }

    /// Returns the maximum page/object count.
    pub const fn max_pages(self) -> u32 {
        self.max_pages
    }

    /// Returns the maximum aggregate response bytes.
    pub const fn max_total_bytes(self) -> u64 {
        self.max_total_bytes
    }

    /// Returns the maximum aggregate decoded records.
    pub const fn max_total_records(self) -> u64 {
        self.max_total_records
    }

    /// Returns the maximum retained conflicts.
    pub const fn max_conflicts(self) -> usize {
        self.max_conflicts
    }
}

/// One bounded, explicit publication request.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PublicationRequest {
    request_id: SourceIdentifier,
    requested_at: Timestamp,
    deadline: Timestamp,
    surfaces: Vec<ReferenceSurface>,
    limits: PublicationLimits,
}

impl PublicationRequest {
    /// Constructs a request whose exact surface closure is sorted and duplicate-free.
    ///
    /// # Errors
    ///
    /// Rejects an empty, duplicate, oversized, or already-expired surface request.
    pub fn try_new(
        request_id: SourceIdentifier,
        requested_at: Timestamp,
        deadline: Timestamp,
        mut surfaces: Vec<ReferenceSurface>,
        limits: PublicationLimits,
    ) -> Result<Self, PublicationError> {
        if surfaces.is_empty()
            || surfaces.len() > limits.max_surfaces
            || u32::try_from(surfaces.len()).map_or(true, |pages| pages > limits.max_pages)
            || deadline <= requested_at
        {
            return Err(PublicationError::InvalidRequest);
        }
        surfaces.sort();
        if surfaces.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(PublicationError::InvalidRequest);
        }
        Ok(Self {
            request_id,
            requested_at,
            deadline,
            surfaces,
            limits,
        })
    }

    /// Returns the opaque request identity.
    pub const fn request_id(&self) -> &SourceIdentifier {
        &self.request_id
    }

    /// Returns when the publication request was admitted.
    pub const fn requested_at(&self) -> Timestamp {
        self.requested_at
    }

    /// Returns the hard request deadline.
    pub const fn deadline(&self) -> Timestamp {
        self.deadline
    }

    /// Returns the exact requested surface closure.
    pub fn surfaces(&self) -> &[ReferenceSurface] {
        &self.surfaces
    }

    /// Returns the publication limits.
    pub const fn limits(&self) -> PublicationLimits {
        self.limits
    }
}

/// Process-local safety accounting for one exact provider-reference request.
///
/// This capability retains only bounded counters and the admitted surface closure. It is not raw
/// retention, a publication catalog, an immutable generation, a PIT selector, or restart evidence.
pub struct ReferenceRequestBudget {
    request_id: SourceIdentifier,
    requested_at: Timestamp,
    deadline: Timestamp,
    requested_surfaces: BTreeSet<ReferenceSurface>,
    observed_surfaces: BTreeSet<ReferenceSurface>,
    strict_row_set_digests: BTreeMap<ReferenceSurface, EvidenceDigest>,
    alias_assertion_sets: BTreeMap<ReferenceSurface, ReferenceAliasAssertionSetEvidence>,
    limits: PublicationLimits,
    completed_pages: u32,
    payload_bytes: u64,
    returned_records: u64,
    failed: bool,
}

impl std::fmt::Debug for ReferenceRequestBudget {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ReferenceRequestBudget")
            .field("request_id", &self.request_id)
            .field("requested_surfaces", &self.requested_surfaces.len())
            .field("observed_surfaces", &self.observed_surfaces.len())
            .field("completed_pages", &self.completed_pages)
            .field("payload_bytes", &self.payload_bytes)
            .field("returned_records", &self.returned_records)
            .field("failed", &self.failed)
            .finish()
    }
}

impl ReferenceRequestBudget {
    /// Starts non-durable accounting for one exact request closure.
    ///
    /// # Errors
    ///
    /// Rejects a request whose declared page ceiling cannot admit its exact surface closure.
    pub fn try_for_publication(request: &PublicationRequest) -> Result<Self, PublicationError> {
        if u32::try_from(request.surfaces.len())
            .map_or(true, |pages| pages > request.limits.max_pages)
        {
            return Err(PublicationError::InvalidRequest);
        }
        Ok(Self {
            request_id: request.request_id.clone(),
            requested_at: request.requested_at,
            deadline: request.deadline,
            requested_surfaces: request.surfaces.iter().cloned().collect(),
            observed_surfaces: BTreeSet::new(),
            strict_row_set_digests: BTreeMap::new(),
            alias_assertion_sets: BTreeMap::new(),
            limits: request.limits,
            completed_pages: 0,
            payload_bytes: 0,
            returned_records: 0,
            failed: false,
        })
    }

    /// Accounts one completed raw/typed handoff without retaining any decoded row.
    ///
    /// # Errors
    ///
    /// Rejects cross-request, unrequested, duplicate, or internally inconsistent evidence and any
    /// checked aggregate that exceeds the request's declared ceilings.
    pub fn observe_typed_handoff(
        &mut self,
        handoff: &crate::ReferenceTypedHandoff,
    ) -> Result<(), PublicationError> {
        let raw = handoff.raw_receipt();
        let context = handoff.context();
        let page = handoff.page_receipt();
        if raw.content_digest() != context.payload_digest()
            || raw.size_bytes() != context.payload_bytes()
            || page.context() != context
        {
            self.failed = true;
            return Err(PublicationError::InvalidObjectContext);
        }
        let result = self.observe_object(context, u64::from(page.returned_records()));
        if result.is_ok()
            && self
                .strict_row_set_digests
                .insert(context.surface().clone(), page.strict_row_set_digest())
                .is_some()
        {
            self.failed = true;
            return Err(PublicationError::InvalidRequest);
        }
        if result.is_ok()
            && self
                .alias_assertion_sets
                .insert(context.surface().clone(), page.alias_assertion_set())
                .is_some()
        {
            self.failed = true;
            return Err(PublicationError::InvalidRequest);
        }
        if result.is_err() {
            self.failed = true;
        }
        result
    }

    /// Accounts one completed explicitly uninterpreted OCC memo handoff.
    ///
    /// # Errors
    ///
    /// Rejects cross-request, unrequested, duplicate, or internally inconsistent evidence and any
    /// checked aggregate that exceeds the request's declared ceilings.
    pub fn observe_uninterpreted_memo_handoff(
        &mut self,
        handoff: &crate::ReferenceUninterpretedMemoHandoff,
    ) -> Result<(), PublicationError> {
        let raw = handoff.raw_receipt();
        let context = handoff.context();
        if raw.content_digest() != context.payload_digest()
            || raw.size_bytes() != context.payload_bytes()
        {
            self.failed = true;
            return Err(PublicationError::InvalidObjectContext);
        }
        let result = self.observe_object(context, 0);
        if result.is_ok()
            && self
                .strict_row_set_digests
                .insert(context.surface().clone(), strict_empty_row_set_digest()?)
                .is_some()
        {
            self.failed = true;
            return Err(PublicationError::InvalidRequest);
        }
        if result.is_ok()
            && self
                .alias_assertion_sets
                .insert(
                    context.surface().clone(),
                    ReferenceAliasAssertionSetEvidence::empty(),
                )
                .is_some()
        {
            self.failed = true;
            return Err(PublicationError::InvalidRequest);
        }
        if result.is_err() {
            self.failed = true;
        }
        result
    }

    /// Consumes complete counters after deterministic conflict reconciliation terminates.
    ///
    /// # Errors
    ///
    /// Rejects incomplete surface closure, cross-request conflict evidence, or any terminal count
    /// beyond the declared request ceilings. Caller-owned staged output must be discarded on error.
    pub fn finish(
        self,
        reconciliation: &crate::ReferenceConflictReconciliationReceipt,
    ) -> Result<ReferenceRequestAccountingReceipt, PublicationError> {
        if self.failed
            || reconciliation.request_id() != &self.request_id
            || self.observed_surfaces != self.requested_surfaces
            || self.strict_row_set_digests.len() != self.requested_surfaces.len()
            || self.alias_assertion_sets.len() != self.requested_surfaces.len()
        {
            return Err(PublicationError::InvalidRequest);
        }
        if self.completed_pages > self.limits.max_pages
            || self.payload_bytes > self.limits.max_total_bytes
            || self.returned_records > self.limits.max_total_records
            || reconciliation.conflicts() > self.limits.max_conflicts
        {
            return Err(PublicationError::LimitsExceeded);
        }
        let strict_row_set_digest = strict_request_row_set_digest(
            &self.request_id,
            &self.strict_row_set_digests,
            self.returned_records,
        )?;
        let mut expected_assertion_set = ReferenceAliasAssertionSetEvidence::empty();
        for assertion_set in self.alias_assertion_sets.into_values() {
            expected_assertion_set
                .try_merge(assertion_set)
                .map_err(|_| PublicationError::LimitsExceeded)?;
        }
        if expected_assertion_set != reconciliation.assertion_set() {
            return Err(PublicationError::InvalidRequest);
        }
        let alias_assertion_closure_digest = expected_assertion_set
            .closure_digest(strict_row_set_digest)
            .map_err(|_| PublicationError::InvalidRequest)?;
        Ok(ReferenceRequestAccountingReceipt {
            request_id: self.request_id,
            completed_pages: self.completed_pages,
            payload_bytes: self.payload_bytes,
            returned_records: self.returned_records,
            conflicts: reconciliation.conflicts(),
            strict_row_set_digest,
            alias_assertions: expected_assertion_set.assertions(),
            alias_assertion_closure_digest,
        })
    }

    fn observe_object(
        &mut self,
        context: &ReferenceObjectContext,
        returned_records: u64,
    ) -> Result<(), PublicationError> {
        let official_request = context.transport_evidence().request();
        if self.failed
            || official_request.request_id() != &self.request_id
            || official_request.wall_started_at() != self.requested_at
            || official_request.wall_deadline() != self.deadline
            || !self.requested_surfaces.contains(context.surface())
            || !self.observed_surfaces.insert(context.surface().clone())
        {
            return Err(PublicationError::InvalidRequest);
        }
        self.completed_pages = self
            .completed_pages
            .checked_add(1)
            .ok_or(PublicationError::LimitsExceeded)?;
        self.payload_bytes = self
            .payload_bytes
            .checked_add(context.payload_bytes())
            .ok_or(PublicationError::LimitsExceeded)?;
        self.returned_records = self
            .returned_records
            .checked_add(returned_records)
            .ok_or(PublicationError::LimitsExceeded)?;
        if self.completed_pages > self.limits.max_pages
            || self.payload_bytes > self.limits.max_total_bytes
            || self.returned_records > self.limits.max_total_records
        {
            return Err(PublicationError::LimitsExceeded);
        }
        Ok(())
    }
}

/// Terminal non-durable request counters safe for caller-owned composition decisions.
#[derive(Debug, Eq, PartialEq)]
pub struct ReferenceRequestAccountingReceipt {
    request_id: SourceIdentifier,
    completed_pages: u32,
    payload_bytes: u64,
    returned_records: u64,
    conflicts: usize,
    strict_row_set_digest: EvidenceDigest,
    alias_assertions: u64,
    alias_assertion_closure_digest: EvidenceDigest,
}

impl ReferenceRequestAccountingReceipt {
    /// Returns the exact publication request identity.
    pub const fn request_id(&self) -> &SourceIdentifier {
        &self.request_id
    }

    /// Returns completed official object/page count.
    pub const fn completed_pages(&self) -> u32 {
        self.completed_pages
    }

    /// Returns exact aggregate raw payload bytes.
    pub const fn payload_bytes(&self) -> u64 {
        self.payload_bytes
    }

    /// Returns exact aggregate strictly decoded record count.
    pub const fn returned_records(&self) -> u64 {
        self.returned_records
    }

    /// Returns exact deterministic conflict count.
    pub const fn conflicts(&self) -> usize {
        self.conflicts
    }

    /// Returns the exact ordered-row identity across the complete requested surface closure.
    pub const fn strict_row_set_digest(&self) -> EvidenceDigest {
        self.strict_row_set_digest
    }

    /// Returns the exact number of provider alias assertions bound to the typed row closure.
    pub const fn alias_assertions(&self) -> u64 {
        self.alias_assertions
    }

    /// Returns the assertion multiset commitment bound to the complete strict row-set identity.
    pub const fn alias_assertion_closure_digest(&self) -> EvidenceDigest {
        self.alias_assertion_closure_digest
    }
}

/// One non-cloneable modified OCC/Cboe object after its exact terminal parser contract.
pub enum ReferenceModifiedObjectHandoff {
    /// A strictly decoded Cboe, OCC DLP, or OCC memo-index object.
    Typed(crate::ReferenceTypedHandoff),
    /// A complete retained OCC memo whose economics remain explicitly uninterpreted.
    UninterpretedMemo(crate::ReferenceUninterpretedMemoHandoff),
}

impl std::fmt::Debug for ReferenceModifiedObjectHandoff {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ReferenceModifiedObjectHandoff")
            .field("object_id", self.context().object_id())
            .field("surface", self.context().surface())
            .finish_non_exhaustive()
    }
}

impl From<crate::ReferenceTypedHandoff> for ReferenceModifiedObjectHandoff {
    fn from(value: crate::ReferenceTypedHandoff) -> Self {
        Self::Typed(value)
    }
}

impl From<crate::ReferenceUninterpretedMemoHandoff> for ReferenceModifiedObjectHandoff {
    fn from(value: crate::ReferenceUninterpretedMemoHandoff) -> Self {
        Self::UninterpretedMemo(value)
    }
}

impl ReferenceModifiedObjectHandoff {
    /// Returns exact provider object, schema, checksum, and clock evidence.
    pub const fn context(&self) -> &ReferenceObjectContext {
        match self {
            Self::Typed(value) => value.context(),
            Self::UninterpretedMemo(value) => value.context(),
        }
    }

    /// Returns the complete common raw-object receipt.
    pub const fn raw_receipt(&self) -> &market_squawk_platform::ResearchObjectReceipt {
        match self {
            Self::Typed(value) => value.raw_receipt(),
            Self::UninterpretedMemo(value) => value.raw_receipt(),
        }
    }

    /// Returns exact modified-response evidence.
    pub const fn http_receipt(&self) -> &crate::ReferenceHttpReceipt {
        match self {
            Self::Typed(value) => value.http_receipt(),
            Self::UninterpretedMemo(value) => value.http_receipt(),
        }
    }
}

/// Complete non-cloneable modified OCC/Cboe request closure for root composition.
///
/// Every selected surface appears exactly once in request order, every object has a final strict
/// parse receipt, and aggregate bytes, rows, aliases, and conflicts are closed. This value still
/// does not grant generation publication: the application must atomically rejoin it with the
/// caller-owned staged canonical partitions and the forthcoming large-logical-object seal token.
pub struct CompletedModifiedReferencePublicationCapture {
    request: PublicationRequest,
    objects: Box<[ReferenceModifiedObjectHandoff]>,
    reconciliation: crate::ReferenceConflictReconciliationReceipt,
    accounting: ReferenceRequestAccountingReceipt,
}

impl std::fmt::Debug for CompletedModifiedReferencePublicationCapture {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CompletedModifiedReferencePublicationCapture")
            .field("request_id", self.request.request_id())
            .field("objects", &self.objects.len())
            .field("records", &self.accounting.returned_records())
            .field("conflicts", &self.accounting.conflicts())
            .finish_non_exhaustive()
    }
}

impl CompletedModifiedReferencePublicationCapture {
    /// Closes one exact selected-surface request around its final typed/raw handoffs.
    ///
    /// # Errors
    ///
    /// Rejects incomplete, duplicate, reordered, cross-request, or internally inconsistent
    /// objects and reconciliation evidence.
    pub fn try_new(
        request: PublicationRequest,
        objects: Vec<ReferenceModifiedObjectHandoff>,
        reconciliation: crate::ReferenceConflictReconciliationReceipt,
    ) -> Result<Self, PublicationError> {
        let accounting =
            validate_completed_modified_reference_publication(&request, &objects, &reconciliation)?;
        Ok(Self {
            request,
            objects: objects.into_boxed_slice(),
            reconciliation,
            accounting,
        })
    }

    /// Returns the exact selected request closed by this capture.
    pub const fn request(&self) -> &PublicationRequest {
        &self.request
    }

    /// Returns each complete object in the request's sorted selected-surface order.
    pub fn objects(&self) -> &[ReferenceModifiedObjectHandoff] {
        &self.objects
    }

    /// Returns aggregate exact bytes, rows, conflicts, and strict row-set identity.
    pub const fn accounting(&self) -> &ReferenceRequestAccountingReceipt {
        &self.accounting
    }

    /// Returns terminal ambiguity/conflict reconciliation evidence for the same request.
    pub const fn reconciliation(&self) -> &crate::ReferenceConflictReconciliationReceipt {
        &self.reconciliation
    }

    /// Consumes the closure for application-owned staged-partition and seal rejoin.
    pub fn into_parts(
        self,
    ) -> (
        PublicationRequest,
        Box<[ReferenceModifiedObjectHandoff]>,
        crate::ReferenceConflictReconciliationReceipt,
        ReferenceRequestAccountingReceipt,
    ) {
        (
            self.request,
            self.objects,
            self.reconciliation,
            self.accounting,
        )
    }
}

pub(crate) fn validate_completed_modified_reference_publication(
    request: &PublicationRequest,
    objects: &[ReferenceModifiedObjectHandoff],
    reconciliation: &crate::ReferenceConflictReconciliationReceipt,
) -> Result<ReferenceRequestAccountingReceipt, PublicationError> {
    if objects.len() != request.surfaces().len()
        || objects
            .iter()
            .zip(request.surfaces())
            .any(|(object, surface)| object.context().surface() != surface)
    {
        return Err(PublicationError::InvalidRequest);
    }
    let mut budget = ReferenceRequestBudget::try_for_publication(request)?;
    for object in objects {
        match object {
            ReferenceModifiedObjectHandoff::Typed(value) => {
                budget.observe_typed_handoff(value)?;
            }
            ReferenceModifiedObjectHandoff::UninterpretedMemo(value) => {
                budget.observe_uninterpreted_memo_handoff(value)?;
            }
        }
    }
    budget.finish(reconciliation)
}

fn strict_request_row_set_digest(
    request_id: &SourceIdentifier,
    surface_digests: &BTreeMap<ReferenceSurface, EvidenceDigest>,
    returned_records: u64,
) -> Result<EvidenceDigest, PublicationError> {
    if surface_digests.is_empty() {
        return Err(PublicationError::InvalidRequest);
    }
    let mut digest = Sha256::new();
    digest.update(STRICT_REFERENCE_REQUEST_ROW_SET_DIGEST_DOMAIN);
    digest.update(
        u64::try_from(request_id.as_str().len())
            .map_err(|_| PublicationError::LimitsExceeded)?
            .to_be_bytes(),
    );
    digest.update(request_id.as_str().as_bytes());
    for (surface, row_digest) in surface_digests {
        ensure_sha256(*row_digest)?;
        let encoded_surface =
            serde_json::to_vec(surface).map_err(|_| PublicationError::InvalidObjectContext)?;
        digest.update(
            u64::try_from(encoded_surface.len())
                .map_err(|_| PublicationError::LimitsExceeded)?
                .to_be_bytes(),
        );
        digest.update(encoded_surface);
        digest.update(row_digest.bytes());
    }
    digest.update(returned_records.to_be_bytes());
    Ok(EvidenceDigest::new(
        DigestAlgorithm::Sha256,
        digest.finalize().into(),
    ))
}

/// Provider clocks retained without converting date-only values to invented instants.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ObjectClockEvidence {
    posted: Option<ResearchTemporalCoordinate>,
    effective: Option<ResearchTemporalCoordinate>,
    availability: AvailabilityEvidence,
    received_at: Timestamp,
    transport_elapsed_nanos: u64,
}

impl ObjectClockEvidence {
    /// Constructs source and local clocks for an exact object.
    ///
    /// # Errors
    ///
    /// Rejects a reported availability instant later than local receipt.
    pub fn try_new(
        posted: Option<ResearchTemporalCoordinate>,
        effective: Option<ResearchTemporalCoordinate>,
        availability: AvailabilityEvidence,
        received_at: Timestamp,
        transport_elapsed_nanos: u64,
    ) -> Result<Self, PublicationError> {
        if availability
            .reported_at()
            .is_some_and(|available_at| available_at > received_at)
            || transport_elapsed_nanos == 0
            || transport_elapsed_nanos > MAX_REFERENCE_TRANSPORT_ELAPSED_NANOS
        {
            return Err(PublicationError::InvalidClockOrder);
        }
        Ok(Self {
            posted,
            effective,
            availability,
            received_at,
            transport_elapsed_nanos,
        })
    }

    /// Returns the source-posted coordinate when supplied.
    pub fn posted(&self) -> Option<&ResearchTemporalCoordinate> {
        self.posted.as_ref()
    }

    /// Returns the source-effective coordinate when supplied.
    pub fn effective(&self) -> Option<&ResearchTemporalCoordinate> {
        self.effective.as_ref()
    }

    /// Returns conservative availability evidence.
    pub const fn availability(&self) -> &AvailabilityEvidence {
        &self.availability
    }

    /// Returns the local receipt instant.
    pub const fn received_at(&self) -> Timestamp {
        self.received_at
    }

    /// Returns monotonic HTTP send through terminal response-body elapsed time.
    pub const fn transport_elapsed_nanos(&self) -> u64 {
        self.transport_elapsed_nanos
    }
}

/// HTTP method sealed into every official request receipt.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ReferenceRequestMethod {
    /// Idempotent retrieval with no request body.
    Get,
}

/// Exact request-body disposition. The selected official surfaces permit no request body.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case", tag = "state")]
pub enum ReferenceRequestBodyEvidence {
    /// The request intentionally carried no body; the digest is SHA-256 of the empty byte string.
    Absent {
        /// Digest of the exact empty request body.
        digest: EvidenceDigest,
        /// Exact request body length, always zero for this variant.
        bytes: u64,
    },
}

impl ReferenceRequestBodyEvidence {
    fn absent() -> Self {
        Self::Absent {
            digest: sha256_digest(&[]),
            bytes: 0,
        }
    }

    /// Returns the exact request-body digest.
    pub const fn digest(&self) -> EvidenceDigest {
        match self {
            Self::Absent { digest, .. } => *digest,
        }
    }

    /// Returns the exact request-body byte count.
    pub const fn bytes(&self) -> u64 {
        match self {
            Self::Absent { bytes, .. } => *bytes,
        }
    }
}

/// Closed conditional-validator field selected from one exact prior receipt.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case", tag = "kind", content = "value")]
pub enum ReferenceConditionalValidatorEvidence {
    /// Exact `If-None-Match` entity tag.
    EntityTag(String),
    /// Exact `If-Modified-Since` IMF-fixdate.
    LastModified(String),
}

impl ReferenceConditionalValidatorEvidence {
    fn value(&self) -> &str {
        match self {
            Self::EntityTag(value) | Self::LastModified(value) => value,
        }
    }
}

/// Exact provider-native decoder identity, version, and semantic contract fingerprint.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ReferenceNativeSchemaIdentity {
    name: SourceIdentifier,
    version: NonZeroU32,
    fingerprint: EvidenceDigest,
}

impl ReferenceNativeSchemaIdentity {
    /// Constructs a complete native schema identity.
    ///
    /// # Errors
    ///
    /// Rejects a non-SHA-256 or all-zero semantic fingerprint.
    pub(crate) fn try_new(
        name: SourceIdentifier,
        version: NonZeroU32,
        fingerprint: EvidenceDigest,
    ) -> Result<Self, PublicationError> {
        ensure_sha256(fingerprint)?;
        Ok(Self {
            name,
            version,
            fingerprint,
        })
    }

    /// Returns the stable code-owned schema name.
    pub const fn name(&self) -> &SourceIdentifier {
        &self.name
    }

    /// Returns the nonzero native schema version.
    pub const fn version(&self) -> NonZeroU32 {
        self.version
    }

    /// Returns the SHA-256 fingerprint of the complete code-owned native schema contract.
    pub const fn fingerprint(&self) -> EvidenceDigest {
        self.fingerprint
    }

    fn canonical_digest(&self) -> EvidenceDigest {
        let mut hash =
            CanonicalEvidenceHasher::new(b"market-squawk:options-reference-native-schema:v1\0");
        hash.identifier(1, &self.name);
        hash.u32(2, self.version.get());
        hash.digest(3, self.fingerprint);
        hash.finish()
    }
}

/// Complete prior-object edge authorizing a conditional GET and exact 304 reuse.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ReferenceConditionalPriorEvidence {
    validator: ReferenceConditionalValidatorEvidence,
    surface: ReferenceSurface,
    configured_locator: SourceIdentifier,
    canonical_media_type: SourceIdentifier,
    native_schema: ReferenceNativeSchemaIdentity,
    prior_payload_digest: EvidenceDigest,
    prior_payload_bytes: u64,
    prior_object_id: SourceIdentifier,
    prior_transport_receipt_digest: EvidenceDigest,
}

impl ReferenceConditionalPriorEvidence {
    #[allow(
        clippy::too_many_arguments,
        reason = "conditional reuse authority is intentionally explicit and closed"
    )]
    pub(crate) fn try_new(
        validator: ReferenceConditionalValidatorEvidence,
        surface: ReferenceSurface,
        configured_locator: SourceIdentifier,
        canonical_media_type: SourceIdentifier,
        native_schema: ReferenceNativeSchemaIdentity,
        prior_payload_digest: EvidenceDigest,
        prior_payload_bytes: u64,
        prior_object_id: SourceIdentifier,
        prior_transport_receipt_digest: EvidenceDigest,
    ) -> Result<Self, PublicationError> {
        if !valid_transport_header(validator.value()) || prior_payload_bytes == 0 {
            return Err(PublicationError::InvalidObjectContext);
        }
        ensure_sha256(prior_payload_digest)?;
        ensure_sha256(prior_transport_receipt_digest)?;
        Ok(Self {
            validator,
            surface,
            configured_locator,
            canonical_media_type,
            native_schema,
            prior_payload_digest,
            prior_payload_bytes,
            prior_object_id,
            prior_transport_receipt_digest,
        })
    }

    /// Returns the exact conditional validator.
    pub const fn validator(&self) -> &ReferenceConditionalValidatorEvidence {
        &self.validator
    }

    /// Returns the prior object's selected surface.
    pub const fn surface(&self) -> &ReferenceSurface {
        &self.surface
    }

    /// Returns the prior object's configured official locator.
    pub const fn configured_locator(&self) -> &SourceIdentifier {
        &self.configured_locator
    }

    /// Returns the prior object's canonical media type.
    pub const fn canonical_media_type(&self) -> &SourceIdentifier {
        &self.canonical_media_type
    }

    /// Returns the prior object's exact native schema identity.
    pub const fn native_schema(&self) -> &ReferenceNativeSchemaIdentity {
        &self.native_schema
    }

    /// Returns the prior payload digest authorized for reuse.
    pub const fn prior_payload_digest(&self) -> EvidenceDigest {
        self.prior_payload_digest
    }

    /// Returns the prior payload byte count authorized for reuse.
    pub const fn prior_payload_bytes(&self) -> u64 {
        self.prior_payload_bytes
    }

    /// Returns the exact prior object identity.
    pub const fn prior_object_id(&self) -> &SourceIdentifier {
        &self.prior_object_id
    }

    /// Returns the exact prior transport receipt used to mint this conditional edge.
    pub const fn prior_transport_receipt_digest(&self) -> EvidenceDigest {
        self.prior_transport_receipt_digest
    }

    fn canonical_digest(&self) -> EvidenceDigest {
        let mut hash =
            CanonicalEvidenceHasher::new(b"market-squawk:options-reference-conditional-prior:v1\0");
        match &self.validator {
            ReferenceConditionalValidatorEvidence::EntityTag(value) => {
                hash.u8(1, 1);
                hash.string(2, value);
            }
            ReferenceConditionalValidatorEvidence::LastModified(value) => {
                hash.u8(1, 2);
                hash.string(2, value);
            }
        }
        hash.surface(3, &self.surface);
        hash.identifier(4, &self.configured_locator);
        hash.identifier(5, &self.canonical_media_type);
        hash.digest(6, self.native_schema.canonical_digest());
        hash.digest(7, self.prior_payload_digest);
        hash.u64(8, self.prior_payload_bytes);
        hash.identifier(9, &self.prior_object_id);
        hash.digest(10, self.prior_transport_receipt_digest);
        hash.finish()
    }
}

/// Secret-free, immutable request seal from which the exact HTTP GET is constructed.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ReferenceOfficialRequestEvidence {
    source_id: SourceIdentifier,
    provider_id: SourceIdentifier,
    source_contract_digest: EvidenceDigest,
    provider: ReferenceProvider,
    surface: ReferenceSurface,
    request_id: SourceIdentifier,
    method: ReferenceRequestMethod,
    configured_locator: SourceIdentifier,
    accept: String,
    accept_encoding_identity: bool,
    user_agent: String,
    body: ReferenceRequestBodyEvidence,
    maximum_decoded_bytes: u64,
    maximum_redirects: u8,
    connect_timeout_nanos: u64,
    read_timeout_nanos: u64,
    total_timeout_nanos: u64,
    operation_timeout_nanos: u64,
    wall_started_at: Timestamp,
    wall_deadline: Timestamp,
    expected_publication_date: Option<CalendarDate>,
    native_schema: ReferenceNativeSchemaIdentity,
    conditional_prior: Option<ReferenceConditionalPriorEvidence>,
}

impl ReferenceOfficialRequestEvidence {
    #[allow(
        clippy::too_many_arguments,
        reason = "the complete official request seal is intentionally explicit"
    )]
    pub(crate) fn try_new(
        source_id: SourceIdentifier,
        provider_id: SourceIdentifier,
        source_contract_digest: EvidenceDigest,
        provider: ReferenceProvider,
        surface: ReferenceSurface,
        request_id: SourceIdentifier,
        configured_locator: SourceIdentifier,
        accept: impl Into<String>,
        user_agent: impl Into<String>,
        maximum_decoded_bytes: u64,
        maximum_redirects: u8,
        connect_timeout_nanos: u64,
        read_timeout_nanos: u64,
        total_timeout_nanos: u64,
        operation_timeout_nanos: u64,
        wall_started_at: Timestamp,
        wall_deadline: Timestamp,
        expected_publication_date: Option<CalendarDate>,
        native_schema: ReferenceNativeSchemaIdentity,
        conditional_prior: Option<ReferenceConditionalPriorEvidence>,
    ) -> Result<Self, PublicationError> {
        let accept = accept.into();
        let user_agent = user_agent.into();
        ensure_sha256(source_contract_digest)?;
        if provider != surface.provider()
            || !valid_transport_header(&accept)
            || !valid_transport_header(&user_agent)
            || maximum_decoded_bytes == 0
            || usize::from(maximum_redirects) > MAX_TRANSPORT_REDIRECTS
            || connect_timeout_nanos == 0
            || read_timeout_nanos == 0
            || total_timeout_nanos == 0
            || operation_timeout_nanos == 0
            || connect_timeout_nanos > total_timeout_nanos
            || read_timeout_nanos > total_timeout_nanos
            || operation_timeout_nanos > total_timeout_nanos
            || wall_deadline <= wall_started_at
            || conditional_prior.as_ref().is_some_and(|prior| {
                prior.surface != surface
                    || prior.configured_locator != configured_locator
                    || prior.native_schema != native_schema
                    || prior.prior_payload_bytes > maximum_decoded_bytes
            })
        {
            return Err(PublicationError::InvalidObjectContext);
        }
        Ok(Self {
            source_id,
            provider_id,
            source_contract_digest,
            provider,
            surface,
            request_id,
            method: ReferenceRequestMethod::Get,
            configured_locator,
            accept,
            accept_encoding_identity: true,
            user_agent,
            body: ReferenceRequestBodyEvidence::absent(),
            maximum_decoded_bytes,
            maximum_redirects,
            connect_timeout_nanos,
            read_timeout_nanos,
            total_timeout_nanos,
            operation_timeout_nanos,
            wall_started_at,
            wall_deadline,
            expected_publication_date,
            native_schema,
            conditional_prior,
        })
    }

    /// Returns the exact Market Squawk source identity.
    pub const fn source_id(&self) -> &SourceIdentifier {
        &self.source_id
    }

    /// Returns the exact shared-budget provider identity.
    pub const fn provider_id(&self) -> &SourceIdentifier {
        &self.provider_id
    }

    /// Returns the digest of the validated source authority contract.
    pub const fn source_contract_digest(&self) -> EvidenceDigest {
        self.source_contract_digest
    }

    /// Returns the exact provider namespace.
    pub const fn provider(&self) -> ReferenceProvider {
        self.provider
    }

    /// Returns the exact requested provider surface.
    pub const fn surface(&self) -> &ReferenceSurface {
        &self.surface
    }

    /// Returns the parent publication request identity.
    pub const fn request_id(&self) -> &SourceIdentifier {
        &self.request_id
    }

    /// Returns the exact HTTP method.
    pub const fn method(&self) -> ReferenceRequestMethod {
        self.method
    }

    /// Returns the configured code-owned locator.
    pub const fn configured_locator(&self) -> &SourceIdentifier {
        &self.configured_locator
    }

    /// Returns the exact `Accept` field.
    pub fn accept(&self) -> &str {
        &self.accept
    }

    /// Returns whether the request explicitly selected identity transfer encoding.
    pub const fn accept_encoding_identity(&self) -> bool {
        self.accept_encoding_identity
    }

    /// Returns the exact secret-free product User-Agent.
    pub fn user_agent(&self) -> &str {
        &self.user_agent
    }

    /// Returns the exact absent request-body evidence.
    pub const fn body(&self) -> &ReferenceRequestBodyEvidence {
        &self.body
    }

    /// Returns the decoded body safety ceiling.
    pub const fn maximum_decoded_bytes(&self) -> u64 {
        self.maximum_decoded_bytes
    }

    /// Returns the exact redirect ceiling used by the HTTP client.
    pub const fn maximum_redirects(&self) -> u8 {
        self.maximum_redirects
    }

    /// Returns the configured connection timeout.
    pub const fn connect_timeout_nanos(&self) -> u64 {
        self.connect_timeout_nanos
    }

    /// Returns the configured no-progress read timeout.
    pub const fn read_timeout_nanos(&self) -> u64 {
        self.read_timeout_nanos
    }

    /// Returns the configured whole-request timeout.
    pub const fn total_timeout_nanos(&self) -> u64 {
        self.total_timeout_nanos
    }

    /// Returns the exact smaller operation window admitted at send time.
    pub const fn operation_timeout_nanos(&self) -> u64 {
        self.operation_timeout_nanos
    }

    /// Returns the parent request admission time.
    pub const fn wall_started_at(&self) -> Timestamp {
        self.wall_started_at
    }

    /// Returns the hard wall-clock deadline.
    pub const fn wall_deadline(&self) -> Timestamp {
        self.wall_deadline
    }

    /// Returns the exact dated-file coordinate required by the request, when applicable.
    pub const fn expected_publication_date(&self) -> Option<CalendarDate> {
        self.expected_publication_date
    }

    /// Returns the exact native decoder contract selected before request construction.
    pub const fn native_schema(&self) -> &ReferenceNativeSchemaIdentity {
        &self.native_schema
    }

    /// Returns the exact prior-object conditional edge, when supplied.
    pub const fn conditional_prior(&self) -> Option<&ReferenceConditionalPriorEvidence> {
        self.conditional_prior.as_ref()
    }

    /// Returns the code-owned canonical SHA-256 request identity.
    pub fn evidence_digest(&self) -> EvidenceDigest {
        let mut hash =
            CanonicalEvidenceHasher::new(b"market-squawk:options-reference-official-request:v4\0");
        hash.identifier(1, &self.source_id);
        hash.identifier(2, &self.provider_id);
        hash.digest(3, self.source_contract_digest);
        hash.provider(4, self.provider);
        hash.surface(5, &self.surface);
        hash.identifier(6, &self.request_id);
        hash.u8(7, 1);
        hash.identifier(8, &self.configured_locator);
        hash.string(9, &self.accept);
        hash.bool(10, self.accept_encoding_identity);
        hash.string(11, &self.user_agent);
        hash.digest(12, self.body.digest());
        hash.u64(13, self.body.bytes());
        hash.u64(14, self.maximum_decoded_bytes);
        hash.u8(15, self.maximum_redirects);
        hash.u64(16, self.connect_timeout_nanos);
        hash.u64(17, self.read_timeout_nanos);
        hash.u64(18, self.total_timeout_nanos);
        hash.u64(19, self.operation_timeout_nanos);
        hash.timestamp(20, self.wall_started_at);
        hash.timestamp(21, self.wall_deadline);
        hash.optional_date(22, self.expected_publication_date);
        hash.digest(23, self.native_schema.canonical_digest());
        hash.optional_digest(
            24,
            self.conditional_prior
                .as_ref()
                .map(ReferenceConditionalPriorEvidence::canonical_digest),
        );
        hash.finish()
    }
}

/// Whether one HTTP receipt supplied new bytes or revalidated one exact prior object.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case", tag = "state")]
pub enum ReferenceResponseDisposition {
    /// HTTP 200 supplied a new, complete representation.
    Modified,
    /// HTTP 304 supplied no representation and revalidated this exact prior object.
    NotModified {
        /// Prior object identity authorized for reuse.
        prior_object_id: SourceIdentifier,
        /// Prior payload digest authorized for reuse.
        prior_payload_digest: EvidenceDigest,
        /// Prior payload bytes authorized for reuse.
        prior_payload_bytes: u64,
        /// Prior transport receipt that minted the conditional edge.
        prior_transport_receipt_digest: EvidenceDigest,
    },
}

/// Complete secret-free request and HTTP response lineage retained with every acquisition result.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ReferenceTransportEvidence {
    request: ReferenceOfficialRequestEvidence,
    request_digest: EvidenceDigest,
    status: u16,
    final_locator: SourceIdentifier,
    redirect_chain: Vec<SourceIdentifier>,
    observed_content_type: Option<String>,
    observed_content_disposition: Option<String>,
    observed_content_encoding: Option<String>,
    declared_content_length: Option<u64>,
    etag: Option<String>,
    cache_last_modified: Option<String>,
    headers_received_at: Timestamp,
    body_completed_at: Timestamp,
    transport_elapsed_nanos: u64,
    response_body_digest: EvidenceDigest,
    response_body_bytes: u64,
    body_complete: bool,
    canonical_media_type: SourceIdentifier,
    native_schema: ReferenceNativeSchemaIdentity,
    disposition: ReferenceResponseDisposition,
    receipt_digest: EvidenceDigest,
}

impl ReferenceTransportEvidence {
    #[allow(
        clippy::too_many_arguments,
        reason = "the complete modified-response receipt is intentionally explicit"
    )]
    pub(crate) fn try_modified(
        request: ReferenceOfficialRequestEvidence,
        status: u16,
        final_locator: SourceIdentifier,
        redirect_chain: Vec<SourceIdentifier>,
        observed_content_type: impl Into<String>,
        observed_content_disposition: Option<String>,
        observed_content_encoding: Option<String>,
        declared_content_length: Option<u64>,
        etag: Option<String>,
        cache_last_modified: Option<String>,
        headers_received_at: Timestamp,
        body_completed_at: Timestamp,
        transport_elapsed_nanos: u64,
        response_body_digest: EvidenceDigest,
        response_body_bytes: u64,
        canonical_media_type: SourceIdentifier,
        native_schema: ReferenceNativeSchemaIdentity,
    ) -> Result<Self, PublicationError> {
        let evidence = Self::try_common(
            request,
            status,
            final_locator,
            redirect_chain,
            Some(observed_content_type.into()),
            observed_content_disposition,
            observed_content_encoding,
            declared_content_length,
            etag,
            cache_last_modified,
            headers_received_at,
            body_completed_at,
            transport_elapsed_nanos,
            response_body_digest,
            response_body_bytes,
            canonical_media_type,
            native_schema,
            ReferenceResponseDisposition::Modified,
        )?;
        if evidence.status != 200
            || evidence.response_body_bytes == 0
            || evidence
                .declared_content_length
                .is_some_and(|bytes| bytes != evidence.response_body_bytes)
            || evidence
                .observed_content_encoding
                .as_deref()
                .is_some_and(|value| !value.eq_ignore_ascii_case("identity"))
        {
            return Err(PublicationError::InvalidObjectContext);
        }
        Ok(evidence)
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "the complete not-modified receipt is intentionally explicit"
    )]
    pub(crate) fn try_not_modified(
        request: ReferenceOfficialRequestEvidence,
        status: u16,
        final_locator: SourceIdentifier,
        redirect_chain: Vec<SourceIdentifier>,
        observed_content_type: Option<String>,
        observed_content_disposition: Option<String>,
        observed_content_encoding: Option<String>,
        declared_content_length: Option<u64>,
        etag: Option<String>,
        cache_last_modified: Option<String>,
        headers_received_at: Timestamp,
        body_completed_at: Timestamp,
        transport_elapsed_nanos: u64,
    ) -> Result<Self, PublicationError> {
        let prior = request
            .conditional_prior()
            .ok_or(PublicationError::InvalidObjectContext)?;
        let disposition = ReferenceResponseDisposition::NotModified {
            prior_object_id: prior.prior_object_id.clone(),
            prior_payload_digest: prior.prior_payload_digest,
            prior_payload_bytes: prior.prior_payload_bytes,
            prior_transport_receipt_digest: prior.prior_transport_receipt_digest,
        };
        let canonical_media_type = prior.canonical_media_type.clone();
        let native_schema = prior.native_schema.clone();
        let evidence = Self::try_common(
            request,
            status,
            final_locator,
            redirect_chain,
            observed_content_type,
            observed_content_disposition,
            observed_content_encoding,
            declared_content_length,
            etag,
            cache_last_modified,
            headers_received_at,
            body_completed_at,
            transport_elapsed_nanos,
            sha256_digest(&[]),
            0,
            canonical_media_type,
            native_schema,
            disposition,
        )?;
        if evidence.status != 304
            || evidence
                .declared_content_length
                .is_some_and(|bytes| bytes != 0)
            || evidence.observed_content_encoding.is_some()
        {
            return Err(PublicationError::InvalidObjectContext);
        }
        Ok(evidence)
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "the shared request/response receipt boundary is intentionally explicit"
    )]
    fn try_common(
        request: ReferenceOfficialRequestEvidence,
        status: u16,
        final_locator: SourceIdentifier,
        redirect_chain: Vec<SourceIdentifier>,
        observed_content_type: Option<String>,
        observed_content_disposition: Option<String>,
        observed_content_encoding: Option<String>,
        declared_content_length: Option<u64>,
        etag: Option<String>,
        cache_last_modified: Option<String>,
        headers_received_at: Timestamp,
        body_completed_at: Timestamp,
        transport_elapsed_nanos: u64,
        response_body_digest: EvidenceDigest,
        response_body_bytes: u64,
        canonical_media_type: SourceIdentifier,
        native_schema: ReferenceNativeSchemaIdentity,
        disposition: ReferenceResponseDisposition,
    ) -> Result<Self, PublicationError> {
        ensure_sha256(response_body_digest)?;
        if redirect_chain.len() > usize::from(request.maximum_redirects)
            || redirect_chain.len() > MAX_TRANSPORT_REDIRECTS
            || (redirect_chain.is_empty() && final_locator != *request.configured_locator())
            || (!redirect_chain.is_empty()
                && redirect_chain
                    .last()
                    .is_none_or(|last| last != &final_locator))
            || observed_content_type
                .as_deref()
                .is_some_and(|value| !valid_transport_header(value))
            || observed_content_disposition
                .as_deref()
                .is_some_and(|value| !valid_transport_header(value))
            || observed_content_encoding
                .as_deref()
                .is_some_and(|value| !valid_transport_header(value))
            || etag
                .as_deref()
                .is_some_and(|value| !valid_transport_header(value))
            || cache_last_modified
                .as_deref()
                .is_some_and(|value| !valid_transport_header(value))
            || headers_received_at < request.wall_started_at
            || body_completed_at < headers_received_at
            || body_completed_at > request.wall_deadline
            || transport_elapsed_nanos == 0
            || transport_elapsed_nanos > MAX_REFERENCE_TRANSPORT_ELAPSED_NANOS
            || transport_elapsed_nanos > request.operation_timeout_nanos
            || native_schema != request.native_schema
        {
            return Err(PublicationError::InvalidObjectContext);
        }
        let request_digest = request.evidence_digest();
        let mut evidence = Self {
            request,
            request_digest,
            status,
            final_locator,
            redirect_chain,
            observed_content_type,
            observed_content_disposition,
            observed_content_encoding,
            declared_content_length,
            etag,
            cache_last_modified,
            headers_received_at,
            body_completed_at,
            transport_elapsed_nanos,
            response_body_digest,
            response_body_bytes,
            body_complete: true,
            canonical_media_type,
            native_schema,
            disposition,
            receipt_digest: sha256_digest(b"pending-reference-receipt"),
        };
        evidence.receipt_digest = evidence.compute_receipt_digest();
        Ok(evidence)
    }

    fn compute_receipt_digest(&self) -> EvidenceDigest {
        let mut hash =
            CanonicalEvidenceHasher::new(b"market-squawk:options-reference-http-receipt:v4\0");
        hash.digest(1, self.request_digest);
        hash.u16(2, self.status);
        hash.identifier(3, &self.final_locator);
        hash.u64(
            4,
            u64::try_from(self.redirect_chain.len()).unwrap_or(u64::MAX),
        );
        for locator in &self.redirect_chain {
            hash.identifier(5, locator);
        }
        hash.optional_string(6, self.observed_content_type.as_deref());
        hash.optional_string(7, self.observed_content_disposition.as_deref());
        hash.optional_string(8, self.observed_content_encoding.as_deref());
        hash.optional_u64(9, self.declared_content_length);
        hash.optional_string(10, self.etag.as_deref());
        hash.optional_string(11, self.cache_last_modified.as_deref());
        hash.timestamp(12, self.headers_received_at);
        hash.timestamp(13, self.body_completed_at);
        hash.u64(14, self.transport_elapsed_nanos);
        hash.digest(15, self.response_body_digest);
        hash.u64(16, self.response_body_bytes);
        hash.bool(17, self.body_complete);
        hash.identifier(18, &self.canonical_media_type);
        hash.digest(19, self.native_schema.canonical_digest());
        match &self.disposition {
            ReferenceResponseDisposition::Modified => hash.u8(20, 1),
            ReferenceResponseDisposition::NotModified {
                prior_object_id,
                prior_payload_digest,
                prior_payload_bytes,
                prior_transport_receipt_digest,
            } => {
                hash.u8(20, 2);
                hash.identifier(21, prior_object_id);
                hash.digest(22, *prior_payload_digest);
                hash.u64(23, *prior_payload_bytes);
                hash.digest(24, *prior_transport_receipt_digest);
            }
        }
        hash.finish()
    }

    /// Returns the exact sealed request this response answered.
    pub const fn request(&self) -> &ReferenceOfficialRequestEvidence {
        &self.request
    }

    /// Returns the SHA-256 identity of the exact official request contract.
    pub const fn request_digest(&self) -> EvidenceDigest {
        self.request_digest
    }

    /// Returns the SHA-256 identity of this complete admitted HTTP receipt.
    pub const fn receipt_digest(&self) -> EvidenceDigest {
        self.receipt_digest
    }

    /// Returns the admitted HTTP status.
    pub const fn status(&self) -> u16 {
        self.status
    }

    /// Returns the final admitted locator.
    pub const fn final_locator(&self) -> &SourceIdentifier {
        &self.final_locator
    }

    /// Returns each exact admitted redirect locator in observation order.
    pub fn redirect_chain(&self) -> &[SourceIdentifier] {
        &self.redirect_chain
    }

    /// Returns the exact observed Content-Type field before canonical media selection.
    pub fn observed_content_type(&self) -> Option<&str> {
        self.observed_content_type.as_deref()
    }

    /// Returns the exact observed Content-Disposition field when supplied.
    pub fn observed_content_disposition(&self) -> Option<&str> {
        self.observed_content_disposition.as_deref()
    }

    /// Returns the exact observed Content-Encoding field when supplied.
    pub fn observed_content_encoding(&self) -> Option<&str> {
        self.observed_content_encoding.as_deref()
    }

    /// Returns the exact declared response length when supplied.
    pub const fn declared_content_length(&self) -> Option<u64> {
        self.declared_content_length
    }

    /// Returns the exact observed ETag when supplied.
    pub fn etag(&self) -> Option<&str> {
        self.etag.as_deref()
    }

    /// Returns the exact observed cache Last-Modified field when supplied.
    pub fn cache_last_modified(&self) -> Option<&str> {
        self.cache_last_modified.as_deref()
    }

    /// Returns the trusted local response-header observation time.
    pub const fn headers_received_at(&self) -> Timestamp {
        self.headers_received_at
    }

    /// Returns the trusted local terminal-body observation time.
    pub const fn body_completed_at(&self) -> Timestamp {
        self.body_completed_at
    }

    /// Returns request-send through terminal-body elapsed time.
    pub const fn transport_elapsed_nanos(&self) -> u64 {
        self.transport_elapsed_nanos
    }

    /// Returns the exact HTTP response-body digest.
    pub const fn response_body_digest(&self) -> EvidenceDigest {
        self.response_body_digest
    }

    /// Returns the exact HTTP response-body byte count.
    pub const fn response_body_bytes(&self) -> u64 {
        self.response_body_bytes
    }

    /// Returns terminal body-completion evidence.
    pub const fn body_complete(&self) -> bool {
        self.body_complete
    }

    /// Returns the canonical media type selected after response admission.
    pub const fn canonical_media_type(&self) -> &SourceIdentifier {
        &self.canonical_media_type
    }

    /// Returns the exact native schema identity used to admit the body.
    pub const fn native_schema(&self) -> &ReferenceNativeSchemaIdentity {
        &self.native_schema
    }

    /// Returns whether the response supplied bytes or revalidated one exact prior object.
    pub const fn disposition(&self) -> &ReferenceResponseDisposition {
        &self.disposition
    }

    /// Returns whether this receipt represents a new complete body.
    pub const fn is_modified(&self) -> bool {
        matches!(&self.disposition, ReferenceResponseDisposition::Modified)
    }
}

/// Exact raw-object and decoder lineage shared by every record decoded from the object.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ReferenceObjectContext {
    provider: ReferenceProvider,
    surface: ReferenceSurface,
    object_id: SourceIdentifier,
    configured_locator: SourceIdentifier,
    final_locator: SourceIdentifier,
    media_type: SourceIdentifier,
    payload_digest: EvidenceDigest,
    payload_bytes: u64,
    native_schema: ReferenceNativeSchemaIdentity,
    clocks: ObjectClockEvidence,
    source_filename: Option<SourceIdentifier>,
    source_publication_date: Option<CalendarDate>,
    http_last_modified: Option<HttpLastModifiedEvidence>,
    transport_evidence: ReferenceTransportEvidence,
}

impl ReferenceObjectContext {
    /// Constructs a complete exact provider-native object receipt.
    ///
    /// # Errors
    ///
    /// Rejects any mismatch among provider, request, response, body, media, schema, clocks, or
    /// source-file evidence. A context cannot exist without a complete modified HTTP receipt.
    #[allow(
        clippy::too_many_arguments,
        reason = "the complete source-object evidence boundary is intentionally explicit"
    )]
    pub fn try_new(
        provider: ReferenceProvider,
        surface: ReferenceSurface,
        object_id: SourceIdentifier,
        configured_locator: SourceIdentifier,
        final_locator: SourceIdentifier,
        media_type: SourceIdentifier,
        payload_digest: EvidenceDigest,
        payload_bytes: u64,
        native_schema: ReferenceNativeSchemaIdentity,
        clocks: ObjectClockEvidence,
        source_filename: Option<SourceIdentifier>,
        source_publication_date: Option<CalendarDate>,
        http_last_modified: Option<HttpLastModifiedEvidence>,
        transport_evidence: ReferenceTransportEvidence,
    ) -> Result<Self, PublicationError> {
        ensure_sha256(payload_digest)?;
        let expected_posted =
            source_publication_date.map(ResearchTemporalCoordinate::calendar_date);
        if provider != surface.provider()
            || provider != transport_evidence.request.provider
            || surface != transport_evidence.request.surface
            || configured_locator != transport_evidence.request.configured_locator
            || final_locator != transport_evidence.final_locator
            || media_type != transport_evidence.canonical_media_type
            || payload_bytes == 0
            || payload_digest != transport_evidence.response_body_digest
            || payload_bytes != transport_evidence.response_body_bytes
            || native_schema != transport_evidence.native_schema
            || !transport_evidence.is_modified()
            || expected_posted.as_ref() != clocks.posted()
            || source_publication_date.is_some() && source_filename.is_none()
            || clocks.received_at() != transport_evidence.body_completed_at
            || clocks.transport_elapsed_nanos() != transport_evidence.transport_elapsed_nanos
            || http_last_modified
                .as_ref()
                .is_some_and(|evidence| evidence.instant() > clocks.received_at())
            || http_last_modified
                .as_ref()
                .map(HttpLastModifiedEvidence::as_str)
                != transport_evidence.cache_last_modified.as_deref()
        {
            return Err(PublicationError::InvalidObjectContext);
        }
        Ok(Self {
            provider,
            surface,
            object_id,
            configured_locator,
            final_locator,
            media_type,
            payload_digest,
            payload_bytes,
            native_schema,
            clocks,
            source_filename,
            source_publication_date,
            http_last_modified,
            transport_evidence,
        })
    }

    /// Returns the provider namespace.
    pub const fn provider(&self) -> ReferenceProvider {
        self.provider
    }

    /// Returns the exact selected surface.
    pub const fn surface(&self) -> &ReferenceSurface {
        &self.surface
    }

    /// Returns the object identity.
    pub const fn object_id(&self) -> &SourceIdentifier {
        &self.object_id
    }

    /// Returns the configured locator before redirects.
    pub const fn configured_locator(&self) -> &SourceIdentifier {
        &self.configured_locator
    }

    /// Returns the final admitted locator after redirects.
    pub const fn final_locator(&self) -> &SourceIdentifier {
        &self.final_locator
    }

    /// Returns the exact canonical media type.
    pub const fn media_type(&self) -> &SourceIdentifier {
        &self.media_type
    }

    /// Returns the exact payload digest.
    pub const fn payload_digest(&self) -> EvidenceDigest {
        self.payload_digest
    }

    /// Returns the exact response byte count.
    pub const fn payload_bytes(&self) -> u64 {
        self.payload_bytes
    }

    /// Returns the closed provider-native decoder name.
    pub const fn native_schema(&self) -> &SourceIdentifier {
        self.native_schema.name()
    }

    /// Returns the complete provider-native decoder identity.
    pub const fn native_schema_identity(&self) -> &ReferenceNativeSchemaIdentity {
        &self.native_schema
    }

    /// Returns the retained clock evidence.
    pub const fn clocks(&self) -> &ObjectClockEvidence {
        &self.clocks
    }

    /// Returns the exact validated provider filename when this surface publishes one.
    pub const fn source_filename(&self) -> Option<&SourceIdentifier> {
        self.source_filename.as_ref()
    }

    /// Returns the provider filename/report calendar date without an invented time zone.
    pub const fn source_publication_date(&self) -> Option<CalendarDate> {
        self.source_publication_date
    }

    /// Returns the exact HTTP `Last-Modified` field independently of provider/local clocks.
    pub const fn http_last_modified(&self) -> Option<&HttpLastModifiedEvidence> {
        self.http_last_modified.as_ref()
    }

    /// Returns complete retained request/transport evidence.
    pub const fn transport_evidence(&self) -> &ReferenceTransportEvidence {
        &self.transport_evidence
    }
}

fn valid_transport_header(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_TRANSPORT_HEADER_EVIDENCE_BYTES
        && value.is_ascii()
        && !value.bytes().any(|byte| byte.is_ascii_control())
}

fn ensure_sha256(digest: EvidenceDigest) -> Result<(), PublicationError> {
    if digest.algorithm() != DigestAlgorithm::Sha256
        || digest.bytes().len() != REFERENCE_EVIDENCE_DIGEST_BYTES
        || digest.bytes().iter().all(|byte| *byte == 0)
    {
        Err(PublicationError::InvalidObjectContext)
    } else {
        Ok(())
    }
}

fn sha256_digest(bytes: &[u8]) -> EvidenceDigest {
    EvidenceDigest::new(
        DigestAlgorithm::Sha256,
        <[u8; REFERENCE_EVIDENCE_DIGEST_BYTES]>::from(Sha256::digest(bytes)),
    )
}

struct CanonicalEvidenceHasher(Sha256);

impl CanonicalEvidenceHasher {
    fn new(domain: &'static [u8]) -> Self {
        let mut hash = Sha256::new();
        hash.update(domain);
        Self(hash)
    }

    fn field(&mut self, tag: u16, bytes: &[u8]) {
        self.0.update(tag.to_be_bytes());
        self.0
            .update(u64::try_from(bytes.len()).unwrap_or(u64::MAX).to_be_bytes());
        self.0.update(bytes);
    }

    fn bool(&mut self, tag: u16, value: bool) {
        self.field(tag, &[u8::from(value)]);
    }

    fn u8(&mut self, tag: u16, value: u8) {
        self.field(tag, &[value]);
    }

    fn u16(&mut self, tag: u16, value: u16) {
        self.field(tag, &value.to_be_bytes());
    }

    fn u32(&mut self, tag: u16, value: u32) {
        self.field(tag, &value.to_be_bytes());
    }

    fn u64(&mut self, tag: u16, value: u64) {
        self.field(tag, &value.to_be_bytes());
    }

    fn string(&mut self, tag: u16, value: &str) {
        self.field(tag, value.as_bytes());
    }

    fn identifier(&mut self, tag: u16, value: &SourceIdentifier) {
        self.string(tag, value.as_str());
    }

    fn digest(&mut self, tag: u16, value: EvidenceDigest) {
        self.field(tag, &[1]);
        self.field(tag.saturating_add(1_000), &value.bytes());
    }

    fn provider(&mut self, tag: u16, provider: ReferenceProvider) {
        self.u8(
            tag,
            match provider {
                ReferenceProvider::Occ => 1,
                ReferenceProvider::Cboe => 2,
            },
        );
    }

    fn surface(&mut self, tag: u16, surface: &ReferenceSurface) {
        let (kind, coordinate, ordinal) = match surface {
            ReferenceSurface::CboeAllSeries { venue } => (1, Some(venue.stable_label()), None),
            ReferenceSurface::OccDlpSelectedText => (2, None, None),
            ReferenceSurface::OccDlpDailyText => (3, None, None),
            ReferenceSurface::OccDlpDailyXml => (4, None, None),
            ReferenceSurface::OccMemoIndexCsv => (5, None, None),
            ReferenceSurface::OccMemoIndexJson => (6, None, None),
            ReferenceSurface::OccMemoDocument { memo_number } => (7, None, Some(*memo_number)),
            ReferenceSurface::OccMemoAttachment {
                memo_number,
                ordinal: attachment_ordinal,
            } => (
                8,
                Some(if attachment_ordinal.get() == 0 {
                    "invalid"
                } else {
                    "attachment"
                }),
                Some(*memo_number),
            ),
        };
        self.u8(tag, kind);
        self.optional_string(tag.saturating_add(100), coordinate);
        self.optional_u64(tag.saturating_add(200), ordinal);
        if let ReferenceSurface::OccMemoAttachment { ordinal, .. } = surface {
            self.u32(tag.saturating_add(300), ordinal.get());
        }
    }

    fn timestamp(&mut self, tag: u16, value: Timestamp) {
        self.field(tag, &value.unix_nanos().to_be_bytes());
    }

    fn optional_string(&mut self, tag: u16, value: Option<&str>) {
        self.bool(tag, value.is_some());
        if let Some(value) = value {
            self.string(tag.saturating_add(500), value);
        }
    }

    fn optional_u64(&mut self, tag: u16, value: Option<u64>) {
        self.bool(tag, value.is_some());
        if let Some(value) = value {
            self.u64(tag.saturating_add(500), value);
        }
    }

    fn optional_date(&mut self, tag: u16, value: Option<CalendarDate>) {
        self.bool(tag, value.is_some());
        if let Some(value) = value {
            self.u16(tag.saturating_add(500), value.year());
            self.u8(tag.saturating_add(501), value.month());
            self.u8(tag.saturating_add(502), value.day());
        }
    }

    fn optional_digest(&mut self, tag: u16, value: Option<EvidenceDigest>) {
        self.bool(tag, value.is_some());
        if let Some(value) = value {
            self.digest(tag.saturating_add(500), value);
        }
    }

    fn finish(self) -> EvidenceDigest {
        EvidenceDigest::new(DigestAlgorithm::Sha256, self.0.finalize().into())
    }
}

/// Provider pagination state observed after one page/object.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case", tag = "state")]
pub enum PageTerminalState {
    /// The provider supplied a next cursor.
    More {
        /// Opaque provider cursor retained only as request/completeness evidence.
        next_cursor: SourceIdentifier,
    },
    /// The provider/file contract reported terminal completion.
    Terminal,
    /// No reviewed terminal signal was available.
    Unknown,
}

/// Reconciled receipt for one exact file or page.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ReferencePageReceipt {
    context: ReferenceObjectContext,
    page_ordinal: NonZeroU32,
    returned_records: u32,
    rejected_records: u32,
    strict_row_set_digest: EvidenceDigest,
    alias_assertion_set: ReferenceAliasAssertionSetEvidence,
    terminal_state: PageTerminalState,
}

impl ReferencePageReceipt {
    /// Constructs parser-owned page evidence without upgrading an unknown terminal signal.
    ///
    /// Construction is crate-private because completeness is an authority boundary: external
    /// callers may inspect and persist a receipt, but only this crate's strict provider parsers
    /// may mint one.
    pub(crate) const fn new(
        context: ReferenceObjectContext,
        page_ordinal: NonZeroU32,
        returned_records: u32,
        rejected_records: u32,
        strict_row_set_digest: EvidenceDigest,
        alias_assertion_set: ReferenceAliasAssertionSetEvidence,
        terminal_state: PageTerminalState,
    ) -> Self {
        Self {
            context,
            page_ordinal,
            returned_records,
            rejected_records,
            strict_row_set_digest,
            alias_assertion_set,
            terminal_state,
        }
    }

    /// Returns exact object evidence.
    pub const fn context(&self) -> &ReferenceObjectContext {
        &self.context
    }

    /// Returns the one-based page ordinal.
    pub const fn page_ordinal(&self) -> NonZeroU32 {
        self.page_ordinal
    }

    /// Returns valid decoded records, not requested identifiers.
    pub const fn returned_records(&self) -> u32 {
        self.returned_records
    }

    /// Returns rows rejected by a parser only when the calling acquisition explicitly retained a
    /// partial page; strict decoders normally fail before producing a receipt.
    pub const fn rejected_records(&self) -> u32 {
        self.rejected_records
    }

    /// Returns the exact ordered identity of every typed row delivered by the strict parser.
    pub const fn strict_row_set_digest(&self) -> EvidenceDigest {
        self.strict_row_set_digest
    }

    pub(crate) const fn alias_assertion_set(&self) -> ReferenceAliasAssertionSetEvidence {
        self.alias_assertion_set
    }

    /// Returns the observed terminal state.
    pub const fn terminal_state(&self) -> &PageTerminalState {
        &self.terminal_state
    }
}

/// Publication request, object evidence, clock, or request-accounting failure.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum PublicationError {
    /// Caller limits were zero or exceeded code-owned safety ceilings.
    #[error("invalid option-reference publication limits")]
    InvalidLimits,
    /// The surface closure or request chronology was invalid.
    #[error("invalid option-reference publication request")]
    InvalidRequest,
    /// Provider and surface, bytes, or other object evidence did not agree.
    #[error("invalid option-reference object context")]
    InvalidObjectContext,
    /// Source availability was later than local receipt.
    #[error("invalid option-reference object clock order")]
    InvalidClockOrder,
    /// Aggregate page, byte, record, or conflict bounds were exceeded.
    #[error("option-reference publication limit exceeded")]
    LimitsExceeded,
}
