use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroU32;

use market_squawk_domain::{
    AvailabilityEvidence, EvidenceDigest, ResearchTemporalCoordinate, SourceIdentifier, Timestamp,
};
use serde::Serialize;
use thiserror::Error;

use crate::{CboeSeriesReference, CboeVenue, OccDlpProductReference, OccMemoDiscovery};

const MAX_PUBLICATION_SURFACES: usize = 64;
const MAX_PUBLICATION_PAGES: u32 = 10_000;
const MAX_PUBLICATION_BYTES: u64 = 2 * 1024 * 1024 * 1024;
const MAX_PUBLICATION_RECORDS: u64 = 12_000_000;
const MAX_PUBLICATION_CONFLICTS: usize = 100_000;

/// Provider namespace retained by every reference object and record.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ReferenceProvider {
    /// The Options Clearing Corporation.
    Occ,
    /// Cboe U.S. Options exchanges.
    Cboe,
}

/// Exact selected reference surface represented by one requested publication component.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
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
        if surfaces.is_empty() || surfaces.len() > limits.max_surfaces || deadline <= requested_at {
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

/// Provider clocks retained without converting date-only values to invented instants.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ObjectClockEvidence {
    posted: Option<ResearchTemporalCoordinate>,
    effective: Option<ResearchTemporalCoordinate>,
    availability: AvailabilityEvidence,
    received_at: Timestamp,
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
    ) -> Result<Self, PublicationError> {
        if availability
            .reported_at()
            .is_some_and(|available_at| available_at > received_at)
        {
            return Err(PublicationError::InvalidClockOrder);
        }
        Ok(Self {
            posted,
            effective,
            availability,
            received_at,
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
    native_schema: SourceIdentifier,
    clocks: ObjectClockEvidence,
}

impl ReferenceObjectContext {
    /// Constructs exact provider-native object evidence.
    ///
    /// # Errors
    ///
    /// Rejects a provider/surface mismatch or an empty payload.
    #[allow(
        clippy::too_many_arguments,
        reason = "the source-object evidence boundary is intentionally explicit"
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
        native_schema: SourceIdentifier,
        clocks: ObjectClockEvidence,
    ) -> Result<Self, PublicationError> {
        if provider != surface.provider() || payload_bytes == 0 {
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

    /// Returns the exact media type.
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

    /// Returns the closed provider-native decoder identity.
    pub const fn native_schema(&self) -> &SourceIdentifier {
        &self.native_schema
    }

    /// Returns the retained clock evidence.
    pub const fn clocks(&self) -> &ObjectClockEvidence {
        &self.clocks
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
    terminal_state: PageTerminalState,
}

impl ReferencePageReceipt {
    /// Constructs one page receipt without upgrading an unknown terminal signal.
    pub const fn new(
        context: ReferenceObjectContext,
        page_ordinal: NonZeroU32,
        returned_records: u32,
        rejected_records: u32,
        terminal_state: PageTerminalState,
    ) -> Self {
        Self {
            context,
            page_ordinal,
            returned_records,
            rejected_records,
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

    /// Returns the observed terminal state.
    pub const fn terminal_state(&self) -> &PageTerminalState {
        &self.terminal_state
    }
}

/// Completeness for one exact requested surface.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case", tag = "state")]
pub enum SurfaceCompleteness {
    /// Every page was contiguous, valid, and ended with an observed terminal signal.
    Complete,
    /// No page/object was admitted for the requested surface.
    Missing,
    /// Pages were noncontiguous or did not end in an observed terminal signal.
    IncompletePageChain,
    /// At least one retained page reported rejected records.
    RejectedRecords {
        /// Aggregate rejected record count.
        count: u64,
    },
    /// Page receipts and decoded catalog records did not reconcile.
    ObservationCountMismatch {
        /// Valid rows claimed by the page receipts.
        receipt_records: u64,
        /// Valid rows admitted through the provider-native catalog path.
        catalog_records: u64,
    },
}

/// Whole-request completeness, independent of provider conflicts.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case", tag = "state")]
pub enum PublicationCompleteness {
    /// Every exact requested surface completed without rejected records.
    Complete,
    /// At least one requested surface was missing, incomplete, or rejected.
    Partial {
        /// Per-surface dispositions in request order.
        surfaces: Vec<(ReferenceSurface, SurfaceCompleteness)>,
    },
}

/// Closed conflict class preserved instead of selecting an arbitrary provider row.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CatalogConflictKind {
    /// One Cboe Symbol ID mapped to distinct OSI contracts.
    CboeSymbolMapsMultipleOsi,
    /// One OSI contract mapped to distinct Cboe Symbol IDs.
    CboeOsiMapsMultipleSymbols,
    /// One page coordinate was observed with different exact objects.
    PageCoordinateDivergence,
    /// One OCC memo number was observed with different discovery content.
    OccMemoRevisionDivergence,
}

/// Exact conflicting source identities; no implicit winner is selected.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CatalogConflict {
    kind: CatalogConflictKind,
    natural_key: SourceIdentifier,
    first_evidence: SourceIdentifier,
    second_evidence: SourceIdentifier,
}

impl CatalogConflict {
    fn new(
        kind: CatalogConflictKind,
        natural_key: SourceIdentifier,
        first_evidence: SourceIdentifier,
        second_evidence: SourceIdentifier,
    ) -> Self {
        Self {
            kind,
            natural_key,
            first_evidence,
            second_evidence,
        }
    }

    /// Returns the conflict class.
    pub const fn kind(&self) -> CatalogConflictKind {
        self.kind
    }

    /// Returns the conflicting natural key.
    pub const fn natural_key(&self) -> &SourceIdentifier {
        &self.natural_key
    }

    /// Returns the first exact row/object evidence identity.
    pub const fn first_evidence(&self) -> &SourceIdentifier {
        &self.first_evidence
    }

    /// Returns the second exact row/object evidence identity.
    pub const fn second_evidence(&self) -> &SourceIdentifier {
        &self.second_evidence
    }
}

/// Actual observations and objects admitted for one catalog assembly.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CatalogCounts {
    pages: u64,
    bytes: u64,
    returned_records: u64,
    rejected_records: u64,
    cboe_series: u64,
    occ_dlp_products: u64,
    occ_memo_discoveries: u64,
    duplicate_records: u64,
}

impl CatalogCounts {
    /// Returns admitted exact page/object count.
    pub const fn pages(self) -> u64 {
        self.pages
    }

    /// Returns exact source bytes represented by page receipts.
    pub const fn bytes(self) -> u64 {
        self.bytes
    }

    /// Returns valid decoded records reported by page receipts.
    pub const fn returned_records(self) -> u64 {
        self.returned_records
    }

    /// Returns rejected records reported by page receipts.
    pub const fn rejected_records(self) -> u64 {
        self.rejected_records
    }

    /// Returns admitted Cboe series observations.
    pub const fn cboe_series(self) -> u64 {
        self.cboe_series
    }

    /// Returns admitted OCC DLP product/root observations.
    pub const fn occ_dlp_products(self) -> u64 {
        self.occ_dlp_products
    }

    /// Returns admitted OCC memo discoveries.
    pub const fn occ_memo_discoveries(self) -> u64 {
        self.occ_memo_discoveries
    }

    /// Returns exact duplicate provider records encountered.
    pub const fn duplicate_records(self) -> u64 {
        self.duplicate_records
    }
}

/// Final bounded catalog evidence for one publication request.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PublicationCatalog {
    request: PublicationRequest,
    completeness: PublicationCompleteness,
    counts: CatalogCounts,
    conflicts: Vec<CatalogConflict>,
}

impl PublicationCatalog {
    /// Returns the exact request this catalog answers.
    pub const fn request(&self) -> &PublicationRequest {
        &self.request
    }

    /// Returns whole-request completeness.
    pub const fn completeness(&self) -> &PublicationCompleteness {
        &self.completeness
    }

    /// Returns actual object/observation counts.
    pub const fn counts(&self) -> CatalogCounts {
        self.counts
    }

    /// Returns every retained source conflict.
    pub fn conflicts(&self) -> &[CatalogConflict] {
        &self.conflicts
    }

    /// Returns whether the catalog is complete and conflict-free enough for downstream atomic
    /// publication. This does not assert canonical identity resolution or workflow availability.
    pub fn publication_eligible(&self) -> bool {
        matches!(self.completeness, PublicationCompleteness::Complete) && self.conflicts.is_empty()
    }
}

#[derive(Clone, Debug)]
struct MappingEvidence {
    value: String,
    evidence: SourceIdentifier,
}

/// Bounded catalog assembler that preserves provider disagreement.
#[derive(Debug)]
pub struct PublicationCatalogBuilder {
    request: PublicationRequest,
    pages: BTreeMap<(ReferenceSurface, u32), ReferencePageReceipt>,
    cboe_symbols: BTreeMap<String, MappingEvidence>,
    cboe_osi: BTreeMap<String, MappingEvidence>,
    cboe_records: BTreeSet<(ReferenceSurface, String, String, SourceIdentifier)>,
    occ_dlp_records: BTreeSet<(ReferenceSurface, String, SourceIdentifier)>,
    occ_memos: BTreeMap<u64, MappingEvidence>,
    observations_by_surface: BTreeMap<ReferenceSurface, u64>,
    conflicts: Vec<CatalogConflict>,
    counts: CatalogCounts,
}

impl PublicationCatalogBuilder {
    /// Begins one catalog assembly under the request's fixed limits.
    pub fn new(request: PublicationRequest) -> Self {
        Self {
            request,
            pages: BTreeMap::new(),
            cboe_symbols: BTreeMap::new(),
            cboe_osi: BTreeMap::new(),
            cboe_records: BTreeSet::new(),
            occ_dlp_records: BTreeSet::new(),
            occ_memos: BTreeMap::new(),
            observations_by_surface: BTreeMap::new(),
            conflicts: Vec::new(),
            counts: CatalogCounts::default(),
        }
    }

    /// Retains one exact page receipt.
    ///
    /// # Errors
    ///
    /// Rejects unrequested surfaces or aggregate bounds. A divergent duplicate coordinate is
    /// preserved as a conflict.
    pub fn record_page(&mut self, receipt: ReferencePageReceipt) -> Result<(), PublicationError> {
        if self
            .request
            .surfaces
            .binary_search(receipt.context.surface())
            .is_err()
        {
            return Err(PublicationError::UnrequestedSurface);
        }
        let next_pages = self.counts.pages.saturating_add(1);
        let next_bytes = self
            .counts
            .bytes
            .checked_add(receipt.context.payload_bytes)
            .ok_or(PublicationError::LimitsExceeded)?;
        let next_returned = self
            .counts
            .returned_records
            .checked_add(u64::from(receipt.returned_records))
            .ok_or(PublicationError::LimitsExceeded)?;
        let next_rejected = self
            .counts
            .rejected_records
            .checked_add(u64::from(receipt.rejected_records))
            .ok_or(PublicationError::LimitsExceeded)?;
        if next_pages > u64::from(self.request.limits.max_pages)
            || next_bytes > self.request.limits.max_total_bytes
            || next_returned.saturating_add(next_rejected) > self.request.limits.max_total_records
        {
            return Err(PublicationError::LimitsExceeded);
        }

        let key = (receipt.context.surface.clone(), receipt.page_ordinal.get());
        if let Some(existing) = self.pages.get(&key) {
            if existing == &receipt {
                self.counts.duplicate_records = self.counts.duplicate_records.saturating_add(1);
                return Ok(());
            }
            let existing_object = existing.context.object_id.clone();
            self.push_conflict(CatalogConflict::new(
                CatalogConflictKind::PageCoordinateDivergence,
                source_identifier(format!(
                    "page:{}:{}",
                    receipt.context.object_id.as_str(),
                    receipt.page_ordinal.get()
                ))?,
                existing_object,
                receipt.context.object_id.clone(),
            ))?;
            return Ok(());
        }
        self.counts.pages = next_pages;
        self.counts.bytes = next_bytes;
        self.counts.returned_records = next_returned;
        self.counts.rejected_records = next_rejected;
        self.pages.insert(key, receipt);
        Ok(())
    }

    /// Records one Cboe mapping while preserving cross-venue multiplicity and mapping conflicts.
    ///
    /// # Errors
    ///
    /// Rejects a record outside the request or any conflict/record bound.
    pub fn record_cboe_series(
        &mut self,
        record: &CboeSeriesReference,
    ) -> Result<(), PublicationError> {
        if self
            .request
            .surfaces
            .binary_search(record.object_context().surface())
            .is_err()
        {
            return Err(PublicationError::UnrequestedSurface);
        }
        let next_records = self.counts.cboe_series.saturating_add(1);
        if next_records > self.request.limits.max_total_records {
            return Err(PublicationError::LimitsExceeded);
        }
        let symbol = record.cboe_symbol_id().as_str().to_owned();
        let osi = record.contract().osi().as_str().to_owned();
        let evidence = record.record_id().clone();
        let exact_key = (
            record.object_context().surface().clone(),
            symbol.clone(),
            osi.clone(),
            evidence.clone(),
        );
        if !self.cboe_records.insert(exact_key) {
            self.counts.duplicate_records = self.counts.duplicate_records.saturating_add(1);
            return Ok(());
        }

        if let Some(existing) = self.cboe_symbols.get(&symbol).cloned() {
            if existing.value != osi {
                self.push_conflict(CatalogConflict::new(
                    CatalogConflictKind::CboeSymbolMapsMultipleOsi,
                    source_identifier(format!("cboe-symbol:{symbol}"))?,
                    existing.evidence,
                    evidence.clone(),
                ))?;
            }
        } else {
            self.cboe_symbols.insert(
                symbol.clone(),
                MappingEvidence {
                    value: osi.clone(),
                    evidence: evidence.clone(),
                },
            );
        }
        if let Some(existing) = self.cboe_osi.get(&osi).cloned() {
            if existing.value != symbol {
                self.push_conflict(CatalogConflict::new(
                    CatalogConflictKind::CboeOsiMapsMultipleSymbols,
                    source_identifier(format!("osi:{osi}"))?,
                    existing.evidence,
                    evidence,
                ))?;
            }
        } else {
            self.cboe_osi.insert(
                osi,
                MappingEvidence {
                    value: symbol,
                    evidence,
                },
            );
        }
        self.counts.cboe_series = next_records;
        self.increment_surface_observations(record.object_context().surface())?;
        Ok(())
    }

    /// Records one OCC DLP product/root observation for receipt reconciliation.
    ///
    /// # Errors
    ///
    /// Rejects records outside the exact request or aggregate record bounds.
    pub fn record_occ_dlp_product(
        &mut self,
        record: &OccDlpProductReference,
    ) -> Result<(), PublicationError> {
        if self
            .request
            .surfaces
            .binary_search(record.object_context().surface())
            .is_err()
        {
            return Err(PublicationError::UnrequestedSurface);
        }
        let key = (
            record.object_context().surface().clone(),
            record.options_symbol().as_str().to_owned(),
            record.record_id().clone(),
        );
        if !self.occ_dlp_records.insert(key) {
            self.counts.duplicate_records = self.counts.duplicate_records.saturating_add(1);
            return Ok(());
        }
        let next = self.counts.occ_dlp_products.saturating_add(1);
        if next > self.request.limits.max_total_records {
            return Err(PublicationError::LimitsExceeded);
        }
        self.counts.occ_dlp_products = next;
        self.increment_surface_observations(record.object_context().surface())?;
        Ok(())
    }

    /// Records one OCC memo discovery. A changed title/category/effective-date digest is retained
    /// as a source revision conflict; it is never interpreted as contract economics.
    ///
    /// # Errors
    ///
    /// Rejects a record outside the request or any configured bound.
    pub fn record_occ_memo(&mut self, memo: &OccMemoDiscovery) -> Result<(), PublicationError> {
        if self
            .request
            .surfaces
            .binary_search(memo.object_context().surface())
            .is_err()
        {
            return Err(PublicationError::UnrequestedSurface);
        }
        let next_records = self.counts.occ_memo_discoveries.saturating_add(1);
        if next_records > self.request.limits.max_total_records {
            return Err(PublicationError::LimitsExceeded);
        }
        let content = memo.discovery_digest_hex();
        let evidence = memo.record_id().clone();
        if let Some(existing) = self.occ_memos.get(&memo.memo_number()).cloned() {
            if existing.value == content {
                self.counts.duplicate_records = self.counts.duplicate_records.saturating_add(1);
                return Ok(());
            }
            self.push_conflict(CatalogConflict::new(
                CatalogConflictKind::OccMemoRevisionDivergence,
                source_identifier(format!("occ-memo:{}", memo.memo_number()))?,
                existing.evidence,
                evidence,
            ))?;
        } else {
            self.occ_memos.insert(
                memo.memo_number(),
                MappingEvidence {
                    value: content,
                    evidence,
                },
            );
        }
        self.counts.occ_memo_discoveries = next_records;
        self.increment_surface_observations(memo.object_context().surface())?;
        Ok(())
    }

    /// Completes the bounded request and derives per-surface page closure.
    pub fn finish(self) -> PublicationCatalog {
        let mut states = Vec::with_capacity(self.request.surfaces.len());
        let mut all_complete = true;
        for surface in &self.request.surfaces {
            let pages: Vec<&ReferencePageReceipt> = self
                .pages
                .iter()
                .filter_map(|((candidate, _), receipt)| (candidate == surface).then_some(receipt))
                .collect();
            let catalog_records = self
                .observations_by_surface
                .get(surface)
                .copied()
                .unwrap_or(0);
            let state = surface_completeness(&pages, catalog_records);
            all_complete &= matches!(&state, SurfaceCompleteness::Complete);
            states.push((surface.clone(), state));
        }
        let completeness = if all_complete {
            PublicationCompleteness::Complete
        } else {
            PublicationCompleteness::Partial { surfaces: states }
        };
        PublicationCatalog {
            request: self.request,
            completeness,
            counts: self.counts,
            conflicts: self.conflicts,
        }
    }

    fn push_conflict(&mut self, conflict: CatalogConflict) -> Result<(), PublicationError> {
        if self.conflicts.len() >= self.request.limits.max_conflicts {
            return Err(PublicationError::LimitsExceeded);
        }
        self.conflicts.push(conflict);
        Ok(())
    }

    fn increment_surface_observations(
        &mut self,
        surface: &ReferenceSurface,
    ) -> Result<(), PublicationError> {
        let entry = self
            .observations_by_surface
            .entry(surface.clone())
            .or_default();
        *entry = entry
            .checked_add(1)
            .ok_or(PublicationError::LimitsExceeded)?;
        if *entry > self.request.limits.max_total_records {
            return Err(PublicationError::LimitsExceeded);
        }
        Ok(())
    }
}

fn surface_completeness(
    pages: &[&ReferencePageReceipt],
    catalog_records: u64,
) -> SurfaceCompleteness {
    if pages.is_empty() {
        return SurfaceCompleteness::Missing;
    }
    let rejected = pages.iter().fold(0_u64, |total, page| {
        total.saturating_add(u64::from(page.rejected_records))
    });
    if rejected > 0 {
        return SurfaceCompleteness::RejectedRecords { count: rejected };
    }
    for (index, page) in pages.iter().enumerate() {
        let Ok(expected) = u32::try_from(index + 1) else {
            return SurfaceCompleteness::IncompletePageChain;
        };
        if page.page_ordinal.get() != expected {
            return SurfaceCompleteness::IncompletePageChain;
        }
        let last = index + 1 == pages.len();
        match (&page.terminal_state, last) {
            (PageTerminalState::More { .. }, false) | (PageTerminalState::Terminal, true) => {}
            (PageTerminalState::More { .. }, true)
            | (PageTerminalState::Terminal, false)
            | (PageTerminalState::Unknown, _) => {
                return SurfaceCompleteness::IncompletePageChain;
            }
        }
    }
    let receipt_records = pages.iter().fold(0_u64, |total, page| {
        total.saturating_add(u64::from(page.returned_records))
    });
    if receipt_records != catalog_records {
        return SurfaceCompleteness::ObservationCountMismatch {
            receipt_records,
            catalog_records,
        };
    }
    SurfaceCompleteness::Complete
}

fn source_identifier(value: String) -> Result<SourceIdentifier, PublicationError> {
    SourceIdentifier::try_from(value).map_err(|_| PublicationError::InvalidIdentifier)
}

/// Publication request, evidence, limit, or reconciliation failure.
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
    /// A decoder or catalog produced a malformed bounded identifier.
    #[error("invalid option-reference evidence identifier")]
    InvalidIdentifier,
    /// The catalog attempted to admit a surface not present in the exact request.
    #[error("option-reference surface was not requested")]
    UnrequestedSurface,
    /// Aggregate page, byte, record, or conflict bounds were exceeded.
    #[error("option-reference publication limit exceeded")]
    LimitsExceeded,
}
