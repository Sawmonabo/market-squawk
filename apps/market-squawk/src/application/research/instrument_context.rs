//! Provider-neutral, point-in-time instrument identity context.
//!
//! This leaf joins an already admitted canonical instrument definition to an immutable official
//! listing directory. The join is possible only through an exact canonical venue-symbol mapping
//! and the source-qualified official row for the same asset family. A ticker, display name, or
//! fuzzy match can never create the relationship.

use std::fmt;
use std::time::Instant;

use market_squawk_data::{
    ListingReferenceError, ListingReferenceGenerationSelection, ListingReferenceMembershipCursor,
    ListingReferenceMembershipPageState, ListingReferenceMembershipSelectionReceipt,
    ListingReferenceReadCapability, ListingReferenceRecord, ListingReferenceRightsState,
    MAX_LISTING_REFERENCE_MEMBERSHIP_PAGE_ROWS, MAX_LISTING_REFERENCE_RECORDS,
    MAX_MARKET_DATA_INSTRUMENT_SEARCH_ROWS, MarketDataInstrumentCatalogError,
    MarketDataInstrumentMatchKind, MarketDataInstrumentPopulationDisposition,
    MarketDataInstrumentPopulationQuery, MarketDataInstrumentPopulationSelection,
    MarketDataInstrumentReadCapability, MarketDataInstrumentSearchMatch,
    MarketDataInstrumentSearchPage,
};
use market_squawk_domain::{
    AssetClass, AssignmentVerification, Currency, EffectiveInterval, ExternalIdentifier,
    InstrumentId, MarketDataInstrumentDefinition, Timestamp, VenueId,
};
use thiserror::Error;
use tokio_util::sync::CancellationToken;

const MAX_RETAINED_DIRECTORY_RECEIPTS: usize =
    MAX_LISTING_REFERENCE_RECORDS.div_ceil(MAX_LISTING_REFERENCE_MEMBERSHIP_PAGE_ROWS);
const MAX_RETAINED_AMBIGUOUS_LISTINGS: usize = 2;

/// Product searches stay small even though the catalog admits a larger diagnostic bound.
const MAX_INSTRUMENT_SEARCH_ROWS: usize = 32;

/// Closed point-in-time request for one canonical investment identity.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct InstrumentContextRequest {
    instrument_id: InstrumentId,
    knowledge_at: Timestamp,
    effective_at: Timestamp,
}

impl InstrumentContextRequest {
    /// Constructs an identity-only request with independent knowledge and economic clocks.
    pub(crate) fn try_new(
        instrument_id: InstrumentId,
        knowledge_at: Timestamp,
        effective_at: Timestamp,
    ) -> Result<Self, InstrumentContextReadError> {
        if effective_at > knowledge_at {
            return Err(InstrumentContextReadError::InvalidRequest);
        }
        Ok(Self {
            instrument_id,
            knowledge_at,
            effective_at,
        })
    }

    pub(crate) const fn instrument_id(self) -> InstrumentId {
        self.instrument_id
    }

    pub(crate) const fn knowledge_at(self) -> Timestamp {
        self.knowledge_at
    }

    pub(crate) const fn effective_at(self) -> Timestamp {
        self.effective_at
    }
}

/// Read-only composition of canonical identity and official-directory authorities.
pub(crate) struct InstrumentContextReadCapability {
    instruments: MarketDataInstrumentReadCapability,
    listings: ListingReferenceReadCapability,
}

impl fmt::Debug for InstrumentContextReadCapability {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("InstrumentContextReadCapability")
            .field("instruments", &"[CANONICAL INSTRUMENT READ AUTHORITY]")
            .field("listings", &"[OFFICIAL DIRECTORY READ AUTHORITY]")
            .finish()
    }
}

impl InstrumentContextReadCapability {
    pub(crate) const fn new(
        instruments: MarketDataInstrumentReadCapability,
        listings: ListingReferenceReadCapability,
    ) -> Self {
        Self {
            instruments,
            listings,
        }
    }

    /// Resolves one canonical instrument without ticker/name inference or current-time fallback.
    pub(crate) fn read(
        &self,
        request: InstrumentContextRequest,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<InstrumentContextRead, InstrumentContextReadError> {
        check_operation(deadline, cancellation)?;
        let mut instrument_ids = Vec::new();
        instrument_ids
            .try_reserve_exact(1)
            .map_err(|_| InstrumentContextReadError::ResourceExhausted)?;
        instrument_ids.push(request.instrument_id);
        let query = MarketDataInstrumentPopulationQuery::try_new(
            instrument_ids,
            request.knowledge_at,
            request.effective_at,
        )
        .map_err(map_instrument_error)?;
        let definition_selection = self
            .instruments
            .pin_population_as_of(query, deadline, cancellation)
            .map_err(map_instrument_error)?;
        if definition_selection.disposition()
            == MarketDataInstrumentPopulationDisposition::Unavailable
        {
            return Ok(InstrumentContextRead::new(
                request,
                InstrumentContextOutcome::Missing(
                    InstrumentContextMissingReason::CanonicalDefinition,
                ),
                InstrumentContextEvidence::new(definition_selection)?,
            ));
        }
        let [definition_record] = definition_selection.records() else {
            return Err(InstrumentContextReadError::EvidenceConflict);
        };
        let definition = definition_record.definition().clone();
        if definition.instrument_id() != request.instrument_id
            || !interval_contains(definition.effective_interval(), request.effective_at)
            || definition_record.published_at() > request.knowledge_at
        {
            return Err(InstrumentContextReadError::EvidenceConflict);
        }

        self.read_official_listing(
            request,
            definition_selection,
            &definition,
            deadline,
            cancellation,
        )
    }

    /// Repeats the complete as-of join and rejects any product or private-evidence drift.
    pub(crate) fn verify_restart(
        &self,
        expected: &InstrumentContextRead,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<InstrumentContextRead, InstrumentContextReadError> {
        let replay = self.read(expected.request, deadline, cancellation)?;
        if replay != *expected {
            return Err(InstrumentContextReadError::RestartConflict);
        }
        Ok(replay)
    }

    /// Returns provider-neutral candidates at fixed knowledge and effective clocks.
    ///
    /// Candidate search never selects an instrument. The user or a separately authorized exact
    /// resolution step must carry the stable [`InstrumentId`] into subsequent research reads.
    pub(crate) fn search(
        &self,
        request: &InstrumentSearchRequest,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<InstrumentSearchRead, InstrumentContextReadError> {
        check_operation(deadline, cancellation)?;
        let evidence = self
            .instruments
            .search_as_of(
                request.query(),
                request.knowledge_at(),
                request.effective_at(),
                request.maximum_rows(),
                deadline,
                cancellation,
            )
            .map_err(map_instrument_error)?;
        build_search_read(request.clone(), evidence)
    }

    /// Resolves an exact admitted term while preserving every ambiguity as a terminal outcome.
    pub(crate) fn resolve_exact(
        &self,
        request: &InstrumentIdentityResolutionRequest,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<InstrumentIdentityResolutionRead, InstrumentContextReadError> {
        check_operation(deadline, cancellation)?;
        let evidence = self
            .instruments
            .resolve_exact_as_of(
                request.query(),
                request.knowledge_at(),
                request.effective_at(),
                deadline,
                cancellation,
            )
            .map_err(map_instrument_error)?;
        build_identity_resolution(request.clone(), evidence)
    }

    /// Replays one local point-in-time candidate search after process restart.
    pub(crate) fn verify_search_restart(
        &self,
        expected: &InstrumentSearchRead,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<InstrumentSearchRead, InstrumentContextReadError> {
        let replay = self.search(expected.request(), deadline, cancellation)?;
        if replay != *expected {
            return Err(InstrumentContextReadError::RestartConflict);
        }
        Ok(replay)
    }

    /// Replays one exact ambiguity-safe resolution after process restart.
    pub(crate) fn verify_resolution_restart(
        &self,
        expected: &InstrumentIdentityResolutionRead,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<InstrumentIdentityResolutionRead, InstrumentContextReadError> {
        let replay = self.resolve_exact(expected.request(), deadline, cancellation)?;
        if replay != *expected {
            return Err(InstrumentContextReadError::RestartConflict);
        }
        Ok(replay)
    }

    fn read_official_listing(
        &self,
        request: InstrumentContextRequest,
        definition_selection: MarketDataInstrumentPopulationSelection,
        definition: &MarketDataInstrumentDefinition,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<InstrumentContextRead, InstrumentContextReadError> {
        let mut evidence = InstrumentContextEvidence::new(definition_selection)?;
        let mut cursor: Option<ListingReferenceMembershipCursor> = None;
        let mut scanned_rows = 0_usize;
        let mut matched_rows = 0_usize;

        loop {
            check_operation(deadline, cancellation)?;
            let page = self
                .listings
                .memberships(
                    ListingReferenceGenerationSelection::AsOf(request.knowledge_at),
                    cursor.as_ref(),
                    MAX_LISTING_REFERENCE_MEMBERSHIP_PAGE_ROWS,
                    deadline,
                    cancellation,
                )
                .map_err(map_listing_error)?;
            evidence.push_receipt(page.receipt().clone())?;
            let Some(generation) = page.generation() else {
                if cursor.is_some()
                    || page.state() != ListingReferenceMembershipPageState::Complete
                    || !page.records().is_empty()
                {
                    return Err(InstrumentContextReadError::EvidenceConflict);
                }
                return Ok(InstrumentContextRead::new(
                    request,
                    InstrumentContextOutcome::Missing(
                        InstrumentContextMissingReason::OfficialDirectory,
                    ),
                    evidence,
                ));
            };
            if generation.published_at() > request.knowledge_at
                || page.receipt().selected_generation_digest()
                    != Some(generation.generation_digest())
                || page.receipt().requested_knowledge_at() != request.knowledge_at
            {
                return Err(InstrumentContextReadError::EvidenceConflict);
            }

            scanned_rows = scanned_rows
                .checked_add(page.records().len())
                .ok_or(InstrumentContextReadError::ResourceExhausted)?;
            if scanned_rows > MAX_LISTING_REFERENCE_RECORDS {
                return Err(InstrumentContextReadError::EvidenceConflict);
            }
            for record in page.records() {
                if record.generation().generation_digest() != generation.generation_digest()
                    || record.generation().published_at() > request.knowledge_at
                    || record.source_file().available_at() > request.knowledge_at
                    || record.effective_at() > request.knowledge_at
                {
                    return Err(InstrumentContextReadError::EvidenceConflict);
                }
                if official_identity_matches(record, definition, request.effective_at) {
                    matched_rows = matched_rows
                        .checked_add(1)
                        .ok_or(InstrumentContextReadError::ResourceExhausted)?;
                    evidence.retain_match(record.clone());
                }
            }

            match page.state() {
                ListingReferenceMembershipPageState::Complete => break,
                ListingReferenceMembershipPageState::Truncated => {
                    if scanned_rows == MAX_LISTING_REFERENCE_RECORDS {
                        return Ok(InstrumentContextRead::new(
                            request,
                            InstrumentContextOutcome::Unavailable(
                                InstrumentContextUnavailableReason::DirectoryReadBound,
                            ),
                            evidence,
                        ));
                    }
                    cursor = Some(
                        page.next_cursor()
                            .ok_or(InstrumentContextReadError::EvidenceConflict)?
                            .clone(),
                    );
                }
            }
        }

        let outcome = match matched_rows {
            0 => InstrumentContextOutcome::Missing(
                InstrumentContextMissingReason::OfficialMembership,
            ),
            1 => {
                let record = evidence
                    .retained_matches
                    .first()
                    .ok_or(InstrumentContextReadError::EvidenceConflict)?;
                InstrumentContextOutcome::Exact(InstrumentContext::try_new(
                    request, definition, record,
                )?)
            }
            _ => InstrumentContextOutcome::Ambiguous,
        };
        Ok(InstrumentContextRead::new(request, outcome, evidence))
    }
}

/// Bounded provider-neutral text search request at independent PIT coordinates.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct InstrumentSearchRequest {
    query: Box<str>,
    knowledge_at: Timestamp,
    effective_at: Timestamp,
    maximum_rows: usize,
}

impl InstrumentSearchRequest {
    pub(crate) fn try_new(
        query: &str,
        knowledge_at: Timestamp,
        effective_at: Timestamp,
        maximum_rows: usize,
    ) -> Result<Self, InstrumentContextReadError> {
        validate_product_search(query, knowledge_at, effective_at, maximum_rows)?;
        Ok(Self {
            query: try_boxed_text(query.trim())?,
            knowledge_at,
            effective_at,
            maximum_rows,
        })
    }

    pub(crate) fn query(&self) -> &str {
        &self.query
    }

    pub(crate) const fn knowledge_at(&self) -> Timestamp {
        self.knowledge_at
    }

    pub(crate) const fn effective_at(&self) -> Timestamp {
        self.effective_at
    }

    pub(crate) const fn maximum_rows(&self) -> usize {
        self.maximum_rows
    }
}

/// Exact term-resolution request. It contains no provider or dataset route.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct InstrumentIdentityResolutionRequest {
    query: Box<str>,
    knowledge_at: Timestamp,
    effective_at: Timestamp,
}

impl InstrumentIdentityResolutionRequest {
    pub(crate) fn try_new(
        query: &str,
        knowledge_at: Timestamp,
        effective_at: Timestamp,
    ) -> Result<Self, InstrumentContextReadError> {
        validate_product_search(query, knowledge_at, effective_at, 1)?;
        Ok(Self {
            query: try_boxed_text(query.trim())?,
            knowledge_at,
            effective_at,
        })
    }

    pub(crate) fn query(&self) -> &str {
        &self.query
    }

    pub(crate) const fn knowledge_at(&self) -> Timestamp {
        self.knowledge_at
    }

    pub(crate) const fn effective_at(&self) -> Timestamp {
        self.effective_at
    }
}

/// Provider-neutral candidate-search result with private catalog restart evidence.
#[derive(Eq, PartialEq)]
pub(crate) struct InstrumentSearchRead {
    request: InstrumentSearchRequest,
    candidates: Box<[InstrumentSearchCandidate]>,
    has_more: bool,
    evidence: MarketDataInstrumentSearchPage,
}

impl InstrumentSearchRead {
    pub(crate) const fn request(&self) -> &InstrumentSearchRequest {
        &self.request
    }

    pub(crate) fn candidates(&self) -> &[InstrumentSearchCandidate] {
        &self.candidates
    }

    pub(crate) const fn has_more(&self) -> bool {
        self.has_more
    }
}

impl fmt::Debug for InstrumentSearchRead {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("InstrumentSearchRead")
            .field("request", &self.request)
            .field("candidates", &self.candidates)
            .field("has_more", &self.has_more)
            .field("evidence", &"[PRIVATE CANONICAL SEARCH EVIDENCE]")
            .finish()
    }
}

/// Terminal exact identity result. Ambiguity never carries an implicit winner.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum InstrumentIdentityResolutionOutcome {
    Missing,
    Exact(InstrumentSearchCandidate),
    /// A symbol, name, or provider alias found candidates but cannot establish canonical identity.
    CandidateOnly {
        candidates: Box<[InstrumentSearchCandidate]>,
        has_more: bool,
    },
    Ambiguous {
        candidates: Box<[InstrumentSearchCandidate]>,
        has_more: bool,
    },
}

/// Provider-neutral exact-resolution result with private restart evidence.
#[derive(Eq, PartialEq)]
pub(crate) struct InstrumentIdentityResolutionRead {
    request: InstrumentIdentityResolutionRequest,
    outcome: InstrumentIdentityResolutionOutcome,
    evidence: MarketDataInstrumentSearchPage,
}

impl InstrumentIdentityResolutionRead {
    pub(crate) const fn request(&self) -> &InstrumentIdentityResolutionRequest {
        &self.request
    }

    pub(crate) const fn outcome(&self) -> &InstrumentIdentityResolutionOutcome {
        &self.outcome
    }
}

impl fmt::Debug for InstrumentIdentityResolutionRead {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("InstrumentIdentityResolutionRead")
            .field("request", &self.request)
            .field("outcome", &self.outcome)
            .field("evidence", &"[PRIVATE CANONICAL RESOLUTION EVIDENCE]")
            .finish()
    }
}

/// Ordinary product reason explaining why a candidate was found.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum InstrumentSearchMatchReason {
    ExternalIdentifier,
    DisplayName,
    ListedSymbol,
    Alias,
}

/// Exchange-qualified symbol exposed without provider routing.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct InstrumentSearchListing {
    venue: VenueId,
    symbol: Box<str>,
}

impl InstrumentSearchListing {
    pub(crate) const fn venue(&self) -> &VenueId {
        &self.venue
    }

    pub(crate) fn symbol(&self) -> &str {
        &self.symbol
    }
}

/// Honest lifecycle limit of the selected official snapshot sources.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum InstrumentOfficialLifecycleEvidence {
    /// Nasdaq Trader, OCC, and Cboe snapshots do not establish delisting or successor events.
    SuccessorAndDelistingNotEstablished,
}

/// Provider-neutral stable-ID candidate for ordinary product selection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct InstrumentSearchCandidate {
    instrument_id: InstrumentId,
    display_name: Option<Box<str>>,
    asset_class: AssetClass,
    quote_currency: Currency,
    listings: Box<[InstrumentSearchListing]>,
    validity: EffectiveInterval,
    matched_by: InstrumentSearchMatchReason,
    official_lifecycle: InstrumentOfficialLifecycleEvidence,
}

impl InstrumentSearchCandidate {
    pub(crate) const fn instrument_id(&self) -> InstrumentId {
        self.instrument_id
    }

    pub(crate) fn display_name(&self) -> Option<&str> {
        self.display_name.as_deref()
    }

    pub(crate) const fn asset_class(&self) -> AssetClass {
        self.asset_class
    }

    pub(crate) const fn quote_currency(&self) -> Currency {
        self.quote_currency
    }

    pub(crate) fn listings(&self) -> &[InstrumentSearchListing] {
        &self.listings
    }

    pub(crate) const fn validity(&self) -> EffectiveInterval {
        self.validity
    }

    pub(crate) const fn matched_by(&self) -> InstrumentSearchMatchReason {
        self.matched_by
    }

    pub(crate) const fn official_lifecycle(&self) -> InstrumentOfficialLifecycleEvidence {
        self.official_lifecycle
    }
}

/// Provider-neutral result plus private restart evidence.
#[derive(Eq, PartialEq)]
pub(crate) struct InstrumentContextRead {
    request: InstrumentContextRequest,
    outcome: InstrumentContextOutcome,
    evidence: InstrumentContextEvidence,
}

impl InstrumentContextRead {
    const fn new(
        request: InstrumentContextRequest,
        outcome: InstrumentContextOutcome,
        evidence: InstrumentContextEvidence,
    ) -> Self {
        Self {
            request,
            outcome,
            evidence,
        }
    }

    pub(crate) const fn request(&self) -> InstrumentContextRequest {
        self.request
    }

    pub(crate) const fn outcome(&self) -> &InstrumentContextOutcome {
        &self.outcome
    }
}

impl fmt::Debug for InstrumentContextRead {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("InstrumentContextRead")
            .field("request", &self.request)
            .field("outcome", &self.outcome)
            .field("evidence", &"[PRIVATE RESTART RECEIPT]")
            .finish()
    }
}

/// Closed application-level identity outcome.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum InstrumentContextOutcome {
    Exact(InstrumentContext),
    Missing(InstrumentContextMissingReason),
    Ambiguous,
    Unavailable(InstrumentContextUnavailableReason),
}

/// Product identity and listing meaning without provider or storage coordinates.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct InstrumentContext {
    instrument_id: InstrumentId,
    display_name: Box<str>,
    asset_class: AssetClass,
    quote_currency: Currency,
    listing_venue: VenueId,
    listed_symbol: Box<str>,
    validity: EffectiveInterval,
    known_at: Timestamp,
    official_directory_updated_at: Timestamp,
    exchange_traded_fund: bool,
    round_lot_size: u32,
    official_lifecycle: InstrumentOfficialLifecycleEvidence,
}

impl InstrumentContext {
    fn try_new(
        request: InstrumentContextRequest,
        definition: &MarketDataInstrumentDefinition,
        listing: &ListingReferenceRecord,
    ) -> Result<Self, InstrumentContextReadError> {
        if listing.generation().published_at() > request.knowledge_at
            || listing.source_file().available_at() > request.knowledge_at
            || listing.effective_at() > request.knowledge_at
            || !interval_contains(definition.effective_interval(), request.effective_at)
        {
            return Err(InstrumentContextReadError::EvidenceConflict);
        }
        let display_name = definition.display_name().map_or_else(
            || try_boxed_text(listing.display_name()),
            |name| try_boxed_text(name.as_str()),
        )?;
        let listed_symbol = official_venue_symbol(listing, definition)
            .ok_or(InstrumentContextReadError::EvidenceConflict)?;
        Ok(Self {
            instrument_id: request.instrument_id,
            display_name,
            asset_class: definition.asset_class(),
            quote_currency: definition.quote_currency(),
            listing_venue: listing.listing_venue().clone(),
            listed_symbol: try_boxed_text(listed_symbol)?,
            validity: definition.effective_interval(),
            known_at: request.knowledge_at,
            official_directory_updated_at: listing.effective_at(),
            exchange_traded_fund: listing.is_etf(),
            round_lot_size: listing.round_lot_size(),
            official_lifecycle:
                InstrumentOfficialLifecycleEvidence::SuccessorAndDelistingNotEstablished,
        })
    }

    pub(crate) const fn instrument_id(&self) -> InstrumentId {
        self.instrument_id
    }
    pub(crate) fn display_name(&self) -> &str {
        &self.display_name
    }
    pub(crate) const fn asset_class(&self) -> AssetClass {
        self.asset_class
    }
    pub(crate) const fn quote_currency(&self) -> Currency {
        self.quote_currency
    }
    pub(crate) const fn listing_venue(&self) -> &VenueId {
        &self.listing_venue
    }
    pub(crate) fn listed_symbol(&self) -> &str {
        &self.listed_symbol
    }
    pub(crate) const fn validity(&self) -> EffectiveInterval {
        self.validity
    }
    pub(crate) const fn known_at(&self) -> Timestamp {
        self.known_at
    }
    pub(crate) const fn official_directory_updated_at(&self) -> Timestamp {
        self.official_directory_updated_at
    }
    pub(crate) const fn exchange_traded_fund(&self) -> bool {
        self.exchange_traded_fund
    }
    pub(crate) const fn round_lot_size(&self) -> u32 {
        self.round_lot_size
    }
    pub(crate) const fn official_lifecycle(&self) -> InstrumentOfficialLifecycleEvidence {
        self.official_lifecycle
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum InstrumentContextMissingReason {
    CanonicalDefinition,
    OfficialDirectory,
    OfficialMembership,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum InstrumentContextUnavailableReason {
    DirectoryReadBound,
}

/// Raw catalog and official-directory evidence is deliberately inaccessible outside this leaf.
#[derive(Eq, PartialEq)]
struct InstrumentContextEvidence {
    definition: MarketDataInstrumentPopulationSelection,
    directory_receipts: Vec<ListingReferenceMembershipSelectionReceipt>,
    retained_matches: Vec<ListingReferenceRecord>,
}

impl InstrumentContextEvidence {
    fn new(
        definition: MarketDataInstrumentPopulationSelection,
    ) -> Result<Self, InstrumentContextReadError> {
        let mut receipts = Vec::new();
        receipts
            .try_reserve_exact(MAX_RETAINED_DIRECTORY_RECEIPTS)
            .map_err(|_| InstrumentContextReadError::ResourceExhausted)?;
        let mut matches = Vec::new();
        matches
            .try_reserve_exact(MAX_RETAINED_AMBIGUOUS_LISTINGS)
            .map_err(|_| InstrumentContextReadError::ResourceExhausted)?;
        Ok(Self {
            definition,
            directory_receipts: receipts,
            retained_matches: matches,
        })
    }

    fn push_receipt(
        &mut self,
        receipt: ListingReferenceMembershipSelectionReceipt,
    ) -> Result<(), InstrumentContextReadError> {
        if self.directory_receipts.len() == MAX_RETAINED_DIRECTORY_RECEIPTS {
            return Err(InstrumentContextReadError::EvidenceConflict);
        }
        self.directory_receipts.push(receipt);
        Ok(())
    }

    fn retain_match(&mut self, record: ListingReferenceRecord) {
        if self.retained_matches.len() == MAX_RETAINED_AMBIGUOUS_LISTINGS {
            return;
        }
        self.retained_matches.push(record);
    }
}

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub(crate) enum InstrumentContextReadError {
    #[error("instrument context request is invalid")]
    InvalidRequest,
    #[error("instrument context read was cancelled")]
    Cancelled,
    #[error("instrument context read deadline elapsed")]
    DeadlineExceeded,
    #[error("instrument context authority is unavailable")]
    AuthorityUnavailable,
    #[error("instrument context evidence is inconsistent")]
    EvidenceConflict,
    #[error("instrument context read exceeded its fixed resource bound")]
    ResourceExhausted,
    #[error("instrument context restart did not reproduce the same result")]
    RestartConflict,
}

fn official_identity_matches(
    listing: &ListingReferenceRecord,
    definition: &MarketDataInstrumentDefinition,
    effective_at: Timestamp,
) -> bool {
    listing.effective_at() <= effective_at
        && listing.generation().rights_state() == ListingReferenceRightsState::AdmittedScoped
        && !listing.is_test_issue()
        && matches!(
            definition.asset_class(),
            AssetClass::Equity | AssetClass::Fund
        )
        && listing.is_etf() == (definition.asset_class() == AssetClass::Fund)
        && official_venue_symbol(listing, definition).is_some()
}

fn official_venue_symbol<'definition>(
    listing: &ListingReferenceRecord,
    definition: &'definition MarketDataInstrumentDefinition,
) -> Option<&'definition str> {
    definition
        .venue_mappings()
        .iter()
        .find(|mapping| {
            mapping.venue_id() == listing.listing_venue()
                && listing_asserts_symbol(listing, mapping.venue_symbol().as_str())
        })
        .map(|mapping| mapping.venue_symbol().as_str())
}

fn listing_asserts_symbol(listing: &ListingReferenceRecord, symbol: &str) -> bool {
    listing.provider_symbol() == symbol
        || listing.cqs_symbol() == Some(symbol)
        || listing.nasdaq_symbol() == Some(symbol)
}

fn interval_contains(interval: EffectiveInterval, at: Timestamp) -> bool {
    interval.starts_at() <= at
        && match interval.ends_at() {
            Some(end) => at < end,
            None => true,
        }
}

fn validate_product_search(
    query: &str,
    knowledge_at: Timestamp,
    effective_at: Timestamp,
    maximum_rows: usize,
) -> Result<(), InstrumentContextReadError> {
    const MAX_QUERY_BYTES: usize = 512;
    let query = query.trim();
    if effective_at > knowledge_at
        || query.is_empty()
        || query.len() > MAX_QUERY_BYTES
        || query.chars().any(char::is_control)
        || maximum_rows == 0
        || maximum_rows > MAX_INSTRUMENT_SEARCH_ROWS
        || maximum_rows > MAX_MARKET_DATA_INSTRUMENT_SEARCH_ROWS
    {
        return Err(InstrumentContextReadError::InvalidRequest);
    }
    Ok(())
}

fn build_search_read(
    request: InstrumentSearchRequest,
    evidence: MarketDataInstrumentSearchPage,
) -> Result<InstrumentSearchRead, InstrumentContextReadError> {
    validate_search_evidence(&evidence, request.knowledge_at(), request.effective_at())?;
    let candidates = build_search_candidates(&evidence)?;
    Ok(InstrumentSearchRead {
        request,
        candidates,
        has_more: evidence.has_more(),
        evidence,
    })
}

fn build_identity_resolution(
    request: InstrumentIdentityResolutionRequest,
    evidence: MarketDataInstrumentSearchPage,
) -> Result<InstrumentIdentityResolutionRead, InstrumentContextReadError> {
    validate_search_evidence(&evidence, request.knowledge_at(), request.effective_at())?;
    let candidates = build_search_candidates(&evidence)?;
    let outcome = if candidates.is_empty() && !evidence.has_more() {
        InstrumentIdentityResolutionOutcome::Missing
    } else if let [candidate] = candidates.as_ref()
        && !evidence.has_more()
        && evidence
            .matches()
            .first()
            .is_some_and(match_establishes_canonical_identity)
    {
        InstrumentIdentityResolutionOutcome::Exact(candidate.clone())
    } else if candidates.len() == 1 && !evidence.has_more() {
        InstrumentIdentityResolutionOutcome::CandidateOnly {
            candidates,
            has_more: false,
        }
    } else {
        InstrumentIdentityResolutionOutcome::Ambiguous {
            candidates,
            has_more: evidence.has_more(),
        }
    };
    Ok(InstrumentIdentityResolutionRead {
        request,
        outcome,
        evidence,
    })
}

fn match_establishes_canonical_identity(matched: &MarketDataInstrumentSearchMatch) -> bool {
    matched.match_kind() == MarketDataInstrumentMatchKind::ExternalIdentifier
        && matched
            .record()
            .definition()
            .identifiers()
            .iter()
            .any(|identifier| {
                identifier.assignment_verification() == AssignmentVerification::VerifiedAssigned
                    && !matches!(identifier.identifier(), ExternalIdentifier::Ticker(_))
                    && identifier.identifier().to_string() == matched.matched_value()
            })
}

fn validate_search_evidence(
    evidence: &MarketDataInstrumentSearchPage,
    knowledge_at: Timestamp,
    effective_at: Timestamp,
) -> Result<(), InstrumentContextReadError> {
    if evidence.knowledge_at() != Some(knowledge_at)
        || evidence.effective_at() != Some(effective_at)
        || evidence.matches().iter().any(|candidate| {
            candidate.record().published_at() > knowledge_at
                || !interval_contains(
                    candidate.record().definition().effective_interval(),
                    effective_at,
                )
        })
    {
        return Err(InstrumentContextReadError::EvidenceConflict);
    }
    Ok(())
}

fn build_search_candidates(
    evidence: &MarketDataInstrumentSearchPage,
) -> Result<Box<[InstrumentSearchCandidate]>, InstrumentContextReadError> {
    let mut candidates = Vec::new();
    candidates
        .try_reserve_exact(evidence.matches().len())
        .map_err(|_| InstrumentContextReadError::ResourceExhausted)?;
    for matched in evidence.matches() {
        let candidate = build_search_candidate(matched)?;
        if candidates
            .iter()
            .any(|existing: &InstrumentSearchCandidate| {
                existing.instrument_id() == candidate.instrument_id()
            })
        {
            return Err(InstrumentContextReadError::EvidenceConflict);
        }
        candidates.push(candidate);
    }
    Ok(candidates.into_boxed_slice())
}

fn build_search_candidate(
    matched: &MarketDataInstrumentSearchMatch,
) -> Result<InstrumentSearchCandidate, InstrumentContextReadError> {
    let definition = matched.record().definition();
    let display_name = definition
        .display_name()
        .map(|name| try_boxed_text(name.as_str()))
        .transpose()?;
    let mut listings = Vec::new();
    listings
        .try_reserve_exact(definition.venue_mappings().len())
        .map_err(|_| InstrumentContextReadError::ResourceExhausted)?;
    for mapping in definition.venue_mappings() {
        listings.push(InstrumentSearchListing {
            venue: mapping.venue_id().clone(),
            symbol: try_boxed_text(mapping.venue_symbol().as_str())?,
        });
    }
    let matched_by = match matched.match_kind() {
        MarketDataInstrumentMatchKind::ExternalIdentifier => {
            InstrumentSearchMatchReason::ExternalIdentifier
        }
        MarketDataInstrumentMatchKind::DisplayName => InstrumentSearchMatchReason::DisplayName,
        MarketDataInstrumentMatchKind::VenueSymbol => InstrumentSearchMatchReason::ListedSymbol,
        MarketDataInstrumentMatchKind::ProviderSymbol => InstrumentSearchMatchReason::Alias,
    };
    Ok(InstrumentSearchCandidate {
        instrument_id: definition.instrument_id(),
        display_name,
        asset_class: definition.asset_class(),
        quote_currency: definition.quote_currency(),
        listings: listings.into_boxed_slice(),
        validity: definition.effective_interval(),
        matched_by,
        official_lifecycle:
            InstrumentOfficialLifecycleEvidence::SuccessorAndDelistingNotEstablished,
    })
}

fn try_boxed_text(value: &str) -> Result<Box<str>, InstrumentContextReadError> {
    let mut owned = String::new();
    owned
        .try_reserve_exact(value.len())
        .map_err(|_| InstrumentContextReadError::ResourceExhausted)?;
    owned.push_str(value);
    Ok(owned.into_boxed_str())
}

fn check_operation(
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<(), InstrumentContextReadError> {
    if cancellation.is_cancelled() {
        Err(InstrumentContextReadError::Cancelled)
    } else if Instant::now() >= deadline {
        Err(InstrumentContextReadError::DeadlineExceeded)
    } else {
        Ok(())
    }
}

fn map_instrument_error(error: MarketDataInstrumentCatalogError) -> InstrumentContextReadError {
    match error {
        MarketDataInstrumentCatalogError::Cancelled => InstrumentContextReadError::Cancelled,
        MarketDataInstrumentCatalogError::DeadlineExceeded => {
            InstrumentContextReadError::DeadlineExceeded
        }
        MarketDataInstrumentCatalogError::AuthorityUnavailable => {
            InstrumentContextReadError::AuthorityUnavailable
        }
        MarketDataInstrumentCatalogError::ResultByteLimitExceeded => {
            InstrumentContextReadError::ResourceExhausted
        }
        MarketDataInstrumentCatalogError::InvalidInput
        | MarketDataInstrumentCatalogError::InvalidPopulationQuery
        | MarketDataInstrumentCatalogError::InvalidLimit => {
            InstrumentContextReadError::InvalidRequest
        }
        _ => InstrumentContextReadError::EvidenceConflict,
    }
}

fn map_listing_error(error: ListingReferenceError) -> InstrumentContextReadError {
    match error {
        ListingReferenceError::Cancelled => InstrumentContextReadError::Cancelled,
        ListingReferenceError::DeadlineExceeded => InstrumentContextReadError::DeadlineExceeded,
        ListingReferenceError::AuthorityUnavailable => {
            InstrumentContextReadError::AuthorityUnavailable
        }
        ListingReferenceError::MemoryLimitExceeded => InstrumentContextReadError::ResourceExhausted,
        ListingReferenceError::InvalidKnowledgeCutoff
        | ListingReferenceError::InvalidInput
        | ListingReferenceError::InvalidLimit => InstrumentContextReadError::InvalidRequest,
        _ => InstrumentContextReadError::EvidenceConflict,
    }
}
