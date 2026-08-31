//! Immutable repository-owned definitions for non-execution market-data discovery.

use std::collections::BTreeSet;
use std::fmt;
use std::mem::size_of;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use market_squawk_domain::{
    AssignmentVerification, DigestAlgorithm, EffectiveInterval, EvidenceDigest, InstrumentId,
    MarketDataInstrumentDefinition, MetadataRevision, ProviderInstrumentId, SourceId, Timestamp,
    VenueId,
};
use rusqlite::{OptionalExtension as _, Row, Transaction, params};
use sha2::{Digest as _, Sha256};
use thiserror::Error;
use tokio_util::sync::CancellationToken;

use super::CatalogAuthority;
use super::storage::{ResultBudget, append_audit, sha256, trusted_catalog_now};

/// Maximum definitions accepted in one atomic synchronization.
pub const MAX_MARKET_DATA_INSTRUMENT_SYNC_ROWS: usize = 65_536;
/// Maximum current definitions returned by one search.
pub const MAX_MARKET_DATA_INSTRUMENT_SEARCH_ROWS: usize = 256;
/// Maximum exact stable identities admitted by one point-in-time population pin.
pub const MAX_MARKET_DATA_INSTRUMENT_POPULATION_ROWS: usize = 256;
const MAX_SEARCH_QUERY_BYTES: usize = 512;
const MAX_REVISIONS_PER_INSTRUMENT: u32 = 16_384;
const SQLITE_PROGRESS_OPERATIONS: i32 = 1_000;
const POPULATION_QUERY_DOMAIN: &[u8] =
    b"market-squawk/market-data-instrument-population-query/v1\0";
const POPULATION_RECEIPT_DOMAIN: &[u8] =
    b"market-squawk/market-data-instrument-population-receipt/v1\0";
const PROVIDER_IDENTITY_QUERY_DOMAIN: &[u8] =
    b"market-squawk/provider-instrument-identity-query/v1\0";
const PROVIDER_IDENTITY_RECEIPT_DOMAIN: &[u8] =
    b"market-squawk/provider-instrument-identity-receipt/v1\0";

/// Complete caller-declared synchronization batch.
///
/// `expected_definition_count` binds the catalog call to the producer's complete result
/// cardinality. Any mismatch is rejected before catalog access; any later row failure rolls back
/// the entire transaction.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MarketDataInstrumentSynchronization {
    definitions: Box<[MarketDataInstrumentDefinition]>,
}

impl MarketDataInstrumentSynchronization {
    /// Constructs one nonempty, bounded, complete synchronization batch.
    pub fn try_new(
        definitions: Vec<MarketDataInstrumentDefinition>,
        expected_definition_count: usize,
    ) -> Result<Self, MarketDataInstrumentCatalogError> {
        if definitions.is_empty() {
            return Err(MarketDataInstrumentCatalogError::InvalidInput);
        }
        if definitions.len() != expected_definition_count {
            return Err(MarketDataInstrumentCatalogError::PartialBatch {
                expected: expected_definition_count,
                actual: definitions.len(),
            });
        }
        if definitions.len() > MAX_MARKET_DATA_INSTRUMENT_SYNC_ROWS {
            return Err(MarketDataInstrumentCatalogError::BatchLimitExceeded {
                max: MAX_MARKET_DATA_INSTRUMENT_SYNC_ROWS,
            });
        }
        Ok(Self {
            definitions: definitions.into_boxed_slice(),
        })
    }

    /// Returns the complete submitted definition set.
    pub fn definitions(&self) -> &[MarketDataInstrumentDefinition] {
        &self.definitions
    }
}

/// Durable result of one atomic synchronization.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MarketDataInstrumentSynchronizationReceipt {
    batch_digest: EvidenceDigest,
    submitted: usize,
    inserted: usize,
    replayed: usize,
}

impl MarketDataInstrumentSynchronizationReceipt {
    /// Returns the deterministic digest of the ordered submitted revision digests.
    pub const fn batch_digest(&self) -> EvidenceDigest {
        self.batch_digest
    }

    /// Returns the complete submitted cardinality.
    pub const fn submitted(&self) -> usize {
        self.submitted
    }

    /// Returns definitions appended as immutable current successors.
    pub const fn inserted(&self) -> usize {
        self.inserted
    }

    /// Returns exact-current replays that performed no catalog mutation.
    pub const fn replayed(&self) -> usize {
        self.replayed
    }
}

/// One digest-verified non-execution definition revision.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MarketDataInstrumentRecord {
    definition: MarketDataInstrumentDefinition,
    revision_digest: EvidenceDigest,
    revision_sequence: u32,
    published_at: Timestamp,
}

impl MarketDataInstrumentRecord {
    /// Returns the complete definition; execution terms and eligibility are absent by type.
    pub const fn definition(&self) -> &MarketDataInstrumentDefinition {
        &self.definition
    }

    /// Returns the SHA-256 digest of the exact canonical serialized definition.
    pub const fn revision_digest(&self) -> EvidenceDigest {
        self.revision_digest
    }

    /// Returns the monotonic revision position for this repository-owned instrument identity.
    pub const fn revision_sequence(&self) -> u32 {
        self.revision_sequence
    }

    /// Returns when this immutable revision first became durable locally.
    pub const fn published_at(&self) -> Timestamp {
        self.published_at
    }
}

/// Canonical nonempty point-in-time request over exact stable instrument identities.
///
/// The constructor orders the caller's set and rejects repetition instead of silently changing
/// its declared membership. Tickers, names, provider symbols, and execution eligibility are absent.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MarketDataInstrumentPopulationQuery {
    instrument_ids: Box<[InstrumentId]>,
    knowledge_at: Timestamp,
    effective_at: Timestamp,
    query_digest: EvidenceDigest,
}

impl MarketDataInstrumentPopulationQuery {
    /// Constructs a bounded exact-identity population query with independent PIT coordinates.
    pub fn try_new(
        mut instrument_ids: Vec<InstrumentId>,
        knowledge_at: Timestamp,
        effective_at: Timestamp,
    ) -> Result<Self, MarketDataInstrumentCatalogError> {
        if instrument_ids.is_empty()
            || instrument_ids.len() > MAX_MARKET_DATA_INSTRUMENT_POPULATION_ROWS
        {
            return Err(MarketDataInstrumentCatalogError::InvalidPopulationQuery);
        }
        instrument_ids.sort_unstable();
        if instrument_ids.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(MarketDataInstrumentCatalogError::InvalidPopulationQuery);
        }
        let instrument_ids = instrument_ids.into_boxed_slice();
        let query_digest = population_query_digest(&instrument_ids, knowledge_at, effective_at);
        Ok(Self {
            instrument_ids,
            knowledge_at,
            effective_at,
            query_digest,
        })
    }

    /// Returns the exact stable identities in canonical UUID order.
    pub fn instrument_ids(&self) -> &[InstrumentId] {
        &self.instrument_ids
    }

    /// Returns the inclusive local-knowledge cutoff for immutable catalog publication.
    pub const fn knowledge_at(&self) -> Timestamp {
        self.knowledge_at
    }

    /// Returns the exact instant at which the source-authored definition must be effective.
    pub const fn effective_at(&self) -> Timestamp {
        self.effective_at
    }

    /// Returns the canonical SHA-256 identity of the complete query.
    pub const fn query_digest(&self) -> EvidenceDigest {
        self.query_digest
    }
}

/// Closed reason one requested stable identity has no selectable as-of definition.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MarketDataInstrumentPopulationExclusionReason {
    /// No immutable revision was durably knowable at the requested knowledge cutoff.
    NoKnownRevision,
    /// Known revisions did not establish a definition effective at the requested instant.
    NoEffectiveRevision,
}

/// One exact requested identity excluded without substituting a symbol or later revision.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MarketDataInstrumentPopulationExclusion {
    instrument_id: InstrumentId,
    reason: MarketDataInstrumentPopulationExclusionReason,
}

impl MarketDataInstrumentPopulationExclusion {
    /// Returns the exact requested stable identity.
    pub const fn instrument_id(self) -> InstrumentId {
        self.instrument_id
    }

    /// Returns the fail-closed exclusion reason.
    pub const fn reason(self) -> MarketDataInstrumentPopulationExclusionReason {
        self.reason
    }
}

/// Closed completeness state for an exact requested population.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MarketDataInstrumentPopulationDisposition {
    /// Every requested member had exactly one knowable and effective immutable revision.
    Complete,
    /// At least one exact requested member was excluded; partial records are not a complete set.
    Unavailable,
}

/// Ordered, digest-bound result of one atomic point-in-time market-definition pin.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MarketDataInstrumentPopulationSelection {
    query: MarketDataInstrumentPopulationQuery,
    disposition: MarketDataInstrumentPopulationDisposition,
    records: Box<[MarketDataInstrumentRecord]>,
    exclusions: Box<[MarketDataInstrumentPopulationExclusion]>,
    receipt_digest: EvidenceDigest,
}

impl MarketDataInstrumentPopulationSelection {
    /// Returns the complete canonical request and its query digest.
    pub const fn query(&self) -> &MarketDataInstrumentPopulationQuery {
        &self.query
    }

    /// Returns whether the exact requested set was completely selectable.
    pub const fn disposition(&self) -> MarketDataInstrumentPopulationDisposition {
        self.disposition
    }

    /// Returns selected definitions in canonical stable-instrument order.
    pub fn records(&self) -> &[MarketDataInstrumentRecord] {
        &self.records
    }

    /// Returns excluded requested identities in canonical stable-instrument order.
    pub fn exclusions(&self) -> &[MarketDataInstrumentPopulationExclusion] {
        &self.exclusions
    }

    /// Returns the SHA-256 receipt binding query, disposition, records, and exclusions.
    pub const fn receipt_digest(&self) -> EvidenceDigest {
        self.receipt_digest
    }
}

/// Exact source-qualified point-in-time request for one provider-native instrument identity.
///
/// This backend request is deliberately separate from ordinary text search. It cannot infer a
/// provider, venue, ticker, or current-time fallback from a bare symbol.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MarketDataProviderIdentityQuery {
    source_id: SourceId,
    provider_instrument_id: ProviderInstrumentId,
    knowledge_at: Timestamp,
    effective_at: Timestamp,
    query_digest: EvidenceDigest,
}

impl MarketDataProviderIdentityQuery {
    /// Constructs one exact source-qualified request at independent knowledge/effective clocks.
    pub fn try_new(
        source_id: SourceId,
        provider_instrument_id: ProviderInstrumentId,
        knowledge_at: Timestamp,
        effective_at: Timestamp,
    ) -> Result<Self, MarketDataInstrumentCatalogError> {
        if effective_at > knowledge_at {
            return Err(MarketDataInstrumentCatalogError::InvalidInput);
        }
        let query_digest = provider_identity_query_digest(
            &source_id,
            &provider_instrument_id,
            knowledge_at,
            effective_at,
        );
        Ok(Self {
            source_id,
            provider_instrument_id,
            knowledge_at,
            effective_at,
            query_digest,
        })
    }

    /// Returns the exact provider namespace; it is never inferred from the provider symbol.
    pub const fn source_id(&self) -> &SourceId {
        &self.source_id
    }

    /// Returns the exact provider-native identity; ticker guessing is not admitted.
    pub const fn provider_instrument_id(&self) -> &ProviderInstrumentId {
        &self.provider_instrument_id
    }

    /// Returns the inclusive durable-knowledge cutoff.
    pub const fn knowledge_at(&self) -> Timestamp {
        self.knowledge_at
    }

    /// Returns the instant at which the definition and provider assertion must be effective.
    pub const fn effective_at(&self) -> Timestamp {
        self.effective_at
    }

    /// Returns the canonical identity of the complete source-qualified request.
    pub const fn query_digest(&self) -> EvidenceDigest {
        self.query_digest
    }
}

/// Non-forgeable exact definition/provider-identity/currentness receipt.
///
/// Construction is private to the digest-verifying catalog read. The receipt binds the immutable
/// definition revision, source assertion revision and digest, independent clocks, and any venue
/// symbols whose exact value agrees with the provider identity.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MarketDataProviderIdentityExactReceipt {
    instrument_id: InstrumentId,
    definition_revision_digest: EvidenceDigest,
    definition_revision_sequence: u32,
    definition_reference_revision: MetadataRevision,
    definition_reference_payload_digest: EvidenceDigest,
    definition_published_at: Timestamp,
    provider_identity_revision: MetadataRevision,
    provider_identity_payload_digest: EvidenceDigest,
    provider_identity_validity: EffectiveInterval,
    matching_venues: Box<[VenueId]>,
}

impl MarketDataProviderIdentityExactReceipt {
    /// Returns the exact canonical instrument established by the source-qualified assertion.
    pub const fn instrument_id(&self) -> InstrumentId {
        self.instrument_id
    }

    /// Returns the immutable serialized-definition digest.
    pub const fn definition_revision_digest(&self) -> EvidenceDigest {
        self.definition_revision_digest
    }

    /// Returns the monotonic definition revision position.
    pub const fn definition_revision_sequence(&self) -> u32 {
        self.definition_revision_sequence
    }

    /// Returns the source-authored reference revision bound by the definition.
    pub const fn definition_reference_revision(&self) -> &MetadataRevision {
        &self.definition_reference_revision
    }

    /// Returns the exact payload digest supporting the reference definition.
    pub const fn definition_reference_payload_digest(&self) -> EvidenceDigest {
        self.definition_reference_payload_digest
    }

    /// Returns when the selected immutable definition became locally durable.
    pub const fn definition_published_at(&self) -> Timestamp {
        self.definition_published_at
    }

    /// Returns the exact source assertion revision.
    pub const fn provider_identity_revision(&self) -> &MetadataRevision {
        &self.provider_identity_revision
    }

    /// Returns the exact source assertion payload digest.
    pub const fn provider_identity_payload_digest(&self) -> EvidenceDigest {
        self.provider_identity_payload_digest
    }

    /// Returns the half-open validity interval of the selected source assertion.
    pub const fn provider_identity_validity(&self) -> EffectiveInterval {
        self.provider_identity_validity
    }

    /// Returns exact venue mappings whose symbol equals the provider-native identity.
    ///
    /// An empty set is valid for provider products such as crypto pairs that do not reuse a
    /// listing-venue symbol. Every retained venue is still bounded by the selected definition's
    /// own effective interval.
    pub fn matching_venues(&self) -> &[VenueId] {
        &self.matching_venues
    }
}

/// Closed terminal state for a source-qualified provider identity.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MarketDataProviderIdentityResolutionOutcome {
    /// No knowable and effective exact source assertion exists.
    Missing,
    /// Exactly one immutable definition contains the exact source assertion.
    Exact(MarketDataProviderIdentityExactReceipt),
    /// More than one immutable definition contains the exact source assertion; no winner exists.
    Ambiguous,
}

/// Digest-bound result of one least-authority source-qualified provider identity read.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MarketDataProviderIdentityResolution {
    query: MarketDataProviderIdentityQuery,
    outcome: MarketDataProviderIdentityResolutionOutcome,
    receipt_digest: EvidenceDigest,
}

impl MarketDataProviderIdentityResolution {
    /// Returns the complete immutable query.
    pub const fn query(&self) -> &MarketDataProviderIdentityQuery {
        &self.query
    }

    /// Returns exact, ambiguous, or missing without implicit selection.
    pub const fn outcome(&self) -> &MarketDataProviderIdentityResolutionOutcome {
        &self.outcome
    }

    /// Returns the canonical digest binding request, outcome, and retained exact evidence.
    pub const fn receipt_digest(&self) -> EvidenceDigest {
        self.receipt_digest
    }
}

/// Current reference field responsible for a search match.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MarketDataInstrumentMatchKind {
    /// Evidence-bearing external identifier such as a FIGI, ISIN, CUSIP, or ticker.
    ExternalIdentifier,
    /// Rights-admitted display name.
    DisplayName,
    /// Current venue symbol.
    VenueSymbol,
    /// Accepted source-qualified provider symbol.
    ProviderSymbol,
}

/// One deterministic current-definition search result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MarketDataInstrumentSearchMatch {
    record: MarketDataInstrumentRecord,
    match_kind: MarketDataInstrumentMatchKind,
    matched_value: Box<str>,
}

impl MarketDataInstrumentSearchMatch {
    /// Returns the current digest-verified reference definition.
    pub const fn record(&self) -> &MarketDataInstrumentRecord {
        &self.record
    }

    /// Returns which admitted field matched.
    pub const fn match_kind(&self) -> MarketDataInstrumentMatchKind {
        self.match_kind
    }

    /// Returns the exact retained value responsible for the match.
    pub fn matched_value(&self) -> &str {
        &self.matched_value
    }
}

/// Bounded page of deterministic current-definition matches.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MarketDataInstrumentSearchPage {
    matches: Box<[MarketDataInstrumentSearchMatch]>,
    has_more: bool,
    knowledge_at: Option<Timestamp>,
    effective_at: Option<Timestamp>,
}

impl MarketDataInstrumentSearchPage {
    /// Returns the bounded ordered matches.
    pub fn matches(&self) -> &[MarketDataInstrumentSearchMatch] {
        &self.matches
    }

    /// Reports whether another matching definition exists beyond this page.
    pub const fn has_more(&self) -> bool {
        self.has_more
    }

    /// Returns the inclusive durable-knowledge cutoff for a point-in-time search.
    ///
    /// Current-definition searches return `None`; callers that require restart-stable selection
    /// must use [`MarketDataInstrumentReadCapability::search_as_of`] or
    /// [`MarketDataInstrumentReadCapability::resolve_exact_as_of`].
    pub const fn knowledge_at(&self) -> Option<Timestamp> {
        self.knowledge_at
    }

    /// Returns the instant at which every selected definition and matched identity was effective.
    pub const fn effective_at(&self) -> Option<Timestamp> {
        self.effective_at
    }
}

/// Cloneable least-authority capability that can only synchronize reference definitions.
#[derive(Clone)]
pub struct MarketDataInstrumentSynchronizationCapability {
    authority: Arc<Mutex<CatalogAuthority>>,
}

impl fmt::Debug for MarketDataInstrumentSynchronizationCapability {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MarketDataInstrumentSynchronizationCapability")
            .field("authority", &"[SEALED MARKET-DATA DEFINITION AUTHORITY]")
            .finish()
    }
}

impl MarketDataInstrumentSynchronizationCapability {
    /// Binds the capability to the sole catalog writer session.
    pub const fn new(authority: Arc<Mutex<CatalogAuthority>>) -> Self {
        Self { authority }
    }

    /// Atomically appends changed definitions and no-ops exact-current replays.
    pub fn synchronize(
        &self,
        synchronization: MarketDataInstrumentSynchronization,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<MarketDataInstrumentSynchronizationReceipt, MarketDataInstrumentCatalogError> {
        check_operation(deadline, cancellation)?;
        self.authority
            .try_lock()
            .map_err(|_| MarketDataInstrumentCatalogError::AuthorityUnavailable)?
            .synchronize_market_data_instruments(synchronization, deadline, cancellation)
    }
}

/// Cloneable least-authority capability for bounded current-definition reads.
#[derive(Clone)]
pub struct MarketDataInstrumentReadCapability {
    authority: Arc<Mutex<CatalogAuthority>>,
}

impl fmt::Debug for MarketDataInstrumentReadCapability {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MarketDataInstrumentReadCapability")
            .field(
                "authority",
                &"[SEALED MARKET-DATA DEFINITION READ AUTHORITY]",
            )
            .finish()
    }
}

impl MarketDataInstrumentReadCapability {
    /// Binds the reader to the sole catalog writer session without mutation authority.
    pub const fn new(authority: Arc<Mutex<CatalogAuthority>>) -> Self {
        Self { authority }
    }

    /// Returns the current definition for one deterministic internal identity.
    pub fn latest(
        &self,
        instrument_id: InstrumentId,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<Option<MarketDataInstrumentRecord>, MarketDataInstrumentCatalogError> {
        check_operation(deadline, cancellation)?;
        self.authority
            .try_lock()
            .map_err(|_| MarketDataInstrumentCatalogError::AuthorityUnavailable)?
            .latest_market_data_instrument(instrument_id, deadline, cancellation)
    }

    /// Atomically pins one exact, bounded stable-identity set at independent knowledge/effective
    /// coordinates.
    ///
    /// Each member selects the uniquely latest effective start durably published by the knowledge
    /// cutoff. Missing or non-effective members remain ordered exclusions; the caller must require
    /// [`MarketDataInstrumentPopulationDisposition::Complete`] before treating the records as the
    /// requested population.
    pub fn pin_population_as_of(
        &self,
        query: MarketDataInstrumentPopulationQuery,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<MarketDataInstrumentPopulationSelection, MarketDataInstrumentCatalogError> {
        check_operation(deadline, cancellation)?;
        self.authority
            .try_lock()
            .map_err(|_| MarketDataInstrumentCatalogError::AuthorityUnavailable)?
            .pin_market_data_instrument_population(query, deadline, cancellation)
    }

    /// Searches external identifiers, admitted display names, venue symbols, and accepted provider
    /// symbols.
    pub fn search(
        &self,
        query: &str,
        maximum_rows: usize,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<MarketDataInstrumentSearchPage, MarketDataInstrumentCatalogError> {
        if maximum_rows == 0 || maximum_rows > MAX_MARKET_DATA_INSTRUMENT_SEARCH_ROWS {
            return Err(MarketDataInstrumentCatalogError::InvalidLimit);
        }
        let query = query.trim();
        if query.is_empty()
            || query.len() > MAX_SEARCH_QUERY_BYTES
            || query.chars().any(char::is_control)
        {
            return Err(MarketDataInstrumentCatalogError::InvalidInput);
        }
        check_operation(deadline, cancellation)?;
        self.authority
            .try_lock()
            .map_err(|_| MarketDataInstrumentCatalogError::AuthorityUnavailable)?
            .search_market_data_instruments(query, maximum_rows, deadline, cancellation)
    }

    /// Searches the uniquely latest definition knowable and effective at independent clocks.
    ///
    /// This is candidate discovery only. A ticker, name, identifier, venue symbol, or provider
    /// alias never becomes a canonical identity merely because it appears first. Provider aliases
    /// and external identifiers participate only inside their own asserted validity intervals.
    pub fn search_as_of(
        &self,
        query: &str,
        knowledge_at: Timestamp,
        effective_at: Timestamp,
        maximum_rows: usize,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<MarketDataInstrumentSearchPage, MarketDataInstrumentCatalogError> {
        validate_search(query, maximum_rows)?;
        check_operation(deadline, cancellation)?;
        self.authority
            .try_lock()
            .map_err(|_| MarketDataInstrumentCatalogError::AuthorityUnavailable)?
            .search_market_data_instruments_as_of(
                query.trim(),
                knowledge_at,
                effective_at,
                maximum_rows,
                SearchMode::Candidate,
                deadline,
                cancellation,
            )
    }

    /// Resolves an exact admitted search term without selecting through ambiguity.
    ///
    /// Zero matches means missing. Exactly one match with `has_more == false` proves only that the
    /// admitted term is structurally unique; it does not turn a ticker, name, venue symbol, or
    /// provider alias into canonical identity authority. Every multi-row or bounded-incomplete
    /// result must be rejected. The fixed two-row result is sufficient to prove ordinary
    /// ambiguity without exposing provider routing in a product contract.
    pub fn resolve_exact_as_of(
        &self,
        query: &str,
        knowledge_at: Timestamp,
        effective_at: Timestamp,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<MarketDataInstrumentSearchPage, MarketDataInstrumentCatalogError> {
        const MAX_EXACT_CANDIDATES: usize = 2;
        validate_search(query, MAX_EXACT_CANDIDATES)?;
        check_operation(deadline, cancellation)?;
        self.authority
            .try_lock()
            .map_err(|_| MarketDataInstrumentCatalogError::AuthorityUnavailable)?
            .search_market_data_instruments_as_of(
                query.trim(),
                knowledge_at,
                effective_at,
                MAX_EXACT_CANDIDATES,
                SearchMode::Exact,
                deadline,
                cancellation,
            )
    }

    /// Resolves one exact provider-native identity inside its explicit source namespace.
    ///
    /// This backend seam never guesses from a ticker or an unqualified symbol. Exact selection is
    /// returned only when one digest-verified definition contains one effective accepted provider
    /// assertion at the requested clocks; every collision remains ambiguous.
    pub fn resolve_provider_identity_as_of(
        &self,
        query: MarketDataProviderIdentityQuery,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<MarketDataProviderIdentityResolution, MarketDataInstrumentCatalogError> {
        check_operation(deadline, cancellation)?;
        self.authority
            .try_lock()
            .map_err(|_| MarketDataInstrumentCatalogError::AuthorityUnavailable)?
            .resolve_market_data_provider_identity(query, deadline, cancellation)
    }

    /// Replays a source-qualified provider identity read and rejects any evidence/currentness
    /// drift after process restart.
    pub fn verify_provider_identity_restart(
        &self,
        expected: &MarketDataProviderIdentityResolution,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<MarketDataProviderIdentityResolution, MarketDataInstrumentCatalogError> {
        let replay =
            self.resolve_provider_identity_as_of(expected.query().clone(), deadline, cancellation)?;
        if replay != *expected {
            return Err(MarketDataInstrumentCatalogError::CorruptCatalog);
        }
        Ok(replay)
    }
}

fn validate_search(
    query: &str,
    maximum_rows: usize,
) -> Result<(), MarketDataInstrumentCatalogError> {
    if maximum_rows == 0 || maximum_rows > MAX_MARKET_DATA_INSTRUMENT_SEARCH_ROWS {
        return Err(MarketDataInstrumentCatalogError::InvalidLimit);
    }
    let query = query.trim();
    if query.is_empty()
        || query.len() > MAX_SEARCH_QUERY_BYTES
        || query.chars().any(char::is_control)
    {
        return Err(MarketDataInstrumentCatalogError::InvalidInput);
    }
    Ok(())
}

/// Market-data definition validation, publication, or bounded-read failure.
#[derive(Debug, Error)]
pub enum MarketDataInstrumentCatalogError {
    /// Required input was absent or invalid.
    #[error("market-data instrument input is invalid")]
    InvalidInput,
    /// A population query was empty, repeated an identity, or exceeded its fixed set bound.
    #[error("market-data instrument population query is invalid")]
    InvalidPopulationQuery,
    /// Producer-declared batch cardinality did not match the submitted batch.
    #[error("partial market-data instrument batch: expected {expected}, received {actual}")]
    PartialBatch { expected: usize, actual: usize },
    /// Synchronization exceeded its hard row bound.
    #[error("market-data instrument batch exceeds maximum {max}")]
    BatchLimitExceeded { max: usize },
    /// A submitted batch repeated one internal identity.
    #[error("market-data instrument batch contains a duplicate instrument identity")]
    DuplicateInstrumentId,
    /// A changed definition predates the current effective revision.
    #[error("market-data instrument definition revision is stale")]
    StaleRevision,
    /// A changed definition reused the current effective start.
    #[error("market-data instrument definition conflicts at the current effective time")]
    EqualTimeRevisionConflict,
    /// One instrument exhausted its immutable revision bound.
    #[error("market-data instrument revision limit was exceeded")]
    RevisionLimitExceeded,
    /// A read or synchronization result exceeded configured byte limits.
    #[error("market-data instrument result byte limit was exceeded")]
    ResultByteLimitExceeded,
    /// The catalog capability is busy or poisoned.
    #[error("market-data instrument catalog authority is unavailable")]
    AuthorityUnavailable,
    /// Caller cancelled the operation.
    #[error("market-data instrument operation was cancelled")]
    Cancelled,
    /// Caller deadline elapsed.
    #[error("market-data instrument operation deadline elapsed")]
    DeadlineExceeded,
    /// Requested result count was zero or exceeded its hard bound.
    #[error("market-data instrument result limit is invalid")]
    InvalidLimit,
    /// Durable bytes or relational state failed verification.
    #[error("market-data instrument catalog content is corrupt")]
    CorruptCatalog,
    /// Canonical definition serialization failed.
    #[error("market-data instrument serialization failed")]
    Serialization(#[from] serde_json::Error),
    /// SQLite rejected the bounded catalog operation.
    #[error("market-data instrument storage operation failed")]
    Storage(#[from] rusqlite::Error),
}

#[derive(Debug)]
struct PreparedDefinition {
    definition: MarketDataInstrumentDefinition,
    json: String,
    digest: [u8; 32],
    terms: Vec<SearchTerm>,
}

#[derive(Debug)]
struct SearchTerm {
    kind: &'static str,
    ordinal: usize,
    normalized: String,
    display: String,
    source_id: Option<String>,
    effective_start_ns: i64,
    effective_end_ns: Option<i64>,
}

#[derive(Debug)]
enum PublicationPlan {
    Replay,
    Insert {
        sequence: u32,
        previous: Option<[u8; 32]>,
        identity_is_new: bool,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SearchMode {
    Candidate,
    Exact,
}

#[derive(Debug)]
struct StoredDefinitionRow {
    digest: Vec<u8>,
    instrument_id: String,
    revision_sequence: i64,
    effective_start_ns: i64,
    effective_end_ns: Option<i64>,
    reference_revision: String,
    reference_algorithm: i64,
    reference_payload_digest: Vec<u8>,
    definition_json: String,
    published_at_ns: i64,
}

impl CatalogAuthority {
    fn synchronize_market_data_instruments(
        &self,
        synchronization: MarketDataInstrumentSynchronization,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<MarketDataInstrumentSynchronizationReceipt, MarketDataInstrumentCatalogError> {
        let mut prepared = prepare_definitions(synchronization.definitions)?;
        if prepared.iter().any(|definition| {
            definition.json.len() > self.catalog().result_bytes.max_record_bytes()
        }) {
            return Err(MarketDataInstrumentCatalogError::ResultByteLimitExceeded);
        }
        prepared.sort_by_key(|definition| definition.definition.instrument_id());
        let batch_digest = batch_digest(&prepared);
        let connection = &self.catalog().connection;
        install_progress_handler(connection, deadline, cancellation)?;
        let result = (|| {
            let transaction = connection.unchecked_transaction()?;
            let mut plans = Vec::new();
            plans
                .try_reserve_exact(prepared.len())
                .map_err(|_| MarketDataInstrumentCatalogError::ResultByteLimitExceeded)?;
            for definition in &prepared {
                check_operation(deadline, cancellation)?;
                plans.push(plan_publication(&transaction, definition)?);
            }
            let inserted = plans
                .iter()
                .filter(|plan| matches!(plan, PublicationPlan::Insert { .. }))
                .count();
            if inserted == 0 {
                return Ok(MarketDataInstrumentSynchronizationReceipt {
                    batch_digest: digest(batch_digest),
                    submitted: prepared.len(),
                    inserted: 0,
                    replayed: prepared.len(),
                });
            }
            let published_at = trusted_catalog_now(&transaction)
                .map_err(|_| MarketDataInstrumentCatalogError::CorruptCatalog)?;
            for (definition, plan) in prepared.iter().zip(&plans) {
                check_operation(deadline, cancellation)?;
                if let PublicationPlan::Insert {
                    sequence,
                    previous,
                    identity_is_new,
                } = plan
                {
                    insert_definition(
                        &transaction,
                        definition,
                        *sequence,
                        *previous,
                        *identity_is_new,
                        published_at,
                    )?;
                }
            }
            check_operation(deadline, cancellation)?;
            append_audit(
                &transaction,
                "market-data-instrument.synchronized",
                "repository-owned-market-data-definitions",
                batch_digest,
                published_at,
            )
            .map_err(|_| MarketDataInstrumentCatalogError::CorruptCatalog)?;
            transaction.commit()?;
            Ok(MarketDataInstrumentSynchronizationReceipt {
                batch_digest: digest(batch_digest),
                submitted: prepared.len(),
                inserted,
                replayed: prepared.len().saturating_sub(inserted),
            })
        })();
        clear_progress_handler(connection)?;
        classify_operation(result, deadline, cancellation)
    }

    fn latest_market_data_instrument(
        &self,
        instrument_id: InstrumentId,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<Option<MarketDataInstrumentRecord>, MarketDataInstrumentCatalogError> {
        self.read_latest(
            "current_.instrument_id=?1",
            instrument_id.to_string(),
            deadline,
            cancellation,
        )
    }

    fn read_latest(
        &self,
        predicate: &str,
        key: String,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<Option<MarketDataInstrumentRecord>, MarketDataInstrumentCatalogError> {
        check_operation(deadline, cancellation)?;
        let connection = &self.catalog().connection;
        install_progress_handler(connection, deadline, cancellation)?;
        let result = (|| {
            let sql = format!(
                "SELECT {STORED_COLUMNS}
                 FROM market_data_instrument_current AS current_
                 JOIN market_data_instrument_revisions AS revisions
                   ON revisions.revision_digest=current_.revision_digest
                 WHERE {predicate}"
            );
            let row = connection
                .query_row(&sql, [key], decode_stored_row)
                .optional()?;
            let Some(row) = row else {
                return Ok(None);
            };
            let mut budget = ResultBudget::new(self.catalog().result_bytes);
            charge_row(&row, &mut budget)?;
            rebuild_record(row).map(Some)
        })();
        clear_progress_handler(connection)?;
        classify_operation(result, deadline, cancellation)
    }

    fn pin_market_data_instrument_population(
        &self,
        query: MarketDataInstrumentPopulationQuery,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<MarketDataInstrumentPopulationSelection, MarketDataInstrumentCatalogError> {
        check_operation(deadline, cancellation)?;
        let connection = &self.catalog().connection;
        install_progress_handler(connection, deadline, cancellation)?;
        let result = (|| {
            let transaction = connection.unchecked_transaction()?;
            let mut budget = ResultBudget::new(self.catalog().result_bytes);
            let mut records = Vec::new();
            records
                .try_reserve_exact(query.instrument_ids.len())
                .map_err(|_| MarketDataInstrumentCatalogError::ResultByteLimitExceeded)?;
            let mut exclusions = Vec::new();
            exclusions
                .try_reserve_exact(query.instrument_ids.len())
                .map_err(|_| MarketDataInstrumentCatalogError::ResultByteLimitExceeded)?;
            for instrument_id in query.instrument_ids() {
                check_operation(deadline, cancellation)?;
                match select_population_member(
                    &transaction,
                    *instrument_id,
                    query.knowledge_at,
                    query.effective_at,
                    &mut budget,
                )? {
                    PopulationMember::Record(record) => records.push(record),
                    PopulationMember::Excluded(reason) => {
                        budget
                            .charge([size_of::<MarketDataInstrumentPopulationExclusion>()])
                            .map_err(|_| {
                                MarketDataInstrumentCatalogError::ResultByteLimitExceeded
                            })?;
                        exclusions.push(MarketDataInstrumentPopulationExclusion {
                            instrument_id: *instrument_id,
                            reason,
                        });
                    }
                }
            }
            let disposition =
                if exclusions.is_empty() && records.len() == query.instrument_ids().len() {
                    MarketDataInstrumentPopulationDisposition::Complete
                } else {
                    MarketDataInstrumentPopulationDisposition::Unavailable
                };
            if records.len() + exclusions.len() != query.instrument_ids().len() {
                return Err(MarketDataInstrumentCatalogError::CorruptCatalog);
            }
            let receipt_digest =
                population_receipt_digest(query.query_digest, disposition, &records, &exclusions);
            transaction.commit()?;
            Ok(MarketDataInstrumentPopulationSelection {
                query,
                disposition,
                records: records.into_boxed_slice(),
                exclusions: exclusions.into_boxed_slice(),
                receipt_digest,
            })
        })();
        clear_progress_handler(connection)?;
        classify_operation(result, deadline, cancellation)
    }

    fn resolve_market_data_provider_identity(
        &self,
        query: MarketDataProviderIdentityQuery,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<MarketDataProviderIdentityResolution, MarketDataInstrumentCatalogError> {
        const MAX_RETAINED_EXACT_MATCHES: usize = 2;
        check_operation(deadline, cancellation)?;
        let connection = &self.catalog().connection;
        install_progress_handler(connection, deadline, cancellation)?;
        let result = (|| {
            let retrieval_limit = i64::try_from(MAX_RETAINED_EXACT_MATCHES.saturating_add(1))
                .map_err(|_| MarketDataInstrumentCatalogError::InvalidLimit)?;
            let mut statement = connection.prepare(PROVIDER_IDENTITY_AS_OF_SQL)?;
            let rows = statement.query_map(
                params![
                    query.source_id().as_str(),
                    query.provider_instrument_id().as_str(),
                    query.knowledge_at().unix_nanos(),
                    query.effective_at().unix_nanos(),
                    retrieval_limit,
                ],
                |row| {
                    Ok((
                        decode_stored_row(row)?,
                        row.get::<_, i64>(10)?,
                        row.get::<_, Option<i64>>(11)?,
                    ))
                },
            )?;
            let mut budget = ResultBudget::new(self.catalog().result_bytes);
            let mut receipts = Vec::new();
            receipts
                .try_reserve_exact(MAX_RETAINED_EXACT_MATCHES.saturating_add(1))
                .map_err(|_| MarketDataInstrumentCatalogError::ResultByteLimitExceeded)?;
            for row in rows {
                check_operation(deadline, cancellation)?;
                let (stored, term_start_ns, term_end_ns) = row?;
                charge_row(&stored, &mut budget)?;
                let record = rebuild_record(stored)?;
                let definition = record.definition();
                if record.published_at() > query.knowledge_at()
                    || !interval_contains(definition.effective_interval(), query.effective_at())
                {
                    return Err(MarketDataInstrumentCatalogError::CorruptCatalog);
                }
                let provider_identity = definition
                    .provider_identity_at(
                        query.source_id(),
                        query.provider_instrument_id(),
                        query.effective_at(),
                    )
                    .ok_or(MarketDataInstrumentCatalogError::CorruptCatalog)?;
                if provider_identity.instrument_id() != definition.instrument_id()
                    || provider_identity.source_id() != query.source_id()
                    || provider_identity.provider_instrument_id() != query.provider_instrument_id()
                    || provider_identity.validity().starts_at().unix_nanos() != term_start_ns
                    || provider_identity
                        .validity()
                        .ends_at()
                        .map(Timestamp::unix_nanos)
                        != term_end_ns
                {
                    return Err(MarketDataInstrumentCatalogError::CorruptCatalog);
                }
                let mut matching_venues = Vec::new();
                matching_venues
                    .try_reserve_exact(definition.venue_mappings().len())
                    .map_err(|_| MarketDataInstrumentCatalogError::ResultByteLimitExceeded)?;
                for mapping in definition.venue_mappings() {
                    if mapping.venue_symbol().as_str() == query.provider_instrument_id().as_str() {
                        budget
                            .charge([size_of::<VenueId>(), mapping.venue_id().as_str().len()])
                            .map_err(|_| {
                                MarketDataInstrumentCatalogError::ResultByteLimitExceeded
                            })?;
                        matching_venues.push(mapping.venue_id().clone());
                    }
                }
                budget
                    .charge([size_of::<MarketDataProviderIdentityExactReceipt>()])
                    .map_err(|_| MarketDataInstrumentCatalogError::ResultByteLimitExceeded)?;
                receipts.push(MarketDataProviderIdentityExactReceipt {
                    instrument_id: definition.instrument_id(),
                    definition_revision_digest: record.revision_digest(),
                    definition_revision_sequence: record.revision_sequence(),
                    definition_reference_revision: definition.reference_revision().clone(),
                    definition_reference_payload_digest: definition
                        .reference_payload_evidence()
                        .content_digest(),
                    definition_published_at: record.published_at(),
                    provider_identity_revision: provider_identity.metadata_revision().clone(),
                    provider_identity_payload_digest: provider_identity.evidence().content_digest(),
                    provider_identity_validity: provider_identity.validity(),
                    matching_venues: matching_venues.into_boxed_slice(),
                });
            }
            let has_more = receipts.len() > MAX_RETAINED_EXACT_MATCHES;
            receipts.truncate(MAX_RETAINED_EXACT_MATCHES);
            let outcome = match receipts.as_slice() {
                [] => MarketDataProviderIdentityResolutionOutcome::Missing,
                [exact] if !has_more => {
                    MarketDataProviderIdentityResolutionOutcome::Exact(exact.clone())
                }
                _ => MarketDataProviderIdentityResolutionOutcome::Ambiguous,
            };
            let receipt_digest = provider_identity_resolution_digest(
                query.query_digest(),
                &outcome,
                &receipts,
                has_more,
            );
            Ok(MarketDataProviderIdentityResolution {
                query,
                outcome,
                receipt_digest,
            })
        })();
        clear_progress_handler(connection)?;
        classify_operation(result, deadline, cancellation)
    }

    fn search_market_data_instruments(
        &self,
        query: &str,
        maximum_rows: usize,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<MarketDataInstrumentSearchPage, MarketDataInstrumentCatalogError> {
        check_operation(deadline, cancellation)?;
        let connection = &self.catalog().connection;
        install_progress_handler(connection, deadline, cancellation)?;
        let result = (|| {
            let retrieval_limit = i64::try_from(maximum_rows.saturating_add(1))
                .map_err(|_| MarketDataInstrumentCatalogError::InvalidLimit)?;
            let mut statement = connection.prepare(SEARCH_SQL)?;
            let rows = statement.query_map(params![normalize(query), retrieval_limit], |row| {
                Ok((
                    decode_stored_row(row)?,
                    row.get::<_, String>(10)?,
                    row.get::<_, String>(11)?,
                ))
            })?;
            let mut budget = ResultBudget::new(self.catalog().result_bytes);
            let mut matches = Vec::new();
            matches
                .try_reserve_exact(maximum_rows.saturating_add(1))
                .map_err(|_| MarketDataInstrumentCatalogError::ResultByteLimitExceeded)?;
            for row in rows {
                check_operation(deadline, cancellation)?;
                let (stored, kind, matched_value) = row?;
                charge_row(&stored, &mut budget)?;
                budget
                    .charge([
                        size_of::<MarketDataInstrumentSearchMatch>(),
                        matched_value.len(),
                    ])
                    .map_err(|_| MarketDataInstrumentCatalogError::ResultByteLimitExceeded)?;
                matches.push(MarketDataInstrumentSearchMatch {
                    record: rebuild_record(stored)?,
                    match_kind: parse_match_kind(&kind)?,
                    matched_value: matched_value.into_boxed_str(),
                });
            }
            let has_more = matches.len() > maximum_rows;
            matches.truncate(maximum_rows);
            Ok(MarketDataInstrumentSearchPage {
                matches: matches.into_boxed_slice(),
                has_more,
                knowledge_at: None,
                effective_at: None,
            })
        })();
        clear_progress_handler(connection)?;
        classify_operation(result, deadline, cancellation)
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "point-in-time identity coordinates and operation controls stay explicit"
    )]
    fn search_market_data_instruments_as_of(
        &self,
        query: &str,
        knowledge_at: Timestamp,
        effective_at: Timestamp,
        maximum_rows: usize,
        mode: SearchMode,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<MarketDataInstrumentSearchPage, MarketDataInstrumentCatalogError> {
        check_operation(deadline, cancellation)?;
        let connection = &self.catalog().connection;
        install_progress_handler(connection, deadline, cancellation)?;
        let result = (|| {
            let retrieval_limit =
                i64::try_from(MAX_MARKET_DATA_INSTRUMENT_SEARCH_ROWS.saturating_add(1))
                    .map_err(|_| MarketDataInstrumentCatalogError::InvalidLimit)?;
            let exact_only = i64::from(mode == SearchMode::Exact);
            let normalized_query = normalize(query);
            let mut statement = connection.prepare(SEARCH_AS_OF_SQL)?;
            let rows = statement.query_map(
                params![
                    normalized_query,
                    knowledge_at.unix_nanos(),
                    effective_at.unix_nanos(),
                    exact_only,
                    retrieval_limit,
                ],
                |row| {
                    Ok((
                        decode_stored_row(row)?,
                        row.get::<_, String>(10)?,
                        row.get::<_, String>(11)?,
                    ))
                },
            )?;
            let mut budget = ResultBudget::new(self.catalog().result_bytes);
            let mut matches = Vec::new();
            matches
                .try_reserve_exact(maximum_rows.saturating_add(1))
                .map_err(|_| MarketDataInstrumentCatalogError::ResultByteLimitExceeded)?;
            let mut candidate_rows = 0_usize;
            for row in rows {
                check_operation(deadline, cancellation)?;
                candidate_rows = candidate_rows
                    .checked_add(1)
                    .ok_or(MarketDataInstrumentCatalogError::ResultByteLimitExceeded)?;
                let (stored, kind, matched_value) = row?;
                charge_row(&stored, &mut budget)?;
                let record = rebuild_record(stored)?;
                let match_kind = parse_match_kind(&kind)?;
                if !matched_identity_is_effective(
                    record.definition(),
                    match_kind,
                    &matched_value,
                    effective_at,
                ) {
                    continue;
                }
                budget
                    .charge([
                        size_of::<MarketDataInstrumentSearchMatch>(),
                        matched_value.len(),
                    ])
                    .map_err(|_| MarketDataInstrumentCatalogError::ResultByteLimitExceeded)?;
                if matches.len() <= maximum_rows {
                    matches.push(MarketDataInstrumentSearchMatch {
                        record,
                        match_kind,
                        matched_value: matched_value.into_boxed_str(),
                    });
                }
            }
            let has_more = matches.len() > maximum_rows
                || candidate_rows > MAX_MARKET_DATA_INSTRUMENT_SEARCH_ROWS;
            matches.truncate(maximum_rows);
            Ok(MarketDataInstrumentSearchPage {
                matches: matches.into_boxed_slice(),
                has_more,
                knowledge_at: Some(knowledge_at),
                effective_at: Some(effective_at),
            })
        })();
        clear_progress_handler(connection)?;
        classify_operation(result, deadline, cancellation)
    }
}

enum PopulationMember {
    Record(MarketDataInstrumentRecord),
    Excluded(MarketDataInstrumentPopulationExclusionReason),
}

fn select_population_member(
    transaction: &Transaction<'_>,
    instrument_id: InstrumentId,
    knowledge_at: Timestamp,
    effective_at: Timestamp,
    budget: &mut ResultBudget,
) -> Result<PopulationMember, MarketDataInstrumentCatalogError> {
    let instrument_text = instrument_id.to_string();
    let mut statement = transaction.prepare(POPULATION_AS_OF_SQL)?;
    let mut rows = statement.query_map(
        params![
            instrument_text,
            knowledge_at.unix_nanos(),
            effective_at.unix_nanos()
        ],
        decode_stored_row,
    )?;
    let selected = rows.next().transpose()?;
    if rows.next().transpose()?.is_some() {
        return Err(MarketDataInstrumentCatalogError::CorruptCatalog);
    }
    drop(rows);
    drop(statement);
    let Some(selected) = selected else {
        let known: bool = transaction.query_row(
            POPULATION_KNOWN_SQL,
            params![instrument_id.to_string(), knowledge_at.unix_nanos()],
            |row| row.get(0),
        )?;
        return Ok(PopulationMember::Excluded(if known {
            MarketDataInstrumentPopulationExclusionReason::NoEffectiveRevision
        } else {
            MarketDataInstrumentPopulationExclusionReason::NoKnownRevision
        }));
    };
    charge_row(&selected, budget)?;
    let record = rebuild_record(selected)?;
    let interval = record.definition().effective_interval();
    if record.definition().instrument_id() != instrument_id
        || record.published_at() > knowledge_at
        || interval.starts_at() > effective_at
    {
        return Err(MarketDataInstrumentCatalogError::CorruptCatalog);
    }
    if interval.ends_at().is_some_and(|end| effective_at >= end) {
        return Ok(PopulationMember::Excluded(
            MarketDataInstrumentPopulationExclusionReason::NoEffectiveRevision,
        ));
    }
    Ok(PopulationMember::Record(record))
}

fn prepare_definitions(
    definitions: Box<[MarketDataInstrumentDefinition]>,
) -> Result<Vec<PreparedDefinition>, MarketDataInstrumentCatalogError> {
    let mut instrument_ids = BTreeSet::new();
    let mut prepared = Vec::new();
    prepared
        .try_reserve_exact(definitions.len())
        .map_err(|_| MarketDataInstrumentCatalogError::ResultByteLimitExceeded)?;
    for definition in Vec::from(definitions) {
        if !instrument_ids.insert(definition.instrument_id()) {
            return Err(MarketDataInstrumentCatalogError::DuplicateInstrumentId);
        }
        let json = serde_json::to_string(&definition)?;
        let digest = sha256(json.as_bytes());
        let terms = search_terms(&definition)?;
        prepared.push(PreparedDefinition {
            definition,
            json,
            digest,
            terms,
        });
    }
    Ok(prepared)
}

fn plan_publication(
    transaction: &Transaction<'_>,
    incoming: &PreparedDefinition,
) -> Result<PublicationPlan, MarketDataInstrumentCatalogError> {
    let instrument_id = incoming.definition.instrument_id().to_string();
    let identity_exists: bool = transaction.query_row(
        "SELECT EXISTS(SELECT 1 FROM market_data_instrument_identities WHERE instrument_id=?1)",
        [&instrument_id],
        |row| row.get(0),
    )?;

    let current: Option<([u8; 32], u32, i64)> = transaction
        .query_row(
            "SELECT revisions.revision_digest, revisions.revision_sequence,
                    revisions.effective_start_ns
             FROM market_data_instrument_current AS current_
             JOIN market_data_instrument_revisions AS revisions
               ON revisions.revision_digest=current_.revision_digest
             WHERE current_.instrument_id=?1",
            [&instrument_id],
            |row| {
                let digest: Vec<u8> = row.get(0)?;
                let digest: [u8; 32] = digest.try_into().map_err(|_| {
                    rusqlite::Error::InvalidColumnType(
                        0,
                        "revision_digest".to_owned(),
                        rusqlite::types::Type::Blob,
                    )
                })?;
                let sequence: i64 = row.get(1)?;
                let sequence = u32::try_from(sequence)
                    .map_err(|_| rusqlite::Error::IntegralValueOutOfRange(1, sequence))?;
                Ok((digest, sequence, row.get(2)?))
            },
        )
        .optional()?;
    let incoming_start = incoming
        .definition
        .effective_interval()
        .starts_at()
        .unix_nanos();
    match current {
        None if identity_exists => Err(MarketDataInstrumentCatalogError::CorruptCatalog),
        None => Ok(PublicationPlan::Insert {
            sequence: 1,
            previous: None,
            identity_is_new: true,
        }),
        Some((current_digest, _, _)) if current_digest == incoming.digest => {
            let retained = load_record_by_digest(transaction, current_digest)?
                .ok_or(MarketDataInstrumentCatalogError::CorruptCatalog)?;
            if retained.definition != incoming.definition {
                return Err(MarketDataInstrumentCatalogError::CorruptCatalog);
            }
            Ok(PublicationPlan::Replay)
        }
        Some((_, _, current_start)) if incoming_start < current_start => {
            Err(MarketDataInstrumentCatalogError::StaleRevision)
        }
        Some((_, _, current_start)) if incoming_start == current_start => {
            Err(MarketDataInstrumentCatalogError::EqualTimeRevisionConflict)
        }
        Some((current_digest, current_sequence, _)) => {
            if transaction
                .query_row(
                    "SELECT 1 FROM market_data_instrument_revisions WHERE revision_digest=?1",
                    [incoming.digest],
                    |_| Ok(()),
                )
                .optional()?
                .is_some()
            {
                return Err(MarketDataInstrumentCatalogError::StaleRevision);
            }
            let sequence = current_sequence
                .checked_add(1)
                .filter(|sequence| *sequence <= MAX_REVISIONS_PER_INSTRUMENT)
                .ok_or(MarketDataInstrumentCatalogError::RevisionLimitExceeded)?;
            Ok(PublicationPlan::Insert {
                sequence,
                previous: Some(current_digest),
                identity_is_new: false,
            })
        }
    }
}

fn insert_definition(
    transaction: &Transaction<'_>,
    prepared: &PreparedDefinition,
    sequence: u32,
    previous: Option<[u8; 32]>,
    identity_is_new: bool,
    published_at: Timestamp,
) -> Result<(), MarketDataInstrumentCatalogError> {
    let definition = &prepared.definition;
    let instrument_id = definition.instrument_id().to_string();
    if identity_is_new {
        transaction.execute(
            "INSERT INTO market_data_instrument_identities
             (instrument_id, created_at_ns) VALUES (?1, ?2)",
            params![instrument_id, published_at.unix_nanos()],
        )?;
    }
    let reference = definition.reference_payload_evidence().content_digest();
    transaction.execute(
        "INSERT INTO market_data_instrument_revisions
         (revision_digest, instrument_id, revision_sequence,
          previous_revision_digest, effective_start_ns, effective_end_ns, reference_revision,
          reference_algorithm, reference_payload_digest, definition_json, published_at_ns)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
        params![
            prepared.digest,
            instrument_id,
            i64::from(sequence),
            previous,
            definition.effective_interval().starts_at().unix_nanos(),
            definition
                .effective_interval()
                .ends_at()
                .map(Timestamp::unix_nanos),
            definition
                .reference_revision()
                .as_source_identifier()
                .as_str(),
            algorithm_code(reference.algorithm()),
            reference.bytes(),
            prepared.json,
            published_at.unix_nanos(),
        ],
    )?;
    for term in &prepared.terms {
        transaction.execute(
            "INSERT INTO market_data_instrument_search_terms
             (revision_digest, term_kind, term_ordinal, normalized_term, display_term,
              source_id, effective_start_ns, effective_end_ns)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            params![
                prepared.digest,
                term.kind,
                i64::try_from(term.ordinal)
                    .map_err(|_| MarketDataInstrumentCatalogError::CorruptCatalog)?,
                term.normalized,
                term.display,
                term.source_id,
                term.effective_start_ns,
                term.effective_end_ns,
            ],
        )?;
    }
    if identity_is_new {
        transaction.execute(
            "INSERT INTO market_data_instrument_current
             (instrument_id, revision_digest, advanced_at_ns)
             VALUES (?1, ?2, ?3)",
            params![instrument_id, prepared.digest, published_at.unix_nanos()],
        )?;
    } else {
        let changed = transaction.execute(
            "UPDATE market_data_instrument_current
             SET revision_digest=?1, advanced_at_ns=?2
             WHERE instrument_id=?3 AND revision_digest=?4",
            params![
                prepared.digest,
                published_at.unix_nanos(),
                instrument_id,
                previous,
            ],
        )?;
        if changed != 1 {
            return Err(MarketDataInstrumentCatalogError::CorruptCatalog);
        }
    }
    Ok(())
}

fn search_terms(
    definition: &MarketDataInstrumentDefinition,
) -> Result<Vec<SearchTerm>, MarketDataInstrumentCatalogError> {
    let mut terms = Vec::new();
    for identifier in definition.identifiers() {
        push_term(
            &mut terms,
            "external_identifier",
            &identifier.identifier().to_string(),
            None,
            identifier.validity(),
        )?;
    }
    if let Some(name) = definition.display_name() {
        push_term(
            &mut terms,
            "display_name",
            name.as_str(),
            None,
            definition.effective_interval(),
        )?;
    }
    for mapping in definition.venue_mappings() {
        push_term(
            &mut terms,
            "venue_symbol",
            mapping.venue_symbol().as_str(),
            None,
            definition.effective_interval(),
        )?;
    }
    for identity in definition.provider_identities() {
        push_term(
            &mut terms,
            "provider_symbol",
            identity.provider_instrument_id().as_str(),
            Some(identity.source_id()),
            identity.validity(),
        )?;
    }
    terms.sort_by(|left, right| {
        left.kind
            .cmp(right.kind)
            .then_with(|| left.source_id.cmp(&right.source_id))
            .then_with(|| left.normalized.cmp(&right.normalized))
            .then_with(|| left.display.cmp(&right.display))
            .then_with(|| left.effective_start_ns.cmp(&right.effective_start_ns))
            .then_with(|| left.effective_end_ns.cmp(&right.effective_end_ns))
    });
    terms.dedup_by(|left, right| {
        left.kind == right.kind
            && left.source_id == right.source_id
            && left.normalized == right.normalized
            && left.display == right.display
            && left.effective_start_ns == right.effective_start_ns
            && left.effective_end_ns == right.effective_end_ns
    });
    let mut previous_kind = "";
    let mut ordinal = 0_usize;
    for term in &mut terms {
        if term.kind != previous_kind {
            previous_kind = term.kind;
            ordinal = 0;
        }
        term.ordinal = ordinal;
        ordinal = ordinal
            .checked_add(1)
            .ok_or(MarketDataInstrumentCatalogError::InvalidInput)?;
        if term.ordinal > 255 {
            return Err(MarketDataInstrumentCatalogError::InvalidInput);
        }
    }
    Ok(terms)
}

fn push_term(
    terms: &mut Vec<SearchTerm>,
    kind: &'static str,
    display: &str,
    source_id: Option<&SourceId>,
    validity: EffectiveInterval,
) -> Result<(), MarketDataInstrumentCatalogError> {
    let normalized = normalize(display);
    if normalized.is_empty() || normalized.len() > MAX_SEARCH_QUERY_BYTES {
        return Err(MarketDataInstrumentCatalogError::InvalidInput);
    }
    terms.push(SearchTerm {
        kind,
        ordinal: 0,
        normalized,
        display: display.to_owned(),
        source_id: source_id.map(ToString::to_string),
        effective_start_ns: validity.starts_at().unix_nanos(),
        effective_end_ns: validity.ends_at().map(Timestamp::unix_nanos),
    });
    Ok(())
}

fn load_record_by_digest(
    transaction: &Transaction<'_>,
    digest: [u8; 32],
) -> Result<Option<MarketDataInstrumentRecord>, MarketDataInstrumentCatalogError> {
    let row = transaction
        .query_row(
            &format!(
                "SELECT {STORED_COLUMNS} FROM market_data_instrument_revisions AS revisions
                 WHERE revisions.revision_digest=?1"
            ),
            [digest],
            decode_stored_row,
        )
        .optional()?;
    row.map(rebuild_record).transpose()
}

fn decode_stored_row(row: &Row<'_>) -> rusqlite::Result<StoredDefinitionRow> {
    Ok(StoredDefinitionRow {
        digest: row.get(0)?,
        instrument_id: row.get(1)?,
        revision_sequence: row.get(2)?,
        effective_start_ns: row.get(3)?,
        effective_end_ns: row.get(4)?,
        reference_revision: row.get(5)?,
        reference_algorithm: row.get(6)?,
        reference_payload_digest: row.get(7)?,
        definition_json: row.get(8)?,
        published_at_ns: row.get(9)?,
    })
}

fn rebuild_record(
    row: StoredDefinitionRow,
) -> Result<MarketDataInstrumentRecord, MarketDataInstrumentCatalogError> {
    let revision_digest: [u8; 32] = row
        .digest
        .as_slice()
        .try_into()
        .map_err(|_| MarketDataInstrumentCatalogError::CorruptCatalog)?;
    if revision_digest == [0; 32] || sha256(row.definition_json.as_bytes()) != revision_digest {
        return Err(MarketDataInstrumentCatalogError::CorruptCatalog);
    }
    let definition: MarketDataInstrumentDefinition = serde_json::from_str(&row.definition_json)
        .map_err(|_| MarketDataInstrumentCatalogError::CorruptCatalog)?;
    if serde_json::to_string(&definition)
        .map_err(|_| MarketDataInstrumentCatalogError::CorruptCatalog)?
        != row.definition_json
        || definition.instrument_id().to_string() != row.instrument_id
        || definition.effective_interval().starts_at().unix_nanos() != row.effective_start_ns
        || definition
            .effective_interval()
            .ends_at()
            .map(Timestamp::unix_nanos)
            != row.effective_end_ns
        || definition
            .reference_revision()
            .as_source_identifier()
            .as_str()
            != row.reference_revision
        || !digest_matches(
            definition.reference_payload_evidence().content_digest(),
            row.reference_algorithm,
            &row.reference_payload_digest,
        )
    {
        return Err(MarketDataInstrumentCatalogError::CorruptCatalog);
    }
    let revision_sequence = u32::try_from(row.revision_sequence)
        .ok()
        .filter(|sequence| (1..=MAX_REVISIONS_PER_INSTRUMENT).contains(sequence))
        .ok_or(MarketDataInstrumentCatalogError::CorruptCatalog)?;
    Ok(MarketDataInstrumentRecord {
        definition,
        revision_digest: digest(revision_digest),
        revision_sequence,
        published_at: Timestamp::from_unix_nanos(row.published_at_ns),
    })
}

fn charge_row(
    row: &StoredDefinitionRow,
    budget: &mut ResultBudget,
) -> Result<(), MarketDataInstrumentCatalogError> {
    budget
        .charge([
            size_of::<MarketDataInstrumentRecord>(),
            row.digest.len(),
            row.instrument_id.len(),
            row.reference_revision.len(),
            row.reference_payload_digest.len(),
            row.definition_json.len(),
        ])
        .map_err(|_| MarketDataInstrumentCatalogError::ResultByteLimitExceeded)
}

fn digest_matches(expected: EvidenceDigest, algorithm: i64, bytes: &[u8]) -> bool {
    algorithm_code(expected.algorithm()) == algorithm && expected.bytes().as_slice() == bytes
}

const fn algorithm_code(algorithm: DigestAlgorithm) -> i64 {
    match algorithm {
        DigestAlgorithm::Sha256 => 1,
        DigestAlgorithm::Blake3 => 2,
    }
}

fn batch_digest(prepared: &[PreparedDefinition]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(b"market-squawk/market-data-definition-batch/v2");
    hasher.update((prepared.len() as u64).to_be_bytes());
    for definition in prepared {
        hasher.update(definition.digest);
    }
    hasher.finalize().into()
}

fn population_query_digest(
    instrument_ids: &[InstrumentId],
    knowledge_at: Timestamp,
    effective_at: Timestamp,
) -> EvidenceDigest {
    let mut hasher = Sha256::new();
    hasher.update(POPULATION_QUERY_DOMAIN);
    hasher.update((instrument_ids.len() as u64).to_be_bytes());
    for instrument_id in instrument_ids {
        hasher.update(instrument_id.as_uuid().as_bytes());
    }
    hasher.update(knowledge_at.unix_nanos().to_be_bytes());
    hasher.update(effective_at.unix_nanos().to_be_bytes());
    digest(hasher.finalize().into())
}

fn population_receipt_digest(
    query_digest: EvidenceDigest,
    disposition: MarketDataInstrumentPopulationDisposition,
    records: &[MarketDataInstrumentRecord],
    exclusions: &[MarketDataInstrumentPopulationExclusion],
) -> EvidenceDigest {
    let mut hasher = Sha256::new();
    hasher.update(POPULATION_RECEIPT_DOMAIN);
    hash_evidence(&mut hasher, query_digest);
    hasher.update([match disposition {
        MarketDataInstrumentPopulationDisposition::Complete => 1,
        MarketDataInstrumentPopulationDisposition::Unavailable => 2,
    }]);
    hasher.update((records.len() as u64).to_be_bytes());
    for record in records {
        let definition = record.definition();
        hasher.update(definition.instrument_id().as_uuid().as_bytes());
        hash_evidence(&mut hasher, record.revision_digest());
        hasher.update(record.revision_sequence().to_be_bytes());
        hasher.update(record.published_at().unix_nanos().to_be_bytes());
        let interval = definition.effective_interval();
        hasher.update(interval.starts_at().unix_nanos().to_be_bytes());
        match interval.ends_at() {
            Some(end) => {
                hasher.update([1]);
                hasher.update(end.unix_nanos().to_be_bytes());
            }
            None => hasher.update([0]),
        }
    }
    hasher.update((exclusions.len() as u64).to_be_bytes());
    for exclusion in exclusions {
        hasher.update(exclusion.instrument_id.as_uuid().as_bytes());
        hasher.update([match exclusion.reason {
            MarketDataInstrumentPopulationExclusionReason::NoKnownRevision => 1,
            MarketDataInstrumentPopulationExclusionReason::NoEffectiveRevision => 2,
        }]);
    }
    digest(hasher.finalize().into())
}

fn provider_identity_query_digest(
    source_id: &SourceId,
    provider_instrument_id: &ProviderInstrumentId,
    knowledge_at: Timestamp,
    effective_at: Timestamp,
) -> EvidenceDigest {
    let mut hasher = Sha256::new();
    hasher.update(PROVIDER_IDENTITY_QUERY_DOMAIN);
    hash_text(&mut hasher, source_id.as_str());
    hash_text(&mut hasher, provider_instrument_id.as_str());
    hasher.update(knowledge_at.unix_nanos().to_be_bytes());
    hasher.update(effective_at.unix_nanos().to_be_bytes());
    digest(hasher.finalize().into())
}

fn provider_identity_resolution_digest(
    query_digest: EvidenceDigest,
    outcome: &MarketDataProviderIdentityResolutionOutcome,
    retained: &[MarketDataProviderIdentityExactReceipt],
    has_more: bool,
) -> EvidenceDigest {
    let mut hasher = Sha256::new();
    hasher.update(PROVIDER_IDENTITY_RECEIPT_DOMAIN);
    hash_evidence(&mut hasher, query_digest);
    hasher.update([match outcome {
        MarketDataProviderIdentityResolutionOutcome::Missing => 1,
        MarketDataProviderIdentityResolutionOutcome::Exact(_) => 2,
        MarketDataProviderIdentityResolutionOutcome::Ambiguous => 3,
    }]);
    hasher.update([u8::from(has_more)]);
    hasher.update((retained.len() as u64).to_be_bytes());
    for receipt in retained {
        hasher.update(receipt.instrument_id.as_uuid().as_bytes());
        hash_evidence(&mut hasher, receipt.definition_revision_digest);
        hasher.update(receipt.definition_revision_sequence.to_be_bytes());
        hash_text(
            &mut hasher,
            receipt
                .definition_reference_revision
                .as_source_identifier()
                .as_str(),
        );
        hash_evidence(&mut hasher, receipt.definition_reference_payload_digest);
        hasher.update(receipt.definition_published_at.unix_nanos().to_be_bytes());
        hash_text(
            &mut hasher,
            receipt
                .provider_identity_revision
                .as_source_identifier()
                .as_str(),
        );
        hash_evidence(&mut hasher, receipt.provider_identity_payload_digest);
        hash_interval(&mut hasher, receipt.provider_identity_validity);
        hasher.update((receipt.matching_venues.len() as u64).to_be_bytes());
        for venue in &receipt.matching_venues {
            hash_text(&mut hasher, venue.as_str());
        }
    }
    digest(hasher.finalize().into())
}

fn hash_interval(hasher: &mut Sha256, interval: EffectiveInterval) {
    hasher.update(interval.starts_at().unix_nanos().to_be_bytes());
    match interval.ends_at() {
        Some(end) => {
            hasher.update([1]);
            hasher.update(end.unix_nanos().to_be_bytes());
        }
        None => hasher.update([0]),
    }
}

fn hash_text(hasher: &mut Sha256, value: &str) {
    hasher.update((value.len() as u64).to_be_bytes());
    hasher.update(value.as_bytes());
}

fn hash_evidence(hasher: &mut Sha256, evidence: EvidenceDigest) {
    hasher.update([match evidence.algorithm() {
        DigestAlgorithm::Sha256 => 1,
        DigestAlgorithm::Blake3 => 2,
    }]);
    hasher.update(evidence.bytes());
}

const fn digest(bytes: [u8; 32]) -> EvidenceDigest {
    EvidenceDigest::new(DigestAlgorithm::Sha256, bytes)
}

fn normalize(value: &str) -> String {
    value.to_lowercase()
}

fn matched_identity_is_effective(
    definition: &MarketDataInstrumentDefinition,
    match_kind: MarketDataInstrumentMatchKind,
    matched_value: &str,
    effective_at: Timestamp,
) -> bool {
    if !interval_contains(definition.effective_interval(), effective_at) {
        return false;
    }
    match match_kind {
        MarketDataInstrumentMatchKind::ExternalIdentifier => {
            definition.identifiers().iter().any(|identifier| {
                identifier.assignment_verification() == AssignmentVerification::VerifiedAssigned
                    && normalize(&identifier.identifier().to_string()) == normalize(matched_value)
                    && interval_contains(identifier.validity(), effective_at)
            })
        }
        MarketDataInstrumentMatchKind::DisplayName => definition
            .display_name()
            .is_some_and(|name| normalize(name.as_str()) == normalize(matched_value)),
        MarketDataInstrumentMatchKind::VenueSymbol => definition
            .venue_mappings()
            .iter()
            .any(|mapping| normalize(mapping.venue_symbol().as_str()) == normalize(matched_value)),
        MarketDataInstrumentMatchKind::ProviderSymbol => {
            definition.provider_identities().iter().any(|identity| {
                normalize(identity.provider_instrument_id().as_str()) == normalize(matched_value)
                    && interval_contains(identity.validity(), effective_at)
            })
        }
    }
}

fn interval_contains(interval: market_squawk_domain::EffectiveInterval, at: Timestamp) -> bool {
    interval.starts_at() <= at && interval.ends_at().is_none_or(|end| at < end)
}

fn parse_match_kind(
    value: &str,
) -> Result<MarketDataInstrumentMatchKind, MarketDataInstrumentCatalogError> {
    match value {
        "external_identifier" => Ok(MarketDataInstrumentMatchKind::ExternalIdentifier),
        "display_name" => Ok(MarketDataInstrumentMatchKind::DisplayName),
        "venue_symbol" => Ok(MarketDataInstrumentMatchKind::VenueSymbol),
        "provider_symbol" => Ok(MarketDataInstrumentMatchKind::ProviderSymbol),
        _ => Err(MarketDataInstrumentCatalogError::CorruptCatalog),
    }
}

fn install_progress_handler(
    connection: &rusqlite::Connection,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<(), MarketDataInstrumentCatalogError> {
    let token = cancellation.clone();
    connection.progress_handler(
        SQLITE_PROGRESS_OPERATIONS,
        Some(move || token.is_cancelled() || Instant::now() >= deadline),
    )?;
    Ok(())
}

fn clear_progress_handler(
    connection: &rusqlite::Connection,
) -> Result<(), MarketDataInstrumentCatalogError> {
    connection.progress_handler::<fn() -> bool>(0, None)?;
    Ok(())
}

fn check_operation(
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<(), MarketDataInstrumentCatalogError> {
    if cancellation.is_cancelled() {
        Err(MarketDataInstrumentCatalogError::Cancelled)
    } else if Instant::now() >= deadline {
        Err(MarketDataInstrumentCatalogError::DeadlineExceeded)
    } else {
        Ok(())
    }
}

fn classify_operation<T>(
    result: Result<T, MarketDataInstrumentCatalogError>,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<T, MarketDataInstrumentCatalogError> {
    if cancellation.is_cancelled() {
        Err(MarketDataInstrumentCatalogError::Cancelled)
    } else if Instant::now() >= deadline {
        Err(MarketDataInstrumentCatalogError::DeadlineExceeded)
    } else {
        result
    }
}

const STORED_COLUMNS: &str = "revisions.revision_digest, revisions.instrument_id,
    revisions.revision_sequence, revisions.effective_start_ns, revisions.effective_end_ns,
    revisions.reference_revision, revisions.reference_algorithm,
    revisions.reference_payload_digest, revisions.definition_json, revisions.published_at_ns";

const POPULATION_AS_OF_SQL: &str = "
WITH selected_start AS (
    SELECT MAX(candidate.effective_start_ns) AS effective_start_ns
    FROM market_data_instrument_revisions AS candidate
    WHERE candidate.instrument_id=?1
      AND candidate.published_at_ns<=?2
      AND candidate.effective_start_ns<=?3
)
SELECT revisions.revision_digest, revisions.instrument_id, revisions.revision_sequence,
       revisions.effective_start_ns,
       revisions.effective_end_ns, revisions.reference_revision,
       revisions.reference_algorithm, revisions.reference_payload_digest, revisions.definition_json,
       revisions.published_at_ns
FROM market_data_instrument_revisions AS revisions
JOIN selected_start
  ON selected_start.effective_start_ns=revisions.effective_start_ns
WHERE revisions.instrument_id=?1
  AND revisions.published_at_ns<=?2
ORDER BY revisions.revision_digest
LIMIT 2";

const POPULATION_KNOWN_SQL: &str = "
SELECT EXISTS(
    SELECT 1
    FROM market_data_instrument_revisions AS revisions
    WHERE revisions.instrument_id=?1 AND revisions.published_at_ns<=?2
)";

const PROVIDER_IDENTITY_AS_OF_SQL: &str = "
WITH selectable_revisions AS (
    SELECT revisions.revision_digest, revisions.instrument_id, revisions.revision_sequence,
           revisions.effective_start_ns, revisions.effective_end_ns,
           revisions.reference_revision, revisions.reference_algorithm,
           revisions.reference_payload_digest, revisions.definition_json,
           revisions.published_at_ns,
           row_number() OVER (
               PARTITION BY revisions.instrument_id
               ORDER BY revisions.effective_start_ns DESC,
                        revisions.published_at_ns DESC,
                        revisions.revision_digest
           ) AS revision_position
    FROM market_data_instrument_revisions AS revisions
    WHERE revisions.published_at_ns<=?3
      AND revisions.effective_start_ns<=?4
)
SELECT revisions.revision_digest, revisions.instrument_id, revisions.revision_sequence,
       revisions.effective_start_ns, revisions.effective_end_ns,
       revisions.reference_revision, revisions.reference_algorithm,
       revisions.reference_payload_digest, revisions.definition_json,
       revisions.published_at_ns, terms.effective_start_ns, terms.effective_end_ns
FROM selectable_revisions AS revisions
JOIN market_data_instrument_search_terms AS terms
  ON terms.revision_digest=revisions.revision_digest
WHERE revisions.revision_position=1
  AND (revisions.effective_end_ns IS NULL OR ?4<revisions.effective_end_ns)
  AND terms.term_kind='provider_symbol'
  AND terms.source_id=?1
  AND terms.display_term=?2
  AND terms.effective_start_ns<=?4
  AND (terms.effective_end_ns IS NULL OR ?4<terms.effective_end_ns)
ORDER BY revisions.instrument_id
LIMIT ?5";

const SEARCH_SQL: &str = "
WITH matches AS (
    SELECT revisions.revision_digest, revisions.instrument_id, revisions.revision_sequence,
           revisions.effective_start_ns,
           revisions.effective_end_ns, revisions.reference_revision,
           revisions.reference_algorithm, revisions.reference_payload_digest, revisions.definition_json,
           revisions.published_at_ns, terms.term_kind, terms.display_term,
           CASE
             WHEN terms.normalized_term=?1 THEN
               CASE terms.term_kind WHEN 'external_identifier' THEN 0 WHEN 'venue_symbol' THEN 1
                    WHEN 'provider_symbol' THEN 2 ELSE 3 END
             WHEN instr(terms.normalized_term, ?1)=1 THEN
               10 + CASE terms.term_kind WHEN 'external_identifier' THEN 0 WHEN 'venue_symbol' THEN 1
                    WHEN 'provider_symbol' THEN 2 ELSE 3 END
             ELSE
               20 + CASE terms.term_kind WHEN 'external_identifier' THEN 0 WHEN 'venue_symbol' THEN 1
                    WHEN 'provider_symbol' THEN 2 ELSE 3 END
           END AS match_rank
    FROM market_data_instrument_current AS current_
    JOIN market_data_instrument_revisions AS revisions
      ON revisions.revision_digest=current_.revision_digest
    JOIN market_data_instrument_search_terms AS terms
      ON terms.revision_digest=current_.revision_digest
    WHERE instr(terms.normalized_term, ?1)>0
), ranked AS (
    SELECT matches.*,
           row_number() OVER (
               PARTITION BY instrument_id
               ORDER BY match_rank, term_kind, display_term
           ) AS match_position
    FROM matches
)
SELECT revision_digest, instrument_id, revision_sequence,
       effective_start_ns, effective_end_ns, reference_revision, reference_algorithm,
       reference_payload_digest, definition_json, published_at_ns,
       term_kind, display_term
FROM ranked
WHERE match_position=1
ORDER BY match_rank, instrument_id
LIMIT ?2";

const SEARCH_AS_OF_SQL: &str = "
WITH selectable_revisions AS (
    SELECT revisions.revision_digest, revisions.instrument_id, revisions.revision_sequence,
           revisions.effective_start_ns, revisions.effective_end_ns,
           revisions.reference_revision, revisions.reference_algorithm,
           revisions.reference_payload_digest, revisions.definition_json,
           revisions.published_at_ns,
           row_number() OVER (
               PARTITION BY revisions.instrument_id
               ORDER BY revisions.effective_start_ns DESC,
                        revisions.published_at_ns DESC,
                        revisions.revision_digest
           ) AS revision_position
    FROM market_data_instrument_revisions AS revisions
    WHERE revisions.published_at_ns<=?2
      AND revisions.effective_start_ns<=?3
), matches AS (
    SELECT revisions.revision_digest, revisions.instrument_id, revisions.revision_sequence,
           revisions.effective_start_ns, revisions.effective_end_ns,
           revisions.reference_revision, revisions.reference_algorithm,
           revisions.reference_payload_digest, revisions.definition_json,
           revisions.published_at_ns, terms.term_kind, terms.display_term,
           CASE
             WHEN terms.normalized_term=?1 THEN
               CASE terms.term_kind WHEN 'external_identifier' THEN 0 WHEN 'venue_symbol' THEN 1
                    WHEN 'provider_symbol' THEN 2 ELSE 3 END
             WHEN instr(terms.normalized_term, ?1)=1 THEN
               10 + CASE terms.term_kind WHEN 'external_identifier' THEN 0 WHEN 'venue_symbol' THEN 1
                    WHEN 'provider_symbol' THEN 2 ELSE 3 END
             ELSE
               20 + CASE terms.term_kind WHEN 'external_identifier' THEN 0 WHEN 'venue_symbol' THEN 1
                    WHEN 'provider_symbol' THEN 2 ELSE 3 END
           END AS match_rank
    FROM selectable_revisions AS revisions
    JOIN market_data_instrument_search_terms AS terms
      ON terms.revision_digest=revisions.revision_digest
    WHERE revisions.revision_position=1
      AND (revisions.effective_end_ns IS NULL OR ?3<revisions.effective_end_ns)
      AND terms.effective_start_ns<=?3
      AND (terms.effective_end_ns IS NULL OR ?3<terms.effective_end_ns)
      AND ((?4=0 AND instr(terms.normalized_term, ?1)>0)
           OR (?4=1 AND terms.normalized_term=?1))
), ranked AS (
    SELECT matches.*,
           row_number() OVER (
               PARTITION BY instrument_id
               ORDER BY match_rank, term_kind, display_term
           ) AS match_position
    FROM matches
)
SELECT revision_digest, instrument_id, revision_sequence,
       effective_start_ns, effective_end_ns, reference_revision, reference_algorithm,
       reference_payload_digest, definition_json, published_at_ns,
       term_kind, display_term
FROM ranked
WHERE match_position=1
ORDER BY match_rank, instrument_id
LIMIT ?5";
