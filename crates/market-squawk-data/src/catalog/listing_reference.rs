//! Immutable Nasdaq listing-reference generations and bounded current-directory discovery.
//!
//! This catalog is deliberately separate from the canonical instrument master. A directory row
//! proves only what the exact official reference file contained; it carries no quote, order-book,
//! trading-status, or execution authority.

mod canonical;
mod persistence;
mod read;

use std::fmt;
use std::mem::size_of;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use market_squawk_domain::{
    DataQuality, DigestAlgorithm, EvidenceDigest, ExactPayloadEvidence, SourceId, SourceIdentifier,
    Timestamp, VenueId,
};
use market_squawk_sources::{CoverageDomain, SourceMetadata};
use rusqlite::{OptionalExtension as _, params};
use sha2::{Digest as _, Sha256};
use thiserror::Error;
use tokio_util::sync::CancellationToken;

use super::CatalogAuthority;
use super::storage::{ResultBudget, now_timestamp, sha256};
use crate::RegisteredRightsGrant;

pub use persistence::ListingReferencePublicationDisposition;

/// Maximum rows accepted across the two official current-directory files.
pub const MAX_LISTING_REFERENCE_RECORDS: usize = 65_536;
/// Maximum rows returned by one interactive listing-reference search.
pub const MAX_LISTING_REFERENCE_SEARCH_ROWS: usize = 1_000;
/// Maximum listing memberships returned by one policy-facing discovery request.
///
/// A page reported as [`ListingReferenceMembershipPageState::Truncated`] is not a complete
/// opportunity universe. Callers must make a separately authorized pagination decision; this
/// catalog capability never follows a cursor automatically.
pub const MAX_LISTING_REFERENCE_MEMBERSHIP_PAGE_ROWS: usize = 16_384;
const MAX_FILE_RECORDS: usize = 32_768;
const MAX_RETAINED_INPUT_BYTES: usize = 64 * 1024 * 1024;
const MAX_SEARCH_QUERY_BYTES: usize = 256;
const SQLITE_PROGRESS_OPERATIONS: i32 = 1_000;
const MEMBERSHIP_SELECTION_RECEIPT_DOMAIN: &[u8] =
    b"market-squawk/listing-reference-membership-selection/v1";
const ORDERED_MEMBERSHIP_ROWS_DOMAIN: &[u8] = b"market-squawk/listing-reference-membership-rows/v1";

/// One exact official Nasdaq Trader directory file.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum ListingReferenceFileKind {
    /// `nasdaqlisted.txt`.
    NasdaqListed,
    /// `otherlisted.txt`.
    OtherListed,
}

/// Qualified meaning of a row's presence in an official current directory.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ListingReferenceDirectoryPresence {
    /// The row was present in the exact published current-directory generation. This is not a
    /// live trading-status claim.
    CurrentDirectory,
}

impl ListingReferenceFileKind {
    pub(super) const fn database_name(self) -> &'static str {
        match self {
            Self::NasdaqListed => "nasdaq_listed",
            Self::OtherListed => "other_listed",
        }
    }

    pub(super) fn from_database(value: &str) -> Result<Self, ListingReferenceError> {
        match value {
            "nasdaq_listed" => Ok(Self::NasdaqListed),
            "other_listed" => Ok(Self::OtherListed),
            _ => Err(ListingReferenceError::CorruptCatalog),
        }
    }
}

/// Nasdaq listing tier retained exactly from `nasdaqlisted.txt`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ListingReferenceMarketCategory {
    /// Nasdaq Global Select Market (`Q`).
    GlobalSelect,
    /// Nasdaq Global Market (`G`).
    GlobalMarket,
    /// Nasdaq Capital Market (`S`).
    CapitalMarket,
}

impl ListingReferenceMarketCategory {
    pub(super) const fn database_name(self) -> &'static str {
        match self {
            Self::GlobalSelect => "Q",
            Self::GlobalMarket => "G",
            Self::CapitalMarket => "S",
        }
    }

    pub(super) fn from_database(value: &str) -> Result<Self, ListingReferenceError> {
        match value {
            "Q" => Ok(Self::GlobalSelect),
            "G" => Ok(Self::GlobalMarket),
            "S" => Ok(Self::CapitalMarket),
            _ => Err(ListingReferenceError::CorruptCatalog),
        }
    }
}

/// Nasdaq financial-status code retained without converting it into trading status.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ListingReferenceFinancialStatus {
    Normal,
    Deficient,
    Delinquent,
    Bankrupt,
    DeficientAndBankrupt,
    DeficientAndDelinquent,
    DelinquentAndBankrupt,
    DeficientDelinquentAndBankrupt,
}

impl ListingReferenceFinancialStatus {
    pub(super) const fn database_name(self) -> &'static str {
        match self {
            Self::Normal => "N",
            Self::Deficient => "D",
            Self::Delinquent => "E",
            Self::Bankrupt => "Q",
            Self::DeficientAndBankrupt => "G",
            Self::DeficientAndDelinquent => "H",
            Self::DelinquentAndBankrupt => "J",
            Self::DeficientDelinquentAndBankrupt => "K",
        }
    }

    pub(super) fn from_database(value: &str) -> Result<Self, ListingReferenceError> {
        match value {
            "N" => Ok(Self::Normal),
            "D" => Ok(Self::Deficient),
            "E" => Ok(Self::Delinquent),
            "Q" => Ok(Self::Bankrupt),
            "G" => Ok(Self::DeficientAndBankrupt),
            "H" => Ok(Self::DeficientAndDelinquent),
            "J" => Ok(Self::DelinquentAndBankrupt),
            "K" => Ok(Self::DeficientDelinquentAndBankrupt),
            _ => Err(ListingReferenceError::CorruptCatalog),
        }
    }
}

/// Other-listing exchange code retained from `otherlisted.txt`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ListingReferenceExchangeCode {
    NyseAmerican,
    Nyse,
    NyseArca,
    NyseTexas,
    CboeBzx,
    Iex,
}

impl ListingReferenceExchangeCode {
    pub(super) const fn database_name(self) -> &'static str {
        match self {
            Self::NyseAmerican => "A",
            Self::Nyse => "N",
            Self::NyseArca => "P",
            Self::NyseTexas => "M",
            Self::CboeBzx => "Z",
            Self::Iex => "V",
        }
    }

    pub(super) fn expected_venue(self) -> &'static str {
        match self {
            Self::NyseAmerican => "XASE",
            Self::Nyse => "XNYS",
            Self::NyseArca => "ARCX",
            Self::NyseTexas => "XCHI",
            Self::CboeBzx => "BATS",
            Self::Iex => "IEXG",
        }
    }

    pub(super) fn from_database(value: &str) -> Result<Self, ListingReferenceError> {
        match value {
            "A" => Ok(Self::NyseAmerican),
            "N" => Ok(Self::Nyse),
            "P" => Ok(Self::NyseArca),
            "M" => Ok(Self::NyseTexas),
            "Z" => Ok(Self::CboeBzx),
            "V" => Ok(Self::Iex),
            _ => Err(ListingReferenceError::CorruptCatalog),
        }
    }
}

/// Exact source-file evidence required to publish one half of a complete directory generation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ListingReferenceSourceFileInput {
    kind: ListingReferenceFileKind,
    source_object_id: SourceIdentifier,
    source_reference: SourceIdentifier,
    file_creation_time: String,
    payload_evidence: ExactPayloadEvidence,
    source_last_modified_at: Timestamp,
    received_at: Timestamp,
    available_at: Timestamp,
}

impl ListingReferenceSourceFileInput {
    /// Constructs exact, bounded source-file provenance.
    #[allow(
        clippy::too_many_arguments,
        reason = "source-file evidence coordinates stay explicit"
    )]
    pub fn try_new(
        kind: ListingReferenceFileKind,
        source_object_id: SourceIdentifier,
        source_reference: SourceIdentifier,
        file_creation_time: impl Into<String>,
        payload_evidence: ExactPayloadEvidence,
        source_last_modified_at: Timestamp,
        received_at: Timestamp,
        available_at: Timestamp,
    ) -> Result<Self, ListingReferenceError> {
        let file_creation_time = file_creation_time.into();
        if !canonical::valid_file_creation_time(&file_creation_time)
            || payload_evidence.content_digest().bytes() == [0; 32]
            || source_last_modified_at > received_at
            || source_last_modified_at > available_at
            || available_at < received_at
        {
            return Err(ListingReferenceError::InvalidInput);
        }
        Ok(Self {
            kind,
            source_object_id,
            source_reference,
            file_creation_time,
            payload_evidence,
            source_last_modified_at,
            received_at,
            available_at,
        })
    }

    pub const fn kind(&self) -> ListingReferenceFileKind {
        self.kind
    }
    pub const fn source_object_id(&self) -> &SourceIdentifier {
        &self.source_object_id
    }
    pub const fn source_reference(&self) -> &SourceIdentifier {
        &self.source_reference
    }
    pub fn file_creation_time(&self) -> &str {
        &self.file_creation_time
    }
    pub const fn payload_evidence(&self) -> &ExactPayloadEvidence {
        &self.payload_evidence
    }
    pub const fn source_last_modified_at(&self) -> Timestamp {
        self.source_last_modified_at
    }
    pub const fn received_at(&self) -> Timestamp {
        self.received_at
    }
    pub const fn available_at(&self) -> Timestamp {
        self.available_at
    }
}

/// Exact provider values from one listing-directory row.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ListingReferenceRecordInput {
    provider_row_number: u32,
    provider_symbol: String,
    security_name: String,
    listing_venue: VenueId,
    exchange_code: Option<ListingReferenceExchangeCode>,
    cqs_symbol: Option<String>,
    nasdaq_symbol: Option<String>,
    market_category: Option<ListingReferenceMarketCategory>,
    financial_status: Option<ListingReferenceFinancialStatus>,
    is_etf: bool,
    is_test_issue: bool,
    round_lot_size: u32,
    is_next_shares: Option<bool>,
    record_revision: SourceIdentifier,
    record_payload_evidence: ExactPayloadEvidence,
    source_file_creation_time: String,
    source_last_modified_at: Timestamp,
    first_observed_at: Timestamp,
    source_file_payload_evidence: ExactPayloadEvidence,
}

impl ListingReferenceRecordInput {
    /// Constructs one exact `nasdaqlisted.txt` provider row.
    #[allow(
        clippy::too_many_arguments,
        reason = "the provider row and lineage remain explicit"
    )]
    pub fn try_nasdaq_listed(
        provider_row_number: u32,
        provider_symbol: impl Into<String>,
        security_name: impl Into<String>,
        listing_venue: VenueId,
        market_category: ListingReferenceMarketCategory,
        financial_status: ListingReferenceFinancialStatus,
        is_etf: bool,
        is_test_issue: bool,
        round_lot_size: u32,
        is_next_shares: bool,
        record_revision: SourceIdentifier,
        record_payload_evidence: ExactPayloadEvidence,
        source_file_creation_time: impl Into<String>,
        source_last_modified_at: Timestamp,
        first_observed_at: Timestamp,
        source_file_payload_evidence: ExactPayloadEvidence,
    ) -> Result<Self, ListingReferenceError> {
        let record = Self {
            provider_row_number,
            provider_symbol: provider_symbol.into(),
            security_name: security_name.into(),
            listing_venue,
            exchange_code: None,
            cqs_symbol: None,
            nasdaq_symbol: None,
            market_category: Some(market_category),
            financial_status: Some(financial_status),
            is_etf,
            is_test_issue,
            round_lot_size,
            is_next_shares: Some(is_next_shares),
            record_revision,
            record_payload_evidence,
            source_file_creation_time: source_file_creation_time.into(),
            source_last_modified_at,
            first_observed_at,
            source_file_payload_evidence,
        };
        canonical::validate_record(ListingReferenceFileKind::NasdaqListed, &record)?;
        Ok(record)
    }

    /// Constructs one exact `otherlisted.txt` provider row.
    #[allow(
        clippy::too_many_arguments,
        reason = "the provider row and lineage remain explicit"
    )]
    pub fn try_other_listed(
        provider_row_number: u32,
        provider_symbol: impl Into<String>,
        security_name: impl Into<String>,
        listing_venue: VenueId,
        exchange_code: ListingReferenceExchangeCode,
        cqs_symbol: impl Into<String>,
        nasdaq_symbol: impl Into<String>,
        is_etf: bool,
        is_test_issue: bool,
        round_lot_size: u32,
        record_revision: SourceIdentifier,
        record_payload_evidence: ExactPayloadEvidence,
        source_file_creation_time: impl Into<String>,
        source_last_modified_at: Timestamp,
        first_observed_at: Timestamp,
        source_file_payload_evidence: ExactPayloadEvidence,
    ) -> Result<Self, ListingReferenceError> {
        let record = Self {
            provider_row_number,
            provider_symbol: provider_symbol.into(),
            security_name: security_name.into(),
            listing_venue,
            exchange_code: Some(exchange_code),
            cqs_symbol: Some(cqs_symbol.into()),
            nasdaq_symbol: Some(nasdaq_symbol.into()),
            market_category: None,
            financial_status: None,
            is_etf,
            is_test_issue,
            round_lot_size,
            is_next_shares: None,
            record_revision,
            record_payload_evidence,
            source_file_creation_time: source_file_creation_time.into(),
            source_last_modified_at,
            first_observed_at,
            source_file_payload_evidence,
        };
        canonical::validate_record(ListingReferenceFileKind::OtherListed, &record)?;
        Ok(record)
    }

    pub const fn provider_row_number(&self) -> u32 {
        self.provider_row_number
    }
    pub fn provider_symbol(&self) -> &str {
        &self.provider_symbol
    }
    pub fn security_name(&self) -> &str {
        &self.security_name
    }
    pub const fn listing_venue(&self) -> &VenueId {
        &self.listing_venue
    }
    pub const fn record_revision(&self) -> &SourceIdentifier {
        &self.record_revision
    }
    pub const fn record_payload_evidence(&self) -> &ExactPayloadEvidence {
        &self.record_payload_evidence
    }
}

/// Complete two-file input for one exact current-directory generation.
#[derive(Clone, Debug)]
pub struct ListingReferenceGenerationInput {
    source: SourceMetadata,
    expected_previous_generation: Option<EvidenceDigest>,
    files: Box<[ListingReferenceSourceFileInput]>,
    records: Box<[(ListingReferenceFileKind, ListingReferenceRecordInput)]>,
}

impl ListingReferenceGenerationInput {
    /// Validates a complete, bounded generation before durable publication.
    pub fn try_new(
        source: SourceMetadata,
        expected_previous_generation: Option<EvidenceDigest>,
        files: Vec<ListingReferenceSourceFileInput>,
        records: Vec<(ListingReferenceFileKind, ListingReferenceRecordInput)>,
    ) -> Result<Self, ListingReferenceError> {
        if source.quality_ceiling() != DataQuality::OfficialDelayed
            || source.coverage().domain() != CoverageDomain::Instruments
            || source.capabilities().live()
            || !source.capabilities().extraction()
            || expected_previous_generation
                .is_some_and(|digest| digest.algorithm() != DigestAlgorithm::Sha256)
        {
            return Err(ListingReferenceError::InvalidSourceContract);
        }
        let mut files = files;
        let mut records = records;
        canonical::validate_and_order_generation(&files, &mut records)?;
        files.sort_by_key(|file| file.kind);
        canonical::enforce_retained_input_bound(&source, &files, &records)?;
        Ok(Self {
            source,
            expected_previous_generation,
            files: files.into_boxed_slice(),
            records: records.into_boxed_slice(),
        })
    }

    pub const fn source(&self) -> &SourceMetadata {
        &self.source
    }
    pub const fn expected_previous_generation(&self) -> Option<EvidenceDigest> {
        self.expected_previous_generation
    }
    pub fn files(&self) -> &[ListingReferenceSourceFileInput] {
        &self.files
    }
    pub fn records(&self) -> &[(ListingReferenceFileKind, ListingReferenceRecordInput)] {
        &self.records
    }

    /// Returns the canonical identity of the two exact provider files covered by publication
    /// rights. Local observation times and normalized records do not alter this source identity.
    pub fn source_payload_set_digest(&self) -> EvidenceDigest {
        EvidenceDigest::new(
            DigestAlgorithm::Sha256,
            canonical::source_payload_set_digest(&self.files),
        )
    }
}

/// Durable rights state retained with one published generation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ListingReferenceRightsState {
    /// Exact display and persistence rights were admitted when this generation was published.
    AdmittedScoped,
}

/// Immutable generation identity and authority evidence.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ListingReferenceGenerationReceipt {
    dataset: SourceIdentifier,
    generation_digest: EvidenceDigest,
    generation_sequence: u32,
    previous_generation_digest: Option<EvidenceDigest>,
    source_id: SourceId,
    source_revision: SourceIdentifier,
    source_revision_digest: EvidenceDigest,
    rights_id: [u8; 32],
    rights_state: ListingReferenceRightsState,
    record_count: usize,
    published_at: Timestamp,
}

impl ListingReferenceGenerationReceipt {
    pub const fn dataset(&self) -> &SourceIdentifier {
        &self.dataset
    }
    pub const fn generation_digest(&self) -> EvidenceDigest {
        self.generation_digest
    }
    pub const fn generation_sequence(&self) -> u32 {
        self.generation_sequence
    }
    pub const fn previous_generation_digest(&self) -> Option<EvidenceDigest> {
        self.previous_generation_digest
    }
    pub const fn source_id(&self) -> &SourceId {
        &self.source_id
    }
    pub const fn source_revision(&self) -> &SourceIdentifier {
        &self.source_revision
    }
    pub const fn source_revision_digest(&self) -> EvidenceDigest {
        self.source_revision_digest
    }
    pub const fn rights_id(&self) -> [u8; 32] {
        self.rights_id
    }
    pub const fn rights_state(&self) -> ListingReferenceRightsState {
        self.rights_state
    }
    pub const fn record_count(&self) -> usize {
        self.record_count
    }
    pub const fn published_at(&self) -> Timestamp {
        self.published_at
    }
}

/// Exact source-file provenance attached to a returned reference row.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ListingReferenceFileEvidence {
    kind: ListingReferenceFileKind,
    source_object_id: SourceIdentifier,
    source_reference: SourceIdentifier,
    file_creation_time: String,
    payload_evidence: ExactPayloadEvidence,
    source_last_modified_at: Timestamp,
    received_at: Timestamp,
    available_at: Timestamp,
    ingested_at: Timestamp,
    record_count: usize,
}

impl ListingReferenceFileEvidence {
    pub const fn kind(&self) -> ListingReferenceFileKind {
        self.kind
    }
    pub const fn source_object_id(&self) -> &SourceIdentifier {
        &self.source_object_id
    }
    pub const fn source_reference(&self) -> &SourceIdentifier {
        &self.source_reference
    }
    pub fn file_creation_time(&self) -> &str {
        &self.file_creation_time
    }
    pub const fn payload_evidence(&self) -> &ExactPayloadEvidence {
        &self.payload_evidence
    }
    pub const fn source_last_modified_at(&self) -> Timestamp {
        self.source_last_modified_at
    }
    pub const fn received_at(&self) -> Timestamp {
        self.received_at
    }
    pub const fn available_at(&self) -> Timestamp {
        self.available_at
    }
    pub const fn ingested_at(&self) -> Timestamp {
        self.ingested_at
    }
    pub const fn record_count(&self) -> usize {
        self.record_count
    }
}

/// One reference-only listing row from an exact immutable directory generation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ListingReferenceRecord {
    generation: ListingReferenceGenerationReceipt,
    source_file: ListingReferenceFileEvidence,
    provider_row_number: u32,
    provider_symbol: String,
    security_name: String,
    listing_venue: VenueId,
    exchange_code: Option<ListingReferenceExchangeCode>,
    cqs_symbol: Option<String>,
    nasdaq_symbol: Option<String>,
    market_category: Option<ListingReferenceMarketCategory>,
    financial_status: Option<ListingReferenceFinancialStatus>,
    is_etf: bool,
    is_test_issue: bool,
    round_lot_size: u32,
    is_next_shares: Option<bool>,
    record_revision: SourceIdentifier,
    record_payload_evidence: ExactPayloadEvidence,
}

impl ListingReferenceRecord {
    /// Returns the canonical listing-reference schema version.
    pub const fn schema_version(&self) -> u16 {
        1
    }
    pub const fn generation(&self) -> &ListingReferenceGenerationReceipt {
        &self.generation
    }
    pub const fn source_file(&self) -> &ListingReferenceFileEvidence {
        &self.source_file
    }
    pub const fn provider_row_number(&self) -> u32 {
        self.provider_row_number
    }
    pub fn provider_symbol(&self) -> &str {
        &self.provider_symbol
    }
    pub fn security_name(&self) -> &str {
        &self.security_name
    }
    pub fn display_name(&self) -> &str {
        self.security_name.trim()
    }
    pub const fn listing_venue(&self) -> &VenueId {
        &self.listing_venue
    }
    pub const fn exchange_code(&self) -> Option<ListingReferenceExchangeCode> {
        self.exchange_code
    }
    pub fn cqs_symbol(&self) -> Option<&str> {
        self.cqs_symbol.as_deref()
    }
    pub fn nasdaq_symbol(&self) -> Option<&str> {
        self.nasdaq_symbol.as_deref()
    }
    pub const fn market_category(&self) -> Option<ListingReferenceMarketCategory> {
        self.market_category
    }
    pub const fn financial_status(&self) -> Option<ListingReferenceFinancialStatus> {
        self.financial_status
    }
    pub const fn is_etf(&self) -> bool {
        self.is_etf
    }
    pub const fn is_test_issue(&self) -> bool {
        self.is_test_issue
    }
    pub const fn round_lot_size(&self) -> u32 {
        self.round_lot_size
    }
    pub const fn is_next_shares(&self) -> Option<bool> {
        self.is_next_shares
    }
    pub const fn quality(&self) -> DataQuality {
        DataQuality::OfficialDelayed
    }
    /// Returns why the record is included without claiming that it is currently tradable.
    pub const fn directory_presence(&self) -> ListingReferenceDirectoryPresence {
        ListingReferenceDirectoryPresence::CurrentDirectory
    }
    /// Returns the exact source-file timestamp used as this reference row's effective time.
    pub const fn effective_at(&self) -> Timestamp {
        self.source_file.source_last_modified_at
    }
    /// Returns the exact source-file timestamp used as this reference row's publication time.
    pub const fn published_at(&self) -> Timestamp {
        self.source_file.source_last_modified_at
    }
    /// Row membership describes the provider's current-directory state within the selected
    /// generation. Generation succession is retained on [`ListingReferenceGenerationReceipt`]
    /// rather than projected as a synthetic row-level supersession timestamp.
    pub const fn superseded_at(&self) -> Option<Timestamp> {
        None
    }
    pub const fn record_revision(&self) -> &SourceIdentifier {
        &self.record_revision
    }
    pub const fn record_payload_evidence(&self) -> &ExactPayloadEvidence {
        &self.record_payload_evidence
    }
}

/// Field responsible for one deterministic search match.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ListingReferenceMatchKind {
    ProviderSymbol,
    SecurityName,
    CqsSymbol,
    NasdaqSymbol,
}

/// One current reference row and its strongest match reason.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ListingReferenceSearchMatch {
    record: ListingReferenceRecord,
    match_kind: ListingReferenceMatchKind,
}

impl ListingReferenceSearchMatch {
    pub const fn record(&self) -> &ListingReferenceRecord {
        &self.record
    }
    pub const fn match_kind(&self) -> ListingReferenceMatchKind {
        self.match_kind
    }
}

/// Deterministic bounded current-directory search result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ListingReferenceSearchPage {
    matches: Box<[ListingReferenceSearchMatch]>,
    has_more: bool,
}

impl ListingReferenceSearchPage {
    pub fn matches(&self) -> &[ListingReferenceSearchMatch] {
        &self.matches
    }
    pub const fn has_more(&self) -> bool {
        self.has_more
    }
}

/// Generation-selection policy for deterministic listing-membership discovery.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ListingReferenceGenerationSelection {
    /// Selects the latest generation known when the catalog begins the read.
    Current,
    /// Selects the latest generation published no later than the inclusive knowledge cutoff.
    AsOf(Timestamp),
}

/// Opaque continuation position minted from one exact immutable generation.
///
/// A cursor cannot cross a generation boundary. It contains only official directory coordinates
/// and does not contain, derive, or imply a FIGI or canonical instrument identity.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ListingReferenceMembershipCursor {
    generation_digest: EvidenceDigest,
    file_kind: ListingReferenceFileKind,
    provider_row_number: u32,
    provider_symbol: String,
}

impl ListingReferenceMembershipCursor {
    pub const fn generation_digest(&self) -> EvidenceDigest {
        self.generation_digest
    }

    pub const fn file_kind(&self) -> ListingReferenceFileKind {
        self.file_kind
    }

    pub const fn provider_row_number(&self) -> u32 {
        self.provider_row_number
    }

    pub fn provider_symbol(&self) -> &str {
        &self.provider_symbol
    }
}

/// Completeness state for one bounded membership page.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ListingReferenceMembershipPageState {
    /// Every row after the requested cursor in the selected generation was returned.
    Complete,
    /// More rows exist, so this page must not be treated as a complete policy universe.
    Truncated,
}

/// Tamper-evident receipt for one exact, bounded membership selection.
///
/// The receipt binds the dataset and source, current/as-of query, requested knowledge cutoff,
/// selected immutable generation and its publication time, incoming cursor, requested limit,
/// ordered verified membership rows, completeness state, and the rights/source-revision evidence
/// checked at read time.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ListingReferenceMembershipSelectionReceipt {
    dataset: SourceIdentifier,
    source_id: SourceId,
    selection: ListingReferenceGenerationSelection,
    requested_knowledge_at: Timestamp,
    authorization_checked_at: Timestamp,
    selected_generation_digest: Option<EvidenceDigest>,
    selected_generation_published_at: Option<Timestamp>,
    rights_id: Option<[u8; 32]>,
    source_revision_digest: Option<EvidenceDigest>,
    requested_cursor: Option<ListingReferenceMembershipCursor>,
    maximum_rows: usize,
    returned_rows: usize,
    state: ListingReferenceMembershipPageState,
    ordered_rows_digest: EvidenceDigest,
    receipt_digest: EvidenceDigest,
}

impl ListingReferenceMembershipSelectionReceipt {
    pub const fn dataset(&self) -> &SourceIdentifier {
        &self.dataset
    }

    pub const fn source_id(&self) -> &SourceId {
        &self.source_id
    }

    pub const fn selection(&self) -> ListingReferenceGenerationSelection {
        self.selection
    }

    pub const fn requested_knowledge_at(&self) -> Timestamp {
        self.requested_knowledge_at
    }

    pub const fn authorization_checked_at(&self) -> Timestamp {
        self.authorization_checked_at
    }

    pub const fn selected_generation_digest(&self) -> Option<EvidenceDigest> {
        self.selected_generation_digest
    }

    pub const fn selected_generation_published_at(&self) -> Option<Timestamp> {
        self.selected_generation_published_at
    }

    pub const fn rights_id(&self) -> Option<[u8; 32]> {
        self.rights_id
    }

    pub const fn source_revision_digest(&self) -> Option<EvidenceDigest> {
        self.source_revision_digest
    }

    pub const fn requested_cursor(&self) -> Option<&ListingReferenceMembershipCursor> {
        self.requested_cursor.as_ref()
    }

    pub const fn maximum_rows(&self) -> usize {
        self.maximum_rows
    }

    pub const fn returned_rows(&self) -> usize {
        self.returned_rows
    }

    pub const fn state(&self) -> ListingReferenceMembershipPageState {
        self.state
    }

    pub const fn ordered_rows_digest(&self) -> EvidenceDigest {
        self.ordered_rows_digest
    }

    pub const fn receipt_digest(&self) -> EvidenceDigest {
        self.receipt_digest
    }
}

/// Deterministic, bounded membership page from one immutable official-directory generation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ListingReferenceMembershipPage {
    generation: Option<ListingReferenceGenerationReceipt>,
    records: Box<[ListingReferenceRecord]>,
    state: ListingReferenceMembershipPageState,
    next_cursor: Option<ListingReferenceMembershipCursor>,
    receipt: ListingReferenceMembershipSelectionReceipt,
}

impl ListingReferenceMembershipPage {
    /// Returns `None` only for an empty complete selection before any admitted generation existed.
    pub const fn generation(&self) -> Option<&ListingReferenceGenerationReceipt> {
        self.generation.as_ref()
    }

    pub fn records(&self) -> &[ListingReferenceRecord] {
        &self.records
    }

    pub const fn state(&self) -> ListingReferenceMembershipPageState {
        self.state
    }

    /// Returns a cursor only when [`Self::state`] is
    /// [`ListingReferenceMembershipPageState::Truncated`]. Following it is an explicit caller
    /// action and is never performed automatically by this capability.
    pub const fn next_cursor(&self) -> Option<&ListingReferenceMembershipCursor> {
        self.next_cursor.as_ref()
    }

    pub const fn receipt(&self) -> &ListingReferenceMembershipSelectionReceipt {
        &self.receipt
    }
}

/// Receipt for an inserted or exact-replayed current generation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ListingReferencePublicationReceipt {
    disposition: ListingReferencePublicationDisposition,
    generation: ListingReferenceGenerationReceipt,
}

impl ListingReferencePublicationReceipt {
    pub const fn disposition(&self) -> ListingReferencePublicationDisposition {
        self.disposition
    }
    pub const fn generation(&self) -> &ListingReferenceGenerationReceipt {
        &self.generation
    }
}

/// Cloneable least-authority publisher bound to one dataset, source, and admitted rights grant.
#[derive(Clone)]
pub struct ListingReferencePublicationCapability {
    authority: Arc<Mutex<CatalogAuthority>>,
    dataset: SourceIdentifier,
    source_id: SourceId,
    rights: RegisteredRightsGrant,
}

impl fmt::Debug for ListingReferencePublicationCapability {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ListingReferencePublicationCapability")
            .field("dataset", &self.dataset)
            .field("source_id", &self.source_id)
            .field(
                "authority",
                &"[SEALED LISTING-REFERENCE PUBLICATION AUTHORITY]",
            )
            .finish()
    }
}

impl ListingReferencePublicationCapability {
    /// Binds publication to the sole catalog session and one already admitted grant.
    pub fn try_new(
        authority: Arc<Mutex<CatalogAuthority>>,
        dataset: SourceIdentifier,
        source_id: SourceId,
        rights: RegisteredRightsGrant,
    ) -> Result<Self, ListingReferenceError> {
        let session_matches = authority
            .try_lock()
            .map_err(|_| ListingReferenceError::AuthorityUnavailable)?
            .session_id()
            == rights.catalog_id;
        if !session_matches {
            return Err(ListingReferenceError::InvalidRightsCapability);
        }
        Ok(Self {
            authority,
            dataset,
            source_id,
            rights,
        })
    }

    /// Atomically publishes or reconciles one complete official directory generation.
    pub fn publish(
        &self,
        input: ListingReferenceGenerationInput,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<ListingReferencePublicationReceipt, ListingReferenceError> {
        if input.source().source_id() != &self.source_id {
            return Err(ListingReferenceError::InvalidSourceContract);
        }
        canonical::check_operation(deadline, cancellation)?;
        self.authority
            .try_lock()
            .map_err(|_| ListingReferenceError::AuthorityUnavailable)?
            .publish_listing_reference_generation(
                &self.dataset,
                &self.source_id,
                &self.rights,
                input,
                deadline,
                cancellation,
            )
    }
}

/// Cloneable least-authority reader bound to one dataset and source.
#[derive(Clone)]
pub struct ListingReferenceReadCapability {
    authority: Arc<Mutex<CatalogAuthority>>,
    dataset: SourceIdentifier,
    source_id: SourceId,
}

impl fmt::Debug for ListingReferenceReadCapability {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ListingReferenceReadCapability")
            .field("dataset", &self.dataset)
            .field("source_id", &self.source_id)
            .field("authority", &"[SEALED LISTING-REFERENCE READ AUTHORITY]")
            .finish()
    }
}

impl ListingReferenceReadCapability {
    /// Binds bounded reference reads to one catalog, dataset, and source.
    pub fn new(
        authority: Arc<Mutex<CatalogAuthority>>,
        dataset: SourceIdentifier,
        source_id: SourceId,
    ) -> Self {
        Self {
            authority,
            dataset,
            source_id,
        }
    }

    /// Returns the current immutable generation, if one exists and remains display-authorized.
    pub fn current(
        &self,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<Option<ListingReferenceGenerationReceipt>, ListingReferenceError> {
        canonical::check_operation(deadline, cancellation)?;
        self.authority
            .try_lock()
            .map_err(|_| ListingReferenceError::AuthorityUnavailable)?
            .current_listing_reference_generation(
                &self.dataset,
                &self.source_id,
                deadline,
                cancellation,
            )
    }

    /// Searches the current official directory without creating tradable instruments.
    pub fn search(
        &self,
        query: &str,
        maximum_rows: usize,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<ListingReferenceSearchPage, ListingReferenceError> {
        if maximum_rows == 0 || maximum_rows > MAX_LISTING_REFERENCE_SEARCH_ROWS {
            return Err(ListingReferenceError::InvalidLimit);
        }
        let query = query.trim();
        if query.is_empty()
            || query.len() > MAX_SEARCH_QUERY_BYTES
            || query.chars().any(char::is_control)
        {
            return Err(ListingReferenceError::InvalidInput);
        }
        canonical::check_operation(deadline, cancellation)?;
        self.authority
            .try_lock()
            .map_err(|_| ListingReferenceError::AuthorityUnavailable)?
            .search_listing_references(
                &self.dataset,
                &self.source_id,
                query,
                maximum_rows,
                deadline,
                cancellation,
            )
    }

    /// Enumerates one immutable current or point-in-time generation in canonical row order.
    ///
    /// The caller receives at most [`MAX_LISTING_REFERENCE_MEMBERSHIP_PAGE_ROWS`] records. A
    /// truncated page is explicit and must never be treated as a complete opportunity universe.
    /// This operation validates display rights and the exact source revision at read time; it
    /// does not create or infer canonical instruments or FIGIs.
    pub fn memberships(
        &self,
        selection: ListingReferenceGenerationSelection,
        after: Option<&ListingReferenceMembershipCursor>,
        maximum_rows: usize,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<ListingReferenceMembershipPage, ListingReferenceError> {
        if maximum_rows == 0 || maximum_rows > MAX_LISTING_REFERENCE_MEMBERSHIP_PAGE_ROWS {
            return Err(ListingReferenceError::InvalidLimit);
        }
        canonical::check_operation(deadline, cancellation)?;
        let authority = self
            .authority
            .try_lock()
            .map_err(|_| ListingReferenceError::AuthorityUnavailable)?;
        read_listing_reference_memberships(
            &authority,
            &self.dataset,
            &self.source_id,
            selection,
            after,
            maximum_rows,
            deadline,
            cancellation,
        )
    }
}

#[allow(
    clippy::too_many_arguments,
    reason = "bounded read coordinates and authority evidence stay explicit"
)]
fn read_listing_reference_memberships(
    authority: &CatalogAuthority,
    dataset: &SourceIdentifier,
    source_id: &SourceId,
    selection: ListingReferenceGenerationSelection,
    after: Option<&ListingReferenceMembershipCursor>,
    maximum_rows: usize,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<ListingReferenceMembershipPage, ListingReferenceError> {
    canonical::check_operation(deadline, cancellation)?;
    let connection = &authority.catalog().connection;
    let authorization_checked_at =
        now_timestamp().map_err(|_| ListingReferenceError::CorruptCatalog)?;
    let durable_clock: i64 = connection.query_row(
        "SELECT last_timestamp_ns FROM catalog_authority_clock WHERE singleton=1",
        [],
        |row| row.get(0),
    )?;
    if authorization_checked_at.unix_nanos() < durable_clock {
        return Err(ListingReferenceError::CorruptCatalog);
    }
    let requested_knowledge_at = match selection {
        ListingReferenceGenerationSelection::Current => authorization_checked_at,
        ListingReferenceGenerationSelection::AsOf(knowledge_at) => {
            if knowledge_at > authorization_checked_at {
                return Err(ListingReferenceError::InvalidKnowledgeCutoff);
            }
            knowledge_at
        }
    };
    let generation_digest: Option<Vec<u8>> = connection
        .query_row(
            "SELECT generation_digest FROM listing_reference_generations
             WHERE dataset_id=?1 AND published_at_ns<=?2
             ORDER BY published_at_ns DESC, generation_sequence DESC LIMIT 1",
            params![dataset.as_str(), requested_knowledge_at.unix_nanos(),],
            |row| row.get(0),
        )
        .optional()?;
    let Some(generation_digest) = generation_digest else {
        if after.is_some() {
            return Err(ListingReferenceError::PositionConflict);
        }
        let receipt = build_membership_selection_receipt(
            dataset,
            source_id,
            selection,
            requested_knowledge_at,
            authorization_checked_at,
            None,
            None,
            maximum_rows,
            ListingReferenceMembershipPageState::Complete,
            &[],
        )?;
        return Ok(ListingReferenceMembershipPage {
            generation: None,
            records: Box::new([]),
            state: ListingReferenceMembershipPageState::Complete,
            next_cursor: None,
            receipt,
        });
    };
    let generation_digest: [u8; 32] = generation_digest
        .try_into()
        .map_err(|_| ListingReferenceError::CorruptCatalog)?;
    let generation = persistence::load_generation_receipt(connection, generation_digest)?
        .ok_or(ListingReferenceError::CorruptCatalog)?;
    if generation.dataset() != dataset
        || generation.source_id() != source_id
        || generation.published_at() > requested_knowledge_at
    {
        return Err(ListingReferenceError::CorruptCatalog);
    }
    require_membership_read_authority(connection, &generation, authorization_checked_at)?;
    if let Some(cursor) = after {
        if cursor.generation_digest != generation.generation_digest() {
            return Err(ListingReferenceError::PositionConflict);
        }
        require_exact_membership_cursor(connection, cursor)?;
    }
    canonical::check_operation(deadline, cancellation)?;

    let token = cancellation.clone();
    connection.progress_handler(
        SQLITE_PROGRESS_OPERATIONS,
        Some(move || token.is_cancelled() || Instant::now() >= deadline),
    )?;
    let result = (|| {
        let retrieval_limit = maximum_rows
            .checked_add(1)
            .and_then(|value| i64::try_from(value).ok())
            .ok_or(ListingReferenceError::InvalidLimit)?;
        let (cursor_kind, cursor_row, cursor_symbol) = after.map_or((None, None, None), |cursor| {
            (
                Some(cursor.file_kind.database_name()),
                Some(i64::from(cursor.provider_row_number)),
                Some(cursor.provider_symbol.as_str()),
            )
        });
        let mut statement = connection.prepare(LISTING_REFERENCE_MEMBERSHIP_PAGE_SQL)?;
        let rows = statement.query_map(
            params![
                generation_digest,
                cursor_kind,
                cursor_row,
                cursor_symbol,
                retrieval_limit,
            ],
            decode_membership_row,
        )?;
        let mut budget = ResultBudget::new(authority.catalog().result_bytes);
        let retained_capacity = maximum_rows
            .checked_add(1)
            .ok_or(ListingReferenceError::InvalidLimit)?;
        let mut records = Vec::new();
        records
            .try_reserve_exact(retained_capacity)
            .map_err(|_| ListingReferenceError::MemoryLimitExceeded)?;
        let mut row_digests = Vec::new();
        row_digests
            .try_reserve_exact(retained_capacity)
            .map_err(|_| ListingReferenceError::MemoryLimitExceeded)?;
        for stored in rows {
            canonical::check_operation(deadline, cancellation)?;
            let stored = stored?;
            charge_membership_row_budget(&mut budget, &generation, &stored)?;
            let (record, row_digest) = rebuild_membership_record(&generation, stored)?;
            records.push(record);
            row_digests.push(row_digest);
        }
        let state = if records.len() > maximum_rows {
            records.truncate(maximum_rows);
            row_digests.truncate(maximum_rows);
            ListingReferenceMembershipPageState::Truncated
        } else {
            ListingReferenceMembershipPageState::Complete
        };
        let next_cursor = if state == ListingReferenceMembershipPageState::Truncated {
            records
                .last()
                .map(|record| ListingReferenceMembershipCursor {
                    generation_digest: generation.generation_digest(),
                    file_kind: record.source_file.kind,
                    provider_row_number: record.provider_row_number,
                    provider_symbol: record.provider_symbol.clone(),
                })
        } else {
            None
        };
        let receipt = build_membership_selection_receipt(
            dataset,
            source_id,
            selection,
            requested_knowledge_at,
            authorization_checked_at,
            Some(&generation),
            after,
            maximum_rows,
            state,
            &row_digests,
        )?;
        Ok(ListingReferenceMembershipPage {
            generation: Some(generation),
            records: records.into_boxed_slice(),
            state,
            next_cursor,
            receipt,
        })
    })();
    connection.progress_handler::<fn() -> bool>(0, None)?;
    classify_membership_operation(result, deadline, cancellation)
}

fn require_membership_read_authority(
    connection: &rusqlite::Connection,
    generation: &ListingReferenceGenerationReceipt,
    checked_at: Timestamp,
) -> Result<(), ListingReferenceError> {
    let authorized: bool = connection.query_row(
        "SELECT EXISTS(
             SELECT 1 FROM source_rights
             WHERE rights_id=?1 AND source_id=?2
               AND (operation_mask & 2)=2
               AND admitted_at_ns<=?3
               AND (authorization_expires_at_ns IS NULL OR authorization_expires_at_ns>?3)
         )",
        params![
            generation.rights_id(),
            generation.source_id().as_str(),
            checked_at.unix_nanos(),
        ],
        |row| row.get(0),
    )?;
    if !authorized {
        return Err(ListingReferenceError::RightsUnavailable);
    }
    let metadata_json: Option<String> = connection
        .query_row(
            "SELECT metadata_json FROM source_revisions
             WHERE source_id=?1 AND revision_digest=?2",
            params![
                generation.source_id().as_str(),
                generation.source_revision_digest().bytes(),
            ],
            |row| row.get(0),
        )
        .optional()?;
    let metadata_json = metadata_json.ok_or(ListingReferenceError::CorruptCatalog)?;
    if sha256(metadata_json.as_bytes()) != generation.source_revision_digest().bytes() {
        return Err(ListingReferenceError::CorruptCatalog);
    }
    let metadata: SourceMetadata =
        serde_json::from_str(&metadata_json).map_err(|_| ListingReferenceError::CorruptCatalog)?;
    if metadata.source_id() != generation.source_id()
        || metadata.revision().as_source_identifier() != generation.source_revision()
        || !metadata.is_effective_at(generation.published_at())
        || !metadata.is_effective_at(checked_at)
    {
        return Err(ListingReferenceError::RightsUnavailable);
    }
    Ok(())
}

fn require_exact_membership_cursor(
    connection: &rusqlite::Connection,
    cursor: &ListingReferenceMembershipCursor,
) -> Result<(), ListingReferenceError> {
    let exists: bool = connection.query_row(
        "SELECT EXISTS(
             SELECT 1 FROM listing_reference_memberships
             WHERE generation_digest=?1 AND file_kind=?2
               AND provider_row_number=?3 AND provider_symbol=?4
         )",
        params![
            cursor.generation_digest.bytes(),
            cursor.file_kind.database_name(),
            i64::from(cursor.provider_row_number),
            cursor.provider_symbol,
        ],
        |row| row.get(0),
    )?;
    if exists {
        Ok(())
    } else {
        Err(ListingReferenceError::PositionConflict)
    }
}

fn classify_membership_operation<T>(
    result: Result<T, ListingReferenceError>,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<T, ListingReferenceError> {
    if cancellation.is_cancelled() {
        Err(ListingReferenceError::Cancelled)
    } else if Instant::now() >= deadline {
        Err(ListingReferenceError::DeadlineExceeded)
    } else {
        result
    }
}

#[derive(Debug)]
struct StoredListingMembershipRow {
    file_kind: String,
    source_object_id: String,
    source_reference: String,
    file_creation_time: String,
    file_algorithm: i64,
    file_payload_digest: Vec<u8>,
    file_locator_reference: Option<String>,
    file_locator_version: Option<String>,
    source_last_modified_at: i64,
    received_at: i64,
    available_at: i64,
    ingested_at: i64,
    file_record_count: i64,
    provider_row_number: i64,
    provider_symbol: String,
    security_name: String,
    listing_venue: String,
    exchange_code: Option<String>,
    cqs_symbol: Option<String>,
    nasdaq_symbol: Option<String>,
    market_category: Option<String>,
    financial_status: Option<String>,
    is_etf: i64,
    is_test_issue: i64,
    round_lot_size: i64,
    is_next_shares: Option<i64>,
    directory_presence: String,
    data_quality: String,
    authority_class: String,
    record_revision: String,
    record_algorithm: i64,
    record_payload_digest: Vec<u8>,
    record_locator_reference: Option<String>,
    record_locator_version: Option<String>,
    value_digest: Vec<u8>,
    record_digest: Vec<u8>,
}

fn decode_membership_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<StoredListingMembershipRow> {
    Ok(StoredListingMembershipRow {
        file_kind: row.get(0)?,
        source_object_id: row.get(1)?,
        source_reference: row.get(2)?,
        file_creation_time: row.get(3)?,
        file_algorithm: row.get(4)?,
        file_payload_digest: row.get(5)?,
        file_locator_reference: row.get(6)?,
        file_locator_version: row.get(7)?,
        source_last_modified_at: row.get(8)?,
        received_at: row.get(9)?,
        available_at: row.get(10)?,
        ingested_at: row.get(11)?,
        file_record_count: row.get(12)?,
        provider_row_number: row.get(13)?,
        provider_symbol: row.get(14)?,
        security_name: row.get(15)?,
        listing_venue: row.get(16)?,
        exchange_code: row.get(17)?,
        cqs_symbol: row.get(18)?,
        nasdaq_symbol: row.get(19)?,
        market_category: row.get(20)?,
        financial_status: row.get(21)?,
        is_etf: row.get(22)?,
        is_test_issue: row.get(23)?,
        round_lot_size: row.get(24)?,
        is_next_shares: row.get(25)?,
        directory_presence: row.get(26)?,
        data_quality: row.get(27)?,
        authority_class: row.get(28)?,
        record_revision: row.get(29)?,
        record_algorithm: row.get(30)?,
        record_payload_digest: row.get(31)?,
        record_locator_reference: row.get(32)?,
        record_locator_version: row.get(33)?,
        value_digest: row.get(34)?,
        record_digest: row.get(35)?,
    })
}

fn charge_membership_row_budget(
    budget: &mut ResultBudget,
    generation: &ListingReferenceGenerationReceipt,
    row: &StoredListingMembershipRow,
) -> Result<(), ListingReferenceError> {
    budget
        .charge([
            size_of::<ListingReferenceRecord>(),
            generation.dataset.as_str().len(),
            generation.source_id.as_str().len(),
            generation.source_revision.as_str().len(),
            row.file_kind.len(),
            row.source_object_id.len(),
            row.source_reference.len(),
            row.file_creation_time.len(),
            row.file_locator_reference.as_ref().map_or(0, String::len),
            row.file_locator_version.as_ref().map_or(0, String::len),
            row.provider_symbol.len(),
            row.security_name.len(),
            row.listing_venue.len(),
            row.exchange_code.as_ref().map_or(0, String::len),
            row.cqs_symbol.as_ref().map_or(0, String::len),
            row.nasdaq_symbol.as_ref().map_or(0, String::len),
            row.market_category.as_ref().map_or(0, String::len),
            row.financial_status.as_ref().map_or(0, String::len),
            row.directory_presence.len(),
            row.data_quality.len(),
            row.authority_class.len(),
            row.record_revision.len(),
            row.record_locator_reference.as_ref().map_or(0, String::len),
            row.record_locator_version.as_ref().map_or(0, String::len),
        ])
        .map_err(|_| ListingReferenceError::MemoryLimitExceeded)
}

fn rebuild_membership_record(
    generation: &ListingReferenceGenerationReceipt,
    row: StoredListingMembershipRow,
) -> Result<(ListingReferenceRecord, [u8; 32]), ListingReferenceError> {
    let kind = ListingReferenceFileKind::from_database(&row.file_kind)?;
    let file_payload_evidence = persistence::exact_evidence(
        row.file_algorithm,
        row.file_payload_digest,
        row.file_locator_reference,
        row.file_locator_version,
    )?;
    let record_payload_evidence = persistence::exact_evidence(
        row.record_algorithm,
        row.record_payload_digest,
        row.record_locator_reference,
        row.record_locator_version,
    )?;
    let venue =
        VenueId::try_from(row.listing_venue).map_err(|_| ListingReferenceError::CorruptCatalog)?;
    let provider_row_number = u32::try_from(row.provider_row_number)
        .map_err(|_| ListingReferenceError::CorruptCatalog)?;
    let round_lot_size =
        u32::try_from(row.round_lot_size).map_err(|_| ListingReferenceError::CorruptCatalog)?;
    let record_revision = SourceIdentifier::try_from(row.record_revision)
        .map_err(|_| ListingReferenceError::CorruptCatalog)?;
    let is_etf = parse_membership_bool(row.is_etf)?;
    let is_test_issue = parse_membership_bool(row.is_test_issue)?;
    let record_input = match kind {
        ListingReferenceFileKind::NasdaqListed => ListingReferenceRecordInput::try_nasdaq_listed(
            provider_row_number,
            row.provider_symbol.clone(),
            row.security_name.clone(),
            venue.clone(),
            ListingReferenceMarketCategory::from_database(
                row.market_category
                    .as_deref()
                    .ok_or(ListingReferenceError::CorruptCatalog)?,
            )?,
            ListingReferenceFinancialStatus::from_database(
                row.financial_status
                    .as_deref()
                    .ok_or(ListingReferenceError::CorruptCatalog)?,
            )?,
            is_etf,
            is_test_issue,
            round_lot_size,
            parse_membership_optional_bool(row.is_next_shares)?
                .ok_or(ListingReferenceError::CorruptCatalog)?,
            record_revision.clone(),
            record_payload_evidence.clone(),
            row.file_creation_time.clone(),
            Timestamp::from_unix_nanos(row.source_last_modified_at),
            Timestamp::from_unix_nanos(row.received_at),
            file_payload_evidence.clone(),
        )?,
        ListingReferenceFileKind::OtherListed => ListingReferenceRecordInput::try_other_listed(
            provider_row_number,
            row.provider_symbol.clone(),
            row.security_name.clone(),
            venue.clone(),
            ListingReferenceExchangeCode::from_database(
                row.exchange_code
                    .as_deref()
                    .ok_or(ListingReferenceError::CorruptCatalog)?,
            )?,
            row.cqs_symbol
                .clone()
                .ok_or(ListingReferenceError::CorruptCatalog)?,
            row.nasdaq_symbol
                .clone()
                .ok_or(ListingReferenceError::CorruptCatalog)?,
            is_etf,
            is_test_issue,
            round_lot_size,
            record_revision.clone(),
            record_payload_evidence.clone(),
            row.file_creation_time.clone(),
            Timestamp::from_unix_nanos(row.source_last_modified_at),
            Timestamp::from_unix_nanos(row.received_at),
            file_payload_evidence.clone(),
        )?,
    };
    let value_digest: [u8; 32] = row
        .value_digest
        .try_into()
        .map_err(|_| ListingReferenceError::CorruptCatalog)?;
    let record_digest: [u8; 32] = row
        .record_digest
        .try_into()
        .map_err(|_| ListingReferenceError::CorruptCatalog)?;
    if canonical::value_digest(&record_input) != value_digest
        || canonical::record_digest(kind, &record_input, value_digest) != record_digest
        || row.directory_presence != "current_directory"
        || row.data_quality != "official_delayed"
        || row.authority_class != "reference_only"
        || row.available_at != row.received_at
        || row.ingested_at < row.received_at
        || row.ingested_at < row.available_at
    {
        return Err(ListingReferenceError::CorruptCatalog);
    }
    let source_file = ListingReferenceFileEvidence {
        kind,
        source_object_id: SourceIdentifier::try_from(row.source_object_id)
            .map_err(|_| ListingReferenceError::CorruptCatalog)?,
        source_reference: SourceIdentifier::try_from(row.source_reference)
            .map_err(|_| ListingReferenceError::CorruptCatalog)?,
        file_creation_time: row.file_creation_time,
        payload_evidence: file_payload_evidence,
        source_last_modified_at: Timestamp::from_unix_nanos(row.source_last_modified_at),
        received_at: Timestamp::from_unix_nanos(row.received_at),
        available_at: Timestamp::from_unix_nanos(row.available_at),
        ingested_at: Timestamp::from_unix_nanos(row.ingested_at),
        record_count: usize::try_from(row.file_record_count)
            .map_err(|_| ListingReferenceError::CorruptCatalog)?,
    };
    Ok((
        ListingReferenceRecord {
            generation: generation.clone(),
            source_file,
            provider_row_number: record_input.provider_row_number,
            provider_symbol: record_input.provider_symbol,
            security_name: record_input.security_name,
            listing_venue: record_input.listing_venue,
            exchange_code: record_input.exchange_code,
            cqs_symbol: record_input.cqs_symbol,
            nasdaq_symbol: record_input.nasdaq_symbol,
            market_category: record_input.market_category,
            financial_status: record_input.financial_status,
            is_etf: record_input.is_etf,
            is_test_issue: record_input.is_test_issue,
            round_lot_size: record_input.round_lot_size,
            is_next_shares: record_input.is_next_shares,
            record_revision: record_input.record_revision,
            record_payload_evidence,
        },
        record_digest,
    ))
}

fn parse_membership_bool(value: i64) -> Result<bool, ListingReferenceError> {
    match value {
        0 => Ok(false),
        1 => Ok(true),
        _ => Err(ListingReferenceError::CorruptCatalog),
    }
}

fn parse_membership_optional_bool(
    value: Option<i64>,
) -> Result<Option<bool>, ListingReferenceError> {
    value.map(parse_membership_bool).transpose()
}

#[allow(
    clippy::too_many_arguments,
    reason = "the exact query, PIT, authority, and ordered-selection coordinates stay explicit"
)]
fn build_membership_selection_receipt(
    dataset: &SourceIdentifier,
    source_id: &SourceId,
    selection: ListingReferenceGenerationSelection,
    requested_knowledge_at: Timestamp,
    authorization_checked_at: Timestamp,
    generation: Option<&ListingReferenceGenerationReceipt>,
    requested_cursor: Option<&ListingReferenceMembershipCursor>,
    maximum_rows: usize,
    state: ListingReferenceMembershipPageState,
    ordered_row_digests: &[[u8; 32]],
) -> Result<ListingReferenceMembershipSelectionReceipt, ListingReferenceError> {
    let returned_rows = ordered_row_digests.len();
    if returned_rows > maximum_rows
        || (state == ListingReferenceMembershipPageState::Truncated
            && returned_rows != maximum_rows)
        || (generation.is_none()
            && (returned_rows != 0 || state != ListingReferenceMembershipPageState::Complete))
    {
        return Err(ListingReferenceError::CorruptCatalog);
    }
    let ordered_rows_digest = ordered_membership_rows_digest(ordered_row_digests);
    let mut hash = Sha256::new();
    hash_receipt_field(&mut hash, b"domain", MEMBERSHIP_SELECTION_RECEIPT_DOMAIN);
    hash_receipt_field(&mut hash, b"dataset", dataset.as_str().as_bytes());
    hash_receipt_field(&mut hash, b"source_id", source_id.as_str().as_bytes());
    match selection {
        ListingReferenceGenerationSelection::Current => {
            hash_receipt_field(&mut hash, b"selection", b"current");
        }
        ListingReferenceGenerationSelection::AsOf(knowledge_at) => {
            hash_receipt_field(&mut hash, b"selection", b"as_of");
            hash_receipt_field(
                &mut hash,
                b"selection_as_of",
                &knowledge_at.unix_nanos().to_be_bytes(),
            );
        }
    }
    hash_receipt_field(
        &mut hash,
        b"requested_knowledge_at",
        &requested_knowledge_at.unix_nanos().to_be_bytes(),
    );
    hash_receipt_field(
        &mut hash,
        b"authorization_checked_at",
        &authorization_checked_at.unix_nanos().to_be_bytes(),
    );
    if let Some(generation) = generation {
        hash_receipt_field(&mut hash, b"generation_present", &[1]);
        hash_evidence_digest(
            &mut hash,
            b"generation_digest",
            generation.generation_digest(),
        );
        hash_receipt_field(
            &mut hash,
            b"generation_published_at",
            &generation.published_at().unix_nanos().to_be_bytes(),
        );
        hash_receipt_field(&mut hash, b"rights_id", &generation.rights_id());
        hash_evidence_digest(
            &mut hash,
            b"source_revision_digest",
            generation.source_revision_digest(),
        );
    } else {
        hash_receipt_field(&mut hash, b"generation_present", &[0]);
    }
    if let Some(cursor) = requested_cursor {
        hash_receipt_field(&mut hash, b"cursor_present", &[1]);
        hash_evidence_digest(
            &mut hash,
            b"cursor_generation_digest",
            cursor.generation_digest,
        );
        hash_receipt_field(
            &mut hash,
            b"cursor_file_kind",
            cursor.file_kind.database_name().as_bytes(),
        );
        hash_receipt_field(
            &mut hash,
            b"cursor_provider_row_number",
            &cursor.provider_row_number.to_be_bytes(),
        );
        hash_receipt_field(
            &mut hash,
            b"cursor_provider_symbol",
            cursor.provider_symbol.as_bytes(),
        );
    } else {
        hash_receipt_field(&mut hash, b"cursor_present", &[0]);
    }
    hash_receipt_field(
        &mut hash,
        b"maximum_rows",
        &(maximum_rows as u64).to_be_bytes(),
    );
    hash_receipt_field(
        &mut hash,
        b"returned_rows",
        &(returned_rows as u64).to_be_bytes(),
    );
    hash_receipt_field(
        &mut hash,
        b"state",
        match state {
            ListingReferenceMembershipPageState::Complete => b"complete",
            ListingReferenceMembershipPageState::Truncated => b"truncated",
        },
    );
    hash_evidence_digest(&mut hash, b"ordered_rows_digest", ordered_rows_digest);
    let receipt_digest = EvidenceDigest::new(DigestAlgorithm::Sha256, hash.finalize().into());
    Ok(ListingReferenceMembershipSelectionReceipt {
        dataset: dataset.clone(),
        source_id: source_id.clone(),
        selection,
        requested_knowledge_at,
        authorization_checked_at,
        selected_generation_digest: generation.map(|value| value.generation_digest()),
        selected_generation_published_at: generation.map(|value| value.published_at()),
        rights_id: generation.map(|value| value.rights_id()),
        source_revision_digest: generation.map(|value| value.source_revision_digest()),
        requested_cursor: requested_cursor.cloned(),
        maximum_rows,
        returned_rows,
        state,
        ordered_rows_digest,
        receipt_digest,
    })
}

fn ordered_membership_rows_digest(row_digests: &[[u8; 32]]) -> EvidenceDigest {
    let mut hash = Sha256::new();
    hash_receipt_field(&mut hash, b"domain", ORDERED_MEMBERSHIP_ROWS_DOMAIN);
    hash_receipt_field(
        &mut hash,
        b"row_count",
        &(row_digests.len() as u64).to_be_bytes(),
    );
    for row_digest in row_digests {
        hash_receipt_field(&mut hash, b"row", row_digest);
    }
    EvidenceDigest::new(DigestAlgorithm::Sha256, hash.finalize().into())
}

fn hash_evidence_digest(hash: &mut Sha256, tag: &[u8], digest: EvidenceDigest) {
    hash_receipt_field(
        hash,
        tag,
        &[match digest.algorithm() {
            DigestAlgorithm::Sha256 => 1,
            DigestAlgorithm::Blake3 => 2,
        }],
    );
    hash_receipt_field(hash, b"digest_bytes", &digest.bytes());
}

fn hash_receipt_field(hash: &mut Sha256, tag: &[u8], value: &[u8]) {
    hash.update((tag.len() as u64).to_be_bytes());
    hash.update(tag);
    hash.update((value.len() as u64).to_be_bytes());
    hash.update(value);
}

const LISTING_REFERENCE_MEMBERSHIP_PAGE_SQL: &str = r#"
SELECT files.file_kind,
       files.source_object_id,
       files.source_reference,
       files.file_creation_time,
       files.payload_algorithm,
       files.payload_digest,
       files.payload_locator_reference,
       files.payload_locator_version,
       files.source_last_modified_at_ns,
       files.received_at_ns,
       files.available_at_ns,
       files.ingested_at_ns,
       files.record_count,
       memberships.provider_row_number,
       values_.provider_symbol,
       values_.security_name,
       values_.listing_venue,
       values_.exchange_code,
       values_.cqs_symbol,
       values_.nasdaq_symbol,
       values_.market_category,
       values_.financial_status,
       values_.is_etf,
       values_.is_test_issue,
       values_.round_lot_size,
       values_.is_next_shares,
       values_.directory_presence,
       values_.data_quality,
       values_.authority_class,
       memberships.record_revision,
       memberships.record_algorithm,
       memberships.record_payload_digest,
       memberships.record_locator_reference,
       memberships.record_locator_version,
       memberships.value_digest,
       memberships.record_digest
FROM listing_reference_memberships AS memberships
JOIN listing_reference_values AS values_ ON values_.value_digest=memberships.value_digest
JOIN listing_reference_files AS files
  ON files.generation_digest=memberships.generation_digest
 AND files.file_kind=memberships.file_kind
WHERE memberships.generation_digest=?1
  AND (
      ?2 IS NULL
      OR memberships.file_kind > ?2
      OR (
          memberships.file_kind = ?2
          AND memberships.provider_row_number > ?3
      )
      OR (
          memberships.file_kind = ?2
          AND memberships.provider_row_number = ?3
          AND memberships.provider_symbol > ?4
      )
  )
ORDER BY memberships.file_kind, memberships.provider_row_number,
         memberships.provider_symbol
LIMIT ?5
"#;

/// Listing-reference validation, publication, or bounded-read failure.
#[derive(Debug, Error)]
pub enum ListingReferenceError {
    #[error("listing-reference input is invalid")]
    InvalidInput,
    #[error("listing-reference source metadata does not authorize this reference contract")]
    InvalidSourceContract,
    #[error("listing-reference rights capability belongs to another catalog session")]
    InvalidRightsCapability,
    #[error("listing-reference display or persistence rights are unavailable")]
    RightsUnavailable,
    #[error("listing-reference source metadata revision is not registered exactly")]
    SourceRevisionUnavailable,
    #[error("listing-reference generation position changed")]
    PositionConflict,
    #[error("listing-reference input names a superseded generation")]
    SupersededGeneration,
    #[error("listing-reference authority is busy or poisoned")]
    AuthorityUnavailable,
    #[error("listing-reference operation was cancelled")]
    Cancelled,
    #[error("listing-reference operation deadline elapsed")]
    DeadlineExceeded,
    #[error("listing-reference knowledge cutoff is later than the catalog read time")]
    InvalidKnowledgeCutoff,
    #[error("listing-reference result limit is invalid")]
    InvalidLimit,
    #[error("listing-reference retained-memory bound was exceeded")]
    MemoryLimitExceeded,
    #[error("listing-reference catalog content is corrupt")]
    CorruptCatalog,
    #[error("listing-reference storage operation failed")]
    Storage(#[from] rusqlite::Error),
    #[error("listing-reference source metadata serialization failed")]
    Serialization(#[from] serde_json::Error),
}
