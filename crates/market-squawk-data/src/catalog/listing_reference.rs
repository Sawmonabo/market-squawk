//! Immutable Nasdaq listing-reference generations and bounded current-directory discovery.
//!
//! This catalog is deliberately separate from the canonical instrument master. A directory row
//! proves only what the exact official reference file contained; it carries no quote, order-book,
//! trading-status, or execution authority.

mod canonical;
mod persistence;
mod read;

use std::fmt;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use market_squawk_domain::{
    DataQuality, DigestAlgorithm, EvidenceDigest, ExactPayloadEvidence, SourceId, SourceIdentifier,
    Timestamp, VenueId,
};
use market_squawk_sources::{CoverageDomain, SourceMetadata};
use thiserror::Error;
use tokio_util::sync::CancellationToken;

use super::CatalogAuthority;
use crate::RegisteredRightsGrant;

pub use persistence::ListingReferencePublicationDisposition;

/// Maximum rows accepted across the two official current-directory files.
pub const MAX_LISTING_REFERENCE_RECORDS: usize = 65_536;
/// Maximum rows returned by one interactive listing-reference search.
pub const MAX_LISTING_REFERENCE_SEARCH_ROWS: usize = 1_000;
const MAX_FILE_RECORDS: usize = 32_768;
const MAX_RETAINED_INPUT_BYTES: usize = 64 * 1024 * 1024;
const MAX_SEARCH_QUERY_BYTES: usize = 256;

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

/// One current reference-only listing row with its complete retained provenance.
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
    /// Current-directory reads never return superseded rows.
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
}

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
