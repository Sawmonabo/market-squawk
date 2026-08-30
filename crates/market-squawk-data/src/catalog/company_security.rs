//! Immutable company/security relationship authority and point-in-time selection.

use std::cmp::Ordering;
use std::fmt;
use std::mem::size_of;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use market_squawk_domain::{
    AssignmentVerification, CommonEquitySuitability, CompanyIdentityObservation,
    CompanyIdentitySurface, CompanySecurityIdentityLink, CompanySecurityKind,
    CompanySecurityLinkTransition, CompanySecurityRelationshipKind, CompanySecurityResolutionBasis,
    DigestAlgorithm, EvidenceDigest, ExternalIdentifier, IdentifierEntitlement, InstrumentId,
    MarketDataInstrumentDefinition, SourceId, SourceIdentifier, Timestamp,
};
use rusqlite::{Connection, OptionalExtension as _, Row, Transaction, params};
use sha2::{Digest as _, Sha256};
use thiserror::Error;
use tokio_util::sync::CancellationToken;

use super::CatalogAuthority;
use super::storage::{ResultBudget, append_audit, trusted_catalog_now};

/// Maximum current relationship keys evaluated by one selection.
pub const MAX_COMPANY_SECURITY_SELECTION_ROWS: usize = 256;
const MAX_LINK_EVENTS_PER_RELATIONSHIP: i64 = 16_384;
const MAX_INSTRUMENT_RELATIONSHIP_HISTORY_ROWS: usize = 16_384;
const MAX_INDUSTRY_COHORT_SCAN_ROWS: usize = 1_024;
const MAX_INDUSTRY_COHORT_MEMBERS: usize = 256;
const SQLITE_PROGRESS_OPERATIONS: i32 = 1_000;
const COMPANY_SECURITY_SELECTION_RECEIPT_DOMAIN: &[u8] =
    b"market-squawk/company-security-selection-receipt/v3\0";
const SEC_FUNDAMENTAL_IDENTITY_QUERY_DOMAIN: &[u8] =
    b"market-squawk/sec-fundamental-identity-query/v1\0";
const SEC_FUNDAMENTAL_IDENTITY_RECEIPT_DOMAIN: &[u8] =
    b"market-squawk/sec-fundamental-identity-receipt/v1\0";
const INSTRUMENT_COMPANY_SELECTION_RECEIPT_DOMAIN: &[u8] =
    b"market-squawk/instrument-company-identity-selection-receipt/v3\0";
const INDUSTRY_CLASSIFICATION_QUERY_DOMAIN: &[u8] =
    b"market-squawk/industry-classification-query/v1\0";
const INDUSTRY_CLASSIFICATION_SELECTION_RECEIPT_DOMAIN: &[u8] =
    b"market-squawk/industry-classification-selection-receipt/v1\0";
const INDUSTRY_COHORT_QUERY_DOMAIN: &[u8] = b"market-squawk/industry-cohort-query/v1\0";
const INDUSTRY_COHORT_SELECTION_RECEIPT_DOMAIN: &[u8] =
    b"market-squawk/industry-cohort-selection-receipt/v1\0";

/// Exact company query with an optional exact security restriction.
///
/// Names, tickers, and exchanges are absent by type and cannot select relationship authority.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CompanySecurityIdentityQuery {
    company_source_id: SourceId,
    provider_company_id: SourceIdentifier,
    company_surface: CompanyIdentitySurface,
    instrument_id: Option<InstrumentId>,
    require_suitable_common_equity: bool,
}

impl CompanySecurityIdentityQuery {
    /// Constructs a source-qualified company query.
    pub const fn new(
        company_source_id: SourceId,
        provider_company_id: SourceIdentifier,
        company_surface: CompanyIdentitySurface,
        instrument_id: Option<InstrumentId>,
        require_suitable_common_equity: bool,
    ) -> Self {
        Self {
            company_source_id,
            provider_company_id,
            company_surface,
            instrument_id,
            require_suitable_common_equity,
        }
    }

    /// Returns the exact company source.
    pub const fn company_source_id(&self) -> &SourceId {
        &self.company_source_id
    }
    /// Returns the provider's company identity.
    pub const fn provider_company_id(&self) -> &SourceIdentifier {
        &self.provider_company_id
    }
    /// Returns the company-observation surface.
    pub const fn company_surface(&self) -> CompanyIdentitySurface {
        self.company_surface
    }
    /// Returns an exact security restriction when supplied.
    pub const fn instrument_id(&self) -> Option<InstrumentId> {
        self.instrument_id
    }
    /// Returns whether only direct issuer common equity may qualify.
    pub const fn require_suitable_common_equity(&self) -> bool {
        self.require_suitable_common_equity
    }
}

/// Exact point-in-time SEC company generation requiring one tradable common-equity identity.
///
/// The query deliberately contains no ticker, company name, exchange, or fuzzy lookup field.
/// `effective_at` answers when the security relationship must apply, while `knowledge_at` is the
/// latest information the caller permits the selector to know.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SecFundamentalIdentityQuery {
    company_source_id: SourceId,
    cik: SourceIdentifier,
    company_surface: CompanyIdentitySurface,
    company_observation_digest: EvidenceDigest,
    effective_at: Timestamp,
    knowledge_at: Timestamp,
}

impl SecFundamentalIdentityQuery {
    /// Constructs an exact SEC company-generation query.
    pub fn try_new(
        company_source_id: SourceId,
        cik: SourceIdentifier,
        company_surface: CompanyIdentitySurface,
        company_observation_digest: EvidenceDigest,
        effective_at: Timestamp,
        knowledge_at: Timestamp,
    ) -> Result<Self, CompanySecurityIdentityCatalogError> {
        if !valid_sec_cik(cik.as_str())
            || company_observation_digest.algorithm() != DigestAlgorithm::Sha256
            || company_observation_digest.bytes() == [0; 32]
        {
            return Err(CompanySecurityIdentityCatalogError::InvalidInput);
        }
        Ok(Self {
            company_source_id,
            cik,
            company_surface,
            company_observation_digest,
            effective_at,
            knowledge_at,
        })
    }

    /// Returns the exact SEC source namespace.
    pub const fn company_source_id(&self) -> &SourceId {
        &self.company_source_id
    }
    /// Returns the normalized ten-digit SEC CIK identity.
    pub const fn cik(&self) -> &SourceIdentifier {
        &self.cik
    }
    /// Returns the exact SEC company surface.
    pub const fn company_surface(&self) -> CompanyIdentitySurface {
        self.company_surface
    }
    /// Returns the exact immutable company-observation generation.
    pub const fn company_observation_digest(&self) -> EvidenceDigest {
        self.company_observation_digest
    }
    /// Returns the economic instant at which the identity must apply.
    pub const fn effective_at(&self) -> Timestamp {
        self.effective_at
    }
    /// Returns the latest local/source knowledge the selector may use.
    pub const fn knowledge_at(&self) -> Timestamp {
        self.knowledge_at
    }
}

/// Application-facing SEC fundamental identity state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SecFundamentalIdentityAvailability {
    /// One exact company generation maps to one authoritative tradable security generation.
    Available,
    /// The company is known, but authoritative security identity is not yet usable.
    IdentityPending,
    /// Multiple identities or an ambiguous company parent prevent safe selection.
    Conflict,
    /// The exact company/security relationship is absent, ended, revoked, or unsuitable.
    Unavailable,
}

/// Bounded SEC fundamental identity result with restart-verifiable generation evidence.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SecFundamentalIdentitySelection {
    availability: SecFundamentalIdentityAvailability,
    company_observation_digest: EvidenceDigest,
    instrument_id: Option<InstrumentId>,
    market_instrument_revision_digest: Option<EvidenceDigest>,
    relationship: Option<CompanySecurityIdentityRecord>,
    relationship_selection: CompanySecurityIdentitySelection,
    query_digest: EvidenceDigest,
    receipt_digest: EvidenceDigest,
}

impl SecFundamentalIdentitySelection {
    /// Returns the closed application-facing identity state.
    pub const fn availability(&self) -> SecFundamentalIdentityAvailability {
        self.availability
    }
    /// Returns the exact requested company generation in every state.
    pub const fn company_observation_digest(&self) -> EvidenceDigest {
        self.company_observation_digest
    }
    /// Returns the stable tradable security only when identity is available.
    pub const fn instrument_id(&self) -> Option<InstrumentId> {
        self.instrument_id
    }
    /// Returns the exact immutable market-reference generation only when available.
    pub const fn market_instrument_revision_digest(&self) -> Option<EvidenceDigest> {
        self.market_instrument_revision_digest
    }
    /// Returns the exact authoritative relationship event only when available.
    pub const fn relationship(&self) -> Option<&CompanySecurityIdentityRecord> {
        self.relationship.as_ref()
    }
    /// Returns the complete bounded candidate/exclusion evidence considered.
    pub const fn relationship_selection(&self) -> &CompanySecurityIdentitySelection {
        &self.relationship_selection
    }
    /// Returns the digest of the exact CIK/company/effective/knowledge query.
    pub const fn query_digest(&self) -> EvidenceDigest {
        self.query_digest
    }
    /// Returns the restart-verifiable digest binding both immutable parent generations.
    pub const fn receipt_digest(&self) -> EvidenceDigest {
        self.receipt_digest
    }
}

/// One exact, digest-verified immutable relationship event.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CompanySecurityIdentityRecord {
    link: CompanySecurityIdentityLink,
    link_digest: EvidenceDigest,
    event_sequence: u32,
    published_at: Timestamp,
}

impl CompanySecurityIdentityRecord {
    /// Returns the complete relationship event.
    pub const fn link(&self) -> &CompanySecurityIdentityLink {
        &self.link
    }
    /// Returns the SHA-256 identity of its canonical JSON.
    pub const fn link_digest(&self) -> EvidenceDigest {
        self.link_digest
    }
    /// Returns the one-based immutable event position for this relationship key.
    pub const fn event_sequence(&self) -> u32 {
        self.event_sequence
    }
    /// Returns first durable local publication time.
    pub const fn published_at(&self) -> Timestamp {
        self.published_at
    }
}

/// Closed relationship-selection outcome.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CompanySecurityIdentityDisposition {
    /// Exactly one relationship satisfies the query.
    Complete,
    /// No relationship is currently usable.
    Unavailable,
    /// More than one relationship qualifies or a current company parent is ambiguous.
    Conflict,
    /// A retained relationship names a superseded company or market parent.
    Stale,
    /// The latest applicable relationship event explicitly revoked the link.
    Revoked,
}

/// Why one latest relationship event did not qualify.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CompanySecurityIdentityExclusionReason {
    NotYetAvailable,
    NotYetEffective,
    NoLongerEffective,
    NotSuitableCommonEquity,
    AmbiguousCompanyParent,
    StaleCompanyParent,
    StaleMarketInstrumentParent,
    Revoked,
}

/// One excluded current/as-of relationship event.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CompanySecurityIdentityExclusion {
    record: CompanySecurityIdentityRecord,
    reason: CompanySecurityIdentityExclusionReason,
}

impl CompanySecurityIdentityExclusion {
    /// Returns the excluded exact event.
    pub const fn record(&self) -> &CompanySecurityIdentityRecord {
        &self.record
    }
    /// Returns the fail-closed exclusion reason.
    pub const fn reason(&self) -> CompanySecurityIdentityExclusionReason {
        self.reason
    }
}

/// Exact coordinates bound into one ordered receipt entry.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CompanySecuritySelectionReceiptEntry {
    link_digest: EvidenceDigest,
    event_sequence: u32,
    previous_link_digest: Option<EvidenceDigest>,
    company_source_id: SourceId,
    provider_company_id: SourceIdentifier,
    company_surface: CompanyIdentitySurface,
    linked_company_observation_digest: EvidenceDigest,
    current_company_observation_digest: Option<EvidenceDigest>,
    current_company_available_at: Option<Timestamp>,
    current_company_ingested_at: Option<Timestamp>,
    current_company_completed_at: Option<Timestamp>,
    linked_market_revision_digest: EvidenceDigest,
    instrument_id: InstrumentId,
    security_kind: CompanySecurityKind,
    relationship_kind: CompanySecurityRelationshipKind,
    common_equity_suitability: CommonEquitySuitability,
    current_market_revision_digest: Option<EvidenceDigest>,
    current_market_published_at: Option<Timestamp>,
    current_market_effective_start: Option<Timestamp>,
    current_market_effective_end: Option<Timestamp>,
    company_available_at: Option<Timestamp>,
    company_ingested_at: Timestamp,
    company_completed_at: Timestamp,
    market_published_at: Timestamp,
    market_effective_start: Timestamp,
    market_effective_end: Option<Timestamp>,
    link_available_at: Timestamp,
    link_ingested_at: Timestamp,
    link_published_at: Timestamp,
    effective_start: Timestamp,
    effective_end: Option<Timestamp>,
    rights_policy_id: SourceIdentifier,
    rights_entitlement: IdentifierEntitlement,
    rights_terms_reference: SourceIdentifier,
}

impl CompanySecuritySelectionReceiptEntry {
    /// Returns the immutable link identity.
    pub const fn link_digest(&self) -> EvidenceDigest {
        self.link_digest
    }
    /// Returns the immutable one-based relationship revision.
    pub const fn event_sequence(&self) -> u32 {
        self.event_sequence
    }
    /// Returns the exact predecessor named by a successor or revocation event.
    pub const fn previous_link_digest(&self) -> Option<EvidenceDigest> {
        self.previous_link_digest
    }
    /// Returns the exact company source considered by the selection.
    pub const fn company_source_id(&self) -> &SourceId {
        &self.company_source_id
    }
    /// Returns the exact provider company identity considered by the selection.
    pub const fn provider_company_id(&self) -> &SourceIdentifier {
        &self.provider_company_id
    }
    /// Returns the exact company-observation surface considered by the selection.
    pub const fn company_surface(&self) -> CompanyIdentitySurface {
        self.company_surface
    }
    /// Returns the exact company parent named by the link.
    pub const fn linked_company_observation_digest(&self) -> EvidenceDigest {
        self.linked_company_observation_digest
    }
    /// Returns the company parent current at the knowledge cutoff.
    pub const fn current_company_observation_digest(&self) -> Option<EvidenceDigest> {
        self.current_company_observation_digest
    }
    /// Returns conservative availability of the company parent current at the cutoff.
    pub const fn current_company_available_at(&self) -> Option<Timestamp> {
        self.current_company_available_at
    }
    /// Returns ingestion of the company parent current at the cutoff.
    pub const fn current_company_ingested_at(&self) -> Option<Timestamp> {
        self.current_company_ingested_at
    }
    /// Returns completion of the company parent current at the cutoff.
    pub const fn current_company_completed_at(&self) -> Option<Timestamp> {
        self.current_company_completed_at
    }
    /// Returns the exact market-definition parent named by the link.
    pub const fn linked_market_revision_digest(&self) -> EvidenceDigest {
        self.linked_market_revision_digest
    }
    /// Returns the exact stable instrument identity considered by the selection.
    pub const fn instrument_id(&self) -> InstrumentId {
        self.instrument_id
    }
    /// Returns the evidenced security form.
    pub const fn security_kind(&self) -> CompanySecurityKind {
        self.security_kind
    }
    /// Returns the evidenced company/security relationship.
    pub const fn relationship_kind(&self) -> CompanySecurityRelationshipKind {
        self.relationship_kind
    }
    /// Returns the relationship's explicit common-equity suitability.
    pub const fn common_equity_suitability(&self) -> CommonEquitySuitability {
        self.common_equity_suitability
    }
    /// Returns the market-definition parent current at the knowledge cutoff.
    pub const fn current_market_revision_digest(&self) -> Option<EvidenceDigest> {
        self.current_market_revision_digest
    }
    /// Returns publication of the market parent current at the cutoff.
    pub const fn current_market_published_at(&self) -> Option<Timestamp> {
        self.current_market_published_at
    }
    /// Returns effective start of the market parent current at the cutoff.
    pub const fn current_market_effective_start(&self) -> Option<Timestamp> {
        self.current_market_effective_start
    }
    /// Returns effective end of the market parent current at the cutoff.
    pub const fn current_market_effective_end(&self) -> Option<Timestamp> {
        self.current_market_effective_end
    }
    /// Returns conservative availability of the linked company observation.
    pub const fn company_available_at(&self) -> Option<Timestamp> {
        self.company_available_at
    }
    /// Returns local ingestion of the linked company observation.
    pub const fn company_ingested_at(&self) -> Timestamp {
        self.company_ingested_at
    }
    /// Returns durable completion of the company observation's ingest.
    pub const fn company_completed_at(&self) -> Timestamp {
        self.company_completed_at
    }
    /// Returns durable publication of the linked market definition.
    pub const fn market_published_at(&self) -> Timestamp {
        self.market_published_at
    }
    /// Returns the linked market definition's effective start.
    pub const fn market_effective_start(&self) -> Timestamp {
        self.market_effective_start
    }
    /// Returns the linked market definition's explicit effective end.
    pub const fn market_effective_end(&self) -> Option<Timestamp> {
        self.market_effective_end
    }
    /// Returns conservative availability of the relationship evidence.
    pub const fn link_available_at(&self) -> Timestamp {
        self.link_available_at
    }
    /// Returns local relationship ingestion time.
    pub const fn link_ingested_at(&self) -> Timestamp {
        self.link_ingested_at
    }
    /// Returns durable relationship publication time.
    pub const fn link_published_at(&self) -> Timestamp {
        self.link_published_at
    }
    /// Returns relationship effective start.
    pub const fn effective_start(&self) -> Timestamp {
        self.effective_start
    }
    /// Returns relationship effective end.
    pub const fn effective_end(&self) -> Option<Timestamp> {
        self.effective_end
    }
    /// Returns the policy governing only relationship-evidence use.
    pub const fn rights_policy_id(&self) -> &SourceIdentifier {
        &self.rights_policy_id
    }
    /// Returns the relationship-evidence entitlement.
    pub const fn rights_entitlement(&self) -> IdentifierEntitlement {
        self.rights_entitlement
    }
    /// Returns the exact relationship-evidence terms reference.
    pub const fn rights_terms_reference(&self) -> &SourceIdentifier {
        &self.rights_terms_reference
    }
}

/// Canonical point-in-time selection receipt.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CompanySecurityIdentitySelectionReceipt {
    query_digest: EvidenceDigest,
    effective_at: Timestamp,
    knowledge_at: Timestamp,
    disposition: CompanySecurityIdentityDisposition,
    ordered_candidates: Box<[CompanySecuritySelectionReceiptEntry]>,
    ordered_exclusions: Box<
        [(
            CompanySecuritySelectionReceiptEntry,
            CompanySecurityIdentityExclusionReason,
        )],
    >,
    receipt_digest: EvidenceDigest,
}

impl CompanySecurityIdentitySelectionReceipt {
    /// Returns the exact query identity.
    pub const fn query_digest(&self) -> EvidenceDigest {
        self.query_digest
    }
    /// Returns the economic instant used for relationship and reference validity.
    pub const fn effective_at(&self) -> Timestamp {
        self.effective_at
    }
    /// Returns the explicit knowledge cutoff.
    pub const fn knowledge_at(&self) -> Timestamp {
        self.knowledge_at
    }
    /// Returns the closed selection outcome.
    pub const fn disposition(&self) -> CompanySecurityIdentityDisposition {
        self.disposition
    }
    /// Returns ordered qualifying evidence.
    pub fn ordered_candidates(&self) -> &[CompanySecuritySelectionReceiptEntry] {
        &self.ordered_candidates
    }
    /// Returns ordered excluded evidence and reasons.
    pub fn ordered_exclusions(
        &self,
    ) -> &[(
        CompanySecuritySelectionReceiptEntry,
        CompanySecurityIdentityExclusionReason,
    )] {
        &self.ordered_exclusions
    }
    /// Returns the canonical SHA-256 receipt identity.
    pub const fn receipt_digest(&self) -> EvidenceDigest {
        self.receipt_digest
    }
}

/// Complete bounded result of one relationship query.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CompanySecurityIdentitySelection {
    disposition: CompanySecurityIdentityDisposition,
    candidates: Box<[CompanySecurityIdentityRecord]>,
    exclusions: Box<[CompanySecurityIdentityExclusion]>,
    receipt: CompanySecurityIdentitySelectionReceipt,
}

impl CompanySecurityIdentitySelection {
    /// Returns the closed outcome.
    pub const fn disposition(&self) -> CompanySecurityIdentityDisposition {
        self.disposition
    }
    /// Returns all usable candidates; unresolved contenders remain explicit exclusions.
    pub fn candidates(&self) -> &[CompanySecurityIdentityRecord] {
        &self.candidates
    }
    /// Returns bounded current/as-of exclusions.
    pub fn exclusions(&self) -> &[CompanySecurityIdentityExclusion] {
        &self.exclusions
    }
    /// Returns the canonical selection receipt.
    pub const fn receipt(&self) -> &CompanySecurityIdentitySelectionReceipt {
        &self.receipt
    }
}

/// Exact classification scheme admitted by the current cohort authority.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum IndustryClassificationScheme {
    /// United States Securities and Exchange Commission SIC classification.
    SecSic,
}

/// Exact interpretation revision for an admitted classification scheme.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum IndustryClassificationVersion {
    /// Current SEC-reported SIC field grammar retained by Market Squawk V1.
    SecSicCurrentV1,
}

/// Canonical code under one exact classification scheme/version.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) struct IndustryClassificationCode(String);

impl IndustryClassificationCode {
    pub(crate) fn try_new(
        scheme: IndustryClassificationScheme,
        version: IndustryClassificationVersion,
        value: &str,
    ) -> Result<Self, CompanySecurityIdentityCatalogError> {
        if !valid_industry_scheme_version(scheme, version)
            || value.len() != 4
            || !value.bytes().all(|byte| byte.is_ascii_digit())
        {
            return Err(CompanySecurityIdentityCatalogError::InvalidInput);
        }
        Ok(Self(value.to_owned()))
    }

    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }
}

/// Closed point-in-time result for one exact company classification.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum IndustryClassificationDisposition {
    Complete,
    Unavailable,
    Conflict,
    Stale,
    Revoked,
}

/// Why an exact classification observation or cohort member did not qualify.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum IndustryClassificationExclusionReason {
    ObservationUnavailable,
    NotYetAvailable,
    MissingCode,
    InvalidCode,
    AmbiguousParent,
    StaleParent,
    Revoked,
}

/// Exact, source-qualified SEC SIC classification selected at a knowledge cutoff.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct IndustryClassificationRecord {
    scheme: IndustryClassificationScheme,
    version: IndustryClassificationVersion,
    code: IndustryClassificationCode,
    source_description: Option<String>,
    company_source_id: SourceId,
    provider_company_id: SourceIdentifier,
    company_surface: CompanyIdentitySurface,
    company_observation_digest: EvidenceDigest,
    source_record_reference: Option<SourceIdentifier>,
    source_record_version: Option<SourceIdentifier>,
    classification_evidence_digest: EvidenceDigest,
    parent_ingest_evidence_digest: EvidenceDigest,
    effective_at: Timestamp,
    available_at: Timestamp,
    ingested_at: Timestamp,
    published_at: Timestamp,
}

impl IndustryClassificationRecord {
    pub(crate) const fn scheme(&self) -> IndustryClassificationScheme {
        self.scheme
    }
    pub(crate) const fn version(&self) -> IndustryClassificationVersion {
        self.version
    }
    pub(crate) const fn code(&self) -> &IndustryClassificationCode {
        &self.code
    }
    pub(crate) fn source_description(&self) -> Option<&str> {
        self.source_description.as_deref()
    }
    pub(crate) const fn company_source_id(&self) -> &SourceId {
        &self.company_source_id
    }
    pub(crate) const fn provider_company_id(&self) -> &SourceIdentifier {
        &self.provider_company_id
    }
    pub(crate) const fn company_surface(&self) -> CompanyIdentitySurface {
        self.company_surface
    }
    pub(crate) const fn company_observation_digest(&self) -> EvidenceDigest {
        self.company_observation_digest
    }
    pub(crate) const fn classification_evidence_digest(&self) -> EvidenceDigest {
        self.classification_evidence_digest
    }
    pub(crate) const fn parent_ingest_evidence_digest(&self) -> EvidenceDigest {
        self.parent_ingest_evidence_digest
    }
    pub(crate) const fn source_record_reference(&self) -> Option<&SourceIdentifier> {
        self.source_record_reference.as_ref()
    }
    pub(crate) const fn source_record_version(&self) -> Option<&SourceIdentifier> {
        self.source_record_version.as_ref()
    }
    pub(crate) const fn effective_at(&self) -> Timestamp {
        self.effective_at
    }
    pub(crate) const fn available_at(&self) -> Timestamp {
        self.available_at
    }
    pub(crate) const fn ingested_at(&self) -> Timestamp {
        self.ingested_at
    }
    pub(crate) const fn published_at(&self) -> Timestamp {
        self.published_at
    }
}

/// Complete evidence coordinates bound into a classification/cohort receipt.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct IndustryClassificationReceiptEntry {
    company_observation_digest: EvidenceDigest,
    current_company_observation_digest: Option<EvidenceDigest>,
    company_source_id: SourceId,
    provider_company_id: SourceIdentifier,
    company_surface: CompanyIdentitySurface,
    reported_code: Option<String>,
    source_description: Option<String>,
    source_record_reference: Option<SourceIdentifier>,
    source_record_version: Option<SourceIdentifier>,
    classification_evidence_digest: EvidenceDigest,
    parent_ingest_evidence_digest: EvidenceDigest,
    effective_at: Option<Timestamp>,
    available_at: Option<Timestamp>,
    received_at: Timestamp,
    ingested_at: Timestamp,
    published_at: Timestamp,
    current_available_at: Option<Timestamp>,
    current_ingested_at: Option<Timestamp>,
    current_published_at: Option<Timestamp>,
}

/// Canonical receipt for one exact company-classification query.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct IndustryClassificationSelectionReceipt {
    query_digest: EvidenceDigest,
    knowledge_at: Timestamp,
    disposition: IndustryClassificationDisposition,
    considered: Option<IndustryClassificationReceiptEntry>,
    exclusion_reason: Option<IndustryClassificationExclusionReason>,
    receipt_digest: EvidenceDigest,
}

impl IndustryClassificationSelectionReceipt {
    pub(crate) const fn query_digest(&self) -> EvidenceDigest {
        self.query_digest
    }
    pub(crate) const fn knowledge_at(&self) -> Timestamp {
        self.knowledge_at
    }
    pub(crate) const fn disposition(&self) -> IndustryClassificationDisposition {
        self.disposition
    }
    pub(crate) const fn considered(&self) -> Option<&IndustryClassificationReceiptEntry> {
        self.considered.as_ref()
    }
    pub(crate) const fn exclusion_reason(&self) -> Option<IndustryClassificationExclusionReason> {
        self.exclusion_reason
    }
    pub(crate) const fn receipt_digest(&self) -> EvidenceDigest {
        self.receipt_digest
    }
}

/// Exact closed selection for one company observation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct IndustryClassificationSelection {
    disposition: IndustryClassificationDisposition,
    classification: Option<IndustryClassificationRecord>,
    receipt: IndustryClassificationSelectionReceipt,
}

impl IndustryClassificationSelection {
    pub(crate) const fn disposition(&self) -> IndustryClassificationDisposition {
        self.disposition
    }
    pub(crate) const fn classification(&self) -> Option<&IndustryClassificationRecord> {
        self.classification.as_ref()
    }
    pub(crate) const fn receipt(&self) -> &IndustryClassificationSelectionReceipt {
        &self.receipt
    }
}

/// Whether one bounded cohort receipt covers every exact matching observation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum IndustryCohortCompleteness {
    Complete,
    Truncated,
}

/// One non-member observation retained with a fail-closed reason.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct IndustryCohortExclusion {
    entry: IndustryClassificationReceiptEntry,
    reason: IndustryClassificationExclusionReason,
}

/// Canonical bounded reverse-membership receipt.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct IndustryCohortSelectionReceipt {
    query_digest: EvidenceDigest,
    knowledge_at: Timestamp,
    completeness: IndustryCohortCompleteness,
    scan_truncated: bool,
    member_limit_truncated: bool,
    scan_boundary_digest: Option<EvidenceDigest>,
    ordered_members: Box<[IndustryClassificationReceiptEntry]>,
    ordered_exclusions: Box<[IndustryCohortExclusion]>,
    receipt_digest: EvidenceDigest,
}

/// Bounded exact members of one SEC SIC cohort at a knowledge cutoff.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct IndustryCohortSelection {
    members: Box<[IndustryClassificationRecord]>,
    exclusions: Box<[IndustryCohortExclusion]>,
    receipt: IndustryCohortSelectionReceipt,
}

impl IndustryCohortSelection {
    pub(crate) fn members(&self) -> &[IndustryClassificationRecord] {
        &self.members
    }
    pub(crate) fn exclusions(&self) -> &[IndustryCohortExclusion] {
        &self.exclusions
    }
    pub(crate) const fn receipt(&self) -> &IndustryCohortSelectionReceipt {
        &self.receipt
    }
}

impl IndustryCohortExclusion {
    pub(crate) const fn entry(&self) -> &IndustryClassificationReceiptEntry {
        &self.entry
    }
    pub(crate) const fn reason(&self) -> IndustryClassificationExclusionReason {
        self.reason
    }
}

impl IndustryCohortSelectionReceipt {
    pub(crate) const fn query_digest(&self) -> EvidenceDigest {
        self.query_digest
    }
    pub(crate) const fn knowledge_at(&self) -> Timestamp {
        self.knowledge_at
    }
    pub(crate) const fn completeness(&self) -> IndustryCohortCompleteness {
        self.completeness
    }
    pub(crate) const fn scan_truncated(&self) -> bool {
        self.scan_truncated
    }
    pub(crate) const fn member_limit_truncated(&self) -> bool {
        self.member_limit_truncated
    }
    pub(crate) const fn scan_boundary_digest(&self) -> Option<EvidenceDigest> {
        self.scan_boundary_digest
    }
    pub(crate) fn ordered_members(&self) -> &[IndustryClassificationReceiptEntry] {
        &self.ordered_members
    }
    pub(crate) fn ordered_exclusions(&self) -> &[IndustryCohortExclusion] {
        &self.ordered_exclusions
    }
    pub(crate) const fn receipt_digest(&self) -> EvidenceDigest {
        self.receipt_digest
    }
}

/// Publication outcome for one immutable relationship event.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CompanySecurityLinkPublicationDisposition {
    Inserted,
    Replay,
}

/// Publication receipt for one relationship event.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CompanySecurityLinkPublicationReceipt {
    record: CompanySecurityIdentityRecord,
    disposition: CompanySecurityLinkPublicationDisposition,
}

impl CompanySecurityLinkPublicationReceipt {
    /// Returns the durable event.
    pub const fn record(&self) -> &CompanySecurityIdentityRecord {
        &self.record
    }
    /// Returns whether this inserted or exactly replayed an event.
    pub const fn disposition(&self) -> CompanySecurityLinkPublicationDisposition {
        self.disposition
    }
}

/// Cloneable least-authority relationship publisher.
#[derive(Clone)]
pub struct CompanySecurityLinkPublicationCapability {
    authority: Arc<Mutex<CatalogAuthority>>,
}

impl fmt::Debug for CompanySecurityLinkPublicationCapability {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CompanySecurityLinkPublicationCapability")
            .field(
                "authority",
                &"[SEALED COMPANY/SECURITY PUBLICATION AUTHORITY]",
            )
            .finish()
    }
}

impl CompanySecurityLinkPublicationCapability {
    /// Binds publication to the sole catalog writer.
    pub const fn new(authority: Arc<Mutex<CatalogAuthority>>) -> Self {
        Self { authority }
    }
    /// Publishes one exact, parent-current relationship event.
    pub fn publish(
        &self,
        link: CompanySecurityIdentityLink,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<CompanySecurityLinkPublicationReceipt, CompanySecurityIdentityCatalogError> {
        check_operation(deadline, cancellation)?;
        self.authority
            .try_lock()
            .map_err(|_| CompanySecurityIdentityCatalogError::AuthorityUnavailable)?
            .publish_company_security_link(link, deadline, cancellation)
    }
}

/// Cloneable least-authority exact/current/as-of relationship reader.
#[derive(Clone)]
pub struct CompanySecurityIdentityReadCapability {
    authority: Arc<Mutex<CatalogAuthority>>,
}

impl fmt::Debug for CompanySecurityIdentityReadCapability {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CompanySecurityIdentityReadCapability")
            .field("authority", &"[SEALED COMPANY/SECURITY READ AUTHORITY]")
            .finish()
    }
}

impl CompanySecurityIdentityReadCapability {
    /// Binds reads to the sole catalog writer session.
    pub const fn new(authority: Arc<Mutex<CatalogAuthority>>) -> Self {
        Self { authority }
    }
    /// Reads one exact immutable event revision.
    pub fn exact(
        &self,
        digest: EvidenceDigest,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<Option<CompanySecurityIdentityRecord>, CompanySecurityIdentityCatalogError> {
        check_operation(deadline, cancellation)?;
        self.authority
            .try_lock()
            .map_err(|_| CompanySecurityIdentityCatalogError::AuthorityUnavailable)?
            .exact_company_security_link(digest, deadline, cancellation)
    }
    /// Selects against the catalog's trusted current time.
    pub fn current(
        &self,
        query: &CompanySecurityIdentityQuery,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<CompanySecurityIdentitySelection, CompanySecurityIdentityCatalogError> {
        check_operation(deadline, cancellation)?;
        self.authority
            .try_lock()
            .map_err(|_| CompanySecurityIdentityCatalogError::AuthorityUnavailable)?
            .current_company_security_links(query, deadline, cancellation)
    }
    /// Selects only relationship and parent facts knowable at the exact cutoff.
    pub fn as_of(
        &self,
        query: &CompanySecurityIdentityQuery,
        knowledge_at: Timestamp,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<CompanySecurityIdentitySelection, CompanySecurityIdentityCatalogError> {
        check_operation(deadline, cancellation)?;
        self.authority
            .try_lock()
            .map_err(|_| CompanySecurityIdentityCatalogError::AuthorityUnavailable)?
            .as_of_company_security_links(query, knowledge_at, deadline, cancellation)
    }

    /// Resolves one exact SEC company generation to authoritative tradable-security identity.
    ///
    /// Only a direct crosswalk backed by a verified, non-ticker external identifier in the exact
    /// immutable market-definition generation can return [`SecFundamentalIdentityAvailability::Available`].
    /// Operator resolutions and SEC ticker/name associations remain identity-pending.
    pub fn sec_fundamental_identity_as_of(
        &self,
        query: &SecFundamentalIdentityQuery,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<SecFundamentalIdentitySelection, CompanySecurityIdentityCatalogError> {
        check_operation(deadline, cancellation)?;
        self.authority
            .try_lock()
            .map_err(|_| CompanySecurityIdentityCatalogError::AuthorityUnavailable)?
            .sec_fundamental_identity_as_of(query, deadline, cancellation)
    }

    /// Resolves an exact instrument to its source-qualified issuer company at a knowledge cutoff.
    ///
    /// The caller must explicitly require direct issuer common equity. This least-authority read
    /// cannot infer from ticker, name, exchange, or provider company identifiers; cannot use the
    /// wall clock as an implicit cutoff; and grants no downstream data or execution right.
    pub fn instrument_company_as_of(
        &self,
        instrument_id: InstrumentId,
        company_source_id: &SourceId,
        company_surface: CompanyIdentitySurface,
        knowledge_at: Timestamp,
        required_suitability: CommonEquitySuitability,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<CompanySecurityIdentitySelection, CompanySecurityIdentityCatalogError> {
        check_operation(deadline, cancellation)?;
        if required_suitability != CommonEquitySuitability::SuitableIssuerCommonEquity {
            return Err(CompanySecurityIdentityCatalogError::InvalidInput);
        }
        self.authority
            .try_lock()
            .map_err(|_| CompanySecurityIdentityCatalogError::AuthorityUnavailable)?
            .instrument_company_security_links_as_of(
                instrument_id,
                company_source_id,
                company_surface,
                knowledge_at,
                required_suitability,
                deadline,
                cancellation,
            )
    }

    /// Selects one exact source-qualified SEC SIC classification at a caller cutoff.
    ///
    /// This grants classification/cohort identity only. It grants no valuation, model, filing,
    /// market-data, redistribution, or execution right.
    pub(crate) fn industry_classification_as_of(
        &self,
        company_observation_digest: EvidenceDigest,
        company_source_id: &SourceId,
        company_surface: CompanyIdentitySurface,
        knowledge_at: Timestamp,
        scheme: IndustryClassificationScheme,
        version: IndustryClassificationVersion,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<IndustryClassificationSelection, CompanySecurityIdentityCatalogError> {
        check_operation(deadline, cancellation)?;
        self.authority
            .try_lock()
            .map_err(|_| CompanySecurityIdentityCatalogError::AuthorityUnavailable)?
            .select_industry_classification_as_of(
                company_observation_digest,
                company_source_id,
                company_surface,
                knowledge_at,
                scheme,
                version,
                deadline,
                cancellation,
            )
    }

    /// Selects a bounded exact SEC SIC reverse-membership cohort at a caller cutoff.
    pub(crate) fn industry_cohort_as_of(
        &self,
        company_source_id: &SourceId,
        company_surface: CompanyIdentitySurface,
        knowledge_at: Timestamp,
        scheme: IndustryClassificationScheme,
        version: IndustryClassificationVersion,
        code: &str,
        maximum_members: usize,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<IndustryCohortSelection, CompanySecurityIdentityCatalogError> {
        check_operation(deadline, cancellation)?;
        let code = IndustryClassificationCode::try_new(scheme, version, code)?;
        if maximum_members == 0 || maximum_members > MAX_INDUSTRY_COHORT_MEMBERS {
            return Err(CompanySecurityIdentityCatalogError::InvalidInput);
        }
        self.authority
            .try_lock()
            .map_err(|_| CompanySecurityIdentityCatalogError::AuthorityUnavailable)?
            .select_industry_cohort_as_of(
                company_source_id,
                company_surface,
                knowledge_at,
                scheme,
                version,
                &code,
                maximum_members,
                deadline,
                cancellation,
            )
    }
}

/// Relationship persistence or selection failure.
#[derive(Debug, Error)]
pub enum CompanySecurityIdentityCatalogError {
    #[error("company/security input is invalid")]
    InvalidInput,
    #[error("company/security parent is unavailable or not current")]
    ParentUnavailable,
    #[error("company/security direct authority is not backed by a verified reference identifier")]
    UnverifiedIdentityAuthority,
    #[error("company/security current parent is ambiguous")]
    AmbiguousParent,
    #[error("company/security transition does not follow the exact current event")]
    TransitionConflict,
    #[error("company/security event history exhausted its hard bound")]
    HistoryLimitExceeded,
    #[error("company/security result exceeded its count or byte bound")]
    ResultLimitExceeded,
    #[error("company/security catalog authority is unavailable")]
    AuthorityUnavailable,
    #[error("company/security operation was cancelled")]
    Cancelled,
    #[error("company/security operation deadline elapsed")]
    DeadlineExceeded,
    #[error("company/security durable state is corrupt")]
    CorruptCatalog,
    #[error("company/security serialization failed")]
    Serialization(#[from] serde_json::Error),
    #[error("company/security storage operation failed")]
    Storage(#[from] rusqlite::Error),
}

#[derive(Debug)]
struct StoredLinkRow {
    digest: Vec<u8>,
    company_source_id: String,
    provider_company_id: String,
    company_surface: String,
    company_observation_digest: Vec<u8>,
    instrument_id: String,
    market_revision_digest: Vec<u8>,
    event_sequence: i64,
    security_kind: String,
    relationship_kind: String,
    suitability: String,
    event_kind: String,
    previous_digest: Option<Vec<u8>>,
    effective_start: i64,
    effective_end: Option<i64>,
    resolution_kind: String,
    resolution_algorithm: i64,
    resolution_digest: Vec<u8>,
    rights_policy_id: String,
    rights_entitlement: String,
    rights_terms_reference: String,
    available_at: i64,
    ingested_at: i64,
    json: String,
    published_at: i64,
}

#[derive(Debug)]
struct StoredCompanyObservationRow {
    digest: Vec<u8>,
    available_at: Option<i64>,
    ingested_at: i64,
    completed_at: i64,
    json: String,
    source_id: String,
    provider_company_id: String,
    source_surface: String,
}

#[derive(Debug)]
struct StoredCohortObservationRow {
    observation: StoredCompanyObservationRow,
    term_display: String,
    term_normalized: String,
}

#[derive(Clone, Debug)]
struct CompanyObservationRecord {
    observation: CompanyIdentityObservation,
    parent: CompanyParent,
}

#[derive(Clone, Copy, Debug)]
struct CompanyParent {
    digest: EvidenceDigest,
    available_at: Option<Timestamp>,
    ingested_at: Timestamp,
    completed_at: Timestamp,
}

#[derive(Clone, Debug)]
struct MarketParent {
    digest: EvidenceDigest,
    published_at: Timestamp,
    effective_start: Timestamp,
    effective_end: Option<Timestamp>,
    definition: MarketDataInstrumentDefinition,
}

impl CatalogAuthority {
    pub(crate) fn exact_current_company_identity(
        &self,
        source_id: &SourceId,
        provider_company_id: &SourceIdentifier,
        surface: CompanyIdentitySurface,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<
        Option<(CompanyIdentityObservation, EvidenceDigest, Timestamp)>,
        CompanySecurityIdentityCatalogError,
    > {
        check_operation(deadline, cancellation)?;
        let connection = &self.catalog().connection;
        install_progress_handler(connection, deadline, cancellation)?;
        let result = (|| {
            let transaction = connection.unchecked_transaction()?;
            let knowledge_at = trusted_catalog_now(&transaction)
                .map_err(|_| CompanySecurityIdentityCatalogError::CorruptCatalog)?;
            let Some(parent) = current_company_parent(
                &transaction,
                source_id,
                provider_company_id,
                surface,
                knowledge_at,
            )?
            else {
                transaction.commit()?;
                return Ok(None);
            };
            let json: String = transaction.query_row(
                "SELECT record_json FROM company_identity_observations
                 WHERE record_digest=?1 AND source_id=?2 AND provider_company_id=?3
                   AND source_surface=?4",
                params![
                    parent.digest.bytes(),
                    source_id.as_str(),
                    provider_company_id.as_str(),
                    surface.database_name(),
                ],
                |row| row.get(0),
            )?;
            let observation: CompanyIdentityObservation = serde_json::from_str(&json)?;
            if sha256(json.as_bytes()) != parent.digest.bytes()
                || serde_json::to_string(&observation)? != json
                || observation.source_id() != source_id
                || observation.provider_company_id() != provider_company_id
                || observation.surface() != surface
                || observation.availability().conservative_available_at() != parent.available_at
                || observation.ingested_at() != parent.ingested_at
            {
                return Err(CompanySecurityIdentityCatalogError::CorruptCatalog);
            }
            transaction.commit()?;
            Ok(Some((observation, parent.digest, parent.completed_at)))
        })();
        clear_progress_handler(connection)?;
        classify_operation(result, deadline, cancellation)
    }

    fn publish_company_security_link(
        &self,
        link: CompanySecurityIdentityLink,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<CompanySecurityLinkPublicationReceipt, CompanySecurityIdentityCatalogError> {
        check_operation(deadline, cancellation)?;
        let json = serde_json::to_string(&link)?;
        if json.len() > self.catalog().result_bytes.max_record_bytes() {
            return Err(CompanySecurityIdentityCatalogError::ResultLimitExceeded);
        }
        let digest_bytes = sha256(json.as_bytes());
        let digest = evidence_digest(digest_bytes);
        let connection = &self.catalog().connection;
        install_progress_handler(connection, deadline, cancellation)?;
        let result = (|| {
            let transaction = connection.unchecked_transaction()?;
            let now = trusted_catalog_now(&transaction)
                .map_err(|_| CompanySecurityIdentityCatalogError::CorruptCatalog)?;
            if link.ingested_at() > now {
                return Err(CompanySecurityIdentityCatalogError::InvalidInput);
            }
            if let Some(existing) = load_link_by_digest(&transaction, digest_bytes)? {
                if existing.link == link {
                    return Ok(CompanySecurityLinkPublicationReceipt {
                        record: existing,
                        disposition: CompanySecurityLinkPublicationDisposition::Replay,
                    });
                }
                return Err(CompanySecurityIdentityCatalogError::CorruptCatalog);
            }
            let company_parent = exact_company_parent(
                &transaction,
                link.company_observation_digest(),
                link.company_source_id(),
                link.provider_company_id(),
                link.company_surface(),
            )?
            .ok_or(CompanySecurityIdentityCatalogError::ParentUnavailable)?;
            let market_parent = exact_market_parent(
                &transaction,
                link.market_instrument_revision_digest(),
                link.instrument_id(),
            )?
            .ok_or(CompanySecurityIdentityCatalogError::ParentUnavailable)?;
            if company_parent.available_at.is_none()
                || company_parent.available_at.is_some_and(|time| time > now)
                || company_parent.ingested_at > now
                || company_parent.completed_at > now
                || market_parent.published_at > now
                || !market_interval_covers(&market_parent, link.effective_interval())
            {
                return Err(CompanySecurityIdentityCatalogError::ParentUnavailable);
            }
            if !link.transition().is_revocation()
                && matches!(
                    link.resolution_basis(),
                    CompanySecurityResolutionBasis::DirectAuthoritativeCrosswalk { .. }
                )
                && !direct_crosswalk_is_reference_backed(&link, &market_parent.definition)
            {
                return Err(CompanySecurityIdentityCatalogError::UnverifiedIdentityAuthority);
            }
            if !link.transition().is_revocation() {
                let current_company = current_company_parent(
                    &transaction,
                    link.company_source_id(),
                    link.provider_company_id(),
                    link.company_surface(),
                    now,
                )?;
                let current_market = current_market_parent(
                    &transaction,
                    link.instrument_id(),
                    now,
                    link.effective_interval().starts_at(),
                )?;
                if current_company.map(|parent| parent.digest)
                    != Some(link.company_observation_digest())
                    || current_market.as_ref().map(|parent| parent.digest)
                        != Some(link.market_instrument_revision_digest())
                {
                    return Err(CompanySecurityIdentityCatalogError::ParentUnavailable);
                }
            }
            let key = link_key_params(&link);
            let current: Option<Vec<u8>> = transaction
                .query_row(
                    "SELECT link_digest FROM company_security_link_current
                 WHERE company_source_id=?1 AND provider_company_id=?2
                   AND company_surface=?3 AND instrument_id=?4",
                    params![key.0, key.1, key.2, key.3],
                    |row| row.get(0),
                )
                .optional()?;
            let expected_previous = link
                .transition()
                .previous_link_digest()
                .map(|value| value.bytes());
            let expected_previous = expected_previous.as_ref().map(|value| value.as_slice());
            if current.as_deref() != expected_previous
                || (current.is_none()
                    && !matches!(link.transition(), CompanySecurityLinkTransition::Initial))
                || (current.is_some()
                    && matches!(link.transition(), CompanySecurityLinkTransition::Initial))
            {
                return Err(CompanySecurityIdentityCatalogError::TransitionConflict);
            }
            let history_count: i64 = transaction.query_row(
                "SELECT COUNT(*) FROM company_security_link_events
                 WHERE company_source_id=?1 AND provider_company_id=?2
                   AND company_surface=?3 AND instrument_id=?4",
                params![key.0, key.1, key.2, key.3],
                |row| row.get(0),
            )?;
            if history_count >= MAX_LINK_EVENTS_PER_RELATIONSHIP {
                return Err(CompanySecurityIdentityCatalogError::HistoryLimitExceeded);
            }
            let event_sequence = u32::try_from(history_count + 1)
                .map_err(|_| CompanySecurityIdentityCatalogError::HistoryLimitExceeded)?;
            insert_link(
                &transaction,
                &link,
                &json,
                digest_bytes,
                event_sequence,
                now,
            )?;
            if current.is_some() {
                let changed = transaction.execute(
                    "UPDATE company_security_link_current
                     SET link_digest=?1, advanced_at_ns=?2
                     WHERE company_source_id=?3 AND provider_company_id=?4
                       AND company_surface=?5 AND instrument_id=?6 AND link_digest=?7",
                    params![
                        digest_bytes,
                        now.unix_nanos(),
                        key.0,
                        key.1,
                        key.2,
                        key.3,
                        current
                    ],
                )?;
                if changed != 1 {
                    return Err(CompanySecurityIdentityCatalogError::CorruptCatalog);
                }
            } else {
                transaction.execute(
                    "INSERT INTO company_security_link_current
                     (company_source_id, provider_company_id, company_surface, instrument_id,
                      link_digest, advanced_at_ns) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                    params![key.0, key.1, key.2, key.3, digest_bytes, now.unix_nanos()],
                )?;
            }
            append_audit(
                &transaction,
                if link.transition().is_revocation() {
                    "company-security.revoked"
                } else {
                    "company-security.published"
                },
                link.provider_company_id().as_str(),
                digest_bytes,
                now,
            )
            .map_err(|_| CompanySecurityIdentityCatalogError::CorruptCatalog)?;
            transaction.commit()?;
            Ok(CompanySecurityLinkPublicationReceipt {
                record: CompanySecurityIdentityRecord {
                    link,
                    link_digest: digest,
                    event_sequence,
                    published_at: now,
                },
                disposition: CompanySecurityLinkPublicationDisposition::Inserted,
            })
        })();
        clear_progress_handler(connection)?;
        classify_operation(result, deadline, cancellation)
    }

    fn exact_company_security_link(
        &self,
        digest: EvidenceDigest,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<Option<CompanySecurityIdentityRecord>, CompanySecurityIdentityCatalogError> {
        if digest.algorithm() != DigestAlgorithm::Sha256 || digest.bytes() == [0; 32] {
            return Err(CompanySecurityIdentityCatalogError::InvalidInput);
        }
        let connection = &self.catalog().connection;
        install_progress_handler(connection, deadline, cancellation)?;
        let result = load_link_by_digest(connection, digest.bytes());
        clear_progress_handler(connection)?;
        classify_operation(result, deadline, cancellation)
    }

    fn current_company_security_links(
        &self,
        query: &CompanySecurityIdentityQuery,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<CompanySecurityIdentitySelection, CompanySecurityIdentityCatalogError> {
        let connection = &self.catalog().connection;
        install_progress_handler(connection, deadline, cancellation)?;
        let result = (|| {
            let transaction = connection.unchecked_transaction()?;
            let now = trusted_catalog_now(&transaction)
                .map_err(|_| CompanySecurityIdentityCatalogError::CorruptCatalog)?;
            let selection = select_links(
                &transaction,
                self.catalog().result_bytes,
                query,
                now,
                now,
                true,
                deadline,
                cancellation,
            )?;
            transaction.commit()?;
            Ok(selection)
        })();
        clear_progress_handler(connection)?;
        classify_operation(result, deadline, cancellation)
    }

    fn as_of_company_security_links(
        &self,
        query: &CompanySecurityIdentityQuery,
        knowledge_at: Timestamp,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<CompanySecurityIdentitySelection, CompanySecurityIdentityCatalogError> {
        let connection = &self.catalog().connection;
        install_progress_handler(connection, deadline, cancellation)?;
        let result = select_links(
            connection,
            self.catalog().result_bytes,
            query,
            knowledge_at,
            knowledge_at,
            false,
            deadline,
            cancellation,
        );
        clear_progress_handler(connection)?;
        classify_operation(result, deadline, cancellation)
    }

    fn sec_fundamental_identity_as_of(
        &self,
        query: &SecFundamentalIdentityQuery,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<SecFundamentalIdentitySelection, CompanySecurityIdentityCatalogError> {
        let connection = &self.catalog().connection;
        install_progress_handler(connection, deadline, cancellation)?;
        let result = (|| {
            let relationship_query = CompanySecurityIdentityQuery::new(
                query.company_source_id().clone(),
                query.cik().clone(),
                query.company_surface(),
                None,
                true,
            );
            let relationship_selection = select_links(
                connection,
                self.catalog().result_bytes,
                &relationship_query,
                query.effective_at(),
                query.knowledge_at(),
                false,
                deadline,
                cancellation,
            )?;
            let exact_company = exact_company_parent(
                connection,
                query.company_observation_digest(),
                query.company_source_id(),
                query.cik(),
                query.company_surface(),
            )?;
            let (current_company, company_parent_ambiguous) = match current_company_parent(
                connection,
                query.company_source_id(),
                query.cik(),
                query.company_surface(),
                query.knowledge_at(),
            ) {
                Ok(parent) => (parent, false),
                Err(CompanySecurityIdentityCatalogError::AmbiguousParent) => (None, true),
                Err(error) => return Err(error),
            };

            let mut authoritative = Vec::new();
            authoritative
                .try_reserve_exact(relationship_selection.candidates().len())
                .map_err(|_| CompanySecurityIdentityCatalogError::ResultLimitExceeded)?;
            for record in relationship_selection.candidates().iter().filter(|record| {
                record.link().company_observation_digest() == query.company_observation_digest()
            }) {
                if matches!(
                    record.link().resolution_basis(),
                    CompanySecurityResolutionBasis::DirectAuthoritativeCrosswalk { .. }
                ) {
                    let parent = exact_market_parent(
                        connection,
                        record.link().market_instrument_revision_digest(),
                        record.link().instrument_id(),
                    )?
                    .ok_or(CompanySecurityIdentityCatalogError::CorruptCatalog)?;
                    if !direct_crosswalk_is_reference_backed(record.link(), &parent.definition) {
                        return Err(CompanySecurityIdentityCatalogError::CorruptCatalog);
                    }
                    authoritative.push(record.clone());
                }
            }

            let company_knowable = exact_company.is_some_and(|parent| {
                parent
                    .available_at
                    .is_some_and(|time| time <= query.knowledge_at())
                    && parent.ingested_at <= query.knowledge_at()
                    && parent.completed_at <= query.knowledge_at()
            });
            let company_generation_current = current_company.map(|parent| parent.digest)
                == Some(query.company_observation_digest());
            let availability = if company_parent_ambiguous
                || relationship_selection.disposition()
                    == CompanySecurityIdentityDisposition::Conflict
                || authoritative.len() > 1
            {
                SecFundamentalIdentityAvailability::Conflict
            } else if exact_company.is_none() {
                SecFundamentalIdentityAvailability::Unavailable
            } else if !company_knowable || !company_generation_current {
                SecFundamentalIdentityAvailability::IdentityPending
            } else if authoritative.len() == 1 && relationship_selection.candidates().len() == 1 {
                SecFundamentalIdentityAvailability::Available
            } else if relationship_selection.exclusions().iter().any(|exclusion| {
                matches!(
                    exclusion.reason(),
                    CompanySecurityIdentityExclusionReason::NoLongerEffective
                        | CompanySecurityIdentityExclusionReason::NotSuitableCommonEquity
                        | CompanySecurityIdentityExclusionReason::Revoked
                )
            }) {
                SecFundamentalIdentityAvailability::Unavailable
            } else {
                SecFundamentalIdentityAvailability::IdentityPending
            };
            let relationship = (availability == SecFundamentalIdentityAvailability::Available)
                .then(|| authoritative.remove(0));
            finish_sec_fundamental_identity(
                query,
                relationship_selection,
                availability,
                relationship,
            )
        })();
        clear_progress_handler(connection)?;
        classify_operation(result, deadline, cancellation)
    }

    fn instrument_company_security_links_as_of(
        &self,
        instrument_id: InstrumentId,
        company_source_id: &SourceId,
        company_surface: CompanyIdentitySurface,
        knowledge_at: Timestamp,
        required_suitability: CommonEquitySuitability,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<CompanySecurityIdentitySelection, CompanySecurityIdentityCatalogError> {
        let connection = &self.catalog().connection;
        install_progress_handler(connection, deadline, cancellation)?;
        let result = select_instrument_company_links(
            connection,
            self.catalog().result_bytes,
            instrument_id,
            company_source_id,
            company_surface,
            knowledge_at,
            required_suitability,
            deadline,
            cancellation,
        );
        clear_progress_handler(connection)?;
        classify_operation(result, deadline, cancellation)
    }

    fn select_industry_classification_as_of(
        &self,
        company_observation_digest: EvidenceDigest,
        company_source_id: &SourceId,
        company_surface: CompanyIdentitySurface,
        knowledge_at: Timestamp,
        scheme: IndustryClassificationScheme,
        version: IndustryClassificationVersion,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<IndustryClassificationSelection, CompanySecurityIdentityCatalogError> {
        let connection = &self.catalog().connection;
        install_progress_handler(connection, deadline, cancellation)?;
        let result = select_industry_classification(
            connection,
            self.catalog().result_bytes,
            company_observation_digest,
            company_source_id,
            company_surface,
            knowledge_at,
            scheme,
            version,
            deadline,
            cancellation,
        );
        clear_progress_handler(connection)?;
        classify_operation(result, deadline, cancellation)
    }

    fn select_industry_cohort_as_of(
        &self,
        company_source_id: &SourceId,
        company_surface: CompanyIdentitySurface,
        knowledge_at: Timestamp,
        scheme: IndustryClassificationScheme,
        version: IndustryClassificationVersion,
        code: &IndustryClassificationCode,
        maximum_members: usize,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<IndustryCohortSelection, CompanySecurityIdentityCatalogError> {
        let connection = &self.catalog().connection;
        install_progress_handler(connection, deadline, cancellation)?;
        let result = select_industry_cohort(
            connection,
            self.catalog().result_bytes,
            company_source_id,
            company_surface,
            knowledge_at,
            scheme,
            version,
            code,
            maximum_members,
            deadline,
            cancellation,
        );
        clear_progress_handler(connection)?;
        classify_operation(result, deadline, cancellation)
    }
}

fn insert_link(
    transaction: &Transaction<'_>,
    link: &CompanySecurityIdentityLink,
    json: &str,
    digest: [u8; 32],
    event_sequence: u32,
    published_at: Timestamp,
) -> Result<(), CompanySecurityIdentityCatalogError> {
    let resolution = link.resolution_basis().evidence().content_digest();
    let rights = link.relationship_evidence_rights();
    transaction.execute(
        "INSERT INTO company_security_link_events
         (link_digest, company_source_id, provider_company_id, company_surface,
          company_observation_digest, instrument_id, market_revision_digest, event_sequence,
          security_kind, relationship_kind, common_equity_suitability, event_kind,
          previous_link_digest, effective_start_ns, effective_end_ns, resolution_kind,
          resolution_evidence_algorithm, resolution_evidence_digest,
          relationship_rights_policy_id, relationship_rights_entitlement,
          relationship_rights_terms_reference, available_at_ns, ingested_at_ns, link_json,
          published_at_ns)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13,
                 ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21, ?22, ?23, ?24, ?25)",
        params![
            digest,
            link.company_source_id().as_str(),
            link.provider_company_id().as_str(),
            link.company_surface().database_name(),
            link.company_observation_digest().bytes(),
            link.instrument_id().to_string(),
            link.market_instrument_revision_digest().bytes(),
            i64::from(event_sequence),
            security_kind_name(link.security_kind()),
            relationship_kind_name(link.relationship_kind()),
            suitability_name(link.common_equity_suitability()),
            if link.transition().is_revocation() {
                "revoked"
            } else {
                "active"
            },
            link.transition()
                .previous_link_digest()
                .map(|value| value.bytes()),
            link.effective_interval().starts_at().unix_nanos(),
            link.effective_interval()
                .ends_at()
                .map(Timestamp::unix_nanos),
            resolution_kind_name(link.resolution_basis()),
            algorithm_code(resolution.algorithm()),
            resolution.bytes(),
            rights.policy_id().as_str(),
            entitlement_name(rights.entitlement()),
            rights.terms_reference().as_str(),
            link.available_at().unix_nanos(),
            link.ingested_at().unix_nanos(),
            json,
            published_at.unix_nanos(),
        ],
    )?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn select_industry_classification(
    connection: &Connection,
    result_limits: super::CatalogResultLimits,
    company_observation_digest: EvidenceDigest,
    company_source_id: &SourceId,
    company_surface: CompanyIdentitySurface,
    knowledge_at: Timestamp,
    scheme: IndustryClassificationScheme,
    version: IndustryClassificationVersion,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<IndustryClassificationSelection, CompanySecurityIdentityCatalogError> {
    check_operation(deadline, cancellation)?;
    validate_industry_query(company_observation_digest, scheme, version)?;
    let query_digest = industry_classification_query_digest(
        company_observation_digest,
        company_source_id,
        company_surface,
        scheme,
        version,
    );
    let mut budget = ResultBudget::new(result_limits);
    let Some(observation) = load_exact_company_observation(
        connection,
        company_observation_digest,
        company_source_id,
        company_surface,
        &mut budget,
    )?
    else {
        return finish_industry_classification_selection(
            query_digest,
            knowledge_at,
            IndustryClassificationDisposition::Unavailable,
            None,
            None,
            Some(IndustryClassificationExclusionReason::ObservationUnavailable),
        );
    };
    let knowable = company_observation_is_knowable(&observation, knowledge_at);
    let (current_company, parent_ambiguous) = if knowable {
        match current_company_parent(
            connection,
            observation.observation.source_id(),
            observation.observation.provider_company_id(),
            observation.observation.surface(),
            knowledge_at,
        ) {
            Ok(parent) => (parent, false),
            Err(CompanySecurityIdentityCatalogError::AmbiguousParent) => (None, true),
            Err(error) => return Err(error),
        }
    } else {
        (None, false)
    };
    let entry = industry_classification_receipt_entry(&observation, current_company);
    let (disposition, classification, reason) = if !knowable {
        (
            IndustryClassificationDisposition::Unavailable,
            None,
            Some(IndustryClassificationExclusionReason::NotYetAvailable),
        )
    } else if parent_ambiguous {
        (
            IndustryClassificationDisposition::Conflict,
            None,
            Some(IndustryClassificationExclusionReason::AmbiguousParent),
        )
    } else {
        let current_company =
            current_company.ok_or(CompanySecurityIdentityCatalogError::CorruptCatalog)?;
        if current_company.digest != observation.parent.digest {
            (
                IndustryClassificationDisposition::Stale,
                None,
                Some(IndustryClassificationExclusionReason::StaleParent),
            )
        } else {
            match observation.observation.sic() {
                None => (
                    IndustryClassificationDisposition::Unavailable,
                    None,
                    Some(IndustryClassificationExclusionReason::MissingCode),
                ),
                Some(value) => match IndustryClassificationCode::try_new(scheme, version, value) {
                    Ok(code) => (
                        IndustryClassificationDisposition::Complete,
                        Some(industry_classification_record(
                            &observation,
                            scheme,
                            version,
                            code,
                        )?),
                        None,
                    ),
                    Err(_) => (
                        IndustryClassificationDisposition::Unavailable,
                        None,
                        Some(IndustryClassificationExclusionReason::InvalidCode),
                    ),
                },
            }
        }
    };
    finish_industry_classification_selection(
        query_digest,
        knowledge_at,
        disposition,
        classification,
        Some(entry),
        reason,
    )
}

#[allow(clippy::too_many_arguments)]
fn select_industry_cohort(
    connection: &Connection,
    result_limits: super::CatalogResultLimits,
    company_source_id: &SourceId,
    company_surface: CompanyIdentitySurface,
    knowledge_at: Timestamp,
    scheme: IndustryClassificationScheme,
    version: IndustryClassificationVersion,
    code: &IndustryClassificationCode,
    maximum_members: usize,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<IndustryCohortSelection, CompanySecurityIdentityCatalogError> {
    check_operation(deadline, cancellation)?;
    if !valid_industry_scheme_version(scheme, version)
        || maximum_members == 0
        || maximum_members > MAX_INDUSTRY_COHORT_MEMBERS
    {
        return Err(CompanySecurityIdentityCatalogError::InvalidInput);
    }
    let retrieval_limit = i64::try_from(MAX_INDUSTRY_COHORT_SCAN_ROWS + 1)
        .map_err(|_| CompanySecurityIdentityCatalogError::ResultLimitExceeded)?;
    let mut statement = connection.prepare(INDUSTRY_COHORT_OBSERVATIONS_SQL)?;
    let rows = statement.query_map(
        params![
            code.as_str(),
            company_source_id.as_str(),
            company_surface.database_name(),
            knowledge_at.unix_nanos(),
            retrieval_limit,
        ],
        decode_stored_cohort_observation,
    )?;
    let mut budget = ResultBudget::new(result_limits);
    let mut observations = Vec::new();
    observations
        .try_reserve_exact(MAX_INDUSTRY_COHORT_SCAN_ROWS + 1)
        .map_err(|_| CompanySecurityIdentityCatalogError::ResultLimitExceeded)?;
    for row in rows {
        check_operation(deadline, cancellation)?;
        let row = row?;
        charge_stored_company_observation(&row.observation, &mut budget)?;
        budget
            .charge([row.term_display.len(), row.term_normalized.len()])
            .map_err(|_| CompanySecurityIdentityCatalogError::ResultLimitExceeded)?;
        let observation = rebuild_company_observation(row.observation)?;
        if row.term_display != code.as_str()
            || row.term_normalized != code.as_str()
            || observation.observation.sic() != Some(code.as_str())
            || observation.observation.source_id() != company_source_id
            || observation.observation.surface() != company_surface
            || !company_observation_is_knowable(&observation, knowledge_at)
        {
            return Err(CompanySecurityIdentityCatalogError::CorruptCatalog);
        }
        observations.push(observation);
    }
    let scan_truncated = observations.len() > MAX_INDUSTRY_COHORT_SCAN_ROWS;
    observations.truncate(MAX_INDUSTRY_COHORT_SCAN_ROWS);
    let scan_boundary_digest = observations.last().map(|value| value.parent.digest);
    observations.sort_by(compare_company_observation_records);
    if observations
        .windows(2)
        .any(|pair| pair[0].parent.digest == pair[1].parent.digest)
    {
        return Err(CompanySecurityIdentityCatalogError::CorruptCatalog);
    }

    let mut members = Vec::new();
    let mut member_entries = Vec::new();
    let mut exclusions = Vec::new();
    members
        .try_reserve_exact(observations.len().min(MAX_INDUSTRY_COHORT_MEMBERS + 1))
        .map_err(|_| CompanySecurityIdentityCatalogError::ResultLimitExceeded)?;
    member_entries
        .try_reserve_exact(observations.len().min(MAX_INDUSTRY_COHORT_MEMBERS + 1))
        .map_err(|_| CompanySecurityIdentityCatalogError::ResultLimitExceeded)?;
    exclusions
        .try_reserve_exact(observations.len())
        .map_err(|_| CompanySecurityIdentityCatalogError::ResultLimitExceeded)?;
    let mut group_start = 0;
    while group_start < observations.len() {
        check_operation(deadline, cancellation)?;
        let provider_company_id = observations[group_start].observation.provider_company_id();
        let mut group_end = group_start + 1;
        while group_end < observations.len()
            && observations[group_end].observation.provider_company_id() == provider_company_id
        {
            group_end += 1;
        }
        let (current_company, parent_ambiguous) = match current_company_parent(
            connection,
            company_source_id,
            provider_company_id,
            company_surface,
            knowledge_at,
        ) {
            Ok(Some(parent)) => (Some(parent), false),
            Ok(None) => return Err(CompanySecurityIdentityCatalogError::CorruptCatalog),
            Err(CompanySecurityIdentityCatalogError::AmbiguousParent) => (None, true),
            Err(error) => return Err(error),
        };
        for observation in &observations[group_start..group_end] {
            let entry = industry_classification_receipt_entry(observation, current_company);
            if parent_ambiguous {
                exclusions.push(IndustryCohortExclusion {
                    entry,
                    reason: IndustryClassificationExclusionReason::AmbiguousParent,
                });
            } else if current_company.map(|value| value.digest) != Some(observation.parent.digest) {
                exclusions.push(IndustryCohortExclusion {
                    entry,
                    reason: IndustryClassificationExclusionReason::StaleParent,
                });
            } else {
                members.push(industry_classification_record(
                    observation,
                    scheme,
                    version,
                    code.clone(),
                )?);
                member_entries.push(entry);
            }
        }
        group_start = group_end;
    }
    let member_limit_truncated = members.len() > maximum_members;
    members.truncate(maximum_members);
    member_entries.truncate(maximum_members);
    let completeness = if scan_truncated || member_limit_truncated {
        IndustryCohortCompleteness::Truncated
    } else {
        IndustryCohortCompleteness::Complete
    };
    let query_digest = industry_cohort_query_digest(
        company_source_id,
        company_surface,
        scheme,
        version,
        code,
        maximum_members,
    );
    let receipt_digest = industry_cohort_receipt_digest(
        query_digest,
        knowledge_at,
        completeness,
        scan_truncated,
        member_limit_truncated,
        scan_boundary_digest,
        &member_entries,
        &exclusions,
    );
    let receipt = IndustryCohortSelectionReceipt {
        query_digest,
        knowledge_at,
        completeness,
        scan_truncated,
        member_limit_truncated,
        scan_boundary_digest,
        ordered_members: member_entries.into_boxed_slice(),
        ordered_exclusions: exclusions.clone().into_boxed_slice(),
        receipt_digest,
    };
    Ok(IndustryCohortSelection {
        members: members.into_boxed_slice(),
        exclusions: exclusions.into_boxed_slice(),
        receipt,
    })
}

fn select_instrument_company_links(
    connection: &Connection,
    result_limits: super::CatalogResultLimits,
    instrument_id: InstrumentId,
    company_source_id: &SourceId,
    company_surface: CompanyIdentitySurface,
    knowledge_at: Timestamp,
    required_suitability: CommonEquitySuitability,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<CompanySecurityIdentitySelection, CompanySecurityIdentityCatalogError> {
    check_operation(deadline, cancellation)?;
    if required_suitability != CommonEquitySuitability::SuitableIssuerCommonEquity {
        return Err(CompanySecurityIdentityCatalogError::InvalidInput);
    }
    let retrieval_limit = i64::try_from(MAX_INSTRUMENT_RELATIONSHIP_HISTORY_ROWS + 1)
        .map_err(|_| CompanySecurityIdentityCatalogError::ResultLimitExceeded)?;
    let mut statement = connection.prepare(INSTRUMENT_LINK_HISTORY_SQL)?;
    let rows = statement.query_map(
        params![
            instrument_id.to_string(),
            knowledge_at.unix_nanos(),
            retrieval_limit
        ],
        decode_stored_link,
    )?;
    let mut budget = ResultBudget::new(result_limits);
    let mut history = Vec::new();
    history
        .try_reserve_exact(MAX_INSTRUMENT_RELATIONSHIP_HISTORY_ROWS + 1)
        .map_err(|_| CompanySecurityIdentityCatalogError::ResultLimitExceeded)?;
    for row in rows {
        check_operation(deadline, cancellation)?;
        let row = row?;
        charge_stored_link(&row, &mut budget)?;
        history.push(rebuild_link(row)?);
    }
    if history.len() > MAX_INSTRUMENT_RELATIONSHIP_HISTORY_ROWS {
        return Err(CompanySecurityIdentityCatalogError::ResultLimitExceeded);
    }
    history.retain(|record| {
        record.link().company_source_id() == company_source_id
            && record.link().company_surface() == company_surface
    });
    history.sort_by(compare_relationship_records);

    let mut records = Vec::new();
    records
        .try_reserve_exact(history.len().min(MAX_COMPANY_SECURITY_SELECTION_ROWS))
        .map_err(|_| CompanySecurityIdentityCatalogError::ResultLimitExceeded)?;
    let mut group_start = 0;
    let mut relationship_keys = 0_usize;
    while group_start < history.len() {
        check_operation(deadline, cancellation)?;
        let provider_company_id = history[group_start].link().provider_company_id();
        let mut group_end = group_start + 1;
        while group_end < history.len()
            && history[group_end].link().provider_company_id() == provider_company_id
        {
            group_end += 1;
        }
        relationship_keys = relationship_keys
            .checked_add(1)
            .ok_or(CompanySecurityIdentityCatalogError::ResultLimitExceeded)?;
        if relationship_keys > MAX_COMPANY_SECURITY_SELECTION_ROWS {
            return Err(CompanySecurityIdentityCatalogError::ResultLimitExceeded);
        }
        let relationship_history = &history[group_start..group_end];
        validate_relationship_history(
            connection,
            relationship_history,
            instrument_id,
            company_source_id,
            company_surface,
            knowledge_at,
        )?;
        records.push(
            relationship_history
                .last()
                .ok_or(CompanySecurityIdentityCatalogError::CorruptCatalog)?
                .clone(),
        );
        group_start = group_end;
    }

    let mut candidates = Vec::new();
    let mut exclusions = Vec::new();
    let mut candidate_receipts = Vec::new();
    let mut exclusion_receipts = Vec::new();
    candidates
        .try_reserve_exact(records.len())
        .map_err(|_| CompanySecurityIdentityCatalogError::ResultLimitExceeded)?;
    exclusions
        .try_reserve_exact(records.len())
        .map_err(|_| CompanySecurityIdentityCatalogError::ResultLimitExceeded)?;
    candidate_receipts
        .try_reserve_exact(records.len())
        .map_err(|_| CompanySecurityIdentityCatalogError::ResultLimitExceeded)?;
    exclusion_receipts
        .try_reserve_exact(records.len())
        .map_err(|_| CompanySecurityIdentityCatalogError::ResultLimitExceeded)?;
    for record in records {
        check_operation(deadline, cancellation)?;
        let linked_company = exact_company_parent(
            connection,
            record.link().company_observation_digest(),
            record.link().company_source_id(),
            record.link().provider_company_id(),
            record.link().company_surface(),
        )?
        .ok_or(CompanySecurityIdentityCatalogError::CorruptCatalog)?;
        let linked_market = exact_market_parent(
            connection,
            record.link().market_instrument_revision_digest(),
            record.link().instrument_id(),
        )?
        .ok_or(CompanySecurityIdentityCatalogError::CorruptCatalog)?;
        let (current_company, company_parent_ambiguous) = match current_company_parent(
            connection,
            record.link().company_source_id(),
            record.link().provider_company_id(),
            record.link().company_surface(),
            knowledge_at,
        ) {
            Ok(parent) => (parent, false),
            Err(CompanySecurityIdentityCatalogError::AmbiguousParent) => (None, true),
            Err(error) => return Err(error),
        };
        let current_market = current_market_parent(
            connection,
            record.link().instrument_id(),
            knowledge_at,
            knowledge_at,
        )?;
        let receipt_entry = receipt_entry(
            &record,
            linked_company,
            current_company,
            &linked_market,
            current_market.as_ref(),
        );
        let reason = exclusion_reason(
            &record,
            required_suitability == CommonEquitySuitability::SuitableIssuerCommonEquity,
            knowledge_at,
            knowledge_at,
            linked_company,
            current_company,
            company_parent_ambiguous,
            &linked_market,
            current_market.as_ref(),
        );
        if let Some(reason) = reason {
            exclusion_receipts.push((receipt_entry, reason));
            exclusions.push(CompanySecurityIdentityExclusion { record, reason });
        } else {
            candidate_receipts.push(receipt_entry);
            candidates.push(record);
        }
    }
    finish_selection(
        instrument_company_query_digest(
            instrument_id,
            company_source_id,
            company_surface,
            required_suitability,
        ),
        INSTRUMENT_COMPANY_SELECTION_RECEIPT_DOMAIN,
        knowledge_at,
        knowledge_at,
        candidates,
        exclusions,
        candidate_receipts,
        exclusion_receipts,
    )
}

fn validate_relationship_history(
    connection: &Connection,
    history: &[CompanySecurityIdentityRecord],
    instrument_id: InstrumentId,
    company_source_id: &SourceId,
    company_surface: CompanyIdentitySurface,
    knowledge_at: Timestamp,
) -> Result<(), CompanySecurityIdentityCatalogError> {
    let Some(latest) = history.last() else {
        return Err(CompanySecurityIdentityCatalogError::CorruptCatalog);
    };
    if history.len() > MAX_LINK_EVENTS_PER_RELATIONSHIP as usize {
        return Err(CompanySecurityIdentityCatalogError::CorruptCatalog);
    }
    for (index, record) in history.iter().enumerate() {
        let expected_sequence = u32::try_from(index + 1)
            .map_err(|_| CompanySecurityIdentityCatalogError::CorruptCatalog)?;
        if record.event_sequence() != expected_sequence
            || record.link().instrument_id() != instrument_id
            || record.link().company_source_id() != company_source_id
            || record.link().company_surface() != company_surface
            || record.link().provider_company_id() != latest.link().provider_company_id()
            || (index == 0
                && !matches!(
                    record.link().transition(),
                    CompanySecurityLinkTransition::Initial
                ))
            || (index > 0
                && record.link().transition().previous_link_digest()
                    != Some(history[index - 1].link_digest()))
            || (index > 0 && record.published_at() < history[index - 1].published_at())
        {
            return Err(CompanySecurityIdentityCatalogError::CorruptCatalog);
        }
    }
    let current = load_current_link(
        connection,
        company_source_id,
        latest.link().provider_company_id(),
        company_surface,
        instrument_id,
    )?
    .ok_or(CompanySecurityIdentityCatalogError::CorruptCatalog)?;
    if current.event_sequence() < latest.event_sequence()
        || current.published_at() < latest.published_at()
        || (current.event_sequence() == latest.event_sequence() && &current != latest)
        || (current.event_sequence() > latest.event_sequence()
            && current.published_at() <= knowledge_at)
    {
        return Err(CompanySecurityIdentityCatalogError::CorruptCatalog);
    }
    Ok(())
}

fn select_links(
    connection: &Connection,
    result_limits: super::CatalogResultLimits,
    query: &CompanySecurityIdentityQuery,
    effective_at: Timestamp,
    knowledge_at: Timestamp,
    current_projection: bool,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<CompanySecurityIdentitySelection, CompanySecurityIdentityCatalogError> {
    check_operation(deadline, cancellation)?;
    let sql = if current_projection {
        CURRENT_LINKS_SQL
    } else {
        AS_OF_LINKS_SQL
    };
    let instrument = query.instrument_id().map(|value| value.to_string());
    let retrieval_limit = i64::try_from(MAX_COMPANY_SECURITY_SELECTION_ROWS + 1)
        .map_err(|_| CompanySecurityIdentityCatalogError::ResultLimitExceeded)?;
    let mut statement = connection.prepare(sql)?;
    let mut rows = if current_projection {
        statement.query(params![
            query.company_source_id().as_str(),
            query.provider_company_id().as_str(),
            query.company_surface().database_name(),
            instrument,
            knowledge_at.unix_nanos(),
            retrieval_limit,
        ])?
    } else {
        statement.query(params![
            query.company_source_id().as_str(),
            query.provider_company_id().as_str(),
            query.company_surface().database_name(),
            instrument,
            knowledge_at.unix_nanos(),
            effective_at.unix_nanos(),
            retrieval_limit,
        ])?
    };
    let mut budget = ResultBudget::new(result_limits);
    let mut records = Vec::new();
    records
        .try_reserve_exact(MAX_COMPANY_SECURITY_SELECTION_ROWS + 1)
        .map_err(|_| CompanySecurityIdentityCatalogError::ResultLimitExceeded)?;
    while let Some(row) = rows.next()? {
        check_operation(deadline, cancellation)?;
        let row = decode_stored_link(row)?;
        charge_stored_link(&row, &mut budget)?;
        records.push(rebuild_link(row)?);
    }
    if records.len() > MAX_COMPANY_SECURITY_SELECTION_ROWS {
        return Err(CompanySecurityIdentityCatalogError::ResultLimitExceeded);
    }
    records.sort_by(compare_relationship_records);

    let (current_company, company_parent_ambiguous) = match current_company_parent(
        connection,
        query.company_source_id(),
        query.provider_company_id(),
        query.company_surface(),
        knowledge_at,
    ) {
        Ok(parent) => (parent, false),
        Err(CompanySecurityIdentityCatalogError::AmbiguousParent) => (None, true),
        Err(error) => return Err(error),
    };
    let mut candidates = Vec::new();
    let mut exclusions = Vec::new();
    let mut candidate_receipts = Vec::new();
    let mut exclusion_receipts = Vec::new();
    candidates
        .try_reserve_exact(records.len())
        .map_err(|_| CompanySecurityIdentityCatalogError::ResultLimitExceeded)?;
    exclusions
        .try_reserve_exact(records.len())
        .map_err(|_| CompanySecurityIdentityCatalogError::ResultLimitExceeded)?;
    candidate_receipts
        .try_reserve_exact(records.len())
        .map_err(|_| CompanySecurityIdentityCatalogError::ResultLimitExceeded)?;
    exclusion_receipts
        .try_reserve_exact(records.len())
        .map_err(|_| CompanySecurityIdentityCatalogError::ResultLimitExceeded)?;
    for record in records {
        check_operation(deadline, cancellation)?;
        let linked_company = exact_company_parent(
            connection,
            record.link().company_observation_digest(),
            record.link().company_source_id(),
            record.link().provider_company_id(),
            record.link().company_surface(),
        )?
        .ok_or(CompanySecurityIdentityCatalogError::CorruptCatalog)?;
        let linked_market = exact_market_parent(
            connection,
            record.link().market_instrument_revision_digest(),
            record.link().instrument_id(),
        )?
        .ok_or(CompanySecurityIdentityCatalogError::CorruptCatalog)?;
        let current_market = current_market_parent(
            connection,
            record.link().instrument_id(),
            knowledge_at,
            effective_at,
        )?;
        let receipt_entry = receipt_entry(
            &record,
            linked_company,
            current_company,
            &linked_market,
            current_market.as_ref(),
        );
        let reason = exclusion_reason(
            &record,
            query.require_suitable_common_equity(),
            effective_at,
            knowledge_at,
            linked_company,
            current_company,
            company_parent_ambiguous,
            &linked_market,
            current_market.as_ref(),
        );
        if let Some(reason) = reason {
            exclusion_receipts.push((receipt_entry, reason));
            exclusions.push(CompanySecurityIdentityExclusion { record, reason });
        } else {
            candidate_receipts.push(receipt_entry);
            candidates.push(record);
        }
    }
    finish_selection(
        query_digest(query),
        COMPANY_SECURITY_SELECTION_RECEIPT_DOMAIN,
        effective_at,
        knowledge_at,
        candidates,
        exclusions,
        candidate_receipts,
        exclusion_receipts,
    )
}

fn finish_selection(
    query_digest: EvidenceDigest,
    receipt_domain: &'static [u8],
    effective_at: Timestamp,
    knowledge_at: Timestamp,
    candidates: Vec<CompanySecurityIdentityRecord>,
    exclusions: Vec<CompanySecurityIdentityExclusion>,
    candidate_receipts: Vec<CompanySecuritySelectionReceiptEntry>,
    exclusion_receipts: Vec<(
        CompanySecuritySelectionReceiptEntry,
        CompanySecurityIdentityExclusionReason,
    )>,
) -> Result<CompanySecurityIdentitySelection, CompanySecurityIdentityCatalogError> {
    if candidates.len() != candidate_receipts.len()
        || exclusions.len() != exclusion_receipts.len()
        || candidates.len() + exclusions.len() > MAX_COMPANY_SECURITY_SELECTION_ROWS
    {
        return Err(CompanySecurityIdentityCatalogError::CorruptCatalog);
    }
    let ambiguous_parent = exclusions.iter().any(|value| {
        value.reason() == CompanySecurityIdentityExclusionReason::AmbiguousCompanyParent
    });
    let disposition = if candidates.len() > 1 || ambiguous_parent {
        CompanySecurityIdentityDisposition::Conflict
    } else if candidates.len() == 1 {
        CompanySecurityIdentityDisposition::Complete
    } else if exclusions.iter().any(|value| {
        matches!(
            value.reason(),
            CompanySecurityIdentityExclusionReason::StaleCompanyParent
                | CompanySecurityIdentityExclusionReason::StaleMarketInstrumentParent
        )
    }) {
        CompanySecurityIdentityDisposition::Stale
    } else if exclusions
        .iter()
        .any(|value| value.reason() == CompanySecurityIdentityExclusionReason::Revoked)
    {
        CompanySecurityIdentityDisposition::Revoked
    } else {
        CompanySecurityIdentityDisposition::Unavailable
    };
    let receipt_digest = selection_receipt_digest(
        receipt_domain,
        query_digest,
        effective_at,
        knowledge_at,
        disposition,
        &candidate_receipts,
        &exclusion_receipts,
    );
    let receipt = CompanySecurityIdentitySelectionReceipt {
        query_digest,
        effective_at,
        knowledge_at,
        disposition,
        ordered_candidates: candidate_receipts.into_boxed_slice(),
        ordered_exclusions: exclusion_receipts.into_boxed_slice(),
        receipt_digest,
    };
    Ok(CompanySecurityIdentitySelection {
        disposition,
        candidates: candidates.into_boxed_slice(),
        exclusions: exclusions.into_boxed_slice(),
        receipt,
    })
}

fn compare_relationship_records(
    left: &CompanySecurityIdentityRecord,
    right: &CompanySecurityIdentityRecord,
) -> Ordering {
    let left_link = left.link();
    let right_link = right.link();
    left_link
        .company_source_id()
        .cmp(right_link.company_source_id())
        .then_with(|| {
            left_link
                .provider_company_id()
                .cmp(right_link.provider_company_id())
        })
        .then_with(|| {
            company_surface_tag(left_link.company_surface())
                .cmp(&company_surface_tag(right_link.company_surface()))
        })
        .then_with(|| left_link.instrument_id().cmp(&right_link.instrument_id()))
        .then_with(|| left.event_sequence().cmp(&right.event_sequence()))
        .then_with(|| left.link_digest().bytes().cmp(&right.link_digest().bytes()))
}

fn exclusion_reason(
    record: &CompanySecurityIdentityRecord,
    require_suitable_common_equity: bool,
    effective_at: Timestamp,
    knowledge_at: Timestamp,
    linked_company: CompanyParent,
    current_company: Option<CompanyParent>,
    company_parent_ambiguous: bool,
    linked_market: &MarketParent,
    current_market: Option<&MarketParent>,
) -> Option<CompanySecurityIdentityExclusionReason> {
    let link = record.link();
    if link.available_at() > knowledge_at
        || link.ingested_at() > knowledge_at
        || record.published_at() > knowledge_at
        || linked_company.available_at.is_none()
        || linked_company
            .available_at
            .is_some_and(|available_at| available_at > knowledge_at)
        || linked_company.ingested_at > knowledge_at
        || linked_company.completed_at > knowledge_at
        || linked_market.published_at > knowledge_at
    {
        return Some(CompanySecurityIdentityExclusionReason::NotYetAvailable);
    }
    if link.effective_interval().starts_at() > effective_at {
        return Some(CompanySecurityIdentityExclusionReason::NotYetEffective);
    }
    if link
        .effective_interval()
        .ends_at()
        .is_some_and(|end| end <= effective_at)
    {
        return Some(CompanySecurityIdentityExclusionReason::NoLongerEffective);
    }
    if require_suitable_common_equity
        && link.common_equity_suitability() != CommonEquitySuitability::SuitableIssuerCommonEquity
    {
        return Some(CompanySecurityIdentityExclusionReason::NotSuitableCommonEquity);
    }
    if link.transition().is_revocation() {
        return Some(CompanySecurityIdentityExclusionReason::Revoked);
    }
    if company_parent_ambiguous {
        return Some(CompanySecurityIdentityExclusionReason::AmbiguousCompanyParent);
    }
    if current_company.map(|value| value.digest) != Some(link.company_observation_digest()) {
        return Some(CompanySecurityIdentityExclusionReason::StaleCompanyParent);
    }
    if current_market.map(|value| value.digest) != Some(link.market_instrument_revision_digest()) {
        return Some(CompanySecurityIdentityExclusionReason::StaleMarketInstrumentParent);
    }
    None
}

fn market_interval_covers(
    market: &MarketParent,
    relationship: market_squawk_domain::EffectiveInterval,
) -> bool {
    if relationship.starts_at() < market.effective_start {
        return false;
    }
    match (market.effective_end, relationship.ends_at()) {
        (None, _) => true,
        (Some(_), None) => false,
        (Some(market_end), Some(relationship_end)) => relationship_end <= market_end,
    }
}

fn direct_crosswalk_is_reference_backed(
    link: &CompanySecurityIdentityLink,
    definition: &MarketDataInstrumentDefinition,
) -> bool {
    let CompanySecurityResolutionBasis::DirectAuthoritativeCrosswalk {
        authority_source_id,
        authority_revision,
        evidence,
    } = link.resolution_basis()
    else {
        return false;
    };
    definition.identifiers().iter().any(|record| {
        matches!(
            record.identifier(),
            ExternalIdentifier::Cusip(_)
                | ExternalIdentifier::Isin(_)
                | ExternalIdentifier::Sedol(_)
                | ExternalIdentifier::Figi(_)
        ) && record.assignment_verification() == AssignmentVerification::VerifiedAssigned
            && record.source_id() == authority_source_id
            && record.source_evidence() == evidence
            && record
                .source_evidence()
                .version_pinned_locator()
                .is_some_and(|locator| locator.version() == authority_revision)
            && record.observed_at() <= link.available_at()
            && record
                .source_timestamp()
                .is_none_or(|timestamp| timestamp <= link.available_at())
            && record.rights_policy().entitlement() != IdentifierEntitlement::UnknownOrRestricted
            && interval_covers(record.validity(), link.effective_interval())
    })
}

fn interval_covers(
    authority: market_squawk_domain::EffectiveInterval,
    relationship: market_squawk_domain::EffectiveInterval,
) -> bool {
    if relationship.starts_at() < authority.starts_at() {
        return false;
    }
    match (authority.ends_at(), relationship.ends_at()) {
        (None, _) => true,
        (Some(_), None) => false,
        (Some(authority_end), Some(relationship_end)) => relationship_end <= authority_end,
    }
}

fn receipt_entry(
    record: &CompanySecurityIdentityRecord,
    linked_company: CompanyParent,
    current_company: Option<CompanyParent>,
    linked_market: &MarketParent,
    current_market: Option<&MarketParent>,
) -> CompanySecuritySelectionReceiptEntry {
    let link = record.link();
    CompanySecuritySelectionReceiptEntry {
        link_digest: record.link_digest(),
        event_sequence: record.event_sequence(),
        previous_link_digest: link.transition().previous_link_digest(),
        company_source_id: link.company_source_id().clone(),
        provider_company_id: link.provider_company_id().clone(),
        company_surface: link.company_surface(),
        linked_company_observation_digest: link.company_observation_digest(),
        current_company_observation_digest: current_company.map(|value| value.digest),
        current_company_available_at: current_company.and_then(|value| value.available_at),
        current_company_ingested_at: current_company.map(|value| value.ingested_at),
        current_company_completed_at: current_company.map(|value| value.completed_at),
        linked_market_revision_digest: link.market_instrument_revision_digest(),
        instrument_id: link.instrument_id(),
        security_kind: link.security_kind(),
        relationship_kind: link.relationship_kind(),
        common_equity_suitability: link.common_equity_suitability(),
        current_market_revision_digest: current_market.map(|value| value.digest),
        current_market_published_at: current_market.map(|value| value.published_at),
        current_market_effective_start: current_market.map(|value| value.effective_start),
        current_market_effective_end: current_market.and_then(|value| value.effective_end),
        company_available_at: linked_company.available_at,
        company_ingested_at: linked_company.ingested_at,
        company_completed_at: linked_company.completed_at,
        market_published_at: linked_market.published_at,
        market_effective_start: linked_market.effective_start,
        market_effective_end: linked_market.effective_end,
        link_available_at: link.available_at(),
        link_ingested_at: link.ingested_at(),
        link_published_at: record.published_at(),
        effective_start: link.effective_interval().starts_at(),
        effective_end: link.effective_interval().ends_at(),
        rights_policy_id: link.relationship_evidence_rights().policy_id().clone(),
        rights_entitlement: link.relationship_evidence_rights().entitlement(),
        rights_terms_reference: link
            .relationship_evidence_rights()
            .terms_reference()
            .clone(),
    }
}

fn load_exact_company_observation(
    connection: &Connection,
    digest: EvidenceDigest,
    source_id: &SourceId,
    surface: CompanyIdentitySurface,
    budget: &mut ResultBudget,
) -> Result<Option<CompanyObservationRecord>, CompanySecurityIdentityCatalogError> {
    if digest.algorithm() != DigestAlgorithm::Sha256 || digest.bytes() == [0; 32] {
        return Err(CompanySecurityIdentityCatalogError::InvalidInput);
    }
    let row = connection
        .query_row(
            EXACT_COMPANY_OBSERVATION_SQL,
            params![digest.bytes(), source_id.as_str(), surface.database_name()],
            decode_stored_company_observation,
        )
        .optional()?;
    row.map(|row| {
        charge_stored_company_observation(&row, budget)?;
        rebuild_company_observation(row)
    })
    .transpose()
}

fn decode_stored_company_observation(
    row: &Row<'_>,
) -> rusqlite::Result<StoredCompanyObservationRow> {
    Ok(StoredCompanyObservationRow {
        digest: row.get(0)?,
        available_at: row.get(1)?,
        ingested_at: row.get(2)?,
        completed_at: row.get(3)?,
        json: row.get(4)?,
        source_id: row.get(5)?,
        provider_company_id: row.get(6)?,
        source_surface: row.get(7)?,
    })
}

fn decode_stored_cohort_observation(row: &Row<'_>) -> rusqlite::Result<StoredCohortObservationRow> {
    Ok(StoredCohortObservationRow {
        observation: decode_stored_company_observation(row)?,
        term_display: row.get(8)?,
        term_normalized: row.get(9)?,
    })
}

fn charge_stored_company_observation(
    row: &StoredCompanyObservationRow,
    budget: &mut ResultBudget,
) -> Result<(), CompanySecurityIdentityCatalogError> {
    budget
        .charge([
            size_of::<CompanyObservationRecord>(),
            row.digest.len(),
            row.json.len(),
            row.source_id.len(),
            row.provider_company_id.len(),
            row.source_surface.len(),
        ])
        .map_err(|_| CompanySecurityIdentityCatalogError::ResultLimitExceeded)
}

fn rebuild_company_observation(
    row: StoredCompanyObservationRow,
) -> Result<CompanyObservationRecord, CompanySecurityIdentityCatalogError> {
    let digest = array_digest(&row.digest)?;
    let observation: CompanyIdentityObservation = serde_json::from_str(&row.json)
        .map_err(|_| CompanySecurityIdentityCatalogError::CorruptCatalog)?;
    let available_at = row.available_at.map(Timestamp::from_unix_nanos);
    let ingested_at = Timestamp::from_unix_nanos(row.ingested_at);
    let completed_at = Timestamp::from_unix_nanos(row.completed_at);
    if sha256(row.json.as_bytes()) != digest
        || serde_json::to_string(&observation)
            .map_err(|_| CompanySecurityIdentityCatalogError::CorruptCatalog)?
            != row.json
        || observation.source_id().as_str() != row.source_id
        || observation.provider_company_id().as_str() != row.provider_company_id
        || observation.surface().database_name() != row.source_surface
        || observation.availability().conservative_available_at() != available_at
        || observation.ingested_at() != ingested_at
        || observation.received_at() > ingested_at
        || available_at.is_some_and(|value| value > ingested_at)
        || ingested_at > completed_at
    {
        return Err(CompanySecurityIdentityCatalogError::CorruptCatalog);
    }
    Ok(CompanyObservationRecord {
        observation,
        parent: CompanyParent {
            digest: evidence_digest(digest),
            available_at,
            ingested_at,
            completed_at,
        },
    })
}

fn compare_company_observation_records(
    left: &CompanyObservationRecord,
    right: &CompanyObservationRecord,
) -> Ordering {
    left.observation
        .provider_company_id()
        .cmp(right.observation.provider_company_id())
        .then_with(|| left.parent.digest.bytes().cmp(&right.parent.digest.bytes()))
}

fn company_observation_is_knowable(
    observation: &CompanyObservationRecord,
    knowledge_at: Timestamp,
) -> bool {
    observation
        .parent
        .available_at
        .is_some_and(|value| value <= knowledge_at)
        && observation.observation.received_at() <= knowledge_at
        && observation.parent.ingested_at <= knowledge_at
        && observation.parent.completed_at <= knowledge_at
}

fn industry_classification_record(
    observation: &CompanyObservationRecord,
    scheme: IndustryClassificationScheme,
    version: IndustryClassificationVersion,
    code: IndustryClassificationCode,
) -> Result<IndustryClassificationRecord, CompanySecurityIdentityCatalogError> {
    let available_at = observation
        .parent
        .available_at
        .ok_or(CompanySecurityIdentityCatalogError::CorruptCatalog)?;
    if !valid_industry_scheme_version(scheme, version)
        || observation.observation.sic() != Some(code.as_str())
    {
        return Err(CompanySecurityIdentityCatalogError::CorruptCatalog);
    }
    let locator = observation
        .observation
        .identity_payload_evidence()
        .version_pinned_locator();
    let classification_evidence_digest = observation
        .observation
        .identity_payload_evidence()
        .content_digest();
    let parent_ingest_evidence_digest = observation
        .observation
        .parent_ingest_payload_evidence()
        .content_digest();
    if classification_evidence_digest.bytes() == [0; 32]
        || parent_ingest_evidence_digest.bytes() == [0; 32]
    {
        return Err(CompanySecurityIdentityCatalogError::CorruptCatalog);
    }
    Ok(IndustryClassificationRecord {
        scheme,
        version,
        code,
        source_description: observation.observation.sic_description().map(str::to_owned),
        company_source_id: observation.observation.source_id().clone(),
        provider_company_id: observation.observation.provider_company_id().clone(),
        company_surface: observation.observation.surface(),
        company_observation_digest: observation.parent.digest,
        source_record_reference: locator.map(|value| value.reference().clone()),
        source_record_version: locator.map(|value| value.version().clone()),
        classification_evidence_digest,
        parent_ingest_evidence_digest,
        // SEC company metadata supplies no separate historical SIC effective date. V1 therefore
        // uses the conservative source-availability boundary and makes no earlier claim.
        effective_at: available_at,
        available_at,
        ingested_at: observation.parent.ingested_at,
        published_at: observation.parent.completed_at,
    })
}

fn industry_classification_receipt_entry(
    observation: &CompanyObservationRecord,
    current: Option<CompanyParent>,
) -> IndustryClassificationReceiptEntry {
    let locator = observation
        .observation
        .identity_payload_evidence()
        .version_pinned_locator();
    IndustryClassificationReceiptEntry {
        company_observation_digest: observation.parent.digest,
        current_company_observation_digest: current.map(|value| value.digest),
        company_source_id: observation.observation.source_id().clone(),
        provider_company_id: observation.observation.provider_company_id().clone(),
        company_surface: observation.observation.surface(),
        reported_code: observation.observation.sic().map(str::to_owned),
        source_description: observation.observation.sic_description().map(str::to_owned),
        source_record_reference: locator.map(|value| value.reference().clone()),
        source_record_version: locator.map(|value| value.version().clone()),
        classification_evidence_digest: observation
            .observation
            .identity_payload_evidence()
            .content_digest(),
        parent_ingest_evidence_digest: observation
            .observation
            .parent_ingest_payload_evidence()
            .content_digest(),
        effective_at: observation.parent.available_at,
        available_at: observation.parent.available_at,
        received_at: observation.observation.received_at(),
        ingested_at: observation.parent.ingested_at,
        published_at: observation.parent.completed_at,
        current_available_at: current.and_then(|value| value.available_at),
        current_ingested_at: current.map(|value| value.ingested_at),
        current_published_at: current.map(|value| value.completed_at),
    }
}

fn exact_company_parent(
    connection: &Connection,
    digest: EvidenceDigest,
    source_id: &SourceId,
    provider_company_id: &SourceIdentifier,
    surface: CompanyIdentitySurface,
) -> Result<Option<CompanyParent>, CompanySecurityIdentityCatalogError> {
    if digest.algorithm() != DigestAlgorithm::Sha256 || digest.bytes() == [0; 32] {
        return Err(CompanySecurityIdentityCatalogError::InvalidInput);
    }
    connection
        .query_row(
            "SELECT observations.record_digest, observations.available_at_ns,
                observations.ingested_at_ns, runs.completed_at_ns, observations.record_json,
                observations.source_id, observations.provider_company_id,
                observations.source_surface
         FROM company_identity_observations AS observations
         JOIN ingest_runs AS runs ON runs.run_id=observations.run_id
         WHERE observations.record_digest=?1 AND observations.source_id=?2
           AND observations.provider_company_id=?3 AND observations.source_surface=?4
           AND runs.state='succeeded'",
            params![
                digest.bytes(),
                source_id.as_str(),
                provider_company_id.as_str(),
                surface.database_name()
            ],
            decode_company_parent,
        )
        .optional()
        .map_err(Into::into)
}

fn current_company_parent(
    connection: &Connection,
    source_id: &SourceId,
    provider_company_id: &SourceIdentifier,
    surface: CompanyIdentitySurface,
    knowledge_at: Timestamp,
) -> Result<Option<CompanyParent>, CompanySecurityIdentityCatalogError> {
    let parent = connection
        .query_row(
            "SELECT observations.record_digest, observations.available_at_ns,
                observations.ingested_at_ns, runs.completed_at_ns, observations.record_json,
                observations.source_id, observations.provider_company_id,
                observations.source_surface
         FROM company_identity_observations AS observations
         JOIN ingest_runs AS runs ON runs.run_id=observations.run_id
         WHERE observations.source_id=?1 AND observations.provider_company_id=?2
           AND observations.source_surface=?3 AND runs.state='succeeded'
           AND runs.completed_at_ns<=?4 AND observations.ingested_at_ns<=?4
           AND observations.available_at_ns IS NOT NULL
           AND observations.available_at_ns<=?4
         ORDER BY runs.completed_at_ns DESC, observations.ingested_at_ns DESC,
                  hex(observations.record_digest) LIMIT 1",
            params![
                source_id.as_str(),
                provider_company_id.as_str(),
                surface.database_name(),
                knowledge_at.unix_nanos()
            ],
            decode_company_parent,
        )
        .optional()
        .map_err(CompanySecurityIdentityCatalogError::from)?;
    if let Some(parent) = parent {
        let additional_tied_parent: i64 = connection.query_row(
            "SELECT EXISTS (
                 SELECT 1
                 FROM company_identity_observations AS observations
                 JOIN ingest_runs AS runs ON runs.run_id=observations.run_id
                 WHERE observations.source_id=?1 AND observations.provider_company_id=?2
                   AND observations.source_surface=?3 AND runs.state='succeeded'
                   AND runs.completed_at_ns<=?4 AND observations.ingested_at_ns<=?4
                   AND observations.available_at_ns IS NOT NULL
                   AND observations.available_at_ns<=?4
                   AND runs.completed_at_ns=?5 AND observations.ingested_at_ns=?6
                 LIMIT 1 OFFSET 1
             )",
            params![
                source_id.as_str(),
                provider_company_id.as_str(),
                surface.database_name(),
                knowledge_at.unix_nanos(),
                parent.completed_at.unix_nanos(),
                parent.ingested_at.unix_nanos(),
            ],
            |row| row.get(0),
        )?;
        if additional_tied_parent != 0 {
            return Err(CompanySecurityIdentityCatalogError::AmbiguousParent);
        }
        Ok(Some(parent))
    } else {
        Ok(None)
    }
}

fn decode_company_parent(row: &Row<'_>) -> rusqlite::Result<CompanyParent> {
    let bytes: Vec<u8> = row.get(0)?;
    let bytes: [u8; 32] = bytes
        .try_into()
        .map_err(|_| rusqlite::Error::InvalidQuery)?;
    if bytes == [0; 32] {
        return Err(rusqlite::Error::InvalidQuery);
    }
    let json: String = row.get(4)?;
    let observation: CompanyIdentityObservation =
        serde_json::from_str(&json).map_err(|_| rusqlite::Error::InvalidQuery)?;
    let available_at = row
        .get::<_, Option<i64>>(1)?
        .map(Timestamp::from_unix_nanos);
    let ingested_at = Timestamp::from_unix_nanos(row.get(2)?);
    if sha256(json.as_bytes()) != bytes
        || serde_json::to_string(&observation).map_err(|_| rusqlite::Error::InvalidQuery)? != json
        || observation.source_id().as_str() != row.get::<_, String>(5)?
        || observation.provider_company_id().as_str() != row.get::<_, String>(6)?
        || observation.surface().database_name() != row.get::<_, String>(7)?
        || observation.availability().conservative_available_at() != available_at
        || observation.ingested_at() != ingested_at
    {
        return Err(rusqlite::Error::InvalidQuery);
    }
    Ok(CompanyParent {
        digest: evidence_digest(bytes),
        available_at,
        ingested_at,
        completed_at: Timestamp::from_unix_nanos(row.get(3)?),
    })
}

fn exact_market_parent(
    connection: &Connection,
    digest: EvidenceDigest,
    instrument_id: InstrumentId,
) -> Result<Option<MarketParent>, CompanySecurityIdentityCatalogError> {
    if digest.algorithm() != DigestAlgorithm::Sha256 || digest.bytes() == [0; 32] {
        return Err(CompanySecurityIdentityCatalogError::InvalidInput);
    }
    connection
        .query_row(
            "SELECT revision_digest, published_at_ns, effective_start_ns, effective_end_ns,
                    definition_json, instrument_id
             FROM market_data_instrument_revisions
             WHERE revision_digest=?1 AND instrument_id=?2",
            params![digest.bytes(), instrument_id.to_string()],
            decode_market_parent,
        )
        .optional()
        .map_err(Into::into)
}

fn current_market_parent(
    connection: &Connection,
    instrument_id: InstrumentId,
    knowledge_at: Timestamp,
    effective_at: Timestamp,
) -> Result<Option<MarketParent>, CompanySecurityIdentityCatalogError> {
    connection
        .query_row(
            "SELECT revision_digest, published_at_ns, effective_start_ns, effective_end_ns,
                    definition_json, instrument_id
             FROM market_data_instrument_revisions
             WHERE instrument_id=?1 AND published_at_ns<=?2
               AND effective_start_ns<=?3
               AND (effective_end_ns IS NULL OR effective_end_ns>?3)
             ORDER BY revision_sequence DESC LIMIT 1",
            params![
                instrument_id.to_string(),
                knowledge_at.unix_nanos(),
                effective_at.unix_nanos()
            ],
            decode_market_parent,
        )
        .optional()
        .map_err(Into::into)
}

fn decode_market_parent(row: &Row<'_>) -> rusqlite::Result<MarketParent> {
    let bytes: Vec<u8> = row.get(0)?;
    let bytes: [u8; 32] = bytes
        .try_into()
        .map_err(|_| rusqlite::Error::InvalidQuery)?;
    if bytes == [0; 32] {
        return Err(rusqlite::Error::InvalidQuery);
    }
    let json: String = row.get(4)?;
    let definition: MarketDataInstrumentDefinition =
        serde_json::from_str(&json).map_err(|_| rusqlite::Error::InvalidQuery)?;
    let effective_start = Timestamp::from_unix_nanos(row.get(2)?);
    let effective_end = row
        .get::<_, Option<i64>>(3)?
        .map(Timestamp::from_unix_nanos);
    let instrument_id: String = row.get(5)?;
    if sha256(json.as_bytes()) != bytes
        || serde_json::to_string(&definition).map_err(|_| rusqlite::Error::InvalidQuery)? != json
        || definition.instrument_id().to_string() != instrument_id
        || definition.effective_interval().starts_at() != effective_start
        || definition.effective_interval().ends_at() != effective_end
    {
        return Err(rusqlite::Error::InvalidQuery);
    }
    Ok(MarketParent {
        digest: evidence_digest(bytes),
        published_at: Timestamp::from_unix_nanos(row.get(1)?),
        effective_start,
        effective_end,
        definition,
    })
}

fn load_link_by_digest(
    connection: &Connection,
    digest: [u8; 32],
) -> Result<Option<CompanySecurityIdentityRecord>, CompanySecurityIdentityCatalogError> {
    connection
        .query_row(
            &format!(
                "SELECT {STORED_COLUMNS} FROM company_security_link_events WHERE link_digest=?1"
            ),
            [digest],
            decode_stored_link,
        )
        .optional()?
        .map(rebuild_link)
        .transpose()
}

fn load_current_link(
    connection: &Connection,
    company_source_id: &SourceId,
    provider_company_id: &SourceIdentifier,
    company_surface: CompanyIdentitySurface,
    instrument_id: InstrumentId,
) -> Result<Option<CompanySecurityIdentityRecord>, CompanySecurityIdentityCatalogError> {
    connection
        .query_row(
            CURRENT_LINK_BY_KEY_SQL,
            params![
                company_source_id.as_str(),
                provider_company_id.as_str(),
                company_surface.database_name(),
                instrument_id.to_string(),
            ],
            decode_stored_link,
        )
        .optional()?
        .map(rebuild_link)
        .transpose()
}

fn decode_stored_link(row: &Row<'_>) -> rusqlite::Result<StoredLinkRow> {
    Ok(StoredLinkRow {
        digest: row.get(0)?,
        company_source_id: row.get(1)?,
        provider_company_id: row.get(2)?,
        company_surface: row.get(3)?,
        company_observation_digest: row.get(4)?,
        instrument_id: row.get(5)?,
        market_revision_digest: row.get(6)?,
        event_sequence: row.get(7)?,
        security_kind: row.get(8)?,
        relationship_kind: row.get(9)?,
        suitability: row.get(10)?,
        event_kind: row.get(11)?,
        previous_digest: row.get(12)?,
        effective_start: row.get(13)?,
        effective_end: row.get(14)?,
        resolution_kind: row.get(15)?,
        resolution_algorithm: row.get(16)?,
        resolution_digest: row.get(17)?,
        rights_policy_id: row.get(18)?,
        rights_entitlement: row.get(19)?,
        rights_terms_reference: row.get(20)?,
        available_at: row.get(21)?,
        ingested_at: row.get(22)?,
        json: row.get(23)?,
        published_at: row.get(24)?,
    })
}

fn rebuild_link(
    row: StoredLinkRow,
) -> Result<CompanySecurityIdentityRecord, CompanySecurityIdentityCatalogError> {
    let digest_bytes = array_digest(&row.digest)?;
    if sha256(row.json.as_bytes()) != digest_bytes {
        return Err(CompanySecurityIdentityCatalogError::CorruptCatalog);
    }
    let link: CompanySecurityIdentityLink = serde_json::from_str(&row.json)
        .map_err(|_| CompanySecurityIdentityCatalogError::CorruptCatalog)?;
    if serde_json::to_string(&link)
        .map_err(|_| CompanySecurityIdentityCatalogError::CorruptCatalog)?
        != row.json
        || link.company_source_id().as_str() != row.company_source_id
        || link.provider_company_id().as_str() != row.provider_company_id
        || link.company_surface().database_name() != row.company_surface
        || link.company_observation_digest().bytes().as_slice() != row.company_observation_digest
        || link.instrument_id().to_string() != row.instrument_id
        || link.market_instrument_revision_digest().bytes().as_slice() != row.market_revision_digest
        || security_kind_name(link.security_kind()) != row.security_kind
        || relationship_kind_name(link.relationship_kind()) != row.relationship_kind
        || suitability_name(link.common_equity_suitability()) != row.suitability
        || (if link.transition().is_revocation() {
            "revoked"
        } else {
            "active"
        }) != row.event_kind
        || link
            .transition()
            .previous_link_digest()
            .map(|value| value.bytes().to_vec())
            != row.previous_digest
        || link.effective_interval().starts_at().unix_nanos() != row.effective_start
        || link
            .effective_interval()
            .ends_at()
            .map(Timestamp::unix_nanos)
            != row.effective_end
        || resolution_kind_name(link.resolution_basis()) != row.resolution_kind
        || algorithm_code(
            link.resolution_basis()
                .evidence()
                .content_digest()
                .algorithm(),
        ) != row.resolution_algorithm
        || link
            .resolution_basis()
            .evidence()
            .content_digest()
            .bytes()
            .as_slice()
            != row.resolution_digest
        || link.relationship_evidence_rights().policy_id().as_str() != row.rights_policy_id
        || entitlement_name(link.relationship_evidence_rights().entitlement())
            != row.rights_entitlement
        || link
            .relationship_evidence_rights()
            .terms_reference()
            .as_str()
            != row.rights_terms_reference
        || link.available_at().unix_nanos() != row.available_at
        || link.ingested_at().unix_nanos() != row.ingested_at
    {
        return Err(CompanySecurityIdentityCatalogError::CorruptCatalog);
    }
    let event_sequence = u32::try_from(row.event_sequence)
        .ok()
        .filter(|value| (1..=MAX_LINK_EVENTS_PER_RELATIONSHIP as u32).contains(value))
        .ok_or(CompanySecurityIdentityCatalogError::CorruptCatalog)?;
    Ok(CompanySecurityIdentityRecord {
        link,
        link_digest: evidence_digest(digest_bytes),
        event_sequence,
        published_at: Timestamp::from_unix_nanos(row.published_at),
    })
}

fn charge_stored_link(
    row: &StoredLinkRow,
    budget: &mut ResultBudget,
) -> Result<(), CompanySecurityIdentityCatalogError> {
    budget
        .charge([
            size_of::<CompanySecurityIdentityRecord>(),
            row.digest.len(),
            row.company_source_id.len(),
            row.provider_company_id.len(),
            row.company_surface.len(),
            row.company_observation_digest.len(),
            row.instrument_id.len(),
            row.market_revision_digest.len(),
            row.security_kind.len(),
            row.relationship_kind.len(),
            row.suitability.len(),
            row.event_kind.len(),
            row.previous_digest.as_ref().map_or(0, Vec::len),
            row.resolution_kind.len(),
            row.resolution_digest.len(),
            row.rights_policy_id.len(),
            row.rights_entitlement.len(),
            row.rights_terms_reference.len(),
            row.json.len(),
        ])
        .map_err(|_| CompanySecurityIdentityCatalogError::ResultLimitExceeded)
}

fn valid_industry_scheme_version(
    scheme: IndustryClassificationScheme,
    version: IndustryClassificationVersion,
) -> bool {
    matches!(
        (scheme, version),
        (
            IndustryClassificationScheme::SecSic,
            IndustryClassificationVersion::SecSicCurrentV1
        )
    )
}

fn validate_industry_query(
    company_observation_digest: EvidenceDigest,
    scheme: IndustryClassificationScheme,
    version: IndustryClassificationVersion,
) -> Result<(), CompanySecurityIdentityCatalogError> {
    if company_observation_digest.algorithm() != DigestAlgorithm::Sha256
        || company_observation_digest.bytes() == [0; 32]
        || !valid_industry_scheme_version(scheme, version)
    {
        Err(CompanySecurityIdentityCatalogError::InvalidInput)
    } else {
        Ok(())
    }
}

fn finish_industry_classification_selection(
    query_digest: EvidenceDigest,
    knowledge_at: Timestamp,
    disposition: IndustryClassificationDisposition,
    classification: Option<IndustryClassificationRecord>,
    considered: Option<IndustryClassificationReceiptEntry>,
    exclusion_reason: Option<IndustryClassificationExclusionReason>,
) -> Result<IndustryClassificationSelection, CompanySecurityIdentityCatalogError> {
    if (disposition == IndustryClassificationDisposition::Complete) != classification.is_some()
        || (disposition == IndustryClassificationDisposition::Complete)
            == exclusion_reason.is_some()
    {
        return Err(CompanySecurityIdentityCatalogError::CorruptCatalog);
    }
    let receipt_digest = industry_classification_receipt_digest(
        query_digest,
        knowledge_at,
        disposition,
        considered.as_ref(),
        exclusion_reason,
    );
    Ok(IndustryClassificationSelection {
        disposition,
        classification,
        receipt: IndustryClassificationSelectionReceipt {
            query_digest,
            knowledge_at,
            disposition,
            considered,
            exclusion_reason,
            receipt_digest,
        },
    })
}

fn industry_classification_query_digest(
    company_observation_digest: EvidenceDigest,
    company_source_id: &SourceId,
    company_surface: CompanyIdentitySurface,
    scheme: IndustryClassificationScheme,
    version: IndustryClassificationVersion,
) -> EvidenceDigest {
    let mut hasher = Sha256::new();
    hasher.update(INDUSTRY_CLASSIFICATION_QUERY_DOMAIN);
    hash_digest(&mut hasher, company_observation_digest);
    hash_text(&mut hasher, company_source_id.as_str());
    hasher.update([company_surface_tag(company_surface)]);
    hasher.update([industry_scheme_tag(scheme), industry_version_tag(version)]);
    evidence_digest(hasher.finalize().into())
}

fn industry_cohort_query_digest(
    company_source_id: &SourceId,
    company_surface: CompanyIdentitySurface,
    scheme: IndustryClassificationScheme,
    version: IndustryClassificationVersion,
    code: &IndustryClassificationCode,
    maximum_members: usize,
) -> EvidenceDigest {
    let mut hasher = Sha256::new();
    hasher.update(INDUSTRY_COHORT_QUERY_DOMAIN);
    hash_text(&mut hasher, company_source_id.as_str());
    hasher.update([company_surface_tag(company_surface)]);
    hasher.update([industry_scheme_tag(scheme), industry_version_tag(version)]);
    hash_text(&mut hasher, code.as_str());
    hasher.update((maximum_members as u64).to_be_bytes());
    evidence_digest(hasher.finalize().into())
}

fn industry_classification_receipt_digest(
    query_digest: EvidenceDigest,
    knowledge_at: Timestamp,
    disposition: IndustryClassificationDisposition,
    considered: Option<&IndustryClassificationReceiptEntry>,
    exclusion_reason: Option<IndustryClassificationExclusionReason>,
) -> EvidenceDigest {
    let mut hasher = Sha256::new();
    hasher.update(INDUSTRY_CLASSIFICATION_SELECTION_RECEIPT_DOMAIN);
    hash_digest(&mut hasher, query_digest);
    hasher.update(knowledge_at.unix_nanos().to_be_bytes());
    hasher.update([industry_disposition_tag(disposition)]);
    match considered {
        Some(entry) => {
            hasher.update([1]);
            hash_industry_receipt_entry(&mut hasher, entry);
        }
        None => hasher.update([0]),
    }
    match exclusion_reason {
        Some(reason) => hasher.update([1, industry_exclusion_tag(reason)]),
        None => hasher.update([0]),
    }
    evidence_digest(hasher.finalize().into())
}

#[allow(clippy::too_many_arguments)]
fn industry_cohort_receipt_digest(
    query_digest: EvidenceDigest,
    knowledge_at: Timestamp,
    completeness: IndustryCohortCompleteness,
    scan_truncated: bool,
    member_limit_truncated: bool,
    scan_boundary_digest: Option<EvidenceDigest>,
    members: &[IndustryClassificationReceiptEntry],
    exclusions: &[IndustryCohortExclusion],
) -> EvidenceDigest {
    let mut hasher = Sha256::new();
    hasher.update(INDUSTRY_COHORT_SELECTION_RECEIPT_DOMAIN);
    hash_digest(&mut hasher, query_digest);
    hasher.update(knowledge_at.unix_nanos().to_be_bytes());
    hasher.update([
        industry_cohort_completeness_tag(completeness),
        u8::from(scan_truncated),
        u8::from(member_limit_truncated),
    ]);
    hash_optional_digest(&mut hasher, scan_boundary_digest);
    hasher.update((members.len() as u64).to_be_bytes());
    for entry in members {
        hash_industry_receipt_entry(&mut hasher, entry);
    }
    hasher.update((exclusions.len() as u64).to_be_bytes());
    for exclusion in exclusions {
        hash_industry_receipt_entry(&mut hasher, &exclusion.entry);
        hasher.update([industry_exclusion_tag(exclusion.reason)]);
    }
    evidence_digest(hasher.finalize().into())
}

fn hash_industry_receipt_entry(hasher: &mut Sha256, entry: &IndustryClassificationReceiptEntry) {
    hash_digest(hasher, entry.company_observation_digest);
    hash_optional_digest(hasher, entry.current_company_observation_digest);
    hash_text(hasher, entry.company_source_id.as_str());
    hash_text(hasher, entry.provider_company_id.as_str());
    hasher.update([company_surface_tag(entry.company_surface)]);
    hash_optional_text(hasher, entry.reported_code.as_deref());
    hash_optional_text(hasher, entry.source_description.as_deref());
    hash_optional_text(
        hasher,
        entry
            .source_record_reference
            .as_ref()
            .map(SourceIdentifier::as_str),
    );
    hash_optional_text(
        hasher,
        entry
            .source_record_version
            .as_ref()
            .map(SourceIdentifier::as_str),
    );
    hash_digest(hasher, entry.classification_evidence_digest);
    hash_digest(hasher, entry.parent_ingest_evidence_digest);
    hash_optional_timestamp(hasher, entry.effective_at);
    hash_optional_timestamp(hasher, entry.available_at);
    for value in [entry.received_at, entry.ingested_at, entry.published_at] {
        hasher.update(value.unix_nanos().to_be_bytes());
    }
    hash_optional_timestamp(hasher, entry.current_available_at);
    hash_optional_timestamp(hasher, entry.current_ingested_at);
    hash_optional_timestamp(hasher, entry.current_published_at);
}

fn query_digest(query: &CompanySecurityIdentityQuery) -> EvidenceDigest {
    let mut hasher = Sha256::new();
    hasher.update(b"market-squawk/company-security-query/v1\0");
    hash_text(&mut hasher, query.company_source_id().as_str());
    hash_text(&mut hasher, query.provider_company_id().as_str());
    hasher.update([company_surface_tag(query.company_surface())]);
    match query.instrument_id() {
        Some(value) => {
            hasher.update([1]);
            hasher.update(value.as_uuid().as_bytes());
        }
        None => hasher.update([0]),
    }
    hasher.update([u8::from(query.require_suitable_common_equity())]);
    evidence_digest(hasher.finalize().into())
}

fn sec_fundamental_identity_query_digest(query: &SecFundamentalIdentityQuery) -> EvidenceDigest {
    let mut hasher = Sha256::new();
    hasher.update(SEC_FUNDAMENTAL_IDENTITY_QUERY_DOMAIN);
    hash_text(&mut hasher, query.company_source_id().as_str());
    hash_text(&mut hasher, query.cik().as_str());
    hasher.update([company_surface_tag(query.company_surface())]);
    hash_digest(&mut hasher, query.company_observation_digest());
    hasher.update(query.effective_at().unix_nanos().to_be_bytes());
    hasher.update(query.knowledge_at().unix_nanos().to_be_bytes());
    evidence_digest(hasher.finalize().into())
}

fn finish_sec_fundamental_identity(
    query: &SecFundamentalIdentityQuery,
    relationship_selection: CompanySecurityIdentitySelection,
    availability: SecFundamentalIdentityAvailability,
    relationship: Option<CompanySecurityIdentityRecord>,
) -> Result<SecFundamentalIdentitySelection, CompanySecurityIdentityCatalogError> {
    if (availability == SecFundamentalIdentityAvailability::Available) != relationship.is_some() {
        return Err(CompanySecurityIdentityCatalogError::CorruptCatalog);
    }
    let instrument_id = relationship
        .as_ref()
        .map(|record| record.link().instrument_id());
    let market_instrument_revision_digest = relationship
        .as_ref()
        .map(|record| record.link().market_instrument_revision_digest());
    let query_digest = sec_fundamental_identity_query_digest(query);
    let mut hasher = Sha256::new();
    hasher.update(SEC_FUNDAMENTAL_IDENTITY_RECEIPT_DOMAIN);
    hash_digest(&mut hasher, query_digest);
    hasher.update([sec_fundamental_availability_tag(availability)]);
    hash_digest(
        &mut hasher,
        relationship_selection.receipt().receipt_digest(),
    );
    match &relationship {
        Some(record) => {
            hasher.update([1]);
            hash_digest(&mut hasher, record.link_digest());
            hasher.update(record.link().instrument_id().as_uuid().as_bytes());
            hash_digest(
                &mut hasher,
                record.link().market_instrument_revision_digest(),
            );
        }
        None => hasher.update([0]),
    }
    let receipt_digest = evidence_digest(hasher.finalize().into());
    Ok(SecFundamentalIdentitySelection {
        availability,
        company_observation_digest: query.company_observation_digest(),
        instrument_id,
        market_instrument_revision_digest,
        relationship,
        relationship_selection,
        query_digest,
        receipt_digest,
    })
}

fn instrument_company_query_digest(
    instrument_id: InstrumentId,
    company_source_id: &SourceId,
    company_surface: CompanyIdentitySurface,
    required_suitability: CommonEquitySuitability,
) -> EvidenceDigest {
    let mut hasher = Sha256::new();
    hasher.update(b"market-squawk/instrument-company-identity-query/v1\0");
    hasher.update(instrument_id.as_uuid().as_bytes());
    hash_text(&mut hasher, company_source_id.as_str());
    hasher.update([company_surface_tag(company_surface)]);
    hasher.update([suitability_tag(required_suitability)]);
    evidence_digest(hasher.finalize().into())
}

fn selection_receipt_digest(
    receipt_domain: &'static [u8],
    query_digest: EvidenceDigest,
    effective_at: Timestamp,
    knowledge_at: Timestamp,
    disposition: CompanySecurityIdentityDisposition,
    candidates: &[CompanySecuritySelectionReceiptEntry],
    exclusions: &[(
        CompanySecuritySelectionReceiptEntry,
        CompanySecurityIdentityExclusionReason,
    )],
) -> EvidenceDigest {
    let mut hasher = Sha256::new();
    hasher.update(receipt_domain);
    hash_digest(&mut hasher, query_digest);
    hasher.update(effective_at.unix_nanos().to_be_bytes());
    hasher.update(knowledge_at.unix_nanos().to_be_bytes());
    hasher.update([disposition_tag(disposition)]);
    hasher.update((candidates.len() as u64).to_be_bytes());
    for entry in candidates {
        hash_receipt_entry(&mut hasher, entry);
    }
    hasher.update((exclusions.len() as u64).to_be_bytes());
    for (entry, reason) in exclusions {
        hash_receipt_entry(&mut hasher, entry);
        hasher.update([exclusion_tag(*reason)]);
    }
    evidence_digest(hasher.finalize().into())
}

fn hash_receipt_entry(hasher: &mut Sha256, entry: &CompanySecuritySelectionReceiptEntry) {
    hash_digest(hasher, entry.link_digest);
    hasher.update(entry.event_sequence.to_be_bytes());
    hash_optional_digest(hasher, entry.previous_link_digest);
    hash_text(hasher, entry.company_source_id.as_str());
    hash_text(hasher, entry.provider_company_id.as_str());
    hasher.update([company_surface_tag(entry.company_surface)]);
    hash_digest(hasher, entry.linked_company_observation_digest);
    hash_optional_digest(hasher, entry.current_company_observation_digest);
    hash_optional_timestamp(hasher, entry.current_company_available_at);
    hash_optional_timestamp(hasher, entry.current_company_ingested_at);
    hash_optional_timestamp(hasher, entry.current_company_completed_at);
    hash_digest(hasher, entry.linked_market_revision_digest);
    hasher.update(entry.instrument_id.as_uuid().as_bytes());
    hasher.update([security_kind_tag(entry.security_kind)]);
    hasher.update([relationship_kind_tag(entry.relationship_kind)]);
    hasher.update([suitability_tag(entry.common_equity_suitability)]);
    hash_optional_digest(hasher, entry.current_market_revision_digest);
    hash_optional_timestamp(hasher, entry.current_market_published_at);
    hash_optional_timestamp(hasher, entry.current_market_effective_start);
    hash_optional_timestamp(hasher, entry.current_market_effective_end);
    hash_optional_timestamp(hasher, entry.company_available_at);
    for value in [
        entry.company_ingested_at,
        entry.company_completed_at,
        entry.market_published_at,
        entry.market_effective_start,
        entry.link_available_at,
        entry.link_ingested_at,
        entry.link_published_at,
        entry.effective_start,
    ] {
        hasher.update(value.unix_nanos().to_be_bytes());
    }
    hash_optional_timestamp(hasher, entry.effective_end);
    hash_optional_timestamp(hasher, entry.market_effective_end);
    hash_text(hasher, entry.rights_policy_id.as_str());
    hasher.update([entitlement_tag(entry.rights_entitlement)]);
    hash_text(hasher, entry.rights_terms_reference.as_str());
}

fn hash_digest(hasher: &mut Sha256, digest: EvidenceDigest) {
    hasher.update([algorithm_code(digest.algorithm()) as u8]);
    hasher.update(digest.bytes());
}
fn hash_optional_digest(hasher: &mut Sha256, digest: Option<EvidenceDigest>) {
    match digest {
        Some(value) => {
            hasher.update([1]);
            hash_digest(hasher, value);
        }
        None => hasher.update([0]),
    }
}
fn hash_optional_timestamp(hasher: &mut Sha256, value: Option<Timestamp>) {
    match value {
        Some(value) => {
            hasher.update([1]);
            hasher.update(value.unix_nanos().to_be_bytes());
        }
        None => hasher.update([0]),
    }
}
fn hash_optional_text(hasher: &mut Sha256, value: Option<&str>) {
    match value {
        Some(value) => {
            hasher.update([1]);
            hash_text(hasher, value);
        }
        None => hasher.update([0]),
    }
}
fn hash_text(hasher: &mut Sha256, value: &str) {
    hasher.update((value.len() as u64).to_be_bytes());
    hasher.update(value.as_bytes());
}

fn link_key_params(link: &CompanySecurityIdentityLink) -> (&str, &str, &'static str, String) {
    (
        link.company_source_id().as_str(),
        link.provider_company_id().as_str(),
        link.company_surface().database_name(),
        link.instrument_id().to_string(),
    )
}

fn security_kind_name(value: CompanySecurityKind) -> &'static str {
    match value {
        CompanySecurityKind::CommonEquity => "common_equity",
        CompanySecurityKind::PreferredEquity => "preferred_equity",
        CompanySecurityKind::DepositaryReceipt => "depositary_receipt",
        CompanySecurityKind::Debt => "debt",
        CompanySecurityKind::FundInterest => "fund_interest",
        CompanySecurityKind::Other => "other",
    }
}
fn relationship_kind_name(value: CompanySecurityRelationshipKind) -> &'static str {
    match value {
        CompanySecurityRelationshipKind::Issuer => "issuer",
        CompanySecurityRelationshipKind::Guarantor => "guarantor",
        CompanySecurityRelationshipKind::DepositaryUnderlying => "depositary_underlying",
        CompanySecurityRelationshipKind::FundSponsor => "fund_sponsor",
        CompanySecurityRelationshipKind::Other => "other",
    }
}
fn suitability_name(value: CommonEquitySuitability) -> &'static str {
    match value {
        CommonEquitySuitability::SuitableIssuerCommonEquity => "suitable_issuer_common_equity",
        CommonEquitySuitability::NotSuitable => "not_suitable",
    }
}
const fn security_kind_tag(value: CompanySecurityKind) -> u8 {
    match value {
        CompanySecurityKind::CommonEquity => 1,
        CompanySecurityKind::PreferredEquity => 2,
        CompanySecurityKind::DepositaryReceipt => 3,
        CompanySecurityKind::Debt => 4,
        CompanySecurityKind::FundInterest => 5,
        CompanySecurityKind::Other => 6,
    }
}
const fn relationship_kind_tag(value: CompanySecurityRelationshipKind) -> u8 {
    match value {
        CompanySecurityRelationshipKind::Issuer => 1,
        CompanySecurityRelationshipKind::Guarantor => 2,
        CompanySecurityRelationshipKind::DepositaryUnderlying => 3,
        CompanySecurityRelationshipKind::FundSponsor => 4,
        CompanySecurityRelationshipKind::Other => 5,
    }
}
const fn suitability_tag(value: CommonEquitySuitability) -> u8 {
    match value {
        CommonEquitySuitability::SuitableIssuerCommonEquity => 1,
        CommonEquitySuitability::NotSuitable => 2,
    }
}
fn resolution_kind_name(value: &CompanySecurityResolutionBasis) -> &'static str {
    match value {
        CompanySecurityResolutionBasis::DirectAuthoritativeCrosswalk { .. } => {
            "direct_authoritative_crosswalk"
        }
        CompanySecurityResolutionBasis::OperatorAuthorizedResolution { .. } => {
            "operator_authorized_resolution"
        }
    }
}
fn entitlement_name(value: IdentifierEntitlement) -> &'static str {
    match value {
        IdentifierEntitlement::UnknownOrRestricted => "unknown_or_restricted",
        IdentifierEntitlement::PublicDomain => "public_domain",
        IdentifierEntitlement::UserOwned => "user_owned",
        IdentifierEntitlement::LicensedInternalUse => "licensed_internal_use",
        IdentifierEntitlement::LicensedRedistribution => "licensed_redistribution",
    }
}
const fn entitlement_tag(value: IdentifierEntitlement) -> u8 {
    match value {
        IdentifierEntitlement::UnknownOrRestricted => 0,
        IdentifierEntitlement::PublicDomain => 1,
        IdentifierEntitlement::UserOwned => 2,
        IdentifierEntitlement::LicensedInternalUse => 3,
        IdentifierEntitlement::LicensedRedistribution => 4,
    }
}
const fn company_surface_tag(value: CompanyIdentitySurface) -> u8 {
    match value {
        CompanyIdentitySurface::SecSubmissions => 1,
        CompanyIdentitySurface::SecCompanyFacts => 2,
    }
}
const fn industry_scheme_tag(value: IndustryClassificationScheme) -> u8 {
    match value {
        IndustryClassificationScheme::SecSic => 1,
    }
}
const fn industry_version_tag(value: IndustryClassificationVersion) -> u8 {
    match value {
        IndustryClassificationVersion::SecSicCurrentV1 => 1,
    }
}
const fn industry_disposition_tag(value: IndustryClassificationDisposition) -> u8 {
    match value {
        IndustryClassificationDisposition::Complete => 1,
        IndustryClassificationDisposition::Unavailable => 2,
        IndustryClassificationDisposition::Conflict => 3,
        IndustryClassificationDisposition::Stale => 4,
        IndustryClassificationDisposition::Revoked => 5,
    }
}
const fn industry_exclusion_tag(value: IndustryClassificationExclusionReason) -> u8 {
    match value {
        IndustryClassificationExclusionReason::ObservationUnavailable => 1,
        IndustryClassificationExclusionReason::NotYetAvailable => 2,
        IndustryClassificationExclusionReason::MissingCode => 3,
        IndustryClassificationExclusionReason::InvalidCode => 4,
        IndustryClassificationExclusionReason::AmbiguousParent => 5,
        IndustryClassificationExclusionReason::StaleParent => 6,
        IndustryClassificationExclusionReason::Revoked => 7,
    }
}
const fn industry_cohort_completeness_tag(value: IndustryCohortCompleteness) -> u8 {
    match value {
        IndustryCohortCompleteness::Complete => 1,
        IndustryCohortCompleteness::Truncated => 2,
    }
}
const fn disposition_tag(value: CompanySecurityIdentityDisposition) -> u8 {
    match value {
        CompanySecurityIdentityDisposition::Complete => 1,
        CompanySecurityIdentityDisposition::Unavailable => 2,
        CompanySecurityIdentityDisposition::Conflict => 3,
        CompanySecurityIdentityDisposition::Stale => 4,
        CompanySecurityIdentityDisposition::Revoked => 5,
    }
}
const fn sec_fundamental_availability_tag(value: SecFundamentalIdentityAvailability) -> u8 {
    match value {
        SecFundamentalIdentityAvailability::Available => 1,
        SecFundamentalIdentityAvailability::IdentityPending => 2,
        SecFundamentalIdentityAvailability::Conflict => 3,
        SecFundamentalIdentityAvailability::Unavailable => 4,
    }
}
const fn exclusion_tag(value: CompanySecurityIdentityExclusionReason) -> u8 {
    match value {
        CompanySecurityIdentityExclusionReason::NotYetAvailable => 1,
        CompanySecurityIdentityExclusionReason::NotYetEffective => 2,
        CompanySecurityIdentityExclusionReason::NoLongerEffective => 3,
        CompanySecurityIdentityExclusionReason::NotSuitableCommonEquity => 4,
        CompanySecurityIdentityExclusionReason::AmbiguousCompanyParent => 5,
        CompanySecurityIdentityExclusionReason::StaleCompanyParent => 6,
        CompanySecurityIdentityExclusionReason::StaleMarketInstrumentParent => 7,
        CompanySecurityIdentityExclusionReason::Revoked => 8,
    }
}
const fn algorithm_code(value: DigestAlgorithm) -> i64 {
    match value {
        DigestAlgorithm::Sha256 => 1,
        DigestAlgorithm::Blake3 => 2,
    }
}
const fn evidence_digest(bytes: [u8; 32]) -> EvidenceDigest {
    EvidenceDigest::new(DigestAlgorithm::Sha256, bytes)
}
fn valid_sec_cik(value: &str) -> bool {
    value.len() == 10
        && value.bytes().all(|byte| byte.is_ascii_digit())
        && value.bytes().any(|byte| byte != b'0')
}
fn array_digest(bytes: &[u8]) -> Result<[u8; 32], CompanySecurityIdentityCatalogError> {
    let result: [u8; 32] = bytes
        .try_into()
        .map_err(|_| CompanySecurityIdentityCatalogError::CorruptCatalog)?;
    if result == [0; 32] {
        Err(CompanySecurityIdentityCatalogError::CorruptCatalog)
    } else {
        Ok(result)
    }
}
fn sha256(bytes: &[u8]) -> [u8; 32] {
    Sha256::digest(bytes).into()
}

fn install_progress_handler(
    connection: &Connection,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<(), CompanySecurityIdentityCatalogError> {
    let token = cancellation.clone();
    connection.progress_handler(
        SQLITE_PROGRESS_OPERATIONS,
        Some(move || token.is_cancelled() || Instant::now() >= deadline),
    )?;
    Ok(())
}
fn clear_progress_handler(
    connection: &Connection,
) -> Result<(), CompanySecurityIdentityCatalogError> {
    connection.progress_handler::<fn() -> bool>(0, None)?;
    Ok(())
}
fn check_operation(
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<(), CompanySecurityIdentityCatalogError> {
    if cancellation.is_cancelled() {
        Err(CompanySecurityIdentityCatalogError::Cancelled)
    } else if Instant::now() >= deadline {
        Err(CompanySecurityIdentityCatalogError::DeadlineExceeded)
    } else {
        Ok(())
    }
}
fn classify_operation<T>(
    result: Result<T, CompanySecurityIdentityCatalogError>,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<T, CompanySecurityIdentityCatalogError> {
    if cancellation.is_cancelled() {
        Err(CompanySecurityIdentityCatalogError::Cancelled)
    } else if Instant::now() >= deadline {
        Err(CompanySecurityIdentityCatalogError::DeadlineExceeded)
    } else {
        result
    }
}

const EXACT_COMPANY_OBSERVATION_SQL: &str = "
SELECT observations.record_digest, observations.available_at_ns,
       observations.ingested_at_ns, runs.completed_at_ns, observations.record_json,
       observations.source_id, observations.provider_company_id,
       observations.source_surface
FROM company_identity_observations AS observations
JOIN ingest_runs AS runs ON runs.run_id=observations.run_id
WHERE observations.record_digest=?1 AND observations.source_id=?2
  AND observations.source_surface=?3 AND runs.state='succeeded'";

const INDUSTRY_COHORT_OBSERVATIONS_SQL: &str = "
SELECT observations.record_digest, observations.available_at_ns,
       observations.ingested_at_ns, runs.completed_at_ns, observations.record_json,
       observations.source_id, observations.provider_company_id,
       observations.source_surface, terms.display_value, terms.normalized_value
FROM company_identity_search_terms AS terms
INDEXED BY company_identity_search_terms_lookup
JOIN company_identity_observations AS observations
  ON observations.record_digest=terms.record_digest
JOIN ingest_runs AS runs ON runs.run_id=observations.run_id
WHERE terms.normalized_value=?1 AND terms.term_kind='sic'
  AND observations.source_id=?2 AND observations.source_surface=?3
  AND runs.state='succeeded' AND observations.available_at_ns IS NOT NULL
  AND observations.received_at_ns<=?4 AND observations.available_at_ns<=?4
  AND observations.ingested_at_ns<=?4 AND runs.completed_at_ns<=?4
ORDER BY terms.record_digest
LIMIT ?5";

const STORED_COLUMNS: &str = "link_digest, company_source_id, provider_company_id,
    company_surface, company_observation_digest, instrument_id, market_revision_digest,
    event_sequence, security_kind, relationship_kind, common_equity_suitability,
    event_kind, previous_link_digest, effective_start_ns, effective_end_ns, resolution_kind,
    resolution_evidence_algorithm, resolution_evidence_digest, relationship_rights_policy_id,
    relationship_rights_entitlement, relationship_rights_terms_reference, available_at_ns,
    ingested_at_ns, link_json, published_at_ns";

const INSTRUMENT_LINK_HISTORY_SQL: &str = "
SELECT events.link_digest, events.company_source_id, events.provider_company_id,
       events.company_surface, events.company_observation_digest, events.instrument_id,
       events.market_revision_digest, events.event_sequence,
       events.security_kind, events.relationship_kind, events.common_equity_suitability,
       events.event_kind, events.previous_link_digest, events.effective_start_ns,
       events.effective_end_ns, events.resolution_kind, events.resolution_evidence_algorithm,
       events.resolution_evidence_digest, events.relationship_rights_policy_id,
       events.relationship_rights_entitlement, events.relationship_rights_terms_reference,
       events.available_at_ns, events.ingested_at_ns, events.link_json, events.published_at_ns
FROM company_security_link_events AS events
INDEXED BY company_security_link_instrument_history
WHERE events.instrument_id=?1 AND events.published_at_ns<=?2
ORDER BY events.published_at_ns DESC, events.link_digest
LIMIT ?3";

const CURRENT_LINK_BY_KEY_SQL: &str = "
SELECT events.link_digest, events.company_source_id, events.provider_company_id,
       events.company_surface, events.company_observation_digest, events.instrument_id,
       events.market_revision_digest, events.event_sequence,
       events.security_kind, events.relationship_kind, events.common_equity_suitability,
       events.event_kind, events.previous_link_digest, events.effective_start_ns,
       events.effective_end_ns, events.resolution_kind, events.resolution_evidence_algorithm,
       events.resolution_evidence_digest, events.relationship_rights_policy_id,
       events.relationship_rights_entitlement, events.relationship_rights_terms_reference,
       events.available_at_ns, events.ingested_at_ns, events.link_json, events.published_at_ns
FROM company_security_link_current AS current_
JOIN company_security_link_events AS events ON events.link_digest=current_.link_digest
WHERE current_.company_source_id=?1 AND current_.provider_company_id=?2
  AND current_.company_surface=?3 AND current_.instrument_id=?4";

const CURRENT_LINKS_SQL: &str = "
SELECT events.link_digest, events.company_source_id, events.provider_company_id,
       events.company_surface, events.company_observation_digest, events.instrument_id,
       events.market_revision_digest, events.event_sequence, events.security_kind,
       events.relationship_kind, events.common_equity_suitability, events.event_kind,
       events.previous_link_digest, events.effective_start_ns, events.effective_end_ns,
       events.resolution_kind, events.resolution_evidence_algorithm,
       events.resolution_evidence_digest, events.relationship_rights_policy_id,
       events.relationship_rights_entitlement, events.relationship_rights_terms_reference,
       events.available_at_ns, events.ingested_at_ns, events.link_json, events.published_at_ns
FROM company_security_link_current AS current_
JOIN company_security_link_events AS events ON events.link_digest=current_.link_digest
WHERE events.company_source_id=?1 AND events.provider_company_id=?2
  AND events.company_surface=?3 AND (?4 IS NULL OR events.instrument_id=?4)
  AND events.published_at_ns<=?5
ORDER BY events.instrument_id, hex(events.link_digest)
LIMIT ?6";

const AS_OF_LINKS_SQL: &str = "
WITH ranked AS (
  SELECT events.*,
         row_number() OVER (
           PARTITION BY events.company_source_id, events.provider_company_id,
                        events.company_surface, events.instrument_id
           ORDER BY events.event_sequence DESC
         ) AS as_of_rank
  FROM company_security_link_events AS events
  WHERE events.company_source_id=?1 AND events.provider_company_id=?2
    AND events.company_surface=?3 AND (?4 IS NULL OR events.instrument_id=?4)
    AND events.published_at_ns<=?5
    AND events.effective_start_ns<=?6
)
SELECT link_digest, company_source_id, provider_company_id, company_surface,
       company_observation_digest, instrument_id, market_revision_digest,
       event_sequence, security_kind, relationship_kind, common_equity_suitability, event_kind,
       previous_link_digest, effective_start_ns, effective_end_ns, resolution_kind,
       resolution_evidence_algorithm, resolution_evidence_digest,
       relationship_rights_policy_id, relationship_rights_entitlement,
       relationship_rights_terms_reference, available_at_ns, ingested_at_ns, link_json,
       published_at_ns
FROM ranked WHERE as_of_rank=1
ORDER BY instrument_id, hex(link_digest)
LIMIT ?7";
