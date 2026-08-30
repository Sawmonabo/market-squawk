//! Immutable OCC/Cboe option-reference generations and exact local identity discovery.
//!
//! This leaf retains official provider-native product and series evidence beside, rather than
//! inside, the repository-owned instrument master. It may resolve an exact provider row to an
//! already admitted [`InstrumentId`], but it never allocates an identity or derives one from a
//! ticker, name, OCC root, Cboe symbol, or OSI value.

use std::cmp::Ordering;
use std::collections::BTreeSet;
use std::fmt;
use std::mem::size_of;
use std::num::{NonZeroU16, NonZeroU64};
use std::str::FromStr as _;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use market_squawk_domain::{
    AssetClass, AssignmentVerification, DigestAlgorithm, EffectiveInterval, EvidenceDigest,
    ExternalIdentifier, InstrumentId, MarketDataInstrumentDefinition, OccOptionIdentity,
    ProviderInstrumentId, SourceId, SourceIdentifier, Timestamp, VenueId,
};
use market_squawk_platform::{
    ResearchObjectClaim, ResearchObjectControl, ResearchObjectReceipt, SealedResearchJournalStore,
    SealedResearchJournalStoreError, SealedResearchRawClaim,
};
use market_squawk_sources::SourceMetadata;
use rusqlite::{Connection, OptionalExtension as _, Row, Transaction, params};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use thiserror::Error;
use tokio_util::sync::CancellationToken;

use super::CatalogAuthority;
use super::provider_capture::raw_claim_digest as shared_raw_claim_digest;
use super::storage::{ResultBudget, append_audit, now_timestamp, sha256, trusted_catalog_now};
use super::types::CatalogError;
use crate::RegisteredRightsGrant;

/// Maximum official objects retained in one complete OCC/Cboe generation.
pub const MAX_OFFICIAL_OPTIONS_REFERENCE_OBJECTS: usize = 64;
/// Maximum strictly parsed rows accounted across one complete request closure.
pub const MAX_OFFICIAL_OPTIONS_REFERENCE_STRICT_ROWS: u64 = 12_000_000;
/// Maximum product/series identity rows retained by one generation.
pub const MAX_OFFICIAL_OPTIONS_REFERENCE_RECORDS: u64 = 12_000_000;
/// Maximum alias assertions retained by one generation.
pub const MAX_OFFICIAL_OPTIONS_REFERENCE_ALIAS_ASSERTIONS: u64 = 36_000_000;
/// Maximum request-scoped alias resolutions retained by one generation.
pub const MAX_OFFICIAL_OPTIONS_REFERENCE_ALIAS_RESOLUTIONS: u64 = 36_000_000;
/// Maximum preserved provider conflicts retained by one generation.
pub const MAX_OFFICIAL_OPTIONS_REFERENCE_CONFLICTS: u64 = 100_000;
/// Maximum exact provider rows returned by one identity query.
pub const MAX_OFFICIAL_OPTIONS_REFERENCE_EXACT_ROWS: usize = 1_024;
/// Maximum candidate rows returned by one text search.
pub const MAX_OFFICIAL_OPTIONS_REFERENCE_SEARCH_ROWS: usize = 1_000;
/// Maximum canonical candidates returned for an ambiguous exact provider identity.
pub const MAX_OFFICIAL_OPTIONS_REFERENCE_CANONICAL_CANDIDATES: usize = 256;

const MAX_TOTAL_PAYLOAD_BYTES: u64 = 2 * 1024 * 1024 * 1024;
const MAX_RECORD_JSON_BYTES: usize = 64 * 1024;
const MAX_RAW_CLAIM_JSON_BYTES: usize = 2 * 1024 * 1024;
const MAX_OBJECT_ID_BYTES: usize = 512;
const MAX_NATIVE_SCHEMA_BYTES: usize = 128;
const MAX_CBOE_SYMBOL_BYTES: usize = 6;
const MAX_CBOE_UNDERLYING_BYTES: usize = 8;
const MAX_OCC_SYMBOL_BYTES: usize = 32;
const MAX_OCC_SYMBOL_NAME_BYTES: usize = 512;
const MAX_EXCHANGE_CODES_BYTES: usize = 32;
const MAX_SEARCH_QUERY_BYTES: usize = 256;
const MAX_CURRENT_REFERENCE_DATASET_CANDIDATES: usize = 64;
const MAX_GENERATIONS: u32 = 16_384;
const SQLITE_PROGRESS_OPERATIONS: i32 = 1_000;

const SOURCE_PAYLOAD_SET_DOMAIN: &[u8] =
    b"market-squawk/official-options-reference/source-payload-set/v1";
const RECORD_VALUE_DOMAIN: &[u8] = b"market-squawk/official-options-reference/value/v1";
const RECORD_MEMBERSHIP_DOMAIN: &[u8] = b"market-squawk/official-options-reference/membership/v1";
const ALIAS_KEY_DOMAIN: &[u8] = b"market-squawk/official-options-reference/alias-key/v1";
const ORDERED_RECORD_SET_DOMAIN: &[u8] =
    b"market-squawk/official-options-reference/ordered-record-set/v1";
const ORDERED_RESOLUTION_SET_DOMAIN: &[u8] =
    b"market-squawk/official-options-reference/ordered-resolution-set/v1";
const ORDERED_CONFLICT_SET_DOMAIN: &[u8] =
    b"market-squawk/official-options-reference/ordered-conflict-set/v1";
const GENERATION_DOMAIN: &[u8] = b"market-squawk/official-options-reference/generation/v1";
const CANONICAL_RESOLUTION_RECEIPT_DOMAIN: &[u8] =
    b"market-squawk/official-options-reference/canonical-resolution/v1";
const STRICT_REFERENCE_REQUEST_ROW_SET_DIGEST_DOMAIN: &[u8] =
    b"market-squawk/options-reference-strict-request-row-set/v1";
const REFERENCE_ALIAS_ASSERTION_ELEMENT_DIGEST_DOMAIN: &[u8] =
    b"market-squawk/options-reference-alias-assertion-element/v1";
const REFERENCE_ALIAS_ASSERTION_CLOSURE_DIGEST_DOMAIN: &[u8] =
    b"market-squawk/options-reference-alias-assertion-closure/v1";
const MAX_REFERENCE_ALIAS_ASSERTION_BYTES: usize = 64 * 1024;
const STRICT_REFERENCE_ROW_SET_DIGEST_DOMAIN: &[u8] =
    b"market-squawk/options-reference-strict-row-set/v1";

/// Provider namespace for one exact official reference source.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum OfficialOptionsReferenceProvider {
    /// The Options Clearing Corporation.
    Occ,
    /// Cboe U.S. Options exchanges.
    Cboe,
}

impl OfficialOptionsReferenceProvider {
    const ALL: [Self; 2] = [Self::Occ, Self::Cboe];

    const fn database_name(self) -> &'static str {
        match self {
            Self::Occ => "occ",
            Self::Cboe => "cboe",
        }
    }

    fn from_database(value: &str) -> Result<Self, OfficialOptionsReferenceError> {
        match value {
            "occ" => Ok(Self::Occ),
            "cboe" => Ok(Self::Cboe),
            _ => Err(OfficialOptionsReferenceError::CorruptCatalog),
        }
    }
}

/// Exact selected provider surface represented by one immutable raw object.
#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum OfficialOptionsReferenceSurface {
    /// One venue-specific Cboe `All Series` file.
    CboeAllSeries {
        /// Exact venue namespace retained by the application mapping.
        venue: VenueId,
    },
    /// OCC selected-field DLP text.
    OccDlpSelectedText,
    /// OCC dated DLP text.
    OccDlpDailyText,
    /// OCC dated DLP XML.
    OccDlpDailyXml,
    /// OCC Information Memo CSV index.
    OccMemoIndexCsv,
    /// Complete OCC Information Memo JSON index surface.
    OccMemoIndexJson,
    /// One complete OCC memo document retained without economic interpretation.
    OccMemoDocument {
        /// Exact OCC memo number.
        memo_number: u64,
    },
    /// One retained OCC memo attachment.
    OccMemoAttachment {
        /// Exact OCC memo number.
        memo_number: u64,
        /// One-based attachment ordinal.
        ordinal: u32,
    },
}

impl OfficialOptionsReferenceSurface {
    /// Returns the provider owning this surface.
    pub const fn provider(&self) -> OfficialOptionsReferenceProvider {
        match self {
            Self::CboeAllSeries { .. } => OfficialOptionsReferenceProvider::Cboe,
            Self::OccDlpSelectedText
            | Self::OccDlpDailyText
            | Self::OccDlpDailyXml
            | Self::OccMemoIndexCsv
            | Self::OccMemoIndexJson
            | Self::OccMemoDocument { .. }
            | Self::OccMemoAttachment { .. } => OfficialOptionsReferenceProvider::Occ,
        }
    }

    fn stable_key(&self) -> Result<String, OfficialOptionsReferenceError> {
        match self {
            Self::CboeAllSeries { venue } if cboe_venue_rank(venue).is_some() => {
                Ok(format!("cboe-all-series:{}", venue.as_str()))
            }
            Self::OccDlpSelectedText => Ok("occ-dlp-selected-text".to_owned()),
            Self::OccDlpDailyText => Ok("occ-dlp-daily-text".to_owned()),
            Self::OccDlpDailyXml => Ok("occ-dlp-daily-xml".to_owned()),
            Self::OccMemoIndexCsv => Ok("occ-memo-index-csv".to_owned()),
            Self::OccMemoIndexJson => Ok("occ-memo-index-json".to_owned()),
            Self::OccMemoDocument { memo_number } if *memo_number > 0 => {
                Ok(format!("occ-memo-document:{memo_number}"))
            }
            Self::OccMemoAttachment {
                memo_number,
                ordinal,
            } if *memo_number > 0 && *ordinal > 0 => {
                Ok(format!("occ-memo-attachment:{memo_number}:{ordinal}"))
            }
            _ => Err(OfficialOptionsReferenceError::InvalidInput),
        }
    }

    fn adapter_order_cmp(&self, other: &Self) -> Result<Ordering, OfficialOptionsReferenceError> {
        let rank = |surface: &Self| -> Result<(u8, u64, u32), OfficialOptionsReferenceError> {
            match surface {
                Self::CboeAllSeries { venue } => Ok((
                    0,
                    u64::from(
                        cboe_venue_rank(venue)
                            .ok_or(OfficialOptionsReferenceError::InvalidInput)?,
                    ),
                    0,
                )),
                Self::OccDlpSelectedText => Ok((1, 0, 0)),
                Self::OccDlpDailyText => Ok((2, 0, 0)),
                Self::OccDlpDailyXml => Ok((3, 0, 0)),
                Self::OccMemoIndexCsv => Ok((4, 0, 0)),
                Self::OccMemoIndexJson => Ok((5, 0, 0)),
                Self::OccMemoDocument { memo_number } if *memo_number > 0 => {
                    Ok((6, *memo_number, 0))
                }
                Self::OccMemoAttachment {
                    memo_number,
                    ordinal,
                } if *memo_number > 0 && *ordinal > 0 => Ok((7, *memo_number, *ordinal)),
                _ => Err(OfficialOptionsReferenceError::InvalidInput),
            }
        };
        Ok(rank(self)?.cmp(&rank(other)?))
    }
}

fn cboe_venue_rank(venue: &VenueId) -> Option<u8> {
    match venue.as_str() {
        "c1" => Some(0),
        "bzx" => Some(1),
        "c2" => Some(2),
        "edgx" => Some(3),
        _ => None,
    }
}

/// Exact raw-object and strict-parser evidence for one selected surface.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OfficialOptionsReferenceObjectInput {
    object_ordinal: u16,
    source_id: SourceId,
    surface: OfficialOptionsReferenceSurface,
    object_id: SourceIdentifier,
    native_schema: SourceIdentifier,
    raw_receipt: Option<ResearchObjectReceipt>,
    raw_claim: ResearchObjectClaim,
    payload_digest: EvidenceDigest,
    source_timestamp: Option<Timestamp>,
    available_at: Timestamp,
    received_at: Timestamp,
    strict_row_set_digest: EvidenceDigest,
    strict_row_count: u64,
}

/// Complete checked-object construction fields.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OfficialOptionsReferenceObjectInputFields {
    /// Zero-based position in the exact selected-surface request order.
    pub object_ordinal: u16,
    /// Exact registered source owning the object.
    pub source_id: SourceId,
    /// Exact selected surface.
    pub surface: OfficialOptionsReferenceSurface,
    /// Provider object identity.
    pub object_id: SourceIdentifier,
    /// Closed native decoder identity.
    pub native_schema: SourceIdentifier,
    /// Store-issued authority for the content-addressed raw logical object.
    pub raw_receipt: ResearchObjectReceipt,
    /// Exact provider response-body digest.
    pub payload_digest: EvidenceDigest,
    /// Source-authored object time when the surface supplied one.
    pub source_timestamp: Option<Timestamp>,
    /// Earliest exact local/provider availability instant retained for this object.
    pub available_at: Timestamp,
    /// Local receipt instant.
    pub received_at: Timestamp,
    /// Exact ordered strict typed-row identity for this object.
    pub strict_row_set_digest: EvidenceDigest,
    /// Exact strict typed-row count, including non-identity memo discovery rows.
    pub strict_row_count: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct OfficialOptionsReferenceObjectCoordinates {
    object_ordinal: u16,
    source_id: SourceId,
    surface: OfficialOptionsReferenceSurface,
    object_id: SourceIdentifier,
    native_schema: SourceIdentifier,
    payload_digest: EvidenceDigest,
    source_timestamp: Option<Timestamp>,
    available_at: Timestamp,
    received_at: Timestamp,
    strict_row_set_digest: EvidenceDigest,
    strict_row_count: u64,
}

impl OfficialOptionsReferenceObjectInput {
    /// Returns the adapter-compatible strict-row commitment for a complete zero-row object.
    /// Uninterpreted official memo bodies use this value instead of inventing a typed row.
    pub fn empty_strict_row_set_digest() -> EvidenceDigest {
        strict_empty_row_set_digest()
    }

    /// Closes the complete provider-native decoder identity into the existing bounded catalog
    /// coordinate without adding a parallel schema registry.
    pub fn try_native_schema_identity(
        name: &SourceIdentifier,
        version: std::num::NonZeroU32,
        fingerprint: EvidenceDigest,
    ) -> Result<SourceIdentifier, OfficialOptionsReferenceError> {
        validate_sha256(fingerprint)?;
        let mut encoded = String::new();
        encoded
            .try_reserve_exact(name.as_str().len().saturating_add(78))
            .map_err(|_error| OfficialOptionsReferenceError::CapacityExceeded)?;
        encoded.push_str(name.as_str());
        encoded.push_str("@v");
        encoded.push_str(&version.get().to_string());
        encoded.push_str(":sha256:");
        for byte in fingerprint.bytes() {
            use std::fmt::Write as _;
            write!(&mut encoded, "{byte:02x}")
                .map_err(|_error| OfficialOptionsReferenceError::CapacityExceeded)?;
        }
        if encoded.len() > MAX_NATIVE_SCHEMA_BYTES {
            return Err(OfficialOptionsReferenceError::InvalidInput);
        }
        SourceIdentifier::try_from(encoded)
            .map_err(|_error| OfficialOptionsReferenceError::InvalidInput)
    }

    /// Validates one bounded raw/strict object coordinate.
    pub fn try_new(
        fields: OfficialOptionsReferenceObjectInputFields,
    ) -> Result<Self, OfficialOptionsReferenceError> {
        let OfficialOptionsReferenceObjectInputFields {
            object_ordinal,
            source_id,
            surface,
            object_id,
            native_schema,
            raw_receipt,
            payload_digest,
            source_timestamp,
            available_at,
            received_at,
            strict_row_set_digest,
            strict_row_count,
        } = fields;
        let raw_claim = raw_receipt.claim().clone();
        Self::try_new_with_raw_authority(
            OfficialOptionsReferenceObjectCoordinates {
                object_ordinal,
                source_id,
                surface,
                object_id,
                native_schema,
                payload_digest,
                source_timestamp,
                available_at,
                received_at,
                strict_row_set_digest,
                strict_row_count,
            },
            Some(raw_receipt),
            raw_claim,
        )
    }

    fn try_new_with_raw_authority(
        fields: OfficialOptionsReferenceObjectCoordinates,
        raw_receipt: Option<ResearchObjectReceipt>,
        raw_claim: ResearchObjectClaim,
    ) -> Result<Self, OfficialOptionsReferenceError> {
        if usize::from(fields.object_ordinal) >= MAX_OFFICIAL_OPTIONS_REFERENCE_OBJECTS
            || fields.object_id.as_str().len() > MAX_OBJECT_ID_BYTES
            || fields.native_schema.as_str().len() > MAX_NATIVE_SCHEMA_BYTES
            || raw_claim.content_digest() != fields.payload_digest
            || raw_claim.size_bytes() == 0
            || raw_claim.size_bytes() > MAX_TOTAL_PAYLOAD_BYTES
            || fields
                .source_timestamp
                .is_some_and(|time| time > fields.received_at)
            || fields.available_at > fields.received_at
            || fields.strict_row_count > MAX_OFFICIAL_OPTIONS_REFERENCE_STRICT_ROWS
            || (fields.strict_row_count == 0
                && fields.strict_row_set_digest != strict_empty_row_set_digest())
        {
            return Err(OfficialOptionsReferenceError::InvalidInput);
        }
        validate_sha256(fields.payload_digest)?;
        validate_sha256(fields.strict_row_set_digest)?;
        validate_sha256(raw_claim.physical_receipt_digest())?;
        if raw_receipt
            .as_ref()
            .is_some_and(|receipt| receipt.claim() != &raw_claim)
        {
            return Err(OfficialOptionsReferenceError::InvalidRawObjectAuthority);
        }
        Ok(Self {
            object_ordinal: fields.object_ordinal,
            source_id: fields.source_id,
            surface: fields.surface,
            object_id: fields.object_id,
            native_schema: fields.native_schema,
            raw_receipt,
            raw_claim,
            payload_digest: fields.payload_digest,
            source_timestamp: fields.source_timestamp,
            available_at: fields.available_at,
            received_at: fields.received_at,
            strict_row_set_digest: fields.strict_row_set_digest,
            strict_row_count: fields.strict_row_count,
        })
    }

    /// Returns the zero-based request position.
    pub const fn object_ordinal(&self) -> u16 {
        self.object_ordinal
    }

    /// Returns the exact source identity.
    pub const fn source_id(&self) -> &SourceId {
        &self.source_id
    }

    /// Returns the selected provider surface.
    pub const fn surface(&self) -> &OfficialOptionsReferenceSurface {
        &self.surface
    }

    fn raw_claim(&self) -> &ResearchObjectClaim {
        &self.raw_claim
    }

    fn has_live_raw_authority(&self) -> bool {
        self.raw_receipt
            .as_ref()
            .is_some_and(|receipt| receipt.claim() == &self.raw_claim)
    }

    fn try_from_retained_claim(
        fields: OfficialOptionsReferenceObjectCoordinates,
        claim: ResearchObjectClaim,
    ) -> Result<Self, OfficialOptionsReferenceError> {
        Self::try_new_with_raw_authority(fields, None, claim)
    }
}

fn strict_empty_row_set_digest() -> EvidenceDigest {
    let mut digest = Sha256::new();
    digest.update(STRICT_REFERENCE_ROW_SET_DIGEST_DOMAIN);
    digest.update(0_u32.to_be_bytes());
    finalize(digest)
}

/// Closed OCC DLP product type retained from the provider layout.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub enum OfficialOptionsReferenceOccProductType {
    #[serde(rename = "EU")]
    EquityUnderlying,
    #[serde(rename = "EB")]
    EquityBounds,
    #[serde(rename = "EL")]
    EquityLongTerm,
    #[serde(rename = "EF")]
    EquityFlex,
    #[serde(rename = "CU")]
    CurrencyUnderlying,
    #[serde(rename = "CL")]
    CurrencyLongTerm,
    #[serde(rename = "CM")]
    CurrencyMonthEnd,
    #[serde(rename = "CF")]
    CurrencyFlex,
    #[serde(rename = "IL")]
    IndexLongTerm,
    #[serde(rename = "IU")]
    IndexUnderlying,
    #[serde(rename = "IF")]
    IndexFlex,
    #[serde(rename = "GF")]
    InterestRateFutures,
    #[serde(rename = "SF")]
    StockFutures,
    #[serde(rename = "FC")]
    FuturesCashIndex,
    #[serde(rename = "FP")]
    FuturesPhysicalIndex,
    #[serde(rename = "TU")]
    TreasuryUnderlying,
    #[serde(rename = "TL")]
    TreasuryLongTerm,
}

impl OfficialOptionsReferenceOccProductType {
    /// Returns the exact two-character OCC code.
    pub const fn code(self) -> &'static str {
        match self {
            Self::EquityUnderlying => "EU",
            Self::EquityBounds => "EB",
            Self::EquityLongTerm => "EL",
            Self::EquityFlex => "EF",
            Self::CurrencyUnderlying => "CU",
            Self::CurrencyLongTerm => "CL",
            Self::CurrencyMonthEnd => "CM",
            Self::CurrencyFlex => "CF",
            Self::IndexLongTerm => "IL",
            Self::IndexUnderlying => "IU",
            Self::IndexFlex => "IF",
            Self::InterestRateFutures => "GF",
            Self::StockFutures => "SF",
            Self::FuturesCashIndex => "FC",
            Self::FuturesPhysicalIndex => "FP",
            Self::TreasuryUnderlying => "TU",
            Self::TreasuryLongTerm => "TL",
        }
    }

    const fn is_equity_product(self) -> bool {
        matches!(
            self,
            Self::EquityUnderlying | Self::EquityBounds | Self::EquityLongTerm | Self::EquityFlex
        )
    }
}

/// Exact OCC position-limit state without converting unavailable data to zero.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case", tag = "state", content = "value")]
pub enum OfficialOptionsReferenceOccPositionLimit {
    /// Documented equity-product nonzero limit.
    EquityReported(NonZeroU64),
    /// Documented non-equity unavailable sentinel.
    NonEquityUnavailableZero,
    /// Provider value retained outside the layout's documented non-equity scope.
    NonEquityProviderValueOutsideDocumentedScope(NonZeroU64),
}

/// Exact OCC exchange-list field disposition retained without inferring tradability.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum OfficialOptionsReferenceOccExchangeListingEvidence {
    /// One or more exact OCC exchange codes were reported.
    Reported,
    /// The selected-directory blank sentinel was retained as not reported.
    NotReportedInSelectedDirectory,
}

/// One exact provider-native Cboe series value.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct OfficialOptionsReferenceCboeSeries {
    venue: VenueId,
    cboe_symbol: String,
    osi: OccOptionIdentity,
    underlying_alias: ProviderInstrumentId,
    matching_unit: NonZeroU16,
    closing_only: bool,
}

impl OfficialOptionsReferenceCboeSeries {
    /// Constructs exact Cboe row semantics without resolving the underlying alias.
    pub fn try_new(
        venue: VenueId,
        cboe_symbol: impl Into<String>,
        osi: OccOptionIdentity,
        underlying_alias: ProviderInstrumentId,
        matching_unit: NonZeroU16,
        closing_only: bool,
    ) -> Result<Self, OfficialOptionsReferenceError> {
        let cboe_symbol = cboe_symbol.into();
        if cboe_symbol.len() != MAX_CBOE_SYMBOL_BYTES
            || !cboe_symbol.bytes().all(|byte| byte.is_ascii_alphanumeric())
        {
            return Err(OfficialOptionsReferenceError::InvalidInput);
        }
        let value = Self {
            venue,
            cboe_symbol,
            osi,
            underlying_alias,
            matching_unit,
            closing_only,
        };
        value
            .validate()
            .map_err(|_| OfficialOptionsReferenceError::InvalidInput)?;
        Ok(value)
    }

    pub const fn venue(&self) -> &VenueId {
        &self.venue
    }

    pub fn cboe_symbol(&self) -> &str {
        &self.cboe_symbol
    }

    pub const fn osi(&self) -> &OccOptionIdentity {
        &self.osi
    }

    pub const fn underlying_alias(&self) -> &ProviderInstrumentId {
        &self.underlying_alias
    }

    pub const fn matching_unit(&self) -> NonZeroU16 {
        self.matching_unit
    }

    pub const fn closing_only(&self) -> bool {
        self.closing_only
    }

    fn validate(&self) -> Result<(), OfficialOptionsReferenceError> {
        if cboe_venue_rank(&self.venue).is_some()
            && self.cboe_symbol.len() == MAX_CBOE_SYMBOL_BYTES
            && self
                .cboe_symbol
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric())
            && self.underlying_alias.as_str().len() <= MAX_CBOE_UNDERLYING_BYTES
            && self
                .underlying_alias
                .as_str()
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'.')
        {
            Ok(())
        } else {
            Err(OfficialOptionsReferenceError::CorruptCatalog)
        }
    }
}

/// One exact provider-native OCC DLP product/root value.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct OfficialOptionsReferenceOccProduct {
    options_symbol: ProviderInstrumentId,
    underlying_alias: ProviderInstrumentId,
    symbol_name: String,
    exchange_codes: String,
    exchange_listing_evidence: OfficialOptionsReferenceOccExchangeListingEvidence,
    position_limit: OfficialOptionsReferenceOccPositionLimit,
    product_type: OfficialOptionsReferenceOccProductType,
}

impl OfficialOptionsReferenceOccProduct {
    /// Constructs exact OCC product semantics; symbols remain provider aliases only.
    pub fn try_new(
        options_symbol: ProviderInstrumentId,
        underlying_alias: ProviderInstrumentId,
        symbol_name: impl Into<String>,
        exchange_codes: impl Into<String>,
        exchange_listing_evidence: OfficialOptionsReferenceOccExchangeListingEvidence,
        position_limit: OfficialOptionsReferenceOccPositionLimit,
        product_type: OfficialOptionsReferenceOccProductType,
    ) -> Result<Self, OfficialOptionsReferenceError> {
        let value = Self {
            options_symbol,
            underlying_alias,
            symbol_name: symbol_name.into(),
            exchange_codes: exchange_codes.into(),
            exchange_listing_evidence,
            position_limit,
            product_type,
        };
        value
            .validate()
            .map_err(|_| OfficialOptionsReferenceError::InvalidInput)?;
        Ok(value)
    }

    pub const fn options_symbol(&self) -> &ProviderInstrumentId {
        &self.options_symbol
    }

    pub const fn underlying_alias(&self) -> &ProviderInstrumentId {
        &self.underlying_alias
    }

    pub fn symbol_name(&self) -> &str {
        &self.symbol_name
    }

    pub fn exchange_codes(&self) -> &str {
        &self.exchange_codes
    }

    pub const fn exchange_listing_evidence(
        &self,
    ) -> OfficialOptionsReferenceOccExchangeListingEvidence {
        self.exchange_listing_evidence
    }

    pub const fn position_limit(&self) -> OfficialOptionsReferenceOccPositionLimit {
        self.position_limit
    }

    pub const fn product_type(&self) -> OfficialOptionsReferenceOccProductType {
        self.product_type
    }

    fn validate(&self) -> Result<(), OfficialOptionsReferenceError> {
        const CODES: &str = "ABCDEFHIJKLMPQRTUWXZ";
        let valid_symbol = |value: &ProviderInstrumentId| {
            value.as_str().len() <= MAX_OCC_SYMBOL_BYTES
                && value.as_str().bytes().all(|byte| {
                    byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-' | b'_' | b'/')
                })
        };
        if !valid_symbol(&self.options_symbol)
            || !valid_symbol(&self.underlying_alias)
            || self.symbol_name.is_empty()
            || self.symbol_name.len() > MAX_OCC_SYMBOL_NAME_BYTES
            || self.symbol_name.chars().any(char::is_control)
            || self.exchange_codes.len() > MAX_EXCHANGE_CODES_BYTES
            || !self
                .exchange_codes
                .bytes()
                .all(|byte| byte.is_ascii_uppercase() && CODES.as_bytes().contains(&byte))
            || self
                .exchange_codes
                .as_bytes()
                .windows(2)
                .any(|pair| pair[0] >= pair[1])
            || matches!(
                (
                    self.exchange_codes.is_empty(),
                    self.exchange_listing_evidence,
                ),
                (
                    true,
                    OfficialOptionsReferenceOccExchangeListingEvidence::Reported
                ) | (
                    false,
                    OfficialOptionsReferenceOccExchangeListingEvidence::NotReportedInSelectedDirectory
                )
            )
            || self.product_type.is_equity_product()
                != matches!(
                    self.position_limit,
                    OfficialOptionsReferenceOccPositionLimit::EquityReported(_)
                )
        {
            Err(OfficialOptionsReferenceError::CorruptCatalog)
        } else {
            Ok(())
        }
    }
}

/// Exact durable provider row value. Every symbol remains provider-native reference evidence.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case", tag = "kind", content = "value")]
pub enum OfficialOptionsReferenceRecordValue {
    /// Cboe venue series with exact OSI and unresolved underlying alias.
    CboeSeries(OfficialOptionsReferenceCboeSeries),
    /// OCC product/root row; not an individual option contract.
    OccProduct(OfficialOptionsReferenceOccProduct),
}

impl OfficialOptionsReferenceRecordValue {
    /// Returns the provider owning this row value.
    pub const fn provider(&self) -> OfficialOptionsReferenceProvider {
        match self {
            Self::CboeSeries(_) => OfficialOptionsReferenceProvider::Cboe,
            Self::OccProduct(_) => OfficialOptionsReferenceProvider::Occ,
        }
    }

    fn validate(&self) -> Result<(), OfficialOptionsReferenceError> {
        match self {
            Self::CboeSeries(value) => value.validate(),
            Self::OccProduct(value) => value.validate(),
        }
    }
}

/// One exact staged identity row bound to an object and provider row coordinate.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct OfficialOptionsReferenceRecordInput {
    object_ordinal: u16,
    provider_row_number: u32,
    record_id: SourceIdentifier,
    value: OfficialOptionsReferenceRecordValue,
}

impl OfficialOptionsReferenceRecordInput {
    /// Constructs one exact staged row.
    pub fn try_new(
        object_ordinal: u16,
        provider_row_number: u32,
        record_id: SourceIdentifier,
        value: OfficialOptionsReferenceRecordValue,
    ) -> Result<Self, OfficialOptionsReferenceError> {
        if usize::from(object_ordinal) >= MAX_OFFICIAL_OPTIONS_REFERENCE_OBJECTS
            || provider_row_number == 0
            || u64::from(provider_row_number) > MAX_OFFICIAL_OPTIONS_REFERENCE_STRICT_ROWS + 1
        {
            return Err(OfficialOptionsReferenceError::InvalidInput);
        }
        value
            .validate()
            .map_err(|_| OfficialOptionsReferenceError::InvalidInput)?;
        Ok(Self {
            object_ordinal,
            provider_row_number,
            record_id,
            value,
        })
    }

    pub const fn object_ordinal(&self) -> u16 {
        self.object_ordinal
    }

    pub const fn provider_row_number(&self) -> u32 {
        self.provider_row_number
    }

    pub const fn record_id(&self) -> &SourceIdentifier {
        &self.record_id
    }

    pub const fn value(&self) -> &OfficialOptionsReferenceRecordValue {
        &self.value
    }
}

/// Exact adapter-compatible multiset commitment for all provider alias assertions in a request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OfficialOptionsReferenceAliasAssertionSetEvidence {
    request_id: SourceIdentifier,
    assertions: u64,
    digest_sum: [u8; 32],
    digest_xor: [u8; 32],
}

impl OfficialOptionsReferenceAliasAssertionSetEvidence {
    pub const fn request_id(&self) -> &SourceIdentifier {
        &self.request_id
    }

    pub const fn assertions(&self) -> u64 {
        self.assertions
    }

    fn closure_digest(
        &self,
        strict_row_set_digest: EvidenceDigest,
    ) -> Result<EvidenceDigest, OfficialOptionsReferenceError> {
        validate_sha256(strict_row_set_digest)?;
        let mut digest = Sha256::new();
        digest.update(REFERENCE_ALIAS_ASSERTION_CLOSURE_DIGEST_DOMAIN);
        digest.update(strict_row_set_digest.bytes());
        digest.update(self.assertions.to_be_bytes());
        digest.update(self.digest_sum);
        digest.update(self.digest_xor);
        Ok(finalize(digest))
    }
}

/// Streaming adapter-compatible alias-assertion commitment builder.
#[derive(Debug)]
pub struct OfficialOptionsReferenceAliasAssertionSetBuilder {
    request_id: SourceIdentifier,
    assertions: u64,
    digest_sum: [u8; 32],
    digest_xor: [u8; 32],
}

impl OfficialOptionsReferenceAliasAssertionSetBuilder {
    pub const fn new(request_id: SourceIdentifier) -> Self {
        Self {
            request_id,
            assertions: 0,
            digest_sum: [0; 32],
            digest_xor: [0; 32],
        }
    }

    pub fn try_observe(
        &mut self,
        record: &OfficialOptionsReferenceRecordInput,
    ) -> Result<(), OfficialOptionsReferenceError> {
        record.value.validate()?;
        self.try_observe_parts(&record.record_id, &record.value)
    }

    fn try_observe_parts(
        &mut self,
        record_id: &SourceIdentifier,
        value: &OfficialOptionsReferenceRecordValue,
    ) -> Result<(), OfficialOptionsReferenceError> {
        observe_adapter_alias_assertions(
            &self.request_id,
            record_id,
            value,
            &mut self.assertions,
            &mut self.digest_sum,
            &mut self.digest_xor,
        )
    }

    pub fn finish(self) -> OfficialOptionsReferenceAliasAssertionSetEvidence {
        OfficialOptionsReferenceAliasAssertionSetEvidence {
            request_id: self.request_id,
            assertions: self.assertions,
            digest_sum: self.digest_sum,
            digest_xor: self.digest_xor,
        }
    }
}

#[derive(Serialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
enum AdapterAliasKeyWire<'a> {
    CboeSymbol {
        symbol: &'a str,
    },
    CboeOsi {
        osi: &'a OccOptionIdentity,
    },
    CboeVenueSymbol {
        venue: &'a str,
        symbol: &'a str,
    },
    OccProduct {
        options_symbol: &'a ProviderInstrumentId,
        product_type: OfficialOptionsReferenceOccProductType,
    },
}

#[derive(Serialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
enum AdapterAliasTargetWire<'a> {
    CboeContract {
        osi: &'a OccOptionIdentity,
        underlying: &'a ProviderInstrumentId,
    },
    CboeSymbol {
        symbol: &'a str,
    },
    ProviderRecord,
}

#[derive(Serialize)]
struct AdapterAliasAssertionWire<'a> {
    request_id: &'a SourceIdentifier,
    key: AdapterAliasKeyWire<'a>,
    target: AdapterAliasTargetWire<'a>,
    evidence: &'a SourceIdentifier,
}

fn observe_adapter_alias_assertions(
    request_id: &SourceIdentifier,
    evidence: &SourceIdentifier,
    value: &OfficialOptionsReferenceRecordValue,
    assertions: &mut u64,
    digest_sum: &mut [u8; 32],
    digest_xor: &mut [u8; 32],
) -> Result<(), OfficialOptionsReferenceError> {
    match value {
        OfficialOptionsReferenceRecordValue::CboeSeries(series) => {
            observe_adapter_alias_assertion(
                &AdapterAliasAssertionWire {
                    request_id,
                    key: AdapterAliasKeyWire::CboeSymbol {
                        symbol: series.cboe_symbol(),
                    },
                    target: AdapterAliasTargetWire::CboeContract {
                        osi: series.osi(),
                        underlying: series.underlying_alias(),
                    },
                    evidence,
                },
                assertions,
                digest_sum,
                digest_xor,
            )?;
            observe_adapter_alias_assertion(
                &AdapterAliasAssertionWire {
                    request_id,
                    key: AdapterAliasKeyWire::CboeOsi { osi: series.osi() },
                    target: AdapterAliasTargetWire::CboeSymbol {
                        symbol: series.cboe_symbol(),
                    },
                    evidence,
                },
                assertions,
                digest_sum,
                digest_xor,
            )?;
            observe_adapter_alias_assertion(
                &AdapterAliasAssertionWire {
                    request_id,
                    key: AdapterAliasKeyWire::CboeVenueSymbol {
                        venue: series.venue().as_str(),
                        symbol: series.cboe_symbol(),
                    },
                    target: AdapterAliasTargetWire::ProviderRecord,
                    evidence,
                },
                assertions,
                digest_sum,
                digest_xor,
            )
        }
        OfficialOptionsReferenceRecordValue::OccProduct(product) => {
            observe_adapter_alias_assertion(
                &AdapterAliasAssertionWire {
                    request_id,
                    key: AdapterAliasKeyWire::OccProduct {
                        options_symbol: product.options_symbol(),
                        product_type: product.product_type(),
                    },
                    target: AdapterAliasTargetWire::ProviderRecord,
                    evidence,
                },
                assertions,
                digest_sum,
                digest_xor,
            )
        }
    }
}

fn observe_adapter_alias_assertion(
    assertion: &AdapterAliasAssertionWire<'_>,
    assertions: &mut u64,
    digest_sum: &mut [u8; 32],
    digest_xor: &mut [u8; 32],
) -> Result<(), OfficialOptionsReferenceError> {
    let mut counter = BoundedJsonLength { written: 0 };
    serde_json::to_writer(&mut counter, assertion)
        .map_err(|_| OfficialOptionsReferenceError::InvalidInput)?;
    if counter.written == 0 || counter.written > MAX_REFERENCE_ALIAS_ASSERTION_BYTES {
        return Err(OfficialOptionsReferenceError::InvalidInput);
    }
    let mut digest = Sha256::new();
    digest.update(REFERENCE_ALIAS_ASSERTION_ELEMENT_DIGEST_DOMAIN);
    digest.update(to_u64(counter.written)?.to_be_bytes());
    let mut writer = Sha256JsonWriter {
        digest: &mut digest,
        written: 0,
    };
    serde_json::to_writer(&mut writer, assertion)
        .map_err(|_| OfficialOptionsReferenceError::InvalidInput)?;
    if writer.written != counter.written {
        return Err(OfficialOptionsReferenceError::InvalidInput);
    }
    let element: [u8; 32] = digest.finalize().into();
    add_digest_modulo_256(digest_sum, element);
    for (aggregate, element) in digest_xor.iter_mut().zip(element) {
        *aggregate ^= element;
    }
    *assertions = assertions
        .checked_add(1)
        .filter(|count| *count <= MAX_OFFICIAL_OPTIONS_REFERENCE_ALIAS_ASSERTIONS)
        .ok_or(OfficialOptionsReferenceError::CapacityExceeded)?;
    Ok(())
}

struct BoundedJsonLength {
    written: usize,
}

impl std::io::Write for BoundedJsonLength {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let next = self
            .written
            .checked_add(bytes.len())
            .filter(|next| *next <= MAX_REFERENCE_ALIAS_ASSERTION_BYTES)
            .ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "alias assertion JSON exceeds its bound",
                )
            })?;
        self.written = next;
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

struct Sha256JsonWriter<'a> {
    digest: &'a mut Sha256,
    written: usize,
}

impl std::io::Write for Sha256JsonWriter<'_> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let next = self.written.checked_add(bytes.len()).ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "alias assertion JSON length overflow",
            )
        })?;
        self.digest.update(bytes);
        self.written = next;
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn add_digest_modulo_256(aggregate: &mut [u8; 32], element: [u8; 32]) {
    let mut carry = false;
    for index in (0..aggregate.len()).rev() {
        let (sum, first_carry) = aggregate[index].overflowing_add(element[index]);
        let (sum, second_carry) = sum.overflowing_add(u8::from(carry));
        aggregate[index] = sum;
        carry = first_carry || second_carry;
    }
}

/// Exact request-scoped provider alias or natural key.
#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum OfficialOptionsReferenceAliasKey {
    CboeSymbol {
        symbol: String,
    },
    CboeOsi {
        osi: OccOptionIdentity,
    },
    CboeVenueSymbol {
        venue: VenueId,
        symbol: String,
    },
    OccProduct {
        options_symbol: ProviderInstrumentId,
        product_type: OfficialOptionsReferenceOccProductType,
    },
}

impl OfficialOptionsReferenceAliasKey {
    fn validate(&self) -> Result<(), OfficialOptionsReferenceError> {
        let valid_cboe = |symbol: &str| {
            symbol.len() == MAX_CBOE_SYMBOL_BYTES
                && symbol.bytes().all(|byte| byte.is_ascii_alphanumeric())
        };
        match self {
            Self::CboeSymbol { symbol } | Self::CboeVenueSymbol { symbol, .. }
                if !valid_cboe(symbol) =>
            {
                Err(OfficialOptionsReferenceError::InvalidInput)
            }
            _ => Ok(()),
        }
    }

    fn database_kind(&self) -> &'static str {
        match self {
            Self::CboeSymbol { .. } => "cboe_symbol",
            Self::CboeOsi { .. } => "cboe_osi",
            Self::CboeVenueSymbol { .. } => "cboe_venue_symbol",
            Self::OccProduct { .. } => "occ_product",
        }
    }
}

/// Terminal request-scoped alias state. Ambiguity never selects a winner.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum OfficialOptionsReferenceAliasResolutionState {
    Exact,
    Ambiguous,
}

impl OfficialOptionsReferenceAliasResolutionState {
    const fn database_name(self) -> &'static str {
        match self {
            Self::Exact => "exact",
            Self::Ambiguous => "ambiguous",
        }
    }

    fn from_database(value: &str) -> Result<Self, OfficialOptionsReferenceError> {
        match value {
            "exact" => Ok(Self::Exact),
            "ambiguous" => Ok(Self::Ambiguous),
            _ => Err(OfficialOptionsReferenceError::CorruptCatalog),
        }
    }
}

/// One complete request-scoped alias resolution produced by terminal reconciliation.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct OfficialOptionsReferenceAliasResolutionInput {
    key: OfficialOptionsReferenceAliasKey,
    state: OfficialOptionsReferenceAliasResolutionState,
    observations: u64,
    conflicts: u32,
}

impl OfficialOptionsReferenceAliasResolutionInput {
    pub fn try_new(
        key: OfficialOptionsReferenceAliasKey,
        state: OfficialOptionsReferenceAliasResolutionState,
        observations: u64,
        conflicts: u32,
    ) -> Result<Self, OfficialOptionsReferenceError> {
        key.validate()?;
        if observations == 0
            || observations > MAX_OFFICIAL_OPTIONS_REFERENCE_ALIAS_ASSERTIONS
            || u64::from(conflicts) > MAX_OFFICIAL_OPTIONS_REFERENCE_CONFLICTS
            || (state == OfficialOptionsReferenceAliasResolutionState::Exact && conflicts != 0)
            || (state == OfficialOptionsReferenceAliasResolutionState::Ambiguous && conflicts == 0)
        {
            return Err(OfficialOptionsReferenceError::InvalidInput);
        }
        Ok(Self {
            key,
            state,
            observations,
            conflicts,
        })
    }

    pub const fn key(&self) -> &OfficialOptionsReferenceAliasKey {
        &self.key
    }

    pub const fn state(&self) -> OfficialOptionsReferenceAliasResolutionState {
        self.state
    }

    pub const fn observations(&self) -> u64 {
        self.observations
    }

    pub const fn conflicts(&self) -> u32 {
        self.conflicts
    }
}

/// Provider-local conflict class preserved without a selected winner.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum OfficialOptionsReferenceConflictKind {
    CboeSymbolMapsMultipleOsi,
    CboeOsiMapsMultipleSymbols,
    CboeSymbolMapsMultipleUnderlying,
    DuplicateProviderRecord,
}

impl OfficialOptionsReferenceConflictKind {
    const fn database_name(self) -> &'static str {
        match self {
            Self::CboeSymbolMapsMultipleOsi => "cboe_symbol_maps_multiple_osi",
            Self::CboeOsiMapsMultipleSymbols => "cboe_osi_maps_multiple_symbols",
            Self::CboeSymbolMapsMultipleUnderlying => "cboe_symbol_maps_multiple_underlying",
            Self::DuplicateProviderRecord => "duplicate_provider_record",
        }
    }

    fn from_database(value: &str) -> Result<Self, OfficialOptionsReferenceError> {
        match value {
            "cboe_symbol_maps_multiple_osi" => Ok(Self::CboeSymbolMapsMultipleOsi),
            "cboe_osi_maps_multiple_symbols" => Ok(Self::CboeOsiMapsMultipleSymbols),
            "cboe_symbol_maps_multiple_underlying" => Ok(Self::CboeSymbolMapsMultipleUnderlying),
            "duplicate_provider_record" => Ok(Self::DuplicateProviderRecord),
            _ => Err(OfficialOptionsReferenceError::CorruptCatalog),
        }
    }

    fn accepts(self, key: &OfficialOptionsReferenceAliasKey) -> bool {
        matches!(
            (self, key),
            (
                Self::CboeSymbolMapsMultipleOsi | Self::CboeSymbolMapsMultipleUnderlying,
                OfficialOptionsReferenceAliasKey::CboeSymbol { .. }
            ) | (
                Self::CboeOsiMapsMultipleSymbols,
                OfficialOptionsReferenceAliasKey::CboeOsi { .. }
            ) | (
                Self::DuplicateProviderRecord,
                OfficialOptionsReferenceAliasKey::CboeVenueSymbol { .. }
                    | OfficialOptionsReferenceAliasKey::OccProduct { .. }
            )
        )
    }
}

/// One exact provider conflict and its two retained row-evidence identities.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct OfficialOptionsReferenceConflictInput {
    key: OfficialOptionsReferenceAliasKey,
    kind: OfficialOptionsReferenceConflictKind,
    first_evidence: SourceIdentifier,
    second_evidence: SourceIdentifier,
}

impl OfficialOptionsReferenceConflictInput {
    pub fn try_new(
        key: OfficialOptionsReferenceAliasKey,
        kind: OfficialOptionsReferenceConflictKind,
        first_evidence: SourceIdentifier,
        second_evidence: SourceIdentifier,
    ) -> Result<Self, OfficialOptionsReferenceError> {
        key.validate()?;
        if first_evidence == second_evidence || !kind.accepts(&key) {
            return Err(OfficialOptionsReferenceError::InvalidInput);
        }
        Ok(Self {
            key,
            kind,
            first_evidence,
            second_evidence,
        })
    }

    pub const fn key(&self) -> &OfficialOptionsReferenceAliasKey {
        &self.key
    }

    pub const fn kind(&self) -> OfficialOptionsReferenceConflictKind {
        self.kind
    }

    pub const fn first_evidence(&self) -> &SourceIdentifier {
        &self.first_evidence
    }

    pub const fn second_evidence(&self) -> &SourceIdentifier {
        &self.second_evidence
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum OrderedSetKind {
    Records,
    Resolutions,
    Conflicts,
}

/// Exact count and ordered SHA-256 identity of staged product/series rows.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OfficialOptionsReferenceRecordSetEvidence {
    count: u64,
    digest: EvidenceDigest,
}

impl OfficialOptionsReferenceRecordSetEvidence {
    pub const fn count(self) -> u64 {
        self.count
    }

    pub const fn digest(self) -> EvidenceDigest {
        self.digest
    }
}

/// Exact count and ordered SHA-256 identity of staged alias resolutions.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OfficialOptionsReferenceResolutionSetEvidence {
    count: u64,
    digest: EvidenceDigest,
}

impl OfficialOptionsReferenceResolutionSetEvidence {
    pub const fn count(self) -> u64 {
        self.count
    }

    pub const fn digest(self) -> EvidenceDigest {
        self.digest
    }
}

/// Exact count and ordered SHA-256 identity of staged conflicts.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OfficialOptionsReferenceConflictSetEvidence {
    count: u64,
    digest: EvidenceDigest,
}

impl OfficialOptionsReferenceConflictSetEvidence {
    pub const fn count(self) -> u64 {
        self.count
    }

    pub const fn digest(self) -> EvidenceDigest {
        self.digest
    }
}

#[derive(Debug)]
struct OrderedSetDigestBuilder {
    kind: OrderedSetKind,
    digest: Sha256,
    count: u64,
    previous_sort_key: Option<Vec<u8>>,
}

impl OrderedSetDigestBuilder {
    fn new(kind: OrderedSetKind) -> Self {
        let mut digest = Sha256::new();
        digest.update(match kind {
            OrderedSetKind::Records => ORDERED_RECORD_SET_DOMAIN,
            OrderedSetKind::Resolutions => ORDERED_RESOLUTION_SET_DOMAIN,
            OrderedSetKind::Conflicts => ORDERED_CONFLICT_SET_DOMAIN,
        });
        Self {
            kind,
            digest,
            count: 0,
            previous_sort_key: None,
        }
    }

    fn observe<T: Serialize>(
        &mut self,
        sort_key: Vec<u8>,
        value: &T,
    ) -> Result<(), OfficialOptionsReferenceError> {
        if self
            .previous_sort_key
            .as_ref()
            .is_some_and(|previous| previous >= &sort_key)
        {
            return Err(OfficialOptionsReferenceError::UnorderedStream);
        }
        let encoded = serde_json::to_vec(value)?;
        if encoded.is_empty() || encoded.len() > MAX_RECORD_JSON_BYTES {
            return Err(OfficialOptionsReferenceError::InvalidInput);
        }
        let next = self
            .count
            .checked_add(1)
            .ok_or(OfficialOptionsReferenceError::CapacityExceeded)?;
        let maximum = match self.kind {
            OrderedSetKind::Records => MAX_OFFICIAL_OPTIONS_REFERENCE_RECORDS,
            OrderedSetKind::Resolutions => MAX_OFFICIAL_OPTIONS_REFERENCE_ALIAS_RESOLUTIONS,
            OrderedSetKind::Conflicts => MAX_OFFICIAL_OPTIONS_REFERENCE_CONFLICTS,
        };
        if next > maximum {
            return Err(OfficialOptionsReferenceError::CapacityExceeded);
        }
        self.digest.update(next.to_be_bytes());
        hash_bytes(&mut self.digest, &sort_key)?;
        hash_bytes(&mut self.digest, &encoded)?;
        self.previous_sort_key = Some(sort_key);
        self.count = next;
        Ok(())
    }

    fn finish(mut self) -> (u64, EvidenceDigest) {
        self.digest.update(self.count.to_be_bytes());
        (self.count, finalize(self.digest))
    }
}

/// Incremental ordered digest builder for staged product/series rows.
#[derive(Debug)]
pub struct OfficialOptionsReferenceRecordSetDigestBuilder(OrderedSetDigestBuilder);

impl OfficialOptionsReferenceRecordSetDigestBuilder {
    pub fn new() -> Self {
        Self(OrderedSetDigestBuilder::new(OrderedSetKind::Records))
    }

    pub fn try_observe(
        &mut self,
        value: &OfficialOptionsReferenceRecordInput,
    ) -> Result<(), OfficialOptionsReferenceError> {
        value.value.validate()?;
        self.0.observe(record_sort_key(value)?, value)
    }

    pub fn finish(self) -> OfficialOptionsReferenceRecordSetEvidence {
        let (count, digest) = self.0.finish();
        OfficialOptionsReferenceRecordSetEvidence { count, digest }
    }
}

impl Default for OfficialOptionsReferenceRecordSetDigestBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Incremental ordered digest builder for staged alias resolutions.
#[derive(Debug)]
pub struct OfficialOptionsReferenceResolutionSetDigestBuilder(OrderedSetDigestBuilder);

impl OfficialOptionsReferenceResolutionSetDigestBuilder {
    pub fn new() -> Self {
        Self(OrderedSetDigestBuilder::new(OrderedSetKind::Resolutions))
    }

    pub fn try_observe(
        &mut self,
        value: &OfficialOptionsReferenceAliasResolutionInput,
    ) -> Result<(), OfficialOptionsReferenceError> {
        value.key.validate()?;
        self.0
            .observe(alias_key_json(&value.key)?.into_bytes(), value)
    }

    pub fn finish(self) -> OfficialOptionsReferenceResolutionSetEvidence {
        let (count, digest) = self.0.finish();
        OfficialOptionsReferenceResolutionSetEvidence { count, digest }
    }
}

impl Default for OfficialOptionsReferenceResolutionSetDigestBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Incremental ordered digest builder for staged provider conflicts.
#[derive(Debug)]
pub struct OfficialOptionsReferenceConflictSetDigestBuilder(OrderedSetDigestBuilder);

impl OfficialOptionsReferenceConflictSetDigestBuilder {
    pub fn new() -> Self {
        Self(OrderedSetDigestBuilder::new(OrderedSetKind::Conflicts))
    }

    pub fn try_observe(
        &mut self,
        value: &OfficialOptionsReferenceConflictInput,
    ) -> Result<(), OfficialOptionsReferenceError> {
        value.key.validate()?;
        self.0.observe(conflict_sort_key(value)?, value)
    }

    pub fn finish(self) -> OfficialOptionsReferenceConflictSetEvidence {
        let (count, digest) = self.0.finish();
        OfficialOptionsReferenceConflictSetEvidence { count, digest }
    }
}

impl Default for OfficialOptionsReferenceConflictSetDigestBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Immutable generation header closed around exact staged-set identities.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OfficialOptionsReferenceGenerationHeader {
    expected_previous_generation: Option<EvidenceDigest>,
    request_id: SourceIdentifier,
    requested_at: Timestamp,
    request_deadline: Timestamp,
    strict_row_set_digest: EvidenceDigest,
    alias_assertions: u64,
    alias_assertion_closure_digest: EvidenceDigest,
    alias_assertion_set: OfficialOptionsReferenceAliasAssertionSetEvidence,
    total_payload_bytes: u64,
    strict_row_count: u64,
    records: OfficialOptionsReferenceRecordSetEvidence,
    resolutions: OfficialOptionsReferenceResolutionSetEvidence,
    conflicts: OfficialOptionsReferenceConflictSetEvidence,
    objects: Box<[OfficialOptionsReferenceObjectInput]>,
}

impl OfficialOptionsReferenceGenerationHeader {
    #[allow(
        clippy::too_many_arguments,
        reason = "complete request closure stays explicit"
    )]
    pub fn try_new(
        expected_previous_generation: Option<EvidenceDigest>,
        request_id: SourceIdentifier,
        requested_at: Timestamp,
        request_deadline: Timestamp,
        alias_assertion_set: OfficialOptionsReferenceAliasAssertionSetEvidence,
        records: OfficialOptionsReferenceRecordSetEvidence,
        resolutions: OfficialOptionsReferenceResolutionSetEvidence,
        conflicts: OfficialOptionsReferenceConflictSetEvidence,
        mut objects: Vec<OfficialOptionsReferenceObjectInput>,
    ) -> Result<Self, OfficialOptionsReferenceError> {
        expected_previous_generation
            .map(validate_sha256)
            .transpose()?;
        validate_sha256(records.digest)?;
        validate_sha256(resolutions.digest)?;
        validate_sha256(conflicts.digest)?;
        if requested_at >= request_deadline
            || objects.len() < OfficialOptionsReferenceProvider::ALL.len()
            || objects.len() > MAX_OFFICIAL_OPTIONS_REFERENCE_OBJECTS
            || records.count == 0
            || records.count > MAX_OFFICIAL_OPTIONS_REFERENCE_RECORDS
            || resolutions.count == 0
            || resolutions.count > MAX_OFFICIAL_OPTIONS_REFERENCE_ALIAS_RESOLUTIONS
            || conflicts.count > MAX_OFFICIAL_OPTIONS_REFERENCE_CONFLICTS
            || alias_assertion_set.request_id != request_id
            || alias_assertion_set.assertions == 0
            || alias_assertion_set.assertions > MAX_OFFICIAL_OPTIONS_REFERENCE_ALIAS_ASSERTIONS
        {
            return Err(OfficialOptionsReferenceError::InvalidInput);
        }
        objects.sort_by_key(OfficialOptionsReferenceObjectInput::object_ordinal);
        let mut providers = BTreeSet::new();
        let mut source_ids = BTreeSet::new();
        let mut object_ids = BTreeSet::new();
        let mut surface_keys = BTreeSet::new();
        let mut total_payload_bytes = 0_u64;
        let mut strict_row_count = 0_u64;
        for (index, object) in objects.iter().enumerate() {
            let expected = u16::try_from(index)
                .map_err(|_| OfficialOptionsReferenceError::CapacityExceeded)?;
            if object.object_ordinal != expected
                || object.received_at < requested_at
                || object.received_at > request_deadline
                || (index > 0
                    && objects[index - 1]
                        .surface
                        .adapter_order_cmp(&object.surface)?
                        != Ordering::Less)
                || !object_ids.insert((object.surface.provider(), object.object_id.as_str()))
                || !surface_keys.insert((object.surface.provider(), object.surface.stable_key()?))
            {
                return Err(OfficialOptionsReferenceError::InvalidInput);
            }
            providers.insert(object.surface.provider());
            source_ids.insert((object.surface.provider(), object.source_id.as_str()));
            total_payload_bytes = total_payload_bytes
                .checked_add(object.raw_claim.size_bytes())
                .ok_or(OfficialOptionsReferenceError::CapacityExceeded)?;
            strict_row_count = strict_row_count
                .checked_add(object.strict_row_count)
                .ok_or(OfficialOptionsReferenceError::CapacityExceeded)?;
        }
        if providers.len() != OfficialOptionsReferenceProvider::ALL.len()
            || source_ids.len() != OfficialOptionsReferenceProvider::ALL.len()
            || total_payload_bytes == 0
            || total_payload_bytes > MAX_TOTAL_PAYLOAD_BYTES
            || strict_row_count == 0
            || strict_row_count > MAX_OFFICIAL_OPTIONS_REFERENCE_STRICT_ROWS
            || records.count > strict_row_count
        {
            return Err(OfficialOptionsReferenceError::InvalidInput);
        }
        let strict_row_set_digest =
            strict_request_row_set_digest(&request_id, &objects, strict_row_count)?;
        let alias_assertions = alias_assertion_set.assertions;
        let alias_assertion_closure_digest =
            alias_assertion_set.closure_digest(strict_row_set_digest)?;
        Ok(Self {
            expected_previous_generation,
            request_id,
            requested_at,
            request_deadline,
            strict_row_set_digest,
            alias_assertions,
            alias_assertion_closure_digest,
            alias_assertion_set,
            total_payload_bytes,
            strict_row_count,
            records,
            resolutions,
            conflicts,
            objects: objects.into_boxed_slice(),
        })
    }

    pub const fn expected_previous_generation(&self) -> Option<EvidenceDigest> {
        self.expected_previous_generation
    }

    pub const fn request_id(&self) -> &SourceIdentifier {
        &self.request_id
    }

    pub const fn requested_at(&self) -> Timestamp {
        self.requested_at
    }

    pub const fn request_deadline(&self) -> Timestamp {
        self.request_deadline
    }

    pub fn objects(&self) -> &[OfficialOptionsReferenceObjectInput] {
        &self.objects
    }

    pub const fn strict_row_set_digest(&self) -> EvidenceDigest {
        self.strict_row_set_digest
    }

    pub const fn alias_assertions(&self) -> u64 {
        self.alias_assertions
    }

    pub const fn alias_assertion_closure_digest(&self) -> EvidenceDigest {
        self.alias_assertion_closure_digest
    }

    pub const fn total_payload_bytes(&self) -> u64 {
        self.total_payload_bytes
    }

    pub const fn strict_row_count(&self) -> u64 {
        self.strict_row_count
    }

    /// Computes the exact provider-specific payload-set identity needed for rights admission.
    pub fn source_payload_set_digest(
        &self,
        provider: OfficialOptionsReferenceProvider,
    ) -> Result<EvidenceDigest, OfficialOptionsReferenceError> {
        source_payload_set_digest(provider, &self.objects)
    }
}

fn strict_request_row_set_digest(
    request_id: &SourceIdentifier,
    objects: &[OfficialOptionsReferenceObjectInput],
    strict_row_count: u64,
) -> Result<EvidenceDigest, OfficialOptionsReferenceError> {
    if objects.is_empty() || strict_row_count == 0 {
        return Err(OfficialOptionsReferenceError::InvalidInput);
    }
    let mut digest = Sha256::new();
    digest.update(STRICT_REFERENCE_REQUEST_ROW_SET_DIGEST_DOMAIN);
    digest.update(to_u64(request_id.as_str().len())?.to_be_bytes());
    digest.update(request_id.as_str().as_bytes());
    for object in objects {
        validate_sha256(object.strict_row_set_digest)?;
        let encoded_surface = serde_json::to_vec(&object.surface)?;
        if encoded_surface.is_empty() || encoded_surface.len() > 1_024 {
            return Err(OfficialOptionsReferenceError::InvalidInput);
        }
        digest.update(to_u64(encoded_surface.len())?.to_be_bytes());
        digest.update(encoded_surface);
        digest.update(object.strict_row_set_digest.bytes());
    }
    digest.update(strict_row_count.to_be_bytes());
    Ok(finalize(digest))
}

/// One provider/source/revision and its sealed persist/display grant.
#[derive(Clone)]
pub struct OfficialOptionsReferenceSourceAuthority {
    provider: OfficialOptionsReferenceProvider,
    metadata: SourceMetadata,
    rights: RegisteredRightsGrant,
}

impl fmt::Debug for OfficialOptionsReferenceSourceAuthority {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OfficialOptionsReferenceSourceAuthority")
            .field("provider", &self.provider)
            .field("source_id", self.metadata.source_id())
            .field("rights", &"[SEALED]")
            .finish()
    }
}

impl OfficialOptionsReferenceSourceAuthority {
    /// Binds one registered provider source to one already admitted exact payload-set grant.
    pub const fn new(
        provider: OfficialOptionsReferenceProvider,
        metadata: SourceMetadata,
        rights: RegisteredRightsGrant,
    ) -> Self {
        Self {
            provider,
            metadata,
            rights,
        }
    }

    pub const fn provider(&self) -> OfficialOptionsReferenceProvider {
        self.provider
    }

    pub const fn metadata(&self) -> &SourceMetadata {
        &self.metadata
    }
}

/// Immutable source coordinate retained by one generation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OfficialOptionsReferenceSourceEvidence {
    provider: OfficialOptionsReferenceProvider,
    source_id: SourceId,
    source_revision: SourceIdentifier,
    source_revision_digest: EvidenceDigest,
    rights_id: EvidenceDigest,
    source_payload_set_digest: EvidenceDigest,
}

impl OfficialOptionsReferenceSourceEvidence {
    pub const fn provider(&self) -> OfficialOptionsReferenceProvider {
        self.provider
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

    pub const fn rights_id(&self) -> EvidenceDigest {
        self.rights_id
    }

    pub const fn source_payload_set_digest(&self) -> EvidenceDigest {
        self.source_payload_set_digest
    }
}

/// Compact exact raw-object coordinate retained in generation receipts and result rows.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OfficialOptionsReferenceObjectEvidence {
    object_ordinal: u16,
    provider: OfficialOptionsReferenceProvider,
    source_id: SourceId,
    surface: OfficialOptionsReferenceSurface,
    object_id: SourceIdentifier,
    native_schema: SourceIdentifier,
    raw_claim_digest: EvidenceDigest,
    physical_receipt_digest: EvidenceDigest,
    payload_digest: EvidenceDigest,
    payload_bytes: u64,
    source_timestamp: Option<Timestamp>,
    available_at: Timestamp,
    received_at: Timestamp,
    strict_row_set_digest: EvidenceDigest,
    strict_row_count: u64,
}

impl OfficialOptionsReferenceObjectEvidence {
    pub const fn object_ordinal(&self) -> u16 {
        self.object_ordinal
    }

    pub const fn provider(&self) -> OfficialOptionsReferenceProvider {
        self.provider
    }

    pub const fn source_id(&self) -> &SourceId {
        &self.source_id
    }

    pub const fn surface(&self) -> &OfficialOptionsReferenceSurface {
        &self.surface
    }

    pub const fn object_id(&self) -> &SourceIdentifier {
        &self.object_id
    }

    pub const fn native_schema(&self) -> &SourceIdentifier {
        &self.native_schema
    }

    pub const fn raw_claim_digest(&self) -> EvidenceDigest {
        self.raw_claim_digest
    }

    pub const fn physical_receipt_digest(&self) -> EvidenceDigest {
        self.physical_receipt_digest
    }

    pub const fn payload_digest(&self) -> EvidenceDigest {
        self.payload_digest
    }

    pub const fn payload_bytes(&self) -> u64 {
        self.payload_bytes
    }

    pub const fn source_timestamp(&self) -> Option<Timestamp> {
        self.source_timestamp
    }

    pub const fn available_at(&self) -> Timestamp {
        self.available_at
    }

    pub const fn received_at(&self) -> Timestamp {
        self.received_at
    }

    pub const fn strict_row_set_digest(&self) -> EvidenceDigest {
        self.strict_row_set_digest
    }

    pub const fn strict_row_count(&self) -> u64 {
        self.strict_row_count
    }
}

/// Compact immutable generation manifest returned by publication and reads.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OfficialOptionsReferenceGenerationReceipt {
    dataset: SourceIdentifier,
    generation_digest: EvidenceDigest,
    generation_sequence: u32,
    previous_generation_digest: Option<EvidenceDigest>,
    request_id: SourceIdentifier,
    requested_at: Timestamp,
    request_deadline: Timestamp,
    strict_row_set_digest: EvidenceDigest,
    alias_assertions: u64,
    alias_assertion_closure_digest: EvidenceDigest,
    total_payload_bytes: u64,
    strict_row_count: u64,
    record_count: u64,
    alias_resolution_count: u64,
    conflict_count: u64,
    record_set_digest: EvidenceDigest,
    alias_resolution_set_digest: EvidenceDigest,
    conflict_set_digest: EvidenceDigest,
    published_at: Timestamp,
    sources: Box<[OfficialOptionsReferenceSourceEvidence]>,
    objects: Box<[OfficialOptionsReferenceObjectEvidence]>,
}

/// Exact catalog generation plus freshly reopened store receipts after restart.
///
/// Raw restart claims remain private to this catalog leaf. Callers receive only store-issued
/// receipts after both catalog reconstruction and bounded physical verification succeed.
#[derive(Debug)]
pub struct OfficialOptionsReferenceRestartVerification {
    generation: OfficialOptionsReferenceGenerationReceipt,
    raw_receipts: Box<[ResearchObjectReceipt]>,
}

impl OfficialOptionsReferenceRestartVerification {
    /// Returns the completely recomputed immutable catalog generation.
    pub const fn generation(&self) -> &OfficialOptionsReferenceGenerationReceipt {
        &self.generation
    }

    /// Returns freshly reopened non-forgeable receipts in object-ordinal order.
    pub fn raw_receipts(&self) -> &[ResearchObjectReceipt] {
        &self.raw_receipts
    }
}

impl OfficialOptionsReferenceGenerationReceipt {
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

    pub const fn request_id(&self) -> &SourceIdentifier {
        &self.request_id
    }

    pub const fn requested_at(&self) -> Timestamp {
        self.requested_at
    }

    pub const fn request_deadline(&self) -> Timestamp {
        self.request_deadline
    }

    pub const fn strict_row_set_digest(&self) -> EvidenceDigest {
        self.strict_row_set_digest
    }

    pub const fn alias_assertions(&self) -> u64 {
        self.alias_assertions
    }

    pub const fn alias_assertion_closure_digest(&self) -> EvidenceDigest {
        self.alias_assertion_closure_digest
    }

    pub const fn total_payload_bytes(&self) -> u64 {
        self.total_payload_bytes
    }

    pub const fn strict_row_count(&self) -> u64 {
        self.strict_row_count
    }

    pub const fn record_count(&self) -> u64 {
        self.record_count
    }

    pub const fn alias_resolution_count(&self) -> u64 {
        self.alias_resolution_count
    }

    pub const fn conflict_count(&self) -> u64 {
        self.conflict_count
    }

    pub const fn record_set_digest(&self) -> EvidenceDigest {
        self.record_set_digest
    }

    pub const fn alias_resolution_set_digest(&self) -> EvidenceDigest {
        self.alias_resolution_set_digest
    }

    pub const fn conflict_set_digest(&self) -> EvidenceDigest {
        self.conflict_set_digest
    }

    pub const fn published_at(&self) -> Timestamp {
        self.published_at
    }

    pub fn sources(&self) -> &[OfficialOptionsReferenceSourceEvidence] {
        &self.sources
    }

    pub fn objects(&self) -> &[OfficialOptionsReferenceObjectEvidence] {
        &self.objects
    }
}

/// Outcome of reconciling one content-addressed current generation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OfficialOptionsReferencePublicationDisposition {
    /// A new immutable successor was committed.
    Inserted,
    /// The exact still-current generation was already durable.
    Replay,
}

/// Durable result of one complete streamed publication attempt.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OfficialOptionsReferencePublicationReceipt {
    disposition: OfficialOptionsReferencePublicationDisposition,
    generation: OfficialOptionsReferenceGenerationReceipt,
}

impl OfficialOptionsReferencePublicationReceipt {
    pub const fn disposition(&self) -> OfficialOptionsReferencePublicationDisposition {
        self.disposition
    }

    pub const fn generation(&self) -> &OfficialOptionsReferenceGenerationReceipt {
        &self.generation
    }
}

/// Current, point-in-time, or exact immutable generation selection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OfficialOptionsReferenceGenerationSelection {
    /// Latest generation knowable now; canonical resolution is evaluated now.
    Current,
    /// Latest generation knowable by the independent knowledge cutoff.
    AsOf {
        /// Inclusive catalog-publication knowledge cutoff.
        knowledge_at: Timestamp,
        /// Exact effective instant for canonical definition resolution.
        effective_at: Timestamp,
    },
    /// One exact generation digest, still bounded by the supplied knowledge cutoff.
    Exact {
        /// Exact immutable provider generation.
        generation_digest: EvidenceDigest,
        /// Inclusive catalog-publication knowledge cutoff.
        knowledge_at: Timestamp,
        /// Exact effective instant for canonical definition resolution.
        effective_at: Timestamp,
    },
}

/// One exact provider-native identity query. No fuzzy value can construct this enum.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum OfficialOptionsReferenceIdentityQuery {
    /// Exact OSI contract identity observed in Cboe series rows.
    Osi(OccOptionIdentity),
    /// Exact case-sensitive Cboe compressed symbol across selected venues.
    CboeSymbol(String),
    /// Exact Cboe venue and compressed symbol natural key.
    CboeVenueSymbol { venue: VenueId, symbol: String },
    /// Exact OCC product/root natural key.
    OccProduct {
        options_symbol: ProviderInstrumentId,
        product_type: OfficialOptionsReferenceOccProductType,
    },
}

impl OfficialOptionsReferenceIdentityQuery {
    fn alias_key(&self) -> OfficialOptionsReferenceAliasKey {
        match self {
            Self::Osi(osi) => OfficialOptionsReferenceAliasKey::CboeOsi { osi: osi.clone() },
            Self::CboeSymbol(symbol) => OfficialOptionsReferenceAliasKey::CboeSymbol {
                symbol: symbol.clone(),
            },
            Self::CboeVenueSymbol { venue, symbol } => {
                OfficialOptionsReferenceAliasKey::CboeVenueSymbol {
                    venue: venue.clone(),
                    symbol: symbol.clone(),
                }
            }
            Self::OccProduct {
                options_symbol,
                product_type,
            } => OfficialOptionsReferenceAliasKey::OccProduct {
                options_symbol: options_symbol.clone(),
                product_type: *product_type,
            },
        }
    }

    fn validate(&self) -> Result<(), OfficialOptionsReferenceError> {
        self.alias_key().validate()
    }
}

/// One immutable provider row reconstructed from exact catalog bytes.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OfficialOptionsReferenceRecord {
    object: OfficialOptionsReferenceObjectEvidence,
    provider_row_number: u32,
    record_id: SourceIdentifier,
    value: OfficialOptionsReferenceRecordValue,
    value_digest: EvidenceDigest,
    record_digest: EvidenceDigest,
}

impl OfficialOptionsReferenceRecord {
    pub const fn object(&self) -> &OfficialOptionsReferenceObjectEvidence {
        &self.object
    }

    pub const fn provider_row_number(&self) -> u32 {
        self.provider_row_number
    }

    pub const fn record_id(&self) -> &SourceIdentifier {
        &self.record_id
    }

    pub const fn value(&self) -> &OfficialOptionsReferenceRecordValue {
        &self.value
    }

    pub const fn value_digest(&self) -> EvidenceDigest {
        self.value_digest
    }

    pub const fn record_digest(&self) -> EvidenceDigest {
        self.record_digest
    }
}

/// One immutable provider conflict reconstructed from exact catalog bytes.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OfficialOptionsReferenceConflict {
    kind: OfficialOptionsReferenceConflictKind,
    key: OfficialOptionsReferenceAliasKey,
    first_evidence: SourceIdentifier,
    second_evidence: SourceIdentifier,
    digest: EvidenceDigest,
}

impl OfficialOptionsReferenceConflict {
    pub const fn kind(&self) -> OfficialOptionsReferenceConflictKind {
        self.kind
    }

    pub const fn key(&self) -> &OfficialOptionsReferenceAliasKey {
        &self.key
    }

    pub const fn first_evidence(&self) -> &SourceIdentifier {
        &self.first_evidence
    }

    pub const fn second_evidence(&self) -> &SourceIdentifier {
        &self.second_evidence
    }

    pub const fn digest(&self) -> EvidenceDigest {
        self.digest
    }
}

/// Exact canonical authority by which an already admitted definition matched a Cboe row.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OfficialOptionsReferenceCanonicalMatchKind {
    /// A verified-assigned exact OCC/OSI external identifier matched.
    VerifiedAssignedOsi,
    /// An exact source-qualified Cboe provider identity matched.
    ExactProviderIdentity,
    /// Both independent exact paths selected the same stable identity.
    VerifiedAssignedOsiAndProviderIdentity,
}

/// One already admitted canonical candidate. This type never allocates its `InstrumentId`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OfficialOptionsReferenceCanonicalCandidate {
    instrument_id: InstrumentId,
    revision_digest: EvidenceDigest,
    revision_sequence: u32,
    published_at: Timestamp,
    definition_effective: EffectiveInterval,
    match_kind: OfficialOptionsReferenceCanonicalMatchKind,
    match_validity: EffectiveInterval,
}

impl OfficialOptionsReferenceCanonicalCandidate {
    pub const fn instrument_id(&self) -> InstrumentId {
        self.instrument_id
    }

    pub const fn revision_digest(&self) -> EvidenceDigest {
        self.revision_digest
    }

    pub const fn revision_sequence(&self) -> u32 {
        self.revision_sequence
    }

    pub const fn published_at(&self) -> Timestamp {
        self.published_at
    }

    pub const fn definition_effective(&self) -> EffectiveInterval {
        self.definition_effective
    }

    pub const fn match_kind(&self) -> OfficialOptionsReferenceCanonicalMatchKind {
        self.match_kind
    }

    pub const fn match_validity(&self) -> EffectiveInterval {
        self.match_validity
    }
}

/// Exact, ambiguous, truncated, missing, or intentionally inapplicable canonical resolution.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum OfficialOptionsReferenceCanonicalResolution {
    /// Exactly one already admitted canonical option identity matched.
    Exact(OfficialOptionsReferenceCanonicalCandidate),
    /// More than one already admitted identity carried exact competing authority.
    Ambiguous {
        candidates: Box<[OfficialOptionsReferenceCanonicalCandidate]>,
        has_more: bool,
    },
    /// The bounded pre-verification candidate scan overflowed. The retained exact matches may be
    /// empty and must not be interpreted as exact or missing.
    Truncated {
        candidates: Box<[OfficialOptionsReferenceCanonicalCandidate]>,
    },
    /// No already admitted canonical option identity matched.
    Missing,
    /// OCC DLP describes a product/root rather than an individual option contract.
    ProviderProductOnly,
}

/// Exact provider resolution and independent canonical identity disposition.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OfficialOptionsReferenceExactIdentity {
    records: Box<[OfficialOptionsReferenceRecord]>,
    canonical: OfficialOptionsReferenceCanonicalResolution,
    receipt_digest: EvidenceDigest,
}

impl OfficialOptionsReferenceExactIdentity {
    pub fn records(&self) -> &[OfficialOptionsReferenceRecord] {
        &self.records
    }

    pub const fn canonical(&self) -> &OfficialOptionsReferenceCanonicalResolution {
        &self.canonical
    }

    pub const fn receipt_digest(&self) -> EvidenceDigest {
        self.receipt_digest
    }
}

/// Preserved provider ambiguity. No record or canonical candidate is selected as a winner.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OfficialOptionsReferenceAmbiguity {
    records: Box<[OfficialOptionsReferenceRecord]>,
    conflicts: Box<[OfficialOptionsReferenceConflict]>,
    has_more_records: bool,
    has_more_conflicts: bool,
}

impl OfficialOptionsReferenceAmbiguity {
    pub fn records(&self) -> &[OfficialOptionsReferenceRecord] {
        &self.records
    }

    pub fn conflicts(&self) -> &[OfficialOptionsReferenceConflict] {
        &self.conflicts
    }

    pub const fn has_more_records(&self) -> bool {
        self.has_more_records
    }

    pub const fn has_more_conflicts(&self) -> bool {
        self.has_more_conflicts
    }
}

/// Terminal exact provider query result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum OfficialOptionsReferenceIdentityResolution {
    /// No selected generation or no exact provider row existed.
    Missing {
        generation: Option<OfficialOptionsReferenceGenerationReceipt>,
    },
    /// Provider evidence is conflicting or structurally non-unique.
    Ambiguous {
        generation: OfficialOptionsReferenceGenerationReceipt,
        ambiguity: OfficialOptionsReferenceAmbiguity,
    },
    /// Provider evidence is exact; canonical identity remains independently classified.
    Exact {
        generation: OfficialOptionsReferenceGenerationReceipt,
        identity: OfficialOptionsReferenceExactIdentity,
    },
}

/// Bounded candidate-only text search page. It grants no identity selection authority.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OfficialOptionsReferenceSearchPage {
    generation: Option<OfficialOptionsReferenceGenerationReceipt>,
    records: Box<[OfficialOptionsReferenceRecord]>,
    has_more: bool,
}

/// Provider-neutral outcome of resolving through the current durable reference catalog.
///
/// Dataset identities never cross this boundary. An unavailable or structurally ambiguous
/// reference catalog is a bounded quality limitation for canonical option observations, not a
/// reason to suppress those observations.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum OfficialOptionsReferenceCatalogResolution {
    /// No uniquely eligible immutable reference dataset is currently readable.
    Unavailable,
    /// More than one eligible dataset exists, or the bounded discovery scan could not prove
    /// uniqueness. No dataset is selected as a winner.
    Ambiguous {
        /// Eligible datasets observed inside the bounded scan.
        eligible_dataset_count: u16,
        /// Whether additional catalog candidates existed beyond the discovery bound.
        has_more: bool,
    },
    /// Exactly one eligible dataset was selected and queried at the requested cutoff.
    Selected(OfficialOptionsReferenceIdentityResolution),
}

impl OfficialOptionsReferenceCatalogResolution {
    pub const fn eligible_dataset_count(&self) -> u16 {
        match self {
            Self::Unavailable => 0,
            Self::Ambiguous {
                eligible_dataset_count,
                ..
            } => *eligible_dataset_count,
            Self::Selected(_) => 1,
        }
    }

    pub const fn has_more(&self) -> bool {
        matches!(self, Self::Ambiguous { has_more: true, .. })
    }

    pub const fn selected(&self) -> Option<&OfficialOptionsReferenceIdentityResolution> {
        match self {
            Self::Selected(resolution) => Some(resolution),
            Self::Unavailable | Self::Ambiguous { .. } => None,
        }
    }
}

impl OfficialOptionsReferenceSearchPage {
    pub const fn generation(&self) -> Option<&OfficialOptionsReferenceGenerationReceipt> {
        self.generation.as_ref()
    }

    pub fn records(&self) -> &[OfficialOptionsReferenceRecord] {
        &self.records
    }

    pub const fn has_more(&self) -> bool {
        self.has_more
    }
}

/// Cloneable least-authority publisher bound to one dataset and exact OCC/Cboe grants.
#[derive(Clone)]
pub struct OfficialOptionsReferencePublicationCapability {
    authority: Arc<Mutex<CatalogAuthority>>,
    dataset: SourceIdentifier,
    sources: Box<[OfficialOptionsReferenceSourceAuthority]>,
}

impl fmt::Debug for OfficialOptionsReferencePublicationCapability {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OfficialOptionsReferencePublicationCapability")
            .field("dataset", &self.dataset)
            .field("sources", &self.sources)
            .field(
                "authority",
                &"[SEALED OFFICIAL OPTIONS REFERENCE AUTHORITY]",
            )
            .finish()
    }
}

impl OfficialOptionsReferencePublicationCapability {
    /// Binds publication to exactly one OCC and one Cboe source/grant pair.
    pub fn try_new(
        authority: Arc<Mutex<CatalogAuthority>>,
        dataset: SourceIdentifier,
        mut sources: Vec<OfficialOptionsReferenceSourceAuthority>,
    ) -> Result<Self, OfficialOptionsReferenceError> {
        sources.sort_by_key(OfficialOptionsReferenceSourceAuthority::provider);
        if sources.len() != OfficialOptionsReferenceProvider::ALL.len()
            || sources
                .iter()
                .map(OfficialOptionsReferenceSourceAuthority::provider)
                .ne(OfficialOptionsReferenceProvider::ALL)
            || sources
                .windows(2)
                .any(|pair| pair[0].metadata.source_id() == pair[1].metadata.source_id())
        {
            return Err(OfficialOptionsReferenceError::InvalidSourceAuthority);
        }
        let session_id = authority
            .try_lock()
            .map_err(|_| OfficialOptionsReferenceError::AuthorityUnavailable)?
            .session_id();
        if sources
            .iter()
            .any(|source| source.rights.catalog_id != session_id)
        {
            return Err(OfficialOptionsReferenceError::InvalidRightsCapability);
        }
        Ok(Self {
            authority,
            dataset,
            sources: sources.into_boxed_slice(),
        })
    }

    /// Streams one complete staged generation into an atomic immutable publication.
    ///
    /// Every callback must return terminal `None`. Partial output, count drift, ordering drift,
    /// raw-claim mismatch, stale previous position, or any conflict-accounting mismatch rolls the
    /// transaction back.
    pub fn publish<NR, NA, NC>(
        &self,
        header: OfficialOptionsReferenceGenerationHeader,
        next_record: NR,
        next_resolution: NA,
        next_conflict: NC,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<OfficialOptionsReferencePublicationReceipt, OfficialOptionsReferenceError>
    where
        NR: FnMut() -> Result<
            Option<OfficialOptionsReferenceRecordInput>,
            OfficialOptionsReferenceError,
        >,
        NA: FnMut() -> Result<
            Option<OfficialOptionsReferenceAliasResolutionInput>,
            OfficialOptionsReferenceError,
        >,
        NC: FnMut() -> Result<
            Option<OfficialOptionsReferenceConflictInput>,
            OfficialOptionsReferenceError,
        >,
    {
        check_operation(deadline, cancellation)?;
        self.authority
            .try_lock()
            .map_err(|_| OfficialOptionsReferenceError::AuthorityUnavailable)?
            .publish_official_options_reference_generation(
                &self.dataset,
                &self.sources,
                header,
                next_record,
                next_resolution,
                next_conflict,
                deadline,
                cancellation,
            )
    }
}

/// Cloneable least-authority reader bound to one official options-reference dataset.
#[derive(Clone)]
pub struct OfficialOptionsReferenceReadCapability {
    authority: Arc<Mutex<CatalogAuthority>>,
    dataset: SourceIdentifier,
}

impl fmt::Debug for OfficialOptionsReferenceReadCapability {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OfficialOptionsReferenceReadCapability")
            .field("dataset", &self.dataset)
            .field(
                "authority",
                &"[SEALED OFFICIAL OPTIONS REFERENCE READ AUTHORITY]",
            )
            .finish()
    }
}

impl OfficialOptionsReferenceReadCapability {
    /// Binds bounded reads to one catalog and provider-specific dataset.
    pub fn new(authority: Arc<Mutex<CatalogAuthority>>, dataset: SourceIdentifier) -> Self {
        Self { authority, dataset }
    }

    /// Returns the current immutable generation if both provider sources remain display-admitted.
    pub fn current(
        &self,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<Option<OfficialOptionsReferenceGenerationReceipt>, OfficialOptionsReferenceError>
    {
        check_operation(deadline, cancellation)?;
        let authority = self
            .authority
            .try_lock()
            .map_err(|_| OfficialOptionsReferenceError::AuthorityUnavailable)?;
        let connection = &authority.catalog().connection;
        install_progress_handler(connection, deadline, cancellation)?;
        let result = (|| {
            let now = trusted_read_now(connection)?;
            select_generation(
                connection,
                &self.dataset,
                OfficialOptionsReferenceGenerationSelection::Current,
                now,
            )
            .map(|selected| selected.map(|selected| selected.generation))
        })();
        clear_progress_handler(connection)?;
        classify_operation(result, deadline, cancellation)
    }

    /// Recomputes every stored manifest, raw claim, row, alias, and conflict digest after restart.
    pub fn verify_exact_generation(
        &self,
        generation_digest: EvidenceDigest,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<OfficialOptionsReferenceGenerationReceipt, OfficialOptionsReferenceError> {
        validate_sha256(generation_digest)?;
        check_operation(deadline, cancellation)?;
        let authority = self
            .authority
            .try_lock()
            .map_err(|_| OfficialOptionsReferenceError::AuthorityUnavailable)?;
        verify_exact_generation(
            &authority.catalog().connection,
            &self.dataset,
            generation_digest,
            deadline,
            cancellation,
        )
        .map(|(generation, _header)| generation)
    }

    /// Recomputes the exact catalog generation, then privately reopens every raw object.
    pub fn verify_exact_generation_with_raw_objects(
        &self,
        generation_digest: EvidenceDigest,
        raw_store: &SealedResearchJournalStore,
        control: &dyn ResearchObjectControl,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<OfficialOptionsReferenceRestartVerification, OfficialOptionsReferenceError> {
        validate_sha256(generation_digest)?;
        check_operation(deadline, cancellation)?;
        let (generation, header) = {
            let authority = self
                .authority
                .try_lock()
                .map_err(|_| OfficialOptionsReferenceError::AuthorityUnavailable)?;
            verify_exact_generation(
                &authority.catalog().connection,
                &self.dataset,
                generation_digest,
                deadline,
                cancellation,
            )?
        };
        let mut raw_receipts = Vec::new();
        raw_receipts
            .try_reserve_exact(header.objects.len())
            .map_err(|_| OfficialOptionsReferenceError::CapacityExceeded)?;
        for (object, retained) in header.objects.iter().zip(generation.objects.iter()) {
            check_operation(deadline, cancellation)?;
            let verified =
                raw_store.open_verified_logical_object_claim(object.raw_claim(), control)?;
            let receipt = verified.reverify_for_commit(control)?;
            if receipt.claim() != object.raw_claim()
                || receipt.content_digest() != retained.payload_digest
                || receipt.size_bytes() != retained.payload_bytes
                || receipt.claim().physical_receipt_digest() != retained.physical_receipt_digest
                || raw_claim_digest(receipt.claim())? != retained.raw_claim_digest
            {
                return Err(OfficialOptionsReferenceError::RawClaimConflict);
            }
            raw_receipts.push(receipt);
        }
        check_operation(deadline, cancellation)?;
        Ok(OfficialOptionsReferenceRestartVerification {
            generation,
            raw_receipts: raw_receipts.into_boxed_slice(),
        })
    }

    /// Resolves one exact provider-native key with explicit provider and canonical dispositions.
    pub fn resolve(
        &self,
        selection: OfficialOptionsReferenceGenerationSelection,
        query: OfficialOptionsReferenceIdentityQuery,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<OfficialOptionsReferenceIdentityResolution, OfficialOptionsReferenceError> {
        query.validate()?;
        check_operation(deadline, cancellation)?;
        let authority = self
            .authority
            .try_lock()
            .map_err(|_| OfficialOptionsReferenceError::AuthorityUnavailable)?;
        resolve_identity(
            authority,
            &self.dataset,
            selection,
            query,
            deadline,
            cancellation,
        )
    }

    /// Searches provider symbols, unresolved underlying aliases, and admitted source names.
    /// Returned rows are candidates only and carry no canonical selection authority.
    pub fn search_text(
        &self,
        selection: OfficialOptionsReferenceGenerationSelection,
        query: &str,
        maximum_rows: usize,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<OfficialOptionsReferenceSearchPage, OfficialOptionsReferenceError> {
        if maximum_rows == 0 || maximum_rows > MAX_OFFICIAL_OPTIONS_REFERENCE_SEARCH_ROWS {
            return Err(OfficialOptionsReferenceError::InvalidLimit);
        }
        let query = query.trim();
        if query.is_empty()
            || query.len() > MAX_SEARCH_QUERY_BYTES
            || query.chars().any(char::is_control)
        {
            return Err(OfficialOptionsReferenceError::InvalidInput);
        }
        check_operation(deadline, cancellation)?;
        let authority = self
            .authority
            .try_lock()
            .map_err(|_| OfficialOptionsReferenceError::AuthorityUnavailable)?;
        search_text(
            authority,
            &self.dataset,
            selection,
            query,
            maximum_rows,
            deadline,
            cancellation,
        )
    }
}

/// Cloneable least-authority reader that dynamically selects the uniquely eligible current
/// official reference dataset without accepting a provider or dataset route from its caller.
#[derive(Clone)]
pub struct OfficialOptionsReferenceCatalogReadCapability {
    authority: Arc<Mutex<CatalogAuthority>>,
}

impl fmt::Debug for OfficialOptionsReferenceCatalogReadCapability {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OfficialOptionsReferenceCatalogReadCapability")
            .field(
                "authority",
                &"[SEALED OFFICIAL OPTIONS REFERENCE CATALOG READ AUTHORITY]",
            )
            .finish()
    }
}

impl OfficialOptionsReferenceCatalogReadCapability {
    pub(crate) fn new(authority: Arc<Mutex<CatalogAuthority>>) -> Self {
        Self { authority }
    }

    /// Selects a unique readable immutable dataset and resolves one exact identity atomically.
    ///
    /// `Current` is frozen to one trusted catalog timestamp before dataset discovery, so a
    /// publication committed concurrently after that cutoff cannot change the second-stage read.
    /// Missing, expired, and ambiguous catalog routes are returned as closed product-neutral
    /// dispositions rather than provider errors or arbitrary winners.
    pub fn resolve(
        &self,
        selection: OfficialOptionsReferenceGenerationSelection,
        query: OfficialOptionsReferenceIdentityQuery,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<OfficialOptionsReferenceCatalogResolution, OfficialOptionsReferenceError> {
        query.validate()?;
        check_operation(deadline, cancellation)?;
        let authority = self
            .authority
            .try_lock()
            .map_err(|_| OfficialOptionsReferenceError::AuthorityUnavailable)?;
        let connection = &authority.catalog().connection;
        install_progress_handler(connection, deadline, cancellation)?;
        let discovery = (|| {
            let now = trusted_read_now(connection)?;
            let selection = freeze_generation_selection(selection, now)?;
            select_current_reference_dataset(connection, selection, now)
                .map(|dataset| (dataset, selection))
        })();
        clear_progress_handler(connection)?;
        let (discovery, selection) = classify_operation(discovery, deadline, cancellation)?;
        let dataset = match discovery {
            CurrentReferenceDatasetSelection::Unavailable => {
                return Ok(OfficialOptionsReferenceCatalogResolution::Unavailable);
            }
            CurrentReferenceDatasetSelection::Ambiguous {
                eligible_dataset_count,
                has_more,
            } => {
                return Ok(OfficialOptionsReferenceCatalogResolution::Ambiguous {
                    eligible_dataset_count,
                    has_more,
                });
            }
            CurrentReferenceDatasetSelection::Unique(dataset) => dataset,
        };
        match resolve_identity(
            authority,
            &dataset,
            selection,
            query,
            deadline,
            cancellation,
        ) {
            Ok(resolution) => Ok(OfficialOptionsReferenceCatalogResolution::Selected(
                resolution,
            )),
            Err(
                OfficialOptionsReferenceError::SourceUnavailable
                | OfficialOptionsReferenceError::RightsUnavailable,
            ) => Ok(OfficialOptionsReferenceCatalogResolution::Unavailable),
            Err(error) => Err(error),
        }
    }
}

impl CatalogAuthority {
    #[allow(
        clippy::too_many_arguments,
        reason = "atomic streaming publication coordinates remain explicit"
    )]
    fn publish_official_options_reference_generation<NR, NA, NC>(
        &self,
        dataset: &SourceIdentifier,
        sources: &[OfficialOptionsReferenceSourceAuthority],
        header: OfficialOptionsReferenceGenerationHeader,
        mut next_record: NR,
        mut next_resolution: NA,
        mut next_conflict: NC,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<OfficialOptionsReferencePublicationReceipt, OfficialOptionsReferenceError>
    where
        NR: FnMut() -> Result<
            Option<OfficialOptionsReferenceRecordInput>,
            OfficialOptionsReferenceError,
        >,
        NA: FnMut() -> Result<
            Option<OfficialOptionsReferenceAliasResolutionInput>,
            OfficialOptionsReferenceError,
        >,
        NC: FnMut() -> Result<
            Option<OfficialOptionsReferenceConflictInput>,
            OfficialOptionsReferenceError,
        >,
    {
        check_operation(deadline, cancellation)?;
        if header
            .objects
            .iter()
            .any(|object| !object.has_live_raw_authority())
        {
            return Err(OfficialOptionsReferenceError::InvalidRawObjectAuthority);
        }
        validate_source_authorities(self.session_id(), sources, &header)?;
        let source_manifest = prepare_source_manifest(sources, &header)?;
        let generation_digest = generation_digest(dataset, &source_manifest, &header)?;
        let connection = &self.catalog().connection;
        install_progress_handler(connection, deadline, cancellation)?;
        let result = (|| {
            let transaction = connection.unchecked_transaction()?;
            let published_at = trusted_catalog_now(&transaction)
                .map_err(|_| OfficialOptionsReferenceError::CorruptCatalog)?;
            if header.requested_at > published_at
                || header
                    .objects
                    .iter()
                    .any(|object| object.received_at > published_at)
            {
                return Err(OfficialOptionsReferenceError::InvalidInput);
            }
            validate_durable_sources(&transaction, &source_manifest, published_at)?;
            for object in &header.objects {
                require_registered_raw_claim(&transaction, object.raw_claim(), published_at)?;
            }
            let current = current_generation_position(&transaction, dataset)?;
            let existing = load_generation_receipt(&transaction, dataset, generation_digest)?;
            let replay = existing.is_some();
            if replay
                && current.as_ref().map(|position| position.0) != Some(generation_digest.bytes())
            {
                return Err(OfficialOptionsReferenceError::SupersededGeneration);
            }
            if !replay
                && current.as_ref().map(|position| position.0)
                    != header
                        .expected_previous_generation
                        .map(EvidenceDigest::bytes)
            {
                return Err(OfficialOptionsReferenceError::PositionConflict);
            }
            let sequence = current.as_ref().map_or(Ok(1_u32), |position| {
                if replay {
                    Ok(position.1)
                } else {
                    position
                        .1
                        .checked_add(1)
                        .filter(|value| *value <= MAX_GENERATIONS)
                        .ok_or(OfficialOptionsReferenceError::PositionConflict)
                }
            })?;
            if !replay {
                insert_generation_header(
                    &transaction,
                    dataset,
                    generation_digest,
                    sequence,
                    &source_manifest,
                    &header,
                    published_at,
                )?;
            }
            stream_records(
                &transaction,
                generation_digest,
                &header,
                replay,
                &mut next_record,
                deadline,
                cancellation,
            )?;
            stream_resolutions(
                &transaction,
                generation_digest,
                &header,
                replay,
                &mut next_resolution,
                deadline,
                cancellation,
            )?;
            stream_conflicts(
                &transaction,
                generation_digest,
                &header,
                replay,
                &mut next_conflict,
                deadline,
                cancellation,
            )?;
            validate_alias_closure(&transaction, generation_digest, &header)?;
            let stored = load_generation_receipt(&transaction, dataset, generation_digest)?
                .ok_or(OfficialOptionsReferenceError::CorruptCatalog)?;
            if !generation_receipt_matches_header(
                &stored,
                dataset,
                generation_digest,
                sequence,
                &source_manifest,
                &header,
            ) {
                return Err(OfficialOptionsReferenceError::CorruptCatalog);
            }
            if !replay {
                append_audit(
                    &transaction,
                    "official-options-reference.generation-published",
                    dataset.as_str(),
                    generation_digest.bytes(),
                    published_at,
                )
                .map_err(|_| OfficialOptionsReferenceError::CorruptCatalog)?;
            }
            transaction.commit()?;
            Ok(OfficialOptionsReferencePublicationReceipt {
                disposition: if replay {
                    OfficialOptionsReferencePublicationDisposition::Replay
                } else {
                    OfficialOptionsReferencePublicationDisposition::Inserted
                },
                generation: stored,
            })
        })();
        clear_progress_handler(connection)?;
        classify_operation(result, deadline, cancellation)
    }
}

#[derive(Clone, Debug)]
struct PreparedSourceAuthority {
    provider: OfficialOptionsReferenceProvider,
    metadata: SourceMetadata,
    metadata_json: String,
    source_revision_digest: EvidenceDigest,
    rights_id: EvidenceDigest,
    source_payload_set_digest: EvidenceDigest,
}

fn validate_source_authorities(
    session_id: uuid::Uuid,
    sources: &[OfficialOptionsReferenceSourceAuthority],
    header: &OfficialOptionsReferenceGenerationHeader,
) -> Result<(), OfficialOptionsReferenceError> {
    if sources.len() != OfficialOptionsReferenceProvider::ALL.len() {
        return Err(OfficialOptionsReferenceError::InvalidSourceAuthority);
    }
    for (source, expected_provider) in sources.iter().zip(OfficialOptionsReferenceProvider::ALL) {
        let source_objects: Vec<_> = header
            .objects
            .iter()
            .filter(|object| object.surface.provider() == source.provider)
            .collect();
        if source.provider != expected_provider
            || source.rights.catalog_id != session_id
            || source.metadata.source_id()
                != source_objects
                    .first()
                    .map(|object| &object.source_id)
                    .ok_or(OfficialOptionsReferenceError::InvalidSourceAuthority)?
            || source_objects
                .iter()
                .any(|object| &object.source_id != source.metadata.source_id())
            || !source
                .metadata
                .coverage()
                .asset_classes()
                .contains(&AssetClass::Option)
        {
            return Err(OfficialOptionsReferenceError::InvalidSourceAuthority);
        }
        let expected_payload = header.source_payload_set_digest(source.provider)?;
        if source.rights.payload_digest != expected_payload {
            return Err(OfficialOptionsReferenceError::InvalidRightsCapability);
        }
    }
    Ok(())
}

fn prepare_source_manifest(
    sources: &[OfficialOptionsReferenceSourceAuthority],
    header: &OfficialOptionsReferenceGenerationHeader,
) -> Result<Box<[PreparedSourceAuthority]>, OfficialOptionsReferenceError> {
    let mut prepared = Vec::new();
    prepared
        .try_reserve_exact(sources.len())
        .map_err(|_| OfficialOptionsReferenceError::CapacityExceeded)?;
    for source in sources {
        let metadata_json = serde_json::to_string(&source.metadata)?;
        if metadata_json.is_empty() || metadata_json.len() > 1024 * 1024 {
            return Err(OfficialOptionsReferenceError::InvalidSourceAuthority);
        }
        let source_revision_digest = digest(sha256(metadata_json.as_bytes()));
        prepared.push(PreparedSourceAuthority {
            provider: source.provider,
            metadata: source.metadata.clone(),
            metadata_json,
            source_revision_digest,
            rights_id: digest(source.rights.rights_id),
            source_payload_set_digest: header.source_payload_set_digest(source.provider)?,
        });
    }
    Ok(prepared.into_boxed_slice())
}

fn validate_durable_sources(
    transaction: &Transaction<'_>,
    sources: &[PreparedSourceAuthority],
    published_at: Timestamp,
) -> Result<(), OfficialOptionsReferenceError> {
    for source in sources {
        if !source.metadata.is_effective_at(published_at) {
            return Err(OfficialOptionsReferenceError::SourceUnavailable);
        }
        let retained: Option<String> = transaction
            .query_row(
                "SELECT metadata_json FROM source_revisions
                 WHERE source_id=?1 AND revision_digest=?2",
                params![
                    source.metadata.source_id().as_str(),
                    source.source_revision_digest.bytes().as_slice(),
                ],
                |row| row.get(0),
            )
            .optional()?;
        if retained.as_deref() != Some(source.metadata_json.as_str()) {
            return Err(OfficialOptionsReferenceError::SourceUnavailable);
        }
        let rights_valid: bool = transaction.query_row(
            "SELECT EXISTS(
                 SELECT 1 FROM source_rights
                 WHERE rights_id=?1 AND source_id=?2
                   AND payload_algorithm=1 AND payload_digest=?3
                   AND (operation_mask & 6)=6
                   AND admitted_at_ns<=?4
                   AND (
                       authorization_expires_at_ns IS NULL
                       OR authorization_expires_at_ns>?4
                   )
             )",
            params![
                source.rights_id.bytes().as_slice(),
                source.metadata.source_id().as_str(),
                source.source_payload_set_digest.bytes().as_slice(),
                published_at.unix_nanos(),
            ],
            |row| row.get(0),
        )?;
        if !rights_valid {
            return Err(OfficialOptionsReferenceError::RightsUnavailable);
        }
    }
    Ok(())
}

fn current_generation_position(
    connection: &Connection,
    dataset: &SourceIdentifier,
) -> Result<Option<([u8; 32], u32)>, OfficialOptionsReferenceError> {
    let row: Option<(Vec<u8>, i64)> = connection
        .query_row(
            "SELECT generation_digest, generation_sequence
             FROM official_options_reference_generations
             WHERE dataset_id=?1 ORDER BY generation_sequence DESC LIMIT 1",
            [dataset.as_str()],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    row.map(|(digest, sequence)| {
        Ok((
            digest
                .try_into()
                .map_err(|_| OfficialOptionsReferenceError::CorruptCatalog)?,
            u32::try_from(sequence)
                .ok()
                .filter(|value| (1..=MAX_GENERATIONS).contains(value))
                .ok_or(OfficialOptionsReferenceError::CorruptCatalog)?,
        ))
    })
    .transpose()
}

#[allow(
    clippy::too_many_arguments,
    reason = "immutable generation coordinates remain explicit"
)]
fn insert_generation_header(
    transaction: &Transaction<'_>,
    dataset: &SourceIdentifier,
    generation_digest: EvidenceDigest,
    generation_sequence: u32,
    sources: &[PreparedSourceAuthority],
    header: &OfficialOptionsReferenceGenerationHeader,
    published_at: Timestamp,
) -> Result<(), OfficialOptionsReferenceError> {
    transaction.execute(
        "INSERT INTO official_options_reference_generations
         (generation_digest, dataset_id, generation_sequence, previous_generation_digest,
          request_id, requested_at_ns, request_deadline_ns, strict_row_set_digest,
          alias_assertion_count, alias_assertion_closure_digest, total_payload_bytes,
          strict_row_count, record_count, object_count, alias_resolution_count, conflict_count,
          record_set_digest, alias_resolution_set_digest, conflict_set_digest, published_at_ns)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14,
                 ?15, ?16, ?17, ?18, ?19, ?20)",
        params![
            generation_digest.bytes().as_slice(),
            dataset.as_str(),
            i64::from(generation_sequence),
            header
                .expected_previous_generation
                .map(EvidenceDigest::bytes),
            header.request_id.as_str(),
            header.requested_at.unix_nanos(),
            header.request_deadline.unix_nanos(),
            header.strict_row_set_digest.bytes().as_slice(),
            to_i64(header.alias_assertions)?,
            header.alias_assertion_closure_digest.bytes().as_slice(),
            to_i64(header.total_payload_bytes)?,
            to_i64(header.strict_row_count)?,
            to_i64(header.records.count)?,
            to_i64(header.objects.len())?,
            to_i64(header.resolutions.count)?,
            to_i64(header.conflicts.count)?,
            header.records.digest.bytes().as_slice(),
            header.resolutions.digest.bytes().as_slice(),
            header.conflicts.digest.bytes().as_slice(),
            published_at.unix_nanos(),
        ],
    )?;
    for source in sources {
        transaction.execute(
            "INSERT INTO official_options_reference_generation_sources
             (generation_digest, provider, source_id, source_revision, source_revision_digest,
              rights_id, source_payload_set_digest)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![
                generation_digest.bytes().as_slice(),
                source.provider.database_name(),
                source.metadata.source_id().as_str(),
                source.metadata.revision().as_source_identifier().as_str(),
                source.source_revision_digest.bytes().as_slice(),
                source.rights_id.bytes().as_slice(),
                source.source_payload_set_digest.bytes().as_slice(),
            ],
        )?;
    }
    for object in &header.objects {
        let surface_json = serde_json::to_string(&object.surface)?;
        transaction.execute(
            "INSERT INTO official_options_reference_objects
             (generation_digest, object_ordinal, provider, source_id, surface_json, surface_key,
              object_id, native_schema, raw_claim_digest, physical_receipt_digest,
              payload_digest, payload_bytes, source_timestamp_ns, available_at_ns, received_at_ns,
              strict_row_set_digest, strict_row_count)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14,
                     ?15, ?16, ?17)",
            params![
                generation_digest.bytes().as_slice(),
                i64::from(object.object_ordinal),
                object.surface.provider().database_name(),
                object.source_id.as_str(),
                surface_json,
                object.surface.stable_key()?,
                object.object_id.as_str(),
                object.native_schema.as_str(),
                raw_claim_digest(&object.raw_claim)?.bytes().as_slice(),
                object
                    .raw_claim
                    .physical_receipt_digest()
                    .bytes()
                    .as_slice(),
                object.payload_digest.bytes().as_slice(),
                to_i64(object.raw_claim.size_bytes())?,
                object.source_timestamp.map(Timestamp::unix_nanos),
                object.available_at.unix_nanos(),
                object.received_at.unix_nanos(),
                object.strict_row_set_digest.bytes().as_slice(),
                to_i64(object.strict_row_count)?,
            ],
        )?;
    }
    Ok(())
}

fn require_registered_raw_claim(
    transaction: &Transaction<'_>,
    claim: &ResearchObjectClaim,
    published_at: Timestamp,
) -> Result<(), OfficialOptionsReferenceError> {
    let claim_json = raw_claim_json(claim)?;
    let claim_digest = raw_claim_digest(claim)?;
    if claim_json.len() > MAX_RAW_CLAIM_JSON_BYTES {
        return Err(OfficialOptionsReferenceError::CapacityExceeded);
    }
    let recorded_at: Option<i64> = transaction
        .query_row(
            "SELECT recorded_at_ns FROM sealed_raw_objects
         WHERE raw_claim_digest=?1 AND raw_claim_kind='logical_object'
           AND physical_receipt_digest=?2 AND relative_reference=?3
           AND content_digest=?4 AND size_bytes=?5 AND integrity_chunk_bytes=?6
           AND unit_count=?7 AND raw_claim_json=?8",
            params![
                claim_digest.bytes().as_slice(),
                claim.physical_receipt_digest().bytes().as_slice(),
                claim.relative_reference(),
                claim.content_digest().bytes().as_slice(),
                to_i64(claim.size_bytes())?,
                to_i64(claim.integrity_chunk_bytes())?,
                to_i64(claim.chunks().len())?,
                claim_json,
            ],
            |row| row.get(0),
        )
        .optional()?;
    if recorded_at.is_some_and(|recorded_at| recorded_at <= published_at.unix_nanos()) {
        Ok(())
    } else {
        Err(OfficialOptionsReferenceError::RawClaimConflict)
    }
}

fn stream_records<NR>(
    transaction: &Transaction<'_>,
    generation_digest: EvidenceDigest,
    header: &OfficialOptionsReferenceGenerationHeader,
    replay: bool,
    next_record: &mut NR,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<(), OfficialOptionsReferenceError>
where
    NR: FnMut()
        -> Result<Option<OfficialOptionsReferenceRecordInput>, OfficialOptionsReferenceError>,
{
    let mut builder = OfficialOptionsReferenceRecordSetDigestBuilder::new();
    let mut assertion_builder =
        OfficialOptionsReferenceAliasAssertionSetBuilder::new(header.request_id.clone());
    while let Some(record) = next_record()? {
        check_operation(deadline, cancellation)?;
        validate_record_object(transaction, generation_digest, &record)?;
        assertion_builder.try_observe(&record)?;
        builder.try_observe(&record)?;
        if !replay {
            insert_record(transaction, generation_digest, &record)?;
        }
    }
    let observed = builder.finish();
    if observed != header.records || assertion_builder.finish() != header.alias_assertion_set {
        return Err(OfficialOptionsReferenceError::IncompleteStream);
    }
    if replay {
        verify_stored_record_set(transaction, generation_digest, observed)?;
    }
    Ok(())
}

fn validate_record_object(
    transaction: &Transaction<'_>,
    generation_digest: EvidenceDigest,
    record: &OfficialOptionsReferenceRecordInput,
) -> Result<(), OfficialOptionsReferenceError> {
    record.value.validate()?;
    let retained: Option<(String, i64, String)> = transaction
        .query_row(
            "SELECT provider, strict_row_count, surface_json
             FROM official_options_reference_objects
             WHERE generation_digest=?1 AND object_ordinal=?2",
            params![
                generation_digest.bytes().as_slice(),
                i64::from(record.object_ordinal),
            ],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional()?;
    let Some((provider, strict_rows, surface_json)) = retained else {
        return Err(OfficialOptionsReferenceError::InvalidInput);
    };
    let surface: OfficialOptionsReferenceSurface = serde_json::from_str(&surface_json)
        .map_err(|_| OfficialOptionsReferenceError::CorruptCatalog)?;
    let strict_rows =
        u64::try_from(strict_rows).map_err(|_| OfficialOptionsReferenceError::CorruptCatalog)?;
    if provider != record.value.provider().database_name()
        || !record_value_matches_surface(&record.value, &surface)
        || u64::from(record.provider_row_number) > strict_rows.saturating_add(1)
    {
        return Err(OfficialOptionsReferenceError::InvalidInput);
    }
    Ok(())
}

fn record_value_matches_surface(
    value: &OfficialOptionsReferenceRecordValue,
    surface: &OfficialOptionsReferenceSurface,
) -> bool {
    match (value, surface) {
        (
            OfficialOptionsReferenceRecordValue::CboeSeries(series),
            OfficialOptionsReferenceSurface::CboeAllSeries { venue },
        ) => series.venue() == venue,
        (
            OfficialOptionsReferenceRecordValue::OccProduct(_),
            OfficialOptionsReferenceSurface::OccDlpSelectedText
            | OfficialOptionsReferenceSurface::OccDlpDailyText
            | OfficialOptionsReferenceSurface::OccDlpDailyXml,
        ) => true,
        _ => false,
    }
}

fn insert_record(
    transaction: &Transaction<'_>,
    generation_digest: EvidenceDigest,
    record: &OfficialOptionsReferenceRecordInput,
) -> Result<(), OfficialOptionsReferenceError> {
    let value_json = serde_json::to_string(&record.value)?;
    if value_json.len() > MAX_RECORD_JSON_BYTES {
        return Err(OfficialOptionsReferenceError::CapacityExceeded);
    }
    let value_digest = record_value_digest(&record.value)?;
    let columns = record_columns(&record.value)?;
    let normalized_provider_symbol = columns.provider_symbol.to_lowercase();
    transaction.execute(
        "INSERT OR IGNORE INTO official_options_reference_values
         (value_digest, provider, record_kind, provider_symbol, normalized_provider_symbol,
          secondary_symbol, normalized_search_text, venue, osi, occ_product_type, value_json)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
        params![
            value_digest.bytes().as_slice(),
            record.value.provider().database_name(),
            columns.kind,
            columns.provider_symbol,
            normalized_provider_symbol,
            columns.secondary_symbol,
            &columns.search_text,
            columns.venue,
            columns.osi,
            columns.occ_product_type,
            &value_json,
        ],
    )?;
    let exact_value: bool = transaction.query_row(
        "SELECT EXISTS(
             SELECT 1 FROM official_options_reference_values
             WHERE value_digest=?1 AND provider=?2 AND record_kind=?3
               AND provider_symbol=?4 AND normalized_provider_symbol=?5
               AND secondary_symbol=?6 AND normalized_search_text=?7
               AND venue IS ?8 AND osi IS ?9 AND occ_product_type IS ?10
               AND value_json=?11
         )",
        params![
            value_digest.bytes().as_slice(),
            record.value.provider().database_name(),
            columns.kind,
            columns.provider_symbol,
            normalized_provider_symbol,
            columns.secondary_symbol,
            &columns.search_text,
            columns.venue,
            columns.osi,
            columns.occ_product_type,
            value_json,
        ],
        |row| row.get(0),
    )?;
    if !exact_value {
        return Err(OfficialOptionsReferenceError::ValueConflict);
    }
    let record_digest = record_membership_digest(generation_digest, record, value_digest)?;
    transaction.execute(
        "INSERT INTO official_options_reference_memberships
         (generation_digest, object_ordinal, provider_row_number, record_id, value_digest,
          record_digest)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        params![
            generation_digest.bytes().as_slice(),
            i64::from(record.object_ordinal),
            i64::from(record.provider_row_number),
            record.record_id.as_str(),
            value_digest.bytes().as_slice(),
            record_digest.bytes().as_slice(),
        ],
    )?;
    Ok(())
}

fn stream_resolutions<NA>(
    transaction: &Transaction<'_>,
    generation_digest: EvidenceDigest,
    header: &OfficialOptionsReferenceGenerationHeader,
    replay: bool,
    next_resolution: &mut NA,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<(), OfficialOptionsReferenceError>
where
    NA: FnMut() -> Result<
        Option<OfficialOptionsReferenceAliasResolutionInput>,
        OfficialOptionsReferenceError,
    >,
{
    let mut builder = OfficialOptionsReferenceResolutionSetDigestBuilder::new();
    let mut observations = 0_u64;
    let mut conflicts = 0_u64;
    while let Some(resolution) = next_resolution()? {
        check_operation(deadline, cancellation)?;
        observations = observations
            .checked_add(resolution.observations)
            .ok_or(OfficialOptionsReferenceError::CapacityExceeded)?;
        conflicts = conflicts
            .checked_add(u64::from(resolution.conflicts))
            .ok_or(OfficialOptionsReferenceError::CapacityExceeded)?;
        builder.try_observe(&resolution)?;
        if !replay {
            insert_resolution(transaction, generation_digest, &resolution)?;
        }
    }
    let observed = builder.finish();
    if observed != header.resolutions
        || observations != header.alias_assertions
        || conflicts != header.conflicts.count
    {
        return Err(OfficialOptionsReferenceError::IncompleteStream);
    }
    if replay {
        verify_stored_resolution_set(transaction, generation_digest, observed)?;
    }
    Ok(())
}

fn insert_resolution(
    transaction: &Transaction<'_>,
    generation_digest: EvidenceDigest,
    resolution: &OfficialOptionsReferenceAliasResolutionInput,
) -> Result<(), OfficialOptionsReferenceError> {
    let key_json = alias_key_json(&resolution.key)?;
    let key_digest = alias_key_digest(&resolution.key)?;
    let resolution_digest = canonical_json_digest(b"resolution", resolution)?;
    let columns = alias_key_columns(&resolution.key);
    transaction.execute(
        "INSERT INTO official_options_reference_alias_resolutions
         (generation_digest, key_digest, key_kind, provider_symbol, venue, osi,
          occ_product_type, key_json, state, observation_count, conflict_count,
          resolution_digest)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
        params![
            generation_digest.bytes().as_slice(),
            key_digest.bytes().as_slice(),
            resolution.key.database_kind(),
            columns.provider_symbol,
            columns.venue,
            columns.osi,
            columns.occ_product_type,
            key_json,
            resolution.state.database_name(),
            to_i64(resolution.observations)?,
            i64::from(resolution.conflicts),
            resolution_digest.bytes().as_slice(),
        ],
    )?;
    Ok(())
}

fn stream_conflicts<NC>(
    transaction: &Transaction<'_>,
    generation_digest: EvidenceDigest,
    header: &OfficialOptionsReferenceGenerationHeader,
    replay: bool,
    next_conflict: &mut NC,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<(), OfficialOptionsReferenceError>
where
    NC: FnMut() -> Result<
        Option<OfficialOptionsReferenceConflictInput>,
        OfficialOptionsReferenceError,
    >,
{
    let mut builder = OfficialOptionsReferenceConflictSetDigestBuilder::new();
    let mut ordinal = 0_u64;
    while let Some(conflict) = next_conflict()? {
        check_operation(deadline, cancellation)?;
        builder.try_observe(&conflict)?;
        if !replay {
            insert_conflict(transaction, generation_digest, ordinal, &conflict)?;
        }
        ordinal = ordinal
            .checked_add(1)
            .ok_or(OfficialOptionsReferenceError::CapacityExceeded)?;
    }
    let observed = builder.finish();
    if observed != header.conflicts {
        return Err(OfficialOptionsReferenceError::IncompleteStream);
    }
    if replay {
        verify_stored_conflict_set(transaction, generation_digest, observed)?;
    }
    Ok(())
}

fn insert_conflict(
    transaction: &Transaction<'_>,
    generation_digest: EvidenceDigest,
    ordinal: u64,
    conflict: &OfficialOptionsReferenceConflictInput,
) -> Result<(), OfficialOptionsReferenceError> {
    let evidence_exists: bool = transaction.query_row(
        "SELECT EXISTS(
             SELECT 1 FROM official_options_reference_memberships
             WHERE generation_digest=?1 AND record_id=?2
         ) AND EXISTS(
             SELECT 1 FROM official_options_reference_memberships
             WHERE generation_digest=?1 AND record_id=?3
         )",
        params![
            generation_digest.bytes().as_slice(),
            conflict.first_evidence.as_str(),
            conflict.second_evidence.as_str(),
        ],
        |row| row.get(0),
    )?;
    if !evidence_exists {
        return Err(OfficialOptionsReferenceError::InvalidInput);
    }
    if !record_supports_alias_key(
        transaction,
        generation_digest,
        &conflict.first_evidence,
        &conflict.key,
    )? || !record_supports_alias_key(
        transaction,
        generation_digest,
        &conflict.second_evidence,
        &conflict.key,
    )? || !conflict_semantics_match(transaction, generation_digest, conflict)?
    {
        return Err(OfficialOptionsReferenceError::InvalidInput);
    }
    let key_digest = alias_key_digest(&conflict.key)?;
    let conflict_digest = canonical_json_digest(b"conflict", conflict)?;
    transaction.execute(
        "INSERT INTO official_options_reference_conflicts
         (generation_digest, conflict_ordinal, key_digest, conflict_kind, first_evidence,
          second_evidence, conflict_digest)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        params![
            generation_digest.bytes().as_slice(),
            to_i64(ordinal)?,
            key_digest.bytes().as_slice(),
            conflict.kind.database_name(),
            conflict.first_evidence.as_str(),
            conflict.second_evidence.as_str(),
            conflict_digest.bytes().as_slice(),
        ],
    )?;
    Ok(())
}

fn record_supports_alias_key(
    connection: &Connection,
    generation_digest: EvidenceDigest,
    record_id: &SourceIdentifier,
    key: &OfficialOptionsReferenceAliasKey,
) -> Result<bool, OfficialOptionsReferenceError> {
    Ok(
        load_record_value_by_id(connection, generation_digest, record_id)?
            .is_some_and(|value| record_value_supports_alias_key(&value, key)),
    )
}

fn load_record_value_by_id(
    connection: &Connection,
    generation_digest: EvidenceDigest,
    record_id: &SourceIdentifier,
) -> Result<Option<OfficialOptionsReferenceRecordValue>, OfficialOptionsReferenceError> {
    let value_json: Option<String> = connection
        .query_row(
            "SELECT values_.value_json
             FROM official_options_reference_memberships AS membership
             JOIN official_options_reference_values AS values_
               ON values_.value_digest=membership.value_digest
             WHERE membership.generation_digest=?1 AND membership.record_id=?2",
            params![generation_digest.bytes().as_slice(), record_id.as_str(),],
            |row| row.get(0),
        )
        .optional()?;
    let Some(value_json) = value_json else {
        return Ok(None);
    };
    let value: OfficialOptionsReferenceRecordValue = serde_json::from_str(&value_json)
        .map_err(|_| OfficialOptionsReferenceError::CorruptCatalog)?;
    value
        .validate()
        .map_err(|_| OfficialOptionsReferenceError::CorruptCatalog)?;
    Ok(Some(value))
}

fn conflict_semantics_match(
    connection: &Connection,
    generation_digest: EvidenceDigest,
    conflict: &OfficialOptionsReferenceConflictInput,
) -> Result<bool, OfficialOptionsReferenceError> {
    let Some(first) =
        load_record_value_by_id(connection, generation_digest, &conflict.first_evidence)?
    else {
        return Ok(false);
    };
    let Some(second) =
        load_record_value_by_id(connection, generation_digest, &conflict.second_evidence)?
    else {
        return Ok(false);
    };
    Ok(match (conflict.kind, first, second) {
        (
            OfficialOptionsReferenceConflictKind::CboeSymbolMapsMultipleOsi,
            OfficialOptionsReferenceRecordValue::CboeSeries(first),
            OfficialOptionsReferenceRecordValue::CboeSeries(second),
        ) => first.osi() != second.osi(),
        (
            OfficialOptionsReferenceConflictKind::CboeSymbolMapsMultipleUnderlying,
            OfficialOptionsReferenceRecordValue::CboeSeries(first),
            OfficialOptionsReferenceRecordValue::CboeSeries(second),
        ) => first.underlying_alias() != second.underlying_alias(),
        (
            OfficialOptionsReferenceConflictKind::CboeOsiMapsMultipleSymbols,
            OfficialOptionsReferenceRecordValue::CboeSeries(first),
            OfficialOptionsReferenceRecordValue::CboeSeries(second),
        ) => first.cboe_symbol() != second.cboe_symbol(),
        (OfficialOptionsReferenceConflictKind::DuplicateProviderRecord, _, _) => true,
        _ => false,
    })
}

#[derive(Debug)]
struct RecordColumns<'a> {
    kind: &'static str,
    provider_symbol: &'a str,
    secondary_symbol: &'a str,
    search_text: String,
    venue: Option<&'a str>,
    osi: Option<String>,
    occ_product_type: Option<&'static str>,
}

fn record_columns(
    value: &OfficialOptionsReferenceRecordValue,
) -> Result<RecordColumns<'_>, OfficialOptionsReferenceError> {
    let columns = match value {
        OfficialOptionsReferenceRecordValue::CboeSeries(series) => RecordColumns {
            kind: "cboe_series",
            provider_symbol: series.cboe_symbol(),
            secondary_symbol: series.underlying_alias().as_str(),
            search_text: format!(
                "{} {} {} {}",
                series.cboe_symbol().to_lowercase(),
                series.osi(),
                series.underlying_alias().as_str().to_lowercase(),
                series.venue().as_str().to_lowercase()
            ),
            venue: Some(series.venue().as_str()),
            osi: Some(series.osi().to_string()),
            occ_product_type: None,
        },
        OfficialOptionsReferenceRecordValue::OccProduct(product) => RecordColumns {
            kind: "occ_product",
            provider_symbol: product.options_symbol().as_str(),
            secondary_symbol: product.underlying_alias().as_str(),
            search_text: format!(
                "{} {} {} {} {}",
                product.options_symbol().as_str().to_lowercase(),
                product.underlying_alias().as_str().to_lowercase(),
                product.symbol_name().to_lowercase(),
                product.exchange_codes().to_lowercase(),
                product.product_type().code().to_lowercase()
            ),
            venue: None,
            osi: None,
            occ_product_type: Some(product.product_type().code()),
        },
    };
    if columns.search_text.is_empty() || columns.search_text.len() > 1_024 {
        Err(OfficialOptionsReferenceError::InvalidInput)
    } else {
        Ok(columns)
    }
}

#[derive(Debug)]
struct AliasKeyColumns<'a> {
    provider_symbol: Option<&'a str>,
    venue: Option<&'a str>,
    osi: Option<String>,
    occ_product_type: Option<&'static str>,
}

fn alias_key_columns(key: &OfficialOptionsReferenceAliasKey) -> AliasKeyColumns<'_> {
    match key {
        OfficialOptionsReferenceAliasKey::CboeSymbol { symbol } => AliasKeyColumns {
            provider_symbol: Some(symbol),
            venue: None,
            osi: None,
            occ_product_type: None,
        },
        OfficialOptionsReferenceAliasKey::CboeOsi { osi } => AliasKeyColumns {
            provider_symbol: None,
            venue: None,
            osi: Some(osi.to_string()),
            occ_product_type: None,
        },
        OfficialOptionsReferenceAliasKey::CboeVenueSymbol { venue, symbol } => AliasKeyColumns {
            provider_symbol: Some(symbol),
            venue: Some(venue.as_str()),
            osi: None,
            occ_product_type: None,
        },
        OfficialOptionsReferenceAliasKey::OccProduct {
            options_symbol,
            product_type,
        } => AliasKeyColumns {
            provider_symbol: Some(options_symbol.as_str()),
            venue: None,
            osi: None,
            occ_product_type: Some(product_type.code()),
        },
    }
}

fn record_value_supports_alias_key(
    value: &OfficialOptionsReferenceRecordValue,
    key: &OfficialOptionsReferenceAliasKey,
) -> bool {
    match (value, key) {
        (
            OfficialOptionsReferenceRecordValue::CboeSeries(series),
            OfficialOptionsReferenceAliasKey::CboeOsi { osi },
        ) => series.osi() == osi,
        (
            OfficialOptionsReferenceRecordValue::CboeSeries(series),
            OfficialOptionsReferenceAliasKey::CboeSymbol { symbol },
        ) => series.cboe_symbol() == symbol,
        (
            OfficialOptionsReferenceRecordValue::CboeSeries(series),
            OfficialOptionsReferenceAliasKey::CboeVenueSymbol { venue, symbol },
        ) => series.venue() == venue && series.cboe_symbol() == symbol,
        (
            OfficialOptionsReferenceRecordValue::OccProduct(product),
            OfficialOptionsReferenceAliasKey::OccProduct {
                options_symbol,
                product_type,
            },
        ) => product.options_symbol() == options_symbol && product.product_type() == *product_type,
        _ => false,
    }
}

fn recompute_stored_alias_assertion_set(
    connection: &Connection,
    generation_digest: EvidenceDigest,
    request_id: &SourceIdentifier,
) -> Result<OfficialOptionsReferenceAliasAssertionSetEvidence, OfficialOptionsReferenceError> {
    let mut builder = OfficialOptionsReferenceAliasAssertionSetBuilder::new(request_id.clone());
    let mut statement = connection.prepare(
        "SELECT memberships.record_id, values_.value_json, objects.surface_json
         FROM official_options_reference_memberships AS memberships
         JOIN official_options_reference_values AS values_
           ON values_.value_digest=memberships.value_digest
         JOIN official_options_reference_objects AS objects
           ON objects.generation_digest=memberships.generation_digest
          AND objects.object_ordinal=memberships.object_ordinal
         WHERE memberships.generation_digest=?1
         ORDER BY memberships.object_ordinal, memberships.provider_row_number,
                  memberships.record_id",
    )?;
    let rows = statement.query_map([generation_digest.bytes().as_slice()], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, String>(2)?,
        ))
    })?;
    for row in rows {
        let (record_id, value_json, surface_json) = row?;
        let value: OfficialOptionsReferenceRecordValue = serde_json::from_str(&value_json)
            .map_err(|_| OfficialOptionsReferenceError::CorruptCatalog)?;
        value
            .validate()
            .map_err(|_| OfficialOptionsReferenceError::CorruptCatalog)?;
        let surface: OfficialOptionsReferenceSurface = serde_json::from_str(&surface_json)
            .map_err(|_| OfficialOptionsReferenceError::CorruptCatalog)?;
        if serde_json::to_string(&value)? != value_json
            || serde_json::to_string(&surface)? != surface_json
            || !record_value_matches_surface(&value, &surface)
        {
            return Err(OfficialOptionsReferenceError::CorruptCatalog);
        }
        builder.try_observe_parts(&parse_source_identifier(record_id)?, &value)?;
    }
    Ok(builder.finish())
}

fn validate_alias_closure(
    connection: &Connection,
    generation_digest: EvidenceDigest,
    header: &OfficialOptionsReferenceGenerationHeader,
) -> Result<(), OfficialOptionsReferenceError> {
    let (resolution_count, observations, resolution_conflicts): (i64, i64, i64) = connection
        .query_row(
            "SELECT COUNT(*), COALESCE(SUM(observation_count), 0),
                    COALESCE(SUM(conflict_count), 0)
             FROM official_options_reference_alias_resolutions WHERE generation_digest=?1",
            [generation_digest.bytes().as_slice()],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )?;
    let conflict_count: i64 = connection.query_row(
        "SELECT COUNT(*) FROM official_options_reference_conflicts WHERE generation_digest=?1",
        [generation_digest.bytes().as_slice()],
        |row| row.get(0),
    )?;
    if resolution_count != to_i64(header.resolutions.count)?
        || observations != to_i64(header.alias_assertions)?
        || resolution_conflicts != to_i64(header.conflicts.count)?
        || conflict_count != to_i64(header.conflicts.count)?
    {
        return Err(OfficialOptionsReferenceError::IncompleteStream);
    }

    let assertion_set =
        recompute_stored_alias_assertion_set(connection, generation_digest, &header.request_id)?;
    if assertion_set != header.alias_assertion_set
        || assertion_set.closure_digest(header.strict_row_set_digest)?
            != header.alias_assertion_closure_digest
    {
        return Err(OfficialOptionsReferenceError::IncompleteStream);
    }

    // The four grouped branches emit at most three keys per admitted record. SQLite owns any
    // bounded aggregation scratch; Rust retains only the scalar mismatch count.
    let alias_mismatches: i64 = connection.query_row(
        "WITH expected(
             key_kind, provider_symbol, venue, osi, occ_product_type, observation_count
         ) AS (
             SELECT 'cboe_symbol', values_.provider_symbol, NULL, NULL, NULL, COUNT(*)
             FROM official_options_reference_memberships AS memberships
             JOIN official_options_reference_values AS values_
               ON values_.value_digest=memberships.value_digest
             WHERE memberships.generation_digest=?1 AND values_.record_kind='cboe_series'
             GROUP BY values_.provider_symbol
             UNION ALL
             SELECT 'cboe_osi', NULL, NULL, values_.osi, NULL, COUNT(*)
             FROM official_options_reference_memberships AS memberships
             JOIN official_options_reference_values AS values_
               ON values_.value_digest=memberships.value_digest
             WHERE memberships.generation_digest=?1 AND values_.record_kind='cboe_series'
             GROUP BY values_.osi
             UNION ALL
             SELECT 'cboe_venue_symbol', values_.provider_symbol, values_.venue, NULL, NULL,
                    COUNT(*)
             FROM official_options_reference_memberships AS memberships
             JOIN official_options_reference_values AS values_
               ON values_.value_digest=memberships.value_digest
             WHERE memberships.generation_digest=?1 AND values_.record_kind='cboe_series'
             GROUP BY values_.venue, values_.provider_symbol
             UNION ALL
             SELECT 'occ_product', values_.provider_symbol, NULL, NULL,
                    values_.occ_product_type, COUNT(*)
             FROM official_options_reference_memberships AS memberships
             JOIN official_options_reference_values AS values_
               ON values_.value_digest=memberships.value_digest
             WHERE memberships.generation_digest=?1 AND values_.record_kind='occ_product'
             GROUP BY values_.provider_symbol, values_.occ_product_type
         ), mismatches AS (
             SELECT 1
             FROM expected
             LEFT JOIN official_options_reference_alias_resolutions AS resolutions
               ON resolutions.generation_digest=?1
              AND resolutions.key_kind=expected.key_kind
              AND resolutions.provider_symbol IS expected.provider_symbol
              AND resolutions.venue IS expected.venue
              AND resolutions.osi IS expected.osi
              AND resolutions.occ_product_type IS expected.occ_product_type
             WHERE resolutions.key_digest IS NULL
                OR resolutions.observation_count<>expected.observation_count
             UNION ALL
             SELECT 1
             FROM official_options_reference_alias_resolutions AS resolutions
             LEFT JOIN expected
               ON resolutions.key_kind=expected.key_kind
              AND resolutions.provider_symbol IS expected.provider_symbol
              AND resolutions.venue IS expected.venue
              AND resolutions.osi IS expected.osi
              AND resolutions.occ_product_type IS expected.occ_product_type
             WHERE resolutions.generation_digest=?1 AND expected.key_kind IS NULL
         )
         SELECT COUNT(*) FROM mismatches",
        [generation_digest.bytes().as_slice()],
        |row| row.get(0),
    )?;

    let conflict_mismatches: i64 = connection.query_row(
        "WITH actual AS (
             SELECT key_digest, COUNT(*) AS conflict_count
             FROM official_options_reference_conflicts
             WHERE generation_digest=?1 GROUP BY key_digest
         )
         SELECT COUNT(*)
         FROM official_options_reference_alias_resolutions AS resolutions
         LEFT JOIN actual ON actual.key_digest=resolutions.key_digest
         WHERE resolutions.generation_digest=?1
           AND resolutions.conflict_count<>COALESCE(actual.conflict_count, 0)",
        [generation_digest.bytes().as_slice()],
        |row| row.get(0),
    )?;
    let unbound_conflicts: i64 = connection.query_row(
        "SELECT COUNT(*) FROM official_options_reference_conflicts AS conflict
         WHERE conflict.generation_digest=?1
           AND (
             NOT EXISTS (
               SELECT 1 FROM official_options_reference_memberships AS first_
               WHERE first_.generation_digest=conflict.generation_digest
                 AND first_.record_id=conflict.first_evidence
             )
             OR NOT EXISTS (
               SELECT 1 FROM official_options_reference_memberships AS second_
               WHERE second_.generation_digest=conflict.generation_digest
                 AND second_.record_id=conflict.second_evidence
             )
           )",
        [generation_digest.bytes().as_slice()],
        |row| row.get(0),
    )?;
    if alias_mismatches != 0 || conflict_mismatches != 0 || unbound_conflicts != 0 {
        return Err(OfficialOptionsReferenceError::IncompleteStream);
    }
    Ok(())
}
fn verify_stored_record_set(
    connection: &Connection,
    generation_digest: EvidenceDigest,
    expected: OfficialOptionsReferenceRecordSetEvidence,
) -> Result<(), OfficialOptionsReferenceError> {
    let mut builder = OfficialOptionsReferenceRecordSetDigestBuilder::new();
    let mut statement = connection.prepare(
        "SELECT memberships.object_ordinal, memberships.provider_row_number,
                memberships.record_id, values_.value_json
         FROM official_options_reference_memberships AS memberships
         JOIN official_options_reference_values AS values_
           ON values_.value_digest=memberships.value_digest
         WHERE memberships.generation_digest=?1
         ORDER BY memberships.object_ordinal, memberships.provider_row_number,
                  memberships.record_id",
    )?;
    let rows = statement.query_map([generation_digest.bytes().as_slice()], |row| {
        Ok((
            row.get::<_, i64>(0)?,
            row.get::<_, i64>(1)?,
            row.get::<_, String>(2)?,
            row.get::<_, String>(3)?,
        ))
    })?;
    for row in rows {
        let (object, provider_row, record_id, value_json) = row?;
        let value: OfficialOptionsReferenceRecordValue = serde_json::from_str(&value_json)
            .map_err(|_| OfficialOptionsReferenceError::CorruptCatalog)?;
        let record = OfficialOptionsReferenceRecordInput::try_new(
            u16::try_from(object).map_err(|_| OfficialOptionsReferenceError::CorruptCatalog)?,
            u32::try_from(provider_row)
                .map_err(|_| OfficialOptionsReferenceError::CorruptCatalog)?,
            parse_source_identifier(record_id)?,
            value,
        )
        .map_err(|_| OfficialOptionsReferenceError::CorruptCatalog)?;
        builder.try_observe(&record)?;
    }
    if builder.finish() == expected {
        Ok(())
    } else {
        Err(OfficialOptionsReferenceError::CorruptCatalog)
    }
}

fn verify_stored_resolution_set(
    connection: &Connection,
    generation_digest: EvidenceDigest,
    expected: OfficialOptionsReferenceResolutionSetEvidence,
) -> Result<(), OfficialOptionsReferenceError> {
    let mut builder = OfficialOptionsReferenceResolutionSetDigestBuilder::new();
    let mut statement = connection.prepare(
        "SELECT key_json, state, observation_count, conflict_count
         FROM official_options_reference_alias_resolutions
         WHERE generation_digest=?1 ORDER BY key_json",
    )?;
    let rows = statement.query_map([generation_digest.bytes().as_slice()], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, i64>(2)?,
            row.get::<_, i64>(3)?,
        ))
    })?;
    for row in rows {
        let (key_json, state, observations, conflicts) = row?;
        let key: OfficialOptionsReferenceAliasKey = serde_json::from_str(&key_json)
            .map_err(|_| OfficialOptionsReferenceError::CorruptCatalog)?;
        let resolution = OfficialOptionsReferenceAliasResolutionInput::try_new(
            key,
            OfficialOptionsReferenceAliasResolutionState::from_database(&state)?,
            u64::try_from(observations)
                .map_err(|_| OfficialOptionsReferenceError::CorruptCatalog)?,
            u32::try_from(conflicts).map_err(|_| OfficialOptionsReferenceError::CorruptCatalog)?,
        )
        .map_err(|_| OfficialOptionsReferenceError::CorruptCatalog)?;
        builder.try_observe(&resolution)?;
    }
    if builder.finish() == expected {
        Ok(())
    } else {
        Err(OfficialOptionsReferenceError::CorruptCatalog)
    }
}

fn verify_stored_conflict_set(
    connection: &Connection,
    generation_digest: EvidenceDigest,
    expected: OfficialOptionsReferenceConflictSetEvidence,
) -> Result<(), OfficialOptionsReferenceError> {
    let mut builder = OfficialOptionsReferenceConflictSetDigestBuilder::new();
    let mut statement = connection.prepare(
        "SELECT resolutions.key_json, conflicts.conflict_kind, conflicts.first_evidence,
                conflicts.second_evidence
         FROM official_options_reference_conflicts AS conflicts
         JOIN official_options_reference_alias_resolutions AS resolutions
           ON resolutions.generation_digest=conflicts.generation_digest
          AND resolutions.key_digest=conflicts.key_digest
         WHERE conflicts.generation_digest=?1
         ORDER BY resolutions.key_json, conflicts.conflict_kind,
                  conflicts.first_evidence, conflicts.second_evidence",
    )?;
    let rows = statement.query_map([generation_digest.bytes().as_slice()], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, String>(2)?,
            row.get::<_, String>(3)?,
        ))
    })?;
    for row in rows {
        let (key_json, kind, first, second) = row?;
        let conflict = OfficialOptionsReferenceConflictInput::try_new(
            serde_json::from_str(&key_json)
                .map_err(|_| OfficialOptionsReferenceError::CorruptCatalog)?,
            OfficialOptionsReferenceConflictKind::from_database(&kind)?,
            parse_source_identifier(first)?,
            parse_source_identifier(second)?,
        )
        .map_err(|_| OfficialOptionsReferenceError::CorruptCatalog)?;
        if !record_supports_alias_key(
            connection,
            generation_digest,
            &conflict.first_evidence,
            &conflict.key,
        )? || !record_supports_alias_key(
            connection,
            generation_digest,
            &conflict.second_evidence,
            &conflict.key,
        )? || !conflict_semantics_match(connection, generation_digest, &conflict)?
        {
            return Err(OfficialOptionsReferenceError::CorruptCatalog);
        }
        builder.try_observe(&conflict)?;
    }
    if builder.finish() == expected {
        Ok(())
    } else {
        Err(OfficialOptionsReferenceError::CorruptCatalog)
    }
}

#[derive(Debug)]
struct GenerationDatabaseRow {
    dataset: String,
    sequence: i64,
    previous: Option<Vec<u8>>,
    request_id: String,
    requested_at: i64,
    request_deadline: i64,
    strict_row_set_digest: Vec<u8>,
    alias_assertions: i64,
    alias_assertion_closure_digest: Vec<u8>,
    total_payload_bytes: i64,
    strict_row_count: i64,
    record_count: i64,
    object_count: i64,
    resolution_count: i64,
    conflict_count: i64,
    record_set_digest: Vec<u8>,
    resolution_set_digest: Vec<u8>,
    conflict_set_digest: Vec<u8>,
    published_at: i64,
}

fn load_generation_receipt(
    connection: &Connection,
    expected_dataset: &SourceIdentifier,
    generation_digest: EvidenceDigest,
) -> Result<Option<OfficialOptionsReferenceGenerationReceipt>, OfficialOptionsReferenceError> {
    let row: Option<GenerationDatabaseRow> = connection
        .query_row(
            "SELECT dataset_id, generation_sequence, previous_generation_digest, request_id,
                    requested_at_ns, request_deadline_ns, strict_row_set_digest,
                    alias_assertion_count, alias_assertion_closure_digest, total_payload_bytes,
                    strict_row_count, record_count, object_count, alias_resolution_count,
                    conflict_count, record_set_digest, alias_resolution_set_digest,
                    conflict_set_digest, published_at_ns
             FROM official_options_reference_generations WHERE generation_digest=?1",
            [generation_digest.bytes().as_slice()],
            |row| {
                Ok(GenerationDatabaseRow {
                    dataset: row.get(0)?,
                    sequence: row.get(1)?,
                    previous: row.get(2)?,
                    request_id: row.get(3)?,
                    requested_at: row.get(4)?,
                    request_deadline: row.get(5)?,
                    strict_row_set_digest: row.get(6)?,
                    alias_assertions: row.get(7)?,
                    alias_assertion_closure_digest: row.get(8)?,
                    total_payload_bytes: row.get(9)?,
                    strict_row_count: row.get(10)?,
                    record_count: row.get(11)?,
                    object_count: row.get(12)?,
                    resolution_count: row.get(13)?,
                    conflict_count: row.get(14)?,
                    record_set_digest: row.get(15)?,
                    resolution_set_digest: row.get(16)?,
                    conflict_set_digest: row.get(17)?,
                    published_at: row.get(18)?,
                })
            },
        )
        .optional()?;
    let Some(row) = row else {
        return Ok(None);
    };
    if row.dataset != expected_dataset.as_str()
        || row.requested_at >= row.request_deadline
        || row.requested_at > row.published_at
    {
        return Err(OfficialOptionsReferenceError::CorruptCatalog);
    }
    let dataset = parse_source_identifier(row.dataset)?;
    let sequence = u32::try_from(row.sequence)
        .ok()
        .filter(|value| (1..=MAX_GENERATIONS).contains(value))
        .ok_or(OfficialOptionsReferenceError::CorruptCatalog)?;
    let previous = row.previous.map(evidence_from_database).transpose()?;
    if (sequence == 1) != previous.is_none() {
        return Err(OfficialOptionsReferenceError::CorruptCatalog);
    }
    let record_count = database_u64(row.record_count, 1, MAX_OFFICIAL_OPTIONS_REFERENCE_RECORDS)?;
    let strict_row_count = database_u64(
        row.strict_row_count,
        1,
        MAX_OFFICIAL_OPTIONS_REFERENCE_STRICT_ROWS,
    )?;
    if record_count > strict_row_count {
        return Err(OfficialOptionsReferenceError::CorruptCatalog);
    }
    let object_count = database_usize(
        row.object_count,
        OfficialOptionsReferenceProvider::ALL.len(),
        MAX_OFFICIAL_OPTIONS_REFERENCE_OBJECTS,
    )?;
    let resolution_count = database_u64(
        row.resolution_count,
        1,
        MAX_OFFICIAL_OPTIONS_REFERENCE_ALIAS_RESOLUTIONS,
    )?;
    let conflict_count = database_u64(
        row.conflict_count,
        0,
        MAX_OFFICIAL_OPTIONS_REFERENCE_CONFLICTS,
    )?;
    let sources = load_source_evidence(connection, generation_digest)?;
    let objects = load_object_evidence(connection, generation_digest)?;
    if sources.len() != OfficialOptionsReferenceProvider::ALL.len() || objects.len() != object_count
    {
        return Err(OfficialOptionsReferenceError::CorruptCatalog);
    }
    let retained_counts: (i64, i64, i64) = connection.query_row(
        "SELECT
             (SELECT COUNT(*) FROM official_options_reference_memberships
              WHERE generation_digest=?1),
             (SELECT COUNT(*) FROM official_options_reference_alias_resolutions
              WHERE generation_digest=?1),
             (SELECT COUNT(*) FROM official_options_reference_conflicts
              WHERE generation_digest=?1)",
        [generation_digest.bytes().as_slice()],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
    )?;
    if retained_counts
        != (
            to_i64(record_count)?,
            to_i64(resolution_count)?,
            to_i64(conflict_count)?,
        )
    {
        return Err(OfficialOptionsReferenceError::CorruptCatalog);
    }
    let receipt = OfficialOptionsReferenceGenerationReceipt {
        dataset,
        generation_digest,
        generation_sequence: sequence,
        previous_generation_digest: previous,
        request_id: parse_source_identifier(row.request_id)?,
        requested_at: Timestamp::from_unix_nanos(row.requested_at),
        request_deadline: Timestamp::from_unix_nanos(row.request_deadline),
        strict_row_set_digest: evidence_from_database(row.strict_row_set_digest)?,
        alias_assertions: database_u64(
            row.alias_assertions,
            1,
            MAX_OFFICIAL_OPTIONS_REFERENCE_ALIAS_ASSERTIONS,
        )?,
        alias_assertion_closure_digest: evidence_from_database(row.alias_assertion_closure_digest)?,
        total_payload_bytes: database_u64(row.total_payload_bytes, 1, MAX_TOTAL_PAYLOAD_BYTES)?,
        strict_row_count,
        record_count,
        alias_resolution_count: resolution_count,
        conflict_count,
        record_set_digest: evidence_from_database(row.record_set_digest)?,
        alias_resolution_set_digest: evidence_from_database(row.resolution_set_digest)?,
        conflict_set_digest: evidence_from_database(row.conflict_set_digest)?,
        published_at: Timestamp::from_unix_nanos(row.published_at),
        sources,
        objects,
    };
    if generation_digest_from_receipt(&receipt)? != generation_digest {
        return Err(OfficialOptionsReferenceError::CorruptCatalog);
    }
    Ok(Some(receipt))
}

fn load_source_evidence(
    connection: &Connection,
    generation_digest: EvidenceDigest,
) -> Result<Box<[OfficialOptionsReferenceSourceEvidence]>, OfficialOptionsReferenceError> {
    let mut statement = connection.prepare(
        "SELECT provider, source_id, source_revision, source_revision_digest, rights_id,
                source_payload_set_digest
         FROM official_options_reference_generation_sources
         WHERE generation_digest=?1 ORDER BY provider DESC",
    )?;
    let rows = statement.query_map([generation_digest.bytes().as_slice()], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, String>(2)?,
            row.get::<_, Vec<u8>>(3)?,
            row.get::<_, Vec<u8>>(4)?,
            row.get::<_, Vec<u8>>(5)?,
        ))
    })?;
    let mut sources = Vec::with_capacity(OfficialOptionsReferenceProvider::ALL.len());
    for row in rows {
        let (provider, source_id, revision, revision_digest, rights_id, payload_digest) = row?;
        sources.push(OfficialOptionsReferenceSourceEvidence {
            provider: OfficialOptionsReferenceProvider::from_database(&provider)?,
            source_id: parse_source_id(source_id)?,
            source_revision: parse_source_identifier(revision)?,
            source_revision_digest: evidence_from_database(revision_digest)?,
            rights_id: evidence_from_database(rights_id)?,
            source_payload_set_digest: evidence_from_database(payload_digest)?,
        });
    }
    sources.sort_by_key(OfficialOptionsReferenceSourceEvidence::provider);
    if sources
        .iter()
        .map(OfficialOptionsReferenceSourceEvidence::provider)
        .ne(OfficialOptionsReferenceProvider::ALL)
    {
        return Err(OfficialOptionsReferenceError::CorruptCatalog);
    }
    Ok(sources.into_boxed_slice())
}

fn load_object_evidence(
    connection: &Connection,
    generation_digest: EvidenceDigest,
) -> Result<Box<[OfficialOptionsReferenceObjectEvidence]>, OfficialOptionsReferenceError> {
    let mut statement = connection.prepare(
        "SELECT object_ordinal, provider, source_id, surface_json, surface_key, object_id,
                native_schema, raw_claim_digest, physical_receipt_digest, payload_digest,
                payload_bytes, source_timestamp_ns, available_at_ns, received_at_ns,
                strict_row_set_digest, strict_row_count
         FROM official_options_reference_objects WHERE generation_digest=?1
         ORDER BY object_ordinal",
    )?;
    let rows = statement.query_map([generation_digest.bytes().as_slice()], |row| {
        Ok((
            row.get::<_, i64>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, String>(2)?,
            row.get::<_, String>(3)?,
            row.get::<_, String>(4)?,
            row.get::<_, String>(5)?,
            row.get::<_, String>(6)?,
            row.get::<_, Vec<u8>>(7)?,
            row.get::<_, Vec<u8>>(8)?,
            row.get::<_, Vec<u8>>(9)?,
            row.get::<_, i64>(10)?,
            row.get::<_, Option<i64>>(11)?,
            row.get::<_, i64>(12)?,
            row.get::<_, i64>(13)?,
            row.get::<_, Vec<u8>>(14)?,
            row.get::<_, i64>(15)?,
        ))
    })?;
    let mut objects = Vec::new();
    for row in rows {
        let (
            ordinal,
            provider,
            source_id,
            surface_json,
            surface_key,
            object_id,
            native_schema,
            raw_claim_digest,
            physical_receipt_digest,
            payload_digest,
            payload_bytes,
            source_timestamp,
            available_at,
            received_at,
            strict_row_set_digest,
            strict_row_count,
        ) = row?;
        let ordinal =
            u16::try_from(ordinal).map_err(|_| OfficialOptionsReferenceError::CorruptCatalog)?;
        if usize::from(ordinal) != objects.len() {
            return Err(OfficialOptionsReferenceError::CorruptCatalog);
        }
        let surface: OfficialOptionsReferenceSurface = serde_json::from_str(&surface_json)
            .map_err(|_| OfficialOptionsReferenceError::CorruptCatalog)?;
        if serde_json::to_string(&surface)? != surface_json
            || surface.stable_key()? != surface_key
            || surface.provider() != OfficialOptionsReferenceProvider::from_database(&provider)?
            || available_at > received_at
            || source_timestamp.is_some_and(|value| value > received_at)
        {
            return Err(OfficialOptionsReferenceError::CorruptCatalog);
        }
        objects.push(OfficialOptionsReferenceObjectEvidence {
            object_ordinal: ordinal,
            provider: surface.provider(),
            source_id: parse_source_id(source_id)?,
            surface,
            object_id: parse_source_identifier(object_id)?,
            native_schema: parse_source_identifier(native_schema)?,
            raw_claim_digest: evidence_from_database(raw_claim_digest)?,
            physical_receipt_digest: evidence_from_database(physical_receipt_digest)?,
            payload_digest: evidence_from_database(payload_digest)?,
            payload_bytes: database_u64(payload_bytes, 1, MAX_TOTAL_PAYLOAD_BYTES)?,
            source_timestamp: source_timestamp.map(Timestamp::from_unix_nanos),
            available_at: Timestamp::from_unix_nanos(available_at),
            received_at: Timestamp::from_unix_nanos(received_at),
            strict_row_set_digest: evidence_from_database(strict_row_set_digest)?,
            strict_row_count: database_u64(
                strict_row_count,
                0,
                MAX_OFFICIAL_OPTIONS_REFERENCE_STRICT_ROWS,
            )?,
        });
    }
    Ok(objects.into_boxed_slice())
}

fn generation_digest_from_receipt(
    receipt: &OfficialOptionsReferenceGenerationReceipt,
) -> Result<EvidenceDigest, OfficialOptionsReferenceError> {
    let mut hasher = Sha256::new();
    hasher.update(GENERATION_DOMAIN);
    hash_bytes(&mut hasher, receipt.dataset.as_str().as_bytes())?;
    match receipt.previous_generation_digest {
        Some(previous) => {
            hasher.update([1]);
            hash_evidence(&mut hasher, previous);
        }
        None => hasher.update([0]),
    }
    hash_bytes(&mut hasher, receipt.request_id.as_str().as_bytes())?;
    hasher.update(receipt.requested_at.unix_nanos().to_be_bytes());
    hasher.update(receipt.request_deadline.unix_nanos().to_be_bytes());
    hash_evidence(&mut hasher, receipt.strict_row_set_digest);
    hasher.update(receipt.alias_assertions.to_be_bytes());
    hash_evidence(&mut hasher, receipt.alias_assertion_closure_digest);
    hasher.update(receipt.total_payload_bytes.to_be_bytes());
    hasher.update(receipt.strict_row_count.to_be_bytes());
    hasher.update(receipt.record_count.to_be_bytes());
    hash_evidence(&mut hasher, receipt.record_set_digest);
    hasher.update(receipt.alias_resolution_count.to_be_bytes());
    hash_evidence(&mut hasher, receipt.alias_resolution_set_digest);
    hasher.update(receipt.conflict_count.to_be_bytes());
    hash_evidence(&mut hasher, receipt.conflict_set_digest);
    hasher.update(to_u64(receipt.sources.len())?.to_be_bytes());
    for source in &receipt.sources {
        hash_bytes(&mut hasher, source.provider.database_name().as_bytes())?;
        hash_bytes(&mut hasher, source.source_id.as_str().as_bytes())?;
        hash_bytes(&mut hasher, source.source_revision.as_str().as_bytes())?;
        hash_evidence(&mut hasher, source.source_revision_digest);
        hash_evidence(&mut hasher, source.rights_id);
        hash_evidence(&mut hasher, source.source_payload_set_digest);
    }
    hasher.update(to_u64(receipt.objects.len())?.to_be_bytes());
    for object in &receipt.objects {
        hasher.update(object.object_ordinal.to_be_bytes());
        hash_bytes(&mut hasher, &serde_json::to_vec(&object.surface)?)?;
        hash_bytes(&mut hasher, object.source_id.as_str().as_bytes())?;
        hash_bytes(&mut hasher, object.object_id.as_str().as_bytes())?;
        hash_bytes(&mut hasher, object.native_schema.as_str().as_bytes())?;
        hash_evidence(&mut hasher, object.raw_claim_digest);
        hash_evidence(&mut hasher, object.physical_receipt_digest);
        hash_evidence(&mut hasher, object.payload_digest);
        hasher.update(object.payload_bytes.to_be_bytes());
        hash_optional_timestamp(&mut hasher, object.source_timestamp);
        hasher.update(object.available_at.unix_nanos().to_be_bytes());
        hasher.update(object.received_at.unix_nanos().to_be_bytes());
        hash_evidence(&mut hasher, object.strict_row_set_digest);
        hasher.update(object.strict_row_count.to_be_bytes());
    }
    Ok(finalize(hasher))
}

fn generation_receipt_matches_header(
    receipt: &OfficialOptionsReferenceGenerationReceipt,
    dataset: &SourceIdentifier,
    generation_digest: EvidenceDigest,
    sequence: u32,
    sources: &[PreparedSourceAuthority],
    header: &OfficialOptionsReferenceGenerationHeader,
) -> bool {
    receipt.dataset == *dataset
        && receipt.generation_digest == generation_digest
        && receipt.generation_sequence == sequence
        && receipt.previous_generation_digest == header.expected_previous_generation
        && receipt.request_id == header.request_id
        && receipt.requested_at == header.requested_at
        && receipt.request_deadline == header.request_deadline
        && receipt.strict_row_set_digest == header.strict_row_set_digest
        && receipt.alias_assertions == header.alias_assertions
        && receipt.alias_assertion_closure_digest == header.alias_assertion_closure_digest
        && receipt.total_payload_bytes == header.total_payload_bytes
        && receipt.strict_row_count == header.strict_row_count
        && receipt.record_count == header.records.count
        && receipt.alias_resolution_count == header.resolutions.count
        && receipt.conflict_count == header.conflicts.count
        && receipt.record_set_digest == header.records.digest
        && receipt.alias_resolution_set_digest == header.resolutions.digest
        && receipt.conflict_set_digest == header.conflicts.digest
        && receipt.sources.len() == sources.len()
        && receipt.objects.len() == header.objects.len()
        && receipt.sources.iter().zip(sources).all(|(stored, input)| {
            stored.provider == input.provider
                && stored.source_id == *input.metadata.source_id()
                && stored.source_revision == *input.metadata.revision().as_source_identifier()
                && stored.source_revision_digest == input.source_revision_digest
                && stored.rights_id == input.rights_id
                && stored.source_payload_set_digest == input.source_payload_set_digest
        })
        && receipt
            .objects
            .iter()
            .zip(header.objects.iter())
            .all(|(stored, input)| {
                stored.object_ordinal == input.object_ordinal
                    && stored.provider == input.surface.provider()
                    && stored.source_id == input.source_id
                    && stored.surface == input.surface
                    && stored.object_id == input.object_id
                    && stored.native_schema == input.native_schema
                    && raw_claim_digest(&input.raw_claim).ok() == Some(stored.raw_claim_digest)
                    && stored.physical_receipt_digest == input.raw_claim.physical_receipt_digest()
                    && stored.payload_digest == input.payload_digest
                    && stored.payload_bytes == input.raw_claim.size_bytes()
                    && stored.source_timestamp == input.source_timestamp
                    && stored.available_at == input.available_at
                    && stored.received_at == input.received_at
                    && stored.strict_row_set_digest == input.strict_row_set_digest
                    && stored.strict_row_count == input.strict_row_count
            })
}

fn database_u64(
    value: i64,
    minimum: u64,
    maximum: u64,
) -> Result<u64, OfficialOptionsReferenceError> {
    u64::try_from(value)
        .ok()
        .filter(|value| (minimum..=maximum).contains(value))
        .ok_or(OfficialOptionsReferenceError::CorruptCatalog)
}

fn database_usize(
    value: i64,
    minimum: usize,
    maximum: usize,
) -> Result<usize, OfficialOptionsReferenceError> {
    usize::try_from(value)
        .ok()
        .filter(|value| (minimum..=maximum).contains(value))
        .ok_or(OfficialOptionsReferenceError::CorruptCatalog)
}

#[derive(Debug)]
struct SelectedGeneration {
    generation: OfficialOptionsReferenceGenerationReceipt,
    knowledge_at: Timestamp,
    effective_at: Timestamp,
}

#[derive(Debug)]
enum CurrentReferenceDatasetSelection {
    Unavailable,
    Unique(SourceIdentifier),
    Ambiguous {
        eligible_dataset_count: u16,
        has_more: bool,
    },
}

fn trusted_read_now(connection: &Connection) -> Result<Timestamp, OfficialOptionsReferenceError> {
    let wall_now = now_timestamp()?;
    let durable: i64 = connection.query_row(
        "SELECT last_timestamp_ns FROM catalog_authority_clock WHERE singleton=1",
        [],
        |row| row.get(0),
    )?;
    if wall_now.unix_nanos() < durable {
        Err(OfficialOptionsReferenceError::CorruptCatalog)
    } else {
        Ok(wall_now)
    }
}

fn freeze_generation_selection(
    selection: OfficialOptionsReferenceGenerationSelection,
    now: Timestamp,
) -> Result<OfficialOptionsReferenceGenerationSelection, OfficialOptionsReferenceError> {
    let (knowledge_at, effective_at, exact_digest) =
        generation_selection_coordinates(selection, now)?;
    Ok(match exact_digest {
        Some(generation_digest) => OfficialOptionsReferenceGenerationSelection::Exact {
            generation_digest,
            knowledge_at,
            effective_at,
        },
        None => OfficialOptionsReferenceGenerationSelection::AsOf {
            knowledge_at,
            effective_at,
        },
    })
}

fn generation_selection_coordinates(
    selection: OfficialOptionsReferenceGenerationSelection,
    now: Timestamp,
) -> Result<(Timestamp, Timestamp, Option<EvidenceDigest>), OfficialOptionsReferenceError> {
    let coordinates = match selection {
        OfficialOptionsReferenceGenerationSelection::Current => (now, now, None),
        OfficialOptionsReferenceGenerationSelection::AsOf {
            knowledge_at,
            effective_at,
        } => (knowledge_at, effective_at, None),
        OfficialOptionsReferenceGenerationSelection::Exact {
            generation_digest,
            knowledge_at,
            effective_at,
        } => {
            validate_sha256(generation_digest)?;
            (knowledge_at, effective_at, Some(generation_digest))
        }
    };
    if coordinates.0 > now || coordinates.1 > coordinates.0 {
        return Err(OfficialOptionsReferenceError::InvalidInput);
    }
    Ok(coordinates)
}

fn select_current_reference_dataset(
    connection: &Connection,
    selection: OfficialOptionsReferenceGenerationSelection,
    now: Timestamp,
) -> Result<CurrentReferenceDatasetSelection, OfficialOptionsReferenceError> {
    let (knowledge_at, _effective_at, exact_digest) =
        generation_selection_coordinates(selection, now)?;
    let exact_digest = exact_digest.map(|digest| digest.bytes().to_vec());
    let mut statement = connection.prepare(
        "WITH candidates AS (
             SELECT dataset_id, generation_digest,
                    ROW_NUMBER() OVER (
                        PARTITION BY dataset_id ORDER BY generation_sequence DESC
                    ) AS dataset_rank
             FROM official_options_reference_generations
             WHERE published_at_ns<=?1
               AND (?2 IS NULL OR generation_digest=?2)
         )
         SELECT dataset_id, generation_digest
         FROM candidates WHERE dataset_rank=1
         ORDER BY dataset_id
         LIMIT ?3",
    )?;
    let scan_limit = MAX_CURRENT_REFERENCE_DATASET_CANDIDATES
        .checked_add(1)
        .ok_or(OfficialOptionsReferenceError::CapacityExceeded)?;
    let mut rows = statement.query(params![
        knowledge_at.unix_nanos(),
        exact_digest.as_deref(),
        to_i64(scan_limit)?,
    ])?;
    let mut candidates = Vec::new();
    candidates
        .try_reserve_exact(scan_limit)
        .map_err(|_| OfficialOptionsReferenceError::CapacityExceeded)?;
    while let Some(row) = rows.next()? {
        candidates.push((row.get::<_, String>(0)?, row.get::<_, Vec<u8>>(1)?));
    }
    drop(rows);
    drop(statement);

    let has_more = candidates.len() > MAX_CURRENT_REFERENCE_DATASET_CANDIDATES;
    candidates.truncate(MAX_CURRENT_REFERENCE_DATASET_CANDIDATES);
    let mut unique = None;
    let mut eligible_dataset_count = 0_u16;
    for (dataset, generation_digest) in candidates {
        let dataset = parse_source_identifier(dataset)?;
        let generation_digest = evidence_from_database(generation_digest)?;
        let generation = load_generation_receipt(connection, &dataset, generation_digest)?
            .ok_or(OfficialOptionsReferenceError::CorruptCatalog)?;
        if generation.dataset != dataset
            || generation.generation_digest != generation_digest
            || generation.published_at > knowledge_at
        {
            return Err(OfficialOptionsReferenceError::CorruptCatalog);
        }
        match require_generation_read_authority(connection, &generation, now) {
            Ok(()) => {}
            Err(
                OfficialOptionsReferenceError::SourceUnavailable
                | OfficialOptionsReferenceError::RightsUnavailable,
            ) => continue,
            Err(error) => return Err(error),
        }
        eligible_dataset_count = eligible_dataset_count
            .checked_add(1)
            .ok_or(OfficialOptionsReferenceError::CapacityExceeded)?;
        if unique.is_none() {
            unique = Some(dataset);
        }
    }
    if has_more || eligible_dataset_count > 1 {
        return Ok(CurrentReferenceDatasetSelection::Ambiguous {
            eligible_dataset_count,
            has_more,
        });
    }
    Ok(match unique {
        Some(dataset) => CurrentReferenceDatasetSelection::Unique(dataset),
        None => CurrentReferenceDatasetSelection::Unavailable,
    })
}

fn select_generation(
    connection: &Connection,
    dataset: &SourceIdentifier,
    selection: OfficialOptionsReferenceGenerationSelection,
    now: Timestamp,
) -> Result<Option<SelectedGeneration>, OfficialOptionsReferenceError> {
    let (knowledge_at, effective_at, exact_digest) =
        generation_selection_coordinates(selection, now)?;
    let digest_bytes: Option<Vec<u8>> = if let Some(exact) = exact_digest {
        connection
            .query_row(
                "SELECT generation_digest FROM official_options_reference_generations
                 WHERE dataset_id=?1 AND generation_digest=?2 AND published_at_ns<=?3",
                params![
                    dataset.as_str(),
                    exact.bytes().as_slice(),
                    knowledge_at.unix_nanos(),
                ],
                |row| row.get(0),
            )
            .optional()?
    } else {
        connection
            .query_row(
                "SELECT generation_digest FROM official_options_reference_generations
                 WHERE dataset_id=?1 AND published_at_ns<=?2
                 ORDER BY generation_sequence DESC LIMIT 1",
                params![dataset.as_str(), knowledge_at.unix_nanos()],
                |row| row.get(0),
            )
            .optional()?
    };
    let Some(digest_bytes) = digest_bytes else {
        return Ok(None);
    };
    let selected_digest = evidence_from_database(digest_bytes)?;
    if exact_digest.is_some_and(|expected| expected != selected_digest) {
        return Err(OfficialOptionsReferenceError::CorruptCatalog);
    }
    let generation = load_generation_receipt(connection, dataset, selected_digest)?
        .ok_or(OfficialOptionsReferenceError::CorruptCatalog)?;
    if generation.published_at > knowledge_at {
        return Err(OfficialOptionsReferenceError::CorruptCatalog);
    }
    require_generation_read_authority(connection, &generation, now)?;
    Ok(Some(SelectedGeneration {
        generation,
        knowledge_at,
        effective_at,
    }))
}

fn require_generation_read_authority(
    connection: &Connection,
    generation: &OfficialOptionsReferenceGenerationReceipt,
    now: Timestamp,
) -> Result<(), OfficialOptionsReferenceError> {
    for source in &generation.sources {
        let metadata_json: Option<String> = connection
            .query_row(
                "SELECT metadata_json FROM source_revisions
                 WHERE source_id=?1 AND revision_digest=?2",
                params![
                    source.source_id.as_str(),
                    source.source_revision_digest.bytes().as_slice(),
                ],
                |row| row.get(0),
            )
            .optional()?;
        let metadata_json =
            metadata_json.ok_or(OfficialOptionsReferenceError::SourceUnavailable)?;
        let metadata: SourceMetadata = serde_json::from_str(&metadata_json)
            .map_err(|_| OfficialOptionsReferenceError::CorruptCatalog)?;
        let authorized: bool = connection.query_row(
            "SELECT EXISTS(
                 SELECT 1 FROM source_rights
                 WHERE rights_id=?1 AND source_id=?2 AND payload_algorithm=1
                   AND payload_digest=?3 AND (operation_mask & 2)=2
                   AND admitted_at_ns<=?4
                   AND (authorization_expires_at_ns IS NULL
                        OR authorization_expires_at_ns>?4)
             )",
            params![
                source.rights_id.bytes().as_slice(),
                source.source_id.as_str(),
                source.source_payload_set_digest.bytes().as_slice(),
                now.unix_nanos(),
            ],
            |row| row.get(0),
        )?;
        if digest(sha256(metadata_json.as_bytes())) != source.source_revision_digest
            || serde_json::to_string(&metadata)? != metadata_json
            || metadata.source_id() != &source.source_id
            || metadata.revision().as_source_identifier() != &source.source_revision
            || !metadata.is_effective_at(now)
            || !authorized
        {
            return Err(if authorized {
                OfficialOptionsReferenceError::SourceUnavailable
            } else {
                OfficialOptionsReferenceError::RightsUnavailable
            });
        }
    }
    Ok(())
}

fn resolve_identity(
    authority: std::sync::MutexGuard<'_, CatalogAuthority>,
    dataset: &SourceIdentifier,
    selection: OfficialOptionsReferenceGenerationSelection,
    query: OfficialOptionsReferenceIdentityQuery,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<OfficialOptionsReferenceIdentityResolution, OfficialOptionsReferenceError> {
    let connection = &authority.catalog().connection;
    install_progress_handler(connection, deadline, cancellation)?;
    let result = (|| {
        let now = trusted_read_now(connection)?;
        let Some(selected) = select_generation(connection, dataset, selection, now)? else {
            return Ok(OfficialOptionsReferenceIdentityResolution::Missing { generation: None });
        };
        let mut budget = ResultBudget::new(authority.catalog().result_bytes);
        let (mut records, has_more_records) = load_query_records(
            connection,
            &selected.generation,
            &query,
            MAX_OFFICIAL_OPTIONS_REFERENCE_EXACT_ROWS,
            &mut budget,
        )?;
        if records.is_empty() {
            if load_alias_resolution(
                connection,
                selected.generation.generation_digest,
                &query.alias_key(),
            )?
            .is_some()
            {
                return Err(OfficialOptionsReferenceError::CorruptCatalog);
            }
            return Ok(OfficialOptionsReferenceIdentityResolution::Missing {
                generation: Some(selected.generation),
            });
        }
        let key = query.alias_key();
        let resolution =
            load_alias_resolution(connection, selected.generation.generation_digest, &key)?
                .ok_or(OfficialOptionsReferenceError::CorruptCatalog)?;
        let (mut conflicts, has_more_conflicts) = load_conflicts(
            connection,
            selected.generation.generation_digest,
            &key,
            MAX_OFFICIAL_OPTIONS_REFERENCE_EXACT_ROWS,
            &mut budget,
        )?;
        let records_match = records
            .iter()
            .all(|record| record_matches_query(record, &query));
        let retained_records = to_u64(records.len())?;
        let retained_conflicts = to_u64(conflicts.len())?;
        if (!has_more_records && retained_records != resolution.observations)
            || (has_more_records
                && resolution.observations <= to_u64(MAX_OFFICIAL_OPTIONS_REFERENCE_EXACT_ROWS)?)
            || (!has_more_conflicts && retained_conflicts != u64::from(resolution.conflicts))
            || (has_more_conflicts
                && u64::from(resolution.conflicts)
                    <= to_u64(MAX_OFFICIAL_OPTIONS_REFERENCE_EXACT_ROWS)?)
            || (resolution.state == OfficialOptionsReferenceAliasResolutionState::Exact
                && resolution.conflicts != 0)
        {
            return Err(OfficialOptionsReferenceError::CorruptCatalog);
        }
        if !records_match
            || has_more_records
            || resolution.state == OfficialOptionsReferenceAliasResolutionState::Ambiguous
        {
            return Ok(OfficialOptionsReferenceIdentityResolution::Ambiguous {
                generation: selected.generation,
                ambiguity: OfficialOptionsReferenceAmbiguity {
                    records: records.into_boxed_slice(),
                    conflicts: conflicts.into_boxed_slice(),
                    has_more_records: has_more_records || !records_match,
                    has_more_conflicts,
                },
            });
        }
        if has_more_conflicts || resolution.observations != to_u64(records.len())? {
            return Err(OfficialOptionsReferenceError::CorruptCatalog);
        }
        let canonical = resolve_canonical_identity(
            connection,
            &records,
            selected.knowledge_at,
            selected.effective_at,
            &mut budget,
        )?;
        let receipt_digest = canonical_resolution_receipt_digest(
            selected.generation.generation_digest,
            selected.knowledge_at,
            selected.effective_at,
            &key,
            &records,
            &canonical,
        )?;
        records.shrink_to_fit();
        conflicts.clear();
        Ok(OfficialOptionsReferenceIdentityResolution::Exact {
            generation: selected.generation,
            identity: OfficialOptionsReferenceExactIdentity {
                records: records.into_boxed_slice(),
                canonical,
                receipt_digest,
            },
        })
    })();
    clear_progress_handler(connection)?;
    classify_operation(result, deadline, cancellation)
}

#[derive(Debug)]
struct StoredAliasResolution {
    state: OfficialOptionsReferenceAliasResolutionState,
    observations: u64,
    conflicts: u32,
}

fn load_alias_resolution(
    connection: &Connection,
    generation_digest: EvidenceDigest,
    key: &OfficialOptionsReferenceAliasKey,
) -> Result<Option<StoredAliasResolution>, OfficialOptionsReferenceError> {
    let key_json = alias_key_json(key)?;
    let expected_digest = alias_key_digest(key)?;
    type ResolutionRow = (
        String,
        Option<String>,
        Option<String>,
        Option<String>,
        Option<String>,
        String,
        String,
        i64,
        i64,
        Vec<u8>,
    );
    let row: Option<ResolutionRow> = connection
        .query_row(
            "SELECT key_kind, provider_symbol, venue, osi, occ_product_type, key_json,
                    state, observation_count, conflict_count, resolution_digest
             FROM official_options_reference_alias_resolutions
             WHERE generation_digest=?1 AND key_digest=?2",
            params![
                generation_digest.bytes().as_slice(),
                expected_digest.bytes().as_slice(),
            ],
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
                ))
            },
        )
        .optional()?;
    row.map(
        |(
            key_kind,
            provider_symbol,
            venue,
            osi,
            occ_product_type,
            stored_json,
            state,
            observations,
            conflicts,
            stored_digest,
        )| {
            let state = OfficialOptionsReferenceAliasResolutionState::from_database(&state)?;
            let observations = database_u64(
                observations,
                1,
                MAX_OFFICIAL_OPTIONS_REFERENCE_ALIAS_ASSERTIONS,
            )?;
            let conflicts = u32::try_from(conflicts)
                .map_err(|_| OfficialOptionsReferenceError::CorruptCatalog)?;
            let reconstructed = OfficialOptionsReferenceAliasResolutionInput::try_new(
                serde_json::from_str(&stored_json)
                    .map_err(|_| OfficialOptionsReferenceError::CorruptCatalog)?,
                state,
                observations,
                conflicts,
            )
            .map_err(|_| OfficialOptionsReferenceError::CorruptCatalog)?;
            let columns = alias_key_columns(&reconstructed.key);
            if stored_json != key_json
                || reconstructed.key != *key
                || key_kind != key.database_kind()
                || provider_symbol.as_deref() != columns.provider_symbol
                || venue.as_deref() != columns.venue
                || osi != columns.osi
                || occ_product_type.as_deref() != columns.occ_product_type
                || evidence_from_database(stored_digest)?
                    != canonical_json_digest(b"resolution", &reconstructed)?
            {
                return Err(OfficialOptionsReferenceError::CorruptCatalog);
            }
            Ok(StoredAliasResolution {
                state,
                observations,
                conflicts,
            })
        },
    )
    .transpose()
}

fn load_query_records(
    connection: &Connection,
    generation: &OfficialOptionsReferenceGenerationReceipt,
    query: &OfficialOptionsReferenceIdentityQuery,
    maximum_rows: usize,
    budget: &mut ResultBudget,
) -> Result<(Vec<OfficialOptionsReferenceRecord>, bool), OfficialOptionsReferenceError> {
    let limit = maximum_rows
        .checked_add(1)
        .ok_or(OfficialOptionsReferenceError::CapacityExceeded)?;
    let (predicate, first, second) = match query {
        OfficialOptionsReferenceIdentityQuery::Osi(osi) => {
            ("values_.osi=?2", osi.to_string(), None)
        }
        OfficialOptionsReferenceIdentityQuery::CboeSymbol(symbol) => (
            "values_.record_kind='cboe_series' AND values_.provider_symbol=?2",
            symbol.clone(),
            None,
        ),
        OfficialOptionsReferenceIdentityQuery::CboeVenueSymbol { venue, symbol } => (
            "values_.record_kind='cboe_series' AND values_.venue=?2 AND values_.provider_symbol=?3",
            venue.as_str().to_owned(),
            Some(symbol.clone()),
        ),
        OfficialOptionsReferenceIdentityQuery::OccProduct {
            options_symbol,
            product_type,
        } => (
            "values_.record_kind='occ_product' AND values_.provider_symbol=?2 AND values_.occ_product_type=?3",
            options_symbol.as_str().to_owned(),
            Some(product_type.code().to_owned()),
        ),
    };
    let sql = format!(
        "SELECT memberships.object_ordinal, memberships.provider_row_number,
                memberships.record_id, memberships.value_digest, memberships.record_digest,
                values_.provider, values_.record_kind, values_.provider_symbol,
                values_.normalized_provider_symbol, values_.secondary_symbol,
                values_.normalized_search_text, values_.venue, values_.osi,
                values_.occ_product_type, values_.value_json
         FROM official_options_reference_memberships AS memberships
         JOIN official_options_reference_values AS values_
           ON values_.value_digest=memberships.value_digest
         WHERE memberships.generation_digest=?1 AND {predicate}
         ORDER BY memberships.object_ordinal, memberships.provider_row_number,
                  memberships.record_id LIMIT ?4"
    );
    let mut statement = connection.prepare(&sql)?;
    let mut rows = statement.query(params![
        generation.generation_digest.bytes().as_slice(),
        first,
        second,
        to_i64(limit)?,
    ])?;
    let mut records = Vec::with_capacity(limit);
    while let Some(row) = rows.next()? {
        records.push(rebuild_provider_record(row, generation, budget)?);
    }
    let has_more = records.len() > maximum_rows;
    records.truncate(maximum_rows);
    Ok((records, has_more))
}

fn rebuild_provider_record(
    row: &Row<'_>,
    generation: &OfficialOptionsReferenceGenerationReceipt,
    budget: &mut ResultBudget,
) -> Result<OfficialOptionsReferenceRecord, OfficialOptionsReferenceError> {
    let object_ordinal: i64 = row.get(0)?;
    let provider_row_number: i64 = row.get(1)?;
    let record_id: String = row.get(2)?;
    let value_digest = evidence_from_database(row.get(3)?)?;
    let record_digest = evidence_from_database(row.get(4)?)?;
    let provider: String = row.get(5)?;
    let record_kind: String = row.get(6)?;
    let provider_symbol: String = row.get(7)?;
    let normalized_provider_symbol: String = row.get(8)?;
    let secondary_symbol: String = row.get(9)?;
    let normalized_search_text: String = row.get(10)?;
    let venue: Option<String> = row.get(11)?;
    let osi: Option<String> = row.get(12)?;
    let occ_product_type: Option<String> = row.get(13)?;
    let value_json: String = row.get(14)?;
    budget
        .charge([
            size_of::<OfficialOptionsReferenceRecord>(),
            record_id.len(),
            provider_symbol.len(),
            secondary_symbol.len(),
            normalized_search_text.len(),
            value_json.len(),
        ])
        .map_err(|_| OfficialOptionsReferenceError::ResultByteLimitExceeded)?;
    let object_ordinal =
        u16::try_from(object_ordinal).map_err(|_| OfficialOptionsReferenceError::CorruptCatalog)?;
    let object = generation
        .objects
        .get(usize::from(object_ordinal))
        .filter(|object| object.object_ordinal == object_ordinal)
        .ok_or(OfficialOptionsReferenceError::CorruptCatalog)?
        .clone();
    budget
        .charge([
            object.source_id.as_str().len(),
            serde_json::to_vec(&object.surface)?.len(),
            object.object_id.as_str().len(),
            object.native_schema.as_str().len(),
        ])
        .map_err(|_| OfficialOptionsReferenceError::ResultByteLimitExceeded)?;
    let provider_row_number = u32::try_from(provider_row_number)
        .map_err(|_| OfficialOptionsReferenceError::CorruptCatalog)?;
    if u64::from(provider_row_number) > object.strict_row_count.saturating_add(1) {
        return Err(OfficialOptionsReferenceError::CorruptCatalog);
    }
    let value: OfficialOptionsReferenceRecordValue = serde_json::from_str(&value_json)
        .map_err(|_| OfficialOptionsReferenceError::CorruptCatalog)?;
    value.validate()?;
    let columns =
        record_columns(&value).map_err(|_| OfficialOptionsReferenceError::CorruptCatalog)?;
    if serde_json::to_string(&value)? != value_json
        || value.provider().database_name() != provider
        || value.provider() != object.provider
        || columns.kind != record_kind
        || columns.provider_symbol != provider_symbol
        || columns.provider_symbol.to_lowercase() != normalized_provider_symbol
        || columns.secondary_symbol != secondary_symbol
        || columns.search_text != normalized_search_text
        || columns.venue != venue.as_deref()
        || columns.osi != osi
        || columns.occ_product_type != occ_product_type.as_deref()
        || record_value_digest(&value)? != value_digest
    {
        return Err(OfficialOptionsReferenceError::CorruptCatalog);
    }
    let input = OfficialOptionsReferenceRecordInput::try_new(
        object_ordinal,
        provider_row_number,
        parse_source_identifier(record_id)?,
        value.clone(),
    )
    .map_err(|_| OfficialOptionsReferenceError::CorruptCatalog)?;
    if record_membership_digest(generation.generation_digest, &input, value_digest)?
        != record_digest
    {
        return Err(OfficialOptionsReferenceError::CorruptCatalog);
    }
    Ok(OfficialOptionsReferenceRecord {
        object,
        provider_row_number: input.provider_row_number,
        record_id: input.record_id,
        value,
        value_digest,
        record_digest,
    })
}

fn record_matches_query(
    record: &OfficialOptionsReferenceRecord,
    query: &OfficialOptionsReferenceIdentityQuery,
) -> bool {
    match (record.value(), query) {
        (
            OfficialOptionsReferenceRecordValue::CboeSeries(series),
            OfficialOptionsReferenceIdentityQuery::Osi(osi),
        ) => series.osi() == osi,
        (
            OfficialOptionsReferenceRecordValue::CboeSeries(series),
            OfficialOptionsReferenceIdentityQuery::CboeSymbol(symbol),
        ) => series.cboe_symbol() == symbol,
        (
            OfficialOptionsReferenceRecordValue::CboeSeries(series),
            OfficialOptionsReferenceIdentityQuery::CboeVenueSymbol { venue, symbol },
        ) => series.venue() == venue && series.cboe_symbol() == symbol,
        (
            OfficialOptionsReferenceRecordValue::OccProduct(product),
            OfficialOptionsReferenceIdentityQuery::OccProduct {
                options_symbol,
                product_type,
            },
        ) => product.options_symbol() == options_symbol && product.product_type() == *product_type,
        _ => false,
    }
}

fn load_conflicts(
    connection: &Connection,
    generation_digest: EvidenceDigest,
    key: &OfficialOptionsReferenceAliasKey,
    maximum_rows: usize,
    budget: &mut ResultBudget,
) -> Result<(Vec<OfficialOptionsReferenceConflict>, bool), OfficialOptionsReferenceError> {
    let limit = maximum_rows
        .checked_add(1)
        .ok_or(OfficialOptionsReferenceError::CapacityExceeded)?;
    let key_digest = alias_key_digest(key)?;
    let mut statement = connection.prepare(
        "SELECT conflict_kind, first_evidence, second_evidence, conflict_digest
         FROM official_options_reference_conflicts
         WHERE generation_digest=?1 AND key_digest=?2
         ORDER BY conflict_ordinal LIMIT ?3",
    )?;
    let rows = statement.query_map(
        params![
            generation_digest.bytes().as_slice(),
            key_digest.bytes().as_slice(),
            to_i64(limit)?,
        ],
        |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, Vec<u8>>(3)?,
            ))
        },
    )?;
    let mut conflicts = Vec::with_capacity(limit);
    for row in rows {
        let (kind, first, second, stored_digest) = row?;
        budget
            .charge([
                size_of::<OfficialOptionsReferenceConflict>(),
                first.len(),
                second.len(),
            ])
            .map_err(|_| OfficialOptionsReferenceError::ResultByteLimitExceeded)?;
        let input = OfficialOptionsReferenceConflictInput::try_new(
            key.clone(),
            OfficialOptionsReferenceConflictKind::from_database(&kind)?,
            parse_source_identifier(first)?,
            parse_source_identifier(second)?,
        )
        .map_err(|_| OfficialOptionsReferenceError::CorruptCatalog)?;
        let stored_digest = evidence_from_database(stored_digest)?;
        if canonical_json_digest(b"conflict", &input)? != stored_digest {
            return Err(OfficialOptionsReferenceError::CorruptCatalog);
        }
        if !record_supports_alias_key(
            connection,
            generation_digest,
            &input.first_evidence,
            &input.key,
        )? || !record_supports_alias_key(
            connection,
            generation_digest,
            &input.second_evidence,
            &input.key,
        )? || !conflict_semantics_match(connection, generation_digest, &input)?
        {
            return Err(OfficialOptionsReferenceError::CorruptCatalog);
        }
        conflicts.push(OfficialOptionsReferenceConflict {
            kind: input.kind,
            key: input.key,
            first_evidence: input.first_evidence,
            second_evidence: input.second_evidence,
            digest: stored_digest,
        });
    }
    let has_more = conflicts.len() > maximum_rows;
    conflicts.truncate(maximum_rows);
    Ok((conflicts, has_more))
}

fn search_text(
    authority: std::sync::MutexGuard<'_, CatalogAuthority>,
    dataset: &SourceIdentifier,
    selection: OfficialOptionsReferenceGenerationSelection,
    query: &str,
    maximum_rows: usize,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<OfficialOptionsReferenceSearchPage, OfficialOptionsReferenceError> {
    let connection = &authority.catalog().connection;
    install_progress_handler(connection, deadline, cancellation)?;
    let result = (|| {
        let now = trusted_read_now(connection)?;
        let Some(selected) = select_generation(connection, dataset, selection, now)? else {
            return Ok(OfficialOptionsReferenceSearchPage {
                generation: None,
                records: Box::new([]),
                has_more: false,
            });
        };
        let limit = maximum_rows
            .checked_add(1)
            .ok_or(OfficialOptionsReferenceError::CapacityExceeded)?;
        let normalized = query.to_lowercase();
        let mut budget = ResultBudget::new(authority.catalog().result_bytes);
        let mut statement = connection.prepare(
            "SELECT memberships.object_ordinal, memberships.provider_row_number,
                    memberships.record_id, memberships.value_digest, memberships.record_digest,
                    values_.provider, values_.record_kind, values_.provider_symbol,
                    values_.normalized_provider_symbol, values_.secondary_symbol,
                    values_.normalized_search_text, values_.venue, values_.osi,
                    values_.occ_product_type, values_.value_json
             FROM official_options_reference_memberships AS memberships
             JOIN official_options_reference_values AS values_
               ON values_.value_digest=memberships.value_digest
             WHERE memberships.generation_digest=?1
               AND instr(values_.normalized_search_text, ?2)>0
             ORDER BY CASE
                 WHEN values_.normalized_provider_symbol=?2 THEN 0
                 WHEN instr(values_.normalized_provider_symbol, ?2)=1 THEN 1
                 ELSE 2
             END,
             values_.provider_symbol, values_.record_kind, memberships.object_ordinal,
             memberships.provider_row_number
             LIMIT ?3",
        )?;
        let mut rows = statement.query(params![
            selected.generation.generation_digest.bytes().as_slice(),
            normalized,
            to_i64(limit)?,
        ])?;
        let mut records = Vec::with_capacity(limit);
        while let Some(row) = rows.next()? {
            records.push(rebuild_provider_record(
                row,
                &selected.generation,
                &mut budget,
            )?);
        }
        let has_more = records.len() > maximum_rows;
        records.truncate(maximum_rows);
        Ok(OfficialOptionsReferenceSearchPage {
            generation: Some(selected.generation),
            records: records.into_boxed_slice(),
            has_more,
        })
    })();
    clear_progress_handler(connection)?;
    classify_operation(result, deadline, cancellation)
}

fn resolve_canonical_identity(
    connection: &Connection,
    records: &[OfficialOptionsReferenceRecord],
    knowledge_at: Timestamp,
    effective_at: Timestamp,
    budget: &mut ResultBudget,
) -> Result<OfficialOptionsReferenceCanonicalResolution, OfficialOptionsReferenceError> {
    if records.iter().all(|record| {
        matches!(
            record.value(),
            OfficialOptionsReferenceRecordValue::OccProduct(_)
        )
    }) {
        return Ok(OfficialOptionsReferenceCanonicalResolution::ProviderProductOnly);
    }
    if records.iter().any(|record| {
        !matches!(
            record.value(),
            OfficialOptionsReferenceRecordValue::CboeSeries(_)
        )
    }) {
        return Err(OfficialOptionsReferenceError::CorruptCatalog);
    }
    let mut terms = BTreeSet::new();
    for record in records {
        let OfficialOptionsReferenceRecordValue::CboeSeries(series) = record.value() else {
            return Err(OfficialOptionsReferenceError::CorruptCatalog);
        };
        terms.insert((
            series.osi().to_string().to_lowercase(),
            "external_identifier",
        ));
        terms.insert((series.cboe_symbol().to_lowercase(), "provider_symbol"));
    }
    let mut definitions = Vec::new();
    definitions
        .try_reserve_exact(MAX_OFFICIAL_OPTIONS_REFERENCE_CANONICAL_CANDIDATES)
        .map_err(|_| OfficialOptionsReferenceError::CapacityExceeded)?;
    let mut seen_revisions = BTreeSet::new();
    let mut scan_truncated = false;
    for (term, term_kind) in terms {
        let mut statement = connection.prepare(
            "SELECT DISTINCT revisions.revision_digest, revisions.instrument_id,
                    revisions.revision_sequence, revisions.effective_start_ns,
                    revisions.effective_end_ns, revisions.definition_json,
                    revisions.published_at_ns
             FROM market_data_instrument_revisions AS revisions
             JOIN market_data_instrument_search_terms AS terms
               ON terms.revision_digest=revisions.revision_digest
             WHERE terms.normalized_term=?1
               AND terms.term_kind=?2
               AND revisions.published_at_ns<=?3
               AND revisions.effective_start_ns<=?4
               AND (revisions.effective_end_ns IS NULL OR ?4<revisions.effective_end_ns)
               AND NOT EXISTS (
                   SELECT 1 FROM market_data_instrument_revisions AS later
                   WHERE later.instrument_id=revisions.instrument_id
                     AND later.published_at_ns<=?3
                     AND later.effective_start_ns<=?4
                     AND later.effective_start_ns>revisions.effective_start_ns
               )
             ORDER BY revisions.instrument_id, revisions.revision_digest
             LIMIT ?5",
        )?;
        let rows = statement.query_map(
            params![
                term,
                term_kind,
                knowledge_at.unix_nanos(),
                effective_at.unix_nanos(),
                to_i64(MAX_OFFICIAL_OPTIONS_REFERENCE_CANONICAL_CANDIDATES + 1)?,
            ],
            |row| {
                Ok((
                    row.get::<_, Vec<u8>>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, i64>(3)?,
                    row.get::<_, Option<i64>>(4)?,
                    row.get::<_, String>(5)?,
                    row.get::<_, i64>(6)?,
                ))
            },
        )?;
        let mut term_rows = 0_usize;
        for row in rows {
            let row = row?;
            term_rows = term_rows
                .checked_add(1)
                .ok_or(OfficialOptionsReferenceError::CapacityExceeded)?;
            let digest = array32(row.0.clone())?;
            if seen_revisions.insert(digest) {
                if definitions.len() == MAX_OFFICIAL_OPTIONS_REFERENCE_CANONICAL_CANDIDATES {
                    scan_truncated = true;
                    continue;
                }
                budget
                    .charge([
                        size_of::<OfficialOptionsReferenceCanonicalCandidate>(),
                        row.1.len(),
                        row.5.len(),
                    ])
                    .map_err(|_| OfficialOptionsReferenceError::ResultByteLimitExceeded)?;
                definitions.push(row);
            }
        }
        if term_rows > MAX_OFFICIAL_OPTIONS_REFERENCE_CANONICAL_CANDIDATES {
            scan_truncated = true;
        }
    }
    let mut candidates = Vec::new();
    candidates
        .try_reserve_exact(definitions.len())
        .map_err(|_| OfficialOptionsReferenceError::CapacityExceeded)?;
    for (revision_bytes, instrument_text, sequence, start, end, json, published_at) in definitions {
        let revision_digest = evidence_from_database(revision_bytes)?;
        if digest(sha256(json.as_bytes())) != revision_digest {
            return Err(OfficialOptionsReferenceError::CorruptCatalog);
        }
        let definition: MarketDataInstrumentDefinition = serde_json::from_str(&json)
            .map_err(|_| OfficialOptionsReferenceError::CorruptCatalog)?;
        let instrument_id = InstrumentId::from_str(&instrument_text)
            .map_err(|_| OfficialOptionsReferenceError::CorruptCatalog)?;
        let interval = EffectiveInterval::new(
            Timestamp::from_unix_nanos(start),
            end.map(Timestamp::from_unix_nanos),
        )
        .map_err(|_| OfficialOptionsReferenceError::CorruptCatalog)?;
        if definition.instrument_id() != instrument_id
            || definition.asset_class() != AssetClass::Option
            || definition.effective_interval() != interval
            || !interval_contains(interval, effective_at)
            || published_at > knowledge_at.unix_nanos()
        {
            continue;
        }
        let mut osi_match = false;
        let mut provider_match = false;
        let mut valid_from = interval.starts_at();
        let mut valid_until = interval.ends_at();
        for record in records {
            let OfficialOptionsReferenceRecordValue::CboeSeries(series) = record.value() else {
                continue;
            };
            if let Some(identifier) = definition.identifiers().iter().find(|identifier| {
                matches!(identifier.identifier(), ExternalIdentifier::OccOption(value) if value == series.osi())
                    && identifier.assignment_verification()
                        == AssignmentVerification::VerifiedAssigned
                    && interval_contains(identifier.validity(), effective_at)
            }) {
                osi_match = true;
                valid_from = valid_from.max(identifier.validity().starts_at());
                valid_until =
                    minimum_optional_timestamp(valid_until, identifier.validity().ends_at());
            }
            let provider_id = ProviderInstrumentId::try_from(series.cboe_symbol().to_owned())
                .map_err(|_| OfficialOptionsReferenceError::CorruptCatalog)?;
            if let Some(identity) = definition.provider_identity_at(
                record.object().source_id(),
                &provider_id,
                effective_at,
            ) {
                provider_match = true;
                valid_from = valid_from.max(identity.validity().starts_at());
                valid_until =
                    minimum_optional_timestamp(valid_until, identity.validity().ends_at());
            }
        }
        let match_kind = match (osi_match, provider_match) {
            (true, true) => {
                OfficialOptionsReferenceCanonicalMatchKind::VerifiedAssignedOsiAndProviderIdentity
            }
            (true, false) => OfficialOptionsReferenceCanonicalMatchKind::VerifiedAssignedOsi,
            (false, true) => OfficialOptionsReferenceCanonicalMatchKind::ExactProviderIdentity,
            (false, false) => continue,
        };
        let revision_sequence = u32::try_from(sequence)
            .ok()
            .filter(|sequence| (1..=MAX_GENERATIONS).contains(sequence))
            .ok_or(OfficialOptionsReferenceError::CorruptCatalog)?;
        let match_validity = EffectiveInterval::new(valid_from, valid_until)
            .map_err(|_| OfficialOptionsReferenceError::CorruptCatalog)?;
        candidates.push(OfficialOptionsReferenceCanonicalCandidate {
            instrument_id,
            revision_digest,
            revision_sequence,
            published_at: Timestamp::from_unix_nanos(published_at),
            definition_effective: interval,
            match_kind,
            match_validity,
        });
    }
    candidates
        .sort_by_key(|candidate| (candidate.instrument_id, candidate.revision_digest.bytes()));
    candidates.dedup_by_key(|candidate| candidate.instrument_id);
    if scan_truncated {
        Ok(OfficialOptionsReferenceCanonicalResolution::Truncated {
            candidates: candidates.into_boxed_slice(),
        })
    } else if candidates.is_empty() {
        Ok(OfficialOptionsReferenceCanonicalResolution::Missing)
    } else if candidates.len() == 1 {
        Ok(OfficialOptionsReferenceCanonicalResolution::Exact(
            candidates.remove(0),
        ))
    } else {
        Ok(OfficialOptionsReferenceCanonicalResolution::Ambiguous {
            candidates: candidates.into_boxed_slice(),
            has_more: false,
        })
    }
}

fn interval_contains(interval: EffectiveInterval, at: Timestamp) -> bool {
    interval.starts_at() <= at && interval.ends_at().is_none_or(|end| at < end)
}

fn minimum_optional_timestamp(
    left: Option<Timestamp>,
    right: Option<Timestamp>,
) -> Option<Timestamp> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.min(right)),
        (Some(value), None) | (None, Some(value)) => Some(value),
        (None, None) => None,
    }
}

fn canonical_resolution_receipt_digest(
    generation_digest: EvidenceDigest,
    knowledge_at: Timestamp,
    effective_at: Timestamp,
    key: &OfficialOptionsReferenceAliasKey,
    records: &[OfficialOptionsReferenceRecord],
    resolution: &OfficialOptionsReferenceCanonicalResolution,
) -> Result<EvidenceDigest, OfficialOptionsReferenceError> {
    let mut hasher = Sha256::new();
    hasher.update(CANONICAL_RESOLUTION_RECEIPT_DOMAIN);
    hash_evidence(&mut hasher, generation_digest);
    hasher.update(knowledge_at.unix_nanos().to_be_bytes());
    hasher.update(effective_at.unix_nanos().to_be_bytes());
    hash_evidence(&mut hasher, alias_key_digest(key)?);
    hasher.update(to_u64(records.len())?.to_be_bytes());
    for record in records {
        hash_evidence(&mut hasher, record.record_digest);
    }
    match resolution {
        OfficialOptionsReferenceCanonicalResolution::Exact(candidate) => {
            hasher.update([1]);
            hash_canonical_candidate(&mut hasher, candidate);
        }
        OfficialOptionsReferenceCanonicalResolution::Ambiguous {
            candidates,
            has_more,
        } => {
            hasher.update([2]);
            hasher.update([u8::from(*has_more)]);
            hasher.update(to_u64(candidates.len())?.to_be_bytes());
            for candidate in candidates {
                hash_canonical_candidate(&mut hasher, candidate);
            }
        }
        OfficialOptionsReferenceCanonicalResolution::Truncated { candidates } => {
            hasher.update([5]);
            hasher.update(to_u64(candidates.len())?.to_be_bytes());
            for candidate in candidates {
                hash_canonical_candidate(&mut hasher, candidate);
            }
        }
        OfficialOptionsReferenceCanonicalResolution::Missing => hasher.update([3]),
        OfficialOptionsReferenceCanonicalResolution::ProviderProductOnly => hasher.update([4]),
    }
    Ok(finalize(hasher))
}

fn hash_canonical_candidate(
    hasher: &mut Sha256,
    candidate: &OfficialOptionsReferenceCanonicalCandidate,
) {
    hasher.update(candidate.instrument_id.as_uuid().as_bytes());
    hash_evidence(hasher, candidate.revision_digest);
    hasher.update(candidate.revision_sequence.to_be_bytes());
    hasher.update(candidate.published_at.unix_nanos().to_be_bytes());
    hasher.update(
        candidate
            .definition_effective
            .starts_at()
            .unix_nanos()
            .to_be_bytes(),
    );
    hash_optional_timestamp(hasher, candidate.definition_effective.ends_at());
    hasher.update([match candidate.match_kind {
        OfficialOptionsReferenceCanonicalMatchKind::VerifiedAssignedOsi => 1,
        OfficialOptionsReferenceCanonicalMatchKind::ExactProviderIdentity => 2,
        OfficialOptionsReferenceCanonicalMatchKind::VerifiedAssignedOsiAndProviderIdentity => 3,
    }]);
    hasher.update(
        candidate
            .match_validity
            .starts_at()
            .unix_nanos()
            .to_be_bytes(),
    );
    hash_optional_timestamp(hasher, candidate.match_validity.ends_at());
}

fn verify_exact_generation(
    connection: &Connection,
    dataset: &SourceIdentifier,
    expected_generation_digest: EvidenceDigest,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<
    (
        OfficialOptionsReferenceGenerationReceipt,
        OfficialOptionsReferenceGenerationHeader,
    ),
    OfficialOptionsReferenceError,
> {
    install_progress_handler(connection, deadline, cancellation)?;
    let result = (|| {
        let now = trusted_read_now(connection)?;
        let generation = load_generation_receipt(connection, dataset, expected_generation_digest)?
            .ok_or(OfficialOptionsReferenceError::CorruptCatalog)?;
        require_generation_read_authority(connection, &generation, now)?;
        let header = reconstruct_generation_header(connection, &generation)?;
        verify_stored_record_set(
            connection,
            expected_generation_digest,
            OfficialOptionsReferenceRecordSetEvidence {
                count: generation.record_count,
                digest: generation.record_set_digest,
            },
        )?;
        verify_stored_resolution_set(
            connection,
            expected_generation_digest,
            OfficialOptionsReferenceResolutionSetEvidence {
                count: generation.alias_resolution_count,
                digest: generation.alias_resolution_set_digest,
            },
        )?;
        verify_stored_conflict_set(
            connection,
            expected_generation_digest,
            OfficialOptionsReferenceConflictSetEvidence {
                count: generation.conflict_count,
                digest: generation.conflict_set_digest,
            },
        )?;
        validate_alias_closure(connection, expected_generation_digest, &header)
            .map_err(|_| OfficialOptionsReferenceError::CorruptCatalog)?;
        let sources = reconstruct_source_manifest(connection, &generation)?;
        if sources.iter().any(|source| {
            header.source_payload_set_digest(source.provider).ok()
                != Some(source.source_payload_set_digest)
        }) || generation_digest(dataset, &sources, &header)? != expected_generation_digest
        {
            return Err(OfficialOptionsReferenceError::CorruptCatalog);
        }
        check_operation(deadline, cancellation)?;
        Ok((generation, header))
    })();
    clear_progress_handler(connection)?;
    classify_operation(result, deadline, cancellation)
}

fn reconstruct_generation_header(
    connection: &Connection,
    generation: &OfficialOptionsReferenceGenerationReceipt,
) -> Result<OfficialOptionsReferenceGenerationHeader, OfficialOptionsReferenceError> {
    let mut objects = Vec::with_capacity(generation.objects.len());
    for stored in &generation.objects {
        let row: (String, String, Vec<u8>, i64, i64, i64, String) = connection.query_row(
            "SELECT raw.raw_claim_kind, raw.raw_claim_json, raw.content_digest,
                    raw.size_bytes, raw.integrity_chunk_bytes, raw.unit_count,
                    raw.relative_reference
             FROM sealed_raw_objects AS raw
             WHERE raw.raw_claim_digest=?1 AND raw.physical_receipt_digest=?2",
            params![
                stored.raw_claim_digest.bytes().as_slice(),
                stored.physical_receipt_digest.bytes().as_slice(),
            ],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                    row.get(6)?,
                ))
            },
        )?;
        let sealed: SealedResearchRawClaim = serde_json::from_str(&row.1)
            .map_err(|_| OfficialOptionsReferenceError::CorruptCatalog)?;
        let SealedResearchRawClaim::LogicalObject(claim) = sealed else {
            return Err(OfficialOptionsReferenceError::CorruptCatalog);
        };
        if row.0 != "logical_object"
            || serde_json::to_string(&SealedResearchRawClaim::LogicalObject(claim.clone()))?
                != row.1
            || raw_claim_digest(&claim)? != stored.raw_claim_digest
            || evidence_from_database(row.2)? != stored.payload_digest
            || row.3 != to_i64(stored.payload_bytes)?
            || row.4 != to_i64(claim.integrity_chunk_bytes())?
            || row.5 != to_i64(claim.chunks().len())?
            || row.6 != claim.relative_reference()
            || claim.content_digest() != stored.payload_digest
            || claim.physical_receipt_digest() != stored.physical_receipt_digest
        {
            return Err(OfficialOptionsReferenceError::CorruptCatalog);
        }
        objects.push(
            OfficialOptionsReferenceObjectInput::try_from_retained_claim(
                OfficialOptionsReferenceObjectCoordinates {
                    object_ordinal: stored.object_ordinal,
                    source_id: stored.source_id.clone(),
                    surface: stored.surface.clone(),
                    object_id: stored.object_id.clone(),
                    native_schema: stored.native_schema.clone(),
                    payload_digest: stored.payload_digest,
                    source_timestamp: stored.source_timestamp,
                    available_at: stored.available_at,
                    received_at: stored.received_at,
                    strict_row_set_digest: stored.strict_row_set_digest,
                    strict_row_count: stored.strict_row_count,
                },
                claim,
            )?,
        );
    }
    let alias_assertion_set = recompute_stored_alias_assertion_set(
        connection,
        generation.generation_digest,
        &generation.request_id,
    )?;
    let header = OfficialOptionsReferenceGenerationHeader::try_new(
        generation.previous_generation_digest,
        generation.request_id.clone(),
        generation.requested_at,
        generation.request_deadline,
        alias_assertion_set,
        OfficialOptionsReferenceRecordSetEvidence {
            count: generation.record_count,
            digest: generation.record_set_digest,
        },
        OfficialOptionsReferenceResolutionSetEvidence {
            count: generation.alias_resolution_count,
            digest: generation.alias_resolution_set_digest,
        },
        OfficialOptionsReferenceConflictSetEvidence {
            count: generation.conflict_count,
            digest: generation.conflict_set_digest,
        },
        objects,
    )
    .map_err(|_| OfficialOptionsReferenceError::CorruptCatalog)?;
    if header.strict_row_set_digest != generation.strict_row_set_digest
        || header.alias_assertions != generation.alias_assertions
        || header.alias_assertion_closure_digest != generation.alias_assertion_closure_digest
    {
        return Err(OfficialOptionsReferenceError::CorruptCatalog);
    }
    Ok(header)
}

fn reconstruct_source_manifest(
    connection: &Connection,
    generation: &OfficialOptionsReferenceGenerationReceipt,
) -> Result<Box<[PreparedSourceAuthority]>, OfficialOptionsReferenceError> {
    let mut sources = Vec::with_capacity(generation.sources.len());
    for source in &generation.sources {
        let metadata_json: String = connection.query_row(
            "SELECT metadata_json FROM source_revisions
             WHERE source_id=?1 AND revision_digest=?2",
            params![
                source.source_id.as_str(),
                source.source_revision_digest.bytes().as_slice(),
            ],
            |row| row.get(0),
        )?;
        let metadata: SourceMetadata = serde_json::from_str(&metadata_json)
            .map_err(|_| OfficialOptionsReferenceError::CorruptCatalog)?;
        if serde_json::to_string(&metadata)? != metadata_json
            || metadata.source_id() != &source.source_id
            || metadata.revision().as_source_identifier() != &source.source_revision
            || digest(sha256(metadata_json.as_bytes())) != source.source_revision_digest
        {
            return Err(OfficialOptionsReferenceError::CorruptCatalog);
        }
        sources.push(PreparedSourceAuthority {
            provider: source.provider,
            metadata,
            metadata_json,
            source_revision_digest: source.source_revision_digest,
            rights_id: source.rights_id,
            source_payload_set_digest: source.source_payload_set_digest,
        });
    }
    Ok(sources.into_boxed_slice())
}

fn install_progress_handler(
    connection: &Connection,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<(), OfficialOptionsReferenceError> {
    let token = cancellation.clone();
    connection.progress_handler(
        SQLITE_PROGRESS_OPERATIONS,
        Some(move || token.is_cancelled() || Instant::now() >= deadline),
    )?;
    Ok(())
}

fn clear_progress_handler(connection: &Connection) -> Result<(), OfficialOptionsReferenceError> {
    connection.progress_handler::<fn() -> bool>(0, None)?;
    Ok(())
}

fn check_operation(
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<(), OfficialOptionsReferenceError> {
    if cancellation.is_cancelled() {
        Err(OfficialOptionsReferenceError::Cancelled)
    } else if Instant::now() >= deadline {
        Err(OfficialOptionsReferenceError::DeadlineExceeded)
    } else {
        Ok(())
    }
}

fn classify_operation<T>(
    result: Result<T, OfficialOptionsReferenceError>,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<T, OfficialOptionsReferenceError> {
    if cancellation.is_cancelled() {
        Err(OfficialOptionsReferenceError::Cancelled)
    } else if Instant::now() >= deadline {
        Err(OfficialOptionsReferenceError::DeadlineExceeded)
    } else {
        result
    }
}

fn record_sort_key(
    record: &OfficialOptionsReferenceRecordInput,
) -> Result<Vec<u8>, OfficialOptionsReferenceError> {
    let mut key = Vec::with_capacity(size_of::<u16>() + size_of::<u32>());
    key.extend_from_slice(&record.object_ordinal.to_be_bytes());
    key.extend_from_slice(&record.provider_row_number.to_be_bytes());
    Ok(key)
}

fn conflict_sort_key(
    conflict: &OfficialOptionsReferenceConflictInput,
) -> Result<Vec<u8>, OfficialOptionsReferenceError> {
    let mut key = alias_key_json(&conflict.key)?.into_bytes();
    key.push(0);
    key.extend_from_slice(conflict.kind.database_name().as_bytes());
    key.push(0);
    key.extend_from_slice(conflict.first_evidence.as_str().as_bytes());
    key.push(0);
    key.extend_from_slice(conflict.second_evidence.as_str().as_bytes());
    Ok(key)
}

fn alias_key_json(
    key: &OfficialOptionsReferenceAliasKey,
) -> Result<String, OfficialOptionsReferenceError> {
    key.validate()?;
    serde_json::to_string(key).map_err(Into::into)
}

fn canonical_json_digest<T: Serialize>(
    domain: &[u8],
    value: &T,
) -> Result<EvidenceDigest, OfficialOptionsReferenceError> {
    let encoded = serde_json::to_vec(value)?;
    let mut hasher = Sha256::new();
    hasher.update(domain);
    hash_bytes(&mut hasher, &encoded)?;
    Ok(finalize(hasher))
}

fn alias_key_digest(
    key: &OfficialOptionsReferenceAliasKey,
) -> Result<EvidenceDigest, OfficialOptionsReferenceError> {
    canonical_json_digest(ALIAS_KEY_DOMAIN, key)
}

fn record_value_digest(
    value: &OfficialOptionsReferenceRecordValue,
) -> Result<EvidenceDigest, OfficialOptionsReferenceError> {
    value.validate()?;
    canonical_json_digest(RECORD_VALUE_DOMAIN, value)
}

fn record_membership_digest(
    generation_digest: EvidenceDigest,
    record: &OfficialOptionsReferenceRecordInput,
    value_digest: EvidenceDigest,
) -> Result<EvidenceDigest, OfficialOptionsReferenceError> {
    let mut hasher = Sha256::new();
    hasher.update(RECORD_MEMBERSHIP_DOMAIN);
    hash_evidence(&mut hasher, generation_digest);
    hash_evidence(&mut hasher, value_digest);
    hash_bytes(&mut hasher, &serde_json::to_vec(record)?)?;
    Ok(finalize(hasher))
}

fn raw_claim_json(claim: &ResearchObjectClaim) -> Result<String, OfficialOptionsReferenceError> {
    serde_json::to_string(&SealedResearchRawClaim::LogicalObject(claim.clone())).map_err(Into::into)
}

fn raw_claim_digest(
    claim: &ResearchObjectClaim,
) -> Result<EvidenceDigest, OfficialOptionsReferenceError> {
    Ok(shared_raw_claim_digest(raw_claim_json(claim)?.as_bytes()))
}

fn source_payload_set_digest(
    provider: OfficialOptionsReferenceProvider,
    objects: &[OfficialOptionsReferenceObjectInput],
) -> Result<EvidenceDigest, OfficialOptionsReferenceError> {
    let mut hasher = Sha256::new();
    hasher.update(SOURCE_PAYLOAD_SET_DOMAIN);
    hash_bytes(&mut hasher, provider.database_name().as_bytes())?;
    let selected: Vec<_> = objects
        .iter()
        .filter(|object| object.surface.provider() == provider)
        .collect();
    if selected.is_empty() {
        return Err(OfficialOptionsReferenceError::InvalidInput);
    }
    hasher.update(to_u64(selected.len())?.to_be_bytes());
    for object in selected {
        hasher.update(object.object_ordinal.to_be_bytes());
        hash_bytes(&mut hasher, object.source_id.as_str().as_bytes())?;
        hash_bytes(&mut hasher, &serde_json::to_vec(&object.surface)?)?;
        hash_bytes(&mut hasher, object.object_id.as_str().as_bytes())?;
        hash_evidence(&mut hasher, object.payload_digest);
        hash_evidence(&mut hasher, raw_claim_digest(&object.raw_claim)?);
        hasher.update(object.raw_claim.size_bytes().to_be_bytes());
    }
    Ok(finalize(hasher))
}

fn generation_digest(
    dataset: &SourceIdentifier,
    sources: &[PreparedSourceAuthority],
    header: &OfficialOptionsReferenceGenerationHeader,
) -> Result<EvidenceDigest, OfficialOptionsReferenceError> {
    let mut hasher = Sha256::new();
    hasher.update(GENERATION_DOMAIN);
    hash_bytes(&mut hasher, dataset.as_str().as_bytes())?;
    match header.expected_previous_generation {
        Some(previous) => {
            hasher.update([1]);
            hash_evidence(&mut hasher, previous);
        }
        None => hasher.update([0]),
    }
    hash_bytes(&mut hasher, header.request_id.as_str().as_bytes())?;
    hasher.update(header.requested_at.unix_nanos().to_be_bytes());
    hasher.update(header.request_deadline.unix_nanos().to_be_bytes());
    hash_evidence(&mut hasher, header.strict_row_set_digest);
    hasher.update(header.alias_assertions.to_be_bytes());
    hash_evidence(&mut hasher, header.alias_assertion_closure_digest);
    hasher.update(header.total_payload_bytes.to_be_bytes());
    hasher.update(header.strict_row_count.to_be_bytes());
    hasher.update(header.records.count.to_be_bytes());
    hash_evidence(&mut hasher, header.records.digest);
    hasher.update(header.resolutions.count.to_be_bytes());
    hash_evidence(&mut hasher, header.resolutions.digest);
    hasher.update(header.conflicts.count.to_be_bytes());
    hash_evidence(&mut hasher, header.conflicts.digest);
    hasher.update(to_u64(sources.len())?.to_be_bytes());
    for source in sources {
        hash_bytes(&mut hasher, source.provider.database_name().as_bytes())?;
        hash_bytes(&mut hasher, source.metadata.source_id().as_str().as_bytes())?;
        hash_bytes(
            &mut hasher,
            source
                .metadata
                .revision()
                .as_source_identifier()
                .as_str()
                .as_bytes(),
        )?;
        hash_evidence(&mut hasher, source.source_revision_digest);
        hash_evidence(&mut hasher, source.rights_id);
        hash_evidence(&mut hasher, source.source_payload_set_digest);
    }
    hasher.update(to_u64(header.objects.len())?.to_be_bytes());
    for object in &header.objects {
        hasher.update(object.object_ordinal.to_be_bytes());
        hash_bytes(&mut hasher, &serde_json::to_vec(&object.surface)?)?;
        hash_bytes(&mut hasher, object.source_id.as_str().as_bytes())?;
        hash_bytes(&mut hasher, object.object_id.as_str().as_bytes())?;
        hash_bytes(&mut hasher, object.native_schema.as_str().as_bytes())?;
        hash_evidence(&mut hasher, raw_claim_digest(&object.raw_claim)?);
        hash_evidence(&mut hasher, object.raw_claim.physical_receipt_digest());
        hash_evidence(&mut hasher, object.payload_digest);
        hasher.update(object.raw_claim.size_bytes().to_be_bytes());
        hash_optional_timestamp(&mut hasher, object.source_timestamp);
        hasher.update(object.available_at.unix_nanos().to_be_bytes());
        hasher.update(object.received_at.unix_nanos().to_be_bytes());
        hash_evidence(&mut hasher, object.strict_row_set_digest);
        hasher.update(object.strict_row_count.to_be_bytes());
    }
    Ok(finalize(hasher))
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

fn hash_bytes(hasher: &mut Sha256, bytes: &[u8]) -> Result<(), OfficialOptionsReferenceError> {
    hasher.update(to_u64(bytes.len())?.to_be_bytes());
    hasher.update(bytes);
    Ok(())
}

fn hash_evidence(hasher: &mut Sha256, evidence: EvidenceDigest) {
    hasher.update([match evidence.algorithm() {
        DigestAlgorithm::Sha256 => 1,
        DigestAlgorithm::Blake3 => 2,
    }]);
    hasher.update(evidence.bytes());
}

fn finalize(hasher: Sha256) -> EvidenceDigest {
    digest(hasher.finalize().into())
}

const fn digest(bytes: [u8; 32]) -> EvidenceDigest {
    EvidenceDigest::new(DigestAlgorithm::Sha256, bytes)
}

fn validate_sha256(value: EvidenceDigest) -> Result<(), OfficialOptionsReferenceError> {
    if value.algorithm() == DigestAlgorithm::Sha256 && value.bytes() != [0; 32] {
        Ok(())
    } else {
        Err(OfficialOptionsReferenceError::InvalidInput)
    }
}

fn array32(value: Vec<u8>) -> Result<[u8; 32], OfficialOptionsReferenceError> {
    value
        .try_into()
        .map_err(|_| OfficialOptionsReferenceError::CorruptCatalog)
}

fn evidence_from_database(value: Vec<u8>) -> Result<EvidenceDigest, OfficialOptionsReferenceError> {
    let value = digest(array32(value)?);
    if value.bytes() == [0; 32] {
        Err(OfficialOptionsReferenceError::CorruptCatalog)
    } else {
        Ok(value)
    }
}

fn parse_source_identifier(
    value: String,
) -> Result<SourceIdentifier, OfficialOptionsReferenceError> {
    SourceIdentifier::try_from(value).map_err(|_| OfficialOptionsReferenceError::CorruptCatalog)
}

fn parse_source_id(value: String) -> Result<SourceId, OfficialOptionsReferenceError> {
    SourceId::try_from(value).map_err(|_| OfficialOptionsReferenceError::CorruptCatalog)
}

fn to_i64<T>(value: T) -> Result<i64, OfficialOptionsReferenceError>
where
    T: TryInto<i64>,
{
    value
        .try_into()
        .map_err(|_| OfficialOptionsReferenceError::CapacityExceeded)
}

fn to_u64<T>(value: T) -> Result<u64, OfficialOptionsReferenceError>
where
    T: TryInto<u64>,
{
    value
        .try_into()
        .map_err(|_| OfficialOptionsReferenceError::CapacityExceeded)
}

/// Typed failure for immutable official options-reference publication and reads.
#[derive(Debug, Error)]
pub enum OfficialOptionsReferenceError {
    #[error("official options-reference input is invalid")]
    InvalidInput,
    #[error("official options-reference raw object lacks exact store-issued authority")]
    InvalidRawObjectAuthority,
    #[error("official options-reference result limit is invalid")]
    InvalidLimit,
    #[error("official options-reference source authority is invalid")]
    InvalidSourceAuthority,
    #[error("official options-reference rights capability is invalid")]
    InvalidRightsCapability,
    #[error("official options-reference source is unavailable")]
    SourceUnavailable,
    #[error("official options-reference rights are unavailable")]
    RightsUnavailable,
    #[error("official options-reference bound was exceeded")]
    CapacityExceeded,
    #[error("official options-reference staged stream is not strictly ordered")]
    UnorderedStream,
    #[error("official options-reference staged stream is incomplete")]
    IncompleteStream,
    #[error("official options-reference generation position conflicts")]
    PositionConflict,
    #[error("official options-reference generation was superseded")]
    SupersededGeneration,
    #[error("official options-reference raw claim conflicts with retained bytes")]
    RawClaimConflict,
    #[error("official options-reference value digest conflicts with retained bytes")]
    ValueConflict,
    #[error("official options-reference catalog authority is busy")]
    AuthorityUnavailable,
    #[error("official options-reference operation was cancelled")]
    Cancelled,
    #[error("official options-reference deadline was exceeded")]
    DeadlineExceeded,
    #[error("official options-reference catalog is corrupt")]
    CorruptCatalog,
    #[error("official options-reference result byte limit was exceeded")]
    ResultByteLimitExceeded,
    #[error("official options-reference serialization failed: {0}")]
    Serialization(#[from] serde_json::Error),
    #[error("official options-reference storage failed: {0}")]
    Storage(#[from] rusqlite::Error),
    #[error("official options-reference shared catalog failed: {0}")]
    Catalog(#[from] CatalogError),
    #[error("official options-reference physical raw-object verification failed: {0}")]
    PhysicalStore(#[from] SealedResearchJournalStoreError),
}
