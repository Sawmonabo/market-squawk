//! Durable large-object capture and bounded provider-native reference decoding.

use std::fmt::Write as _;
use std::io::ErrorKind;
use std::num::NonZeroU32;
use std::sync::Arc;

use cap_fs_ext::{FollowSymlinks, OpenOptionsFollowExt as _};
use cap_std::fs::{Dir, OpenOptions};
use chrono::{DateTime, Datelike as _, Utc};
use market_squawk_domain::{
    CalendarDate, DataQuality, DigestAlgorithm, EvidenceDigest, ExactPayloadEvidence, OptionKind,
    ProviderInstrumentId, SourceIdentifier, Timestamp,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use thiserror::Error;
use tokio::io::{AsyncBufReadExt as _, AsyncReadExt as _, AsyncSeekExt as _, AsyncWriteExt as _};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::model::{
    NasdaqDirectoryKind, NasdaqDirectoryPresence, NasdaqFileCreationTime, NasdaqFinancialStatus,
    NasdaqListingRecord, NasdaqMarketCategory, NasdaqOtherExchange, NasdaqProviderFields,
};
/// Official current Nasdaq option-series reference object.
pub const OPTIONS_URL: &str = "https://www.nasdaqtrader.com/dynamic/SymDir/options.txt";
/// Official current Nasdaq-listed bond reference object.
pub const BONDS_LIST_URL: &str = "https://www.nasdaqtrader.com/dynamic/SymDir/bondslist.txt";
/// Maximum admitted exact `options.txt` bytes.
pub const MAX_OPTIONS_SOURCE_BYTES: usize = 128 * 1024 * 1024;
/// Maximum admitted exact `bondslist.txt` bytes.
pub const MAX_BONDS_SOURCE_BYTES: usize = 8 * 1024 * 1024;
/// Maximum admitted option-series rows in one exact object.
pub const MAX_OPTIONS_RECORDS: u64 = 2_500_000;
/// Maximum admitted bond rows in one exact object.
pub const MAX_BONDS_RECORDS: u64 = 32_768;
/// Maximum records decoded into one in-memory typed page.
pub const MAX_REFERENCE_PAGE_RECORDS: u32 = 4_096;
/// Maximum fixed-width provider-key index bytes built for one exact generation.
pub const MAX_REFERENCE_INDEX_BYTES: u64 = 128 * 1024 * 1024;

const MAX_LINE_BYTES: usize = 512;
const READ_CHUNK_BYTES: usize = 64 * 1024;
const AUTHENTICATED_CHUNK_BYTES: usize = 64 * 1024;
const INDEX_ENTRY_BYTES: u64 = 48;
const INDEX_HEADER_BYTES: u64 = 88;
const INDEX_MAGIC: &[u8; 4] = b"NSI1";
const NASDAQ_LISTED_SCHEMA: &str = "nasdaq.nasdaqlisted.pipe-v1";
const OTHER_LISTED_SCHEMA: &str = "nasdaq.otherlisted.pipe-v1";
const BONDS_SCHEMA: &str = "nasdaq.bondslist.pipe-v1";
const OPTIONS_SCHEMA: &str = "nasdaq.options.pipe-v1";
const NASDAQ_LISTED_HEADER: &str = "Symbol|Security Name|Market Category|Test Issue|Financial Status|Round Lot Size|ETF|NextShares";
const OTHER_LISTED_HEADER: &str =
    "ACT Symbol|Security Name|Exchange|CQS Symbol|ETF|Round Lot Size|Test Issue|NASDAQ Symbol";
const BONDS_HEADER: &str = "Symbol|Security Name|Financial Status";
const OPTIONS_HEADER: &str = "Root Symbol|Options Closing Type|Options Type|Expiration Date|Explicit Strike Price|Underlying Symbol|Underlying Issue Name|Pending";
const FILE_CREATION_PREFIX: &str = "File Creation Time: ";

impl NasdaqDirectoryKind {
    pub(crate) const fn maximum_source_bytes(self) -> usize {
        match self {
            Self::NasdaqListed | Self::OtherListed => crate::parser::MAX_SOURCE_BYTES,
            Self::Bonds => MAX_BONDS_SOURCE_BYTES,
            Self::Options => MAX_OPTIONS_SOURCE_BYTES,
        }
    }

    const fn maximum_records(self) -> u64 {
        match self {
            Self::NasdaqListed | Self::OtherListed => crate::parser::MAX_DIRECTORY_RECORDS as u64,
            Self::Bonds => MAX_BONDS_RECORDS,
            Self::Options => MAX_OPTIONS_RECORDS,
        }
    }

    const fn native_schema_name(self) -> &'static str {
        match self {
            Self::NasdaqListed => NASDAQ_LISTED_SCHEMA,
            Self::OtherListed => OTHER_LISTED_SCHEMA,
            Self::Bonds => BONDS_SCHEMA,
            Self::Options => OPTIONS_SCHEMA,
        }
    }

    const fn expected_header(self) -> &'static str {
        match self {
            Self::NasdaqListed => NASDAQ_LISTED_HEADER,
            Self::OtherListed => OTHER_LISTED_HEADER,
            Self::Bonds => BONDS_HEADER,
            Self::Options => OPTIONS_HEADER,
        }
    }

    const fn footer_delimiters(self) -> usize {
        match self {
            Self::NasdaqListed | Self::Options => 7,
            Self::OtherListed => 6,
            Self::Bonds => 5,
        }
    }
}

/// Why a provider row cannot itself establish a canonical security identity.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum NasdaqIdentityDisposition {
    /// The row is an exact provider-native reference candidate only.
    ProviderNativeReferenceOnly,
}

/// Provider-local doctor disposition when Nasdaq publishes no exact refresh interval.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum NasdaqReferenceCurrentnessDisposition {
    /// Integrity and endpoint reachability passed; application freshness policy must classify age.
    RequiresApplicationFreshnessClassification,
}

/// What the exact current-directory object establishes about a row's validity interval.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum NasdaqReferenceValidityDisposition {
    /// The row was present in one exact source snapshot; its effective start and end are unknown.
    ExactSourceSnapshotOnly,
}

/// Provider-supported symbol-lifecycle meaning available from one current directory object.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum NasdaqReferenceLifecycleDisposition {
    /// Presence is observed only in this snapshot; no rename, predecessor, or successor is inferred.
    CurrentDirectoryObservationOnly,
}

/// Execution or live-trading meaning available from a Nasdaq reference row.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum NasdaqReferenceTradabilityDisposition {
    /// The reference directory does not establish current tradability or execution eligibility.
    UnknownFromReferenceDirectory,
}

/// Whole-object parser completeness admitted by a restartable generation.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum NasdaqReferenceCompleteness {
    /// Every source row and the terminal footer passed the strict family schema with no rejection.
    StrictObjectComplete,
}

/// Nasdaq option close-processing code from `options.txt`.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub enum NasdaqOptionClosingType {
    /// `N` — normal close processing.
    #[serde(rename = "N")]
    Normal,
    /// `L` — late close processing.
    #[serde(rename = "L")]
    Late,
}

/// Exact decimal provider value with a normalized numeric coordinate and preserved raw text.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct NasdaqProviderDecimal {
    raw: String,
    coefficient: u64,
    scale: u8,
}

impl NasdaqProviderDecimal {
    /// Parses an exact nonnegative provider decimal without binary floating-point conversion.
    pub fn try_from_provider(value: &str) -> Result<Self, NasdaqReferenceError> {
        let (coefficient, scale) = parse_decimal_coordinate(value)?;
        Ok(Self {
            raw: value.to_owned(),
            coefficient,
            scale,
        })
    }

    /// Returns exact provider text.
    pub fn raw(&self) -> &str {
        &self.raw
    }

    /// Returns the normalized unsigned coefficient.
    pub const fn coefficient(&self) -> u64 {
        self.coefficient
    }

    /// Returns the normalized base-ten scale.
    pub const fn scale(&self) -> u8 {
        self.scale
    }
}

/// Shared exact-object provenance carried by every bond and option row.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct NasdaqReferenceProvenance {
    provider_row_number: u64,
    file_creation_time: NasdaqFileCreationTime,
    source_last_modified_at: Timestamp,
    first_observed_at: Timestamp,
    source_payload_evidence: ExactPayloadEvidence,
    quality: DataQuality,
    directory_presence: NasdaqDirectoryPresence,
    identity_disposition: NasdaqIdentityDisposition,
}

impl NasdaqReferenceProvenance {
    fn new(object: &NasdaqValidatedObject, provider_row_number: u64) -> Self {
        Self {
            provider_row_number,
            file_creation_time: object.file_creation_time.clone(),
            source_last_modified_at: object.sealed.source_last_modified_at(),
            first_observed_at: object.sealed.first_observed_at(),
            source_payload_evidence: object.sealed.payload_evidence.clone(),
            quality: DataQuality::OfficialDelayed,
            directory_presence: NasdaqDirectoryPresence::CurrentDirectory,
            identity_disposition: NasdaqIdentityDisposition::ProviderNativeReferenceOnly,
        }
    }

    /// Returns the one-based row coordinate in the exact source object.
    pub const fn provider_row_number(&self) -> u64 {
        self.provider_row_number
    }

    /// Returns the source file-creation coordinate without assigning a time zone.
    pub const fn file_creation_time(&self) -> &NasdaqFileCreationTime {
        &self.file_creation_time
    }

    /// Returns the HTTP `Last-Modified` clock.
    pub const fn source_last_modified_at(&self) -> Timestamp {
        self.source_last_modified_at
    }

    /// Returns the local first-observed clock.
    pub const fn first_observed_at(&self) -> Timestamp {
        self.first_observed_at
    }

    /// Returns exact whole-object content evidence.
    pub const fn source_payload_evidence(&self) -> &ExactPayloadEvidence {
        &self.source_payload_evidence
    }

    /// Returns the explicit non-canonical identity disposition.
    pub const fn identity_disposition(&self) -> NasdaqIdentityDisposition {
        self.identity_disposition
    }
}

/// Exact Nasdaq-listed bond reference candidate.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct NasdaqBondReferenceRecord {
    schema_version: u16,
    provider_symbol: ProviderInstrumentId,
    security_name: String,
    financial_status: NasdaqFinancialStatus,
    provenance: NasdaqReferenceProvenance,
}

impl NasdaqBondReferenceRecord {
    /// Returns the provider-native bond symbol; it is not a canonical instrument ID.
    pub const fn provider_symbol(&self) -> &ProviderInstrumentId {
        &self.provider_symbol
    }

    /// Returns the exact provider security name without parsing terms from it.
    pub fn security_name(&self) -> &str {
        &self.security_name
    }

    /// Returns Nasdaq's exact financial-status code.
    pub const fn financial_status(&self) -> NasdaqFinancialStatus {
        self.financial_status
    }

    /// Returns exact source-object and policy provenance.
    pub const fn provenance(&self) -> &NasdaqReferenceProvenance {
        &self.provenance
    }
}

/// Exact Nasdaq option-series reference candidate without an invented OCC/OSI identity.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct NasdaqOptionReferenceRecord {
    schema_version: u16,
    root_symbol: ProviderInstrumentId,
    closing_type: NasdaqOptionClosingType,
    option_kind: OptionKind,
    expiration_date: CalendarDate,
    explicit_strike_price: NasdaqProviderDecimal,
    underlying_symbol: ProviderInstrumentId,
    underlying_issue_name: String,
    pending: bool,
    provenance: NasdaqReferenceProvenance,
}

impl NasdaqOptionReferenceRecord {
    /// Returns Nasdaq's provider-native root symbol.
    pub const fn root_symbol(&self) -> &ProviderInstrumentId {
        &self.root_symbol
    }

    /// Returns Nasdaq's close-processing classification.
    pub const fn closing_type(&self) -> NasdaqOptionClosingType {
        self.closing_type
    }

    /// Returns call or put exactly as supplied by Nasdaq.
    pub const fn option_kind(&self) -> OptionKind {
        self.option_kind
    }

    /// Returns the full provider-reported expiration date.
    pub const fn expiration_date(&self) -> CalendarDate {
        self.expiration_date
    }

    /// Returns the exact strike text and normalized decimal coordinate.
    pub const fn explicit_strike_price(&self) -> &NasdaqProviderDecimal {
        &self.explicit_strike_price
    }

    /// Returns the source's underlying-symbol field without claiming an economic identity bridge.
    pub const fn underlying_symbol(&self) -> &ProviderInstrumentId {
        &self.underlying_symbol
    }

    /// Returns the exact provider issue-name field without using it as an identity bridge.
    pub fn underlying_issue_name(&self) -> &str {
        &self.underlying_issue_name
    }

    /// Returns whether Nasdaq marked the series pending.
    pub const fn pending(&self) -> bool {
        self.pending
    }

    /// Returns exact source-object and policy provenance.
    pub const fn provenance(&self) -> &NasdaqReferenceProvenance {
        &self.provenance
    }
}

/// Closed typed row emitted by the bounded reference page decoder.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case", tag = "family", content = "record")]
pub enum NasdaqReferenceRecord {
    /// Equity or ETF listing-reference candidate.
    Listing(NasdaqListingRecord),
    /// Bond provider-reference candidate.
    Bond(NasdaqBondReferenceRecord),
    /// Option-series provider-reference candidate.
    Option(NasdaqOptionReferenceRecord),
}

impl NasdaqReferenceRecord {
    /// Returns an exact row revision bound to family, row coordinate, and whole-object generation.
    pub fn revision(&self) -> Result<SourceIdentifier, NasdaqReferenceError> {
        let (family, row, evidence) = match self {
            Self::Listing(record) => (
                record.provider_fields().directory_kind(),
                u64::from(record.provider_row_number()),
                record.source_payload_evidence(),
            ),
            Self::Bond(record) => (
                NasdaqDirectoryKind::Bonds,
                record.provenance.provider_row_number,
                &record.provenance.source_payload_evidence,
            ),
            Self::Option(record) => (
                NasdaqDirectoryKind::Options,
                record.provenance.provider_row_number,
                &record.provenance.source_payload_evidence,
            ),
        };
        let mut digest = String::with_capacity(64);
        for byte in evidence.content_digest().bytes() {
            write!(&mut digest, "{byte:02x}").map_err(|_| NasdaqReferenceError::NameFormatting)?;
        }
        SourceIdentifier::try_from(format!(
            "nasdaq-reference:{}:row-{row}:{digest}",
            family.object_component()
        ))
        .map_err(|_| NasdaqReferenceError::InvalidSchema)
    }
}

/// Adapter-local identity handoff retaining provider listing, security, exchange, and symbol facts.
///
/// This candidate deliberately contains no root [`market_squawk_domain::InstrumentId`]. Canonical
/// selection, cross-source ambiguity resolution, and lifecycle publication remain root-owned.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct NasdaqReferenceIdentityCandidate {
    schema_version: u16,
    provider_record: NasdaqReferenceRecord,
    record_revision: SourceIdentifier,
    identity_disposition: NasdaqIdentityDisposition,
    validity_disposition: NasdaqReferenceValidityDisposition,
    lifecycle_disposition: NasdaqReferenceLifecycleDisposition,
    tradability_disposition: NasdaqReferenceTradabilityDisposition,
}

impl NasdaqReferenceIdentityCandidate {
    fn try_from_record(
        provider_record: NasdaqReferenceRecord,
    ) -> Result<Self, NasdaqReferenceError> {
        let record_revision = provider_record.revision()?;
        Ok(Self {
            schema_version: 1,
            provider_record,
            record_revision,
            identity_disposition: NasdaqIdentityDisposition::ProviderNativeReferenceOnly,
            validity_disposition: NasdaqReferenceValidityDisposition::ExactSourceSnapshotOnly,
            lifecycle_disposition:
                NasdaqReferenceLifecycleDisposition::CurrentDirectoryObservationOnly,
            tradability_disposition:
                NasdaqReferenceTradabilityDisposition::UnknownFromReferenceDirectory,
        })
    }

    /// Returns the exact provider-native row carrying listing/security/exchange/symbol facts.
    pub const fn provider_record(&self) -> &NasdaqReferenceRecord {
        &self.provider_record
    }

    /// Returns the family/row/raw-generation-bound provider record revision.
    pub const fn record_revision(&self) -> &SourceIdentifier {
        &self.record_revision
    }

    /// Returns the explicit requirement for root-owned canonical identity resolution.
    pub const fn identity_disposition(&self) -> NasdaqIdentityDisposition {
        self.identity_disposition
    }

    /// Returns the exact snapshot-only validity meaning.
    pub const fn validity_disposition(&self) -> NasdaqReferenceValidityDisposition {
        self.validity_disposition
    }

    /// Returns the provider-supported symbol-lifecycle meaning.
    pub const fn lifecycle_disposition(&self) -> NasdaqReferenceLifecycleDisposition {
        self.lifecycle_disposition
    }

    /// Returns the explicit unknown-tradability state.
    pub const fn tradability_disposition(&self) -> NasdaqReferenceTradabilityDisposition {
        self.tradability_disposition
    }
}

/// Exact durable HTTP response evidence retained alongside an archived body.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct NasdaqHttpResponseEvidence {
    status: u16,
    content_type: String,
    content_encoding: Option<String>,
    declared_content_length: Option<u64>,
    etag: Option<String>,
    transport_elapsed_nanos: u64,
    last_modified_at: Timestamp,
    received_at: Timestamp,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct NasdaqHttpResponseEvidenceWire {
    status: u16,
    content_type: String,
    content_encoding: Option<String>,
    declared_content_length: Option<u64>,
    etag: Option<String>,
    transport_elapsed_nanos: u64,
    last_modified_at: Timestamp,
    received_at: Timestamp,
}

impl<'de> Deserialize<'de> for NasdaqHttpResponseEvidence {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let wire = NasdaqHttpResponseEvidenceWire::deserialize(deserializer)?;
        Self::try_new(
            wire.status,
            wire.content_type,
            wire.content_encoding,
            wire.declared_content_length,
            wire.etag,
            wire.transport_elapsed_nanos,
            wire.last_modified_at,
            wire.received_at,
        )
        .map_err(serde::de::Error::custom)
    }
}

impl NasdaqHttpResponseEvidence {
    #[allow(
        clippy::too_many_arguments,
        reason = "each retained HTTP response coordinate remains explicit"
    )]
    pub(crate) fn try_new(
        status: u16,
        content_type: String,
        content_encoding: Option<String>,
        declared_content_length: Option<u64>,
        etag: Option<String>,
        transport_elapsed_nanos: u64,
        last_modified_at: Timestamp,
        received_at: Timestamp,
    ) -> Result<Self, NasdaqReferenceError> {
        let value = Self {
            status,
            content_type,
            content_encoding,
            declared_content_length,
            etag,
            transport_elapsed_nanos,
            last_modified_at,
            received_at,
        };
        value.validate()?;
        Ok(value)
    }

    fn validate(&self) -> Result<(), NasdaqReferenceError> {
        let media_type_is_text = self
            .content_type
            .split(';')
            .next()
            .is_some_and(|value| value.trim().eq_ignore_ascii_case("text/plain"));
        let valid_header = |value: &str, max: usize| {
            !value.is_empty()
                && value.len() <= max
                && value
                    .bytes()
                    .all(|byte| byte.is_ascii() && !byte.is_ascii_control())
        };
        if self.status != 200
            || !media_type_is_text
            || !valid_header(&self.content_type, 128)
            || self
                .content_encoding
                .as_deref()
                .is_some_and(|value| !value.eq_ignore_ascii_case("identity"))
            || self
                .content_encoding
                .as_deref()
                .is_some_and(|value| !valid_header(value, 64))
            || self.declared_content_length == Some(0)
            || self
                .etag
                .as_deref()
                .is_some_and(|value| !valid_header(value, 512))
            || self.transport_elapsed_nanos == 0
            || self.last_modified_at > self.received_at
        {
            Err(NasdaqReferenceError::InvalidHttpEvidence)
        } else {
            Ok(())
        }
    }

    /// Returns the exact successful HTTP status.
    pub const fn status(&self) -> u16 {
        self.status
    }

    /// Returns the exact admitted `Content-Type` header value.
    pub fn content_type(&self) -> &str {
        &self.content_type
    }

    /// Returns the exact declared body length when the provider supplied one.
    pub const fn declared_content_length(&self) -> Option<u64> {
        self.declared_content_length
    }

    /// Returns the exact provider ETag when supplied.
    pub fn etag(&self) -> Option<&str> {
        self.etag.as_deref()
    }

    /// Returns monotonic request-send through complete-body transport elapsed time.
    pub const fn transport_elapsed_nanos(&self) -> u64 {
        self.transport_elapsed_nanos
    }

    /// Returns the exact HTTP `Last-Modified` timestamp.
    pub const fn last_modified_at(&self) -> Timestamp {
        self.last_modified_at
    }

    /// Returns the socket-complete local observation timestamp.
    pub const fn received_at(&self) -> Timestamp {
        self.received_at
    }
}

/// Durable exact-object receipt. The content address is the generation identity.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct NasdaqSealedRawObject {
    schema_version: u16,
    family: NasdaqDirectoryKind,
    locator: SourceIdentifier,
    payload_evidence: ExactPayloadEvidence,
    payload_bytes: u64,
    response_evidence: NasdaqHttpResponseEvidence,
}

impl NasdaqSealedRawObject {
    pub(crate) fn try_new(
        family: NasdaqDirectoryKind,
        locator: &str,
        content_digest: EvidenceDigest,
        payload_bytes: u64,
        response_evidence: NasdaqHttpResponseEvidence,
    ) -> Result<Self, NasdaqReferenceError> {
        response_evidence.validate()?;
        if content_digest.algorithm() != DigestAlgorithm::Sha256
            || content_digest.bytes() == [0; 32]
            || payload_bytes == 0
            || payload_bytes > family.maximum_source_bytes() as u64
            || response_evidence
                .declared_content_length
                .is_some_and(|declared| declared != payload_bytes)
        {
            return Err(NasdaqReferenceError::InvalidObjectReceipt);
        }
        Ok(Self {
            schema_version: 1,
            family,
            locator: SourceIdentifier::try_from(locator)
                .map_err(|_| NasdaqReferenceError::InvalidObjectReceipt)?,
            payload_evidence: ExactPayloadEvidence::from_content_digest(content_digest),
            payload_bytes,
            response_evidence,
        })
    }

    /// Revalidates a serialized durable receipt before reopening its content address.
    pub fn from_json(payload: &[u8]) -> Result<Self, NasdaqReferenceError> {
        let wire: NasdaqSealedRawObjectWire = serde_json::from_slice(payload)
            .map_err(|_| NasdaqReferenceError::InvalidObjectReceipt)?;
        let rebuilt = Self::try_new(
            wire.family,
            wire.locator.as_str(),
            wire.payload_evidence.content_digest(),
            wire.payload_bytes,
            wire.response_evidence,
        )?;
        if wire.schema_version != rebuilt.schema_version
            || wire.payload_evidence != rebuilt.payload_evidence
        {
            return Err(NasdaqReferenceError::InvalidObjectReceipt);
        }
        Ok(rebuilt)
    }

    /// Returns the provider file family.
    pub const fn family(&self) -> NasdaqDirectoryKind {
        self.family
    }

    /// Returns the exact official locator.
    pub const fn locator(&self) -> &SourceIdentifier {
        &self.locator
    }

    /// Returns whole-object content evidence and generation identity.
    pub const fn payload_evidence(&self) -> &ExactPayloadEvidence {
        &self.payload_evidence
    }

    /// Returns the exact persisted body size.
    pub const fn payload_bytes(&self) -> u64 {
        self.payload_bytes
    }

    /// Returns the response's exact HTTP `Last-Modified` timestamp.
    pub const fn source_last_modified_at(&self) -> Timestamp {
        self.response_evidence.last_modified_at
    }

    /// Returns when the exact response was first observed locally.
    pub const fn first_observed_at(&self) -> Timestamp {
        self.response_evidence.received_at
    }

    /// Returns durable status, media, declared-length, validator, and response-clock evidence.
    pub const fn response_evidence(&self) -> &NasdaqHttpResponseEvidence {
        &self.response_evidence
    }

    /// Returns monotonic request-send through complete-body latency for the successful probe.
    pub const fn transport_elapsed_nanos(&self) -> u64 {
        self.response_evidence.transport_elapsed_nanos
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct NasdaqSealedRawObjectWire {
    schema_version: u16,
    family: NasdaqDirectoryKind,
    locator: SourceIdentifier,
    payload_evidence: ExactPayloadEvidence,
    payload_bytes: u64,
    response_evidence: NasdaqHttpResponseEvidence,
}

/// Whole-object schema/footer validation receipt used for restart-safe paging.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct NasdaqValidatedObject {
    sealed: NasdaqSealedRawObject,
    native_schema: SourceIdentifier,
    file_creation_time: NasdaqFileCreationTime,
    record_count: u64,
    first_data_offset: u64,
    index_file_name: SourceIdentifier,
    index_entry_count: u64,
    index_content_digest: EvidenceDigest,
    #[serde(skip_serializing)]
    query_index: Arc<Vec<ReferenceIndexEntry>>,
    #[serde(skip_serializing)]
    row_offsets: Arc<Vec<u64>>,
    #[serde(skip_serializing)]
    content_chunk_digests: Arc<Vec<[u8; 32]>>,
}

/// Exact raw-object, parser, clock, schema, and index binding for one adapter generation.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct NasdaqReferenceGenerationEvidence {
    schema_version: u16,
    family: NasdaqDirectoryKind,
    generation_identity: EvidenceDigest,
    evidence_binding_digest: EvidenceDigest,
    raw_content_digest: EvidenceDigest,
    raw_payload_bytes: u64,
    native_schema: SourceIdentifier,
    parsed_records: u64,
    rejected_records: u64,
    completeness: NasdaqReferenceCompleteness,
    file_creation_time: NasdaqFileCreationTime,
    source_last_modified_at: Timestamp,
    first_observed_at: Timestamp,
    response_evidence: NasdaqHttpResponseEvidence,
    index_file_name: SourceIdentifier,
    index_content_digest: EvidenceDigest,
}

impl NasdaqReferenceGenerationEvidence {
    fn try_from_object(object: &NasdaqValidatedObject) -> Result<Self, NasdaqReferenceError> {
        if object.record_count == 0
            || object.index_entry_count != object.record_count
            || object.index_content_digest.algorithm() != DigestAlgorithm::Sha256
            || object.index_content_digest.bytes() == [0; 32]
            || u64::try_from(object.query_index.len())
                .map_or(true, |count| count != object.record_count)
            || u64::try_from(object.row_offsets.len())
                .map_or(true, |count| count != object.record_count)
        {
            return Err(NasdaqReferenceError::InvalidGenerationEvidence);
        }
        let mut value = Self {
            schema_version: 1,
            family: object.sealed.family,
            generation_identity: object.sealed.payload_evidence.content_digest(),
            evidence_binding_digest: EvidenceDigest::new(DigestAlgorithm::Sha256, [0; 32]),
            raw_content_digest: object.sealed.payload_evidence.content_digest(),
            raw_payload_bytes: object.sealed.payload_bytes,
            native_schema: object.native_schema.clone(),
            parsed_records: object.record_count,
            rejected_records: 0,
            completeness: NasdaqReferenceCompleteness::StrictObjectComplete,
            file_creation_time: object.file_creation_time.clone(),
            source_last_modified_at: object.sealed.source_last_modified_at(),
            first_observed_at: object.sealed.first_observed_at(),
            response_evidence: object.sealed.response_evidence.clone(),
            index_file_name: object.index_file_name.clone(),
            index_content_digest: object.index_content_digest,
        };
        value.evidence_binding_digest = value.expected_evidence_binding();
        if value.generation_identity.algorithm() != DigestAlgorithm::Sha256
            || value.generation_identity.bytes() == [0; 32]
            || value.evidence_binding_digest.bytes() == [0; 32]
        {
            return Err(NasdaqReferenceError::InvalidGenerationEvidence);
        }
        Ok(value)
    }

    fn expected_evidence_binding(&self) -> EvidenceDigest {
        let mut hash = Sha256::new();
        hash.update(b"market-squawk/nasdaq-reference-generation/v1");
        hash_generation_field(&mut hash, self.family.object_component().as_bytes());
        hash.update(self.raw_content_digest.bytes());
        hash.update(self.raw_payload_bytes.to_be_bytes());
        hash_generation_field(&mut hash, self.native_schema.as_str().as_bytes());
        hash.update(self.parsed_records.to_be_bytes());
        hash.update(self.rejected_records.to_be_bytes());
        hash.update([1]);
        hash_generation_field(&mut hash, self.file_creation_time.raw().as_bytes());
        hash.update(self.source_last_modified_at.unix_nanos().to_be_bytes());
        hash.update(self.first_observed_at.unix_nanos().to_be_bytes());
        hash.update(self.response_evidence.status.to_be_bytes());
        hash_generation_field(&mut hash, self.response_evidence.content_type.as_bytes());
        hash_optional_generation_field(
            &mut hash,
            self.response_evidence.content_encoding.as_deref(),
        );
        match self.response_evidence.declared_content_length {
            Some(value) => {
                hash.update([1]);
                hash.update(value.to_be_bytes());
            }
            None => hash.update([0]),
        }
        hash_optional_generation_field(&mut hash, self.response_evidence.etag.as_deref());
        hash.update(self.response_evidence.transport_elapsed_nanos.to_be_bytes());
        hash_generation_field(&mut hash, self.index_file_name.as_str().as_bytes());
        hash.update(self.index_content_digest.bytes());
        EvidenceDigest::new(DigestAlgorithm::Sha256, hash.finalize().into())
    }

    /// Returns the independently fetched official file family.
    pub const fn family(&self) -> NasdaqDirectoryKind {
        self.family
    }

    /// Returns the content-addressed immutable raw-object generation identity.
    pub const fn generation_identity(&self) -> EvidenceDigest {
        self.generation_identity
    }

    /// Returns the complete handoff binding over raw, parse, clock, response, and index facts.
    pub const fn evidence_binding_digest(&self) -> EvidenceDigest {
        self.evidence_binding_digest
    }

    /// Returns the exact immutable provider-local raw-object digest.
    pub const fn raw_content_digest(&self) -> EvidenceDigest {
        self.raw_content_digest
    }

    /// Returns the exact immutable provider-local raw-object byte count.
    pub const fn raw_payload_bytes(&self) -> u64 {
        self.raw_payload_bytes
    }

    /// Returns the closed provider-native schema identity.
    pub const fn native_schema(&self) -> &SourceIdentifier {
        &self.native_schema
    }

    /// Returns source rows admitted by the strict whole-object parser.
    pub const fn parsed_records(&self) -> u64 {
        self.parsed_records
    }

    /// Returns source rows rejected inside this admitted generation.
    ///
    /// This is always zero because any invalid row rejects the complete object.
    pub const fn rejected_records(&self) -> u64 {
        self.rejected_records
    }

    /// Returns strict whole-object completeness rather than inferred partial success.
    pub const fn completeness(&self) -> NasdaqReferenceCompleteness {
        self.completeness
    }

    /// Returns the provider file-creation coordinate without assigning a time zone.
    pub const fn file_creation_time(&self) -> &NasdaqFileCreationTime {
        &self.file_creation_time
    }

    /// Returns the exact HTTP `Last-Modified` clock.
    pub const fn source_last_modified_at(&self) -> Timestamp {
        self.source_last_modified_at
    }

    /// Returns the local socket-complete observation clock.
    pub const fn first_observed_at(&self) -> Timestamp {
        self.first_observed_at
    }

    /// Returns exact status, media, length, validator, latency, and clock evidence.
    pub const fn response_evidence(&self) -> &NasdaqHttpResponseEvidence {
        &self.response_evidence
    }

    /// Returns the immutable generation/schema-bound provider-key index object.
    pub const fn index_file_name(&self) -> &SourceIdentifier {
        &self.index_file_name
    }

    /// Returns the verified provider-key index content digest.
    pub const fn index_content_digest(&self) -> EvidenceDigest {
        self.index_content_digest
    }
}

/// Fresh integrity doctor for one exact persisted generation.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct NasdaqReferenceDoctorReport {
    generation: NasdaqReferenceGenerationEvidence,
    currentness: NasdaqReferenceCurrentnessDisposition,
    archive_reopened_and_revalidated: bool,
    provider_key_index_reconciled: bool,
}

impl NasdaqReferenceDoctorReport {
    /// Returns the exact raw/parser/clock/index generation evidence.
    pub const fn generation(&self) -> &NasdaqReferenceGenerationEvidence {
        &self.generation
    }

    /// Returns the exact generation identity.
    pub const fn generation_identity(&self) -> EvidenceDigest {
        self.generation.generation_identity
    }

    /// Returns the verified family.
    pub const fn family(&self) -> NasdaqDirectoryKind {
        self.generation.family
    }

    /// Returns the fully reconciled record count.
    pub const fn record_count(&self) -> u64 {
        self.generation.parsed_records
    }

    /// Returns the exact source-object content identity.
    pub const fn content_digest(&self) -> EvidenceDigest {
        self.generation.raw_content_digest
    }

    /// Returns the exact verified source-object byte count.
    pub const fn payload_bytes(&self) -> u64 {
        self.generation.raw_payload_bytes
    }

    /// Returns the code-owned provider-native schema identity.
    pub const fn native_schema(&self) -> &SourceIdentifier {
        &self.generation.native_schema
    }

    /// Returns the provider file-creation coordinate without assigning a time zone.
    pub const fn file_creation_time(&self) -> &NasdaqFileCreationTime {
        &self.generation.file_creation_time
    }

    /// Returns the exact HTTP `Last-Modified` clock.
    pub const fn source_last_modified_at(&self) -> Timestamp {
        self.generation.source_last_modified_at
    }

    /// Returns the local socket-complete observation clock.
    pub const fn first_observed_at(&self) -> Timestamp {
        self.generation.first_observed_at
    }

    /// Returns the explicit root-owned currentness-classification requirement.
    pub const fn currentness(&self) -> NasdaqReferenceCurrentnessDisposition {
        self.currentness
    }

    /// Returns the immutable generation/schema-bound provider-key index object.
    pub const fn index_file_name(&self) -> &SourceIdentifier {
        &self.generation.index_file_name
    }

    /// Returns whether content, size, schema, footer, and clocks were freshly revalidated.
    pub const fn archive_reopened_and_revalidated(&self) -> bool {
        self.archive_reopened_and_revalidated
    }

    /// Returns whether the durable provider-key index exactly reconciled to the source rows.
    pub const fn provider_key_index_reconciled(&self) -> bool {
        self.provider_key_index_reconciled
    }

    /// Returns the verified fixed-width index content identity.
    pub const fn index_content_digest(&self) -> EvidenceDigest {
        self.generation.index_content_digest
    }

    /// Returns durable exact HTTP status/media/length/validator/clock evidence.
    pub const fn response_evidence(&self) -> &NasdaqHttpResponseEvidence {
        &self.generation.response_evidence
    }

    /// Returns monotonic request-send through complete-body latency for the successful probe.
    pub const fn transport_elapsed_nanos(&self) -> u64 {
        self.generation.response_evidence.transport_elapsed_nanos
    }
}

impl NasdaqValidatedObject {
    /// Returns the durable object receipt.
    pub const fn sealed(&self) -> &NasdaqSealedRawObject {
        &self.sealed
    }

    /// Returns the exact provider-native decoder schema.
    pub const fn native_schema(&self) -> &SourceIdentifier {
        &self.native_schema
    }

    /// Returns the provider file-creation coordinate without inventing a time zone.
    pub const fn file_creation_time(&self) -> &NasdaqFileCreationTime {
        &self.file_creation_time
    }

    /// Returns the fully validated data-row count.
    pub const fn record_count(&self) -> u64 {
        self.record_count
    }

    /// Returns the immutable generation/schema-bound provider-key index object name.
    pub const fn index_file_name(&self) -> &SourceIdentifier {
        &self.index_file_name
    }

    /// Returns the exact verified durable provider-key index content identity.
    pub const fn index_content_digest(&self) -> EvidenceDigest {
        self.index_content_digest
    }

    /// Builds the adapter-local canonical-identity handoff binding for this exact generation.
    pub fn generation_evidence(
        &self,
    ) -> Result<NasdaqReferenceGenerationEvidence, NasdaqReferenceError> {
        NasdaqReferenceGenerationEvidence::try_from_object(self)
    }

    /// Starts a bounded typed decode at the first provider row.
    pub fn first_page(
        &self,
        max_records: NonZeroU32,
    ) -> Result<NasdaqReferencePageRequest, NasdaqReferenceError> {
        NasdaqReferencePageRequest::first(self, max_records)
    }

    /// Resumes only a digest-bound cursor whose row/offset coordinate reconciles to this object.
    pub fn resume_page(
        &self,
        cursor: &NasdaqReferencePageCursor,
        max_records: NonZeroU32,
    ) -> Result<NasdaqReferencePageRequest, NasdaqReferenceError> {
        cursor.validate()?;
        if cursor.family != self.sealed.family
            || cursor.content_digest != self.sealed.payload_evidence.content_digest()
            || cursor.native_schema != self.native_schema
            || cursor.next_row_number < 2
            || cursor.next_byte_offset < self.first_data_offset
            || cursor.next_byte_offset >= self.sealed.payload_bytes
            || cursor
                .next_row_number
                .checked_sub(2)
                .and_then(|ordinal| usize::try_from(ordinal).ok())
                .and_then(|ordinal| self.row_offsets.get(ordinal))
                != Some(&cursor.next_byte_offset)
        {
            return Err(NasdaqReferenceError::CrossGenerationCursor);
        }
        NasdaqReferencePageRequest::try_new(
            self,
            cursor.next_row_number,
            cursor.next_byte_offset,
            max_records,
            Some(cursor.binding_digest),
        )
    }
}

/// Opaque restart cursor bound to one exact object generation and native schema.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct NasdaqReferencePageCursor {
    schema_version: u16,
    family: NasdaqDirectoryKind,
    content_digest: EvidenceDigest,
    native_schema: SourceIdentifier,
    next_row_number: u64,
    next_byte_offset: u64,
    binding_digest: EvidenceDigest,
}

impl NasdaqReferencePageCursor {
    fn try_new(
        object: &NasdaqValidatedObject,
        next_row_number: u64,
        next_byte_offset: u64,
    ) -> Result<Self, NasdaqReferenceError> {
        let mut value = Self {
            schema_version: 1,
            family: object.sealed.family,
            content_digest: object.sealed.payload_evidence.content_digest(),
            native_schema: object.native_schema.clone(),
            next_row_number,
            next_byte_offset,
            binding_digest: EvidenceDigest::new(DigestAlgorithm::Sha256, [0; 32]),
        };
        value.binding_digest = value.expected_binding();
        value.validate()?;
        Ok(value)
    }

    /// Revalidates an untrusted serialized cursor.
    pub fn from_json(payload: &[u8]) -> Result<Self, NasdaqReferenceError> {
        let wire: NasdaqReferencePageCursorWire =
            serde_json::from_slice(payload).map_err(|_| NasdaqReferenceError::InvalidCursor)?;
        let value = Self {
            schema_version: wire.schema_version,
            family: wire.family,
            content_digest: wire.content_digest,
            native_schema: wire.native_schema,
            next_row_number: wire.next_row_number,
            next_byte_offset: wire.next_byte_offset,
            binding_digest: wire.binding_digest,
        };
        value.validate()?;
        Ok(value)
    }

    fn validate(&self) -> Result<(), NasdaqReferenceError> {
        if self.schema_version != 1
            || self.content_digest.algorithm() != DigestAlgorithm::Sha256
            || self.content_digest.bytes() == [0; 32]
            || self.binding_digest != self.expected_binding()
            || self.next_row_number < 2
        {
            return Err(NasdaqReferenceError::InvalidCursor);
        }
        Ok(())
    }

    fn expected_binding(&self) -> EvidenceDigest {
        let mut hash = Sha256::new();
        hash.update(b"market-squawk/nasdaq-reference-page-cursor/v1");
        hash.update((self.family.object_component().len() as u64).to_be_bytes());
        hash.update(self.family.object_component().as_bytes());
        hash.update(self.content_digest.bytes());
        hash.update((self.native_schema.as_str().len() as u64).to_be_bytes());
        hash.update(self.native_schema.as_str().as_bytes());
        hash.update(self.next_row_number.to_be_bytes());
        hash.update(self.next_byte_offset.to_be_bytes());
        EvidenceDigest::new(DigestAlgorithm::Sha256, hash.finalize().into())
    }

    /// Returns the next one-based provider row number.
    pub const fn next_row_number(&self) -> u64 {
        self.next_row_number
    }

    /// Returns the exact raw-object generation to which this cursor belongs.
    pub const fn content_digest(&self) -> EvidenceDigest {
        self.content_digest
    }

    /// Returns the code-owned provider-native schema to which this cursor belongs.
    pub const fn native_schema(&self) -> &SourceIdentifier {
        &self.native_schema
    }

    /// Returns the exact row-start byte offset inside the immutable raw object.
    pub const fn next_byte_offset(&self) -> u64 {
        self.next_byte_offset
    }

    /// Returns the cursor binding identity over generation, schema, row, and byte offset.
    pub const fn cursor_identity(&self) -> EvidenceDigest {
        self.binding_digest
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct NasdaqReferencePageCursorWire {
    schema_version: u16,
    family: NasdaqDirectoryKind,
    content_digest: EvidenceDigest,
    native_schema: SourceIdentifier,
    next_row_number: u64,
    next_byte_offset: u64,
    binding_digest: EvidenceDigest,
}

/// Exact bounded page request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NasdaqReferencePageRequest {
    family: NasdaqDirectoryKind,
    content_digest: EvidenceDigest,
    native_schema: SourceIdentifier,
    first_row_number: u64,
    byte_offset: u64,
    max_records: NonZeroU32,
    cursor_identity: Option<EvidenceDigest>,
}

impl NasdaqReferencePageRequest {
    fn first(
        object: &NasdaqValidatedObject,
        max_records: NonZeroU32,
    ) -> Result<Self, NasdaqReferenceError> {
        Self::try_new(object, 2, object.first_data_offset, max_records, None)
    }

    fn try_new(
        object: &NasdaqValidatedObject,
        first_row_number: u64,
        byte_offset: u64,
        max_records: NonZeroU32,
        cursor_identity: Option<EvidenceDigest>,
    ) -> Result<Self, NasdaqReferenceError> {
        if max_records.get() > MAX_REFERENCE_PAGE_RECORDS {
            return Err(NasdaqReferenceError::PageLimitExceeded);
        }
        Ok(Self {
            family: object.sealed.family,
            content_digest: object.sealed.payload_evidence.content_digest(),
            native_schema: object.native_schema.clone(),
            first_row_number,
            byte_offset,
            max_records,
            cursor_identity,
        })
    }
}

/// Bounded typed page and exact continuation state.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct NasdaqReferencePage {
    generation: NasdaqReferenceGenerationEvidence,
    first_row_number: u64,
    request_cursor_identity: Option<EvidenceDigest>,
    records: Vec<NasdaqReferenceIdentityCandidate>,
    next_cursor: Option<NasdaqReferencePageCursor>,
}

impl NasdaqReferencePage {
    /// Returns the exact raw/parser/clock/index generation evidence for every returned candidate.
    pub const fn generation(&self) -> &NasdaqReferenceGenerationEvidence {
        &self.generation
    }

    /// Returns the exact provider family.
    pub const fn family(&self) -> NasdaqDirectoryKind {
        self.generation.family
    }

    /// Returns the whole-object generation identity.
    pub const fn content_digest(&self) -> EvidenceDigest {
        self.generation.raw_content_digest
    }

    /// Returns the one-based provider row number of the first returned record.
    pub const fn first_row_number(&self) -> u64 {
        self.first_row_number
    }

    /// Returns bounded provider-native records in exact source order.
    pub fn records(&self) -> &[NasdaqReferenceIdentityCandidate] {
        &self.records
    }

    /// Returns the exact cursor identity that produced this page, or `None` for the first page.
    pub const fn request_cursor_identity(&self) -> Option<EvidenceDigest> {
        self.request_cursor_identity
    }

    /// Returns a generation-bound continuation, or `None` after the validated footer.
    pub const fn next_cursor(&self) -> Option<&NasdaqReferencePageCursor> {
        self.next_cursor.as_ref()
    }
}

/// Exact provider-namespace query; names are deliberately not admitted as keys.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum NasdaqReferenceQuery {
    /// One provider symbol inside one exact equity directory namespace.
    Listing {
        /// Exact directory namespace.
        directory: NasdaqDirectoryKind,
        /// Provider-native symbol.
        provider_symbol: ProviderInstrumentId,
    },
    /// One provider-native bond symbol.
    Bond {
        /// Provider-native symbol.
        provider_symbol: ProviderInstrumentId,
    },
    /// One exact Nasdaq option-series tuple. This is not an OCC/OSI identity.
    Option {
        /// Nasdaq root-symbol field.
        root_symbol: ProviderInstrumentId,
        /// Provider-reported call or put.
        option_kind: OptionKind,
        /// Provider-reported full expiration date.
        expiration_date: CalendarDate,
        /// Exact normalized provider strike.
        explicit_strike_price: NasdaqProviderDecimal,
    },
}

impl NasdaqReferenceQuery {
    /// Constructs an equity-directory provider-symbol query.
    pub fn listing(
        directory: NasdaqDirectoryKind,
        provider_symbol: &str,
    ) -> Result<Self, NasdaqReferenceError> {
        if !NasdaqDirectoryKind::EQUITY_DIRECTORIES.contains(&directory) {
            return Err(NasdaqReferenceError::InvalidQuery);
        }
        Ok(Self::Listing {
            directory,
            provider_symbol: ProviderInstrumentId::try_from(provider_symbol)
                .map_err(|_| NasdaqReferenceError::InvalidQuery)?,
        })
    }

    /// Constructs a bond provider-symbol query.
    pub fn bond(provider_symbol: &str) -> Result<Self, NasdaqReferenceError> {
        Ok(Self::Bond {
            provider_symbol: ProviderInstrumentId::try_from(provider_symbol)
                .map_err(|_| NasdaqReferenceError::InvalidQuery)?,
        })
    }

    /// Constructs a provider-native option tuple query without minting OCC/OSI identity.
    pub fn option(
        root_symbol: &str,
        option_kind: OptionKind,
        expiration_date: CalendarDate,
        explicit_strike_price: &str,
    ) -> Result<Self, NasdaqReferenceError> {
        Ok(Self::Option {
            root_symbol: ProviderInstrumentId::try_from(root_symbol)
                .map_err(|_| NasdaqReferenceError::InvalidQuery)?,
            option_kind,
            expiration_date,
            explicit_strike_price: NasdaqProviderDecimal::try_from_provider(explicit_strike_price)?,
        })
    }

    fn family(&self) -> NasdaqDirectoryKind {
        match self {
            Self::Listing { directory, .. } => *directory,
            Self::Bond { .. } => NasdaqDirectoryKind::Bonds,
            Self::Option { .. } => NasdaqDirectoryKind::Options,
        }
    }

    fn matches(&self, candidate: &NasdaqReferenceIdentityCandidate) -> bool {
        let record = candidate.provider_record();
        match (self, record) {
            (
                Self::Listing {
                    directory,
                    provider_symbol,
                },
                NasdaqReferenceRecord::Listing(record),
            ) => {
                record.provider_fields().directory_kind() == *directory
                    && record.primary_symbol() == provider_symbol
            }
            (Self::Bond { provider_symbol }, NasdaqReferenceRecord::Bond(record)) => {
                record.provider_symbol() == provider_symbol
            }
            (
                Self::Option {
                    root_symbol,
                    option_kind,
                    expiration_date,
                    explicit_strike_price,
                },
                NasdaqReferenceRecord::Option(record),
            ) => {
                record.root_symbol() == root_symbol
                    && record.option_kind() == *option_kind
                    && record.expiration_date() == *expiration_date
                    && record.explicit_strike_price().coefficient()
                        == explicit_strike_price.coefficient()
                    && record.explicit_strike_price().scale() == explicit_strike_price.scale()
            }
            _ => false,
        }
    }

    fn key_digest(&self) -> [u8; 32] {
        match self {
            Self::Listing {
                directory,
                provider_symbol,
            } => symbol_query_key(*directory, provider_symbol.as_str()),
            Self::Bond { provider_symbol } => {
                symbol_query_key(NasdaqDirectoryKind::Bonds, provider_symbol.as_str())
            }
            Self::Option {
                root_symbol,
                option_kind,
                expiration_date,
                explicit_strike_price,
            } => option_query_key(
                root_symbol.as_str(),
                *option_kind,
                *expiration_date,
                explicit_strike_price.coefficient(),
                explicit_strike_price.scale(),
            ),
        }
    }
}

/// Exact/ambiguous/missing provider-key outcome.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum NasdaqReferenceQueryDisposition {
    /// No row in the exact generation matched the provider key.
    Missing,
    /// Exactly one row matched the provider key.
    Exact,
    /// Multiple rows matched; consumers must not select one arbitrarily.
    Ambiguous,
}

/// Bounded provider-key query result retaining every conflicting candidate.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct NasdaqReferenceQueryResult {
    generation: NasdaqReferenceGenerationEvidence,
    disposition: NasdaqReferenceQueryDisposition,
    matches: Vec<NasdaqReferenceIdentityCandidate>,
}

impl NasdaqReferenceQueryResult {
    /// Returns the exact raw/parser/clock/index generation searched by this result.
    pub const fn generation(&self) -> &NasdaqReferenceGenerationEvidence {
        &self.generation
    }

    /// Returns the exact generation searched by this result.
    pub const fn content_digest(&self) -> EvidenceDigest {
        self.generation.raw_content_digest
    }

    /// Returns exact/ambiguous/missing disposition.
    pub const fn disposition(&self) -> NasdaqReferenceQueryDisposition {
        self.disposition
    }

    /// Returns every exact provider candidate retained for the key.
    pub fn matches(&self) -> &[NasdaqReferenceIdentityCandidate] {
        &self.matches
    }
}

/// Capability-scoped immutable content-addressed store for large Nasdaq reference objects.
#[derive(Debug)]
pub struct NasdaqRawObjectStore {
    directory: Dir,
}

impl NasdaqRawObjectStore {
    /// Adopts an already-opened directory capability.
    ///
    /// The store never resolves an ambient path. Callers choose and authorize the root once, then
    /// pass the opened capability into this constructor.
    pub fn try_new(directory: Dir) -> Result<Self, NasdaqReferenceError> {
        let metadata = directory.metadata(".")?;
        if !metadata.is_dir() || !directory.symlink_metadata(".")?.is_dir() {
            return Err(NasdaqReferenceError::UnsafeArchiveRoot);
        }
        Ok(Self { directory })
    }

    /// Verifies private staging creation, file durability, cleanup, and directory durability.
    pub fn activation_check(&self) -> Result<(), NasdaqReferenceError> {
        let name = format!(".nasdaq-activation-{}.tmp", Uuid::new_v4());
        let mut options = OpenOptions::new();
        options.read(true).write(true).create_new(true);
        options.follow(FollowSymlinks::No);
        configure_private_creation(&mut options);
        let mut file = self.directory.open_with(&name, &options)?.into_std();
        use std::io::Write as _;
        file.write_all(b"market-squawk/nasdaq-archive-activation/v1")?;
        file.sync_all()?;
        drop(file);
        self.directory.remove_file(&name)?;
        sync_publication_directory(&self.directory)?;
        Ok(())
    }

    pub(crate) fn begin(
        &self,
        family: NasdaqDirectoryKind,
        cancellation: &CancellationToken,
    ) -> Result<NasdaqRawObjectWriter, NasdaqReferenceError> {
        check_cancelled(cancellation)?;
        let staging_name = format!(".nasdaq-reference-{}.tmp", Uuid::new_v4());
        let mut options = OpenOptions::new();
        options.read(true).write(true).create_new(true);
        options.follow(FollowSymlinks::No);
        configure_private_creation(&mut options);
        let file = self
            .directory
            .open_with(&staging_name, &options)?
            .into_std();
        Ok(NasdaqRawObjectWriter {
            directory: self.directory.try_clone()?,
            staging_name,
            file: Some(tokio::fs::File::from_std(file)),
            family,
            bytes_written: 0,
            hash: Sha256::new(),
        })
    }

    /// Reopens and fully revalidates an immutable object after acquisition or process restart.
    pub async fn recover(
        &self,
        sealed: &NasdaqSealedRawObject,
        cancellation: &CancellationToken,
    ) -> Result<NasdaqValidatedObject, NasdaqReferenceError> {
        check_cancelled(cancellation)?;
        let expected_locator = crate::source::directory_locator(sealed.family);
        if sealed.locator.as_str() != expected_locator
            || sealed.payload_bytes > sealed.family.maximum_source_bytes() as u64
        {
            return Err(NasdaqReferenceError::InvalidObjectReceipt);
        }
        let mut reader = self.open_verified_file(sealed)?;
        let scan = scan_and_validate(
            &mut reader,
            sealed.family,
            sealed.payload_bytes,
            sealed.payload_evidence.content_digest(),
            cancellation,
        )
        .await?;
        validate_footer_clock(&scan.file_creation_time, sealed.source_last_modified_at())?;
        let native_schema = SourceIdentifier::try_from(sealed.family.native_schema_name())
            .map_err(|_| NasdaqReferenceError::InvalidSchema)?;
        let (index_file_name, index_content_digest) = self
            .ensure_index(sealed, &native_schema, &scan.index_entries, cancellation)
            .await?;
        let index_entry_count = u64::try_from(scan.index_entries.len())
            .map_err(|_| NasdaqReferenceError::IndexLimitExceeded)?;
        Ok(NasdaqValidatedObject {
            sealed: sealed.clone(),
            native_schema,
            file_creation_time: scan.file_creation_time,
            record_count: scan.record_count,
            first_data_offset: scan.first_data_offset,
            index_file_name,
            index_entry_count,
            index_content_digest,
            query_index: Arc::new(scan.index_entries),
            row_offsets: Arc::new(scan.row_offsets),
            content_chunk_digests: Arc::new(scan.content_chunk_digests),
        })
    }

    /// Runs a fresh restart-equivalent integrity doctor without making a freshness-cadence claim.
    pub async fn doctor(
        &self,
        sealed: &NasdaqSealedRawObject,
        cancellation: &CancellationToken,
    ) -> Result<NasdaqReferenceDoctorReport, NasdaqReferenceError> {
        let object = self.recover(sealed, cancellation).await?;
        self.validated_report(&object)
    }

    /// Builds a doctor receipt from the just-fetched, whole-object-validated generation.
    pub fn validated_report(
        &self,
        object: &NasdaqValidatedObject,
    ) -> Result<NasdaqReferenceDoctorReport, NasdaqReferenceError> {
        if object.index_entry_count != object.record_count
            || object.index_content_digest.algorithm() != DigestAlgorithm::Sha256
            || u64::try_from(object.query_index.len())
                .map_or(true, |count| count != object.index_entry_count)
            || u64::try_from(object.row_offsets.len())
                .map_or(true, |count| count != object.record_count)
        {
            return Err(NasdaqReferenceError::IndexVerificationFailed);
        }
        if object.content_chunk_digests.len() != content_chunk_count(object.sealed.payload_bytes)? {
            return Err(NasdaqReferenceError::ArchiveVerificationFailed);
        }
        Ok(NasdaqReferenceDoctorReport {
            generation: object.generation_evidence()?,
            currentness:
                NasdaqReferenceCurrentnessDisposition::RequiresApplicationFreshnessClassification,
            archive_reopened_and_revalidated: true,
            provider_key_index_reconciled: true,
        })
    }

    async fn ensure_index(
        &self,
        sealed: &NasdaqSealedRawObject,
        native_schema: &SourceIdentifier,
        entries: &[ReferenceIndexEntry],
        cancellation: &CancellationToken,
    ) -> Result<(SourceIdentifier, EvidenceDigest), NasdaqReferenceError> {
        let name = index_name(sealed, native_schema)?;
        match self
            .verify_index(sealed, native_schema, &name, entries, cancellation)
            .await
        {
            Ok(digest) => {
                let name = SourceIdentifier::try_from(name)
                    .map_err(|_| NasdaqReferenceError::InvalidSchema)?;
                return Ok((name, digest));
            }
            Err(NasdaqReferenceError::Io(error)) if error.kind() == ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
        let staging_name = format!(".nasdaq-index-{}.tmp", Uuid::new_v4());
        let cleanup = IndexStagingCleanup {
            directory: &self.directory,
            name: &staging_name,
        };
        let mut options = OpenOptions::new();
        options.read(true).write(true).create_new(true);
        options.follow(FollowSymlinks::No);
        configure_private_creation(&mut options);
        let file = self
            .directory
            .open_with(&staging_name, &options)?
            .into_std();
        let mut writer = tokio::fs::File::from_std(file);
        writer
            .write_all(&index_header(sealed, native_schema, entries.len())?)
            .await?;
        for chunk in entries.chunks(4_096) {
            check_cancelled(cancellation)?;
            let mut bytes = Vec::new();
            bytes
                .try_reserve_exact(chunk.len().saturating_mul(INDEX_ENTRY_BYTES as usize))
                .map_err(|_| NasdaqReferenceError::AllocationFailed)?;
            for entry in chunk {
                bytes.extend_from_slice(&entry.key);
                bytes.extend_from_slice(&entry.row_number.to_be_bytes());
                bytes.extend_from_slice(&entry.byte_offset.to_be_bytes());
            }
            writer.write_all(&bytes).await?;
        }
        writer.flush().await?;
        writer.sync_all().await?;
        let mut permissions = writer.metadata().await?.permissions();
        permissions.set_readonly(true);
        writer.set_permissions(permissions).await?;
        writer.sync_all().await?;
        drop(writer);
        match self
            .directory
            .hard_link(&staging_name, &self.directory, &name)
        {
            Ok(()) => {}
            Err(error) if error.kind() == ErrorKind::AlreadyExists => {}
            Err(error) => return Err(error.into()),
        }
        sync_publication_directory(&self.directory)?;
        let digest = self
            .verify_index(sealed, native_schema, &name, entries, cancellation)
            .await?;
        drop(cleanup);
        let name =
            SourceIdentifier::try_from(name).map_err(|_| NasdaqReferenceError::InvalidSchema)?;
        Ok((name, digest))
    }

    async fn verify_index(
        &self,
        sealed: &NasdaqSealedRawObject,
        native_schema: &SourceIdentifier,
        name: &str,
        entries: &[ReferenceIndexEntry],
        cancellation: &CancellationToken,
    ) -> Result<EvidenceDigest, NasdaqReferenceError> {
        let mut options = OpenOptions::new();
        options.read(true);
        options.follow(FollowSymlinks::No);
        let file = self.directory.open_with(name, &options)?;
        let metadata = file.metadata()?;
        let expected_size = index_size(entries.len())?;
        if !metadata.is_file()
            || !self.directory.symlink_metadata(name)?.is_file()
            || metadata.len() != expected_size
        {
            return Err(NasdaqReferenceError::IndexVerificationFailed);
        }
        let mut reader = tokio::fs::File::from_std(file.into_std());
        let mut header = [0_u8; INDEX_HEADER_BYTES as usize];
        reader.read_exact(&mut header).await?;
        if header != index_header(sealed, native_schema, entries.len())? {
            return Err(NasdaqReferenceError::IndexVerificationFailed);
        }
        let mut content_hash = Sha256::new();
        content_hash.update(header);
        let mut encoded = [0_u8; INDEX_ENTRY_BYTES as usize];
        for entry in entries {
            check_cancelled(cancellation)?;
            reader.read_exact(&mut encoded).await?;
            content_hash.update(encoded);
            if encoded[..32] != entry.key
                || encoded[32..40] != entry.row_number.to_be_bytes()
                || encoded[40..48] != entry.byte_offset.to_be_bytes()
            {
                return Err(NasdaqReferenceError::IndexVerificationFailed);
            }
        }
        let mut trailing = [0_u8; 1];
        if reader.read(&mut trailing).await? != 0 {
            return Err(NasdaqReferenceError::IndexVerificationFailed);
        }
        Ok(EvidenceDigest::new(
            DigestAlgorithm::Sha256,
            content_hash.finalize().into(),
        ))
    }

    /// Decodes one bounded page from a previously whole-object-validated immutable generation.
    pub async fn decode_page(
        &self,
        object: &NasdaqValidatedObject,
        request: &NasdaqReferencePageRequest,
        cancellation: &CancellationToken,
    ) -> Result<NasdaqReferencePage, NasdaqReferenceError> {
        check_cancelled(cancellation)?;
        if request.family != object.sealed.family
            || request.content_digest != object.sealed.payload_evidence.content_digest()
            || request.native_schema != object.native_schema
            || request.max_records.get() > MAX_REFERENCE_PAGE_RECORDS
            || request.first_row_number < 2
            || request.byte_offset < object.first_data_offset
            || request.byte_offset >= object.sealed.payload_bytes
        {
            return Err(NasdaqReferenceError::CrossGenerationCursor);
        }
        let authenticated_bytes = u64::from(request.max_records.get())
            .checked_add(1)
            .and_then(|lines| lines.checked_mul(MAX_LINE_BYTES as u64))
            .ok_or(NasdaqReferenceError::PageLimitExceeded)?;
        let window = self
            .read_authenticated_window(
                object,
                request.byte_offset,
                authenticated_bytes,
                cancellation,
            )
            .await?;
        let mut window_offset = 0_usize;
        let mut records = Vec::new();
        records
            .try_reserve_exact(request.max_records.get() as usize)
            .map_err(|_| NasdaqReferenceError::AllocationFailed)?;
        let mut row_number = request.first_row_number;
        let mut reached_footer = false;
        while records.len() < request.max_records.get() as usize {
            check_cancelled(cancellation)?;
            let text = read_window_line(&window, &mut window_offset, row_number)?
                .ok_or(NasdaqReferenceError::MissingFooter)?;
            if text.starts_with(FILE_CREATION_PREFIX) {
                let footer = parse_footer(request.family, text, row_number)?;
                if footer != object.file_creation_time {
                    return Err(NasdaqReferenceError::GenerationMismatch);
                }
                reached_footer = true;
                break;
            }
            records.push(NasdaqReferenceIdentityCandidate::try_from_record(
                parse_typed_record(object, row_number, text)?,
            )?);
            row_number = row_number
                .checked_add(1)
                .ok_or(NasdaqReferenceError::RecordLimitExceeded)?;
        }
        if records.is_empty() && !reached_footer {
            return Err(NasdaqReferenceError::InvalidProviderRow);
        }
        let next_cursor = if reached_footer {
            None
        } else {
            let next_byte_offset = request
                .byte_offset
                .checked_add(
                    u64::try_from(window_offset).map_err(|_| NasdaqReferenceError::BodyTooLarge)?,
                )
                .ok_or(NasdaqReferenceError::BodyTooLarge)?;
            let text = read_window_line(&window, &mut window_offset, row_number)?
                .ok_or(NasdaqReferenceError::MissingFooter)?;
            if text.starts_with(FILE_CREATION_PREFIX) {
                let footer = parse_footer(request.family, text, row_number)?;
                if footer != object.file_creation_time {
                    return Err(NasdaqReferenceError::GenerationMismatch);
                }
                None
            } else {
                Some(NasdaqReferencePageCursor::try_new(
                    object,
                    row_number,
                    next_byte_offset,
                )?)
            }
        };
        Ok(NasdaqReferencePage {
            generation: object.generation_evidence()?,
            first_row_number: request.first_row_number,
            request_cursor_identity: request.cursor_identity,
            records,
            next_cursor,
        })
    }

    async fn read_authenticated_window(
        &self,
        object: &NasdaqValidatedObject,
        byte_offset: u64,
        requested_bytes: u64,
        cancellation: &CancellationToken,
    ) -> Result<Vec<u8>, NasdaqReferenceError> {
        check_cancelled(cancellation)?;
        let payload_bytes = object.sealed.payload_bytes;
        if requested_bytes == 0
            || byte_offset >= payload_bytes
            || object.content_chunk_digests.len() != content_chunk_count(payload_bytes)?
        {
            return Err(NasdaqReferenceError::ArchiveVerificationFailed);
        }
        let requested_end = byte_offset
            .saturating_add(requested_bytes)
            .min(payload_bytes);
        let chunk_bytes = AUTHENTICATED_CHUNK_BYTES as u64;
        let first_chunk = byte_offset / chunk_bytes;
        let last_chunk = requested_end
            .checked_sub(1)
            .ok_or(NasdaqReferenceError::ArchiveVerificationFailed)?
            / chunk_bytes;
        let read_start = first_chunk
            .checked_mul(chunk_bytes)
            .ok_or(NasdaqReferenceError::BodyTooLarge)?;
        let read_end = last_chunk
            .saturating_add(1)
            .saturating_mul(chunk_bytes)
            .min(payload_bytes);
        let read_len = usize::try_from(
            read_end
                .checked_sub(read_start)
                .ok_or(NasdaqReferenceError::BodyTooLarge)?,
        )
        .map_err(|_| NasdaqReferenceError::BodyTooLarge)?;
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(read_len)
            .map_err(|_| NasdaqReferenceError::AllocationFailed)?;
        bytes.resize(read_len, 0);
        let mut file = self.open_verified_file(&object.sealed)?;
        file.seek(std::io::SeekFrom::Start(read_start)).await?;
        let mut buffer_offset = 0_usize;
        for chunk_index in first_chunk..=last_chunk {
            check_cancelled(cancellation)?;
            let chunk_start = chunk_index
                .checked_mul(chunk_bytes)
                .ok_or(NasdaqReferenceError::BodyTooLarge)?;
            let expected_chunk_bytes =
                usize::try_from(payload_bytes.saturating_sub(chunk_start).min(chunk_bytes))
                    .map_err(|_| NasdaqReferenceError::BodyTooLarge)?;
            let buffer_end = buffer_offset
                .checked_add(expected_chunk_bytes)
                .ok_or(NasdaqReferenceError::BodyTooLarge)?;
            let destination = bytes
                .get_mut(buffer_offset..buffer_end)
                .ok_or(NasdaqReferenceError::ArchiveVerificationFailed)?;
            file.read_exact(destination).await?;
            let chunk_ordinal =
                usize::try_from(chunk_index).map_err(|_| NasdaqReferenceError::BodyTooLarge)?;
            let observed_digest: [u8; 32] = Sha256::digest(destination).into();
            if &observed_digest
                != object
                    .content_chunk_digests
                    .get(chunk_ordinal)
                    .ok_or(NasdaqReferenceError::ArchiveVerificationFailed)?
            {
                return Err(NasdaqReferenceError::ArchiveVerificationFailed);
            }
            buffer_offset = buffer_end;
        }
        if buffer_offset != read_len {
            return Err(NasdaqReferenceError::ArchiveVerificationFailed);
        }
        let prefix = usize::try_from(
            byte_offset
                .checked_sub(read_start)
                .ok_or(NasdaqReferenceError::BodyTooLarge)?,
        )
        .map_err(|_| NasdaqReferenceError::BodyTooLarge)?;
        let retained = usize::try_from(
            requested_end
                .checked_sub(byte_offset)
                .ok_or(NasdaqReferenceError::BodyTooLarge)?,
        )
        .map_err(|_| NasdaqReferenceError::BodyTooLarge)?;
        let retained_end = prefix
            .checked_add(retained)
            .ok_or(NasdaqReferenceError::BodyTooLarge)?;
        if retained_end > bytes.len() {
            return Err(NasdaqReferenceError::ArchiveVerificationFailed);
        }
        bytes.copy_within(prefix..retained_end, 0);
        bytes.truncate(retained);
        Ok(bytes)
    }

    /// Scans one exact generation for an exact provider-native key.
    ///
    /// Results never bridge a ticker, root, or name to a canonical instrument. Multiple matching
    /// source rows remain an explicit ambiguity.
    pub async fn query(
        &self,
        object: &NasdaqValidatedObject,
        query: &NasdaqReferenceQuery,
        cancellation: &CancellationToken,
    ) -> Result<NasdaqReferenceQueryResult, NasdaqReferenceError> {
        if query.family() != object.sealed.family {
            return Err(NasdaqReferenceError::InvalidQuery);
        }
        if object.index_entry_count != object.record_count
            || object.index_content_digest.algorithm() != DigestAlgorithm::Sha256
            || u64::try_from(object.query_index.len())
                .map_or(true, |count| count != object.index_entry_count)
        {
            return Err(NasdaqReferenceError::IndexVerificationFailed);
        }
        let target = query.key_digest();
        let low = object
            .query_index
            .partition_point(|entry| entry.key < target);
        let one = NonZeroU32::new(1).ok_or(NasdaqReferenceError::PageLimitExceeded)?;
        let mut matches = Vec::new();
        let mut ordinal = low;
        while ordinal < object.query_index.len() {
            check_cancelled(cancellation)?;
            let entry = object.query_index[ordinal];
            if entry.key != target {
                break;
            }
            if ordinal.saturating_sub(low) >= 32_usize {
                return Err(NasdaqReferenceError::QueryConflictLimitExceeded);
            }
            let request = NasdaqReferencePageRequest::try_new(
                object,
                entry.row_number,
                entry.byte_offset,
                one,
                None,
            )?;
            let page = self.decode_page(object, &request, cancellation).await?;
            for record in page.records.into_iter().take(1) {
                if query.matches(&record) {
                    matches
                        .try_reserve(1)
                        .map_err(|_| NasdaqReferenceError::AllocationFailed)?;
                    matches.push(record);
                }
            }
            ordinal = ordinal
                .checked_add(1)
                .ok_or(NasdaqReferenceError::IndexLimitExceeded)?;
        }
        let disposition = match matches.len() {
            0 => NasdaqReferenceQueryDisposition::Missing,
            1 => NasdaqReferenceQueryDisposition::Exact,
            _ => NasdaqReferenceQueryDisposition::Ambiguous,
        };
        Ok(NasdaqReferenceQueryResult {
            generation: object.generation_evidence()?,
            disposition,
            matches,
        })
    }

    fn open_verified_file(
        &self,
        sealed: &NasdaqSealedRawObject,
    ) -> Result<tokio::fs::File, NasdaqReferenceError> {
        let name = evidence_name(sealed.payload_evidence.content_digest())?;
        let mut options = OpenOptions::new();
        options.read(true);
        options.follow(FollowSymlinks::No);
        let file = self.directory.open_with(&name, &options)?;
        let metadata = file.metadata()?;
        if !metadata.is_file()
            || !self.directory.symlink_metadata(&name)?.is_file()
            || metadata.len() != sealed.payload_bytes
        {
            return Err(NasdaqReferenceError::ArchiveVerificationFailed);
        }
        Ok(tokio::fs::File::from_std(file.into_std()))
    }
}

/// Single-response streaming writer. Publication is content-addressed and atomic.
#[derive(Debug)]
pub(crate) struct NasdaqRawObjectWriter {
    directory: Dir,
    staging_name: String,
    file: Option<tokio::fs::File>,
    family: NasdaqDirectoryKind,
    bytes_written: u64,
    hash: Sha256,
}

impl NasdaqRawObjectWriter {
    pub(crate) async fn write_chunk(
        &mut self,
        bytes: &[u8],
        cancellation: &CancellationToken,
    ) -> Result<(), NasdaqReferenceError> {
        check_cancelled(cancellation)?;
        let next = self
            .bytes_written
            .checked_add(
                u64::try_from(bytes.len()).map_err(|_| NasdaqReferenceError::BodyTooLarge)?,
            )
            .ok_or(NasdaqReferenceError::BodyTooLarge)?;
        if next > self.family.maximum_source_bytes() as u64 {
            return Err(NasdaqReferenceError::BodyTooLarge);
        }
        self.file
            .as_mut()
            .ok_or(NasdaqReferenceError::InvalidArchiveState)?
            .write_all(bytes)
            .await?;
        self.hash.update(bytes);
        self.bytes_written = next;
        Ok(())
    }

    pub(crate) async fn commit(
        mut self,
        locator: &str,
        response_evidence: NasdaqHttpResponseEvidence,
        cancellation: &CancellationToken,
    ) -> Result<NasdaqSealedRawObject, NasdaqReferenceError> {
        check_cancelled(cancellation)?;
        if self.bytes_written == 0 {
            return Err(NasdaqReferenceError::EmptyBody);
        }
        let mut file = self
            .file
            .take()
            .ok_or(NasdaqReferenceError::InvalidArchiveState)?;
        file.flush().await?;
        file.sync_all().await?;
        let digest =
            EvidenceDigest::new(DigestAlgorithm::Sha256, self.hash.clone().finalize().into());
        file.seek(std::io::SeekFrom::Start(0)).await?;
        verify_async_reader(&mut file, self.bytes_written, digest, cancellation).await?;
        let mut permissions = file.metadata().await?.permissions();
        permissions.set_readonly(true);
        file.set_permissions(permissions).await?;
        file.sync_all().await?;
        drop(file);

        let final_name = evidence_name(digest)?;
        match self
            .directory
            .hard_link(&self.staging_name, &self.directory, &final_name)
        {
            Ok(()) => {}
            Err(error) if error.kind() == ErrorKind::AlreadyExists => {}
            Err(error) => return Err(error.into()),
        }
        sync_publication_directory(&self.directory)?;
        let mut options = OpenOptions::new();
        options.read(true);
        options.follow(FollowSymlinks::No);
        let final_file = self.directory.open_with(&final_name, &options)?;
        let metadata = final_file.metadata()?;
        if !metadata.is_file()
            || !self.directory.symlink_metadata(&final_name)?.is_file()
            || metadata.len() != self.bytes_written
        {
            return Err(NasdaqReferenceError::ArchiveVerificationFailed);
        }
        let mut reopened = tokio::fs::File::from_std(final_file.into_std());
        verify_async_reader(&mut reopened, self.bytes_written, digest, cancellation).await?;
        sync_publication_directory(&self.directory)?;
        NasdaqSealedRawObject::try_new(
            self.family,
            locator,
            digest,
            self.bytes_written,
            response_evidence,
        )
    }
}

impl Drop for NasdaqRawObjectWriter {
    fn drop(&mut self) {
        let _ignored = self.directory.remove_file(&self.staging_name);
    }
}

struct IndexStagingCleanup<'a> {
    directory: &'a Dir,
    name: &'a str,
}

impl Drop for IndexStagingCleanup<'_> {
    fn drop(&mut self) {
        let _ignored = self.directory.remove_file(self.name);
    }
}

fn index_size(entry_count: usize) -> Result<u64, NasdaqReferenceError> {
    let entries = u64::try_from(entry_count)
        .map_err(|_| NasdaqReferenceError::IndexLimitExceeded)?
        .checked_mul(INDEX_ENTRY_BYTES)
        .ok_or(NasdaqReferenceError::IndexLimitExceeded)?;
    if entries > MAX_REFERENCE_INDEX_BYTES {
        return Err(NasdaqReferenceError::IndexLimitExceeded);
    }
    INDEX_HEADER_BYTES
        .checked_add(entries)
        .ok_or(NasdaqReferenceError::IndexLimitExceeded)
}

fn index_name(
    sealed: &NasdaqSealedRawObject,
    native_schema: &SourceIdentifier,
) -> Result<String, NasdaqReferenceError> {
    let mut name = String::with_capacity(133);
    for byte in sealed.payload_evidence.content_digest().bytes() {
        write!(&mut name, "{byte:02x}").map_err(|_| NasdaqReferenceError::NameFormatting)?;
    }
    name.push('-');
    for byte in Sha256::digest(native_schema.as_str().as_bytes()) {
        write!(&mut name, "{byte:02x}").map_err(|_| NasdaqReferenceError::NameFormatting)?;
    }
    name.push_str(".nsi");
    Ok(name)
}

fn index_header(
    sealed: &NasdaqSealedRawObject,
    native_schema: &SourceIdentifier,
    entry_count: usize,
) -> Result<[u8; INDEX_HEADER_BYTES as usize], NasdaqReferenceError> {
    let mut value = [0_u8; INDEX_HEADER_BYTES as usize];
    value[..4].copy_from_slice(INDEX_MAGIC);
    value[4..6].copy_from_slice(&1_u16.to_be_bytes());
    value[6] = family_tag(sealed.family);
    value[8..40].copy_from_slice(&sealed.payload_evidence.content_digest().bytes());
    value[40..72].copy_from_slice(&Sha256::digest(native_schema.as_str().as_bytes()));
    value[72..80].copy_from_slice(&sealed.payload_bytes.to_be_bytes());
    value[80..88].copy_from_slice(
        &u64::try_from(entry_count)
            .map_err(|_| NasdaqReferenceError::IndexLimitExceeded)?
            .to_be_bytes(),
    );
    Ok(value)
}

const fn family_tag(family: NasdaqDirectoryKind) -> u8 {
    match family {
        NasdaqDirectoryKind::NasdaqListed => 1,
        NasdaqDirectoryKind::OtherListed => 2,
        NasdaqDirectoryKind::Bonds => 3,
        NasdaqDirectoryKind::Options => 4,
    }
}

#[derive(Debug)]
struct ObjectScanReceipt {
    file_creation_time: NasdaqFileCreationTime,
    record_count: u64,
    first_data_offset: u64,
    index_entries: Vec<ReferenceIndexEntry>,
    row_offsets: Vec<u64>,
    content_chunk_digests: Vec<[u8; 32]>,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct ReferenceIndexEntry {
    key: [u8; 32],
    row_number: u64,
    byte_offset: u64,
}

async fn scan_and_validate(
    file: &mut tokio::fs::File,
    family: NasdaqDirectoryKind,
    expected_bytes: u64,
    expected_digest: EvidenceDigest,
    cancellation: &CancellationToken,
) -> Result<ObjectScanReceipt, NasdaqReferenceError> {
    file.seek(std::io::SeekFrom::Start(0)).await?;
    let mut reader = tokio::io::BufReader::with_capacity(READ_CHUNK_BYTES, file);
    let mut hash = Sha256::new();
    let mut content_chunk = Vec::with_capacity(AUTHENTICATED_CHUNK_BYTES);
    let mut content_chunk_digests = Vec::new();
    let mut observed_bytes = 0_u64;
    let mut line = Vec::with_capacity(MAX_LINE_BYTES);
    let header_bytes = read_line(&mut reader, &mut line, 1).await?;
    if header_bytes == 0 {
        return Err(NasdaqReferenceError::EmptyBody);
    }
    hash.update(&line);
    update_content_chunks(&line, &mut content_chunk, &mut content_chunk_digests)?;
    observed_bytes = observed_bytes
        .checked_add(header_bytes as u64)
        .ok_or(NasdaqReferenceError::BodyTooLarge)?;
    if normalized_line(&line, 1)? != family.expected_header() {
        return Err(NasdaqReferenceError::InvalidHeader);
    }
    let first_data_offset = observed_bytes;
    let mut row_number = 2_u64;
    let mut record_count = 0_u64;
    let mut index_entries = Vec::new();
    let mut row_offsets = Vec::new();
    let file_creation_time = loop {
        check_cancelled(cancellation)?;
        let byte_offset = observed_bytes;
        let read = read_line(&mut reader, &mut line, row_number).await?;
        if read == 0 {
            return Err(NasdaqReferenceError::MissingFooter);
        }
        hash.update(&line);
        update_content_chunks(&line, &mut content_chunk, &mut content_chunk_digests)?;
        observed_bytes = observed_bytes
            .checked_add(read as u64)
            .ok_or(NasdaqReferenceError::BodyTooLarge)?;
        let text = normalized_line(&line, row_number)?;
        if text.starts_with(FILE_CREATION_PREFIX) {
            break parse_footer(family, text, row_number)?;
        }
        validate_provider_row(family, text)?;
        if index_entries.len().is_multiple_of(4_096) {
            index_entries
                .try_reserve_exact(4_096)
                .map_err(|_| NasdaqReferenceError::AllocationFailed)?;
            row_offsets
                .try_reserve_exact(4_096)
                .map_err(|_| NasdaqReferenceError::AllocationFailed)?;
        }
        index_entries.push(ReferenceIndexEntry {
            key: provider_query_key(family, text)?,
            row_number,
            byte_offset,
        });
        row_offsets.push(byte_offset);
        record_count = record_count
            .checked_add(1)
            .ok_or(NasdaqReferenceError::RecordLimitExceeded)?;
        if record_count > family.maximum_records() {
            return Err(NasdaqReferenceError::RecordLimitExceeded);
        }
        row_number = row_number
            .checked_add(1)
            .ok_or(NasdaqReferenceError::RecordLimitExceeded)?;
    };
    if record_count == 0 {
        return Err(NasdaqReferenceError::NoRecords);
    }
    let index_bytes = record_count
        .checked_mul(INDEX_ENTRY_BYTES)
        .ok_or(NasdaqReferenceError::IndexLimitExceeded)?;
    if index_bytes > MAX_REFERENCE_INDEX_BYTES {
        return Err(NasdaqReferenceError::IndexLimitExceeded);
    }
    let extra = read_line(&mut reader, &mut line, row_number.saturating_add(1)).await?;
    if extra != 0 {
        return Err(NasdaqReferenceError::DataAfterFooter);
    }
    if observed_bytes != expected_bytes
        || expected_digest.algorithm() != DigestAlgorithm::Sha256
        || hash.finalize().as_slice() != expected_digest.bytes()
    {
        return Err(NasdaqReferenceError::ArchiveVerificationFailed);
    }
    if !content_chunk.is_empty() {
        content_chunk_digests
            .try_reserve(1)
            .map_err(|_| NasdaqReferenceError::AllocationFailed)?;
        content_chunk_digests.push(Sha256::digest(&content_chunk).into());
    }
    if content_chunk_digests.len() != content_chunk_count(expected_bytes)? {
        return Err(NasdaqReferenceError::ArchiveVerificationFailed);
    }
    index_entries.sort_unstable();
    Ok(ObjectScanReceipt {
        file_creation_time,
        record_count,
        first_data_offset,
        index_entries,
        row_offsets,
        content_chunk_digests,
    })
}

fn update_content_chunks(
    mut bytes: &[u8],
    buffer: &mut Vec<u8>,
    digests: &mut Vec<[u8; 32]>,
) -> Result<(), NasdaqReferenceError> {
    while !bytes.is_empty() {
        let remaining = AUTHENTICATED_CHUNK_BYTES
            .checked_sub(buffer.len())
            .ok_or(NasdaqReferenceError::ArchiveVerificationFailed)?;
        let take = remaining.min(bytes.len());
        buffer
            .try_reserve(take)
            .map_err(|_| NasdaqReferenceError::AllocationFailed)?;
        buffer.extend_from_slice(&bytes[..take]);
        bytes = &bytes[take..];
        if buffer.len() == AUTHENTICATED_CHUNK_BYTES {
            digests
                .try_reserve(1)
                .map_err(|_| NasdaqReferenceError::AllocationFailed)?;
            digests.push(Sha256::digest(buffer.as_slice()).into());
            buffer.clear();
        }
    }
    Ok(())
}

fn content_chunk_count(payload_bytes: u64) -> Result<usize, NasdaqReferenceError> {
    let chunk_bytes = AUTHENTICATED_CHUNK_BYTES as u64;
    let chunks = payload_bytes.div_ceil(chunk_bytes);
    usize::try_from(chunks).map_err(|_| NasdaqReferenceError::BodyTooLarge)
}

fn read_window_line<'a>(
    bytes: &'a [u8],
    offset: &mut usize,
    row: u64,
) -> Result<Option<&'a str>, NasdaqReferenceError> {
    if *offset == bytes.len() {
        return Ok(None);
    }
    let remaining = bytes
        .get(*offset..)
        .ok_or(NasdaqReferenceError::ArchiveVerificationFailed)?;
    let newline = remaining
        .iter()
        .position(|byte| *byte == b'\n')
        .ok_or(NasdaqReferenceError::UnterminatedLine { row })?;
    let line_bytes = newline
        .checked_add(1)
        .ok_or(NasdaqReferenceError::LineTooLong { row })?;
    if line_bytes > MAX_LINE_BYTES {
        return Err(NasdaqReferenceError::LineTooLong { row });
    }
    let end = (*offset)
        .checked_add(line_bytes)
        .ok_or(NasdaqReferenceError::BodyTooLarge)?;
    let line = bytes
        .get(*offset..end)
        .ok_or(NasdaqReferenceError::ArchiveVerificationFailed)?;
    *offset = end;
    normalized_line(line, row).map(Some)
}

async fn read_line<R: tokio::io::AsyncBufRead + Unpin>(
    reader: &mut R,
    line: &mut Vec<u8>,
    row: u64,
) -> Result<usize, NasdaqReferenceError> {
    line.clear();
    loop {
        let available = reader.fill_buf().await?;
        if available.is_empty() {
            if line.is_empty() {
                return Ok(0);
            }
            return Err(NasdaqReferenceError::UnterminatedLine { row });
        }
        let newline = available.iter().position(|byte| *byte == b'\n');
        let take = newline.map_or(available.len(), |index| index + 1);
        if line.len().saturating_add(take) > MAX_LINE_BYTES {
            return Err(NasdaqReferenceError::LineTooLong { row });
        }
        line.try_reserve(take)
            .map_err(|_| NasdaqReferenceError::AllocationFailed)?;
        line.extend_from_slice(&available[..take]);
        reader.consume(take);
        if newline.is_some() {
            return Ok(line.len());
        }
    }
}

fn normalized_line(line: &[u8], row: u64) -> Result<&str, NasdaqReferenceError> {
    let without_newline = line
        .strip_suffix(b"\n")
        .ok_or(NasdaqReferenceError::UnterminatedLine { row })?;
    let value = without_newline
        .strip_suffix(b"\r")
        .unwrap_or(without_newline);
    if value.is_empty() || value.contains(&0) {
        return Err(NasdaqReferenceError::InvalidProviderRow);
    }
    std::str::from_utf8(value).map_err(|_| NasdaqReferenceError::InvalidUtf8)
}

fn parse_footer(
    family: NasdaqDirectoryKind,
    line: &str,
    _row: u64,
) -> Result<NasdaqFileCreationTime, NasdaqReferenceError> {
    let value_and_delimiters = line
        .strip_prefix(FILE_CREATION_PREFIX)
        .ok_or(NasdaqReferenceError::InvalidFooter)?;
    let mut fields = value_and_delimiters.split('|');
    let value = fields.next().ok_or(NasdaqReferenceError::InvalidFooter)?;
    let mut delimiters = 0_usize;
    for field in fields {
        if !field.is_empty() {
            return Err(NasdaqReferenceError::InvalidFooter);
        }
        delimiters = delimiters
            .checked_add(1)
            .ok_or(NasdaqReferenceError::InvalidFooter)?;
    }
    if delimiters != family.footer_delimiters() {
        return Err(NasdaqReferenceError::InvalidFooter);
    }
    NasdaqFileCreationTime::try_from_provider_value(value)
        .map_err(|_| NasdaqReferenceError::InvalidFooter)
}

fn validate_provider_row(
    family: NasdaqDirectoryKind,
    line: &str,
) -> Result<(), NasdaqReferenceError> {
    match family {
        NasdaqDirectoryKind::NasdaqListed => {
            let fields = split_exact::<8>(line)?;
            validate_symbol(fields[0])?;
            validate_name(fields[1])?;
            parse_market_category(fields[2])?;
            parse_bool(fields[3])?;
            parse_financial_status(fields[4])?;
            parse_round_lot(fields[5])?;
            parse_bool(fields[6])?;
            parse_bool(fields[7])?;
        }
        NasdaqDirectoryKind::OtherListed => {
            let fields = split_exact::<8>(line)?;
            validate_symbol(fields[0])?;
            validate_name(fields[1])?;
            parse_other_exchange(fields[2])?;
            validate_symbol(fields[3])?;
            parse_bool(fields[4])?;
            parse_round_lot(fields[5])?;
            parse_bool(fields[6])?;
            validate_symbol(fields[7])?;
        }
        NasdaqDirectoryKind::Bonds => {
            let fields = split_exact::<3>(line)?;
            validate_symbol(fields[0])?;
            validate_name(fields[1])?;
            parse_financial_status(fields[2])?;
        }
        NasdaqDirectoryKind::Options => {
            let fields = split_exact::<8>(line)?;
            validate_symbol(fields[0])?;
            parse_closing_type(fields[1])?;
            parse_option_kind(fields[2])?;
            parse_provider_date(fields[3])?;
            validate_decimal(fields[4])?;
            validate_symbol(fields[5])?;
            validate_name(fields[6])?;
            parse_bool(fields[7])?;
        }
    }
    Ok(())
}

fn parse_typed_record(
    object: &NasdaqValidatedObject,
    row_number: u64,
    line: &str,
) -> Result<NasdaqReferenceRecord, NasdaqReferenceError> {
    validate_provider_row(object.sealed.family, line)?;
    let provenance = || NasdaqReferenceProvenance::new(object, row_number);
    match object.sealed.family {
        NasdaqDirectoryKind::NasdaqListed => {
            let fields = split_exact::<8>(line)?;
            let provider = NasdaqProviderFields::try_nasdaq_listed(
                fields[0].to_owned(),
                fields[1].to_owned(),
                parse_market_category(fields[2])?,
                parse_bool(fields[3])?,
                parse_financial_status(fields[4])?,
                parse_round_lot(fields[5])?,
                parse_bool(fields[6])?,
                parse_bool(fields[7])?,
            )
            .map_err(|_| NasdaqReferenceError::InvalidProviderRow)?;
            Ok(NasdaqReferenceRecord::Listing(
                NasdaqListingRecord::try_new(
                    u32::try_from(row_number)
                        .map_err(|_| NasdaqReferenceError::RecordLimitExceeded)?,
                    object.file_creation_time.clone(),
                    object.sealed.source_last_modified_at(),
                    object.sealed.first_observed_at(),
                    object.sealed.payload_evidence.clone(),
                    provider,
                )
                .map_err(|_| NasdaqReferenceError::InvalidProviderRow)?,
            ))
        }
        NasdaqDirectoryKind::OtherListed => {
            let fields = split_exact::<8>(line)?;
            let provider = NasdaqProviderFields::try_other_listed(
                fields[0].to_owned(),
                fields[1].to_owned(),
                parse_other_exchange(fields[2])?,
                fields[3].to_owned(),
                parse_bool(fields[4])?,
                parse_round_lot(fields[5])?,
                parse_bool(fields[6])?,
                fields[7].to_owned(),
            )
            .map_err(|_| NasdaqReferenceError::InvalidProviderRow)?;
            Ok(NasdaqReferenceRecord::Listing(
                NasdaqListingRecord::try_new(
                    u32::try_from(row_number)
                        .map_err(|_| NasdaqReferenceError::RecordLimitExceeded)?,
                    object.file_creation_time.clone(),
                    object.sealed.source_last_modified_at(),
                    object.sealed.first_observed_at(),
                    object.sealed.payload_evidence.clone(),
                    provider,
                )
                .map_err(|_| NasdaqReferenceError::InvalidProviderRow)?,
            ))
        }
        NasdaqDirectoryKind::Bonds => {
            let fields = split_exact::<3>(line)?;
            Ok(NasdaqReferenceRecord::Bond(NasdaqBondReferenceRecord {
                schema_version: 1,
                provider_symbol: ProviderInstrumentId::try_from(fields[0])
                    .map_err(|_| NasdaqReferenceError::InvalidProviderRow)?,
                security_name: fields[1].to_owned(),
                financial_status: parse_financial_status(fields[2])?,
                provenance: provenance(),
            }))
        }
        NasdaqDirectoryKind::Options => {
            let fields = split_exact::<8>(line)?;
            Ok(NasdaqReferenceRecord::Option(NasdaqOptionReferenceRecord {
                schema_version: 1,
                root_symbol: ProviderInstrumentId::try_from(fields[0])
                    .map_err(|_| NasdaqReferenceError::InvalidProviderRow)?,
                closing_type: parse_closing_type(fields[1])?,
                option_kind: parse_option_kind(fields[2])?,
                expiration_date: parse_provider_date(fields[3])?,
                explicit_strike_price: NasdaqProviderDecimal::try_from_provider(fields[4])?,
                underlying_symbol: ProviderInstrumentId::try_from(fields[5])
                    .map_err(|_| NasdaqReferenceError::InvalidProviderRow)?,
                underlying_issue_name: fields[6].to_owned(),
                pending: parse_bool(fields[7])?,
                provenance: provenance(),
            }))
        }
    }
}

fn split_exact<const N: usize>(line: &str) -> Result<[&str; N], NasdaqReferenceError> {
    let mut fields = [""; N];
    let mut values = line.split('|');
    for field in &mut fields {
        *field = values
            .next()
            .ok_or(NasdaqReferenceError::InvalidFieldCount)?;
    }
    if values.next().is_some() {
        return Err(NasdaqReferenceError::InvalidFieldCount);
    }
    Ok(fields)
}

fn provider_query_key(
    family: NasdaqDirectoryKind,
    line: &str,
) -> Result<[u8; 32], NasdaqReferenceError> {
    match family {
        NasdaqDirectoryKind::NasdaqListed | NasdaqDirectoryKind::OtherListed => {
            let fields = split_exact::<8>(line)?;
            Ok(symbol_query_key(family, fields[0]))
        }
        NasdaqDirectoryKind::Bonds => {
            let fields = split_exact::<3>(line)?;
            Ok(symbol_query_key(family, fields[0]))
        }
        NasdaqDirectoryKind::Options => {
            let fields = split_exact::<8>(line)?;
            let date = parse_provider_date(fields[3])?;
            let (coefficient, scale) = parse_decimal_coordinate(fields[4])?;
            Ok(option_query_key(
                fields[0],
                parse_option_kind(fields[2])?,
                date,
                coefficient,
                scale,
            ))
        }
    }
}

fn symbol_query_key(family: NasdaqDirectoryKind, symbol: &str) -> [u8; 32] {
    let mut hash = query_key_hasher(family);
    hash_key_field(&mut hash, symbol.as_bytes());
    hash.finalize().into()
}

fn option_query_key(
    root: &str,
    kind: OptionKind,
    expiration: CalendarDate,
    coefficient: u64,
    scale: u8,
) -> [u8; 32] {
    let mut hash = query_key_hasher(NasdaqDirectoryKind::Options);
    hash_key_field(&mut hash, root.as_bytes());
    hash.update(match kind {
        OptionKind::Call => b"call".as_slice(),
        OptionKind::Put => b"put".as_slice(),
    });
    hash.update(expiration.year().to_be_bytes());
    hash.update([expiration.month(), expiration.day()]);
    hash.update(coefficient.to_be_bytes());
    hash.update([scale]);
    hash.finalize().into()
}

fn query_key_hasher(family: NasdaqDirectoryKind) -> Sha256 {
    let mut hash = Sha256::new();
    hash.update(b"market-squawk/nasdaq-provider-query-key/v1");
    hash_key_field(&mut hash, family.object_component().as_bytes());
    hash
}

fn hash_key_field(hash: &mut Sha256, field: &[u8]) {
    hash.update((field.len() as u64).to_be_bytes());
    hash.update(field);
}

fn hash_generation_field(hash: &mut Sha256, field: &[u8]) {
    hash.update((field.len() as u64).to_be_bytes());
    hash.update(field);
}

fn hash_optional_generation_field(hash: &mut Sha256, field: Option<&str>) {
    match field {
        Some(field) => {
            hash.update([1]);
            hash_generation_field(hash, field.as_bytes());
        }
        None => hash.update([0]),
    }
}

fn validate_symbol(value: &str) -> Result<(), NasdaqReferenceError> {
    if value.is_empty()
        || value.len() > 32
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_graphic() && byte != b'|')
    {
        Err(NasdaqReferenceError::InvalidProviderRow)
    } else {
        Ok(())
    }
}

fn validate_name(value: &str) -> Result<(), NasdaqReferenceError> {
    if value.trim().is_empty()
        || value.len() > 255
        || value.contains('|')
        || value.chars().any(char::is_control)
    {
        Err(NasdaqReferenceError::InvalidProviderRow)
    } else {
        Ok(())
    }
}

fn parse_bool(value: &str) -> Result<bool, NasdaqReferenceError> {
    match value {
        "Y" => Ok(true),
        "N" => Ok(false),
        _ => Err(NasdaqReferenceError::InvalidProviderRow),
    }
}

fn parse_market_category(value: &str) -> Result<NasdaqMarketCategory, NasdaqReferenceError> {
    match value {
        "Q" => Ok(NasdaqMarketCategory::GlobalSelect),
        "G" => Ok(NasdaqMarketCategory::GlobalMarket),
        "S" => Ok(NasdaqMarketCategory::CapitalMarket),
        _ => Err(NasdaqReferenceError::InvalidProviderRow),
    }
}

fn parse_financial_status(value: &str) -> Result<NasdaqFinancialStatus, NasdaqReferenceError> {
    match value {
        "N" => Ok(NasdaqFinancialStatus::Normal),
        "D" => Ok(NasdaqFinancialStatus::Deficient),
        "E" => Ok(NasdaqFinancialStatus::Delinquent),
        "Q" => Ok(NasdaqFinancialStatus::Bankrupt),
        "G" => Ok(NasdaqFinancialStatus::DeficientAndBankrupt),
        "H" => Ok(NasdaqFinancialStatus::DeficientAndDelinquent),
        "J" => Ok(NasdaqFinancialStatus::DelinquentAndBankrupt),
        "K" => Ok(NasdaqFinancialStatus::DeficientDelinquentAndBankrupt),
        _ => Err(NasdaqReferenceError::InvalidProviderRow),
    }
}

fn parse_other_exchange(value: &str) -> Result<NasdaqOtherExchange, NasdaqReferenceError> {
    match value {
        "A" => Ok(NasdaqOtherExchange::NyseAmerican),
        "N" => Ok(NasdaqOtherExchange::Nyse),
        "P" => Ok(NasdaqOtherExchange::NyseArca),
        "M" => Ok(NasdaqOtherExchange::NyseTexas),
        "Z" => Ok(NasdaqOtherExchange::CboeBzx),
        "V" => Ok(NasdaqOtherExchange::Iex),
        _ => Err(NasdaqReferenceError::InvalidProviderRow),
    }
}

fn parse_round_lot(value: &str) -> Result<u32, NasdaqReferenceError> {
    if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(NasdaqReferenceError::InvalidProviderRow);
    }
    value
        .parse::<u32>()
        .ok()
        .filter(|value| *value > 0 && *value <= 999_999)
        .ok_or(NasdaqReferenceError::InvalidProviderRow)
}

fn parse_closing_type(value: &str) -> Result<NasdaqOptionClosingType, NasdaqReferenceError> {
    match value {
        "N" => Ok(NasdaqOptionClosingType::Normal),
        "L" => Ok(NasdaqOptionClosingType::Late),
        _ => Err(NasdaqReferenceError::InvalidProviderRow),
    }
}

fn parse_option_kind(value: &str) -> Result<OptionKind, NasdaqReferenceError> {
    match value {
        "C" => Ok(OptionKind::Call),
        "P" => Ok(OptionKind::Put),
        _ => Err(NasdaqReferenceError::InvalidProviderRow),
    }
}

fn parse_provider_date(value: &str) -> Result<CalendarDate, NasdaqReferenceError> {
    if value.len() != 10
        || value.as_bytes().get(2) != Some(&b'/')
        || value.as_bytes().get(5) != Some(&b'/')
        || !value
            .bytes()
            .enumerate()
            .all(|(index, byte)| matches!(index, 2 | 5) || byte.is_ascii_digit())
    {
        return Err(NasdaqReferenceError::InvalidProviderRow);
    }
    let month = value[0..2]
        .parse::<u8>()
        .map_err(|_| NasdaqReferenceError::InvalidProviderRow)?;
    let day = value[3..5]
        .parse::<u8>()
        .map_err(|_| NasdaqReferenceError::InvalidProviderRow)?;
    let year = value[6..10]
        .parse::<u16>()
        .map_err(|_| NasdaqReferenceError::InvalidProviderRow)?;
    CalendarDate::new(year, month, day).map_err(|_| NasdaqReferenceError::InvalidProviderRow)
}

fn validate_decimal(value: &str) -> Result<(), NasdaqReferenceError> {
    parse_decimal_coordinate(value).map(|_| ())
}

fn parse_decimal_coordinate(value: &str) -> Result<(u64, u8), NasdaqReferenceError> {
    let (whole, fractional) = value
        .split_once('.')
        .ok_or(NasdaqReferenceError::InvalidProviderRow)?;
    if whole.is_empty()
        || whole.len() > 12
        || fractional.is_empty()
        || fractional.len() > 9
        || !whole
            .bytes()
            .chain(fractional.bytes())
            .all(|byte| byte.is_ascii_digit())
    {
        return Err(NasdaqReferenceError::InvalidProviderRow);
    }
    let coefficient = whole
        .bytes()
        .chain(fractional.bytes())
        .try_fold(0_u64, |value, byte| {
            value
                .checked_mul(10)
                .and_then(|value| value.checked_add(u64::from(byte - b'0')))
        })
        .ok_or(NasdaqReferenceError::InvalidProviderRow)?;
    let mut normalized = coefficient;
    let mut scale =
        u8::try_from(fractional.len()).map_err(|_| NasdaqReferenceError::InvalidProviderRow)?;
    while scale > 0 && normalized.is_multiple_of(10) {
        normalized /= 10;
        scale -= 1;
    }
    Ok((normalized, scale))
}

pub(crate) fn validate_footer_clock(
    creation: &NasdaqFileCreationTime,
    last_modified: Timestamp,
) -> Result<(), NasdaqReferenceError> {
    let utc = DateTime::<Utc>::from_timestamp_nanos(last_modified.unix_nanos());
    let last_modified_date = CalendarDate::new(
        u16::try_from(utc.year()).map_err(|_| NasdaqReferenceError::InvalidClock)?,
        u8::try_from(utc.month()).map_err(|_| NasdaqReferenceError::InvalidClock)?,
        u8::try_from(utc.day()).map_err(|_| NasdaqReferenceError::InvalidClock)?,
    )
    .map_err(|_| NasdaqReferenceError::InvalidClock)?;
    if creation.date() > last_modified_date {
        Err(NasdaqReferenceError::InvalidClock)
    } else {
        Ok(())
    }
}

async fn verify_async_reader(
    reader: &mut tokio::fs::File,
    expected_bytes: u64,
    expected_digest: EvidenceDigest,
    cancellation: &CancellationToken,
) -> Result<(), NasdaqReferenceError> {
    reader.seek(std::io::SeekFrom::Start(0)).await?;
    let mut hash = Sha256::new();
    let mut total = 0_u64;
    let mut buffer = [0_u8; READ_CHUNK_BYTES];
    loop {
        check_cancelled(cancellation)?;
        let read = reader.read(&mut buffer).await?;
        if read == 0 {
            break;
        }
        total = total
            .checked_add(u64::try_from(read).map_err(|_| NasdaqReferenceError::BodyTooLarge)?)
            .ok_or(NasdaqReferenceError::BodyTooLarge)?;
        if total > expected_bytes {
            return Err(NasdaqReferenceError::ArchiveVerificationFailed);
        }
        hash.update(&buffer[..read]);
    }
    if total != expected_bytes
        || expected_digest.algorithm() != DigestAlgorithm::Sha256
        || hash.finalize().as_slice() != expected_digest.bytes()
    {
        return Err(NasdaqReferenceError::ArchiveVerificationFailed);
    }
    Ok(())
}

fn evidence_name(evidence: EvidenceDigest) -> Result<String, NasdaqReferenceError> {
    if evidence.algorithm() != DigestAlgorithm::Sha256 {
        return Err(NasdaqReferenceError::UnsupportedDigest);
    }
    let mut name = String::with_capacity(68);
    for byte in evidence.bytes() {
        write!(&mut name, "{byte:02x}").map_err(|_| NasdaqReferenceError::NameFormatting)?;
    }
    name.push_str(".raw");
    Ok(name)
}

fn check_cancelled(cancellation: &CancellationToken) -> Result<(), NasdaqReferenceError> {
    if cancellation.is_cancelled() {
        Err(NasdaqReferenceError::Cancelled)
    } else {
        Ok(())
    }
}

#[cfg(unix)]
fn configure_private_creation(options: &mut OpenOptions) {
    use cap_std::fs::OpenOptionsExt as _;
    options.mode(0o600);
}

#[cfg(not(unix))]
fn configure_private_creation(_options: &mut OpenOptions) {}

#[cfg(unix)]
fn sync_publication_directory(directory: &Dir) -> Result<(), std::io::Error> {
    use cap_std::fs::OpenOptionsExt as _;

    let mut options = OpenOptions::new();
    options.read(true);
    options.follow(FollowSymlinks::No);
    options.custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC);
    directory
        .open_with(".", &options)
        .map(cap_std::fs::File::into_std)
        .and_then(|opened| opened.sync_all())
}

#[cfg(windows)]
fn sync_publication_directory(_directory: &Dir) -> Result<(), std::io::Error> {
    Ok(())
}

#[cfg(all(not(unix), not(windows)))]
fn sync_publication_directory(directory: &Dir) -> Result<(), std::io::Error> {
    directory.try_clone()?.into_std_file().sync_all()
}

/// Durable capture, whole-object validation, paging, or recovery failure.
#[derive(Debug, Error)]
pub enum NasdaqReferenceError {
    /// Capability-scoped filesystem I/O failed.
    #[error("Nasdaq reference archive I/O failed: {0}")]
    Io(#[from] std::io::Error),
    /// Cooperative cancellation was observed before completion.
    #[error("Nasdaq reference operation cancelled")]
    Cancelled,
    /// The response body was empty.
    #[error("Nasdaq reference response was empty")]
    EmptyBody,
    /// The exact object exceeded its provider-family ceiling.
    #[error("Nasdaq reference response exceeded its family byte bound")]
    BodyTooLarge,
    /// The object was not valid UTF-8.
    #[error("Nasdaq reference object is not UTF-8")]
    InvalidUtf8,
    /// The code-owned schema header did not match exactly.
    #[error("Nasdaq reference header is invalid")]
    InvalidHeader,
    /// A line exceeded the hard schema bound.
    #[error("Nasdaq reference row {row} exceeds the line bound")]
    LineTooLong {
        /// One-based source row.
        row: u64,
    },
    /// A line was not terminated by the provider file contract.
    #[error("Nasdaq reference row {row} is not newline terminated")]
    UnterminatedLine {
        /// One-based source row.
        row: u64,
    },
    /// A row had the wrong exact column count.
    #[error("Nasdaq reference row has an invalid field count")]
    InvalidFieldCount,
    /// A provider value violated its closed schema.
    #[error("Nasdaq reference row has an invalid provider value")]
    InvalidProviderRow,
    /// The exact footer was missing.
    #[error("Nasdaq reference footer is missing")]
    MissingFooter,
    /// The exact footer shape or file-creation coordinate was invalid.
    #[error("Nasdaq reference footer is invalid")]
    InvalidFooter,
    /// Data followed the terminal footer.
    #[error("Nasdaq reference data followed the terminal footer")]
    DataAfterFooter,
    /// No provider records preceded the footer.
    #[error("Nasdaq reference object contains no records")]
    NoRecords,
    /// The family-specific record ceiling was exceeded.
    #[error("Nasdaq reference object exceeds its record ceiling")]
    RecordLimitExceeded,
    /// File-creation and response clocks were inconsistent.
    #[error("Nasdaq reference clocks are inconsistent")]
    InvalidClock,
    /// Retained status, media type, encoding, length, validator, or response clocks were invalid.
    #[error("Nasdaq HTTP response evidence is invalid")]
    InvalidHttpEvidence,
    /// A durable receipt was malformed or inconsistent.
    #[error("Nasdaq sealed-object receipt is invalid")]
    InvalidObjectReceipt,
    /// Raw, parser, clock, schema, count, or index evidence could not form one generation.
    #[error("Nasdaq reference generation evidence is invalid")]
    InvalidGenerationEvidence,
    /// The archive root was not an unambiguous directory capability.
    #[error("Nasdaq archive root is unsafe")]
    UnsafeArchiveRoot,
    /// Persisted bytes, size, or content address did not reverify.
    #[error("Nasdaq archived object failed verification")]
    ArchiveVerificationFailed,
    /// The archive writer was used after its terminal state.
    #[error("Nasdaq archive writer is in an invalid state")]
    InvalidArchiveState,
    /// Only SHA-256 content identities are supported.
    #[error("Nasdaq archive digest algorithm is unsupported")]
    UnsupportedDigest,
    /// Formatting a content-addressed name failed.
    #[error("Nasdaq archive content name could not be formatted")]
    NameFormatting,
    /// A bounded allocation failed.
    #[error("Nasdaq reference allocation failed")]
    AllocationFailed,
    /// The bounded fixed-width provider-key index exceeded its production ceiling.
    #[error("Nasdaq provider-key index exceeds its byte ceiling")]
    IndexLimitExceeded,
    /// The durable provider-key index failed generation/schema/content reconciliation.
    #[error("Nasdaq provider-key index failed verification")]
    IndexVerificationFailed,
    /// The code-owned native schema identity was invalid.
    #[error("Nasdaq native schema identity is invalid")]
    InvalidSchema,
    /// An untrusted cursor failed its internal binding.
    #[error("Nasdaq reference cursor is invalid")]
    InvalidCursor,
    /// A cursor or page request belonged to another generation, family, or schema.
    #[error("Nasdaq reference cursor does not belong to this exact generation")]
    CrossGenerationCursor,
    /// A requested page exceeded the bounded in-memory page ceiling.
    #[error("Nasdaq reference page exceeds the record ceiling")]
    PageLimitExceeded,
    /// The provider namespace query was malformed or addressed another family.
    #[error("Nasdaq provider-native query is invalid")]
    InvalidQuery,
    /// One provider key produced more conflicts than the bounded query result can retain.
    #[error("Nasdaq provider-native query exceeded its conflict ceiling")]
    QueryConflictLimitExceeded,
    /// Immutable state changed after whole-object validation.
    #[error("Nasdaq archived generation changed after validation")]
    GenerationMismatch,
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::io::{Seek as _, Write as _};
    use std::num::NonZeroU32;

    use cap_std::{ambient_authority, fs::Dir};
    use chrono::DateTime;
    use tokio_util::sync::CancellationToken;

    use super::{NasdaqRawObjectStore, NasdaqReferenceError};
    use market_squawk_domain::{CalendarDate, DigestAlgorithm, OptionKind};

    use crate::{
        NasdaqDirectoryKind, NasdaqIdentityDisposition, NasdaqReferenceCompleteness,
        NasdaqReferenceLifecycleDisposition, NasdaqReferenceQuery, NasdaqReferenceQueryDisposition,
        NasdaqReferenceTradabilityDisposition, NasdaqReferenceValidityDisposition,
    };

    const OPTIONS_FIXTURE: &[u8] = b"Root Symbol|Options Closing Type|Options Type|Expiration Date|Explicit Strike Price|Underlying Symbol|Underlying Issue Name|Pending\r\nA|N|P|03/19/2027|210.000|A|AGILENT TECH INC|N\r\nA|N|C|03/19/2027|220.000|A|AGILENT TECH INC|N\r\nFile Creation Time: 0813202621:32|||||||\r\n";
    const NEXT_GENERATION: &[u8] = b"Root Symbol|Options Closing Type|Options Type|Expiration Date|Explicit Strike Price|Underlying Symbol|Underlying Issue Name|Pending\r\nA|N|P|03/19/2027|211.000|A|AGILENT TECH INC|N\r\nFile Creation Time: 0813202621:33|||||||\r\n";

    #[tokio::test]
    async fn durable_pages_recover_and_reject_cross_generation_cursor() -> Result<(), Box<dyn Error>>
    {
        let temporary = tempfile::tempdir()?;
        let store = NasdaqRawObjectStore::try_new(Dir::open_ambient_dir(
            temporary.path(),
            ambient_authority(),
        )?)?;
        store.activation_check()?;
        let cancellation = CancellationToken::new();
        let last_modified = timestamp("2026-08-14T01:32:37Z")?;
        let first_observed = timestamp("2026-08-14T05:05:00Z")?;

        let mut writer = store.begin(NasdaqDirectoryKind::Options, &cancellation)?;
        for chunk in OPTIONS_FIXTURE.chunks(17) {
            writer.write_chunk(chunk, &cancellation).await?;
        }
        let sealed = writer
            .commit(
                super::OPTIONS_URL,
                super::NasdaqHttpResponseEvidence::try_new(
                    200,
                    "text/plain".to_owned(),
                    None,
                    Some(OPTIONS_FIXTURE.len() as u64),
                    None,
                    1,
                    last_modified,
                    first_observed,
                )?,
                &cancellation,
            )
            .await?;
        assert!(!serde_json::to_string(&sealed)?.contains("\"policy_digest\""));
        let recovered = store.recover(&sealed, &cancellation).await?;
        assert_eq!(recovered.record_count(), 2);
        let generation = recovered.generation_evidence()?;
        assert_eq!(
            generation.raw_content_digest(),
            sealed.payload_evidence().content_digest()
        );
        assert_eq!(generation.parsed_records(), 2);
        assert_eq!(generation.rejected_records(), 0);
        assert_eq!(
            generation.completeness(),
            NasdaqReferenceCompleteness::StrictObjectComplete
        );
        assert_eq!(
            generation.generation_identity().algorithm(),
            DigestAlgorithm::Sha256
        );
        let doctor = store.validated_report(&recovered)?;
        assert_eq!(
            doctor.generation_identity(),
            generation.generation_identity()
        );
        let query = NasdaqReferenceQuery::option(
            "A",
            OptionKind::Put,
            CalendarDate::new(2027, 3, 19)?,
            "210.0",
        )?;
        let result = store.query(&recovered, &query, &cancellation).await?;
        assert_eq!(result.disposition(), NasdaqReferenceQueryDisposition::Exact);
        assert_eq!(result.matches().len(), 1);
        assert_eq!(
            result.generation().generation_identity(),
            generation.generation_identity()
        );
        let one = NonZeroU32::new(1).ok_or("nonzero page size")?;
        let first = store
            .decode_page(&recovered, &recovered.first_page(one)?, &cancellation)
            .await?;
        assert!(!serde_json::to_string(&first)?.contains("\"policy_digest\""));
        assert_eq!(first.records().len(), 1);
        assert_eq!(
            first.generation().generation_identity(),
            generation.generation_identity()
        );
        let first_candidate = &first.records()[0];
        assert_eq!(
            first_candidate.identity_disposition(),
            NasdaqIdentityDisposition::ProviderNativeReferenceOnly
        );
        assert_eq!(
            first_candidate.validity_disposition(),
            NasdaqReferenceValidityDisposition::ExactSourceSnapshotOnly
        );
        assert_eq!(
            first_candidate.lifecycle_disposition(),
            NasdaqReferenceLifecycleDisposition::CurrentDirectoryObservationOnly
        );
        assert_eq!(
            first_candidate.tradability_disposition(),
            NasdaqReferenceTradabilityDisposition::UnknownFromReferenceDirectory
        );
        let cursor = first.next_cursor().ok_or("missing continuation")?;
        let cursor = super::NasdaqReferencePageCursor::from_json(&serde_json::to_vec(cursor)?)?;
        assert!(!serde_json::to_string(&cursor)?.contains("\"policy_digest\""));
        assert_eq!(
            cursor.cursor_identity().algorithm(),
            DigestAlgorithm::Sha256
        );
        let second = store
            .decode_page(
                &recovered,
                &recovered.resume_page(&cursor, one)?,
                &cancellation,
            )
            .await?;
        assert_eq!(second.records().len(), 1);
        assert!(second.next_cursor().is_none());
        assert_eq!(
            second.request_cursor_identity(),
            Some(cursor.cursor_identity())
        );

        let mut writer = store.begin(NasdaqDirectoryKind::Options, &cancellation)?;
        writer.write_chunk(NEXT_GENERATION, &cancellation).await?;
        let next = writer
            .commit(
                super::OPTIONS_URL,
                super::NasdaqHttpResponseEvidence::try_new(
                    200,
                    "text/plain".to_owned(),
                    None,
                    Some(NEXT_GENERATION.len() as u64),
                    None,
                    1,
                    last_modified,
                    first_observed,
                )?,
                &cancellation,
            )
            .await?;
        let next = store.recover(&next, &cancellation).await?;
        assert!(matches!(
            next.resume_page(&cursor, one),
            Err(NasdaqReferenceError::CrossGenerationCursor)
        ));

        let mut forged_same_generation_cursor = cursor.clone();
        forged_same_generation_cursor.next_row_number = 2;
        forged_same_generation_cursor.binding_digest =
            forged_same_generation_cursor.expected_binding();
        assert!(matches!(
            recovered.resume_page(&forged_same_generation_cursor, one),
            Err(NasdaqReferenceError::CrossGenerationCursor)
        ));

        let raw_name = super::evidence_name(sealed.payload_evidence().content_digest())?;
        let raw_path = temporary.path().join(raw_name);
        let mut permissions = std::fs::metadata(&raw_path)?.permissions();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            permissions.set_mode(0o600);
        }
        #[cfg(not(unix))]
        permissions.set_readonly(false);
        std::fs::set_permissions(&raw_path, permissions)?;
        let mutation_offset = OPTIONS_FIXTURE
            .windows(b"AGILENT".len())
            .position(|window| window == b"AGILENT")
            .ok_or("fixture mutation coordinate is missing")?;
        let mut raw = std::fs::OpenOptions::new().write(true).open(&raw_path)?;
        raw.seek(std::io::SeekFrom::Start(u64::try_from(mutation_offset)?))?;
        raw.write_all(b"X")?;
        raw.sync_all()?;
        drop(raw);
        let request = recovered.first_page(one)?;
        assert!(matches!(
            store.decode_page(&recovered, &request, &cancellation).await,
            Err(NasdaqReferenceError::ArchiveVerificationFailed)
        ));
        Ok(())
    }

    fn timestamp(value: &str) -> Result<market_squawk_domain::Timestamp, Box<dyn Error>> {
        let nanos = DateTime::parse_from_rfc3339(value)?
            .timestamp_nanos_opt()
            .ok_or("timestamp out of range")?;
        Ok(market_squawk_domain::Timestamp::from_unix_nanos(nanos))
    }
}
