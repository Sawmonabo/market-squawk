//! Platform-backed logical raw-object handoff and bounded provider-native reference decoding.

use std::fmt::Write as _;
use std::io::{BufRead, BufReader, Seek as _, SeekFrom};
use std::num::NonZeroU32;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use chrono::{DateTime, Datelike as _, Utc};
use market_squawk_domain::{
    CalendarDate, DataQuality, DigestAlgorithm, EvidenceDigest, ExactPayloadEvidence, OptionKind,
    ProviderInstrumentId, SourceIdentifier, Timestamp,
};
use market_squawk_platform::{
    ResearchObjectAdmission, ResearchObjectControl, ResearchObjectControlError,
    ResearchObjectControlPoint, ResearchObjectReceipt, SealedResearchJournalStoreError,
    VerifiedResearchObject,
};
use market_squawk_sources::ExtractionAuthority;
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use thiserror::Error;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, oneshot};
use tokio_util::sync::CancellationToken;

use crate::model::{
    NasdaqDirectoryKind, NasdaqDirectoryPresence, NasdaqFileCreationTime, NasdaqFinancialStatus,
    NasdaqListingRecord, NasdaqMarketCategory, NasdaqOtherExchange, NasdaqProviderFields,
};
/// Official current Nasdaq option-series reference object.
pub const OPTIONS_URL: &str = "https://www.nasdaqtrader.com/dynamic/SymDir/options.txt";
/// Maximum admitted exact `options.txt` bytes.
pub const MAX_OPTIONS_SOURCE_BYTES: usize = 128 * 1024 * 1024;
/// Bond-schema byte ceiling reserved until a supported HTTPS retrieval contract is frozen.
pub const MAX_BONDS_SOURCE_BYTES: usize = 8 * 1024 * 1024;
/// Maximum admitted option-series rows in one exact object.
pub const MAX_OPTIONS_RECORDS: u64 = 2_500_000;
/// Bond-schema row ceiling reserved until a supported HTTPS retrieval contract is frozen.
pub const MAX_BONDS_RECORDS: u64 = 32_768;
/// Maximum records decoded into one in-memory typed page.
pub const MAX_REFERENCE_PAGE_RECORDS: u32 = 4_096;
/// Maximum fixed-width provider-key index bytes built for one exact generation.
pub const MAX_REFERENCE_INDEX_BYTES: u64 = 128 * 1024 * 1024;

const MAX_LINE_BYTES: usize = 512;
const READ_CHUNK_BYTES: usize = 64 * 1024;
const INDEX_ENTRY_BYTES: u64 = 48;
const ROW_OFFSET_BYTES: u64 = 8;
const MAX_REFERENCE_ROW_OFFSET_BYTES: u64 = 32 * 1024 * 1024;
const MAX_QUERY_CONFLICTS: usize = 32;
const SOURCE_AUTHORITY_CHECKPOINT_INTERVAL: usize = 4_096;
const FINAL_VERIFICATION_WORKERS_PER_SOURCE: usize = 1;
const BLOCKING_WORKER_CONTROL_POLL_INTERVAL: Duration = Duration::from_millis(25);
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

/// Currentness disposition when Nasdaq publishes no exact refresh interval.
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
    /// Presence is snapshot-only; no listing start, delisting, rename, or successor is inferred.
    CurrentDirectoryObservationOnly,
}

/// Execution or live-trading meaning available from a Nasdaq reference row.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum NasdaqReferenceTradabilityDisposition {
    /// The reference directory does not establish current tradability or execution eligibility.
    UnknownFromReferenceDirectory,
}

/// Whole-object parser completeness admitted by one pending typed handoff.
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
    fn new(
        file_creation_time: &NasdaqFileCreationTime,
        response_evidence: &NasdaqHttpResponseEvidence,
        raw_content_digest: EvidenceDigest,
        provider_row_number: u64,
    ) -> Self {
        Self {
            provider_row_number,
            file_creation_time: file_creation_time.clone(),
            source_last_modified_at: response_evidence.last_modified_at(),
            first_observed_at: response_evidence.received_at(),
            source_payload_evidence: ExactPayloadEvidence::from_content_digest(raw_content_digest),
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

    /// Returns the bounded official-source quality; this is not execution eligibility.
    pub const fn quality(&self) -> DataQuality {
        self.quality
    }

    /// Returns the qualified meaning of presence in the downloaded current directory.
    pub const fn directory_presence(&self) -> NasdaqDirectoryPresence {
        self.directory_presence
    }

    /// Returns the explicit non-canonical identity disposition.
    pub const fn identity_disposition(&self) -> NasdaqIdentityDisposition {
        self.identity_disposition
    }
}

/// Exact Nasdaq-listed bond schema candidate.
///
/// Official acquisition remains unavailable until a supported HTTPS contract is frozen.
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
            write!(&mut digest, "{byte:02x}")
                .map_err(|_| NasdaqReferenceError::EvidenceFormatting)?;
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

/// Exact HTTP response evidence retained alongside one logical raw body.
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

    /// Returns the exact admitted `Content-Encoding` header value, when supplied.
    pub fn content_encoding(&self) -> Option<&str> {
        self.content_encoding.as_deref()
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

/// Exact registered source, dataset, family, and official-file identity.
///
/// This is provider/file provenance only. It neither mints a canonical instrument identity nor
/// asserts that a downstream catalog has published the object.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct NasdaqReferenceFileIdentity {
    schema_version: u16,
    provider: SourceIdentifier,
    source_id: SourceIdentifier,
    metadata_revision: SourceIdentifier,
    dataset: SourceIdentifier,
    family: NasdaqDirectoryKind,
    configured_locator: SourceIdentifier,
    final_locator: SourceIdentifier,
}

impl NasdaqReferenceFileIdentity {
    /// Constructs the exact identity of one independently clocked official file.
    ///
    /// # Errors
    ///
    /// Rejects any source, revision, dataset, or official locator that cannot be represented.
    pub(crate) fn try_new(
        source_id: &str,
        metadata_revision: &str,
        dataset: &str,
        family: NasdaqDirectoryKind,
    ) -> Result<Self, NasdaqReferenceError> {
        let configured_locator = crate::source::directory_locator(family)
            .ok_or(NasdaqReferenceError::ProviderContractUnavailable)
            .and_then(|locator| {
                SourceIdentifier::try_from(locator)
                    .map_err(|_| NasdaqReferenceError::InvalidFileIdentity)
            })?;
        Ok(Self {
            schema_version: 1,
            provider: SourceIdentifier::try_from(crate::NASDAQ_SYMBOL_DIRECTORY_PROVIDER)
                .map_err(|_| NasdaqReferenceError::InvalidFileIdentity)?,
            source_id: SourceIdentifier::try_from(source_id)
                .map_err(|_| NasdaqReferenceError::InvalidFileIdentity)?,
            metadata_revision: SourceIdentifier::try_from(metadata_revision)
                .map_err(|_| NasdaqReferenceError::InvalidFileIdentity)?,
            dataset: SourceIdentifier::try_from(dataset)
                .map_err(|_| NasdaqReferenceError::InvalidFileIdentity)?,
            family,
            final_locator: configured_locator.clone(),
            configured_locator,
        })
    }

    /// Returns the provider namespace.
    pub const fn provider(&self) -> &SourceIdentifier {
        &self.provider
    }

    /// Returns the root-registered source identity.
    pub const fn source_id(&self) -> &SourceIdentifier {
        &self.source_id
    }

    /// Returns the exact root source-contract revision.
    pub const fn metadata_revision(&self) -> &SourceIdentifier {
        &self.metadata_revision
    }

    /// Returns the code-owned dataset namespace.
    pub const fn dataset(&self) -> &SourceIdentifier {
        &self.dataset
    }

    /// Returns the independently clocked provider file family.
    pub const fn family(&self) -> NasdaqDirectoryKind {
        self.family
    }

    /// Returns the exact configured official locator.
    pub const fn configured_locator(&self) -> &SourceIdentifier {
        &self.configured_locator
    }

    /// Returns the exact final response locator after the client's no-redirect check.
    pub const fn final_locator(&self) -> &SourceIdentifier {
        &self.final_locator
    }
}

/// Cooperative cancellation and monotonic deadline shared by capture and typed reads.
pub(crate) struct NasdaqReferenceOperationControl {
    cancellation: CancellationToken,
    worker_cancellation: Option<CancellationToken>,
    deadline: Instant,
    authority: Option<ExtractionAuthority>,
    authority_checkpoint_counter: AtomicUsize,
}

impl NasdaqReferenceOperationControl {
    pub(crate) fn try_new(
        deadline: Timestamp,
        cancellation: &CancellationToken,
    ) -> Result<Self, NasdaqReferenceError> {
        Self::try_new_inner(deadline, cancellation, None, None)
    }

    pub(crate) fn try_new_for_source(
        deadline: Timestamp,
        cancellation: &CancellationToken,
        authority: &ExtractionAuthority,
    ) -> Result<Self, NasdaqReferenceError> {
        Self::try_new_inner(deadline, cancellation, None, Some(authority.clone()))
    }

    fn try_new_for_source_worker(
        deadline: Timestamp,
        cancellation: &CancellationToken,
        worker_cancellation: &CancellationToken,
        authority: &ExtractionAuthority,
    ) -> Result<Self, NasdaqReferenceError> {
        Self::try_new_inner(
            deadline,
            cancellation,
            Some(worker_cancellation.clone()),
            Some(authority.clone()),
        )
    }

    fn try_new_inner(
        deadline: Timestamp,
        cancellation: &CancellationToken,
        worker_cancellation: Option<CancellationToken>,
        authority: Option<ExtractionAuthority>,
    ) -> Result<Self, NasdaqReferenceError> {
        if cancellation.is_cancelled()
            || worker_cancellation
                .as_ref()
                .is_some_and(CancellationToken::is_cancelled)
        {
            return Err(NasdaqReferenceError::Cancelled);
        }
        let now = trusted_system_timestamp()?;
        let remaining = deadline
            .unix_nanos()
            .checked_sub(now.unix_nanos())
            .and_then(|value| u64::try_from(value).ok())
            .filter(|value| *value > 0)
            .map(Duration::from_nanos)
            .ok_or(NasdaqReferenceError::DeadlineExceeded)?;
        let deadline = Instant::now()
            .checked_add(remaining)
            .ok_or(NasdaqReferenceError::DeadlineExceeded)?;
        Ok(Self {
            cancellation: cancellation.clone(),
            worker_cancellation,
            deadline,
            authority,
            authority_checkpoint_counter: AtomicUsize::new(0),
        })
    }

    pub(crate) fn checkpoint(&self) -> Result<(), NasdaqReferenceError> {
        self.checkpoint_local()?;
        if self.authority.is_some()
            && self
                .authority_checkpoint_counter
                .fetch_add(1, Ordering::Relaxed)
                .is_multiple_of(SOURCE_AUTHORITY_CHECKPOINT_INTERVAL)
        {
            self.checkpoint_authority()?;
        }
        Ok(())
    }

    fn checkpoint_strict(&self) -> Result<(), NasdaqReferenceError> {
        self.checkpoint_local()?;
        self.checkpoint_authority()
    }

    fn checkpoint_local(&self) -> Result<(), NasdaqReferenceError> {
        if self.cancellation.is_cancelled()
            || self
                .worker_cancellation
                .as_ref()
                .is_some_and(CancellationToken::is_cancelled)
        {
            Err(NasdaqReferenceError::Cancelled)
        } else if Instant::now() >= self.deadline {
            Err(NasdaqReferenceError::DeadlineExceeded)
        } else {
            Ok(())
        }
    }

    fn checkpoint_authority(&self) -> Result<(), NasdaqReferenceError> {
        if self
            .authority
            .as_ref()
            .is_some_and(|authority| authority.validate_current().is_err())
        {
            Err(NasdaqReferenceError::ControlUnavailable)
        } else {
            Ok(())
        }
    }
}

/// Source-owned hard admission for final logical-object verification workers.
#[derive(Clone, Debug)]
pub(crate) struct NasdaqReferenceBlockingAdmission {
    permits: Arc<Semaphore>,
}

impl NasdaqReferenceBlockingAdmission {
    pub(crate) fn new() -> Self {
        Self {
            permits: Arc::new(Semaphore::new(FINAL_VERIFICATION_WORKERS_PER_SOURCE)),
        }
    }

    fn try_acquire(
        &self,
        control: &NasdaqReferenceOperationControl,
    ) -> Result<OwnedSemaphorePermit, NasdaqReferenceError> {
        control.checkpoint_strict()?;
        let permit = Arc::clone(&self.permits)
            .try_acquire_owned()
            .map_err(|_| NasdaqReferenceError::BlockingWorkerCapacityUnavailable)?;
        control.checkpoint_strict()?;
        Ok(permit)
    }
}

/// Async lifecycle owner whose detached supervisor always reaps its one blocking task.
struct ReapedBlockingVerification {
    terminal: oneshot::Receiver<Result<NasdaqConsumedReferenceHandoff, NasdaqReferenceError>>,
    cancellation: CancellationToken,
    active: bool,
}

impl ReapedBlockingVerification {
    fn spawn(
        pending: NasdaqPendingReferenceHandoff,
        permit: OwnedSemaphorePermit,
        control: Arc<NasdaqReferenceOperationControl>,
        cancellation: CancellationToken,
    ) -> Result<Self, NasdaqReferenceError> {
        let runtime = tokio::runtime::Handle::try_current()
            .map_err(|_| NasdaqReferenceError::BlockingWorkerUnavailable)?;
        let (sender, terminal) = oneshot::channel();
        let worker_control = Arc::clone(&control);
        std::mem::drop(runtime.spawn(async move {
            let joined = tokio::task::spawn_blocking(move || {
                let _permit = permit;
                pending.reverify_with_control(worker_control.as_ref())
            })
            .await;
            let result = match joined {
                Ok(result) => result,
                Err(_) => Err(NasdaqReferenceError::BlockingWorkerUnavailable),
            };
            let _ = sender.send(result);
        }));
        Ok(Self {
            terminal,
            cancellation,
            active: true,
        })
    }

    async fn complete(
        mut self,
        control: &NasdaqReferenceOperationControl,
    ) -> Result<NasdaqConsumedReferenceHandoff, NasdaqReferenceError> {
        loop {
            tokio::select! {
                biased;
                result = &mut self.terminal => {
                    let result = match result {
                        Ok(result) => {
                            self.active = false;
                            control.checkpoint_strict()?;
                            result
                        }
                        Err(_) => {
                            self.cancellation.cancel();
                            self.active = false;
                            Err(NasdaqReferenceError::BlockingWorkerUnavailable)
                        }
                    };
                    return result;
                }
                () = tokio::time::sleep(BLOCKING_WORKER_CONTROL_POLL_INTERVAL) => {
                    if let Err(error) = control.checkpoint_strict() {
                        self.cancellation.cancel();
                        let _ = (&mut self.terminal).await;
                        self.active = false;
                        return Err(error);
                    }
                }
            }
        }
    }
}

impl Drop for ReapedBlockingVerification {
    fn drop(&mut self) {
        if self.active {
            self.cancellation.cancel();
        }
    }
}

impl ResearchObjectControl for NasdaqReferenceOperationControl {
    fn checkpoint(
        &self,
        _point: ResearchObjectControlPoint,
    ) -> Result<(), ResearchObjectControlError> {
        match self.checkpoint_strict() {
            Ok(()) => Ok(()),
            Err(NasdaqReferenceError::Cancelled) => Err(ResearchObjectControlError::Cancelled),
            Err(NasdaqReferenceError::DeadlineExceeded) => {
                Err(ResearchObjectControlError::DeadlineExceeded)
            }
            Err(_) => Err(ResearchObjectControlError::Unavailable),
        }
    }
}

/// Returns the platform logical-object limits for one provider family.
pub(crate) fn logical_object_admission(
    family: NasdaqDirectoryKind,
) -> Result<ResearchObjectAdmission, NasdaqReferenceError> {
    const PLATFORM_INTEGRITY_CHUNK_BYTES: u64 = 16 * 1024 * 1024;
    let maximum_bytes = u64::try_from(family.maximum_source_bytes())
        .map_err(|_| NasdaqReferenceError::BodyTooLarge)?;
    let maximum_chunks = usize::try_from(maximum_bytes.div_ceil(PLATFORM_INTEGRITY_CHUNK_BYTES))
        .map_err(|_| NasdaqReferenceError::BodyTooLarge)?;
    ResearchObjectAdmission::try_new(maximum_bytes, maximum_chunks).map_err(Into::into)
}

/// Exact raw/parser/clock/index evidence for one provider-native generation.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct NasdaqReferenceGenerationEvidence {
    schema_version: u16,
    file_identity: NasdaqReferenceFileIdentity,
    generation_identity: EvidenceDigest,
    evidence_binding_digest: EvidenceDigest,
    raw_content_digest: EvidenceDigest,
    raw_payload_bytes: u64,
    native_schema: SourceIdentifier,
    parsed_records: u64,
    rejected_records: u64,
    completeness: NasdaqReferenceCompleteness,
    currentness: NasdaqReferenceCurrentnessDisposition,
    file_creation_time: NasdaqFileCreationTime,
    source_last_modified_at: Timestamp,
    first_observed_at: Timestamp,
    response_evidence: NasdaqHttpResponseEvidence,
    provider_key_index_digest: EvidenceDigest,
}

impl NasdaqReferenceGenerationEvidence {
    #[allow(
        clippy::too_many_arguments,
        reason = "every independent source and parse coordinate is bound explicitly"
    )]
    fn try_new(
        file_identity: NasdaqReferenceFileIdentity,
        raw_content_digest: EvidenceDigest,
        raw_payload_bytes: u64,
        native_schema: SourceIdentifier,
        parsed_records: u64,
        file_creation_time: NasdaqFileCreationTime,
        response_evidence: NasdaqHttpResponseEvidence,
        provider_key_index_digest: EvidenceDigest,
    ) -> Result<Self, NasdaqReferenceError> {
        if raw_content_digest.algorithm() != DigestAlgorithm::Sha256
            || raw_content_digest.bytes() == [0; 32]
            || raw_payload_bytes == 0
            || raw_payload_bytes > file_identity.family.maximum_source_bytes() as u64
            || parsed_records == 0
            || parsed_records > file_identity.family.maximum_records()
            || provider_key_index_digest.algorithm() != DigestAlgorithm::Sha256
            || provider_key_index_digest.bytes() == [0; 32]
            || response_evidence
                .declared_content_length
                .is_some_and(|declared| declared != raw_payload_bytes)
        {
            return Err(NasdaqReferenceError::InvalidGenerationEvidence);
        }
        response_evidence.validate()?;
        let mut value = Self {
            schema_version: 1,
            file_identity,
            generation_identity: raw_content_digest,
            evidence_binding_digest: EvidenceDigest::new(DigestAlgorithm::Sha256, [0; 32]),
            raw_content_digest,
            raw_payload_bytes,
            native_schema,
            parsed_records,
            rejected_records: 0,
            completeness: NasdaqReferenceCompleteness::StrictObjectComplete,
            currentness:
                NasdaqReferenceCurrentnessDisposition::RequiresApplicationFreshnessClassification,
            file_creation_time,
            source_last_modified_at: response_evidence.last_modified_at,
            first_observed_at: response_evidence.received_at,
            response_evidence,
            provider_key_index_digest,
        };
        value.evidence_binding_digest = value.expected_evidence_binding();
        if value.evidence_binding_digest.bytes() == [0; 32] {
            return Err(NasdaqReferenceError::InvalidGenerationEvidence);
        }
        Ok(value)
    }

    fn expected_evidence_binding(&self) -> EvidenceDigest {
        let mut hash = Sha256::new();
        hash.update(b"market-squawk/nasdaq-reference-generation/v2");
        hash_generation_field(&mut hash, self.file_identity.provider.as_str().as_bytes());
        hash_generation_field(&mut hash, self.file_identity.source_id.as_str().as_bytes());
        hash_generation_field(
            &mut hash,
            self.file_identity.metadata_revision.as_str().as_bytes(),
        );
        hash_generation_field(&mut hash, self.file_identity.dataset.as_str().as_bytes());
        hash.update([family_tag(self.file_identity.family)]);
        hash_generation_field(
            &mut hash,
            self.file_identity.configured_locator.as_str().as_bytes(),
        );
        hash_generation_field(
            &mut hash,
            self.file_identity.final_locator.as_str().as_bytes(),
        );
        hash.update(self.raw_content_digest.bytes());
        hash.update(self.raw_payload_bytes.to_be_bytes());
        hash_generation_field(&mut hash, self.native_schema.as_str().as_bytes());
        hash.update(self.parsed_records.to_be_bytes());
        hash.update(self.rejected_records.to_be_bytes());
        hash.update([completeness_tag(self.completeness)]);
        hash.update([currentness_tag(self.currentness)]);
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
        hash.update(self.provider_key_index_digest.bytes());
        EvidenceDigest::new(DigestAlgorithm::Sha256, hash.finalize().into())
    }

    /// Returns the complete provider/file identity.
    pub const fn file_identity(&self) -> &NasdaqReferenceFileIdentity {
        &self.file_identity
    }

    /// Returns the exact raw-body generation identity.
    pub const fn generation_identity(&self) -> EvidenceDigest {
        self.generation_identity
    }

    /// Returns the binding over file, raw, parse, clock, response, and derived-index evidence.
    pub const fn evidence_binding_digest(&self) -> EvidenceDigest {
        self.evidence_binding_digest
    }

    /// Returns the exact SHA-256 of the provider-native body.
    pub const fn raw_content_digest(&self) -> EvidenceDigest {
        self.raw_content_digest
    }

    /// Returns the exact provider-native body length.
    pub const fn raw_payload_bytes(&self) -> u64 {
        self.raw_payload_bytes
    }

    /// Returns the closed provider-native decoder schema.
    pub const fn native_schema(&self) -> &SourceIdentifier {
        &self.native_schema
    }

    /// Returns all rows admitted by complete strict validation.
    pub const fn parsed_records(&self) -> u64 {
        self.parsed_records
    }

    /// Returns rows rejected inside an admitted generation.
    ///
    /// This is always zero because any invalid row rejects the complete object.
    pub const fn rejected_records(&self) -> u64 {
        self.rejected_records
    }

    /// Returns complete-object rather than partial-success semantics.
    pub const fn completeness(&self) -> NasdaqReferenceCompleteness {
        self.completeness
    }

    /// Returns the explicit application-owned currentness classification requirement.
    pub const fn currentness(&self) -> NasdaqReferenceCurrentnessDisposition {
        self.currentness
    }

    /// Returns the provider file-creation coordinate without inventing a time zone.
    pub const fn file_creation_time(&self) -> &NasdaqFileCreationTime {
        &self.file_creation_time
    }

    /// Returns the exact HTTP Last-Modified clock.
    pub const fn source_last_modified_at(&self) -> Timestamp {
        self.source_last_modified_at
    }

    /// Returns the local socket-complete observation clock.
    pub const fn first_observed_at(&self) -> Timestamp {
        self.first_observed_at
    }

    /// Returns exact status, media, length, validator, latency, and clocks.
    pub const fn response_evidence(&self) -> &NasdaqHttpResponseEvidence {
        &self.response_evidence
    }

    /// Returns the deterministic digest of the checked in-memory provider-key coordinates.
    pub const fn provider_key_index_digest(&self) -> EvidenceDigest {
        self.provider_key_index_digest
    }
}

/// Validation-only diagnostic evidence; it makes no freshness or catalog-publication claim.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct NasdaqReferenceDoctorReport {
    generation: NasdaqReferenceGenerationEvidence,
    same_verified_descriptor_completely_validated: bool,
    provider_key_index_reconciled: bool,
}

impl NasdaqReferenceDoctorReport {
    /// Returns the exact generation inspected.
    pub const fn generation(&self) -> &NasdaqReferenceGenerationEvidence {
        &self.generation
    }

    /// Returns the explicit application-owned currentness classification requirement.
    pub const fn currentness(&self) -> NasdaqReferenceCurrentnessDisposition {
        self.generation.currentness
    }

    /// Returns whether one opaque verified descriptor supplied every validated byte.
    pub const fn same_verified_descriptor_completely_validated(&self) -> bool {
        self.same_verified_descriptor_completely_validated
    }

    /// Returns whether every provider key, row number, and byte offset reconciled.
    pub const fn provider_key_index_reconciled(&self) -> bool {
        self.provider_key_index_reconciled
    }
}

/// Noncloneable, non-Serde provider-native handoff over one opaque verified logical object.
///
/// Typed reads remain bounded and fallible. Consuming this value only re-verifies raw storage
/// authority; downstream code still owns canonical identity, point-in-time selection, catalog
/// publication, and application durability.
pub struct NasdaqPendingReferenceHandoff {
    raw_object: VerifiedResearchObject,
    blocking_admission: NasdaqReferenceBlockingAdmission,
    generation: NasdaqReferenceGenerationEvidence,
    file_creation_time: NasdaqFileCreationTime,
    response_evidence: NasdaqHttpResponseEvidence,
    raw_content_digest: EvidenceDigest,
    raw_payload_bytes: u64,
    native_schema: SourceIdentifier,
    record_count: u64,
    first_data_offset: u64,
    query_index: Vec<ReferenceIndexEntry>,
    row_offsets: Vec<u64>,
}

impl std::fmt::Debug for NasdaqPendingReferenceHandoff {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("NasdaqPendingReferenceHandoff")
            .field("file_identity", self.generation.file_identity())
            .field("raw_content_digest", &self.raw_content_digest)
            .field("raw_payload_bytes", &self.raw_payload_bytes)
            .field("record_count", &self.record_count)
            .finish_non_exhaustive()
    }
}

impl NasdaqPendingReferenceHandoff {
    pub(crate) fn try_from_verified(
        file_identity: NasdaqReferenceFileIdentity,
        response_evidence: NasdaqHttpResponseEvidence,
        mut raw_object: VerifiedResearchObject,
        blocking_admission: NasdaqReferenceBlockingAdmission,
        control: &NasdaqReferenceOperationControl,
    ) -> Result<Self, NasdaqReferenceError> {
        control.checkpoint()?;
        response_evidence.validate()?;
        let family = file_identity.family;
        let raw_content_digest = raw_object.content_digest();
        let raw_payload_bytes = raw_object.size_bytes();
        if raw_payload_bytes == 0
            || raw_payload_bytes > family.maximum_source_bytes() as u64
            || response_evidence
                .declared_content_length
                .is_some_and(|declared| declared != raw_payload_bytes)
        {
            return Err(NasdaqReferenceError::InvalidObjectReceipt);
        }
        let scan = scan_and_validate(
            &mut raw_object,
            family,
            raw_payload_bytes,
            raw_content_digest,
            control,
        )?;
        validate_footer_clock(
            &scan.file_creation_time,
            response_evidence.last_modified_at(),
        )?;
        validate_all_typed_records(
            &mut raw_object,
            family,
            scan.first_data_offset,
            scan.record_count,
            &scan.file_creation_time,
            &response_evidence,
            raw_content_digest,
            control,
        )?;
        let native_schema = SourceIdentifier::try_from(family.native_schema_name())
            .map_err(|_| NasdaqReferenceError::InvalidSchema)?;
        let provider_key_index_digest = provider_key_index_digest(
            family,
            raw_content_digest,
            raw_payload_bytes,
            &native_schema,
            &scan.index_entries,
            control,
        )?;
        let generation = NasdaqReferenceGenerationEvidence::try_new(
            file_identity,
            raw_content_digest,
            raw_payload_bytes,
            native_schema.clone(),
            scan.record_count,
            scan.file_creation_time.clone(),
            response_evidence.clone(),
            provider_key_index_digest,
        )?;
        raw_object.seek(SeekFrom::Start(0))?;
        control.checkpoint()?;
        Ok(Self {
            raw_object,
            blocking_admission,
            generation,
            file_creation_time: scan.file_creation_time,
            response_evidence,
            raw_content_digest,
            raw_payload_bytes,
            native_schema,
            record_count: scan.record_count,
            first_data_offset: scan.first_data_offset,
            query_index: scan.index_entries,
            row_offsets: scan.row_offsets,
        })
    }

    /// Returns the complete provider/file/raw/parser/clock evidence.
    pub const fn generation_evidence(&self) -> &NasdaqReferenceGenerationEvidence {
        &self.generation
    }

    /// Returns the exact validated provider-row count.
    pub const fn record_count(&self) -> u64 {
        self.record_count
    }

    /// Returns a truthful validation-only diagnostic report.
    pub fn validation_report(&self) -> NasdaqReferenceDoctorReport {
        NasdaqReferenceDoctorReport {
            generation: self.generation.clone(),
            same_verified_descriptor_completely_validated: true,
            provider_key_index_reconciled: true,
        }
    }

    /// Starts a bounded typed decode at the first provider row.
    pub fn first_page(
        &self,
        max_records: NonZeroU32,
    ) -> Result<NasdaqReferencePageRequest, NasdaqReferenceError> {
        NasdaqReferencePageRequest::first(self, max_records)
    }

    /// Resumes only a digest-bound cursor reconciled to this handoff's checked row coordinates.
    pub fn resume_page(
        &self,
        cursor: &NasdaqReferencePageCursor,
        max_records: NonZeroU32,
    ) -> Result<NasdaqReferencePageRequest, NasdaqReferenceError> {
        cursor.validate()?;
        if cursor.family != self.generation.file_identity.family
            || cursor.content_digest != self.raw_content_digest
            || cursor.native_schema != self.native_schema
            || cursor.next_row_number < 2
            || cursor.next_byte_offset < self.first_data_offset
            || cursor.next_byte_offset >= self.raw_payload_bytes
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

    /// Decodes one bounded page from the same opaque descriptor validated at construction.
    pub fn decode_page(
        &mut self,
        request: &NasdaqReferencePageRequest,
        deadline: Timestamp,
        cancellation: &CancellationToken,
    ) -> Result<NasdaqReferencePage, NasdaqReferenceError> {
        let control = NasdaqReferenceOperationControl::try_new(deadline, cancellation)?;
        control.checkpoint()?;
        if request.family != self.generation.file_identity.family
            || request.content_digest != self.raw_content_digest
            || request.native_schema != self.native_schema
            || request.max_records.get() > MAX_REFERENCE_PAGE_RECORDS
            || request.first_row_number < 2
            || request.byte_offset < self.first_data_offset
            || request.byte_offset >= self.raw_payload_bytes
            || request
                .first_row_number
                .checked_sub(2)
                .and_then(|ordinal| usize::try_from(ordinal).ok())
                .and_then(|ordinal| self.row_offsets.get(ordinal))
                != Some(&request.byte_offset)
        {
            return Err(NasdaqReferenceError::CrossGenerationCursor);
        }

        self.raw_object.seek(SeekFrom::Start(request.byte_offset))?;
        let file_creation_time = self.file_creation_time.clone();
        let response_evidence = self.response_evidence.clone();
        let raw_content_digest = self.raw_content_digest;
        let mut records = Vec::new();
        records
            .try_reserve_exact(request.max_records.get() as usize)
            .map_err(|_| NasdaqReferenceError::AllocationFailed)?;
        let mut reader = BufReader::with_capacity(READ_CHUNK_BYTES, &mut self.raw_object);
        let mut line = Vec::with_capacity(MAX_LINE_BYTES);
        let mut row_number = request.first_row_number;
        let mut byte_offset = request.byte_offset;
        let mut reached_footer = false;
        while records.len() < request.max_records.get() as usize {
            control.checkpoint()?;
            let read = read_line(&mut reader, &mut line, row_number)?;
            if read == 0 {
                return Err(NasdaqReferenceError::MissingFooter);
            }
            let text = normalized_line(&line, row_number)?;
            if text.starts_with(FILE_CREATION_PREFIX) {
                let footer = parse_footer(request.family, text, row_number)?;
                if footer != self.file_creation_time {
                    return Err(NasdaqReferenceError::GenerationMismatch);
                }
                reached_footer = true;
                break;
            }
            records.push(NasdaqReferenceIdentityCandidate::try_from_record(
                parse_typed_record(
                    request.family,
                    &file_creation_time,
                    &response_evidence,
                    raw_content_digest,
                    row_number,
                    text,
                )?,
            )?);
            byte_offset = byte_offset
                .checked_add(u64::try_from(read).map_err(|_| NasdaqReferenceError::BodyTooLarge)?)
                .ok_or(NasdaqReferenceError::BodyTooLarge)?;
            row_number = row_number
                .checked_add(1)
                .ok_or(NasdaqReferenceError::RecordLimitExceeded)?;
        }
        if records.is_empty() {
            return Err(NasdaqReferenceError::InvalidProviderRow);
        }
        let next_cursor = if reached_footer {
            None
        } else {
            control.checkpoint()?;
            let next_byte_offset = byte_offset;
            let read = read_line(&mut reader, &mut line, row_number)?;
            if read == 0 {
                return Err(NasdaqReferenceError::MissingFooter);
            }
            let text = normalized_line(&line, row_number)?;
            if text.starts_with(FILE_CREATION_PREFIX) {
                let footer = parse_footer(request.family, text, row_number)?;
                if footer != self.file_creation_time {
                    return Err(NasdaqReferenceError::GenerationMismatch);
                }
                None
            } else {
                validate_provider_row(request.family, text)?;
                Some(NasdaqReferencePageCursor::try_new(
                    self,
                    row_number,
                    next_byte_offset,
                )?)
            }
        };
        control.checkpoint()?;
        Ok(NasdaqReferencePage {
            generation: self.generation.clone(),
            first_row_number: request.first_row_number,
            request_cursor_identity: request.cursor_identity,
            records,
            next_cursor,
        })
    }

    /// Resolves one exact provider-native key without crossing into canonical identity.
    pub fn query(
        &mut self,
        query: &NasdaqReferenceQuery,
        deadline: Timestamp,
        cancellation: &CancellationToken,
    ) -> Result<NasdaqReferenceQueryResult, NasdaqReferenceError> {
        let control = NasdaqReferenceOperationControl::try_new(deadline, cancellation)?;
        control.checkpoint()?;
        if query.family() != self.generation.file_identity.family
            || u64::try_from(self.query_index.len()).ok() != Some(self.record_count)
        {
            return Err(NasdaqReferenceError::InvalidQuery);
        }
        let target = query.key_digest();
        let low = self.query_index.partition_point(|entry| entry.key < target);
        let one = NonZeroU32::new(1).ok_or(NasdaqReferenceError::PageLimitExceeded)?;
        let mut matches = Vec::new();
        let mut ordinal = low;
        while ordinal < self.query_index.len() {
            control.checkpoint()?;
            let entry = self.query_index[ordinal];
            if entry.key != target {
                break;
            }
            if ordinal.saturating_sub(low) >= MAX_QUERY_CONFLICTS {
                return Err(NasdaqReferenceError::QueryConflictLimitExceeded);
            }
            let request = NasdaqReferencePageRequest::try_new(
                self,
                entry.row_number,
                entry.byte_offset,
                one,
                None,
            )?;
            let page = self.decode_page(&request, deadline, cancellation)?;
            if let Some(record) = page.records.into_iter().next()
                && query.matches(&record)
            {
                matches
                    .try_reserve(1)
                    .map_err(|_| NasdaqReferenceError::AllocationFailed)?;
                matches.push(record);
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
        control.checkpoint()?;
        Ok(NasdaqReferenceQueryResult {
            generation: self.generation.clone(),
            disposition,
            matches,
        })
    }

    /// Consumes the pending typed handoff and re-verifies the same opaque raw descriptor.
    ///
    /// The returned logical-object receipt is raw-storage authority only. It is not a catalog,
    /// point-in-time, application-durability, or canonical-identity publication receipt.
    pub async fn into_reverified_handoff(
        self,
        authority: &ExtractionAuthority,
        deadline: Timestamp,
        cancellation: &CancellationToken,
    ) -> Result<NasdaqConsumedReferenceHandoff, NasdaqReferenceError> {
        authority
            .validate_current()
            .map_err(|_| NasdaqReferenceError::ControlUnavailable)?;
        if authority.metadata().source_id().as_str()
            != self.generation.file_identity.source_id.as_str()
            || authority
                .metadata()
                .revision()
                .as_source_identifier()
                .as_str()
                != self.generation.file_identity.metadata_revision.as_str()
        {
            return Err(NasdaqReferenceError::ControlUnavailable);
        }
        let worker_cancellation = CancellationToken::new();
        let control = Arc::new(NasdaqReferenceOperationControl::try_new_for_source_worker(
            deadline,
            cancellation,
            &worker_cancellation,
            authority,
        )?);
        let permit = self.blocking_admission.try_acquire(control.as_ref())?;
        let worker = ReapedBlockingVerification::spawn(
            self,
            permit,
            Arc::clone(&control),
            worker_cancellation,
        )?;
        worker.complete(control.as_ref()).await
    }

    fn reverify_with_control(
        self,
        control: &NasdaqReferenceOperationControl,
    ) -> Result<NasdaqConsumedReferenceHandoff, NasdaqReferenceError> {
        control.checkpoint_strict()?;
        let generation = self.generation;
        let receipt = self.raw_object.reverify_for_commit(control)?;
        control.checkpoint_strict()?;
        if receipt.content_digest() != generation.raw_content_digest
            || receipt.size_bytes() != generation.raw_payload_bytes
        {
            return Err(NasdaqReferenceError::GenerationMismatch);
        }
        NasdaqConsumedReferenceHandoff::try_new(generation, receipt)
    }
}

/// Consumed, reverified raw-storage handoff. This is not publication evidence.
pub struct NasdaqConsumedReferenceHandoff {
    generation: NasdaqReferenceGenerationEvidence,
    logical_object_receipt: ResearchObjectReceipt,
    handoff_binding_digest: EvidenceDigest,
}

impl std::fmt::Debug for NasdaqConsumedReferenceHandoff {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("NasdaqConsumedReferenceHandoff")
            .field("generation", &self.generation)
            .field("logical_object_receipt", &self.logical_object_receipt)
            .field("handoff_binding_digest", &self.handoff_binding_digest)
            .finish()
    }
}

impl NasdaqConsumedReferenceHandoff {
    fn try_new(
        generation: NasdaqReferenceGenerationEvidence,
        logical_object_receipt: ResearchObjectReceipt,
    ) -> Result<Self, NasdaqReferenceError> {
        if logical_object_receipt.content_digest() != generation.raw_content_digest
            || logical_object_receipt.size_bytes() != generation.raw_payload_bytes
        {
            return Err(NasdaqReferenceError::GenerationMismatch);
        }
        let mut hash = Sha256::new();
        hash.update(b"market-squawk/nasdaq-reference-consumed-handoff/v1");
        hash.update(generation.evidence_binding_digest.bytes());
        hash.update(logical_object_receipt.content_digest().bytes());
        hash.update(logical_object_receipt.size_bytes().to_be_bytes());
        hash.update(
            logical_object_receipt
                .claim()
                .physical_receipt_digest()
                .bytes(),
        );
        let handoff_binding_digest =
            EvidenceDigest::new(DigestAlgorithm::Sha256, hash.finalize().into());
        Ok(Self {
            generation,
            logical_object_receipt,
            handoff_binding_digest,
        })
    }

    /// Returns the exact typed generation evidence.
    pub const fn generation(&self) -> &NasdaqReferenceGenerationEvidence {
        &self.generation
    }

    /// Returns the non-forgeable platform raw-storage receipt.
    pub const fn logical_object_receipt(&self) -> &ResearchObjectReceipt {
        &self.logical_object_receipt
    }

    /// Returns the binding between typed evidence and the physical logical-object receipt.
    pub const fn handoff_binding_digest(&self) -> EvidenceDigest {
        self.handoff_binding_digest
    }

    /// Consumes the adapter receipt into typed evidence, raw-storage authority, and their binding.
    ///
    /// None of these values asserts canonical publication or application durability.
    pub fn into_parts(
        self,
    ) -> (
        NasdaqReferenceGenerationEvidence,
        ResearchObjectReceipt,
        EvidenceDigest,
    ) {
        (
            self.generation,
            self.logical_object_receipt,
            self.handoff_binding_digest,
        )
    }
}

/// Opaque cursor bound to one exact raw generation and native schema.
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
        object: &NasdaqPendingReferenceHandoff,
        next_row_number: u64,
        next_byte_offset: u64,
    ) -> Result<Self, NasdaqReferenceError> {
        let mut value = Self {
            schema_version: 1,
            family: object.generation.file_identity.family,
            content_digest: object.raw_content_digest,
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
        hash.update(b"market-squawk/nasdaq-reference-page-cursor/v2");
        hash.update([family_tag(self.family)]);
        hash.update(self.content_digest.bytes());
        hash_generation_field(&mut hash, self.native_schema.as_str().as_bytes());
        hash.update(self.next_row_number.to_be_bytes());
        hash.update(self.next_byte_offset.to_be_bytes());
        EvidenceDigest::new(DigestAlgorithm::Sha256, hash.finalize().into())
    }

    /// Returns the next one-based provider row number.
    pub const fn next_row_number(&self) -> u64 {
        self.next_row_number
    }

    /// Returns the exact raw generation.
    pub const fn content_digest(&self) -> EvidenceDigest {
        self.content_digest
    }

    /// Returns the provider-native decoder schema.
    pub const fn native_schema(&self) -> &SourceIdentifier {
        &self.native_schema
    }

    /// Returns the exact row-start byte offset.
    pub const fn next_byte_offset(&self) -> u64 {
        self.next_byte_offset
    }

    /// Returns the cursor binding identity.
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
        object: &NasdaqPendingReferenceHandoff,
        max_records: NonZeroU32,
    ) -> Result<Self, NasdaqReferenceError> {
        Self::try_new(object, 2, object.first_data_offset, max_records, None)
    }

    fn try_new(
        object: &NasdaqPendingReferenceHandoff,
        first_row_number: u64,
        byte_offset: u64,
        max_records: NonZeroU32,
        cursor_identity: Option<EvidenceDigest>,
    ) -> Result<Self, NasdaqReferenceError> {
        if max_records.get() > MAX_REFERENCE_PAGE_RECORDS {
            return Err(NasdaqReferenceError::PageLimitExceeded);
        }
        Ok(Self {
            family: object.generation.file_identity.family,
            content_digest: object.raw_content_digest,
            native_schema: object.native_schema.clone(),
            first_row_number,
            byte_offset,
            max_records,
            cursor_identity,
        })
    }
}

/// Bounded typed page retaining exact raw-generation evidence.
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
    /// Returns the exact generation for every returned candidate.
    pub const fn generation(&self) -> &NasdaqReferenceGenerationEvidence {
        &self.generation
    }

    /// Returns the exact provider family.
    pub const fn family(&self) -> NasdaqDirectoryKind {
        self.generation.file_identity.family
    }

    /// Returns the raw generation identity.
    pub const fn content_digest(&self) -> EvidenceDigest {
        self.generation.raw_content_digest
    }

    /// Returns the one-based provider row number of the first candidate.
    pub const fn first_row_number(&self) -> u64 {
        self.first_row_number
    }

    /// Returns bounded provider-native candidates in source order.
    pub fn records(&self) -> &[NasdaqReferenceIdentityCandidate] {
        &self.records
    }

    /// Returns the cursor identity that produced this page.
    pub const fn request_cursor_identity(&self) -> Option<EvidenceDigest> {
        self.request_cursor_identity
    }

    /// Returns a generation-bound continuation, if another row remains.
    pub const fn next_cursor(&self) -> Option<&NasdaqReferencePageCursor> {
        self.next_cursor.as_ref()
    }
}

/// Exact provider-namespace query; names are deliberately not keys.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum NasdaqReferenceQuery {
    /// One provider symbol in one exact equity-directory namespace.
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
    /// One exact Nasdaq option tuple. This is not an OCC/OSI identity.
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

    /// Constructs a provider-native option tuple without minting OCC/OSI identity.
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
    /// Multiple rows matched; consumers must not choose one arbitrarily.
    Ambiguous,
}

/// Bounded provider-key result retaining every admitted conflict.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct NasdaqReferenceQueryResult {
    generation: NasdaqReferenceGenerationEvidence,
    disposition: NasdaqReferenceQueryDisposition,
    matches: Vec<NasdaqReferenceIdentityCandidate>,
}

impl NasdaqReferenceQueryResult {
    /// Returns the exact generation searched.
    pub const fn generation(&self) -> &NasdaqReferenceGenerationEvidence {
        &self.generation
    }

    /// Returns the exact raw generation searched.
    pub const fn content_digest(&self) -> EvidenceDigest {
        self.generation.raw_content_digest
    }

    /// Returns exact, ambiguous, or missing.
    pub const fn disposition(&self) -> NasdaqReferenceQueryDisposition {
        self.disposition
    }

    /// Returns every exact provider candidate retained for the key.
    pub fn matches(&self) -> &[NasdaqReferenceIdentityCandidate] {
        &self.matches
    }
}

const fn family_tag(family: NasdaqDirectoryKind) -> u8 {
    match family {
        NasdaqDirectoryKind::NasdaqListed => 1,
        NasdaqDirectoryKind::OtherListed => 2,
        NasdaqDirectoryKind::Bonds => 3,
        NasdaqDirectoryKind::Options => 4,
    }
}

const fn completeness_tag(value: NasdaqReferenceCompleteness) -> u8 {
    match value {
        NasdaqReferenceCompleteness::StrictObjectComplete => 1,
    }
}

const fn currentness_tag(value: NasdaqReferenceCurrentnessDisposition) -> u8 {
    match value {
        NasdaqReferenceCurrentnessDisposition::RequiresApplicationFreshnessClassification => 1,
    }
}

struct ObjectScanReceipt {
    file_creation_time: NasdaqFileCreationTime,
    record_count: u64,
    first_data_offset: u64,
    index_entries: Vec<ReferenceIndexEntry>,
    row_offsets: Vec<u64>,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct ReferenceIndexEntry {
    key: [u8; 32],
    row_number: u64,
    byte_offset: u64,
}

fn scan_and_validate(
    file: &mut VerifiedResearchObject,
    family: NasdaqDirectoryKind,
    expected_bytes: u64,
    expected_digest: EvidenceDigest,
    control: &NasdaqReferenceOperationControl,
) -> Result<ObjectScanReceipt, NasdaqReferenceError> {
    control.checkpoint()?;
    file.seek(SeekFrom::Start(0))?;
    let mut reader = BufReader::with_capacity(READ_CHUNK_BYTES, file);
    let mut hash = Sha256::new();
    let mut observed_bytes = 0_u64;
    let mut line = Vec::with_capacity(MAX_LINE_BYTES);
    let header_bytes = read_line(&mut reader, &mut line, 1)?;
    if header_bytes == 0 {
        return Err(NasdaqReferenceError::EmptyBody);
    }
    hash.update(&line);
    observed_bytes = observed_bytes
        .checked_add(u64::try_from(header_bytes).map_err(|_| NasdaqReferenceError::BodyTooLarge)?)
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
        control.checkpoint()?;
        let byte_offset = observed_bytes;
        let read = read_line(&mut reader, &mut line, row_number)?;
        if read == 0 {
            return Err(NasdaqReferenceError::MissingFooter);
        }
        hash.update(&line);
        observed_bytes = observed_bytes
            .checked_add(u64::try_from(read).map_err(|_| NasdaqReferenceError::BodyTooLarge)?)
            .ok_or(NasdaqReferenceError::BodyTooLarge)?;
        if observed_bytes > expected_bytes || observed_bytes > family.maximum_source_bytes() as u64
        {
            return Err(NasdaqReferenceError::BodyTooLarge);
        }
        let text = normalized_line(&line, row_number)?;
        if text.starts_with(FILE_CREATION_PREFIX) {
            break parse_footer(family, text, row_number)?;
        }
        validate_provider_row(family, text)?;
        let next_count = record_count
            .checked_add(1)
            .ok_or(NasdaqReferenceError::RecordLimitExceeded)?;
        if next_count > family.maximum_records() {
            return Err(NasdaqReferenceError::RecordLimitExceeded);
        }
        let index_bytes = next_count
            .checked_mul(INDEX_ENTRY_BYTES)
            .ok_or(NasdaqReferenceError::IndexLimitExceeded)?;
        let offset_bytes = next_count
            .checked_mul(ROW_OFFSET_BYTES)
            .ok_or(NasdaqReferenceError::IndexLimitExceeded)?;
        if index_bytes > MAX_REFERENCE_INDEX_BYTES || offset_bytes > MAX_REFERENCE_ROW_OFFSET_BYTES
        {
            return Err(NasdaqReferenceError::IndexLimitExceeded);
        }
        let remaining_rows = family
            .maximum_records()
            .saturating_sub(record_count)
            .min(4_096);
        let reserve =
            usize::try_from(remaining_rows).map_err(|_| NasdaqReferenceError::AllocationFailed)?;
        if index_entries.len() == index_entries.capacity() {
            index_entries
                .try_reserve_exact(reserve)
                .map_err(|_| NasdaqReferenceError::AllocationFailed)?;
        }
        if row_offsets.len() == row_offsets.capacity() {
            row_offsets
                .try_reserve_exact(reserve)
                .map_err(|_| NasdaqReferenceError::AllocationFailed)?;
        }
        index_entries.push(ReferenceIndexEntry {
            key: provider_query_key(family, text)?,
            row_number,
            byte_offset,
        });
        row_offsets.push(byte_offset);
        record_count = next_count;
        row_number = row_number
            .checked_add(1)
            .ok_or(NasdaqReferenceError::RecordLimitExceeded)?;
    };
    if record_count == 0 {
        return Err(NasdaqReferenceError::NoRecords);
    }
    control.checkpoint()?;
    let extra = read_line(&mut reader, &mut line, row_number.saturating_add(1))?;
    if extra != 0 {
        return Err(NasdaqReferenceError::DataAfterFooter);
    }
    if observed_bytes != expected_bytes
        || expected_digest.algorithm() != DigestAlgorithm::Sha256
        || hash.finalize().as_slice() != expected_digest.bytes()
    {
        return Err(NasdaqReferenceError::LogicalObjectVerificationFailed);
    }
    control.checkpoint()?;
    index_entries.sort_unstable();
    control.checkpoint()?;
    Ok(ObjectScanReceipt {
        file_creation_time,
        record_count,
        first_data_offset,
        index_entries,
        row_offsets,
    })
}

#[allow(
    clippy::too_many_arguments,
    reason = "the complete typed pass binds every independent raw and provenance coordinate"
)]
fn validate_all_typed_records(
    file: &mut VerifiedResearchObject,
    family: NasdaqDirectoryKind,
    first_data_offset: u64,
    expected_records: u64,
    expected_file_creation_time: &NasdaqFileCreationTime,
    response_evidence: &NasdaqHttpResponseEvidence,
    raw_content_digest: EvidenceDigest,
    control: &NasdaqReferenceOperationControl,
) -> Result<(), NasdaqReferenceError> {
    control.checkpoint()?;
    file.seek(SeekFrom::Start(first_data_offset))?;
    let mut reader = BufReader::with_capacity(READ_CHUNK_BYTES, file);
    let mut line = Vec::with_capacity(MAX_LINE_BYTES);
    let mut row_number = 2_u64;
    let mut parsed_records = 0_u64;

    loop {
        control.checkpoint()?;
        let read = read_line(&mut reader, &mut line, row_number)?;
        if read == 0 {
            return Err(NasdaqReferenceError::MissingFooter);
        }
        let text = normalized_line(&line, row_number)?;
        if text.starts_with(FILE_CREATION_PREFIX) {
            if parse_footer(family, text, row_number)? != *expected_file_creation_time
                || parsed_records != expected_records
            {
                return Err(NasdaqReferenceError::GenerationMismatch);
            }
            control.checkpoint()?;
            if read_line(&mut reader, &mut line, row_number.saturating_add(1))? != 0 {
                return Err(NasdaqReferenceError::DataAfterFooter);
            }
            return Ok(());
        }
        if parsed_records >= expected_records {
            return Err(NasdaqReferenceError::GenerationMismatch);
        }
        let _ = NasdaqReferenceIdentityCandidate::try_from_record(parse_typed_record(
            family,
            expected_file_creation_time,
            response_evidence,
            raw_content_digest,
            row_number,
            text,
        )?)?;
        parsed_records = parsed_records
            .checked_add(1)
            .ok_or(NasdaqReferenceError::RecordLimitExceeded)?;
        row_number = row_number
            .checked_add(1)
            .ok_or(NasdaqReferenceError::RecordLimitExceeded)?;
    }
}

fn read_line<R: BufRead>(
    reader: &mut R,
    line: &mut Vec<u8>,
    row: u64,
) -> Result<usize, NasdaqReferenceError> {
    line.clear();
    loop {
        let available = reader.fill_buf()?;
        if available.is_empty() {
            if line.is_empty() {
                return Ok(0);
            }
            return Err(NasdaqReferenceError::UnterminatedLine { row });
        }
        let newline = available.iter().position(|byte| *byte == b'\n');
        let take = newline.map_or(available.len(), |index| index + 1);
        let next_len = line
            .len()
            .checked_add(take)
            .ok_or(NasdaqReferenceError::LineTooLong { row })?;
        if next_len > MAX_LINE_BYTES {
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

fn provider_key_index_digest(
    family: NasdaqDirectoryKind,
    raw_content_digest: EvidenceDigest,
    raw_payload_bytes: u64,
    native_schema: &SourceIdentifier,
    entries: &[ReferenceIndexEntry],
    control: &NasdaqReferenceOperationControl,
) -> Result<EvidenceDigest, NasdaqReferenceError> {
    const CONTROL_CHECKPOINT_INTERVAL: usize = 4_096;
    let mut hash = Sha256::new();
    hash.update(b"market-squawk/nasdaq-reference-provider-key-index/v2");
    hash.update([family_tag(family)]);
    hash.update(raw_content_digest.bytes());
    hash.update(raw_payload_bytes.to_be_bytes());
    hash_generation_field(&mut hash, native_schema.as_str().as_bytes());
    hash.update((entries.len() as u64).to_be_bytes());
    for (ordinal, entry) in entries.iter().enumerate() {
        if ordinal.is_multiple_of(CONTROL_CHECKPOINT_INTERVAL) {
            control.checkpoint_strict()?;
        }
        hash.update(entry.key);
        hash.update(entry.row_number.to_be_bytes());
        hash.update(entry.byte_offset.to_be_bytes());
    }
    control.checkpoint_strict()?;
    Ok(EvidenceDigest::new(
        DigestAlgorithm::Sha256,
        hash.finalize().into(),
    ))
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
    family: NasdaqDirectoryKind,
    file_creation_time: &NasdaqFileCreationTime,
    response_evidence: &NasdaqHttpResponseEvidence,
    raw_content_digest: EvidenceDigest,
    row_number: u64,
    line: &str,
) -> Result<NasdaqReferenceRecord, NasdaqReferenceError> {
    validate_provider_row(family, line)?;
    let provenance = || {
        NasdaqReferenceProvenance::new(
            file_creation_time,
            response_evidence,
            raw_content_digest,
            row_number,
        )
    };
    match family {
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
                    file_creation_time.clone(),
                    response_evidence.last_modified_at(),
                    response_evidence.received_at(),
                    ExactPayloadEvidence::from_content_digest(raw_content_digest),
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
                    file_creation_time.clone(),
                    response_evidence.last_modified_at(),
                    response_evidence.received_at(),
                    ExactPayloadEvidence::from_content_digest(raw_content_digest),
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

fn trusted_system_timestamp() -> Result<Timestamp, NasdaqReferenceError> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| NasdaqReferenceError::ClockUnavailable)?;
    let nanos = u128::from(duration.as_secs())
        .checked_mul(1_000_000_000)
        .and_then(|value| value.checked_add(u128::from(duration.subsec_nanos())))
        .and_then(|value| i64::try_from(value).ok())
        .ok_or(NasdaqReferenceError::ClockUnavailable)?;
    Ok(Timestamp::from_unix_nanos(nanos))
}
/// Logical-object capture, complete validation, typed read, or consuming-handoff failure.
#[derive(Debug, Error)]
pub enum NasdaqReferenceError {
    /// Bounded reading of the opaque platform descriptor failed.
    #[error("Nasdaq reference logical-object I/O failed: {0}")]
    Io(#[from] std::io::Error),
    /// Platform logical-object admission, verification, or storage failed.
    #[error("Nasdaq reference logical-object storage failed: {0}")]
    LogicalObject(#[source] SealedResearchJournalStoreError),
    /// Cooperative cancellation was observed before completion.
    #[error("Nasdaq reference operation cancelled")]
    Cancelled,
    /// The caller-owned operation deadline elapsed.
    #[error("Nasdaq reference operation deadline was exceeded")]
    DeadlineExceeded,
    /// Trusted wall-clock time could not be represented.
    #[error("Nasdaq reference trusted clock is unavailable")]
    ClockUnavailable,
    /// Caller-owned trusted control state was unavailable.
    #[error("Nasdaq reference operation control is unavailable")]
    ControlUnavailable,
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
    /// Logical-object size or digest evidence was malformed or inconsistent.
    #[error("Nasdaq logical-object evidence is invalid")]
    InvalidObjectReceipt,
    /// Registered source, revision, dataset, family, or official locator was invalid.
    #[error("Nasdaq provider file identity is invalid")]
    InvalidFileIdentity,
    /// No reviewed supported acquisition contract exists for this provider file family.
    #[error("Nasdaq provider file acquisition contract is unavailable")]
    ProviderContractUnavailable,
    /// Raw, parser, clock, schema, count, or index evidence could not form one generation.
    #[error("Nasdaq reference generation evidence is invalid")]
    InvalidGenerationEvidence,
    /// Same-descriptor bytes, size, or digest did not reconcile.
    #[error("Nasdaq logical raw object failed verification")]
    LogicalObjectVerificationFailed,
    /// Formatting bounded evidence identity text failed.
    #[error("Nasdaq reference evidence identity could not be formatted")]
    EvidenceFormatting,
    /// A bounded allocation failed.
    #[error("Nasdaq reference allocation failed")]
    AllocationFailed,
    /// The bounded fixed-width provider-key index exceeded its production ceiling.
    #[error("Nasdaq provider-key index exceeds its byte ceiling")]
    IndexLimitExceeded,
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
    /// Tokio could not complete the owned blocking verification worker.
    #[error("Nasdaq logical-object blocking verification worker is unavailable")]
    BlockingWorkerUnavailable,
    /// This source already owns its maximum admitted final-verification worker count.
    #[error("Nasdaq logical-object blocking verification capacity is unavailable")]
    BlockingWorkerCapacityUnavailable,
    /// The provider namespace query was malformed or addressed another family.
    #[error("Nasdaq provider-native query is invalid")]
    InvalidQuery,
    /// One provider key produced more conflicts than the bounded query result can retain.
    #[error("Nasdaq provider-native query exceeded its conflict ceiling")]
    QueryConflictLimitExceeded,
    /// Immutable state changed after whole-object validation.
    #[error("Nasdaq logical-object generation changed after validation")]
    GenerationMismatch,
}

impl From<SealedResearchJournalStoreError> for NasdaqReferenceError {
    fn from(error: SealedResearchJournalStoreError) -> Self {
        match error {
            SealedResearchJournalStoreError::ObjectControl(
                ResearchObjectControlError::Cancelled,
            ) => Self::Cancelled,
            SealedResearchJournalStoreError::ObjectControl(
                ResearchObjectControlError::DeadlineExceeded,
            ) => Self::DeadlineExceeded,
            SealedResearchJournalStoreError::ObjectControl(
                ResearchObjectControlError::Unavailable,
            ) => Self::ControlUnavailable,
            error => Self::LogicalObject(error),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::io::Write as _;
    use std::num::{NonZeroU16, NonZeroU32, NonZeroU64};

    use chrono::DateTime;
    use market_squawk_domain::{
        AssetClass, AuthorizationBasis, ChecksumCapability, CoverageDelay, DataQuality,
        DeliveryEvidence, DigestAlgorithm, EffectiveInterval, EvidenceDigest, ExactPayloadEvidence,
        MetadataRevision, RevisionBoundPayloadEvidence, SchemaVersion, SequenceCapability,
        SourceId, SourceIdentifier, VenueId,
    };
    use market_squawk_platform::LocalPaths;
    use market_squawk_sources::{
        AuthoritativeSourceRegistry, AuthorizationGrant, AuthorizationMode, BackoffPolicy,
        BudgetScope, CoverageTopology, ExtractionAuthority, FreshnessPolicy, HistoricalCapability,
        HttpRequestBounds, InstrumentCoverage, NetworkAccessPolicy, ProviderBudgetPolicy,
        SourceCapabilities, SourceClass, SourceCoverage, SourceMetadata, SourceMetadataInput,
        SourceMetadataProvider, SourceProtocolProfile,
    };
    use sha2::{Digest as _, Sha256};
    use tokio_util::sync::CancellationToken;

    use super::{NasdaqPendingReferenceHandoff, NasdaqReferenceError, NasdaqReferenceFileIdentity};

    use crate::{
        NasdaqDirectoryKind, NasdaqIdentityDisposition, NasdaqReferenceCompleteness,
        NasdaqReferenceLifecycleDisposition, NasdaqReferenceQuery, NasdaqReferenceQueryDisposition,
        NasdaqReferenceRecord, NasdaqReferenceTradabilityDisposition,
        NasdaqReferenceValidityDisposition, NasdaqSymbolDirectoryConfig,
        NasdaqSymbolDirectorySource,
    };

    const LISTING_FIXTURE: &[u8] = b"ACT Symbol|Security Name|Exchange|CQS Symbol|ETF|Round Lot Size|Test Issue|NASDAQ Symbol\r\nBRK.A|BERKSHIRE HATHAWAY INC|N|BRK.A|N|100|N|BRK-A\r\nDUPL|DUPLICATE ONE|A|DUPL|N|100|N|DUPL\r\nDUPL|DUPLICATE TEST ISSUE|P|DUPL|N|100|Y|DUPL\r\nFile Creation Time: 0813202621:32||||||\r\n";

    #[tokio::test]
    async fn logical_object_becomes_one_shot_typed_reference_handoff() -> Result<(), Box<dyn Error>>
    {
        let temporary = tempfile::tempdir()?;
        let paths = LocalPaths::prepare(temporary.path())?;
        let store = paths.sealed_research_journal_store()?;
        let cancellation = CancellationToken::new();
        let deadline = timestamp("2099-08-14T05:10:00Z")?;
        let last_modified = timestamp("2026-08-14T01:32:37Z")?;
        let first_observed = timestamp("2026-08-14T05:05:00Z")?;
        let (source, _registry, authority) = source_harness()?;
        let identity = NasdaqReferenceFileIdentity::try_new(
            source.metadata().source_id().as_str(),
            source.metadata().revision().as_source_identifier().as_str(),
            crate::NASDAQ_SYMBOL_DIRECTORY_DATASET,
            NasdaqDirectoryKind::OtherListed,
        )?;
        let admission = super::logical_object_admission(NasdaqDirectoryKind::OtherListed)?;
        let mut pending = store.begin_logical_object(admission)?;
        for chunk in LISTING_FIXTURE.chunks(17) {
            pending.write_all(chunk)?;
        }
        let control = super::NasdaqReferenceOperationControl::try_new_for_source(
            deadline,
            &cancellation,
            &authority,
        )?;
        let verified = store.finish_logical_object(pending, &control)?;
        let expected_digest = EvidenceDigest::new(
            DigestAlgorithm::Sha256,
            Sha256::digest(LISTING_FIXTURE).into(),
        );
        assert_eq!(verified.content_digest(), expected_digest);
        assert_eq!(verified.size_bytes(), LISTING_FIXTURE.len() as u64);
        let blocking_admission = source.final_verification_admission();
        let held_worker_permit = blocking_admission.try_acquire(&control)?;
        assert!(matches!(
            source.final_verification_admission().try_acquire(&control),
            Err(NasdaqReferenceError::BlockingWorkerCapacityUnavailable)
        ));
        drop(held_worker_permit);

        let mut handoff = NasdaqPendingReferenceHandoff::try_from_verified(
            identity,
            super::NasdaqHttpResponseEvidence::try_new(
                200,
                "text/plain".to_owned(),
                None,
                Some(LISTING_FIXTURE.len() as u64),
                Some("official-etag".to_owned()),
                1,
                last_modified,
                first_observed,
            )?,
            verified,
            blocking_admission,
            &control,
        )?;
        assert_eq!(handoff.record_count(), 3);
        let generation = handoff.generation_evidence().clone();
        assert_eq!(generation.raw_content_digest(), expected_digest);
        assert_eq!(generation.raw_payload_bytes(), LISTING_FIXTURE.len() as u64);
        assert_eq!(generation.parsed_records(), 3);
        assert_eq!(generation.rejected_records(), 0);
        assert_eq!(
            generation.completeness(),
            NasdaqReferenceCompleteness::StrictObjectComplete
        );
        assert_eq!(
            generation.generation_identity().algorithm(),
            DigestAlgorithm::Sha256
        );
        assert_eq!(
            generation.file_identity().configured_locator().as_str(),
            crate::OTHER_LISTED_URL
        );
        assert_eq!(
            generation.file_identity().final_locator().as_str(),
            crate::OTHER_LISTED_URL
        );

        let cancelled = CancellationToken::new();
        cancelled.cancel();
        assert!(matches!(
            handoff.query(
                &NasdaqReferenceQuery::listing(NasdaqDirectoryKind::OtherListed, "BRK.A")?,
                deadline,
                &cancelled,
            ),
            Err(NasdaqReferenceError::Cancelled)
        ));

        let exact = NasdaqReferenceQuery::listing(NasdaqDirectoryKind::OtherListed, "BRK.A")?;
        let result = handoff.query(&exact, deadline, &cancellation)?;
        assert_eq!(result.disposition(), NasdaqReferenceQueryDisposition::Exact);
        assert_eq!(result.matches().len(), 1);
        assert_eq!(
            result.generation().generation_identity(),
            generation.generation_identity()
        );
        let NasdaqReferenceRecord::Listing(listing) = result.matches()[0].provider_record() else {
            return Err("expected listing row".into());
        };
        assert_eq!(listing.primary_symbol().as_str(), "BRK.A");
        assert_eq!(
            listing.provider_fields().security_name(),
            "BERKSHIRE HATHAWAY INC"
        );
        assert_eq!(listing.provider_fields().cqs_symbol(), Some("BRK.A"));
        assert_eq!(listing.provider_fields().nasdaq_symbol(), Some("BRK-A"));
        assert!(!listing.provider_fields().is_test_issue());

        let ambiguous = NasdaqReferenceQuery::listing(NasdaqDirectoryKind::OtherListed, "DUPL")?;
        let result = handoff.query(&ambiguous, deadline, &cancellation)?;
        assert_eq!(
            result.disposition(),
            NasdaqReferenceQueryDisposition::Ambiguous
        );
        assert_eq!(result.matches().len(), 2);
        assert!(result.matches().iter().any(|candidate| {
            matches!(candidate.provider_record(), NasdaqReferenceRecord::Listing(record) if record.provider_fields().is_test_issue())
        }));
        let missing = NasdaqReferenceQuery::listing(NasdaqDirectoryKind::OtherListed, "MISSING")?;
        assert_eq!(
            handoff
                .query(&missing, deadline, &cancellation)?
                .disposition(),
            NasdaqReferenceQueryDisposition::Missing
        );

        let one = NonZeroU32::new(1).ok_or("nonzero page size")?;
        let first_request = handoff.first_page(one)?;
        let first = handoff.decode_page(&first_request, deadline, &cancellation)?;
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
        let second_request = handoff.resume_page(&cursor, one)?;
        let second = handoff.decode_page(&second_request, deadline, &cancellation)?;
        assert_eq!(second.records().len(), 1);
        assert!(second.next_cursor().is_some());
        assert_eq!(
            second.request_cursor_identity(),
            Some(cursor.cursor_identity())
        );

        let consumed = handoff
            .into_reverified_handoff(&authority, deadline, &cancellation)
            .await?;
        assert_eq!(consumed.generation().raw_content_digest(), expected_digest);
        assert_eq!(
            consumed.logical_object_receipt().content_digest(),
            expected_digest
        );
        assert_eq!(
            consumed.logical_object_receipt().size_bytes(),
            LISTING_FIXTURE.len() as u64
        );
        assert_eq!(
            consumed.handoff_binding_digest().algorithm(),
            DigestAlgorithm::Sha256
        );
        Ok(())
    }

    fn source_harness() -> Result<
        (
            NasdaqSymbolDirectorySource,
            AuthoritativeSourceRegistry,
            ExtractionAuthority,
        ),
        Box<dyn Error>,
    > {
        const MINUTE_NANOS: u64 = 60_000_000_000;
        const FIVE_MINUTES_NANOS: u64 = 5 * MINUTE_NANOS;
        let contract_evidence = ExactPayloadEvidence::from_content_digest(EvidenceDigest::new(
            DigestAlgorithm::Sha256,
            [19; 32],
        ));
        let effective = EffectiveInterval::new(super::Timestamp::from_unix_nanos(0), None)?;
        let provider = SourceIdentifier::try_from(crate::NASDAQ_SYMBOL_DIRECTORY_PROVIDER)?;
        let authorization = AuthorizationGrant::new(
            AuthorizationMode::PublicInterface,
            AuthorizationBasis::new(SourceIdentifier::try_from("official-public-interface")?),
            contract_evidence.clone(),
            effective,
        );
        let venues = crate::NASDAQ_SYMBOL_DIRECTORY_VENUES
            .into_iter()
            .map(VenueId::try_from)
            .collect::<Result<Vec<_>, _>>()?;
        let maximum_response_bytes = u64::try_from(crate::MAX_OPTIONS_SOURCE_BYTES)?;
        let request_bounds = HttpRequestBounds::try_new(
            NonZeroU64::new(30_000_000_000).ok_or("nonzero connect timeout")?,
            NonZeroU64::new(MINUTE_NANOS).ok_or("nonzero read timeout")?,
            NonZeroU64::new(FIVE_MINUTES_NANOS).ok_or("nonzero total timeout")?,
            0,
            NonZeroU64::new(maximum_response_bytes).ok_or("nonzero response bound")?,
        )?;
        let budget = ProviderBudgetPolicy::try_new(
            BudgetScope::new(provider.clone()),
            NonZeroU32::new(crate::NASDAQ_APPLICATION_REQUESTS_PER_MINUTE)
                .ok_or("nonzero request budget")?,
            NonZeroU64::new(crate::NASDAQ_APPLICATION_BUDGET_WINDOW_NANOS)
                .ok_or("nonzero budget window")?,
            NonZeroU16::new(crate::NASDAQ_APPLICATION_MAX_CONCURRENT_REQUESTS)
                .ok_or("nonzero concurrency budget")?,
            BackoffPolicy::try_new(
                NonZeroU64::new(1_000_000_000).ok_or("nonzero backoff")?,
                NonZeroU64::new(crate::NASDAQ_APPLICATION_MIN_BACKOFF_MAXIMUM_NANOS)
                    .ok_or("nonzero backoff maximum")?,
                2_000,
            )?,
        )?;
        let metadata = SourceMetadata::try_new(SourceMetadataInput::new(
            SchemaVersion::CURRENT,
            SourceId::try_from("nasdaq-reference-test")?,
            RevisionBoundPayloadEvidence::new(
                MetadataRevision::new(SourceIdentifier::try_from(
                    "nasdaq-reference-test-revision",
                )?),
                contract_evidence.clone(),
            ),
            SourceClass::Exchange,
            provider,
            authorization,
            SourceCoverage::try_instrument(
                contract_evidence,
                effective,
                vec![AssetClass::Equity, AssetClass::Fund, AssetClass::Option],
                CoverageTopology::consolidated(venues)?,
                InstrumentCoverage::partial(),
                None,
                CoverageDelay::Delayed(MINUTE_NANOS),
                DeliveryEvidence::Indirect,
            )?,
            DataQuality::OfficialDelayed,
            NetworkAccessPolicy::Allowlisted(crate::nasdaq_reference_endpoint_policy(
                request_bounds,
            )?),
            FreshnessPolicy::try_new(
                FIVE_MINUTES_NANOS,
                FIVE_MINUTES_NANOS,
                FIVE_MINUTES_NANOS,
                FIVE_MINUTES_NANOS,
                MINUTE_NANOS,
            )?,
            Some(budget),
            SourceCapabilities::new(
                false,
                true,
                SequenceCapability::Unsupported,
                ChecksumCapability::Unsupported,
                HistoricalCapability::None,
                false,
            ),
            SourceProtocolProfile::NotLive,
        ))?;
        let source = NasdaqSymbolDirectorySource::try_new(
            metadata.clone(),
            NasdaqSymbolDirectoryConfig::try_new()?,
        )?;
        let mut registry = AuthoritativeSourceRegistry::try_new_ephemeral_for_diagnostics()?;
        let registered = registry.register(metadata, super::trusted_system_timestamp()?)?;
        let authority = registry.extraction_authority(&registered, &source)?;
        authority.validate_current()?;
        Ok((source, registry, authority))
    }

    fn timestamp(value: &str) -> Result<market_squawk_domain::Timestamp, Box<dyn Error>> {
        let nanos = DateTime::parse_from_rfc3339(value)?
            .timestamp_nanos_opt()
            .ok_or("timestamp out of range")?;
        Ok(market_squawk_domain::Timestamp::from_unix_nanos(nanos))
    }
}
