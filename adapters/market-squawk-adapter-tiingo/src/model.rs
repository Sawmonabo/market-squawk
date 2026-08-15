use std::fmt;

use market_squawk_domain::{CalendarDate, EvidenceDigest, SourceIdentifier, Timestamp};
use rust_decimal::Decimal;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::{TiingoAdapterError, TiingoRequestSpec};

pub(crate) const MAX_TICKER_BYTES: usize = 64;

/// A bounded exact Tiingo ticker. It is never silently normalized or used as canonical identity.
#[derive(Clone, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct TiingoTicker(Box<str>);

impl TiingoTicker {
    /// Constructs a code-safe U.S. EOD ticker segment.
    ///
    /// This is an application safety grammar, not a statement of Tiingo's complete coverage.
    pub fn try_new(value: impl Into<String>) -> Result<Self, TiingoAdapterError> {
        let value = value.into();
        if value.is_empty()
            || value.len() > MAX_TICKER_BYTES
            || !value
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-' | b'_'))
        {
            return Err(TiingoAdapterError::InvalidTicker);
        }
        Ok(Self(value.into_boxed_str()))
    }

    /// Returns the exact provider ticker.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for TiingoTicker {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("TiingoTicker")
            .field(&self.0)
            .finish()
    }
}

impl fmt::Display for TiingoTicker {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl Serialize for TiingoTicker {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for TiingoTicker {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::try_new(value).map_err(serde::de::Error::custom)
    }
}

/// Ordinal and total size of application-created date-window pagination.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TiingoApplicationPage {
    ordinal: u16,
    count: u16,
}

impl TiingoApplicationPage {
    /// Constructs a valid one-based page coordinate.
    pub fn try_new(ordinal: u16, count: u16) -> Result<Self, TiingoAdapterError> {
        if ordinal == 0 || count == 0 || ordinal > count {
            return Err(TiingoAdapterError::InvalidDateRange);
        }
        Ok(Self { ordinal, count })
    }

    /// Returns the one-based page ordinal.
    pub const fn ordinal(self) -> u16 {
        self.ordinal
    }

    /// Returns the complete application page count.
    pub const fn count(self) -> u16 {
        self.count
    }
}

/// Exact availability interval established by the per-ticker metadata endpoint.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TiingoCoverage {
    /// Both non-null provider coverage dates were present.
    Supported {
        /// Inclusive first supported date.
        start_date: CalendarDate,
        /// Inclusive last supported date in the retrieved metadata generation.
        end_date: CalendarDate,
    },
    /// Both provider coverage dates were null; archive membership alone is not availability.
    Unsupported,
}

impl TiingoCoverage {
    /// Returns whether one NAV/EOD date is inside the exact metadata coverage interval.
    pub fn contains(self, date: CalendarDate) -> bool {
        match self {
            Self::Supported {
                start_date,
                end_date,
            } => date >= start_date && date <= end_date,
            Self::Unsupported => false,
        }
    }
}

/// Tiingo metadata retained for exact provider-instrument admission.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TiingoMetadata {
    pub(crate) ticker: TiingoTicker,
    pub(crate) name: Box<str>,
    pub(crate) exchange_code: Box<str>,
    pub(crate) description: Option<Box<str>>,
    pub(crate) coverage: TiingoCoverage,
}

impl TiingoMetadata {
    /// Returns the exact provider ticker.
    pub const fn ticker(&self) -> &TiingoTicker {
        &self.ticker
    }

    /// Returns the provider display name without treating it as identity.
    pub const fn name(&self) -> &str {
        &self.name
    }

    /// Returns the provider exchange code.
    pub const fn exchange_code(&self) -> &str {
        &self.exchange_code
    }

    /// Returns the optional provider description.
    pub fn description(&self) -> Option<&str> {
        self.description.as_deref()
    }

    /// Returns the exact non-null-date availability result.
    pub const fn coverage(&self) -> TiingoCoverage {
        self.coverage
    }
}

/// Provider-pagination evidence retained without inventing an undocumented cursor.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TiingoPaginationEvidence {
    /// Metadata or latest-price request with no pagination coordinate.
    NotApplicable,
    /// Application-created date page; Tiingo supplied no provider cursor contract.
    ApplicationDateWindow(TiingoApplicationPage),
}

/// Request-versus-actual-observation accounting for one exact response.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TiingoRequestDisposition {
    requested_symbols: u16,
    returned_symbols: u16,
    missing_symbols: u16,
    returned_rows: u32,
    response_bytes: u64,
}

impl TiingoRequestDisposition {
    pub(crate) fn one_symbol(returned_rows: usize, response_bytes: usize) -> Self {
        let returned = u16::from(returned_rows != 0);
        Self {
            requested_symbols: 1,
            returned_symbols: returned,
            missing_symbols: 1 - returned,
            returned_rows: u32::try_from(returned_rows).unwrap_or(u32::MAX),
            response_bytes: u64::try_from(response_bytes).unwrap_or(u64::MAX),
        }
    }

    /// Returns the exact requested provider-symbol count.
    pub const fn requested_symbols(self) -> u16 {
        self.requested_symbols
    }

    /// Returns the symbols with at least one accepted provider row.
    pub const fn returned_symbols(self) -> u16 {
        self.returned_symbols
    }

    /// Returns requested symbols with no accepted provider row.
    pub const fn missing_symbols(self) -> u16 {
        self.missing_symbols
    }

    /// Returns actual valid decoded rows, never requested date slots.
    pub const fn returned_rows(self) -> u32 {
        self.returned_rows
    }

    /// Returns the exact HTTP body bytes observed for quota and storage accounting.
    pub const fn response_bytes(self) -> u64 {
        self.response_bytes
    }
}

/// Secret-free receipt for one exact bounded response body.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TiingoResponseEvidence {
    request: TiingoRequestSpec,
    native_contract_revision: SourceIdentifier,
    entitlement_generation: SourceIdentifier,
    status: u16,
    body_digest: EvidenceDigest,
    response_bytes: u64,
    received_at: Timestamp,
    decoded_at: Timestamp,
}

impl TiingoResponseEvidence {
    pub(crate) const fn new(
        request: TiingoRequestSpec,
        native_contract_revision: SourceIdentifier,
        entitlement_generation: SourceIdentifier,
        status: u16,
        body_digest: EvidenceDigest,
        response_bytes: u64,
        received_at: Timestamp,
        decoded_at: Timestamp,
    ) -> Self {
        Self {
            request,
            native_contract_revision,
            entitlement_generation,
            status,
            body_digest,
            response_bytes,
            received_at,
            decoded_at,
        }
    }

    /// Returns the exact credential-free request description.
    pub const fn request(&self) -> &TiingoRequestSpec {
        &self.request
    }

    /// Returns the exact reviewed provider-native decoder contract revision.
    pub const fn native_contract_revision(&self) -> &SourceIdentifier {
        &self.native_contract_revision
    }

    /// Returns the exact credential/entitlement generation that authorized this response.
    pub const fn entitlement_generation(&self) -> &SourceIdentifier {
        &self.entitlement_generation
    }

    /// Returns the HTTP status.
    pub const fn status(&self) -> u16 {
        self.status
    }

    /// Returns the SHA-256 response-body identity.
    pub const fn body_digest(&self) -> EvidenceDigest {
        self.body_digest
    }

    /// Returns the exact response-body byte count.
    pub const fn response_bytes(&self) -> u64 {
        self.response_bytes
    }

    /// Returns when exact body receipt completed locally.
    pub const fn received_at(&self) -> Timestamp {
        self.received_at
    }

    /// Returns when strict native decoding completed locally.
    pub const fn decoded_at(&self) -> Timestamp {
        self.decoded_at
    }
}

/// Strict metadata decoding result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TiingoMetadataReceipt {
    pub(crate) metadata: TiingoMetadata,
    pub(crate) evidence: TiingoResponseEvidence,
    pub(crate) disposition: TiingoRequestDisposition,
}

impl TiingoMetadataReceipt {
    /// Returns exact provider metadata.
    pub const fn metadata(&self) -> &TiingoMetadata {
        &self.metadata
    }

    /// Returns raw-response identity and receive/decode clocks.
    pub const fn evidence(&self) -> &TiingoResponseEvidence {
        &self.evidence
    }

    /// Returns requested, returned, missing, row, and byte accounting.
    pub const fn disposition(&self) -> TiingoRequestDisposition {
        self.disposition
    }
}

/// One strict Tiingo daily row. Raw and adjusted values remain separate provider evidence.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TiingoEodRow {
    pub(crate) provider_date: Box<str>,
    pub(crate) date: CalendarDate,
    pub(crate) open: Option<Decimal>,
    pub(crate) high: Option<Decimal>,
    pub(crate) low: Option<Decimal>,
    pub(crate) close: Option<Decimal>,
    pub(crate) volume: Option<Decimal>,
    pub(crate) adjusted_open: Option<Decimal>,
    pub(crate) adjusted_high: Option<Decimal>,
    pub(crate) adjusted_low: Option<Decimal>,
    pub(crate) adjusted_close: Option<Decimal>,
    pub(crate) adjusted_volume: Option<Decimal>,
    pub(crate) cash_dividend: Option<Decimal>,
    pub(crate) split_factor: Option<Decimal>,
    pub(crate) row_digest: EvidenceDigest,
}

impl TiingoEodRow {
    /// Returns the source date string retained exactly.
    pub const fn provider_date(&self) -> &str {
        &self.provider_date
    }

    /// Returns the effective daily civil date without manufacturing an instant.
    pub const fn date(&self) -> CalendarDate {
        self.date
    }

    /// Returns raw OHLC in provider order.
    pub const fn raw_ohlc(
        &self,
    ) -> (
        Option<Decimal>,
        Option<Decimal>,
        Option<Decimal>,
        Option<Decimal>,
    ) {
        (self.open, self.high, self.low, self.close)
    }

    /// Returns the exact raw close used as NAV only after mutual-fund semantic validation.
    pub const fn close(&self) -> Option<Decimal> {
        self.close
    }

    /// Returns raw provider volume.
    pub const fn volume(&self) -> Option<Decimal> {
        self.volume
    }

    /// Returns adjusted OHLC as a distinct evidence surface.
    pub const fn adjusted_ohlc(
        &self,
    ) -> (
        Option<Decimal>,
        Option<Decimal>,
        Option<Decimal>,
        Option<Decimal>,
    ) {
        (
            self.adjusted_open,
            self.adjusted_high,
            self.adjusted_low,
            self.adjusted_close,
        )
    }

    /// Returns adjusted volume.
    pub const fn adjusted_volume(&self) -> Option<Decimal> {
        self.adjusted_volume
    }

    /// Returns source-reported daily cash dividend without creating a corporate action.
    pub const fn cash_dividend(&self) -> Option<Decimal> {
        self.cash_dividend
    }

    /// Returns source-reported split factor without creating a corporate action.
    pub const fn split_factor(&self) -> Option<Decimal> {
        self.split_factor
    }

    /// Returns a digest of every typed provider-native field.
    pub const fn row_digest(&self) -> EvidenceDigest {
        self.row_digest
    }
}

/// Strict EOD/NAV array decoding result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TiingoEodReceipt {
    pub(crate) rows: Box<[TiingoEodRow]>,
    pub(crate) evidence: TiingoResponseEvidence,
    pub(crate) disposition: TiingoRequestDisposition,
    pub(crate) pagination: TiingoPaginationEvidence,
}

impl TiingoEodReceipt {
    /// Returns exact validated rows in strictly increasing date order.
    pub fn rows(&self) -> &[TiingoEodRow] {
        &self.rows
    }

    /// Returns raw-response identity and receive/decode clocks.
    pub const fn evidence(&self) -> &TiingoResponseEvidence {
        &self.evidence
    }

    /// Returns requested, returned, missing, row, and byte accounting.
    pub const fn disposition(&self) -> TiingoRequestDisposition {
        self.disposition
    }

    /// Returns application page evidence without inventing a provider cursor.
    pub const fn pagination(&self) -> TiingoPaginationEvidence {
        self.pagination
    }
}
