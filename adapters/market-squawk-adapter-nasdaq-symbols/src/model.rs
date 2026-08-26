use market_squawk_domain::{
    CalendarDate, DataQuality, ExactPayloadEvidence, ProviderInstrumentId, Timestamp, VenueId,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;

const MAX_SYMBOL_BYTES: usize = 14;
const MAX_SECURITY_NAME_BYTES: usize = 255;
const MAX_ROUND_LOT_SIZE: u32 = 999_999;

/// Exact Nasdaq Trader directory file represented by one source object.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum NasdaqDirectoryKind {
    /// Nasdaq-listed securities in `nasdaqlisted.txt`.
    NasdaqListed,
    /// Securities listed on other represented U.S. exchanges in `otherlisted.txt`.
    OtherListed,
    /// Nasdaq-listed bonds in `bondslist.txt`.
    Bonds,
    /// Nasdaq's current option-series reference file in `options.txt`.
    Options,
}

impl NasdaqDirectoryKind {
    /// All independently fetched and independently clocked admitted official files.
    pub const ALL: [Self; 4] = [
        Self::NasdaqListed,
        Self::OtherListed,
        Self::Bonds,
        Self::Options,
    ];

    /// The two files comprising the existing complete U.S.-listed equity directory graph.
    pub const EQUITY_DIRECTORIES: [Self; 2] = [Self::NasdaqListed, Self::OtherListed];

    pub(crate) const fn object_component(self) -> &'static str {
        match self {
            Self::NasdaqListed => "nasdaq-listed",
            Self::OtherListed => "other-listed",
            Self::Bonds => "bonds",
            Self::Options => "options",
        }
    }
}

/// Provider file-creation coordinate retained without inventing a time zone.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct NasdaqFileCreationTime {
    raw: String,
    date: CalendarDate,
    hour: u8,
    minute: u8,
}

impl NasdaqFileCreationTime {
    pub(crate) fn try_from_provider_value(value: &str) -> Result<Self, NasdaqModelError> {
        if value.len() != 13
            || value.as_bytes().get(10) != Some(&b':')
            || !value
                .bytes()
                .enumerate()
                .all(|(index, byte)| index == 10 || byte.is_ascii_digit())
        {
            return Err(NasdaqModelError::InvalidFileCreationTime);
        }
        let month = parse_u8(&value[0..2])?;
        let day = parse_u8(&value[2..4])?;
        let year = parse_u16(&value[4..8])?;
        let hour = parse_u8(&value[8..10])?;
        let minute = parse_u8(&value[11..13])?;
        let date = CalendarDate::new(year, month, day)
            .map_err(|_| NasdaqModelError::InvalidFileCreationTime)?;
        if hour > 23 || minute > 59 {
            return Err(NasdaqModelError::InvalidFileCreationTime);
        }
        Ok(Self {
            raw: value.to_owned(),
            date,
            hour,
            minute,
        })
    }

    /// Returns the exact provider value in `MMDDYYYYHH:MM` form.
    pub fn raw(&self) -> &str {
        &self.raw
    }

    /// Returns the provider-reported calendar date.
    pub const fn date(&self) -> CalendarDate {
        self.date
    }

    /// Returns the provider-reported hour without assigning a time zone.
    pub const fn hour(&self) -> u8 {
        self.hour
    }

    /// Returns the provider-reported minute without assigning a time zone.
    pub const fn minute(&self) -> u8 {
        self.minute
    }
}

/// Nasdaq listing tier code from `nasdaqlisted.txt`.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum NasdaqMarketCategory {
    /// `Q` — Nasdaq Global Select Market.
    #[serde(rename = "Q")]
    GlobalSelect,
    /// `G` — Nasdaq Global Market.
    #[serde(rename = "G")]
    GlobalMarket,
    /// `S` — Nasdaq Capital Market.
    #[serde(rename = "S")]
    CapitalMarket,
}

/// Nasdaq financial-status code retained independently of listing presence.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum NasdaqFinancialStatus {
    /// `N` — normal.
    #[serde(rename = "N")]
    Normal,
    /// `D` — deficient.
    #[serde(rename = "D")]
    Deficient,
    /// `E` — delinquent.
    #[serde(rename = "E")]
    Delinquent,
    /// `Q` — bankrupt.
    #[serde(rename = "Q")]
    Bankrupt,
    /// `G` — deficient and bankrupt.
    #[serde(rename = "G")]
    DeficientAndBankrupt,
    /// `H` — deficient and delinquent.
    #[serde(rename = "H")]
    DeficientAndDelinquent,
    /// `J` — delinquent and bankrupt.
    #[serde(rename = "J")]
    DelinquentAndBankrupt,
    /// `K` — deficient, delinquent, and bankrupt.
    #[serde(rename = "K")]
    DeficientDelinquentAndBankrupt,
}

/// Other-listing exchange code from `otherlisted.txt`.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum NasdaqOtherExchange {
    /// `A` — NYSE American.
    #[serde(rename = "A")]
    NyseAmerican,
    /// `N` — New York Stock Exchange.
    #[serde(rename = "N")]
    Nyse,
    /// `P` — NYSE Arca.
    #[serde(rename = "P")]
    NyseArca,
    /// `M` — NYSE Texas, formerly NYSE Chicago.
    #[serde(rename = "M")]
    NyseTexas,
    /// `Z` — Cboe BZX.
    #[serde(rename = "Z")]
    CboeBzx,
    /// `V` — Investors Exchange.
    #[serde(rename = "V")]
    Iex,
}

impl NasdaqOtherExchange {
    fn venue_id(self) -> Result<VenueId, NasdaqModelError> {
        let mic = match self {
            Self::NyseAmerican => "XASE",
            Self::Nyse => "XNYS",
            Self::NyseArca => "ARCX",
            Self::NyseTexas => "XCHI",
            Self::CboeBzx => "BATS",
            Self::Iex => "IEXG",
        };
        VenueId::try_from(mic).map_err(|_| NasdaqModelError::InvalidIdentifier)
    }
}

/// Meaning of a record's presence in the downloaded current directory.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum NasdaqDirectoryPresence {
    /// Present in the exact current directory object; this is not a live trading-status claim.
    CurrentDirectory,
}

/// Exact provider fields retained in their source-specific shape.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(transparent)]
pub struct NasdaqProviderFields(ProviderFields);

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(tag = "directory", rename_all = "snake_case")]
enum ProviderFields {
    NasdaqListed {
        symbol: String,
        security_name: String,
        market_category: NasdaqMarketCategory,
        test_issue: bool,
        financial_status: NasdaqFinancialStatus,
        round_lot_size: u32,
        etf: bool,
        next_shares: bool,
    },
    OtherListed {
        act_symbol: String,
        security_name: String,
        exchange: NasdaqOtherExchange,
        cqs_symbol: String,
        etf: bool,
        round_lot_size: u32,
        test_issue: bool,
        nasdaq_symbol: String,
    },
}

impl NasdaqProviderFields {
    #[allow(
        clippy::too_many_arguments,
        reason = "the provider row has eight exact columns"
    )]
    pub(crate) fn try_nasdaq_listed(
        symbol: String,
        security_name: String,
        market_category: NasdaqMarketCategory,
        test_issue: bool,
        financial_status: NasdaqFinancialStatus,
        round_lot_size: u32,
        etf: bool,
        next_shares: bool,
    ) -> Result<Self, NasdaqModelError> {
        validate_symbol("symbol", &symbol)?;
        validate_security_name(&security_name)?;
        validate_round_lot(round_lot_size)?;
        Ok(Self(ProviderFields::NasdaqListed {
            symbol,
            security_name,
            market_category,
            test_issue,
            financial_status,
            round_lot_size,
            etf,
            next_shares,
        }))
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "the provider row has eight exact columns"
    )]
    pub(crate) fn try_other_listed(
        act_symbol: String,
        security_name: String,
        exchange: NasdaqOtherExchange,
        cqs_symbol: String,
        etf: bool,
        round_lot_size: u32,
        test_issue: bool,
        nasdaq_symbol: String,
    ) -> Result<Self, NasdaqModelError> {
        validate_symbol("act_symbol", &act_symbol)?;
        validate_symbol("cqs_symbol", &cqs_symbol)?;
        validate_symbol("nasdaq_symbol", &nasdaq_symbol)?;
        validate_security_name(&security_name)?;
        validate_round_lot(round_lot_size)?;
        Ok(Self(ProviderFields::OtherListed {
            act_symbol,
            security_name,
            exchange,
            cqs_symbol,
            etf,
            round_lot_size,
            test_issue,
            nasdaq_symbol,
        }))
    }

    /// Returns the source file supplying these fields.
    pub const fn directory_kind(&self) -> NasdaqDirectoryKind {
        match &self.0 {
            ProviderFields::NasdaqListed { .. } => NasdaqDirectoryKind::NasdaqListed,
            ProviderFields::OtherListed { .. } => NasdaqDirectoryKind::OtherListed,
        }
    }

    /// Returns the provider symbol used as the record's reference-candidate identity.
    pub fn primary_symbol(&self) -> &str {
        match &self.0 {
            ProviderFields::NasdaqListed { symbol, .. } => symbol,
            ProviderFields::OtherListed { act_symbol, .. } => act_symbol,
        }
    }

    /// Returns the exact provider security-name field, including provider whitespace.
    pub fn security_name(&self) -> &str {
        match &self.0 {
            ProviderFields::NasdaqListed { security_name, .. }
            | ProviderFields::OtherListed { security_name, .. } => security_name,
        }
    }

    /// Returns a display-oriented borrowed name without changing preserved source data.
    pub fn display_name(&self) -> &str {
        self.security_name().trim()
    }

    /// Returns whether the provider marks this row as a test issue.
    pub const fn is_test_issue(&self) -> bool {
        match &self.0 {
            ProviderFields::NasdaqListed { test_issue, .. }
            | ProviderFields::OtherListed { test_issue, .. } => *test_issue,
        }
    }

    /// Returns whether the provider marks this row as an ETF.
    pub const fn is_etf(&self) -> bool {
        match &self.0 {
            ProviderFields::NasdaqListed { etf, .. } | ProviderFields::OtherListed { etf, .. } => {
                *etf
            }
        }
    }

    /// Returns the provider round-lot size in shares.
    pub const fn round_lot_size(&self) -> u32 {
        match &self.0 {
            ProviderFields::NasdaqListed { round_lot_size, .. }
            | ProviderFields::OtherListed { round_lot_size, .. } => *round_lot_size,
        }
    }

    /// Returns the Nasdaq listing tier when supplied by `nasdaqlisted.txt`.
    pub const fn market_category(&self) -> Option<NasdaqMarketCategory> {
        match &self.0 {
            ProviderFields::NasdaqListed {
                market_category, ..
            } => Some(*market_category),
            ProviderFields::OtherListed { .. } => None,
        }
    }

    /// Returns Nasdaq financial status when supplied by `nasdaqlisted.txt`.
    pub const fn financial_status(&self) -> Option<NasdaqFinancialStatus> {
        match &self.0 {
            ProviderFields::NasdaqListed {
                financial_status, ..
            } => Some(*financial_status),
            ProviderFields::OtherListed { .. } => None,
        }
    }

    /// Returns the other-listing provider exchange code when present.
    pub const fn other_exchange(&self) -> Option<NasdaqOtherExchange> {
        match &self.0 {
            ProviderFields::NasdaqListed { .. } => None,
            ProviderFields::OtherListed { exchange, .. } => Some(*exchange),
        }
    }

    /// Returns the CQS/CTS symbol when supplied by `otherlisted.txt`.
    pub fn cqs_symbol(&self) -> Option<&str> {
        match &self.0 {
            ProviderFields::NasdaqListed { .. } => None,
            ProviderFields::OtherListed { cqs_symbol, .. } => Some(cqs_symbol),
        }
    }

    /// Returns Nasdaq's symbol for an other-listed security when present.
    pub fn nasdaq_symbol(&self) -> Option<&str> {
        match &self.0 {
            ProviderFields::NasdaqListed { symbol, .. } => Some(symbol),
            ProviderFields::OtherListed { nasdaq_symbol, .. } => Some(nasdaq_symbol),
        }
    }

    /// Returns the Nasdaq NextShares flag when supplied by `nasdaqlisted.txt`.
    pub const fn is_next_shares(&self) -> Option<bool> {
        match &self.0 {
            ProviderFields::NasdaqListed { next_shares, .. } => Some(*next_shares),
            ProviderFields::OtherListed { .. } => None,
        }
    }

    fn venue_id(&self) -> Result<VenueId, NasdaqModelError> {
        match &self.0 {
            ProviderFields::NasdaqListed { .. } => {
                VenueId::try_from("XNAS").map_err(|_| NasdaqModelError::InvalidIdentifier)
            }
            ProviderFields::OtherListed { exchange, .. } => (*exchange).venue_id(),
        }
    }
}

/// One invariant-preserving listing-reference candidate with exact source-object provenance.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct NasdaqListingRecord {
    schema_version: u16,
    provider_row_number: u32,
    primary_symbol: ProviderInstrumentId,
    listing_venue: VenueId,
    directory_presence: NasdaqDirectoryPresence,
    quality: DataQuality,
    file_creation_time: NasdaqFileCreationTime,
    source_last_modified_at: Timestamp,
    first_observed_at: Timestamp,
    source_payload_evidence: ExactPayloadEvidence,
    provider_fields: NasdaqProviderFields,
}

impl NasdaqListingRecord {
    pub(crate) fn try_new(
        provider_row_number: u32,
        file_creation_time: NasdaqFileCreationTime,
        source_last_modified_at: Timestamp,
        first_observed_at: Timestamp,
        source_payload_evidence: ExactPayloadEvidence,
        provider_fields: NasdaqProviderFields,
    ) -> Result<Self, NasdaqModelError> {
        if provider_row_number < 2 {
            return Err(NasdaqModelError::InvalidRowNumber);
        }
        if source_last_modified_at > first_observed_at {
            return Err(NasdaqModelError::InvalidTemporalOrder);
        }
        let content_digest = source_payload_evidence.content_digest();
        if content_digest.algorithm() != market_squawk_domain::DigestAlgorithm::Sha256
            || content_digest.bytes() == [0; 32]
        {
            return Err(NasdaqModelError::InvalidPayloadDigest);
        }
        let primary_symbol = ProviderInstrumentId::try_from(provider_fields.primary_symbol())
            .map_err(|_| NasdaqModelError::InvalidIdentifier)?;
        let listing_venue = provider_fields.venue_id()?;
        Ok(Self {
            schema_version: 1,
            provider_row_number,
            primary_symbol,
            listing_venue,
            directory_presence: NasdaqDirectoryPresence::CurrentDirectory,
            quality: DataQuality::OfficialDelayed,
            file_creation_time,
            source_last_modified_at,
            first_observed_at,
            source_payload_evidence,
            provider_fields,
        })
    }

    /// Decodes and revalidates one normalized adapter payload.
    ///
    /// # Errors
    ///
    /// Rejects unknown fields, invalid provider values, or tampered derived fields.
    pub fn from_json(payload: &[u8]) -> Result<Self, NasdaqModelError> {
        let wire: NasdaqListingRecordWire =
            serde_json::from_slice(payload).map_err(|_| NasdaqModelError::InvalidWire)?;
        let file_creation_time =
            NasdaqFileCreationTime::try_from_provider_value(&wire.file_creation_time.raw)?;
        if file_creation_time.date != wire.file_creation_time.date
            || file_creation_time.hour != wire.file_creation_time.hour
            || file_creation_time.minute != wire.file_creation_time.minute
        {
            return Err(NasdaqModelError::DerivedFieldMismatch);
        }
        let provider_fields = NasdaqProviderFields::try_from(wire.provider_fields)?;
        let rebuilt = Self::try_new(
            wire.provider_row_number,
            file_creation_time,
            wire.source_last_modified_at,
            wire.first_observed_at,
            wire.source_payload_evidence,
            provider_fields,
        )?;
        if rebuilt.schema_version != wire.schema_version
            || rebuilt.primary_symbol != wire.primary_symbol
            || rebuilt.listing_venue != wire.listing_venue
            || rebuilt.directory_presence != wire.directory_presence
            || rebuilt.quality != wire.quality
        {
            return Err(NasdaqModelError::DerivedFieldMismatch);
        }
        Ok(rebuilt)
    }

    /// Returns this normalized payload's schema version.
    pub const fn schema_version(&self) -> u16 {
        self.schema_version
    }

    /// Returns the one-based row number in the exact provider file.
    pub const fn provider_row_number(&self) -> u32 {
        self.provider_row_number
    }

    /// Returns the provider symbol chosen as the reference-candidate identity.
    pub const fn primary_symbol(&self) -> &ProviderInstrumentId {
        &self.primary_symbol
    }

    /// Returns the normalized listing-venue MIC.
    pub const fn listing_venue(&self) -> &VenueId {
        &self.listing_venue
    }

    /// Returns the qualified meaning of presence in the downloaded directory.
    pub const fn directory_presence(&self) -> NasdaqDirectoryPresence {
        self.directory_presence
    }

    /// Returns the bounded source quality; this record is never execution eligible.
    pub const fn quality(&self) -> DataQuality {
        self.quality
    }

    /// Returns the provider file-creation coordinate without an invented time zone.
    pub const fn file_creation_time(&self) -> &NasdaqFileCreationTime {
        &self.file_creation_time
    }

    /// Returns the exact UTC `Last-Modified` timestamp supplied with the source object.
    pub const fn source_last_modified_at(&self) -> Timestamp {
        self.source_last_modified_at
    }

    /// Returns when this process first observed the exact source object.
    pub const fn first_observed_at(&self) -> Timestamp {
        self.first_observed_at
    }

    /// Returns exact evidence for the complete source file containing this row.
    pub const fn source_payload_evidence(&self) -> &ExactPayloadEvidence {
        &self.source_payload_evidence
    }

    /// Returns the exact source-specific provider fields.
    pub const fn provider_fields(&self) -> &NasdaqProviderFields {
        &self.provider_fields
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct NasdaqFileCreationTimeWire {
    raw: String,
    date: CalendarDate,
    hour: u8,
    minute: u8,
}

#[derive(Deserialize)]
#[serde(tag = "directory", rename_all = "snake_case", deny_unknown_fields)]
enum ProviderFieldsWire {
    NasdaqListed {
        symbol: String,
        security_name: String,
        market_category: NasdaqMarketCategory,
        test_issue: bool,
        financial_status: NasdaqFinancialStatus,
        round_lot_size: u32,
        etf: bool,
        next_shares: bool,
    },
    OtherListed {
        act_symbol: String,
        security_name: String,
        exchange: NasdaqOtherExchange,
        cqs_symbol: String,
        etf: bool,
        round_lot_size: u32,
        test_issue: bool,
        nasdaq_symbol: String,
    },
}

impl TryFrom<ProviderFieldsWire> for NasdaqProviderFields {
    type Error = NasdaqModelError;

    fn try_from(value: ProviderFieldsWire) -> Result<Self, Self::Error> {
        match value {
            ProviderFieldsWire::NasdaqListed {
                symbol,
                security_name,
                market_category,
                test_issue,
                financial_status,
                round_lot_size,
                etf,
                next_shares,
            } => Self::try_nasdaq_listed(
                symbol,
                security_name,
                market_category,
                test_issue,
                financial_status,
                round_lot_size,
                etf,
                next_shares,
            ),
            ProviderFieldsWire::OtherListed {
                act_symbol,
                security_name,
                exchange,
                cqs_symbol,
                etf,
                round_lot_size,
                test_issue,
                nasdaq_symbol,
            } => Self::try_other_listed(
                act_symbol,
                security_name,
                exchange,
                cqs_symbol,
                etf,
                round_lot_size,
                test_issue,
                nasdaq_symbol,
            ),
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct NasdaqListingRecordWire {
    schema_version: u16,
    provider_row_number: u32,
    primary_symbol: ProviderInstrumentId,
    listing_venue: VenueId,
    directory_presence: NasdaqDirectoryPresence,
    quality: DataQuality,
    file_creation_time: NasdaqFileCreationTimeWire,
    source_last_modified_at: Timestamp,
    first_observed_at: Timestamp,
    source_payload_evidence: ExactPayloadEvidence,
    provider_fields: ProviderFieldsWire,
}

/// Normalized-record construction or hostile-wire failure.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum NasdaqModelError {
    /// Provider file-creation value was not a valid `MMDDYYYYHH:MM` civil coordinate.
    #[error("invalid Nasdaq file creation time")]
    InvalidFileCreationTime,
    /// A required provider field was invalid.
    #[error("invalid Nasdaq provider field: {field}")]
    InvalidProviderField {
        /// Stable provider-field name.
        field: &'static str,
    },
    /// A provider row number did not refer to a data row after the header.
    #[error("invalid Nasdaq provider row number")]
    InvalidRowNumber,
    /// Source publication time followed this process's first observation.
    #[error("Nasdaq source timestamp follows its local first-observed timestamp")]
    InvalidTemporalOrder,
    /// A normalized provider or venue identifier could not be constructed.
    #[error("invalid Nasdaq normalized identifier")]
    InvalidIdentifier,
    /// JSON did not match the bounded normalized record schema.
    #[error("invalid Nasdaq normalized record wire payload")]
    InvalidWire,
    /// Serialized derived fields disagreed with the exact provider fields.
    #[error("Nasdaq normalized record derived fields do not match provider fields")]
    DerivedFieldMismatch,
    /// Exact source-object evidence was not a nonzero SHA-256 identity.
    #[error("invalid Nasdaq exact source payload digest")]
    InvalidPayloadDigest,
}

fn validate_symbol(field: &'static str, value: &str) -> Result<(), NasdaqModelError> {
    if value.is_empty()
        || value.len() > MAX_SYMBOL_BYTES
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_graphic() && byte != b'|')
    {
        Err(NasdaqModelError::InvalidProviderField { field })
    } else {
        Ok(())
    }
}

fn validate_security_name(value: &str) -> Result<(), NasdaqModelError> {
    if value.len() > MAX_SECURITY_NAME_BYTES
        || value.trim().is_empty()
        || value.chars().any(char::is_control)
        || value.contains('|')
    {
        Err(NasdaqModelError::InvalidProviderField {
            field: "security_name",
        })
    } else {
        Ok(())
    }
}

fn validate_round_lot(value: u32) -> Result<(), NasdaqModelError> {
    if value == 0 || value > MAX_ROUND_LOT_SIZE {
        Err(NasdaqModelError::InvalidProviderField {
            field: "round_lot_size",
        })
    } else {
        Ok(())
    }
}

fn parse_u8(value: &str) -> Result<u8, NasdaqModelError> {
    value
        .parse()
        .map_err(|_| NasdaqModelError::InvalidFileCreationTime)
}

fn parse_u16(value: &str) -> Result<u16, NasdaqModelError> {
    value
        .parse()
        .map_err(|_| NasdaqModelError::InvalidFileCreationTime)
}
