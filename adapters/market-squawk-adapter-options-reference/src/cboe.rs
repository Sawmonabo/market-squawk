use std::fmt;
use std::num::{NonZeroU16, NonZeroU32};

use csv::{ReaderBuilder, StringRecord};
use market_squawk_domain::{DigestAlgorithm, ProviderInstrumentId, SourceIdentifier};
use serde::Serialize;
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::{
    OptionContractIdentity, PageTerminalState, ReferenceObjectContext, ReferencePageReceipt,
    ReferenceProvider, ReferenceSurface,
};

/// Application maximum for one Cboe `All Series` source object.
///
/// This is a parser-safety policy, not a provider-published file-size guarantee.
pub const CBOE_ALL_SERIES_MAX_BYTES: usize = 128 * 1024 * 1024;

/// Application maximum for valid records in one Cboe `All Series` file.
///
/// This is a parser-safety policy, not a provider-published row ceiling.
pub const CBOE_ALL_SERIES_MAX_RECORDS: u32 = 2_500_000;

const MAX_UNDERLYING_BYTES: usize = 8;

/// One of the four independently published Cboe U.S. option venues.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CboeVenue {
    /// Cboe Options Exchange (C1).
    C1,
    /// Cboe BZX Options.
    Bzx,
    /// Cboe C2 Options.
    C2,
    /// Cboe EDGX Options.
    Edgx,
}

impl CboeVenue {
    /// Returns the exact selected `All Series` locator.
    pub const fn all_series_locator(self) -> &'static str {
        match self {
            Self::C1 => {
                "https://cdn.cboe.com/data/us/options/market_statistics/symbol_reference/cone-all-series.csv"
            }
            Self::Bzx => {
                "https://cdn.cboe.com/data/us/options/market_statistics/symbol_reference/opt-all-series.csv"
            }
            Self::C2 => {
                "https://cdn.cboe.com/data/us/options/market_statistics/symbol_reference/ctwo-all-series.csv"
            }
            Self::Edgx => {
                "https://cdn.cboe.com/data/us/options/market_statistics/symbol_reference/exo-all-series.csv"
            }
        }
    }

    const fn stable_label(self) -> &'static str {
        match self {
            Self::C1 => "c1",
            Self::Bzx => "bzx",
            Self::C2 => "c2",
            Self::Edgx => "edgx",
        }
    }
}

/// Exact code-owned header contract for a selected Cboe `All Series` CSV generation.
///
/// Each variant is closed and order-sensitive. The parser never accepts a generic property map or
/// silently ignores new columns.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CboeAllSeriesCsvSchema {
    /// Header names using the website's `Symbol` label.
    SymbolV1,
    /// Header names using the feed specification's `Feed Symbol` label.
    FeedSymbolV1,
    /// Header names using the symbology specification's `Cboe Symbol ID` label.
    CboeSymbolIdV1,
}

impl CboeAllSeriesCsvSchema {
    const fn header(self) -> [&'static str; 5] {
        let symbol = match self {
            Self::SymbolV1 => "Symbol",
            Self::FeedSymbolV1 => "Feed Symbol",
            Self::CboeSymbolIdV1 => "Cboe Symbol ID",
        };
        [
            symbol,
            "OSI Symbol",
            "Symbol Condition",
            "Underlying",
            "Unit",
        ]
    }

    /// Returns the stable provider-native decoder identity required in object evidence.
    pub const fn native_schema(self) -> &'static str {
        match self {
            Self::SymbolV1 => "cboe-all-series-csv-symbol-v1",
            Self::FeedSymbolV1 => "cboe-all-series-csv-feed-symbol-v1",
            Self::CboeSymbolIdV1 => "cboe-all-series-csv-cboe-symbol-id-v1",
        }
    }

    pub(crate) fn matches_header_line(self, line: &[u8]) -> bool {
        let expected = self.header();
        let mut cursor = 0_usize;
        for (index, field) in expected.iter().enumerate() {
            let bytes = field.as_bytes();
            let Some(end) = cursor.checked_add(bytes.len()) else {
                return false;
            };
            if line.get(cursor..end) != Some(bytes) {
                return false;
            }
            cursor = end;
            if index + 1 < expected.len() {
                if line.get(cursor) != Some(&b',') {
                    return false;
                }
                cursor = cursor.saturating_add(1);
            }
        }
        cursor == line.len()
    }
}

/// Six-character, case-sensitive base-62 Cboe Symbol ID.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct CboeSymbolId(String);

impl CboeSymbolId {
    /// Parses the exact six-character base-62 identity.
    ///
    /// # Errors
    ///
    /// Rejects any length other than six and characters outside ASCII `0-9A-Za-z`.
    pub fn try_from_provider(value: &str) -> Result<Self, CboeParseError> {
        if value.len() != 6 || !value.bytes().all(|byte| byte.is_ascii_alphanumeric()) {
            return Err(CboeParseError::InvalidSymbolId);
        }
        Ok(Self(value.to_owned()))
    }

    /// Returns the source-preserved case-sensitive identity.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for CboeSymbolId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

/// Venue-specific series status reported by the selected Cboe file.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CboeSeriesStatus {
    /// `N` — normal in this venue publication.
    Normal,
    /// `C` — closing-only in this venue publication.
    ClosingOnly,
}

impl CboeSeriesStatus {
    fn try_from_provider(value: &str) -> Result<Self, CboeParseError> {
        match value {
            "N" => Ok(Self::Normal),
            "C" => Ok(Self::ClosingOnly),
            _ => Err(CboeParseError::InvalidSeriesStatus),
        }
    }
}

/// Qualified meaning of a row's presence in an `All Series` file.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CboeListingEvidence {
    /// The series appeared in this exact venue/family publication.
    PresentInVenuePublication,
}

/// One provider-native Cboe option-series reference observation.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CboeSeriesReference {
    schema_version: u16,
    record_id: SourceIdentifier,
    provider_row_number: u32,
    venue: CboeVenue,
    cboe_symbol_id: CboeSymbolId,
    contract: OptionContractIdentity,
    underlying: ProviderInstrumentId,
    unit: NonZeroU16,
    status: CboeSeriesStatus,
    listing_evidence: CboeListingEvidence,
    object_context: ReferenceObjectContext,
}

impl CboeSeriesReference {
    #[allow(
        clippy::too_many_arguments,
        reason = "the exact five-column provider row plus provenance is intentional"
    )]
    fn try_new(
        provider_row_number: u32,
        venue: CboeVenue,
        cboe_symbol_id: CboeSymbolId,
        contract: OptionContractIdentity,
        underlying: &str,
        unit: NonZeroU16,
        status: CboeSeriesStatus,
        object_context: ReferenceObjectContext,
    ) -> Result<Self, CboeParseError> {
        if provider_row_number < 2
            || underlying.is_empty()
            || underlying.len() > MAX_UNDERLYING_BYTES
            || !underlying.bytes().all(|byte| byte.is_ascii_alphanumeric())
        {
            return Err(CboeParseError::InvalidUnderlying);
        }
        let underlying = ProviderInstrumentId::try_from(underlying)
            .map_err(|_| CboeParseError::InvalidUnderlying)?;
        let record_id = SourceIdentifier::try_from(format!(
            "cboe-all-series:{}:{}:row-{provider_row_number}",
            venue.stable_label(),
            object_context.object_id().as_str()
        ))
        .map_err(|_| CboeParseError::InvalidEvidence)?;
        Ok(Self {
            schema_version: 1,
            record_id,
            provider_row_number,
            venue,
            cboe_symbol_id,
            contract,
            underlying,
            unit,
            status,
            listing_evidence: CboeListingEvidence::PresentInVenuePublication,
            object_context,
        })
    }

    /// Returns the stable source-record identity.
    pub const fn record_id(&self) -> &SourceIdentifier {
        &self.record_id
    }

    /// Returns the one-based source row number, including the header as row one.
    pub const fn provider_row_number(&self) -> u32 {
        self.provider_row_number
    }

    /// Returns the venue whose file supplied this presence evidence.
    pub const fn venue(&self) -> CboeVenue {
        self.venue
    }

    /// Returns the Cboe compressed symbol identity.
    pub const fn cboe_symbol_id(&self) -> &CboeSymbolId {
        &self.cboe_symbol_id
    }

    /// Returns parsed OSI identity and only the terms that source evidence established.
    pub const fn contract(&self) -> &OptionContractIdentity {
        &self.contract
    }

    /// Returns the provider-reported underlying alias. It is not a canonical underlying ID.
    pub const fn underlying(&self) -> &ProviderInstrumentId {
        &self.underlying
    }

    /// Returns the venue matching-engine unit reported by the file.
    pub const fn unit(&self) -> NonZeroU16 {
        self.unit
    }

    /// Returns the venue-specific series status.
    pub const fn status(&self) -> CboeSeriesStatus {
        self.status
    }

    /// Returns the qualified listing-reference meaning.
    pub const fn listing_evidence(&self) -> CboeListingEvidence {
        self.listing_evidence
    }

    /// Returns exact raw-object, decoder, and clock lineage.
    pub const fn object_context(&self) -> &ReferenceObjectContext {
        &self.object_context
    }
}

/// Successful strict decode evidence for one complete Cboe file.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CboeAllSeriesParseReceipt {
    schema: CboeAllSeriesCsvSchema,
    context: ReferenceObjectContext,
    returned_records: u32,
}

impl CboeAllSeriesParseReceipt {
    /// Returns the exact decoder schema.
    pub const fn schema(&self) -> CboeAllSeriesCsvSchema {
        self.schema
    }

    /// Returns the count of valid observations delivered to the sink.
    pub const fn returned_records(&self) -> u32 {
        self.returned_records
    }

    /// Converts the strict single-file decode into terminal page-completeness evidence.
    pub fn page_receipt(&self) -> ReferencePageReceipt {
        ReferencePageReceipt::new(
            self.context.clone(),
            NonZeroU32::MIN,
            self.returned_records,
            0,
            PageTerminalState::Terminal,
        )
    }
}

/// Strict streaming decoder for one exact venue-specific `All Series` CSV.
#[derive(Clone, Debug)]
pub struct CboeAllSeriesParser {
    schema: CboeAllSeriesCsvSchema,
    context: ReferenceObjectContext,
}

impl CboeAllSeriesParser {
    /// Binds one exact raw-object context to a closed header layout.
    ///
    /// # Errors
    ///
    /// Rejects non-Cboe, non-`All Series`, wrong-venue, or wrong-schema evidence.
    pub fn try_new(
        venue: CboeVenue,
        schema: CboeAllSeriesCsvSchema,
        context: ReferenceObjectContext,
    ) -> Result<Self, CboeParseError> {
        let expected_surface = ReferenceSurface::CboeAllSeries { venue };
        if context.provider() != ReferenceProvider::Cboe
            || context.surface() != &expected_surface
            || context.native_schema().as_str() != schema.native_schema()
            || context.media_type().as_str() != "text/csv"
        {
            return Err(CboeParseError::InvalidContext);
        }
        Ok(Self { schema, context })
    }

    /// Decodes a bounded exact file and delivers each row to a caller-owned bounded sink.
    ///
    /// No success receipt is returned if any row is malformed or the sink rejects a record, so a
    /// partial file cannot be mislabeled complete.
    ///
    /// # Errors
    ///
    /// Rejects size/digest drift, unknown headers, malformed CSV, invalid identities/status, row
    /// overflow, or sink failure.
    pub fn parse<F>(
        &self,
        bytes: &[u8],
        mut sink: F,
    ) -> Result<CboeAllSeriesParseReceipt, CboeParseError>
    where
        F: FnMut(CboeSeriesReference) -> Result<(), CboeParseError>,
    {
        validate_payload(&self.context, bytes)?;
        if bytes.len() > CBOE_ALL_SERIES_MAX_BYTES {
            return Err(CboeParseError::BodyTooLarge);
        }
        let mut reader = ReaderBuilder::new()
            .has_headers(true)
            .flexible(false)
            .trim(csv::Trim::None)
            .from_reader(bytes);
        let headers = reader.headers().map_err(|_| CboeParseError::MalformedCsv)?;
        validate_header(self.schema, headers)?;
        let venue = match self.context.surface() {
            ReferenceSurface::CboeAllSeries { venue } => *venue,
            _ => return Err(CboeParseError::InvalidContext),
        };
        let mut returned = 0_u32;
        for result in reader.records() {
            returned = returned
                .checked_add(1)
                .ok_or(CboeParseError::RecordLimitExceeded)?;
            if returned > CBOE_ALL_SERIES_MAX_RECORDS {
                return Err(CboeParseError::RecordLimitExceeded);
            }
            let record = result.map_err(|_| CboeParseError::MalformedCsv)?;
            let row_number = returned
                .checked_add(1)
                .ok_or(CboeParseError::RecordLimitExceeded)?;
            sink(parse_record(
                row_number,
                venue,
                &record,
                self.context.clone(),
            )?)?;
        }
        if returned == 0 {
            return Err(CboeParseError::EmptyPublication);
        }
        Ok(CboeAllSeriesParseReceipt {
            schema: self.schema,
            context: self.context.clone(),
            returned_records: returned,
        })
    }
}

fn validate_header(
    schema: CboeAllSeriesCsvSchema,
    headers: &StringRecord,
) -> Result<(), CboeParseError> {
    let expected = schema.header();
    if headers.len() != expected.len()
        || headers
            .iter()
            .zip(expected)
            .any(|(actual, expected)| actual != expected)
    {
        return Err(CboeParseError::UnknownHeader);
    }
    Ok(())
}

fn parse_record(
    row_number: u32,
    venue: CboeVenue,
    record: &StringRecord,
    context: ReferenceObjectContext,
) -> Result<CboeSeriesReference, CboeParseError> {
    if record.len() != 5 {
        return Err(CboeParseError::MalformedCsv);
    }
    let field = |index| record.get(index).ok_or(CboeParseError::MalformedCsv);
    let symbol = CboeSymbolId::try_from_provider(field(0)?)?;
    let contract = OptionContractIdentity::try_from_osi(field(1)?)
        .map_err(|_| CboeParseError::InvalidOsiIdentity)?;
    let status = CboeSeriesStatus::try_from_provider(field(2)?)?;
    let underlying = field(3)?;
    let unit = field(4)?
        .parse::<u16>()
        .ok()
        .and_then(NonZeroU16::new)
        .ok_or(CboeParseError::InvalidUnit)?;
    CboeSeriesReference::try_new(
        row_number, venue, symbol, contract, underlying, unit, status, context,
    )
}

fn validate_payload(context: &ReferenceObjectContext, bytes: &[u8]) -> Result<(), CboeParseError> {
    if usize::try_from(context.payload_bytes()).ok() != Some(bytes.len())
        || context.payload_digest().algorithm() != DigestAlgorithm::Sha256
        || context.payload_digest().bytes() != <[u8; 32]>::from(Sha256::digest(bytes))
    {
        return Err(CboeParseError::PayloadMismatch);
    }
    Ok(())
}

/// Cboe header, row, identity, or bounded-stream decoding failure.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum CboeParseError {
    /// Raw-object evidence did not describe the supplied bytes.
    #[error("Cboe source payload does not match retained evidence")]
    PayloadMismatch,
    /// Provider, surface, venue, media type, or native schema was incompatible.
    #[error("invalid Cboe All Series object context")]
    InvalidContext,
    /// The source object exceeded the application parser limit.
    #[error("Cboe All Series object exceeds parser byte limit")]
    BodyTooLarge,
    /// The exact header was not one of the closed admitted revisions.
    #[error("unrecognized Cboe All Series CSV header")]
    UnknownHeader,
    /// CSV framing or field count was malformed.
    #[error("malformed Cboe All Series CSV")]
    MalformedCsv,
    /// The publication contained no series rows.
    #[error("empty Cboe All Series publication")]
    EmptyPublication,
    /// Valid row count exceeded the application parser bound.
    #[error("Cboe All Series record limit exceeded")]
    RecordLimitExceeded,
    /// The Cboe compressed symbol was not six-character base-62 text.
    #[error("invalid Cboe Symbol ID")]
    InvalidSymbolId,
    /// OSI identity was malformed.
    #[error("invalid Cboe OSI identity")]
    InvalidOsiIdentity,
    /// Underlying alias was missing or outside the selected file contract.
    #[error("invalid Cboe underlying alias")]
    InvalidUnderlying,
    /// Venue unit was missing, zero, or nonnumeric.
    #[error("invalid Cboe venue unit")]
    InvalidUnit,
    /// Series status was neither normal nor closing-only.
    #[error("invalid Cboe series status")]
    InvalidSeriesStatus,
    /// A stable row-evidence identifier could not be constructed.
    #[error("invalid Cboe row evidence")]
    InvalidEvidence,
    /// The caller-owned bounded sink rejected the record.
    #[error("Cboe record sink rejected the record")]
    SinkRejected,
}
