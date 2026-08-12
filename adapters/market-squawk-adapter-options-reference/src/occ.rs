use std::collections::BTreeSet;
use std::fmt::Write as _;
use std::num::NonZeroU32;

use csv::{ReaderBuilder, StringRecord};
use market_squawk_domain::{
    CalendarDate, DigestAlgorithm, EvidenceDigest, ProviderInstrumentId, SourceIdentifier,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::{
    PageTerminalState, ReferenceObjectContext, ReferencePageReceipt, ReferenceProvider,
    ReferenceSurface,
};

/// Application maximum for one OCC DLP text object.
///
/// This is a parser-safety policy, not a provider-published response ceiling.
pub const OCC_DLP_MAX_BYTES: usize = 64 * 1024 * 1024;

/// Application maximum for records in one OCC DLP text object.
///
/// This is a parser-safety policy, not a provider-published row ceiling.
pub const OCC_DLP_MAX_RECORDS: u32 = 1_000_000;

/// Application maximum for one OCC Information Memo index page/export.
///
/// This is a parser-safety policy, not a provider-published response ceiling.
pub const OCC_MEMO_MAX_BYTES: usize = 16 * 1024 * 1024;

/// Application maximum for memo discoveries in one page/export.
///
/// This is a parser-safety policy, not a provider-published row ceiling.
pub const OCC_MEMO_MAX_RECORDS: u32 = 100_000;

const MAX_SYMBOL_BYTES: usize = 32;
const MAX_SYMBOL_NAME_BYTES: usize = 512;
const MAX_MEMO_TITLE_BYTES: usize = 1_024;
const MAX_MEMO_CATEGORIES: usize = 16;

/// OCC product type from the current DLP download record layout.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub enum OccProductType {
    /// `EU` — equity underlying.
    #[serde(rename = "EU")]
    EquityUnderlying,
    /// `EB` — equity bounds.
    #[serde(rename = "EB")]
    EquityBounds,
    /// `EL` — equity long term.
    #[serde(rename = "EL")]
    EquityLongTerm,
    /// `EF` — equity FLEX.
    #[serde(rename = "EF")]
    EquityFlex,
    /// `CU` — currency underlying.
    #[serde(rename = "CU")]
    CurrencyUnderlying,
    /// `CL` — currency long term.
    #[serde(rename = "CL")]
    CurrencyLongTerm,
    /// `CM` — currency month end.
    #[serde(rename = "CM")]
    CurrencyMonthEnd,
    /// `CF` — currency FLEX.
    #[serde(rename = "CF")]
    CurrencyFlex,
    /// `IL` — index long term.
    #[serde(rename = "IL")]
    IndexLongTerm,
    /// `IU` — index underlying.
    #[serde(rename = "IU")]
    IndexUnderlying,
    /// `IF` — index FLEX.
    #[serde(rename = "IF")]
    IndexFlex,
    /// `GF` — interest-rate futures.
    #[serde(rename = "GF")]
    InterestRateFutures,
    /// `SF` — stock futures.
    #[serde(rename = "SF")]
    StockFutures,
    /// `FC` — futures cash index.
    #[serde(rename = "FC")]
    FuturesCashIndex,
    /// `FP` — futures physical index.
    #[serde(rename = "FP")]
    FuturesPhysicalIndex,
    /// `TU` — Treasury underlying.
    #[serde(rename = "TU")]
    TreasuryUnderlying,
    /// `TL` — Treasury long term.
    #[serde(rename = "TL")]
    TreasuryLongTerm,
}

impl OccProductType {
    fn try_from_provider(value: &str) -> Result<Self, OccParseError> {
        match value {
            "EU" => Ok(Self::EquityUnderlying),
            "EB" => Ok(Self::EquityBounds),
            "EL" => Ok(Self::EquityLongTerm),
            "EF" => Ok(Self::EquityFlex),
            "CU" => Ok(Self::CurrencyUnderlying),
            "CL" => Ok(Self::CurrencyLongTerm),
            "CM" => Ok(Self::CurrencyMonthEnd),
            "CF" => Ok(Self::CurrencyFlex),
            "IL" => Ok(Self::IndexLongTerm),
            "IU" => Ok(Self::IndexUnderlying),
            "IF" => Ok(Self::IndexFlex),
            "GF" => Ok(Self::InterestRateFutures),
            "SF" => Ok(Self::StockFutures),
            "FC" => Ok(Self::FuturesCashIndex),
            "FP" => Ok(Self::FuturesPhysicalIndex),
            "TU" => Ok(Self::TreasuryUnderlying),
            "TL" => Ok(Self::TreasuryLongTerm),
            _ => Err(OccParseError::InvalidProductType),
        }
    }

    const fn position_limit_required(self) -> bool {
        matches!(
            self,
            Self::EquityUnderlying | Self::EquityBounds | Self::EquityLongTerm | Self::EquityFlex
        )
    }
}

/// Single-letter exchange code from the current OCC DLP record layouts.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub enum OccExchangeCode {
    /// `A` — AMEX.
    #[serde(rename = "A")]
    Amex,
    /// `B` — BOX.
    #[serde(rename = "B")]
    Box,
    /// `C` — Cboe Options.
    #[serde(rename = "C")]
    Cboe,
    /// `D` — EMLD.
    #[serde(rename = "D")]
    Emld,
    /// `E` — EDGX.
    #[serde(rename = "E")]
    Edgx,
    /// `F` — CFE.
    #[serde(rename = "F")]
    Cfe,
    /// `H` — GEM.
    #[serde(rename = "H")]
    Gem,
    /// `I` — ISE.
    #[serde(rename = "I")]
    Ise,
    /// `J` — MCRY.
    #[serde(rename = "J")]
    Mcry,
    /// `K` — XMFE.
    #[serde(rename = "K")]
    Xmfe,
    /// `L` — SPHR.
    #[serde(rename = "L")]
    Sphr,
    /// `M` — MIAX.
    #[serde(rename = "M")]
    Miax,
    /// `P` — ARCA.
    #[serde(rename = "P")]
    Arca,
    /// `Q` — Nasdaq.
    #[serde(rename = "Q")]
    Nasdaq,
    /// `R` — MPRL.
    #[serde(rename = "R")]
    Mprl,
    /// `T` — NOBO.
    #[serde(rename = "T")]
    Nobo,
    /// `U` — MEMX.
    #[serde(rename = "U")]
    Memx,
    /// `W` — C2.
    #[serde(rename = "W")]
    C2,
    /// `X` — PHLX.
    #[serde(rename = "X")]
    Phlx,
    /// `Z` — BATS.
    #[serde(rename = "Z")]
    Bats,
}

impl OccExchangeCode {
    fn try_from_byte(value: u8) -> Result<Self, OccParseError> {
        match value {
            b'A' => Ok(Self::Amex),
            b'B' => Ok(Self::Box),
            b'C' => Ok(Self::Cboe),
            b'D' => Ok(Self::Emld),
            b'E' => Ok(Self::Edgx),
            b'F' => Ok(Self::Cfe),
            b'H' => Ok(Self::Gem),
            b'I' => Ok(Self::Ise),
            b'J' => Ok(Self::Mcry),
            b'K' => Ok(Self::Xmfe),
            b'L' => Ok(Self::Sphr),
            b'M' => Ok(Self::Miax),
            b'P' => Ok(Self::Arca),
            b'Q' => Ok(Self::Nasdaq),
            b'R' => Ok(Self::Mprl),
            b'T' => Ok(Self::Nobo),
            b'U' => Ok(Self::Memx),
            b'W' => Ok(Self::C2),
            b'X' => Ok(Self::Phlx),
            b'Z' => Ok(Self::Bats),
            _ => Err(OccParseError::InvalidExchangeCode),
        }
    }
}

/// Position-limit field state retained without converting unavailable to zero.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case", tag = "state", content = "value")]
pub enum OccPositionLimit {
    /// OCC reported an exact integral limit.
    Reported(u64),
    /// The DLP layout states the field is unavailable for the product type.
    NotAvailableForProduct,
}

/// Qualified meaning of presence in one exact OCC DLP publication.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum OccDlpPresence {
    /// Product/root appeared in this exact reference object.
    PresentInDirectoryPublication,
}

/// One provider-native OCC Directory of Listed Products row.
///
/// DLP is product/root reference evidence. It is not a contract series, quote, expiration, strike,
/// multiplier, deliverable, or tradability assertion.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct OccDlpProductReference {
    schema_version: u16,
    record_id: SourceIdentifier,
    provider_row_number: u32,
    options_symbol: ProviderInstrumentId,
    underlying_symbol: ProviderInstrumentId,
    symbol_name: String,
    trading_exchanges: Vec<OccExchangeCode>,
    position_limit: OccPositionLimit,
    product_type: OccProductType,
    presence: OccDlpPresence,
    object_context: ReferenceObjectContext,
}

impl OccDlpProductReference {
    /// Returns the stable provider row identity.
    pub const fn record_id(&self) -> &SourceIdentifier {
        &self.record_id
    }

    /// Returns the one-based provider row number.
    pub const fn provider_row_number(&self) -> u32 {
        self.provider_row_number
    }

    /// Returns the OCC options/product symbol.
    pub const fn options_symbol(&self) -> &ProviderInstrumentId {
        &self.options_symbol
    }

    /// Returns the provider-reported underlying alias.
    pub const fn underlying_symbol(&self) -> &ProviderInstrumentId {
        &self.underlying_symbol
    }

    /// Returns the source-preserved symbol name.
    pub fn symbol_name(&self) -> &str {
        &self.symbol_name
    }

    /// Returns every provider-reported trading-exchange code in deterministic order.
    pub fn trading_exchanges(&self) -> &[OccExchangeCode] {
        &self.trading_exchanges
    }

    /// Returns exact position-limit state.
    pub const fn position_limit(&self) -> OccPositionLimit {
        self.position_limit
    }

    /// Returns OCC's product type.
    pub const fn product_type(&self) -> OccProductType {
        self.product_type
    }

    /// Returns the qualified DLP-presence meaning.
    pub const fn presence(&self) -> OccDlpPresence {
        self.presence
    }

    /// Returns exact object, decoder, and clock lineage.
    pub const fn object_context(&self) -> &ReferenceObjectContext {
        &self.object_context
    }
}

/// Successful strict decode evidence for one OCC DLP text object.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct OccDlpParseReceipt {
    context: ReferenceObjectContext,
    returned_records: u32,
}

impl OccDlpParseReceipt {
    /// Returns valid decoded record count.
    pub const fn returned_records(&self) -> u32 {
        self.returned_records
    }

    /// Converts the strict complete text decode to terminal page evidence.
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

/// Strict streaming parser for OCC DLP `txt` with selected fields
/// `OS;US;SN;EXCH;PL;ONN` in that order.
#[derive(Clone, Debug)]
pub struct OccDlpParser {
    context: ReferenceObjectContext,
}

impl OccDlpParser {
    /// Binds exact text-object evidence to the closed six-field DLP decoder.
    ///
    /// # Errors
    ///
    /// Rejects a non-OCC/non-DLP context, wrong media type, or wrong native schema identity.
    pub fn try_new(context: ReferenceObjectContext) -> Result<Self, OccParseError> {
        if context.provider() != ReferenceProvider::Occ
            || !matches!(
                context.surface(),
                ReferenceSurface::OccDlpSelectedText | ReferenceSurface::OccDlpDailyText
            )
            || context.media_type().as_str() != "text/plain"
            || context.native_schema().as_str() != "occ-dlp-text-os-us-sn-exch-pl-onn-v1"
        {
            return Err(OccParseError::InvalidContext);
        }
        Ok(Self { context })
    }

    /// Decodes strict tab-separated provider text into a caller-owned bounded sink.
    ///
    /// # Errors
    ///
    /// Rejects payload drift, size/row bounds, malformed rows, unknown provider codes, missing
    /// required position limits, or sink rejection. Any failure suppresses a completion receipt.
    pub fn parse<F>(&self, bytes: &[u8], mut sink: F) -> Result<OccDlpParseReceipt, OccParseError>
    where
        F: FnMut(OccDlpProductReference) -> Result<(), OccParseError>,
    {
        validate_payload(&self.context, bytes, OCC_DLP_MAX_BYTES)?;
        let mut reader = ReaderBuilder::new()
            .delimiter(b'\t')
            .has_headers(false)
            .flexible(false)
            .trim(csv::Trim::None)
            .from_reader(bytes);
        let mut returned = 0_u32;
        for result in reader.records() {
            returned = returned
                .checked_add(1)
                .ok_or(OccParseError::RecordLimitExceeded)?;
            if returned > OCC_DLP_MAX_RECORDS {
                return Err(OccParseError::RecordLimitExceeded);
            }
            let row = result.map_err(|_| OccParseError::MalformedText)?;
            sink(parse_dlp_row(returned, &row, self.context.clone())?)?;
        }
        if returned == 0 {
            return Err(OccParseError::EmptyPublication);
        }
        Ok(OccDlpParseReceipt {
            context: self.context.clone(),
            returned_records: returned,
        })
    }
}

fn parse_dlp_row(
    row_number: u32,
    row: &StringRecord,
    context: ReferenceObjectContext,
) -> Result<OccDlpProductReference, OccParseError> {
    if row.len() != 6 {
        return Err(OccParseError::MalformedText);
    }
    let field = |index| row.get(index).ok_or(OccParseError::MalformedText);
    let options_symbol = parse_provider_symbol(field(0)?)?;
    let underlying_symbol = parse_provider_symbol(field(1)?)?;
    let symbol_name = field(2)?;
    if symbol_name.is_empty() || symbol_name.len() > MAX_SYMBOL_NAME_BYTES {
        return Err(OccParseError::InvalidSymbolName);
    }
    let trading_exchanges = parse_exchange_codes(field(3)?)?;
    let product_type = OccProductType::try_from_provider(field(5)?)?;
    let position_limit = match field(4)? {
        "" if product_type.position_limit_required() => {
            return Err(OccParseError::InvalidPositionLimit);
        }
        "" => OccPositionLimit::NotAvailableForProduct,
        value => OccPositionLimit::Reported(
            value
                .parse::<u64>()
                .map_err(|_| OccParseError::InvalidPositionLimit)?,
        ),
    };
    let record_id = SourceIdentifier::try_from(format!(
        "occ-dlp:{}:row-{row_number}",
        context.object_id().as_str()
    ))
    .map_err(|_| OccParseError::InvalidEvidence)?;
    Ok(OccDlpProductReference {
        schema_version: 1,
        record_id,
        provider_row_number: row_number,
        options_symbol,
        underlying_symbol,
        symbol_name: symbol_name.to_owned(),
        trading_exchanges,
        position_limit,
        product_type,
        presence: OccDlpPresence::PresentInDirectoryPublication,
        object_context: context,
    })
}

fn parse_provider_symbol(value: &str) -> Result<ProviderInstrumentId, OccParseError> {
    if value.is_empty()
        || value.len() > MAX_SYMBOL_BYTES
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-' | b'_' | b'/'))
    {
        return Err(OccParseError::InvalidSymbol);
    }
    ProviderInstrumentId::try_from(value).map_err(|_| OccParseError::InvalidSymbol)
}

fn parse_exchange_codes(value: &str) -> Result<Vec<OccExchangeCode>, OccParseError> {
    if value.is_empty() || value.starts_with(' ') || value.ends_with(' ') || value.contains("  ") {
        return Err(OccParseError::InvalidExchangeCode);
    }
    let mut result = BTreeSet::new();
    for byte in value.bytes().filter(|byte| *byte != b' ') {
        if !result.insert(OccExchangeCode::try_from_byte(byte)?) {
            return Err(OccParseError::DuplicateExchangeCode);
        }
    }
    if result.is_empty() {
        return Err(OccParseError::InvalidExchangeCode);
    }
    Ok(result.into_iter().collect())
}

/// OCC Information Memo category from the selected search/export surface.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum OccMemoCategory {
    /// Contract Adjustment.
    ContractAdjustment,
    /// Options.
    Options,
    /// Futures.
    Futures,
    /// Data Distribution Services.
    DataDistributionServices,
    /// Expiration.
    Expiration,
    /// Margins / Collateral / Clearing Fund.
    MarginsCollateralClearingFund,
    /// Operational.
    Operational,
    /// Options Disclosure Document.
    OptionsDisclosureDocument,
    /// Product / Series.
    ProductSeries,
    /// Other.
    Other,
    /// Outages.
    Outages,
}

impl OccMemoCategory {
    fn try_from_provider(value: &str) -> Result<Self, OccParseError> {
        match value {
            "Contract Adjustment" => Ok(Self::ContractAdjustment),
            "Options" => Ok(Self::Options),
            "Futures" => Ok(Self::Futures),
            "Data Distribution Services" => Ok(Self::DataDistributionServices),
            "Expiration" => Ok(Self::Expiration),
            "Margins/Collateral/Clearing Fund" | "Margins / Collateral / Clearing Fund" => {
                Ok(Self::MarginsCollateralClearingFund)
            }
            "Operational" => Ok(Self::Operational),
            "Options Disclosure Document" => Ok(Self::OptionsDisclosureDocument),
            "Product/Series" | "Product / Series" => Ok(Self::ProductSeries),
            "Other" => Ok(Self::Other),
            "Outages" => Ok(Self::Outages),
            _ => Err(OccParseError::InvalidMemoCategory),
        }
    }
}

/// Interpretation state enforced for an Information Memo search/index hit.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum OccMemoInterpretation {
    /// The title/categories are discovery metadata only; full memo and required attachments must
    /// be retained and parsed before any contract economics or lifecycle mutation is published.
    FullOperativeDocumentsRequired,
}

/// One source-reported OCC memo/event discovery with distinct posted and effective dates.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct OccMemoDiscovery {
    schema_version: u16,
    record_id: SourceIdentifier,
    memo_number: u64,
    posted_date: CalendarDate,
    effective_date: Option<CalendarDate>,
    title: String,
    categories: Vec<OccMemoCategory>,
    memo_locator: SourceIdentifier,
    interpretation: OccMemoInterpretation,
    discovery_digest: EvidenceDigest,
    object_context: ReferenceObjectContext,
}

impl OccMemoDiscovery {
    #[allow(
        clippy::too_many_arguments,
        reason = "memo discovery preserves every exact index field and object lineage"
    )]
    fn try_new(
        row_number: u32,
        memo_number: u64,
        posted_date: CalendarDate,
        effective_date: Option<CalendarDate>,
        title: String,
        categories: Vec<OccMemoCategory>,
        memo_locator: SourceIdentifier,
        object_context: ReferenceObjectContext,
    ) -> Result<Self, OccParseError> {
        if row_number == 0
            || memo_number == 0
            || title.is_empty()
            || title.len() > MAX_MEMO_TITLE_BYTES
            || categories.is_empty()
            || categories.len() > MAX_MEMO_CATEGORIES
        {
            return Err(OccParseError::InvalidMemo);
        }
        let mut sorted = categories;
        sorted.sort();
        if sorted.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(OccParseError::DuplicateMemoCategory);
        }
        let record_id = SourceIdentifier::try_from(format!(
            "occ-memo:{memo_number}:{}:row-{row_number}",
            object_context.object_id().as_str()
        ))
        .map_err(|_| OccParseError::InvalidEvidence)?;
        let discovery_digest = memo_discovery_digest(
            memo_number,
            posted_date,
            effective_date,
            &title,
            &sorted,
            &memo_locator,
            object_context.payload_digest(),
        );
        Ok(Self {
            schema_version: 1,
            record_id,
            memo_number,
            posted_date,
            effective_date,
            title,
            categories: sorted,
            memo_locator,
            interpretation: OccMemoInterpretation::FullOperativeDocumentsRequired,
            discovery_digest,
            object_context,
        })
    }

    /// Returns the exact row identity.
    pub const fn record_id(&self) -> &SourceIdentifier {
        &self.record_id
    }

    /// Returns the OCC memo number.
    pub const fn memo_number(&self) -> u64 {
        self.memo_number
    }

    /// Returns OCC's posting date without inventing a time of day.
    pub const fn posted_date(&self) -> CalendarDate {
        self.posted_date
    }

    /// Returns OCC's optional event-effective date, distinct from posting.
    pub const fn effective_date(&self) -> Option<CalendarDate> {
        self.effective_date
    }

    /// Returns the source-preserved title as discovery metadata only.
    pub fn title(&self) -> &str {
        &self.title
    }

    /// Returns exact provider categories.
    pub fn categories(&self) -> &[OccMemoCategory] {
        &self.categories
    }

    /// Returns the retained memo locator; it is not operative-document content evidence.
    pub const fn memo_locator(&self) -> &SourceIdentifier {
        &self.memo_locator
    }

    /// Returns the mandatory fail-closed interpretation state.
    pub const fn interpretation(&self) -> OccMemoInterpretation {
        self.interpretation
    }

    /// Returns the digest of every discovery field and source-object payload identity.
    pub const fn discovery_digest(&self) -> EvidenceDigest {
        self.discovery_digest
    }

    /// Returns the discovery digest as lowercase hexadecimal for stable conflict accounting.
    pub fn discovery_digest_hex(&self) -> String {
        let mut output = String::with_capacity(64);
        for byte in self.discovery_digest.bytes() {
            let _ = write!(&mut output, "{byte:02x}");
        }
        output
    }

    /// Returns exact object, decoder, and availability lineage.
    pub const fn object_context(&self) -> &ReferenceObjectContext {
        &self.object_context
    }
}

/// Exact admitted header label for the memo category column.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum OccMemoCsvSchema {
    /// Final header label is singular `Category`.
    CategoryV1,
    /// Final header label is plural `Categories`.
    CategoriesV1,
}

impl OccMemoCsvSchema {
    const fn header(self) -> [&'static str; 5] {
        [
            "Number",
            "Post Date",
            "Effective Date",
            "Title",
            match self {
                Self::CategoryV1 => "Category",
                Self::CategoriesV1 => "Categories",
            },
        ]
    }

    /// Returns the closed provider-native decoder identity.
    pub const fn native_schema(self) -> &'static str {
        match self {
            Self::CategoryV1 => "occ-memo-index-csv-category-v1",
            Self::CategoriesV1 => "occ-memo-index-csv-categories-v1",
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

/// Successful strict memo index decode and page closure.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct OccMemoParseReceipt {
    context: ReferenceObjectContext,
    page_ordinal: NonZeroU32,
    returned_records: u32,
    terminal_state: PageTerminalState,
}

impl OccMemoParseReceipt {
    /// Returns valid memo discovery count.
    pub const fn returned_records(&self) -> u32 {
        self.returned_records
    }

    /// Returns the one-based page coordinate.
    pub const fn page_ordinal(&self) -> NonZeroU32 {
        self.page_ordinal
    }

    /// Returns provider pagination terminal evidence.
    pub const fn terminal_state(&self) -> &PageTerminalState {
        &self.terminal_state
    }

    /// Converts this strict decode to a general publication page receipt.
    pub fn page_receipt(&self) -> ReferencePageReceipt {
        ReferencePageReceipt::new(
            self.context.clone(),
            self.page_ordinal,
            self.returned_records,
            0,
            self.terminal_state.clone(),
        )
    }
}

/// Strict OCC Information Memo CSV and closed JSON page decoder.
#[derive(Clone, Copy, Debug, Default)]
pub struct OccMemoParser;

impl OccMemoParser {
    /// Decodes one exact CSV export. The export is a single terminal object.
    ///
    /// # Errors
    ///
    /// Rejects context/payload drift, unknown headers, malformed dates/categories, bounds, or a
    /// sink failure. Titles remain uninterpreted discovery text.
    pub fn parse_csv<F>(
        schema: OccMemoCsvSchema,
        context: ReferenceObjectContext,
        bytes: &[u8],
        mut sink: F,
    ) -> Result<OccMemoParseReceipt, OccParseError>
    where
        F: FnMut(OccMemoDiscovery) -> Result<(), OccParseError>,
    {
        validate_memo_context(
            &context,
            &ReferenceSurface::OccMemoIndexCsv,
            "text/csv",
            schema.native_schema(),
        )?;
        validate_payload(&context, bytes, OCC_MEMO_MAX_BYTES)?;
        let mut reader = ReaderBuilder::new()
            .has_headers(true)
            .flexible(false)
            .trim(csv::Trim::None)
            .from_reader(bytes);
        let headers = reader.headers().map_err(|_| OccParseError::MalformedCsv)?;
        let expected = schema.header();
        if headers.len() != expected.len()
            || headers
                .iter()
                .zip(expected)
                .any(|(actual, expected)| actual != expected)
        {
            return Err(OccParseError::UnknownHeader);
        }
        let mut returned = 0_u32;
        for result in reader.records() {
            returned = returned
                .checked_add(1)
                .ok_or(OccParseError::RecordLimitExceeded)?;
            if returned > OCC_MEMO_MAX_RECORDS {
                return Err(OccParseError::RecordLimitExceeded);
            }
            let row = result.map_err(|_| OccParseError::MalformedCsv)?;
            sink(parse_memo_csv_row(
                returned.saturating_add(1),
                &row,
                context.clone(),
            )?)?;
        }
        Ok(OccMemoParseReceipt {
            context,
            page_ordinal: NonZeroU32::MIN,
            returned_records: returned,
            terminal_state: PageTerminalState::Terminal,
        })
    }

    /// Decodes the closed JSON memo-index page contract with explicit total-page evidence.
    ///
    /// # Errors
    ///
    /// Rejects unknown fields, context/payload drift, invalid pagination, malformed records,
    /// bounds, or sink failure. The JSON surface is not a contract-event economics parser.
    pub fn parse_json<F>(
        context: ReferenceObjectContext,
        bytes: &[u8],
        mut sink: F,
    ) -> Result<OccMemoParseReceipt, OccParseError>
    where
        F: FnMut(OccMemoDiscovery) -> Result<(), OccParseError>,
    {
        validate_memo_context(
            &context,
            &ReferenceSurface::OccMemoIndexJson,
            "application/json",
            "occ-memo-index-json-page-v1",
        )?;
        validate_payload(&context, bytes, OCC_MEMO_MAX_BYTES)?;
        let wire: MemoPageWire =
            serde_json::from_slice(bytes).map_err(|_| OccParseError::MalformedJson)?;
        let page = NonZeroU32::new(wire.page).ok_or(OccParseError::InvalidPagination)?;
        let total_pages =
            NonZeroU32::new(wire.total_pages).ok_or(OccParseError::InvalidPagination)?;
        if page > total_pages
            || wire.results.len() > OCC_MEMO_MAX_RECORDS as usize
            || (page < total_pages && wire.next_cursor.is_none())
            || (page == total_pages && wire.next_cursor.is_some())
        {
            return Err(OccParseError::InvalidPagination);
        }
        let terminal_state = match wire.next_cursor {
            Some(cursor) => PageTerminalState::More {
                next_cursor: SourceIdentifier::try_from(cursor)
                    .map_err(|_| OccParseError::InvalidPagination)?,
            },
            None => PageTerminalState::Terminal,
        };
        let mut returned = 0_u32;
        for record in wire.results {
            returned = returned
                .checked_add(1)
                .ok_or(OccParseError::RecordLimitExceeded)?;
            sink(parse_memo_wire(returned, record, context.clone())?)?;
        }
        Ok(OccMemoParseReceipt {
            context,
            page_ordinal: page,
            returned_records: returned,
            terminal_state,
        })
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct MemoPageWire {
    page: u32,
    total_pages: u32,
    next_cursor: Option<String>,
    results: Vec<MemoWire>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct MemoWire {
    number: u64,
    post_date: String,
    effective_date: Option<String>,
    title: String,
    categories: Vec<String>,
    memo_url: String,
}

fn parse_memo_csv_row(
    row_number: u32,
    row: &StringRecord,
    context: ReferenceObjectContext,
) -> Result<OccMemoDiscovery, OccParseError> {
    if row.len() != 5 {
        return Err(OccParseError::MalformedCsv);
    }
    let field = |index| row.get(index).ok_or(OccParseError::MalformedCsv);
    let memo_number = field(0)?
        .parse::<u64>()
        .map_err(|_| OccParseError::InvalidMemo)?;
    let posted_date = parse_mm_dd_yyyy(field(1)?)?;
    let effective_date = match field(2)? {
        "" => None,
        value => Some(parse_mm_dd_yyyy(value)?),
    };
    let categories = parse_categories(field(4)?.split('|'))?;
    let memo_locator = SourceIdentifier::try_from(format!(
        "https://infomemo.theocc.com/infomemos?number={memo_number}"
    ))
    .map_err(|_| OccParseError::InvalidMemo)?;
    OccMemoDiscovery::try_new(
        row_number,
        memo_number,
        posted_date,
        effective_date,
        field(3)?.to_owned(),
        categories,
        memo_locator,
        context,
    )
}

fn parse_memo_wire(
    row_number: u32,
    wire: MemoWire,
    context: ReferenceObjectContext,
) -> Result<OccMemoDiscovery, OccParseError> {
    let posted_date = parse_mm_dd_yyyy(&wire.post_date)?;
    let effective_date = wire
        .effective_date
        .as_deref()
        .map(parse_mm_dd_yyyy)
        .transpose()?;
    let categories = parse_categories(wire.categories.iter().map(String::as_str))?;
    let memo_locator =
        SourceIdentifier::try_from(wire.memo_url).map_err(|_| OccParseError::InvalidMemo)?;
    OccMemoDiscovery::try_new(
        row_number,
        wire.number,
        posted_date,
        effective_date,
        wire.title,
        categories,
        memo_locator,
        context,
    )
}

fn parse_categories<'a, I>(values: I) -> Result<Vec<OccMemoCategory>, OccParseError>
where
    I: IntoIterator<Item = &'a str>,
{
    let mut result = Vec::new();
    for value in values {
        if value.is_empty() || result.len() >= MAX_MEMO_CATEGORIES {
            return Err(OccParseError::InvalidMemoCategory);
        }
        result.push(OccMemoCategory::try_from_provider(value)?);
    }
    if result.is_empty() {
        return Err(OccParseError::InvalidMemoCategory);
    }
    Ok(result)
}

fn parse_mm_dd_yyyy(value: &str) -> Result<CalendarDate, OccParseError> {
    if value.len() != 10
        || value.as_bytes().get(2) != Some(&b'/')
        || value.as_bytes().get(5) != Some(&b'/')
    {
        return Err(OccParseError::InvalidDate);
    }
    let month = value[0..2]
        .parse::<u8>()
        .map_err(|_| OccParseError::InvalidDate)?;
    let day = value[3..5]
        .parse::<u8>()
        .map_err(|_| OccParseError::InvalidDate)?;
    let year = value[6..10]
        .parse::<u16>()
        .map_err(|_| OccParseError::InvalidDate)?;
    CalendarDate::new(year, month, day).map_err(|_| OccParseError::InvalidDate)
}

fn validate_memo_context(
    context: &ReferenceObjectContext,
    surface: &ReferenceSurface,
    media_type: &str,
    native_schema: &str,
) -> Result<(), OccParseError> {
    if context.provider() != ReferenceProvider::Occ
        || context.surface() != surface
        || context.media_type().as_str() != media_type
        || context.native_schema().as_str() != native_schema
    {
        return Err(OccParseError::InvalidContext);
    }
    Ok(())
}

fn validate_payload(
    context: &ReferenceObjectContext,
    bytes: &[u8],
    maximum: usize,
) -> Result<(), OccParseError> {
    if bytes.len() > maximum {
        return Err(OccParseError::BodyTooLarge);
    }
    if usize::try_from(context.payload_bytes()).ok() != Some(bytes.len())
        || context.payload_digest().algorithm() != DigestAlgorithm::Sha256
        || context.payload_digest().bytes() != <[u8; 32]>::from(Sha256::digest(bytes))
    {
        return Err(OccParseError::PayloadMismatch);
    }
    Ok(())
}

fn memo_discovery_digest(
    memo_number: u64,
    posted_date: CalendarDate,
    effective_date: Option<CalendarDate>,
    title: &str,
    categories: &[OccMemoCategory],
    memo_locator: &SourceIdentifier,
    payload_digest: EvidenceDigest,
) -> EvidenceDigest {
    let mut digest = Sha256::new();
    digest.update(b"market-squawk:occ-memo-discovery:v1\0");
    digest.update(memo_number.to_be_bytes());
    digest.update(posted_date.year().to_be_bytes());
    digest.update([posted_date.month(), posted_date.day()]);
    match effective_date {
        Some(date) => {
            digest.update([1]);
            digest.update(date.year().to_be_bytes());
            digest.update([date.month(), date.day()]);
        }
        None => digest.update([0]),
    }
    digest.update((title.len() as u64).to_be_bytes());
    digest.update(title.as_bytes());
    for category in categories {
        digest.update([*category as u8]);
    }
    digest.update(memo_locator.as_str().as_bytes());
    digest.update(payload_digest.bytes());
    EvidenceDigest::new(DigestAlgorithm::Sha256, digest.finalize().into())
}

/// OCC source-object, row, pagination, or bounded-parser failure.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum OccParseError {
    /// Raw-object evidence did not match supplied bytes.
    #[error("OCC source payload does not match retained evidence")]
    PayloadMismatch,
    /// Provider, surface, media type, or native schema was incompatible.
    #[error("invalid OCC source object context")]
    InvalidContext,
    /// The source object exceeded the application parser byte limit.
    #[error("OCC source object exceeds parser byte limit")]
    BodyTooLarge,
    /// Text framing or exact field count was malformed.
    #[error("malformed OCC DLP text")]
    MalformedText,
    /// CSV framing or exact field count was malformed.
    #[error("malformed OCC Information Memo CSV")]
    MalformedCsv,
    /// JSON framing or its closed object shape was malformed.
    #[error("malformed OCC Information Memo JSON")]
    MalformedJson,
    /// The exact CSV header was not admitted.
    #[error("unrecognized OCC Information Memo CSV header")]
    UnknownHeader,
    /// A strict DLP publication contained no rows.
    #[error("empty OCC DLP publication")]
    EmptyPublication,
    /// Decoded record count exceeded the application parser bound.
    #[error("OCC record limit exceeded")]
    RecordLimitExceeded,
    /// An OCC product or underlying alias was invalid.
    #[error("invalid OCC product symbol")]
    InvalidSymbol,
    /// Symbol name was absent or exceeded its bound.
    #[error("invalid OCC symbol name")]
    InvalidSymbolName,
    /// Product type was outside the closed DLP layout.
    #[error("invalid OCC product type")]
    InvalidProductType,
    /// Exchange code was outside the closed DLP layout.
    #[error("invalid OCC exchange code")]
    InvalidExchangeCode,
    /// An exchange code was repeated in one row.
    #[error("duplicate OCC exchange code")]
    DuplicateExchangeCode,
    /// Position-limit state contradicted the product or field contract.
    #[error("invalid OCC position limit")]
    InvalidPositionLimit,
    /// Memo number, title, category closure, or locator was invalid.
    #[error("invalid OCC memo discovery")]
    InvalidMemo,
    /// Posting or effective date was malformed.
    #[error("invalid OCC memo date")]
    InvalidDate,
    /// Memo category was unknown or exceeded the closed bound.
    #[error("invalid OCC memo category")]
    InvalidMemoCategory,
    /// Memo category was repeated.
    #[error("duplicate OCC memo category")]
    DuplicateMemoCategory,
    /// Page ordinals, totals, or next-cursor evidence were inconsistent.
    #[error("invalid OCC memo pagination")]
    InvalidPagination,
    /// A stable evidence identity could not be constructed.
    #[error("invalid OCC evidence identity")]
    InvalidEvidence,
    /// The caller-owned bounded sink rejected a record.
    #[error("OCC record sink rejected the record")]
    SinkRejected,
}
