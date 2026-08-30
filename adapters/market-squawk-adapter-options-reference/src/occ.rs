use std::cell::Cell;
use std::collections::BTreeSet;
use std::fmt::Write as _;
use std::io::{BufReader, Read};
use std::num::{NonZeroU32, NonZeroU64};

use csv::{ReaderBuilder, StringRecord};
use market_squawk_domain::{
    CalendarDate, DigestAlgorithm, EvidenceDigest, ProviderInstrumentId, SourceIdentifier,
};
use quick_xml::Reader;
use quick_xml::events::Event;
use serde::{
    Deserialize, Serialize,
    de::{self, DeserializeSeed, MapAccess, SeqAccess, Visitor},
};
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::{
    PageTerminalState, ReferenceObjectContext, ReferencePageReceipt, ReferenceProvider,
    ReferenceSurface,
    export::{ReferenceAliasAssertionSetEvidence, visit_occ_alias_assertions},
    payload::{BoundedTokenReader, ExactPayloadReader},
    publication::StrictReferenceRowSetDigestBuilder,
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
const OCC_PARSER_MAX_TOKEN_BYTES: usize = 4 * 1024;

/// Exact code-owned OCC DLP body contract selected after transport admission.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum OccDlpSchema {
    /// Current selected-field `delo-download` text: fixed-width fields plus a terminal empty field.
    SelectedTextV1,
    /// Current dated `daily-delo-download` headerless six-column text publication.
    DailyTextV1,
    /// Current dated `results/record` XML publication with the same six financial fields.
    DailyXmlV1,
}
impl OccDlpSchema {
    /// Returns the stable provider-native decoder identity.
    pub const fn native_schema(self) -> &'static str {
        match self {
            Self::SelectedTextV1 => "occ-dlp-selected-text-os-us-sn-exch-pl-onn-v1",
            Self::DailyTextV1 => "occ-dlp-daily-text-v1",
            Self::DailyXmlV1 => "occ-dlp-daily-xml-results-record-v1",
        }
    }

    /// Returns the canonical media type retained after exact response admission.
    pub const fn media_type(self) -> &'static str {
        match self {
            Self::SelectedTextV1 | Self::DailyTextV1 => "text/plain",
            Self::DailyXmlV1 => "application/xml",
        }
    }
}

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
    pub(crate) fn try_from_provider(value: &str) -> Result<Self, OccParseError> {
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

    const fn is_equity_product(self) -> bool {
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
    pub(crate) fn try_from_byte(value: u8) -> Result<Self, OccParseError> {
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
    /// A documented equity product carried an exact nonzero position limit.
    EquityReported(NonZeroU64),
    /// A non-equity product carried source zero, consistent with the layout's unavailable rule.
    NonEquityUnavailableZero,
    /// A non-equity row carried a nonzero source value despite the published layout saying the
    /// field is unavailable. The raw value is retained but is not promoted to decision-usable
    /// position-limit authority.
    NonEquityProviderValueOutsideDocumentedScope {
        /// Exact unexpected nonzero source value.
        raw_value: NonZeroU64,
    },
}

/// Qualified meaning of presence in one exact OCC DLP publication.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum OccDlpPresence {
    /// Product/root appeared in this exact reference object.
    PresentInDirectoryPublication,
}

/// Exact exchange-list evidence carried by an OCC DLP row.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum OccExchangeListingEvidence {
    /// One or more exact OCC exchange codes were reported.
    Reported,
    /// The selected-directory wire carried its documented single-blank exchange sentinel.
    /// Directory presence alone must not be promoted to current tradability.
    NotReportedInSelectedDirectory,
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
    exchange_listing_evidence: OccExchangeListingEvidence,
    position_limit: OccPositionLimit,
    product_type: OccProductType,
    presence: OccDlpPresence,
    object_context: ReferenceObjectContext,
}

#[derive(Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct OccDlpNativeSemanticsV1<'a> {
    options_symbol: &'a ProviderInstrumentId,
    underlying_symbol: &'a ProviderInstrumentId,
    symbol_name: &'a str,
    trading_exchanges: &'a [OccExchangeCode],
    exchange_listing_evidence: OccExchangeListingEvidence,
    position_limit: OccPositionLimit,
    product_type: OccProductType,
    presence: OccDlpPresence,
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

    /// Returns whether exact exchange codes were present or the selected-directory blank sentinel
    /// was retained.
    pub const fn exchange_listing_evidence(&self) -> OccExchangeListingEvidence {
        self.exchange_listing_evidence
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

    pub(crate) fn native_semantics(&self) -> OccDlpNativeSemanticsV1<'_> {
        OccDlpNativeSemanticsV1 {
            options_symbol: &self.options_symbol,
            underlying_symbol: &self.underlying_symbol,
            symbol_name: &self.symbol_name,
            trading_exchanges: &self.trading_exchanges,
            exchange_listing_evidence: self.exchange_listing_evidence,
            position_limit: self.position_limit,
            product_type: self.product_type,
            presence: self.presence,
        }
    }
}

/// Successful strict decode evidence for one OCC DLP text object.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct OccDlpParseReceipt {
    context: ReferenceObjectContext,
    returned_records: u32,
    strict_row_set_digest: EvidenceDigest,
    alias_assertion_set: ReferenceAliasAssertionSetEvidence,
}

impl OccDlpParseReceipt {
    /// Returns valid decoded record count.
    pub const fn returned_records(&self) -> u32 {
        self.returned_records
    }

    /// Returns the exact ordered identity of every accepted provider-native row.
    pub const fn strict_row_set_digest(&self) -> EvidenceDigest {
        self.strict_row_set_digest
    }

    /// Converts the strict complete text decode to terminal page evidence.
    pub fn page_receipt(&self) -> ReferencePageReceipt {
        ReferencePageReceipt::new(
            self.context.clone(),
            NonZeroU32::MIN,
            self.returned_records,
            0,
            self.strict_row_set_digest,
            self.alias_assertion_set,
            PageTerminalState::Terminal,
        )
    }
}

/// Strict streaming parser for OCC DLP `txt` with selected fields
/// `OS;US;SN;EXCH;PL;ONN` in that order.
#[derive(Clone, Debug)]
pub struct OccDlpParser {
    context: ReferenceObjectContext,
    schema: OccDlpSchema,
}

impl OccDlpParser {
    /// Binds exact text-object evidence to the closed six-field DLP decoder.
    ///
    /// # Errors
    ///
    /// Rejects a non-OCC/non-DLP context, wrong media type, or wrong native schema identity.
    pub fn try_new(context: ReferenceObjectContext) -> Result<Self, OccParseError> {
        let schema = match context.native_schema().as_str() {
            "occ-dlp-selected-text-os-us-sn-exch-pl-onn-v1" => OccDlpSchema::SelectedTextV1,
            "occ-dlp-daily-text-v1" => OccDlpSchema::DailyTextV1,
            "occ-dlp-daily-xml-results-record-v1" => OccDlpSchema::DailyXmlV1,
            _ => return Err(OccParseError::InvalidContext),
        };
        if context.provider() != ReferenceProvider::Occ
            || !matches!(
                context.surface(),
                ReferenceSurface::OccDlpSelectedText
                    | ReferenceSurface::OccDlpDailyText
                    | ReferenceSurface::OccDlpDailyXml
            )
            || context.media_type().as_str() != schema.media_type()
            || !matches!(
                (context.surface(), schema),
                (
                    ReferenceSurface::OccDlpSelectedText,
                    OccDlpSchema::SelectedTextV1
                ) | (ReferenceSurface::OccDlpDailyText, OccDlpSchema::DailyTextV1)
                    | (ReferenceSurface::OccDlpDailyXml, OccDlpSchema::DailyXmlV1)
            )
        {
            return Err(OccParseError::InvalidContext);
        }
        Ok(Self { context, schema })
    }

    /// Decodes strict tab-separated provider text into a caller-owned bounded sink.
    ///
    /// # Errors
    ///
    /// Rejects payload drift, size/row bounds, malformed rows, unknown provider codes, missing
    /// required position limits, or sink rejection. Any failure suppresses a completion receipt.
    pub fn parse<R, F>(&self, source: R, sink: F) -> Result<OccDlpParseReceipt, OccParseError>
    where
        R: Read,
        F: FnMut(OccDlpProductReference) -> Result<(), OccParseError>,
    {
        if usize::try_from(self.context.payload_bytes())
            .map_or(true, |bytes| bytes > OCC_DLP_MAX_BYTES)
        {
            return Err(OccParseError::BodyTooLarge);
        }
        let payload = ExactPayloadReader::try_new(source, &self.context, OCC_DLP_MAX_BYTES)
            .map_err(|_| OccParseError::PayloadMismatch)?;
        match self.schema {
            OccDlpSchema::SelectedTextV1 => {
                self.parse_text(payload, TextWireSchema::Selected, sink)
            }
            OccDlpSchema::DailyTextV1 => self.parse_text(payload, TextWireSchema::Daily, sink),
            OccDlpSchema::DailyXmlV1 => self.parse_xml(payload, sink),
        }
    }

    fn parse_text<R, F>(
        &self,
        payload: ExactPayloadReader<R>,
        wire_schema: TextWireSchema,
        mut sink: F,
    ) -> Result<OccDlpParseReceipt, OccParseError>
    where
        R: Read,
        F: FnMut(OccDlpProductReference) -> Result<(), OccParseError>,
    {
        let mut framed = BoundedTokenReader::lines(payload, OCC_PARSER_MAX_TOKEN_BYTES)
            .map_err(|_| OccParseError::BodyTooLarge)?;
        let mut reader = ReaderBuilder::new()
            .delimiter(b'\t')
            .has_headers(false)
            .flexible(false)
            .quoting(false)
            .trim(csv::Trim::None)
            .from_reader(&mut framed);
        let mut returned = 0_u32;
        let mut strict_rows = StrictReferenceRowSetDigestBuilder::new();
        let mut alias_assertion_set = ReferenceAliasAssertionSetEvidence::empty();
        for result in reader.records() {
            returned = returned
                .checked_add(1)
                .ok_or(OccParseError::RecordLimitExceeded)?;
            if returned > OCC_DLP_MAX_RECORDS {
                return Err(OccParseError::RecordLimitExceeded);
            }
            let row = result.map_err(|_| OccParseError::MalformedText)?;
            let record = parse_dlp_text_row(returned, &row, wire_schema, self.context.clone())?;
            strict_rows
                .try_observe(&record.native_semantics())
                .map_err(|_| OccParseError::InvalidEvidence)?;
            visit_occ_alias_assertions(&record, |assertion| {
                alias_assertion_set.try_observe(&assertion)
            })
            .map_err(|_| OccParseError::InvalidEvidence)?;
            sink(record)?;
        }
        drop(reader);
        let terminal = framed
            .into_inner()
            .finish()
            .map_err(|_| OccParseError::PayloadMismatch)?;
        if !terminal.has_only_complete_crlf_records() || terminal.contains_nul() {
            return Err(OccParseError::IncompletePublication);
        }
        if returned == 0 {
            return Err(OccParseError::EmptyPublication);
        }
        Ok(OccDlpParseReceipt {
            context: self.context.clone(),
            returned_records: returned,
            strict_row_set_digest: strict_rows
                .finish(returned)
                .map_err(|_| OccParseError::InvalidEvidence)?,
            alias_assertion_set,
        })
    }

    fn parse_xml<R, F>(
        &self,
        payload: ExactPayloadReader<R>,
        mut sink: F,
    ) -> Result<OccDlpParseReceipt, OccParseError>
    where
        R: Read,
        F: FnMut(OccDlpProductReference) -> Result<(), OccParseError>,
    {
        let mut framed = BoundedTokenReader::markup(payload, OCC_PARSER_MAX_TOKEN_BYTES)
            .map_err(|_| OccParseError::BodyTooLarge)?;
        let mut reader = Reader::from_reader(BufReader::new(&mut framed));
        reader.config_mut().trim_text(false);
        let mut event_buffer = Vec::with_capacity(4_096);
        let mut depth = 0_usize;
        let mut saw_declaration = false;
        let mut saw_root = false;
        let mut closed_root = false;
        let mut in_record = false;
        let mut record = OccXmlRecord::default();
        let mut returned = 0_u32;
        let mut strict_rows = StrictReferenceRowSetDigestBuilder::new();
        let mut alias_assertion_set = ReferenceAliasAssertionSetEvidence::empty();

        loop {
            match reader.read_event_into(&mut event_buffer) {
                Ok(Event::Decl(declaration)) if depth == 0 && !saw_declaration && !saw_root => {
                    if declaration.version().ok().as_deref() != Some(b"1.0")
                        || declaration.encoding().transpose().ok().flatten().as_deref()
                            != Some(b"ISO-8859-1")
                    {
                        return Err(OccParseError::UnknownXmlSchema);
                    }
                    saw_declaration = true;
                }
                Ok(Event::Start(start)) => {
                    if start.attributes().next().is_some() {
                        return Err(OccParseError::UnknownXmlSchema);
                    }
                    depth = depth
                        .checked_add(1)
                        .ok_or(OccParseError::UnknownXmlSchema)?;
                    let name = start.name();
                    match depth {
                        1 if name.as_ref() == b"results" && !saw_root => saw_root = true,
                        2 if name.as_ref() == b"record" && saw_root && !in_record => {
                            in_record = true;
                            record = OccXmlRecord::default();
                        }
                        3 if in_record => record.begin(name.as_ref())?,
                        _ => return Err(OccParseError::UnknownXmlSchema),
                    }
                }
                Ok(Event::Text(text)) => {
                    let decoded = text.decode().map_err(|_| OccParseError::MalformedXml)?;
                    let value = quick_xml::escape::unescape(&decoded)
                        .map_err(|_| OccParseError::MalformedXml)?;
                    if record.active_field.is_none() {
                        let structural_depth =
                            depth == 0 || (depth == 1 && !in_record) || (depth == 2 && in_record);
                        if !structural_depth
                            || !value.bytes().all(|byte| byte.is_ascii_whitespace())
                        {
                            return Err(OccParseError::UnknownXmlSchema);
                        }
                    } else if depth == 3 && in_record && record.active_field == Some(2) {
                        return Err(OccParseError::UnknownXmlSchema);
                    } else if depth == 3 && in_record {
                        record.append(value.as_ref())?;
                    } else {
                        return Err(OccParseError::UnknownXmlSchema);
                    }
                }
                Ok(Event::CData(text)) if depth == 3 && in_record => {
                    let value = text.decode().map_err(|_| OccParseError::MalformedXml)?;
                    record.append_cdata(value.as_ref())?;
                }
                Ok(Event::End(end)) => {
                    let name = end.name();
                    match depth {
                        3 if in_record => record.end(name.as_ref())?,
                        2 if in_record && name.as_ref() == b"record" => {
                            returned = returned
                                .checked_add(1)
                                .ok_or(OccParseError::RecordLimitExceeded)?;
                            if returned > OCC_DLP_MAX_RECORDS {
                                return Err(OccParseError::RecordLimitExceeded);
                            }
                            let completed = record.finish(returned, self.context.clone())?;
                            strict_rows
                                .try_observe(&completed.native_semantics())
                                .map_err(|_| OccParseError::InvalidEvidence)?;
                            visit_occ_alias_assertions(&completed, |assertion| {
                                alias_assertion_set.try_observe(&assertion)
                            })
                            .map_err(|_| OccParseError::InvalidEvidence)?;
                            sink(completed)?;
                            in_record = false;
                            record = OccXmlRecord::default();
                        }
                        1 if name.as_ref() == b"results" && saw_root && !closed_root => {
                            closed_root = true;
                        }
                        _ => return Err(OccParseError::UnknownXmlSchema),
                    }
                    depth = depth
                        .checked_sub(1)
                        .ok_or(OccParseError::UnknownXmlSchema)?;
                }
                Ok(Event::Eof) => break,
                Ok(
                    Event::Decl(_)
                    | Event::Empty(_)
                    | Event::Comment(_)
                    | Event::DocType(_)
                    | Event::PI(_)
                    | Event::GeneralRef(_)
                    | Event::CData(_),
                ) => return Err(OccParseError::UnknownXmlSchema),
                Err(_) => return Err(OccParseError::MalformedXml),
            }
            event_buffer.clear();
        }
        drop(reader);
        let terminal = framed
            .into_inner()
            .finish()
            .map_err(|_| OccParseError::PayloadMismatch)?;
        if depth != 0
            || !saw_declaration
            || !saw_root
            || !closed_root
            || in_record
            || !record.is_empty()
            || terminal.contains_nul()
            || returned == 0
        {
            return Err(OccParseError::IncompletePublication);
        }
        Ok(OccDlpParseReceipt {
            context: self.context.clone(),
            returned_records: returned,
            strict_row_set_digest: strict_rows
                .finish(returned)
                .map_err(|_| OccParseError::InvalidEvidence)?,
            alias_assertion_set,
        })
    }
}

const OCC_XML_FIELDS: [&[u8]; 6] = [
    b"optionSymbol",
    b"underlyingSymbol",
    b"symbolName",
    b"positionLimit",
    b"onnProductType",
    b"exchanges",
];

struct OccXmlRecord {
    fields: [Option<String>; 6],
    next_field: usize,
    active_field: Option<usize>,
}

impl Default for OccXmlRecord {
    fn default() -> Self {
        Self {
            fields: std::array::from_fn(|_| None),
            next_field: 0,
            active_field: None,
        }
    }
}

impl OccXmlRecord {
    fn begin(&mut self, name: &[u8]) -> Result<(), OccParseError> {
        let expected = OCC_XML_FIELDS
            .get(self.next_field)
            .ok_or(OccParseError::UnknownXmlSchema)?;
        if self.active_field.is_some() || name != *expected {
            return Err(OccParseError::UnknownXmlSchema);
        }
        self.fields[self.next_field] = Some(String::new());
        self.active_field = Some(self.next_field);
        Ok(())
    }

    fn append(&mut self, value: &str) -> Result<(), OccParseError> {
        let index = self.active_field.ok_or(OccParseError::UnknownXmlSchema)?;
        let maximum = match index {
            0 | 1 => MAX_SYMBOL_BYTES,
            2 => MAX_SYMBOL_NAME_BYTES,
            3 => 20,
            4 => 2,
            5 => 32,
            _ => return Err(OccParseError::UnknownXmlSchema),
        };
        let target = self.fields[index]
            .as_mut()
            .ok_or(OccParseError::UnknownXmlSchema)?;
        if target.len().saturating_add(value.len()) > maximum {
            return Err(OccParseError::FieldTooLarge);
        }
        target.push_str(value);
        Ok(())
    }

    fn append_cdata(&mut self, value: &str) -> Result<(), OccParseError> {
        if self.active_field != Some(2) {
            return Err(OccParseError::UnknownXmlSchema);
        }
        self.append(value)
    }

    fn end(&mut self, name: &[u8]) -> Result<(), OccParseError> {
        let index = self.active_field.ok_or(OccParseError::UnknownXmlSchema)?;
        if name != OCC_XML_FIELDS[index] {
            return Err(OccParseError::UnknownXmlSchema);
        }
        self.active_field = None;
        self.next_field = self
            .next_field
            .checked_add(1)
            .ok_or(OccParseError::UnknownXmlSchema)?;
        Ok(())
    }

    fn finish(
        self,
        row_number: u32,
        context: ReferenceObjectContext,
    ) -> Result<OccDlpProductReference, OccParseError> {
        if self.next_field != OCC_XML_FIELDS.len() || self.active_field.is_some() {
            return Err(OccParseError::UnknownXmlSchema);
        }
        let mut fields = self.fields.into_iter();
        let values: [String; 6] =
            std::array::from_fn(|_| fields.next().flatten().unwrap_or_default());
        parse_dlp_values(
            row_number,
            [
                values[0].as_str(),
                values[1].as_str(),
                values[2].as_str(),
                values[5].as_str(),
                values[3].as_str(),
                values[4].as_str(),
            ],
            PositionLimitWireFormat::CanonicalDecimal,
            ExchangeWireFormat::CodesRequired,
            context,
        )
    }

    fn is_empty(&self) -> bool {
        self.next_field == 0
            && self.active_field.is_none()
            && self.fields.iter().all(Option::is_none)
    }
}

fn parse_dlp_text_row(
    row_number: u32,
    row: &StringRecord,
    wire_schema: TextWireSchema,
    context: ReferenceObjectContext,
) -> Result<OccDlpProductReference, OccParseError> {
    let field = |index| row.get(index).ok_or(OccParseError::MalformedText);
    match wire_schema {
        TextWireSchema::Daily => {
            if row.len() != 6 {
                return Err(OccParseError::MalformedText);
            }
            parse_dlp_values(
                row_number,
                [
                    field(0)?,
                    field(1)?,
                    field(2)?,
                    field(3)?,
                    field(4)?,
                    field(5)?,
                ],
                PositionLimitWireFormat::TextFixedTwelveDigits,
                ExchangeWireFormat::CodesRequired,
                context,
            )
        }
        TextWireSchema::Selected => {
            if row.len() != 7
                || !field(6)?.is_empty()
                || field(0)?.len() != 6
                || field(1)?.len() != 6
                || field(2)?.len() != 50
            {
                return Err(OccParseError::MalformedText);
            }
            let options_symbol = right_trim_fixed_field(field(0)?)?;
            let underlying_symbol = right_trim_fixed_field(field(1)?)?;
            let symbol_name = right_trim_fixed_field(field(2)?)?;
            parse_dlp_values(
                row_number,
                [
                    options_symbol,
                    underlying_symbol,
                    symbol_name,
                    field(3)?,
                    field(4)?,
                    field(5)?,
                ],
                PositionLimitWireFormat::CanonicalDecimal,
                ExchangeWireFormat::SelectedBlankAllowed,
                context,
            )
        }
    }
}

#[derive(Clone, Copy)]
enum PositionLimitWireFormat {
    TextFixedTwelveDigits,
    CanonicalDecimal,
}

#[derive(Clone, Copy)]
enum TextWireSchema {
    Selected,
    Daily,
}

#[derive(Clone, Copy)]
enum ExchangeWireFormat {
    CodesRequired,
    SelectedBlankAllowed,
}

fn parse_dlp_values(
    row_number: u32,
    fields: [&str; 6],
    position_wire_format: PositionLimitWireFormat,
    exchange_wire_format: ExchangeWireFormat,
    context: ReferenceObjectContext,
) -> Result<OccDlpProductReference, OccParseError> {
    let options_symbol = parse_provider_symbol(fields[0])?;
    let underlying_symbol = parse_provider_symbol(fields[1])?;
    let symbol_name = fields[2];
    if symbol_name.is_empty()
        || symbol_name.len() > MAX_SYMBOL_NAME_BYTES
        || symbol_name.chars().any(char::is_control)
    {
        return Err(OccParseError::InvalidSymbolName);
    }
    let (trading_exchanges, exchange_listing_evidence) =
        parse_exchange_codes(fields[3], exchange_wire_format)?;
    let product_type = OccProductType::try_from_provider(fields[5])?;
    let raw_position_limit = fields[4];
    let valid_position_wire = match position_wire_format {
        PositionLimitWireFormat::TextFixedTwelveDigits => raw_position_limit.len() == 12,
        PositionLimitWireFormat::CanonicalDecimal => {
            !raw_position_limit.is_empty()
                && raw_position_limit.len() <= 20
                && (raw_position_limit == "0" || !raw_position_limit.starts_with('0'))
        }
    } && raw_position_limit.bytes().all(|byte| byte.is_ascii_digit());
    if !valid_position_wire {
        return Err(OccParseError::InvalidPositionLimit);
    }
    let parsed_position_limit = raw_position_limit
        .parse::<u64>()
        .map_err(|_| OccParseError::InvalidPositionLimit)?;
    let position_limit = match (product_type.is_equity_product(), parsed_position_limit) {
        (true, 0) => return Err(OccParseError::InvalidPositionLimit),
        (true, value) => OccPositionLimit::EquityReported(
            NonZeroU64::new(value).ok_or(OccParseError::InvalidPositionLimit)?,
        ),
        (false, 0) => OccPositionLimit::NonEquityUnavailableZero,
        (false, value) => OccPositionLimit::NonEquityProviderValueOutsideDocumentedScope {
            raw_value: NonZeroU64::new(value).ok_or(OccParseError::InvalidPositionLimit)?,
        },
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
        exchange_listing_evidence,
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

fn parse_exchange_codes(
    value: &str,
    wire_format: ExchangeWireFormat,
) -> Result<(Vec<OccExchangeCode>, OccExchangeListingEvidence), OccParseError> {
    if matches!(wire_format, ExchangeWireFormat::SelectedBlankAllowed) && value == " " {
        return Ok((
            Vec::new(),
            OccExchangeListingEvidence::NotReportedInSelectedDirectory,
        ));
    }
    if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_uppercase()) {
        return Err(OccParseError::InvalidExchangeCode);
    }
    let mut result = BTreeSet::new();
    let mut previous = None;
    for byte in value.bytes() {
        if previous.is_some_and(|prior| prior >= byte) {
            return Err(OccParseError::DuplicateExchangeCode);
        }
        if !result.insert(OccExchangeCode::try_from_byte(byte)?) {
            return Err(OccParseError::DuplicateExchangeCode);
        }
        previous = Some(byte);
    }
    if result.is_empty() {
        return Err(OccParseError::InvalidExchangeCode);
    }
    Ok((
        result.into_iter().collect(),
        OccExchangeListingEvidence::Reported,
    ))
}

fn right_trim_fixed_field(value: &str) -> Result<&str, OccParseError> {
    let trimmed = value.trim_end_matches(' ');
    if trimmed.is_empty()
        || value[..trimmed.len()].contains('\t')
        || value[trimmed.len()..].bytes().any(|byte| byte != b' ')
    {
        Err(OccParseError::MalformedText)
    } else {
        Ok(trimmed)
    }
}

/// OCC Information Memo category from the selected search/export surface.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
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

#[derive(Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct OccMemoNativeSemanticsV1<'a> {
    memo_number: u64,
    posted_date: CalendarDate,
    effective_date: Option<CalendarDate>,
    title: &'a str,
    categories: &'a [OccMemoCategory],
    memo_locator: &'a SourceIdentifier,
    interpretation: OccMemoInterpretation,
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
            || title.chars().any(char::is_control)
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

    pub(crate) fn native_semantics(&self) -> OccMemoNativeSemanticsV1<'_> {
        OccMemoNativeSemanticsV1 {
            memo_number: self.memo_number,
            posted_date: self.posted_date,
            effective_date: self.effective_date,
            title: &self.title,
            categories: &self.categories,
            memo_locator: &self.memo_locator,
            interpretation: self.interpretation,
        }
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
    strict_row_set_digest: EvidenceDigest,
    alias_assertion_set: ReferenceAliasAssertionSetEvidence,
    terminal_state: PageTerminalState,
}

impl OccMemoParseReceipt {
    /// Returns valid memo discovery count.
    pub const fn returned_records(&self) -> u32 {
        self.returned_records
    }

    /// Returns the exact ordered identity of every accepted provider-native discovery row.
    pub const fn strict_row_set_digest(&self) -> EvidenceDigest {
        self.strict_row_set_digest
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
            self.strict_row_set_digest,
            self.alias_assertion_set,
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
    pub fn parse_csv<R, F>(
        schema: OccMemoCsvSchema,
        context: ReferenceObjectContext,
        source: R,
        mut sink: F,
    ) -> Result<OccMemoParseReceipt, OccParseError>
    where
        R: Read,
        F: FnMut(OccMemoDiscovery) -> Result<(), OccParseError>,
    {
        validate_memo_context(
            &context,
            &ReferenceSurface::OccMemoIndexCsv,
            "text/csv",
            schema.native_schema(),
        )?;
        if usize::try_from(context.payload_bytes()).map_or(true, |bytes| bytes > OCC_MEMO_MAX_BYTES)
        {
            return Err(OccParseError::BodyTooLarge);
        }
        let payload = ExactPayloadReader::try_new(source, &context, OCC_MEMO_MAX_BYTES)
            .map_err(|_| OccParseError::PayloadMismatch)?;
        let mut framed = BoundedTokenReader::csv(payload, OCC_PARSER_MAX_TOKEN_BYTES)
            .map_err(|_| OccParseError::BodyTooLarge)?;
        let mut reader = ReaderBuilder::new()
            .has_headers(true)
            .flexible(false)
            .trim(csv::Trim::None)
            .from_reader(&mut framed);
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
        let mut strict_rows = StrictReferenceRowSetDigestBuilder::new();
        for result in reader.records() {
            returned = returned
                .checked_add(1)
                .ok_or(OccParseError::RecordLimitExceeded)?;
            if returned > OCC_MEMO_MAX_RECORDS {
                return Err(OccParseError::RecordLimitExceeded);
            }
            let row = result.map_err(|_| OccParseError::MalformedCsv)?;
            let record = parse_memo_csv_row(returned.saturating_add(1), &row, context.clone())?;
            strict_rows
                .try_observe(&record.native_semantics())
                .map_err(|_| OccParseError::InvalidEvidence)?;
            sink(record)?;
        }
        drop(reader);
        framed
            .into_inner()
            .finish()
            .map_err(|_| OccParseError::PayloadMismatch)?;
        Ok(OccMemoParseReceipt {
            context,
            page_ordinal: NonZeroU32::MIN,
            returned_records: returned,
            strict_row_set_digest: strict_rows
                .finish(returned)
                .map_err(|_| OccParseError::InvalidEvidence)?,
            alias_assertion_set: ReferenceAliasAssertionSetEvidence::empty(),
            terminal_state: PageTerminalState::Terminal,
        })
    }

    /// Decodes the closed JSON memo-index page contract with explicit total-page evidence.
    ///
    /// # Errors
    ///
    /// Rejects unknown fields, context/payload drift, invalid pagination, malformed records,
    /// bounds, or sink failure. The JSON surface is not a contract-event economics parser.
    pub fn parse_json<R, F>(
        context: ReferenceObjectContext,
        source: R,
        mut sink: F,
    ) -> Result<OccMemoParseReceipt, OccParseError>
    where
        R: Read,
        F: FnMut(OccMemoDiscovery) -> Result<(), OccParseError>,
    {
        validate_memo_context(
            &context,
            &ReferenceSurface::OccMemoIndexJson,
            "application/json",
            "occ-memo-index-json-page-v1",
        )?;
        if usize::try_from(context.payload_bytes()).map_or(true, |bytes| bytes > OCC_MEMO_MAX_BYTES)
        {
            return Err(OccParseError::BodyTooLarge);
        }
        let payload = ExactPayloadReader::try_new(source, &context, OCC_MEMO_MAX_BYTES)
            .map_err(|_| OccParseError::PayloadMismatch)?;
        let mut framed = BoundedTokenReader::json(payload, OCC_PARSER_MAX_TOKEN_BYTES)
            .map_err(|_| OccParseError::BodyTooLarge)?;
        let mut strict_rows = StrictReferenceRowSetDigestBuilder::new();
        let mut digesting_sink = |record: OccMemoDiscovery| {
            strict_rows
                .try_observe(&record.native_semantics())
                .map_err(|_| OccParseError::InvalidEvidence)?;
            sink(record)
        };
        let sink_error = Cell::new(None);
        let mut deserializer = serde_json::Deserializer::from_reader(&mut framed);
        let summary = MemoPageSeed {
            context: &context,
            sink: &mut digesting_sink,
            sink_error: &sink_error,
        }
        .deserialize(&mut deserializer)
        .map_err(|_| sink_error.take().unwrap_or(OccParseError::MalformedJson))?;
        deserializer
            .end()
            .map_err(|_| OccParseError::MalformedJson)?;
        drop(deserializer);
        drop(digesting_sink);
        framed
            .into_inner()
            .finish()
            .map_err(|_| OccParseError::PayloadMismatch)?;
        let page = NonZeroU32::new(summary.page).ok_or(OccParseError::InvalidPagination)?;
        let total_pages =
            NonZeroU32::new(summary.total_pages).ok_or(OccParseError::InvalidPagination)?;
        if page > total_pages
            || (page < total_pages && summary.next_cursor.is_none())
            || (page == total_pages && summary.next_cursor.is_some())
        {
            return Err(OccParseError::InvalidPagination);
        }
        let terminal_state = match summary.next_cursor {
            Some(cursor) => PageTerminalState::More {
                next_cursor: SourceIdentifier::try_from(cursor)
                    .map_err(|_| OccParseError::InvalidPagination)?,
            },
            None => PageTerminalState::Terminal,
        };
        Ok(OccMemoParseReceipt {
            context,
            page_ordinal: page,
            returned_records: summary.returned_records,
            strict_row_set_digest: strict_rows
                .finish(summary.returned_records)
                .map_err(|_| OccParseError::InvalidEvidence)?,
            alias_assertion_set: ReferenceAliasAssertionSetEvidence::empty(),
            terminal_state,
        })
    }
}

struct MemoPageSummary {
    page: u32,
    total_pages: u32,
    next_cursor: Option<String>,
    returned_records: u32,
}

struct MemoPageSeed<'a, F> {
    context: &'a ReferenceObjectContext,
    sink: &'a mut F,
    sink_error: &'a Cell<Option<OccParseError>>,
}

impl<'de, F> DeserializeSeed<'de> for MemoPageSeed<'_, F>
where
    F: FnMut(OccMemoDiscovery) -> Result<(), OccParseError>,
{
    type Value = MemoPageSummary;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        deserializer.deserialize_map(MemoPageVisitor {
            context: self.context,
            sink: self.sink,
            sink_error: self.sink_error,
        })
    }
}

struct MemoPageVisitor<'a, F> {
    context: &'a ReferenceObjectContext,
    sink: &'a mut F,
    sink_error: &'a Cell<Option<OccParseError>>,
}

impl<'de, F> Visitor<'de> for MemoPageVisitor<'_, F>
where
    F: FnMut(OccMemoDiscovery) -> Result<(), OccParseError>,
{
    type Value = MemoPageSummary;

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("the closed OCC memo page object")
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        let mut page = None;
        let mut total_pages = None;
        let mut next_cursor = None;
        let mut returned_records = None;
        while let Some(field) = map.next_key::<MemoPageField>()? {
            match field {
                MemoPageField::Page => {
                    if page.replace(map.next_value()?).is_some() {
                        return Err(de::Error::duplicate_field("page"));
                    }
                }
                MemoPageField::TotalPages => {
                    if total_pages.replace(map.next_value()?).is_some() {
                        return Err(de::Error::duplicate_field("total_pages"));
                    }
                }
                MemoPageField::NextCursor => {
                    if next_cursor.replace(map.next_value()?).is_some() {
                        return Err(de::Error::duplicate_field("next_cursor"));
                    }
                }
                MemoPageField::Results => {
                    if returned_records.is_some() {
                        return Err(de::Error::duplicate_field("results"));
                    }
                    returned_records = Some(map.next_value_seed(MemoResultsSeed {
                        context: self.context,
                        sink: self.sink,
                        sink_error: self.sink_error,
                    })?);
                }
            }
        }
        Ok(MemoPageSummary {
            page: page.ok_or_else(|| de::Error::missing_field("page"))?,
            total_pages: total_pages.ok_or_else(|| de::Error::missing_field("total_pages"))?,
            next_cursor: next_cursor.ok_or_else(|| de::Error::missing_field("next_cursor"))?,
            returned_records: returned_records
                .ok_or_else(|| de::Error::missing_field("results"))?,
        })
    }
}

#[derive(Deserialize)]
#[serde(field_identifier, rename_all = "snake_case")]
enum MemoPageField {
    Page,
    TotalPages,
    NextCursor,
    Results,
}

struct MemoResultsSeed<'a, F> {
    context: &'a ReferenceObjectContext,
    sink: &'a mut F,
    sink_error: &'a Cell<Option<OccParseError>>,
}

impl<'de, F> DeserializeSeed<'de> for MemoResultsSeed<'_, F>
where
    F: FnMut(OccMemoDiscovery) -> Result<(), OccParseError>,
{
    type Value = u32;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        deserializer.deserialize_seq(MemoResultsVisitor {
            context: self.context,
            sink: self.sink,
            sink_error: self.sink_error,
        })
    }
}

struct MemoResultsVisitor<'a, F> {
    context: &'a ReferenceObjectContext,
    sink: &'a mut F,
    sink_error: &'a Cell<Option<OccParseError>>,
}

impl<'de, F> Visitor<'de> for MemoResultsVisitor<'_, F>
where
    F: FnMut(OccMemoDiscovery) -> Result<(), OccParseError>,
{
    type Value = u32;

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("a bounded OCC memo result sequence")
    }

    fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let mut returned = 0_u32;
        while let Some(wire) = sequence.next_element::<MemoWire>()? {
            returned = returned.checked_add(1).ok_or_else(|| {
                self.sink_error
                    .set(Some(OccParseError::RecordLimitExceeded));
                de::Error::custom("OCC memo record count overflowed")
            })?;
            if returned > OCC_MEMO_MAX_RECORDS {
                self.sink_error
                    .set(Some(OccParseError::RecordLimitExceeded));
                return Err(de::Error::custom("OCC memo record limit exceeded"));
            }
            let discovery =
                parse_memo_wire(returned, wire, self.context.clone()).map_err(|error| {
                    self.sink_error.set(Some(error));
                    de::Error::custom("invalid OCC memo discovery")
                })?;
            (self.sink)(discovery).map_err(|error| {
                self.sink_error.set(Some(error));
                de::Error::custom("OCC memo sink rejected a record")
            })?;
        }
        Ok(returned)
    }
}

struct MemoWire {
    number: u64,
    post_date: String,
    effective_date: Option<String>,
    title: String,
    categories: Vec<String>,
    memo_url: String,
}

impl<'de> Deserialize<'de> for MemoWire {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        deserializer.deserialize_map(MemoWireVisitor)
    }
}

struct MemoWireVisitor;

impl<'de> Visitor<'de> for MemoWireVisitor {
    type Value = MemoWire;

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("the closed bounded OCC memo record")
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        let mut number = None;
        let mut post_date = None;
        let mut effective_date = None;
        let mut title = None;
        let mut categories = None;
        let mut memo_url = None;
        while let Some(field) = map.next_key::<MemoWireField>()? {
            match field {
                MemoWireField::Number => {
                    if number.replace(map.next_value()?).is_some() {
                        return Err(de::Error::duplicate_field("number"));
                    }
                }
                MemoWireField::PostDate => {
                    if post_date.replace(map.next_value()?).is_some() {
                        return Err(de::Error::duplicate_field("post_date"));
                    }
                }
                MemoWireField::EffectiveDate => {
                    if effective_date.replace(map.next_value()?).is_some() {
                        return Err(de::Error::duplicate_field("effective_date"));
                    }
                }
                MemoWireField::Title => {
                    if title.replace(map.next_value()?).is_some() {
                        return Err(de::Error::duplicate_field("title"));
                    }
                }
                MemoWireField::Categories => {
                    if categories.is_some() {
                        return Err(de::Error::duplicate_field("categories"));
                    }
                    categories = Some(map.next_value::<BoundedMemoCategories>()?.0);
                }
                MemoWireField::MemoUrl => {
                    if memo_url.replace(map.next_value()?).is_some() {
                        return Err(de::Error::duplicate_field("memo_url"));
                    }
                }
            }
        }
        Ok(MemoWire {
            number: number.ok_or_else(|| de::Error::missing_field("number"))?,
            post_date: post_date.ok_or_else(|| de::Error::missing_field("post_date"))?,
            effective_date: effective_date
                .ok_or_else(|| de::Error::missing_field("effective_date"))?,
            title: title.ok_or_else(|| de::Error::missing_field("title"))?,
            categories: categories.ok_or_else(|| de::Error::missing_field("categories"))?,
            memo_url: memo_url.ok_or_else(|| de::Error::missing_field("memo_url"))?,
        })
    }
}

#[derive(Deserialize)]
#[serde(field_identifier, rename_all = "snake_case")]
enum MemoWireField {
    Number,
    PostDate,
    EffectiveDate,
    Title,
    Categories,
    MemoUrl,
}

struct BoundedMemoCategories(Vec<String>);

impl<'de> Deserialize<'de> for BoundedMemoCategories {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        deserializer.deserialize_seq(BoundedMemoCategoriesVisitor)
    }
}

struct BoundedMemoCategoriesVisitor;

impl<'de> Visitor<'de> for BoundedMemoCategoriesVisitor {
    type Value = BoundedMemoCategories;

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("a bounded OCC memo category sequence")
    }

    fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let mut categories = Vec::with_capacity(MAX_MEMO_CATEGORIES);
        while let Some(category) = sequence.next_element::<String>()? {
            if categories.len() == MAX_MEMO_CATEGORIES {
                return Err(de::Error::custom("OCC memo category limit exceeded"));
            }
            categories.push(category);
        }
        Ok(BoundedMemoCategories(categories))
    }
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
    let memo_locator = parse_occ_memo_locator(&wire.memo_url, wire.number)?;
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

fn parse_occ_memo_locator(
    value: &str,
    memo_number: u64,
) -> Result<SourceIdentifier, OccParseError> {
    let url = reqwest::Url::parse(value).map_err(|_| OccParseError::InvalidMemo)?;
    let expected_query = format!("number={memo_number}");
    if url.scheme() != "https"
        || url.username() != ""
        || url.password().is_some()
        || url.host_str() != Some("infomemo.theocc.com")
        || url.port().is_some()
        || url.path() != "/infomemos"
        || url.query() != Some(expected_query.as_str())
        || url.fragment().is_some()
        || url.as_str() != value
    {
        return Err(OccParseError::InvalidMemo);
    }
    SourceIdentifier::try_from(value).map_err(|_| OccParseError::InvalidMemo)
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
    /// XML tokenization or declared source encoding was malformed.
    #[error("malformed OCC DLP XML")]
    MalformedXml,
    /// XML root, record, field ordering, or declaration differed from the frozen contract.
    #[error("unrecognized OCC DLP XML schema")]
    UnknownXmlSchema,
    /// The source object did not contain its required terminal framing/root closure.
    #[error("incomplete OCC DLP publication")]
    IncompletePublication,
    /// One provider field exceeded its exact local bound.
    #[error("OCC DLP provider field exceeds its bound")]
    FieldTooLarge,
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
