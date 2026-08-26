use std::fmt;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

/// Exact SHA-256 content identity.
#[derive(Clone, Copy, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct Sha256Digest([u8; 32]);

impl Sha256Digest {
    /// Computes the digest of exact bytes.
    #[must_use]
    pub fn of(bytes: &[u8]) -> Self {
        Self(Sha256::digest(bytes).into())
    }

    /// Constructs an identity from already validated digest bytes.
    #[must_use]
    pub const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// Returns the fixed-width digest bytes.
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    /// Returns lowercase hexadecimal without weakening the binary identity.
    #[must_use]
    pub fn to_hex(self) -> String {
        const HEX: &[u8; 16] = b"0123456789abcdef";
        let mut value = String::with_capacity(64);
        for byte in self.0 {
            value.push(char::from(HEX[usize::from(byte >> 4)]));
            value.push(char::from(HEX[usize::from(byte & 0x0f)]));
        }
        value
    }
}

impl fmt::Debug for Sha256Digest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("Sha256Digest")
            .field(&self.to_hex())
            .finish()
    }
}

/// Gregorian trade date retained as the provider's `YYYYMMDD` coordinate.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct TradeDate {
    year: u16,
    month: u8,
    day: u8,
}

impl TradeDate {
    /// Parses the exact eight-digit catalog representation.
    ///
    /// # Errors
    ///
    /// Rejects non-canonical text and invalid Gregorian dates.
    pub fn parse(value: &str) -> Result<Self, DateError> {
        if value.len() != 8 || !value.bytes().all(|byte| byte.is_ascii_digit()) {
            return Err(DateError::InvalidFormat);
        }
        let year = parse_u16(&value[0..4])?;
        let month = parse_u8(&value[4..6])?;
        let day = parse_u8(&value[6..8])?;
        Self::new(year, month, day)
    }

    /// Constructs a validated Gregorian date.
    ///
    /// # Errors
    ///
    /// Rejects years outside the provider-era range and impossible month/day combinations.
    pub fn new(year: u16, month: u8, day: u8) -> Result<Self, DateError> {
        if !(2000..=2200).contains(&year) || !(1..=12).contains(&month) {
            return Err(DateError::InvalidDate);
        }
        let max_day = days_in_month(year, month);
        if day == 0 || day > max_day {
            return Err(DateError::InvalidDate);
        }
        Ok(Self { year, month, day })
    }

    /// Returns the calendar year.
    #[must_use]
    pub const fn year(self) -> u16 {
        self.year
    }

    /// Returns the calendar month.
    #[must_use]
    pub const fn month(self) -> u8 {
        self.month
    }

    /// Returns the day of month.
    #[must_use]
    pub const fn day(self) -> u8 {
        self.day
    }

    /// Returns the canonical provider representation.
    #[must_use]
    pub fn compact(self) -> String {
        format!("{:04}{:02}{:02}", self.year, self.month, self.day)
    }

    pub(crate) fn next_day(self) -> Result<Self, DateError> {
        let max_day = days_in_month(self.year, self.month);
        if self.day < max_day {
            return Self::new(self.year, self.month, self.day + 1);
        }
        if self.month < 12 {
            return Self::new(self.year, self.month + 1, 1);
        }
        Self::new(
            self.year.checked_add(1).ok_or(DateError::InvalidDate)?,
            1,
            1,
        )
    }

    pub(crate) fn rolling_year_start(self) -> Result<Self, DateError> {
        let prior_year = self.year.checked_sub(1).ok_or(DateError::InvalidDate)?;
        let day = self.day.min(days_in_month(prior_year, self.month));
        Self::new(prior_year, self.month, day)
    }

    pub(crate) fn start_epoch_nanos(self) -> Result<i64, DateError> {
        let days = days_from_civil(
            i64::from(self.year),
            i64::from(self.month),
            i64::from(self.day),
        );
        days.checked_mul(86_400)
            .and_then(|seconds| seconds.checked_mul(1_000_000_000))
            .ok_or(DateError::InvalidDate)
    }
}

fn parse_u8(value: &str) -> Result<u8, DateError> {
    value.parse().map_err(|_| DateError::InvalidFormat)
}

fn parse_u16(value: &str) -> Result<u16, DateError> {
    value.parse().map_err(|_| DateError::InvalidFormat)
}

const fn is_leap_year(year: u16) -> bool {
    year.is_multiple_of(4) && (!year.is_multiple_of(100) || year.is_multiple_of(400))
}

const fn days_in_month(year: u16, month: u8) -> u8 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if is_leap_year(year) => 29,
        2 => 28,
        _ => 0,
    }
}

// Howard Hinnant's civil-date transform, shifted to the Unix epoch.
const fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let adjusted_year = if month <= 2 { year - 1 } else { year };
    let era = if adjusted_year >= 0 {
        adjusted_year
    } else {
        adjusted_year - 399
    } / 400;
    let year_of_era = adjusted_year - era * 400;
    let adjusted_month = month + if month > 2 { -3 } else { 9 };
    let day_of_year = (153 * adjusted_month + 2) / 5 + day - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    era * 146_097 + day_of_era - 719_468
}

/// Date validation failure.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum DateError {
    /// The date was not exactly eight decimal digits.
    #[error("date is not canonical YYYYMMDD")]
    InvalidFormat,
    /// The fields do not form an admitted Gregorian date.
    #[error("date is outside the admitted Gregorian range")]
    InvalidDate,
}

/// Selected IEX feed family supported by this decoder core.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum FeedKind {
    /// IEX top-of-book and last-sale feed.
    Tops,
    /// IEX displayed price-level depth feed.
    Deep,
    /// Exact catalog `DPLS` family carrying IEX DEEP+ order-by-order messages.
    #[serde(rename = "DPLS")]
    DeepPlusDpls,
    /// Exact catalog `DPLC` family carrying IEX DEEP+ order-by-order messages.
    #[serde(rename = "DPLC")]
    DeepPlusDplc,
}

impl FeedKind {
    pub(crate) const fn catalog_name(self) -> &'static str {
        match self {
            Self::Tops => "TOPS",
            Self::Deep => "DEEP",
            Self::DeepPlusDpls => "DPLS",
            Self::DeepPlusDplc => "DPLC",
        }
    }

    pub(crate) const fn protocol_id(self) -> u16 {
        match self {
            Self::Tops => 0x8003,
            Self::Deep => 0x8004,
            Self::DeepPlusDpls | Self::DeepPlusDplc => 0x8005,
        }
    }
}

/// Exact historical feed-version family from an admitted catalog descriptor.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub enum FeedVersion {
    /// Catalog `TOPS` version `1.6`, decoded from the stable prefix defined by TOPS 1.6x.
    #[serde(rename = "1.6")]
    Tops1_6,
    /// Catalog `DEEP` version `1.0`, decoded from the stable prefix defined by DEEP 1.0x.
    #[serde(rename = "1.0")]
    Deep1_0,
    /// Catalog `DPLS` version `1.0`, decoded against the selected DEEP+ 1.0x schema.
    #[serde(rename = "1.0")]
    DeepPlusDpls1_0,
    /// Catalog `DPLC` version `1`, decoded against the selected DEEP+ 1.0x schema.
    #[serde(rename = "1")]
    DeepPlusDplc1,
}

impl FeedVersion {
    pub(crate) const fn catalog_value(self) -> &'static str {
        match self {
            Self::Tops1_6 => "1.6",
            Self::Deep1_0 => "1.0",
            Self::DeepPlusDpls1_0 => "1.0",
            Self::DeepPlusDplc1 => "1",
        }
    }

    /// Returns the exact first-party native specification selected by this decoder schema.
    #[must_use]
    pub const fn specification_value(self) -> &'static str {
        match self {
            Self::Tops1_6 => "TOPS-1.66",
            Self::Deep1_0 => "DEEP-1.08",
            Self::DeepPlusDpls1_0 | Self::DeepPlusDplc1 => "DEEP+-1.04",
        }
    }

    /// Returns the matching feed.
    #[must_use]
    pub const fn feed(self) -> FeedKind {
        match self {
            Self::Tops1_6 => FeedKind::Tops,
            Self::Deep1_0 => FeedKind::Deep,
            Self::DeepPlusDpls1_0 => FeedKind::DeepPlusDpls,
            Self::DeepPlusDplc1 => FeedKind::DeepPlusDplc,
        }
    }
}

/// Exact IEX transport family admitted by the current HIST catalog contract.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub enum TransportVersion {
    /// Catalog `IEXTP1`, whose outbound header version byte is `1`.
    #[serde(rename = "IEXTP1")]
    IexTp1,
}

impl TransportVersion {
    pub(crate) const fn catalog_value(self) -> &'static str {
        "IEXTP1"
    }

    /// Returns the exact first-party transport specification selected by this decoder.
    #[must_use]
    pub const fn specification_value(self) -> &'static str {
        "IEX-TP-1.26"
    }
}

/// Exact provider object representation selected from the mutable HIST catalog.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub enum PcapObjectEncoding {
    /// A gzip member whose validated output is one classic PCAP stream.
    Gzip,
    /// An identity-encoded classic PCAP object; no gzip framing or trailer exists.
    Identity,
}

impl PcapObjectEncoding {
    pub(crate) const fn identity_value(self) -> &'static str {
        match self {
            Self::Gzip => "gzip-pcap",
            Self::Identity => "identity-pcap",
        }
    }
}

/// Exact non-negative nanoseconds since the Unix epoch.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct EpochNanos(i64);

impl EpochNanos {
    pub(crate) fn try_new(value: i64) -> Result<Self, ModelError> {
        if value < 0 {
            Err(ModelError::InvalidTimestamp)
        } else {
            Ok(Self(value))
        }
    }

    /// Returns exact integer nanoseconds; no floating-point conversion is performed.
    #[must_use]
    pub const fn value(self) -> i64 {
        self.0
    }
}

/// Exact signed fixed-point IEX price with four implied decimal places.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct PriceUnits1e4(i64);

impl PriceUnits1e4 {
    pub(crate) fn try_new(value: i64) -> Result<Self, ModelError> {
        if value < 0 {
            Err(ModelError::InvalidPrice)
        } else {
            Ok(Self(value))
        }
    }

    /// Returns the exact integer whose scale is `10^-4` dollars.
    #[must_use]
    pub const fn value(self) -> i64 {
        self.0
    }
}

/// Venue and quality ceiling that accompanies every decoded event.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub struct IexVenueSemantics;

impl IexVenueSemantics {
    /// Exchange MIC retained by the adapter.
    pub const VENUE_MIC: &'static str = "IEXG";
    /// Provider label retained by the adapter.
    pub const PROVIDER: &'static str = "IEX";
    /// IEX HIST is venue-specific and never consolidated.
    pub const CONSOLIDATED: bool = false;
    /// TOPS is IEX top-of-book, never national best bid and offer.
    pub const NBBO: bool = false;
    /// DEEP and DEEP+ are displayed IEX depth, never complete market-wide depth.
    pub const MARKET_WIDE_DEPTH: bool = false;
    /// Hidden and reserve liquidity are not represented.
    pub const HIDDEN_LIQUIDITY: bool = false;
    /// This source is T+1 historical, not live.
    pub const LIVE: bool = false;
}

/// System-session event code from TOPS, DEEP, or DEEP+.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub enum SystemEventCode {
    /// Start of feed messages.
    StartMessages,
    /// Start of system hours.
    StartSystemHours,
    /// Start of regular market hours.
    StartRegularMarket,
    /// End of regular market hours.
    EndRegularMarket,
    /// End of system hours.
    EndSystemHours,
    /// End of feed messages.
    EndMessages,
}

/// IEX trading-status code.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub enum TradingStatus {
    /// Halted across U.S. equity markets.
    Halted,
    /// IEX order-acceptance period.
    OrderAcceptance,
    /// Paused and in an IEX order-acceptance period.
    Paused,
    /// Trading on IEX.
    Trading,
}

/// Displayed IEX price-level side.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub enum PriceLevelSide {
    /// Displayed bid level.
    Buy,
    /// Displayed offer level.
    Sell,
}

/// IEX official price type.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub enum PriceType {
    /// IEX official opening price.
    Opening,
    /// IEX official closing price.
    Closing,
}

/// Limit-Up/Limit-Down tier published in a security-directory message.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub enum LuldTier {
    /// The LULD tier is not applicable.
    NotApplicable,
    /// Tier 1 NMS stock.
    Tier1,
    /// Tier 2 NMS stock.
    Tier2,
}

/// IEX retail-liquidity interest for one security.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub enum RetailLiquidityIndicator {
    /// Retail indicator is not applicable.
    NotApplicable,
    /// Buy interest for retail.
    Buy,
    /// Sell interest for retail.
    Sell,
    /// Buy and sell interest for retail.
    BuyAndSell,
}

/// IEX-specific operational halt state.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub enum OperationalHaltStatus {
    /// Trading is operationally halted on IEX.
    Halted,
    /// Trading is not operationally halted on IEX.
    NotHalted,
}

/// Provider detail accompanying a Regulation SHO price-test state.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub enum ShortSalePriceTestDetail {
    /// No price test is in place.
    NoPriceTest,
    /// The test was activated by an intraday price drop.
    Activated,
    /// The test continues from the prior day.
    Continued,
    /// The test was deactivated.
    Deactivated,
    /// Provider detail is unavailable for this security.
    NotAvailable,
}

/// Security opening/closing process event.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub enum SecurityEventCode {
    /// Opening process completed.
    OpeningProcessComplete,
    /// Closing process completed.
    ClosingProcessComplete,
}

/// IEX auction type.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub enum AuctionType {
    /// Opening auction.
    Opening,
    /// Closing auction.
    Closing,
    /// Initial-public-offering auction.
    Ipo,
    /// Halt auction.
    Halt,
    /// Volatility auction.
    Volatility,
}

/// Side of the unpaired shares in an auction message.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub enum AuctionImbalanceSide {
    /// Buy-side imbalance.
    Buy,
    /// Sell-side imbalance.
    Sell,
    /// No imbalance.
    None,
}

/// Displayed DEEP+ order side.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub enum OrderSide {
    /// Displayed buy order.
    Buy,
    /// Displayed sell order.
    Sell,
}

/// Selected typed event families plus digest-only evidence for intentionally unmapped messages.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum IexEvent {
    /// Feed-wide system event.
    System {
        /// Event code.
        event: SystemEventCode,
        /// Exact source timestamp.
        source_time: EpochNanos,
    },
    /// Provider security-directory state.
    SecurityDirectory {
        /// Provider flags retained intact.
        flags: u8,
        /// Exact trimmed Nasdaq Integrated symbol.
        symbol: String,
        /// Shares representing a round lot.
        round_lot_size: u32,
        /// Corporate-action-adjusted previous official close.
        adjusted_poc_price: PriceUnits1e4,
        /// LULD tier.
        luld_tier: LuldTier,
        /// Exact source timestamp.
        source_time: EpochNanos,
    },
    /// Security-specific opening/closing process event from DEEP or DEEP+.
    SecurityEvent {
        /// Exact trimmed Nasdaq Integrated symbol.
        symbol: String,
        /// Interpreted opening/closing event.
        event: SecurityEventCode,
        /// Exact source timestamp.
        source_time: EpochNanos,
    },
    /// Security trading-status change.
    TradingStatus {
        /// Exact trimmed Nasdaq Integrated symbol.
        symbol: String,
        /// Interpreted status.
        status: TradingStatus,
        /// Trimmed exact reason code.
        reason: String,
        /// Exact source timestamp.
        source_time: EpochNanos,
    },
    /// IEX retail-liquidity interest state.
    RetailLiquidity {
        /// Exact trimmed Nasdaq Integrated symbol.
        symbol: String,
        /// Interpreted interest state.
        indicator: RetailLiquidityIndicator,
        /// Exact source timestamp.
        source_time: EpochNanos,
    },
    /// IEX-specific operational halt state.
    OperationalHalt {
        /// Exact trimmed Nasdaq Integrated symbol.
        symbol: String,
        /// Interpreted halt state.
        status: OperationalHaltStatus,
        /// Exact source timestamp.
        source_time: EpochNanos,
    },
    /// Regulation SHO short-sale price-test state.
    ShortSalePriceTest {
        /// Exact trimmed Nasdaq Integrated symbol.
        symbol: String,
        /// Whether the restriction is in effect.
        in_effect: bool,
        /// Provider detail.
        detail: ShortSalePriceTestDetail,
        /// Exact source timestamp.
        source_time: EpochNanos,
    },
    /// IEX TOPS quote, never NBBO.
    Quote {
        /// Exact trimmed Nasdaq Integrated symbol.
        symbol: String,
        /// Provider flag byte retained intact.
        flags: u8,
        /// Aggregate IEX best-bid shares.
        bid_size: u32,
        /// Exact IEX best-bid price.
        bid_price: PriceUnits1e4,
        /// Exact IEX best-offer price.
        ask_price: PriceUnits1e4,
        /// Aggregate IEX best-offer shares.
        ask_size: u32,
        /// Exact source timestamp.
        source_time: EpochNanos,
    },
    /// Displayed IEX DEEP price-level update.
    PriceLevel {
        /// Exact trimmed Nasdaq Integrated symbol.
        symbol: String,
        /// Displayed side.
        side: PriceLevelSide,
        /// True only when the provider's event-complete flag is set.
        event_complete: bool,
        /// Aggregate displayed shares; zero removes the level.
        size: u32,
        /// Exact level price.
        price: PriceUnits1e4,
        /// Exact source timestamp.
        source_time: EpochNanos,
    },
    /// Individual IEX fill.
    Trade {
        /// Exact trimmed Nasdaq Integrated symbol.
        symbol: String,
        /// Provider sale-condition flags retained intact.
        sale_condition_flags: u8,
        /// Shares executed.
        size: u32,
        /// Exact execution price.
        price: PriceUnits1e4,
        /// IEX trade identifier.
        trade_id: i64,
        /// Exact source timestamp.
        source_time: EpochNanos,
    },
    /// Same-day cancellation of a previously reported IEX trade.
    TradeBreak {
        /// Exact trimmed Nasdaq Integrated symbol.
        symbol: String,
        /// Provider sale-condition flags retained intact.
        sale_condition_flags: u8,
        /// Broken shares.
        size: u32,
        /// Exact broken-trade price.
        price: PriceUnits1e4,
        /// Referenced IEX trade identifier.
        trade_id: i64,
        /// Exact source timestamp.
        source_time: EpochNanos,
    },
    /// IEX official opening or closing price.
    OfficialPrice {
        /// Exact trimmed Nasdaq Integrated symbol.
        symbol: String,
        /// Price role.
        price_type: PriceType,
        /// Exact official price.
        price: PriceUnits1e4,
        /// Exact source timestamp.
        source_time: EpochNanos,
    },
    /// IEX auction information for one IEX-listed security.
    Auction {
        /// Auction type.
        auction_type: AuctionType,
        /// Exact trimmed Nasdaq Integrated symbol.
        symbol: String,
        /// Shares paired at the reference price.
        paired_shares: u32,
        /// Reference price.
        reference_price: PriceUnits1e4,
        /// Indicative clearing price.
        indicative_clearing_price: PriceUnits1e4,
        /// Unpaired shares at the reference price.
        imbalance_shares: u32,
        /// Side of the imbalance.
        imbalance_side: AuctionImbalanceSide,
        /// Number of automatic extensions.
        extension_number: u8,
        /// Projected auction match time in whole seconds since the Unix epoch.
        scheduled_auction_time_unix_seconds: u32,
        /// Clearing price using auction-book orders.
        auction_book_clearing_price: PriceUnits1e4,
        /// Auction-collar reference price, or zero when unused.
        collar_reference_price: PriceUnits1e4,
        /// Lower auction collar, or zero when unused.
        lower_auction_collar: PriceUnits1e4,
        /// Upper auction collar, or zero when unused.
        upper_auction_collar: PriceUnits1e4,
        /// Exact source timestamp.
        source_time: EpochNanos,
    },
    /// Displayed order added to the IEX DEEP+ book.
    AddOrder {
        /// Exact trimmed Nasdaq Integrated symbol.
        symbol: String,
        /// Displayed side.
        side: OrderSide,
        /// IEX order identifier.
        order_id: i64,
        /// Quoted shares.
        size: u32,
        /// Booking price.
        price: PriceUnits1e4,
        /// Exact source timestamp.
        source_time: EpochNanos,
    },
    /// Displayed IEX DEEP+ order modification.
    ModifyOrder {
        /// Exact trimmed Nasdaq Integrated symbol.
        symbol: String,
        /// Referenced IEX order identifier.
        order_id: i64,
        /// New total quoted shares.
        size: u32,
        /// New booking price.
        price: PriceUnits1e4,
        /// Whether the provider says priority is maintained.
        maintains_priority: bool,
        /// Exact source timestamp.
        source_time: EpochNanos,
    },
    /// Displayed IEX DEEP+ order removal.
    DeleteOrder {
        /// Exact trimmed Nasdaq Integrated symbol.
        symbol: String,
        /// Referenced IEX order identifier.
        order_id: i64,
        /// Exact source timestamp.
        source_time: EpochNanos,
    },
    /// Execution against a displayed IEX DEEP+ order.
    ExecuteOrder {
        /// Exact trimmed Nasdaq Integrated symbol.
        symbol: String,
        /// Provider sale-condition flags retained intact.
        sale_condition_flags: u8,
        /// Referenced IEX order identifier.
        order_id: i64,
        /// Shares executed.
        size: u32,
        /// Exact execution price.
        price: PriceUnits1e4,
        /// IEX trade identifier.
        trade_id: i64,
        /// Exact source timestamp.
        source_time: EpochNanos,
    },
    /// All displayed orders for one symbol were cleared from the DEEP+ book.
    ClearBook {
        /// Exact trimmed Nasdaq Integrated symbol.
        symbol: String,
        /// Exact source timestamp.
        source_time: EpochNanos,
    },
    /// A valid length-framed message outside the selected typed families.
    Unmapped {
        /// Provider message-type byte.
        message_type: u8,
        /// Exact message-data byte length.
        byte_len: u16,
        /// Digest of the complete message data.
        message_sha256: Sha256Digest,
    },
}

/// Fully provenance-bound decoded message.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct DecodedIexEvent {
    /// Fixed IEX-venue historical quality ceiling; never SIP, NBBO, live, or market-wide depth.
    pub semantics: IexVenueSemantics,
    /// Exact selected trade date.
    pub trade_date: TradeDate,
    /// Exact selected feed.
    pub feed: FeedKind,
    /// Exact selected feed-version family.
    pub feed_version: FeedVersion,
    /// Exact transport version.
    pub transport_version: TransportVersion,
    /// Digest binding the catalog descriptor and raw capture receipt.
    pub source_file_identity: Sha256Digest,
    /// Exact immutable decoder contract selected by the cold plan.
    pub decoder_contract_sha256: Sha256Digest,
    /// Exact authority-owned attempt that performed this decode.
    pub decode_attempt_evidence_sha256: Sha256Digest,
    /// IEX-TP channel; TOPS, DEEP, and DPLS use 1 while DPLC admits 1 through 16.
    pub channel_id: u32,
    /// IEX-TP session identifier.
    pub session_id: u32,
    /// Exact higher-layer sequence number.
    pub sequence: i64,
    /// Exact byte offset of the message block in the IEX-TP stream.
    pub stream_offset: i64,
    /// Transport send timestamp.
    pub send_time: EpochNanos,
    /// PCAP capture timestamp normalized exactly to nanoseconds.
    pub capture_time_unix_nanos: u64,
    /// Complete higher-layer message-data bytes, including any valid appended extension.
    pub message_data_bytes: u16,
    /// Digest of complete higher-layer message data.
    pub message_data_sha256: Sha256Digest,
    /// Prefix bytes interpreted by this exact decoder; any remainder stays digest-bound only.
    pub mapped_prefix_bytes: u16,
    /// Selected typed or digest-only message evidence.
    pub event: IexEvent,
}

#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub(crate) enum ModelError {
    #[error("timestamp is negative or outside its selected feed date")]
    InvalidTimestamp,
    #[error("price is negative")]
    InvalidPrice,
}
