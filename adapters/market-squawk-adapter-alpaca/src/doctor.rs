//! Sealed Paper/IEX readiness doctor for one imported Alpaca key generation.
//!
//! This transport cannot accept a caller-selected URL, symbol, feed, account realm, or market-data
//! product. It proves only the fixed read-only Paper/IEX routes needed by the installed product and
//! deliberately exposes no account, position, order, or trading operation.

use std::collections::BTreeSet;
use std::num::NonZeroU64;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use chrono::{DateTime, Datelike as _, Days, NaiveDate, Utc};
use futures_util::{SinkExt as _, StreamExt as _};
use market_squawk_domain::{CalendarDate, DigestAlgorithm, EvidenceDigest, Timestamp};
use market_squawk_sources::{
    BudgetDecision, BudgetPermit, HttpRequestBounds, RetryAfter, SharedProviderBudget,
};
use reqwest::header::{HeaderMap, RETRY_AFTER};
use rust_decimal::Decimal;
use serde::Deserialize;
use serde::de::{MapAccess, Visitor};
use serde_json::{Number, Value};
use sha2::{Digest as _, Sha256};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_tungstenite::tungstenite::client::IntoClientRequest as _;
use tokio_tungstenite::tungstenite::http::header::{HeaderName, HeaderValue};
use tokio_tungstenite::tungstenite::{Error as WebSocketError, Message, protocol::WebSocketConfig};
use tokio_tungstenite::{WebSocketStream, connect_async_with_config};
use tokio_util::sync::CancellationToken;
use url::Url;

use crate::historical_calendar::{
    authenticated_bounded_get, hardened_client, singleton_bounded_header,
};
use crate::{
    ALPACA_HISTORICAL_CALENDAR_MAX_RESPONSE_BYTES, AlpacaAuthenticatedCalendarRequest,
    AlpacaCredentials, AlpacaError, AlpacaTradingApiEnvironment, AlpacaTransportLimits,
};

/// Exact code-owned stock-snapshot sentinel cardinality.
///
/// A complete result proves only this one request. It is not the broad scheduler's sustained
/// effective-batch admission by itself.
pub const ALPACA_PAPER_IEX_DOCTOR_BATCH_SYMBOL_COUNT: usize = 50;

const DOCTOR_SYMBOL: &str = "AAPL";
const DOCTOR_FEED: &str = "iex";
const DOCTOR_TIMEFRAME: &str = "1Day";
const DOCTOR_ADJUSTMENT: &str = "raw";
const DOCTOR_SORT: &str = "asc";
const DOCTOR_MARKET: &str = "IEX";
const DOCTOR_TIMEZONE: &str = "UTC";
const QUOTE_ENDPOINT: &str = "https://data.alpaca.markets/v2/stocks/AAPL/quotes/latest?feed=iex";
const SNAPSHOT_ENDPOINT: &str = "https://data.alpaca.markets/v2/stocks/snapshots";
const HISTORICAL_ENDPOINT: &str = "https://data.alpaca.markets/v2/stocks/AAPL/bars";
const STREAM_ENDPOINT: &str = "wss://stream.data.alpaca.markets/v2/iex";
const USER_AGENT: &str = "market-squawk/0.1 alpaca-paper-iex-doctor";
const MAX_QUOTE_RESPONSE_BYTES: usize = 256 * 1024;
const MAX_BATCH_RESPONSE_BYTES: usize = 8 * 1024 * 1024;
const MAX_HISTORICAL_PAGE_BYTES: usize = 8 * 1024 * 1024;
const MAX_HISTORICAL_PAGES: usize = 8;
const MAX_HISTORICAL_TOTAL_BYTES: usize = 16 * 1024 * 1024;
const MAX_HISTORICAL_BARS: usize = 1_000;
const MAX_PAGE_TOKEN_BYTES: usize = 256;
const MAX_CONTROL_FRAMES: usize = 8;
const MAX_CLOSE_FRAMES: usize = 2;
const MAX_RETRY_AFTER_BYTES: usize = 128;
const DOCTOR_HISTORY_CALENDAR_DAYS: u64 = 45;
const HISTORICAL_PAGE_LIMIT: &str = "1000";
const KEY_ID_HEADER: HeaderName = HeaderName::from_static("apca-api-key-id");
const SECRET_KEY_HEADER: HeaderName = HeaderName::from_static("apca-api-secret-key");
const STREAM_SUBSCRIPTION: &str = r#"{"action":"subscribe","trades":["AAPL"],"quotes":["AAPL"]}"#;

const BATCH_SYMBOLS: [&str; ALPACA_PAPER_IEX_DOCTOR_BATCH_SYMBOL_COUNT] = [
    "AAPL", "ABBV", "ABT", "ACN", "ADBE", "AMD", "AMGN", "AMZN", "AVGO", "BAC", "BKNG", "BLK",
    "BMY", "CAT", "CMCSA", "COP", "COST", "CRM", "CSCO", "CVS", "CVX", "DIS", "GE", "GILD", "GOOG",
    "GOOGL", "GS", "HD", "HON", "IBM", "INTC", "ISRG", "JNJ", "JPM", "KO", "LIN", "LLY", "LMT",
    "LOW", "MA", "MCD", "META", "MRK", "MS", "MSFT", "NFLX", "NVDA", "ORCL", "PEP", "PG",
];

/// Observed readiness for one exact doctor surface.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AlpacaDoctorObservationDisposition {
    /// The exact route and semantic sentinel were observed.
    ObservedAvailable,
    /// The route responded, but the complete semantic sentinel was only partially satisfied.
    ObservedDegraded,
    /// The exact route returned an explicit unavailable response.
    ObservedUnavailable,
    /// This run intentionally did not reach the surface.
    Unprobed,
    /// The Paper/IEX doctor contract excludes the surface.
    Unsupported,
}

/// Authority origin of one complete doctor observation.
///
/// Provider observations and installed-fixture observations intentionally occupy distinct digest
/// domains. A fixture result can exercise composition and persistence, but can never be presented
/// as evidence that Alpaca authenticated or served the requests.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AlpacaDoctorObservationOrigin {
    /// The credential-bearing production transport observed Alpaca's responses.
    ProviderObserved,
    /// The closed nondefault installed fixture replayed code-owned raw protocol bytes.
    InstalledFixture,
}

/// An exact provider header was either observed once or absent.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AlpacaDoctorObservedField<T> {
    /// Exactly one valid field value was observed.
    Observed(T),
    /// The provider omitted the field.
    Missing,
}

impl<T> AlpacaDoctorObservedField<T> {
    /// Borrows an observed value, if present.
    pub const fn observed(&self) -> Option<&T> {
        match self {
            Self::Observed(value) => Some(value),
            Self::Missing => None,
        }
    }
}

/// Parsed bounded `Retry-After` evidence.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AlpacaDoctorRetryAfter {
    /// Provider-authored nonnegative relative seconds.
    DelaySeconds(u64),
    /// Provider-authored absolute HTTP-date converted to whole Unix seconds.
    AtUnixSeconds(i64),
}

/// Exact observed-or-missing capacity fields for one response.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AlpacaDoctorRateEvidence {
    limit: AlpacaDoctorObservedField<u32>,
    remaining: AlpacaDoctorObservedField<u32>,
    reset_unix_seconds: AlpacaDoctorObservedField<i64>,
    retry_after: AlpacaDoctorObservedField<AlpacaDoctorRetryAfter>,
}

impl AlpacaDoctorRateEvidence {
    /// Returns the exact `X-RateLimit-Limit` observation.
    pub const fn limit(&self) -> &AlpacaDoctorObservedField<u32> {
        &self.limit
    }

    /// Returns the exact `X-RateLimit-Remaining` observation.
    pub const fn remaining(&self) -> &AlpacaDoctorObservedField<u32> {
        &self.remaining
    }

    /// Returns the exact `X-RateLimit-Reset` observation.
    pub const fn reset_unix_seconds(&self) -> &AlpacaDoctorObservedField<i64> {
        &self.reset_unix_seconds
    }

    /// Returns parsed exact `Retry-After` evidence.
    pub const fn retry_after(&self) -> &AlpacaDoctorObservedField<AlpacaDoctorRetryAfter> {
        &self.retry_after
    }
}

/// Secret-free evidence from one bounded HTTP response.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AlpacaDoctorHttpEvidence {
    endpoint_contract_digest: EvidenceDigest,
    request_digest: EvidenceDigest,
    status_code: u16,
    body_digest: EvidenceDigest,
    response_bytes: u64,
    received_at: Timestamp,
    latency_nanos: u64,
    rate: AlpacaDoctorRateEvidence,
}

impl AlpacaDoctorHttpEvidence {
    /// Returns the code-owned endpoint/method/query-grammar contract digest.
    pub const fn endpoint_contract_digest(&self) -> EvidenceDigest {
        self.endpoint_contract_digest
    }

    /// Returns the exact credential-free request identity.
    pub const fn request_digest(&self) -> EvidenceDigest {
        self.request_digest
    }

    /// Returns the provider status code.
    pub const fn status_code(&self) -> u16 {
        self.status_code
    }

    /// Returns the exact raw-body digest.
    pub const fn body_digest(&self) -> EvidenceDigest {
        self.body_digest
    }

    /// Returns the bounded raw-body byte count.
    pub const fn response_bytes(&self) -> u64 {
        self.response_bytes
    }

    /// Returns the local complete-body receive time.
    pub const fn received_at(&self) -> Timestamp {
        self.received_at
    }

    /// Returns elapsed request nanoseconds.
    pub const fn latency_nanos(&self) -> u64 {
        self.latency_nanos
    }

    /// Returns typed observed-or-missing rate evidence.
    pub const fn rate(&self) -> &AlpacaDoctorRateEvidence {
        &self.rate
    }
}

/// One raw daily-history page and its pagination-token evidence.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AlpacaDoctorHttpPageEvidence {
    http: AlpacaDoctorHttpEvidence,
    request_page_token_digest: Option<EvidenceDigest>,
    response_page_token_digest: Option<EvidenceDigest>,
}

impl AlpacaDoctorHttpPageEvidence {
    /// Returns the page's HTTP evidence.
    pub const fn http(&self) -> &AlpacaDoctorHttpEvidence {
        &self.http
    }

    /// Returns the incoming continuation-token digest, absent on page zero.
    pub const fn request_page_token_digest(&self) -> Option<EvidenceDigest> {
        self.request_page_token_digest
    }

    /// Returns the provider continuation-token digest, absent on the terminal page.
    pub const fn response_page_token_digest(&self) -> Option<EvidenceDigest> {
        self.response_page_token_digest
    }
}

/// Exact single-symbol IEX latest-quote result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AlpacaDoctorQuoteObservation {
    disposition: AlpacaDoctorObservationDisposition,
    http: AlpacaDoctorHttpEvidence,
    semantic_result_digest: EvidenceDigest,
    quote_timestamp: Option<Timestamp>,
    bid_price: Option<Decimal>,
    ask_price: Option<Decimal>,
    bid_size: Option<u64>,
    ask_size: Option<u64>,
}

impl AlpacaDoctorQuoteObservation {
    pub const fn disposition(&self) -> AlpacaDoctorObservationDisposition {
        self.disposition
    }
    pub const fn http(&self) -> &AlpacaDoctorHttpEvidence {
        &self.http
    }
    /// Returns the digest of the exact parsed quote semantics.
    pub const fn semantic_result_digest(&self) -> EvidenceDigest {
        self.semantic_result_digest
    }
    pub const fn symbol(&self) -> &'static str {
        DOCTOR_SYMBOL
    }
    pub const fn feed(&self) -> &'static str {
        DOCTOR_FEED
    }
    pub const fn quote_timestamp(&self) -> Option<Timestamp> {
        self.quote_timestamp
    }
    pub const fn bid_price(&self) -> Option<Decimal> {
        self.bid_price
    }
    pub const fn ask_price(&self) -> Option<Decimal> {
        self.ask_price
    }
    pub const fn bid_size(&self) -> Option<u64> {
        self.bid_size
    }
    pub const fn ask_size(&self) -> Option<u64> {
        self.ask_size
    }
}

/// Exact 50-symbol IEX stock-snapshot result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AlpacaDoctorBatchObservation {
    disposition: AlpacaDoctorObservationDisposition,
    http: AlpacaDoctorHttpEvidence,
    semantic_result_digest: EvidenceDigest,
    returned_count: u32,
    missing_count: u32,
    unexpected_count: u32,
    duplicate_count: u32,
    invalid_count: u32,
    effective_cardinality: u32,
    requested_symbols_digest: EvidenceDigest,
    returned_symbols_digest: EvidenceDigest,
    missing_symbols_digest: EvidenceDigest,
    unexpected_symbols_digest: EvidenceDigest,
}

impl AlpacaDoctorBatchObservation {
    pub const fn disposition(&self) -> AlpacaDoctorObservationDisposition {
        self.disposition
    }
    pub const fn http(&self) -> &AlpacaDoctorHttpEvidence {
        &self.http
    }
    /// Returns the digest of all validated batch counts and symbol sets.
    pub const fn semantic_result_digest(&self) -> EvidenceDigest {
        self.semantic_result_digest
    }
    pub const fn requested_count(&self) -> u32 {
        ALPACA_PAPER_IEX_DOCTOR_BATCH_SYMBOL_COUNT as u32
    }
    pub const fn returned_count(&self) -> u32 {
        self.returned_count
    }
    pub const fn missing_count(&self) -> u32 {
        self.missing_count
    }
    pub const fn unexpected_count(&self) -> u32 {
        self.unexpected_count
    }
    pub const fn duplicate_count(&self) -> u32 {
        self.duplicate_count
    }
    pub const fn invalid_count(&self) -> u32 {
        self.invalid_count
    }
    pub const fn effective_cardinality(&self) -> u32 {
        self.effective_cardinality
    }
    pub const fn requested_symbols_digest(&self) -> EvidenceDigest {
        self.requested_symbols_digest
    }
    pub const fn returned_symbols_digest(&self) -> EvidenceDigest {
        self.returned_symbols_digest
    }
    pub const fn missing_symbols_digest(&self) -> EvidenceDigest {
        self.missing_symbols_digest
    }
    pub const fn unexpected_symbols_digest(&self) -> EvidenceDigest {
        self.unexpected_symbols_digest
    }
}

/// Exact bounded IEX WebSocket control-plane result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AlpacaDoctorStreamObservation {
    disposition: AlpacaDoctorObservationDisposition,
    endpoint_contract_digest: EvidenceDigest,
    request_digest: EvidenceDigest,
    handshake_status: u16,
    handshake_rate: AlpacaDoctorRateEvidence,
    connected_frame_digest: EvidenceDigest,
    authenticated_frame_digest: EvidenceDigest,
    subscription_frame_digest: EvidenceDigest,
    semantic_result_digest: EvidenceDigest,
    subscribed_trade_count: u32,
    subscribed_quote_count: u32,
    frames_observed: u32,
    bytes_observed: u64,
    authenticated_at: Timestamp,
    subscribed_at: Timestamp,
    close_sent: bool,
    clean_close_observed: bool,
    completed_at: Timestamp,
}

impl AlpacaDoctorStreamObservation {
    pub const fn disposition(&self) -> AlpacaDoctorObservationDisposition {
        self.disposition
    }
    /// Returns the exact WSS endpoint/auth/subscription contract digest.
    pub const fn endpoint_contract_digest(&self) -> EvidenceDigest {
        self.endpoint_contract_digest
    }
    pub const fn request_digest(&self) -> EvidenceDigest {
        self.request_digest
    }
    pub const fn handshake_status(&self) -> u16 {
        self.handshake_status
    }
    pub const fn handshake_rate(&self) -> &AlpacaDoctorRateEvidence {
        &self.handshake_rate
    }
    pub const fn connected_frame_digest(&self) -> EvidenceDigest {
        self.connected_frame_digest
    }
    pub const fn authenticated_frame_digest(&self) -> EvidenceDigest {
        self.authenticated_frame_digest
    }
    pub const fn subscription_frame_digest(&self) -> EvidenceDigest {
        self.subscription_frame_digest
    }
    /// Returns the digest of the complete validated stream control exchange.
    pub const fn semantic_result_digest(&self) -> EvidenceDigest {
        self.semantic_result_digest
    }
    pub const fn subscribed_trade_count(&self) -> u32 {
        self.subscribed_trade_count
    }
    pub const fn subscribed_quote_count(&self) -> u32 {
        self.subscribed_quote_count
    }
    pub const fn frames_observed(&self) -> u32 {
        self.frames_observed
    }
    pub const fn bytes_observed(&self) -> u64 {
        self.bytes_observed
    }
    pub const fn authenticated_at(&self) -> Timestamp {
        self.authenticated_at
    }
    pub const fn subscribed_at(&self) -> Timestamp {
        self.subscribed_at
    }
    pub const fn close_sent(&self) -> bool {
        self.close_sent
    }
    pub const fn clean_close_observed(&self) -> bool {
        self.clean_close_observed
    }
    pub const fn completed_at(&self) -> Timestamp {
        self.completed_at
    }
}

/// Exact terminal raw daily-history pagination result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AlpacaDoctorHistoricalObservation {
    disposition: AlpacaDoctorObservationDisposition,
    endpoint_contract_digest: EvidenceDigest,
    request_digest: EvidenceDigest,
    semantic_result_digest: EvidenceDigest,
    start_date: CalendarDate,
    end_date: CalendarDate,
    page_count: u32,
    returned_bar_count: u32,
    distinct_date_count: u32,
    first_bar_timestamp: Option<Timestamp>,
    last_bar_timestamp: Option<Timestamp>,
    returned_dates_digest: EvidenceDigest,
    pagination_graph_digest: EvidenceDigest,
    terminal_page_observed: bool,
    pages: Box<[AlpacaDoctorHttpPageEvidence]>,
    returned_dates: Box<[CalendarDate]>,
}

impl AlpacaDoctorHistoricalObservation {
    pub const fn disposition(&self) -> AlpacaDoctorObservationDisposition {
        self.disposition
    }
    /// Returns the exact AAPL/IEX/raw/daily/ascending endpoint contract digest.
    pub const fn endpoint_contract_digest(&self) -> EvidenceDigest {
        self.endpoint_contract_digest
    }
    /// Returns the exact bounded date-request digest, excluding continuation tokens.
    pub const fn request_digest(&self) -> EvidenceDigest {
        self.request_digest
    }
    /// Returns the digest of the complete parsed terminal pagination result.
    pub const fn semantic_result_digest(&self) -> EvidenceDigest {
        self.semantic_result_digest
    }
    pub const fn symbol(&self) -> &'static str {
        DOCTOR_SYMBOL
    }
    pub const fn feed(&self) -> &'static str {
        DOCTOR_FEED
    }
    pub const fn timeframe(&self) -> &'static str {
        DOCTOR_TIMEFRAME
    }
    pub const fn adjustment(&self) -> &'static str {
        DOCTOR_ADJUSTMENT
    }
    pub const fn sort(&self) -> &'static str {
        DOCTOR_SORT
    }
    pub const fn start_date(&self) -> CalendarDate {
        self.start_date
    }
    pub const fn end_date(&self) -> CalendarDate {
        self.end_date
    }
    pub const fn page_count(&self) -> u32 {
        self.page_count
    }
    pub const fn returned_bar_count(&self) -> u32 {
        self.returned_bar_count
    }
    pub const fn distinct_date_count(&self) -> u32 {
        self.distinct_date_count
    }
    pub const fn first_bar_timestamp(&self) -> Option<Timestamp> {
        self.first_bar_timestamp
    }
    pub const fn last_bar_timestamp(&self) -> Option<Timestamp> {
        self.last_bar_timestamp
    }
    pub const fn returned_dates_digest(&self) -> EvidenceDigest {
        self.returned_dates_digest
    }
    pub const fn pagination_graph_digest(&self) -> EvidenceDigest {
        self.pagination_graph_digest
    }
    pub const fn terminal_page_observed(&self) -> bool {
        self.terminal_page_observed
    }
    pub fn pages(&self) -> &[AlpacaDoctorHttpPageEvidence] {
        &self.pages
    }
}

/// Exact Paper `/v3/calendar/IEX` UTC reconciliation result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AlpacaDoctorCalendarObservation {
    disposition: AlpacaDoctorObservationDisposition,
    http: AlpacaDoctorHttpEvidence,
    semantic_result_digest: EvidenceDigest,
    start_date: CalendarDate,
    end_date: CalendarDate,
    session_count: u32,
    history_date_count: u32,
    matched_count: u32,
    missing_history_count: u32,
    unexpected_history_count: u32,
    session_dates_digest: EvidenceDigest,
    history_dates_digest: EvidenceDigest,
    exact_date_reconciliation: bool,
}

impl AlpacaDoctorCalendarObservation {
    pub const fn disposition(&self) -> AlpacaDoctorObservationDisposition {
        self.disposition
    }
    pub const fn http(&self) -> &AlpacaDoctorHttpEvidence {
        &self.http
    }
    /// Returns the digest of exact IEX/UTC date reconciliation semantics.
    pub const fn semantic_result_digest(&self) -> EvidenceDigest {
        self.semantic_result_digest
    }
    pub const fn market(&self) -> &'static str {
        DOCTOR_MARKET
    }
    pub const fn timezone(&self) -> &'static str {
        DOCTOR_TIMEZONE
    }
    pub const fn start_date(&self) -> CalendarDate {
        self.start_date
    }
    pub const fn end_date(&self) -> CalendarDate {
        self.end_date
    }
    pub const fn session_count(&self) -> u32 {
        self.session_count
    }
    pub const fn history_date_count(&self) -> u32 {
        self.history_date_count
    }
    pub const fn matched_count(&self) -> u32 {
        self.matched_count
    }
    pub const fn missing_history_count(&self) -> u32 {
        self.missing_history_count
    }
    pub const fn unexpected_history_count(&self) -> u32 {
        self.unexpected_history_count
    }
    pub const fn session_dates_digest(&self) -> EvidenceDigest {
        self.session_dates_digest
    }
    pub const fn history_dates_digest(&self) -> EvidenceDigest {
        self.history_dates_digest
    }
    pub const fn exact_date_reconciliation(&self) -> bool {
        self.exact_date_reconciliation
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct AlpacaPaperIexDoctorObservationData {
    origin: AlpacaDoctorObservationOrigin,
    market_data_principal_sha256: EvidenceDigest,
    quote: AlpacaDoctorQuoteObservation,
    batch: AlpacaDoctorBatchObservation,
    stream: AlpacaDoctorStreamObservation,
    historical: AlpacaDoctorHistoricalObservation,
    calendar: AlpacaDoctorCalendarObservation,
    completed_at: Timestamp,
    observation_digest: EvidenceDigest,
}

/// Provider-observed secret-free output from the credential-bearing Paper/IEX doctor.
///
/// Only [`AlpacaPaperIexDoctor::observe`] can construct this authority-bearing wrapper. The
/// installed fixture returns a distinct type with no conversion into this one.
#[derive(Debug, Eq, PartialEq)]
pub struct AlpacaPaperIexDoctorObservation {
    data: AlpacaPaperIexDoctorObservationData,
}

/// Installed-fixture output from the closed raw Paper/IEX protocol transcript.
///
/// This type cannot be converted into [`AlpacaPaperIexDoctorObservation`] and therefore cannot
/// satisfy an application seam that requires provider-observed evidence.
#[cfg(feature = "scripted-transport-fixture")]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AlpacaPaperIexDoctorFixtureObservation {
    data: AlpacaPaperIexDoctorObservationData,
}

macro_rules! impl_observation_accessors {
    ($observation:ty) => {
        impl $observation {
            /// Returns the digest-bound authority origin of this typed observation.
            pub const fn origin(&self) -> AlpacaDoctorObservationOrigin {
                self.data.origin
            }

            /// Returns the digest-only Paper credential principal used by every probe.
            pub const fn market_data_principal_sha256(&self) -> EvidenceDigest {
                self.data.market_data_principal_sha256
            }

            pub const fn quote(&self) -> &AlpacaDoctorQuoteObservation {
                &self.data.quote
            }
            pub const fn batch(&self) -> &AlpacaDoctorBatchObservation {
                &self.data.batch
            }
            pub const fn stream(&self) -> &AlpacaDoctorStreamObservation {
                &self.data.stream
            }
            pub const fn historical(&self) -> &AlpacaDoctorHistoricalObservation {
                &self.data.historical
            }
            pub const fn calendar(&self) -> &AlpacaDoctorCalendarObservation {
                &self.data.calendar
            }
            pub const fn completed_at(&self) -> Timestamp {
                self.data.completed_at
            }
            pub const fn observation_digest(&self) -> EvidenceDigest {
                self.data.observation_digest
            }

            pub const fn indicative_options_rest(&self) -> AlpacaDoctorObservationDisposition {
                AlpacaDoctorObservationDisposition::Unprobed
            }
            pub const fn indicative_options_stream(&self) -> AlpacaDoctorObservationDisposition {
                AlpacaDoctorObservationDisposition::Unprobed
            }
            pub const fn fixed_income(&self) -> AlpacaDoctorObservationDisposition {
                AlpacaDoctorObservationDisposition::Unprobed
            }
            pub const fn corporate_actions(&self) -> AlpacaDoctorObservationDisposition {
                AlpacaDoctorObservationDisposition::Unprobed
            }
            pub const fn consolidated_sip(&self) -> AlpacaDoctorObservationDisposition {
                AlpacaDoctorObservationDisposition::Unsupported
            }
            pub const fn nbbo(&self) -> AlpacaDoctorObservationDisposition {
                AlpacaDoctorObservationDisposition::Unsupported
            }
            pub const fn opra(&self) -> AlpacaDoctorObservationDisposition {
                AlpacaDoctorObservationDisposition::Unsupported
            }
            pub const fn price_level_depth(&self) -> AlpacaDoctorObservationDisposition {
                AlpacaDoctorObservationDisposition::Unsupported
            }
            pub const fn order_level_depth(&self) -> AlpacaDoctorObservationDisposition {
                AlpacaDoctorObservationDisposition::Unsupported
            }
            pub const fn brokerage_account(&self) -> AlpacaDoctorObservationDisposition {
                AlpacaDoctorObservationDisposition::Unsupported
            }
            pub const fn positions(&self) -> AlpacaDoctorObservationDisposition {
                AlpacaDoctorObservationDisposition::Unsupported
            }
            pub const fn orders(&self) -> AlpacaDoctorObservationDisposition {
                AlpacaDoctorObservationDisposition::Unsupported
            }
            pub const fn trading(&self) -> AlpacaDoctorObservationDisposition {
                AlpacaDoctorObservationDisposition::Unsupported
            }
        }
    };
}

impl_observation_accessors!(AlpacaPaperIexDoctorObservation);
#[cfg(feature = "scripted-transport-fixture")]
impl_observation_accessors!(AlpacaPaperIexDoctorFixtureObservation);

/// Credential-bearing fixed-route doctor. It is not an account or execution client.
pub struct AlpacaPaperIexDoctor {
    credentials: Arc<AlpacaCredentials>,
    client: reqwest::Client,
    bounds: HttpRequestBounds,
    stream_limits: AlpacaTransportLimits,
}

impl AlpacaPaperIexDoctor {
    /// Constructs the closed Paper/IEX doctor with code-owned REST bounds.
    pub fn try_new(
        credentials: Arc<AlpacaCredentials>,
        stream_limits: AlpacaTransportLimits,
    ) -> Result<Self, AlpacaError> {
        let bounds = HttpRequestBounds::default();
        Ok(Self {
            credentials,
            client: hardened_client(bounds, USER_AGENT)?,
            bounds,
            stream_limits,
        })
    }

    /// Executes the five fixed probes through one shared provider/account budget.
    pub async fn observe(
        &self,
        budget: &SharedProviderBudget,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<AlpacaPaperIexDoctorObservation, AlpacaError> {
        ensure_before(deadline, cancellation)?;
        let quote = self.quote(budget, deadline, cancellation).await?;
        let batch = self.batch(budget, deadline, cancellation).await?;
        let stream = self.stream(budget, deadline, cancellation).await?;
        let historical = self.historical(budget, deadline, cancellation).await?;
        let calendar = self
            .calendar(&historical, budget, deadline, cancellation)
            .await?;
        let completed_at = system_timestamp()?;
        Ok(complete_provider_observation(
            self.credentials.paper_market_data_principal_sha256(),
            quote,
            batch,
            stream,
            historical,
            calendar,
            completed_at,
        ))
    }

    async fn quote(
        &self,
        budget: &SharedProviderBudget,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<AlpacaDoctorQuoteObservation, AlpacaError> {
        let url = exact_url(QUOTE_ENDPOINT)?;
        let response = self
            .get(
                url,
                DoctorEndpointContract::Quote,
                MAX_QUOTE_RESPONSE_BYTES,
                budget,
                deadline,
                cancellation,
            )
            .await?;
        quote_observation(response)
    }

    async fn batch(
        &self,
        budget: &SharedProviderBudget,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<AlpacaDoctorBatchObservation, AlpacaError> {
        let response = self
            .get(
                batch_url()?,
                DoctorEndpointContract::Batch,
                MAX_BATCH_RESPONSE_BYTES,
                budget,
                deadline,
                cancellation,
            )
            .await?;
        batch_observation(response)
    }

    async fn get(
        &self,
        url: Url,
        endpoint: DoctorEndpointContract,
        hard_maximum_bytes: usize,
        budget: &SharedProviderBudget,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<HttpProbeResponse, AlpacaError> {
        let permit = acquire_budget(budget, deadline, cancellation).await?;
        let started = Instant::now();
        let request_digest = request_digest("GET", url.as_str())?;
        let response = authenticated_bounded_get(
            &self.client,
            &self.credentials,
            &url,
            self.bounds,
            hard_maximum_bytes,
            deadline,
            cancellation,
        )
        .await?;
        let retry_after_raw =
            singleton_bounded_header(&response.headers, RETRY_AFTER, MAX_RETRY_AFTER_BYTES)?;
        let rate = rate_evidence(&response.headers, retry_after_raw.as_deref())?;
        if matches!(response.status, 429 | 503) {
            apply_refusal(budget, retry_after_raw.as_deref());
        } else if response.status == 200 {
            budget.record_success().map_err(|_| AlpacaError::Network)?;
        }
        permit.release();
        if matches!(response.status, 401 | 403) {
            return Err(AlpacaError::InvalidAuthorization);
        }
        let response_bytes =
            u64::try_from(response.body.len()).map_err(|_| AlpacaError::BodyTooLarge)?;
        let latency_nanos =
            u64::try_from(started.elapsed().as_nanos()).map_err(|_| AlpacaError::Network)?;
        let body_digest = sha256(&response.body);
        Ok(HttpProbeResponse {
            evidence: AlpacaDoctorHttpEvidence {
                endpoint_contract_digest: endpoint_contract_digest(endpoint)?,
                request_digest,
                status_code: response.status,
                body_digest,
                response_bytes,
                received_at: response.received_at,
                latency_nanos,
                rate,
            },
            body: response.body,
        })
    }
}

fn complete_provider_observation(
    market_data_principal_sha256: EvidenceDigest,
    quote: AlpacaDoctorQuoteObservation,
    batch: AlpacaDoctorBatchObservation,
    stream: AlpacaDoctorStreamObservation,
    historical: AlpacaDoctorHistoricalObservation,
    calendar: AlpacaDoctorCalendarObservation,
    completed_at: Timestamp,
) -> AlpacaPaperIexDoctorObservation {
    AlpacaPaperIexDoctorObservation {
        data: complete_observation_data(
            AlpacaDoctorObservationOrigin::ProviderObserved,
            market_data_principal_sha256,
            quote,
            batch,
            stream,
            historical,
            calendar,
            completed_at,
        ),
    }
}

#[cfg(feature = "scripted-transport-fixture")]
fn complete_fixture_observation(
    quote: AlpacaDoctorQuoteObservation,
    batch: AlpacaDoctorBatchObservation,
    stream: AlpacaDoctorStreamObservation,
    historical: AlpacaDoctorHistoricalObservation,
    calendar: AlpacaDoctorCalendarObservation,
    completed_at: Timestamp,
) -> AlpacaPaperIexDoctorFixtureObservation {
    AlpacaPaperIexDoctorFixtureObservation {
        data: complete_observation_data(
            AlpacaDoctorObservationOrigin::InstalledFixture,
            fixture_market_data_principal_sha256(),
            quote,
            batch,
            stream,
            historical,
            calendar,
            completed_at,
        ),
    }
}

fn complete_observation_data(
    origin: AlpacaDoctorObservationOrigin,
    market_data_principal_sha256: EvidenceDigest,
    quote: AlpacaDoctorQuoteObservation,
    batch: AlpacaDoctorBatchObservation,
    stream: AlpacaDoctorStreamObservation,
    historical: AlpacaDoctorHistoricalObservation,
    calendar: AlpacaDoctorCalendarObservation,
    completed_at: Timestamp,
) -> AlpacaPaperIexDoctorObservationData {
    let observation_digest = doctor_observation_digest(
        origin,
        market_data_principal_sha256,
        &quote,
        &batch,
        &stream,
        &historical,
        &calendar,
        completed_at,
    );
    AlpacaPaperIexDoctorObservationData {
        origin,
        market_data_principal_sha256,
        quote,
        batch,
        stream,
        historical,
        calendar,
        completed_at,
        observation_digest,
    }
}

/// Replays one closed raw Paper/IEX transcript through the production parsers.
///
/// The fixture has no credential, network, provider-budget, or caller-selected request surface.
#[cfg(feature = "scripted-transport-fixture")]
pub(crate) fn installed_fixture_observation(
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<AlpacaPaperIexDoctorFixtureObservation, AlpacaError> {
    ensure_before(deadline, cancellation)?;
    let completed_at = system_timestamp()?;
    let quote_timestamp = completed_at
        .checked_sub_nanos(10_000_000)
        .map_err(|_| AlpacaError::Protocol)?;
    let quote_timestamp_text = fixture_rfc3339(quote_timestamp)?;

    let quote_body = serde_json::to_vec(&serde_json::json!({
        "quote": {
            "t": quote_timestamp_text,
            "bp": 100.00,
            "ap": 100.01,
            "bs": 100,
            "as": 100
        },
        "symbol": DOCTOR_SYMBOL
    }))
    .map_err(|_| AlpacaError::Serialization)?;
    let quote = quote_observation(fixture_http_response(
        DoctorEndpointContract::Quote,
        exact_url(QUOTE_ENDPOINT)?,
        quote_body,
        completed_at
            .checked_sub_nanos(9_000_000)
            .map_err(|_| AlpacaError::Protocol)?,
        MAX_QUOTE_RESPONSE_BYTES,
    )?)?;

    let snapshot_quote = serde_json::json!({
        "t": quote_timestamp_text,
        "bp": 100.00,
        "ap": 100.01,
        "bs": 100,
        "as": 100
    });
    let mut snapshots = serde_json::Map::new();
    for symbol in BATCH_SYMBOLS {
        snapshots.insert(
            symbol.to_owned(),
            serde_json::json!({"latestQuote": snapshot_quote.clone()}),
        );
    }
    let batch_body = serde_json::to_vec(&snapshots).map_err(|_| AlpacaError::Serialization)?;
    let batch = batch_observation(fixture_http_response(
        DoctorEndpointContract::Batch,
        batch_url()?,
        batch_body,
        completed_at
            .checked_sub_nanos(8_000_000)
            .map_err(|_| AlpacaError::Protocol)?,
        MAX_BATCH_RESPONSE_BYTES,
    )?)?;

    let stream = fixture_stream_observation(completed_at)?;
    let (start_date, end_date) = doctor_date_range()?;
    let bar_timestamp = fixture_midnight_timestamp(end_date)?;
    let historical_body = serde_json::to_vec(&serde_json::json!({
        "bars": [{
            "t": fixture_rfc3339(bar_timestamp)?,
            "o": 100.00,
            "h": 101.00,
            "l": 99.00,
            "c": 100.50,
            "v": 1_000,
            "n": 100,
            "vw": 100.25
        }],
        "symbol": DOCTOR_SYMBOL,
        "next_page_token": null
    }))
    .map_err(|_| AlpacaError::Serialization)?;
    let history_url = historical_url(start_date, end_date, None)?;
    let history_response = fixture_http_response(
        DoctorEndpointContract::Historical,
        history_url,
        historical_body,
        completed_at
            .checked_sub_nanos(5_000_000)
            .map_err(|_| AlpacaError::Protocol)?,
        MAX_HISTORICAL_PAGE_BYTES,
    )?;
    let parsed_history = parse_historical_page(&history_response.body, start_date, end_date)?;
    if parsed_history.next_page_token.is_some() {
        return Err(AlpacaError::Protocol);
    }
    let mut returned_dates = BTreeSet::new();
    let mut first_bar_timestamp = None;
    let mut last_bar_timestamp = None;
    for (date, timestamp) in parsed_history.bars.iter().copied() {
        if last_bar_timestamp.is_some_and(|prior| prior >= timestamp)
            || !returned_dates.insert(date)
        {
            return Err(AlpacaError::Protocol);
        }
        first_bar_timestamp.get_or_insert(timestamp);
        last_bar_timestamp = Some(timestamp);
    }
    let returned_bar_count = parsed_history.bars.len();
    let historical = historical_observation(
        AlpacaDoctorObservationDisposition::ObservedAvailable,
        start_date,
        end_date,
        vec![AlpacaDoctorHttpPageEvidence {
            http: history_response.evidence,
            request_page_token_digest: None,
            response_page_token_digest: None,
        }],
        returned_dates,
        returned_bar_count,
        first_bar_timestamp,
        last_bar_timestamp,
        true,
    )?;

    let calendar_request = AlpacaAuthenticatedCalendarRequest::try_new(
        AlpacaTradingApiEnvironment::Paper,
        start_date,
        end_date,
    )?;
    let calendar_url = exact_url(&format!(
        "{}{}",
        calendar_request.origin(),
        calendar_request.path_and_query()
    ))?;
    let calendar_body = serde_json::to_vec(&serde_json::json!({
        "market": {"acronym": DOCTOR_MARKET, "timezone": DOCTOR_TIMEZONE},
        "calendar": [{"date": end_date.to_string()}]
    }))
    .map_err(|_| AlpacaError::Serialization)?;
    let calendar = calendar_observation(
        fixture_http_response(
            DoctorEndpointContract::Calendar,
            calendar_url,
            calendar_body,
            completed_at
                .checked_sub_nanos(2_000_000)
                .map_err(|_| AlpacaError::Protocol)?,
            ALPACA_HISTORICAL_CALENDAR_MAX_RESPONSE_BYTES,
        )?,
        &historical,
    )?;

    ensure_before(deadline, cancellation)?;
    Ok(complete_fixture_observation(
        quote,
        batch,
        stream,
        historical,
        calendar,
        completed_at,
    ))
}

#[cfg(feature = "scripted-transport-fixture")]
fn fixture_http_response(
    contract: DoctorEndpointContract,
    url: Url,
    body: Vec<u8>,
    received_at: Timestamp,
    maximum_bytes: usize,
) -> Result<HttpProbeResponse, AlpacaError> {
    if body.len() > maximum_bytes {
        return Err(AlpacaError::BodyTooLarge);
    }
    let response_bytes = u64::try_from(body.len()).map_err(|_| AlpacaError::BodyTooLarge)?;
    Ok(HttpProbeResponse {
        evidence: AlpacaDoctorHttpEvidence {
            endpoint_contract_digest: endpoint_contract_digest(contract)?,
            request_digest: request_digest("GET", url.as_str())?,
            status_code: 200,
            body_digest: sha256(&body),
            response_bytes,
            received_at,
            latency_nanos: 1_000_000,
            rate: missing_rate_evidence(),
        },
        body: body.into_boxed_slice(),
    })
}

#[cfg(feature = "scripted-transport-fixture")]
fn fixture_stream_observation(
    completed_at: Timestamp,
) -> Result<AlpacaDoctorStreamObservation, AlpacaError> {
    const CONNECTED: &[u8] = br#"[{"T":"success","msg":"connected"}]"#;
    const AUTHENTICATED: &[u8] = br#"[{"T":"success","msg":"authenticated"}]"#;
    const SUBSCRIBED: &[u8] = br#"[{"T":"subscription","trades":["AAPL"],"quotes":["AAPL"],"bars":[],"updatedBars":[],"dailyBars":[],"statuses":[],"lulds":[],"corrections":[],"cancelErrors":[]}]"#;
    let connected = parse_control_frame(CONNECTED)?;
    let authenticated = parse_control_frame(AUTHENTICATED)?;
    let subscribed = parse_control_frame(SUBSCRIBED)?;
    if !connected.connected
        || connected.authenticated
        || connected.subscription.is_some()
        || !authenticated.authenticated
        || authenticated.connected
        || authenticated.subscription.is_some()
        || subscribed.subscription != Some((1, 1))
        || subscribed.connected
        || subscribed.authenticated
        || connected.error_code.is_some()
        || authenticated.error_code.is_some()
        || subscribed.error_code.is_some()
    {
        return Err(AlpacaError::Protocol);
    }
    let authenticated_at = completed_at
        .checked_sub_nanos(7_000_000)
        .map_err(|_| AlpacaError::Protocol)?;
    let subscribed_at = completed_at
        .checked_sub_nanos(6_000_000)
        .map_err(|_| AlpacaError::Protocol)?;
    let stream_completed_at = completed_at
        .checked_sub_nanos(5_500_000)
        .map_err(|_| AlpacaError::Protocol)?;
    let frames_observed = 3;
    let bytes_observed = [CONNECTED, AUTHENTICATED, SUBSCRIBED]
        .into_iter()
        .try_fold(0_u64, |total, payload| {
            total.checked_add(u64::try_from(payload.len()).ok()?)
        })
        .ok_or(AlpacaError::Protocol)?;
    let connected_frame_digest = sha256(CONNECTED);
    let authenticated_frame_digest = sha256(AUTHENTICATED);
    let subscription_frame_digest = sha256(SUBSCRIBED);
    let handshake_rate = missing_rate_evidence();
    let disposition = AlpacaDoctorObservationDisposition::ObservedAvailable;
    let semantic_result_digest = stream_semantic_digest(
        disposition,
        101,
        &handshake_rate,
        connected_frame_digest,
        authenticated_frame_digest,
        subscription_frame_digest,
        1,
        1,
        frames_observed,
        bytes_observed,
        authenticated_at,
        subscribed_at,
        true,
        true,
        stream_completed_at,
    );
    Ok(AlpacaDoctorStreamObservation {
        disposition,
        endpoint_contract_digest: endpoint_contract_digest(DoctorEndpointContract::Stream)?,
        request_digest: stream_request_digest()?,
        handshake_status: 101,
        handshake_rate,
        connected_frame_digest,
        authenticated_frame_digest,
        subscription_frame_digest,
        semantic_result_digest,
        subscribed_trade_count: 1,
        subscribed_quote_count: 1,
        frames_observed,
        bytes_observed,
        authenticated_at,
        subscribed_at,
        close_sent: true,
        clean_close_observed: true,
        completed_at: stream_completed_at,
    })
}

#[cfg(feature = "scripted-transport-fixture")]
fn fixture_midnight_timestamp(date: CalendarDate) -> Result<Timestamp, AlpacaError> {
    let date = NaiveDate::from_ymd_opt(
        i32::from(date.year()),
        u32::from(date.month()),
        u32::from(date.day()),
    )
    .ok_or(AlpacaError::Protocol)?;
    let value = date.and_hms_opt(0, 0, 0).ok_or(AlpacaError::Protocol)?;
    DateTime::<Utc>::from_naive_utc_and_offset(value, Utc)
        .timestamp_nanos_opt()
        .map(Timestamp::from_unix_nanos)
        .ok_or(AlpacaError::Protocol)
}

#[cfg(feature = "scripted-transport-fixture")]
fn fixture_rfc3339(timestamp: Timestamp) -> Result<String, AlpacaError> {
    DateTime::<Utc>::from_timestamp_nanos(timestamp.unix_nanos())
        .to_rfc3339_opts(chrono::SecondsFormat::Nanos, true)
        .parse()
        .map_err(|_| AlpacaError::Protocol)
}

#[cfg(feature = "scripted-transport-fixture")]
const fn missing_rate_evidence() -> AlpacaDoctorRateEvidence {
    AlpacaDoctorRateEvidence {
        limit: AlpacaDoctorObservedField::Missing,
        remaining: AlpacaDoctorObservedField::Missing,
        reset_unix_seconds: AlpacaDoctorObservedField::Missing,
        retry_after: AlpacaDoctorObservedField::Missing,
    }
}

fn quote_observation(
    response: HttpProbeResponse,
) -> Result<AlpacaDoctorQuoteObservation, AlpacaError> {
    if response.evidence.status_code != 200 {
        let semantic_result_digest = quote_semantic_digest(
            AlpacaDoctorObservationDisposition::ObservedUnavailable,
            None,
            None,
            None,
            None,
            None,
        )?;
        return Ok(AlpacaDoctorQuoteObservation {
            disposition: AlpacaDoctorObservationDisposition::ObservedUnavailable,
            http: response.evidence,
            semantic_result_digest,
            quote_timestamp: None,
            bid_price: None,
            ask_price: None,
            bid_size: None,
            ask_size: None,
        });
    }
    let wire: LatestQuoteEnvelope =
        serde_json::from_slice(&response.body).map_err(|_| AlpacaError::Protocol)?;
    if wire.symbol != DOCTOR_SYMBOL {
        return Err(AlpacaError::Protocol);
    }
    let parsed = parse_quote(wire.quote)?;
    let disposition = if parsed.bid_price.is_zero()
        || parsed.ask_price.is_zero()
        || parsed.bid_price > parsed.ask_price
    {
        AlpacaDoctorObservationDisposition::ObservedDegraded
    } else {
        AlpacaDoctorObservationDisposition::ObservedAvailable
    };
    let semantic_result_digest = quote_semantic_digest(
        disposition,
        Some(parsed.timestamp),
        Some(parsed.bid_price),
        Some(parsed.ask_price),
        Some(parsed.bid_size),
        Some(parsed.ask_size),
    )?;
    Ok(AlpacaDoctorQuoteObservation {
        disposition,
        http: response.evidence,
        semantic_result_digest,
        quote_timestamp: Some(parsed.timestamp),
        bid_price: Some(parsed.bid_price),
        ask_price: Some(parsed.ask_price),
        bid_size: Some(parsed.bid_size),
        ask_size: Some(parsed.ask_size),
    })
}

fn batch_url() -> Result<Url, AlpacaError> {
    let mut url = exact_url(SNAPSHOT_ENDPOINT)?;
    url.query_pairs_mut()
        .append_pair("symbols", &BATCH_SYMBOLS.join(","))
        .append_pair("feed", DOCTOR_FEED);
    Ok(url)
}

fn batch_observation(
    response: HttpProbeResponse,
) -> Result<AlpacaDoctorBatchObservation, AlpacaError> {
    let requested: BTreeSet<&str> = BATCH_SYMBOLS.into_iter().collect();
    let requested_symbols_digest = symbol_set_digest(requested.iter().copied())?;
    if response.evidence.status_code != 200 {
        let empty = symbol_set_digest(std::iter::empty())?;
        let returned_count = 0;
        let missing_count = u32::try_from(requested.len()).map_err(|_| AlpacaError::Protocol)?;
        let unexpected_count = 0;
        let duplicate_count = 0;
        let invalid_count = 0;
        let effective_cardinality = 0;
        let semantic_result_digest = batch_semantic_digest(
            AlpacaDoctorObservationDisposition::ObservedUnavailable,
            returned_count,
            missing_count,
            unexpected_count,
            duplicate_count,
            invalid_count,
            effective_cardinality,
            requested_symbols_digest,
            empty,
            requested_symbols_digest,
            empty,
        );
        return Ok(AlpacaDoctorBatchObservation {
            disposition: AlpacaDoctorObservationDisposition::ObservedUnavailable,
            http: response.evidence,
            semantic_result_digest,
            returned_count,
            missing_count,
            unexpected_count,
            duplicate_count,
            invalid_count,
            effective_cardinality,
            requested_symbols_digest,
            returned_symbols_digest: empty,
            missing_symbols_digest: requested_symbols_digest,
            unexpected_symbols_digest: empty,
        });
    }
    let wire: SnapshotMapWire =
        serde_json::from_slice(&response.body).map_err(|_| AlpacaError::Protocol)?;
    let mut seen = BTreeSet::new();
    let mut returned = BTreeSet::new();
    let mut unexpected = BTreeSet::new();
    let mut duplicate_count = 0_u32;
    let mut invalid_count = 0_u32;
    let mut effective = BTreeSet::new();
    for (symbol, snapshot) in wire.0 {
        validate_symbol(&symbol)?;
        if !seen.insert(symbol.clone()) {
            duplicate_count = duplicate_count
                .checked_add(1)
                .ok_or(AlpacaError::Protocol)?;
            continue;
        }
        if requested.contains(symbol.as_str()) {
            returned.insert(symbol.clone());
            if validate_snapshot(&snapshot).is_ok() {
                effective.insert(symbol);
            } else {
                invalid_count = invalid_count.checked_add(1).ok_or(AlpacaError::Protocol)?;
            }
        } else {
            unexpected.insert(symbol);
        }
    }
    let missing = requested
        .iter()
        .copied()
        .filter(|symbol| !returned.contains(*symbol))
        .collect::<BTreeSet<_>>();
    let returned_count = u32_count(returned.len())?;
    let missing_count = u32_count(missing.len())?;
    let unexpected_count = u32_count(unexpected.len())?;
    let effective_cardinality = u32_count(effective.len())?;
    let disposition = if missing_count == 0
        && unexpected_count == 0
        && duplicate_count == 0
        && invalid_count == 0
        && effective_cardinality
            == u32::try_from(ALPACA_PAPER_IEX_DOCTOR_BATCH_SYMBOL_COUNT)
                .map_err(|_| AlpacaError::Protocol)?
    {
        AlpacaDoctorObservationDisposition::ObservedAvailable
    } else {
        AlpacaDoctorObservationDisposition::ObservedDegraded
    };
    let returned_symbols_digest = symbol_set_digest(returned.iter().map(String::as_str))?;
    let missing_symbols_digest = symbol_set_digest(missing.iter().copied())?;
    let unexpected_symbols_digest = symbol_set_digest(unexpected.iter().map(String::as_str))?;
    let semantic_result_digest = batch_semantic_digest(
        disposition,
        returned_count,
        missing_count,
        unexpected_count,
        duplicate_count,
        invalid_count,
        effective_cardinality,
        requested_symbols_digest,
        returned_symbols_digest,
        missing_symbols_digest,
        unexpected_symbols_digest,
    );
    Ok(AlpacaDoctorBatchObservation {
        disposition,
        http: response.evidence,
        semantic_result_digest,
        returned_count,
        missing_count,
        unexpected_count,
        duplicate_count,
        invalid_count,
        effective_cardinality,
        requested_symbols_digest,
        returned_symbols_digest,
        missing_symbols_digest,
        unexpected_symbols_digest,
    })
}

impl std::fmt::Debug for AlpacaPaperIexDoctor {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AlpacaPaperIexDoctor")
            .field("credentials", &"[REDACTED ZEROIZING ARC]")
            .field("bounds", &self.bounds)
            .field("stream_limits", &self.stream_limits)
            .finish_non_exhaustive()
    }
}

struct HttpProbeResponse {
    evidence: AlpacaDoctorHttpEvidence,
    body: Box<[u8]>,
}

#[derive(Deserialize)]
struct LatestQuoteEnvelope {
    quote: QuoteWire,
    symbol: String,
}

#[derive(Deserialize)]
struct QuoteWire {
    #[serde(rename = "t")]
    timestamp: String,
    #[serde(rename = "bp")]
    bid_price: Number,
    #[serde(rename = "ap")]
    ask_price: Number,
    #[serde(rename = "bs")]
    bid_size: Number,
    #[serde(rename = "as")]
    ask_size: Number,
}

struct ParsedQuote {
    timestamp: Timestamp,
    bid_price: Decimal,
    ask_price: Decimal,
    bid_size: u64,
    ask_size: u64,
}

struct SnapshotMapWire(Vec<(String, Value)>);

impl<'de> Deserialize<'de> for SnapshotMapWire {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_map(SnapshotMapVisitor)
    }
}

struct SnapshotMapVisitor;

impl<'de> Visitor<'de> for SnapshotMapVisitor {
    type Value = SnapshotMapWire;

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("an Alpaca multi-symbol snapshot object")
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        let mut values = Vec::new();
        values
            .try_reserve(ALPACA_PAPER_IEX_DOCTOR_BATCH_SYMBOL_COUNT + 1)
            .map_err(|_| serde::de::Error::custom("snapshot allocation failed"))?;
        while let Some(entry) = map.next_entry::<String, Value>()? {
            if values.len() > ALPACA_PAPER_IEX_DOCTOR_BATCH_SYMBOL_COUNT * 2 {
                return Err(serde::de::Error::custom("snapshot object exceeds bound"));
            }
            values.push(entry);
        }
        Ok(SnapshotMapWire(values))
    }
}

impl AlpacaPaperIexDoctor {
    async fn stream(
        &self,
        budget: &SharedProviderBudget,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<AlpacaDoctorStreamObservation, AlpacaError> {
        ensure_before(deadline, cancellation)?;
        let permit = acquire_budget(budget, deadline, cancellation).await?;
        let endpoint_contract_digest = endpoint_contract_digest(DoctorEndpointContract::Stream)?;
        let request_digest = stream_request_digest()?;
        let request = authenticated_stream_request(&self.credentials)?;
        let websocket_config = WebSocketConfig::default()
            .read_buffer_size(
                self.stream_limits
                    .max_frame_bytes()
                    .clamp(4 * 1024, 128 * 1024),
            )
            .write_buffer_size(16 * 1024)
            .max_write_buffer_size(64 * 1024)
            .max_message_size(Some(self.stream_limits.max_frame_bytes()))
            .max_frame_size(Some(self.stream_limits.max_frame_bytes()));
        let connect_timeout =
            bounded_timeout(deadline, self.stream_limits.connect_timeout(), cancellation)?;
        let connected = tokio::select! {
            biased;
            () = cancellation.cancelled() => return Err(AlpacaError::Cancelled),
            result = tokio::time::timeout(
                connect_timeout,
                connect_async_with_config(request, Some(websocket_config), true),
            ) => match result {
                Ok(Ok(value)) => value,
                Ok(Err(error)) => return Err(map_stream_connect_error(error, budget)),
                Err(_) => return Err(AlpacaError::DeadlineExceeded),
            },
        };
        let (mut socket, response) = connected;
        if response.status().as_u16() != 101 {
            return Err(AlpacaError::Protocol);
        }
        let retry_after =
            singleton_bounded_header(response.headers(), RETRY_AFTER, MAX_RETRY_AFTER_BYTES)?;
        let handshake_rate = rate_evidence(response.headers(), retry_after.as_deref())?;
        let handshake_status = response.status().as_u16();
        drop(response);

        let mut connected_frame_digest = None;
        let mut authenticated_frame_digest = None;
        let mut authenticated_at = None;
        let mut frames_observed = 0_u32;
        let mut bytes_observed = 0_u64;
        for _ in 0..MAX_CONTROL_FRAMES {
            let message = read_stream_message(
                &mut socket,
                deadline,
                self.stream_limits.io_timeout(),
                self.stream_limits.max_frame_bytes(),
                cancellation,
            )
            .await?;
            let Some(payload) = control_payload(
                message,
                &mut socket,
                deadline,
                self.stream_limits.io_timeout(),
                cancellation,
            )
            .await?
            else {
                continue;
            };
            frames_observed = frames_observed
                .checked_add(1)
                .ok_or(AlpacaError::Protocol)?;
            bytes_observed = bytes_observed
                .checked_add(u64::try_from(payload.len()).map_err(|_| AlpacaError::Protocol)?)
                .ok_or(AlpacaError::Protocol)?;
            let facts = parse_control_frame(&payload)?;
            if let Some(code) = facts.error_code {
                return Err(if matches!(code, 401 | 402) {
                    AlpacaError::InvalidAuthorization
                } else {
                    AlpacaError::Protocol
                });
            }
            let digest = sha256(&payload);
            if facts.connected {
                if connected_frame_digest.replace(digest).is_some() {
                    return Err(AlpacaError::Protocol);
                }
            }
            if facts.authenticated {
                if authenticated_frame_digest.replace(digest).is_some()
                    || authenticated_at.replace(system_timestamp()?).is_some()
                {
                    return Err(AlpacaError::Protocol);
                }
            }
            if connected_frame_digest.is_some() && authenticated_frame_digest.is_some() {
                break;
            }
        }
        let connected_frame_digest = connected_frame_digest.ok_or(AlpacaError::Protocol)?;
        let authenticated_frame_digest = authenticated_frame_digest.ok_or(AlpacaError::Protocol)?;
        let authenticated_at = authenticated_at.ok_or(AlpacaError::Protocol)?;

        send_stream_message(
            &mut socket,
            Message::Text(STREAM_SUBSCRIPTION.into()),
            deadline,
            self.stream_limits.io_timeout(),
            cancellation,
        )
        .await?;
        let mut subscription_frame_digest = None;
        let mut subscribed_at = None;
        let mut subscribed_trade_count = 0_u32;
        let mut subscribed_quote_count = 0_u32;
        for _ in 0..MAX_CONTROL_FRAMES {
            let message = read_stream_message(
                &mut socket,
                deadline,
                self.stream_limits.io_timeout(),
                self.stream_limits.max_frame_bytes(),
                cancellation,
            )
            .await?;
            let Some(payload) = control_payload(
                message,
                &mut socket,
                deadline,
                self.stream_limits.io_timeout(),
                cancellation,
            )
            .await?
            else {
                continue;
            };
            frames_observed = frames_observed
                .checked_add(1)
                .ok_or(AlpacaError::Protocol)?;
            bytes_observed = bytes_observed
                .checked_add(u64::try_from(payload.len()).map_err(|_| AlpacaError::Protocol)?)
                .ok_or(AlpacaError::Protocol)?;
            let facts = parse_control_frame(&payload)?;
            if facts.error_code.is_some() {
                return Err(AlpacaError::Protocol);
            }
            if let Some((trades, quotes)) = facts.subscription {
                if trades != 1 || quotes != 1 {
                    return Err(AlpacaError::Protocol);
                }
                subscribed_trade_count = trades;
                subscribed_quote_count = quotes;
                subscription_frame_digest = Some(sha256(&payload));
                subscribed_at = Some(system_timestamp()?);
                break;
            }
        }
        let subscription_frame_digest = subscription_frame_digest.ok_or(AlpacaError::Protocol)?;
        let subscribed_at = subscribed_at.ok_or(AlpacaError::Protocol)?;
        budget.record_success().map_err(|_| AlpacaError::Network)?;

        send_stream_message(
            &mut socket,
            Message::Close(None),
            deadline,
            self.stream_limits.io_timeout(),
            cancellation,
        )
        .await?;
        let close_sent = true;
        let mut clean_close_observed = false;
        for _ in 0..MAX_CLOSE_FRAMES {
            let Some(message) = read_optional_close_message(
                &mut socket,
                deadline,
                self.stream_limits.io_timeout(),
                self.stream_limits.max_frame_bytes(),
                cancellation,
            )
            .await?
            else {
                break;
            };
            match message {
                Message::Close(_) => {
                    clean_close_observed = true;
                    break;
                }
                Message::Ping(payload) => {
                    send_stream_message(
                        &mut socket,
                        Message::Pong(payload),
                        deadline,
                        self.stream_limits.io_timeout(),
                        cancellation,
                    )
                    .await?;
                }
                Message::Pong(_) => {}
                Message::Text(text) => {
                    add_frame_bytes(&mut frames_observed, &mut bytes_observed, text.len())?;
                }
                Message::Binary(payload) => {
                    add_frame_bytes(&mut frames_observed, &mut bytes_observed, payload.len())?;
                }
                Message::Frame(_) => return Err(AlpacaError::Protocol),
            }
        }
        permit.release();
        let completed_at = system_timestamp()?;
        let disposition = if clean_close_observed {
            AlpacaDoctorObservationDisposition::ObservedAvailable
        } else {
            AlpacaDoctorObservationDisposition::ObservedDegraded
        };
        let semantic_result_digest = stream_semantic_digest(
            disposition,
            handshake_status,
            &handshake_rate,
            connected_frame_digest,
            authenticated_frame_digest,
            subscription_frame_digest,
            subscribed_trade_count,
            subscribed_quote_count,
            frames_observed,
            bytes_observed,
            authenticated_at,
            subscribed_at,
            close_sent,
            clean_close_observed,
            completed_at,
        );
        Ok(AlpacaDoctorStreamObservation {
            disposition,
            endpoint_contract_digest,
            request_digest,
            handshake_status,
            handshake_rate,
            connected_frame_digest,
            authenticated_frame_digest,
            subscription_frame_digest,
            semantic_result_digest,
            subscribed_trade_count,
            subscribed_quote_count,
            frames_observed,
            bytes_observed,
            authenticated_at,
            subscribed_at,
            close_sent,
            clean_close_observed,
            completed_at,
        })
    }

    async fn historical(
        &self,
        budget: &SharedProviderBudget,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<AlpacaDoctorHistoricalObservation, AlpacaError> {
        let (start_date, end_date) = doctor_date_range()?;
        let mut pages = Vec::new();
        pages
            .try_reserve_exact(MAX_HISTORICAL_PAGES)
            .map_err(|_| AlpacaError::Allocation)?;
        let mut returned_dates = BTreeSet::new();
        let mut seen_tokens = BTreeSet::new();
        let mut request_token: Option<String> = None;
        let mut returned_bar_count = 0_usize;
        let mut first_bar_timestamp = None;
        let mut last_bar_timestamp = None;
        let mut total_bytes = 0_usize;
        let mut terminal_page_observed = false;
        for _ in 0..MAX_HISTORICAL_PAGES {
            let url = historical_url(start_date, end_date, request_token.as_deref())?;
            let response = self
                .get(
                    url,
                    DoctorEndpointContract::Historical,
                    MAX_HISTORICAL_PAGE_BYTES,
                    budget,
                    deadline,
                    cancellation,
                )
                .await?;
            total_bytes = total_bytes
                .checked_add(
                    usize::try_from(response.evidence.response_bytes)
                        .map_err(|_| AlpacaError::BodyTooLarge)?,
                )
                .filter(|bytes| *bytes <= MAX_HISTORICAL_TOTAL_BYTES)
                .ok_or(AlpacaError::BodyTooLarge)?;
            let request_page_token_digest = request_token.as_deref().map(token_digest);
            if response.evidence.status_code != 200 {
                pages.push(AlpacaDoctorHttpPageEvidence {
                    http: response.evidence,
                    request_page_token_digest,
                    response_page_token_digest: None,
                });
                return historical_observation(
                    AlpacaDoctorObservationDisposition::ObservedUnavailable,
                    start_date,
                    end_date,
                    pages,
                    returned_dates,
                    returned_bar_count,
                    first_bar_timestamp,
                    last_bar_timestamp,
                    false,
                );
            }
            let parsed = parse_historical_page(&response.body, start_date, end_date)?;
            for (date, timestamp) in parsed.bars {
                if last_bar_timestamp.is_some_and(|prior| prior >= timestamp)
                    || !returned_dates.insert(date)
                {
                    return Err(AlpacaError::Protocol);
                }
                first_bar_timestamp.get_or_insert(timestamp);
                last_bar_timestamp = Some(timestamp);
                returned_bar_count = returned_bar_count
                    .checked_add(1)
                    .filter(|count| *count <= MAX_HISTORICAL_BARS)
                    .ok_or(AlpacaError::BodyTooLarge)?;
            }
            let next_token = parsed.next_page_token;
            if let Some(token) = next_token.as_deref() {
                validate_page_token(token)?;
                if !seen_tokens.insert(token.to_owned()) {
                    return Err(AlpacaError::Protocol);
                }
            }
            let response_page_token_digest = next_token.as_deref().map(token_digest);
            pages.push(AlpacaDoctorHttpPageEvidence {
                http: response.evidence,
                request_page_token_digest,
                response_page_token_digest,
            });
            match next_token {
                Some(token) => request_token = Some(token),
                None => {
                    terminal_page_observed = true;
                    break;
                }
            }
        }
        if !terminal_page_observed {
            return Err(AlpacaError::BodyTooLarge);
        }
        historical_observation(
            if returned_dates.is_empty() {
                AlpacaDoctorObservationDisposition::ObservedDegraded
            } else {
                AlpacaDoctorObservationDisposition::ObservedAvailable
            },
            start_date,
            end_date,
            pages,
            returned_dates,
            returned_bar_count,
            first_bar_timestamp,
            last_bar_timestamp,
            true,
        )
    }

    async fn calendar(
        &self,
        historical: &AlpacaDoctorHistoricalObservation,
        budget: &SharedProviderBudget,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<AlpacaDoctorCalendarObservation, AlpacaError> {
        let request = AlpacaAuthenticatedCalendarRequest::try_new(
            AlpacaTradingApiEnvironment::Paper,
            historical.start_date,
            historical.end_date,
        )?;
        let target = format!("{}{}", request.origin(), request.path_and_query());
        let response = self
            .get(
                exact_url(&target)?,
                DoctorEndpointContract::Calendar,
                ALPACA_HISTORICAL_CALENDAR_MAX_RESPONSE_BYTES,
                budget,
                deadline,
                cancellation,
            )
            .await?;
        calendar_observation(response, historical)
    }
}

fn calendar_observation(
    response: HttpProbeResponse,
    historical: &AlpacaDoctorHistoricalObservation,
) -> Result<AlpacaDoctorCalendarObservation, AlpacaError> {
    let history_dates = historical
        .returned_dates
        .iter()
        .copied()
        .collect::<BTreeSet<_>>();
    let history_dates_digest = date_set_digest(history_dates.iter().copied())?;
    if response.evidence.status_code != 200 {
        let empty = date_set_digest(std::iter::empty())?;
        let session_count = 0;
        let history_date_count = u32_count(history_dates.len())?;
        let matched_count = 0;
        let missing_history_count = history_date_count;
        let unexpected_history_count = 0;
        let exact_date_reconciliation = false;
        let semantic_result_digest = calendar_semantic_digest(
            AlpacaDoctorObservationDisposition::ObservedUnavailable,
            historical.start_date,
            historical.end_date,
            session_count,
            history_date_count,
            matched_count,
            missing_history_count,
            unexpected_history_count,
            empty,
            history_dates_digest,
            exact_date_reconciliation,
        );
        return Ok(AlpacaDoctorCalendarObservation {
            disposition: AlpacaDoctorObservationDisposition::ObservedUnavailable,
            http: response.evidence,
            semantic_result_digest,
            start_date: historical.start_date,
            end_date: historical.end_date,
            session_count,
            history_date_count,
            matched_count,
            missing_history_count,
            unexpected_history_count,
            session_dates_digest: empty,
            history_dates_digest,
            exact_date_reconciliation: false,
        });
    }
    let sessions =
        parse_calendar_sessions(&response.body, historical.start_date, historical.end_date)?;
    let matched_count = u32_count(sessions.intersection(&history_dates).count())?;
    let missing_history_count = u32_count(history_dates.difference(&sessions).count())?;
    let unexpected_history_count = u32_count(sessions.difference(&history_dates).count())?;
    let exact_date_reconciliation =
        !sessions.is_empty() && missing_history_count == 0 && unexpected_history_count == 0;
    let session_count = u32_count(sessions.len())?;
    let history_date_count = u32_count(history_dates.len())?;
    let session_dates_digest = date_set_digest(sessions.iter().copied())?;
    let semantic_result_digest = calendar_semantic_digest(
        if exact_date_reconciliation {
            AlpacaDoctorObservationDisposition::ObservedAvailable
        } else {
            AlpacaDoctorObservationDisposition::ObservedDegraded
        },
        historical.start_date,
        historical.end_date,
        session_count,
        history_date_count,
        matched_count,
        missing_history_count,
        unexpected_history_count,
        session_dates_digest,
        history_dates_digest,
        exact_date_reconciliation,
    );
    Ok(AlpacaDoctorCalendarObservation {
        disposition: if exact_date_reconciliation {
            AlpacaDoctorObservationDisposition::ObservedAvailable
        } else {
            AlpacaDoctorObservationDisposition::ObservedDegraded
        },
        http: response.evidence,
        semantic_result_digest,
        start_date: historical.start_date,
        end_date: historical.end_date,
        session_count,
        history_date_count,
        matched_count,
        missing_history_count,
        unexpected_history_count,
        session_dates_digest,
        history_dates_digest,
        exact_date_reconciliation,
    })
}

#[derive(Deserialize)]
struct HistoricalPageWire {
    bars: Vec<HistoricalBarWire>,
    symbol: String,
    next_page_token: Option<String>,
}

#[derive(Deserialize)]
struct HistoricalBarWire {
    #[serde(rename = "t")]
    timestamp: String,
    #[serde(rename = "o")]
    open: Number,
    #[serde(rename = "h")]
    high: Number,
    #[serde(rename = "l")]
    low: Number,
    #[serde(rename = "c")]
    close: Number,
    #[serde(rename = "v")]
    volume: Number,
    #[serde(rename = "n")]
    trade_count: Number,
    #[serde(rename = "vw")]
    volume_weighted: Number,
}

struct ParsedHistoricalPage {
    bars: Box<[(CalendarDate, Timestamp)]>,
    next_page_token: Option<String>,
}

#[derive(Deserialize)]
struct CalendarEnvelope {
    market: CalendarMarketWire,
    calendar: Vec<CalendarRowWire>,
}

#[derive(Deserialize)]
struct CalendarMarketWire {
    acronym: String,
    timezone: String,
}

#[derive(Deserialize)]
struct CalendarRowWire {
    date: String,
}

fn parse_historical_page(
    body: &[u8],
    start_date: CalendarDate,
    end_date: CalendarDate,
) -> Result<ParsedHistoricalPage, AlpacaError> {
    let wire: HistoricalPageWire =
        serde_json::from_slice(body).map_err(|_| AlpacaError::Protocol)?;
    if wire.symbol != DOCTOR_SYMBOL || wire.bars.len() > MAX_HISTORICAL_BARS {
        return Err(AlpacaError::Protocol);
    }
    let mut bars = Vec::new();
    bars.try_reserve_exact(wire.bars.len())
        .map_err(|_| AlpacaError::Allocation)?;
    for bar in wire.bars {
        let timestamp = validate_historical_bar(&bar, start_date, end_date)?;
        bars.push((calendar_date(timestamp)?, timestamp));
    }
    Ok(ParsedHistoricalPage {
        bars: bars.into_boxed_slice(),
        next_page_token: wire.next_page_token,
    })
}

fn parse_calendar_sessions(
    body: &[u8],
    start_date: CalendarDate,
    end_date: CalendarDate,
) -> Result<BTreeSet<CalendarDate>, AlpacaError> {
    let wire: CalendarEnvelope = serde_json::from_slice(body).map_err(|_| AlpacaError::Protocol)?;
    if wire.market.acronym != DOCTOR_MARKET || wire.market.timezone != DOCTOR_TIMEZONE {
        return Err(AlpacaError::Protocol);
    }
    let mut sessions = BTreeSet::new();
    for row in wire.calendar {
        let date = parse_date(&row.date)?;
        if date < start_date || date > end_date || !sessions.insert(date) {
            return Err(AlpacaError::Protocol);
        }
    }
    Ok(sessions)
}

#[derive(Default)]
struct ControlFacts {
    connected: bool,
    authenticated: bool,
    subscription: Option<(u32, u32)>,
    error_code: Option<u64>,
}

#[derive(Clone, Copy)]
enum DoctorEndpointContract {
    Quote,
    Batch,
    Stream,
    Historical,
    Calendar,
}

fn parse_quote(wire: QuoteWire) -> Result<ParsedQuote, AlpacaError> {
    let bid_price = decimal(&wire.bid_price, true)?;
    let ask_price = decimal(&wire.ask_price, true)?;
    Ok(ParsedQuote {
        timestamp: parse_timestamp(&wire.timestamp)?,
        bid_price,
        ask_price,
        bid_size: wire.bid_size.as_u64().ok_or(AlpacaError::Protocol)?,
        ask_size: wire.ask_size.as_u64().ok_or(AlpacaError::Protocol)?,
    })
}

fn validate_snapshot(snapshot: &Value) -> Result<(), AlpacaError> {
    let object = snapshot.as_object().ok_or(AlpacaError::Protocol)?;
    let quote = object
        .get("latestQuote")
        .cloned()
        .ok_or(AlpacaError::Protocol)?;
    let wire: QuoteWire = serde_json::from_value(quote).map_err(|_| AlpacaError::Protocol)?;
    let parsed = parse_quote(wire)?;
    if parsed.bid_price.is_zero()
        || parsed.ask_price.is_zero()
        || parsed.bid_price > parsed.ask_price
    {
        return Err(AlpacaError::Protocol);
    }
    Ok(())
}

fn validate_historical_bar(
    wire: &HistoricalBarWire,
    start_date: CalendarDate,
    end_date: CalendarDate,
) -> Result<Timestamp, AlpacaError> {
    let timestamp = parse_timestamp(&wire.timestamp)?;
    let date = calendar_date(timestamp)?;
    if date < start_date || date > end_date {
        return Err(AlpacaError::Protocol);
    }
    let open = decimal(&wire.open, false)?;
    let high = decimal(&wire.high, false)?;
    let low = decimal(&wire.low, false)?;
    let close = decimal(&wire.close, false)?;
    let weighted = decimal(&wire.volume_weighted, true)?;
    if low > open || low > close || high < open || high < close || high < low {
        return Err(AlpacaError::Protocol);
    }
    let _volume = wire.volume.as_u64().ok_or(AlpacaError::Protocol)?;
    let _trade_count = wire.trade_count.as_u64().ok_or(AlpacaError::Protocol)?;
    if weighted.is_sign_negative() {
        return Err(AlpacaError::Protocol);
    }
    Ok(timestamp)
}

fn historical_observation(
    disposition: AlpacaDoctorObservationDisposition,
    start_date: CalendarDate,
    end_date: CalendarDate,
    pages: Vec<AlpacaDoctorHttpPageEvidence>,
    returned_dates: BTreeSet<CalendarDate>,
    returned_bar_count: usize,
    first_bar_timestamp: Option<Timestamp>,
    last_bar_timestamp: Option<Timestamp>,
    terminal_page_observed: bool,
) -> Result<AlpacaDoctorHistoricalObservation, AlpacaError> {
    if pages.is_empty() || pages.len() > MAX_HISTORICAL_PAGES {
        return Err(AlpacaError::Protocol);
    }
    let returned_dates_digest = date_set_digest(returned_dates.iter().copied())?;
    let pagination_graph_digest = pagination_graph_digest(&pages, terminal_page_observed)?;
    let returned_dates = returned_dates.into_iter().collect::<Vec<_>>();
    let endpoint_contract_digest = endpoint_contract_digest(DoctorEndpointContract::Historical)?;
    let request_digest = historical_request_digest(start_date, end_date)?;
    let page_count = u32_count(pages.len())?;
    let returned_bar_count = u32_count(returned_bar_count)?;
    let distinct_date_count = u32_count(returned_dates.len())?;
    let semantic_result_digest = historical_semantic_digest(
        disposition,
        start_date,
        end_date,
        page_count,
        returned_bar_count,
        distinct_date_count,
        first_bar_timestamp,
        last_bar_timestamp,
        returned_dates_digest,
        pagination_graph_digest,
        terminal_page_observed,
    );
    Ok(AlpacaDoctorHistoricalObservation {
        disposition,
        endpoint_contract_digest,
        request_digest,
        semantic_result_digest,
        start_date,
        end_date,
        page_count,
        returned_bar_count,
        distinct_date_count,
        first_bar_timestamp,
        last_bar_timestamp,
        returned_dates_digest,
        pagination_graph_digest,
        terminal_page_observed,
        pages: pages.into_boxed_slice(),
        returned_dates: returned_dates.into_boxed_slice(),
    })
}

fn historical_url(
    start_date: CalendarDate,
    end_date: CalendarDate,
    page_token: Option<&str>,
) -> Result<Url, AlpacaError> {
    if let Some(token) = page_token {
        validate_page_token(token)?;
    }
    let mut url = exact_url(HISTORICAL_ENDPOINT)?;
    url.query_pairs_mut()
        .append_pair("timeframe", DOCTOR_TIMEFRAME)
        .append_pair("start", &format!("{start_date}T00:00:00Z"))
        .append_pair("end", &format!("{end_date}T23:59:59.999999999Z"))
        .append_pair("limit", HISTORICAL_PAGE_LIMIT)
        .append_pair("adjustment", DOCTOR_ADJUSTMENT)
        .append_pair("feed", DOCTOR_FEED)
        .append_pair("sort", DOCTOR_SORT);
    if let Some(token) = page_token {
        url.query_pairs_mut().append_pair("page_token", token);
    }
    Ok(url)
}

fn doctor_date_range() -> Result<(CalendarDate, CalendarDate), AlpacaError> {
    let today = DateTime::<Utc>::from(SystemTime::now()).date_naive();
    let end = today
        .checked_sub_days(Days::new(1))
        .ok_or(AlpacaError::Protocol)?;
    let start = end
        .checked_sub_days(Days::new(DOCTOR_HISTORY_CALENDAR_DAYS - 1))
        .ok_or(AlpacaError::Protocol)?;
    Ok((
        calendar_date_from_naive(start)?,
        calendar_date_from_naive(end)?,
    ))
}

fn calendar_date_from_naive(value: NaiveDate) -> Result<CalendarDate, AlpacaError> {
    CalendarDate::new(
        u16::try_from(value.year()).map_err(|_| AlpacaError::Protocol)?,
        u8::try_from(value.month()).map_err(|_| AlpacaError::Protocol)?,
        u8::try_from(value.day()).map_err(|_| AlpacaError::Protocol)?,
    )
    .map_err(|_| AlpacaError::Protocol)
}

fn calendar_date(value: Timestamp) -> Result<CalendarDate, AlpacaError> {
    let date = DateTime::<Utc>::from_timestamp_nanos(value.unix_nanos()).date_naive();
    calendar_date_from_naive(date)
}

fn parse_date(value: &str) -> Result<CalendarDate, AlpacaError> {
    if value.len() != 10 {
        return Err(AlpacaError::Protocol);
    }
    NaiveDate::parse_from_str(value, "%Y-%m-%d")
        .map_err(|_| AlpacaError::Protocol)
        .and_then(calendar_date_from_naive)
}

fn parse_timestamp(value: &str) -> Result<Timestamp, AlpacaError> {
    if value.len() > 64 || !(value.ends_with('Z') || value.ends_with("+00:00")) {
        return Err(AlpacaError::Protocol);
    }
    let parsed = DateTime::parse_from_rfc3339(value).map_err(|_| AlpacaError::Protocol)?;
    if parsed.offset().local_minus_utc() != 0 {
        return Err(AlpacaError::Protocol);
    }
    parsed
        .timestamp_nanos_opt()
        .map(Timestamp::from_unix_nanos)
        .ok_or(AlpacaError::Protocol)
}

fn decimal(value: &Number, allow_zero: bool) -> Result<Decimal, AlpacaError> {
    let parsed = Decimal::from_str_exact(&value.to_string())
        .map_err(|_| AlpacaError::Protocol)?
        .normalize();
    if parsed.is_sign_negative() || (!allow_zero && parsed.is_zero()) {
        return Err(AlpacaError::Protocol);
    }
    Ok(parsed)
}

fn validate_symbol(symbol: &str) -> Result<(), AlpacaError> {
    if symbol.is_empty()
        || symbol.len() > 32
        || !symbol.bytes().all(|byte| {
            byte.is_ascii_uppercase() || byte.is_ascii_digit() || matches!(byte, b'.' | b'-')
        })
    {
        return Err(AlpacaError::Protocol);
    }
    Ok(())
}

fn validate_page_token(token: &str) -> Result<(), AlpacaError> {
    if token.is_empty()
        || token.len() > MAX_PAGE_TOKEN_BYTES
        || !token.is_ascii()
        || token
            .bytes()
            .any(|byte| byte.is_ascii_control() || byte.is_ascii_whitespace())
    {
        return Err(AlpacaError::Protocol);
    }
    Ok(())
}

fn rate_evidence(
    headers: &HeaderMap,
    retry_after: Option<&[u8]>,
) -> Result<AlpacaDoctorRateEvidence, AlpacaError> {
    let limit = observed_integer_header(headers, "x-ratelimit-limit")?;
    let remaining = observed_integer_header(headers, "x-ratelimit-remaining")?;
    let reset_unix_seconds = observed_integer_header(headers, "x-ratelimit-reset")?;
    if matches!(&limit, AlpacaDoctorObservedField::Observed(0))
        || matches!(&reset_unix_seconds, AlpacaDoctorObservedField::Observed(value) if *value < 0)
    {
        return Err(AlpacaError::Protocol);
    }
    if matches!(
        (&limit, &remaining),
        (
            AlpacaDoctorObservedField::Observed(limit),
            AlpacaDoctorObservedField::Observed(remaining)
        ) if remaining > limit
    ) {
        return Err(AlpacaError::Protocol);
    }
    Ok(AlpacaDoctorRateEvidence {
        limit,
        remaining,
        reset_unix_seconds,
        retry_after: parse_retry_after(retry_after)?,
    })
}

fn observed_integer_header<T>(
    headers: &HeaderMap,
    name: &'static str,
) -> Result<AlpacaDoctorObservedField<T>, AlpacaError>
where
    T: FromStr,
{
    let name = reqwest::header::HeaderName::from_static(name);
    match singleton_bounded_header(headers, name, 32)? {
        Some(value) => {
            let text = std::str::from_utf8(&value).map_err(|_| AlpacaError::Protocol)?;
            let parsed = text.parse::<T>().map_err(|_| AlpacaError::Protocol)?;
            Ok(AlpacaDoctorObservedField::Observed(parsed))
        }
        None => Ok(AlpacaDoctorObservedField::Missing),
    }
}

fn parse_retry_after(
    value: Option<&[u8]>,
) -> Result<AlpacaDoctorObservedField<AlpacaDoctorRetryAfter>, AlpacaError> {
    let Some(value) = value else {
        return Ok(AlpacaDoctorObservedField::Missing);
    };
    if value.is_empty() || value.len() > MAX_RETRY_AFTER_BYTES || !value.is_ascii() {
        return Err(AlpacaError::Protocol);
    }
    let text = std::str::from_utf8(value).map_err(|_| AlpacaError::Protocol)?;
    if text.bytes().all(|byte| byte.is_ascii_digit()) {
        return text
            .parse::<u64>()
            .map(AlpacaDoctorRetryAfter::DelaySeconds)
            .map(AlpacaDoctorObservedField::Observed)
            .map_err(|_| AlpacaError::Protocol);
    }
    let parsed = DateTime::parse_from_rfc2822(text).map_err(|_| AlpacaError::Protocol)?;
    if parsed.timestamp() < 0 {
        return Err(AlpacaError::Protocol);
    }
    Ok(AlpacaDoctorObservedField::Observed(
        AlpacaDoctorRetryAfter::AtUnixSeconds(parsed.timestamp()),
    ))
}

fn apply_refusal(budget: &SharedProviderBudget, raw: Option<&[u8]>) {
    let parsed = parse_retry_after(raw).ok();
    let decision = match parsed {
        Some(AlpacaDoctorObservedField::Observed(AlpacaDoctorRetryAfter::DelaySeconds(
            seconds,
        ))) => seconds
            .checked_mul(1_000_000_000)
            .and_then(NonZeroU64::new)
            .map(RetryAfter::Delay)
            .map_or_else(
                || budget.apply_refusal(1_000),
                |value| budget.apply_retry_after(value),
            ),
        Some(AlpacaDoctorObservedField::Observed(AlpacaDoctorRetryAfter::AtUnixSeconds(
            seconds,
        ))) => seconds
            .checked_mul(1_000_000_000)
            .map(Timestamp::from_unix_nanos)
            .map(RetryAfter::AtWallClock)
            .map_or_else(
                || budget.apply_refusal(1_000),
                |value| budget.apply_retry_after(value),
            ),
        Some(AlpacaDoctorObservedField::Missing) | None => budget.apply_refusal(1_000),
    };
    drop(decision);
}

async fn acquire_budget(
    budget: &SharedProviderBudget,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<BudgetPermit, AlpacaError> {
    loop {
        ensure_before(deadline, cancellation)?;
        match budget.try_acquire() {
            BudgetDecision::Ready(permit) => return Ok(permit),
            BudgetDecision::WaitUntil(wait_until) => {
                let wait = budget
                    .remaining_wait(wait_until)
                    .map_err(|_| AlpacaError::Network)?;
                let remaining = deadline
                    .checked_duration_since(Instant::now())
                    .ok_or(AlpacaError::DeadlineExceeded)?;
                if wait > remaining {
                    return Err(AlpacaError::DeadlineExceeded);
                }
                tokio::select! {
                    biased;
                    () = cancellation.cancelled() => return Err(AlpacaError::Cancelled),
                    () = tokio::time::sleep(wait) => {}
                }
            }
            BudgetDecision::Unavailable(_) => return Err(AlpacaError::Network),
        }
    }
}

fn authenticated_stream_request(
    credentials: &AlpacaCredentials,
) -> Result<tokio_tungstenite::tungstenite::http::Request<()>, AlpacaError> {
    let mut request = STREAM_ENDPOINT
        .into_client_request()
        .map_err(|_| AlpacaError::Protocol)?;
    let mut key =
        HeaderValue::from_str(credentials.key_id()).map_err(|_| AlpacaError::InvalidCredentials)?;
    key.set_sensitive(true);
    let mut secret = HeaderValue::from_str(credentials.secret_key())
        .map_err(|_| AlpacaError::InvalidCredentials)?;
    secret.set_sensitive(true);
    request.headers_mut().insert(KEY_ID_HEADER, key);
    request.headers_mut().insert(SECRET_KEY_HEADER, secret);
    Ok(request)
}

fn map_stream_connect_error(error: WebSocketError, budget: &SharedProviderBudget) -> AlpacaError {
    if let WebSocketError::Http(response) = &error {
        let status = response.status().as_u16();
        if matches!(status, 401 | 403) {
            return AlpacaError::InvalidAuthorization;
        }
        if matches!(status, 429 | 503) || status >= 500 {
            let retry_after = response
                .headers()
                .get(tokio_tungstenite::tungstenite::http::header::RETRY_AFTER)
                .map(|value| value.as_bytes());
            apply_refusal(budget, retry_after);
        }
    }
    AlpacaError::Network
}

async fn send_stream_message<S>(
    socket: &mut WebSocketStream<S>,
    message: Message,
    deadline: Instant,
    io_timeout: Duration,
    cancellation: &CancellationToken,
) -> Result<(), AlpacaError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let timeout = bounded_timeout(deadline, io_timeout, cancellation)?;
    tokio::select! {
        biased;
        () = cancellation.cancelled() => Err(AlpacaError::Cancelled),
        result = tokio::time::timeout(timeout, socket.send(message)) => match result {
            Ok(Ok(())) => Ok(()),
            Ok(Err(_)) => Err(AlpacaError::Network),
            Err(_) => Err(AlpacaError::DeadlineExceeded),
        }
    }
}

async fn read_stream_message<S>(
    socket: &mut WebSocketStream<S>,
    deadline: Instant,
    io_timeout: Duration,
    maximum_frame_bytes: usize,
    cancellation: &CancellationToken,
) -> Result<Message, AlpacaError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let timeout = bounded_timeout(deadline, io_timeout, cancellation)?;
    let next = tokio::select! {
        biased;
        () = cancellation.cancelled() => return Err(AlpacaError::Cancelled),
        result = tokio::time::timeout(timeout, socket.next()) => match result {
            Ok(value) => value,
            Err(_) => return Err(AlpacaError::DeadlineExceeded),
        }
    };
    let message = next
        .ok_or(AlpacaError::Network)?
        .map_err(|_| AlpacaError::Network)?;
    if message.len() > maximum_frame_bytes {
        return Err(AlpacaError::BodyTooLarge);
    }
    Ok(message)
}

async fn read_optional_close_message<S>(
    socket: &mut WebSocketStream<S>,
    deadline: Instant,
    io_timeout: Duration,
    maximum_frame_bytes: usize,
    cancellation: &CancellationToken,
) -> Result<Option<Message>, AlpacaError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let timeout = bounded_timeout(deadline, io_timeout, cancellation)?;
    let next = tokio::select! {
        biased;
        () = cancellation.cancelled() => return Err(AlpacaError::Cancelled),
        result = tokio::time::timeout(timeout, socket.next()) => match result {
            Ok(value) => value,
            Err(_) => return Ok(None),
        }
    };
    let Some(message) = next else {
        return Ok(None);
    };
    let message = message.map_err(|_| AlpacaError::Network)?;
    if message.len() > maximum_frame_bytes {
        return Err(AlpacaError::BodyTooLarge);
    }
    Ok(Some(message))
}

async fn control_payload<S>(
    message: Message,
    socket: &mut WebSocketStream<S>,
    deadline: Instant,
    io_timeout: Duration,
    cancellation: &CancellationToken,
) -> Result<Option<Box<[u8]>>, AlpacaError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    match message {
        Message::Text(text) => Ok(Some(text.as_bytes().to_vec().into_boxed_slice())),
        Message::Binary(payload) => Ok(Some(payload.to_vec().into_boxed_slice())),
        Message::Ping(payload) => {
            send_stream_message(
                socket,
                Message::Pong(payload),
                deadline,
                io_timeout,
                cancellation,
            )
            .await?;
            Ok(None)
        }
        Message::Pong(_) => Ok(None),
        Message::Close(_) | Message::Frame(_) => Err(AlpacaError::Protocol),
    }
}

fn parse_control_frame(payload: &[u8]) -> Result<ControlFacts, AlpacaError> {
    let values: Vec<Value> = serde_json::from_slice(payload).map_err(|_| AlpacaError::Protocol)?;
    if values.is_empty() || values.len() > 16 {
        return Err(AlpacaError::Protocol);
    }
    let mut facts = ControlFacts::default();
    for value in values {
        let object = value.as_object().ok_or(AlpacaError::Protocol)?;
        match object.get("T").and_then(Value::as_str) {
            Some("success") => match object.get("msg").and_then(Value::as_str) {
                Some("connected") if !facts.connected => facts.connected = true,
                Some("authenticated") if !facts.authenticated => facts.authenticated = true,
                _ => return Err(AlpacaError::Protocol),
            },
            Some("subscription") => {
                if facts.subscription.is_some() {
                    return Err(AlpacaError::Protocol);
                }
                let trades = exact_aapl_subscription(object.get("trades"))?;
                let quotes = exact_aapl_subscription(object.get("quotes"))?;
                for key in [
                    "bars",
                    "updatedBars",
                    "dailyBars",
                    "statuses",
                    "lulds",
                    "corrections",
                    "cancelErrors",
                ] {
                    if object
                        .get(key)
                        .and_then(Value::as_array)
                        .is_some_and(|values| !values.is_empty())
                    {
                        return Err(AlpacaError::Protocol);
                    }
                }
                facts.subscription = Some((trades, quotes));
            }
            Some("error") => {
                if facts.error_code.is_some() {
                    return Err(AlpacaError::Protocol);
                }
                facts.error_code = Some(
                    object
                        .get("code")
                        .and_then(Value::as_u64)
                        .ok_or(AlpacaError::Protocol)?,
                );
            }
            _ => return Err(AlpacaError::Protocol),
        }
    }
    Ok(facts)
}

fn exact_aapl_subscription(value: Option<&Value>) -> Result<u32, AlpacaError> {
    let values = value
        .and_then(Value::as_array)
        .ok_or(AlpacaError::Protocol)?;
    if values.len() != 1 || values[0].as_str() != Some(DOCTOR_SYMBOL) {
        return Err(AlpacaError::Protocol);
    }
    Ok(1)
}

fn add_frame_bytes(
    frames_observed: &mut u32,
    bytes_observed: &mut u64,
    bytes: usize,
) -> Result<(), AlpacaError> {
    *frames_observed = frames_observed
        .checked_add(1)
        .ok_or(AlpacaError::Protocol)?;
    *bytes_observed = bytes_observed
        .checked_add(u64::try_from(bytes).map_err(|_| AlpacaError::Protocol)?)
        .ok_or(AlpacaError::Protocol)?;
    Ok(())
}

fn bounded_timeout(
    deadline: Instant,
    operation: Duration,
    cancellation: &CancellationToken,
) -> Result<Duration, AlpacaError> {
    ensure_before(deadline, cancellation)?;
    let remaining = deadline
        .checked_duration_since(Instant::now())
        .ok_or(AlpacaError::DeadlineExceeded)?;
    let timeout = remaining.min(operation);
    if timeout.is_zero() {
        return Err(AlpacaError::DeadlineExceeded);
    }
    Ok(timeout)
}

fn ensure_before(deadline: Instant, cancellation: &CancellationToken) -> Result<(), AlpacaError> {
    if cancellation.is_cancelled() {
        Err(AlpacaError::Cancelled)
    } else if Instant::now() >= deadline {
        Err(AlpacaError::DeadlineExceeded)
    } else {
        Ok(())
    }
}

fn system_timestamp() -> Result<Timestamp, AlpacaError> {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| AlpacaError::Network)?
        .as_nanos();
    Ok(Timestamp::from_unix_nanos(
        i64::try_from(nanos).map_err(|_| AlpacaError::Network)?,
    ))
}

fn exact_url(value: &str) -> Result<Url, AlpacaError> {
    let url = Url::parse(value).map_err(|_| AlpacaError::Protocol)?;
    if url.scheme() != "https"
        || url.username() != ""
        || url.password().is_some()
        || url.fragment().is_some()
    {
        return Err(AlpacaError::Protocol);
    }
    Ok(url)
}

fn endpoint_contract_digest(
    contract: DoctorEndpointContract,
) -> Result<EvidenceDigest, AlpacaError> {
    let mut digest = Sha256::new();
    digest.update(b"market-squawk/alpaca-paper-iex-doctor-endpoint-contract/v1\0");
    let contract = match contract {
        DoctorEndpointContract::Quote => {
            "GET https://data.alpaca.markets/v2/stocks/AAPL/quotes/latest?feed=iex"
        }
        DoctorEndpointContract::Batch => {
            "GET https://data.alpaca.markets/v2/stocks/snapshots?symbols=<code-owned-50>&feed=iex"
        }
        DoctorEndpointContract::Stream => {
            "WSS wss://stream.data.alpaca.markets/v2/iex;header-auth;subscribe=AAPL-trades+quotes;close"
        }
        DoctorEndpointContract::Historical => {
            "GET https://data.alpaca.markets/v2/stocks/AAPL/bars?timeframe=1Day&start=<utc>&end=<utc>&limit=1000&adjustment=raw&feed=iex&sort=asc&page_token=<optional>"
        }
        DoctorEndpointContract::Calendar => {
            "GET https://paper-api.alpaca.markets/v3/calendar/IEX?start=<date>&end=<date>&timezone=UTC"
        }
    };
    hash_text(&mut digest, contract)?;
    Ok(evidence(digest.finalize().into()))
}

fn request_digest(method: &str, url: &str) -> Result<EvidenceDigest, AlpacaError> {
    let mut digest = Sha256::new();
    digest.update(b"market-squawk/alpaca-paper-iex-doctor-http-request/v1\0");
    hash_text(&mut digest, method)?;
    hash_text(&mut digest, url)?;
    Ok(evidence(digest.finalize().into()))
}

fn stream_request_digest() -> Result<EvidenceDigest, AlpacaError> {
    let mut digest = Sha256::new();
    digest.update(b"market-squawk/alpaca-paper-iex-doctor-stream-request/v1\0");
    hash_text(&mut digest, STREAM_ENDPOINT)?;
    hash_text(&mut digest, "header-auth")?;
    hash_text(&mut digest, STREAM_SUBSCRIPTION)?;
    Ok(evidence(digest.finalize().into()))
}

fn historical_request_digest(
    start_date: CalendarDate,
    end_date: CalendarDate,
) -> Result<EvidenceDigest, AlpacaError> {
    let mut digest = Sha256::new();
    digest.update(b"market-squawk/alpaca-paper-iex-doctor-historical-request/v1\0");
    hash_evidence(
        &mut digest,
        endpoint_contract_digest(DoctorEndpointContract::Historical)?,
    );
    hash_date(&mut digest, start_date);
    hash_date(&mut digest, end_date);
    Ok(evidence(digest.finalize().into()))
}

#[allow(
    clippy::too_many_arguments,
    reason = "closed quote fields stay explicit"
)]
fn quote_semantic_digest(
    disposition: AlpacaDoctorObservationDisposition,
    timestamp: Option<Timestamp>,
    bid_price: Option<Decimal>,
    ask_price: Option<Decimal>,
    bid_size: Option<u64>,
    ask_size: Option<u64>,
) -> Result<EvidenceDigest, AlpacaError> {
    let mut digest = Sha256::new();
    digest.update(b"market-squawk/alpaca-paper-iex-doctor-quote-result/v1\0");
    digest.update([disposition_tag(disposition)]);
    hash_optional_timestamp(&mut digest, timestamp);
    hash_optional_decimal(&mut digest, bid_price)?;
    hash_optional_decimal(&mut digest, ask_price)?;
    hash_optional_u64(&mut digest, bid_size);
    hash_optional_u64(&mut digest, ask_size);
    Ok(evidence(digest.finalize().into()))
}

#[allow(
    clippy::too_many_arguments,
    reason = "closed batch fields stay explicit"
)]
fn batch_semantic_digest(
    disposition: AlpacaDoctorObservationDisposition,
    returned_count: u32,
    missing_count: u32,
    unexpected_count: u32,
    duplicate_count: u32,
    invalid_count: u32,
    effective_cardinality: u32,
    requested_symbols_digest: EvidenceDigest,
    returned_symbols_digest: EvidenceDigest,
    missing_symbols_digest: EvidenceDigest,
    unexpected_symbols_digest: EvidenceDigest,
) -> EvidenceDigest {
    let mut digest = Sha256::new();
    digest.update(b"market-squawk/alpaca-paper-iex-doctor-batch-result/v1\0");
    digest.update([disposition_tag(disposition)]);
    for count in [
        ALPACA_PAPER_IEX_DOCTOR_BATCH_SYMBOL_COUNT as u32,
        returned_count,
        missing_count,
        unexpected_count,
        duplicate_count,
        invalid_count,
        effective_cardinality,
    ] {
        digest.update(count.to_be_bytes());
    }
    for evidence_digest in [
        requested_symbols_digest,
        returned_symbols_digest,
        missing_symbols_digest,
        unexpected_symbols_digest,
    ] {
        hash_evidence(&mut digest, evidence_digest);
    }
    evidence(digest.finalize().into())
}

#[allow(
    clippy::too_many_arguments,
    reason = "closed stream fields stay explicit"
)]
fn stream_semantic_digest(
    disposition: AlpacaDoctorObservationDisposition,
    handshake_status: u16,
    handshake_rate: &AlpacaDoctorRateEvidence,
    connected_frame_digest: EvidenceDigest,
    authenticated_frame_digest: EvidenceDigest,
    subscription_frame_digest: EvidenceDigest,
    subscribed_trade_count: u32,
    subscribed_quote_count: u32,
    frames_observed: u32,
    bytes_observed: u64,
    authenticated_at: Timestamp,
    subscribed_at: Timestamp,
    close_sent: bool,
    clean_close_observed: bool,
    completed_at: Timestamp,
) -> EvidenceDigest {
    let mut digest = Sha256::new();
    digest.update(b"market-squawk/alpaca-paper-iex-doctor-stream-result/v1\0");
    digest.update([disposition_tag(disposition)]);
    digest.update(handshake_status.to_be_bytes());
    hash_rate(&mut digest, handshake_rate);
    for evidence_digest in [
        connected_frame_digest,
        authenticated_frame_digest,
        subscription_frame_digest,
    ] {
        hash_evidence(&mut digest, evidence_digest);
    }
    digest.update(subscribed_trade_count.to_be_bytes());
    digest.update(subscribed_quote_count.to_be_bytes());
    digest.update(frames_observed.to_be_bytes());
    digest.update(bytes_observed.to_be_bytes());
    digest.update(authenticated_at.unix_nanos().to_be_bytes());
    digest.update(subscribed_at.unix_nanos().to_be_bytes());
    digest.update([u8::from(close_sent), u8::from(clean_close_observed)]);
    digest.update(completed_at.unix_nanos().to_be_bytes());
    evidence(digest.finalize().into())
}

#[allow(
    clippy::too_many_arguments,
    reason = "closed history fields stay explicit"
)]
fn historical_semantic_digest(
    disposition: AlpacaDoctorObservationDisposition,
    start_date: CalendarDate,
    end_date: CalendarDate,
    page_count: u32,
    returned_bar_count: u32,
    distinct_date_count: u32,
    first_bar_timestamp: Option<Timestamp>,
    last_bar_timestamp: Option<Timestamp>,
    returned_dates_digest: EvidenceDigest,
    pagination_graph_digest: EvidenceDigest,
    terminal_page_observed: bool,
) -> EvidenceDigest {
    let mut digest = Sha256::new();
    digest.update(b"market-squawk/alpaca-paper-iex-doctor-history-result/v1\0");
    digest.update([disposition_tag(disposition)]);
    hash_date(&mut digest, start_date);
    hash_date(&mut digest, end_date);
    digest.update(page_count.to_be_bytes());
    digest.update(returned_bar_count.to_be_bytes());
    digest.update(distinct_date_count.to_be_bytes());
    hash_optional_timestamp(&mut digest, first_bar_timestamp);
    hash_optional_timestamp(&mut digest, last_bar_timestamp);
    hash_evidence(&mut digest, returned_dates_digest);
    hash_evidence(&mut digest, pagination_graph_digest);
    digest.update([u8::from(terminal_page_observed)]);
    evidence(digest.finalize().into())
}

#[allow(
    clippy::too_many_arguments,
    reason = "closed calendar fields stay explicit"
)]
fn calendar_semantic_digest(
    disposition: AlpacaDoctorObservationDisposition,
    start_date: CalendarDate,
    end_date: CalendarDate,
    session_count: u32,
    history_date_count: u32,
    matched_count: u32,
    missing_history_count: u32,
    unexpected_history_count: u32,
    session_dates_digest: EvidenceDigest,
    history_dates_digest: EvidenceDigest,
    exact_date_reconciliation: bool,
) -> EvidenceDigest {
    let mut digest = Sha256::new();
    digest.update(b"market-squawk/alpaca-paper-iex-doctor-calendar-result/v1\0");
    digest.update([disposition_tag(disposition)]);
    hash_date(&mut digest, start_date);
    hash_date(&mut digest, end_date);
    for count in [
        session_count,
        history_date_count,
        matched_count,
        missing_history_count,
        unexpected_history_count,
    ] {
        digest.update(count.to_be_bytes());
    }
    hash_evidence(&mut digest, session_dates_digest);
    hash_evidence(&mut digest, history_dates_digest);
    digest.update([u8::from(exact_date_reconciliation)]);
    evidence(digest.finalize().into())
}

fn token_digest(value: &str) -> EvidenceDigest {
    let mut digest = Sha256::new();
    digest.update(b"market-squawk/alpaca-paper-iex-doctor-page-token/v1\0");
    digest.update(value.as_bytes());
    evidence(digest.finalize().into())
}

fn symbol_set_digest<'a>(
    symbols: impl Iterator<Item = &'a str>,
) -> Result<EvidenceDigest, AlpacaError> {
    let mut digest = Sha256::new();
    digest.update(b"market-squawk/alpaca-paper-iex-doctor-symbol-set/v1\0");
    for symbol in symbols {
        validate_symbol(symbol)?;
        hash_text(&mut digest, symbol)?;
    }
    Ok(evidence(digest.finalize().into()))
}

fn date_set_digest(
    dates: impl Iterator<Item = CalendarDate>,
) -> Result<EvidenceDigest, AlpacaError> {
    let mut digest = Sha256::new();
    digest.update(b"market-squawk/alpaca-paper-iex-doctor-date-set/v1\0");
    for date in dates {
        digest.update(date.year().to_be_bytes());
        digest.update([date.month(), date.day()]);
    }
    Ok(evidence(digest.finalize().into()))
}

fn pagination_graph_digest(
    pages: &[AlpacaDoctorHttpPageEvidence],
    terminal: bool,
) -> Result<EvidenceDigest, AlpacaError> {
    let mut digest = Sha256::new();
    digest.update(b"market-squawk/alpaca-paper-iex-doctor-pagination-graph/v1\0");
    digest.update([u8::from(terminal)]);
    digest.update(u32_count(pages.len())?.to_be_bytes());
    for page in pages {
        hash_http(&mut digest, &page.http);
        hash_optional_evidence(&mut digest, page.request_page_token_digest);
        hash_optional_evidence(&mut digest, page.response_page_token_digest);
    }
    Ok(evidence(digest.finalize().into()))
}

fn doctor_observation_digest(
    origin: AlpacaDoctorObservationOrigin,
    market_data_principal_sha256: EvidenceDigest,
    quote: &AlpacaDoctorQuoteObservation,
    batch: &AlpacaDoctorBatchObservation,
    stream: &AlpacaDoctorStreamObservation,
    historical: &AlpacaDoctorHistoricalObservation,
    calendar: &AlpacaDoctorCalendarObservation,
    completed_at: Timestamp,
) -> EvidenceDigest {
    let mut digest = Sha256::new();
    digest.update(b"market-squawk/alpaca-paper-iex-doctor-observation/v3\0");
    digest.update([origin_tag(origin)]);
    hash_evidence(&mut digest, market_data_principal_sha256);
    digest.update([disposition_tag(quote.disposition)]);
    hash_http(&mut digest, &quote.http);
    hash_evidence(&mut digest, quote.semantic_result_digest);
    digest.update([disposition_tag(batch.disposition)]);
    hash_http(&mut digest, &batch.http);
    hash_evidence(&mut digest, batch.semantic_result_digest);
    for value in [
        batch.returned_count,
        batch.missing_count,
        batch.unexpected_count,
        batch.duplicate_count,
        batch.invalid_count,
        batch.effective_cardinality,
    ] {
        digest.update(value.to_be_bytes());
    }
    for value in [
        batch.requested_symbols_digest,
        batch.returned_symbols_digest,
        batch.missing_symbols_digest,
        batch.unexpected_symbols_digest,
        stream.endpoint_contract_digest,
        stream.request_digest,
        stream.connected_frame_digest,
        stream.authenticated_frame_digest,
        stream.subscription_frame_digest,
        stream.semantic_result_digest,
        historical.endpoint_contract_digest,
        historical.request_digest,
        historical.semantic_result_digest,
        historical.pagination_graph_digest,
        historical.returned_dates_digest,
        calendar.semantic_result_digest,
        calendar.session_dates_digest,
        calendar.history_dates_digest,
    ] {
        hash_evidence(&mut digest, value);
    }
    digest.update([disposition_tag(stream.disposition)]);
    digest.update(stream.handshake_status.to_be_bytes());
    hash_rate(&mut digest, &stream.handshake_rate);
    digest.update(stream.subscribed_trade_count.to_be_bytes());
    digest.update(stream.subscribed_quote_count.to_be_bytes());
    digest.update(stream.frames_observed.to_be_bytes());
    digest.update(stream.bytes_observed.to_be_bytes());
    digest.update(stream.authenticated_at.unix_nanos().to_be_bytes());
    digest.update(stream.subscribed_at.unix_nanos().to_be_bytes());
    digest.update([
        u8::from(stream.close_sent),
        u8::from(stream.clean_close_observed),
    ]);
    digest.update(stream.completed_at.unix_nanos().to_be_bytes());
    digest.update([disposition_tag(historical.disposition)]);
    hash_date(&mut digest, historical.start_date);
    hash_date(&mut digest, historical.end_date);
    digest.update(historical.page_count.to_be_bytes());
    digest.update(historical.returned_bar_count.to_be_bytes());
    digest.update(historical.distinct_date_count.to_be_bytes());
    hash_optional_timestamp(&mut digest, historical.first_bar_timestamp);
    hash_optional_timestamp(&mut digest, historical.last_bar_timestamp);
    digest.update([u8::from(historical.terminal_page_observed)]);
    digest.update([disposition_tag(calendar.disposition)]);
    hash_http(&mut digest, &calendar.http);
    hash_date(&mut digest, calendar.start_date);
    hash_date(&mut digest, calendar.end_date);
    digest.update(calendar.session_count.to_be_bytes());
    digest.update(calendar.history_date_count.to_be_bytes());
    digest.update(calendar.matched_count.to_be_bytes());
    digest.update(calendar.missing_history_count.to_be_bytes());
    digest.update(calendar.unexpected_history_count.to_be_bytes());
    digest.update([u8::from(calendar.exact_date_reconciliation)]);
    for disposition in [
        AlpacaDoctorObservationDisposition::Unprobed,
        AlpacaDoctorObservationDisposition::Unprobed,
        AlpacaDoctorObservationDisposition::Unprobed,
        AlpacaDoctorObservationDisposition::Unprobed,
        AlpacaDoctorObservationDisposition::Unsupported,
        AlpacaDoctorObservationDisposition::Unsupported,
        AlpacaDoctorObservationDisposition::Unsupported,
        AlpacaDoctorObservationDisposition::Unsupported,
        AlpacaDoctorObservationDisposition::Unsupported,
        AlpacaDoctorObservationDisposition::Unsupported,
        AlpacaDoctorObservationDisposition::Unsupported,
        AlpacaDoctorObservationDisposition::Unsupported,
        AlpacaDoctorObservationDisposition::Unsupported,
    ] {
        digest.update([disposition_tag(disposition)]);
    }
    digest.update(completed_at.unix_nanos().to_be_bytes());
    evidence(digest.finalize().into())
}

#[cfg(feature = "scripted-transport-fixture")]
fn fixture_market_data_principal_sha256() -> EvidenceDigest {
    let mut digest = Sha256::new();
    digest.update(b"market-squawk/alpaca-paper-iex-doctor-installed-fixture-principal/v1\0");
    evidence(digest.finalize().into())
}

fn hash_http(digest: &mut Sha256, value: &AlpacaDoctorHttpEvidence) {
    hash_evidence(digest, value.endpoint_contract_digest);
    hash_evidence(digest, value.request_digest);
    digest.update(value.status_code.to_be_bytes());
    hash_evidence(digest, value.body_digest);
    digest.update(value.response_bytes.to_be_bytes());
    digest.update(value.received_at.unix_nanos().to_be_bytes());
    digest.update(value.latency_nanos.to_be_bytes());
    hash_rate(digest, &value.rate);
}

fn hash_rate(digest: &mut Sha256, value: &AlpacaDoctorRateEvidence) {
    hash_observed_u32(digest, &value.limit);
    hash_observed_u32(digest, &value.remaining);
    match &value.reset_unix_seconds {
        AlpacaDoctorObservedField::Observed(reset) => {
            digest.update([1]);
            digest.update(reset.to_be_bytes());
        }
        AlpacaDoctorObservedField::Missing => digest.update([0]),
    }
    match &value.retry_after {
        AlpacaDoctorObservedField::Observed(AlpacaDoctorRetryAfter::DelaySeconds(seconds)) => {
            digest.update([1]);
            digest.update(seconds.to_be_bytes());
        }
        AlpacaDoctorObservedField::Observed(AlpacaDoctorRetryAfter::AtUnixSeconds(seconds)) => {
            digest.update([2]);
            digest.update(seconds.to_be_bytes());
        }
        AlpacaDoctorObservedField::Missing => digest.update([0]),
    }
}

fn hash_observed_u32(digest: &mut Sha256, value: &AlpacaDoctorObservedField<u32>) {
    match value {
        AlpacaDoctorObservedField::Observed(value) => {
            digest.update([1]);
            digest.update(value.to_be_bytes());
        }
        AlpacaDoctorObservedField::Missing => digest.update([0]),
    }
}

fn hash_optional_evidence(digest: &mut Sha256, value: Option<EvidenceDigest>) {
    match value {
        Some(value) => {
            digest.update([1]);
            hash_evidence(digest, value);
        }
        None => digest.update([0]),
    }
}

fn hash_optional_timestamp(digest: &mut Sha256, value: Option<Timestamp>) {
    match value {
        Some(value) => {
            digest.update([1]);
            digest.update(value.unix_nanos().to_be_bytes());
        }
        None => digest.update([0]),
    }
}

fn hash_optional_decimal(digest: &mut Sha256, value: Option<Decimal>) -> Result<(), AlpacaError> {
    match value {
        Some(value) => {
            digest.update([1]);
            hash_text(digest, &value.normalize().to_string())?;
        }
        None => digest.update([0]),
    }
    Ok(())
}

fn hash_optional_u64(digest: &mut Sha256, value: Option<u64>) {
    match value {
        Some(value) => {
            digest.update([1]);
            digest.update(value.to_be_bytes());
        }
        None => digest.update([0]),
    }
}

fn hash_date(digest: &mut Sha256, value: CalendarDate) {
    digest.update(value.year().to_be_bytes());
    digest.update([value.month(), value.day()]);
}

fn hash_evidence(digest: &mut Sha256, value: EvidenceDigest) {
    digest.update([match value.algorithm() {
        DigestAlgorithm::Sha256 => 1,
        DigestAlgorithm::Blake3 => 2,
    }]);
    digest.update(value.bytes());
}

fn hash_text(digest: &mut Sha256, value: &str) -> Result<(), AlpacaError> {
    digest.update(
        u32::try_from(value.len())
            .map_err(|_| AlpacaError::Protocol)?
            .to_be_bytes(),
    );
    digest.update(value.as_bytes());
    Ok(())
}

fn sha256(value: &[u8]) -> EvidenceDigest {
    evidence(Sha256::digest(value).into())
}

const fn evidence(bytes: [u8; 32]) -> EvidenceDigest {
    EvidenceDigest::new(DigestAlgorithm::Sha256, bytes)
}

fn u32_count(value: usize) -> Result<u32, AlpacaError> {
    u32::try_from(value).map_err(|_| AlpacaError::Protocol)
}

const fn disposition_tag(value: AlpacaDoctorObservationDisposition) -> u8 {
    match value {
        AlpacaDoctorObservationDisposition::ObservedAvailable => 1,
        AlpacaDoctorObservationDisposition::ObservedDegraded => 2,
        AlpacaDoctorObservationDisposition::ObservedUnavailable => 3,
        AlpacaDoctorObservationDisposition::Unprobed => 4,
        AlpacaDoctorObservationDisposition::Unsupported => 5,
    }
}

const fn origin_tag(value: AlpacaDoctorObservationOrigin) -> u8 {
    match value {
        AlpacaDoctorObservationOrigin::ProviderObserved => 1,
        AlpacaDoctorObservationOrigin::InstalledFixture => 2,
    }
}
