//! Single source-native calendar grammar and physically retained session replay.
//!
//! Wire values and parsing helpers are reconstruction data. Only the sealed replay map below
//! binds an exact request and verified capture. Account, rights, creating publication, and PIT
//! admission remain with the caller's existing authority owners.

use crate::{
    ALPACA_HISTORICAL_CALENDAR_MAX_RESPONSE_BYTES, ALPACA_HISTORICAL_MAX_LOOKBACK_DAYS,
    AlpacaAuthenticatedCalendarRequest, AlpacaCalendarMarket,
};
use chrono::{DateTime, Datelike as _, LocalResult, NaiveDate, TimeZone as _, Utc};
use chrono_tz::{America::New_York, IANA_TZDB_VERSION};
use market_squawk_domain::{CalendarDate, DigestAlgorithm, EvidenceDigest, Timestamp};
use market_squawk_platform::{
    ResearchObjectControl, ResearchObjectControlError, ResearchObjectControlPoint,
    SealedResearchJournalSegment,
};
use market_squawk_sources::{
    ProviderCaptureSetReceipt, ProviderCaptureTerminalDisposition, SealedProviderCaptureSetReceipt,
};
use serde::{
    Deserialize, Deserializer,
    de::{SeqAccess, Visitor},
};
use sha2::{Digest as _, Sha256};
use std::fmt;
use thiserror::Error;

const ALPACA_MARKET_TIME_ZONE: &str = "America/New_York";
const MAXIMUM_ALPACA_MARKET_NAME_BYTES: usize = 256;
const MAXIMUM_CALENDAR_ROWS: usize = ALPACA_HISTORICAL_MAX_LOOKBACK_DAYS as usize + 1;
/// Existing source-specific daily aggregation rule, independent of native core endpoints.
pub const ALPACA_IEX_DAILY_AGGREGATION_RULE: &[u8] = b"market-squawk/alpaca-v3-iex-utc-daily/v1\0provider-timestamp=period-start\0period=America/New_York-civil-day\0session=provider-defined\0";

/// Failure to replay exact retained native source evidence under caller control.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum AlpacaCalendarDecodeError {
    /// The source bytes, request, or physical capture do not reconcile exactly.
    #[error("Alpaca calendar retained evidence is invalid")]
    InvalidResponse,
    /// Native market identity does not match the exact requested market.
    #[error("Alpaca calendar market identity is unavailable")]
    UnknownMarketIdentity,
    /// Exact versioned New York midnight resolution is unavailable.
    #[error("Alpaca calendar timezone rules are unavailable")]
    TimeZoneRulesUnavailable,
    /// The bounded decoded allocation could not be admitted.
    #[error("Alpaca calendar replay resource bound exceeded")]
    ResourceBoundExceeded,
    /// The caller cancelled, expired, or revoked trusted operation control.
    #[error(transparent)]
    Control(#[from] ResearchObjectControlError),
}

/// Source-native market metadata, before source identity validation.
#[derive(Debug, Deserialize)]
pub struct AlpacaMarketWire {
    /// Exact provider field; this wire value is not authority.
    pub acronym: String,
    /// Exact provider field; this wire value is not authority.
    pub name: String,
    /// Exact provider field; this wire value is not authority.
    pub timezone: String,
    /// Exact provider field; this wire value is not authority.
    #[serde(default)]
    pub bic: OptionalWire<String>,
    /// Exact provider field; this wire value is not authority.
    #[serde(default)]
    pub mic: OptionalWire<String>,
}

/// Source-native day fields, before request and interval validation.
#[derive(Clone, Debug, Deserialize)]
pub struct AlpacaCalendarDayWire {
    /// Exact provider field; this wire value is not authority.
    pub date: String,
    /// Exact provider field; this wire value is not authority.
    pub core_start: String,
    /// Exact provider field; this wire value is not authority.
    pub core_end: String,
    /// Exact provider field; this wire value is not authority.
    #[serde(default)]
    pub pre_start: OptionalWire<String>,
    /// Exact provider field; this wire value is not authority.
    #[serde(default)]
    pub pre_end: OptionalWire<String>,
    /// Exact provider field; this wire value is not authority.
    #[serde(default)]
    pub post_start: OptionalWire<String>,
    /// Exact provider field; this wire value is not authority.
    #[serde(default)]
    pub post_end: OptionalWire<String>,
    /// Exact provider field; this wire value is not authority.
    #[serde(default)]
    pub lunch_start: OptionalWire<String>,
    /// Exact provider field; this wire value is not authority.
    #[serde(default)]
    pub lunch_end: OptionalWire<String>,
    /// Exact provider field; this wire value is not authority.
    #[serde(default)]
    pub settlement_date: OptionalWire<String>,
}

/// Retains absent fields distinctly; explicit null is rejected by the native grammar.
#[derive(Clone, Debug)]
pub enum OptionalWire<T> {
    /// The source omitted this field.
    Missing,
    /// The source supplied this exact value.
    Present(T),
}

impl<T> Default for OptionalWire<T> {
    fn default() -> Self {
        Self::Missing
    }
}

impl<'de, T> Deserialize<'de> for OptionalWire<T>
where
    T: Deserialize<'de>,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        T::deserialize(deserializer).map(Self::Present)
    }
}

impl OptionalWire<String> {
    /// Borrows the exact reported string, retaining absent fields.
    pub fn as_deref(&self) -> Option<&str> {
        match self {
            Self::Missing => None,
            Self::Present(value) => Some(value),
        }
    }
}

/// Validated reported half-open interval, distinct from provider daily aggregation.
#[derive(Clone, Copy, Debug)]
pub struct ExactInterval {
    /// Validated native endpoint or interval.
    pub start: Timestamp,
    /// Validated native endpoint or interval.
    pub end: Timestamp,
}

impl ExactInterval {
    fn try_new(start: Timestamp, end: Timestamp) -> Result<Self, AlpacaCalendarDecodeError> {
        if start >= end {
            return Err(AlpacaCalendarDecodeError::InvalidResponse);
        }
        Ok(Self { start, end })
    }

    /// Builds an application partition from endpoints already validated by the source parser.
    pub const fn new_unchecked(start: Timestamp, end: Timestamp) -> Self {
        Self { start, end }
    }
}

/// Parsed native sessions for one exact calendar date.
#[derive(Debug)]
pub struct ParsedCalendarDay {
    /// Validated native endpoint or interval.
    pub core: ExactInterval,
    /// Validated native endpoint or interval.
    pub pre: Option<ExactInterval>,
    /// Validated native endpoint or interval.
    pub post: Option<ExactInterval>,
    /// Validated native endpoint or interval.
    pub lunch: Option<ExactInterval>,
    /// Validated native endpoint or interval.
    pub settlement_date: Option<CalendarDate>,
}

/// Replays the existing exact native calendar grammar; no account authority is conferred.
pub fn validate_market_identity(
    market: &AlpacaMarketWire,
) -> Result<(), AlpacaCalendarDecodeError> {
    validate_market_identity_for(market, AlpacaCalendarMarket::Iex)
}

/// Replays the existing exact native calendar grammar; no account authority is conferred.
pub fn validate_market_identity_for(
    market: &AlpacaMarketWire,
    requested: AlpacaCalendarMarket,
) -> Result<(), AlpacaCalendarDecodeError> {
    if market.acronym != requested.acronym()
        || market.timezone != ALPACA_MARKET_TIME_ZONE
        || market.name.is_empty()
        || market.name.len() > MAXIMUM_ALPACA_MARKET_NAME_BYTES
        || market.name.trim() != market.name
        || market.name.chars().any(char::is_control)
    {
        return Err(AlpacaCalendarDecodeError::UnknownMarketIdentity);
    }
    if requested != AlpacaCalendarMarket::Iex
        && market
            .mic
            .as_deref()
            .is_some_and(|mic| mic != requested.mic())
    {
        return Err(AlpacaCalendarDecodeError::UnknownMarketIdentity);
    }
    if market
        .mic
        .as_deref()
        .is_some_and(|value| !is_upper_alphanumeric(value, 4))
        || market
            .bic
            .as_deref()
            .is_some_and(|value| !is_upper_alphanumeric(value, 11))
    {
        return Err(AlpacaCalendarDecodeError::InvalidResponse);
    }
    Ok(())
}

fn is_upper_alphanumeric(value: &str, exact_length: usize) -> bool {
    value.len() == exact_length
        && value
            .bytes()
            .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit())
}

/// Replays the existing exact native calendar grammar; no account authority is conferred.
pub fn parse_calendar_day(
    day: AlpacaCalendarDayWire,
    response_date: CalendarDate,
    period_start: Timestamp,
    period_end_exclusive: Timestamp,
) -> Result<ParsedCalendarDay, AlpacaCalendarDecodeError> {
    let AlpacaCalendarDayWire {
        date: _,
        core_start,
        core_end,
        pre_start,
        pre_end,
        post_start,
        post_end,
        lunch_start,
        lunch_end,
        settlement_date,
    } = day;
    let core = ExactInterval::try_new(
        parse_utc_timestamp(&core_start)?,
        parse_utc_timestamp(&core_end)?,
    )?;
    let pre = parse_optional_interval(pre_start, pre_end)?;
    let post = parse_optional_interval(post_start, post_end)?;
    let lunch = parse_optional_interval(lunch_start, lunch_end)?;
    let settlement_date = match settlement_date {
        OptionalWire::Missing => None,
        OptionalWire::Present(value) => Some(parse_calendar_date(&value)?),
    };
    if settlement_date.is_some_and(|settlement_date| settlement_date < response_date) {
        return Err(AlpacaCalendarDecodeError::InvalidResponse);
    }
    for interval in [Some(core), pre, post, lunch].into_iter().flatten() {
        if interval.start < period_start || interval.end > period_end_exclusive {
            return Err(AlpacaCalendarDecodeError::InvalidResponse);
        }
    }
    if pre.is_some_and(|interval| interval.end > core.start)
        || post.is_some_and(|interval| interval.start < core.end)
        || lunch.is_some_and(|interval| interval.start <= core.start || interval.end >= core.end)
    {
        return Err(AlpacaCalendarDecodeError::InvalidResponse);
    }
    Ok(ParsedCalendarDay {
        core,
        pre,
        post,
        lunch,
        settlement_date,
    })
}

fn parse_optional_interval(
    start: OptionalWire<String>,
    end: OptionalWire<String>,
) -> Result<Option<ExactInterval>, AlpacaCalendarDecodeError> {
    match (start, end) {
        (OptionalWire::Missing, OptionalWire::Missing) => Ok(None),
        (OptionalWire::Present(start), OptionalWire::Present(end)) => {
            ExactInterval::try_new(parse_utc_timestamp(&start)?, parse_utc_timestamp(&end)?)
                .map(Some)
        }
        (OptionalWire::Missing, OptionalWire::Present(_))
        | (OptionalWire::Present(_), OptionalWire::Missing) => {
            Err(AlpacaCalendarDecodeError::InvalidResponse)
        }
    }
}

/// Replays the existing exact native calendar grammar; no account authority is conferred.
pub fn parse_calendar_date(value: &str) -> Result<CalendarDate, AlpacaCalendarDecodeError> {
    let bytes = value.as_bytes();
    if bytes.len() != 10
        || bytes[4] != b'-'
        || bytes[7] != b'-'
        || bytes
            .iter()
            .enumerate()
            .any(|(index, byte)| index != 4 && index != 7 && !byte.is_ascii_digit())
    {
        return Err(AlpacaCalendarDecodeError::InvalidResponse);
    }
    let year = value[0..4]
        .parse::<u16>()
        .map_err(|_| AlpacaCalendarDecodeError::InvalidResponse)?;
    let month = value[5..7]
        .parse::<u8>()
        .map_err(|_| AlpacaCalendarDecodeError::InvalidResponse)?;
    let day = value[8..10]
        .parse::<u8>()
        .map_err(|_| AlpacaCalendarDecodeError::InvalidResponse)?;
    CalendarDate::new(year, month, day).map_err(|_| AlpacaCalendarDecodeError::InvalidResponse)
}

/// Replays the existing exact native calendar grammar; no account authority is conferred.
pub fn parse_utc_timestamp(value: &str) -> Result<Timestamp, AlpacaCalendarDecodeError> {
    if value.len() > 64
        || value.as_bytes().get(10) != Some(&b'T')
        || !(value.ends_with('Z') || value.ends_with("+00:00"))
    {
        return Err(AlpacaCalendarDecodeError::InvalidResponse);
    }
    let parsed = DateTime::parse_from_rfc3339(value)
        .map_err(|_| AlpacaCalendarDecodeError::InvalidResponse)?;
    if parsed.offset().local_minus_utc() != 0 {
        return Err(AlpacaCalendarDecodeError::InvalidResponse);
    }
    parsed
        .timestamp_nanos_opt()
        .map(Timestamp::from_unix_nanos)
        .ok_or(AlpacaCalendarDecodeError::InvalidResponse)
}

/// Replays the existing exact native calendar grammar; no account authority is conferred.
pub fn new_york_civil_day(
    date: CalendarDate,
) -> Result<(CalendarDate, Timestamp, Timestamp), AlpacaCalendarDecodeError> {
    let naive = NaiveDate::from_ymd_opt(
        i32::from(date.year()),
        u32::from(date.month()),
        u32::from(date.day()),
    )
    .ok_or(AlpacaCalendarDecodeError::InvalidResponse)?;
    let next = naive
        .succ_opt()
        .ok_or(AlpacaCalendarDecodeError::TimeZoneRulesUnavailable)?;
    let next_year = u16::try_from(next.year())
        .map_err(|_| AlpacaCalendarDecodeError::TimeZoneRulesUnavailable)?;
    let next_date = CalendarDate::new(
        next_year,
        u8::try_from(next.month())
            .map_err(|_| AlpacaCalendarDecodeError::TimeZoneRulesUnavailable)?,
        u8::try_from(next.day())
            .map_err(|_| AlpacaCalendarDecodeError::TimeZoneRulesUnavailable)?,
    )
    .map_err(|_| AlpacaCalendarDecodeError::TimeZoneRulesUnavailable)?;
    let start = resolve_new_york_midnight(naive)?;
    let end = resolve_new_york_midnight(next)?;
    if start >= end {
        return Err(AlpacaCalendarDecodeError::TimeZoneRulesUnavailable);
    }
    Ok((next_date, start, end))
}

fn resolve_new_york_midnight(date: NaiveDate) -> Result<Timestamp, AlpacaCalendarDecodeError> {
    let local = date
        .and_hms_opt(0, 0, 0)
        .ok_or(AlpacaCalendarDecodeError::TimeZoneRulesUnavailable)?;
    let resolved = match New_York.from_local_datetime(&local) {
        LocalResult::Single(resolved) => resolved,
        LocalResult::Ambiguous(_, _) | LocalResult::None => {
            return Err(AlpacaCalendarDecodeError::TimeZoneRulesUnavailable);
        }
    };
    resolved
        .with_timezone(&Utc)
        .timestamp_nanos_opt()
        .map(Timestamp::from_unix_nanos)
        .ok_or(AlpacaCalendarDecodeError::TimeZoneRulesUnavailable)
}

/// Bounded native calendar response, requiring request/physical replay before authority use.
#[derive(Debug, Deserialize)]
pub struct CalendarRangeWire {
    /// Native market identity.
    pub market: AlpacaMarketWire,
    /// Bounded source date rows.
    pub calendar: CalendarDays,
}

/// Source rows bounded by the existing historical request ceiling.
#[derive(Debug)]
pub struct CalendarDays(
    /// Exact returned order, not yet a completeness receipt.
    pub Vec<AlpacaCalendarDayWire>,
);

impl<'de> Deserialize<'de> for CalendarDays {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct DaysVisitor;
        impl<'de> Visitor<'de> for DaysVisitor {
            type Value = CalendarDays;
            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a bounded ordered calendar range")
            }
            fn visit_seq<A: SeqAccess<'de>>(
                self,
                mut sequence: A,
            ) -> Result<Self::Value, A::Error> {
                let mut days = Vec::new();
                while let Some(day) = sequence.next_element::<AlpacaCalendarDayWire>()? {
                    if days.len() == MAXIMUM_CALENDAR_ROWS {
                        return Err(serde::de::Error::custom("calendar row limit exceeded"));
                    }
                    if days.len() == days.capacity() {
                        let additional =
                            (MAXIMUM_CALENDAR_ROWS - days.len()).min(days.len().max(16));
                        days.try_reserve_exact(additional)
                            .map_err(serde::de::Error::custom)?;
                    }
                    days.push(day);
                }
                Ok(CalendarDays(days))
            }
        }
        deserializer.deserialize_seq(DaysVisitor)
    }
}

/// Exact native session and source-specific provider aggregation coordinates from sealed replay.
/// There is no constructor accepting caller-authored dates or endpoints.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AlpacaNativeCalendarSession {
    date: CalendarDate,
    opens_at: Timestamp,
    closes_at_exclusive: Timestamp,
    pre: Option<(Timestamp, Timestamp)>,
    post: Option<(Timestamp, Timestamp)>,
    intermission: Option<(Timestamp, Timestamp)>,
    settlement_date: Option<CalendarDate>,
    provider_period: Option<(Timestamp, Timestamp)>,
}
impl AlpacaNativeCalendarSession {
    /// Exact reported native trading date.
    pub const fn date(&self) -> CalendarDate {
        self.date
    }
    /// Exact reported core opening, never substituted with the daily period start.
    pub const fn opens_at(&self) -> Timestamp {
        self.opens_at
    }
    /// Exact reported core close. A reported intermission remains separately visible.
    pub const fn closes_at_exclusive(&self) -> Timestamp {
        self.closes_at_exclusive
    }
    /// Exact optional reported pre-market interval.
    pub const fn pre_market(&self) -> Option<(Timestamp, Timestamp)> {
        self.pre
    }
    /// Exact optional reported post-market interval.
    pub const fn post_market(&self) -> Option<(Timestamp, Timestamp)> {
        self.post
    }
    /// Exact optional reported core intermission; no continuous trading is inferred across it.
    pub const fn intermission(&self) -> Option<(Timestamp, Timestamp)> {
        self.intermission
    }
    /// Exact optional provider settlement date, without rolling.
    pub const fn settlement_date(&self) -> Option<CalendarDate> {
        self.settlement_date
    }
    /// Admitted Alpaca IEX 1Day timestamp, absent for calendars lacking that source rule.
    pub const fn provider_timestamp(&self) -> Option<Timestamp> {
        match self.provider_period {
            Some((start, _)) => Some(start),
            None => None,
        }
    }
    /// Exact Alpaca IEX 1Day aggregation start; other markets retain native hours only.
    pub const fn period_start(&self) -> Option<Timestamp> {
        match self.provider_period {
            Some((start, _)) => Some(start),
            None => None,
        }
    }
    /// Exact Alpaca IEX 1Day aggregation end; other markets retain native hours only.
    pub const fn period_end_exclusive(&self) -> Option<Timestamp> {
        match self.provider_period {
            Some((_, end)) => Some(end),
            None => None,
        }
    }
}

/// Complete source-reported range reconstructed from one exact physically verified capture.
/// This is immutable source replay evidence, not account, publication, or trading authority.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AlpacaRetainedCalendarSessions {
    market: AlpacaCalendarMarket,
    requested_dates: (CalendarDate, CalendarDate),
    capture_receipt_digest: EvidenceDigest,
    request_identity: EvidenceDigest,
    component: Option<(u16, EvidenceDigest, u16)>,
    calendar_page_ordinal: u16,
    received_at: Timestamp,
    complete_from: Timestamp,
    complete_until: Timestamp,
    replay_digest: EvidenceDigest,
    sessions: Box<[AlpacaNativeCalendarSession]>,
}
impl AlpacaRetainedCalendarSessions {
    /// Replays one exact admitted market/UTC request against its complete physical capture.
    /// Native hours for NYSE/NASDAQ do not establish any provider's daily aggregation bounds.
    /// The creating publication and retained source metadata must be checked by the data owner;
    /// this function does not admit a source account or confer point-in-time eligibility.
    pub fn try_replay(
        request: &AlpacaAuthenticatedCalendarRequest,
        sealed: &SealedProviderCaptureSetReceipt,
        segment: &SealedResearchJournalSegment,
        control: &dyn ResearchObjectControl,
    ) -> Result<Self, AlpacaCalendarDecodeError> {
        control.checkpoint(ResearchObjectControlPoint::BeforeVerification)?;
        if sealed.segment() != segment.receipt() {
            return Err(AlpacaCalendarDecodeError::InvalidResponse);
        }
        let capture = sealed.capture();
        let request_identity = request
            .capture_request_identity()
            .map_err(|_| AlpacaCalendarDecodeError::InvalidResponse)?;
        let (calendar_page_ordinal, component) = calendar_page(capture, request_identity)?;
        let page_index = usize::from(calendar_page_ordinal);
        let page = capture
            .pages()
            .get(page_index)
            .ok_or(AlpacaCalendarDecodeError::InvalidResponse)?;
        let record = segment
            .records()
            .get(page_index)
            .ok_or(AlpacaCalendarDecodeError::InvalidResponse)?;
        if segment.records().len() != capture.pages().len()
            || page.http_status() != 200
            || page.request_identity() != request_identity
            || page.request_page_token_digest().is_some()
            || page.response_next_page_token_digest().is_some()
            || record.source() != capture.source_id().as_str()
            || record.payload().is_empty()
            || record.payload().len() > ALPACA_HISTORICAL_CALENDAR_MAX_RESPONSE_BYTES
            || u64::try_from(record.payload().len()).ok() != Some(page.body_bytes())
        {
            return Err(AlpacaCalendarDecodeError::InvalidResponse);
        }
        let mut body_hash = Sha256::new();
        for (index, chunk) in record.payload().chunks(8192).enumerate() {
            control.checkpoint(ResearchObjectControlPoint::BeforeVerificationChunk {
                offset_bytes: (index as u64) * 8192,
            })?;
            body_hash.update(chunk);
        }
        if EvidenceDigest::new(DigestAlgorithm::Sha256, body_hash.finalize().into())
            != page.body_digest()
        {
            return Err(AlpacaCalendarDecodeError::InvalidResponse);
        }
        let wire =
            serde_json::from_reader::<_, CalendarRangeWire>(std::io::BufReader::with_capacity(
                8192,
                ControlledCalendarBytes {
                    bytes: record.payload(),
                    offset: 0,
                    control,
                },
            ));
        // Preserve cancellation/deadline instead of erasing a controlled reader error into JSON.
        control.checkpoint(ResearchObjectControlPoint::BeforeVerification)?;
        let wire = wire.map_err(|_| AlpacaCalendarDecodeError::InvalidResponse)?;
        validate_market_identity_for(&wire.market, request.market())?;
        let complete_from = new_york_civil_day(request.start_date())?.1;
        let complete_until = new_york_civil_day(request.end_date())?.2;
        let mut sessions = Vec::new();
        sessions
            .try_reserve_exact(wire.calendar.0.len())
            .map_err(|_| AlpacaCalendarDecodeError::ResourceBoundExceeded)?;
        let mut previous = None;
        for day in wire.calendar.0 {
            control.checkpoint(ResearchObjectControlPoint::BeforeVerification)?;
            let date = parse_calendar_date(&day.date)?;
            if date < request.start_date()
                || date > request.end_date()
                || previous.is_some_and(|earlier| earlier >= date)
            {
                return Err(AlpacaCalendarDecodeError::InvalidResponse);
            }
            previous = Some(date);
            let (_, period_start, period_end_exclusive) = new_york_civil_day(date)?;
            let parsed = parse_calendar_day(day, date, period_start, period_end_exclusive)?;
            let endpoints = |interval: ExactInterval| (interval.start, interval.end);
            sessions.push(AlpacaNativeCalendarSession {
                date,
                opens_at: parsed.core.start,
                closes_at_exclusive: parsed.core.end,
                pre: parsed.pre.map(endpoints),
                post: parsed.post.map(endpoints),
                intermission: parsed.lunch.map(endpoints),
                settlement_date: parsed.settlement_date,
                provider_period: (request.market() == AlpacaCalendarMarket::Iex)
                    .then_some((period_start, period_end_exclusive)),
            });
        }
        let mut digest = Sha256::new();
        digest.update(b"market-squawk/alpaca-sealed-native-session-replay/v1\0");
        if request.market() == AlpacaCalendarMarket::Iex {
            digest.update(ALPACA_IEX_DAILY_AGGREGATION_RULE);
        } else {
            digest.update(b"native-listed-market-sessions-only/v1\0");
        }
        digest.update(IANA_TZDB_VERSION.as_bytes());
        digest.update(sealed.receipt_digest().bytes());
        digest.update(request_identity.bytes());
        match component {
            Some((ordinal, content, pages)) => {
                digest.update([1]);
                digest.update(ordinal.to_be_bytes());
                digest.update(content.bytes());
                digest.update(pages.to_be_bytes());
            }
            None => digest.update([0]),
        }
        digest.update(complete_from.unix_nanos().to_be_bytes());
        digest.update(complete_until.unix_nanos().to_be_bytes());
        digest.update((sessions.len() as u32).to_be_bytes());
        let replay_digest = EvidenceDigest::new(DigestAlgorithm::Sha256, digest.finalize().into());
        control.checkpoint(ResearchObjectControlPoint::BeforeCommit)?;
        Ok(Self {
            market: request.market(),
            requested_dates: (request.start_date(), request.end_date()),
            capture_receipt_digest: sealed.receipt_digest(),
            request_identity,
            component,
            calendar_page_ordinal,
            received_at: page.received_at(),
            complete_from,
            complete_until,
            replay_digest,
            sessions: sessions.into_boxed_slice(),
        })
    }
    /// Inclusive original request coverage, including nontrading dates and empty session sets.
    /// These bounds are retained only after the request identity and physical capture rejoin.
    pub const fn requested_dates(&self) -> (CalendarDate, CalendarDate) {
        self.requested_dates
    }
    /// Exact requested/replayed native market, never inferred from a quote subscription.
    pub const fn market(&self) -> AlpacaCalendarMarket {
        self.market
    }
    /// Exact whole graph physical receipt digest, including original source observation clocks.
    pub const fn capture_receipt_digest(&self) -> EvidenceDigest {
        self.capture_receipt_digest
    }
    /// Exact authenticated request grammar commitment reconstructed by this replay.
    pub const fn request_identity(&self) -> EvidenceDigest {
        self.request_identity
    }
    /// Exact graph component ordinal/content/page count, absent for a standalone calendar.
    pub const fn component(&self) -> Option<(u16, EvidenceDigest, u16)> {
        self.component
    }
    /// Exact graph component content digest, absent for a standalone calendar.
    pub fn component_digest(&self) -> Option<EvidenceDigest> {
        self.component.map(|(_, digest, _)| digest)
    }
    /// Exact flattened raw page ordinal within the physically verified graph.
    pub const fn calendar_page_ordinal(&self) -> u16 {
        self.calendar_page_ordinal
    }
    /// Original complete-calendar response receipt time; publication availability is separate.
    pub const fn received_at(&self) -> Timestamp {
        self.received_at
    }
    /// Exact beginning of the source-requested complete calendar interval.
    pub const fn complete_from(&self) -> Timestamp {
        self.complete_from
    }
    /// Exact exclusive end of the source-requested complete calendar interval.
    pub const fn complete_until(&self) -> Timestamp {
        self.complete_until
    }
    /// Source/parser/request/physical-evidence commitment of this complete replay.
    pub const fn replay_digest(&self) -> EvidenceDigest {
        self.replay_digest
    }
    /// Every reported native session in exact source date order, including an empty complete set.
    pub fn sessions(&self) -> &[AlpacaNativeCalendarSession] {
        &self.sessions
    }
}

fn calendar_page(
    capture: &ProviderCaptureSetReceipt,
    request: EvidenceDigest,
) -> Result<(u16, Option<(u16, EvidenceDigest, u16)>), AlpacaCalendarDecodeError> {
    match capture.terminal() {
        ProviderCaptureTerminalDisposition::StandaloneResponse
            if capture.request_set_identity() == request && capture.pages().len() == 1 =>
        {
            Ok((0, None))
        }
        ProviderCaptureTerminalDisposition::CompleteRequestGraph => {
            let mut matching = capture
                .request_graph_components()
                .iter()
                .filter(|component| component.request_set_identity() == request);
            let component = matching
                .next()
                .ok_or(AlpacaCalendarDecodeError::InvalidResponse)?;
            if matching.next().is_some()
                || component.page_count().get() != 1
                || component.terminal() != ProviderCaptureTerminalDisposition::StandaloneResponse
                || component.source_id() != capture.source_id()
                || component.metadata_revision() != capture.metadata_revision()
                || component.dataset() != capture.dataset()
            {
                return Err(AlpacaCalendarDecodeError::InvalidResponse);
            }
            Ok((
                component.first_page_ordinal(),
                Some((
                    component.ordinal(),
                    component.content_digest(),
                    component.page_count().get(),
                )),
            ))
        }
        _ => Err(AlpacaCalendarDecodeError::InvalidResponse),
    }
}

struct ControlledCalendarBytes<'a> {
    bytes: &'a [u8],
    offset: usize,
    control: &'a dyn ResearchObjectControl,
}
impl std::io::Read for ControlledCalendarBytes<'_> {
    fn read(&mut self, output: &mut [u8]) -> std::io::Result<usize> {
        self.control
            .checkpoint(ResearchObjectControlPoint::BeforeVerificationChunk {
                offset_bytes: self.offset as u64,
            })
            .map_err(std::io::Error::other)?;
        let count = output.len().min(8192).min(self.bytes.len() - self.offset);
        output[..count].copy_from_slice(&self.bytes[self.offset..self.offset + count]);
        self.offset += count;
        Ok(count)
    }
}
