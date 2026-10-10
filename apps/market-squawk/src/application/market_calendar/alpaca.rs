//! Alpaca adapter over exact pre-authorized provider period rows.

mod completed;
mod durable;
mod publication;
pub(crate) use completed::{AlpacaCalendarSessionDateReceipt, AlpacaCompletedSessionEvidence};
pub(crate) use durable::{
    PublishedAlpacaCalendar, publish_alpaca_market_calendar,
    publish_alpaca_market_calendar_with_job_context, read_alpaca_completed_calendar,
    read_alpaca_completed_calendar_with_job_context,
    read_alpaca_retained_calendar_with_job_context,
};
pub(crate) use publication::try_bind_alpaca_calendar_publication;

use std::{fmt, fmt::Write as _, sync::Arc};

use chrono_tz::{America::New_York, IANA_TZDB_VERSION};

use market_squawk_adapter_alpaca::calendar_decode::{
    ALPACA_IEX_DAILY_AGGREGATION_RULE as ALPACA_DAILY_AGGREGATION_RULE, AlpacaCalendarDayWire,
    AlpacaCalendarDecodeError, AlpacaMarketWire, ExactInterval, OptionalWire, ParsedCalendarDay,
    new_york_civil_day, parse_calendar_date, parse_calendar_day, validate_market_identity,
    validate_market_identity_for,
};

use market_squawk_adapter_alpaca::{
    AlpacaError, AlpacaHistoricalBarTimeAuthority, AlpacaHistoricalBarTimeRequest,
    AlpacaHistoricalSeriesSemantics,
};
use market_squawk_domain::{
    AvailabilityEvidence, BarTimeSemantics, BarTimestampBasis, CalendarDate, DigestAlgorithm,
    EffectiveInterval, EvidenceDigest, ExactPayloadEvidence, MarketBarSessionKind, RuleVersion,
    SourceIdentifier, Timestamp, VenueId,
};
use serde::de::{IgnoredAny, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer};
use sha2::{Digest as _, Sha256};
use thiserror::Error;

use super::{
    MarketCalendarAuthority, MarketCalendarClock, MarketCalendarError, MarketCalendarPeriod,
    MarketCalendarRulesetInput, MarketCalendarScheduleInput, MarketCalendarSession,
    MarketCalendarSessionConstraint, MarketCalendarSourceEvidence, MarketCalendarTimeframeRule,
};

const ALPACA_PROVIDER_ID: &str = "alpaca-market-data";
const ALPACA_IEX_VENUE: &str = "iex";
const ALPACA_DAILY_TIMEFRAME: &str = "1Day";
const ALPACA_CALENDAR_ID: &str = "alpaca-v3-calendar-iex-utc";
const ALPACA_DAILY_RULESET_ID: &str = "alpaca-v3-iex-utc-daily-rules-v1";
pub(crate) const MAXIMUM_ALPACA_CALENDAR_RESPONSE_BYTES: usize = 64 * 1024;
const MAXIMUM_ALPACA_CALENDAR_SEGMENTS: usize = 4;

/// Exact Alpaca trading API origin selected by the active account environment.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AlpacaCalendarApiEnvironment {
    /// Live trading API origin.
    Live,
    /// Paper trading API origin.
    Paper,
}

impl AlpacaCalendarApiEnvironment {
    /// Returns the sole official trading API origin for this explicit environment.
    pub(crate) const fn origin(self) -> &'static str {
        match self {
            Self::Live => "https://api.alpaca.markets",
            Self::Paper => "https://paper-api.alpaca.markets",
        }
    }
}

/// Credential-free exact request coordinates for one authenticated v3 IEX/UTC calendar call.
#[derive(Debug, Eq, PartialEq)]
pub(crate) struct AlpacaIexUtcCalendarFetchRequest {
    environment: AlpacaCalendarApiEnvironment,
    requested_date: CalendarDate,
    path_and_query: SourceIdentifier,
    raw_request: Box<[u8]>,
    request_evidence: ExactPayloadEvidence,
}

impl AlpacaIexUtcCalendarFetchRequest {
    /// Constructs exactly one positive single-day request; callers cannot omit or default a query.
    pub(crate) fn try_new(
        environment: AlpacaCalendarApiEnvironment,
        requested_date: CalendarDate,
    ) -> Result<Self, AlpacaIexUtcCalendarError> {
        if requested_date.year() > 9_999 {
            return Err(AlpacaIexUtcCalendarError::InvalidRequest);
        }
        let target_length = "/v3/calendar/IEX?start="
            .len()
            .checked_add(10)
            .and_then(|length| length.checked_add("&end=".len()))
            .and_then(|length| length.checked_add(10))
            .and_then(|length| length.checked_add("&timezone=UTC".len()))
            .ok_or(AlpacaIexUtcCalendarError::ResourceBoundExceeded)?;
        let mut target = String::new();
        target
            .try_reserve_exact(target_length)
            .map_err(|_| AlpacaIexUtcCalendarError::Allocation)?;
        write!(
            target,
            "/v3/calendar/IEX?start={requested_date}&end={requested_date}&timezone=UTC"
        )
        .map_err(|_| AlpacaIexUtcCalendarError::Allocation)?;

        let request_length = b"GET "
            .len()
            .checked_add(environment.origin().len())
            .and_then(|length| length.checked_add(target.len()))
            .ok_or(AlpacaIexUtcCalendarError::ResourceBoundExceeded)?;
        let mut raw_request = Vec::new();
        raw_request
            .try_reserve_exact(request_length)
            .map_err(|_| AlpacaIexUtcCalendarError::Allocation)?;
        raw_request.extend_from_slice(b"GET ");
        raw_request.extend_from_slice(environment.origin().as_bytes());
        raw_request.extend_from_slice(target.as_bytes());
        let request_evidence = exact_payload_evidence(&raw_request);
        let path_and_query = SourceIdentifier::try_from(target)
            .map_err(|_| AlpacaIexUtcCalendarError::InvalidRequest)?;
        Ok(Self {
            environment,
            requested_date,
            path_and_query,
            raw_request: raw_request.into_boxed_slice(),
            request_evidence,
        })
    }

    /// Returns the only admitted HTTP method.
    pub(crate) const fn method(&self) -> &'static str {
        "GET"
    }

    /// Returns the explicit live or paper origin selected by the caller.
    pub(crate) const fn origin(&self) -> &'static str {
        self.environment.origin()
    }

    /// Returns the exact current v3 path and complete IEX/date/date/UTC query.
    pub(crate) const fn path_and_query(&self) -> &SourceIdentifier {
        &self.path_and_query
    }

    /// Returns the one civil date this request must reconcile.
    pub(crate) const fn requested_date(&self) -> CalendarDate {
        self.requested_date
    }
}

/// Bounded authenticated HTTP result supplied by a runtime that owns credentials and transport.
#[derive(Debug, Eq, PartialEq)]
pub(crate) struct AlpacaIexUtcCalendarFetchResult {
    request: AlpacaIexUtcCalendarFetchRequest,
    http_status: u16,
    raw_response: Box<[u8]>,
    response_evidence: ExactPayloadEvidence,
    retrieval_evidence: ExactPayloadEvidence,
    availability: AvailabilityEvidence,
    retrieved_at: Timestamp,
}

impl AlpacaIexUtcCalendarFetchResult {
    /// Admits only an exact successful, nonempty, caller-bounded response body.
    #[allow(
        clippy::too_many_arguments,
        reason = "HTTP outcome, raw body, retrieval receipt, availability, and chronology are independent"
    )]
    pub(crate) fn try_new(
        request: AlpacaIexUtcCalendarFetchRequest,
        http_status: u16,
        raw_response: Box<[u8]>,
        retrieval_evidence: ExactPayloadEvidence,
        availability: AvailabilityEvidence,
        retrieved_at: Timestamp,
    ) -> Result<Self, AlpacaIexUtcCalendarError> {
        if http_status != 200 {
            return Err(AlpacaIexUtcCalendarUnavailable::HttpStatus(http_status).into());
        }
        if raw_response.is_empty() {
            return Err(AlpacaIexUtcCalendarError::InvalidResponse);
        }
        if raw_response.len() > MAXIMUM_ALPACA_CALENDAR_RESPONSE_BYTES {
            return Err(AlpacaIexUtcCalendarError::ResourceBoundExceeded);
        }
        if retrieval_evidence.content_digest().bytes() == [0; 32] {
            return Err(AlpacaIexUtcCalendarError::InvalidRetrievalEvidence);
        }
        let response_evidence = exact_payload_evidence(&raw_response);
        Ok(Self {
            request,
            http_status,
            raw_response,
            response_evidence,
            retrieval_evidence,
            availability,
            retrieved_at,
        })
    }
}

/// Parses one exact current v3 row and produces a revocable 1Day IEX calendar authority.
pub(crate) fn try_produce_alpaca_iex_utc_daily_calendar(
    result: AlpacaIexUtcCalendarFetchResult,
) -> Result<MarketCalendarAuthority, AlpacaIexUtcCalendarError> {
    let wire: AlpacaCalendarEnvelope = serde_json::from_slice(&result.raw_response)
        .map_err(|_| AlpacaIexUtcCalendarError::InvalidResponse)?;
    validate_market_identity(&wire.market)?;
    let day = match wire.calendar {
        CalendarRows::Empty => return Err(AlpacaIexUtcCalendarUnavailable::EmptyCalendar.into()),
        CalendarRows::Multiple => {
            return Err(AlpacaIexUtcCalendarUnavailable::MultipleCalendarRows.into());
        }
        CalendarRows::One(day) => day,
    };
    let response_date = parse_calendar_date(&day.date)?;
    if response_date != result.request.requested_date {
        return Err(AlpacaIexUtcCalendarUnavailable::RequestedDateMismatch.into());
    }

    let (next_date, period_start, period_end_exclusive) = new_york_civil_day(response_date)?;
    let parsed = parse_calendar_day(day, response_date, period_start, period_end_exclusive)?;
    let interpretation_evidence = interpretation_evidence(
        &wire.market,
        &parsed,
        response_date,
        next_date,
        period_start,
        period_end_exclusive,
        result.response_evidence.content_digest(),
    )?;

    let mut sessions = Vec::new();
    sessions
        .try_reserve_exact(MAXIMUM_ALPACA_CALENDAR_SEGMENTS)
        .map_err(|_| AlpacaIexUtcCalendarError::Allocation)?;
    if let Some(pre) = parsed.pre {
        sessions.push(calendar_session(response_date, "pre", pre)?);
    }
    match parsed.lunch {
        Some(lunch) => {
            sessions.push(calendar_session(
                response_date,
                "core-before-lunch",
                ExactInterval::new_unchecked(parsed.core.start, lunch.start),
            )?);
            sessions.push(calendar_session(
                response_date,
                "core-after-lunch",
                ExactInterval::new_unchecked(lunch.end, parsed.core.end),
            )?);
        }
        None => sessions.push(calendar_session(response_date, "core", parsed.core)?),
    }
    if let Some(post) = parsed.post {
        sessions.push(calendar_session(response_date, "post", post)?);
    }
    let session_count = u32::try_from(sessions.len())
        .map_err(|_| AlpacaIexUtcCalendarError::ResourceBoundExceeded)?;

    let mut timeframes = Vec::new();
    timeframes
        .try_reserve_exact(1)
        .map_err(|_| AlpacaIexUtcCalendarError::Allocation)?;
    timeframes.push(MarketCalendarTimeframeRule::new(
        source_identifier(ALPACA_DAILY_TIMEFRAME)?,
        BarTimestampBasis::PeriodStart,
        MarketBarSessionKind::ProviderDefined,
        MarketCalendarSessionConstraint::CompleteConsecutiveSessions,
    ));
    let mut periods = Vec::new();
    periods
        .try_reserve_exact(1)
        .map_err(|_| AlpacaIexUtcCalendarError::Allocation)?;
    periods.push(MarketCalendarPeriod::try_new(
        0,
        period_start,
        period_start,
        period_end_exclusive,
        0,
        session_count,
    )?);

    let source_evidence = MarketCalendarSourceEvidence::try_new(
        result.request.raw_request,
        result.request.request_evidence,
        result.raw_response,
        result.http_status,
        result.response_evidence,
        result.retrieval_evidence,
        interpretation_evidence,
        result.retrieved_at,
    )?;
    let effective = EffectiveInterval::new(result.retrieved_at, None)
        .map_err(|_| AlpacaIexUtcCalendarError::InvalidRetrievalEvidence)?;
    let ruleset = MarketCalendarRulesetInput::try_new(
        source_identifier(ALPACA_PROVIDER_ID)?,
        source_identifier(ALPACA_CALENDAR_ID)?,
        VenueId::try_from(ALPACA_IEX_VENUE)
            .map_err(|_| AlpacaIexUtcCalendarError::InvalidResponse)?,
        source_identifier(ALPACA_DAILY_RULESET_ID)?,
        RuleVersion::new(1).map_err(|_| AlpacaIexUtcCalendarError::InvalidResponse)?,
        source_evidence,
        result.availability,
        effective,
    )?;
    MarketCalendarAuthority::try_new(MarketCalendarScheduleInput::new(
        ruleset, timeframes, sessions, periods,
    ))
    .map_err(Into::into)
}

#[derive(Deserialize)]
struct AlpacaCalendarEnvelope {
    market: AlpacaMarketWire,
    calendar: CalendarRows,
}

enum CalendarRows {
    Empty,
    One(AlpacaCalendarDayWire),
    Multiple,
}

impl<'de> Deserialize<'de> for CalendarRows {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_seq(CalendarRowsVisitor)
    }
}

struct CalendarRowsVisitor;

impl<'de> Visitor<'de> for CalendarRowsVisitor {
    type Value = CalendarRows;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("an Alpaca calendar row array")
    }

    fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let Some(first) = sequence.next_element::<AlpacaCalendarDayWire>()? else {
            return Ok(CalendarRows::Empty);
        };
        if sequence.next_element::<IgnoredAny>()?.is_none() {
            return Ok(CalendarRows::One(first));
        }
        while sequence.next_element::<IgnoredAny>()?.is_some() {}
        Ok(CalendarRows::Multiple)
    }
}

fn calendar_session(
    date: CalendarDate,
    segment: &str,
    interval: ExactInterval,
) -> Result<MarketCalendarSession, AlpacaIexUtcCalendarError> {
    let required = 11_usize
        .checked_add(segment.len())
        .ok_or(AlpacaIexUtcCalendarError::ResourceBoundExceeded)?;
    let mut identity = String::new();
    identity
        .try_reserve_exact(required)
        .map_err(|_| AlpacaIexUtcCalendarError::Allocation)?;
    write!(identity, "{date}-{segment}").map_err(|_| AlpacaIexUtcCalendarError::Allocation)?;
    MarketCalendarSession::try_new(
        SourceIdentifier::try_from(identity)
            .map_err(|_| AlpacaIexUtcCalendarError::InvalidResponse)?,
        interval.start,
        interval.end,
        MarketBarSessionKind::ProviderDefined,
    )
    .map_err(Into::into)
}

#[allow(
    clippy::too_many_arguments,
    reason = "market row, parsed session, timezone revision, and exact response remain explicit"
)]
fn interpretation_evidence(
    market: &AlpacaMarketWire,
    day: &ParsedCalendarDay,
    date: CalendarDate,
    next_date: CalendarDate,
    period_start: Timestamp,
    period_end_exclusive: Timestamp,
    response_digest: EvidenceDigest,
) -> Result<ExactPayloadEvidence, AlpacaIexUtcCalendarError> {
    let mut digest = Sha256::new();
    digest.update(ALPACA_DAILY_AGGREGATION_RULE);
    hash_text(&mut digest, New_York.name())?;
    hash_text(&mut digest, IANA_TZDB_VERSION)?;
    hash_date(&mut digest, date);
    hash_date(&mut digest, next_date);
    digest.update(period_start.unix_nanos().to_be_bytes());
    digest.update(period_end_exclusive.unix_nanos().to_be_bytes());
    hash_text(&mut digest, &market.acronym)?;
    hash_text(&mut digest, &market.name)?;
    hash_text(&mut digest, &market.timezone)?;
    hash_optional_text(&mut digest, market.bic.as_deref())?;
    hash_optional_text(&mut digest, market.mic.as_deref())?;
    hash_interval(&mut digest, day.core);
    hash_optional_interval(&mut digest, day.pre);
    hash_optional_interval(&mut digest, day.post);
    hash_optional_interval(&mut digest, day.lunch);
    match day.settlement_date {
        Some(settlement_date) => {
            digest.update([1]);
            hash_date(&mut digest, settlement_date);
        }
        None => digest.update([0]),
    }
    digest.update([match response_digest.algorithm() {
        DigestAlgorithm::Sha256 => 1,
        DigestAlgorithm::Blake3 => 2,
    }]);
    digest.update(response_digest.bytes());
    Ok(ExactPayloadEvidence::from_content_digest(
        EvidenceDigest::new(DigestAlgorithm::Sha256, digest.finalize().into()),
    ))
}

fn hash_text(digest: &mut Sha256, value: &str) -> Result<(), AlpacaIexUtcCalendarError> {
    digest.update(
        u32::try_from(value.len())
            .map_err(|_| AlpacaIexUtcCalendarError::ResourceBoundExceeded)?
            .to_be_bytes(),
    );
    digest.update(value.as_bytes());
    Ok(())
}

fn hash_optional_text(
    digest: &mut Sha256,
    value: Option<&str>,
) -> Result<(), AlpacaIexUtcCalendarError> {
    match value {
        Some(value) => {
            digest.update([1]);
            hash_text(digest, value)
        }
        None => {
            digest.update([0]);
            Ok(())
        }
    }
}

fn hash_date(digest: &mut Sha256, date: CalendarDate) {
    digest.update(date.year().to_be_bytes());
    digest.update([date.month(), date.day()]);
}

fn hash_interval(digest: &mut Sha256, interval: ExactInterval) {
    digest.update(interval.start.unix_nanos().to_be_bytes());
    digest.update(interval.end.unix_nanos().to_be_bytes());
}

fn hash_optional_interval(digest: &mut Sha256, interval: Option<ExactInterval>) {
    match interval {
        Some(interval) => {
            digest.update([1]);
            hash_interval(digest, interval);
        }
        None => digest.update([0]),
    }
}

fn exact_payload_evidence(bytes: &[u8]) -> ExactPayloadEvidence {
    ExactPayloadEvidence::from_content_digest(EvidenceDigest::new(
        DigestAlgorithm::Sha256,
        Sha256::digest(bytes).into(),
    ))
}

fn source_identifier(value: &str) -> Result<SourceIdentifier, AlpacaIexUtcCalendarError> {
    let mut owned = String::new();
    owned
        .try_reserve_exact(value.len())
        .map_err(|_| AlpacaIexUtcCalendarError::Allocation)?;
    owned.push_str(value);
    SourceIdentifier::try_from(owned).map_err(|_| AlpacaIexUtcCalendarError::InvalidResponse)
}

/// Exact reasons a one-day v3 calendar cannot safely authorize a daily period.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub(crate) enum AlpacaIexUtcCalendarUnavailable {
    /// The authenticated endpoint did not return a successful response.
    #[error("Alpaca calendar HTTP status {0} is unavailable")]
    HttpStatus(u16),
    /// A successful response carried no positive trading-calendar row.
    #[error("Alpaca calendar returned no row for the requested day")]
    EmptyCalendar,
    /// A single-day request returned more than one row and is ambiguous.
    #[error("Alpaca calendar returned multiple rows for one requested day")]
    MultipleCalendarRows,
    /// The sole row did not identify the exact requested date.
    #[error("Alpaca calendar row does not match the requested day")]
    RequestedDateMismatch,
    /// The response did not establish the requested IEX market in UTC.
    #[error("Alpaca calendar market identity is unavailable")]
    UnknownMarketIdentity,
    /// Versioned timezone rules could not resolve both exact civil midnights.
    #[error("New York civil-day timezone rules are unavailable")]
    TimeZoneRulesUnavailable,
}

/// Invalid, unavailable, or resource-unsafe Alpaca v3 calendar production.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub(crate) enum AlpacaIexUtcCalendarError {
    /// A truthful typed unavailable result, never a fabricated schedule.
    #[error(transparent)]
    Unavailable(AlpacaIexUtcCalendarUnavailable),
    /// Request construction violated the one current code-owned request grammar.
    #[error("Alpaca calendar request is invalid")]
    InvalidRequest,
    /// The success body violated the one current code-owned response grammar.
    #[error("Alpaca calendar response is invalid")]
    InvalidResponse,
    /// Retrieval proof was absent or could not establish exact chronology.
    #[error("Alpaca calendar retrieval evidence is invalid")]
    InvalidRetrievalEvidence,
    /// An explicit request, response, or collection ceiling was exceeded.
    #[error("Alpaca calendar resource bound was exceeded")]
    ResourceBoundExceeded,
    /// A bounded allocation failed.
    #[error("Alpaca calendar bounded allocation failed")]
    Allocation,
    /// The parsed schedule failed source-neutral authority admission.
    #[error(transparent)]
    Calendar(#[from] MarketCalendarError),
}

impl From<AlpacaCalendarDecodeError> for AlpacaIexUtcCalendarError {
    fn from(error: AlpacaCalendarDecodeError) -> Self {
        match error {
            AlpacaCalendarDecodeError::UnknownMarketIdentity => {
                AlpacaIexUtcCalendarUnavailable::UnknownMarketIdentity.into()
            }
            AlpacaCalendarDecodeError::TimeZoneRulesUnavailable => {
                AlpacaIexUtcCalendarUnavailable::TimeZoneRulesUnavailable.into()
            }
            AlpacaCalendarDecodeError::ResourceBoundExceeded => Self::ResourceBoundExceeded,
            AlpacaCalendarDecodeError::InvalidResponse | AlpacaCalendarDecodeError::Control(_) => {
                Self::InvalidResponse
            }
        }
    }
}

impl From<AlpacaIexUtcCalendarUnavailable> for AlpacaIexUtcCalendarError {
    fn from(unavailable: AlpacaIexUtcCalendarUnavailable) -> Self {
        Self::Unavailable(unavailable)
    }
}

/// Alpaca historical-bar time authority backed only by exact controlled schedule rows.
pub(crate) struct AlpacaPreauthorizedBarTimeAuthority {
    calendar: Arc<MarketCalendarAuthority>,
    clock: Arc<dyn MarketCalendarClock>,
}

impl AlpacaPreauthorizedBarTimeAuthority {
    /// Admits exact Alpaca/IEX rows whose controlled evidence declares left-bound timestamps.
    pub(crate) fn try_new(
        calendar: Arc<MarketCalendarAuthority>,
        clock: Arc<dyn MarketCalendarClock>,
    ) -> Result<Self, MarketCalendarError> {
        if calendar.provider_id().as_str() != ALPACA_PROVIDER_ID
            || calendar.venue_id().as_str() != ALPACA_IEX_VENUE
        {
            return Err(MarketCalendarError::UnknownVenue);
        }
        for timeframe in calendar.timeframes() {
            if timeframe.timeframe().as_str() != ALPACA_DAILY_TIMEFRAME
                || timeframe.timestamp_basis() != BarTimestampBasis::PeriodStart
                || timeframe.session_kind() != MarketBarSessionKind::ProviderDefined
            {
                return Err(MarketCalendarError::InvalidTimeframeCoverage);
            }
        }
        let now = clock.now()?;
        calendar.validate_current_at(now)?;
        Ok(Self { calendar, clock })
    }

    /// Returns the exact authority-bound semantics required to register one historical series.
    pub(crate) fn series_semantics(
        &self,
        timeframe: &SourceIdentifier,
    ) -> Result<AlpacaHistoricalSeriesSemantics, MarketCalendarError> {
        let now = self.clock.now()?;
        let semantics =
            self.calendar
                .series_semantics_at(self.calendar.venue_id(), timeframe, now)?;
        Ok(AlpacaHistoricalSeriesSemantics::new(
            semantics.timestamp_basis(),
            semantics.into_session(),
        ))
    }
}

impl fmt::Debug for AlpacaPreauthorizedBarTimeAuthority {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AlpacaPreauthorizedBarTimeAuthority")
            .field("provider_id", self.calendar.provider_id())
            .field("venue_id", self.calendar.venue_id())
            .finish_non_exhaustive()
    }
}

impl AlpacaHistoricalBarTimeAuthority for AlpacaPreauthorizedBarTimeAuthority {
    fn validate_current(&self) -> Result<(), AlpacaError> {
        let now = self.clock.now().map_err(map_calendar_error)?;
        self.calendar
            .validate_current_at(now)
            .map_err(map_calendar_error)
    }

    fn resolve(
        &self,
        request: &AlpacaHistoricalBarTimeRequest,
    ) -> Result<BarTimeSemantics, AlpacaError> {
        let now = self.clock.now().map_err(map_calendar_error)?;
        self.calendar
            .resolve_at(
                request.venue_id(),
                request.timeframe(),
                request.provider_timestamp(),
                now,
            )
            .map_err(map_calendar_error)
    }
}

const fn map_calendar_error(_: MarketCalendarError) -> AlpacaError {
    AlpacaError::Protocol
}
