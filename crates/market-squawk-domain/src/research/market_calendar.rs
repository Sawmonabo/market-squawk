//! Source calendar coverage and dated sessions, independent of bar aggregation rules.

use std::fmt;

use serde::de::{IgnoredAny, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize};

use super::intraday_candle::{MarketSourceText, validate_market_raw_context};
use crate::{
    CalendarDate, DigestAlgorithm, EvidenceDigest, ExactPayloadEvidence, MetadataRevision,
    ProviderChannel, ProviderProduct, ResearchContext, RevisionNumber, Timestamp,
};

/// Canonical range ceiling; provider-specific request bounds may be smaller.
pub const MAX_MARKET_CALENDAR_DAYS: u32 = 4_096;
/// Maximum distinct native intervals retained for one source product and nominal date.
pub const MAX_MARKET_CALENDAR_INTERVALS: usize = 32;

/// SHA-256 domain for ordered returned-date membership. Hash these exact bytes, followed by a
/// big-endian u32 count, then each strictly increasing date as big-endian u16 year, u8 month,
/// and u8 day. The empty list still hashes the domain and zero count. Raw replay and publication
/// recompute this commitment; provider-specific JSON formatting is not part of its grammar.
pub const MARKET_CALENDAR_DATE_MEMBERSHIP_DOMAIN: &[u8] =
    b"market-squawk/market-calendar/reported-dates/v1\0";

/// Calendar source-evidence validation failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MarketCalendarObservationError {
    /// A calendar incorrectly claims one financial instrument.
    InstrumentScoped,
    /// Exact request or raw response evidence is absent.
    InvalidEvidence,
    /// Source, observation, availability, or effective-date clocks disagree.
    InvalidChronology,
    /// A date range is reversed or exceeds the canonical ceiling.
    InvalidDateScope,
    /// Declared coverage cannot reconcile its count, scope, or ordered-date digest.
    InvalidCoverage,
    /// A source UTC offset or resulting local timestamp is unrepresentable.
    InvalidOffset,
    /// A session is empty, reversed, duplicated, or inconsistent with source presence.
    InvalidSessions,
    /// A bounded collection exceeds its ceiling or cannot reserve memory.
    ResourceBound,
}

impl fmt::Display for MarketCalendarObservationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InstrumentScoped => "calendar observations must not claim a financial instrument",
            Self::InvalidEvidence => "calendar observation requires exact request and raw evidence",
            Self::InvalidChronology => "calendar observation clocks are inconsistent",
            Self::InvalidDateScope => "calendar date scope is invalid or exceeds its bound",
            Self::InvalidCoverage => "calendar coverage evidence is inconsistent",
            Self::InvalidOffset => "calendar timestamp offset is invalid or unrepresentable",
            Self::InvalidSessions => "calendar session evidence is inconsistent",
            Self::ResourceBound => "calendar observation exceeds a resource bound",
        })
    }
}
impl std::error::Error for MarketCalendarObservationError {}

/// Source absence and explicit null remain distinct from a reported value.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(
    deny_unknown_fields,
    tag = "state",
    content = "value",
    rename_all = "snake_case"
)]
pub enum MarketCalendarField<T> {
    /// Exact source-reported field.
    Reported(T),
    /// The source omitted the field.
    Missing,
    /// The source explicitly reported null.
    SourceNull,
}

/// Inclusive requested coverage or one source-returned date when the request left date implicit.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(deny_unknown_fields, tag = "kind", rename_all = "snake_case")]
pub enum MarketCalendarDateScope {
    /// Exact inclusive range explicitly bound by the captured request.
    RequestedRange {
        /// First requested nominal calendar date.
        start_date: CalendarDate,
        /// Last requested nominal calendar date.
        end_date: CalendarDate,
    },
    /// Date supplied by the response; it does not prove any broader requested range.
    ReturnedDate {
        /// Exact native nominal date, not the local receipt's UTC date.
        date: CalendarDate,
    },
}

impl MarketCalendarDateScope {
    /// Returns the inclusive first represented nominal date.
    pub const fn start_date(self) -> CalendarDate {
        match self {
            Self::RequestedRange { start_date, .. } => start_date,
            Self::ReturnedDate { date } => date,
        }
    }
    /// Returns the inclusive last represented nominal date.
    pub const fn end_date(self) -> CalendarDate {
        match self {
            Self::RequestedRange { end_date, .. } => end_date,
            Self::ReturnedDate { date } => date,
        }
    }
    fn day_count(self) -> Result<u32, MarketCalendarObservationError> {
        let days = self
            .end_date()
            .days_since_unix_epoch()
            .checked_sub(self.start_date().days_since_unix_epoch())
            .and_then(|days| days.checked_add(1))
            .and_then(|days| u32::try_from(days).ok())
            .filter(|days| (1..=MAX_MARKET_CALENDAR_DAYS).contains(days))
            .ok_or(MarketCalendarObservationError::InvalidDateScope)?;
        Ok(days)
    }
}

/// Exact source/product/date scope shared by the coverage row and every sibling session-day row.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct MarketCalendarScope {
    /// Source product from the admitted metadata, never a substitute venue name.
    pub provider_product: ProviderProduct,
    /// Exact calendar/hours endpoint family.
    pub provider_channel: ProviderChannel,
    /// Code-owned native contract revision used for interpretation.
    pub source_contract_revision: MetadataRevision,
    /// Native market category when the source supplies one, such as an outer hours-map key.
    pub native_market_type: MarketCalendarField<MarketSourceText>,
    /// Exact selected/returned native product key, such as an exchange acronym.
    pub native_product: MarketSourceText,
    /// Explicit request output timezone, independently of the source market's home timezone.
    pub requested_timezone: MarketCalendarField<MarketSourceText>,
    /// Exact nominal date range or source-defaulted returned date.
    pub date_scope: MarketCalendarDateScope,
    /// Nonzero digest of the complete credential-free request identity.
    pub request_evidence: ExactPayloadEvidence,
}

/// Native market metadata; no field is inferred from a UTC session offset or another provider.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct MarketCalendarMetadata {
    /// Source market acronym.
    pub acronym: MarketCalendarField<MarketSourceText>,
    /// Source market name, preserving spaces.
    pub name: MarketCalendarField<MarketSourceText>,
    /// Source-reported home timezone, distinct from the requested output timezone.
    pub timezone: MarketCalendarField<MarketSourceText>,
    /// Source BIC when supplied.
    pub bic: MarketCalendarField<MarketSourceText>,
    /// Source MIC when supplied; no alternate exchange code is substituted.
    pub mic: MarketCalendarField<MarketSourceText>,
}

/// Response coverage is separate from a returned day's open/closed state.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MarketCalendarCompleteness {
    /// Every scheduled session-day in the exact requested range was returned and retained.
    /// Omitted dates imply no scheduled session only after publication verifies all siblings.
    CompleteSessionEnumeration,
    /// Only the returned hours entries are established; omitted dates remain missing.
    ReturnedEntriesOnly,
    /// The source did not establish response completeness.
    Unknown,
}

/// Closed distinction between reported schedule, explicit market status, and unknown status.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MarketCalendarDayStatus {
    /// The source calendar row asserts scheduled session intervals, without an is-open flag.
    ScheduledSessions,
    /// The source explicitly reports open for this product/date; it does not prove complete hours.
    ExplicitlyOpen,
    /// The source explicitly reports closed for this product/date.
    ExplicitlyClosed,
    /// No source status was established. Empty hours never imply closure.
    Unknown,
}

/// Source-proven interval role; native category is also retained without replacement.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MarketCalendarSessionRole {
    /// Source-defined core/regular interval, never an inferred daily bar aggregation period.
    Core,
    /// Source-defined session before the core interval.
    Pre,
    /// Source-defined session after the core interval.
    Post,
    /// An interruption inside the core interval; this is not another open trading session.
    Intermission,
    /// Native session category has no established mapping to one of the roles above.
    SourceDefined,
}

/// Exact UTC instant plus the independently retained original numeric UTC offset.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(try_from = "MarketCalendarBoundaryWire")]
pub struct MarketCalendarBoundary {
    at: Timestamp,
    utc_offset_seconds: i32,
}

impl MarketCalendarBoundary {
    /// Retains a parsed source boundary without inferring its timezone from the offset.
    pub fn try_new(
        at: Timestamp,
        utc_offset_seconds: i32,
    ) -> Result<Self, MarketCalendarObservationError> {
        if !(-86_340..=86_340).contains(&utc_offset_seconds)
            || utc_offset_seconds % 60 != 0
            || at
                .checked_add_nanos(i64::from(utc_offset_seconds) * 1_000_000_000)
                .is_err()
        {
            return Err(MarketCalendarObservationError::InvalidOffset);
        }
        Ok(Self {
            at,
            utc_offset_seconds,
        })
    }
    /// Returns the exact normalized UTC instant.
    pub const fn at(self) -> Timestamp {
        self.at
    }
    /// Returns the original source numeric UTC offset, including zero when explicitly supplied.
    pub const fn utc_offset_seconds(self) -> i32 {
        self.utc_offset_seconds
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct MarketCalendarBoundaryWire {
    at: Timestamp,
    utc_offset_seconds: i32,
}
impl TryFrom<MarketCalendarBoundaryWire> for MarketCalendarBoundary {
    type Error = MarketCalendarObservationError;
    fn try_from(value: MarketCalendarBoundaryWire) -> Result<Self, Self::Error> {
        Self::try_new(value.at, value.utc_offset_seconds)
    }
}

/// One native interval; preserving core and lunch separately avoids inventing trading sessions.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct MarketCalendarInterval {
    /// Source category such as core, regularMarket, preMarket, or lunch.
    pub native_kind: MarketSourceText,
    /// Original zero-based ordinal within the source category's interval list.
    pub native_ordinal: u16,
    /// Established financial role, or SourceDefined when the source contract does not establish it.
    pub role: MarketCalendarSessionRole,
    /// Exact start with original offset.
    pub start: MarketCalendarBoundary,
    /// Exact end with original offset; no completion/finality assertion is implied.
    pub end: MarketCalendarBoundary,
}

/// Presence of the native session-hours object independently of the number of parsed intervals.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MarketCalendarSessionPresence {
    /// Native hours were supplied, including an explicitly empty collection.
    Reported,
    /// The source omitted the hours field.
    Missing,
    /// The source supplied a null hours field.
    SourceNull,
    /// Retained native projection cannot distinguish omission, null, or an empty collection.
    Unknown,
}

/// Source-native day values before validated bounded retention.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct MarketCalendarDayInput {
    /// Exact source trading date, which can differ from an interval's UTC date.
    pub date: CalendarDate,
    /// Source status or schedule evidence; missing hours never invent closure.
    pub status: MarketCalendarDayStatus,
    /// Source product category, preserving missing and explicit null.
    pub category: MarketCalendarField<MarketSourceText>,
    /// Exact nominal settlement date when reported; no midnight is manufactured.
    pub settlement_date: MarketCalendarField<CalendarDate>,
    /// Native hours-field presence.
    pub session_presence: MarketCalendarSessionPresence,
    /// Native interval order/category/offsets, bounded before collection growth.
    #[serde(deserialize_with = "deserialize_intervals")]
    pub intervals: Vec<MarketCalendarInterval>,
}

/// One validated source date; ordinary closure and source missingness remain separate.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(try_from = "MarketCalendarDayInput")]
pub struct MarketCalendarDay {
    date: CalendarDate,
    status: MarketCalendarDayStatus,
    category: MarketCalendarField<MarketSourceText>,
    settlement_date: MarketCalendarField<CalendarDate>,
    session_presence: MarketCalendarSessionPresence,
    intervals: Box<[MarketCalendarInterval]>,
}

impl MarketCalendarDay {
    /// Validates bounds and interval/presence relationships without altering native categories.
    pub fn try_new(input: MarketCalendarDayInput) -> Result<Self, MarketCalendarObservationError> {
        if input.intervals.len() > MAX_MARKET_CALENDAR_INTERVALS {
            return Err(MarketCalendarObservationError::ResourceBound);
        }
        if (input.session_presence != MarketCalendarSessionPresence::Reported
            && !input.intervals.is_empty())
            || (input.status == MarketCalendarDayStatus::ScheduledSessions
                && !input
                    .intervals
                    .iter()
                    .any(|interval| interval.role != MarketCalendarSessionRole::Intermission))
        {
            return Err(MarketCalendarObservationError::InvalidSessions);
        }
        for (index, interval) in input.intervals.iter().enumerate() {
            if interval.native_kind.as_str().len() > 128
                || interval.start.at() >= interval.end.at()
                || usize::from(interval.native_ordinal) >= MAX_MARKET_CALENDAR_INTERVALS
                || input.intervals[..index].iter().any(|prior| {
                    prior.native_kind == interval.native_kind
                        && prior.native_ordinal == interval.native_ordinal
                })
            {
                return Err(MarketCalendarObservationError::InvalidSessions);
            }
        }
        Ok(Self {
            date: input.date,
            status: input.status,
            category: input.category,
            settlement_date: input.settlement_date,
            session_presence: input.session_presence,
            intervals: input.intervals.into_boxed_slice(),
        })
    }
    /// Returns the exact native nominal trading date.
    pub const fn date(&self) -> CalendarDate {
        self.date
    }
    /// Returns source day status, never inferred from an empty list.
    pub const fn status(&self) -> MarketCalendarDayStatus {
        self.status
    }
    /// Returns the native product category.
    pub const fn category(&self) -> &MarketCalendarField<MarketSourceText> {
        &self.category
    }
    /// Returns the nominal settlement-date evidence.
    pub const fn settlement_date(&self) -> MarketCalendarField<CalendarDate> {
        self.settlement_date
    }
    /// Returns the native hours-field presence.
    pub const fn session_presence(&self) -> MarketCalendarSessionPresence {
        self.session_presence
    }
    /// Returns bounded intervals in retained native order.
    pub fn intervals(&self) -> &[MarketCalendarInterval] {
        &self.intervals
    }
}
impl TryFrom<MarketCalendarDayInput> for MarketCalendarDay {
    type Error = MarketCalendarObservationError;
    fn try_from(value: MarketCalendarDayInput) -> Result<Self, Self::Error> {
        Self::try_new(value)
    }
}

/// Coverage always has a real row, even when the complete source response contains zero days.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, tag = "kind", rename_all = "snake_case")]
pub enum MarketCalendarPayload {
    /// Request-wide coverage whose sibling rows must reconcile before it authorizes absence.
    Coverage {
        /// Native market metadata supplied with the response.
        market: MarketCalendarMetadata,
        /// Exact response-coverage meaning; never derived from weekdays or empty hours.
        completeness: MarketCalendarCompleteness,
        /// Count of distinct returned nominal dates for this exact source/product scope.
        reported_day_count: u32,
        /// SHA-256 of the canonical ordered-date grammar in MARKET_CALENDAR_DATE_MEMBERSHIP_DOMAIN.
        reported_days_digest: EvidenceDigest,
    },
    /// One returned date, always bound to the same scope/request/raw generation as its coverage.
    SessionDay {
        /// Exact native date and session evidence.
        day: MarketCalendarDay,
    },
}

/// Exact input for one canonical coverage or returned-day observation.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct MarketCalendarObservationInput {
    /// Source/raw/PIT evidence; no instrument. Venue is retained only when actually established.
    pub context: ResearchContext,
    /// Exact source/product/date/request coordinates.
    pub scope: MarketCalendarScope,
    /// Time the complete raw response was first observed locally, independent of any source clock.
    pub observed_at: Timestamp,
    /// Closed coverage or day payload.
    pub payload: MarketCalendarPayload,
}

/// A source-calendar observation suitable for immutable publication and point-in-time selection.
///
/// Coverage plus all returned SessionDay rows publish as one immutable generation. A value-only
/// Coverage row does not itself authorize absent dates: native replay and the publisher must
/// verify its exact request, complete row membership, count, ordered-date digest, and raw body.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(try_from = "MarketCalendarObservationInput")]
pub struct MarketCalendarObservation {
    context: ResearchContext,
    scope: MarketCalendarScope,
    observed_at: Timestamp,
    payload: MarketCalendarPayload,
}

impl MarketCalendarObservation {
    /// Validates exact date scope, source/local chronology, raw evidence, and payload consistency.
    pub fn try_new(
        input: MarketCalendarObservationInput,
    ) -> Result<Self, MarketCalendarObservationError> {
        if input.context.provenance().instrument_id().is_some() {
            return Err(MarketCalendarObservationError::InstrumentScoped);
        }
        validate_market_raw_context(&input.context)
            .map_err(|_| MarketCalendarObservationError::InvalidEvidence)?;
        if input.scope.request_evidence.content_digest().bytes() == [0; 32] {
            return Err(MarketCalendarObservationError::InvalidEvidence);
        }
        let provenance = input.context.provenance();
        if input.observed_at != provenance.received_at()
            || provenance
                .source_timestamp()
                .is_some_and(|source| source > input.observed_at)
            || provenance
                .availability()
                .conservative_available_at()
                .is_none_or(|available| available < input.observed_at)
        {
            return Err(MarketCalendarObservationError::InvalidChronology);
        }
        if let Some(published) = input.context.time().published()
            && (published.exact_timestamp() != provenance.source_timestamp()
                || provenance.source_timestamp().is_none())
        {
            return Err(MarketCalendarObservationError::InvalidChronology);
        }
        let scope_days = input.scope.date_scope.day_count()?;
        let effective_date = match &input.payload {
            MarketCalendarPayload::Coverage {
                completeness,
                reported_day_count,
                reported_days_digest,
                ..
            } => {
                if *reported_day_count > scope_days
                    || reported_days_digest.algorithm() != DigestAlgorithm::Sha256
                    || reported_days_digest.bytes() == [0; 32]
                    || *completeness == MarketCalendarCompleteness::CompleteSessionEnumeration
                        && !matches!(
                            input.scope.date_scope,
                            MarketCalendarDateScope::RequestedRange { .. }
                        )
                {
                    return Err(MarketCalendarObservationError::InvalidCoverage);
                }
                input.scope.date_scope.start_date()
            }
            MarketCalendarPayload::SessionDay { day } => {
                if day.date() < input.scope.date_scope.start_date()
                    || day.date() > input.scope.date_scope.end_date()
                {
                    return Err(MarketCalendarObservationError::InvalidDateScope);
                }
                day.date()
            }
        };
        if input.context.time().effective().calendar_date_value() != Some(effective_date) {
            return Err(MarketCalendarObservationError::InvalidChronology);
        }
        Ok(Self {
            context: input.context,
            scope: input.scope,
            observed_at: input.observed_at,
            payload: input.payload,
        })
    }
    /// Returns source/raw/PIT context without inventing a source publication instant.
    pub const fn context(&self) -> &ResearchContext {
        &self.context
    }
    /// Returns the exact source/product/native/request scope.
    pub const fn scope(&self) -> &MarketCalendarScope {
        &self.scope
    }
    /// Returns the independent local observation timestamp.
    pub const fn observed_at(&self) -> Timestamp {
        self.observed_at
    }
    /// Returns coverage or one source-returned day.
    pub const fn payload(&self) -> &MarketCalendarPayload {
        &self.payload
    }
    /// Rebinds only the canonical revision, preserving source dates, clocks, and raw lineage.
    pub fn with_revision(&self, revision: RevisionNumber) -> Self {
        Self {
            context: self.context.with_revision(revision),
            ..self.clone()
        }
    }
}
impl TryFrom<MarketCalendarObservationInput> for MarketCalendarObservation {
    type Error = MarketCalendarObservationError;
    fn try_from(value: MarketCalendarObservationInput) -> Result<Self, Self::Error> {
        Self::try_new(value)
    }
}

fn deserialize_intervals<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<Vec<MarketCalendarInterval>, D::Error> {
    struct IntervalsVisitor;
    impl<'de> Visitor<'de> for IntervalsVisitor {
        type Value = Vec<MarketCalendarInterval>;
        fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(
                formatter,
                "at most {MAX_MARKET_CALENDAR_INTERVALS} native session intervals"
            )
        }
        fn visit_seq<A: SeqAccess<'de>>(self, mut sequence: A) -> Result<Self::Value, A::Error> {
            let mut intervals = Vec::new();
            intervals
                .try_reserve_exact(MAX_MARKET_CALENDAR_INTERVALS)
                .map_err(|_| {
                    serde::de::Error::custom(MarketCalendarObservationError::ResourceBound)
                })?;
            while intervals.len() < MAX_MARKET_CALENDAR_INTERVALS {
                let Some(interval) = sequence.next_element()? else {
                    return Ok(intervals);
                };
                intervals.push(interval);
            }
            if sequence.next_element::<IgnoredAny>()?.is_some() {
                return Err(serde::de::Error::custom(
                    MarketCalendarObservationError::ResourceBound,
                ));
            }
            Ok(intervals)
        }
    }
    deserializer.deserialize_seq(IntervalsVisitor)
}
