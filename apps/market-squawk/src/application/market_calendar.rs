//! Revocable exact-period market-calendar authority.

#![allow(
    dead_code,
    reason = "the provider schedule producer is the next historical-composition dependency"
)]

pub(crate) mod alpaca;

use std::collections::HashSet;
use std::mem::size_of;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use market_squawk_domain::{
    AvailabilityEvidence, BarTimeSemantics, BarTimestampBasis, DigestAlgorithm, EffectiveInterval,
    EvidenceDigest, ExactPayloadEvidence, MarketBarSessionEvidence, MarketBarSessionKind,
    RuleVersion, SourceIdentifier, Timestamp, VenueId,
};
use sha2::{Digest as _, Sha256};
use thiserror::Error;

/// Maximum exact timeframe rules retained by one provider/venue calendar revision.
const MAXIMUM_TIMEFRAMES: usize = 64;
/// Maximum explicit venue sessions retained by one provider/venue calendar revision.
const MAXIMUM_SESSIONS: usize = 16_384;
/// Maximum exact provider bar periods retained by one provider/venue calendar revision.
const MAXIMUM_PERIODS: usize = 2_000_000;
/// Maximum dynamically retained bytes for one provider/venue calendar revision.
const MAXIMUM_RETAINED_BYTES: usize = 256 * 1024 * 1024;
/// Maximum credential-free request bytes retained with one source response.
const MAXIMUM_SOURCE_REQUEST_BYTES: usize = 4 * 1024;

/// Exact credential-free request, raw response, and interpretation evidence retained by a ruleset.
#[derive(Debug, Eq, PartialEq)]
pub(crate) struct MarketCalendarSourceEvidence {
    raw_request: Box<[u8]>,
    request_evidence: ExactPayloadEvidence,
    raw_response: Box<[u8]>,
    response_status: u16,
    response_evidence: ExactPayloadEvidence,
    retrieval_evidence: ExactPayloadEvidence,
    interpretation_evidence: ExactPayloadEvidence,
    retrieved_at: Timestamp,
}

impl MarketCalendarSourceEvidence {
    /// Retains exact source bytes and independently digest-bound retrieval and interpretation proof.
    #[allow(
        clippy::too_many_arguments,
        reason = "request, response, retrieval, interpretation, and chronology are independent evidence"
    )]
    pub(crate) fn try_new(
        raw_request: Box<[u8]>,
        request_evidence: ExactPayloadEvidence,
        raw_response: Box<[u8]>,
        response_status: u16,
        response_evidence: ExactPayloadEvidence,
        retrieval_evidence: ExactPayloadEvidence,
        interpretation_evidence: ExactPayloadEvidence,
        retrieved_at: Timestamp,
    ) -> Result<Self, MarketCalendarError> {
        if raw_request.is_empty()
            || raw_request.len() > MAXIMUM_SOURCE_REQUEST_BYTES
            || raw_response.is_empty()
            || raw_response.len() > MAXIMUM_RETAINED_BYTES
        {
            return Err(MarketCalendarError::ResourceBoundExceeded);
        }
        validate_exact_sha256(&raw_request, &request_evidence)?;
        validate_exact_sha256(&raw_response, &response_evidence)?;
        validate_nonzero_evidence(&retrieval_evidence)?;
        validate_nonzero_evidence(&interpretation_evidence)?;
        Ok(Self {
            raw_request,
            request_evidence,
            raw_response,
            response_status,
            response_evidence,
            retrieval_evidence,
            interpretation_evidence,
            retrieved_at,
        })
    }
}

/// Exact source evidence and lifecycle interval for one calendar ruleset revision.
#[derive(Debug, Eq, PartialEq)]
pub(crate) struct MarketCalendarRulesetInput {
    provider_id: SourceIdentifier,
    calendar_id: SourceIdentifier,
    venue_id: VenueId,
    versioned_ruleset_id: SourceIdentifier,
    ruleset_version: RuleVersion,
    source_evidence: MarketCalendarSourceEvidence,
    availability: AvailabilityEvidence,
    effective: EffectiveInterval,
}

impl MarketCalendarRulesetInput {
    /// Binds a versioned ruleset to exact payload, availability, venue, and lifecycle evidence.
    #[allow(
        clippy::too_many_arguments,
        reason = "calendar authority coordinates must remain independently explicit"
    )]
    pub(crate) fn try_new(
        provider_id: SourceIdentifier,
        calendar_id: SourceIdentifier,
        venue_id: VenueId,
        versioned_ruleset_id: SourceIdentifier,
        ruleset_version: RuleVersion,
        source_evidence: MarketCalendarSourceEvidence,
        availability: AvailabilityEvidence,
        effective: EffectiveInterval,
    ) -> Result<Self, MarketCalendarError> {
        let available_at = availability
            .conservative_available_at()
            .ok_or(MarketCalendarError::UnqualifiedAvailability)?;
        if available_at > source_evidence.retrieved_at
            || matches!(
                &availability,
                AvailabilityEvidence::LocalFirstObserved { observed_at }
                    if *observed_at != source_evidence.retrieved_at
            )
        {
            return Err(MarketCalendarError::InvalidRetrievalChronology);
        }
        Ok(Self {
            provider_id,
            calendar_id,
            venue_id,
            versioned_ruleset_id,
            ruleset_version,
            source_evidence,
            availability,
            effective,
        })
    }
}

/// How one exact bar period may relate to the admitted venue sessions.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum MarketCalendarSessionConstraint {
    /// The complete bar period must stay inside one exact session.
    WithinSingleSession,
    /// The bar period may contain one or more whole consecutive sessions and no partial session.
    CompleteConsecutiveSessions,
}

/// Exact provider timeframe semantics admitted by one ruleset revision.
#[derive(Debug, Eq, PartialEq)]
pub(crate) struct MarketCalendarTimeframeRule {
    timeframe: SourceIdentifier,
    timestamp_basis: BarTimestampBasis,
    session_kind: MarketBarSessionKind,
    session_constraint: MarketCalendarSessionConstraint,
}

impl MarketCalendarTimeframeRule {
    /// Constructs one explicit timeframe rule; the authority admits no implicit timeframe.
    pub(crate) const fn new(
        timeframe: SourceIdentifier,
        timestamp_basis: BarTimestampBasis,
        session_kind: MarketBarSessionKind,
        session_constraint: MarketCalendarSessionConstraint,
    ) -> Self {
        Self {
            timeframe,
            timestamp_basis,
            session_kind,
            session_constraint,
        }
    }

    /// Returns the exact provider timeframe identity.
    pub(crate) const fn timeframe(&self) -> &SourceIdentifier {
        &self.timeframe
    }

    /// Returns the provider timestamp boundary admitted for this timeframe.
    pub(crate) const fn timestamp_basis(&self) -> BarTimestampBasis {
        self.timestamp_basis
    }

    /// Returns the exact source-neutral session class admitted for this timeframe.
    pub(crate) const fn session_kind(&self) -> MarketBarSessionKind {
        self.session_kind
    }
}

/// One exact nonempty venue session parsed from controlled calendar evidence.
#[derive(Debug, Eq, PartialEq)]
pub(crate) struct MarketCalendarSession {
    session_id: SourceIdentifier,
    opens_at: Timestamp,
    closes_at_exclusive: Timestamp,
    kind: MarketBarSessionKind,
}

impl MarketCalendarSession {
    /// Constructs one explicit half-open session, including an exact early close when applicable.
    pub(crate) fn try_new(
        session_id: SourceIdentifier,
        opens_at: Timestamp,
        closes_at_exclusive: Timestamp,
        kind: MarketBarSessionKind,
    ) -> Result<Self, MarketCalendarError> {
        if opens_at >= closes_at_exclusive {
            return Err(MarketCalendarError::InvalidSessionInterval);
        }
        Ok(Self {
            session_id,
            opens_at,
            closes_at_exclusive,
            kind,
        })
    }
}

/// One exact provider timestamp to aggregation-period mapping produced from controlled evidence.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct MarketCalendarPeriod {
    timeframe_ordinal: u16,
    provider_timestamp: Timestamp,
    period_start: Timestamp,
    period_end_exclusive: Timestamp,
    first_session_ordinal: u32,
    session_count: u32,
}

impl MarketCalendarPeriod {
    /// Constructs one exact period row without deriving a date, duration, or holiday boundary.
    #[allow(
        clippy::too_many_arguments,
        reason = "period and schedule coordinates must remain independently explicit"
    )]
    pub(crate) fn try_new(
        timeframe_ordinal: u16,
        provider_timestamp: Timestamp,
        period_start: Timestamp,
        period_end_exclusive: Timestamp,
        first_session_ordinal: u32,
        session_count: u32,
    ) -> Result<Self, MarketCalendarError> {
        if period_start >= period_end_exclusive {
            return Err(MarketCalendarError::InvalidPeriodInterval);
        }
        if session_count == 0 {
            return Err(MarketCalendarError::MissingSessionCoverage);
        }
        Ok(Self {
            timeframe_ordinal,
            provider_timestamp,
            period_start,
            period_end_exclusive,
            first_session_ordinal,
            session_count,
        })
    }
}

/// Complete bounded output expected from a provider calendar and aggregation-evidence producer.
#[derive(Debug, Eq, PartialEq)]
pub(crate) struct MarketCalendarScheduleInput {
    ruleset: MarketCalendarRulesetInput,
    timeframes: Vec<MarketCalendarTimeframeRule>,
    sessions: Vec<MarketCalendarSession>,
    periods: Vec<MarketCalendarPeriod>,
}

impl MarketCalendarScheduleInput {
    /// Retains already-canonical rows without sorting, truncating, or synthesizing missing rows.
    pub(crate) fn new(
        ruleset: MarketCalendarRulesetInput,
        timeframes: Vec<MarketCalendarTimeframeRule>,
        sessions: Vec<MarketCalendarSession>,
        periods: Vec<MarketCalendarPeriod>,
    ) -> Self {
        Self {
            ruleset,
            timeframes,
            sessions,
            periods,
        }
    }
}

/// Injected wall-time authority used only to validate calendar lifecycle currentness.
pub(crate) trait MarketCalendarClock: Send + Sync + 'static {
    /// Returns the current checked UTC instant.
    fn now(&self) -> Result<Timestamp, MarketCalendarError>;
}

/// Production system-clock implementation of [`MarketCalendarClock`].
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct SystemMarketCalendarClock;

impl MarketCalendarClock for SystemMarketCalendarClock {
    fn now(&self) -> Result<Timestamp, MarketCalendarError> {
        let elapsed = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| MarketCalendarError::ClockUnavailable)?;
        let nanos =
            i64::try_from(elapsed.as_nanos()).map_err(|_| MarketCalendarError::ClockUnavailable)?;
        Ok(Timestamp::from_unix_nanos(nanos))
    }
}

/// Stable provider timestamp and session identity for one admitted timeframe.
///
/// Dataset composition consumes this receipt directly instead of probing an arbitrary bar row.
#[derive(Debug, Eq, PartialEq)]
pub(crate) struct MarketCalendarSeriesSemantics {
    timestamp_basis: BarTimestampBasis,
    session: MarketBarSessionEvidence,
}

impl MarketCalendarSeriesSemantics {
    /// Returns which exact period boundary the provider timestamp identifies.
    pub(crate) const fn timestamp_basis(&self) -> BarTimestampBasis {
        self.timestamp_basis
    }

    /// Consumes the receipt and returns its exact versioned session evidence.
    pub(crate) fn into_session(self) -> MarketBarSessionEvidence {
        self.session
    }
}

/// Immutable exact-period calendar with one-way process-lifetime revocation.
#[derive(Debug)]
pub(crate) struct MarketCalendarAuthority {
    ruleset: MarketCalendarRulesetInput,
    available_at: Timestamp,
    timeframes: Vec<MarketCalendarTimeframeRule>,
    sessions: Vec<MarketCalendarSession>,
    periods: Vec<MarketCalendarPeriod>,
    authority_digest: EvidenceDigest,
    revoked: AtomicBool,
}

impl MarketCalendarAuthority {
    /// Admits one canonical, exact, bounded provider/venue schedule revision.
    ///
    /// The input must already be parsed and ordered by a controlled producer. This authority never
    /// fetches provider data and never invents missing holiday or aggregation rows.
    pub(crate) fn try_new(input: MarketCalendarScheduleInput) -> Result<Self, MarketCalendarError> {
        validate_collection_bounds(&input)?;
        let available_at = input
            .ruleset
            .availability
            .conservative_available_at()
            .ok_or(MarketCalendarError::UnqualifiedAvailability)?;
        validate_timeframes(&input.timeframes)?;
        validate_sessions(&input.sessions)?;
        validate_periods(&input.timeframes, &input.sessions, &input.periods)?;
        validate_retained_bytes(&input)?;
        let authority_digest = calendar_authority_digest(&input)?;
        Ok(Self {
            ruleset: input.ruleset,
            available_at,
            timeframes: input.timeframes,
            sessions: input.sessions,
            periods: input.periods,
            authority_digest,
            revoked: AtomicBool::new(false),
        })
    }

    /// Permanently revokes this in-memory ruleset revision.
    pub(crate) fn revoke(&self) {
        self.revoked.store(true, Ordering::Release);
    }

    /// Validates exact availability, lifecycle, and revocation state at a trusted instant.
    pub(crate) fn validate_current_at(&self, now: Timestamp) -> Result<(), MarketCalendarError> {
        if self.revoked.load(Ordering::Acquire) {
            return Err(MarketCalendarError::Revoked);
        }
        if now < self.available_at
            || now < self.ruleset.effective.starts_at()
            || self
                .ruleset
                .effective
                .ends_at()
                .is_some_and(|ends_at| now >= ends_at)
        {
            return Err(MarketCalendarError::StaleRuleset);
        }
        Ok(())
    }

    /// Returns exact stable series semantics without inferring or probing a bar coordinate.
    pub(crate) fn series_semantics_at(
        &self,
        venue_id: &VenueId,
        timeframe: &SourceIdentifier,
        now: Timestamp,
    ) -> Result<MarketCalendarSeriesSemantics, MarketCalendarError> {
        self.validate_current_at(now)?;
        if venue_id != &self.ruleset.venue_id {
            return Err(MarketCalendarError::UnknownVenue);
        }
        let timeframe_rule = self
            .timeframes
            .binary_search_by(|candidate| candidate.timeframe.cmp(timeframe))
            .ok()
            .and_then(|index| self.timeframes.get(index))
            .ok_or(MarketCalendarError::UnknownTimeframe)?;
        let semantics = MarketCalendarSeriesSemantics {
            timestamp_basis: timeframe_rule.timestamp_basis,
            session: self.session_evidence(timeframe_rule.session_kind)?,
        };
        if self.revoked.load(Ordering::Acquire) {
            return Err(MarketCalendarError::Revoked);
        }
        Ok(semantics)
    }

    /// Resolves only an exact pre-authorized provider timestamp row.
    pub(crate) fn resolve_at(
        &self,
        venue_id: &VenueId,
        timeframe: &SourceIdentifier,
        provider_timestamp: Timestamp,
        now: Timestamp,
    ) -> Result<BarTimeSemantics, MarketCalendarError> {
        self.validate_current_at(now)?;
        if venue_id != &self.ruleset.venue_id {
            return Err(MarketCalendarError::UnknownVenue);
        }
        let timeframe_index = self
            .timeframes
            .binary_search_by(|candidate| candidate.timeframe.cmp(timeframe))
            .map_err(|_| MarketCalendarError::UnknownTimeframe)?;
        let timeframe_rule = self
            .timeframes
            .get(timeframe_index)
            .ok_or(MarketCalendarError::UnknownTimeframe)?;
        let timeframe_ordinal = u16::try_from(timeframe_index)
            .map_err(|_| MarketCalendarError::ResourceBoundExceeded)?;
        let period = self
            .periods
            .binary_search_by(|candidate| {
                candidate
                    .timeframe_ordinal
                    .cmp(&timeframe_ordinal)
                    .then_with(|| candidate.provider_timestamp.cmp(&provider_timestamp))
            })
            .ok()
            .and_then(|index| self.periods.get(index))
            .ok_or(MarketCalendarError::MissingOrNonTradingPeriod)?;
        let first_session = usize::try_from(period.first_session_ordinal)
            .map_err(|_| MarketCalendarError::ResourceBoundExceeded)?;
        let session = self
            .sessions
            .get(first_session)
            .ok_or(MarketCalendarError::MissingSessionCoverage)?;
        let session = self.session_evidence(session.kind)?;
        let semantics = BarTimeSemantics::try_new(
            period.period_start,
            period.period_end_exclusive,
            timeframe_rule.timestamp_basis,
            session,
        )
        .map_err(|_| MarketCalendarError::InvalidPeriodInterval)?;
        if semantics.provider_timestamp() != provider_timestamp {
            return Err(MarketCalendarError::ProviderBoundaryMismatch);
        }
        if self.revoked.load(Ordering::Acquire) {
            return Err(MarketCalendarError::Revoked);
        }
        Ok(semantics)
    }

    /// Returns the exact provider identity whose schedule rows are admitted.
    pub(crate) const fn provider_id(&self) -> &SourceIdentifier {
        &self.ruleset.provider_id
    }

    /// Returns the exact venue whose schedule rows are admitted.
    pub(crate) const fn venue_id(&self) -> &VenueId {
        &self.ruleset.venue_id
    }

    /// Returns every exact admitted timeframe rule in canonical order.
    pub(crate) fn timeframes(&self) -> impl ExactSizeIterator<Item = &MarketCalendarTimeframeRule> {
        self.timeframes.iter()
    }

    fn session_evidence(
        &self,
        kind: MarketBarSessionKind,
    ) -> Result<MarketBarSessionEvidence, MarketCalendarError> {
        let ruleset_id = try_clone_identifier(&self.ruleset.versioned_ruleset_id)?;
        MarketBarSessionEvidence::try_new(kind, ruleset_id, self.authority_digest)
            .map_err(|_| MarketCalendarError::InvalidSessionEvidence)
    }
}

fn validate_collection_bounds(
    input: &MarketCalendarScheduleInput,
) -> Result<(), MarketCalendarError> {
    if input.timeframes.is_empty() || input.timeframes.len() > MAXIMUM_TIMEFRAMES {
        return Err(MarketCalendarError::InvalidTimeframeCoverage);
    }
    if input.sessions.is_empty() || input.sessions.len() > MAXIMUM_SESSIONS {
        return Err(MarketCalendarError::MissingSessionCoverage);
    }
    if input.periods.is_empty() || input.periods.len() > MAXIMUM_PERIODS {
        return Err(MarketCalendarError::MissingPeriodCoverage);
    }
    if input.timeframes.capacity() > MAXIMUM_TIMEFRAMES
        || input.sessions.capacity() > MAXIMUM_SESSIONS
        || input.periods.capacity() > MAXIMUM_PERIODS
    {
        return Err(MarketCalendarError::ResourceBoundExceeded);
    }
    Ok(())
}

fn validate_timeframes(
    timeframes: &[MarketCalendarTimeframeRule],
) -> Result<(), MarketCalendarError> {
    for pair in timeframes.windows(2) {
        if pair[0].timeframe >= pair[1].timeframe {
            return Err(MarketCalendarError::AmbiguousTimeframeCoverage);
        }
    }
    Ok(())
}

fn validate_sessions(sessions: &[MarketCalendarSession]) -> Result<(), MarketCalendarError> {
    let mut session_ids = HashSet::new();
    session_ids
        .try_reserve(sessions.len())
        .map_err(|_| MarketCalendarError::Allocation)?;
    for (index, session) in sessions.iter().enumerate() {
        if !session_ids.insert(session.session_id.as_str()) {
            return Err(MarketCalendarError::AmbiguousSessionCoverage);
        }
        if index > 0 {
            let previous = &sessions[index - 1];
            if previous.opens_at >= session.opens_at
                || previous.closes_at_exclusive > session.opens_at
            {
                return Err(MarketCalendarError::AmbiguousSessionCoverage);
            }
        }
    }
    Ok(())
}

fn validate_periods(
    timeframes: &[MarketCalendarTimeframeRule],
    sessions: &[MarketCalendarSession],
    periods: &[MarketCalendarPeriod],
) -> Result<(), MarketCalendarError> {
    let mut used_timeframes = Vec::new();
    used_timeframes
        .try_reserve_exact(timeframes.len())
        .map_err(|_| MarketCalendarError::Allocation)?;
    used_timeframes.resize(timeframes.len(), false);
    let mut previous: Option<&MarketCalendarPeriod> = None;
    for period in periods {
        let timeframe_index = usize::from(period.timeframe_ordinal);
        let timeframe = timeframes
            .get(timeframe_index)
            .ok_or(MarketCalendarError::UnknownTimeframe)?;
        used_timeframes[timeframe_index] = true;
        let expected_provider_timestamp = match timeframe.timestamp_basis {
            BarTimestampBasis::PeriodStart => period.period_start,
            BarTimestampBasis::PeriodEnd => period.period_end_exclusive,
        };
        if expected_provider_timestamp != period.provider_timestamp {
            return Err(MarketCalendarError::ProviderBoundaryMismatch);
        }
        if let Some(previous) = previous {
            if previous.timeframe_ordinal > period.timeframe_ordinal
                || (previous.timeframe_ordinal == period.timeframe_ordinal
                    && previous.provider_timestamp >= period.provider_timestamp)
            {
                return Err(MarketCalendarError::AmbiguousPeriodCoverage);
            }
            if previous.timeframe_ordinal == period.timeframe_ordinal
                && previous.period_end_exclusive > period.period_start
            {
                return Err(MarketCalendarError::AmbiguousPeriodCoverage);
            }
        }
        validate_period_sessions(timeframe, sessions, period)?;
        previous = Some(period);
    }
    if used_timeframes.iter().any(|used| !used) {
        return Err(MarketCalendarError::MissingPeriodCoverage);
    }
    Ok(())
}

fn validate_period_sessions(
    timeframe: &MarketCalendarTimeframeRule,
    sessions: &[MarketCalendarSession],
    period: &MarketCalendarPeriod,
) -> Result<(), MarketCalendarError> {
    let first = usize::try_from(period.first_session_ordinal)
        .map_err(|_| MarketCalendarError::ResourceBoundExceeded)?;
    let count = usize::try_from(period.session_count)
        .map_err(|_| MarketCalendarError::ResourceBoundExceeded)?;
    let end = first
        .checked_add(count)
        .ok_or(MarketCalendarError::ResourceBoundExceeded)?;
    let selected = sessions
        .get(first..end)
        .ok_or(MarketCalendarError::MissingSessionCoverage)?;
    if selected
        .iter()
        .any(|session| session.kind != timeframe.session_kind)
    {
        return Err(MarketCalendarError::SessionKindMismatch);
    }
    match timeframe.session_constraint {
        MarketCalendarSessionConstraint::WithinSingleSession => {
            if selected.len() != 1
                || period.period_start < selected[0].opens_at
                || period.period_end_exclusive > selected[0].closes_at_exclusive
            {
                return Err(MarketCalendarError::UnsupportedSessionBoundary);
            }
        }
        MarketCalendarSessionConstraint::CompleteConsecutiveSessions => {
            let first_selected = selected
                .first()
                .ok_or(MarketCalendarError::MissingSessionCoverage)?;
            let last_selected = selected
                .last()
                .ok_or(MarketCalendarError::MissingSessionCoverage)?;
            if first_selected.opens_at < period.period_start
                || last_selected.closes_at_exclusive > period.period_end_exclusive
                || first
                    .checked_sub(1)
                    .and_then(|index| sessions.get(index))
                    .is_some_and(|session| session.closes_at_exclusive > period.period_start)
                || sessions
                    .get(end)
                    .is_some_and(|session| session.opens_at < period.period_end_exclusive)
            {
                return Err(MarketCalendarError::UnsupportedSessionBoundary);
            }
        }
    }
    Ok(())
}

fn validate_retained_bytes(input: &MarketCalendarScheduleInput) -> Result<(), MarketCalendarError> {
    let mut retained = 0_usize;
    for value in [
        input.ruleset.provider_id.retained_bytes(),
        input.ruleset.calendar_id.retained_bytes(),
        input.ruleset.venue_id.retained_bytes(),
        input.ruleset.versioned_ruleset_id.retained_bytes(),
    ] {
        retained = retained
            .checked_add(value)
            .ok_or(MarketCalendarError::ResourceBoundExceeded)?;
    }
    let source = &input.ruleset.source_evidence;
    retained = retained
        .checked_add(source.raw_request.len())
        .and_then(|value| value.checked_add(source.raw_response.len()))
        .ok_or(MarketCalendarError::ResourceBoundExceeded)?;
    for evidence in [
        &source.request_evidence,
        &source.response_evidence,
        &source.retrieval_evidence,
        &source.interpretation_evidence,
    ] {
        retained = retained
            .checked_add(
                evidence
                    .dynamic_retained_bytes()
                    .ok_or(MarketCalendarError::ResourceBoundExceeded)?,
            )
            .ok_or(MarketCalendarError::ResourceBoundExceeded)?;
    }
    retained = retained
        .checked_add(availability_retained_bytes(&input.ruleset.availability))
        .ok_or(MarketCalendarError::ResourceBoundExceeded)?;
    retained = retained
        .checked_add(
            input
                .timeframes
                .capacity()
                .checked_mul(size_of::<MarketCalendarTimeframeRule>())
                .ok_or(MarketCalendarError::ResourceBoundExceeded)?,
        )
        .and_then(|value| {
            input
                .sessions
                .capacity()
                .checked_mul(size_of::<MarketCalendarSession>())
                .and_then(|bytes| value.checked_add(bytes))
        })
        .and_then(|value| {
            input
                .periods
                .capacity()
                .checked_mul(size_of::<MarketCalendarPeriod>())
                .and_then(|bytes| value.checked_add(bytes))
        })
        .ok_or(MarketCalendarError::ResourceBoundExceeded)?;
    for timeframe in &input.timeframes {
        retained = retained
            .checked_add(timeframe.timeframe.retained_bytes())
            .ok_or(MarketCalendarError::ResourceBoundExceeded)?;
    }
    for session in &input.sessions {
        retained = retained
            .checked_add(session.session_id.retained_bytes())
            .ok_or(MarketCalendarError::ResourceBoundExceeded)?;
    }
    if retained > MAXIMUM_RETAINED_BYTES {
        return Err(MarketCalendarError::ResourceBoundExceeded);
    }
    Ok(())
}

fn availability_retained_bytes(availability: &AvailabilityEvidence) -> usize {
    match availability {
        AvailabilityEvidence::Evidenced { evidence, .. } => evidence.retained_bytes(),
        AvailabilityEvidence::LocalFirstObserved { .. }
        | AvailabilityEvidence::Inferred { .. }
        | AvailabilityEvidence::Unknown => 0,
    }
}

fn try_clone_identifier(
    identifier: &SourceIdentifier,
) -> Result<SourceIdentifier, MarketCalendarError> {
    let mut value = String::new();
    value
        .try_reserve_exact(identifier.as_str().len())
        .map_err(|_| MarketCalendarError::Allocation)?;
    value.push_str(identifier.as_str());
    SourceIdentifier::try_from(value).map_err(|_| MarketCalendarError::InvalidRulesetIdentity)
}

fn calendar_authority_digest(
    input: &MarketCalendarScheduleInput,
) -> Result<EvidenceDigest, MarketCalendarError> {
    let mut digest = Sha256::new();
    digest.update(b"market-squawk/market-calendar-authority/v2\0");
    hash_text(&mut digest, input.ruleset.provider_id.as_str())?;
    hash_text(&mut digest, input.ruleset.calendar_id.as_str())?;
    hash_text(&mut digest, input.ruleset.venue_id.as_str())?;
    hash_text(&mut digest, input.ruleset.versioned_ruleset_id.as_str())?;
    digest.update(input.ruleset.ruleset_version.get().to_be_bytes());
    let source = &input.ruleset.source_evidence;
    for evidence in [
        &source.request_evidence,
        &source.response_evidence,
        &source.retrieval_evidence,
        &source.interpretation_evidence,
    ] {
        hash_evidence_digest(&mut digest, evidence.content_digest());
        hash_optional_locator(&mut digest, evidence)?;
    }
    digest.update(source.response_status.to_be_bytes());
    digest.update(source.retrieved_at.unix_nanos().to_be_bytes());
    hash_availability(&mut digest, &input.ruleset.availability)?;
    digest.update(
        input
            .ruleset
            .effective
            .starts_at()
            .unix_nanos()
            .to_be_bytes(),
    );
    hash_optional_timestamp(&mut digest, input.ruleset.effective.ends_at());
    hash_len(&mut digest, input.timeframes.len())?;
    for timeframe in &input.timeframes {
        hash_text(&mut digest, timeframe.timeframe.as_str())?;
        digest.update([timestamp_basis_tag(timeframe.timestamp_basis)]);
        digest.update([session_kind_tag(timeframe.session_kind)]);
        digest.update([session_constraint_tag(timeframe.session_constraint)]);
    }
    hash_len(&mut digest, input.sessions.len())?;
    for session in &input.sessions {
        hash_text(&mut digest, session.session_id.as_str())?;
        digest.update(session.opens_at.unix_nanos().to_be_bytes());
        digest.update(session.closes_at_exclusive.unix_nanos().to_be_bytes());
        digest.update([session_kind_tag(session.kind)]);
    }
    hash_len(&mut digest, input.periods.len())?;
    for period in &input.periods {
        digest.update(period.timeframe_ordinal.to_be_bytes());
        digest.update(period.provider_timestamp.unix_nanos().to_be_bytes());
        digest.update(period.period_start.unix_nanos().to_be_bytes());
        digest.update(period.period_end_exclusive.unix_nanos().to_be_bytes());
        digest.update(period.first_session_ordinal.to_be_bytes());
        digest.update(period.session_count.to_be_bytes());
    }
    Ok(EvidenceDigest::new(
        DigestAlgorithm::Sha256,
        digest.finalize().into(),
    ))
}

fn hash_optional_locator(
    digest: &mut Sha256,
    evidence: &ExactPayloadEvidence,
) -> Result<(), MarketCalendarError> {
    match evidence.version_pinned_locator() {
        Some(locator) => {
            digest.update([1]);
            hash_text(digest, locator.reference().as_str())?;
            hash_text(digest, locator.version().as_str())?;
        }
        None => digest.update([0]),
    }
    Ok(())
}

fn hash_availability(
    digest: &mut Sha256,
    availability: &AvailabilityEvidence,
) -> Result<(), MarketCalendarError> {
    match availability {
        AvailabilityEvidence::Evidenced {
            available_at,
            evidence,
        } => {
            digest.update([1]);
            digest.update(available_at.unix_nanos().to_be_bytes());
            hash_text(digest, evidence.as_str())?;
        }
        AvailabilityEvidence::LocalFirstObserved { observed_at } => {
            digest.update([2]);
            digest.update(observed_at.unix_nanos().to_be_bytes());
        }
        AvailabilityEvidence::Inferred { .. } | AvailabilityEvidence::Unknown => {
            return Err(MarketCalendarError::UnqualifiedAvailability);
        }
    }
    Ok(())
}

fn hash_evidence_digest(digest: &mut Sha256, evidence: EvidenceDigest) {
    digest.update([match evidence.algorithm() {
        DigestAlgorithm::Sha256 => 1,
        DigestAlgorithm::Blake3 => 2,
    }]);
    digest.update(evidence.bytes());
}

fn validate_exact_sha256(
    bytes: &[u8],
    evidence: &ExactPayloadEvidence,
) -> Result<(), MarketCalendarError> {
    let expected = evidence.content_digest();
    let actual: [u8; 32] = Sha256::digest(bytes).into();
    if expected.algorithm() != DigestAlgorithm::Sha256
        || expected.bytes() == [0; 32]
        || expected.bytes() != actual
    {
        return Err(MarketCalendarError::InvalidPayloadEvidence);
    }
    Ok(())
}

fn validate_nonzero_evidence(evidence: &ExactPayloadEvidence) -> Result<(), MarketCalendarError> {
    if evidence.content_digest().bytes() == [0; 32] {
        Err(MarketCalendarError::InvalidPayloadEvidence)
    } else {
        Ok(())
    }
}

fn hash_optional_timestamp(digest: &mut Sha256, timestamp: Option<Timestamp>) {
    match timestamp {
        Some(timestamp) => {
            digest.update([1]);
            digest.update(timestamp.unix_nanos().to_be_bytes());
        }
        None => digest.update([0]),
    }
}

fn hash_len(digest: &mut Sha256, length: usize) -> Result<(), MarketCalendarError> {
    digest.update(
        u32::try_from(length)
            .map_err(|_| MarketCalendarError::ResourceBoundExceeded)?
            .to_be_bytes(),
    );
    Ok(())
}

fn hash_text(digest: &mut Sha256, value: &str) -> Result<(), MarketCalendarError> {
    digest.update(
        u32::try_from(value.len())
            .map_err(|_| MarketCalendarError::ResourceBoundExceeded)?
            .to_be_bytes(),
    );
    digest.update(value.as_bytes());
    Ok(())
}

const fn timestamp_basis_tag(basis: BarTimestampBasis) -> u8 {
    match basis {
        BarTimestampBasis::PeriodStart => 1,
        BarTimestampBasis::PeriodEnd => 2,
    }
}

const fn session_kind_tag(kind: MarketBarSessionKind) -> u8 {
    match kind {
        MarketBarSessionKind::Regular => 1,
        MarketBarSessionKind::Extended => 2,
        MarketBarSessionKind::Continuous => 3,
        MarketBarSessionKind::ProviderDefined => 4,
    }
}

const fn session_constraint_tag(constraint: MarketCalendarSessionConstraint) -> u8 {
    match constraint {
        MarketCalendarSessionConstraint::WithinSingleSession => 1,
        MarketCalendarSessionConstraint::CompleteConsecutiveSessions => 2,
    }
}

/// Invalid, unavailable, stale, ambiguous, or resource-unsafe market-calendar authority.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub(crate) enum MarketCalendarError {
    /// Exact provider schedule payload evidence was absent or all-zero.
    #[error("market calendar requires nonzero exact provider payload evidence")]
    InvalidPayloadEvidence,
    /// Retrieval or conservative availability chronology contradicted the retained response.
    #[error("market calendar retrieval chronology is invalid")]
    InvalidRetrievalChronology,
    /// Schedule availability was inferred or unknown rather than evidenced.
    #[error("market calendar availability is not point-in-time qualified")]
    UnqualifiedAvailability,
    /// A ruleset identity could not retain its exact version.
    #[error("market calendar ruleset identity is invalid or unbounded")]
    InvalidRulesetIdentity,
    /// No valid bounded timeframe rule set was supplied.
    #[error("market calendar timeframe coverage is missing or out of bounds")]
    InvalidTimeframeCoverage,
    /// Timeframe rows were duplicated or not in strict canonical order.
    #[error("market calendar timeframe coverage is ambiguous")]
    AmbiguousTimeframeCoverage,
    /// One explicit session interval was empty or reversed.
    #[error("market calendar session interval must be nonempty")]
    InvalidSessionInterval,
    /// Session evidence was absent or referenced outside the admitted schedule.
    #[error("market calendar session coverage is missing")]
    MissingSessionCoverage,
    /// Session identities, order, or intervals were ambiguous.
    #[error("market calendar session coverage is ambiguous")]
    AmbiguousSessionCoverage,
    /// One explicit aggregation period was empty or reversed.
    #[error("market calendar period interval must be nonempty")]
    InvalidPeriodInterval,
    /// No exact period row existed for one or more admitted timeframes.
    #[error("market calendar period coverage is missing")]
    MissingPeriodCoverage,
    /// Period rows were duplicated, overlapped, or not in strict canonical order.
    #[error("market calendar period coverage is ambiguous")]
    AmbiguousPeriodCoverage,
    /// The provider timestamp did not equal the declared period boundary.
    #[error("provider bar timestamp does not match the declared period boundary")]
    ProviderBoundaryMismatch,
    /// A period and its exact sessions did not retain one source-neutral session kind.
    #[error("market calendar period and session kinds do not match")]
    SessionKindMismatch,
    /// A period crossed or partially covered a session boundary its rule does not support.
    #[error("market calendar aggregation crosses an unsupported session boundary")]
    UnsupportedSessionBoundary,
    /// No exact pre-authorized row exists; nontrading dates are never synthesized.
    #[error("exact provider period is missing or falls on an unadmitted nontrading coordinate")]
    MissingOrNonTradingPeriod,
    /// The requested venue is not governed by this calendar authority.
    #[error("market calendar venue is unknown")]
    UnknownVenue,
    /// The requested timeframe is not governed by this calendar authority.
    #[error("market calendar timeframe is unknown")]
    UnknownTimeframe,
    /// The exact calendar ruleset is not yet available or is no longer effective.
    #[error("market calendar ruleset is stale")]
    StaleRuleset,
    /// Calendar authority was permanently revoked.
    #[error("market calendar authority was revoked")]
    Revoked,
    /// Session evidence could not be represented by the current domain contract.
    #[error("market calendar session evidence is invalid")]
    InvalidSessionEvidence,
    /// A checked collection or retained-byte ceiling was exceeded.
    #[error("market calendar resource bound was exceeded")]
    ResourceBoundExceeded,
    /// A bounded allocation failed.
    #[error("market calendar bounded allocation failed")]
    Allocation,
    /// The trusted wall clock could not be represented exactly.
    #[error("market calendar current time is unavailable")]
    ClockUnavailable,
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use super::*;

    #[test]
    fn exact_regular_and_early_close_periods_revoke_together() -> Result<(), Box<dyn Error>> {
        let raw_request = b"GET https://paper-api.alpaca.markets/v3/calendar/IEX".to_vec();
        let raw_response = br#"{"market":"IEX"}"#.to_vec();
        let source_evidence = MarketCalendarSourceEvidence::try_new(
            raw_request.clone().into_boxed_slice(),
            exact_payload(&raw_request),
            raw_response.clone().into_boxed_slice(),
            200,
            exact_payload(&raw_response),
            ExactPayloadEvidence::from_content_digest(EvidenceDigest::new(
                DigestAlgorithm::Sha256,
                [8; 32],
            )),
            ExactPayloadEvidence::from_content_digest(EvidenceDigest::new(
                DigestAlgorithm::Sha256,
                [9; 32],
            )),
            Timestamp::from_unix_nanos(90),
        )?;
        let ruleset = MarketCalendarRulesetInput::try_new(
            SourceIdentifier::try_from("alpaca-market-data")?,
            SourceIdentifier::try_from("alpaca-equities-calendar")?,
            VenueId::try_from("iex")?,
            SourceIdentifier::try_from("alpaca-equities-calendar-rules-v1")?,
            RuleVersion::new(1)?,
            source_evidence,
            AvailabilityEvidence::local_first_observed(Timestamp::from_unix_nanos(90)),
            EffectiveInterval::new(
                Timestamp::from_unix_nanos(100),
                Some(Timestamp::from_unix_nanos(200)),
            )?,
        )?;
        let authority = MarketCalendarAuthority::try_new(MarketCalendarScheduleInput::new(
            ruleset,
            vec![MarketCalendarTimeframeRule::new(
                SourceIdentifier::try_from("1Day")?,
                BarTimestampBasis::PeriodStart,
                MarketBarSessionKind::Regular,
                MarketCalendarSessionConstraint::CompleteConsecutiveSessions,
            )],
            vec![
                MarketCalendarSession::try_new(
                    SourceIdentifier::try_from("regular-session")?,
                    Timestamp::from_unix_nanos(10),
                    Timestamp::from_unix_nanos(20),
                    MarketBarSessionKind::Regular,
                )?,
                MarketCalendarSession::try_new(
                    SourceIdentifier::try_from("early-close-session")?,
                    Timestamp::from_unix_nanos(40),
                    Timestamp::from_unix_nanos(45),
                    MarketBarSessionKind::Regular,
                )?,
            ],
            vec![
                MarketCalendarPeriod::try_new(
                    0,
                    Timestamp::from_unix_nanos(0),
                    Timestamp::from_unix_nanos(0),
                    Timestamp::from_unix_nanos(30),
                    0,
                    1,
                )?,
                MarketCalendarPeriod::try_new(
                    0,
                    Timestamp::from_unix_nanos(30),
                    Timestamp::from_unix_nanos(30),
                    Timestamp::from_unix_nanos(60),
                    1,
                    1,
                )?,
            ],
        ))?;
        let venue = VenueId::try_from("iex")?;
        let timeframe = SourceIdentifier::try_from("1Day")?;
        let series =
            authority.series_semantics_at(&venue, &timeframe, Timestamp::from_unix_nanos(150))?;
        let regular = authority.resolve_at(
            &venue,
            &timeframe,
            Timestamp::from_unix_nanos(0),
            Timestamp::from_unix_nanos(150),
        )?;
        let early_close = authority.resolve_at(
            &venue,
            &timeframe,
            Timestamp::from_unix_nanos(30),
            Timestamp::from_unix_nanos(150),
        )?;
        assert_eq!(
            regular.period_end_exclusive(),
            Timestamp::from_unix_nanos(30)
        );
        assert_eq!(
            early_close.period_end_exclusive(),
            Timestamp::from_unix_nanos(60)
        );
        assert_eq!(
            regular.session().evidence(),
            early_close.session().evidence()
        );
        assert_eq!(series.timestamp_basis(), regular.timestamp_basis());
        assert_eq!(&series.into_session(), regular.session());
        authority.revoke();
        assert_eq!(
            authority.series_semantics_at(&venue, &timeframe, Timestamp::from_unix_nanos(150),),
            Err(MarketCalendarError::Revoked)
        );

        let date = market_squawk_domain::CalendarDate::new(2024, 11, 29)?;
        let request = alpaca::AlpacaIexUtcCalendarFetchRequest::try_new(
            alpaca::AlpacaCalendarApiEnvironment::Paper,
            date,
        )?;
        assert_eq!(request.method(), "GET");
        assert_eq!(request.origin(), "https://paper-api.alpaca.markets");
        assert_eq!(
            request.path_and_query().as_str(),
            "/v3/calendar/IEX?start=2024-11-29&end=2024-11-29&timezone=UTC"
        );
        assert_eq!(request.requested_date(), date);
        let retrieved_at = Timestamp::from_unix_nanos(1_800_000_000_000_000_000);
        let raw_response = br#"{
            "market": {
                "acronym": "IEX",
                "name": "Investors Exchange",
                "timezone": "UTC",
                "bic": "IEXGUS33XXX",
                "mic": "IEXG",
                "future_market_field": {"retained_only": true}
            },
            "calendar": [{
                "date": "2024-11-29",
                "core_start": "2024-11-29T14:30:00Z",
                "core_end": "2024-11-29T18:00:00Z",
                "pre_start": "2024-11-29T09:00:00Z",
                "pre_end": "2024-11-29T14:30:00Z",
                "post_start": "2024-11-29T18:00:00Z",
                "post_end": "2024-11-30T01:00:00Z",
                "settlement_date": "2024-12-03",
                "future_day_field": [1, 2, 3]
            }],
            "future_envelope_field": "retained-only"
        }"#
        .to_vec()
        .into_boxed_slice();
        let fetch = alpaca::AlpacaIexUtcCalendarFetchResult::try_new(
            request,
            200,
            raw_response,
            ExactPayloadEvidence::from_content_digest(EvidenceDigest::new(
                DigestAlgorithm::Sha256,
                [10; 32],
            )),
            AvailabilityEvidence::local_first_observed(retrieved_at),
            retrieved_at,
        )?;
        let produced = alpaca::try_produce_alpaca_iex_utc_daily_calendar(fetch)?;
        let provider_timestamp = Timestamp::from_unix_nanos(1_732_856_400_000_000_000);
        let next_new_york_midnight = Timestamp::from_unix_nanos(1_732_942_800_000_000_000);
        let produced_bar =
            produced.resolve_at(&venue, &timeframe, provider_timestamp, retrieved_at)?;
        assert_eq!(produced_bar.period_start(), provider_timestamp);
        assert_eq!(produced_bar.period_end_exclusive(), next_new_york_midnight);
        assert_eq!(
            produced_bar.session().kind(),
            MarketBarSessionKind::ProviderDefined
        );
        produced.revoke();
        assert_eq!(
            produced.resolve_at(&venue, &timeframe, provider_timestamp, retrieved_at),
            Err(MarketCalendarError::Revoked)
        );
        Ok(())
    }

    fn exact_payload(bytes: &[u8]) -> ExactPayloadEvidence {
        ExactPayloadEvidence::from_content_digest(EvidenceDigest::new(
            DigestAlgorithm::Sha256,
            Sha256::digest(bytes).into(),
        ))
    }
}
