//! Native calendar membership attached only through an opaque source replay capability.

use super::*;
use market_squawk_adapter_alpaca::AlpacaRetainedCalendarSessions;

/// One genuine native session joined to its exact provider aggregation period and retained bar.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RetainedHistoryNativeSession {
    native_date: CalendarDate,
    opens_at: Timestamp,
    closes_at_exclusive: Timestamp,
    provider_timestamp: Option<Timestamp>,
    period_start: Option<Timestamp>,
    period_end_exclusive: Option<Timestamp>,
    bar_present: bool,
}
impl RetainedHistoryNativeSession {
    /// Native nominal session date.
    pub const fn native_date(&self) -> CalendarDate {
        self.native_date
    }
    /// Source-native regular session opening instant, distinct from daily period start.
    pub const fn opens_at(&self) -> Timestamp {
        self.opens_at
    }
    /// Source-native regular session closing instant, distinct from daily period end.
    pub const fn closes_at_exclusive(&self) -> Timestamp {
        self.closes_at_exclusive
    }
    /// Exact provider timestamp associated with this session by the source decoder.
    pub const fn provider_timestamp(&self) -> Option<Timestamp> {
        self.provider_timestamp
    }
    /// Independently retained source aggregation interval.
    pub const fn provider_period(&self) -> Option<(Timestamp, Timestamp)> {
        match (self.period_start, self.period_end_exclusive) {
            (Some(start), Some(end)) => Some((start, end)),
            _ => None,
        }
    }
    /// Whether this exact expected session has a selected retained bar.
    pub const fn bar_present(&self) -> bool {
        self.bar_present
    }
}

/// Non-forgeable native calendar replay joined to one immutable complete-history read.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RetainedHistoryNativeSessions {
    mapping_digest: EvidenceDigest,
    source_replay_digest: EvidenceDigest,
    capture_receipt_digest: EvidenceDigest,
    calendar_component_digest: Option<EvidenceDigest>,
    calendar_origin_content_digest: EvidenceDigest,
    calendar_capture_binding_digest: EvidenceDigest,
    published_at: Timestamp,
    received_at: Timestamp,
    sessions: Box<[RetainedHistoryNativeSession]>,
}
impl RetainedHistoryNativeSessions {
    /// Exact source replay, original history publication and ordered native-session mapping.
    pub const fn mapping_digest(&self) -> EvidenceDigest {
        self.mapping_digest
    }
    /// Exact source-owned native calendar replay identity.
    pub const fn source_replay_digest(&self) -> EvidenceDigest {
        self.source_replay_digest
    }
    /// Exact physical/logical capture identity bound by the creating generation.
    pub const fn capture_receipt_digest(&self) -> EvidenceDigest {
        self.capture_receipt_digest
    }
    /// Actual calendar component; an external calendar is never represented by metadata.
    pub const fn calendar_component_digest(&self) -> Option<EvidenceDigest> {
        self.calendar_component_digest
    }
    pub const fn calendar_origin_content_digest(&self) -> EvidenceDigest {
        self.calendar_origin_content_digest
    }
    pub const fn calendar_capture_binding_digest(&self) -> EvidenceDigest {
        self.calendar_capture_binding_digest
    }
    pub const fn published_at(&self) -> Timestamp {
        self.published_at
    }
    /// Original source response receipt and conservative local calendar availability.
    pub const fn received_at(&self) -> Timestamp {
        self.received_at
    }
    /// Every exact expected native session in original source order.
    pub fn sessions(&self) -> &[RetainedHistoryNativeSession] {
        &self.sessions
    }
}

impl CompleteMarketBarHistoryOutput {
    /// Returns genuine attached native sessions. Absence is an explicit source-evidence gap and
    /// cannot authorize guided next-session execution or a native-session harmonic workflow.
    pub const fn native_sessions(&self) -> Option<&RetainedHistoryNativeSessions> {
        self.native_sessions.as_ref()
    }

    /// Consumes an opaque source replay of the exact original captured calendar. Applications
    /// obtain that capability through their existing controlled raw store; no caller session
    /// vectors or provider timestamp aliases can be supplied here.
    pub fn try_with_native_sessions(
        mut self,
        replay: AlpacaRetainedCalendarSessions,
        control: &dyn market_squawk_platform::ResearchObjectControl,
    ) -> Result<Self, AnalyticalReadError> {
        let invalid = || AnalyticalReadError::InvalidMarketBarResult;
        control
            .checkpoint(market_squawk_platform::ResearchObjectControlPoint::BeforeVerification)
            .map_err(AnalyticalReadError::NativeSessionControl)?;
        let receipt = self.selection.receipt();
        let (component_ordinal, component_digest, component_pages) =
            receipt.session_calendar_component().ok_or_else(invalid)?;
        if self.native_sessions.is_some()
            || receipt.source_id().as_str() != "alpaca-basic-iex-market-data"
            || receipt.graph_purpose().as_str() != "alpaca-iex-historical-bars-and-calendar/v1"
            || replay.capture_receipt_digest().bytes() != receipt.capture_receipt_digest().bytes()
            || replay.component()
                != Some((
                    component_ordinal,
                    EvidenceDigest::new(DigestAlgorithm::Sha256, component_digest.bytes()),
                    component_pages,
                ))
            || replay.received_at() > receipt.published_at()
        {
            return Err(invalid());
        }
        if replay
            .sessions()
            .windows(2)
            .any(|pair| pair[0].provider_timestamp() >= pair[1].provider_timestamp())
            || self.bars.windows(2).any(|pair| {
                pair[0].time_semantics().provider_timestamp()
                    >= pair[1].time_semantics().provider_timestamp()
            })
        {
            return Err(invalid());
        }
        let mut sessions = Vec::new();
        sessions
            .try_reserve_exact(receipt.expected_provider_timestamps().len())
            .map_err(|_| invalid())?;
        let mut hash = Sha256::new();
        hash.update(b"market-squawk/retained-native-history-sessions/v1");
        hash.update(receipt.receipt_digest().bytes());
        hash.update(self.read_receipt.history_content_digest().bytes());
        hash.update(replay.replay_digest().bytes());
        for expected in receipt.expected_provider_timestamps() {
            control
                .checkpoint(market_squawk_platform::ResearchObjectControlPoint::BeforeVerification)
                .map_err(AnalyticalReadError::NativeSessionControl)?;
            let native_index = replay
                .sessions()
                .binary_search_by_key(&Some(*expected), |session| session.provider_timestamp())
                .map_err(|_| invalid())?;
            let native = &replay.sessions()[native_index];
            let period_start = native.period_start().ok_or_else(invalid)?;
            let period_end_exclusive = native.period_end_exclusive().ok_or_else(invalid)?;
            let bar = self
                .bars
                .binary_search_by_key(&Some(*expected), |bar| {
                    bar.time_semantics().provider_timestamp()
                })
                .ok()
                .map(|index| &self.bars[index]);
            if bar.is_some_and(|bar| {
                bar.time_semantics().period_start() != Some(period_start)
                    || bar.time_semantics().period_end_exclusive() != Some(period_end_exclusive)
            }) {
                return Err(invalid());
            }
            let session = RetainedHistoryNativeSession {
                native_date: native.date(),
                opens_at: native.opens_at(),
                closes_at_exclusive: native.closes_at_exclusive(),
                provider_timestamp: Some(*expected),
                period_start: Some(period_start),
                period_end_exclusive: Some(period_end_exclusive),
                bar_present: bar.is_some(),
            };
            hash.update(session.native_date.year().to_be_bytes());
            hash.update([session.native_date.month(), session.native_date.day()]);
            for clock in [
                session.opens_at,
                session.closes_at_exclusive,
                session.provider_timestamp.ok_or_else(invalid)?,
                session.period_start.ok_or_else(invalid)?,
                session.period_end_exclusive.ok_or_else(invalid)?,
            ] {
                hash.update(clock.unix_nanos().to_be_bytes());
            }
            hash.update([u8::from(session.bar_present)]);
            sessions.push(session);
        }
        let (start, end) = receipt.requested_range().ok_or_else(invalid)?;
        if replay
            .sessions()
            .iter()
            .filter(|session| {
                session
                    .provider_timestamp()
                    .is_some_and(|timestamp| timestamp >= start)
                    && session
                        .period_end_exclusive()
                        .is_some_and(|timestamp| timestamp <= end)
            })
            .count()
            != sessions.len()
        {
            return Err(invalid());
        }
        control
            .checkpoint(market_squawk_platform::ResearchObjectControlPoint::BeforeCommit)
            .map_err(AnalyticalReadError::NativeSessionControl)?;
        self.native_sessions = Some(RetainedHistoryNativeSessions {
            mapping_digest: EvidenceDigest::new(DigestAlgorithm::Sha256, hash.finalize().into()),
            source_replay_digest: replay.replay_digest(),
            capture_receipt_digest: replay.capture_receipt_digest(),
            calendar_component_digest: Some(EvidenceDigest::new(
                DigestAlgorithm::Sha256,
                component_digest.bytes(),
            )),
            calendar_origin_content_digest: EvidenceDigest::new(
                DigestAlgorithm::Sha256,
                self.read_receipt.origin_manifest().content_hash().bytes(),
            ),
            calendar_capture_binding_digest: EvidenceDigest::new(
                DigestAlgorithm::Sha256,
                receipt.binding_digest().bytes(),
            ),
            published_at: receipt.published_at(),
            received_at: replay.received_at(),
            sessions: sessions.into_boxed_slice(),
        });
        let mut read_hash = Sha256::new();
        read_hash.update(b"market-squawk/native-session-history-read/v1");
        read_hash.update(self.read_receipt.result_digest.bytes());
        read_hash.update(
            self.native_sessions
                .as_ref()
                .ok_or_else(invalid)?
                .mapping_digest
                .bytes(),
        );
        self.read_receipt.result_digest = Sha256Digest::new(read_hash.finalize().into());
        Ok(self)
    }
}

impl CompleteMarketBarHistoryOutput {
    /// Attaches independently retained genuine native sessions to original civil-date bars.
    /// Venue differences require the exact code-owned reviewed relation bound in the source graph.
    pub fn try_with_nominal_native_sessions(
        mut self,
        calendar: &crate::RetainedCorporateActionCalendar,
        control: &dyn market_squawk_platform::ResearchObjectControl,
    ) -> Result<Self, AnalyticalReadError> {
        let invalid = || AnalyticalReadError::InvalidMarketBarResult;
        let receipt = self.selection.receipt();
        let graph = receipt.date_windows().ok_or_else(invalid)?;
        let retained = graph.calendar();
        if self.native_sessions.is_some()
            || calendar.knowledge_cutoff() != self.read_receipt.knowledge_cutoff()
            || calendar.available_at() > self.read_receipt.knowledge_cutoff()
            || calendar.manifest().content_hash().bytes() != retained.origin_content_digest.bytes()
            || calendar.binding_digest() != retained.capture_binding_digest
            || !retained.relationship.matches(
                calendar.venue_id(),
                graph.venue_id(),
                graph.requested_dates(),
            )
            || !calendar
                .native_dates_in(graph.requested_dates())
                .eq(graph.sessions().iter().map(|row| row.date))
        {
            return Err(invalid());
        }
        let mut sessions = Vec::new();
        sessions
            .try_reserve_exact(graph.sessions().len())
            .map_err(|_| invalid())?;
        let mut hash = Sha256::new();
        hash.update(b"market-squawk/retained-nominal-native-history-sessions/v1");
        hash.update(receipt.receipt_digest().bytes());
        hash.update(self.read_receipt.history_content_digest().bytes());
        hash.update(calendar.evidence_digest().bytes());
        hash.update(retained.relationship.relationship_digest().bytes());
        for (bar, expected) in self.bars.iter().zip(graph.sessions()) {
            control
                .checkpoint(market_squawk_platform::ResearchObjectControlPoint::BeforeVerification)
                .map_err(AnalyticalReadError::NativeSessionControl)?;
            if bar.time_semantics() != &expected.time
                || bar
                    .time_semantics()
                    .nominal_daily_date()
                    .is_none_or(|date| date.date() != expected.date)
            {
                return Err(invalid());
            }
            let native = calendar
                .date_session_on(
                    expected.date,
                    self.read_receipt.knowledge_cutoff(),
                    self.read_receipt.knowledge_cutoff(),
                )
                .ok_or_else(invalid)?;
            hash.update(expected.date.year().to_be_bytes());
            hash.update([expected.date.month(), expected.date.day()]);
            hash.update(native.opens_at.unix_nanos().to_be_bytes());
            hash.update(native.closes_at_exclusive.unix_nanos().to_be_bytes());
            sessions.push(RetainedHistoryNativeSession {
                native_date: expected.date,
                opens_at: native.opens_at,
                closes_at_exclusive: native.closes_at_exclusive,
                provider_timestamp: None,
                period_start: None,
                period_end_exclusive: None,
                bar_present: true,
            });
        }
        if sessions.len() != graph.sessions().len() {
            return Err(invalid());
        }
        control
            .checkpoint(market_squawk_platform::ResearchObjectControlPoint::BeforeCommit)
            .map_err(AnalyticalReadError::NativeSessionControl)?;
        let mapping_digest = EvidenceDigest::new(DigestAlgorithm::Sha256, hash.finalize().into());
        self.native_sessions = Some(RetainedHistoryNativeSessions {
            mapping_digest,
            source_replay_digest: calendar.evidence_digest(),
            capture_receipt_digest: calendar.native_replay().capture_receipt_digest(),
            calendar_component_digest: None,
            calendar_origin_content_digest: retained.origin_content_digest,
            calendar_capture_binding_digest: retained.capture_binding_digest,
            published_at: calendar.available_at(),
            received_at: calendar.native_replay().received_at(),
            sessions: sessions.into_boxed_slice(),
        });
        let mut read_hash = Sha256::new();
        read_hash.update(b"market-squawk/native-session-history-read/v1");
        read_hash.update(self.read_receipt.result_digest.bytes());
        read_hash.update(mapping_digest.bytes());
        self.read_receipt.result_digest = Sha256Digest::new(read_hash.finalize().into());
        Ok(self)
    }
}
