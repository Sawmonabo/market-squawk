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
    sessions: RetainedHistorySessionRows,
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
    pub const fn sessions(&self) -> &RetainedHistorySessionRows {
        &self.sessions
    }
}

/// Complete native sessions with bounded fallible access. The representation remains private;
/// only the source replay join can construct a native-session authority.
#[derive(Clone, Debug)]
pub struct RetainedHistorySessionRows {
    storage: SessionStorage,
    count: usize,
    content_digest: [u8; 32],
}
#[derive(Clone, Debug)]
enum SessionStorage {
    Memory(Arc<[RetainedHistoryNativeSession]>),
    Disk {
        connection: Arc<std::sync::Mutex<rusqlite::Connection>>,
        _scratch: Arc<crate::OperationScratchDirectory>,
        deadline: Instant,
        cancellation: CancellationToken,
    },
}
impl PartialEq for RetainedHistorySessionRows {
    fn eq(&self, other: &Self) -> bool {
        self.count == other.count && self.content_digest == other.content_digest
    }
}
impl Eq for RetainedHistorySessionRows {}
impl RetainedHistorySessionRows {
    pub const fn len(&self) -> usize {
        self.count
    }
    pub const fn is_empty(&self) -> bool {
        self.count == 0
    }
    /// Loads one exact original session ordinal; storage/cancellation failures are explicit.
    pub fn get(
        &self,
        ordinal: usize,
    ) -> Result<Option<RetainedHistoryNativeSession>, AnalyticalReadError> {
        if ordinal >= self.count {
            return Ok(None);
        }
        match &self.storage {
            SessionStorage::Memory(rows) => Ok(rows.get(ordinal).cloned()),
            SessionStorage::Disk {
                connection,
                deadline,
                cancellation,
                ..
            } => {
                native_checkpoint(*deadline, cancellation)?;
                let connection = connection.lock().map_err(|_| invalid_native())?;
                let (bytes, digest): (Vec<u8>, Vec<u8>) = connection
                    .query_row(
                        "SELECT payload,digest FROM native_sessions WHERE ordinal=?1",
                        [i64::try_from(ordinal).map_err(|_| invalid_native())?],
                        |row| Ok((row.get(0)?, row.get(1)?)),
                    )
                    .map_err(native_storage)?;
                if digest.as_slice() != Sha256::digest(&bytes).as_slice() {
                    return Err(invalid_native());
                }
                SessionWire::deserialize(&mut serde_json::Deserializer::from_slice(&bytes))
                    .map(Some)
                    .map_err(|_| invalid_native())
            }
        }
    }
    pub fn first(&self) -> Result<Option<RetainedHistoryNativeSession>, AnalyticalReadError> {
        self.get(0)
    }
    pub fn last(&self) -> Result<Option<RetainedHistoryNativeSession>, AnalyticalReadError> {
        self.count
            .checked_sub(1)
            .map(|index| self.get(index))
            .unwrap_or(Ok(None))
    }
    pub fn iter(
        &self,
    ) -> impl Iterator<Item = Result<RetainedHistoryNativeSession, AnalyticalReadError>> + '_ {
        (0..self.count).map(|index| self.get(index)?.ok_or_else(invalid_native))
    }
    /// Performs a checked date lookup in the source's strictly ordered native session calendar.
    pub fn find_date(
        &self,
        date: CalendarDate,
    ) -> Result<Option<RetainedHistoryNativeSession>, AnalyticalReadError> {
        let (mut low, mut high) = (0, self.count);
        while low < high {
            let middle = low + (high - low) / 2;
            let row = self.get(middle)?.ok_or_else(invalid_native)?;
            match row.native_date.cmp(&date) {
                std::cmp::Ordering::Less => low = middle + 1,
                std::cmp::Ordering::Greater => high = middle,
                std::cmp::Ordering::Equal => return Ok(Some(row)),
            }
        }
        Ok(None)
    }
    /// Compares the complete row content while propagating failures from either retained store.
    pub fn same_rows(&self, other: &Self) -> Result<bool, AnalyticalReadError> {
        if self.count != other.count {
            return Ok(false);
        }
        for (left, right) in self.iter().zip(other.iter()) {
            if left? != right? {
                return Ok(false);
            }
        }
        Ok(true)
    }
}

struct SessionSink {
    storage: SinkStorage,
    count: usize,
    digest: Sha256,
    previous_date: Option<CalendarDate>,
}
enum SinkStorage {
    Memory(Vec<RetainedHistoryNativeSession>),
    Disk {
        connection: Arc<std::sync::Mutex<rusqlite::Connection>>,
        scratch: Arc<crate::OperationScratchDirectory>,
        deadline: Instant,
        cancellation: CancellationToken,
    },
}
impl SessionSink {
    fn memory() -> Self {
        Self {
            storage: SinkStorage::Memory(Vec::new()),
            count: 0,
            digest: Sha256::new(),
            previous_date: None,
        }
    }
    fn disk(
        connection: Arc<std::sync::Mutex<rusqlite::Connection>>,
        scratch: Arc<crate::OperationScratchDirectory>,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<Self, AnalyticalReadError> {
        native_checkpoint(deadline, &cancellation)?;
        connection.lock().map_err(|_|invalid_native())?.execute_batch(
            "CREATE TABLE native_sessions(ordinal INTEGER PRIMARY KEY,payload BLOB NOT NULL,digest BLOB NOT NULL); BEGIN;"
        ).map_err(native_storage)?;
        Ok(Self {
            storage: SinkStorage::Disk {
                connection,
                scratch,
                deadline,
                cancellation,
            },
            count: 0,
            digest: Sha256::new(),
            previous_date: None,
        })
    }
    fn push(&mut self, row: RetainedHistoryNativeSession) -> Result<(), AnalyticalReadError> {
        if self
            .previous_date
            .is_some_and(|prior| prior >= row.native_date)
        {
            return Err(invalid_native());
        }
        self.previous_date = Some(row.native_date);
        let mut bytes = Vec::new();
        SessionWire::serialize(&row, &mut serde_json::Serializer::new(&mut bytes))
            .map_err(|_| invalid_native())?;
        self.digest.update((bytes.len() as u64).to_be_bytes());
        self.digest.update(&bytes);
        match &mut self.storage {
            SinkStorage::Memory(rows) => {
                rows.try_reserve(1).map_err(|_| invalid_native())?;
                rows.push(row);
            }
            SinkStorage::Disk {
                connection,
                deadline,
                cancellation,
                ..
            } => {
                native_checkpoint(*deadline, cancellation)?;
                let digest: [u8; 32] = Sha256::digest(&bytes).into();
                connection
                    .lock()
                    .map_err(|_| invalid_native())?
                    .execute(
                        "INSERT INTO native_sessions VALUES(?1,?2,?3)",
                        rusqlite::params![
                            i64::try_from(self.count).map_err(|_| invalid_native())?,
                            bytes,
                            digest.as_slice()
                        ],
                    )
                    .map_err(native_storage)?;
            }
        }
        self.count = self.count.checked_add(1).ok_or_else(invalid_native)?;
        Ok(())
    }
    fn finish(self) -> Result<RetainedHistorySessionRows, AnalyticalReadError> {
        let storage = match self.storage {
            SinkStorage::Memory(rows) => SessionStorage::Memory(rows.into()),
            SinkStorage::Disk {
                connection,
                scratch,
                deadline,
                cancellation,
            } => {
                native_checkpoint(deadline, &cancellation)?;
                connection
                    .lock()
                    .map_err(|_| invalid_native())?
                    .execute_batch("COMMIT")
                    .map_err(native_storage)?;
                SessionStorage::Disk {
                    connection,
                    _scratch: scratch,
                    deadline,
                    cancellation,
                }
            }
        };
        Ok(RetainedHistorySessionRows {
            storage,
            count: self.count,
            content_digest: self.digest.finalize().into(),
        })
    }
}
impl RetainedHistoryNativeSessions {
    pub(super) fn into_disk(
        mut self,
        connection: Arc<std::sync::Mutex<rusqlite::Connection>>,
        scratch: Arc<crate::OperationScratchDirectory>,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<Self, AnalyticalReadError> {
        let mut sink = SessionSink::disk(connection, scratch, deadline, cancellation)?;
        for row in self.sessions.iter() {
            sink.push(row?)?;
        }
        self.sessions = sink.finish()?;
        Ok(self)
    }
}

impl CompleteMarketBarHistoryOutput {
    pub const fn native_sessions(&self) -> Option<&RetainedHistoryNativeSessions> {
        self.native_sessions.as_ref()
    }
    /// Joins genuine opaque native calendar replay without changing any original source value.
    pub fn try_with_native_sessions(
        mut self,
        replay: AlpacaRetainedCalendarSessions,
        control: &dyn market_squawk_platform::ResearchObjectControl,
    ) -> Result<Self, AnalyticalReadError> {
        if self.native_sessions.is_some() {
            return Err(invalid_native());
        }
        let native = join_provider(
            &self.selection,
            &self.read_receipt,
            self.bars.iter().cloned().map(Ok),
            replay,
            control,
            SessionSink::memory(),
        )?;
        bind_native_read(&mut self.read_receipt, &native);
        self.native_sessions = Some(native);
        Ok(self)
    }
    /// Joins independently retained native sessions to original civil-date bars.
    pub fn try_with_nominal_native_sessions(
        mut self,
        calendar: &crate::RetainedCorporateActionCalendar,
        control: &dyn market_squawk_platform::ResearchObjectControl,
    ) -> Result<Self, AnalyticalReadError> {
        if self.native_sessions.is_some() {
            return Err(invalid_native());
        }
        let native = join_nominal(
            &self.selection,
            &self.read_receipt,
            self.bars.iter().cloned().map(Ok),
            calendar,
            control,
            SessionSink::memory(),
        )?;
        bind_native_read(&mut self.read_receipt, &native);
        self.native_sessions = Some(native);
        Ok(self)
    }
}
impl CompleteMarketBarHistoryCursor {
    /// Attaches the original source calendar directly to sealed disk observations.
    pub fn try_with_native_sessions(
        mut self,
        replay: AlpacaRetainedCalendarSessions,
        control: &dyn market_squawk_platform::ResearchObjectControl,
    ) -> Result<Self, AnalyticalReadError> {
        if self.native_sessions.is_some() {
            return Err(invalid_native());
        }
        let sink = SessionSink::disk(
            Arc::clone(&self.connection),
            self.operation_scratch(),
            self.deadline,
            self.cancellation.clone(),
        )?;
        let native = join_provider(
            &self.selection,
            &self.read_receipt,
            self.bars(),
            replay,
            control,
            sink,
        )?;
        bind_native_read(&mut self.read_receipt, &native);
        self.native_sessions = Some(native);
        Ok(self)
    }
    /// Rejoins genuine native dates without materializing complete bars, actions or sessions.
    pub fn try_with_nominal_native_sessions(
        mut self,
        calendar: &crate::RetainedCorporateActionCalendar,
        control: &dyn market_squawk_platform::ResearchObjectControl,
    ) -> Result<Self, AnalyticalReadError> {
        if self.native_sessions.is_some() {
            return Err(invalid_native());
        }
        let sink = SessionSink::disk(
            Arc::clone(&self.connection),
            self.operation_scratch(),
            self.deadline,
            self.cancellation.clone(),
        )?;
        let native = join_nominal(
            &self.selection,
            &self.read_receipt,
            self.bars(),
            calendar,
            control,
            sink,
        )?;
        bind_native_read(&mut self.read_receipt, &native);
        self.native_sessions = Some(native);
        Ok(self)
    }
}

fn join_provider(
    selection: &CompleteMarketBarHistorySelection,
    read_receipt: &CompleteMarketBarHistoryReadReceipt,
    bars: impl Iterator<Item = Result<MarketBarObservation, AnalyticalReadError>>,
    replay: AlpacaRetainedCalendarSessions,
    control: &dyn market_squawk_platform::ResearchObjectControl,
    mut sink: SessionSink,
) -> Result<RetainedHistoryNativeSessions, AnalyticalReadError> {
    let invalid = invalid_native;
    control
        .checkpoint(market_squawk_platform::ResearchObjectControlPoint::BeforeVerification)
        .map_err(AnalyticalReadError::NativeSessionControl)?;
    let receipt = selection.receipt();
    let (component_ordinal, component_digest, component_pages) =
        receipt.session_calendar_component().ok_or_else(invalid)?;
    if receipt.source_id().as_str() != "alpaca-basic-iex-market-data"
        || receipt.graph_purpose().as_str() != "alpaca-iex-historical-bars-and-calendar/v1"
        || replay.capture_receipt_digest().bytes() != receipt.capture_receipt_digest().bytes()
        || replay.component()
            != Some((
                component_ordinal,
                EvidenceDigest::new(DigestAlgorithm::Sha256, component_digest.bytes()),
                component_pages,
            ))
        || replay.received_at() > receipt.published_at()
        || replay
            .sessions()
            .windows(2)
            .any(|pair| pair[0].provider_timestamp() >= pair[1].provider_timestamp())
    {
        return Err(invalid());
    }
    let mut bars = bars;
    let mut bar = bars.next().transpose()?;
    let mut previous_bar = None;
    let mut hash = Sha256::new();
    hash.update(b"market-squawk/retained-native-history-sessions/v1");
    hash.update(receipt.receipt_digest().bytes());
    hash.update(read_receipt.history_content_digest().bytes());
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
        let bar_timestamp = bar
            .as_ref()
            .map(|bar| {
                bar.time_semantics()
                    .provider_timestamp()
                    .ok_or_else(invalid)
            })
            .transpose()?;
        if bar_timestamp.is_some_and(|timestamp| {
            timestamp < *expected || previous_bar.is_some_and(|prior| prior >= timestamp)
        }) {
            return Err(invalid());
        }
        let present = bar_timestamp == Some(*expected);
        if present {
            let selected = bar.take().ok_or_else(invalid)?;
            if selected.time_semantics().period_start() != Some(period_start)
                || selected.time_semantics().period_end_exclusive() != Some(period_end_exclusive)
            {
                return Err(invalid());
            }
            previous_bar = bar_timestamp;
            bar = bars.next().transpose()?;
        }
        let session = RetainedHistoryNativeSession {
            native_date: native.date(),
            opens_at: native.opens_at(),
            closes_at_exclusive: native.closes_at_exclusive(),
            provider_timestamp: Some(*expected),
            period_start: Some(period_start),
            period_end_exclusive: Some(period_end_exclusive),
            bar_present: present,
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
        sink.push(session)?;
    }
    if bar.is_some() {
        return Err(invalid());
    }
    let (start, end) = receipt.requested_range().ok_or_else(invalid)?;
    if replay
        .sessions()
        .iter()
        .filter(|session| {
            session.provider_timestamp().is_some_and(|at| at >= start)
                // The provider request includes its end; canonical periods exclude theirs.
                && session.period_end_exclusive().is_some_and(|at| {
                    at.checked_sub_nanos(1)
                        .is_ok_and(|last_included| last_included <= end)
                })
        })
        .count()
        != sink.count
    {
        return Err(invalid());
    }
    control
        .checkpoint(market_squawk_platform::ResearchObjectControlPoint::BeforeCommit)
        .map_err(AnalyticalReadError::NativeSessionControl)?;
    Ok(RetainedHistoryNativeSessions {
        mapping_digest: EvidenceDigest::new(DigestAlgorithm::Sha256, hash.finalize().into()),
        source_replay_digest: replay.replay_digest(),
        capture_receipt_digest: replay.capture_receipt_digest(),
        calendar_component_digest: Some(EvidenceDigest::new(
            DigestAlgorithm::Sha256,
            component_digest.bytes(),
        )),
        calendar_origin_content_digest: EvidenceDigest::new(
            DigestAlgorithm::Sha256,
            read_receipt.origin_manifest().content_hash().bytes(),
        ),
        calendar_capture_binding_digest: EvidenceDigest::new(
            DigestAlgorithm::Sha256,
            receipt.binding_digest().bytes(),
        ),
        published_at: receipt.published_at(),
        received_at: replay.received_at(),
        sessions: sink.finish()?,
    })
}
fn join_nominal(
    selection: &CompleteMarketBarHistorySelection,
    read_receipt: &CompleteMarketBarHistoryReadReceipt,
    bars: impl Iterator<Item = Result<MarketBarObservation, AnalyticalReadError>>,
    calendar: &crate::RetainedCorporateActionCalendar,
    control: &dyn market_squawk_platform::ResearchObjectControl,
    mut sink: SessionSink,
) -> Result<RetainedHistoryNativeSessions, AnalyticalReadError> {
    let invalid = invalid_native;
    let receipt = selection.receipt();
    let graph = receipt.date_windows().ok_or_else(invalid)?;
    let retained = graph.calendar();
    if calendar.knowledge_cutoff() != read_receipt.knowledge_cutoff()
        || calendar.available_at() > read_receipt.knowledge_cutoff()
        || calendar.manifest().content_hash().bytes() != retained.origin_content_digest.bytes()
        || calendar.binding_digest() != retained.capture_binding_digest
        || !retained.relationship.matches(
            calendar.venue_id(),
            graph.venue_id(),
            graph.requested_dates(),
        )
    {
        return Err(invalid());
    }
    let mut hash = Sha256::new();
    hash.update(b"market-squawk/retained-nominal-native-history-sessions/v1");
    hash.update(receipt.receipt_digest().bytes());
    hash.update(read_receipt.history_content_digest().bytes());
    hash.update(calendar.evidence_digest().bytes());
    hash.update(retained.relationship.relationship_digest().bytes());
    let mut bars = bars;
    let mut dates = Sha256::new();
    dates.update(b"market-squawk/market-bar-history-original-dates/v1");
    dates.update((receipt.bar_count() as u64).to_be_bytes());
    for date in calendar.native_dates_in(graph.requested_dates()) {
        control
            .checkpoint(market_squawk_platform::ResearchObjectControlPoint::BeforeVerification)
            .map_err(AnalyticalReadError::NativeSessionControl)?;
        let bar = bars.next().transpose()?.ok_or_else(invalid)?;
        // The sealed history cursor already verifies the complete canonical bar digest and
        // original nominal time semantics. Rejoin its dates directly with calendar authority.
        if bar
            .time_semantics()
            .nominal_daily_date()
            .is_none_or(|value| value.date() != date)
        {
            return Err(invalid());
        }
        let native = calendar
            .date_session_on(
                date,
                read_receipt.knowledge_cutoff(),
                read_receipt.knowledge_cutoff(),
            )
            .ok_or_else(invalid)?;
        dates.update(date.year().to_be_bytes());
        dates.update([date.month(), date.day()]);
        hash.update(date.year().to_be_bytes());
        hash.update([date.month(), date.day()]);
        hash.update(native.opens_at.unix_nanos().to_be_bytes());
        hash.update(native.closes_at_exclusive.unix_nanos().to_be_bytes());
        sink.push(RetainedHistoryNativeSession {
            native_date: date,
            opens_at: native.opens_at,
            closes_at_exclusive: native.closes_at_exclusive,
            provider_timestamp: None,
            period_start: None,
            period_end_exclusive: None,
            bar_present: true,
        })?;
    }
    if bars.next().transpose()?.is_some()
        || sink.count != receipt.bar_count()
        || dates.finalize().as_slice() != receipt.expected_timestamp_set_digest().bytes()
    {
        return Err(invalid());
    }
    control
        .checkpoint(market_squawk_platform::ResearchObjectControlPoint::BeforeCommit)
        .map_err(AnalyticalReadError::NativeSessionControl)?;
    Ok(RetainedHistoryNativeSessions {
        mapping_digest: EvidenceDigest::new(DigestAlgorithm::Sha256, hash.finalize().into()),
        source_replay_digest: calendar.evidence_digest(),
        capture_receipt_digest: calendar.native_replay().capture_receipt_digest(),
        calendar_component_digest: None,
        calendar_origin_content_digest: retained.origin_content_digest,
        calendar_capture_binding_digest: retained.capture_binding_digest,
        published_at: calendar.available_at(),
        received_at: calendar.native_replay().received_at(),
        sessions: sink.finish()?,
    })
}
fn bind_native_read(
    receipt: &mut CompleteMarketBarHistoryReadReceipt,
    native: &RetainedHistoryNativeSessions,
) {
    let mut hash = Sha256::new();
    hash.update(b"market-squawk/native-session-history-read/v1");
    hash.update(receipt.result_digest.bytes());
    hash.update(native.mapping_digest.bytes());
    receipt.result_digest = Sha256Digest::new(hash.finalize().into());
}
fn invalid_native() -> AnalyticalReadError {
    AnalyticalReadError::InvalidMarketBarResult
}
fn native_storage(error: rusqlite::Error) -> AnalyticalReadError {
    match error.sqlite_error_code() {
        Some(rusqlite::ErrorCode::DiskFull) => QueryError::SpillStorageExhausted.into(),
        Some(rusqlite::ErrorCode::OperationInterrupted) => QueryError::Cancelled.into(),
        _ => invalid_native(),
    }
}
fn native_checkpoint(
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<(), AnalyticalReadError> {
    if cancellation.is_cancelled() {
        Err(QueryError::Cancelled.into())
    } else if Instant::now() >= deadline {
        Err(QueryError::DeadlineExceeded.into())
    } else {
        Ok(())
    }
}
#[derive(serde::Serialize, serde::Deserialize)]
#[serde(remote = "RetainedHistoryNativeSession")]
struct SessionWire {
    native_date: CalendarDate,
    opens_at: Timestamp,
    closes_at_exclusive: Timestamp,
    provider_timestamp: Option<Timestamp>,
    period_start: Option<Timestamp>,
    period_end_exclusive: Option<Timestamp>,
    bar_present: bool,
}
