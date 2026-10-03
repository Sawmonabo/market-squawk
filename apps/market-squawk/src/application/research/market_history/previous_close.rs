//! Persisted completed-session display evidence over original canonical history and calendar.

use super::{MarketHistoryReadCapability, MarketHistoryUnavailableReason, unavailable_reason};
use crate::{
    ResearchService,
    application::{
        market_calendar::CompletedMarketSessionRead,
        model::forecast::{
            authorize_projection_parents, chart_storage_error, recheck_projection_parents,
        },
        research::corporate_actions::map_research_error,
    },
};
use market_squawk_data::{
    AnalyticalReadError, ChartProjectionRow, CompleteMarketBarHistorySelection, DatasetManifestRef,
    LatestCanonicalMarketBarHistoryWindowRequest, LatestCanonicalMarketBarHistoryWindowSelection,
    MarketHistorySelectionPolicy,
};
use market_squawk_domain::{
    CalendarDate, Currency, DataQuality, InstrumentId, MarketBarAdjustment, Money, Timestamp,
};
use market_squawk_modeling::ForecastArtifactManifestRecord;
use market_squawk_services::{RequestContext, ServiceError};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

/// Display evidence only; it carries no current-price or execution authority.
pub(crate) struct PreviousClose {
    selection: CompleteMarketBarHistorySelection,
    metadata: CloseMetadata,
    point: CloseComparisonPoint,
}

/// An original observation affiliated with an admitted native session and its preceding close.
/// This value never carries current-mark or publication authority.
pub(crate) struct DailyPriceComparison {
    close: ClosePoint,
    session: DisplaySession,
}

impl DailyPriceComparison {
    pub(crate) const fn close(&self) -> Money {
        self.close.close
    }
    pub(crate) const fn instrument_id(&self) -> InstrumentId {
        self.close.instrument_id
    }
    pub(crate) const fn native_date(&self) -> CalendarDate {
        self.close.native_date
    }
    pub(crate) const fn session_close(&self) -> Timestamp {
        self.close.session_close
    }
    pub(crate) const fn price_session_date(&self) -> CalendarDate {
        self.session.date
    }
    pub(crate) const fn price_session_period(&self) -> (Timestamp, Timestamp) {
        (self.session.starts_at, self.session.ends_at)
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct DisplaySession {
    date: CalendarDate,
    starts_at: Timestamp,
    ends_at: Timestamp,
    opens_at: Timestamp,
    closes_at: Timestamp,
}
impl DisplaySession {
    fn contains(&self, at: Timestamp) -> bool {
        self.starts_at <= at && at < self.ends_at
    }
    fn from_close(close: &ClosePoint) -> Self {
        let (starts_at, ends_at) = close
            .provider_period
            .unwrap_or((close.session_open, close.session_close));
        Self {
            date: close.native_date,
            starts_at,
            ends_at,
            opens_at: close.session_open,
            closes_at: close.session_close,
        }
    }
    fn valid(&self) -> bool {
        self.starts_at <= self.opens_at
            && self.opens_at < self.closes_at
            && self.closes_at <= self.ends_at
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct DisplayCalendar {
    origin: ForecastArtifactManifestRecord,
    binding: [u8; 32],
    replay: [u8; 32],
    available_at: Timestamp,
    received_at: Timestamp,
    complete_from: Timestamp,
    complete_until: Timestamp,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CloseComparisonPoint {
    latest: ClosePoint,
    prior: Option<ClosePoint>,
    latest_ordinal: usize,
    prior_ordinal: Option<usize>,
    latest_session: DisplaySession,
    successor: Option<DisplaySession>,
}
/// Receives every admitted native calendar ordinal, including genuine missing bars.
/// Missing sessions break adjacency; a later bar never rolls over the missing baseline.
#[derive(Default)]
struct CompletedCloseAccumulator {
    latest: Option<CloseComparisonPoint>,
    previous: Option<(usize, ClosePoint)>,
}
impl CompletedCloseAccumulator {
    fn observe(&mut self, ordinal: usize, close: Option<ClosePoint>) -> Result<(), ServiceError> {
        let Some(close) = close else {
            self.previous = None;
            return Ok(());
        };
        if self
            .previous
            .as_ref()
            .is_some_and(|(previous, _)| previous.checked_add(1) != Some(ordinal))
        {
            return Err(ServiceError::InvalidResult);
        }
        let prior = self.previous.take();
        self.latest = Some(CloseComparisonPoint {
            latest_session: DisplaySession::from_close(&close),
            latest: close.clone(),
            prior_ordinal: prior.as_ref().map(|(ordinal, _)| *ordinal),
            prior: prior.map(|(_, point)| point),
            latest_ordinal: ordinal,
            successor: None,
        });
        self.previous = Some((ordinal, close));
        Ok(())
    }
}

impl CloseComparisonPoint {
    fn comparison(
        &self,
        observed: Timestamp,
        completed_close: bool,
    ) -> Option<DailyPriceComparison> {
        let (close, session) = if completed_close {
            if observed != self.latest.session_close {
                return None;
            }
            (self.prior.as_ref()?, &self.latest_session)
        } else if self.latest_session.contains(observed) {
            (self.prior.as_ref()?, &self.latest_session)
        } else {
            let successor = self
                .successor
                .as_ref()
                .filter(|session| session.contains(observed))?;
            (&self.latest, successor)
        };
        if close.native_date >= session.date || close.session_close >= observed {
            return None;
        }
        Some(DailyPriceComparison {
            close: close.clone(),
            session: session.clone(),
        })
    }
}

/// One bounded lookup stage; Drop also attributes errors and abandoned request futures.
struct CloseReadStage<'a> {
    context: &'a RequestContext,
    instrument_id: Option<InstrumentId>,
    stage: &'static str,
    started: Instant,
    completed: bool,
}

impl<'a> CloseReadStage<'a> {
    fn new(
        context: &'a RequestContext,
        instrument_id: Option<InstrumentId>,
        stage: &'static str,
    ) -> Self {
        Self {
            context,
            instrument_id,
            stage,
            started: Instant::now(),
            completed: false,
        }
    }

    fn complete(mut self) {
        self.completed = true;
    }
}

impl Drop for CloseReadStage<'_> {
    fn drop(&mut self) {
        tracing::debug!(
            request_id = ?self.context.request_id(),
            instrument_id = ?self.instrument_id,
            stage = self.stage,
            completed = self.completed,
            elapsed_ms = %self.started.elapsed().as_millis(),
            remaining_at_stage_entry_ms = %self.context.deadline().saturating_duration_since(self.started).as_millis(),
            "previous close read stage finished"
        );
    }
}

/// Stable original evidence only: request cutoffs, descendant selections and rights expire
/// independently and must never change an immutable publication's projection.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CloseMetadata {
    publication: [u8; 32],
    origin: ForecastArtifactManifestRecord,
    artifact: String,
    object_ordinal: u16,
    object_content: [u8; 32],
    object_lineage: [u8; 32],
    object_rows: u64,
    object_bytes: u64,
    history_content: [u8; 32],
    bar_count: usize,
    native_mapping: [u8; 32],
    calendar_replay: [u8; 32],
    calendar_capture: [u8; 32],
    calendar_component: Option<[u8; 32]>,
    calendar_origin: [u8; 32],
    calendar_binding: [u8; 32],
    calendar_published_at: Timestamp,
    calendar_received_at: Timestamp,
    display_calendar: Option<DisplayCalendar>,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ClosePoint {
    instrument_id: InstrumentId,
    close: Money,
    native_date: CalendarDate,
    session_open: Timestamp,
    session_close: Timestamp,
    provider_timestamp: Option<Timestamp>,
    provider_period: Option<(Timestamp, Timestamp)>,
    available_at: Timestamp,
    received_at: Timestamp,
    ingested_at: Timestamp,
    quality: DataQuality,
}

impl PreviousClose {
    pub(crate) const fn instrument_id(&self) -> InstrumentId {
        self.point.latest.instrument_id
    }

    pub(crate) const fn currency(&self) -> Currency {
        self.point.latest.close.currency()
    }

    pub(crate) const fn close(&self) -> Money {
        self.point.latest.close
    }

    pub(crate) const fn session_close(&self) -> Timestamp {
        self.point.latest.session_close
    }

    pub(crate) const fn native_date(&self) -> CalendarDate {
        self.point.latest.native_date
    }
}

impl MarketHistoryReadCapability {
    /// Reads a selected page without reconstructing history or replaying raw captures.
    /// Absence or denied source use stays local to its instrument; integrity failures do not.
    pub(crate) async fn read_latest_previous_closes(
        &self,
        research: &ResearchService,
        instruments: &[InstrumentId],
        knowledge_cutoff: Timestamp,
        context: &RequestContext,
    ) -> Result<Vec<Option<PreviousClose>>, ServiceError> {
        check(context)?;
        let selections = self
            .select_previous_closes(research, instruments, knowledge_cutoff, context)
            .await?;
        if selections.len() != instruments.len() {
            return Err(ServiceError::InvalidResult);
        }
        let mut selected = Vec::new();
        let mut positions = Vec::new();
        let mut closes: Vec<_> = instruments.iter().map(|_| None).collect();
        for (index, selection) in selections.into_iter().enumerate() {
            match selection {
                Ok(Some(selection)) => {
                    if selection.selection().receipt().instrument_id() != instruments[index] {
                        return Err(ServiceError::InvalidResult);
                    }
                    positions.push(index);
                    selected.push(selection.selection().clone());
                }
                Ok(None) | Err(ServiceError::Unavailable | ServiceError::Unauthorized) => {}
                Err(error) => return Err(error),
            }
        }
        let projected =
            read_projections(research, &selected, None, knowledge_cutoff, context).await?;
        if projected.len() != positions.len() {
            return Err(ServiceError::InvalidResult);
        }
        for (index, close) in positions.into_iter().zip(projected) {
            closes[index] = match close {
                Ok(close) => close,
                Err(ServiceError::Unavailable | ServiceError::Unauthorized) => None,
                Err(error) => return Err(error),
            };
        }
        check(context)?;
        Ok(closes)
    }

    /// Selects a daily baseline for the actual final displayed observation. The history-only
    /// pair already handles closed-session display. A newer session additionally requires an
    /// original, prepared calendar proof; timestamps after the latest close do not imply it.
    pub(crate) async fn read_daily_price_comparisons(
        &self,
        research: &ResearchService,
        closes: &[Option<PreviousClose>],
        observations: &[Option<(Timestamp, bool)>],
        cutoff: Timestamp,
        context: &RequestContext,
    ) -> Result<Vec<Option<DailyPriceComparison>>, ServiceError> {
        if closes.len() != observations.len() {
            return Err(ServiceError::InvalidRequest);
        }
        let mut results = Vec::with_capacity(closes.len());
        let mut pending = Vec::new();
        for (index, (close, observation)) in closes.iter().zip(observations).enumerate() {
            check(context)?;
            let comparison =
                close
                    .as_ref()
                    .zip(*observation)
                    .and_then(|(close, (at, is_close))| {
                        (at <= cutoff)
                            .then(|| close.point.comparison(at, is_close))
                            .flatten()
                    });
            if comparison.is_none()
                && close
                    .as_ref()
                    .zip(*observation)
                    .is_some_and(|(close, (at, is_close))| {
                        !is_close
                            && at <= cutoff
                            && at >= close.point.latest.session_open
                            && !close.point.latest_session.contains(at)
                    })
            {
                pending.push(index);
            }
            results.push(comparison);
        }
        if !pending.is_empty() {
            if let Some(origin) = research.market_display_calendar_origin() {
                let selections = pending
                    .iter()
                    .map(|index| {
                        closes[*index]
                            .as_ref()
                            .map(|close| close.selection.clone())
                            .ok_or(ServiceError::InvalidResult)
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                let projected =
                    read_projections(research, &selections, Some(&origin), cutoff, context).await?;
                for (index, projected) in pending.into_iter().zip(projected) {
                    let (at, is_close) = observations[index].ok_or(ServiceError::InvalidResult)?;
                    match projected {
                        Ok(Some(close)) => results[index] = close.point.comparison(at, is_close),
                        Ok(None) | Err(ServiceError::Unauthorized | ServiceError::Unavailable) => {}
                        Err(error) => return Err(error),
                    }
                }
            }
        }
        check(context)?;
        Ok(results)
    }

    /// Extends the original latest/prior projection with the exact calendar's immediately
    /// succeeding session. Preparation may replay once; product reads only reopen this row.
    pub(crate) async fn prepare_daily_price_comparison(
        &self,
        research: &ResearchService,
        instrument: InstrumentId,
        calendar: Option<&CompletedMarketSessionRead>,
        cutoff: Timestamp,
        context: &RequestContext,
    ) -> Result<Option<PreviousClose>, ServiceError> {
        let Some(mut close) = self
            .prepare_latest_previous_close(research, instrument, cutoff, context)
            .await?
        else {
            return Ok(None);
        };
        let Some(calendar) = calendar else {
            return Ok(Some(close));
        };
        // This calendar is IEX-native. Another provider's nominal calendar must not silently
        // inherit its day/venue conventions merely because regular endpoints happen to match.
        if close.selection.receipt().source_id().as_str() != "alpaca-basic-iex-market-data"
            || calendar.venue_id().as_str() != "iex"
        {
            return Ok(Some(close));
        }
        let origin = calendar.source_action_calendar().manifest();
        let mut stored = read_projections(
            research,
            std::slice::from_ref(&close.selection),
            Some(origin),
            cutoff,
            context,
        )
        .await?;
        if let Some(stored) = stored.pop().ok_or(ServiceError::InvalidResult)?? {
            return Ok(Some(stored));
        }
        let replay = calendar.native_session_replay();
        let Ok(index) = replay
            .sessions()
            .binary_search_by_key(&close.native_date(), |session| session.date())
        else {
            return Ok(Some(close));
        };
        let latest = &replay.sessions()[index];
        if latest.opens_at() != close.point.latest.session_open
            || latest.closes_at_exclusive() != close.session_close()
            || calendar.available_at() > cutoff
            || replay.received_at() > cutoff
        {
            return Err(ServiceError::InvalidResult);
        }
        if let Some(prior) = &close.point.prior {
            if index
                .checked_sub(1)
                .and_then(|index| replay.sessions().get(index))
                .is_some_and(|session| {
                    session.date() != prior.native_date
                        || session.opens_at() != prior.session_open
                        || session.closes_at_exclusive() != prior.session_close
                })
            {
                return Err(ServiceError::InvalidResult);
            }
        }
        let session = |native: &market_squawk_adapter_alpaca::AlpacaNativeCalendarSession| -> Result<DisplaySession, ServiceError> {
            let (Some(starts_at), Some(ends_at)) = (native.period_start(), native.period_end_exclusive()) else {
                return Err(ServiceError::InvalidResult);
            };
            Ok(DisplaySession { date: native.date(), starts_at, ends_at,
                opens_at: native.opens_at(), closes_at: native.closes_at_exclusive() })
        };
        close.point.latest_session = session(latest)?;
        close.point.successor = replay.sessions().get(index + 1).map(session).transpose()?;
        close.metadata.display_calendar = Some(DisplayCalendar {
            origin: ForecastArtifactManifestRecord::from_manifest(origin),
            binding: calendar.reference().capture_binding_digest().bytes(),
            replay: replay.replay_digest().bytes(),
            available_at: calendar.available_at(),
            received_at: replay.received_at(),
            complete_from: replay.complete_from(),
            complete_until: replay.complete_until(),
        });
        validate_projection(
            &close.selection,
            &close.metadata,
            &close.point,
            Some(origin),
            cutoff,
        )?;
        let mut parents = projection_parents(&close.selection);
        if !parents.contains(origin) {
            parents.push(origin.clone());
        }
        let permit = authorize_projection_parents(research, &parents, cutoff, context).await?;
        let source = projection_source(&close.selection, Some(origin));
        let metadata =
            serde_json::to_vec(&close.metadata).map_err(|_| ServiceError::InvalidResult)?;
        let row = ChartProjectionRow {
            time_nanos: close.session_close().unix_nanos(),
            values: vec![Some(close.close().amount().into())],
            point: serde_json::to_value(&close.point).map_err(|_| ServiceError::InvalidResult)?,
        };
        let projections = research.chart_projections();
        let deadline = context.deadline();
        research
            .run_owned_research_io(deadline, context.cancellation(), move |cancellation| {
                projections.publish(source, &metadata, 1, [Ok(row)], deadline, &cancellation)
            })
            .await
            .map_err(map_research_error)?
            .map_err(chart_storage_error)?;
        recheck_projection_parents(research, permit, context).await?;
        Ok(Some(close))
    }

    async fn select_previous_close(
        &self,
        research: &ResearchService,
        instrument_id: InstrumentId,
        knowledge_cutoff: Timestamp,
        context: &RequestContext,
    ) -> Result<Option<LatestCanonicalMarketBarHistoryWindowSelection>, ServiceError> {
        self.select_previous_closes(research, &[instrument_id], knowledge_cutoff, context)
            .await?
            .into_iter()
            .next()
            .ok_or(ServiceError::InvalidResult)?
    }

    async fn select_previous_closes(
        &self,
        research: &ResearchService,
        instruments: &[InstrumentId],
        knowledge_cutoff: Timestamp,
        context: &RequestContext,
    ) -> Result<
        Vec<Result<Option<LatestCanonicalMarketBarHistoryWindowSelection>, ServiceError>>,
        ServiceError,
    > {
        check(context)?;
        if instruments.is_empty() {
            return Ok(Vec::new());
        }
        let requests = instruments
            .iter()
            .map(|instrument| {
                LatestCanonicalMarketBarHistoryWindowRequest::try_new(
                    *instrument,
                    MarketHistorySelectionPolicy::COMPLETE_DAILY_RAW_V1,
                    knowledge_cutoff,
                )
                .map_err(|_| ServiceError::InvalidRequest)
            })
            .collect::<Result<Vec<_>, _>>()?;
        let reader = self.reader.clone();
        let deadline = context.deadline();
        let timing = CloseReadStage::new(context, None, "page_selection");
        let selections = research
            .run_owned_research_read(deadline, context.cancellation(), move |cancellation| {
                reader.select_latest_canonical_market_bar_history_windows(
                    &requests,
                    deadline,
                    &cancellation,
                )
            })
            .await
            .map_err(map_research_error)?
            .map_err(read_error)?;
        timing.complete();
        check(context)?;
        if selections.len() != instruments.len() {
            return Err(ServiceError::InvalidResult);
        }
        Ok(selections
            .into_iter()
            .zip(instruments)
            .map(|(selection, instrument)| {
                selection.map_err(read_error).inspect_err(|error| {
                    trace_close_read_failure(*instrument, "latest-window-selection", error)
                })
            })
            .collect())
    }

    /// Validates original history/calendar once and commits latest plus its exact predecessor.
    pub(crate) async fn prepare_latest_previous_close(
        &self,
        research: &ResearchService,
        instrument_id: InstrumentId,
        knowledge_cutoff: Timestamp,
        context: &RequestContext,
    ) -> Result<Option<PreviousClose>, ServiceError> {
        let selection = self
            .select_previous_close(research, instrument_id, knowledge_cutoff, context)
            .await?;
        let Some(selection) = selection else {
            return Ok(None);
        };
        if let Some(close) =
            read_projection(research, selection.selection(), knowledge_cutoff, context).await?
        {
            return Ok(Some(close));
        }
        let selected = selection.selection().clone();
        let Some(history) = self
            .reader
            .read_canonical_market_bar_history_cursor(
                selection.into_exact_request(),
                context.deadline(),
                context.cancellation().clone(),
            )
            .await
            .map_err(read_error)
            .inspect_err(|error| {
                trace_close_read_failure(instrument_id, "canonical-cursor-read", error)
            })?
        else {
            return Ok(None);
        };
        let history = research
            .rejoin_market_history_native_sessions(
                history,
                context.deadline(),
                context.cancellation(),
            )
            .await
            .map_err(map_research_error)
            .inspect_err(|error| {
                trace_close_read_failure(instrument_id, "native-session-rejoin", error)
            })?;
        check(context)?;
        let Some(native) = history.native_sessions() else {
            // A date or aggregation boundary alone cannot establish a session close.
            return Ok(None);
        };
        let publication = history.selection().receipt();
        if publication.instrument_id() != instrument_id
            || publication.receipt_digest() != selected.receipt().receipt_digest()
            || publication.adjustment() != MarketBarAdjustment::Raw
            || !publication.current_research_eligible()
            || publication.published_at() > knowledge_cutoff
            || history.read_receipt().knowledge_cutoff() != knowledge_cutoff
            || history.bar_count() != publication.bar_count()
            || native.published_at() > knowledge_cutoff
            || native.received_at() > knowledge_cutoff
        {
            return Err(invalid_close_read(instrument_id, "publication-receipt"));
        }
        let currency = publication.currency();
        let mut parents = vec![history.selection().pinned().manifest().clone()];
        if !parents.contains(history.read_receipt().origin_manifest()) {
            parents.push(history.read_receipt().origin_manifest().clone());
        }
        let permit = authorize_projection_parents(research, &parents, knowledge_cutoff, context)
            .await
            .inspect_err(|error| {
                trace_close_read_failure(instrument_id, "projection-rights", error)
            })?;
        let mut bars = history.bars();
        let mut bar_count = 0usize;
        let mut previous = None;
        let mut completed = CompletedCloseAccumulator::default();
        for (ordinal, session) in native.sessions().iter().enumerate() {
            check(context)?;
            let session = session.map_err(read_error).inspect_err(|error| {
                trace_close_read_failure(instrument_id, "native-session-read", error)
            })?;
            let session_close = session.closes_at_exclusive();
            if session.opens_at() >= session_close
                || previous.is_some_and(|(date, close)| {
                    date >= session.native_date() || close >= session_close
                })
            {
                return Err(invalid_close_read(instrument_id, "native-session-order"));
            }
            previous = Some((session.native_date(), session_close));
            if !session.bar_present() {
                completed.observe(ordinal, None)?;
                continue;
            }
            let bar = bars
                .next()
                .transpose()
                .map_err(read_error)?
                .ok_or_else(|| invalid_close_read(instrument_id, "missing-bar"))?;
            bar_count = bar_count
                .checked_add(1)
                .ok_or(ServiceError::ResourceExhausted)?;
            let provenance = bar.context().provenance();
            let available = provenance
                .availability()
                .conservative_available_at()
                .ok_or_else(|| invalid_close_read(instrument_id, "missing-availability"))?;
            let coordinate_matches = if let Some(date) = bar.time_semantics().nominal_daily_date() {
                session.provider_timestamp().is_none()
                    && session.provider_period().is_none()
                    && date.date() == session.native_date()
                    && bar.context().time().effective().calendar_date_value() == Some(date.date())
                    && session_close <= available
            } else {
                bar.time_semantics()
                    .timestamped_period()
                    .is_some_and(|period| {
                        session.provider_period()
                            == Some((period.period_start(), period.period_end_exclusive()))
                            && session.provider_timestamp() == Some(period.provider_timestamp())
                            && period.period_end_exclusive() <= available
                    })
            };
            if !coordinate_matches
                || provenance.instrument_id() != Some(instrument_id)
                || bar.currency() != currency
                || bar.close().currency() != currency
                || bar.adjustment() != MarketBarAdjustment::Raw
                || bar.close().amount() <= Decimal::ZERO
                || available > knowledge_cutoff
                || provenance.ingested_at() > knowledge_cutoff
                || matches!(
                    provenance.quality(),
                    DataQuality::Modeled
                        | DataQuality::Estimated
                        | DataQuality::Stale
                        | DataQuality::Quarantined
                )
            {
                return Err(invalid_close_read(
                    instrument_id,
                    "bar-coordinate-or-provenance",
                ));
            }
            if session_close <= knowledge_cutoff
                && session
                    .provider_period()
                    .is_none_or(|(_, end)| end <= knowledge_cutoff)
            {
                let point = ClosePoint {
                    instrument_id,
                    close: bar.close(),
                    native_date: session.native_date(),
                    session_open: session.opens_at(),
                    session_close,
                    provider_timestamp: session.provider_timestamp(),
                    provider_period: session.provider_period(),
                    available_at: available,
                    received_at: provenance.received_at(),
                    ingested_at: provenance.ingested_at(),
                    quality: provenance.quality(),
                };
                completed.observe(ordinal, Some(point))?;
            }
        }
        if bars.next().transpose().map_err(read_error)?.is_some()
            || bar_count != history.bar_count()
        {
            return Err(invalid_close_read(instrument_id, "terminal-bar-count"));
        }
        check(context)?;
        recheck_projection_parents(research, std::sync::Arc::clone(&permit), context).await?;
        let Some(point) = completed.latest else {
            return Ok(None);
        };
        let read = history.read_receipt();
        let (content, lineage, rows, bytes) = read.object_evidence();
        let metadata = CloseMetadata {
            publication: publication.receipt_digest().bytes(),
            origin: ForecastArtifactManifestRecord::from_manifest(read.origin_manifest()),
            artifact: read.origin_object().0.to_string(),
            object_ordinal: read.origin_object().1,
            object_content: content.bytes(),
            object_lineage: lineage.bytes(),
            object_rows: rows,
            object_bytes: bytes,
            history_content: read.history_content_digest().bytes(),
            bar_count,
            native_mapping: native.mapping_digest().bytes(),
            calendar_replay: native.source_replay_digest().bytes(),
            calendar_capture: native.capture_receipt_digest().bytes(),
            calendar_component: native
                .calendar_component_digest()
                .map(|digest| digest.bytes()),
            calendar_origin: native.calendar_origin_content_digest().bytes(),
            calendar_binding: native.calendar_capture_binding_digest().bytes(),
            calendar_published_at: native.published_at(),
            calendar_received_at: native.received_at(),
            display_calendar: None,
        };
        validate_projection(&selected, &metadata, &point, None, knowledge_cutoff)?;
        let metadata = serde_json::to_vec(&metadata).map_err(|_| ServiceError::InvalidResult)?;
        let row = ChartProjectionRow {
            time_nanos: point.latest.session_close.unix_nanos(),
            values: vec![Some(point.latest.close.amount().into())],
            point: serde_json::to_value(&point).map_err(|_| ServiceError::InvalidResult)?,
        };
        let source = projection_source(&selected, None);
        let projections = research.chart_projections();
        let deadline = context.deadline();
        research
            .run_owned_research_io(deadline, context.cancellation(), move |cancellation| {
                projections.publish(source, &metadata, 1, [Ok(row)], deadline, &cancellation)
            })
            .await
            .map_err(map_research_error)?
            .map_err(chart_storage_error)?;
        check(context)?;
        recheck_projection_parents(research, std::sync::Arc::clone(&permit), context).await?;
        read_projection(research, &selected, knowledge_cutoff, context).await
    }
}

fn projection_source(
    selection: &CompleteMarketBarHistorySelection,
    calendar: Option<&DatasetManifestRef>,
) -> [u8; 32] {
    let mut digest = Sha256::new();
    digest.update(b"market-squawk/completed-session-daily-comparison/v1\0");
    digest.update(selection.policy_digest().bytes());
    digest.update(selection.receipt().receipt_digest().bytes());
    digest.update(calendar.map_or([0; 32], |manifest| manifest.content_hash().bytes()));
    digest.finalize().into()
}

async fn read_projection(
    research: &ResearchService,
    selection: &CompleteMarketBarHistorySelection,
    cutoff: Timestamp,
    context: &RequestContext,
) -> Result<Option<PreviousClose>, ServiceError> {
    read_projections(
        research,
        std::slice::from_ref(selection),
        None,
        cutoff,
        context,
    )
    .await?
    .into_iter()
    .next()
    .ok_or(ServiceError::InvalidResult)?
}

fn projection_parents(
    selection: &CompleteMarketBarHistorySelection,
) -> Vec<market_squawk_data::DatasetManifestRef> {
    let mut parents = vec![selection.pinned().manifest().clone()];
    if !parents.contains(selection.receipt().origin_manifest()) {
        parents.push(selection.receipt().origin_manifest().clone());
    }
    parents
}

async fn read_projections(
    research: &ResearchService,
    selections: &[CompleteMarketBarHistorySelection],
    calendar: Option<&DatasetManifestRef>,
    cutoff: Timestamp,
    context: &RequestContext,
) -> Result<Vec<Result<Option<PreviousClose>, ServiceError>>, ServiceError> {
    check(context)?;
    if selections.is_empty() {
        return Ok(Vec::new());
    }
    let projections = research.chart_projections();
    let sources: Vec<_> = selections
        .iter()
        .map(|selection| projection_source(selection, calendar))
        .collect();
    let deadline = context.deadline();
    let timing = CloseReadStage::new(context, None, "stored_projection_page");
    let stored = research
        .run_owned_research_read(deadline, context.cancellation(), move |cancellation| {
            projections.read_verified_single_rows(&sources, deadline, &cancellation)
        })
        .await
        .map_err(map_research_error)?
        .map_err(chart_storage_error)?;
    timing.complete();
    if stored.len() != selections.len() {
        return Err(ServiceError::InvalidResult);
    }
    let mut points = Vec::with_capacity(stored.len());
    let mut parents = Vec::new();
    for (selection, stored) in selections.iter().zip(stored) {
        check(context)?;
        let point = (|| {
            let Some((metadata, row)) = stored.map_err(chart_storage_error)? else {
                return Ok(None);
            };
            let metadata: CloseMetadata =
                serde_json::from_slice(&metadata).map_err(|_| ServiceError::InvalidResult)?;
            let close: CloseComparisonPoint =
                serde_json::from_value(row.point).map_err(|_| ServiceError::InvalidResult)?;
            if row.time_nanos != close.latest.session_close.unix_nanos()
                || row.values != vec![Some(close.latest.close.amount().into())]
            {
                return Err(ServiceError::InvalidResult);
            }
            validate_projection(selection, &metadata, &close, calendar, cutoff)?;
            Ok(Some(PreviousClose {
                selection: selection.clone(),
                metadata,
                point: close,
            }))
        })();
        let point = match point {
            Ok(Some(close)) => {
                let mut roots = projection_parents(selection);
                if let Some(calendar) = calendar {
                    if !roots.contains(calendar) {
                        roots.push(calendar.clone());
                    }
                }
                for parent in roots {
                    if !parents.contains(&parent) {
                        parents.push(parent);
                    }
                }
                Ok(Some(close))
            }
            Ok(None) => Ok(None),
            Err(error @ (ServiceError::Unavailable | ServiceError::Unauthorized)) => Err(error),
            Err(error) => return Err(error),
        };
        points.push(point);
    }
    let mut expiries = vec![None; points.len()];
    if !parents.is_empty() {
        let timing = CloseReadStage::new(context, None, "page_rights_authorization");
        match authorize_projection_parents(research, &parents, cutoff, context).await {
            Ok(permit) => {
                for (index, point) in points.iter().enumerate() {
                    if matches!(point, Ok(Some(_))) {
                        expiries[index] = Some(permit.expires_at());
                    }
                }
            }
            Err(ServiceError::Unauthorized | ServiceError::Unavailable) => {
                // One denied source must not suppress other instruments. Recheck the original
                // exact per-close roots; this path grants no authority from the failed union.
                for (index, selection) in selections.iter().enumerate() {
                    if !matches!(&points[index], Ok(Some(_))) {
                        continue;
                    }
                    check(context)?;
                    let mut roots = projection_parents(selection);
                    if let Some(calendar) = calendar {
                        if !roots.contains(calendar) {
                            roots.push(calendar.clone());
                        }
                    }
                    match authorize_projection_parents(research, &roots, cutoff, context).await {
                        Ok(permit) => expiries[index] = Some(permit.expires_at()),
                        Err(error @ (ServiceError::Unauthorized | ServiceError::Unavailable)) => {
                            points[index] = Err(error)
                        }
                        Err(error) => return Err(error),
                    }
                }
            }
            Err(error) => return Err(error),
        }
        timing.complete();
    }
    check(context)?;
    let now = wall_now()?;
    points
        .into_iter()
        .zip(expiries)
        .map(|(point, expires)| match point {
            Ok(Some(close)) => {
                let expires = expires.ok_or(ServiceError::InvalidResult)?;
                Ok(if now >= expires {
                    Err(ServiceError::Unauthorized)
                } else {
                    Ok(Some(close))
                })
            }
            Ok(None) => Ok(Ok(None)),
            Err(error) => Ok(Err(error)),
        })
        .collect()
}

fn validate_projection(
    selection: &CompleteMarketBarHistorySelection,
    metadata: &CloseMetadata,
    point: &CloseComparisonPoint,
    calendar: Option<&DatasetManifestRef>,
    cutoff: Timestamp,
) -> Result<(), ServiceError> {
    validate_close_point(selection, metadata, &point.latest, true, cutoff)?;
    if !point.latest_session.valid()
        || point.latest_session.date != point.latest.native_date
        || point.latest_session.opens_at != point.latest.session_open
        || point.latest_session.closes_at != point.latest.session_close
        || point.prior.is_some() != point.prior_ordinal.is_some()
    {
        return Err(ServiceError::InvalidResult);
    }
    if let Some(prior) = &point.prior {
        validate_close_point(selection, metadata, prior, false, cutoff)?;
        if point
            .prior_ordinal
            .and_then(|ordinal| ordinal.checked_add(1))
            != Some(point.latest_ordinal)
            || prior.native_date >= point.latest.native_date
            || prior.session_close >= point.latest.session_open
        {
            return Err(ServiceError::InvalidResult);
        }
    }
    match (&metadata.display_calendar, calendar) {
        (None, None) => {
            let original = DisplaySession::from_close(&point.latest);
            if point.successor.is_some()
                || point.latest_session.starts_at != original.starts_at
                || point.latest_session.ends_at != original.ends_at
            {
                return Err(ServiceError::InvalidResult);
            }
        }
        (Some(evidence), Some(expected)) => {
            if evidence
                .origin
                .typed()
                .map_err(|_| ServiceError::InvalidResult)?
                != *expected
                || evidence.binding == [0; 32]
                || evidence.replay == [0; 32]
                || evidence.available_at > cutoff
                || evidence.received_at > cutoff
                || evidence.complete_from > point.latest_session.starts_at
                || evidence.complete_until < point.latest_session.ends_at
            {
                return Err(ServiceError::InvalidResult);
            }
            if let Some(successor) = &point.successor {
                if !successor.valid()
                    || successor.date <= point.latest.native_date
                    || successor.starts_at < point.latest_session.ends_at
                    || successor.ends_at > evidence.complete_until
                {
                    return Err(ServiceError::InvalidResult);
                }
            }
        }
        _ => return Err(ServiceError::InvalidResult),
    }
    Ok(())
}

fn validate_close_point(
    selection: &CompleteMarketBarHistorySelection,
    metadata: &CloseMetadata,
    close: &ClosePoint,
    terminal: bool,
    cutoff: Timestamp,
) -> Result<(), ServiceError> {
    let receipt = selection.receipt();
    let origin = metadata
        .origin
        .typed()
        .map_err(|_| ServiceError::InvalidResult)?;
    let (available, received, ingested) = receipt.knowledge_clocks();
    let coordinate_matches = match (close.provider_timestamp, close.provider_period) {
        (Some(timestamp), Some((start, end))) => {
            start <= timestamp
                && timestamp < end
                && start <= close.session_open
                && close.session_close <= end
                && end <= close.available_at
                && end <= cutoff
                && receipt.coverage().is_some_and(|(first, last, complete)| {
                    first <= timestamp
                        && timestamp <= last
                        && end <= complete
                        && (!terminal || (timestamp == last && end == complete))
                })
        }
        (None, None) => {
            receipt.requested_dates().is_some() && close.session_close <= close.available_at
        }
        _ => false,
    };
    if metadata.publication != receipt.receipt_digest().bytes()
        || origin != *receipt.origin_manifest()
        || metadata.artifact != receipt.origin_artifact_id().to_string()
        || metadata.object_ordinal != receipt.origin_object_ordinal()
        || metadata.bar_count != receipt.bar_count()
        || metadata.object_rows < metadata.bar_count as u64
        || metadata.object_bytes == 0
        || [
            metadata.object_content,
            metadata.object_lineage,
            metadata.history_content,
            metadata.native_mapping,
            metadata.calendar_replay,
        ]
        .contains(&[0; 32])
        || metadata.calendar_capture != receipt.capture_receipt_digest().bytes()
        || metadata.calendar_component
            != receipt
                .session_calendar_component()
                .map(|(_, digest, _)| digest.bytes())
        || metadata.calendar_origin != receipt.origin_manifest().content_hash().bytes()
        || metadata.calendar_binding != receipt.binding_digest().bytes()
        || close.instrument_id != receipt.instrument_id()
        || close.close.currency() != receipt.currency()
        || close.close.amount() <= Decimal::ZERO
        || receipt.adjustment() != MarketBarAdjustment::Raw
        || !receipt.current_research_eligible()
        || close.session_open >= close.session_close
        || !coordinate_matches
        || [
            receipt.published_at(),
            receipt.capture_recorded_at(),
            available,
            received,
            ingested,
            metadata.calendar_published_at,
            metadata.calendar_received_at,
            close.session_close,
            close.available_at,
            close.received_at,
            close.ingested_at,
        ]
        .into_iter()
        .any(|at| at > cutoff)
        || matches!(
            close.quality,
            DataQuality::Modeled
                | DataQuality::Estimated
                | DataQuality::Stale
                | DataQuality::Quarantined
        )
    {
        return Err(invalid_close_read(
            receipt.instrument_id(),
            "stored-projection-evidence",
        ));
    }
    Ok(())
}

fn wall_now() -> Result<Timestamp, ServiceError> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|elapsed| i64::try_from(elapsed.as_nanos()).ok())
        .map(Timestamp::from_unix_nanos)
        .ok_or(ServiceError::Internal)
}

fn check(context: &RequestContext) -> Result<(), ServiceError> {
    if context.cancellation().is_cancelled() {
        Err(ServiceError::Cancelled)
    } else if Instant::now() >= context.deadline() {
        Err(ServiceError::DeadlineExceeded)
    } else {
        Ok(())
    }
}

fn read_error(error: AnalyticalReadError) -> ServiceError {
    match unavailable_reason(&error) {
        MarketHistoryUnavailableReason::Cancelled => ServiceError::Cancelled,
        MarketHistoryUnavailableReason::DeadlineExceeded => ServiceError::DeadlineExceeded,
        MarketHistoryUnavailableReason::CapacityExceeded => ServiceError::ResourceExhausted,
        MarketHistoryUnavailableReason::StorageUnavailable => ServiceError::Unavailable,
        MarketHistoryUnavailableReason::IntegrityUnproven => ServiceError::InvalidResult,
    }
}

fn invalid_close_read(instrument: InstrumentId, stage: &'static str) -> ServiceError {
    let error = ServiceError::InvalidResult;
    trace_close_read_failure(instrument, stage, &error);
    error
}

fn trace_close_read_failure(instrument: InstrumentId, stage: &'static str, error: &ServiceError) {
    tracing::warn!(%instrument, stage, ?error, "Previous close evidence could not be read");
}

#[cfg(test)]
mod tests {
    use super::*;
    use market_squawk_data::{CatalogConfig, CatalogLimit, CatalogResultLimits, ObjectStoreConfig};
    use market_squawk_platform::LocalPaths;
    use std::time::Duration;
    use tokio_util::sync::CancellationToken;

    #[test]
    fn completed_session_pair_reopens_with_native_predecessor_and_gaps()
    -> Result<(), Box<dyn std::error::Error>> {
        let at = |value: &str| -> Result<Timestamp, Box<dyn std::error::Error>> {
            Ok(Timestamp::from_unix_nanos(
                chrono::DateTime::parse_from_rfc3339(value)?
                    .timestamp_nanos_opt()
                    .ok_or("timestamp")?,
            ))
        };
        let point = |day: u8, value: i64| -> Result<ClosePoint, Box<dyn std::error::Error>> {
            let start = at(&format!("2026-10-{day:02}T04:00:00Z"))?;
            let end = at(&format!("2026-10-{:02}T04:00:00Z", day + 1))?;
            Ok(ClosePoint {
                instrument_id: "00000000-0000-0000-0000-000000000101".parse()?,
                close: Money::new(Decimal::new(value, 2), Currency::try_from("USD")?),
                native_date: CalendarDate::new(2026, 10, day)?,
                session_open: at(&format!("2026-10-{day:02}T13:30:00Z"))?,
                session_close: at(&format!("2026-10-{day:02}T20:00:00Z"))?,
                provider_timestamp: Some(start),
                provider_period: Some((start, end)),
                available_at: end,
                received_at: end,
                ingested_at: end,
                quality: DataQuality::DirectUnverified,
            })
        };
        let thursday = point(1, 51_271)?;
        let friday = point(2, 51_713)?;
        let mut accumulation = CompletedCloseAccumulator::default();
        accumulation.observe(0, Some(thursday.clone()))?;
        accumulation.observe(1, Some(friday.clone()))?;
        let mut pair = accumulation.latest.ok_or("missing pair")?;
        // This is the source-native successor association retained by preparation. The weekend
        // has no native session; Monday is the next admitted session, not date + one day.
        pair.successor = Some(DisplaySession::from_close(&point(5, 52_000)?));
        let directory = tempfile::tempdir()?;
        let paths = LocalPaths::prepare(directory.path().join("comparison"))?;
        let open = || -> Result<ResearchService, Box<dyn std::error::Error>> {
            Ok(ResearchService::open_or_initialize(
                &paths,
                CatalogConfig::try_new(
                    paths.catalog()?.clone(),
                    Duration::from_millis(750),
                    CatalogLimit::new(64)?,
                    CatalogResultLimits::try_new(1024 * 1024, 8 * 1024 * 1024)?,
                )?,
                8,
                ObjectStoreConfig::try_new(8 * 1024 * 1024, 1024, Duration::from_secs(60))?,
            )?)
        };
        let research = open()?;
        let deadline = Instant::now() + Duration::from_secs(10);
        let cancellation = CancellationToken::new();
        let source = [37; 32];
        research.chart_projections().publish(
            source,
            b"native-pair-fixture",
            1,
            [Ok(ChartProjectionRow {
                time_nanos: pair.latest.session_close.unix_nanos(),
                values: vec![Some(pair.latest.close.amount().into())],
                point: serde_json::to_value(&pair)?,
            })],
            deadline,
            &cancellation,
        )?;
        drop(research);
        let research = open()?;
        let (_, row) = research
            .chart_projections()
            .read_verified_single_rows(&[source], deadline, &cancellation)?
            .pop()
            .ok_or("missing row result")??
            .ok_or("missing projection")?;
        let reopened: CloseComparisonPoint = serde_json::from_value(row.point)?;
        for original_price_at in [
            "2026-10-02T12:00:00Z",
            "2026-10-02T15:00:00Z",
            "2026-10-02T20:01:26Z",
            "2026-10-03T00:00:00Z",
        ] {
            let comparison = reopened
                .comparison(at(original_price_at)?, false)
                .ok_or("missing Friday comparison")?;
            assert_eq!(comparison.native_date(), thursday.native_date);
            assert_eq!(comparison.close(), thursday.close);
            assert_eq!(comparison.price_session_date(), friday.native_date);
        }
        assert_eq!(
            reopened
                .comparison(friday.session_close, true)
                .ok_or("close comparison")?
                .close(),
            thursday.close
        );
        assert_eq!(
            reopened
                .comparison(at("2026-10-05T12:00:00Z")?, false)
                .ok_or("premarket comparison")?
                .close(),
            friday.close
        );
        assert!(
            reopened
                .comparison(at("2026-10-03T15:00:00Z")?, false)
                .is_none()
        );
        assert!(
            reopened
                .comparison(at("2026-10-06T15:00:00Z")?, false)
                .is_none()
        );
        assert!(
            reopened
                .comparison(at("2026-10-01T21:00:00Z")?, false)
                .is_none()
        );
        let mut gap = CompletedCloseAccumulator::default();
        gap.observe(0, Some(thursday))?;
        gap.observe(1, None)?;
        gap.observe(2, Some(point(5, 52_000)?))?;
        let gap = gap.latest.ok_or("missing latest after gap")?;
        assert!(gap.prior.is_none());
        assert!(gap.comparison(at("2026-10-05T21:00:00Z")?, false).is_none());
        assert!(gap.comparison(gap.latest.session_close, true).is_none());
        Ok(())
    }
}
