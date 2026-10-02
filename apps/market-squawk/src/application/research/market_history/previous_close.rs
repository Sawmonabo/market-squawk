//! Persisted completed-session display evidence over original canonical history and calendar.

use super::{MarketHistoryReadCapability, MarketHistoryUnavailableReason, unavailable_reason};
use crate::{
    ResearchService,
    application::{
        model::forecast::{authorize_projection_parents, chart_storage_error},
        research::corporate_actions::map_research_error,
    },
};
use market_squawk_data::{
    AnalyticalReadError, ChartProjectionRow, CompleteMarketBarHistorySelection,
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
    instrument_id: InstrumentId,
    close: Money,
    native_date: CalendarDate,
    session_close: Timestamp,
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
#[derive(Serialize, Deserialize)]
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
}

#[derive(Serialize, Deserialize)]
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
        self.instrument_id
    }

    pub(crate) const fn currency(&self) -> Currency {
        self.close.currency()
    }

    pub(crate) const fn close(&self) -> Money {
        self.close
    }

    pub(crate) const fn session_close(&self) -> Timestamp {
        self.session_close
    }

    pub(crate) const fn native_date(&self) -> CalendarDate {
        self.native_date
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
        let projected = read_projections(research, &selected, knowledge_cutoff, context).await?;
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

    /// Validates the entire original history and calendar once, then commits its terminal close.
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
        let mut latest = None;
        for session in native.sessions().iter() {
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
                latest = Some(ClosePoint {
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
                });
            }
        }
        if bars.next().transpose().map_err(read_error)?.is_some()
            || bar_count != history.bar_count()
        {
            return Err(invalid_close_read(instrument_id, "terminal-bar-count"));
        }
        check(context)?;
        if wall_now()? >= permit.expires_at() {
            return Err(ServiceError::Unauthorized);
        }
        let Some(latest) = latest else {
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
        };
        validate_projection(&selected, &metadata, &latest, knowledge_cutoff)?;
        let metadata = serde_json::to_vec(&metadata).map_err(|_| ServiceError::InvalidResult)?;
        let row = ChartProjectionRow {
            time_nanos: latest.session_close.unix_nanos(),
            values: vec![Some(latest.close.amount().into())],
            point: serde_json::to_value(&latest).map_err(|_| ServiceError::InvalidResult)?,
        };
        let source = projection_source(&selected);
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
        if wall_now()? >= permit.expires_at() {
            return Err(ServiceError::Unauthorized);
        }
        read_projection(research, &selected, knowledge_cutoff, context).await
    }
}

fn projection_source(selection: &CompleteMarketBarHistorySelection) -> [u8; 32] {
    let mut digest = Sha256::new();
    digest.update(b"market-squawk/completed-session-close/v1\0");
    digest.update(selection.policy_digest().bytes());
    digest.update(selection.receipt().receipt_digest().bytes());
    digest.finalize().into()
}

async fn read_projection(
    research: &ResearchService,
    selection: &CompleteMarketBarHistorySelection,
    cutoff: Timestamp,
    context: &RequestContext,
) -> Result<Option<PreviousClose>, ServiceError> {
    read_projections(research, std::slice::from_ref(selection), cutoff, context)
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
    cutoff: Timestamp,
    context: &RequestContext,
) -> Result<Vec<Result<Option<PreviousClose>, ServiceError>>, ServiceError> {
    check(context)?;
    if selections.is_empty() {
        return Ok(Vec::new());
    }
    let projections = research.chart_projections();
    let sources: Vec<_> = selections.iter().map(projection_source).collect();
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
            let close: ClosePoint =
                serde_json::from_value(row.point).map_err(|_| ServiceError::InvalidResult)?;
            if row.time_nanos != close.session_close.unix_nanos()
                || row.values != vec![Some(close.close.amount().into())]
            {
                return Err(ServiceError::InvalidResult);
            }
            validate_projection(selection, &metadata, &close, cutoff)?;
            Ok(Some(close))
        })();
        let point = match point {
            Ok(Some(close)) => {
                for parent in projection_parents(selection) {
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
                    match authorize_projection_parents(
                        research,
                        &projection_parents(selection),
                        cutoff,
                        context,
                    )
                    .await
                    {
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
                    Ok(Some(PreviousClose {
                        instrument_id: close.instrument_id,
                        close: close.close,
                        native_date: close.native_date,
                        session_close: close.session_close,
                    }))
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
    close: &ClosePoint,
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
                && receipt
                    .coverage()
                    .is_some_and(|(_, last, complete)| timestamp == last && end == complete)
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
