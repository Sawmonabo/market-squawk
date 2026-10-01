//! Read-only completed-session close projection over original canonical history and calendar.

use super::{MarketHistoryReadCapability, MarketHistoryUnavailableReason, unavailable_reason};
use crate::{
    ResearchService,
    application::{
        model::forecast::authorize_projection_parents,
        research::corporate_actions::map_research_error,
    },
};
use market_squawk_data::{
    AnalyticalReadError, LatestCanonicalMarketBarHistoryWindowRequest, MarketHistorySelectionPolicy,
};
use market_squawk_domain::{
    CalendarDate, Currency, DataQuality, InstrumentId, MarketBarAdjustment, Money, Timestamp,
};
use market_squawk_services::{RequestContext, ServiceError};
use rust_decimal::Decimal;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

/// Display evidence only; it carries no current-price or execution authority.
pub(crate) struct PreviousClose {
    instrument_id: InstrumentId,
    close: Money,
    native_date: CalendarDate,
    session_close: Timestamp,
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
    pub(crate) async fn read_latest_previous_close(
        &self,
        research: &ResearchService,
        instrument_id: InstrumentId,
        knowledge_cutoff: Timestamp,
        context: &RequestContext,
    ) -> Result<Option<PreviousClose>, ServiceError> {
        check(context)?;
        let request = LatestCanonicalMarketBarHistoryWindowRequest::try_new(
            instrument_id,
            MarketHistorySelectionPolicy::COMPLETE_DAILY_RAW_V1,
            knowledge_cutoff,
        )
        .map_err(|_| ServiceError::InvalidRequest)?;
        let reader = self.reader.clone();
        let deadline = context.deadline();
        let selection = research
            .run_owned_research_io(deadline, context.cancellation(), move |cancellation| {
                reader.select_latest_canonical_market_bar_history_window(
                    request,
                    deadline,
                    &cancellation,
                )
            })
            .await
            .map_err(map_research_error)?
            .map_err(read_error)?;
        check(context)?;
        let Some(selection) = selection else {
            return Ok(None);
        };
        let Some(history) = self
            .reader
            .read_canonical_market_bar_history_cursor(
                selection.into_exact_request(),
                context.deadline(),
                context.cancellation().clone(),
            )
            .await
            .map_err(read_error)?
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
            .map_err(map_research_error)?;
        check(context)?;
        let Some(native) = history.native_sessions() else {
            // A date or aggregation boundary alone cannot establish a session close.
            return Ok(None);
        };
        let publication = history.selection().receipt();
        if publication.instrument_id() != instrument_id
            || publication.adjustment() != MarketBarAdjustment::Raw
            || !publication.current_research_eligible()
            || publication.published_at() > knowledge_cutoff
            || history.read_receipt().knowledge_cutoff() != knowledge_cutoff
            || history.bar_count() != publication.bar_count()
            || native.published_at() > knowledge_cutoff
            || native.received_at() > knowledge_cutoff
        {
            return Err(ServiceError::InvalidResult);
        }
        let currency = publication.currency();
        let mut parents = vec![history.selection().pinned().manifest().clone()];
        if !parents.contains(history.read_receipt().origin_manifest()) {
            parents.push(history.read_receipt().origin_manifest().clone());
        }
        let permit =
            authorize_projection_parents(research, &parents, knowledge_cutoff, context).await?;
        let mut bars = history.bars();
        let mut bar_count = 0usize;
        let mut previous = None;
        let mut latest = None;
        for session in native.sessions().iter() {
            check(context)?;
            let session = session.map_err(read_error)?;
            let session_close = session.closes_at_exclusive();
            if session.opens_at() >= session_close
                || previous.is_some_and(|(date, close)| {
                    date >= session.native_date() || close >= session_close
                })
            {
                return Err(ServiceError::InvalidResult);
            }
            previous = Some((session.native_date(), session_close));
            if !session.bar_present() {
                continue;
            }
            let bar = bars
                .next()
                .transpose()
                .map_err(read_error)?
                .ok_or(ServiceError::InvalidResult)?;
            bar_count = bar_count
                .checked_add(1)
                .ok_or(ServiceError::ResourceExhausted)?;
            let provenance = bar.context().provenance();
            let available = provenance
                .availability()
                .conservative_available_at()
                .ok_or(ServiceError::InvalidResult)?;
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
                return Err(ServiceError::InvalidResult);
            }
            if session_close <= knowledge_cutoff
                && session
                    .provider_period()
                    .is_none_or(|(_, end)| end <= knowledge_cutoff)
            {
                latest = Some(PreviousClose {
                    instrument_id,
                    close: bar.close(),
                    native_date: session.native_date(),
                    session_close,
                });
            }
        }
        if bars.next().transpose().map_err(read_error)?.is_some()
            || bar_count != history.bar_count()
        {
            return Err(ServiceError::InvalidResult);
        }
        check(context)?;
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .ok()
            .and_then(|elapsed| i64::try_from(elapsed.as_nanos()).ok())
            .map(Timestamp::from_unix_nanos)
            .ok_or(ServiceError::Internal)?;
        if now >= permit.expires_at() {
            return Err(ServiceError::Unauthorized);
        }
        Ok(latest)
    }
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
