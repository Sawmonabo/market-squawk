//! Single-instrument later-horizon source preparation through the existing acquisition owners.

use super::super::SourceAppliedCorporateActionPlanReference;
use super::*;
use market_squawk_data::CompleteMarketBarHistoryOutput;
use market_squawk_domain::MarketBarAdjustment;

/// Actual later seal and original immutable source proof; callers retain both with the outcome.
pub(crate) struct PreparedForecastOutcomeSource {
    reference: SourceAppliedCorporateActionPlanReference,
    cutoff: Timestamp,
}
impl PreparedForecastOutcomeSource {
    pub(crate) fn reference(&self) -> &SourceAppliedCorporateActionPlanReference {
        &self.reference
    }
    pub(crate) const fn cutoff(&self) -> Timestamp {
        self.cutoff
    }
}

impl SourceActionPreparationCapability {
    /// The history is an already completed ordinary source read covering origin through target.
    /// New all-family query acquisition receives its ACTUAL later clock, never the forecast seal.
    pub(crate) async fn prepare_for_forecast_outcome(
        &self,
        original: &CompleteMarketBarHistoryOutput,
        interval: (CalendarDate, CalendarDate),
        target_at: Timestamp,
        context: &RequestContext,
    ) -> Result<PreparedForecastOutcomeSource, ServiceError> {
        check(context)?;
        let started_at = now()?;
        let receipt = original.selection().receipt();
        let graph = receipt.date_windows().ok_or(ServiceError::Unavailable)?;
        let dates = graph.requested_dates();
        let native = original
            .native_sessions()
            .ok_or(ServiceError::Unavailable)?;
        if interval.0 > interval.1
            || target_at > started_at
            || receipt.adjustment() != MarketBarAdjustment::Raw
            || original.read_receipt().knowledge_cutoff() > started_at
            || dates.0 > interval.0
            || dates.1 < interval.1
            || !native.sessions().iter().any(|session| {
                session.native_date() == interval.1
                    && session.bar_present()
                    && session.closes_at_exclusive() <= target_at
            })
        {
            return Err(ServiceError::InvalidRequest);
        }
        let instruments = BTreeSet::from([receipt.instrument_id()]);
        let runtime = self
            .runtime
            .current_alpaca_calendar_runtime(context.deadline(), context.cancellation())
            .await
            .map_err(|_| controlled(context, ServiceError::Unavailable))?;
        let today = new_york_date(started_at)?;
        if interval.1 > today {
            return Err(ServiceError::InvalidRequest);
        }
        let reference = self
            .calendars
            .preflight(
                &VenueId::try_from("iex").map_err(|_| ServiceError::Internal)?,
                interval.0,
                today,
                context.deadline(),
                context.cancellation().clone(),
            )
            .await
            .map_err(map_calendar_error)?
            .ok_or(ServiceError::Unavailable)?;
        let calendar = self
            .calendars
            .read_reference(
                &reference,
                now()?,
                context.deadline(),
                context.cancellation().clone(),
            )
            .await
            .map_err(map_calendar_error)?
            .ok_or(ServiceError::Unavailable)?;
        let published = self
            .publish_query(
                &runtime,
                &instruments,
                (interval.0, today),
                &calendar,
                context,
            )
            .await?;
        runtime
            .require_current(context.deadline(), context.cancellation())
            .await
            .map_err(map_capability_error)?;
        let cutoff = now()?;
        if cutoff < started_at || new_york_date(cutoff)? != today {
            return Err(ServiceError::Unavailable);
        }
        let calendar = self
            .calendars
            .read_reference(
                &reference,
                cutoff,
                context.deadline(),
                context.cancellation().clone(),
            )
            .await
            .map_err(map_calendar_error)?
            .ok_or(ServiceError::Unavailable)?;
        let plan = self
            .read_query(
                published,
                calendar,
                instruments,
                interval,
                cutoff,
                target_at,
                context,
            )
            .await?;
        let (history, calendar) = self
            .reopen_original_ordinary_history(original, cutoff, context)
            .await?;
        let ordinary = self
            .research
            .rejoin_tiingo_eod_history_actions(history, context.deadline(), context.cancellation())
            .await
            .map_err(map_research_error)?;
        let plan = plan
            .with_complete_ordinary_history(
                vec![(ordinary, calendar)],
                limits()?,
                context.deadline(),
                context.cancellation(),
            )
            .map_err(|error| map_plan_error(error, context))?;
        let reference = plan
            .price_reference()
            .map_err(|error| map_plan_error(error, context))?;
        check(context)?;
        Ok(PreparedForecastOutcomeSource { reference, cutoff })
    }
}
