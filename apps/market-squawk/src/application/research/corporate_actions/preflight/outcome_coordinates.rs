//! Authentic later calendar resolution for a retained exact nominal forecast target.

use super::*;
use crate::application::model::forecast::ForecastOutcomePreparationOrigin;
use market_squawk_domain::MarketBarObservation;

/// Target dates are minted only after an actual completed-calendar read matches the saved exact
/// origin and target. No caller-authored dates, prices, completeness flags or product JSON.
pub(crate) struct ForecastOutcomePreparationCoordinates {
    origin: ForecastOutcomePreparationOrigin,
    native_dates: (CalendarDate, CalendarDate),
    calendar: CompletedMarketSessionReference,
}
impl ForecastOutcomePreparationCoordinates {
    pub(crate) fn instrument_id(&self) -> InstrumentId {
        self.origin.instrument_id()
    }
    pub(crate) fn origin_bar(&self) -> &MarketBarObservation {
        self.origin.origin_bar()
    }
    pub(crate) const fn native_dates(&self) -> (CalendarDate, CalendarDate) {
        self.native_dates
    }
    pub(crate) fn target_at(&self) -> Timestamp {
        self.origin.target_at()
    }
    pub(crate) fn venue_id(&self) -> &VenueId {
        self.origin.venue_id()
    }
    pub(crate) const fn calendar_reference(&self) -> &CompletedMarketSessionReference {
        &self.calendar
    }
}

impl SourceActionPreparationCapability {
    /// Provider/calendar acquisition remains inside the existing source preparation owner. UTC
    /// dates only bound its query; the target native date comes from an exact retained session.
    pub(crate) async fn resolve_outcome_preparation(
        &self,
        origin: ForecastOutcomePreparationOrigin,
        context: &RequestContext,
    ) -> Result<Option<ForecastOutcomePreparationCoordinates>, ServiceError> {
        check(context)?;
        let started_at = now()?;
        if origin.target_at() > started_at {
            return Ok(None);
        }
        let Some(origin_date) = origin.native_origin_date() else {
            return Ok(None);
        };
        if origin
            .origin_bar()
            .time_semantics()
            .nominal_daily_date()
            .is_none()
        {
            return Ok(None);
        }
        // One extra UTC day is a bounded calendar request envelope, never an economic-date guess.
        let query_end = origin
            .target_at()
            .checked_add_nanos(86_400_000_000_000)
            .map_err(|_| ServiceError::InvalidRequest)?
            .utc_calendar_date()
            .map_err(|_| ServiceError::InvalidRequest)?;
        if query_end < origin_date {
            return Err(ServiceError::InvalidRequest);
        }
        let Some(reference) = self
            .calendars
            .preflight(
                origin.venue_id(),
                origin_date,
                query_end,
                context.deadline(),
                context.cancellation().clone(),
            )
            .await
            .map_err(map_calendar_error)?
        else {
            return Ok(None);
        };
        let cutoff = now()?;
        if cutoff < started_at {
            return Err(ServiceError::Unavailable);
        }
        let Some(calendar) = self
            .calendars
            .read_reference(
                &reference,
                cutoff,
                context.deadline(),
                context.cancellation().clone(),
            )
            .await
            .map_err(map_calendar_error)?
        else {
            return Ok(None);
        };
        if calendar.venue_id() != origin.venue_id() {
            return Ok(None);
        }
        let Some(original) = calendar.date_session_on(origin_date, cutoff, cutoff) else {
            return Ok(None);
        };
        if original.closes_at_exclusive() != origin.origin_at()
            || Some(original.opens_at()) != origin.native_origin_open()
        {
            return Ok(None);
        }
        let mut target_date = None;
        for native in calendar.native_session_replay().sessions() {
            check(context)?;
            let Some(session) = calendar.date_session_on(native.date(), cutoff, cutoff) else {
                continue;
            };
            if session.closes_at_exclusive() == origin.target_at()
                && session.closes_at_exclusive() <= cutoff
            {
                if target_date.replace(session.date()).is_some() {
                    return Err(ServiceError::InvalidResult);
                }
            }
        }
        let Some(target_date) = target_date else {
            return Ok(None);
        };
        if target_date <= origin_date {
            return Ok(None);
        }
        check(context)?;
        Ok(Some(ForecastOutcomePreparationCoordinates {
            origin,
            native_dates: (origin_date, target_date),
            calendar: reference,
        }))
    }
}
