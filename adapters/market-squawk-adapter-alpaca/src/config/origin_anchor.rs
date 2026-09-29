//! Bounded original-coordinate request plans, before any source/capture authority is created.

use super::*;
use market_squawk_domain::{MarketBarAdjustment, MarketBarObservation};

impl AlpacaHistoricalEquityPreflightPlan {
    /// Requeries the original completed IEX daily coordinate in the existing minimum thirty-day
    /// source window, with actual Raw or Split selection. This is an inert request plan, not
    /// evidence that a response exists or
    /// that units are unchanged. The normal current runtime, identity, calendar, complete capture
    /// and canonical publication authorities must still admit the result independently.
    ///
    /// `analysis_at` is the actual local evaluation clock. The existing 15-minute exclusion is
    /// retained. Inclusive HTTP end is derived from the original authentic exclusive period end;
    /// no midnight, calendar date or provider as-of adjustment clock is manufactured.
    pub fn try_for_original_price_coordinate(
        mapping: AlpacaInstrumentMapping,
        original: &MarketBarObservation,
        adjustment: AlpacaAdjustment,
        analysis_at: Timestamp,
    ) -> Result<Self, AlpacaError> {
        let exclusion = i64::try_from(ALPACA_HISTORICAL_EXCLUSION_NANOS)
            .map_err(|_| AlpacaError::InvalidHistoricalPlan)?;
        let latest_end = analysis_at
            .checked_sub_nanos(exclusion)
            .map_err(|_| AlpacaError::InvalidHistoricalPlan)?;
        let time = original.time_semantics().timestamped_period().ok_or(AlpacaError::InvalidHistoricalPlan)?;
        let end = time
            .period_end_exclusive()
            .checked_sub_nanos(1)
            .map_err(|_| AlpacaError::InvalidHistoricalPlan)?;
        let lookback_nanos = i64::from(ALPACA_HISTORICAL_MIN_LOOKBACK_DAYS)
            .checked_mul(
                i64::try_from(NANOS_PER_DAY).map_err(|_| AlpacaError::InvalidHistoricalPlan)?,
            )
            .ok_or(AlpacaError::InvalidHistoricalPlan)?;
        let start = end
            .checked_sub_nanos(lookback_nanos)
            .map_err(|_| AlpacaError::InvalidHistoricalPlan)?;
        if !matches!(adjustment, AlpacaAdjustment::Raw | AlpacaAdjustment::Split)
            || original.adjustment() != MarketBarAdjustment::Split
            || original.context().provenance().source_id().as_str()
                != "alpaca-basic-iex-market-data"
            || original.context().provenance().instrument_id() != Some(mapping.instrument())
            || original
                .context()
                .provenance()
                .venue_id()
                .is_none_or(|venue| venue.as_str() != IEX_VENUE)
            || original.provider_instrument_id().as_str() != mapping.symbol()
            || original.interval().as_str() != AlpacaTimeframe::day().provider_value()
            || original.feed().as_str() != "iex"
            || time.timestamp_basis() != BarTimestampBasis::PeriodStart
            || time.period_start() < start
            || start.unix_nanos() < HISTORICAL_FLOOR_UNIX_NANOS
            || end <= start
            || time.period_end_exclusive() > latest_end
            || original
                .context()
                .provenance()
                .availability()
                .conservative_available_at()
                .is_none_or(|available| available > analysis_at)
        {
            return Err(AlpacaError::InvalidHistoricalPlan);
        }
        Ok(Self {
            mapping,
            timeframe: AlpacaTimeframe::day(),
            start,
            end,
            adjustment,
            page_limit: NonZeroU16::new(HISTORICAL_PAGE_LIMIT).ok_or(AlpacaError::Protocol)?,
        })
    }
}
