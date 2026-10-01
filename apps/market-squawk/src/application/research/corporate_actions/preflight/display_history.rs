//! Finite display-history preparation under the already active source runtime.

use super::*;
use crate::application::research::MarketHistoryReadCapability;
use market_squawk_adapter_alpaca::{
    ALPACA_HISTORICAL_MIN_LOOKBACK_DAYS, AlpacaAdjustment, AlpacaHistoricalEquityPreflightPlan,
    AlpacaHistoricalLookback, AlpacaInstrumentMapping, AlpacaTimeframe,
};

impl SourceActionPreparationCapability {
    /// Warms display evidence only. Each missing instrument gets one ordinary raw daily
    /// acquisition through the existing canonical publisher; successful publications survive a
    /// later failure. The lifecycle caller retains ownership of the healthy source connection.
    pub(crate) async fn prepare_market_display_histories(
        &self,
        runtime: &AlpacaHistoricalRuntimeCapability,
        instruments: &[MarketDataInstrumentRecord],
        context: &RequestContext,
    ) -> Result<(), ServiceError> {
        check(context)?;
        if instruments.is_empty() {
            return Ok(());
        }
        let mut unique = BTreeSet::new();
        for instrument in instruments {
            if !unique.insert(instrument.definition().instrument_id()) {
                return Err(ServiceError::InvalidRequest);
            }
        }
        runtime
            .require_current(context.deadline(), context.cancellation())
            .await
            .map_err(|error| {
                tracing::warn!(
                    ?error,
                    stage = "active-calendar-runtime",
                    "market display history preparation unavailable"
                );
                controlled(context, ServiceError::Unavailable)
            })?;
        let analysis_at = now()?;
        let calendar = self
            .calendars
            .select(
                analysis_at,
                context.deadline(),
                context.cancellation().clone(),
            )
            .await
            .map_err(|error| {
                tracing::warn!(
                    ?error,
                    stage = "current-calendar-selection-error",
                    "market display history preparation unavailable"
                );
                map_calendar_error(error)
            })?
            .ok_or_else(|| {
                tracing::warn!(
                    stage = "current-calendar-selection",
                    "market display history preparation unavailable"
                );
                ServiceError::Unavailable
            })?;
        if calendar.venue_id().as_str() != "iex" {
            tracing::warn!(
                stage = "current-calendar-venue",
                "market display history preparation unavailable"
            );
            return Err(ServiceError::InvalidResult);
        }
        let history = MarketHistoryReadCapability::new(self.research.analytical_reader());
        let mut first_failure = None;
        for instrument in instruments {
            check(context)?;
            runtime
                .require_current(context.deadline(), context.cancellation())
                .await
                .map_err(map_capability_error)?;
            let instrument_id = instrument.definition().instrument_id();
            let mut stage = "configured-iex-mapping";
            let result = async {
                stage = "configured-iex-mapping";
                let mut listings = instrument
                    .definition()
                    .venue_mappings()
                    .iter()
                    .filter(|mapping| mapping.venue_id().as_str() == "iex");
                let listing = listings.next().ok_or(ServiceError::Unavailable)?;
                if listings.next().is_some() {
                    return Err(ServiceError::Unavailable);
                }
                let mapping = AlpacaInstrumentMapping::try_new(
                    listing.venue_symbol().as_str().to_owned(),
                    instrument_id,
                    instrument.definition().asset_class(),
                )
                .map_err(|_| ServiceError::Unavailable)?;
                // This is the provider's existing minimum initial request, never a cap on
                // retained canonical history or the read-only display projection.
                let lookback =
                    AlpacaHistoricalLookback::try_from_days(ALPACA_HISTORICAL_MIN_LOOKBACK_DAYS)
                        .map_err(|_| ServiceError::Internal)?;
                stage = "raw-daily-plan";
                let plan = AlpacaHistoricalEquityPreflightPlan::try_new(
                    mapping,
                    AlpacaTimeframe::day(),
                    analysis_at,
                    lookback,
                    AlpacaAdjustment::Raw,
                )
                .map_err(|_| ServiceError::Unavailable)?;
                let (latest_session, _) = calendar
                    .latest_completed_regular_session(analysis_at, plan.end(), analysis_at)
                    .map_err(map_calendar_error)?;
                let latest_session = latest_session.ok_or(ServiceError::Unavailable)?;
                stage = "retained-close-read";
                if let Some(retained) = history
                    .read_latest_previous_close(&self.research, instrument_id, analysis_at, context)
                    .await?
                {
                    if retained.instrument_id() != instrument_id
                        || retained.currency() != instrument.definition().quote_currency()
                    {
                        return Err(ServiceError::InvalidResult);
                    }
                    if retained.native_date() >= latest_session.date()
                        && retained.session_close() >= latest_session.closes_at_exclusive()
                    {
                        return Ok(());
                    }
                }
                stage = "canonical-publication";
                self.publish_canonical_history(runtime, plan, instrument, context)
                    .await?;
                stage = "published-close-read";
                let retained = history
                    .read_latest_previous_close(&self.research, instrument_id, now()?, context)
                    .await?
                    .ok_or(ServiceError::Unavailable)?;
                if retained.instrument_id() != instrument_id
                    || retained.currency() != instrument.definition().quote_currency()
                {
                    return Err(ServiceError::InvalidResult);
                }
                // A provider may not have published its newest daily bar yet. Preserve an
                // older genuine completed close without looping or rejecting its evidence.
                Ok(())
            }
            .await;
            if let Err(error) = result {
                if matches!(
                    error,
                    ServiceError::Cancelled | ServiceError::DeadlineExceeded
                ) {
                    return Err(error);
                }
                tracing::warn!(
                    %instrument_id,
                    ?error,
                    stage,
                    "market display history preparation unavailable"
                );
                first_failure.get_or_insert(error);
            }
        }
        runtime
            .require_current(context.deadline(), context.cancellation())
            .await
            .map_err(map_capability_error)?;
        check(context)?;
        first_failure.map_or(Ok(()), Err)
    }
}
