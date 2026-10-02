//! Finite display-history preparation under the already active source runtime.

use super::*;
use crate::application::{ResearchIngestCommitAuthority, research::MarketHistoryReadCapability};
use market_squawk_adapter_alpaca::{
    ALPACA_HISTORICAL_MIN_LOOKBACK_DAYS, AlpacaAdjustment, AlpacaHistoricalEquityPreflightPlan,
    AlpacaHistoricalLookback, AlpacaInstrumentMapping, AlpacaTimeframe,
};
use market_squawk_data::CompleteMarketBarHistoryOutput;
use market_squawk_domain::MarketBarAdjustment;

impl SourceActionPreparationCapability {
    /// Acquires and exactly reopens the selected chart's admitted adjusted daily coverage.
    /// The durable job owns cancellation, deadline, per-instrument admission and commit authority.
    pub(crate) async fn prepare_selected_market_chart_history(
        &self,
        instrument: &MarketDataInstrumentRecord,
        lookback: AlpacaHistoricalLookback,
        captured_at: Timestamp,
        commit: Arc<dyn ResearchIngestCommitAuthority>,
        context: &RequestContext,
    ) -> Result<CompleteMarketBarHistoryOutput, ServiceError> {
        check(context)?;
        if captured_at > now()? || instrument.published_at() > captured_at {
            return Err(ServiceError::InvalidRequest);
        }
        let runtime = self
            .runtime
            .current_alpaca_calendar_runtime(context.deadline(), context.cancellation())
            .await
            .map_err(|_| controlled(context, ServiceError::Unavailable))?;
        runtime
            .require_current(context.deadline(), context.cancellation())
            .await
            .map_err(map_capability_error)?;
        let plan = display_history_plan(instrument, captured_at, lookback, AlpacaAdjustment::All)?;
        let published = self
            .publish_canonical_history_with_commit(
                &runtime,
                plan,
                instrument,
                Some(commit),
                context,
            )
            .await?;
        // This read is pinned to the actual committed manifest; it never selects another latest
        // window or substitutes raw prices. Projection is independently derived by Market.GetHistory.
        let history = self
            .reopen_published_history(published, now()?, context)
            .await?;
        let receipt = history.selection().receipt();
        if receipt.instrument_id() != instrument.definition().instrument_id()
            || receipt.adjustment() != MarketBarAdjustment::All
            || !receipt.current_research_eligible()
            || history.bars().is_empty()
            || receipt.bar_count() != history.bars().len()
        {
            return Err(ServiceError::InvalidResult);
        }
        runtime
            .require_current(context.deadline(), context.cancellation())
            .await
            .map_err(map_capability_error)?;
        check(context)?;
        Ok(history)
    }

    /// Warms display evidence only. A missing instrument gets one ordinary raw daily
    /// acquisition through the existing canonical publisher; successful publications survive a
    /// later failure. The lifecycle caller retains ownership of the healthy source connection.
    pub(crate) async fn prepare_market_display_history(
        &self,
        runtime: &AlpacaHistoricalRuntimeCapability,
        instrument: &MarketDataInstrumentRecord,
        retained_only: bool,
        context: &RequestContext,
    ) -> Result<(), ServiceError> {
        let started = Instant::now();
        let instrument_id = instrument.definition().instrument_id();
        let mut stage = "active-calendar-runtime";
        let result = async {
            check(context)?;
            runtime
                .require_current(context.deadline(), context.cancellation())
                .await
                .map_err(map_capability_error)?;
            let history = MarketHistoryReadCapability::new(self.research.analytical_reader());
            let published = async {
                // A later committed history generation only refreshes its retained projection.
                // It never turns a data-change notification into another provider acquisition.
                if retained_only {
                    if let Some(retained) = history
                        .prepare_latest_previous_close(
                            &self.research,
                            instrument_id,
                            now()?,
                            context,
                        )
                        .await?
                    {
                        if retained.instrument_id() != instrument_id
                            || retained.currency() != instrument.definition().quote_currency()
                        {
                            return Err(ServiceError::InvalidResult);
                        }
                    }
                    return Ok(false);
                }
                stage = "current-calendar-selection";
                let analysis_at = now()?;
                let calendar = self
                    .calendars
                    .select(
                        analysis_at,
                        context.deadline(),
                        context.cancellation().clone(),
                    )
                    .await
                    .map_err(map_calendar_error)?
                    .ok_or(ServiceError::Unavailable)?;
                stage = "current-calendar-venue";
                if calendar.venue_id().as_str() != "iex" {
                    return Err(ServiceError::InvalidResult);
                }
                // This is the provider's existing minimum initial request, never a cap on
                // retained canonical history or the read-only display projection.
                let lookback =
                    AlpacaHistoricalLookback::try_from_days(ALPACA_HISTORICAL_MIN_LOOKBACK_DAYS)
                        .map_err(|_| ServiceError::Internal)?;
                stage = "raw-daily-plan";
                let plan =
                    display_history_plan(instrument, analysis_at, lookback, AlpacaAdjustment::Raw)?;
                let (latest_session, _) = calendar
                    .latest_completed_regular_session(analysis_at, plan.end(), analysis_at)
                    .map_err(map_calendar_error)?;
                let latest_session = latest_session.ok_or(ServiceError::Unavailable)?;
                stage = "retained-close-read";
                if let Some(retained) = history
                    .prepare_latest_previous_close(
                        &self.research,
                        instrument_id,
                        analysis_at,
                        context,
                    )
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
                        return Ok(false);
                    }
                }
                stage = "canonical-publication";
                self.publish_canonical_history(runtime, plan, instrument, context)
                    .await?;
                stage = "published-close-read";
                let retained = history
                    .prepare_latest_previous_close(&self.research, instrument_id, now()?, context)
                    .await?
                    .ok_or(ServiceError::Unavailable)?;
                if retained.instrument_id() != instrument_id
                    || retained.currency() != instrument.definition().quote_currency()
                {
                    return Err(ServiceError::InvalidResult);
                }
                // A provider may not have published its newest daily bar yet. Preserve an
                // older genuine completed close without looping or rejecting its evidence.
                Ok(true)
            }
            .await?;
            stage = "final-runtime-validation";
            runtime
                .require_current(context.deadline(), context.cancellation())
                .await
                .map_err(map_capability_error)?;
            check(context)?;
            Ok(published)
        }
        .await;
        match result {
            Ok(published) => {
                tracing::info!(
                    %instrument_id,
                    published,
                    elapsed_ms = started.elapsed().as_millis(),
                    "market display history completed close verified"
                );
                Ok(())
            }
            Err(ServiceError::Cancelled) => Err(ServiceError::Cancelled),
            Err(error) => {
                tracing::warn!(
                    %instrument_id,
                    ?error,
                    stage,
                    elapsed_ms = started.elapsed().as_millis(),
                    "market display history preparation unavailable"
                );
                Err(error)
            }
        }
    }
}

/// Constructs the exact native IEX daily plan shared by raw-close and selected-chart acquisition.
fn display_history_plan(
    instrument: &MarketDataInstrumentRecord,
    analysis_at: Timestamp,
    lookback: AlpacaHistoricalLookback,
    adjustment: AlpacaAdjustment,
) -> Result<AlpacaHistoricalEquityPreflightPlan, ServiceError> {
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
        instrument.definition().instrument_id(),
        instrument.definition().asset_class(),
    )
    .map_err(|_| ServiceError::Unavailable)?;
    AlpacaHistoricalEquityPreflightPlan::try_new(
        mapping,
        AlpacaTimeframe::day(),
        analysis_at,
        lookback,
        adjustment,
    )
    .map_err(|_| ServiceError::Unavailable)
}
