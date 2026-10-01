//! The same all-family source producer joined to three original complete ordinary histories.

use super::*;
use crate::application::research::{
    RecommendationBenchmarkSelection, RecommendationBenchmarkSelectionReadCapability,
};
use futures_util::{FutureExt as _, future::BoxFuture};
use market_squawk_data::{CompleteMarketBarHistoryCursor, CompleteMarketBarHistoryRequest};
use market_squawk_domain::MarketBarAdjustment;
use std::collections::BTreeMap;

/// All returned reads use the one actual post-acquisition cutoff. Original publication roots and
/// native dates remain unchanged; the caller must retain its earlier requested cutoff separately.
pub(crate) struct PreparedHistorySourceActions {
    histories: [CompleteMarketBarHistoryCursor; 3],
    actions: SourceAppliedCorporateActionPlan,
    snapshot_as_of: Timestamp,
}
impl PreparedHistorySourceActions {
    pub(crate) fn histories(&self) -> &[CompleteMarketBarHistoryCursor; 3] {
        &self.histories
    }
    pub(crate) fn actions(&self) -> &SourceAppliedCorporateActionPlan {
        &self.actions
    }
    pub(crate) const fn snapshot_as_of(&self) -> Timestamp {
        self.snapshot_as_of
    }
    pub(crate) fn into_parts(
        self,
    ) -> (
        [CompleteMarketBarHistoryCursor; 3],
        SourceAppliedCorporateActionPlan,
        Timestamp,
    ) {
        (self.histories, self.actions, self.snapshot_as_of)
    }
}

impl SourceActionPreparationCapability {
    /// Retrospective source preparation only: actual new acquisition is never backdated to the
    /// caller's original history selection. The source-qualified plan admits ordinary cash/split
    /// fields only after the existing single overlap selector has reconciled every native row.
    pub(crate) async fn prepare_for_histories(
        &self,
        histories: [CompleteMarketBarHistoryCursor; 3],
        benchmarks: &RecommendationBenchmarkSelection,
        interval: (CalendarDate, CalendarDate),
        valuation_cutoff: Timestamp,
        context: &RequestContext,
    ) -> Result<PreparedHistorySourceActions, ServiceError> {
        check(context)?;
        let started_at = now()?;
        if interval.0 > interval.1
            || valuation_cutoff > started_at
            || histories[1].selection().receipt().instrument_id()
                != benchmarks.primary().instrument_id()
            || histories[2].selection().receipt().instrument_id()
                != benchmarks.accompanying().instrument_id()
        {
            return Err(ServiceError::InvalidRequest);
        }
        let benchmark_reader = RecommendationBenchmarkSelectionReadCapability::new(
            self.research.market_data_instruments().clone(),
        );
        benchmark_reader
            .read_reference(
                benchmarks.reference(),
                context.deadline(),
                context.cancellation(),
            )?
            .ok_or(ServiceError::Unavailable)?;
        let (histories, actions, snapshot_as_of) = self
            .prepare_selected_nominal_histories(
                Vec::from(histories),
                interval,
                valuation_cutoff,
                context,
            )
            .await?;
        benchmark_reader
            .read_reference(
                benchmarks.reference(),
                context.deadline(),
                context.cancellation(),
            )?
            .ok_or(ServiceError::Unavailable)?;
        // Existing fixed premium contract still requires the complete accounting authority.
        actions
            .covered_accounting_plan()
            .map_err(|error| map_plan_error(error, context))?;
        let histories = histories
            .try_into()
            .map_err(|_| ServiceError::InvalidResult)?;
        Ok(PreparedHistorySourceActions {
            histories,
            actions,
            snapshot_as_of,
        })
    }

    // Return the allocation before polling: complete-history preparation must not embed this
    // producer's state or retain its construction frame while its source children run.
    pub(super) fn prepare_selected_nominal_histories<'a>(
        &'a self,
        histories: Vec<CompleteMarketBarHistoryCursor>,
        interval: (CalendarDate, CalendarDate),
        valuation_cutoff: Timestamp,
        context: &'a RequestContext,
    ) -> BoxFuture<
        'a,
        Result<
            (
                Vec<CompleteMarketBarHistoryCursor>,
                SourceAppliedCorporateActionPlan,
                Timestamp,
            ),
            ServiceError,
        >,
    > {
        Box::pin(self.prepare_selected_nominal_histories_impl(
            histories,
            interval,
            valuation_cutoff,
            context,
        ))
    }

    async fn prepare_selected_nominal_histories_impl(
        &self,
        histories: Vec<CompleteMarketBarHistoryCursor>,
        interval: (CalendarDate, CalendarDate),
        valuation_cutoff: Timestamp,
        context: &RequestContext,
    ) -> Result<
        (
            Vec<CompleteMarketBarHistoryCursor>,
            SourceAppliedCorporateActionPlan,
            Timestamp,
        ),
        ServiceError,
    > {
        check(context)?;
        let started_at = now()?;
        if histories.is_empty()
            || histories.len() > 3
            || interval.0 > interval.1
            || valuation_cutoff > started_at
        {
            return Err(ServiceError::InvalidRequest);
        }
        let mut originals = BTreeMap::new();
        for (index, history) in histories.iter().enumerate() {
            let receipt = history.selection().receipt();
            let graph = receipt.date_windows().ok_or(ServiceError::Unavailable)?;
            let dates = graph.requested_dates();
            if receipt.adjustment() != MarketBarAdjustment::Raw
                || history.read_receipt().knowledge_cutoff() > started_at
                || dates.0 > interval.0
                || dates.1 < interval.1
                || history.native_sessions().is_none()
            {
                return Err(ServiceError::Unavailable);
            }
            if let Some(previous) = originals.insert(receipt.instrument_id(), index) {
                same_original(&histories[previous], history)?;
            }
        }
        let instruments: BTreeSet<_> = originals.keys().copied().collect();
        let runtime = self
            .runtime
            .current_alpaca_calendar_runtime(context.deadline(), context.cancellation())
            .await
            .map_err(|_| controlled(context, ServiceError::Unavailable))?;
        let today = new_york_date(started_at)?;
        if interval.1 > today {
            return Err(ServiceError::InvalidRequest);
        }
        let calendar_reference = self
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
        let calendar_at = now()?;
        let calendar = self
            .calendars
            .read_reference(
                &calendar_reference,
                calendar_at,
                context.deadline(),
                context.cancellation().clone(),
            )
            .await
            .map_err(map_calendar_error)?
            .ok_or(ServiceError::Unavailable)?;
        // An explicit finite process-date query supplements complete native economic-date ordinary
        // fields. This lower bound is the requested study date, never a claimed inception date.
        let published = self
            .publish_query(
                &runtime,
                &instruments,
                (interval.0, today),
                &calendar,
                context,
            )
            .boxed()
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
                &calendar_reference,
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
                valuation_cutoff,
                context,
            )
            .await?;
        let mut ordinary = Vec::new();
        ordinary
            .try_reserve_exact(originals.len())
            .map_err(|_| ServiceError::ResourceExhausted)?;
        for &index in originals.values() {
            check(context)?;
            let (history, calendar) = self
                .reopen_original_ordinary_history(&histories[index], cutoff, context)
                .await?;
            let read = self
                .research
                .rejoin_tiingo_eod_history_actions(
                    history,
                    context.deadline(),
                    context.cancellation(),
                )
                .await
                .map_err(map_research_error)?;
            ordinary.push((read, calendar));
        }
        let plan = plan
            .with_complete_ordinary_history(
                ordinary,
                limits()?,
                context.deadline(),
                context.cancellation(),
            )
            .map_err(|error| map_plan_error(error, context))?;
        // Missing monetary units, native fields, sessions, ordering, or applicable lifecycle
        // dispositions remain unavailable. Neither query termination nor empty rows bypasses it.
        plan.covered_price_plan()
            .map_err(|error| map_plan_error(error, context))?;
        plan.price_reference()
            .map_err(|error| map_plan_error(error, context))?;
        let mut reopened = Vec::with_capacity(histories.len());
        for history in &histories {
            reopened.push(
                self.reopen_original_ordinary_history(history, cutoff, context)
                    .await?
                    .0,
            );
        }
        check(context)?;
        Ok((reopened, plan, cutoff))
    }

    pub(super) fn reopen_original_ordinary_history<'a>(
        &'a self,
        original: &'a CompleteMarketBarHistoryCursor,
        cutoff: Timestamp,
        context: &'a RequestContext,
    ) -> BoxFuture<
        'a,
        Result<(CompleteMarketBarHistoryCursor, CompletedMarketSessionRead), ServiceError>,
    > {
        Box::pin(self.reopen_original_ordinary_history_impl(original, cutoff, context))
    }

    async fn reopen_original_ordinary_history_impl(
        &self,
        original: &CompleteMarketBarHistoryCursor,
        cutoff: Timestamp,
        context: &RequestContext,
    ) -> Result<(CompleteMarketBarHistoryCursor, CompletedMarketSessionRead), ServiceError> {
        check(context)?;
        let receipt = original.selection().receipt();
        let graph = receipt.date_windows().ok_or(ServiceError::Unavailable)?;
        let (start, end) = graph.requested_dates();
        let request = CompleteMarketBarHistoryRequest::try_exact_nominal(
            receipt.instrument_id(),
            start,
            end,
            receipt.provider_instrument_id().clone(),
            receipt.venue_id().clone(),
            receipt.feed().clone(),
            receipt.interval().clone(),
            receipt.adjustment(),
            receipt.session_ruleset().clone(),
            cutoff,
            original.selection().pinned().manifest().clone(),
        )
        .and_then(|request| {
            request.try_with_surface_requirement(original.selection().surface_requirement())
        })
        .map_err(|_| ServiceError::InvalidResult)?;
        let reopened = self
            .research
            .analytical_reader()
            .read_complete_market_bar_history_cursor(
                request,
                context.deadline(),
                context.cancellation().clone(),
            )
            .boxed()
            .await
            .map_err(map_analytical_error)?
            .ok_or(ServiceError::Unavailable)?;
        same_original(original, &reopened)?;
        let retained = graph.calendar();
        let reference = CompletedMarketSessionReference::try_from_retained_digests(
            retained.origin_content_digest,
            retained.capture_binding_digest,
        )
        .map_err(|_| ServiceError::InvalidResult)?;
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
        let reopened = self
            .research
            .rejoin_market_history_native_sessions_with_calendar(
                reopened,
                &calendar,
                context.deadline(),
                context.cancellation(),
            )
            .boxed()
            .await
            .map_err(map_research_error)?;
        if reopened.read_receipt().knowledge_cutoff() != cutoff
            || !match (reopened.native_sessions(), original.native_sessions()) {
                (Some(reopened), Some(original)) => reopened
                    .sessions()
                    .same_rows(original.sessions())
                    .map_err(map_analytical_error)?,
                (None, None) => true,
                _ => false,
            }
        {
            return Err(ServiceError::InvalidResult);
        }
        check(context)?;
        Ok((reopened, calendar))
    }
}

fn same_original(
    a: &CompleteMarketBarHistoryCursor,
    b: &CompleteMarketBarHistoryCursor,
) -> Result<(), ServiceError> {
    let a_receipt = a.selection().receipt();
    let b_receipt = b.selection().receipt();
    if a_receipt.origin_manifest() != b_receipt.origin_manifest()
        || a_receipt.receipt_digest() != b_receipt.receipt_digest()
        || a_receipt.binding_digest() != b_receipt.binding_digest()
        || a_receipt.date_windows() != b_receipt.date_windows()
        || a_receipt.bar_set_digest() != b_receipt.bar_set_digest()
        || a.read_receipt().history_content_digest() != b.read_receipt().history_content_digest()
    {
        return Err(ServiceError::InvalidResult);
    }
    let mut a_bars = a.bars();
    let mut b_bars = b.bars();
    loop {
        match (
            a_bars.next().transpose().map_err(map_analytical_error)?,
            b_bars.next().transpose().map_err(map_analytical_error)?,
        ) {
            (None, None) => break,
            (Some(a), Some(b)) if a == b => {}
            _ => return Err(ServiceError::InvalidResult),
        }
    }
    let mut a_actions = a.source_actions();
    let mut b_actions = b.source_actions();
    loop {
        match (
            a_actions.next().transpose().map_err(map_analytical_error)?,
            b_actions.next().transpose().map_err(map_analytical_error)?,
        ) {
            (None, None) => break,
            (Some(a), Some(b)) if a == b => {}
            _ => return Err(ServiceError::InvalidResult),
        }
    }
    Ok(())
}

/// Actual selected subject and optional comparison acquisition under existing source owners.
/// Complete bodies are discarded after the reference records their physical replay coordinates.
pub(crate) struct PreparedSelectedHistorySources {
    subject_manifest: DatasetManifestRef,
    plan: SourceAppliedCorporateActionPlan,
    cutoff: Timestamp,
}
impl PreparedSelectedHistorySources {
    pub(crate) fn subject_manifest(&self) -> &DatasetManifestRef {
        &self.subject_manifest
    }
    pub(crate) fn plan(&self) -> &SourceAppliedCorporateActionPlan {
        &self.plan
    }
    pub(crate) const fn cutoff(&self) -> Timestamp {
        self.cutoff
    }
}
impl SourceActionPreparationCapability {
    pub(crate) fn prepare_selected_complete_histories<'a>(
        &'a self,
        instruments: &'a [MarketDataInstrumentRecord],
        population_start: Timestamp,
        analysis_at: Timestamp,
        context: &'a RequestContext,
    ) -> BoxFuture<'a, Result<PreparedSelectedHistorySources, ServiceError>> {
        Box::pin(self.prepare_selected_complete_histories_impl(
            instruments,
            population_start,
            analysis_at,
            context,
        ))
    }

    async fn prepare_selected_complete_histories_impl(
        &self,
        instruments: &[MarketDataInstrumentRecord],
        population_start: Timestamp,
        analysis_at: Timestamp,
        context: &RequestContext,
    ) -> Result<PreparedSelectedHistorySources, ServiceError> {
        use market_squawk_adapter_alpaca::{
            AlpacaAdjustment, AlpacaHistoricalEquityPreflightPlan, AlpacaHistoricalLookback,
            AlpacaInstrumentMapping, AlpacaTimeframe,
        };
        check(context)?;
        if instruments.is_empty()
            || instruments.len() > 2
            || population_start >= analysis_at
            || analysis_at > now()?
        {
            return Err(ServiceError::InvalidRequest);
        }
        let runtime = self
            .runtime
            .current_alpaca_calendar_runtime(context.deadline(), context.cancellation())
            .await
            .map_err(|_| controlled(context, ServiceError::Unavailable))?;
        let nanos = analysis_at
            .unix_nanos()
            .checked_sub(population_start.unix_nanos())
            .ok_or(ServiceError::InvalidRequest)?;
        let days = nanos
            .checked_add(86_400_000_000_000 - 1)
            .and_then(|n| n.checked_div(86_400_000_000_000))
            .and_then(|n| n.checked_add(1))
            .and_then(|n| u16::try_from(n).ok())
            .ok_or(ServiceError::Unavailable)?;
        let lookback =
            AlpacaHistoricalLookback::try_from_days(days).map_err(|_| ServiceError::Unavailable)?;
        let mut published = Vec::with_capacity(instruments.len());
        let mut unique = BTreeSet::new();
        for record in instruments {
            if !unique.insert(record.definition().instrument_id()) {
                return Err(ServiceError::InvalidRequest);
            }
            let mut listings = record
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
                record.definition().instrument_id(),
                record.definition().asset_class(),
            )
            .map_err(|_| ServiceError::Unavailable)?;
            let plan = AlpacaHistoricalEquityPreflightPlan::try_new(
                mapping,
                AlpacaTimeframe::day(),
                analysis_at,
                lookback,
                AlpacaAdjustment::Raw,
            )
            .map_err(|_| ServiceError::Unavailable)?;
            published.push(
                self.publish_canonical_history(&runtime, plan, record, context)
                    .await?,
            );
        }
        // Inspect original native coordinates, never infer session dates from nominal midnight.
        let inspection_at = now()?;
        let mut interval = None;
        let mut bounds = None;
        for original in &published {
            let history = self
                .reopen_published_history_ref(original, inspection_at, context)
                .boxed()
                .await?;
            let sessions = history
                .native_sessions()
                .ok_or(ServiceError::Unavailable)?
                .sessions();
            let first = sessions
                .first()
                .map_err(map_analytical_error)?
                .ok_or(ServiceError::Unavailable)?;
            let last = sessions
                .last()
                .map_err(map_analytical_error)?
                .ok_or(ServiceError::Unavailable)?;
            let dates = (first.native_date(), last.native_date());
            let times = (first.opens_at(), last.closes_at_exclusive());
            if interval.is_some_and(|prior| prior != dates)
                || bounds.is_some_and(|prior| prior != times)
            {
                return Err(ServiceError::Unavailable);
            }
            interval = Some(dates);
            bounds = Some(times);
            drop(history);
        }
        let (_, terminal_close) = bounds.ok_or(ServiceError::Unavailable)?;
        let dates = interval.ok_or(ServiceError::Unavailable)?;
        let activation = self
            .outcome_history_activation
            .as_ref()
            .ok_or(ServiceError::Unavailable)?;
        let mut nominal = Vec::with_capacity(instruments.len());
        for instrument in instruments {
            // Tiingo's original exchange metadata admits a principal listing, never the IEX
            // feed venue. Its nominal action proof keeps that distinct source identity.
            let mut listings = instrument
                .definition()
                .venue_mappings()
                .iter()
                .filter(|mapping| matches!(mapping.venue_id().as_str(), "ARCX" | "XNYS" | "XNAS"));
            let venue = listings.next().ok_or(ServiceError::Unavailable)?.venue_id();
            if listings.next().is_some() {
                return Err(ServiceError::Unavailable);
            }
            let publication = activation
                .prepare_instrument_eod_history(instrument, venue, &self.calendars, dates, context)
                .boxed()
                .await?;
            let history = self.ingest.read_complete_tiingo_eod_publication(&publication, instrument, venue, dates,
                &self.calendars, now()?, context.deadline(), context.cancellation()).boxed().await
                .map_err(|error| match error {
                    crate::application::research::ingest::TiingoHistoryApplicationError::Read(error) => map_analytical_error(error),
                    crate::application::research::ingest::TiingoHistoryApplicationError::Calendar(error) => map_calendar_error(error),
                    crate::application::research::ingest::TiingoHistoryApplicationError::Research(error) => map_research_error(error),
                    crate::application::research::ingest::TiingoHistoryApplicationError::Ingest(error) => map_ingest_error(error),
                    _ => ServiceError::InvalidResult,
                })?;
            nominal.push(history);
        }
        let (nominal, plan, cutoff) = self
            .prepare_selected_nominal_histories(nominal, dates, terminal_close, context)
            .await?;
        drop(nominal); // Economic action originals remain in the admitted nominal source pool.
        let mut histories = Vec::with_capacity(published.len());
        for original in &published {
            histories.push(
                self.reopen_published_history_ref(original, cutoff, context)
                    .boxed()
                    .await?,
            );
        }
        let plan = plan
            .with_completed_timestamp_price_histories(
                &histories.iter().collect::<Vec<_>>(),
                limits()?,
                context.deadline(),
                context.cancellation(),
            )
            .map_err(|error| map_plan_error(error, context))?;
        plan.covered_price_plan()
            .map_err(|error| map_plan_error(error, context))?;
        let subject_manifest = histories
            .first()
            .ok_or(ServiceError::InvalidResult)?
            .selection()
            .pinned()
            .manifest()
            .clone();
        drop(histories);
        check(context)?;
        Ok(PreparedSelectedHistorySources {
            subject_manifest,
            plan,
            cutoff,
        })
    }
}
