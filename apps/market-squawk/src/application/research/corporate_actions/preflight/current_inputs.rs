//! Bounded source publication before the single final current-feature knowledge cutoff.

use super::super::SourceAppliedCorporateActionPlanReference;
use super::*;
use market_squawk_data::{
    CompleteMarketBarHistoryCursor, CompleteMarketBarHistoryRequest,
    MarketHistoryPriceSurfaceRequirement, Sha256Digest,
};
use market_squawk_domain::{MarketBarAdjustment, ProviderInstrumentId, SourceIdentifier};

/// Process-local pending publication coordinates, never a coverage/absence receipt. It owns no
/// price history. Existing source publishers retain and recover their original raw generations.
pub(crate) struct PendingCurrentPriceActions {
    source: PublishedActionQuery,
    calendar: CompletedMarketSessionReference,
    interval: (CalendarDate, CalendarDate),
    histories: Vec<OriginalCurrentPriceHistory>,
    published_at: Timestamp,
    training: Vec<PendingTrainingPriceActions>,
}
// Raw history bodies never accumulate across the Find population. Only original publication
// coordinates survive acquisition; each training pair is independently reopened at final cutoff.
struct PendingTrainingPriceActions {
    subject: InstrumentId,
    requested_benchmark: Option<InstrumentId>,
    selected_benchmark: Option<InstrumentId>,
    pending: Option<PendingCurrentPriceActions>,
}

struct OriginalCurrentPriceHistory {
    instrument: InstrumentId,
    provider: ProviderInstrumentId,
    venue: VenueId,
    feed: SourceIdentifier,
    interval: SourceIdentifier,
    ruleset: SourceIdentifier,
    dates: (CalendarDate, CalendarDate),
    manifest: DatasetManifestRef,
    receipt_digest: Sha256Digest,
    history_content: Sha256Digest,
    surface: MarketHistoryPriceSurfaceRequirement,
    calendar: CompletedMarketSessionReference,
}
impl PendingCurrentPriceActions {
    pub(crate) fn retain_training(
        &mut self,
        subject: InstrumentId,
        requested_benchmark: Option<InstrumentId>,
        selected_benchmark: Option<InstrumentId>,
        pending: Option<Self>,
    ) -> Result<(), ServiceError> {
        if !self.instruments().any(|id| id == subject)
            || self.training.iter().any(|entry| entry.subject == subject)
            || requested_benchmark
                .is_some_and(|id| selected_benchmark.is_some_and(|selected| selected != id))
            || pending.as_ref().is_some_and(|value| {
                !value.training.is_empty()
                    || !value.instruments().any(|id| id == subject)
                    || value.histories.len() > 2
                    || value
                        .instruments()
                        .any(|id| id != subject && Some(id) != selected_benchmark)
            })
        {
            return Err(ServiceError::InvalidResult);
        }
        self.training.push(PendingTrainingPriceActions {
            subject,
            requested_benchmark,
            selected_benchmark,
            pending,
        });
        Ok(())
    }
    pub(crate) fn instruments(&self) -> impl Iterator<Item = InstrumentId> + '_ {
        self.histories.iter().map(|history| history.instrument)
    }
}
impl SourceActionPreparationCapability {
    /// Publishes one finite query for at most32 authenticated native histories. The interval must
    /// cover the actual requested feature lookback; it is never an all-history absence claim.
    pub(crate) async fn publish_current_price_actions(
        &self,
        histories: &[CompleteMarketBarHistoryCursor],
        interval: (CalendarDate, CalendarDate),
        context: &RequestContext,
    ) -> Result<PendingCurrentPriceActions, ServiceError> {
        check(context)?;
        if histories.is_empty() || histories.len() > 32 || interval.0 > interval.1 {
            return Err(ServiceError::InvalidRequest);
        }
        let started_at = now()?;
        let today = new_york_date(started_at)?;
        if interval.1 > today {
            return Err(ServiceError::InvalidRequest);
        }
        let mut originals = Vec::with_capacity(histories.len());
        let mut instruments = BTreeSet::new();
        for history in histories {
            check(context)?;
            let receipt = history.selection().receipt();
            let graph = receipt.date_windows().ok_or(ServiceError::Unavailable)?;
            let dates = graph.requested_dates();
            if receipt.adjustment() != MarketBarAdjustment::Raw
                || !receipt.current_research_eligible()
                || dates.0 > interval.0
                || dates.1 < interval.1
                || history.native_sessions().is_none()
                || history.read_receipt().knowledge_cutoff() > started_at
                || !instruments.insert(receipt.instrument_id())
            {
                return Err(ServiceError::InvalidResult);
            }
            let calendar = graph.calendar();
            originals.push(OriginalCurrentPriceHistory {
                instrument: receipt.instrument_id(),
                provider: receipt.provider_instrument_id().clone(),
                venue: receipt.venue_id().clone(),
                feed: receipt.feed().clone(),
                interval: receipt.interval().clone(),
                ruleset: receipt.session_ruleset().clone(),
                dates,
                manifest: history.selection().pinned().manifest().clone(),
                receipt_digest: receipt.receipt_digest(),
                history_content: history.read_receipt().history_content_digest(),
                surface: history.selection().surface_requirement(),
                calendar: CompletedMarketSessionReference::try_from_retained_digests(
                    calendar.origin_content_digest,
                    calendar.capture_binding_digest,
                )
                .map_err(|_| ServiceError::InvalidResult)?,
            });
        }
        originals.sort_by_key(|history| history.instrument);
        let runtime = self
            .runtime
            .current_alpaca_calendar_runtime(context.deadline(), context.cancellation())
            .await
            .map_err(|_| controlled(context, ServiceError::Unavailable))?;
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
        let source = self
            .publish_query(
                &runtime,
                &instruments,
                (interval.0, today),
                &calendar,
                context,
            )
            .await?;
        let published_at = now()?;
        if new_york_date(published_at)? != today {
            return Err(ServiceError::Unavailable);
        }
        check(context)?;
        Ok(PendingCurrentPriceActions {
            source,
            calendar: reference,
            interval,
            histories: originals,
            published_at,
            training: Vec::new(),
        })
    }

    /// Training keeps at most two genuine daily histories over a finite 3650-day window.
    pub(crate) async fn publish_training_price_actions(
        &self,
        histories: &[CompleteMarketBarHistoryCursor],
        interval: (CalendarDate, CalendarDate),
        context: &RequestContext,
    ) -> Result<PendingCurrentPriceActions, ServiceError> {
        let days = interval
            .1
            .days_since_unix_epoch()
            .checked_sub(interval.0.days_since_unix_epoch())
            .ok_or(ServiceError::InvalidRequest)?;
        if histories.is_empty()
            || histories.len() > 2
            || !(0..3650).contains(&days)
            || histories.iter().any(|history| history.bar_count() > 3650)
        {
            return Err(ServiceError::InvalidRequest);
        }
        self.publish_current_price_actions(histories, interval, context)
            .await
    }

    /// All batches receive the SAME actual post-acquisition cutoff. Every original publication
    /// is physically reopened; pending scalar coordinates never confer source authority.
    #[allow(
        clippy::type_complexity,
        reason = "bounded original current and training source custody"
    )]
    pub(crate) async fn finish_current_price_actions(
        &self,
        mut pending: PendingCurrentPriceActions,
        cutoff: Timestamp,
        context: &RequestContext,
    ) -> Result<
        (
            SourceAppliedCorporateActionPlanReference,
            Vec<(
                InstrumentId,
                Option<InstrumentId>,
                Option<InstrumentId>,
                Option<SourceAppliedCorporateActionPlanReference>,
            )>,
        ),
        ServiceError,
    > {
        let mut training = std::mem::take(&mut pending.training);
        training.sort_by_key(|entry| entry.subject);
        if !pending
            .instruments()
            .eq(training.iter().map(|entry| entry.subject))
        {
            return Err(ServiceError::InvalidResult);
        }
        // The current feature proof keeps its original two-observation-per-member ceiling.
        let current = self
            .finish_price_actions(pending, cutoff, 64, context)
            .await?;
        let mut references = Vec::with_capacity(training.len());
        for entry in training {
            let reference = match entry.pending {
                Some(pending) => match self
                    .finish_price_actions(pending, cutoff, 2 * 3650, context)
                    .await
                {
                    Ok(reference) => Some(reference),
                    Err(ServiceError::Unavailable | ServiceError::NotFound) => None,
                    Err(error) => return Err(error),
                },
                None => None,
            };
            references.push((
                entry.subject,
                entry.requested_benchmark,
                entry.selected_benchmark,
                reference,
            ));
        }
        check(context)?;
        Ok((current, references))
    }

    async fn finish_price_actions(
        &self,
        pending: PendingCurrentPriceActions,
        cutoff: Timestamp,
        maximum_bars: usize,
        context: &RequestContext,
    ) -> Result<SourceAppliedCorporateActionPlanReference, ServiceError> {
        check(context)?;
        if cutoff < pending.published_at
            || cutoff > now()?
            || new_york_date(cutoff)? != new_york_date(pending.published_at)?
        {
            return Err(ServiceError::InvalidRequest);
        }
        let calendar = self
            .calendars
            .read_reference(
                &pending.calendar,
                cutoff,
                context.deadline(),
                context.cancellation().clone(),
            )
            .await
            .map_err(map_calendar_error)?
            .ok_or(ServiceError::Unavailable)?;
        let instruments = pending.instruments().collect();
        let plan = self
            .read_query(
                pending.source,
                calendar,
                instruments,
                pending.interval,
                cutoff,
                cutoff,
                context,
            )
            .await?;
        let mut reads = Vec::with_capacity(pending.histories.len());
        // One bounded pair for training, or the original two-observation current batch.
        let mut total_bars = 0_usize;
        for original in pending.histories {
            check(context)?;
            let request = CompleteMarketBarHistoryRequest::try_exact_nominal(
                original.instrument,
                original.dates.0,
                original.dates.1,
                original.provider,
                original.venue,
                original.feed,
                original.interval,
                MarketBarAdjustment::Raw,
                original.ruleset,
                cutoff,
                original.manifest,
            )
            .and_then(|request| request.try_with_surface_requirement(original.surface))
            .map_err(|_| ServiceError::InvalidResult)?;
            let history = self
                .research
                .analytical_reader()
                .read_complete_market_bar_history_cursor(
                    request,
                    context.deadline(),
                    context.cancellation().clone(),
                )
                .await
                .map_err(super::super::super::map_read_error)?
                .ok_or(ServiceError::Unavailable)?;
            if history.selection().receipt().receipt_digest() != original.receipt_digest
                || history.read_receipt().history_content_digest() != original.history_content
            {
                return Err(ServiceError::InvalidResult);
            }
            total_bars = total_bars
                .checked_add(history.bar_count())
                .filter(|bars| *bars <= maximum_bars)
                .ok_or(ServiceError::ResourceExhausted)?;
            let calendar = self
                .calendars
                .read_reference(
                    &original.calendar,
                    cutoff,
                    context.deadline(),
                    context.cancellation().clone(),
                )
                .await
                .map_err(map_calendar_error)?
                .ok_or(ServiceError::Unavailable)?;
            let history = self
                .research
                .rejoin_market_history_native_sessions_with_calendar(
                    history,
                    &calendar,
                    context.deadline(),
                    context.cancellation(),
                )
                .await
                .map_err(map_research_error)?;
            let read = self
                .research
                .rejoin_tiingo_eod_history_actions(
                    history,
                    context.deadline(),
                    context.cancellation(),
                )
                .await
                .map_err(map_research_error)?;
            reads.push((read, calendar));
        }
        let plan = plan
            .with_complete_ordinary_history(
                reads,
                limits()?,
                context.deadline(),
                context.cancellation(),
            )
            .map_err(|error| map_plan_error(error, context))?;
        let reference = plan
            .price_reference()
            .map_err(|error| map_plan_error(error, context))?;
        if reference.knowledge_cutoff() != cutoff {
            return Err(ServiceError::InvalidResult);
        }
        check(context)?;
        Ok(reference)
    }
}
