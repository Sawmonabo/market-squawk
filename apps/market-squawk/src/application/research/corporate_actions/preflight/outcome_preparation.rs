//! Later ordinary-history acquisition through the installed source owners.

use super::super::SourceAppliedCorporateActionPlanReference;
use super::*;
use crate::application::model::forecast::ForecastOutcomePreparationOrigin;
use market_squawk_data::{
    MarketDataInstrumentPopulationDisposition, MarketDataInstrumentPopulationQuery,
};

pub(crate) struct PreparedForecastOutcomeMeasurement {
    manifest: DatasetManifestRef,
    reference: SourceAppliedCorporateActionPlanReference,
    cutoff: Timestamp,
}
impl PreparedForecastOutcomeMeasurement {
    pub(crate) fn manifest(&self) -> &DatasetManifestRef {
        &self.manifest
    }
    pub(crate) fn reference(&self) -> &SourceAppliedCorporateActionPlanReference {
        &self.reference
    }
    pub(crate) fn cutoff(&self) -> Timestamp {
        self.cutoff
    }
}
pub(crate) enum ForecastOutcomeSourcePreparation {
    Prepared(PreparedForecastOutcomeMeasurement),
    NotYetCompleted,
    SourceUnavailable,
    UnsupportedHistory,
}
impl SourceActionPreparationCapability {
    pub(crate) async fn prepare_outcome_measurement(
        &self,
        origin: ForecastOutcomePreparationOrigin,
        context: &RequestContext,
    ) -> Result<ForecastOutcomeSourcePreparation, ServiceError> {
        check(context)?;
        if origin.target_at() > now()? {
            return Ok(ForecastOutcomeSourcePreparation::NotYetCompleted);
        }
        if origin.event_target().is_some() {
            return match self.acquire_event_outcome_measurement(&origin, context).await {
                Ok(Some(value)) => Ok(ForecastOutcomeSourcePreparation::Prepared(value)),
                Ok(None) | Err(ServiceError::Unavailable | ServiceError::NotFound) => Ok(ForecastOutcomeSourcePreparation::SourceUnavailable),
                Err(error) => Err(error),
            };
        }
        // This is the existing nominal full-history owner. Never substitute its source for an
        // unrelated saved origin, or assign a provider date to a timestamp-only observation.
        if origin.source_id().as_str() != "tiingo-starter"
            || origin.native_origin_date().is_none()
            || origin
                .origin_bar()
                .time_semantics()
                .nominal_daily_date()
                .is_none()
        {
            return Ok(ForecastOutcomeSourcePreparation::UnsupportedHistory);
        }
        match self.acquire_outcome_measurement(origin, context).await {
            Ok(Some(value)) => Ok(ForecastOutcomeSourcePreparation::Prepared(value)),
            Ok(None) | Err(ServiceError::Unavailable | ServiceError::NotFound) => {
                check(context)?;
                Ok(ForecastOutcomeSourcePreparation::SourceUnavailable)
            }
            Err(error) => Err(error),
        }
    }

    async fn acquire_outcome_measurement(
        &self,
        origin: ForecastOutcomePreparationOrigin,
        context: &RequestContext,
    ) -> Result<Option<PreparedForecastOutcomeMeasurement>, ServiceError> {
        let activation = self
            .outcome_history_activation
            .as_ref()
            .ok_or(ServiceError::Unavailable)?;
        let Some(coordinates) = self.resolve_outcome_preparation(origin, context).await? else {
            return Ok(None);
        };
        let selected_at = now()?;
        let query = MarketDataInstrumentPopulationQuery::try_new(
            vec![coordinates.instrument_id()],
            selected_at,
            selected_at,
        )
        .map_err(|_| ServiceError::InvalidResult)?;
        let reader = self.research.market_data_instruments().clone();
        let deadline = context.deadline();
        let selected = self
            .research
            .run_owned_research_io(deadline, context.cancellation(), move |cancellation| {
                reader.pin_population_as_of(query, deadline, &cancellation)
            })
            .await
            .map_err(map_research_error)?
            .map_err(crate::application::research::map_market_definition_read_error)?;
        if selected.disposition() != MarketDataInstrumentPopulationDisposition::Complete {
            return Ok(None);
        }
        let [record] = selected.records() else {
            return Err(ServiceError::InvalidResult);
        };
        let publication = activation
            .prepare_instrument_eod_history(
                record,
                coordinates.venue_id(),
                &self.calendars,
                coordinates.native_dates(),
                context,
            )
            .await?;
        let history_cutoff = now()?;
        if history_cutoff < selected_at {
            return Err(ServiceError::Unavailable);
        }
        let history = self
            .ingest
            .read_complete_tiingo_eod_publication(
                &publication,
                record,
                coordinates.venue_id(),
                coordinates.native_dates(),
                &self.calendars,
                history_cutoff,
                context.deadline(),
                context.cancellation(),
            )
            .await
            .map_err(|error| {
                use crate::application::research::ingest::TiingoHistoryApplicationError as E;
                // This exact retained-publication reader emits only admission and typed reads.
                match error {
                    E::Read(error) => map_analytical_error(error),
                    E::Calendar(error) => map_calendar_error(error),
                    E::Research(error) => map_research_error(error),
                    E::Ingest(error) => map_ingest_error(error),
                    _ => ServiceError::InvalidResult,
                }
            })?;
        let Some(native) = history.native_sessions() else {
            return Ok(None);
        };
        if !native.sessions().iter().any(|session| {
            session.native_date() == coordinates.native_dates().1
                && session.bar_present()
                && session.closes_at_exclusive() == coordinates.target_at()
        }) {
            return Ok(None);
        }
        let manifest = history.selection().pinned().manifest().clone();
        let source = self
            .prepare_for_forecast_outcome(
                &history,
                coordinates.native_dates(),
                coordinates.target_at(),
                context,
            )
            .await?;
        check(context)?;
        Ok(Some(PreparedForecastOutcomeMeasurement {
            manifest,
            reference: source.reference().clone(),
            cutoff: source.cutoff(),
        }))
    }
}

impl SourceActionPreparationCapability {
    async fn acquire_event_outcome_measurement(
        &self, origin: &ForecastOutcomePreparationOrigin, context: &RequestContext,
    ) -> Result<Option<PreparedForecastOutcomeMeasurement>, ServiceError> {
        use market_squawk_data::{ProbabilityEventTarget as E, FeatureDatasetProductContract as C,
            ForecastDatasetReadLimits};
        let event = origin.event_target().ok_or(ServiceError::InvalidRequest)?;
        let contract = match event {
            E::PriceHigher => C::PriceReturnMacroContextFixedHorizonPriceHigherAnalysisV1,
            E::BenchmarkOutperformance { .. } => C::PriceReturnMacroContextFixedHorizonBenchmarkOutperformanceAnalysisV1,
            E::ProfitAfterCosts { .. } => C::PriceReturnMacroContextFixedHorizonProfitAfterCostsAnalysisV1,
        };
        let selected_at = now()?;
        let original = self.research.analytical_reader().forecast_dataset_evidence(contract,
            origin.analysis_evidence().manifest(), selected_at,
            ForecastDatasetReadLimits::try_new(100_000, 256 * 1024 * 1024).map_err(|_| ServiceError::Internal)?,
            context.deadline(), context.cancellation().clone()).await.map_err(map_analytical_error)?;
        if original.probability_event_target() != Some(event)
            || original.dataset().production_receipt().production_identity() != origin.analysis_evidence().production_identity_sha256()
            || original.dataset().production_receipt().receipt_sha256() != origin.analysis_evidence().production_receipt_sha256() {
            return Err(ServiceError::InvalidResult);
        }
        let study = original.dataset().study_policy().ok_or(ServiceError::InvalidResult)?;
        let first = original.rows().iter().filter(|row| row.instrument_id() == origin.instrument_id())
            .filter_map(|row| row.observed_effective_at()).min().ok_or(ServiceError::InvalidResult)?;
        // A bounded query envelope before the first original training origin also retains its
        // preceding feature observation; no date in this envelope becomes an invented session.
        let start = first.checked_sub_nanos(7 * 86_400_000_000_000).map_err(|_| ServiceError::InvalidResult)?;
        let mut instruments = vec![origin.instrument_id()];
        let mut nominal_benchmark = None;
        if let E::BenchmarkOutperformance { benchmark_instrument_id, benchmark_definition } = event {
            let selected = crate::application::RecommendationBenchmarkSelectionReadCapability::new(self.research.market_data_instruments())
                .select_comparison(Some(benchmark_instrument_id), study.snapshot_as_of(), study.snapshot_as_of(),
                    context.deadline(), context.cancellation())?.ok_or(ServiceError::Unavailable)?;
            if selected.reference_revision_digest() != benchmark_definition { return Ok(None); }
            if origin.origin_bar().time_semantics().nominal_daily_date().is_some() {
                nominal_benchmark = self.original_nominal_benchmark_receipt(&original,
                    benchmark_instrument_id, study.snapshot_as_of(), origin.origin_at(), context).await?;
                if nominal_benchmark.is_none() { return Ok(None); }
            }
            instruments.push(benchmark_instrument_id);
        }
        drop(original);
        let query = MarketDataInstrumentPopulationQuery::try_new(instruments.clone(), selected_at, selected_at)
            .map_err(|_| ServiceError::InvalidResult)?;
        let selected = self.research.market_data_instruments().pin_population_as_of(query, context.deadline(), context.cancellation())
            .map_err(crate::application::research::map_market_definition_read_error)?;
        if selected.disposition() != MarketDataInstrumentPopulationDisposition::Complete { return Ok(None); }
        let records = instruments.iter().map(|instrument| selected.records().iter()
            .find(|record| record.definition().instrument_id() == *instrument).cloned().ok_or(ServiceError::Unavailable))
            .collect::<Result<Vec<_>, _>>()?;
        if origin.origin_bar().time_semantics().timestamped_period().is_some() {
            let runtime = self.runtime.current_alpaca_calendar_runtime(context.deadline(), context.cancellation())
                .await.map_err(|_| controlled(context, ServiceError::Unavailable))?;
            if runtime.historical_metadata().source_id() != origin.source_id() { return Ok(None); }
            let prepared = self.prepare_selected_complete_histories(&records, start, selected_at, context).await?;
            if prepared.cutoff() < origin.target_at() { return Ok(None); }
            return Ok(Some(PreparedForecastOutcomeMeasurement {
                manifest: prepared.subject_manifest().clone(),
                reference: prepared.plan().price_reference().map_err(|error| map_plan_error(error, context))?,
                cutoff: prepared.cutoff(),
            }));
        }
        if origin.source_id().as_str() != "tiingo-starter" { return Ok(None); }
        let activation = self.outcome_history_activation.as_ref().ok_or(ServiceError::Unavailable)?;
        let calendar_ref = self.calendars.preflight(origin.venue_id(),
            start.utc_calendar_date().map_err(|_| ServiceError::InvalidResult)?,
            selected_at.utc_calendar_date().map_err(|_| ServiceError::InvalidResult)?,
            context.deadline(), context.cancellation().clone()).await.map_err(map_calendar_error)?
            .ok_or(ServiceError::Unavailable)?;
        let calendar_at = now()?;
        let calendar = self.calendars.read_reference(&calendar_ref, calendar_at, context.deadline(), context.cancellation().clone())
            .await.map_err(map_calendar_error)?.ok_or(ServiceError::Unavailable)?;
        let mut completed = calendar.native_session_replay().sessions().iter().filter_map(|native|
            calendar.date_session_on(native.date(), calendar_at, calendar_at))
            .filter(|session| session.closes_at_exclusive() <= calendar_at);
        let first_session = completed.next().ok_or(ServiceError::Unavailable)?;
        let last_session = completed.last().ok_or(ServiceError::Unavailable)?;
        let dates = (first_session.date(), last_session.date());
        if last_session.closes_at_exclusive() < origin.target_at() { return Ok(None); }
        let mut histories = Vec::with_capacity(records.len());
        for record in &records {
            let original_benchmark = nominal_benchmark.as_ref()
                .filter(|receipt| receipt.instrument_id() == record.definition().instrument_id());
            // Each original series retains its own listing and calendar. A subject's venue
            // cannot relabel an independently admitted comparison (for example XNAS vs ARCX).
            let venue = original_benchmark.map_or(origin.venue_id(), |receipt| receipt.venue_id());
            if record.definition().instrument_id() != origin.instrument_id() && original_benchmark.is_none() {
                return Err(ServiceError::InvalidResult);
            }
            let mut mappings = record.definition().venue_mappings().iter().filter(|mapping| mapping.venue_id() == venue);
            let Some(mapping) = mappings.next() else { return Ok(None); };
            if mappings.next().is_some() || original_benchmark.is_some_and(|receipt|
                mapping.venue_symbol().as_str() != receipt.provider_instrument_id().as_str()) {
                return Ok(None);
            }
            let publication = activation.prepare_instrument_eod_history(record, venue, &self.calendars, dates, context).await?;
            let history = self.ingest.read_complete_tiingo_eod_publication(&publication, record, venue, dates,
                &self.calendars, now()?, context.deadline(), context.cancellation()).await
                .map_err(|error| match error {
                    crate::application::research::ingest::TiingoHistoryApplicationError::Read(error) => map_analytical_error(error),
                    crate::application::research::ingest::TiingoHistoryApplicationError::Calendar(error) => map_calendar_error(error),
                    crate::application::research::ingest::TiingoHistoryApplicationError::Research(error) => map_research_error(error),
                    crate::application::research::ingest::TiingoHistoryApplicationError::Ingest(error) => map_ingest_error(error),
                    _ => ServiceError::InvalidResult,
                })?;
            if let Some(original) = original_benchmark {
                let actual = history.selection().receipt();
                if actual.source_id() != original.source_id()
                    || actual.provider_instrument_id() != original.provider_instrument_id()
                    || actual.venue_id() != original.venue_id()
                    || actual.feed() != original.feed() || actual.interval() != original.interval()
                    || actual.adjustment() != original.adjustment() || actual.currency() != original.currency()
                    || actual.timestamp_basis() != original.timestamp_basis()
                    || actual.session_kind() != original.session_kind()
                    || actual.session_ruleset() != original.session_ruleset() {
                    return Ok(None);
                }
            }
            histories.push(history);
        }
        let (histories, plan, cutoff) = self.prepare_selected_nominal_histories(histories, dates, last_session.closes_at_exclusive(), context).await?;
        let manifest = histories.first().ok_or(ServiceError::InvalidResult)?.selection().pinned().manifest().clone();
        let reference = plan.price_reference().map_err(|error| map_plan_error(error, context))?;
        drop(histories);
        Ok(Some(PreparedForecastOutcomeMeasurement { manifest, reference, cutoff }))
    }
}

impl SourceActionPreparationCapability {
    /// Reopens the comparison's original history/calendar from the saved Analysis parents.
    /// A current listing or the subject's venue cannot choose this source identity.
    async fn original_nominal_benchmark_receipt(
        &self,
        analysis: &market_squawk_data::ForecastDatasetEvidence,
        instrument: InstrumentId,
        cutoff: Timestamp,
        origin: Timestamp,
        context: &RequestContext,
    ) -> Result<Option<market_squawk_data::MarketBarHistoryPublicationReceipt>, ServiceError> {
        let parents = analysis.dataset().generation().parents();
        if parents.len() > 1024 { return Err(ServiceError::ResourceExhausted); }
        let mut original = None;
        let analytical = self.research.analytical_reader();
        for parent in parents {
            check(context)?;
            let Some(request) = analytical.exact_canonical_market_bar_history_window(
                instrument, parent.manifest().content_hash(),
                market_squawk_data::MarketHistorySelectionPolicy::COMPLETE_DAILY_RAW_V1,
                cutoff, context.deadline(), context.cancellation()).map_err(map_analytical_error)?
                else { continue; };
            if request.exact_manifest() != Some(parent.manifest()) { return Err(ServiceError::InvalidResult); }
            let Some(history) = analytical.read_canonical_market_bar_history(request,
                context.deadline(), context.cancellation().clone()).await.map_err(map_analytical_error)?
                else { continue; };
            let receipt = history.selection().receipt();
            let Some(graph) = receipt.date_windows() else { continue; };
            if receipt.source_id().as_str() != "tiingo-starter" { continue; }
            let reference = CompletedMarketSessionReference::try_from_retained_digests(
                graph.calendar().origin_content_digest, graph.calendar().capture_binding_digest)
                .map_err(|_| ServiceError::InvalidResult)?;
            let calendar = self.calendars.read_reference(&reference, cutoff,
                context.deadline(), context.cancellation().clone()).await.map_err(map_calendar_error)?
                .ok_or(ServiceError::Unavailable)?;
            if calendar.venue_id() != receipt.venue_id() { return Err(ServiceError::InvalidResult); }
            let history = self.research.rejoin_market_history_native_sessions_with_calendar(
                history, &calendar, context.deadline(), context.cancellation()).await.map_err(map_research_error)?;
            let native = history.native_sessions().ok_or(ServiceError::InvalidResult)?;
            if !native.sessions().iter().any(|session| session.bar_present() && session.closes_at_exclusive() == origin) {
                continue;
            }
            if original.is_some() { return Err(ServiceError::InvalidResult); }
            original = Some(history.selection().receipt().clone());
            // Only the original receipt is needed while fresh acquisition runs; complete bodies
            // and calendar replay owners leave this scope before the next parent is considered.
        }
        check(context)?;
        Ok(original)
    }
}
