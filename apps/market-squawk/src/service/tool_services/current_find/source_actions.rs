//! Current feature source acquisition within the existing serialized Find preparation.
use super::*;
use crate::application::RecommendationBenchmarkSelectionReadCapability;
use crate::application::{
    PendingCurrentPriceActions, PreparedFindPopulation,
    market_calendar::ForecastSessionCohortReference,
};
use market_squawk_data::{
    CompleteMarketBarHistoryCursor, MarketDataInstrumentPopulationDisposition,
    MarketDataInstrumentPopulationQuery, MarketDataInstrumentRecord,
};
use market_squawk_domain::{CalendarDate, InstrumentId, VenueId};

impl InstalledCurrentFind {
    pub(super) async fn publish_current_sources(
        &self,
        population: &PreparedFindPopulation,
        profile: &ValidatedAnalyticalProfile,
        benchmark_instrument_id: Option<InstrumentId>,
        context: &RequestContext,
    ) -> Result<
        (
            Vec<PendingCurrentPriceActions>,
            Option<ForecastSessionCohortReference>,
        ),
        ServiceError,
    > {
        self.authorize(context)?;
        if !population.complete() {
            return Err(ServiceError::InvalidResult);
        }
        if population.candidates().is_empty() {
            return Ok((Vec::new(), None));
        }
        let calendar_reference = self
            .calendars
            .preflight_current_session(context.deadline(), context.cancellation().clone())
            .await
            .map_err(map_calendar)?
            .ok_or(ServiceError::Unavailable)?;
        let selected_at = super::super::super::runtime::current_timestamp()
            .map_err(|_| ServiceError::Internal)?;
        let calendar = self
            .calendars
            .read_reference(
                &calendar_reference,
                selected_at,
                context.deadline(),
                context.cancellation().clone(),
            )
            .await
            .map_err(map_calendar)?
            .ok_or(ServiceError::Unavailable)?;
        let horizon = profile
            .horizon()
            .step_nanos()
            .and_then(|step| i64::try_from(step.get()).ok())
            .ok_or(ServiceError::InvalidResult)?;
        let cohort = calendar
            .latest_forecast_session_cohort(
                selected_at,
                horizon,
                selected_at,
                context.deadline(),
                context.cancellation(),
            )
            .map_err(map_calendar)?
            .ok_or(ServiceError::Unavailable)?;
        let last = cohort.reference().session_date();
        let prior = calendar
            .native_session_replay()
            .sessions()
            .iter()
            .rev()
            .find(|session| session.date() < last)
            .ok_or(ServiceError::Unavailable)?
            .date();
        let dates = (prior, last);
        // Calendar arithmetic chooses finite request bounds only. The source publisher and
        // native calendar independently establish every genuine observation and session.
        use chrono::Datelike as _;
        let last_date = chrono::NaiveDate::from_ymd_opt(
            i32::from(last.year()),
            u32::from(last.month()),
            u32::from(last.day()),
        )
        .ok_or(ServiceError::InvalidResult)?;
        let first = last_date
            .checked_sub_days(chrono::Days::new(3649))
            .ok_or(ServiceError::InvalidRequest)?;
        let training_dates = (
            CalendarDate::new(
                u16::try_from(first.year()).map_err(|_| ServiceError::InvalidRequest)?,
                u8::try_from(first.month()).map_err(|_| ServiceError::InvalidRequest)?,
                u8::try_from(first.day()).map_err(|_| ServiceError::InvalidRequest)?,
            )
            .map_err(|_| ServiceError::InvalidRequest)?,
            last,
        );
        let (selected_benchmark, benchmark) =
            self.training_benchmark(benchmark_instrument_id, selected_at, context)?;
        // Two adjacent observations remain the fixed current price-return lookback. Independent
        // long histories are acquired now, before the caller freezes the one saved Find cutoff.
        let mut pending = Vec::with_capacity(population.candidates().len().div_ceil(32));
        for batch in population.candidates().chunks(32) {
            let mut training = Vec::with_capacity(batch.len());
            for candidate in batch {
                let prepared = self
                    .publish_training_pair(
                        candidate.canonical_record(),
                        candidate.context().listing_venue(),
                        benchmark.as_ref(),
                        training_dates,
                        context,
                    )
                    .await;
                let prepared = match prepared {
                    Ok(value) => Some(value),
                    Err(ServiceError::Unavailable | ServiceError::NotFound) => None,
                    Err(error) => return Err(error),
                };
                training.push((candidate.instrument_id(), prepared));
            }
            let mut histories = Vec::with_capacity(batch.len());
            for candidate in batch {
                self.authorize(context)?;
                let record = candidate.canonical_record();
                let venue = candidate.context().listing_venue();
                let history = self
                    .acquire_nominal_history(record, venue, dates, context)
                    .await?;
                if history.bar_count() != 2 {
                    return Err(ServiceError::Unavailable);
                }
                histories.push(history);
            }
            let mut page = self
                .actions
                .publish_current_price_actions(&histories, dates, context)
                .await?;
            for (subject, training) in training {
                page.retain_training(
                    subject,
                    benchmark_instrument_id,
                    selected_benchmark,
                    training,
                )?;
            }
            pending.push(page);
            drop(histories);
        }
        self.authorize(context)?;
        Ok((pending, Some(cohort.reference().clone())))
    }

    /// Resolves the actual comparison before observing any training outcomes. Missing canonical
    /// or supported listing evidence stays absent, with the requested identity retained separately.
    #[allow(
        clippy::type_complexity,
        reason = "requested identity and admitted source listing are distinct"
    )]
    fn training_benchmark(
        &self,
        requested: Option<InstrumentId>,
        selected_at: Timestamp,
        context: &RequestContext,
    ) -> Result<
        (
            Option<InstrumentId>,
            Option<(MarketDataInstrumentRecord, VenueId)>,
        ),
        ServiceError,
    > {
        self.authorize(context)?;
        let reader = self.research.market_data_instruments();
        let selected = RecommendationBenchmarkSelectionReadCapability::new(reader.clone())
            .select_comparison(
                requested,
                selected_at,
                selected_at,
                context.deadline(),
                context.cancellation(),
            )?;
        let Some(selected) = selected else {
            return Ok((None, None));
        };
        let instrument = selected.instrument_id();
        let query = MarketDataInstrumentPopulationQuery::try_new(
            vec![instrument],
            selected_at,
            selected_at,
        )
        .map_err(crate::application::map_market_definition_read_error)?;
        let population = reader
            .pin_population_as_of(query, context.deadline(), context.cancellation())
            .map_err(crate::application::map_market_definition_read_error)?;
        if population.disposition() != MarketDataInstrumentPopulationDisposition::Complete
            || !population.exclusions().is_empty()
        {
            return Ok((Some(instrument), None));
        }
        let [record] = population.records() else {
            return Err(ServiceError::InvalidResult);
        };
        if record.definition().instrument_id() != instrument
            || record.revision_digest() != selected.reference_revision_digest()
        {
            return Err(ServiceError::InvalidResult);
        }
        let mut listings = record
            .definition()
            .venue_mappings()
            .iter()
            .filter(|mapping| {
                mapping.venue_symbol().as_str() == selected.display_symbol()
                    && matches!(mapping.venue_id().as_str(), "ARCX" | "XNYS" | "XNAS")
            });
        let Some(listing) = listings.next() else {
            return Ok((Some(instrument), None));
        };
        if listings.next().is_some() {
            return Ok((Some(instrument), None));
        }
        Ok((
            Some(instrument),
            Some((record.clone(), listing.venue_id().clone())),
        ))
    }

    async fn publish_training_pair(
        &self,
        subject: &MarketDataInstrumentRecord,
        venue: &VenueId,
        benchmark: Option<&(MarketDataInstrumentRecord, VenueId)>,
        dates: (CalendarDate, CalendarDate),
        context: &RequestContext,
    ) -> Result<PendingCurrentPriceActions, ServiceError> {
        let mut histories = vec![
            self.acquire_nominal_history(subject, venue, dates, context)
                .await?,
        ];
        if let Some((record, venue)) = benchmark.filter(|(record, _)| {
            record.definition().instrument_id() != subject.definition().instrument_id()
        }) {
            match self
                .acquire_nominal_history(record, venue, dates, context)
                .await
            {
                Ok(history) => histories.push(history),
                Err(ServiceError::Unavailable | ServiceError::NotFound) => {}
                Err(error) => return Err(error),
            }
        }
        let mut prepared = self
            .actions
            .publish_training_price_actions(&histories, dates, context)
            .await;
        if histories.len() == 2
            && matches!(
                prepared,
                Err(ServiceError::Unavailable | ServiceError::NotFound)
            )
        {
            // A missing comparison never becomes a fabricated flat series or prevents genuine
            // subject-only price/profit evidence. Its selected identity remains in saved custody.
            prepared = self
                .actions
                .publish_training_price_actions(&histories[..1], dates, context)
                .await;
        }
        prepared
    }

    async fn acquire_nominal_history(
        &self,
        record: &MarketDataInstrumentRecord,
        venue: &VenueId,
        dates: (CalendarDate, CalendarDate),
        context: &RequestContext,
    ) -> Result<CompleteMarketBarHistoryCursor, ServiceError> {
        self.authorize(context)?;
        let publication = self
            .activation
            .prepare_instrument_eod_history(record, venue, &self.calendars, dates, context)
            .await?;
        let cutoff = super::super::super::runtime::current_timestamp()
            .map_err(|_| ServiceError::Internal)?;
        self.ingest
            .read_complete_tiingo_eod_publication(
                &publication,
                record,
                venue,
                dates,
                &self.calendars,
                cutoff,
                context.deadline(),
                context.cancellation(),
            )
            .await
            .map_err(|error| {
                use crate::application::TiingoHistoryApplicationError as E;
                match error {
                    E::Read(error) => crate::application::map_source_analytical_error(error),
                    E::Calendar(error) => map_calendar(error),
                    E::Research(error) => crate::application::map_source_research_error(error),
                    _ => ServiceError::InvalidResult,
                }
            })
    }
}

pub(super) fn require_same_source_population(
    before: &PreparedFindPopulation,
    after: &PreparedFindPopulation,
) -> Result<(), ServiceError> {
    let left = before
        .current_population()
        .ok_or(ServiceError::InvalidResult)?;
    let right = after
        .current_population()
        .ok_or(ServiceError::InvalidResult)?;
    if !before.complete()
        || !after.complete()
        || left.instrument_ids() != right.instrument_ids()
        || before.candidates().len() != after.candidates().len()
        || before
            .candidates()
            .iter()
            .zip(after.candidates())
            .any(|(left, right)| {
                left.instrument_id() != right.instrument_id()
                    || left.canonical_record().revision_digest()
                        != right.canonical_record().revision_digest()
                    || left.context().listing_venue() != right.context().listing_venue()
                    || left.context().quote_currency() != right.context().quote_currency()
            })
    {
        return Err(ServiceError::Unavailable);
    }
    Ok(())
}

impl InstalledCurrentFind {
    /// Returns a source selector only for an exact completed partition child used by this screen.
    pub(super) async fn selected_current_inputs(
        &self,
        authority: &InstalledResearchDatasetPreparation,
        parent: &CurrentFindPreparationRecord,
        plan: &PreparedCurrentFindFeatures,
        execution: &market_squawk_decisions::ScreenExecution,
        training: &super::super::training_preparation::InstalledProductTraining,
        context: &RequestContext,
    ) -> Result<
        std::collections::BTreeMap<
            market_squawk_domain::InstrumentId,
            crate::application::model::forecast_preparation::ForecastCurrentFeatureInputSelection,
        >,
        ServiceError,
    > {
        let screen = self
            .decisions
            .current_find_screen(parent)
            .map_err(map_journal)?
            .ok_or(ServiceError::NotFound)?;
        let selected = execution
            .candidates()
            .iter()
            .map(|candidate| candidate.record().instrument_id())
            .collect::<std::collections::BTreeSet<_>>();
        let mut inputs = std::collections::BTreeMap::new();
        for partition in plan.partitions().iter().filter(|partition| {
            partition
                .instrument_ids()
                .iter()
                .any(|id| selected.contains(id))
        }) {
            self.authorize(context)?;
            let ordinal = partition.descriptor().ordinal();
            let retained = self
                .decisions
                .current_find_partition(parent, ordinal)
                .map_err(map_journal)?
                .ok_or(ServiceError::InvalidResult)?;
            let evidence = authority
                .rebind_current_find_partition(plan, &retained)
                .map_err(ServiceError::from)?;
            let part = authority
                .read_current_find_partition(evidence, context.deadline(), context.cancellation())
                .map_err(ServiceError::from)?;
            let job = screen
                .dataset_jobs
                .iter()
                .find(|job| job.ordinal == ordinal)
                .ok_or(ServiceError::InvalidResult)?;
            self.validate_child(
                &part,
                Some(&DatasetJobInput {
                    job_id: job.job_id,
                    generation: job.generation,
                }),
                training,
                context,
            )
            .await?;
            let dataset = part.dataset().ok_or(ServiceError::InvalidResult)?;
            let manifest = dataset.generation().manifest();
            let output = crate::application::model::forecast::reopen_current_price_input(
                &self.research.analytical_reader(),
                manifest,
                context.deadline(),
                context.cancellation().clone(),
            )
            .await?;
            for instrument in partition
                .instrument_ids()
                .iter()
                .filter(|id| selected.contains(id))
            {
                let mut matches = output
                    .epochs()
                    .iter()
                    .filter(|epoch| epoch.instrument_id() == *instrument);
                let epoch = matches.next().ok_or(ServiceError::InvalidResult)?;
                if matches.next().is_some()
                    || epoch.source_selection_as_of() != parent.analytical_cutoff
                    || epoch.purpose() != market_squawk_data::DatasetBuildPurpose::StudyInputs
                    || epoch.basis()
                        != market_squawk_domain::HistoricalStudyBasis::HistoricalAsKnown
                {
                    return Err(ServiceError::InvalidResult);
                }
                let selector = crate::application::model::forecast_preparation::ForecastCurrentFeatureInputSelection::try_new(
                    manifest.clone(), epoch.example_id()).map_err(|_| ServiceError::InvalidResult)?;
                if inputs.insert(*instrument, selector).is_some() {
                    return Err(ServiceError::InvalidResult);
                }
            }
            drop(output);
        }
        if inputs.len() != selected.len() {
            return Err(ServiceError::InvalidResult);
        }
        self.authorize(context)?;
        Ok(inputs)
    }
}
