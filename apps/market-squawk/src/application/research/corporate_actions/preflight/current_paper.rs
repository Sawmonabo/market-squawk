//! Genuine current economic-date acquisition for an original paper holding interval.
use super::*;
use crate::application::market_calendar::CompletedMarketSessionDateReceipt;
use market_squawk_adapter_tiingo::{TiingoRequestSpec, TiingoTicker};
use market_squawk_data::{MarketDataProviderIdentityQuery, MarketDataProviderIdentitySelection};
use market_squawk_domain::{
    CorporateActionEventInstrumentIdentity, CorporateActionQueryInstrumentIdentity,
    ProviderInstrumentId, SourceId, SourceIdentifier,
};
/// Compact original publication custody across current quote acquisition. No caller constructor.
pub(crate) struct PreparedCurrentPaperSources {
    source: PublishedActionQuery,
    publications: Vec<crate::application::research::ingest::PublishedCurrentOrdinaryActions>,
    histories: Vec<OriginalFinancialHistory>,
    reference: CompletedMarketSessionReference,
    requested: BTreeSet<InstrumentId>,
    interval: (CalendarDate, CalendarDate),
    bootstrap_at: Timestamp,
    started: Timestamp,
}
/// Bounded pending original publications. Final quotes are sampled only after ready_at.
pub(crate) struct PreparedFinancialShareSources {
    originals: Vec<(InstrumentId, PreparedCurrentPaperSources)>,
    ready_at: Timestamp,
}

/// Compact original coordinates; complete price bodies do not accumulate across the peer set.
struct OriginalFinancialHistory {
    instrument: InstrumentId,
    provider: ProviderInstrumentId,
    venue: VenueId,
    feed: SourceIdentifier,
    interval: SourceIdentifier,
    ruleset: SourceIdentifier,
    dates: (CalendarDate, CalendarDate),
    manifest: DatasetManifestRef,
    receipt_digest: market_squawk_data::Sha256Digest,
    content_digest: market_squawk_data::Sha256Digest,
    surface: market_squawk_data::MarketHistoryPriceSurfaceRequirement,
    calendar: CompletedMarketSessionReference,
}
impl OriginalFinancialHistory {
    fn retain(
        history: &market_squawk_data::CompleteMarketBarHistoryCursor,
    ) -> Result<Self, ServiceError> {
        let receipt = history.selection().receipt();
        let graph = receipt.date_windows().ok_or(ServiceError::InvalidResult)?;
        if receipt.adjustment() != market_squawk_domain::MarketBarAdjustment::Raw
            || history.native_sessions().is_none()
        {
            return Err(ServiceError::InvalidResult);
        }
        Ok(Self {
            instrument: receipt.instrument_id(),
            provider: receipt.provider_instrument_id().clone(),
            venue: receipt.venue_id().clone(),
            feed: receipt.feed().clone(),
            interval: receipt.interval().clone(),
            ruleset: receipt.session_ruleset().clone(),
            dates: graph.requested_dates(),
            manifest: history.selection().pinned().manifest().clone(),
            receipt_digest: receipt.receipt_digest(),
            content_digest: history.read_receipt().history_content_digest(),
            surface: history.selection().surface_requirement(),
            calendar: CompletedMarketSessionReference::try_from_retained_digests(
                graph.calendar().origin_content_digest,
                graph.calendar().capture_binding_digest,
            )
            .map_err(|_| ServiceError::InvalidResult)?,
        })
    }
}

impl SourceActionPreparationCapability {
    /// Publishes source evidence using preliminary quote dates; no final quote is retained.
    pub(crate) async fn acquire_financial_share_sources(
        &self,
        requirements: &[(InstrumentId, CalendarDate, Timestamp)],
        context: &RequestContext,
    ) -> Result<PreparedFinancialShareSources, ServiceError> {
        check(context)?;
        // The existing comparable calculation admits at most sixteen peers and one subject.
        if requirements.is_empty() {
            return Err(ServiceError::InvalidRequest);
        }
        if requirements.len() > 17 {
            return Err(ServiceError::ResourceExhausted);
        }
        let mut selected = BTreeSet::new();
        let mut originals = Vec::new();
        originals
            .try_reserve_exact(requirements.len())
            .map_err(|_| ServiceError::ResourceExhausted)?;
        for &(instrument, starts_on, quote_at) in requirements {
            check(context)?;
            if !selected.insert(instrument) {
                return Err(ServiceError::InvalidRequest);
            }
            // UTC midnight would fall on the preceding New York date. Preserve the filing's
            // complete civil date using the same market calendar as the action reader.
            let start = chrono::NaiveDate::from_ymd_opt(
                i32::from(starts_on.year()),
                u32::from(starts_on.month()),
                u32::from(starts_on.day()),
            )
            .and_then(|date| date.and_hms_opt(0, 0, 0))
            .and_then(|date| date.and_local_timezone(New_York).single())
            .and_then(|date| date.timestamp_nanos_opt())
            .map(Timestamp::from_unix_nanos)
            .ok_or(ServiceError::InvalidRequest)?;
            let knowledge_at = now()?;
            if start > quote_at || quote_at > knowledge_at {
                return Err(ServiceError::InvalidRequest);
            }
            let query = market_squawk_data::MarketDataInstrumentPopulationQuery::try_new(
                vec![instrument],
                knowledge_at,
                quote_at,
            )
            .map_err(|_| ServiceError::InvalidRequest)?;
            let population = self
                .research
                .market_data_instruments()
                .pin_population_as_of(query, context.deadline(), context.cancellation())
                .map_err(crate::application::research::map_market_definition_read_error)?;
            if population.disposition()
                != market_squawk_data::MarketDataInstrumentPopulationDisposition::Complete
                || !population.exclusions().is_empty()
            {
                return Err(ServiceError::Unavailable);
            }
            let [record] = population.records() else {
                return Err(ServiceError::InvalidResult);
            };
            if record.definition().instrument_id() != instrument {
                return Err(ServiceError::InvalidResult);
            }
            let prepared = self
                .acquire_action_sources(
                    std::slice::from_ref(record),
                    start,
                    quote_at,
                    true,
                    context,
                )
                .await?;
            originals.push((instrument, prepared));
        }
        check(context)?;
        Ok(PreparedFinancialShareSources {
            originals,
            ready_at: now()?,
        })
    }

    /// Reopens acquired originals at each final actual market event. The read selection clock
    /// must follow acquisition, while an authentic closing/last-trade event may be older.
    pub(crate) async fn finish_financial_share_sources(
        &self,
        prepared: PreparedFinancialShareSources,
        quotes: &[(InstrumentId, Timestamp, Timestamp)],
        context: &RequestContext,
    ) -> Result<Vec<super::super::SourceAppliedCorporateActionPlanReference>, ServiceError> {
        check(context)?;
        if quotes.len() != prepared.originals.len() || quotes.is_empty() || quotes.len() > 17 {
            return Err(ServiceError::InvalidRequest);
        }
        let admitted_at = now()?;
        let mut selected = std::collections::BTreeMap::new();
        for &(instrument, effective_at, selected_at) in quotes {
            if effective_at > selected_at
                || selected_at < prepared.ready_at
                || selected_at > admitted_at
                || selected.insert(instrument, effective_at).is_some()
            {
                return Err(ServiceError::InvalidRequest);
            }
        }
        let mut references = Vec::new();
        references
            .try_reserve_exact(quotes.len())
            .map_err(|_| ServiceError::ResourceExhausted)?;
        for (instrument, original) in prepared.originals {
            check(context)?;
            let quote_at = selected
                .remove(&instrument)
                .ok_or(ServiceError::InvalidRequest)?;
            let plan = self
                .finish_current_action_sources(original, quote_at, context)
                .await?;
            plan.share_plan()
                .map_err(|error| map_plan_error(error, context))?;
            references.push(
                plan.source_reference()
                    .map_err(|_| ServiceError::InvalidResult)?,
            );
        }
        check(context)?;
        Ok(references)
    }

    /// Acquires genuine source publications before the caller samples its current paper quote.
    pub(crate) async fn acquire_current_action_sources(
        &self,
        instruments: &[MarketDataInstrumentRecord],
        bootstrap_at: Timestamp,
        request_at: Timestamp,
        context: &RequestContext,
    ) -> Result<PreparedCurrentPaperSources, ServiceError> {
        self.acquire_action_sources(instruments, bootstrap_at, request_at, false, context)
            .await
    }

    async fn acquire_action_sources(
        &self,
        instruments: &[MarketDataInstrumentRecord],
        bootstrap_at: Timestamp,
        request_at: Timestamp,
        completed_prefix: bool,
        context: &RequestContext,
    ) -> Result<PreparedCurrentPaperSources, ServiceError> {
        check(context)?;
        let started = now()?;
        if instruments.is_empty()
            || instruments.len() > 32
            || (completed_prefix && instruments.len() != 1)
            || bootstrap_at > request_at
            || request_at > started
        {
            return Err(ServiceError::InvalidRequest);
        }
        let requested: BTreeSet<_> = instruments
            .iter()
            .map(|record| record.definition().instrument_id())
            .collect();
        if requested.len() != instruments.len() {
            return Err(ServiceError::InvalidRequest);
        }
        let venue = VenueId::try_from("iex").map_err(|_| ServiceError::Internal)?;
        if instruments.iter().any(|record| {
            !record
                .definition()
                .venue_mappings()
                .iter()
                .any(|mapping| mapping.venue_id() == &venue)
        }) {
            return Err(ServiceError::Unavailable);
        }
        let interval = (new_york_date(bootstrap_at)?, new_york_date(request_at)?);
        let reference = self
            .calendars
            .preflight(
                &venue,
                interval.0,
                interval.1,
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
                &reference,
                calendar_at,
                context.deadline(),
                context.cancellation().clone(),
            )
            .await
            .map_err(map_calendar_error)?
            .ok_or(ServiceError::Unavailable)?;
        // Each genuine session requires both source families for every selected instrument.
        // Derive capacity from the existing publication ceiling, not a separate date horizon.
        let publication_limit = limits()?.max_actions().get();
        let publications_per_date = instruments
            .len()
            .checked_mul(2)
            .ok_or(ServiceError::ResourceExhausted)?;
        let maximum_dates = if completed_prefix {
            64_000
        } else {
            publication_limit
                .checked_div(publications_per_date)
                .ok_or(ServiceError::ResourceExhausted)?
        };
        let date_probe_limit = maximum_dates
            .checked_add(1)
            .ok_or(ServiceError::ResourceExhausted)?;
        let mut dates: Vec<_> = calendar
            .native_session_replay()
            .sessions()
            .iter()
            .map(|session| session.date())
            .filter(|date| *date >= interval.0 && *date <= interval.1)
            .take(date_probe_limit)
            .collect();
        if dates.is_empty() || dates.len() > maximum_dates {
            return Err(ServiceError::ResourceExhausted);
        }
        let activation = self
            .outcome_history_activation
            .as_ref()
            .ok_or(ServiceError::Unavailable)?;
        let mut histories = Vec::new();
        if completed_prefix && dates.len() > 1 {
            let end = dates[dates.len() - 2];
            let session = calendar
                .date_session_on(end, calendar_at, calendar_at)
                .ok_or(ServiceError::Unavailable)?;
            if session.closes_at_exclusive() > request_at {
                return Err(ServiceError::Unavailable);
            }
            let record = &instruments[0];
            let mut listings = record
                .definition()
                .venue_mappings()
                .iter()
                .filter(|mapping| matches!(mapping.venue_id().as_str(), "ARCX" | "XNYS" | "XNAS"));
            let listing = listings.next().ok_or(ServiceError::Unavailable)?;
            if listings.next().is_some() {
                return Err(ServiceError::Unavailable);
            }
            let history_dates = (interval.0, end);
            let publication = activation
                .prepare_instrument_eod_history(
                    record,
                    listing.venue_id(),
                    &self.calendars,
                    history_dates,
                    context,
                )
                .await?;
            let history = self
                .ingest
                .read_complete_tiingo_eod_publication(
                    &publication,
                    record,
                    listing.venue_id(),
                    history_dates,
                    &self.calendars,
                    now()?,
                    context.deadline(),
                    context.cancellation(),
                )
                .await
                .map_err(|error| {
                    use crate::application::research::ingest::TiingoHistoryApplicationError as E;
                    match error {
                        E::Read(error) => map_analytical_error(error),
                        E::Calendar(error) => map_calendar_error(error),
                        E::Research(error) => map_research_error(error),
                        E::Ingest(error) => map_ingest_error(error),
                        _ => ServiceError::InvalidResult,
                    }
                })?;
            histories
                .try_reserve_exact(1)
                .map_err(|_| ServiceError::ResourceExhausted)?;
            histories.push(OriginalFinancialHistory::retain(&history)?);
            drop(history);
            let tail = *dates.last().ok_or(ServiceError::Unavailable)?;
            dates.clear();
            dates.push(tail);
        }
        let tiingo = SourceId::try_from("tiingo-starter").map_err(|_| ServiceError::Internal)?;
        let maximum_publications = dates
            .len()
            .checked_mul(instruments.len())
            .and_then(|n| n.checked_mul(2))
            .ok_or(ServiceError::ResourceExhausted)?;
        if maximum_publications > publication_limit {
            return Err(ServiceError::ResourceExhausted);
        }
        let mut publications = Vec::new();
        publications
            .try_reserve_exact(maximum_publications)
            .map_err(|_| ServiceError::ResourceExhausted)?;
        for date in dates {
            let session = calendar
                .date_session_on(date, calendar_at, calendar_at)
                .ok_or(ServiceError::Unavailable)?;
            if session.opens_at() > started {
                continue;
            }
            for record in instruments {
                for splits in [false, true] {
                    check(context)?;
                    let selected_at = now()?;
                    let query = self
                        .research
                        .market_data_instruments()
                        .select_current_ordinary_query_identities(
                            tiingo.clone(),
                            vec![record.definition().instrument_id()],
                            selected_at,
                            session.opens_at(),
                            context.deadline(),
                            context.cancellation(),
                        )
                        .map_err(map_query_identity_error)?;
                    let [selected] = query.selected_definitions() else {
                        return Err(ServiceError::InvalidResult);
                    };
                    let interval = selected.definition().effective_interval();
                    if interval.starts_at() > session.opens_at()
                        || interval
                            .ends_at()
                            .is_some_and(|end| end < session.closes_at_exclusive())
                        || !selected
                            .definition()
                            .venue_mappings()
                            .iter()
                            .any(|mapping| mapping.venue_id() == &venue)
                    {
                        return Err(ServiceError::Unavailable);
                    }
                    let [identity] = query.retained() else {
                        return Err(ServiceError::InvalidResult);
                    };
                    if identity.provider_identity_validity.starts_at() > session.opens_at()
                        || identity
                            .provider_identity_validity
                            .ends_at()
                            .is_some_and(|end| end < session.closes_at_exclusive())
                    {
                        return Err(ServiceError::Unavailable);
                    }
                    let symbol = identity.symbol.as_str().to_owned();
                    let ticker =
                        TiingoTicker::try_new(&symbol).map_err(|_| ServiceError::InvalidResult)?;
                    let request = if splits {
                        TiingoRequestSpec::corporate_action_splits(ticker, date)
                    } else {
                        TiingoRequestSpec::corporate_action_distributions(ticker, date)
                    }
                    .map_err(|_| ServiceError::InvalidResult)?;
                    let acquired = activation
                        .acquire_tiingo_current_actions(request, context)
                        .await?;
                    let mut events = Vec::new();
                    if acquired
                        .receipt()
                        .rows()
                        .iter()
                        .any(|row| row.ticker().as_str() == symbol)
                    {
                        let event = self
                            .current_event_identity(
                                &tiingo,
                                &symbol,
                                &venue,
                                &session,
                                now()?,
                                context,
                            )?
                            .ok_or(ServiceError::Unavailable)?;
                        if event.0.selection.instrument_id != record.definition().instrument_id() {
                            return Err(ServiceError::Unavailable);
                        }
                        events.push(event);
                    }
                    publications.push(
                        self.ingest
                            .publish_current_ordinary_actions(acquired, query, events, context)
                            .await?,
                    );
                }
            }
        }
        let runtime = self
            .runtime
            .current_alpaca_calendar_runtime(context.deadline(), context.cancellation())
            .await
            .map_err(|_| controlled(context, ServiceError::Unavailable))?;
        let source = self
            .publish_query(&runtime, &requested, interval, &calendar, context)
            .await?;
        runtime
            .require_current(context.deadline(), context.cancellation())
            .await
            .map_err(map_capability_error)?;
        check(context)?;
        Ok(PreparedCurrentPaperSources {
            source,
            publications,
            histories,
            reference,
            requested,
            interval,
            bootstrap_at,
            started,
        })
    }
    /// Reopens acquired originals at the actual resulting cutoff, preserving the later authentic
    /// quote valuation clock. A different native date requires a fresh acquisition, not extension.
    pub(crate) async fn finish_current_action_sources(
        &self,
        prepared: PreparedCurrentPaperSources,
        valuation_cutoff: Timestamp,
        context: &RequestContext,
    ) -> Result<SourceAppliedCorporateActionPlan, ServiceError> {
        check(context)?;
        let PreparedCurrentPaperSources {
            source,
            publications,
            histories,
            reference,
            requested,
            interval,
            bootstrap_at,
            started,
        } = prepared;
        let cutoff = now()?;
        if cutoff < started
            || valuation_cutoff > cutoff
            || valuation_cutoff < bootstrap_at
            || new_york_date(valuation_cutoff)? != interval.1
        {
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
                source,
                calendar,
                requested,
                interval,
                cutoff,
                valuation_cutoff,
                context,
            )
            .await?;
        let mut reads = Vec::new();
        let mut retained = 0usize;
        let bound = limits()?;
        if publications.len() > bound.max_actions().get() {
            return Err(ServiceError::ResourceExhausted);
        }
        let mut row_count = plan.source().actions().len();
        if row_count > bound.max_actions().get() {
            return Err(ServiceError::ResourceExhausted);
        }
        for publication in publications {
            let read = self
                .ingest
                .read_current_ordinary_actions(publication, cutoff, context)
                .await?;
            row_count = row_count
                .checked_add(read.rows().len())
                .ok_or(ServiceError::ResourceExhausted)?;
            if row_count > bound.max_actions().get() {
                return Err(ServiceError::ResourceExhausted);
            }
            retained = retained
                .checked_add(
                    read.retained_bytes()
                        .ok_or(ServiceError::ResourceExhausted)?,
                )
                .ok_or(ServiceError::ResourceExhausted)?;
            if retained > bound.max_retained_bytes().get() {
                return Err(ServiceError::ResourceExhausted);
            }
            reads
                .try_reserve(1)
                .map_err(|_| ServiceError::ResourceExhausted)?;
            reads.push(read);
        }
        let mut ordinary = Vec::new();
        ordinary
            .try_reserve_exact(histories.len())
            .map_err(|_| ServiceError::ResourceExhausted)?;
        for original in histories {
            check(context)?;
            let request = market_squawk_data::CompleteMarketBarHistoryRequest::try_exact_nominal(
                original.instrument,
                original.dates.0,
                original.dates.1,
                original.provider,
                original.venue,
                original.feed,
                original.interval,
                market_squawk_domain::MarketBarAdjustment::Raw,
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
                .map_err(map_analytical_error)?
                .ok_or(ServiceError::Unavailable)?;
            if history.selection().receipt().receipt_digest() != original.receipt_digest
                || history.read_receipt().history_content_digest() != original.content_digest
            {
                return Err(ServiceError::InvalidResult);
            }
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
            ordinary.push((read, calendar));
        }
        let plan = plan
            .with_hybrid_ordinary_reads(
                ordinary,
                reads,
                bound,
                context.deadline(),
                context.cancellation(),
            )
            .map_err(|error| map_plan_error(error, context))?;
        let plan = self
            .reads
            .publish_current_recipe(plan, context.deadline(), context.cancellation())
            .await
            .map_err(|error| map_plan_error(error, context))?;
        check(context)?;
        Ok(plan)
    }
    /// Convenience for a previously authenticated fixed valuation; execution still checks quote
    /// currentness. Current quote callers should acquire first, sample, then finish explicitly.
    pub(crate) async fn prepare_current_paper(
        &self,
        instruments: &[MarketDataInstrumentRecord],
        bootstrap_at: Timestamp,
        valuation_cutoff: Timestamp,
        context: &RequestContext,
    ) -> Result<SourceAppliedCorporateActionPlan, ServiceError> {
        let prepared = self
            .acquire_current_paper_sources(instruments, bootstrap_at, valuation_cutoff, context)
            .await?;
        self.finish_current_paper_sources(prepared, valuation_cutoff, context)
            .await
    }
    fn current_event_identity(
        &self,
        source: &SourceId,
        symbol: &str,
        venue: &VenueId,
        session: &CompletedMarketSessionDateReceipt,
        knowledge_at: Timestamp,
        context: &RequestContext,
    ) -> Result<
        Option<(
            CorporateActionEventInstrumentIdentity,
            MarketDataProviderIdentitySelection,
        )>,
        ServiceError,
    > {
        let query = MarketDataProviderIdentityQuery::try_new(
            source.clone(),
            ProviderInstrumentId::try_from(symbol).map_err(|_| ServiceError::InvalidResult)?,
            knowledge_at,
            session.opens_at(),
        )
        .map_err(|_| ServiceError::InvalidResult)?;
        let reader = self.research.market_data_instruments();
        let Some(selection) = reader
            .select_provider_identity_as_of(query, context.deadline(), context.cancellation())
            .map_err(crate::application::research::map_market_definition_read_error)?
        else {
            return Ok(None);
        };
        let record = reader
            .read_selected_provider_definition(
                &selection,
                context.deadline(),
                context.cancellation(),
            )
            .map_err(crate::application::research::map_market_definition_read_error)?;
        let exact = selection
            .exact_receipt()
            .map_err(|_| ServiceError::InvalidResult)?;
        let definition = record.definition();
        let Some(provider) = definition.provider_identity_at(
            source,
            selection.query().provider_instrument_id(),
            session.opens_at(),
        ) else {
            return Ok(None);
        };
        let Some(mapping) = definition.venue_mappings().iter().find(|mapping| {
            mapping.venue_id() == venue && mapping.venue_symbol().as_str() == symbol
        }) else {
            return Ok(None);
        };
        if !exact.matching_venues().contains(venue)
            || definition.effective_interval().starts_at() > session.opens_at()
            || definition
                .effective_interval()
                .ends_at()
                .is_some_and(|end| end < session.closes_at_exclusive())
            || provider.validity().starts_at() > session.opens_at()
            || provider
                .validity()
                .ends_at()
                .is_some_and(|end| end < session.closes_at_exclusive())
        {
            return Ok(None);
        }
        let retained = CorporateActionEventInstrumentIdentity {
            source_id: source.clone(),
            provider_instrument_id: selection.query().provider_instrument_id().clone(),
            venue_id: venue.clone(),
            venue_symbol: mapping.venue_symbol().clone(),
            selection: CorporateActionQueryInstrumentIdentity {
                symbol: SourceIdentifier::try_from(symbol)
                    .map_err(|_| ServiceError::InvalidResult)?,
                instrument_id: exact.instrument_id(),
                knowledge_at: selection.query().knowledge_at(),
                effective_at: selection.query().effective_at(),
                definition_revision_digest: exact.definition_revision_digest(),
                definition_revision_sequence: exact.definition_revision_sequence(),
                definition_published_at: exact.definition_published_at(),
                definition_reference_revision: exact.definition_reference_revision().clone(),
                definition_reference_payload_digest: exact.definition_reference_payload_digest(),
                provider_identity_revision: exact.provider_identity_revision().clone(),
                provider_identity_payload_digest: exact.provider_identity_payload_digest(),
                provider_identity_validity: exact.provider_identity_validity(),
                selection_digest: selection.selection_digest(),
            },
            resolution_receipt_digest: selection.resolution_receipt_digest(),
        };
        Ok(Some((retained, selection)))
    }
}

impl SourceActionPreparationCapability {
    /// Paper retains the same acquisition and bounded original-source custody.
    pub(crate) async fn acquire_current_paper_sources(
        &self,
        instruments: &[MarketDataInstrumentRecord],
        bootstrap_at: Timestamp,
        request_at: Timestamp,
        context: &RequestContext,
    ) -> Result<PreparedCurrentPaperSources, ServiceError> {
        self.acquire_current_action_sources(instruments, bootstrap_at, request_at, context)
            .await
    }
    pub(crate) async fn finish_current_paper_sources(
        &self,
        prepared: PreparedCurrentPaperSources,
        valuation_cutoff: Timestamp,
        context: &RequestContext,
    ) -> Result<SourceAppliedCorporateActionPlan, ServiceError> {
        self.finish_current_action_sources(prepared, valuation_cutoff, context)
            .await
    }
}
