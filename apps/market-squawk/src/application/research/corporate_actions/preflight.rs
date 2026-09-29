//! Installed, finite source acquisition followed by exact original-generation reopening.
//!
//! Provider credentials, rate admission and cancellation remain with the active market runtime;
//! capture sealing and publication remain with ResearchService. This owner retains no registry.

mod current_inputs;
mod current_paper;
pub(crate) use current_paper::{PreparedCurrentPaperSources, PreparedFinancialShareSources};
mod forecast_outcome;
mod outcome_coordinates;
mod outcome_preparation;
pub(crate) use forecast_outcome::PreparedForecastOutcomeSource;
pub(crate) use outcome_coordinates::ForecastOutcomePreparationCoordinates;
pub(crate) use outcome_preparation::{
    ForecastOutcomeSourcePreparation, PreparedForecastOutcomeMeasurement,
};
mod history;
mod history_plan;
mod identity;

pub(crate) use current_inputs::PendingCurrentPriceActions;
pub(crate) use history_plan::PreparedHistorySourceActions;

use super::source_errors::*;
use super::{
    PreparedSourceActionQuery, SourceAppliedCorporateActionPlan,
    SourceAppliedCorporateActionReadCapability,
};
use crate::application::market_calendar::{
    CompletedMarketSessionRead, CompletedMarketSessionReadCapability,
    CompletedMarketSessionReference, MarketCalendarClock as _, SystemMarketCalendarClock,
};
use crate::application::market_runtime::{
    AlpacaHistoricalRuntimeCapability, MarketRuntimeRegistry,
};
use crate::application::model::forecast::LatestValidForecast;
use crate::application::research::ingest::ProductionResearchIngestCoordinator;
use crate::{ResearchIngestRequest, ResearchService};
use chrono::{DateTime, Datelike as _, Utc};
use chrono_tz::America::New_York;
use market_squawk_adapter_alpaca::{
    AlpacaCorporateActionIdentity, AlpacaPreparedCorporateActionsPublication,
};
use market_squawk_data::{
    CorporateActionAdjustment, CorporateActionLimits, CorporateActionPaymentPolicy,
    CorporateActionPolicy, CorporateActionSourceSnapshot, DatasetId, DatasetManifestRef,
    MarketDataInstrumentRecord, extraction_provider_payload_digest,
};
use market_squawk_domain::{CalendarDate, EvidenceDigest, InstrumentId, Timestamp, VenueId};
use market_squawk_services::{RequestContext, ServiceError};
use market_squawk_sources::{
    DiscoveryRequest, ExtractionRequest, MAX_IN_MEMORY_EXTRACTION_BATCH_BYTES,
};
use std::{
    collections::BTreeSet,
    num::{NonZeroU16, NonZeroU32, NonZeroU64, NonZeroUsize},
    sync::Arc,
    time::Instant,
};

/// Composition of existing owners; callers cannot supply a publisher or synthetic action callback.
#[derive(Clone)]
pub(crate) struct SourceActionPreparationCapability {
    research: Arc<ResearchService>,
    runtime: Arc<MarketRuntimeRegistry>,
    ingest: Arc<ProductionResearchIngestCoordinator>,
    calendars: CompletedMarketSessionReadCapability,
    reads: SourceAppliedCorporateActionReadCapability,
    outcome_history_activation: Option<Arc<crate::provider_activation::ProviderAdapterActivation>>,
}

pub(crate) struct PreparedForecastSourceActions {
    plan: SourceAppliedCorporateActionPlan,
    cutoff: Timestamp,
    current_calendar: CompletedMarketSessionReference,
}
impl PreparedForecastSourceActions {
    pub(crate) fn plan(&self) -> &SourceAppliedCorporateActionPlan {
        &self.plan
    }
    pub(crate) const fn source_cutoff(&self) -> Timestamp {
        self.cutoff
    }
    pub(crate) fn current_calendar(&self) -> &CompletedMarketSessionReference {
        &self.current_calendar
    }
}

/// Actual publication coordinates, private to acquisition. A generation becomes read authority
/// only after the common physical capture reader and canonical source reader both reopen it.
struct PublishedActionQuery {
    manifest: DatasetManifestRef,
    binding: EvidenceDigest,
}

impl SourceActionPreparationCapability {
    pub(crate) fn new(
        research: Arc<ResearchService>,
        runtime: Arc<MarketRuntimeRegistry>,
        ingest: Arc<ProductionResearchIngestCoordinator>,
    ) -> Self {
        let calendars =
            CompletedMarketSessionReadCapability::new(Arc::clone(&research), Arc::clone(&runtime));
        let reads = SourceAppliedCorporateActionReadCapability::new(
            Arc::clone(&research),
            calendars.clone(),
        );
        Self {
            research,
            runtime,
            ingest,
            calendars,
            reads,
            outcome_history_activation: None,
        }
    }

    /// Attaches the existing source activation owner for genuine later ordinary-history acquisition.
    pub(crate) fn with_outcome_history_acquisition(
        mut self,
        activation: Arc<crate::provider_activation::ProviderAdapterActivation>,
    ) -> Self {
        self.outcome_history_activation = Some(activation);
        self
    }

    /// Shares the installed controlled artifact owner with the original source reader.
    pub(crate) fn with_current_paper_artifacts(
        mut self,
        artifacts: Arc<dyn market_squawk_services::ArtifactRepository>,
    ) -> Self {
        self.reads = self.reads.with_artifact_repository(artifacts);
        self
    }

    /// Receives the original authenticated forecast and actual selected catalog record. The
    /// operation caller owns profile/job/token validation. One cutoff is captured after every
    /// acquisition, then only the exact resulting generations are reopened at that cutoff.
    pub(crate) async fn prepare_forecast(
        &self,
        forecast: &LatestValidForecast,
        instrument: &MarketDataInstrumentRecord,
        original_source_cutoff: Timestamp,
        maximum_capture_age_nanos: u64,
        context: &RequestContext,
    ) -> Result<PreparedForecastSourceActions, ServiceError> {
        check(context)?;
        let started_at = now()?;
        let serving = forecast
            .selected_distribution()
            .ok_or(ServiceError::InvalidResult)?
            .serving_binding();
        let origin = serving.origin_bar().ok_or(ServiceError::Unavailable)?;
        let origin_exact = origin
            .time_semantics()
            .timestamped_period()
            .ok_or(ServiceError::Unavailable)?;
        if serving.knowledge_cutoff() != original_source_cutoff
            || original_source_cutoff > started_at
        {
            return Err(ServiceError::InvalidRequest);
        }
        let plans = self
            .reads
            .prepare_original_coordinate_anchor_plans(
                forecast,
                instrument,
                started_at,
                context.deadline(),
                context.cancellation(),
            )
            .map_err(|error| map_plan_error(error, context))?;
        let runtime = self
            .runtime
            .current_alpaca_calendar_runtime(context.deadline(), context.cancellation())
            .await
            .map_err(|_| controlled(context, ServiceError::Unavailable))?;
        // These are request dates selected from genuine original/current instants; they do not
        // assign event instants to a date or certify an economic absence interval.
        let interval = (
            new_york_date(origin_exact.provider_timestamp())?,
            new_york_date(started_at)?,
        );
        let calendar_reference = self
            .calendars
            .preflight(
                &VenueId::try_from("iex").map_err(|_| ServiceError::Internal)?,
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
                &calendar_reference,
                calendar_at,
                context.deadline(),
                context.cancellation().clone(),
            )
            .await
            .map_err(map_calendar_error)?
            .ok_or(ServiceError::Unavailable)?;
        let instruments = BTreeSet::from([instrument.definition().instrument_id()]);
        // The finite process query starts at the original analytical snapshot's civil date.
        // Older source economics are represented by the same-origin fresh Raw/Split anchor;
        // terminal process closure never stands for all historical events.
        let source = self
            .publish_query(
                &runtime,
                &instruments,
                (new_york_date(original_source_cutoff)?, interval.1),
                &calendar,
                context,
            )
            .await?;
        let [raw_plan, split_plan] = plans;
        let raw = self
            .publish_history(&runtime, raw_plan, instrument, origin, context)
            .await?;
        let split = self
            .publish_history(&runtime, split_plan, instrument, origin, context)
            .await?;
        // Repeat only the existing current-calendar preparation, after lengthy history work.
        // Market quote/trade freshness is independently enforced by the existing final selector.
        let current_calendar = self
            .calendars
            .preflight_current_session(context.deadline(), context.cancellation().clone())
            .await
            .map_err(map_calendar_error)?
            .ok_or(ServiceError::Unavailable)?;
        runtime
            .require_current(context.deadline(), context.cancellation())
            .await
            .map_err(map_capability_error)?;
        let cutoff = now()?;
        if cutoff < original_source_cutoff || new_york_date(cutoff)? != interval.1 {
            // A civil-day rollover requires a fresh bounded preparation; do not extend the
            // original source query/calendar coordinates after observing its result.
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
        self.calendars
            .read_reference(
                &current_calendar,
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
                instruments,
                interval,
                cutoff,
                cutoff,
                context,
            )
            .await?;
        let raw = self.reopen_published_history(raw, cutoff, context).await?;
        let split = self
            .reopen_published_history(split, cutoff, context)
            .await?;
        let plan = self
            .reads
            .with_fresh_alpaca_split_anchor(
                plan,
                raw,
                split,
                context.deadline(),
                context.cancellation(),
            )
            .await
            .map_err(|error| map_plan_error(error, context))?;
        plan.require_capture_freshness(
            original_source_cutoff,
            maximum_capture_age_nanos,
            context.deadline(),
            context.cancellation(),
        )
        .map_err(|error| map_continuity_error(error, context))?;
        plan.source_reference()
            .map_err(|error| map_plan_error(error, context))?;
        check(context)?;
        Ok(PreparedForecastSourceActions {
            plan,
            cutoff,
            current_calendar,
        })
    }

    async fn publish_query(
        &self,
        runtime: &AlpacaHistoricalRuntimeCapability,
        instruments: &BTreeSet<InstrumentId>,
        process_dates: (CalendarDate, CalendarDate),
        calendar: &CompletedMarketSessionRead,
        context: &RequestContext,
    ) -> Result<PublishedActionQuery, ServiceError> {
        check(context)?;
        let selected_at = now()?;
        let source_id = runtime
            .corporate_action_metadata()
            .map_err(map_runtime_action_error)?
            .source_id()
            .clone();
        let query = PreparedSourceActionQuery::select(
            self.research.market_data_instruments().clone(),
            source_id,
            instruments.iter().copied().collect(),
            selected_at,
            process_dates,
            context.deadline(),
            context.cancellation(),
        )
        .map_err(map_query_error)?;
        let (rejoin, seal_request) = runtime
            .acquire_corporate_actions(
                query.request().clone(),
                context.deadline(),
                context.cancellation(),
            )
            .await
            .map_err(map_runtime_action_error)?;
        let observed_at = now()?;
        let mut identities = Vec::new();
        let mut event_identities = Vec::new();
        for (action_id, symbol, related_symbol, date) in rejoin.returned_action_identities() {
            check(context)?;
            let (Some(symbol), Some(date)) = (symbol, date) else {
                continue;
            };
            let Some(session) = calendar.date_session_on(date, observed_at, observed_at) else {
                continue;
            };
            let Some((subject, subject_selection)) = self.select_event_identity(
                runtime.historical_metadata().source_id(),
                symbol,
                &session,
                observed_at,
                context,
            )?
            else {
                continue;
            };
            let related = match related_symbol {
                Some(symbol) => self.select_event_identity(
                    runtime.historical_metadata().source_id(),
                    symbol,
                    &session,
                    observed_at,
                    context,
                )?,
                None => None,
            };
            identities
                .try_reserve(1)
                .map_err(|_| ServiceError::ResourceExhausted)?;
            event_identities
                .try_reserve(if related.is_some() { 2 } else { 1 })
                .map_err(|_| ServiceError::ResourceExhausted)?;
            let related = related.map(|(value, selection)| {
                event_identities.push(selection);
                value
            });
            event_identities.push(subject_selection);
            identities.push(
                AlpacaCorporateActionIdentity::try_new(action_id, date, subject, related)
                    .map_err(|_| ServiceError::InvalidResult)?,
            );
        }
        let sealed = self
            .research
            .seal_provider_capture(seal_request, context.cancellation(), context.deadline())
            .await
            .map_err(map_research_error)?;
        let prepared = rejoin
            .try_rejoin(sealed)
            .map_err(|_| ServiceError::InvalidResult)?;
        let metadata = prepared.metadata().clone();
        let analytical = DatasetId::try_from(prepared.dataset().as_str())
            .map_err(|_| ServiceError::InvalidResult)?;
        let ingested_at = now()?;
        let remaining = context
            .deadline()
            .checked_duration_since(Instant::now())
            .ok_or(ServiceError::DeadlineExceeded)?;
        let deadline_at = ingested_at
            .checked_add_nanos(
                i64::try_from(remaining.as_nanos()).map_err(|_| ServiceError::InvalidRequest)?,
            )
            .map_err(|_| ServiceError::InvalidRequest)?;
        let discovery = DiscoveryRequest::try_new(
            prepared.dataset().clone(),
            None,
            NonZeroU16::MIN,
            deadline_at,
        )
        .map_err(|_| ServiceError::InvalidResult)?;
        let object = prepared
            .source_object(&discovery)
            .map_err(|_| ServiceError::InvalidResult)?;
        let extraction = ExtractionRequest::try_new(
            object,
            NonZeroU32::new(32_001).ok_or(ServiceError::Internal)?,
            NonZeroU64::new(MAX_IN_MEMORY_EXTRACTION_BATCH_BYTES).ok_or(ServiceError::Internal)?,
            deadline_at,
        )
        .map_err(|_| ServiceError::InvalidResult)?;
        // Existing account publication authority is acquired only after network and source reads;
        // the exact query guard composes original reference revalidation into this same commit.
        let source_guard = runtime
            .acquire_calendar_publication_authority(context.deadline(), context.cancellation())
            .await
            .map_err(map_capability_error)?;
        let publication = query
            .bind(
                prepared,
                &extraction,
                &identities,
                event_identities,
                ingested_at,
                source_guard,
                context.deadline(),
                context.cancellation().clone(),
            )
            .map_err(map_query_error)?;
        let (binding, authority) = publication.into_parts();
        let binding_digest = binding.evidence_digest().evidence();
        let rights = runtime.corporate_action_rights().decision(
            extraction_provider_payload_digest(binding.batch()),
            ingested_at,
        )?;
        let revisions = AlpacaPreparedCorporateActionsPublication::revision_plan(binding.batch())
            .map_err(|_| ServiceError::InvalidResult)?;
        let request = ResearchIngestRequest::with_provider_publication(
            metadata, rights, analytical, binding, revisions,
        )
        .map_err(|_| ServiceError::InvalidResult)?
        .with_precommit_authority(authority);
        let committed = self
            .research
            .ingest(request, context.cancellation().clone())
            .await
            .map_err(map_research_error)?;
        runtime
            .require_current(context.deadline(), context.cancellation())
            .await
            .map_err(map_capability_error)?;
        check(context)?;
        Ok(PublishedActionQuery {
            manifest: committed.manifest().clone(),
            binding: binding_digest,
        })
    }

    async fn read_query(
        &self,
        published: PublishedActionQuery,
        calendar: CompletedMarketSessionRead,
        instruments: BTreeSet<InstrumentId>,
        interval: (CalendarDate, CalendarDate),
        cutoff: Timestamp,
        valuation_cutoff: Timestamp,
        context: &RequestContext,
    ) -> Result<SourceAppliedCorporateActionPlan, ServiceError> {
        let generation = self
            .research
            .read_provider_capture_generation(
                published.manifest,
                context.deadline(),
                context.cancellation(),
                |generation, _, _, _, _| Ok(generation.clone()),
            )
            .await
            .map_err(map_research_error)?;
        let source: CorporateActionSourceSnapshot = self
            .research
            .analytical_reader()
            .read_corporate_action_source_snapshot(
                &generation,
                cutoff,
                context.deadline(),
                context.cancellation().clone(),
            )
            .await
            .map_err(|error| map_source_read_error(error, context))?;
        if source.binding_digest() != published.binding {
            return Err(ServiceError::InvalidResult);
        }
        let identity = self
            .research
            .market_data_instruments()
            .reopen_corporate_action_query_identities(
                &source,
                context.deadline(),
                context.cancellation(),
            )
            .map_err(map_query_identity_error)?;
        SourceAppliedCorporateActionPlan::try_from_read(
            Arc::new(source),
            identity,
            calendar,
            instruments,
            interval,
            CorporateActionPolicy::new(CorporateActionAdjustment::TotalReturn, NonZeroU32::MIN),
            CorporateActionPaymentPolicy::EndOfReportedPayableSessionV1,
            valuation_cutoff,
            cutoff,
            limits()?,
            context.deadline(),
            context.cancellation(),
        )
        .map_err(|error| map_plan_error(error, context))
    }
}

fn limits() -> Result<CorporateActionLimits, ServiceError> {
    CorporateActionLimits::try_new(
        NonZeroUsize::new(16_000).ok_or(ServiceError::Internal)?,
        NonZeroUsize::new(64 * 1024 * 1024).ok_or(ServiceError::Internal)?,
    )
    .map_err(|_| ServiceError::Internal)
}
fn new_york_date(at: Timestamp) -> Result<CalendarDate, ServiceError> {
    let date = DateTime::<Utc>::from_timestamp_nanos(at.unix_nanos())
        .with_timezone(&New_York)
        .date_naive();
    CalendarDate::new(
        u16::try_from(date.year()).map_err(|_| ServiceError::InvalidRequest)?,
        u8::try_from(date.month()).map_err(|_| ServiceError::InvalidRequest)?,
        u8::try_from(date.day()).map_err(|_| ServiceError::InvalidRequest)?,
    )
    .map_err(|_| ServiceError::InvalidRequest)
}
fn now() -> Result<Timestamp, ServiceError> {
    SystemMarketCalendarClock
        .now()
        .map_err(|_| ServiceError::Internal)
}
