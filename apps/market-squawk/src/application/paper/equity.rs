//! Explicit provider-neutral stock purpose through the existing paper request owner.
use super::*;
use crate::application::market_calendar::{
    CompletedMarketSessionReadCapability, MarketCalendarClock as _, SystemMarketCalendarClock,
};
use crate::application::research::{
    SourceActionPreparationCapability, SourceAppliedCorporateActionReadCapability,
};
use crate::live_source::display_market::DisplayMarketReadTime;
use crate::paper_bot::{ProductionPaperBotComposition, VirtualEquityRoute};
use chrono::{Datelike as _, Utc};

#[derive(Clone)]
pub(crate) struct EquityPaperServices {
    pub(super) actions: Arc<SourceActionPreparationCapability>,
    pub(super) sources: Arc<SourceAppliedCorporateActionReadCapability>,
    calendars: CompletedMarketSessionReadCapability,
}
impl EquityPaperServices {
    pub(crate) fn new(
        actions: Arc<SourceActionPreparationCapability>,
        sources: Arc<SourceAppliedCorporateActionReadCapability>,
        calendars: CompletedMarketSessionReadCapability,
    ) -> Self {
        Self {
            actions,
            sources,
            calendars,
        }
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum PaperMarketPurpose {
    Equities,
    DigitalAssets,
}
impl PaperMarketPurpose {
    fn id(self) -> &'static str {
        match self {
            Self::Equities => "equities",
            Self::DigitalAssets => "digital-assets",
        }
    }
    pub(super) fn label(self) -> &'static str {
        match self {
            Self::Equities => "Stocks and funds",
            Self::DigitalAssets => "Digital assets",
        }
    }
    pub(super) fn monitoring_label(self) -> &'static str {
        match self {
            Self::Equities => "Virtual orders are evaluated when you refresh or use this session.",
            Self::DigitalAssets => "Virtual orders are evaluated as market updates arrive.",
        }
    }
    pub(super) fn token(self) -> Result<Box<str>, ServiceError> {
        paper_choice_token("market", self.id())
    }
    pub(super) fn resolve(token: &str) -> Result<Self, ServiceError> {
        for purpose in [Self::Equities, Self::DigitalAssets] {
            if purpose.token()?.as_ref() == token {
                return Ok(purpose);
            }
        }
        Err(ServiceError::InvalidRequest)
    }
}
impl PaperController {
    pub(super) async fn select_market(
        &self,
        purpose: PaperMarketPurpose,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<PaperMarketSurfaceSelection, ServiceError> {
        match purpose {
            PaperMarketPurpose::Equities => {
                self.market_runtime
                    .select_equity_paper_market_surface(deadline, cancellation)
                    .await
            }
            PaperMarketPurpose::DigitalAssets => {
                self.market_runtime
                    .select_paper_market_surface(deadline, cancellation)
                    .await
            }
        }
    }
    pub(super) async fn market_choices(
        &self,
        context: &RequestContext,
    ) -> Result<(Vec<Value>, bool), ServiceError> {
        let mut choices = Vec::new();
        let mut equity_session_closed = false;
        for purpose in [
            PaperMarketPurpose::Equities,
            PaperMarketPurpose::DigitalAssets,
        ] {
            match self
                .select_market(purpose, context.deadline(), context.cancellation())
                .await
            {
                Ok(_) => {
                    if purpose == PaperMarketPurpose::Equities {
                        match self.equity_routes(context.deadline(), context.cancellation()).await {
                            Ok(Some(_routes)) => {}
                            Ok(None) => {
                                equity_session_closed = true;
                                continue;
                            }
                            Err(ServiceError::Unavailable) => continue,
                            Err(error) => return Err(error),
                        }
                    }
                    let modes = PAPER_MODE_CHOICES
                        .iter()
                        .filter(|mode| {
                            purpose != PaperMarketPurpose::Equities
                                || mode.mode == PaperStrategyMode::Manual
                        })
                        .map(|mode| paper_choice_token("mode", mode.id))
                        .collect::<Result<Vec<_>, _>>()?;
                    choices.push(json!({"choiceToken": purpose.token()?, "label": purpose.label(), "modeChoices": modes, "monitoringLabel": purpose.monitoring_label()}));
                }
                Err(ServiceError::Unavailable) => {}
                Err(error) => return Err(error),
            }
        }
        Ok((choices, equity_session_closed))
    }
    pub(super) async fn equity_records(
        &self,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<Vec<market_squawk_data::MarketDataInstrumentRecord>, ServiceError> {
        let ids = self
            .market_runtime
            .equity_paper_instrument_ids(deadline, cancellation)
            .await?;
        ids.into_iter()
            .map(|id| {
                self.market_data_instruments
                    .latest(id, deadline, cancellation)
                    .map_err(|_| ServiceError::Unavailable)?
                    .ok_or(ServiceError::Unavailable)
            })
            .collect()
    }
    pub(super) async fn selected_currency(
        &self,
        purpose: PaperMarketPurpose,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<Currency, ServiceError> {
        if purpose == PaperMarketPurpose::DigitalAssets {
            return configured_paper_currency(&self.config);
        }
        let records = self.equity_records(deadline, cancellation).await?;
        let currency = records
            .first()
            .ok_or(ServiceError::Unavailable)?
            .definition()
            .quote_currency();
        if records
            .iter()
            .any(|r| r.definition().quote_currency() != currency)
        {
            return Err(ServiceError::Unavailable);
        }
        Ok(currency)
    }
    pub(super) async fn equity_composition(
        &self,
        prepared: &PreparedPaperStart,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<ProductionPaperBotComposition, ServiceError> {
        let reference = self
            .equity
            .calendars
            .preflight_current_session(deadline, cancellation.clone())
            .await
            .map_err(|error| {
                tracing::warn!(%error, stage = "calendar_acquisition", "paper equity preparation unavailable");
                ServiceError::Unavailable
            })?
            .ok_or(ServiceError::Unavailable)?;
        let at = SystemMarketCalendarClock
            .now()
            .map_err(|_| ServiceError::Unavailable)?;
        let calendar = self
            .equity
            .calendars
            .read_reference(&reference, at, deadline, cancellation.clone())
            .await
            .map_err(|error| {
                tracing::warn!(%error, stage = "calendar_reopen", "paper equity preparation unavailable");
                ServiceError::Unavailable
            })?
            .ok_or(ServiceError::Unavailable)?;
        let routes = self.equity_routes_from_calendar(calendar, at, deadline, cancellation).await?
            .ok_or(ServiceError::Unavailable)?;
        if routes.iter().any(|route| route.terms().quote_currency() != prepared.currency) {
            return Err(ServiceError::Unavailable);
        }
        crate::paper_bot::local_equity_paper_bot(
            self.config.clone(),
            routes,
            prepared.initial_cash,
            prepared.fee_basis_points,
            prepared.strategy_mode,
        )
        .map(|composition| composition.with_source_action_reader(Arc::clone(&self.equity.sources)))
        .map_err(|error| {
            tracing::warn!(%error, stage = "composition", "paper equity preparation unavailable");
            ServiceError::Unavailable
        })
    }

    /// Read-only preparation selects already published evidence; it cannot acquire provider data.
    /// None means an original native session is proven closed, never merely absent evidence.
    pub(super) async fn equity_routes(
        &self,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<Option<Vec<Arc<VirtualEquityRoute>>>, ServiceError> {
        let at = SystemMarketCalendarClock
            .now()
            .map_err(|_| ServiceError::Unavailable)?;
        let calendar = self.equity.calendars
            .select(at, deadline, cancellation.clone())
            .await
            .map_err(|error| {
                tracing::warn!(%error, stage = "calendar_selection", "paper equity preparation unavailable");
                ServiceError::Unavailable
            })?
            .ok_or(ServiceError::Unavailable)?;
        self.equity_routes_from_calendar(calendar, at, deadline, cancellation).await
    }

    async fn equity_routes_from_calendar(
        &self,
        calendar: crate::application::market_calendar::CompletedMarketSessionRead,
        at: Timestamp,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<Option<Vec<Arc<VirtualEquityRoute>>>, ServiceError> {
        let native_date = chrono::DateTime::<Utc>::from_timestamp_nanos(at.unix_nanos())
            .with_timezone(&chrono_tz::America::New_York)
            .date_naive();
        let date = market_squawk_domain::CalendarDate::new(
            u16::try_from(native_date.year()).map_err(|_| ServiceError::Unavailable)?,
            native_date.month() as u8,
            native_date.day() as u8,
        )
        .map_err(|_| ServiceError::Unavailable)?;
        if crate::application::EquityPaperRouteEvidence::open_session(&calendar, date, at)?.is_none() {
            return Ok(None);
        }
        let records = self.equity_records(deadline, cancellation).await?;
        let currency = records.first().ok_or(ServiceError::Unavailable)?.definition().quote_currency();
        let mut routes = Vec::new();
        for record in records {
            if record.definition().quote_currency() != currency {
                return Err(ServiceError::Unavailable);
            }
            let evidence = crate::application::EquityPaperRouteEvidence::prepare(
                &record, &calendar, date, at,
            )?;
            let snapshots = self
                .market_runtime
                .display_snapshots_for_instrument(
                    record.definition().instrument_id(),
                    std::num::NonZeroUsize::new(32).ok_or(ServiceError::Internal)?,
                    DisplayMarketReadTime::At(at),
                    deadline,
                    cancellation,
                )
                .await?;
            let mut matched = snapshots.snapshots().iter().filter(|selected| {
                selected.surface_id().as_str()
                    == super::super::market_runtime::AccountMarketSurface::AlpacaBasic.surface_id()
                    && selected.matches_definition_record(&record)
            });
            let selected = matched.next().ok_or(ServiceError::Unavailable)?;
            if matched.next().is_some() {
                return Err(ServiceError::Unavailable);
            }
            // Sample the same current quote authority that execution must use. Dropping this
            // read grants no order authority; actual runtime ingress reacquires and revalidates it.
            drop(self.market_runtime
                .equity_virtual_paper_quote(
                    selected, &record, &calendar, &evidence, at, deadline, cancellation,
                )
                .await?);
            let source = crate::application::EquityPaperSourceBinding::from_selected(selected)?;
            let key = ShardKey::new(
                calendar.venue_id().clone(),
                record.definition().instrument_id(),
            );
            routes.push(Arc::new(VirtualEquityRoute {
                key,
                record,
                calendar: calendar.clone(),
                evidence,
                source,
            }));
        }
        Ok(Some(routes))
    }
    pub(super) async fn refresh_equity(
        &self,
        runtime: &ProductionPaperBotRuntime,
        context: &RequestContext,
    ) -> Result<(), ServiceError> {
        runtime
            .refresh_virtual_equity(
                &self.market_runtime,
                &self.equity.actions,
                &self.market_data_instruments,
                context,
            )
            .await
    }
}
