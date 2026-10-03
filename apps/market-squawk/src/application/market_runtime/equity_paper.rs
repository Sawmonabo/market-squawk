//! Explicit equity simulation admission from the currently installed Alpaca quote actor.
use super::*;
use crate::application::market_calendar::{
    CompletedMarketSessionDateReceipt, CompletedMarketSessionRead,
};
use market_squawk_data::MarketDataInstrumentRecord;
use market_squawk_domain::{
    AssetClass, CalendarDate, Denomination, InstrumentDefinitionRevision, InstrumentExecutionTerms,
    LotSize, TickSize,
};
use market_squawk_live::virtual_paper::{ConsumedVirtualPaperAuthority, VirtualPaperPolicy};
use rust_decimal::Decimal;

/// Inert exact installed source identity. It retains no quote ticket and cannot issue authority.
#[derive(Debug)]
pub(crate) struct EquityPaperSourceBinding {
    descriptor: Arc<super::display::DisplaySourceDescriptor>,
}
impl EquityPaperSourceBinding {
    pub(crate) fn from_selected(
        selected: &MarketDisplaySnapshotLease,
    ) -> Result<Self, ServiceError> {
        if selected.surface_id().as_str() != AccountMarketSurface::AlpacaBasic.surface_id() {
            return Err(ServiceError::Unavailable);
        }
        Ok(Self {
            descriptor: Arc::clone(selected.descriptor()),
        })
    }
    pub(crate) fn matches(&self, selected: &MarketDisplaySnapshotLease) -> bool {
        Arc::ptr_eq(&self.descriptor, selected.descriptor())
    }
}

/// Concrete simulator policy joined to the exact canonical definition and original native session.
/// There are no claims about brokerage lot, tick, settlement, or order permissions.
#[derive(Debug)]
pub(crate) struct EquityPaperRouteEvidence {
    terms: InstrumentExecutionTerms,
    definition_digest: [u8; 32],
    session: CompletedMarketSessionDateReceipt,
}
impl EquityPaperRouteEvidence {
    pub(crate) fn prepare(
        record: &MarketDataInstrumentRecord,
        calendar: &CompletedMarketSessionRead,
        date: CalendarDate,
        at: Timestamp,
    ) -> Result<Self, ServiceError> {
        let definition = record.definition();
        if !matches!(
            definition.asset_class(),
            AssetClass::Equity | AssetClass::Fund
        ) || record.published_at() > at
        {
            return Err(ServiceError::Unavailable);
        }
        let session = Self::open_session(calendar, date, at)?
            .ok_or(ServiceError::Unavailable)?;
        // Version one simulation policy: one whole share, six decimal price increments, cash
        // reporting in the canonical quote currency, multiplier one. Source decimals must fit
        // exactly; no rounding, brokerage precision inference, or venue lot-size assertion.
        let terms = InstrumentExecutionTerms::try_new(
            definition.instrument_id(),
            InstrumentDefinitionRevision::try_from(u64::from(record.revision_sequence()))
                .map_err(|_| ServiceError::Unavailable)?,
            TickSize::power_of_ten(6).map_err(|_| ServiceError::InvalidResult)?,
            LotSize::try_from_decimal(Decimal::ONE).map_err(|_| ServiceError::InvalidResult)?,
            definition.quote_currency(),
            Denomination::Currency(definition.quote_currency()),
            Decimal::ONE,
        )
        .map_err(|_| ServiceError::InvalidResult)?;
        Ok(Self {
            terms,
            definition_digest: record.revision_digest().bytes(),
            session,
        })
    }
    /// Only an admitted native receipt can prove the session is closed. Missing or stale
    /// calendar evidence remains unavailable rather than implying a market schedule.
    pub(crate) fn open_session(
        calendar: &CompletedMarketSessionRead,
        date: CalendarDate,
        at: Timestamp,
    ) -> Result<Option<CompletedMarketSessionDateReceipt>, ServiceError> {
        let session = calendar
            .date_session_on(date, at, at)
            .ok_or(ServiceError::Unavailable)?;
        if session.available_at() > at {
            return Err(ServiceError::Unavailable);
        }
        Ok((at >= session.opens_at() && at < session.closes_at_exclusive())
            .then_some(session))
    }
    pub(crate) const fn terms(&self) -> InstrumentExecutionTerms {
        self.terms
    }
    pub(crate) const fn session(&self) -> &CompletedMarketSessionDateReceipt {
        &self.session
    }
    fn policy(&self) -> VirtualPaperPolicy {
        VirtualPaperPolicy {
            terms: self.terms,
            definition_digest: self.definition_digest,
            calendar_digest: self.session.evidence_digest().bytes(),
            calendar_available_at: self.session.available_at(),
            opens_at: self.session.opens_at(),
            closes_at_exclusive: self.session.closes_at_exclusive(),
        }
    }
}
/// Borrowed actual installed routes; no source selection, calendar or price can be fabricated here.
pub(crate) struct EquityPaperSourceRoute<'a> {
    pub(crate) selected: &'a MarketDisplaySnapshotLease,
    pub(crate) record: &'a MarketDataInstrumentRecord,
    pub(crate) calendar: &'a CompletedMarketSessionRead,
    pub(crate) evidence: &'a EquityPaperRouteEvidence,
    pub(crate) ingress: &'a crate::paper_bot::equity::EquityPaperQuoteIngress,
}

impl MarketRuntimeRegistry {
    /// Actual installed-source caller for a bounded complete held stock set. Capture/publish
    /// precedes sampling; original quotes retain their own clocks throughout the atomic join.
    pub(crate) async fn prepare_and_publish_equity_paper_quotes(
        &self,
        actions: &crate::application::research::SourceActionPreparationCapability,
        paper: &crate::paper_bot::ProductionPaperBotRuntime,
        routes: &[EquityPaperSourceRoute<'_>],
        context: &market_squawk_services::RequestContext,
    ) -> Result<(), ServiceError> {
        self.prepare_and_publish_equity_paper_quotes_before_ingress(
            actions,
            paper,
            routes,
            context,
            || Ok(()),
        )
        .await
    }
    pub(crate) async fn prepare_and_publish_equity_paper_quotes_before_ingress<F>(
        &self,
        actions: &crate::application::research::SourceActionPreparationCapability,
        paper: &crate::paper_bot::ProductionPaperBotRuntime,
        routes: &[EquityPaperSourceRoute<'_>],
        context: &market_squawk_services::RequestContext,
        before_ingress: F,
    ) -> Result<(), ServiceError>
    where
        F: FnOnce() -> Result<(), ServiceError> + Send,
    {
        ensure_active(&self.accepting, context.deadline(), context.cancellation())?;
        if routes.is_empty() || routes.len() > 32 {
            return Err(ServiceError::InvalidRequest);
        }
        let mut ids = std::collections::BTreeSet::new();
        for route in routes {
            if !ids.insert(route.record.definition().instrument_id()) {
                return Err(ServiceError::InvalidRequest);
            }
        }
        let initial = paper
            .paper_snapshot(context.deadline(), context.cancellation())
            .await
            .map_err(|_| ServiceError::Unavailable)?;
        if !initial.complete()
            || initial.reconciliation_required()
            || initial
                .positions()
                .iter()
                .any(|position| !ids.contains(&position.instrument_id()))
        {
            return Err(ServiceError::Unavailable);
        }
        let (bootstrap_at, _) = initial
            .original_accounts()
            .ok_or(ServiceError::Unavailable)?;
        let records: Vec<_> = routes.iter().map(|route| route.record.clone()).collect();
        let prepared = actions
            .acquire_current_paper_sources(&records, bootstrap_at, equity_now()?, context)
            .await?;
        let mut authorities = Vec::new();
        authorities
            .try_reserve_exact(routes.len())
            .map_err(|_| ServiceError::ResourceExhausted)?;
        for route in routes {
            authorities.push(
                self.equity_virtual_paper_quote(
                    route.selected,
                    route.record,
                    route.calendar,
                    route.evidence,
                    equity_now()?,
                    context.deadline(),
                    context.cancellation(),
                )
                .await?,
            );
        }
        let valuation = authorities
            .iter()
            .map(ConsumedVirtualPaperAuthority::received_at)
            .max()
            .ok_or(ServiceError::Unavailable)?;
        let source = actions
            .finish_current_paper_sources(prepared, valuation, context)
            .await?;
        for authority in &authorities {
            authority
                .validate_current()
                .map_err(|_| ServiceError::Unavailable)?;
        }
        let before = paper
            .paper_snapshot(context.deadline(), context.cancellation())
            .await
            .map_err(|_| ServiceError::Unavailable)?;
        if !before.complete()
            || before.reconciliation_required()
            || before
                .original_accounts()
                .is_none_or(|(original, _)| original != bootstrap_at)
            || before
                .positions()
                .iter()
                .any(|position| !ids.contains(&position.instrument_id()))
        {
            return Err(ServiceError::Unavailable);
        }
        paper
            .reconcile_equity_quotes(
                source,
                before.sequence(),
                &authorities,
                context.deadline(),
                context.cancellation(),
            )
            .await?;
        ensure_active(&self.accepting, context.deadline(), context.cancellation())?;
        for authority in &authorities {
            authority
                .validate_current()
                .map_err(|_| ServiceError::Unavailable)?;
        }
        // Reserve every bounded destination before accepting a draft. After the callback there
        // is no await or fallible enqueue; a full/closed source cannot strand a rejected draft.
        let permits = routes
            .iter()
            .map(|route| route.ingress.reserve())
            .collect::<Result<Vec<_>, _>>()?;
        for authority in &authorities {
            authority
                .validate_current()
                .map_err(|_| ServiceError::Unavailable)?;
        }
        before_ingress()?;
        for (permit, authority) in permits.into_iter().zip(authorities) {
            permit.publish(authority);
        }
        Ok(())
    }
    /// Called with a fresh canonical catalog record and a genuine reopened native calendar.
    /// Reuses one actual active Alpaca descriptor, generation and actor read admission. No provider
    /// activation, credentials, synthetic snapshot or executable instrument definition is created.
    pub(crate) async fn equity_virtual_paper_quote(
        &self,
        selected: &MarketDisplaySnapshotLease,
        record: &MarketDataInstrumentRecord,
        calendar: &CompletedMarketSessionRead,
        route: &EquityPaperRouteEvidence,
        at: Timestamp,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<ConsumedVirtualPaperAuthority, ServiceError> {
        ensure_active(&self.accepting, deadline, cancellation)?;
        if selected.surface_id().as_str() != AccountMarketSurface::AlpacaBasic.surface_id()
            || !selected.matches_definition_record(record)
            || selected.definition_revision_digest().bytes() != route.definition_digest
            || calendar.venue_id() != selected.lease().key().venue_id()
            || calendar.reference() != route.session.reference()
            || record.definition().instrument_id() != route.terms.instrument_id()
        {
            return Err(ServiceError::Unavailable);
        }
        {
            let entries = bounded_lock(&self.entries, deadline, cancellation).await?;
            if !entries.iter().any(|entry| {
                entry.is_published_healthy()
                    && entry.runtime.owns_display_descriptor(selected.descriptor())
            }) {
                return Err(ServiceError::Unavailable);
            }
        }
        let original = self
            .display
            .virtual_paper_snapshot(selected.lease().key(), at, cancellation, deadline)
            .await
            .map_err(map_display_read_error)?;
        if !selected.descriptor().matches_snapshot(&original) {
            return Err(ServiceError::Unavailable);
        }
        let source = original
            .into_virtual_paper()
            .ok_or(ServiceError::Unavailable)?;
        let entries = bounded_lock(&self.entries, deadline, cancellation).await?;
        if !entries.iter().any(|entry| {
            entry.is_published_healthy()
                && entry.runtime.owns_display_descriptor(selected.descriptor())
        }) {
            return Err(ServiceError::Unavailable);
        }
        ensure_active(&self.accepting, deadline, cancellation)?;
        source
            .admit(record.definition(), route.policy())
            .map_err(|_| ServiceError::Unavailable)
    }
}

fn equity_now() -> Result<Timestamp, ServiceError> {
    use crate::application::market_calendar::{
        MarketCalendarClock as _, SystemMarketCalendarClock,
    };
    SystemMarketCalendarClock
        .now()
        .map_err(|_| ServiceError::Unavailable)
}
