//! Distinct virtual route ownership within the sole production paper execution graph.
use super::*;
use crate::application::market_calendar::CompletedMarketSessionRead;
use crate::application::{EquityPaperRouteEvidence, EquityPaperSourceRoute, MarketRuntimeRegistry};
use crate::live_source::display_market::DisplayMarketReadTime;
use market_squawk_data::MarketDataInstrumentRecord;
use market_squawk_execution::virtual_paper::ExecutionVirtualPaperHook;
use market_squawk_services::{RequestContext, ServiceError};

pub(crate) struct VirtualEquityRoute {
    pub(crate) key: ShardKey,
    pub(crate) record: MarketDataInstrumentRecord,
    pub(crate) calendar: CompletedMarketSessionRead,
    pub(crate) evidence: EquityPaperRouteEvidence,
    pub(crate) source: crate::application::EquityPaperSourceBinding,
}
impl std::fmt::Debug for VirtualEquityRoute {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VirtualEquityRoute")
            .field("key", &self.key)
            .field("evidence", &self.evidence)
            .finish_non_exhaustive()
    }
}
impl VirtualEquityRoute {
    pub(crate) fn terms(&self) -> InstrumentExecutionTerms {
        self.evidence.terms()
    }
}
#[derive(Debug)]
pub(super) struct PaperExecutionRoute {
    key: ShardKey,
    pub(super) terms: InstrumentExecutionTerms,
}
impl PaperExecutionRoute {
    pub(super) fn route(&self) -> &ShardKey {
        &self.key
    }
}
impl PaperBotSourceComposition {
    pub(super) fn execution_routes(&self) -> Vec<PaperExecutionRoute> {
        match self {
            Self::VirtualEquity(routes) => routes
                .iter()
                .map(|route| PaperExecutionRoute {
                    key: route.key.clone(),
                    terms: route.terms(),
                })
                .collect(),
            _ => self
                .routes()
                .iter()
                .map(|route| PaperExecutionRoute {
                    key: route.route().clone(),
                    terms: route.definition().execution_terms(),
                })
                .collect(),
        }
    }
}
impl ProductionPaperBotComposition {
    pub(crate) fn try_new_virtual_equity(
        routes: Vec<Arc<VirtualEquityRoute>>,
        execution: ProductionPaperBotExecutionConfig,
        strategies: Vec<ProductionPaperBotRoute>,
        maximum_hook_bytes: usize,
    ) -> Result<Self, ProductionPaperBotCompositionError> {
        if routes.is_empty()
            || routes.len() > 32
            || maximum_hook_bytes == 0
            || execution.paper_control_timeout.is_zero()
        {
            return Err(ProductionPaperBotCompositionError::StrategyRouteSetMismatch);
        }
        let source = PaperBotSourceComposition::VirtualEquity(routes);
        let descriptions = source.execution_routes();
        validate_strategy_routes(&descriptions, &strategies)?;
        let manual_paper_routes = manual_paper_routes(&descriptions, &strategies)?;
        validate_canonical_accounts(&execution.accounts, &execution.paper_accounts)?;
        Ok(Self {
            source,
            runtime_config: None,
            maximum_virtual_hook_bytes: maximum_hook_bytes,
            execution,
            strategies,
            manual_paper_routes,
        })
    }
    pub(crate) async fn start_virtual_equity(
        self,
        cancellation: CancellationToken,
    ) -> Result<ProductionPaperBotRuntime, ProductionPaperBotStartError> {
        Ok(self
            .start_inner(PaperBotStartMode::VirtualEquity, cancellation)
            .await?
            .runtime)
    }
}
#[derive(Debug)]
struct RunningRoute {
    route: Arc<VirtualEquityRoute>,
    ingress: equity::EquityPaperQuoteIngress,
    owner: equity::EquityPaperRuntime,
}
#[derive(Debug)]
pub(super) struct VirtualEquityRuntime {
    routes: Vec<RunningRoute>,
}
impl VirtualEquityRuntime {
    pub(super) fn is_healthy(&self) -> bool {
        !self.routes.is_empty() && self.routes.iter().all(|r| r.owner.source_is_current())
    }
    pub(super) async fn shutdown(self) -> Result<(), ServiceError> {
        for route in &self.routes {
            route.ingress.invalidate();
        }
        let mut result = Ok(());
        for route in self.routes {
            if let Err(error) = route.owner.shutdown().await {
                result = Err(error);
            }
        }
        result
    }
}
#[allow(
    clippy::too_many_arguments,
    reason = "same original financial owners are shared across route hooks"
)]
pub(super) async fn start_routes(
    routes: &[Arc<VirtualEquityRoute>],
    strategies: Vec<ProductionPaperBotRoute>,
    accounts: &Arc<AccountRiskCoordinator>,
    portfolio: &PortfolioReadCapability,
    limits: &RiskLimits,
    audit: ExecutionAuditWriter,
    risk_config: RiskServiceConfig,
    dispatcher: &Arc<ExecutionDispatcher>,
    market: &Arc<market_squawk_adapter_paper::PaperMarketIngress>,
    task_reaper: &ExecutionTaskReaper,
    maximum_hook_bytes: usize,
    shutdown: Duration,
    cancellation: &CancellationToken,
) -> Result<VirtualEquityRuntime, ProductionPaperBotStartError> {
    let mut started = VirtualEquityRuntime { routes: Vec::new() };
    started
        .routes
        .try_reserve_exact(routes.len())
        .map_err(|_| ProductionPaperBotStartError::Allocation)?;
    for strategy in strategies {
        let result = (|| {
            let route = routes
                .iter()
                .find(|r| r.key == strategy.route)
                .ok_or(ProductionPaperBotStartError::InvalidRecoveryOwnership)?;
            let risk = RiskService::try_new(
                Arc::clone(accounts),
                portfolio.clone(),
                limits.clone(),
                audit.clone(),
                risk_config,
            )
            .map_err(ProductionPaperBotStartError::Risk)?;
            let hook = ExecutionVirtualPaperHook::try_new(
                strategy.route,
                strategy.strategy,
                risk,
                dispatcher.handle(),
                Arc::clone(market) as Arc<dyn ExecutionMarketSink>,
            )
            .map_err(|_| ProductionPaperBotStartError::InvalidRecoveryOwnership)?;
            let (ingress, owner) = equity::EquityPaperRuntime::try_start(
                hook,
                task_reaper,
                maximum_hook_bytes,
                shutdown,
                cancellation.child_token(),
            )
            .map_err(|_| ProductionPaperBotStartError::Allocation)?;
            Ok::<_, ProductionPaperBotStartError>(RunningRoute {
                route: Arc::clone(route),
                ingress,
                owner,
            })
        })();
        match result {
            Ok(route) => started.routes.push(route),
            Err(error) => {
                if let Err(cleanup) = started.shutdown().await {
                    return Err(ProductionPaperBotStartError::VirtualRouteRollback {
                        startup: Box::new(error),
                        cleanup,
                    });
                }
                return Err(error);
            }
        }
    }
    Ok(started)
}
impl ProductionPaperBotRuntime {
    pub(crate) fn is_virtual_equity(&self) -> bool {
        matches!(self.live, PaperBotLiveRuntime::VirtualEquity(_))
    }
    /// One bounded application request drives acquisition and the exact source-before-quote join.
    pub(crate) async fn refresh_virtual_equity(
        &self,
        market: &MarketRuntimeRegistry,
        actions: &crate::application::SourceActionPreparationCapability,
        definitions: &market_squawk_data::MarketDataInstrumentReadCapability,
        context: &RequestContext,
    ) -> Result<(), ServiceError> {
        self.refresh_virtual_equity_before_ingress(
            market,
            actions,
            definitions,
            context,
            || Ok(()),
        )
        .await?;
        while self.is_virtual_equity() && !self.source_is_healthy() {
            if context.cancellation().is_cancelled() {
                return Err(ServiceError::Cancelled);
            }
            if Instant::now() >= context.deadline() {
                return Err(ServiceError::DeadlineExceeded);
            }
            tokio::select! { biased; ()=context.cancellation().cancelled()=>return Err(ServiceError::Cancelled), ()=tokio::time::sleep_until(context.deadline().into())=>return Err(ServiceError::DeadlineExceeded), ()=tokio::time::sleep(Duration::from_millis(5))=>{} }
        }
        Ok(())
    }
    pub(crate) async fn refresh_virtual_equity_before_ingress<F>(
        &self,
        market: &MarketRuntimeRegistry,
        actions: &crate::application::SourceActionPreparationCapability,
        definitions: &market_squawk_data::MarketDataInstrumentReadCapability,
        context: &RequestContext,
        before_ingress: F,
    ) -> Result<(), ServiceError>
    where
        F: FnOnce() -> Result<(), ServiceError> + Send,
    {
        let PaperBotLiveRuntime::VirtualEquity(runtime) = &self.live else {
            return before_ingress();
        };
        let at = defaults::current_timestamp().map_err(|_| ServiceError::Unavailable)?;
        let mut batches = Vec::new();
        for installed in &runtime.routes {
            let original = &installed.route.record;
            if definitions
                .latest(
                    original.definition().instrument_id(),
                    context.deadline(),
                    context.cancellation(),
                )
                .map_err(|_| ServiceError::Unavailable)?
                .as_ref()
                != Some(original)
            {
                installed.ingress.invalidate();
                return Err(ServiceError::Unavailable);
            }
            batches.push(
                market
                    .display_snapshots_for_instrument(
                        original.definition().instrument_id(),
                        NonZeroUsize::new(32).ok_or(ServiceError::Internal)?,
                        DisplayMarketReadTime::At(at),
                        context.deadline(),
                        context.cancellation(),
                    )
                    .await?,
            );
        }
        let mut selected = Vec::new();
        for (installed, batch) in runtime.routes.iter().zip(&batches) {
            let mut matching = batch.snapshots().iter().filter(|s| {
                s.surface_id().as_str()
                    == crate::application::AccountMarketSurface::AlpacaBasic.surface_id()
                    && s.matches_definition_record(&installed.route.record)
            });
            let Some(source) = matching.next() else {
                installed.ingress.invalidate();
                return Err(ServiceError::Unavailable);
            };
            if !installed.route.source.matches(source) {
                installed.ingress.invalidate();
                return Err(ServiceError::Unavailable);
            }
            if matching.next().is_some() {
                installed.ingress.invalidate();
                return Err(ServiceError::Unavailable);
            }
            selected.push(EquityPaperSourceRoute {
                selected: source,
                record: &installed.route.record,
                calendar: &installed.route.calendar,
                evidence: &installed.route.evidence,
                ingress: &installed.ingress,
            });
        }
        market
            .prepare_and_publish_equity_paper_quotes_before_ingress(
                actions,
                self,
                &selected,
                context,
                before_ingress,
            )
            .await
    }
}
