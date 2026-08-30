//! Bounded multi-provider market-runtime ownership shared by every local presentation.

mod alpaca_historical;
mod configuration;
mod display;
mod generation;
mod group;
mod kraken;
mod schwab;
mod schwab_current;
mod schwab_sink;

pub(crate) use alpaca_historical::{
    AlpacaHistoricalCompositeCalendarAuthority, AlpacaHistoricalLookupError,
    AlpacaHistoricalRuntimeCapability,
};
pub(crate) use configuration::{
    AccountMarketSurface, PreparedMarketProviderConfigurationRequest,
    PreparedMarketProviderConfigurationResolver, PreparedSchwabMarketRuntimeResolver,
};
pub(crate) use display::{MarketDisplaySnapshotBatch, MarketDisplaySnapshotLease};
pub(crate) use generation::{MarketRuntimeGroupGeneration, MarketSourceRuntimeGeneration};
pub(crate) use group::MarketProviderGroupLifecycleEvidence;
pub(crate) use kraken::MarketKrakenPriceProjectionLease;
pub(crate) use schwab::{
    SchwabRestQuoteBatch, SchwabRestQuoteBatchOutcome, SchwabRestQuoteEventSink,
    SchwabRestQuoteInstrumentBinding, SchwabRestQuotePollOutcome, SchwabRestQuoteProducer,
    SchwabRestQuotePublicationReceipt, SchwabRestQuoteRuntimeBounds, SchwabRestQuoteRuntimeError,
    SchwabRestQuoteSinkError, SchwabRestQuoteSourceEvidence,
};
pub(crate) use schwab_current::SCHWAB_CURRENT_LIVE_AUTHORITY_KEY;
#[cfg(test)]
pub(crate) use schwab_sink::SchwabRestQuoteSealFirstSink;
pub(crate) use schwab_sink::{SchwabRestQuoteCurrentRuntime, SchwabRestQuoteCurrentRuntimeInput};

use std::{
    fmt,
    future::Future,
    num::{NonZeroU64, NonZeroUsize},
    pin::Pin,
    sync::{Arc, Weak},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use market_squawk_adapter_alpaca::AlpacaHistoricalEquityPreflightPlan;
use market_squawk_adapter_schwab::SchwabOAuthAuthorityReceipt;
use market_squawk_domain::{
    ConnectionGeneration, CoverageDelay, CoverageStatus, DataQuality, EvidenceDigest, InstrumentId,
    MarketDataInstrumentDefinition, MarketDepth, SourceId, SourceIdentifier, StreamIntegrityState,
    Timestamp, VenueId,
};
use market_squawk_live::{
    LiveActionHookGeneration, LiveActionHookReapReceipt, LiveRouteConfig, LiveRuntimeSnapshotLease,
    LiveSnapshotReader, PreparedLiveActionHookGroup, RouteActionHook, ShardKey,
    ShardLifecycleSnapshot, SnapshotCompleteness, StreamPhaseSnapshot,
};
use market_squawk_platform::SecretGeneration;
use market_squawk_services::ServiceError;
use market_squawk_sources::{ProviderRateAuthority, SourceMetadata};
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

use self::{
    alpaca_historical::AlpacaHistoricalCapabilityError,
    display::DisplaySourceDescriptor,
    generation::MarketRuntimeTopology,
    group::{
        AccountMarketRuntimeGroup, AccountMarketRuntimeLimits, PreparedAccountMarketRuntimeStart,
    },
    kraken::KrakenSourceDescriptor,
};
use super::live_fair_value::{LiveFairValueExportDrains, LiveFairValueObservationBuffer};
use super::{
    AlpacaHistoricalAuthorizedPlan, AlpacaHistoricalPlanAdmissionError,
    AlpacaHistoricalPlanReceipt, AlpacaHistoricalSourceMutationAuthority, MarketEventDurableRead,
};
use crate::{
    AppConfig, CoinbaseDirectLiveRuntime, ProductionLiveSourceRuntime, ProductionSourceProvider,
    live_source::display_market::{
        DisplayMarketDirectory, DisplayMarketDirectoryError, DisplayMarketReadError,
        MAX_DISPLAY_MARKET_ROUTES,
    },
    live_source::order_level::{
        MAX_ORDER_LEVEL_DIRECTORY_BOOKS, OrderLevelBookKey, OrderLevelDirectory,
        OrderLevelDirectoryError, OrderLevelOrdersRead, OrderLevelReadError,
    },
    paper_bot::{
        local_coinbase_direct_live_market_with_activation, local_live_market_with_provider_rate,
    },
    provider_activation::{
        AccountMarketRuntimeMutationAuthority, PreparedMarketProviderConfiguration,
        ProviderAdapterActivation,
    },
};

pub(crate) const COINBASE_PUBLIC_SURFACE_ID: &str = "coinbase.public-market-data";
pub(crate) const COINBASE_DIRECT_SURFACE_ID: &str = "coinbase.exchange-direct-market-data";
pub(crate) const KRAKEN_PUBLIC_SURFACE_ID: &str = "kraken.spot-public-market-data";

const MAXIMUM_CONCURRENT_MARKET_SURFACES: usize = 16;
const ACCOUNT_HEALTH_SCAN_INTERVAL: Duration = Duration::from_millis(250);

/// Exact live evidence returned after one registry-owned source lifecycle operation.
#[derive(Clone, Debug)]
pub(crate) struct MarketSourceLifecycleEvidence {
    pub(crate) provider: SourceIdentifier,
    pub(crate) generation: MarketSourceRuntimeGeneration,
    pub(crate) coverage: CoverageStatus,
    pub(crate) integrity: StreamIntegrityState,
    pub(crate) quality: DataQuality,
    pub(crate) observed_at: Timestamp,
}

/// One source-labelled immutable runtime snapshot retained for a bounded request.
#[derive(Debug)]
pub(crate) struct MarketSourceSnapshotLease {
    surface_id: SourceIdentifier,
    metadata: Arc<[SourceMetadata]>,
    lease: LiveRuntimeSnapshotLease,
}

/// Application-owned choice of one healthy live market runtime for virtual paper execution.
///
/// Provider identity remains internal to the runtime boundary. Product callers request the best
/// eligible market evidence and never select or receive this coordinate.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PaperMarketSurfaceSelection {
    surface_id: SourceIdentifier,
    onboarding_session_id: Option<uuid::Uuid>,
}

/// One source-bound immutable market reader joined to one validated runtime route.
///
/// This is an internal application capability. Provider and route coordinates remain below the
/// ordinary product boundary and are used only to prevent cross-source or cross-venue reads.
#[derive(Clone, Debug)]
pub(crate) struct MarketEventDurableRouteRead {
    read: MarketEventDurableRead,
    surface_id: SourceIdentifier,
    metadata: Arc<[SourceMetadata]>,
    source_index: usize,
    route: ShardKey,
}

impl MarketEventDurableRouteRead {
    pub(crate) const fn read(&self) -> &MarketEventDurableRead {
        &self.read
    }

    pub(crate) const fn surface_id(&self) -> &SourceIdentifier {
        &self.surface_id
    }

    pub(crate) fn metadata(&self) -> &SourceMetadata {
        &self.metadata[self.source_index]
    }

    pub(crate) const fn route(&self) -> &ShardKey {
        &self.route
    }
}

impl PaperMarketSurfaceSelection {
    pub(crate) const fn surface_id(&self) -> &SourceIdentifier {
        &self.surface_id
    }

    pub(crate) const fn onboarding_session_id(&self) -> Option<uuid::Uuid> {
        self.onboarding_session_id
    }
}

impl MarketSourceSnapshotLease {
    pub(crate) const fn surface_id(&self) -> &SourceIdentifier {
        &self.surface_id
    }

    pub(crate) const fn lease(&self) -> &LiveRuntimeSnapshotLease {
        &self.lease
    }

    pub(crate) const fn metadata(&self) -> &Arc<[SourceMetadata]> {
        &self.metadata
    }
}

/// Complete bounded set of healthy provider snapshots observed for one application request.
#[derive(Debug)]
pub(crate) struct MarketRuntimeSnapshotBatch {
    sources: Vec<MarketSourceSnapshotLease>,
    failures: Vec<MarketSourceSnapshotFailure>,
}

impl MarketRuntimeSnapshotBatch {
    pub(crate) fn sources(&self) -> &[MarketSourceSnapshotLease] {
        &self.sources
    }

    pub(crate) fn failures(&self) -> &[MarketSourceSnapshotFailure] {
        &self.failures
    }
}

/// One bounded individual-order view bound to an exact source generation.
#[derive(Debug)]
pub(crate) struct MarketOrderLevelSnapshot {
    key: OrderLevelBookKey,
    orders: OrderLevelOrdersRead,
}

impl MarketOrderLevelSnapshot {
    pub(crate) const fn source_id(&self) -> &SourceId {
        self.key.source_id()
    }

    pub(crate) const fn venue_id(&self) -> &VenueId {
        self.key.venue_id()
    }

    pub(crate) const fn instrument_id(&self) -> InstrumentId {
        self.key.instrument_id()
    }

    pub(crate) const fn generation(&self) -> ConnectionGeneration {
        self.key.generation()
    }

    pub(crate) const fn orders(&self) -> &OrderLevelOrdersRead {
        &self.orders
    }
}

/// One source-local snapshot read failure retained without hiding healthy providers.
#[derive(Clone, Debug)]
pub(crate) struct MarketSourceSnapshotFailure {
    surface_id: SourceIdentifier,
    kind: MarketSourceSnapshotFailureKind,
}

impl MarketSourceSnapshotFailure {
    pub(crate) const fn surface_id(&self) -> &SourceIdentifier {
        &self.surface_id
    }

    pub(crate) const fn kind(&self) -> MarketSourceSnapshotFailureKind {
        self.kind
    }
}

/// Closed presentation-safe reason why one provider snapshot could not be retained.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum MarketSourceSnapshotFailureKind {
    ResourceExhausted,
    Unavailable,
}

/// Disabled dynamic hooks bound to one exact existing source runtime.
#[derive(Debug)]
pub(crate) struct PreparedMarketActionHooks {
    prepared: PreparedLiveActionHookGroup,
}

impl PreparedMarketActionHooks {
    pub(crate) fn runtime_incarnation(&self) -> NonZeroU64 {
        self.prepared.runtime_incarnation()
    }

    pub(crate) fn generation(&self) -> LiveActionHookGeneration {
        self.prepared.generation()
    }

    pub(crate) fn activate(
        self,
    ) -> Result<market_squawk_live::ActiveLiveActionHookGroup, ServiceError> {
        self.prepared.activate().map_err(|error| {
            tracing::error!(%error, "market action-hook activation failed");
            ServiceError::Unavailable
        })
    }
}

/// One per-user owner of every active market provider runtime.
pub(crate) struct MarketRuntimeRegistry {
    config: AppConfig,
    provider_rate: ProviderRateAuthority,
    provider_activation: Arc<ProviderAdapterActivation>,
    alpaca_historical_source: AlpacaHistoricalSourceMutationAuthority,
    prepared_configuration: Arc<dyn PreparedMarketProviderConfigurationResolver>,
    prepared_schwab: Arc<dyn PreparedSchwabMarketRuntimeResolver>,
    live_fair_value: Arc<LiveFairValueObservationBuffer>,
    accepting: std::sync::atomic::AtomicBool,
    lifecycle: CancellationToken,
    capture_process: market_squawk_platform::CaptureProcessInfrastructure,
    display: DisplayMarketDirectory,
    order_level: OrderLevelDirectory,
    account_limits: AccountMarketRuntimeLimits,
    shutdown: Mutex<Option<Result<(), ServiceError>>>,
    mutation: Mutex<()>,
    entries: Mutex<Vec<MarketRuntimeEntry>>,
    durable_market_routes: Mutex<Vec<MarketEventDurableRouteRead>>,
    account_health_cancellation: CancellationToken,
    account_health_drain: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct AccountMarketRuntimeHealthSnapshot {
    surface_id: SourceIdentifier,
    group_generation: MarketRuntimeGroupGeneration,
}

enum AccountGroupStartPreparation {
    Existing(MarketProviderGroupLifecycleEvidence),
    Ready {
        surface_id: SourceIdentifier,
        prepared: PreparedAccountMarketRuntimeStart,
        runtime_cancellation: CancellationToken,
    },
}

enum AccountGroupPublicationAttempt {
    Published,
    Rejected {
        entry: MarketRuntimeEntry,
        error: ServiceError,
    },
}

impl MarketRuntimeRegistry {
    pub(crate) fn try_new(
        config: AppConfig,
        provider_rate: ProviderRateAuthority,
        provider_activation: Arc<ProviderAdapterActivation>,
        alpaca_historical_source: AlpacaHistoricalSourceMutationAuthority,
        prepared_configuration: Arc<dyn PreparedMarketProviderConfigurationResolver>,
        prepared_schwab: Arc<dyn PreparedSchwabMarketRuntimeResolver>,
        live_fair_value: Arc<LiveFairValueObservationBuffer>,
    ) -> Result<Arc<Self>, ServiceError> {
        let mut entries = Vec::new();
        entries
            .try_reserve_exact(MAXIMUM_CONCURRENT_MARKET_SURFACES)
            .map_err(|_error| ServiceError::ResourceExhausted)?;
        validate_unique_schwab_account_surface(&entries)?;
        let mut durable_market_routes = Vec::new();
        durable_market_routes
            .try_reserve_exact(MAX_DISPLAY_MARKET_ROUTES)
            .map_err(|_error| ServiceError::ResourceExhausted)?;
        let lifecycle = CancellationToken::new();
        let capture_process = market_squawk_platform::initialize_capture_process_infrastructure(
            market_squawk_platform::CaptureProcessInfrastructureLimits::new(
                config.capture_destination_registry_memory_ceiling_bytes(),
            ),
        )
        .map_err(|error| {
            tracing::error!(%error, "market capture-process authority construction failed");
            ServiceError::Unavailable
        })?;
        let display = DisplayMarketDirectory::try_new(
            NonZeroUsize::new(MAX_DISPLAY_MARKET_ROUTES).ok_or(ServiceError::ResourceExhausted)?,
            lifecycle.child_token(),
        )
        .map_err(|error| {
            tracing::error!(%error, "display-market directory construction failed");
            ServiceError::ResourceExhausted
        })?;
        let order_level = OrderLevelDirectory::try_new(
            NonZeroUsize::new(MAX_ORDER_LEVEL_DIRECTORY_BOOKS)
                .ok_or(ServiceError::ResourceExhausted)?,
            lifecycle.child_token(),
        )
        .map_err(|error| {
            tracing::error!(%error, "order-level market directory construction failed");
            ServiceError::ResourceExhausted
        })?;
        let account_limits = AccountMarketRuntimeLimits::try_v1()?;
        Ok(Arc::new(Self {
            config,
            provider_rate,
            provider_activation,
            alpaca_historical_source,
            prepared_configuration,
            prepared_schwab,
            live_fair_value,
            accepting: std::sync::atomic::AtomicBool::new(true),
            lifecycle,
            capture_process,
            display,
            order_level,
            account_limits,
            shutdown: Mutex::new(None),
            mutation: Mutex::new(()),
            entries: Mutex::new(entries),
            durable_market_routes: Mutex::new(durable_market_routes),
            account_health_cancellation: CancellationToken::new(),
            account_health_drain: Mutex::new(None),
        }))
    }

    async fn ensure_account_health_drain_started(
        self: &Arc<Self>,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<(), ServiceError> {
        if !self.accepting.load(std::sync::atomic::Ordering::Acquire)
            || self.account_health_cancellation.is_cancelled()
        {
            return Err(ServiceError::Unavailable);
        }
        let mut drain = bounded_lock(&self.account_health_drain, deadline, cancellation).await?;
        if !self.accepting.load(std::sync::atomic::Ordering::Acquire)
            || self.account_health_cancellation.is_cancelled()
        {
            return Err(ServiceError::Unavailable);
        }
        if let Some(running) = drain.as_ref() {
            return if running.is_finished() {
                Err(ServiceError::Unavailable)
            } else {
                Ok(())
            };
        }
        *drain = Some(tokio::spawn(run_account_health_drain(
            Arc::downgrade(self),
            self.account_health_cancellation.clone(),
        )));
        Ok(())
    }

    pub(crate) fn active_source_count(&self) -> Result<usize, ServiceError> {
        let entries = self
            .entries
            .try_lock()
            .map_err(|_busy| ServiceError::Unavailable)?;
        Ok(entries
            .iter()
            .filter(|entry| entry.is_published_healthy())
            .count())
    }

    pub(crate) fn is_account_free_source_configured(&self, provider: &SourceIdentifier) -> bool {
        match provider.as_str() {
            COINBASE_PUBLIC_SURFACE_ID => self.config.coinbase().is_some(),
            KRAKEN_PUBLIC_SURFACE_ID => self.config.kraken().is_some(),
            _ => false,
        }
    }

    pub(crate) async fn start(
        &self,
        provider: &SourceIdentifier,
        onboarding_session_id: Option<uuid::Uuid>,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<MarketSourceLifecycleEvidence, ServiceError> {
        let _mutation = bounded_lock(&self.mutation, deadline, cancellation).await?;
        self.start_owned(provider, onboarding_session_id, deadline, cancellation)
            .await
    }

    /// Starts one exact account-backed provider group and publishes it only after every required
    /// child reports ready. Optional children are included only when the prepared configuration
    /// explicitly contains them.
    pub(crate) async fn start_account_group(
        self: &Arc<Self>,
        request: PreparedMarketProviderConfigurationRequest,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<MarketProviderGroupLifecycleEvidence, ServiceError> {
        self.ensure_account_health_drain_started(deadline, cancellation)
            .await?;
        let _mutation = bounded_lock(&self.mutation, deadline, cancellation).await?;
        let startup: Pin<
            Box<
                dyn Future<Output = Result<MarketProviderGroupLifecycleEvidence, ServiceError>>
                    + Send
                    + '_,
            >,
        > = Box::pin(self.start_account_group_owned(request, deadline, cancellation));
        startup.await
    }

    async fn start_account_group_owned(
        &self,
        request: PreparedMarketProviderConfigurationRequest,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<MarketProviderGroupLifecycleEvidence, ServiceError> {
        let preparation: Pin<
            Box<
                dyn Future<Output = Result<AccountGroupStartPreparation, ServiceError>> + Send + '_,
            >,
        > = Box::pin(self.prepare_account_group_start(request, deadline, cancellation));
        let prepared = preparation.await?;
        let (surface_id, prepared, runtime_cancellation) = match prepared {
            AccountGroupStartPreparation::Existing(evidence) => return Ok(evidence),
            AccountGroupStartPreparation::Ready {
                surface_id,
                prepared,
                runtime_cancellation,
            } => (surface_id, prepared, runtime_cancellation),
        };
        let group_start: Pin<
            Box<dyn Future<Output = Result<AccountMarketRuntimeGroup, ServiceError>> + Send + '_>,
        > = Box::pin(AccountMarketRuntimeGroup::start(
            request,
            prepared,
            self.provider_activation.as_ref(),
            self.config.clone(),
            self.provider_rate.clone(),
            self.capture_process,
            self.display.clone(),
            self.order_level.clone(),
            self.account_limits,
            runtime_cancellation.clone(),
            deadline,
            cancellation,
        ));
        let group = group_start.await?;
        let publication: Pin<
            Box<
                dyn Future<Output = Result<MarketProviderGroupLifecycleEvidence, ServiceError>>
                    + Send
                    + '_,
            >,
        > = Box::pin(self.publish_account_group_start(
            request,
            surface_id,
            runtime_cancellation,
            group,
            deadline,
            cancellation,
        ));
        publication.await
    }

    async fn prepare_account_group_start(
        &self,
        request: PreparedMarketProviderConfigurationRequest,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<AccountGroupStartPreparation, ServiceError> {
        ensure_active(&self.accepting, deadline, cancellation)?;
        let surface_id = try_surface_identifier(request.surface())?;
        if request.surface() == AccountMarketSurface::SchwabMarketData {
            let entries = bounded_lock(&self.entries, deadline, cancellation).await?;
            validate_unique_schwab_account_surface(&entries)?;
        }
        match self
            .verify_account_group_owned(request, deadline, cancellation)
            .await
        {
            Ok(Some(evidence)) => return Ok(AccountGroupStartPreparation::Existing(evidence)),
            Ok(None) | Err(ServiceError::Unavailable) => {}
            Err(error) => return Err(error),
        }
        self.remove_unhealthy_account_group_owned(&surface_id, request, deadline, cancellation)
            .await?;

        let mut resolution_guard = StartupCancellation::new(self.lifecycle.child_token());
        let prepared = match request.surface() {
            AccountMarketSurface::SchwabMarketData => PreparedAccountMarketRuntimeStart::Schwab(
                await_service_before(
                    deadline,
                    cancellation,
                    self.prepared_schwab
                        .resolve(request, deadline, resolution_guard.token()),
                )
                .await?,
            ),
            AccountMarketSurface::AlpacaBasic | AccountMarketSurface::KrakenLevel3 => {
                PreparedAccountMarketRuntimeStart::Standard(
                    await_service_before(
                        deadline,
                        cancellation,
                        self.prepared_configuration.resolve(
                            request,
                            deadline,
                            resolution_guard.token(),
                        ),
                    )
                    .await?,
                )
            }
        };
        resolution_guard.disarm();
        let runtime_cancellation = self.lifecycle.child_token();
        Ok(AccountGroupStartPreparation::Ready {
            surface_id,
            prepared,
            runtime_cancellation,
        })
    }

    async fn publish_account_group_start(
        &self,
        request: PreparedMarketProviderConfigurationRequest,
        surface_id: SourceIdentifier,
        runtime_cancellation: CancellationToken,
        group: AccountMarketRuntimeGroup,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<MarketProviderGroupLifecycleEvidence, ServiceError> {
        let account_lease = group.activation_lease().clone();
        if let Err(error) = validate_account_lease(request, &account_lease) {
            let cleanup: Pin<Box<dyn Future<Output = Result<(), ServiceError>> + Send + '_>> =
                Box::pin(group.shutdown_before(deadline, cancellation));
            let _cleanup = cleanup.await;
            return Err(error);
        }
        let evidence = group.evidence().clone();
        let metadata = group.metadata();
        let routes = group.routes();
        let topology = match (metadata.is_empty(), routes.is_empty()) {
            (true, true) => None,
            (false, false) => {
                match MarketRuntimeTopology::try_new(&surface_id, Arc::clone(&metadata), routes) {
                    Ok(topology) => Some(topology),
                    Err(error) => {
                        let cleanup: Pin<
                            Box<dyn Future<Output = Result<(), ServiceError>> + Send + '_>,
                        > = Box::pin(group.shutdown_before(deadline, cancellation));
                        let _cleanup = cleanup.await;
                        return Err(error);
                    }
                }
            }
            (true, false) | (false, true) => {
                let cleanup: Pin<Box<dyn Future<Output = Result<(), ServiceError>> + Send + '_>> =
                    Box::pin(group.shutdown_before(deadline, cancellation));
                let _cleanup = cleanup.await;
                return Err(ServiceError::InvalidResult);
            }
        };
        let entry = MarketRuntimeEntry {
            surface_id,
            onboarding_session_id: Some(request.onboarding_session_id()),
            metadata,
            topology,
            cancellation: runtime_cancellation,
            runtime: MarketRuntime::Account(group),
            exports: None,
            action_hooks_installed: false,
        };
        let attempt: Pin<Box<dyn Future<Output = AccountGroupPublicationAttempt> + Send + '_>> =
            Box::pin(self.attempt_account_group_publication(
                request,
                account_lease,
                entry,
                deadline,
                cancellation,
            ));
        match attempt.await {
            AccountGroupPublicationAttempt::Published => Ok(evidence),
            AccountGroupPublicationAttempt::Rejected { entry, error } => {
                let cleanup: Pin<Box<dyn Future<Output = Result<(), ServiceError>> + Send + '_>> =
                    Box::pin(entry.shutdown(self.config.source_shutdown()));
                let cleanup_result = cleanup.await;
                match cleanup_result {
                    Ok(()) => Err(error),
                    Err(cleanup_error) => Err(cleanup_error),
                }
            }
        }
    }

    async fn attempt_account_group_publication(
        &self,
        request: PreparedMarketProviderConfigurationRequest,
        account_lease: crate::ProviderActivationLease,
        entry: MarketRuntimeEntry,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> AccountGroupPublicationAttempt {
        if let Err(error) = ensure_active(&self.accepting, deadline, cancellation) {
            return AccountGroupPublicationAttempt::Rejected { entry, error };
        }
        let publication_authority = match self
            .account_market_mutation_authority_before(deadline, cancellation)
            .await
        {
            Ok(authority) => authority,
            Err(error) => {
                return AccountGroupPublicationAttempt::Rejected { entry, error };
            }
        };
        let mut entries = match bounded_lock(&self.entries, deadline, cancellation).await {
            Ok(entries) => entries,
            Err(error) => {
                drop(publication_authority);
                return AccountGroupPublicationAttempt::Rejected { entry, error };
            }
        };
        if entries.len() == MAXIMUM_CONCURRENT_MARKET_SURFACES
            || entries
                .iter()
                .any(|current| current.surface_id == entry.surface_id)
        {
            drop(entries);
            drop(publication_authority);
            return AccountGroupPublicationAttempt::Rejected {
                entry,
                error: ServiceError::ResourceExhausted,
            };
        }
        if let Err(error) = ensure_active(&self.accepting, deadline, cancellation) {
            drop(entries);
            drop(publication_authority);
            return AccountGroupPublicationAttempt::Rejected { entry, error };
        }
        if !entry.is_healthy() {
            drop(entries);
            drop(publication_authority);
            return AccountGroupPublicationAttempt::Rejected {
                entry,
                error: ServiceError::Unavailable,
            };
        }
        let active_lease = if request.surface() == AccountMarketSurface::AlpacaBasic {
            match publication_authority.commit_prepared_activation(&account_lease) {
                Ok(active) => active,
                Err(error) => {
                    tracing::error!(%error, "account-market activation commit failed");
                    drop(entries);
                    drop(publication_authority);
                    return AccountGroupPublicationAttempt::Rejected {
                        entry,
                        error: ServiceError::Unauthorized,
                    };
                }
            }
        } else {
            account_lease
        };
        let publication = publication_authority
            .require_active(&active_lease)
            .map_err(|error| {
                tracing::error!(%error, "account-market active-lease validation failed");
                ServiceError::Unauthorized
            })
            .and_then(|()| validate_account_lease(request, &active_lease));
        if let Err(error) = publication {
            tracing::error!(%error, "account-market registry publication authority failed");
            drop(entries);
            drop(publication_authority);
            return AccountGroupPublicationAttempt::Rejected {
                entry,
                error: ServiceError::Unauthorized,
            };
        }
        // Durable Active and the exact runtime remain serialized by one onboarding mutation
        // authority until this infallible registry insertion completes.
        entries.push(entry);
        drop(entries);
        drop(publication_authority);
        AccountGroupPublicationAttempt::Published
    }

    async fn start_owned(
        &self,
        provider: &SourceIdentifier,
        onboarding_session_id: Option<uuid::Uuid>,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<MarketSourceLifecycleEvidence, ServiceError> {
        ensure_active(&self.accepting, deadline, cancellation)?;
        let surface = MarketSurface::parse(provider, onboarding_session_id)?;
        self.require_existing_session(
            provider,
            surface.onboarding_session_id(),
            deadline,
            cancellation,
        )
        .await?;
        match self.verify_owned(provider).await {
            Ok(Some(evidence)) => return Ok(evidence),
            Ok(None) | Err(ServiceError::Unavailable) => {}
            Err(error) => return Err(error),
        }
        self.remove_unhealthy_owned(provider, deadline, cancellation)
            .await?;
        let runtime_cancellation = self.lifecycle.child_token();
        let entry = match surface {
            MarketSurface::Public {
                provider: provider_kind,
                session_id,
            } => {
                let composition = local_live_market_with_provider_rate(
                    self.config.clone(),
                    provider_kind,
                    self.provider_rate.clone(),
                )
                .map_err(|error| {
                    tracing::error!(provider = ?provider_kind, %error, "market source composition failed");
                    ServiceError::Unavailable
                })?;
                let metadata = composition.source_metadata().map_err(|error| {
                    tracing::error!(
                        provider = ?provider_kind,
                        %error,
                        "market source metadata-set construction failed"
                    );
                    ServiceError::Unavailable
                })?;
                let activation_lease = self
                    .provider_activation
                    .activate_public_live_metadata(session_id, provider_kind, metadata.as_ref())
                    .map_err(|error| {
                        tracing::error!(provider = ?provider_kind, %error, "public market activation binding failed");
                        ServiceError::Unavailable
                    })?;
                if activation_lease.session_id() != session_id
                    || activation_lease.surface_id() != provider
                {
                    return Err(ServiceError::Unavailable);
                }
                let publication_cancellation = CancellationToken::new();
                let publication_package = match provider_kind {
                    ProductionSourceProvider::Coinbase => {
                        let source = metadata.first().ok_or(ServiceError::Unavailable)?;
                        if metadata.len() != 1 {
                            return Err(ServiceError::Unavailable);
                        }
                        await_before(
                            deadline,
                            cancellation,
                            self.provider_activation
                                .acquire_coinbase_market_publication_package(
                                    &activation_lease,
                                    source,
                                    publication_cancellation.clone(),
                                ),
                        )
                        .await?
                    }
                    ProductionSourceProvider::Kraken => {
                        let book = metadata.first().ok_or(ServiceError::Unavailable)?;
                        let trades = metadata.get(1).ok_or(ServiceError::Unavailable)?;
                        if metadata.len() != 2 {
                            return Err(ServiceError::Unavailable);
                        }
                        await_before(
                            deadline,
                            cancellation,
                            self.provider_activation
                                .acquire_kraken_market_publication_package(
                                    &activation_lease,
                                    book,
                                    trades,
                                    publication_cancellation.clone(),
                                ),
                        )
                        .await?
                    }
                };
                let route_keys = clone_route_keys(composition.live_routes())?;
                let topology =
                    MarketRuntimeTopology::try_new(provider, Arc::clone(&metadata), route_keys)?;
                let mut durable_reads = Vec::new();
                durable_reads
                    .try_reserve_exact(publication_package.durable_read_count())
                    .map_err(|_error| ServiceError::ResourceExhausted)?;
                publication_package.append_durable_reads(&mut durable_reads);
                let durable_routes = durable_route_bindings(
                    provider,
                    Arc::clone(&metadata),
                    &topology,
                    durable_reads,
                )?;
                self.replace_durable_market_routes(
                    provider,
                    durable_routes,
                    deadline,
                    cancellation,
                )
                .await?;
                let (exports, drains) = LiveFairValueExportDrains::try_start(
                    composition.qualified_market_export_source_id().clone(),
                    composition.live_routes(),
                    composition.maximum_message_bytes(),
                    Arc::clone(&self.live_fair_value),
                    runtime_cancellation.clone(),
                    deadline,
                )
                .await
                .map_err(|error| {
                    tracing::error!(provider = ?provider_kind, %error, "market export startup failed");
                    ServiceError::Unavailable
                })?;
                let started = await_before(
                    deadline,
                    cancellation,
                    composition.start_with_qualified_market_exports_and_crypto_publication(
                        exports,
                        publication_package,
                        publication_cancellation,
                        runtime_cancellation.clone(),
                    ),
                )
                .await;
                let runtime = match started {
                    Ok(runtime) => runtime,
                    Err(error) => {
                        drains.begin_shutdown();
                        runtime_cancellation.cancel();
                        let cleanup = CancellationToken::new();
                        let _cleanup = drains.finish_before(deadline, &cleanup).await;
                        return Err(error);
                    }
                };
                MarketRuntimeEntry {
                    surface_id: provider.clone(),
                    onboarding_session_id: Some(session_id),
                    metadata,
                    topology: Some(topology),
                    cancellation: runtime_cancellation,
                    runtime: MarketRuntime::Public(runtime),
                    exports: Some(drains),
                    action_hooks_installed: false,
                }
            }
            MarketSurface::CoinbaseDirect { session_id } => {
                let composition = await_before(
                    deadline,
                    cancellation,
                    local_coinbase_direct_live_market_with_activation(
                        self.config.clone(),
                        session_id,
                        self.provider_activation.as_ref(),
                        runtime_cancellation.clone(),
                    ),
                )
                .await?;
                let started = await_before(
                    deadline,
                    cancellation,
                    composition.start_with_order_level(
                        self.order_level.clone(),
                        runtime_cancellation.clone(),
                    ),
                )
                .await;
                let runtime = match started {
                    Ok(runtime) => runtime,
                    Err(error) => {
                        runtime_cancellation.cancel();
                        return Err(error);
                    }
                };
                let metadata = runtime.metadata();
                let routes = runtime.routes();
                let topology =
                    match MarketRuntimeTopology::try_new(provider, Arc::clone(&metadata), routes) {
                        Ok(topology) => topology,
                        Err(error) => {
                            runtime_cancellation.cancel();
                            let _cleanup = runtime.shutdown().await;
                            return Err(error);
                        }
                    };
                let durable_reads = runtime.durable_reads();
                let durable_routes = match durable_route_bindings(
                    provider,
                    Arc::clone(&metadata),
                    &topology,
                    durable_reads.iter().cloned().collect(),
                ) {
                    Ok(routes) => routes,
                    Err(error) => {
                        runtime_cancellation.cancel();
                        let _cleanup = runtime.shutdown().await;
                        return Err(error);
                    }
                };
                if let Err(error) = self
                    .replace_durable_market_routes(provider, durable_routes, deadline, cancellation)
                    .await
                {
                    runtime_cancellation.cancel();
                    let _cleanup = runtime.shutdown().await;
                    return Err(error);
                }
                MarketRuntimeEntry {
                    surface_id: provider.clone(),
                    onboarding_session_id: Some(session_id),
                    metadata,
                    topology: Some(topology),
                    cancellation: runtime_cancellation,
                    runtime: MarketRuntime::CoinbaseDirect(runtime),
                    exports: None,
                    action_hooks_installed: false,
                }
            }
        };
        if let Err(error) = ensure_active(&self.accepting, deadline, cancellation) {
            let _cleanup = entry.shutdown(self.config.source_shutdown()).await;
            return Err(error);
        }
        if !entry.is_healthy() {
            entry.shutdown(self.config.source_shutdown()).await?;
            return Err(ServiceError::Unavailable);
        }
        {
            let mut entries = bounded_lock(&self.entries, deadline, cancellation).await?;
            if entries.len() == MAXIMUM_CONCURRENT_MARKET_SURFACES
                || entries
                    .iter()
                    .any(|current| current.surface_id == entry.surface_id)
            {
                drop(entries);
                entry.shutdown(self.config.source_shutdown()).await?;
                return Err(ServiceError::ResourceExhausted);
            }
            entries.push(entry);
        }
        let verification = self.verify_owned(provider).await;
        match verification {
            Ok(Some(evidence)) => Ok(evidence),
            Ok(None) | Err(ServiceError::Unavailable) => {
                let cleanup = CancellationToken::new();
                let entry = self
                    .take_entry(provider, self.cleanup_deadline()?, &cleanup)
                    .await?
                    .ok_or(ServiceError::Unavailable)?;
                entry.shutdown(self.config.source_shutdown()).await?;
                Err(ServiceError::Unavailable)
            }
            Err(error) => {
                let cleanup = CancellationToken::new();
                if let Some(entry) = self
                    .take_entry(provider, self.cleanup_deadline()?, &cleanup)
                    .await?
                {
                    entry.shutdown(self.config.source_shutdown()).await?;
                }
                Err(error)
            }
        }
    }

    pub(crate) async fn verify(
        &self,
        provider: &SourceIdentifier,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<Option<MarketSourceLifecycleEvidence>, ServiceError> {
        ensure_before(deadline, cancellation)?;
        let result = self.verify_owned(provider).await;
        ensure_before(deadline, cancellation)?;
        result
    }

    /// Verifies one exact account-backed group without treating its configuration digest as a
    /// connection generation.
    pub(crate) async fn verify_account_group(
        &self,
        request: PreparedMarketProviderConfigurationRequest,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<Option<MarketProviderGroupLifecycleEvidence>, ServiceError> {
        ensure_before(deadline, cancellation)?;
        self.verify_account_group_owned(request, deadline, cancellation)
            .await
    }

    /// Opens the one-way read gate for the exact registry generation after its durable source
    /// lifecycle record has committed Active.
    pub(crate) async fn admit_account_group_reads(
        &self,
        request: PreparedMarketProviderConfigurationRequest,
        expected_group_generation: MarketRuntimeGroupGeneration,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<(), ServiceError> {
        let _mutation = bounded_lock(&self.mutation, deadline, cancellation).await?;
        ensure_active(&self.accepting, deadline, cancellation)?;
        let admission_authority = self
            .account_market_mutation_authority_before(deadline, cancellation)
            .await?;
        let entries = bounded_lock(&self.entries, deadline, cancellation).await?;
        let surface_id = try_surface_identifier(request.surface())?;
        let entry = entries
            .iter()
            .find(|entry| entry.surface_id == surface_id)
            .ok_or(ServiceError::NotFound)?;
        if entry.onboarding_session_id != Some(request.onboarding_session_id()) {
            return Err(ServiceError::InvalidRequest);
        }
        let group = match &entry.runtime {
            MarketRuntime::Account(group) => group,
            MarketRuntime::Public(_) | MarketRuntime::CoinbaseDirect(_) => {
                return Err(ServiceError::InvalidRequest);
            }
        };
        validate_account_evidence(request, group.evidence())?;
        if group.evidence().group_generation() != expected_group_generation {
            return Err(ServiceError::InvalidRequest);
        }
        validate_account_lease(request, group.activation_lease())?;
        ensure_active(&self.accepting, deadline, cancellation)?;
        admission_authority
            .require_active(group.activation_lease())
            .map_err(|_error| ServiceError::Unauthorized)?;
        ensure_active(&self.accepting, deadline, cancellation)?;
        let durable_routes = match entry.topology.as_ref() {
            Some(topology) => durable_route_bindings(
                &surface_id,
                Arc::clone(&entry.metadata),
                topology,
                group.durable_reads(),
            )?,
            None if group.durable_reads().is_empty() => Vec::new(),
            None => return Err(ServiceError::InvalidResult),
        };
        let mut retained =
            bounded_lock(&self.durable_market_routes, deadline, cancellation).await?;
        let retained_other_count = retained
            .iter()
            .filter(|route| route.surface_id() != &surface_id)
            .count();
        let next_len = retained_other_count
            .checked_add(durable_routes.len())
            .ok_or(ServiceError::ResourceExhausted)?;
        if next_len > MAX_DISPLAY_MARKET_ROUTES {
            return Err(ServiceError::ResourceExhausted);
        }
        let additional_capacity = next_len.saturating_sub(retained.capacity());
        retained
            .try_reserve_exact(additional_capacity)
            .map_err(|_| ServiceError::ResourceExhausted)?;
        group.admit_reads()?;
        retained.retain(|route| route.surface_id() != &surface_id);
        retained.extend(durable_routes);
        Ok(())
    }

    async fn verify_account_group_owned(
        &self,
        request: PreparedMarketProviderConfigurationRequest,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<Option<MarketProviderGroupLifecycleEvidence>, ServiceError> {
        let entries = bounded_lock(&self.entries, deadline, cancellation).await?;
        let Some(entry) = entries
            .iter()
            .find(|entry| entry.surface_id.as_str() == request.surface().surface_id())
        else {
            return Ok(None);
        };
        if entry.onboarding_session_id != Some(request.onboarding_session_id()) {
            return Err(ServiceError::InvalidRequest);
        }
        let evidence = entry
            .runtime
            .account_evidence()
            .ok_or(ServiceError::InvalidRequest)?;
        validate_account_evidence(request, evidence)?;
        if !entry.is_published_healthy() {
            return Err(ServiceError::Unavailable);
        }
        Ok(Some(evidence.clone()))
    }

    /// Returns the historical subordinate for one exact active Alpaca account group.
    ///
    /// This lookup never chooses a provider, resolves setup, reopens account authority, or reads a
    /// credential. The returned capability is bound to the already active session, configuration,
    /// credential generation, and process-wide rate authority.
    pub(crate) async fn alpaca_historical_capability(
        &self,
        request: PreparedMarketProviderConfigurationRequest,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<AlpacaHistoricalRuntimeCapability, AlpacaHistoricalLookupError> {
        if request.surface() != AccountMarketSurface::AlpacaBasic {
            return Err(AlpacaHistoricalLookupError::NotConfigured);
        }
        ensure_alpaca_historical_lookup(&self.accepting, deadline, cancellation)?;
        let _mutation = bounded_lock(&self.mutation, deadline, cancellation)
            .await
            .map_err(|_error| AlpacaHistoricalLookupError::Transitioning)?;
        ensure_alpaca_historical_lookup(&self.accepting, deadline, cancellation)?;
        let surface_id = try_surface_identifier(AccountMarketSurface::AlpacaBasic)
            .map_err(|_error| AlpacaHistoricalLookupError::Transitioning)?;
        let capability = {
            let entries = bounded_lock(&self.entries, deadline, cancellation)
                .await
                .map_err(|_error| AlpacaHistoricalLookupError::Transitioning)?;
            let entry = entries
                .iter()
                .find(|entry| entry.surface_id == surface_id)
                .ok_or(AlpacaHistoricalLookupError::NotConfigured)?;
            validate_alpaca_historical_entry(entry, request)?;
            let MarketRuntime::Account(group) = &entry.runtime else {
                return Err(AlpacaHistoricalLookupError::Stale);
            };
            group
                .alpaca_historical_capability()
                .map_err(map_alpaca_historical_capability_error)?
                .ok_or(AlpacaHistoricalLookupError::Stale)?
        };
        capability
            .require_current(deadline, cancellation)
            .await
            .map_err(map_alpaca_historical_capability_error)?;
        ensure_alpaca_historical_lookup(&self.accepting, deadline, cancellation)?;
        {
            let entries = bounded_lock(&self.entries, deadline, cancellation)
                .await
                .map_err(|_error| AlpacaHistoricalLookupError::Transitioning)?;
            let entry = entries
                .iter()
                .find(|entry| entry.surface_id == surface_id)
                .ok_or(AlpacaHistoricalLookupError::Transitioning)?;
            validate_alpaca_historical_entry(entry, request)?;
            let MarketRuntime::Account(group) = &entry.runtime else {
                return Err(AlpacaHistoricalLookupError::Stale);
            };
            if !group.owns_alpaca_historical_capability(&capability) || capability.is_revoked() {
                return Err(AlpacaHistoricalLookupError::Transitioning);
            }
        }
        Ok(capability)
    }

    /// Lazily installs/reuses the one sealed Alpaca-history source and admits one exact plan.
    ///
    /// This crate-private path accepts only canonical application authority. It exposes neither
    /// provider selection nor credentials, endpoint, feed, dataset, manifest, or transport
    /// coordinates and is intentionally not routed to a public service operation in this wave.
    pub(crate) async fn admit_alpaca_historical_plan(
        &self,
        request: PreparedMarketProviderConfigurationRequest,
        preflight_plan: AlpacaHistoricalEquityPreflightPlan,
        canonical_instrument: MarketDataInstrumentDefinition,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<AlpacaHistoricalPlanReceipt, AlpacaHistoricalPlanAdmissionError> {
        if request.surface() != AccountMarketSurface::AlpacaBasic {
            return Err(AlpacaHistoricalPlanAdmissionError::RuntimeUnavailable);
        }
        ensure_alpaca_historical_lookup(&self.accepting, deadline, cancellation)
            .map_err(|_error| AlpacaHistoricalPlanAdmissionError::RuntimeUnavailable)?;
        let (surface_id, capability) = {
            let _mutation = bounded_lock(&self.mutation, deadline, cancellation)
                .await
                .map_err(|_error| AlpacaHistoricalPlanAdmissionError::RuntimeUnavailable)?;
            ensure_alpaca_historical_lookup(&self.accepting, deadline, cancellation)
                .map_err(|_error| AlpacaHistoricalPlanAdmissionError::RuntimeUnavailable)?;
            let surface_id = try_surface_identifier(AccountMarketSurface::AlpacaBasic)
                .map_err(|_error| AlpacaHistoricalPlanAdmissionError::RuntimeUnavailable)?;
            let entries = bounded_lock(&self.entries, deadline, cancellation)
                .await
                .map_err(|_error| AlpacaHistoricalPlanAdmissionError::RuntimeUnavailable)?;
            let entry = entries
                .iter()
                .find(|entry| entry.surface_id == surface_id)
                .ok_or(AlpacaHistoricalPlanAdmissionError::RuntimeUnavailable)?;
            validate_alpaca_historical_entry(entry, request)
                .map_err(|_error| AlpacaHistoricalPlanAdmissionError::RuntimeUnavailable)?;
            let MarketRuntime::Account(group) = &entry.runtime else {
                return Err(AlpacaHistoricalPlanAdmissionError::RuntimeUnavailable);
            };
            let capability = group
                .alpaca_historical_capability()
                .map_err(|_error| AlpacaHistoricalPlanAdmissionError::RuntimeUnavailable)?
                .ok_or(AlpacaHistoricalPlanAdmissionError::RuntimeUnavailable)?;
            (surface_id, capability)
        };
        capability
            .require_current(deadline, cancellation)
            .await
            .map_err(|_error| AlpacaHistoricalPlanAdmissionError::RuntimeUnavailable)?;
        let lease = self
            .alpaca_historical_source
            .install_or_join_runtime(capability.clone(), deadline, cancellation)
            .await
            .map_err(|_error| AlpacaHistoricalPlanAdmissionError::RuntimeUnavailable)?;
        let receipt = lease
            .admit_plan(preflight_plan, canonical_instrument, deadline, cancellation)
            .await?;
        drop(lease);
        let mutation = bounded_lock(&self.mutation, deadline, cancellation)
            .await
            .map_err(|_error| AlpacaHistoricalPlanAdmissionError::RuntimeUnavailable)?;
        ensure_alpaca_historical_lookup(&self.accepting, deadline, cancellation)
            .map_err(|_error| AlpacaHistoricalPlanAdmissionError::RuntimeUnavailable)?;
        {
            let entries = bounded_lock(&self.entries, deadline, cancellation)
                .await
                .map_err(|_error| AlpacaHistoricalPlanAdmissionError::RuntimeUnavailable)?;
            let entry = entries
                .iter()
                .find(|entry| entry.surface_id == surface_id)
                .ok_or(AlpacaHistoricalPlanAdmissionError::RuntimeUnavailable)?;
            validate_alpaca_historical_entry(entry, request)
                .map_err(|_error| AlpacaHistoricalPlanAdmissionError::RuntimeUnavailable)?;
            let MarketRuntime::Account(group) = &entry.runtime else {
                return Err(AlpacaHistoricalPlanAdmissionError::RuntimeUnavailable);
            };
            if !group.owns_alpaca_historical_capability(&capability)
                || !receipt.matches_group_generation(group.evidence().group_generation())
                || capability.is_revoked()
                || capability.validate_current_now().is_err()
            {
                return Err(AlpacaHistoricalPlanAdmissionError::RuntimeUnavailable);
            }
        }
        let validated = self
            .alpaca_historical_source
            .validate_plan_receipt(&receipt, deadline, cancellation)
            .map_err(|_error| AlpacaHistoricalPlanAdmissionError::RuntimeUnavailable)?;
        let authorized = validated
            .authorize(deadline, cancellation)
            .await
            .map_err(|_error| AlpacaHistoricalPlanAdmissionError::RuntimeUnavailable)?;
        ensure_alpaca_historical_lookup(&self.accepting, deadline, cancellation)
            .map_err(|_error| AlpacaHistoricalPlanAdmissionError::RuntimeUnavailable)?;
        if !receipt.matches_group_generation(capability.group_generation()) {
            return Err(AlpacaHistoricalPlanAdmissionError::RuntimeUnavailable);
        }
        capability
            .validate_current_now()
            .map_err(|_error| AlpacaHistoricalPlanAdmissionError::RuntimeUnavailable)?;
        authorized
            .validate_current()
            .map_err(|_error| AlpacaHistoricalPlanAdmissionError::RuntimeUnavailable)?;
        drop(mutation);
        drop(authorized);
        Ok(receipt)
    }

    /// Converts one retained receipt into bounded current plan authority for a later consumer.
    ///
    /// Wave-C orchestration can retain the opaque receipt and call this method immediately before
    /// discovery/materialization. The returned non-cloneable view owns the specialized source's
    /// publication barrier and exposes only the fixed admitted plan coordinates.
    pub(crate) async fn authorize_alpaca_historical_plan_receipt<'receipt>(
        &self,
        receipt: &'receipt AlpacaHistoricalPlanReceipt,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<AlpacaHistoricalAuthorizedPlan<'receipt>, AlpacaHistoricalPlanAdmissionError> {
        let mutation = bounded_lock(&self.mutation, deadline, cancellation)
            .await
            .map_err(|_error| AlpacaHistoricalPlanAdmissionError::RuntimeUnavailable)?;
        ensure_alpaca_historical_lookup(&self.accepting, deadline, cancellation)
            .map_err(|_error| AlpacaHistoricalPlanAdmissionError::RuntimeUnavailable)?;
        let surface_id = try_surface_identifier(AccountMarketSurface::AlpacaBasic)
            .map_err(|_error| AlpacaHistoricalPlanAdmissionError::RuntimeUnavailable)?;
        let capability = {
            let entries = bounded_lock(&self.entries, deadline, cancellation)
                .await
                .map_err(|_error| AlpacaHistoricalPlanAdmissionError::RuntimeUnavailable)?;
            let entry = entries
                .iter()
                .find(|entry| entry.surface_id == surface_id)
                .ok_or(AlpacaHistoricalPlanAdmissionError::RuntimeUnavailable)?;
            if !entry.is_published_healthy() {
                return Err(AlpacaHistoricalPlanAdmissionError::RuntimeUnavailable);
            }
            let MarketRuntime::Account(group) = &entry.runtime else {
                return Err(AlpacaHistoricalPlanAdmissionError::RuntimeUnavailable);
            };
            let capability = group
                .alpaca_historical_capability()
                .map_err(|_error| AlpacaHistoricalPlanAdmissionError::RuntimeUnavailable)?
                .ok_or(AlpacaHistoricalPlanAdmissionError::RuntimeUnavailable)?;
            if !group.owns_alpaca_historical_capability(&capability)
                || !receipt.matches_group_generation(group.evidence().group_generation())
                || capability.is_revoked()
                || capability.validate_current_now().is_err()
            {
                return Err(AlpacaHistoricalPlanAdmissionError::RuntimeUnavailable);
            }
            capability
        };
        let validated = self
            .alpaca_historical_source
            .validate_plan_receipt(receipt, deadline, cancellation)
            .map_err(|_error| AlpacaHistoricalPlanAdmissionError::RuntimeUnavailable)?;
        let authorized = validated
            .authorize(deadline, cancellation)
            .await
            .map_err(|_error| AlpacaHistoricalPlanAdmissionError::RuntimeUnavailable)?;
        ensure_alpaca_historical_lookup(&self.accepting, deadline, cancellation)
            .map_err(|_error| AlpacaHistoricalPlanAdmissionError::RuntimeUnavailable)?;
        if !receipt.matches_group_generation(capability.group_generation()) {
            return Err(AlpacaHistoricalPlanAdmissionError::RuntimeUnavailable);
        }
        capability
            .validate_current_now()
            .map_err(|_error| AlpacaHistoricalPlanAdmissionError::RuntimeUnavailable)?;
        authorized
            .validate_current()
            .map_err(|_error| AlpacaHistoricalPlanAdmissionError::RuntimeUnavailable)?;
        drop(mutation);
        Ok(authorized)
    }

    async fn verify_owned(
        &self,
        provider: &SourceIdentifier,
    ) -> Result<Option<MarketSourceLifecycleEvidence>, ServiceError> {
        let entries = self.entries.lock().await;
        let Some(entry) = entries.iter().find(|entry| &entry.surface_id == provider) else {
            return Ok(None);
        };
        if !entry.is_healthy() {
            return Err(ServiceError::Unavailable);
        }
        let evidence = aggregate(
            provider.clone(),
            entry.topology()?,
            entry.scalar_snapshots()?,
        )?;
        if !entry.is_healthy() {
            return Err(ServiceError::Unavailable);
        }
        Ok(evidence)
    }

    /// Stops one exact account group with an optional group-digest compare-and-set guard.
    pub(crate) async fn stop_account_group(
        &self,
        request: PreparedMarketProviderConfigurationRequest,
        expected_group_generation: Option<MarketRuntimeGroupGeneration>,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<Option<MarketRuntimeGroupGeneration>, ServiceError> {
        let _mutation = bounded_lock(&self.mutation, deadline, cancellation).await?;
        let entry = self
            .take_account_entry_for_request(
                request,
                expected_group_generation,
                deadline,
                cancellation,
            )
            .await?;
        let Some((entry, group_generation)) = entry else {
            return Ok(None);
        };
        self.clear_durable_market_routes(&entry.surface_id, deadline, cancellation)
            .await?;
        entry.shutdown(self.config.source_shutdown()).await?;
        Ok(Some(group_generation))
    }

    /// Removes an account group even when its onboarding lease has expired, while refusing to
    /// reinterpret a scalar public/direct runtime as an account group.
    pub(crate) async fn remove_account_group(
        &self,
        surface: AccountMarketSurface,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<Option<MarketRuntimeGroupGeneration>, ServiceError> {
        let _mutation = bounded_lock(&self.mutation, deadline, cancellation).await?;
        let surface_id = try_surface_identifier(surface)?;
        let entry = {
            let mut entries = bounded_lock(&self.entries, deadline, cancellation).await?;
            let Some(index) = entries
                .iter()
                .position(|entry| entry.surface_id == surface_id)
            else {
                return Ok(None);
            };
            let evidence = entries[index]
                .runtime
                .account_evidence()
                .ok_or(ServiceError::InvalidRequest)?;
            if evidence.surface_id().as_str() != surface.surface_id() {
                return Err(ServiceError::InvalidRequest);
            }
            let generation = evidence.group_generation();
            (entries.swap_remove(index), generation)
        };
        let (entry, generation) = entry;
        self.clear_durable_market_routes(&entry.surface_id, deadline, cancellation)
            .await?;
        entry.shutdown(self.config.source_shutdown()).await?;
        Ok(Some(generation))
    }

    /// Drains the exact Schwab account runtime before OAuth revocation or credential replacement.
    ///
    /// This path deliberately tolerates an absent runtime: OAuth can be linked before a market
    /// group is started, and shutdown can race a group that has already been removed. A runtime
    /// owned by another onboarding session is never removed.
    pub(crate) async fn drain_schwab_oauth_runtime(
        &self,
        session_id: uuid::Uuid,
        current: Option<SchwabOAuthAuthorityReceipt>,
        cancellation: &CancellationToken,
    ) -> Result<(), ServiceError> {
        if session_id.is_nil() {
            return Err(ServiceError::InvalidRequest);
        }
        let deadline = Instant::now()
            .checked_add(self.config.source_shutdown())
            .ok_or(ServiceError::Unavailable)?;
        let _mutation = bounded_lock(&self.mutation, deadline, cancellation).await?;
        let entry = {
            let mut entries = bounded_lock(&self.entries, deadline, cancellation).await?;
            let Some(index) = entries.iter().position(|entry| {
                entry.surface_id.as_str()
                    == crate::provider_onboarding::SCHWAB_MARKET_DATA_SURFACE_ID
            }) else {
                return Ok(());
            };
            if entries[index].onboarding_session_id != Some(session_id) {
                return Err(ServiceError::InvalidRequest);
            }
            let group = entries[index]
                .runtime
                .account_evidence()
                .ok_or(ServiceError::InvalidRequest)?;
            let receipt = entries[index]
                .runtime
                .account_activation_lease()
                .and_then(|lease| {
                    lease
                        .runtime_verification_evidence()
                        .schwab_market_data_receipt()
                })
                .ok_or(ServiceError::InvalidRequest)?;
            if group.onboarding_session_id() != session_id
                || uuid::Uuid::parse_str(receipt.session_identifier().as_str()) != Ok(session_id)
                || current.is_some_and(|current| {
                    current.generation().get() < receipt.access_token_generation()
                })
            {
                return Err(ServiceError::InvalidRequest);
            }
            entries.swap_remove(index)
        };
        self.clear_durable_market_routes(&entry.surface_id, deadline, cancellation)
            .await?;
        entry.shutdown(self.config.source_shutdown()).await
    }

    pub(crate) async fn stop(
        &self,
        provider: &SourceIdentifier,
        expected_generation: Option<MarketSourceRuntimeGeneration>,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<Option<MarketSourceRuntimeGeneration>, ServiceError> {
        let _mutation = bounded_lock(&self.mutation, deadline, cancellation).await?;
        self.stop_owned(provider, expected_generation, deadline, cancellation)
            .await
    }

    async fn stop_owned(
        &self,
        provider: &SourceIdentifier,
        expected_generation: Option<MarketSourceRuntimeGeneration>,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<Option<MarketSourceRuntimeGeneration>, ServiceError> {
        ensure_before(deadline, cancellation)?;
        let previous = self
            .verify_owned(provider)
            .await?
            .map(|value| value.generation);
        if expected_generation.is_some() && previous != expected_generation {
            return Err(ServiceError::InvalidRequest);
        }
        {
            let entries = bounded_lock(&self.entries, deadline, cancellation).await?;
            if entries
                .iter()
                .any(|entry| &entry.surface_id == provider && entry.action_hooks_installed)
            {
                return Err(ServiceError::InvalidRequest);
            }
        }
        let entry = self.take_entry(provider, deadline, cancellation).await?;
        if let Some(entry) = entry {
            entry.shutdown(self.config.source_shutdown()).await?;
        }
        Ok(previous)
    }

    pub(crate) async fn resynchronize(
        &self,
        provider: &SourceIdentifier,
        expected_generation: MarketSourceRuntimeGeneration,
        onboarding_session_id: Option<uuid::Uuid>,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<(MarketSourceRuntimeGeneration, MarketSourceLifecycleEvidence), ServiceError> {
        let _mutation = bounded_lock(&self.mutation, deadline, cancellation).await?;
        let previous = self
            .stop_owned(provider, Some(expected_generation), deadline, cancellation)
            .await?
            .ok_or(ServiceError::Unavailable)?;
        let current = self
            .start_owned(provider, onboarding_session_id, deadline, cancellation)
            .await?;
        let replaced = match (previous, current.generation) {
            (
                MarketSourceRuntimeGeneration::Scalar(previous),
                MarketSourceRuntimeGeneration::Scalar(current),
            ) => current.get() > previous.get(),
            (
                MarketSourceRuntimeGeneration::Group(previous),
                MarketSourceRuntimeGeneration::Group(current),
            ) => current != previous,
            _ => false,
        };
        if !replaced {
            let cleanup = CancellationToken::new();
            let _stopped = self
                .stop_owned(
                    provider,
                    Some(current.generation),
                    self.cleanup_deadline()?,
                    &cleanup,
                )
                .await?;
            return Err(ServiceError::Unavailable);
        }
        Ok((previous, current))
    }

    pub(crate) async fn remove(
        &self,
        provider: &SourceIdentifier,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<Option<MarketSourceRuntimeGeneration>, ServiceError> {
        self.stop(provider, None, deadline, cancellation).await
    }

    pub(crate) async fn snapshots(
        &self,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<MarketRuntimeSnapshotBatch, ServiceError> {
        ensure_before(deadline, cancellation)?;
        let readers = {
            let entries = bounded_lock(&self.entries, deadline, cancellation).await?;
            let healthy = entries
                .iter()
                .filter(|entry| entry.is_healthy() && entry.runtime.has_scalar_snapshots())
                .count();
            let mut readers = Vec::new();
            readers
                .try_reserve_exact(healthy)
                .map_err(|_error| ServiceError::ResourceExhausted)?;
            for entry in entries
                .iter()
                .filter(|entry| entry.is_healthy() && entry.runtime.has_scalar_snapshots())
            {
                readers.push((
                    entry.surface_id.clone(),
                    Arc::clone(&entry.metadata),
                    entry.scalar_snapshots()?,
                ));
            }
            readers
        };
        let mut sources = Vec::new();
        sources
            .try_reserve_exact(readers.len())
            .map_err(|_error| ServiceError::ResourceExhausted)?;
        let mut failures = Vec::new();
        failures
            .try_reserve_exact(readers.len())
            .map_err(|_error| ServiceError::ResourceExhausted)?;
        for (surface_id, metadata, reader) in readers {
            ensure_before(deadline, cancellation)?;
            match reader.try_load_all() {
                Ok(lease) => sources.push(MarketSourceSnapshotLease {
                    surface_id,
                    metadata,
                    lease,
                }),
                Err(error) => failures.push(MarketSourceSnapshotFailure {
                    surface_id,
                    kind: map_snapshot_failure(error),
                }),
            }
        }
        Ok(MarketRuntimeSnapshotBatch { sources, failures })
    }

    /// Joins each bounded source-bound durable reader to its validated runtime routes.
    ///
    /// The returned coordinates remain internal to neutral product selection. The immutable
    /// topology, rather than a hot snapshot, is the route authority after process restart.
    pub(crate) async fn market_event_durable_route_reads(
        &self,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<Vec<MarketEventDurableRouteRead>, ServiceError> {
        ensure_active(&self.accepting, deadline, cancellation)?;
        let bindings = {
            let retained =
                bounded_lock(&self.durable_market_routes, deadline, cancellation).await?;
            let mut bindings = Vec::new();
            bindings
                .try_reserve_exact(retained.len())
                .map_err(|_error| ServiceError::ResourceExhausted)?;
            bindings.extend(retained.iter().cloned());
            bindings
        };
        ensure_active(&self.accepting, deadline, cancellation)?;
        Ok(bindings)
    }

    async fn replace_durable_market_routes(
        &self,
        surface_id: &SourceIdentifier,
        routes: Vec<MarketEventDurableRouteRead>,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<(), ServiceError> {
        if routes.iter().any(|route| route.surface_id() != surface_id) {
            return Err(ServiceError::InvalidResult);
        }
        let mut retained =
            bounded_lock(&self.durable_market_routes, deadline, cancellation).await?;
        let retained_other_count = retained
            .iter()
            .filter(|route| route.surface_id() != surface_id)
            .count();
        let next_len = retained_other_count
            .checked_add(routes.len())
            .ok_or(ServiceError::ResourceExhausted)?;
        if next_len > MAX_DISPLAY_MARKET_ROUTES {
            return Err(ServiceError::ResourceExhausted);
        }
        let additional_capacity = next_len.saturating_sub(retained.capacity());
        retained
            .try_reserve_exact(additional_capacity)
            .map_err(|_error| ServiceError::ResourceExhausted)?;
        retained.retain(|route| route.surface_id() != surface_id);
        retained.extend(routes);
        Ok(())
    }

    async fn clear_durable_market_routes(
        &self,
        surface_id: &SourceIdentifier,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<(), ServiceError> {
        let mut retained =
            bounded_lock(&self.durable_market_routes, deadline, cancellation).await?;
        retained.retain(|route| route.surface_id() != surface_id);
        Ok(())
    }

    /// Selects the strongest healthy scalar market runtime for virtual paper execution.
    ///
    /// Ranking uses declared observation quality, real-time delivery, represented market depth,
    /// and metadata coverage. Provider identity is used only as a deterministic final tie-breaker
    /// and never crosses the application product boundary.
    pub(crate) async fn select_paper_market_surface(
        &self,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<PaperMarketSurfaceSelection, ServiceError> {
        ensure_active(&self.accepting, deadline, cancellation)?;
        let entries = bounded_lock(&self.entries, deadline, cancellation).await?;
        let mut selected: Option<(&MarketRuntimeEntry, (u8, u8, u8, usize))> = None;
        for entry in entries.iter().filter(|entry| {
            entry.is_published_healthy()
                && entry.runtime.has_scalar_snapshots()
                && !entry.action_hooks_installed
                && matches!(
                    entry.surface_id.as_str(),
                    COINBASE_PUBLIC_SURFACE_ID
                        | COINBASE_DIRECT_SURFACE_ID
                        | KRAKEN_PUBLIC_SURFACE_ID
                )
        }) {
            let rank = paper_market_surface_rank(entry.metadata.as_ref())?;
            let replace = selected.as_ref().is_none_or(|(current, current_rank)| {
                rank > *current_rank
                    || (rank == *current_rank
                        && entry.surface_id.as_str() < current.surface_id.as_str())
            });
            if replace {
                selected = Some((entry, rank));
            }
        }
        let (entry, _rank) = selected.ok_or(ServiceError::Unavailable)?;
        Ok(PaperMarketSurfaceSelection {
            surface_id: entry.surface_id.clone(),
            onboarding_session_id: entry.onboarding_session_id,
        })
    }

    /// Reads every account-backed display source for one instrument in exact actor-key order.
    ///
    /// The complete result is bounded by `maximum_sources`. Each snapshot is joined only to the
    /// exact prepared source metadata and provider symbol whose revision matches the retained
    /// observation; a concurrent replacement or ambiguous source identity fails the whole read.
    pub(crate) async fn display_snapshots_for_instrument(
        &self,
        instrument_id: InstrumentId,
        maximum_sources: NonZeroUsize,
        at: Timestamp,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<MarketDisplaySnapshotBatch, ServiceError> {
        ensure_active(&self.accepting, deadline, cancellation)?;
        let descriptors = {
            let entries = bounded_lock(&self.entries, deadline, cancellation).await?;
            let descriptor_capacity = entries
                .iter()
                .filter(|entry| entry.is_published_healthy())
                .try_fold(0_usize, |count, entry| {
                    count.checked_add(entry.runtime.display_descriptor_count())
                })
                .ok_or(ServiceError::ResourceExhausted)?;
            let mut descriptors = Vec::new();
            descriptors
                .try_reserve_exact(descriptor_capacity)
                .map_err(|_error| ServiceError::ResourceExhausted)?;
            for entry in entries.iter().filter(|entry| entry.is_published_healthy()) {
                entry.runtime.append_display_descriptors(&mut descriptors);
            }
            if descriptors.len() != descriptor_capacity {
                return Err(ServiceError::Unavailable);
            }
            descriptors.retain(|descriptor| descriptor.supports_instrument(instrument_id));
            if descriptors.is_empty() {
                return Err(ServiceError::Unavailable);
            }
            descriptors
        };
        let leases = self
            .display
            .snapshots_for_instrument(instrument_id, maximum_sources, at, cancellation, deadline)
            .await
            .map_err(map_display_read_error)?;
        if leases.len() != descriptors.len() {
            return Err(ServiceError::Unavailable);
        }
        let mut unmatched = descriptors.into_iter().map(Some).collect::<Vec<_>>();
        let mut snapshots = Vec::new();
        snapshots
            .try_reserve_exact(leases.len())
            .map_err(|_error| ServiceError::ResourceExhausted)?;
        for lease in leases {
            ensure_active(&self.accepting, deadline, cancellation)?;
            let mut matched = unmatched
                .iter()
                .enumerate()
                .filter_map(|(index, descriptor)| {
                    descriptor
                        .as_ref()
                        .filter(|descriptor| descriptor.matches_snapshot(&lease))
                        .map(|_descriptor| index)
                });
            let index = matched.next().ok_or(ServiceError::Unavailable)?;
            if matched.next().is_some() {
                return Err(ServiceError::Unavailable);
            }
            let descriptor = unmatched[index].take().ok_or(ServiceError::Unavailable)?;
            snapshots.push(MarketDisplaySnapshotLease::try_new(descriptor, lease)?);
        }
        if unmatched.iter().any(Option::is_some) {
            return Err(ServiceError::Unavailable);
        }
        {
            let entries = bounded_lock(&self.entries, deadline, cancellation).await?;
            if snapshots.iter().any(|snapshot| {
                !entries.iter().any(|entry| {
                    entry.is_published_healthy()
                        && entry.runtime.owns_display_descriptor(snapshot.descriptor())
                })
            }) {
                return Err(ServiceError::Unavailable);
            }
        }
        Ok(MarketDisplaySnapshotBatch::new(snapshots))
    }

    /// Returns the complete sorted set of instruments retained by healthy display groups.
    ///
    /// The result is complete-or-error: exceeding `maximum_instruments` never truncates or
    /// silently drops a configured source binding.
    pub(crate) async fn display_instrument_ids(
        &self,
        maximum_instruments: NonZeroUsize,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<Vec<InstrumentId>, ServiceError> {
        self.instrument_ids(false, maximum_instruments, deadline, cancellation)
            .await
    }

    /// Returns the complete sorted set retained by healthy display and Kraken L3 groups.
    pub(crate) async fn market_instrument_ids(
        &self,
        maximum_instruments: NonZeroUsize,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<Vec<InstrumentId>, ServiceError> {
        self.instrument_ids(true, maximum_instruments, deadline, cancellation)
            .await
    }

    async fn instrument_ids(
        &self,
        include_kraken_level3: bool,
        maximum_instruments: NonZeroUsize,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<Vec<InstrumentId>, ServiceError> {
        ensure_active(&self.accepting, deadline, cancellation)?;
        let mut instrument_ids = {
            let entries = bounded_lock(&self.entries, deadline, cancellation).await?;
            let instrument_count = entries
                .iter()
                .filter(|entry| entry.is_published_healthy())
                .try_fold(0_usize, |count, entry| {
                    count.checked_add(if include_kraken_level3 {
                        entry.runtime.market_instrument_count()?
                    } else {
                        entry.runtime.display_instrument_count()?
                    })
                })
                .ok_or(ServiceError::ResourceExhausted)?;
            let mut instrument_ids = Vec::new();
            instrument_ids
                .try_reserve_exact(instrument_count)
                .map_err(|_error| ServiceError::ResourceExhausted)?;
            for entry in entries.iter().filter(|entry| entry.is_published_healthy()) {
                if include_kraken_level3 {
                    entry
                        .runtime
                        .append_market_instrument_ids(&mut instrument_ids);
                } else {
                    entry
                        .runtime
                        .append_display_instrument_ids(&mut instrument_ids);
                }
            }
            if instrument_ids.len() != instrument_count {
                return Err(ServiceError::Unavailable);
            }
            instrument_ids
        };
        instrument_ids.sort_unstable();
        instrument_ids.dedup();
        if instrument_ids.len() > maximum_instruments.get() {
            return Err(ServiceError::ResourceExhausted);
        }
        ensure_active(&self.accepting, deadline, cancellation)?;
        Ok(instrument_ids)
    }

    /// Reads one exact authenticated Kraken order-level generation as a bounded aggregate.
    pub(crate) async fn kraken_price_projection(
        &self,
        instrument_id: InstrumentId,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<Option<MarketKrakenPriceProjectionLease>, ServiceError> {
        ensure_active(&self.accepting, deadline, cancellation)?;
        let authority = {
            let entries = bounded_lock(&self.entries, deadline, cancellation).await?;
            let mut authority = None;
            for entry in entries.iter().filter(|entry| entry.is_healthy()) {
                let Some(candidate) = entry.runtime.kraken_read_authority(instrument_id) else {
                    continue;
                };
                if authority.replace(candidate).is_some() {
                    return Err(ServiceError::Unavailable);
                }
            }
            authority
        };
        let Some((descriptor, key)) = authority else {
            return Ok(None);
        };
        let read = self
            .order_level
            .read_price_projection(&key, cancellation, deadline)
            .await
            .map_err(map_order_level_read_error)?;
        let snapshot = MarketKrakenPriceProjectionLease::try_new(descriptor, key, read)?;
        {
            let entries = bounded_lock(&self.entries, deadline, cancellation).await?;
            let current = entries.iter().find_map(|entry| {
                entry
                    .is_healthy()
                    .then(|| entry.runtime.kraken_read_authority(instrument_id))
                    .flatten()
                    .filter(|(descriptor, key)| {
                        entry.runtime.owns_kraken_descriptor(snapshot.descriptor())
                            && Arc::ptr_eq(descriptor, snapshot.descriptor())
                            && key == snapshot.key()
                    })
            });
            if current.is_none() {
                return Err(ServiceError::Unavailable);
            }
        }
        Ok(Some(snapshot))
    }

    /// Reads a bounded individual-order sample for one exact non-account scalar runtime.
    ///
    /// Account-backed order-level groups are deliberately excluded from this raw-key path. Their
    /// reads require the non-forgeable projection lease returned by [`Self::kraken_price_projection`].
    pub(crate) async fn scalar_order_level_snapshot(
        &self,
        surface_id: &SourceIdentifier,
        source_id: &SourceId,
        venue_id: &VenueId,
        instrument_id: InstrumentId,
        generation: ConnectionGeneration,
        maximum_orders: NonZeroUsize,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<Option<MarketOrderLevelSnapshot>, ServiceError> {
        ensure_active(&self.accepting, deadline, cancellation)?;
        {
            let entries = bounded_lock(&self.entries, deadline, cancellation).await?;
            require_scalar_order_read_authority(&entries, surface_id, source_id)?;
        }
        let key =
            OrderLevelBookKey::try_from_snapshot(source_id, venue_id, instrument_id, generation)
                .map_err(map_order_level_key_error)?;
        let Some(orders) = self
            .read_order_level_orders(&key, maximum_orders, deadline, cancellation)
            .await?
        else {
            return Ok(None);
        };
        ensure_active(&self.accepting, deadline, cancellation)?;
        {
            let entries = bounded_lock(&self.entries, deadline, cancellation).await?;
            require_scalar_order_read_authority(&entries, surface_id, source_id)?;
        }
        Ok(Some(MarketOrderLevelSnapshot { key, orders }))
    }

    /// Reads a bounded individual-order sample from one exact admitted Kraken account group.
    ///
    /// The projection lease is minted only after the registry validates the group, read gate,
    /// descriptor, and current actor key. Those same coordinates are revalidated both before and
    /// after the awaited actor read so a stopped, replaced, or revoked group cannot publish the
    /// retained result.
    pub(crate) async fn kraken_order_level_snapshot(
        &self,
        projection: &MarketKrakenPriceProjectionLease,
        maximum_orders: NonZeroUsize,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<Option<MarketOrderLevelSnapshot>, ServiceError> {
        ensure_active(&self.accepting, deadline, cancellation)?;
        {
            let entries = bounded_lock(&self.entries, deadline, cancellation).await?;
            require_kraken_order_read_authority(&entries, projection)?;
        }
        let key = projection.key().clone();
        let Some(orders) = self
            .read_order_level_orders(&key, maximum_orders, deadline, cancellation)
            .await?
        else {
            return Ok(None);
        };
        ensure_active(&self.accepting, deadline, cancellation)?;
        {
            let entries = bounded_lock(&self.entries, deadline, cancellation).await?;
            require_kraken_order_read_authority(&entries, projection)?;
        }
        Ok(Some(MarketOrderLevelSnapshot { key, orders }))
    }

    async fn read_order_level_orders(
        &self,
        key: &OrderLevelBookKey,
        maximum_orders: NonZeroUsize,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<Option<OrderLevelOrdersRead>, ServiceError> {
        match self
            .order_level
            .read_orders(key, maximum_orders, cancellation, deadline)
            .await
        {
            Ok(orders) => Ok(Some(orders)),
            Err(
                OrderLevelReadError::Unavailable
                | OrderLevelReadError::NotRegistered
                | OrderLevelReadError::Unregistering
                | OrderLevelReadError::WorkerClosed,
            ) => Ok(None),
            Err(OrderLevelReadError::Cancelled) => Err(ServiceError::Cancelled),
            Err(OrderLevelReadError::Deadline) => Err(ServiceError::DeadlineExceeded),
            Err(error) => {
                tracing::error!(%error, "bounded order-level market read failed");
                Err(ServiceError::Unavailable)
            }
        }
    }

    /// Returns immutable snapshot access for one exact active source/session.
    pub(crate) async fn snapshot_reader(
        &self,
        provider: &SourceIdentifier,
        onboarding_session_id: Option<uuid::Uuid>,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<LiveSnapshotReader, ServiceError> {
        let entries = bounded_lock(&self.entries, deadline, cancellation).await?;
        let entry = entries
            .iter()
            .find(|entry| &entry.surface_id == provider)
            .ok_or(ServiceError::Unavailable)?;
        if !entry.is_healthy() || entry.onboarding_session_id != onboarding_session_id {
            return Err(ServiceError::InvalidRequest);
        }
        entry.scalar_snapshots()
    }

    /// Installs one complete disabled paper-action group on an existing source runtime.
    pub(crate) async fn prepare_action_hooks(
        &self,
        provider: &SourceIdentifier,
        onboarding_session_id: Option<uuid::Uuid>,
        hooks: Vec<RouteActionHook>,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<PreparedMarketActionHooks, ServiceError> {
        let _mutation = bounded_lock(&self.mutation, deadline, cancellation).await?;
        let mut entries = bounded_lock(&self.entries, deadline, cancellation).await?;
        let entry = entries
            .iter_mut()
            .find(|entry| &entry.surface_id == provider)
            .ok_or(ServiceError::Unavailable)?;
        if !entry.is_healthy()
            || entry.onboarding_session_id != onboarding_session_id
            || entry.action_hooks_installed
        {
            return Err(ServiceError::InvalidRequest);
        }
        ensure_before(deadline, cancellation)?;
        let prepared = entry
            .runtime
            .prepare_action_hooks(hooks, cancellation.child_token())
            .await?;
        entry.action_hooks_installed = true;
        Ok(PreparedMarketActionHooks { prepared })
    }

    /// Reaps one exact disabled paper-action group while leaving the source connected.
    pub(crate) async fn reap_action_hooks(
        &self,
        provider: &SourceIdentifier,
        runtime_incarnation: NonZeroU64,
        generation: LiveActionHookGeneration,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<LiveActionHookReapReceipt, ServiceError> {
        let _mutation = bounded_lock(&self.mutation, deadline, cancellation).await?;
        let mut entries = bounded_lock(&self.entries, deadline, cancellation).await?;
        let entry = entries
            .iter_mut()
            .find(|entry| &entry.surface_id == provider)
            .ok_or(ServiceError::Unavailable)?;
        if !entry.action_hooks_installed {
            return Err(ServiceError::InvalidRequest);
        }
        ensure_before(deadline, cancellation)?;
        let receipt = entry
            .runtime
            .reap_action_hooks(cancellation.child_token())
            .await?;
        if receipt.runtime_incarnation() != runtime_incarnation
            || receipt.generation() != generation
        {
            entry.action_hooks_installed = false;
            return Err(ServiceError::Unavailable);
        }
        entry.action_hooks_installed = false;
        Ok(receipt)
    }

    pub(crate) fn begin_shutdown(&self) {
        self.account_health_cancellation.cancel();
        if self
            .accepting
            .swap(false, std::sync::atomic::Ordering::AcqRel)
        {
            self.prepared_configuration.begin_shutdown();
            self.prepared_schwab.begin_shutdown();
        }
    }

    pub(crate) async fn finish_shutdown(&self, deadline: Instant) -> Result<(), ServiceError> {
        self.begin_shutdown();
        let cleanup = CancellationToken::new();
        let mut shutdown = bounded_lock(&self.shutdown, deadline, &cleanup).await?;
        if let Some(result) = *shutdown {
            return result;
        }
        let _mutation = match bounded_lock(&self.mutation, deadline, &cleanup).await {
            Ok(mutation) => mutation,
            Err(error) => {
                self.account_health_cancellation.cancel();
                self.lifecycle.cancel();
                *shutdown = Some(Err(error));
                return Err(error);
            }
        };
        let mut failure = None;
        let account_health_drain =
            match bounded_lock(&self.account_health_drain, deadline, &cleanup).await {
                Ok(mut drain) => drain.take(),
                Err(error) => {
                    self.lifecycle.cancel();
                    *shutdown = Some(Err(error));
                    return Err(error);
                }
            };
        if let Some(mut drain) = account_health_drain {
            let result = tokio::select! {
                biased;
                result = &mut drain => result.map_err(|error| {
                    tracing::error!(%error, "account-market health drain join failed");
                    ServiceError::Unavailable
                }),
                () = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => {
                    drain.abort();
                    let _aborted = drain.await;
                    Err(ServiceError::DeadlineExceeded)
                }
            };
            if let Err(error) = result {
                tracing::error!(%error, "account-market health drain shutdown failed");
                failure = Some(error);
            }
        }
        let entries = {
            let mut entries = match bounded_lock(&self.entries, deadline, &cleanup).await {
                Ok(entries) => entries,
                Err(error) => {
                    self.account_health_cancellation.cancel();
                    self.lifecycle.cancel();
                    *shutdown = Some(Err(error));
                    return Err(error);
                }
            };
            std::mem::take(&mut *entries)
        };
        for entry in &entries {
            entry.begin_shutdown();
        }
        for entry in entries {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                failure.get_or_insert(ServiceError::DeadlineExceeded);
                continue;
            }
            if let Err(error) =
                await_service_before(deadline, &cleanup, entry.shutdown(remaining)).await
                && failure.is_none()
            {
                failure = Some(error);
            }
        }
        match bounded_lock(&self.durable_market_routes, deadline, &cleanup).await {
            Ok(mut routes) => routes.clear(),
            Err(error) => {
                failure.get_or_insert(error);
            }
        }

        self.lifecycle.cancel();
        let display_shutdown = self.display.shutdown(&cleanup, deadline);
        let order_level_shutdown = self.order_level.shutdown(&cleanup, deadline);
        let resolver_shutdown = await_service_before(
            deadline,
            &cleanup,
            self.prepared_configuration.finish_shutdown(deadline),
        );
        let schwab_resolver_shutdown = await_service_before(
            deadline,
            &cleanup,
            self.prepared_schwab.finish_shutdown(deadline),
        );
        let (display_result, order_level_result, resolver_result, schwab_resolver_result) = tokio::join!(
            display_shutdown,
            order_level_shutdown,
            resolver_shutdown,
            schwab_resolver_shutdown
        );
        match display_result {
            Ok(report) if report.is_complete() => {}
            Ok(_report) => {
                tracing::error!("display-market directory shutdown was incomplete");
                failure.get_or_insert(ServiceError::Unavailable);
            }
            Err(error) => {
                tracing::error!(%error, "display-market directory shutdown failed");
                failure.get_or_insert(map_display_directory_shutdown_error(error));
            }
        }
        match order_level_result {
            Ok(report) if report.is_complete() => {}
            Ok(report) => {
                tracing::error!(
                    graceful = report.graceful(),
                    aborted_at_deadline = report.aborted_at_deadline(),
                    aborted_on_cancellation = report.aborted_on_cancellation(),
                    failed = report.failed(),
                    "order-level market directory shutdown was incomplete"
                );
                failure.get_or_insert(ServiceError::Unavailable);
            }
            Err(error) => {
                tracing::error!(%error, "order-level market directory shutdown failed");
                failure.get_or_insert(map_order_level_directory_shutdown_error(error));
            }
        }
        if let Err(error) = resolver_result
            && failure.is_none()
        {
            failure = Some(error);
        }
        if let Err(error) = schwab_resolver_result
            && failure.is_none()
        {
            failure = Some(error);
        }
        if Instant::now() >= deadline {
            failure.get_or_insert(ServiceError::DeadlineExceeded);
        }
        let result = failure.map_or(Ok(()), Err);
        *shutdown = Some(result);
        result
    }

    async fn remove_unhealthy_owned(
        &self,
        provider: &SourceIdentifier,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<(), ServiceError> {
        let entry = {
            let mut entries = bounded_lock(&self.entries, deadline, cancellation).await?;
            let Some(index) = entries
                .iter()
                .position(|entry| &entry.surface_id == provider && !entry.is_healthy())
            else {
                return Ok(());
            };
            Some(entries.swap_remove(index))
        };
        if let Some(entry) = entry {
            entry.shutdown(self.config.source_shutdown()).await?;
        }
        Ok(())
    }

    async fn remove_unhealthy_account_group_owned(
        &self,
        surface_id: &SourceIdentifier,
        request: PreparedMarketProviderConfigurationRequest,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<(), ServiceError> {
        let entry = {
            let mut entries = bounded_lock(&self.entries, deadline, cancellation).await?;
            let Some(index) = entries
                .iter()
                .position(|entry| &entry.surface_id == surface_id)
            else {
                return Ok(());
            };
            let evidence = entries[index]
                .runtime
                .account_evidence()
                .ok_or(ServiceError::InvalidRequest)?;
            validate_account_evidence(request, evidence)?;
            if entries[index].is_healthy() {
                return Err(ServiceError::Unavailable);
            }
            entries.swap_remove(index)
        };
        self.clear_durable_market_routes(&entry.surface_id, deadline, cancellation)
            .await?;
        entry.shutdown(self.config.source_shutdown()).await
    }

    async fn take_account_entry_for_request(
        &self,
        request: PreparedMarketProviderConfigurationRequest,
        expected_group_generation: Option<MarketRuntimeGroupGeneration>,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<Option<(MarketRuntimeEntry, MarketRuntimeGroupGeneration)>, ServiceError> {
        let surface_id = try_surface_identifier(request.surface())?;
        let mut entries = bounded_lock(&self.entries, deadline, cancellation).await?;
        let Some(index) = entries
            .iter()
            .position(|entry| entry.surface_id == surface_id)
        else {
            return Ok(None);
        };
        let evidence = entries[index]
            .runtime
            .account_evidence()
            .ok_or(ServiceError::InvalidRequest)?;
        validate_account_evidence(request, evidence)?;
        let generation = evidence.group_generation();
        if expected_group_generation.is_some() && expected_group_generation != Some(generation) {
            return Err(ServiceError::InvalidRequest);
        }
        Ok(Some((entries.swap_remove(index), generation)))
    }

    async fn require_existing_session(
        &self,
        provider: &SourceIdentifier,
        onboarding_session_id: Option<uuid::Uuid>,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<(), ServiceError> {
        let entries = bounded_lock(&self.entries, deadline, cancellation).await?;
        if entries.iter().any(|entry| {
            &entry.surface_id == provider && entry.onboarding_session_id != onboarding_session_id
        }) {
            Err(ServiceError::InvalidRequest)
        } else {
            Ok(())
        }
    }

    async fn take_entry(
        &self,
        provider: &SourceIdentifier,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<Option<MarketRuntimeEntry>, ServiceError> {
        let mut entries = bounded_lock(&self.entries, deadline, cancellation).await?;
        Ok(entries
            .iter()
            .position(|entry| &entry.surface_id == provider)
            .map(|index| entries.swap_remove(index)))
    }

    pub(crate) fn cleanup_deadline(&self) -> Result<Instant, ServiceError> {
        Instant::now()
            .checked_add(self.config.source_shutdown())
            .ok_or(ServiceError::Unavailable)
    }

    async fn account_market_mutation_authority_before(
        &self,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<AccountMarketRuntimeMutationAuthority<'_>, ServiceError> {
        tokio::select! {
            biased;
            () = cancellation.cancelled() => Err(ServiceError::Cancelled),
            () = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => {
                Err(ServiceError::DeadlineExceeded)
            }
            authority = self.provider_activation.acquire_account_market_runtime_mutation_authority() => {
                Ok(authority)
            }
        }
    }

    async fn first_unhealthy_account_group(&self) -> Option<AccountMarketRuntimeHealthSnapshot> {
        let entries = self.entries.try_lock().ok()?;
        entries.iter().find_map(|entry| {
            if entry.is_healthy() {
                return None;
            }
            let evidence = entry.runtime.account_evidence()?;
            Some(AccountMarketRuntimeHealthSnapshot {
                surface_id: entry.surface_id.clone(),
                group_generation: evidence.group_generation(),
            })
        })
    }

    async fn drain_account_group_generation(
        &self,
        snapshot: &AccountMarketRuntimeHealthSnapshot,
    ) -> Result<(), ServiceError> {
        let _mutation = tokio::select! {
            biased;
            () = self.account_health_cancellation.cancelled() => return Ok(()),
            mutation = self.mutation.lock() => mutation,
        };
        let entry = {
            let mut entries = tokio::select! {
                biased;
                () = self.account_health_cancellation.cancelled() => return Ok(()),
                entries = self.entries.lock() => entries,
            };
            let Some(index) = entries.iter().position(|entry| {
                matches_unhealthy_account_generation(
                    &snapshot.surface_id,
                    snapshot.group_generation.digest(),
                    &entry.surface_id,
                    entry
                        .runtime
                        .account_evidence()
                        .map(|evidence| evidence.group_generation().digest()),
                    entry.is_healthy(),
                )
            }) else {
                return Ok(());
            };
            entries[index].begin_shutdown();
            entries.swap_remove(index)
        };
        let deadline = self.cleanup_deadline()?;
        let cleanup = CancellationToken::new();
        self.clear_durable_market_routes(&entry.surface_id, deadline, &cleanup)
            .await?;
        entry.shutdown(self.config.source_shutdown()).await
    }
}

impl fmt::Debug for MarketRuntimeRegistry {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MarketRuntimeRegistry")
            .field("config", &"[REDACTED EFFECTIVE CONFIG]")
            .field(
                "accepting",
                &self.accepting.load(std::sync::atomic::Ordering::Acquire),
            )
            .finish_non_exhaustive()
    }
}

impl Drop for MarketRuntimeRegistry {
    fn drop(&mut self) {
        self.begin_shutdown();
        if let Ok(entries) = self.entries.try_lock() {
            for entry in entries.iter() {
                entry.begin_shutdown();
            }
        }
        self.account_health_cancellation.cancel();
        self.lifecycle.cancel();
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum MarketSurface {
    Public {
        provider: ProductionSourceProvider,
        session_id: uuid::Uuid,
    },
    CoinbaseDirect {
        session_id: uuid::Uuid,
    },
}

impl MarketSurface {
    fn parse(
        provider: &SourceIdentifier,
        onboarding_session_id: Option<uuid::Uuid>,
    ) -> Result<Self, ServiceError> {
        match provider.as_str() {
            COINBASE_PUBLIC_SURFACE_ID => onboarding_session_id
                .map(|session_id| Self::Public {
                    provider: ProductionSourceProvider::Coinbase,
                    session_id,
                })
                .ok_or(ServiceError::InvalidRequest),
            KRAKEN_PUBLIC_SURFACE_ID => onboarding_session_id
                .map(|session_id| Self::Public {
                    provider: ProductionSourceProvider::Kraken,
                    session_id,
                })
                .ok_or(ServiceError::InvalidRequest),
            COINBASE_DIRECT_SURFACE_ID => onboarding_session_id
                .map(|session_id| Self::CoinbaseDirect { session_id })
                .ok_or(ServiceError::InvalidRequest),
            _ => Err(ServiceError::NotFound),
        }
    }

    const fn onboarding_session_id(self) -> Option<uuid::Uuid> {
        match self {
            Self::Public { session_id, .. } => Some(session_id),
            Self::CoinbaseDirect { session_id } => Some(session_id),
        }
    }
}

struct MarketRuntimeEntry {
    surface_id: SourceIdentifier,
    onboarding_session_id: Option<uuid::Uuid>,
    metadata: Arc<[SourceMetadata]>,
    topology: Option<MarketRuntimeTopology>,
    cancellation: CancellationToken,
    runtime: MarketRuntime,
    exports: Option<LiveFairValueExportDrains>,
    action_hooks_installed: bool,
}

impl MarketRuntimeEntry {
    fn is_healthy(&self) -> bool {
        !self.cancellation.is_cancelled()
            && self.runtime.is_healthy()
            && self
                .exports
                .as_ref()
                .is_none_or(LiveFairValueExportDrains::is_healthy)
    }

    fn is_published_healthy(&self) -> bool {
        self.is_healthy()
            && match &self.runtime {
                MarketRuntime::Account(group) => group.is_published_healthy(),
                MarketRuntime::Public(_) | MarketRuntime::CoinbaseDirect(_) => true,
            }
    }

    fn begin_shutdown(&self) {
        if let Some(exports) = self.exports.as_ref() {
            exports.begin_shutdown();
        }
        self.runtime.begin_shutdown();
        self.cancellation.cancel();
    }

    fn scalar_snapshots(&self) -> Result<LiveSnapshotReader, ServiceError> {
        self.runtime.scalar_snapshots()
    }

    fn topology(&self) -> Result<&MarketRuntimeTopology, ServiceError> {
        self.topology.as_ref().ok_or(ServiceError::InvalidRequest)
    }

    async fn shutdown(mut self, shutdown_budget: std::time::Duration) -> Result<(), ServiceError> {
        let deadline = Instant::now()
            .checked_add(shutdown_budget)
            .ok_or(ServiceError::Unavailable)?;
        let cleanup = CancellationToken::new();
        self.begin_shutdown();
        let runtime_result = self.runtime.shutdown_before(deadline, &cleanup).await;
        let export_result = match self.exports.take() {
            Some(exports) => exports
                .finish_before(deadline, &cleanup)
                .await
                .map_err(|error| {
                    tracing::error!(surface = %self.surface_id.as_str(), %error, "market export shutdown failed");
                    ServiceError::Unavailable
                }),
            None => Ok(()),
        };
        match (runtime_result, export_result) {
            (Ok(()), Ok(())) => Ok(()),
            (Err(error), Ok(())) | (Ok(()), Err(error)) => Err(error),
            (Err(_runtime), Err(_exports)) => Err(ServiceError::Unavailable),
        }
    }
}

fn durable_route_bindings(
    surface_id: &SourceIdentifier,
    metadata: Arc<[SourceMetadata]>,
    topology: &MarketRuntimeTopology,
    reads: Vec<MarketEventDurableRead>,
) -> Result<Vec<MarketEventDurableRouteRead>, ServiceError> {
    if reads.is_empty() {
        return Ok(Vec::new());
    }
    if topology.metadata().as_ref() != metadata.as_ref() {
        return Err(ServiceError::InvalidResult);
    }
    let mut bindings = Vec::new();
    for read in reads {
        let source_id = read.point_in_time_selector().source_surface();
        let mut source_indexes = topology
            .metadata()
            .iter()
            .enumerate()
            .filter(|(_index, metadata)| metadata.source_id() == source_id);
        let (source_index, _metadata) = source_indexes.next().ok_or(ServiceError::InvalidResult)?;
        if source_indexes.next().is_some() {
            return Err(ServiceError::InvalidResult);
        }
        let matching_route_count = topology
            .routes()
            .iter()
            .filter(|route| route.source_indexes().contains(&source_index))
            .count();
        if matching_route_count == 0 {
            return Err(ServiceError::InvalidResult);
        }
        bindings
            .try_reserve_exact(matching_route_count)
            .map_err(|_error| ServiceError::ResourceExhausted)?;
        for route in topology
            .routes()
            .iter()
            .filter(|route| route.source_indexes().contains(&source_index))
        {
            bindings.push(MarketEventDurableRouteRead {
                read: read.clone(),
                surface_id: surface_id.clone(),
                metadata: Arc::clone(&metadata),
                source_index,
                route: route.route().clone(),
            });
        }
    }
    if bindings.iter().enumerate().any(|(index, binding)| {
        bindings.iter().skip(index + 1).any(|candidate| {
            binding.read.point_in_time_selector().source_surface()
                == candidate.read.point_in_time_selector().source_surface()
                && binding.route == candidate.route
        })
    }) {
        return Err(ServiceError::InvalidResult);
    }
    Ok(bindings)
}

fn validate_unique_schwab_account_surface(
    entries: &[MarketRuntimeEntry],
) -> Result<(), ServiceError> {
    if entries
        .iter()
        .filter(|entry| {
            entry.surface_id.as_str() == AccountMarketSurface::SchwabMarketData.surface_id()
        })
        .count()
        > 1
    {
        Err(ServiceError::InvalidResult)
    } else {
        Ok(())
    }
}

impl fmt::Debug for MarketRuntimeEntry {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MarketRuntimeEntry")
            .field("surface_id", &self.surface_id)
            .field("onboarding_session_id", &self.onboarding_session_id)
            .field("healthy", &self.is_healthy())
            .finish_non_exhaustive()
    }
}

enum MarketRuntime {
    Public(ProductionLiveSourceRuntime),
    CoinbaseDirect(CoinbaseDirectLiveRuntime),
    Account(AccountMarketRuntimeGroup),
}

impl MarketRuntime {
    fn is_healthy(&self) -> bool {
        match self {
            Self::Public(runtime) => runtime.is_healthy(),
            Self::CoinbaseDirect(runtime) => runtime.is_healthy(),
            Self::Account(runtime) => runtime.is_healthy(),
        }
    }

    fn begin_shutdown(&self) {
        match self {
            Self::Account(runtime) => runtime.begin_shutdown(),
            Self::Public(_) | Self::CoinbaseDirect(_) => {}
        }
    }

    const fn has_scalar_snapshots(&self) -> bool {
        match self {
            Self::Public(_) | Self::CoinbaseDirect(_) => true,
            Self::Account(_) => false,
        }
    }

    fn scalar_snapshots(&self) -> Result<LiveSnapshotReader, ServiceError> {
        match self {
            Self::Public(runtime) => Ok(runtime.snapshots()),
            Self::CoinbaseDirect(runtime) => Ok(runtime.snapshots()),
            Self::Account(_) => Err(ServiceError::InvalidRequest),
        }
    }

    fn account_evidence(&self) -> Option<&MarketProviderGroupLifecycleEvidence> {
        match self {
            Self::Account(runtime) => Some(runtime.evidence()),
            Self::Public(_) | Self::CoinbaseDirect(_) => None,
        }
    }

    fn account_activation_lease(&self) -> Option<&crate::ProviderActivationLease> {
        match self {
            Self::Account(runtime) => Some(runtime.activation_lease()),
            Self::Public(_) | Self::CoinbaseDirect(_) => None,
        }
    }

    fn display_descriptor_count(&self) -> usize {
        match self {
            Self::Account(runtime) => runtime.display_descriptor_count(),
            Self::Public(_) | Self::CoinbaseDirect(_) => 0,
        }
    }

    fn display_instrument_count(&self) -> Option<usize> {
        match self {
            Self::Account(runtime) => runtime.display_instrument_count(),
            Self::Public(_) | Self::CoinbaseDirect(_) => Some(0),
        }
    }

    fn market_instrument_count(&self) -> Option<usize> {
        match self {
            Self::Account(runtime) => runtime.market_instrument_count(),
            Self::Public(_) | Self::CoinbaseDirect(_) => Some(0),
        }
    }

    fn owns_display_descriptor(&self, descriptor: &Arc<DisplaySourceDescriptor>) -> bool {
        match self {
            Self::Account(runtime) => runtime.owns_display_descriptor(descriptor),
            Self::Public(_) | Self::CoinbaseDirect(_) => false,
        }
    }

    fn append_display_instrument_ids(&self, destination: &mut Vec<InstrumentId>) {
        if let Self::Account(runtime) = self {
            runtime.append_display_instrument_ids(destination);
        }
    }

    fn append_market_instrument_ids(&self, destination: &mut Vec<InstrumentId>) {
        if let Self::Account(runtime) = self {
            runtime.append_market_instrument_ids(destination);
        }
    }

    fn kraken_read_authority(
        &self,
        instrument_id: InstrumentId,
    ) -> Option<(Arc<KrakenSourceDescriptor>, OrderLevelBookKey)> {
        match self {
            Self::Account(runtime) => runtime.kraken_read_authority(instrument_id),
            Self::Public(_) | Self::CoinbaseDirect(_) => None,
        }
    }

    fn owns_kraken_descriptor(&self, descriptor: &Arc<KrakenSourceDescriptor>) -> bool {
        match self {
            Self::Account(runtime) => runtime.owns_kraken_descriptor(descriptor),
            Self::Public(_) | Self::CoinbaseDirect(_) => false,
        }
    }

    fn append_display_descriptors(&self, destination: &mut Vec<Arc<DisplaySourceDescriptor>>) {
        if let Self::Account(runtime) = self {
            runtime.append_display_descriptors(destination);
        }
    }

    async fn prepare_action_hooks(
        &mut self,
        hooks: Vec<RouteActionHook>,
        cancellation: CancellationToken,
    ) -> Result<PreparedLiveActionHookGroup, ServiceError> {
        match self {
            Self::Public(runtime) => runtime
                .prepare_action_hooks(hooks, cancellation)
                .await
                .map_err(|error| {
                    tracing::error!(%error, "public market action-hook preparation failed");
                    ServiceError::Unavailable
                }),
            Self::CoinbaseDirect(runtime) => runtime
                .prepare_action_hooks(hooks, cancellation)
                .await
                .map_err(|error| {
                    tracing::error!(%error, "Coinbase Direct action-hook preparation failed");
                    ServiceError::Unavailable
                }),
            Self::Account(_) => Err(ServiceError::InvalidRequest),
        }
    }

    async fn reap_action_hooks(
        &mut self,
        cancellation: CancellationToken,
    ) -> Result<LiveActionHookReapReceipt, ServiceError> {
        match self {
            Self::Public(runtime) => {
                runtime
                    .reap_action_hooks(cancellation)
                    .await
                    .map_err(|error| {
                        tracing::error!(%error, "public market action-hook cleanup failed");
                        ServiceError::Unavailable
                    })
            }
            Self::CoinbaseDirect(runtime) => {
                runtime
                    .reap_action_hooks(cancellation)
                    .await
                    .map_err(|error| {
                        tracing::error!(%error, "Coinbase Direct action-hook cleanup failed");
                        ServiceError::Unavailable
                    })
            }
            Self::Account(_) => Err(ServiceError::InvalidRequest),
        }
    }

    async fn shutdown_before(
        self,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<(), ServiceError> {
        match self {
            Self::Public(runtime) => await_before(deadline, cancellation, runtime.shutdown()).await,
            Self::CoinbaseDirect(runtime) => {
                await_before(deadline, cancellation, runtime.shutdown()).await
            }
            Self::Account(runtime) => runtime.shutdown_before(deadline, cancellation).await,
        }
    }
}

impl fmt::Debug for MarketRuntime {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MarketRuntime")
            .field("healthy", &self.is_healthy())
            .finish_non_exhaustive()
    }
}

async fn await_before<T, E, F>(
    deadline: Instant,
    cancellation: &CancellationToken,
    future: F,
) -> Result<T, ServiceError>
where
    F: std::future::Future<Output = Result<T, E>>,
    E: fmt::Display,
{
    tokio::select! {
        biased;
        () = cancellation.cancelled() => Err(ServiceError::Cancelled),
        () = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => {
            Err(ServiceError::DeadlineExceeded)
        }
        result = future => result.map_err(|error| {
            tracing::error!(%error, "market runtime operation failed");
            ServiceError::Unavailable
        }),
    }
}

async fn await_service_before<T, F>(
    deadline: Instant,
    cancellation: &CancellationToken,
    future: F,
) -> Result<T, ServiceError>
where
    F: std::future::Future<Output = Result<T, ServiceError>>,
{
    tokio::select! {
        biased;
        () = cancellation.cancelled() => Err(ServiceError::Cancelled),
        () = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => {
            Err(ServiceError::DeadlineExceeded)
        }
        result = future => result,
    }
}

async fn bounded_lock<'state, State>(
    state: &'state Mutex<State>,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<tokio::sync::MutexGuard<'state, State>, ServiceError> {
    ensure_before(deadline, cancellation)?;
    tokio::select! {
        biased;
        () = cancellation.cancelled() => Err(ServiceError::Cancelled),
        () = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => {
            Err(ServiceError::DeadlineExceeded)
        }
        guard = state.lock() => Ok(guard),
    }
}

fn ensure_active(
    accepting: &std::sync::atomic::AtomicBool,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<(), ServiceError> {
    ensure_before(deadline, cancellation)?;
    if accepting.load(std::sync::atomic::Ordering::Acquire) {
        Ok(())
    } else {
        Err(ServiceError::Unavailable)
    }
}

fn paper_market_surface_rank(
    metadata: &[SourceMetadata],
) -> Result<(u8, u8, u8, usize), ServiceError> {
    let mut quality = 0_u8;
    let mut real_time = 0_u8;
    let mut depth = 0_u8;
    let mut live_declarations = 0_usize;
    for source in metadata {
        if !source.capabilities().live() {
            continue;
        }
        let live = source.coverage().live().ok_or(ServiceError::Unavailable)?;
        live_declarations = live_declarations
            .checked_add(1)
            .ok_or(ServiceError::ResourceExhausted)?;
        quality = quality.max(data_quality_rank(source.quality_ceiling()));
        if matches!(source.coverage().delay(), CoverageDelay::RealTime) {
            real_time = 1;
        }
        for rule in live.rules() {
            depth = depth.max(rule.depth().map_or(0, market_depth_rank));
        }
    }
    if live_declarations == 0 {
        return Err(ServiceError::Unavailable);
    }
    Ok((quality, real_time, depth, live_declarations))
}

const fn data_quality_rank(quality: DataQuality) -> u8 {
    match quality {
        DataQuality::DirectVerified => 8,
        DataQuality::DirectUnverified => 7,
        DataQuality::OfficialDelayed => 6,
        DataQuality::Aggregated => 5,
        DataQuality::Indicative => 4,
        DataQuality::Modeled => 3,
        DataQuality::Estimated => 2,
        DataQuality::Stale => 1,
        DataQuality::Quarantined => 0,
    }
}

const fn market_depth_rank(depth: MarketDepth) -> u8 {
    match depth {
        MarketDepth::OrderLevel => 3,
        MarketDepth::PriceLevel => 2,
        MarketDepth::TopOfBook => 1,
    }
}

fn ensure_before(deadline: Instant, cancellation: &CancellationToken) -> Result<(), ServiceError> {
    if cancellation.is_cancelled() {
        Err(ServiceError::Cancelled)
    } else if Instant::now() >= deadline {
        Err(ServiceError::DeadlineExceeded)
    } else {
        Ok(())
    }
}

fn try_surface_identifier(surface: AccountMarketSurface) -> Result<SourceIdentifier, ServiceError> {
    SourceIdentifier::try_from(surface.surface_id())
        .map_err(|_error| ServiceError::ResourceExhausted)
}

fn ensure_alpaca_historical_lookup(
    accepting: &std::sync::atomic::AtomicBool,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<(), AlpacaHistoricalLookupError> {
    if cancellation.is_cancelled()
        || Instant::now() >= deadline
        || !accepting.load(std::sync::atomic::Ordering::Acquire)
    {
        Err(AlpacaHistoricalLookupError::Transitioning)
    } else {
        Ok(())
    }
}

fn validate_alpaca_historical_entry(
    entry: &MarketRuntimeEntry,
    request: PreparedMarketProviderConfigurationRequest,
) -> Result<(), AlpacaHistoricalLookupError> {
    let evidence = entry
        .runtime
        .account_evidence()
        .ok_or(AlpacaHistoricalLookupError::Stale)?;
    validate_alpaca_historical_coordinates(
        request,
        entry.onboarding_session_id,
        AccountRuntimeEvidenceCoordinates::from(evidence),
    )?;
    if !entry.is_healthy() {
        return Err(AlpacaHistoricalLookupError::Inactive);
    }
    Ok(())
}

#[derive(Clone, Copy)]
struct AccountRuntimeEvidenceCoordinates<'a> {
    surface_id: &'a str,
    onboarding_session_id: uuid::Uuid,
    public_configuration_digest: EvidenceDigest,
    runtime_verification_receipt_digest: EvidenceDigest,
    credential_generation: SecretGeneration,
}

impl<'a> From<&'a MarketProviderGroupLifecycleEvidence> for AccountRuntimeEvidenceCoordinates<'a> {
    fn from(evidence: &'a MarketProviderGroupLifecycleEvidence) -> Self {
        Self {
            surface_id: evidence.surface_id().as_str(),
            onboarding_session_id: evidence.onboarding_session_id(),
            public_configuration_digest: evidence.public_configuration_digest(),
            runtime_verification_receipt_digest: evidence.runtime_verification_receipt_digest(),
            credential_generation: evidence.credential_generation(),
        }
    }
}

fn validate_alpaca_historical_coordinates(
    request: PreparedMarketProviderConfigurationRequest,
    entry_onboarding_session_id: Option<uuid::Uuid>,
    actual: AccountRuntimeEvidenceCoordinates<'_>,
) -> Result<(), AlpacaHistoricalLookupError> {
    if entry_onboarding_session_id != Some(request.onboarding_session_id())
        || actual.surface_id != request.surface().surface_id()
        || actual.onboarding_session_id != request.onboarding_session_id()
        || actual.public_configuration_digest != request.expected_public_configuration_digest()
        || actual.runtime_verification_receipt_digest
            != request.expected_runtime_verification_receipt_digest()
        || actual.credential_generation != request.expected_credential_generation()
    {
        Err(AlpacaHistoricalLookupError::Stale)
    } else {
        Ok(())
    }
}

fn matches_unhealthy_account_generation(
    snapshot_surface_id: &SourceIdentifier,
    snapshot_generation: EvidenceDigest,
    entry_surface_id: &SourceIdentifier,
    entry_generation: Option<EvidenceDigest>,
    entry_is_healthy: bool,
) -> bool {
    !entry_is_healthy
        && entry_surface_id == snapshot_surface_id
        && entry_generation == Some(snapshot_generation)
}

async fn run_account_health_drain(
    registry: Weak<MarketRuntimeRegistry>,
    cancellation: CancellationToken,
) {
    let mut interval = tokio::time::interval(ACCOUNT_HEALTH_SCAN_INTERVAL);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            biased;
            () = cancellation.cancelled() => break,
            _ = interval.tick() => {}
        }
        let Some(registry) = registry.upgrade() else {
            break;
        };
        let Some(snapshot) = registry.first_unhealthy_account_group().await else {
            continue;
        };
        if let Err(error) = registry.drain_account_group_generation(&snapshot).await
            && !cancellation.is_cancelled()
        {
            tracing::error!(
                %error,
                surface = %snapshot.surface_id.as_str(),
                generation = ?snapshot.group_generation.digest(),
                "account-market stale generation drain failed"
            );
        }
    }
}

const fn map_alpaca_historical_capability_error(
    error: AlpacaHistoricalCapabilityError,
) -> AlpacaHistoricalLookupError {
    match error {
        AlpacaHistoricalCapabilityError::Stale => AlpacaHistoricalLookupError::Stale,
        AlpacaHistoricalCapabilityError::Revoked
        | AlpacaHistoricalCapabilityError::Cancelled
        | AlpacaHistoricalCapabilityError::DeadlineExceeded => {
            AlpacaHistoricalLookupError::Transitioning
        }
    }
}

fn validate_account_evidence(
    request: PreparedMarketProviderConfigurationRequest,
    evidence: &MarketProviderGroupLifecycleEvidence,
) -> Result<(), ServiceError> {
    if evidence.public_configuration_digest() != request.expected_public_configuration_digest()
        || evidence.surface_id().as_str() != request.surface().surface_id()
        || evidence.onboarding_session_id() != request.onboarding_session_id()
        || evidence.runtime_verification_receipt_digest()
            != request.expected_runtime_verification_receipt_digest()
        || evidence.credential_generation() != request.expected_credential_generation()
    {
        Err(ServiceError::InvalidRequest)
    } else {
        Ok(())
    }
}

fn validate_account_lease(
    request: PreparedMarketProviderConfigurationRequest,
    lease: &crate::ProviderActivationLease,
) -> Result<(), ServiceError> {
    if lease.surface_id().as_str() != request.surface().surface_id()
        || lease.session_id() != request.onboarding_session_id()
        || lease.public_configuration_digest() != request.expected_public_configuration_digest()
        || lease.runtime_evidence_digest() != request.expected_runtime_verification_receipt_digest()
        || lease.generation() != Some(request.expected_credential_generation())
    {
        Err(ServiceError::InvalidRequest)
    } else {
        Ok(())
    }
}

fn require_scalar_order_read_authority(
    entries: &[MarketRuntimeEntry],
    surface_id: &SourceIdentifier,
    source_id: &SourceId,
) -> Result<(), ServiceError> {
    let mut found = false;
    for entry in entries {
        if !entry.is_published_healthy()
            || !entry.runtime.has_scalar_snapshots()
            || &entry.surface_id != surface_id
            || !entry
                .metadata
                .iter()
                .any(|metadata| metadata.source_id() == source_id)
        {
            continue;
        }
        if found {
            return Err(ServiceError::Unavailable);
        }
        found = true;
    }
    if found {
        Ok(())
    } else {
        Err(ServiceError::Unavailable)
    }
}

fn require_kraken_order_read_authority(
    entries: &[MarketRuntimeEntry],
    projection: &MarketKrakenPriceProjectionLease,
) -> Result<(), ServiceError> {
    let mut found = false;
    for entry in entries {
        if !entry.is_published_healthy() || &entry.surface_id != projection.surface_id() {
            continue;
        }
        let Some((descriptor, key)) = entry
            .runtime
            .kraken_read_authority(projection.key().instrument_id())
        else {
            continue;
        };
        if !entry
            .runtime
            .owns_kraken_descriptor(projection.descriptor())
            || !Arc::ptr_eq(&descriptor, projection.descriptor())
            || &key != projection.key()
        {
            continue;
        }
        if found {
            return Err(ServiceError::Unavailable);
        }
        found = true;
    }
    if found {
        Ok(())
    } else {
        Err(ServiceError::Unavailable)
    }
}

fn aggregate(
    provider: SourceIdentifier,
    topology: &MarketRuntimeTopology,
    reader: LiveSnapshotReader,
) -> Result<Option<MarketSourceLifecycleEvidence>, ServiceError> {
    let lease = reader
        .try_load_all()
        .map_err(|_error| ServiceError::Unavailable)?;
    let evaluated_at = market_runtime_timestamp()?;
    let metadata = topology.metadata();
    let mut observed_sources = Vec::new();
    observed_sources
        .try_reserve_exact(metadata.len())
        .map_err(|_| ServiceError::ResourceExhausted)?;
    observed_sources.resize(
        metadata.len(),
        None::<(ConnectionGeneration, SourceIdentifier)>,
    );
    let mut observed_routes = Vec::new();
    observed_routes
        .try_reserve_exact(topology.routes().len())
        .map_err(|_| ServiceError::ResourceExhausted)?;
    observed_routes.resize(topology.routes().len(), false);
    let mut aggregate = None;
    for shard in lease.snapshots() {
        if shard.lifecycle() != ShardLifecycleSnapshot::Ready
            || shard.route_dimension().completeness() != SnapshotCompleteness::Complete
            || shard.evaluated_at() > evaluated_at
            || shard.published_at() > evaluated_at
        {
            return Err(ServiceError::Unavailable);
        }
        for route in shard.routes() {
            let expected_route_index = topology
                .routes()
                .iter()
                .position(|expected| expected.route() == route.route())
                .ok_or(ServiceError::Unavailable)?;
            if std::mem::replace(&mut observed_routes[expected_route_index], true) {
                return Err(ServiceError::Unavailable);
            }
            let expected_route = &topology.routes()[expected_route_index];
            if route.stream_dimension().completeness() != SnapshotCompleteness::Complete
                || route.status_dimension().completeness() != SnapshotCompleteness::Complete
                || route.streams().len() != expected_route.source_indexes().len()
            {
                return Err(ServiceError::Unavailable);
            }
            let mut observed_route_sources = Vec::new();
            observed_route_sources
                .try_reserve_exact(expected_route.source_indexes().len())
                .map_err(|_| ServiceError::ResourceExhausted)?;
            observed_route_sources.resize(expected_route.source_indexes().len(), false);
            for stream in route.streams() {
                let expected_source_position = expected_route
                    .source_indexes()
                    .iter()
                    .position(|source_index| metadata[*source_index].source_id() == stream.source())
                    .ok_or(ServiceError::Unavailable)?;
                if std::mem::replace(&mut observed_route_sources[expected_source_position], true) {
                    return Err(ServiceError::Unavailable);
                }
                let source_index = expected_route.source_indexes()[expected_source_position];
                let expected_source = &metadata[source_index];
                let expected_live = expected_source
                    .coverage()
                    .live()
                    .ok_or(ServiceError::Unavailable)?;
                let runtime = stream
                    .runtime_evidence()
                    .filter(|evidence| evidence.matches_stream(stream))
                    .ok_or(ServiceError::Unavailable)?;
                if stream.venue() != route.route().venue()
                    || stream.instrument() != route.route().instrument()
                    || stream.provider_product() != expected_live.provider_product()
                    || stream.provider_channel() != expected_live.provider_channel()
                    || !expected_source.is_effective_at(evaluated_at)
                    || runtime.coverage_scope().metadata_revision() != expected_source.revision()
                    || !stream.generation_current()
                    || stream.phase() != StreamPhaseSnapshot::Healthy
                    || stream.source_valid_until() < evaluated_at
                    || stream.evaluated_at() > evaluated_at
                    || runtime.health_observed_at() > evaluated_at
                    || runtime.qualification_evaluated_at() > evaluated_at
                    || runtime.qualification_valid_until() < evaluated_at
                    || runtime.coverage_status() != CoverageStatus::Sufficient
                    || runtime.stream_integrity() != StreamIntegrityState::Healthy
                    || matches!(
                        runtime.quality(),
                        DataQuality::Stale | DataQuality::Quarantined
                    )
                {
                    return Err(ServiceError::Unavailable);
                }
                let session = runtime.session_id().clone();
                match &observed_sources[source_index] {
                    Some((generation, prior_session))
                        if *generation != stream.connection_generation()
                            || prior_session != &session =>
                    {
                        return Err(ServiceError::Unavailable);
                    }
                    Some(_) => {}
                    None => {
                        observed_sources[source_index] =
                            Some((stream.connection_generation(), session));
                    }
                }
                let generation = topology
                    .generation((metadata.len() == 1).then_some(stream.connection_generation()))?;
                let candidate = MarketSourceLifecycleEvidence {
                    provider: provider.clone(),
                    generation,
                    coverage: runtime.coverage_status(),
                    integrity: runtime.stream_integrity(),
                    quality: runtime.quality(),
                    observed_at: runtime.health_observed_at(),
                };
                aggregate = Some(match aggregate {
                    None => candidate,
                    Some(previous) => merge(previous, candidate)?,
                });
            }
            if observed_route_sources.iter().any(|observed| !observed) {
                return Err(ServiceError::Unavailable);
            }
        }
    }
    if observed_routes.iter().any(|observed| !observed)
        || observed_sources.iter().any(Option::is_none)
    {
        return Err(ServiceError::Unavailable);
    }
    Ok(aggregate)
}

fn merge(
    mut aggregate: MarketSourceLifecycleEvidence,
    candidate: MarketSourceLifecycleEvidence,
) -> Result<MarketSourceLifecycleEvidence, ServiceError> {
    if aggregate.provider != candidate.provider || aggregate.generation != candidate.generation {
        return Err(ServiceError::Unavailable);
    }
    aggregate.coverage = weakest_coverage(aggregate.coverage, candidate.coverage);
    aggregate.integrity = weakest_integrity(aggregate.integrity, candidate.integrity);
    aggregate.quality = weakest_quality(aggregate.quality, candidate.quality);
    aggregate.observed_at = aggregate.observed_at.min(candidate.observed_at);
    Ok(aggregate)
}

fn clone_route_keys(routes: &[LiveRouteConfig]) -> Result<Arc<[ShardKey]>, ServiceError> {
    let mut keys = Vec::new();
    keys.try_reserve_exact(routes.len())
        .map_err(|_| ServiceError::ResourceExhausted)?;
    keys.extend(routes.iter().map(|route| route.route().clone()));
    Ok(keys.into())
}

fn market_runtime_timestamp() -> Result<Timestamp, ServiceError> {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| ServiceError::Unavailable)?;
    let nanos = i64::try_from(elapsed.as_nanos()).map_err(|_| ServiceError::Unavailable)?;
    Ok(Timestamp::from_unix_nanos(nanos))
}

const fn weakest_coverage(left: CoverageStatus, right: CoverageStatus) -> CoverageStatus {
    match (left, right) {
        (CoverageStatus::Unknown, _) | (_, CoverageStatus::Unknown) => CoverageStatus::Unknown,
        (CoverageStatus::Insufficient, _) | (_, CoverageStatus::Insufficient) => {
            CoverageStatus::Insufficient
        }
        (CoverageStatus::Sufficient, CoverageStatus::Sufficient) => CoverageStatus::Sufficient,
    }
}

const fn weakest_integrity(
    left: StreamIntegrityState,
    right: StreamIntegrityState,
) -> StreamIntegrityState {
    if integrity_rank(left) >= integrity_rank(right) {
        left
    } else {
        right
    }
}

const fn integrity_rank(value: StreamIntegrityState) -> u8 {
    match value {
        StreamIntegrityState::Healthy => 0,
        StreamIntegrityState::Initializing => 1,
        StreamIntegrityState::Synchronizing => 2,
        StreamIntegrityState::Validating => 3,
        StreamIntegrityState::Stale => 4,
        StreamIntegrityState::GapDetected => 5,
        StreamIntegrityState::Divergent => 6,
        StreamIntegrityState::ChecksumFailed => 7,
        StreamIntegrityState::Quarantined => 8,
    }
}

const fn weakest_quality(left: DataQuality, right: DataQuality) -> DataQuality {
    if quality_rank(left) >= quality_rank(right) {
        left
    } else {
        right
    }
}

const fn quality_rank(value: DataQuality) -> u8 {
    match value {
        DataQuality::DirectVerified => 0,
        DataQuality::DirectUnverified => 1,
        DataQuality::OfficialDelayed => 2,
        DataQuality::Aggregated => 3,
        DataQuality::Indicative => 4,
        DataQuality::Modeled => 5,
        DataQuality::Estimated => 6,
        DataQuality::Stale => 7,
        DataQuality::Quarantined => 8,
    }
}

const fn map_snapshot_failure(
    error: market_squawk_live::SnapshotReadError,
) -> MarketSourceSnapshotFailureKind {
    match error {
        market_squawk_live::SnapshotReadError::ReaderLimitReached
        | market_squawk_live::SnapshotReadError::CapacityOverflow => {
            MarketSourceSnapshotFailureKind::ResourceExhausted
        }
        market_squawk_live::SnapshotReadError::UnknownShard
        | market_squawk_live::SnapshotReadError::Closed => {
            MarketSourceSnapshotFailureKind::Unavailable
        }
    }
}

fn map_order_level_key_error(error: OrderLevelDirectoryError) -> ServiceError {
    tracing::error!(%error, "order-level market lookup identity construction failed");
    match error {
        OrderLevelDirectoryError::Allocation => ServiceError::ResourceExhausted,
        _ => ServiceError::Unavailable,
    }
}

fn map_order_level_read_error(error: OrderLevelReadError) -> ServiceError {
    match error {
        OrderLevelReadError::OrderLimit { .. }
        | OrderLevelReadError::AccountingOverflow
        | OrderLevelReadError::ByteLimit { .. }
        | OrderLevelReadError::OrderBudget { .. } => ServiceError::ResourceExhausted,
        OrderLevelReadError::Cancelled => ServiceError::Cancelled,
        OrderLevelReadError::Deadline => ServiceError::DeadlineExceeded,
        OrderLevelReadError::Unavailable
        | OrderLevelReadError::NotRegistered
        | OrderLevelReadError::Unregistering
        | OrderLevelReadError::WorkerClosed => ServiceError::Unavailable,
        OrderLevelReadError::Book(error) => {
            tracing::error!(%error, "canonical order-level presenter read failed");
            ServiceError::Unavailable
        }
        OrderLevelReadError::Projection(error) => {
            tracing::error!(%error, "order-level presenter projection failed");
            ServiceError::Unavailable
        }
    }
}

fn map_display_read_error(error: DisplayMarketReadError) -> ServiceError {
    match error {
        DisplayMarketReadError::Allocation
        | DisplayMarketReadError::AccountingOverflow
        | DisplayMarketReadError::SourceLimit { .. } => ServiceError::ResourceExhausted,
        DisplayMarketReadError::Cancelled => ServiceError::Cancelled,
        DisplayMarketReadError::Deadline => ServiceError::DeadlineExceeded,
        DisplayMarketReadError::Unavailable
        | DisplayMarketReadError::Unregistering
        | DisplayMarketReadError::WorkerClosed => ServiceError::Unavailable,
    }
}

const fn map_display_directory_shutdown_error(error: DisplayMarketDirectoryError) -> ServiceError {
    match error {
        DisplayMarketDirectoryError::Cancelled => ServiceError::Cancelled,
        DisplayMarketDirectoryError::Deadline => ServiceError::DeadlineExceeded,
        _ => ServiceError::Unavailable,
    }
}

const fn map_order_level_directory_shutdown_error(error: OrderLevelDirectoryError) -> ServiceError {
    match error {
        OrderLevelDirectoryError::Cancelled => ServiceError::Cancelled,
        OrderLevelDirectoryError::Deadline => ServiceError::DeadlineExceeded,
        _ => ServiceError::Unavailable,
    }
}

struct StartupCancellation {
    cancellation: CancellationToken,
    armed: bool,
}

impl StartupCancellation {
    const fn new(cancellation: CancellationToken) -> Self {
        Self {
            cancellation,
            armed: true,
        }
    }

    fn token(&self) -> CancellationToken {
        self.cancellation.clone()
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for StartupCancellation {
    fn drop(&mut self) {
        if self.armed {
            self.cancellation.cancel();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use market_squawk_domain::DigestAlgorithm;

    #[test]
    fn historical_lookup_rejects_receipt_and_credential_generation_mismatch() {
        let session_id = uuid::Uuid::new_v4();
        let public_configuration_digest = digest(1);
        let runtime_receipt_digest = digest(2);
        let credential_generation = SecretGeneration::new(3).expect("nonzero generation");
        let request = PreparedMarketProviderConfigurationRequest::try_new(
            AccountMarketSurface::AlpacaBasic,
            session_id,
            public_configuration_digest,
            runtime_receipt_digest,
            credential_generation,
        )
        .expect("exact request");
        let exact = AccountRuntimeEvidenceCoordinates {
            surface_id: AccountMarketSurface::AlpacaBasic.surface_id(),
            onboarding_session_id: session_id,
            public_configuration_digest,
            runtime_verification_receipt_digest: runtime_receipt_digest,
            credential_generation,
        };
        assert_eq!(
            validate_alpaca_historical_coordinates(request, Some(session_id), exact),
            Ok(())
        );
        assert_eq!(
            validate_alpaca_historical_coordinates(
                request,
                Some(session_id),
                AccountRuntimeEvidenceCoordinates {
                    runtime_verification_receipt_digest: digest(4),
                    ..exact
                },
            ),
            Err(AlpacaHistoricalLookupError::Stale)
        );
        assert_eq!(
            validate_alpaca_historical_coordinates(
                request,
                Some(session_id),
                AccountRuntimeEvidenceCoordinates {
                    credential_generation: SecretGeneration::new(5).expect("nonzero generation"),
                    ..exact
                },
            ),
            Err(AlpacaHistoricalLookupError::Stale)
        );
    }

    #[test]
    fn health_drain_cas_rejects_healthy_and_successor_coordinates() {
        let surface = SourceIdentifier::try_from(AccountMarketSurface::AlpacaBasic.surface_id())
            .expect("fixed surface");
        let other = SourceIdentifier::try_from(AccountMarketSurface::KrakenLevel3.surface_id())
            .expect("fixed surface");
        let generation = digest(6);
        assert!(matches_unhealthy_account_generation(
            &surface,
            generation,
            &surface,
            Some(generation),
            false,
        ));
        assert!(!matches_unhealthy_account_generation(
            &surface,
            generation,
            &surface,
            Some(generation),
            true,
        ));
        assert!(!matches_unhealthy_account_generation(
            &surface,
            generation,
            &surface,
            Some(digest(7)),
            false,
        ));
        assert!(!matches_unhealthy_account_generation(
            &surface,
            generation,
            &other,
            Some(generation),
            false,
        ));
    }

    const fn digest(byte: u8) -> EvidenceDigest {
        EvidenceDigest::new(DigestAlgorithm::Sha256, [byte; 32])
    }
}
