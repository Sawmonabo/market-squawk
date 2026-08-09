//! Bounded multi-provider market-runtime ownership shared by every local presentation.

mod configuration;
mod display;
mod generation;
mod group;
mod kraken;

pub(crate) use configuration::{
    AccountMarketSurface, PreparedMarketProviderConfigurationRequest,
    PreparedMarketProviderConfigurationResolver,
};
pub(crate) use display::{MarketDisplaySnapshotBatch, MarketDisplaySnapshotLease};
pub(crate) use generation::MarketRuntimeGroupGeneration;
pub(crate) use group::MarketProviderGroupLifecycleEvidence;
pub(crate) use kraken::MarketKrakenPriceProjectionLease;

use std::{
    fmt,
    num::{NonZeroU64, NonZeroUsize},
    sync::Arc,
    time::Instant,
};

use market_squawk_domain::{
    ConnectionGeneration, CoverageStatus, DataQuality, InstrumentId, SourceId, SourceIdentifier,
    StreamIntegrityState, Timestamp, VenueId,
};
use market_squawk_live::{
    LiveActionHookGeneration, LiveActionHookReapReceipt, LiveRuntimeSnapshotLease,
    LiveSnapshotReader, PreparedLiveActionHookGroup, RouteActionHook,
};
use market_squawk_services::ServiceError;
use market_squawk_sources::{ProviderRateAuthority, SourceMetadata};
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

use self::{
    display::DisplaySourceDescriptor,
    group::{AccountMarketRuntimeGroup, AccountMarketRuntimeLimits},
    kraken::KrakenSourceDescriptor,
};
use super::live_fair_value::{LiveFairValueExportDrains, LiveFairValueObservationBuffer};
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
    provider_activation::ProviderAdapterActivation,
};

pub(crate) const COINBASE_PUBLIC_SURFACE_ID: &str = "coinbase.public-market-data";
pub(crate) const COINBASE_DIRECT_SURFACE_ID: &str = "coinbase.exchange-direct-market-data";
pub(crate) const KRAKEN_PUBLIC_SURFACE_ID: &str = "kraken.spot-public-market-data";

const MAXIMUM_CONCURRENT_MARKET_SURFACES: usize = 16;

/// Exact live evidence returned after one registry-owned source lifecycle operation.
#[derive(Clone, Debug)]
pub(crate) struct MarketSourceLifecycleEvidence {
    pub(crate) provider: SourceIdentifier,
    pub(crate) generation: ConnectionGeneration,
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
    prepared_configuration: Arc<dyn PreparedMarketProviderConfigurationResolver>,
    live_fair_value: Arc<LiveFairValueObservationBuffer>,
    accepting: std::sync::atomic::AtomicBool,
    lifecycle: CancellationToken,
    capture_process: market_squawk_platform::CaptureProcessInfrastructure,
    display: DisplayMarketDirectory,
    order_level: OrderLevelDirectory,
    account_limits: AccountMarketRuntimeLimits,
    mutation: Mutex<()>,
    entries: Mutex<Vec<MarketRuntimeEntry>>,
}

impl MarketRuntimeRegistry {
    pub(crate) fn try_new(
        config: AppConfig,
        provider_rate: ProviderRateAuthority,
        provider_activation: Arc<ProviderAdapterActivation>,
        prepared_configuration: Arc<dyn PreparedMarketProviderConfigurationResolver>,
        live_fair_value: Arc<LiveFairValueObservationBuffer>,
    ) -> Result<Arc<Self>, ServiceError> {
        let mut entries = Vec::new();
        entries
            .try_reserve_exact(MAXIMUM_CONCURRENT_MARKET_SURFACES)
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
            prepared_configuration,
            live_fair_value,
            accepting: std::sync::atomic::AtomicBool::new(true),
            lifecycle,
            capture_process,
            display,
            order_level,
            account_limits,
            mutation: Mutex::new(()),
            entries: Mutex::new(entries),
        }))
    }

    pub(crate) fn active_source_count(&self) -> Result<usize, ServiceError> {
        let entries = self
            .entries
            .try_lock()
            .map_err(|_busy| ServiceError::Unavailable)?;
        Ok(entries.iter().filter(|entry| entry.is_healthy()).count())
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
        &self,
        request: PreparedMarketProviderConfigurationRequest,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<MarketProviderGroupLifecycleEvidence, ServiceError> {
        let _mutation = bounded_lock(&self.mutation, deadline, cancellation).await?;
        self.start_account_group_owned(request, deadline, cancellation)
            .await
    }

    async fn start_account_group_owned(
        &self,
        request: PreparedMarketProviderConfigurationRequest,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<MarketProviderGroupLifecycleEvidence, ServiceError> {
        ensure_active(&self.accepting, deadline, cancellation)?;
        let surface_id = try_surface_identifier(request.surface())?;
        match self
            .verify_account_group_owned(request, deadline, cancellation)
            .await
        {
            Ok(Some(evidence)) => return Ok(evidence),
            Ok(None) | Err(ServiceError::Unavailable) => {}
            Err(error) => return Err(error),
        }
        self.remove_unhealthy_account_group_owned(&surface_id, request, deadline, cancellation)
            .await?;

        let mut resolution_guard = StartupCancellation::new(self.lifecycle.child_token());
        let prepared = await_service_before(
            deadline,
            cancellation,
            self.prepared_configuration
                .resolve(request, deadline, resolution_guard.token()),
        )
        .await?;
        resolution_guard.disarm();
        let runtime_cancellation = self.lifecycle.child_token();
        let group = AccountMarketRuntimeGroup::start(
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
        )
        .await?;
        let evidence = group.evidence().clone();
        let entry = MarketRuntimeEntry {
            surface_id,
            onboarding_session_id: Some(request.onboarding_session_id()),
            metadata: Arc::<[SourceMetadata]>::from([]),
            cancellation: runtime_cancellation,
            runtime: MarketRuntime::Account(group),
            exports: None,
            action_hooks_installed: false,
        };
        if let Err(error) = ensure_active(&self.accepting, deadline, cancellation) {
            let _cleanup = entry.shutdown(self.config.source_shutdown()).await;
            return Err(error);
        }
        let entries = bounded_lock(&self.entries, deadline, cancellation).await;
        let mut entries = match entries {
            Ok(entries) => entries,
            Err(error) => {
                let _cleanup = entry.shutdown(self.config.source_shutdown()).await;
                return Err(error);
            }
        };
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
        Ok(evidence)
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
            MarketSurface::Public(provider_kind) => {
                let composition = local_live_market_with_provider_rate(
                    self.config.clone(),
                    provider_kind,
                    self.provider_rate.clone(),
                )
                .map_err(|error| {
                    tracing::error!(provider = ?provider_kind, %error, "market source composition failed");
                    ServiceError::Unavailable
                })?;
                let metadata: Arc<[SourceMetadata]> = Arc::from([composition.metadata().clone()]);
                let (exports, drains) = LiveFairValueExportDrains::try_start(
                    composition.source_id().clone(),
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
                    composition
                        .start_with_qualified_market_exports(exports, runtime_cancellation.clone()),
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
                    onboarding_session_id: None,
                    metadata,
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
                MarketRuntimeEntry {
                    surface_id: provider.clone(),
                    onboarding_session_id: Some(session_id),
                    metadata,
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
        if !entry.is_healthy() {
            return Err(ServiceError::Unavailable);
        }
        Ok(Some(evidence.clone()))
    }

    async fn verify_owned(
        &self,
        provider: &SourceIdentifier,
    ) -> Result<Option<MarketSourceLifecycleEvidence>, ServiceError> {
        let reader = {
            let entries = self.entries.lock().await;
            let Some(entry) = entries.iter().find(|entry| &entry.surface_id == provider) else {
                return Ok(None);
            };
            if !entry.is_healthy() {
                return Err(ServiceError::Unavailable);
            }
            entry.scalar_snapshots()?
        };
        aggregate(provider.clone(), reader)
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
        entry.shutdown(self.config.source_shutdown()).await?;
        Ok(Some(generation))
    }

    pub(crate) async fn stop(
        &self,
        provider: &SourceIdentifier,
        expected_generation: Option<ConnectionGeneration>,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<Option<ConnectionGeneration>, ServiceError> {
        let _mutation = bounded_lock(&self.mutation, deadline, cancellation).await?;
        self.stop_owned(provider, expected_generation, deadline, cancellation)
            .await
    }

    async fn stop_owned(
        &self,
        provider: &SourceIdentifier,
        expected_generation: Option<ConnectionGeneration>,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<Option<ConnectionGeneration>, ServiceError> {
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
        expected_generation: ConnectionGeneration,
        onboarding_session_id: Option<uuid::Uuid>,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<(ConnectionGeneration, MarketSourceLifecycleEvidence), ServiceError> {
        let _mutation = bounded_lock(&self.mutation, deadline, cancellation).await?;
        let previous = self
            .stop_owned(provider, Some(expected_generation), deadline, cancellation)
            .await?
            .ok_or(ServiceError::Unavailable)?;
        let current = self
            .start_owned(provider, onboarding_session_id, deadline, cancellation)
            .await?;
        if current.generation.get() <= previous.get() {
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
    ) -> Result<Option<ConnectionGeneration>, ServiceError> {
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
            let descriptor_count = entries
                .iter()
                .filter(|entry| entry.is_healthy())
                .try_fold(0_usize, |count, entry| {
                    count.checked_add(entry.runtime.display_descriptor_count())
                })
                .ok_or(ServiceError::ResourceExhausted)?;
            let mut descriptors = Vec::new();
            descriptors
                .try_reserve_exact(descriptor_count)
                .map_err(|_error| ServiceError::ResourceExhausted)?;
            for entry in entries.iter().filter(|entry| entry.is_healthy()) {
                entry.runtime.append_display_descriptors(&mut descriptors);
            }
            if descriptors.len() != descriptor_count {
                return Err(ServiceError::Unavailable);
            }
            descriptors
        };
        let leases = self
            .display
            .snapshots_for_instrument(instrument_id, maximum_sources, at, cancellation, deadline)
            .await
            .map_err(map_display_read_error)?;
        let mut snapshots = Vec::new();
        snapshots
            .try_reserve_exact(leases.len())
            .map_err(|_error| ServiceError::ResourceExhausted)?;
        for lease in leases {
            ensure_active(&self.accepting, deadline, cancellation)?;
            let mut matched = descriptors
                .iter()
                .filter(|descriptor| descriptor.matches_snapshot(&lease));
            let descriptor = matched.next().ok_or(ServiceError::Unavailable)?;
            if matched.next().is_some() {
                return Err(ServiceError::Unavailable);
            }
            snapshots.push(MarketDisplaySnapshotLease::try_new(
                Arc::clone(descriptor),
                lease,
            )?);
        }
        {
            let entries = bounded_lock(&self.entries, deadline, cancellation).await?;
            if snapshots.iter().any(|snapshot| {
                !entries.iter().any(|entry| {
                    entry.is_healthy()
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
                .filter(|entry| entry.is_healthy())
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
            for entry in entries.iter().filter(|entry| entry.is_healthy()) {
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

    /// Reads a bounded individual-order sample only from the exact retained source generation.
    pub(crate) async fn order_level_snapshot(
        &self,
        source_id: &SourceId,
        venue_id: &VenueId,
        instrument_id: InstrumentId,
        generation: ConnectionGeneration,
        maximum_orders: NonZeroUsize,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<Option<MarketOrderLevelSnapshot>, ServiceError> {
        ensure_active(&self.accepting, deadline, cancellation)?;
        let key =
            OrderLevelBookKey::try_from_snapshot(source_id, venue_id, instrument_id, generation)
                .map_err(map_order_level_key_error)?;
        let orders = match self
            .order_level
            .read_orders(&key, maximum_orders, cancellation, deadline)
            .await
        {
            Ok(orders) => orders,
            Err(
                OrderLevelReadError::Unavailable
                | OrderLevelReadError::NotRegistered
                | OrderLevelReadError::Unregistering
                | OrderLevelReadError::WorkerClosed,
            ) => return Ok(None),
            Err(OrderLevelReadError::Cancelled) => return Err(ServiceError::Cancelled),
            Err(OrderLevelReadError::Deadline) => return Err(ServiceError::DeadlineExceeded),
            Err(error) => {
                tracing::error!(%error, "bounded order-level market read failed");
                return Err(ServiceError::Unavailable);
            }
        };
        ensure_active(&self.accepting, deadline, cancellation)?;
        Ok(Some(MarketOrderLevelSnapshot { key, orders }))
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
        if self
            .accepting
            .swap(false, std::sync::atomic::Ordering::AcqRel)
        {
            self.prepared_configuration.begin_shutdown();
        }
    }

    pub(crate) async fn finish_shutdown(&self, deadline: Instant) -> Result<(), ServiceError> {
        self.begin_shutdown();
        let cleanup = CancellationToken::new();
        let _mutation = match bounded_lock(&self.mutation, deadline, &cleanup).await {
            Ok(mutation) => mutation,
            Err(error) => {
                self.lifecycle.cancel();
                return Err(error);
            }
        };
        let entries = {
            let mut entries = match bounded_lock(&self.entries, deadline, &cleanup).await {
                Ok(entries) => entries,
                Err(error) => {
                    self.lifecycle.cancel();
                    return Err(error);
                }
            };
            std::mem::take(&mut *entries)
        };
        for entry in &entries {
            entry.begin_shutdown();
        }
        let mut failure = None;
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

        let display_shutdown = self.display.shutdown(&cleanup, deadline);
        let order_level_shutdown = self.order_level.shutdown(&cleanup, deadline);
        let resolver_shutdown = await_service_before(
            deadline,
            &cleanup,
            self.prepared_configuration.finish_shutdown(deadline),
        );
        let (display_result, order_level_result, resolver_result) =
            tokio::join!(display_shutdown, order_level_shutdown, resolver_shutdown);
        self.lifecycle.cancel();

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
        if Instant::now() >= deadline {
            failure.get_or_insert(ServiceError::DeadlineExceeded);
        }
        failure.map_or(Ok(()), Err)
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

    fn cleanup_deadline(&self) -> Result<Instant, ServiceError> {
        Instant::now()
            .checked_add(self.config.source_shutdown())
            .ok_or(ServiceError::Unavailable)
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
        self.lifecycle.cancel();
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum MarketSurface {
    Public(ProductionSourceProvider),
    CoinbaseDirect { session_id: uuid::Uuid },
}

impl MarketSurface {
    fn parse(
        provider: &SourceIdentifier,
        onboarding_session_id: Option<uuid::Uuid>,
    ) -> Result<Self, ServiceError> {
        match provider.as_str() {
            COINBASE_PUBLIC_SURFACE_ID if onboarding_session_id.is_none() => {
                Ok(Self::Public(ProductionSourceProvider::Coinbase))
            }
            KRAKEN_PUBLIC_SURFACE_ID if onboarding_session_id.is_none() => {
                Ok(Self::Public(ProductionSourceProvider::Kraken))
            }
            COINBASE_DIRECT_SURFACE_ID => onboarding_session_id
                .map(|session_id| Self::CoinbaseDirect { session_id })
                .ok_or(ServiceError::InvalidRequest),
            _ => Err(ServiceError::NotFound),
        }
    }

    const fn onboarding_session_id(self) -> Option<uuid::Uuid> {
        match self {
            Self::Public(_) => None,
            Self::CoinbaseDirect { session_id } => Some(session_id),
        }
    }
}

struct MarketRuntimeEntry {
    surface_id: SourceIdentifier,
    onboarding_session_id: Option<uuid::Uuid>,
    metadata: Arc<[SourceMetadata]>,
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

    fn begin_shutdown(&self) {
        if let Some(exports) = self.exports.as_ref() {
            exports.begin_shutdown();
        }
        self.cancellation.cancel();
    }

    fn scalar_snapshots(&self) -> Result<LiveSnapshotReader, ServiceError> {
        self.runtime.scalar_snapshots()
    }

    async fn shutdown(mut self, shutdown_budget: std::time::Duration) -> Result<(), ServiceError> {
        let deadline = Instant::now()
            .checked_add(shutdown_budget)
            .ok_or(ServiceError::Unavailable)?;
        let cleanup = CancellationToken::new();
        self.begin_shutdown();
        let runtime_result = self.runtime.shutdown().await;
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

    const fn has_scalar_snapshots(&self) -> bool {
        !matches!(self, Self::Account(_))
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

    async fn shutdown(self) -> Result<(), ServiceError> {
        match self {
            Self::Public(runtime) => runtime.shutdown().await.map_err(|error| {
                tracing::error!(%error, "public market source shutdown failed");
                ServiceError::Unavailable
            }),
            Self::CoinbaseDirect(runtime) => runtime.shutdown().await.map_err(|error| {
                tracing::error!(%error, "Coinbase Direct market source shutdown failed");
                ServiceError::Unavailable
            }),
            Self::Account(runtime) => runtime.shutdown().await,
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

fn validate_account_evidence(
    request: PreparedMarketProviderConfigurationRequest,
    evidence: &MarketProviderGroupLifecycleEvidence,
) -> Result<(), ServiceError> {
    if evidence.public_configuration_digest() != request.expected_public_configuration_digest()
        || evidence.surface_id().as_str() != request.surface().surface_id()
        || evidence.onboarding_session_id() != request.onboarding_session_id()
    {
        Err(ServiceError::InvalidRequest)
    } else {
        Ok(())
    }
}

fn aggregate(
    provider: SourceIdentifier,
    reader: LiveSnapshotReader,
) -> Result<Option<MarketSourceLifecycleEvidence>, ServiceError> {
    let lease = reader
        .try_load_all()
        .map_err(|_error| ServiceError::Unavailable)?;
    let mut aggregate = None;
    for shard in lease.snapshots() {
        for route in shard.routes() {
            for stream in route.streams() {
                let runtime = stream
                    .runtime_evidence()
                    .filter(|evidence| evidence.matches_stream(stream))
                    .ok_or(ServiceError::Unavailable)?;
                let candidate = MarketSourceLifecycleEvidence {
                    provider: provider.clone(),
                    generation: stream.connection_generation(),
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
        }
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
