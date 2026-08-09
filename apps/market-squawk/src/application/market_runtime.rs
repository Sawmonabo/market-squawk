//! Bounded multi-provider market-runtime ownership shared by every local presentation.

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

use super::live_fair_value::{LiveFairValueExportDrains, LiveFairValueObservationBuffer};
use crate::{
    AppConfig, CoinbaseDirectLiveRuntime, ProductionLiveSourceRuntime, ProductionSourceProvider,
    ProviderAdapterActivation,
    live_source::order_level::{
        MAX_ORDER_LEVEL_DIRECTORY_BOOKS, OrderLevelBookKey, OrderLevelDirectory,
        OrderLevelDirectoryError, OrderLevelOrdersRead, OrderLevelReadError,
    },
    paper_bot::{
        local_coinbase_direct_live_market_with_activation, local_live_market_with_provider_rate,
    },
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
    live_fair_value: Arc<LiveFairValueObservationBuffer>,
    accepting: std::sync::atomic::AtomicBool,
    lifecycle: CancellationToken,
    order_level: OrderLevelDirectory,
    mutation: Mutex<()>,
    entries: Mutex<Vec<MarketRuntimeEntry>>,
}

impl MarketRuntimeRegistry {
    pub(crate) fn try_new(
        config: AppConfig,
        provider_rate: ProviderRateAuthority,
        provider_activation: Arc<ProviderAdapterActivation>,
        live_fair_value: Arc<LiveFairValueObservationBuffer>,
    ) -> Result<Arc<Self>, ServiceError> {
        let mut entries = Vec::new();
        entries
            .try_reserve_exact(MAXIMUM_CONCURRENT_MARKET_SURFACES)
            .map_err(|_error| ServiceError::ResourceExhausted)?;
        let lifecycle = CancellationToken::new();
        let order_level = OrderLevelDirectory::try_new(
            NonZeroUsize::new(MAX_ORDER_LEVEL_DIRECTORY_BOOKS)
                .ok_or(ServiceError::ResourceExhausted)?,
            lifecycle.child_token(),
        )
        .map_err(|error| {
            tracing::error!(%error, "order-level market directory construction failed");
            ServiceError::ResourceExhausted
        })?;
        Ok(Arc::new(Self {
            config,
            provider_rate,
            provider_activation,
            live_fair_value,
            accepting: std::sync::atomic::AtomicBool::new(true),
            lifecycle,
            order_level,
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

    async fn start_owned(
        &self,
        provider: &SourceIdentifier,
        onboarding_session_id: Option<uuid::Uuid>,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<MarketSourceLifecycleEvidence, ServiceError> {
        ensure_active(&self.accepting, deadline, cancellation)?;
        match self.verify_owned(provider).await {
            Ok(Some(evidence)) => return Ok(evidence),
            Ok(None) | Err(ServiceError::Unavailable) => {}
            Err(error) => return Err(error),
        }
        self.remove_unhealthy_owned(provider, deadline, cancellation)
            .await?;
        let surface = MarketSurface::parse(provider, onboarding_session_id)?;
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
            entry.snapshots()
        };
        aggregate(provider.clone(), reader)
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
            let healthy = entries.iter().filter(|entry| entry.is_healthy()).count();
            let mut readers = Vec::new();
            readers
                .try_reserve_exact(healthy)
                .map_err(|_error| ServiceError::ResourceExhausted)?;
            for entry in entries.iter().filter(|entry| entry.is_healthy()) {
                readers.push((
                    entry.surface_id.clone(),
                    Arc::clone(&entry.metadata),
                    entry.snapshots(),
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
        Ok(entry.snapshots())
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
        self.accepting
            .store(false, std::sync::atomic::Ordering::Release);
    }

    pub(crate) async fn finish_shutdown(&self, deadline: Instant) -> Result<(), ServiceError> {
        self.begin_shutdown();
        self.lifecycle.cancel();
        let cleanup = CancellationToken::new();
        let _mutation = bounded_lock(&self.mutation, deadline, &cleanup).await?;
        let entries = {
            let mut entries = bounded_lock(&self.entries, deadline, &cleanup).await?;
            std::mem::take(&mut *entries)
        };
        let mut failure = None;
        for entry in entries {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                failure.get_or_insert(ServiceError::DeadlineExceeded);
                break;
            }
            if let Err(error) = entry.shutdown(remaining).await
                && failure.is_none()
            {
                failure = Some(error);
            }
        }
        if Instant::now() >= deadline {
            failure.get_or_insert(ServiceError::DeadlineExceeded);
        } else {
            match self.order_level.shutdown(&cleanup, deadline).await {
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
                    failure.get_or_insert(ServiceError::Unavailable);
                }
            }
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

    fn snapshots(&self) -> LiveSnapshotReader {
        self.runtime.snapshots()
    }

    async fn shutdown(mut self, shutdown_budget: std::time::Duration) -> Result<(), ServiceError> {
        let deadline = Instant::now()
            .checked_add(shutdown_budget)
            .ok_or(ServiceError::Unavailable)?;
        let cleanup = CancellationToken::new();
        if let Some(exports) = self.exports.as_ref() {
            exports.begin_shutdown();
        }
        self.cancellation.cancel();
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
}

impl MarketRuntime {
    fn is_healthy(&self) -> bool {
        match self {
            Self::Public(runtime) => runtime.is_healthy(),
            Self::CoinbaseDirect(runtime) => runtime.is_healthy(),
        }
    }

    fn snapshots(&self) -> LiveSnapshotReader {
        match self {
            Self::Public(runtime) => runtime.snapshots(),
            Self::CoinbaseDirect(runtime) => runtime.snapshots(),
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
