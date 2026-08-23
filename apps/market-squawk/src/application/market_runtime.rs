//! Bounded multi-provider market-runtime ownership shared by every local presentation.

mod account_shutdown;
mod alpaca_historical;
mod configuration;
mod display;
mod generation;
mod group;
mod kraken;

pub(crate) use account_shutdown::{
    AccountGroupStopAcknowledgementReceipt, AccountGroupStopDurableProof,
    AccountGroupStopHistoryEvidence, AccountGroupStopKeyEvidence, AccountGroupStopReceipt,
    AccountGroupStopTicket, PreparedAccountGroupStop,
};
pub(crate) use alpaca_historical::{
    AlpacaHistoricalCompositeCalendarAuthority, AlpacaHistoricalLookupError,
    AlpacaHistoricalRuntimeCapability,
};
pub(crate) use configuration::{
    AccountMarketSurface, PreparedMarketProviderConfigurationRequest,
    PreparedMarketProviderConfigurationResolver,
};
pub(crate) use display::{MarketDisplaySnapshotBatch, MarketDisplaySnapshotLease};
pub(crate) use generation::{MarketRuntimeGroupGeneration, MarketSourceRuntimeGeneration};
pub(crate) use group::MarketProviderGroupLifecycleEvidence;
pub(crate) use kraken::MarketKrakenPriceProjectionLease;

use std::{
    fmt,
    future::Future,
    num::{NonZeroU64, NonZeroUsize},
    pin::Pin,
    sync::{Arc, Mutex as SyncMutex, Weak},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use market_squawk_adapter_alpaca::AlpacaHistoricalEquityPreflightPlan;
use market_squawk_domain::{
    ConnectionGeneration, CoverageStatus, DataQuality, EvidenceDigest, InstrumentId,
    MarketDataInstrumentDefinition, SourceId, SourceIdentifier, StreamIntegrityState, Timestamp,
    VenueId,
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

#[cfg(test)]
use self::group::AccountShutdownFixtureProbe;
use self::{
    account_shutdown::{
        AccountGroupStopAcknowledgement, AccountGroupStopAcknowledgementDisposition,
        AccountShutdownAttemptGuard, AccountShutdownKey, RetainedAccountGroupStop,
    },
    alpaca_historical::AlpacaHistoricalCapabilityError,
    display::DisplaySourceDescriptor,
    generation::MarketRuntimeTopology,
    group::{
        AccountMarketRuntimeGroup, AccountMarketRuntimeHistoryClaim, AccountMarketRuntimeLimits,
    },
    kraken::KrakenSourceDescriptor,
};
use super::live_fair_value::{LiveFairValueExportDrains, LiveFairValueObservationBuffer};
use super::{
    AlpacaHistoricalAuthorizedPlan, AlpacaHistoricalDrainReceipt,
    AlpacaHistoricalPlanAdmissionError, AlpacaHistoricalPlanReceipt,
    AlpacaHistoricalSourceMutationAuthority,
};
#[cfg(test)]
use super::{AlpacaHistoricalHeldPublication, activate_shutdown_successor_publication};
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
    registry_incarnation: uuid::Uuid,
    config: AppConfig,
    provider_rate: ProviderRateAuthority,
    provider_activation: RegistryProviderActivation,
    alpaca_historical_source: AlpacaHistoricalSourceMutationAuthority,
    prepared_configuration: Arc<dyn PreparedMarketProviderConfigurationResolver>,
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
    account_stop_acknowledgements: SyncMutex<Vec<AccountGroupStopAcknowledgement>>,
    account_health_cancellation: CancellationToken,
    account_health_drain: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

enum RegistryProviderActivation {
    Production(Arc<ProviderAdapterActivation>),
    #[cfg(test)]
    ShutdownFixture,
}

#[cfg(test)]
struct ShutdownFixtureConfigurationResolver;

#[cfg(test)]
#[async_trait::async_trait]
impl PreparedMarketProviderConfigurationResolver for ShutdownFixtureConfigurationResolver {
    async fn resolve(
        &self,
        _request: PreparedMarketProviderConfigurationRequest,
        _deadline: Instant,
        _cancellation: CancellationToken,
    ) -> Result<PreparedMarketProviderConfiguration, ServiceError> {
        Err(ServiceError::Unavailable)
    }

    fn begin_shutdown(&self) {}

    async fn finish_shutdown(&self, _deadline: Instant) -> Result<(), ServiceError> {
        Ok(())
    }
}

impl RegistryProviderActivation {
    fn production(&self) -> Result<&ProviderAdapterActivation, ServiceError> {
        match self {
            Self::Production(activation) => Ok(activation.as_ref()),
            #[cfg(test)]
            Self::ShutdownFixture => Err(ServiceError::Unavailable),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct AccountMarketRuntimeHealthSnapshot {
    request: PreparedMarketProviderConfigurationRequest,
    group_generation: MarketRuntimeGroupGeneration,
}

/// Exact account-group state used only to prepare a stop compare-and-set operation.
///
/// Ordinary verification continues to expose only healthy Active groups. This separate result
/// lets lifecycle Stop/Resynchronize/Reconfigure join an exact retained stopping generation while
/// rejecting same-surface request or generation mismatches.
pub(crate) enum AccountGroupStopState {
    Absent,
    Active(MarketProviderGroupLifecycleEvidence),
    Stopping(MarketRuntimeGroupGeneration),
}

enum AccountGroupStartPreparation {
    Existing(MarketProviderGroupLifecycleEvidence),
    Ready {
        surface_id: SourceIdentifier,
        prepared: PreparedMarketProviderConfiguration,
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

/// Registry-only proof that one exact published Alpaca group never claimed B3 history ownership.
struct AlpacaHistoricalNeverClaimed {
    group_generation: MarketRuntimeGroupGeneration,
    _private: (),
}

/// Exact history barrier supplied to the published Alpaca runtime finalizer.
enum AlpacaHistoricalPublishedCleanupProof {
    ExactDrain(AlpacaHistoricalDrainReceipt),
    NeverClaimed(AlpacaHistoricalNeverClaimed),
}

/// Registry-only proof that the exact published Kraken group has no Alpaca-history authority.
struct AccountMarketRuntimeNeverApplicable {
    group_generation: MarketRuntimeGroupGeneration,
    _private: (),
}

/// Closed proof set for consuming cleanup of one published account runtime group.
enum AccountMarketRuntimePublishedCleanupProof {
    Alpaca(AlpacaHistoricalPublishedCleanupProof),
    NeverApplicable(AccountMarketRuntimeNeverApplicable),
}

impl MarketRuntimeRegistry {
    pub(crate) fn try_new(
        config: AppConfig,
        provider_rate: ProviderRateAuthority,
        provider_activation: Arc<ProviderAdapterActivation>,
        alpaca_historical_source: AlpacaHistoricalSourceMutationAuthority,
        prepared_configuration: Arc<dyn PreparedMarketProviderConfigurationResolver>,
        live_fair_value: Arc<LiveFairValueObservationBuffer>,
    ) -> Result<Arc<Self>, ServiceError> {
        let mut entries = Vec::new();
        entries
            .try_reserve_exact(MAXIMUM_CONCURRENT_MARKET_SURFACES)
            .map_err(|_error| ServiceError::ResourceExhausted)?;
        let mut account_stop_acknowledgements = Vec::new();
        account_stop_acknowledgements
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
        let registry_incarnation = uuid::Uuid::new_v4();
        if registry_incarnation.is_nil() {
            return Err(ServiceError::Unavailable);
        }
        Ok(Arc::new(Self {
            registry_incarnation,
            config,
            provider_rate,
            provider_activation: RegistryProviderActivation::Production(provider_activation),
            alpaca_historical_source,
            prepared_configuration,
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
            account_stop_acknowledgements: SyncMutex::new(account_stop_acknowledgements),
            account_health_cancellation: CancellationToken::new(),
            account_health_drain: Mutex::new(None),
        }))
    }

    /// Builds the sole non-network registry used by the source-lifecycle stop/retry journey.
    #[cfg(test)]
    pub(crate) fn shutdown_fixture(
        alpaca_historical_source: AlpacaHistoricalSourceMutationAuthority,
        parent: super::AlpacaHistoricalParentGeneration,
        request: PreparedMarketProviderConfigurationRequest,
    ) -> Result<(Arc<Self>, AccountShutdownFixtureProbe), ServiceError> {
        use std::{collections::BTreeMap, ffi::OsString};

        use market_squawk_platform::{ConfigOverrides, ConfigSources};

        let workspace = tempfile::tempdir()
            .map_err(|_error| ServiceError::Unavailable)?
            .keep();
        std::fs::create_dir_all(&workspace).map_err(|_error| ServiceError::Unavailable)?;
        let environment = BTreeMap::<OsString, OsString>::new();
        let config = AppConfig::load(
            ConfigSources::new(None, &environment, ConfigOverrides::default())
                .with_data_directory_default(workspace.clone()),
        )
        .map_err(|_error| ServiceError::Unavailable)?;
        let provider_rate = crate::provider_rate::open_provider_rate_authority(&workspace)
            .map_err(|_error| ServiceError::Unavailable)?;
        let lifecycle = CancellationToken::new();
        let capture_process = market_squawk_platform::initialize_capture_process_infrastructure(
            market_squawk_platform::CaptureProcessInfrastructureLimits::new(
                config.capture_destination_registry_memory_ceiling_bytes(),
            ),
        )
        .map_err(|_error| ServiceError::Unavailable)?;
        let display = DisplayMarketDirectory::try_new(
            NonZeroUsize::new(MAX_DISPLAY_MARKET_ROUTES).ok_or(ServiceError::ResourceExhausted)?,
            lifecycle.child_token(),
        )
        .map_err(|_error| ServiceError::Unavailable)?;
        let order_level = OrderLevelDirectory::try_new(
            NonZeroUsize::new(MAX_ORDER_LEVEL_DIRECTORY_BOOKS)
                .ok_or(ServiceError::ResourceExhausted)?,
            lifecycle.child_token(),
        )
        .map_err(|_error| ServiceError::Unavailable)?;
        let (group, probe) = AccountMarketRuntimeGroup::shutdown_fixture(
            request,
            parent.group_generation(),
            parent,
        )?;
        let surface_id = try_surface_identifier(request.surface())?;
        let entry = MarketRuntimeEntry {
            surface_id,
            onboarding_session_id: Some(request.onboarding_session_id()),
            metadata: Arc::<[SourceMetadata]>::from([]),
            topology: None,
            cancellation: lifecycle.child_token(),
            runtime: MarketRuntime::ActiveAccount(group),
            exports: None,
            action_hooks_installed: false,
        };
        let mut account_stop_acknowledgements = Vec::new();
        account_stop_acknowledgements
            .try_reserve_exact(MAXIMUM_CONCURRENT_MARKET_SURFACES)
            .map_err(|_error| ServiceError::ResourceExhausted)?;
        Ok((
            Arc::new(Self {
                registry_incarnation: uuid::Uuid::new_v4(),
                config,
                provider_rate,
                provider_activation: RegistryProviderActivation::ShutdownFixture,
                alpaca_historical_source,
                prepared_configuration: Arc::new(ShutdownFixtureConfigurationResolver),
                live_fair_value: Arc::new(
                    LiveFairValueObservationBuffer::try_new(
                        NonZeroUsize::new(1).ok_or(ServiceError::ResourceExhausted)?,
                    )
                    .map_err(|_error| ServiceError::Unavailable)?,
                ),
                accepting: std::sync::atomic::AtomicBool::new(true),
                lifecycle,
                capture_process,
                display,
                order_level,
                account_limits: AccountMarketRuntimeLimits::try_v1()?,
                shutdown: Mutex::new(None),
                mutation: Mutex::new(()),
                entries: Mutex::new(vec![entry]),
                account_stop_acknowledgements: SyncMutex::new(account_stop_acknowledgements),
                account_health_cancellation: CancellationToken::new(),
                account_health_drain: Mutex::new(None),
            }),
            probe,
        ))
    }

    /// Publishes one fresh non-network successor through the registry's real surface tombstone.
    #[cfg(test)]
    pub(crate) async fn admit_shutdown_fixture_successor(
        &self,
        parent: super::AlpacaHistoricalParentGeneration,
        request: PreparedMarketProviderConfigurationRequest,
        deadline: Instant,
    ) -> Result<AccountShutdownFixtureProbe, ServiceError> {
        let cancellation = CancellationToken::new();
        let _mutation = bounded_lock(&self.mutation, deadline, &cancellation).await?;
        ensure_active(&self.accepting, deadline, &cancellation)?;
        let surface_id = try_surface_identifier(request.surface())?;
        let mut entries = bounded_lock(&self.entries, deadline, &cancellation).await?;
        if entries.len() == MAXIMUM_CONCURRENT_MARKET_SURFACES
            || entries.iter().any(|entry| entry.surface_id == surface_id)
        {
            return Err(ServiceError::ResourceExhausted);
        }
        self.alpaca_historical_source
            .claim_successor_for_runtime(parent)
            .map_err(|_error| ServiceError::Unavailable)?;
        let (group, probe) = AccountMarketRuntimeGroup::shutdown_fixture(
            request,
            parent.group_generation(),
            parent,
        )?;
        entries.push(MarketRuntimeEntry {
            surface_id,
            onboarding_session_id: Some(request.onboarding_session_id()),
            metadata: Arc::<[SourceMetadata]>::from([]),
            topology: None,
            cancellation: self.lifecycle.child_token(),
            runtime: MarketRuntime::ActiveAccount(group),
            exports: None,
            action_hooks_installed: false,
        });
        Ok(probe)
    }

    /// Installs and retains the real claimed B3 successor for the one shutdown/drop journey.
    #[cfg(test)]
    pub(crate) async fn hold_shutdown_fixture_historical_publication(
        &self,
        parent: super::AlpacaHistoricalParentGeneration,
        request: PreparedMarketProviderConfigurationRequest,
        deadline: Instant,
    ) -> Result<AlpacaHistoricalHeldPublication, ServiceError> {
        let cancellation = CancellationToken::new();
        let _mutation = bounded_lock(&self.mutation, deadline, &cancellation).await?;
        ensure_active(&self.accepting, deadline, &cancellation)?;
        {
            let entries = bounded_lock(&self.entries, deadline, &cancellation).await?;
            let surface_id = try_surface_identifier(request.surface())?;
            let entry = entries
                .iter()
                .find(|entry| entry.surface_id == surface_id)
                .ok_or(ServiceError::NotFound)?;
            let MarketRuntime::ActiveAccount(group) = &entry.runtime else {
                return Err(ServiceError::Unavailable);
            };
            validate_account_evidence(request, group.evidence())?;
            if group.evidence().group_generation() != parent.group_generation()
                || group.historical_parent_claim()
                    != AccountMarketRuntimeHistoryClaim::Alpaca(Some(parent))
            {
                return Err(ServiceError::InvalidRequest);
            }
        }
        activate_shutdown_successor_publication(
            &self.alpaca_historical_source,
            parent,
            deadline,
            &cancellation,
        )
        .await
        .map_err(|_error| ServiceError::Unavailable)
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

    #[cfg(test)]
    pub(crate) async fn is_exact_account_stopping_for_test(
        &self,
        request: PreparedMarketProviderConfigurationRequest,
    ) -> bool {
        let Ok(surface_id) = try_surface_identifier(request.surface()) else {
            return false;
        };
        let entries = self.entries.lock().await;
        entries.iter().any(|entry| {
            entry.surface_id == surface_id
                && entry
                    .account_stopping()
                    .is_some_and(|stopping| stopping.key().matches(request, None))
        })
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
        self: &Arc<Self>,
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
            self.provider_activation.production()?,
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
        self: &Arc<Self>,
        request: PreparedMarketProviderConfigurationRequest,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<AccountGroupStartPreparation, ServiceError> {
        ensure_active(&self.accepting, deadline, cancellation)?;
        let surface_id = try_surface_identifier(request.surface())?;
        match self
            .verify_account_group_owned(request, deadline, cancellation)
            .await
        {
            Ok(Some(evidence)) => return Ok(AccountGroupStartPreparation::Existing(evidence)),
            Ok(None) | Err(ServiceError::Unavailable) => {}
            Err(error) => return Err(error),
        }
        self.remove_unhealthy_account_group_owned(request, deadline, cancellation)
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
        let account_lease = match group.activation_lease() {
            Ok(lease) => lease.clone(),
            Err(error) => {
                let cleanup: Pin<Box<dyn Future<Output = Result<(), ServiceError>> + Send + '_>> =
                    Box::pin(group.shutdown_unpublished_before(deadline, cancellation));
                let _cleanup = cleanup.await;
                return Err(error);
            }
        };
        if let Err(error) = validate_account_lease(request, &account_lease) {
            let cleanup: Pin<Box<dyn Future<Output = Result<(), ServiceError>> + Send + '_>> =
                Box::pin(group.shutdown_unpublished_before(deadline, cancellation));
            let _cleanup = cleanup.await;
            return Err(error);
        }
        let evidence = group.evidence().clone();
        let entry = MarketRuntimeEntry {
            surface_id,
            onboarding_session_id: Some(request.onboarding_session_id()),
            metadata: Arc::<[SourceMetadata]>::from([]),
            topology: None,
            cancellation: runtime_cancellation,
            runtime: MarketRuntime::ActiveAccount(group),
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
                    Box::pin(entry.shutdown_unpublished(self.config.source_shutdown()));
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
                let metadata = composition.source_metadata().map_err(|error| {
                    tracing::error!(
                        provider = ?provider_kind,
                        %error,
                        "market source metadata-set construction failed"
                    );
                    ServiceError::Unavailable
                })?;
                let route_keys = clone_route_keys(composition.live_routes())?;
                let topology =
                    MarketRuntimeTopology::try_new(provider, Arc::clone(&metadata), route_keys)?;
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
                        self.provider_activation.production()?,
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
            let _cleanup = entry
                .shutdown_unpublished(self.config.source_shutdown())
                .await;
            return Err(error);
        }
        if !entry.is_healthy() {
            entry
                .shutdown_unpublished(self.config.source_shutdown())
                .await?;
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
                entry
                    .shutdown_unpublished(self.config.source_shutdown())
                    .await?;
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
                self.shutdown_published_entry(entry, self.config.source_shutdown())
                    .await?;
                Err(ServiceError::Unavailable)
            }
            Err(error) => {
                let cleanup = CancellationToken::new();
                if let Some(entry) = self
                    .take_entry(provider, self.cleanup_deadline()?, &cleanup)
                    .await?
                {
                    self.shutdown_published_entry(entry, self.config.source_shutdown())
                        .await?;
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

    /// Reads only the exact state needed to prepare an account-group stop.
    pub(crate) async fn account_group_stop_state(
        &self,
        request: PreparedMarketProviderConfigurationRequest,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<AccountGroupStopState, ServiceError> {
        ensure_before(deadline, cancellation)?;
        let entries = bounded_lock(&self.entries, deadline, cancellation).await?;
        let Some(entry) = entries
            .iter()
            .find(|entry| entry.surface_id.as_str() == request.surface().surface_id())
        else {
            return Ok(AccountGroupStopState::Absent);
        };
        if entry.onboarding_session_id != Some(request.onboarding_session_id()) {
            return Err(ServiceError::InvalidRequest);
        }
        let state = match &entry.runtime {
            MarketRuntime::ActiveAccount(group) => {
                validate_account_evidence(request, group.evidence())?;
                AccountGroupStopState::Active(group.evidence().clone())
            }
            MarketRuntime::AccountStopping(stopping) => {
                if !stopping.key().matches(request, None) {
                    return Err(ServiceError::InvalidRequest);
                }
                AccountGroupStopState::Stopping(stopping.key().group_generation())
            }
            MarketRuntime::Public(_) | MarketRuntime::CoinbaseDirect(_) => {
                return Err(ServiceError::InvalidRequest);
            }
        };
        ensure_before(deadline, cancellation)?;
        Ok(state)
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
            MarketRuntime::ActiveAccount(group) => group,
            MarketRuntime::AccountStopping(_) => return Err(ServiceError::Unavailable),
            MarketRuntime::Public(_) | MarketRuntime::CoinbaseDirect(_) => {
                return Err(ServiceError::InvalidRequest);
            }
        };
        validate_account_evidence(request, group.evidence())?;
        if group.evidence().group_generation() != expected_group_generation {
            return Err(ServiceError::InvalidRequest);
        }
        let activation_lease = group.activation_lease()?;
        validate_account_lease(request, activation_lease)?;
        ensure_active(&self.accepting, deadline, cancellation)?;
        admission_authority
            .require_active(activation_lease)
            .map_err(|_error| ServiceError::Unauthorized)?;
        ensure_active(&self.accepting, deadline, cancellation)?;
        group.admit_reads()
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
            let MarketRuntime::ActiveAccount(group) = &entry.runtime else {
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
            let MarketRuntime::ActiveAccount(group) = &entry.runtime else {
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
        let mutation = bounded_lock(&self.mutation, deadline, cancellation)
            .await
            .map_err(|_error| AlpacaHistoricalPlanAdmissionError::RuntimeUnavailable)?;
        let (surface_id, capability, parent) = {
            ensure_alpaca_historical_lookup(&self.accepting, deadline, cancellation)
                .map_err(|_error| AlpacaHistoricalPlanAdmissionError::RuntimeUnavailable)?;
            let surface_id = try_surface_identifier(AccountMarketSurface::AlpacaBasic)
                .map_err(|_error| AlpacaHistoricalPlanAdmissionError::RuntimeUnavailable)?;
            let mut entries = bounded_lock(&self.entries, deadline, cancellation)
                .await
                .map_err(|_error| AlpacaHistoricalPlanAdmissionError::RuntimeUnavailable)?;
            let entry = entries
                .iter_mut()
                .find(|entry| entry.surface_id == surface_id)
                .ok_or(AlpacaHistoricalPlanAdmissionError::RuntimeUnavailable)?;
            validate_alpaca_historical_entry(entry, request)
                .map_err(|_error| AlpacaHistoricalPlanAdmissionError::RuntimeUnavailable)?;
            let MarketRuntime::ActiveAccount(group) = &mut entry.runtime else {
                return Err(AlpacaHistoricalPlanAdmissionError::RuntimeUnavailable);
            };
            let capability = group
                .alpaca_historical_capability()
                .map_err(|_error| AlpacaHistoricalPlanAdmissionError::RuntimeUnavailable)?
                .ok_or(AlpacaHistoricalPlanAdmissionError::RuntimeUnavailable)?;
            let parent = self
                .alpaca_historical_source
                .parent_for_runtime(&capability)
                .map_err(|_error| AlpacaHistoricalPlanAdmissionError::RuntimeUnavailable)?;
            let new_market_claim = group
                .claim_alpaca_historical_parent(parent)
                .map_err(|_error| AlpacaHistoricalPlanAdmissionError::RuntimeUnavailable)?;
            if self
                .alpaca_historical_source
                .claim_successor_for_runtime(parent)
                .is_err()
            {
                if new_market_claim {
                    group
                        .rollback_new_alpaca_historical_parent_claim(parent)
                        .map_err(|_error| AlpacaHistoricalPlanAdmissionError::RuntimeUnavailable)?;
                }
                return Err(AlpacaHistoricalPlanAdmissionError::RuntimeUnavailable);
            }
            (surface_id, capability, parent)
        };
        let lease = self
            .alpaca_historical_source
            .install_or_join_runtime(capability.clone(), deadline, cancellation)
            .await
            .map_err(|_error| AlpacaHistoricalPlanAdmissionError::RuntimeUnavailable)?;
        let receipt = lease
            .admit_plan(preflight_plan, canonical_instrument, deadline, cancellation)
            .await?;
        drop(lease);
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
            let MarketRuntime::ActiveAccount(group) = &entry.runtime else {
                return Err(AlpacaHistoricalPlanAdmissionError::RuntimeUnavailable);
            };
            if !group.owns_alpaca_historical_capability(&capability)
                || !receipt.matches_group_generation(group.evidence().group_generation())
                || group.historical_parent_claim()
                    != AccountMarketRuntimeHistoryClaim::Alpaca(Some(parent))
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
            let MarketRuntime::ActiveAccount(group) = &entry.runtime else {
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

    /// Stops one exact account group while retaining its terminal tombstone for durable proof.
    ///
    /// Stage-D lifecycle composition uses the prepare/commit/join methods below so it can persist
    /// the exact key before this physical transition. This compatibility surface never
    /// acknowledges or removes the tombstone.
    pub(crate) async fn stop_account_group(
        self: &Arc<Self>,
        request: PreparedMarketProviderConfigurationRequest,
        expected_group_generation: Option<MarketRuntimeGroupGeneration>,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<Option<MarketRuntimeGroupGeneration>, ServiceError> {
        let Some(ticket) = self
            .begin_or_resume_account_group_stop(
                request,
                expected_group_generation,
                deadline,
                cancellation,
            )
            .await?
        else {
            return Ok(None);
        };
        let generation = ticket.key().group_generation();
        let _receipt = self
            .join_account_group_stop(&ticket, deadline, cancellation)
            .await?;
        // The compatibility caller cannot prove that terminal evidence is durable. Retain the
        // exact tombstone and fail closed until the lifecycle owner resumes through the bridge.
        let _retained_generation = generation;
        Err(ServiceError::Unavailable)
    }

    /// Prepares stable exact key evidence without revoking reads or mutating registry state.
    pub(crate) async fn prepare_account_group_stop(
        &self,
        request: PreparedMarketProviderConfigurationRequest,
        expected_group_generation: Option<MarketRuntimeGroupGeneration>,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<Option<PreparedAccountGroupStop>, ServiceError> {
        let _mutation = bounded_lock(&self.mutation, deadline, cancellation).await?;
        self.prepare_account_group_stop_owned(
            request,
            expected_group_generation,
            deadline,
            cancellation,
        )
        .await
    }

    async fn prepare_account_group_stop_owned(
        &self,
        request: PreparedMarketProviderConfigurationRequest,
        expected_group_generation: Option<MarketRuntimeGroupGeneration>,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<Option<PreparedAccountGroupStop>, ServiceError> {
        ensure_before(deadline, cancellation)?;
        let surface_id = try_surface_identifier(request.surface())?;
        let entries = bounded_lock(&self.entries, deadline, cancellation).await?;
        let Some(entry) = entries.iter().find(|entry| entry.surface_id == surface_id) else {
            return Ok(None);
        };
        let key = if let Some(stopping) = entry.account_stopping() {
            let key = stopping.key();
            if key.registry_incarnation() != self.registry_incarnation
                || !key.matches(request, expected_group_generation)
            {
                return Err(ServiceError::InvalidRequest);
            }
            key
        } else {
            let key = entry.account_shutdown_key(self.registry_incarnation, request)?;
            if expected_group_generation.is_some_and(|expected| expected != key.group_generation())
            {
                return Err(ServiceError::InvalidRequest);
            }
            key
        };
        key.evidence()?;
        Ok(Some(PreparedAccountGroupStop::new(key)))
    }

    /// CAS-validates one previously prepared key before Active -> AccountStopping.
    ///
    /// If another exact caller already committed the same key, this joins the retained owner.
    /// Any absent, successor, request, generation, history, or registry-incarnation mismatch fails
    /// without mutating that entry.
    pub(crate) async fn commit_prepared_account_group_stop(
        self: &Arc<Self>,
        prepared: PreparedAccountGroupStop,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<AccountGroupStopTicket, ServiceError> {
        let _mutation = bounded_lock(&self.mutation, deadline, cancellation).await?;
        self.commit_prepared_account_group_stop_owned(prepared, deadline, cancellation)
            .await
    }

    async fn commit_prepared_account_group_stop_owned(
        self: &Arc<Self>,
        prepared: PreparedAccountGroupStop,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<AccountGroupStopTicket, ServiceError> {
        ensure_before(deadline, cancellation)?;
        let key = prepared.into_key();
        if key.registry_incarnation() != self.registry_incarnation {
            return Err(ServiceError::InvalidRequest);
        }
        key.evidence()?;
        let surface_id = try_surface_identifier(key.request().surface())?;
        let (retained, completion, start_worker) = {
            let mut entries = bounded_lock(&self.entries, deadline, cancellation).await?;
            let index = entries
                .iter()
                .position(|entry| entry.surface_id == surface_id)
                .ok_or(ServiceError::NotFound)?;
            if let Some(stopping) = entries[index].account_stopping() {
                if stopping.key() != key {
                    return Err(ServiceError::InvalidRequest);
                }
                let retained = Arc::clone(stopping);
                let (completion, start_worker) = retained.prepare_drive()?;
                (retained, completion, start_worker)
            } else {
                let current = entries[index]
                    .account_shutdown_key(self.registry_incarnation, key.request())?;
                if current != key {
                    return Err(ServiceError::InvalidRequest);
                }
                let active = entries.remove(index);
                let (tombstone, retained) = active.into_account_stopping(key);
                entries.insert(index, tombstone);
                let (completion, start_worker) = retained.prepare_drive()?;
                (retained, completion, start_worker)
            }
        };
        if start_worker {
            self.spawn_account_group_stop_worker(Arc::clone(&retained), Arc::clone(&completion));
        }
        Ok(AccountGroupStopTicket::new(retained, completion))
    }

    /// Compatibility bridge for existing physical callers; it never acknowledges the tombstone.
    /// Durable lifecycle routing replaces this prepare-then-commit shortcut in the composition
    /// stage, where the key is persisted between these two operations.
    pub(crate) async fn begin_or_resume_account_group_stop(
        self: &Arc<Self>,
        request: PreparedMarketProviderConfigurationRequest,
        expected_group_generation: Option<MarketRuntimeGroupGeneration>,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<Option<AccountGroupStopTicket>, ServiceError> {
        let _mutation = bounded_lock(&self.mutation, deadline, cancellation).await?;
        self.begin_or_resume_account_group_stop_owned(
            request,
            expected_group_generation,
            deadline,
            cancellation,
        )
        .await
    }

    async fn begin_or_resume_account_group_stop_owned(
        self: &Arc<Self>,
        request: PreparedMarketProviderConfigurationRequest,
        expected_group_generation: Option<MarketRuntimeGroupGeneration>,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<Option<AccountGroupStopTicket>, ServiceError> {
        let Some(prepared) = self
            .prepare_account_group_stop_owned(
                request,
                expected_group_generation,
                deadline,
                cancellation,
            )
            .await?
        else {
            return Ok(None);
        };
        self.commit_prepared_account_group_stop_owned(prepared, deadline, cancellation)
            .await
            .map(Some)
    }

    fn spawn_account_group_stop_worker(
        self: &Arc<Self>,
        retained: Arc<RetainedAccountGroupStop>,
        completion: Arc<account_shutdown::AccountShutdownCompletion>,
    ) {
        let registry = Arc::clone(self);
        // Arm before spawning so even an abort before the task's first poll resolves the exact
        // attempt instead of leaving its retained owner in `Driving`.
        let attempt = AccountShutdownAttemptGuard::new(retained, completion);
        tokio::spawn(async move {
            let result = attempt
                .retained()
                .drive(
                    &registry.alpaca_historical_source,
                    registry.config.source_shutdown(),
                )
                .await;
            attempt.finish(result);
        });
    }

    /// Waits for one exact attempt; dropping or timing out this waiter never cancels the worker.
    pub(crate) async fn join_account_group_stop(
        &self,
        ticket: &AccountGroupStopTicket,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<AccountGroupStopReceipt, ServiceError> {
        ticket.wait(deadline, cancellation).await
    }

    /// Reacquires the exact terminal receipt after registry acknowledgement won the race with
    /// durable `TombstoneAcknowledged` persistence in the same registry incarnation.
    pub(crate) async fn reacquire_acknowledged_account_group_stop_receipt(
        &self,
        key_evidence: AccountGroupStopKeyEvidence,
        durable_terminal_proof_digest: EvidenceDigest,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<AccountGroupStopReceipt, ServiceError> {
        let _mutation = bounded_lock(&self.mutation, deadline, cancellation).await?;
        key_evidence.validate()?;
        if key_evidence.registry_incarnation() != self.registry_incarnation {
            return Err(ServiceError::InvalidRequest);
        }
        let surface_id = try_surface_identifier(key_evidence.surface())?;
        {
            let entries = bounded_lock(&self.entries, deadline, cancellation).await?;
            if entries.iter().any(|entry| entry.surface_id == surface_id) {
                return Err(ServiceError::InvalidRequest);
            }
        }
        ensure_before(deadline, cancellation)?;
        let acknowledged = self
            .account_stop_acknowledgements
            .lock()
            .map_err(|_poisoned| ServiceError::Unavailable)?;
        let receipt = acknowledged
            .iter()
            .find(|current| current.matches_surface(key_evidence.surface()))
            .map(|current| current.reacquire_receipt(key_evidence, durable_terminal_proof_digest))
            .transpose()?
            .flatten()
            .ok_or(ServiceError::NotFound)?;
        drop(acknowledged);
        ensure_before(deadline, cancellation)?;
        Ok(receipt)
    }

    /// Removes only the exact terminal tombstone named by completion evidence whose durable proof
    /// coordinate was supplied by the lifecycle owner after its successful proof checkpoint.
    pub(crate) async fn acknowledge_account_group_stop(
        &self,
        receipt: &AccountGroupStopReceipt,
        durable_proof: &AccountGroupStopDurableProof,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<AccountGroupStopAcknowledgementReceipt, ServiceError> {
        let _mutation = bounded_lock(&self.mutation, deadline, cancellation).await?;
        self.acknowledge_account_group_stop_owned(receipt, durable_proof, deadline, cancellation)
            .await
    }

    /// Acknowledges one exact terminal tombstone while the caller retains registry mutation.
    async fn acknowledge_account_group_stop_owned(
        &self,
        receipt: &AccountGroupStopReceipt,
        durable_proof: &AccountGroupStopDurableProof,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<AccountGroupStopAcknowledgementReceipt, ServiceError> {
        let mut entries = bounded_lock(&self.entries, deadline, cancellation).await?;
        let surface = receipt.key().request().surface();
        let surface_id = try_surface_identifier(surface)?;
        let index = entries
            .iter()
            .position(|entry| entry.surface_id == surface_id);
        let Some(index) = index else {
            drop(entries);
            return if self.account_stop_acknowledgement_matches(receipt, durable_proof)? {
                AccountGroupStopAcknowledgementReceipt::try_new(
                    receipt,
                    durable_proof,
                    AccountGroupStopAcknowledgementDisposition::AlreadyAcknowledged,
                )
            } else {
                Err(ServiceError::NotFound)
            };
        };
        let Some(stopping) = entries[index].account_stopping() else {
            drop(entries);
            // An exact successor now owns this surface. Even a valid predecessor receipt must
            // fail closed rather than treating the successor as an idempotent acknowledgement.
            return Err(ServiceError::InvalidRequest);
        };
        if !receipt.matches_retained(stopping) || !stopping.is_complete() {
            return Err(ServiceError::InvalidRequest);
        }
        let acknowledgement =
            AccountGroupStopAcknowledgement::try_new(receipt, durable_proof, stopping)?;
        {
            let mut acknowledged = self
                .account_stop_acknowledgements
                .lock()
                .map_err(|_poisoned| ServiceError::Unavailable)?;
            if let Some(current) = acknowledged
                .iter_mut()
                .find(|current| current.matches_surface(surface))
            {
                *current = acknowledgement;
            } else {
                acknowledged.push(acknowledgement);
            }
        }
        let acknowledged = entries.remove(index);
        drop(entries);
        drop(acknowledged);
        AccountGroupStopAcknowledgementReceipt::try_new(
            receipt,
            durable_proof,
            AccountGroupStopAcknowledgementDisposition::Removed,
        )
    }

    fn account_stop_acknowledgement_matches(
        &self,
        receipt: &AccountGroupStopReceipt,
        durable_proof: &AccountGroupStopDurableProof,
    ) -> Result<bool, ServiceError> {
        let acknowledged = self
            .account_stop_acknowledgements
            .lock()
            .map_err(|_poisoned| ServiceError::Unavailable)?;
        Ok(acknowledged.iter().any(|current| {
            current.matches_surface(receipt.key().request().surface())
                && current.matches_receipt(receipt, durable_proof)
        }))
    }

    /// Removes an account group even when its onboarding lease has expired, while refusing to
    /// reinterpret a scalar public/direct runtime as an account group.
    pub(crate) async fn remove_account_group(
        self: &Arc<Self>,
        surface: AccountMarketSurface,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<Option<MarketRuntimeGroupGeneration>, ServiceError> {
        let _mutation = bounded_lock(&self.mutation, deadline, cancellation).await?;
        let request = {
            let entries = bounded_lock(&self.entries, deadline, cancellation).await?;
            let surface_id = try_surface_identifier(surface)?;
            let Some(entry) = entries.iter().find(|entry| entry.surface_id == surface_id) else {
                return Ok(None);
            };
            let request = entry.prepared_account_request()?;
            if request.surface() != surface {
                return Err(ServiceError::InvalidRequest);
            }
            request
        };
        let ticket = self
            .begin_or_resume_account_group_stop_owned(request, None, deadline, cancellation)
            .await?
            .ok_or(ServiceError::NotFound)?;
        let generation = ticket.key().group_generation();
        drop(_mutation);
        let _receipt = ticket.wait(deadline, cancellation).await?;
        let _retained_generation = generation;
        Err(ServiceError::Unavailable)
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
            self.shutdown_published_entry(entry, self.config.source_shutdown())
                .await?;
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
        let first = self
            .accepting
            .swap(false, std::sync::atomic::Ordering::AcqRel);
        // This is a synchronous fail-closed revocation, not a clean drain. It makes every already
        // held B3 publication view fail its next precommit validation before this method returns.
        self.alpaca_historical_source
            .revoke_for_market_registry_shutdown();
        if first {
            self.prepared_configuration.begin_shutdown();
        }
    }

    pub(crate) async fn finish_shutdown(
        self: &Arc<Self>,
        deadline: Instant,
    ) -> Result<(), ServiceError> {
        self.begin_shutdown();
        let cleanup = CancellationToken::new();
        let mut shutdown = bounded_lock(&self.shutdown, deadline, &cleanup).await?;
        if let Some(result) = *shutdown {
            return result;
        }
        let mut failure = None;
        let account_health_drain =
            match bounded_lock(&self.account_health_drain, deadline, &cleanup).await {
                Ok(mut drain) => drain.take(),
                Err(error) => {
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

        // Move every published account group into its retained tombstone before any account
        // shutdown await. Scalar runtimes remain on their legacy consuming path.
        let mutation = bounded_lock(&self.mutation, deadline, &cleanup).await?;
        let account_requests = {
            let entries = bounded_lock(&self.entries, deadline, &cleanup).await?;
            let mut requests = Vec::new();
            requests
                .try_reserve_exact(entries.len())
                .map_err(|_error| ServiceError::ResourceExhausted)?;
            for entry in entries.iter() {
                if entry.runtime.is_account() {
                    requests.push(entry.prepared_account_request()?);
                }
            }
            requests
        };
        let mut account_tickets = Vec::new();
        account_tickets
            .try_reserve_exact(account_requests.len())
            .map_err(|_error| ServiceError::ResourceExhausted)?;
        for request in account_requests {
            if let Some(ticket) = self
                .begin_or_resume_account_group_stop_owned(request, None, deadline, &cleanup)
                .await?
            {
                account_tickets.push(ticket);
            }
        }
        let scalar_entries = {
            let mut entries = bounded_lock(&self.entries, deadline, &cleanup).await?;
            let mut scalars = Vec::new();
            scalars
                .try_reserve_exact(entries.len())
                .map_err(|_error| ServiceError::ResourceExhausted)?;
            let mut index = 0;
            while index < entries.len() {
                if entries[index].runtime.is_scalar() {
                    let entry = entries.remove(index);
                    entry.begin_shutdown();
                    scalars.push(entry);
                } else {
                    index += 1;
                }
            }
            scalars
        };
        drop(mutation);

        for ticket in account_tickets {
            match ticket.wait(deadline, &cleanup).await {
                Ok(_receipt) if failure.is_none() => {
                    // Product shutdown has not supplied a durable lifecycle proof coordinate.
                    // Retain the terminal tombstone and keep shutdown retryable/fail-closed.
                    failure = Some(ServiceError::Unavailable);
                }
                Ok(_receipt) => {}
                Err(error) if failure.is_none() => failure = Some(error),
                Err(_error) => {}
            }
        }
        for entry in scalar_entries {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                failure.get_or_insert(ServiceError::DeadlineExceeded);
                continue;
            }
            if let Err(error) = await_service_before(
                deadline,
                &cleanup,
                self.shutdown_published_entry_before(entry, deadline, &cleanup),
            )
            .await
                && failure.is_none()
            {
                failure = Some(error);
            }
        }
        if let Some(error) = failure {
            return Err(error);
        }

        self.lifecycle.cancel();
        let display_shutdown = self.display.shutdown(&cleanup, deadline);
        let order_level_shutdown = self.order_level.shutdown(&cleanup, deadline);
        let resolver_shutdown = await_service_before(
            deadline,
            &cleanup,
            self.prepared_configuration.finish_shutdown(deadline),
        );
        let (display_result, order_level_result, resolver_result) =
            tokio::join!(display_shutdown, order_level_shutdown, resolver_shutdown);
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
        let result = failure.map_or(Ok(()), Err);
        if result.is_ok() {
            *shutdown = Some(Ok(()));
        }
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
            let Some(index) = entries.iter().position(|entry| {
                &entry.surface_id == provider && entry.runtime.is_scalar() && !entry.is_healthy()
            }) else {
                return Ok(());
            };
            Some(entries.swap_remove(index))
        };
        if let Some(entry) = entry {
            self.shutdown_published_entry(entry, self.config.source_shutdown())
                .await?;
        }
        Ok(())
    }

    async fn remove_unhealthy_account_group_owned(
        self: &Arc<Self>,
        request: PreparedMarketProviderConfigurationRequest,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<(), ServiceError> {
        let surface_id = try_surface_identifier(request.surface())?;
        {
            let entries = bounded_lock(&self.entries, deadline, cancellation).await?;
            let Some(entry) = entries.iter().find(|entry| entry.surface_id == surface_id) else {
                return Ok(());
            };
            match &entry.runtime {
                MarketRuntime::ActiveAccount(group) => {
                    validate_account_evidence(request, group.evidence())?;
                    if entry.is_healthy() {
                        return Err(ServiceError::Unavailable);
                    }
                }
                MarketRuntime::AccountStopping(stopping)
                    if stopping.key().matches(request, None) => {}
                MarketRuntime::AccountStopping(_)
                | MarketRuntime::Public(_)
                | MarketRuntime::CoinbaseDirect(_) => {
                    return Err(ServiceError::InvalidRequest);
                }
            }
        }
        let Some(ticket) = self
            .begin_or_resume_account_group_stop_owned(request, None, deadline, cancellation)
            .await?
        else {
            return Ok(());
        };
        let _receipt = ticket.wait(deadline, cancellation).await?;
        // The lifecycle owner must persist proof before it can acknowledge this tombstone.
        Err(ServiceError::Unavailable)
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
        let Some(index) = entries
            .iter()
            .position(|entry| &entry.surface_id == provider)
        else {
            return Ok(None);
        };
        if !entries[index].runtime.is_scalar() {
            return Err(ServiceError::InvalidRequest);
        }
        Ok(Some(entries.swap_remove(index)))
    }

    pub(crate) fn cleanup_deadline(&self) -> Result<Instant, ServiceError> {
        Instant::now()
            .checked_add(self.config.source_shutdown())
            .ok_or(ServiceError::Unavailable)
    }

    async fn shutdown_published_entry(
        &self,
        entry: MarketRuntimeEntry,
        shutdown_budget: Duration,
    ) -> Result<(), ServiceError> {
        let deadline = Instant::now()
            .checked_add(shutdown_budget)
            .ok_or(ServiceError::Unavailable)?;
        self.shutdown_published_entry_before(entry, deadline, &CancellationToken::new())
            .await
    }

    /// Consumes one scalar entry already removed under the registry mutation lock. Published
    /// account groups can never cross this boundary; they remain in `AccountStopping`.
    async fn shutdown_published_entry_before(
        &self,
        entry: MarketRuntimeEntry,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<(), ServiceError> {
        if !entry.runtime.is_scalar() {
            return Err(ServiceError::InvalidRequest);
        }
        entry
            .shutdown_published_non_account_before(deadline, cancellation)
            .await
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
            authority = self.provider_activation.production()?.acquire_account_market_runtime_mutation_authority() => {
                Ok(authority)
            }
        }
    }

    async fn first_unhealthy_account_group(&self) -> Option<AccountMarketRuntimeHealthSnapshot> {
        let entries = self.entries.try_lock().ok()?;
        entries.iter().find_map(|entry| match &entry.runtime {
            MarketRuntime::ActiveAccount(group) if !entry.is_healthy() => {
                Some(AccountMarketRuntimeHealthSnapshot {
                    request: entry.prepared_account_request().ok()?,
                    group_generation: group.evidence().group_generation(),
                })
            }
            MarketRuntime::AccountStopping(stopping) => Some(AccountMarketRuntimeHealthSnapshot {
                request: stopping.key().request(),
                group_generation: stopping.key().group_generation(),
            }),
            MarketRuntime::ActiveAccount(_)
            | MarketRuntime::Public(_)
            | MarketRuntime::CoinbaseDirect(_) => None,
        })
    }

    async fn drain_account_group_generation(
        self: &Arc<Self>,
        snapshot: &AccountMarketRuntimeHealthSnapshot,
    ) -> Result<(), ServiceError> {
        let deadline = Instant::now()
            .checked_add(self.config.source_shutdown())
            .ok_or(ServiceError::Unavailable)?;
        let _mutation = tokio::select! {
            biased;
            () = self.account_health_cancellation.cancelled() => return Ok(()),
            mutation = self.mutation.lock() => mutation,
        };
        let Some(ticket) = self
            .begin_or_resume_account_group_stop_owned(
                snapshot.request,
                Some(snapshot.group_generation),
                deadline,
                &self.account_health_cancellation,
            )
            .await?
        else {
            return Ok(());
        };
        drop(_mutation);
        let _receipt = ticket
            .wait(deadline, &self.account_health_cancellation)
            .await?;
        // Health detection reports the retained transaction to the durable lifecycle owner in
        // composition; this leaf cannot acknowledge without that proof.
        Err(ServiceError::Unavailable)
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
    topology: Option<MarketRuntimeTopology>,
    cancellation: CancellationToken,
    runtime: MarketRuntime,
    exports: Option<LiveFairValueExportDrains>,
    action_hooks_installed: bool,
}

enum MarketRuntimeEntryShutdownMode {
    Unpublished,
    PublishedNonAccount,
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
                MarketRuntime::ActiveAccount(group) => group.is_published_healthy(),
                MarketRuntime::AccountStopping(_) => false,
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

    fn prepared_account_request(
        &self,
    ) -> Result<PreparedMarketProviderConfigurationRequest, ServiceError> {
        match &self.runtime {
            MarketRuntime::ActiveAccount(group) => {
                let evidence = group.evidence();
                let surface = AccountMarketSurface::parse(evidence.surface_id().as_str())
                    .ok_or(ServiceError::InvalidRequest)?;
                PreparedMarketProviderConfigurationRequest::try_new(
                    surface,
                    evidence.onboarding_session_id(),
                    evidence.public_configuration_digest(),
                    evidence.runtime_verification_receipt_digest(),
                    evidence.credential_generation(),
                )
            }
            MarketRuntime::AccountStopping(stopping) => Ok(stopping.key().request()),
            MarketRuntime::Public(_) | MarketRuntime::CoinbaseDirect(_) => {
                Err(ServiceError::InvalidRequest)
            }
        }
    }

    fn account_shutdown_key(
        &self,
        registry_incarnation: uuid::Uuid,
        request: PreparedMarketProviderConfigurationRequest,
    ) -> Result<AccountShutdownKey, ServiceError> {
        if self.exports.is_some() || self.action_hooks_installed {
            return Err(ServiceError::InvalidRequest);
        }
        let MarketRuntime::ActiveAccount(group) = &self.runtime else {
            return Err(ServiceError::InvalidRequest);
        };
        AccountShutdownKey::try_from_active(
            registry_incarnation,
            request,
            group.evidence(),
            group.historical_parent_claim(),
        )
    }

    fn into_account_stopping(
        self,
        key: AccountShutdownKey,
    ) -> (Self, Arc<RetainedAccountGroupStop>) {
        self.begin_shutdown();
        let Self {
            surface_id,
            onboarding_session_id,
            metadata,
            topology,
            cancellation,
            runtime,
            exports,
            action_hooks_installed,
        } = self;
        let MarketRuntime::ActiveAccount(group) = runtime else {
            unreachable!("account shutdown key is issued only for an active account entry");
        };
        let owner = group.into_published_stopping_owner();
        let retained = RetainedAccountGroupStop::new(key, owner);
        let tombstone = Self {
            surface_id,
            onboarding_session_id,
            metadata,
            topology,
            cancellation,
            runtime: MarketRuntime::AccountStopping(Arc::clone(&retained)),
            exports,
            action_hooks_installed,
        };
        (tombstone, retained)
    }

    fn account_stopping(&self) -> Option<&Arc<RetainedAccountGroupStop>> {
        match &self.runtime {
            MarketRuntime::AccountStopping(stopping) => Some(stopping),
            MarketRuntime::Public(_)
            | MarketRuntime::CoinbaseDirect(_)
            | MarketRuntime::ActiveAccount(_) => None,
        }
    }

    async fn shutdown_unpublished(
        self,
        shutdown_budget: std::time::Duration,
    ) -> Result<(), ServiceError> {
        let deadline = Instant::now()
            .checked_add(shutdown_budget)
            .ok_or(ServiceError::Unavailable)?;
        let cleanup = CancellationToken::new();
        self.shutdown_unpublished_before(deadline, &cleanup).await
    }

    async fn shutdown_unpublished_before(
        self,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<(), ServiceError> {
        self.shutdown_before(
            MarketRuntimeEntryShutdownMode::Unpublished,
            deadline,
            cancellation,
        )
        .await
    }

    async fn shutdown_published_non_account_before(
        self,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<(), ServiceError> {
        self.shutdown_before(
            MarketRuntimeEntryShutdownMode::PublishedNonAccount,
            deadline,
            cancellation,
        )
        .await
    }

    async fn shutdown_before(
        mut self,
        mode: MarketRuntimeEntryShutdownMode,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<(), ServiceError> {
        self.begin_shutdown();
        let runtime_result = self
            .runtime
            .shutdown_before(mode, deadline, cancellation)
            .await;
        let export_result = match self.exports.take() {
            Some(exports) => exports
                .finish_before(deadline, cancellation)
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
    ActiveAccount(AccountMarketRuntimeGroup),
    AccountStopping(Arc<RetainedAccountGroupStop>),
}

impl MarketRuntime {
    const fn is_account(&self) -> bool {
        matches!(self, Self::ActiveAccount(_) | Self::AccountStopping(_))
    }

    const fn is_scalar(&self) -> bool {
        matches!(self, Self::Public(_) | Self::CoinbaseDirect(_))
    }

    fn is_healthy(&self) -> bool {
        match self {
            Self::Public(runtime) => runtime.is_healthy(),
            Self::CoinbaseDirect(runtime) => runtime.is_healthy(),
            Self::ActiveAccount(runtime) => runtime.is_healthy(),
            Self::AccountStopping(_) => false,
        }
    }

    fn begin_shutdown(&self) {
        match self {
            Self::ActiveAccount(runtime) => runtime.begin_shutdown(),
            Self::AccountStopping(_) => {}
            Self::Public(_) | Self::CoinbaseDirect(_) => {}
        }
    }

    const fn has_scalar_snapshots(&self) -> bool {
        match self {
            Self::Public(_) | Self::CoinbaseDirect(_) => true,
            Self::ActiveAccount(_) | Self::AccountStopping(_) => false,
        }
    }

    fn scalar_snapshots(&self) -> Result<LiveSnapshotReader, ServiceError> {
        match self {
            Self::Public(runtime) => Ok(runtime.snapshots()),
            Self::CoinbaseDirect(runtime) => Ok(runtime.snapshots()),
            Self::ActiveAccount(_) | Self::AccountStopping(_) => Err(ServiceError::InvalidRequest),
        }
    }

    fn account_evidence(&self) -> Option<&MarketProviderGroupLifecycleEvidence> {
        match self {
            Self::ActiveAccount(runtime) => Some(runtime.evidence()),
            Self::AccountStopping(_) => None,
            Self::Public(_) | Self::CoinbaseDirect(_) => None,
        }
    }

    fn display_descriptor_count(&self) -> usize {
        match self {
            Self::ActiveAccount(runtime) => runtime.display_descriptor_count(),
            Self::AccountStopping(_) => 0,
            Self::Public(_) | Self::CoinbaseDirect(_) => 0,
        }
    }

    fn display_instrument_count(&self) -> Option<usize> {
        match self {
            Self::ActiveAccount(runtime) => runtime.display_instrument_count(),
            Self::AccountStopping(_) => Some(0),
            Self::Public(_) | Self::CoinbaseDirect(_) => Some(0),
        }
    }

    fn market_instrument_count(&self) -> Option<usize> {
        match self {
            Self::ActiveAccount(runtime) => runtime.market_instrument_count(),
            Self::AccountStopping(_) => Some(0),
            Self::Public(_) | Self::CoinbaseDirect(_) => Some(0),
        }
    }

    fn owns_display_descriptor(&self, descriptor: &Arc<DisplaySourceDescriptor>) -> bool {
        match self {
            Self::ActiveAccount(runtime) => runtime.owns_display_descriptor(descriptor),
            Self::AccountStopping(_) => false,
            Self::Public(_) | Self::CoinbaseDirect(_) => false,
        }
    }

    fn append_display_instrument_ids(&self, destination: &mut Vec<InstrumentId>) {
        if let Self::ActiveAccount(runtime) = self {
            runtime.append_display_instrument_ids(destination);
        }
    }

    fn append_market_instrument_ids(&self, destination: &mut Vec<InstrumentId>) {
        if let Self::ActiveAccount(runtime) = self {
            runtime.append_market_instrument_ids(destination);
        }
    }

    fn kraken_read_authority(
        &self,
        instrument_id: InstrumentId,
    ) -> Option<(Arc<KrakenSourceDescriptor>, OrderLevelBookKey)> {
        match self {
            Self::ActiveAccount(runtime) => runtime.kraken_read_authority(instrument_id),
            Self::AccountStopping(_) => None,
            Self::Public(_) | Self::CoinbaseDirect(_) => None,
        }
    }

    fn owns_kraken_descriptor(&self, descriptor: &Arc<KrakenSourceDescriptor>) -> bool {
        match self {
            Self::ActiveAccount(runtime) => runtime.owns_kraken_descriptor(descriptor),
            Self::AccountStopping(_) => false,
            Self::Public(_) | Self::CoinbaseDirect(_) => false,
        }
    }

    fn append_display_descriptors(&self, destination: &mut Vec<Arc<DisplaySourceDescriptor>>) {
        if let Self::ActiveAccount(runtime) = self {
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
            Self::ActiveAccount(_) | Self::AccountStopping(_) => Err(ServiceError::InvalidRequest),
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
            Self::ActiveAccount(_) | Self::AccountStopping(_) => Err(ServiceError::InvalidRequest),
        }
    }

    async fn shutdown_before(
        self,
        mode: MarketRuntimeEntryShutdownMode,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<(), ServiceError> {
        match (self, mode) {
            (
                Self::Public(runtime),
                MarketRuntimeEntryShutdownMode::Unpublished
                | MarketRuntimeEntryShutdownMode::PublishedNonAccount,
            ) => await_before(deadline, cancellation, runtime.shutdown()).await,
            (
                Self::CoinbaseDirect(runtime),
                MarketRuntimeEntryShutdownMode::Unpublished
                | MarketRuntimeEntryShutdownMode::PublishedNonAccount,
            ) => await_before(deadline, cancellation, runtime.shutdown()).await,
            (Self::ActiveAccount(runtime), MarketRuntimeEntryShutdownMode::Unpublished) => {
                runtime
                    .shutdown_unpublished_before(deadline, cancellation)
                    .await
            }
            (
                Self::ActiveAccount(_) | Self::AccountStopping(_),
                MarketRuntimeEntryShutdownMode::PublishedNonAccount,
            )
            | (Self::AccountStopping(_), MarketRuntimeEntryShutdownMode::Unpublished) => {
                Err(ServiceError::InvalidRequest)
            }
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
                surface = snapshot.request.surface().surface_id(),
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
