//! Validated platform-to-provider production composition.

use market_squawk_adapter_coinbase::{
    CoinbaseChannel, CoinbaseConfigError, CoinbaseExchangeConfig, CoinbaseExchangeDecoder,
    CoinbaseExchangeSource, CoinbaseTransportLimits,
};
use market_squawk_domain::{
    DigestAlgorithm, EvidenceDigest, ExactPayloadEvidence, IdentityError, InstrumentDefinition,
    MetadataRevision, RevisionBoundPayloadEvidence, SourceId, SourceIdentifier, Timestamp,
};
use market_squawk_live::{
    LiveRouteConfig, LiveRuntimeConfig, LiveSnapshotReader, RouteActionHook,
    RouteQualifiedMarketExport, ShardKey,
};
use market_squawk_platform::{
    AppConfig, CaptureProcessInfrastructure, CaptureProcessInfrastructureLimits,
    CoinbaseAuthorizationAttestation, CoinbaseSourceConfig,
    DestinationFenceRegistryInitializationError, LocalPaths, PathError,
    initialize_capture_process_infrastructure,
};
use market_squawk_sources::{
    AuthorizationGrant, AuthorizationMode, BackoffPolicy, BudgetScope, FreshnessPolicy,
    LiveSourceGeneration, NetworkPolicyError, ProviderBudgetPolicy, ProviderRateAuthority,
    SourceError, SourceMetadata, SourceMetadataError,
};
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::{
    num::{NonZeroU16, NonZeroU32, NonZeroU64},
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use thiserror::Error;
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;

use super::super::live_runtime::{LiveRuntimeComposition, LiveRuntimeCompositionError};
use super::super::provider_rate::open_provider_rate_authority;
use super::instruments::ProductionInstrumentSet;
use super::kraken::{
    KrakenPublicChannel, KrakenPublicCurrentnessObserver, KrakenPublicSupervisorSet,
    KrakenPublicSupervisorSetError, ProductionKrakenProfileError, ProductionKrakenProfileSet,
};
use super::provider::{ProductionProviderError, ProductionSourceProfile, ProductionSourceProvider};
use super::route_actor::RouteBufferLimits;
use super::supervisor::{ProductionSourceSupervisor, ProductionSupervisorError};

const SOURCE_ID: &str = "coinbase-exchange-public";
const PROVISIONAL_METADATA_REVISION: &str = "coinbase-advanced-trade-v1-provisional";
const COINBASE_PROVIDER: &str = "coinbase-exchange";
const IMPLEMENTATION_PROFILE_VERSION: &str = "coinbase-advanced-trade-v1-profile-2026-08-08";
const CONFIGURATION_EVIDENCE_DOMAIN: &[u8] =
    b"market-squawk/coinbase-production-configuration/v2\0";
const PROFILE_EVIDENCE_DOMAIN: &[u8] = b"market-squawk/coinbase-production-profile/v2\0";
const REQUESTS_PER_WINDOW: u32 = 1;
const REQUEST_WINDOW_NANOS: u64 = 1_000_000_000;
const MAX_CONCURRENT_REQUESTS: u16 = 1;
const INITIAL_BACKOFF_NANOS: u64 = 250_000_000;
const MAXIMUM_BACKOFF_NANOS: u64 = 30_000_000_000;
const BACKOFF_JITTER_BASIS_POINTS: u16 = 2_000;
const MAX_CLOCK_SKEW_NANOS: u64 = 1_000_000_000;
const PRE_ACKNOWLEDGEMENT_DATA_MESSAGE_CAPACITY: usize = 64;
const PRE_ACKNOWLEDGEMENT_DATA_BYTE_CAPACITY: usize = 32 * 1024 * 1024;

/// Validated, connector-sealed production Coinbase composition.
///
/// The caller supplies bounded live-route resources, but cannot replace the provider connector,
/// endpoint, decoder, metadata, authorization evidence, or quality ceiling. Starting the owned
/// runtime is deliberately separate so validation can be inspected before any network access.
#[derive(Debug)]
pub struct ProductionLiveSourceComposition {
    config: AppConfig,
    installation: ProductionSourceInstallation,
    routes: Vec<LiveRouteConfig>,
    provider_rate: ProviderRateAuthority,
}

/// Sealed source topology admitted by one public-provider composition.
///
/// Kraken is intentionally not represented as a generic optional companion. Its book and trade
/// profiles form one required pair with independent metadata, capture authority, and lifecycle.
#[derive(Debug)]
enum ProductionSourceInstallation {
    Single(ProductionSourceProfile),
    Kraken {
        book: ProductionSourceProfile,
        trades: ProductionSourceProfile,
    },
}

impl ProductionSourceInstallation {
    fn primary(&self) -> &ProductionSourceProfile {
        match self {
            Self::Single(profile) | Self::Kraken { book: profile, .. } => profile,
        }
    }

    #[cfg(all(test, debug_assertions))]
    fn with_local_kraken_endpoint_for_test(
        self,
        endpoint: &str,
    ) -> Result<Self, ProductionProviderError> {
        let Self::Kraken { book, trades } = self else {
            return Err(ProductionProviderError::TestConnectorMismatch);
        };
        Ok(Self::Kraken {
            book: book.with_local_kraken_endpoint_for_test(endpoint)?,
            trades: trades.with_local_kraken_endpoint_for_test(endpoint)?,
        })
    }
}

impl ProductionLiveSourceComposition {
    /// Validates the exact configured instrument set against complete live-runtime routes.
    ///
    /// # Errors
    ///
    /// Returns a typed error when Coinbase is absent, the production provider profile is invalid,
    /// or routes omit, duplicate, add, or alter a configured instrument definition.
    pub fn try_new(
        config: AppConfig,
        routes: Vec<LiveRouteConfig>,
    ) -> Result<Self, ProductionLiveSourceCompositionError> {
        Self::try_for_provider(config, routes, ProductionSourceProvider::Coinbase)
    }

    /// Validates and seals one explicitly selected production provider.
    ///
    /// # Errors
    ///
    /// Rejects an absent selected profile, route mismatch, or any provider/profile invariant
    /// failure before capture, live actors, or networking start.
    pub fn try_for_provider(
        config: AppConfig,
        routes: Vec<LiveRouteConfig>,
        provider: ProductionSourceProvider,
    ) -> Result<Self, ProductionLiveSourceCompositionError> {
        let paths = LocalPaths::prepare(config.data_dir())?;
        let provider_rate = open_provider_rate_authority(paths.control_root()?.root())?;
        Self::try_for_provider_with_rate_authority(config, routes, provider, provider_rate)
    }

    pub(crate) fn try_for_provider_with_rate_authority(
        config: AppConfig,
        routes: Vec<LiveRouteConfig>,
        provider: ProductionSourceProvider,
        provider_rate: ProviderRateAuthority,
    ) -> Result<Self, ProductionLiveSourceCompositionError> {
        let installation = match provider {
            ProductionSourceProvider::Coinbase => {
                let source = config
                    .coinbase()
                    .ok_or(ProductionLiveSourceCompositionError::MissingCoinbaseConfiguration)?;
                validate_coinbase_routes(source, &routes)?;
                ProductionSourceInstallation::Single(ProductionSourceProfile::coinbase(
                    ProductionCoinbaseProfile::try_from(source)?,
                    source,
                    PRE_ACKNOWLEDGEMENT_DATA_MESSAGE_CAPACITY,
                    PRE_ACKNOWLEDGEMENT_DATA_BYTE_CAPACITY,
                )?)
            }
            ProductionSourceProvider::Kraken => {
                let source = config
                    .kraken()
                    .ok_or(ProductionLiveSourceCompositionError::MissingKrakenConfiguration)?;
                validate_kraken_routes(source, &routes)?;
                let [book, trades] =
                    ProductionKrakenProfileSet::try_from_config(source)?.into_channels();
                ProductionSourceInstallation::Kraken {
                    book: ProductionSourceProfile::kraken(book, source),
                    trades: ProductionSourceProfile::kraken(trades, source),
                }
            }
        };
        Ok(Self {
            config,
            installation,
            routes,
            provider_rate,
        })
    }

    /// Returns the only provider endpoint accepted by the sealed production adapter.
    pub fn endpoint(&self) -> &str {
        self.installation.primary().endpoint()
    }

    /// Returns every exact source-metadata record installed by this composition.
    ///
    /// The ordering is closed and deterministic: a single-source provider contributes one record,
    /// while Kraken contributes `[book, trades]`. Consumers must retain the complete set when
    /// joining native stream snapshots to source provenance.
    pub fn source_metadata(
        &self,
    ) -> Result<Arc<[SourceMetadata]>, ProductionLiveSourceCompositionError> {
        let capacity = match &self.installation {
            ProductionSourceInstallation::Single(_) => 1,
            ProductionSourceInstallation::Kraken { .. } => 2,
        };
        let mut metadata = Vec::new();
        metadata
            .try_reserve_exact(capacity)
            .map_err(|_error| ProductionLiveSourceCompositionError::SourceMetadataAllocation)?;
        match &self.installation {
            ProductionSourceInstallation::Single(profile) => {
                metadata.push(profile.metadata().clone());
            }
            ProductionSourceInstallation::Kraken { book, trades } => {
                metadata.push(book.metadata().clone());
                metadata.push(trades.metadata().clone());
            }
        }
        Ok(metadata.into())
    }

    /// Returns the source whose quote/book observations feed the bounded fair-value export.
    ///
    /// This deliberately does not describe the complete installed topology. Public provenance
    /// consumers must use [`Self::source_metadata`]. Kraken trade observations remain available
    /// through the native market snapshot rather than the quote/book fair-value export.
    pub(crate) fn qualified_market_export_source_id(&self) -> &SourceId {
        self.installation.primary().metadata().source_id()
    }

    /// Returns the complete validated route set that will be reserved before network access.
    pub fn routes(&self) -> &[LiveRouteConfig] {
        &self.routes
    }

    pub(crate) fn validate_qualified_market_export_routes(
        &self,
        qualified_market_exports: &[RouteQualifiedMarketExport],
    ) -> Result<(), ProductionLiveSourceRuntimeError> {
        for (index, export) in qualified_market_exports.iter().enumerate() {
            if qualified_market_exports[index.saturating_add(1)..]
                .iter()
                .any(|other| other.route() == export.route())
            {
                return Err(
                    ProductionLiveSourceRuntimeError::DuplicateQualifiedMarketExportRoute {
                        route: export.route().clone(),
                    },
                );
            }
        }
        if self.routes.len() != qualified_market_exports.len()
            || self.routes.iter().any(|route| {
                !qualified_market_exports
                    .iter()
                    .any(|export| export.route() == route.route())
            })
        {
            return Err(ProductionLiveSourceRuntimeError::QualifiedMarketExportRouteSetMismatch);
        }
        Ok(())
    }

    #[cfg(all(test, debug_assertions))]
    pub(crate) fn with_local_kraken_endpoint_for_test(
        mut self,
        endpoint: &str,
    ) -> Result<Self, ProductionLiveSourceCompositionError> {
        self.installation = self
            .installation
            .with_local_kraken_endpoint_for_test(endpoint)?;
        Ok(self)
    }

    /// Starts the bounded live runtime and exact sealed provider-supervisor topology.
    ///
    /// The method returns only after durable registry admission, capture-writer startup, and every
    /// dormant route reservation succeed. Only the connector or required connector set retained
    /// by this closed composition can be opened by the returned owner.
    ///
    /// # Errors
    ///
    /// Returns a typed startup or rollback failure without leaving an unowned live runtime.
    pub async fn start(
        self,
        runtime_config: LiveRuntimeConfig,
        cancellation: CancellationToken,
    ) -> Result<ProductionLiveSourceRuntime, ProductionLiveSourceRuntimeError> {
        let route_buffer_limits = RouteBufferLimits::new(
            runtime_config.mailbox_count_per_shard(),
            runtime_config.maximum_message_bytes(),
        );
        let paths = LocalPaths::prepare(self.config.data_dir())?;
        let capture_process =
            initialize_capture_process_infrastructure(CaptureProcessInfrastructureLimits::new(
                self.config
                    .capture_destination_registry_memory_ceiling_bytes(),
            ))?;
        let live = LiveRuntimeComposition::start(runtime_config, self.routes.clone()).await?;
        self.start_on_live_runtime(
            live,
            route_buffer_limits,
            paths,
            capture_process,
            cancellation,
        )
        .await
    }

    /// Starts the sealed source only after every route has transferred its execution action hook.
    ///
    /// This is the production paper/live boundary. Hook admission and live actor startup complete
    /// before source supervision can open the provider connection.
    ///
    /// # Errors
    ///
    /// Returns a typed startup or rollback failure without leaving an unowned source or live
    /// runtime.
    pub async fn start_with_action_hooks(
        self,
        runtime_config: LiveRuntimeConfig,
        action_hooks: Vec<RouteActionHook>,
        cancellation: CancellationToken,
    ) -> Result<ProductionLiveSourceRuntime, ProductionLiveSourceRuntimeError> {
        let route_buffer_limits = RouteBufferLimits::new(
            runtime_config.mailbox_count_per_shard(),
            runtime_config.maximum_message_bytes(),
        );
        let paths = LocalPaths::prepare(self.config.data_dir())?;
        let capture_process =
            initialize_capture_process_infrastructure(CaptureProcessInfrastructureLimits::new(
                self.config
                    .capture_destination_registry_memory_ceiling_bytes(),
            ))?;
        let live = LiveRuntimeComposition::start_with_action_hooks(
            runtime_config,
            self.routes.clone(),
            action_hooks,
        )
        .await?;
        self.start_on_live_runtime(
            live,
            route_buffer_limits,
            paths,
            capture_process,
            cancellation,
        )
        .await
    }

    /// Starts the sealed source with exact action hooks and one bounded export for every route.
    ///
    /// Route-set validation completes before local-path or capture initialization. Live-runtime
    /// startup retains the complete export memory reservation and transfers every sender to its
    /// exact route before source supervision can open the provider connection.
    ///
    /// # Errors
    ///
    /// Returns a typed route, startup, or rollback failure without leaving an unowned source,
    /// export sender, or live runtime.
    pub async fn start_with_action_hooks_and_qualified_market_exports(
        self,
        runtime_config: LiveRuntimeConfig,
        action_hooks: Vec<RouteActionHook>,
        qualified_market_exports: Vec<RouteQualifiedMarketExport>,
        cancellation: CancellationToken,
    ) -> Result<ProductionLiveSourceRuntime, ProductionLiveSourceRuntimeError> {
        self.validate_qualified_market_export_routes(&qualified_market_exports)?;
        let route_buffer_limits = RouteBufferLimits::new(
            runtime_config.mailbox_count_per_shard(),
            runtime_config.maximum_message_bytes(),
        );
        let paths = LocalPaths::prepare(self.config.data_dir())?;
        let capture_process =
            initialize_capture_process_infrastructure(CaptureProcessInfrastructureLimits::new(
                self.config
                    .capture_destination_registry_memory_ceiling_bytes(),
            ))?;
        let live = LiveRuntimeComposition::start_with_action_hooks_and_qualified_market_exports(
            runtime_config,
            self.routes.clone(),
            action_hooks,
            qualified_market_exports,
        )
        .await?;
        self.start_on_live_runtime(
            live,
            route_buffer_limits,
            paths,
            capture_process,
            cancellation,
        )
        .await
    }

    /// Starts the sealed source with one bounded qualified-market export per route and no
    /// execution authority.
    ///
    /// This is the production market-data path for dashboard, research, and valuation consumers.
    /// The complete route set is validated before local resources or provider networking start.
    pub async fn start_with_qualified_market_exports(
        self,
        runtime_config: LiveRuntimeConfig,
        qualified_market_exports: Vec<RouteQualifiedMarketExport>,
        cancellation: CancellationToken,
    ) -> Result<ProductionLiveSourceRuntime, ProductionLiveSourceRuntimeError> {
        self.validate_qualified_market_export_routes(&qualified_market_exports)?;
        let route_buffer_limits = RouteBufferLimits::new(
            runtime_config.mailbox_count_per_shard(),
            runtime_config.maximum_message_bytes(),
        );
        let paths = LocalPaths::prepare(self.config.data_dir())?;
        let capture_process =
            initialize_capture_process_infrastructure(CaptureProcessInfrastructureLimits::new(
                self.config
                    .capture_destination_registry_memory_ceiling_bytes(),
            ))?;
        let live = LiveRuntimeComposition::start_with_qualified_market_exports(
            runtime_config,
            self.routes.clone(),
            qualified_market_exports,
        )
        .await?;
        self.start_on_live_runtime(
            live,
            route_buffer_limits,
            paths,
            capture_process,
            cancellation,
        )
        .await
    }

    async fn start_on_live_runtime(
        self,
        live: LiveRuntimeComposition,
        route_buffer_limits: RouteBufferLimits,
        paths: LocalPaths,
        capture_process: CaptureProcessInfrastructure,
        cancellation: CancellationToken,
    ) -> Result<ProductionLiveSourceRuntime, ProductionLiveSourceRuntimeError> {
        let Self {
            config,
            installation,
            routes: configured_routes,
            provider_rate,
        } = self;
        let routes = configured_routes
            .iter()
            .map(|route| route.route().clone())
            .collect::<Vec<_>>();
        let source_shutdown = config.source_shutdown();
        let ingress = live.production_ingress();
        let owner = match installation {
            ProductionSourceInstallation::Single(profile) => {
                match ProductionSourceSupervisor::try_new_with_provider_rate(
                    &config,
                    profile,
                    paths,
                    capture_process,
                    ingress,
                    routes,
                    route_buffer_limits,
                    provider_rate,
                ) {
                    Ok(supervisor) => {
                        ProductionSupervisorOwner::start_single(supervisor, cancellation).await
                    }
                    Err(error) => Err(ProductionLiveSourceRuntimeError::Supervisor(error)),
                }
            }
            ProductionSourceInstallation::Kraken { book, trades } => {
                match KrakenPublicCurrentnessObserver::try_new(
                    live.snapshots(),
                    &routes,
                    book.metadata().source_id().clone(),
                    trades.metadata().source_id().clone(),
                ) {
                    Err(error) => Err(map_kraken_supervisor_error(error)),
                    Ok(currentness) => {
                        let book_supervisor =
                            ProductionSourceSupervisor::try_new_with_provider_rate(
                                &config,
                                book,
                                paths.clone(),
                                capture_process,
                                ingress.clone(),
                                routes.clone(),
                                route_buffer_limits,
                                provider_rate.clone(),
                            )
                            .map_err(ProductionLiveSourceRuntimeError::Supervisor);
                        match book_supervisor {
                            Err(error) => Err(error),
                            Ok(book_supervisor) => {
                                let trade_supervisor =
                                    ProductionSourceSupervisor::try_new_with_provider_rate(
                                        &config,
                                        trades,
                                        paths,
                                        capture_process,
                                        ingress,
                                        routes,
                                        route_buffer_limits,
                                        provider_rate,
                                    );
                                match trade_supervisor {
                                    Ok(trade_supervisor) => KrakenPublicSupervisorSet::start(
                                        book_supervisor,
                                        trade_supervisor,
                                        cancellation,
                                        source_shutdown,
                                        currentness,
                                    )
                                    .await
                                    .map(ProductionSupervisorOwner::Kraken)
                                    .map_err(map_kraken_supervisor_error),
                                    Err(source) => match book_supervisor.shutdown() {
                                        Ok(()) => Err(
                                            ProductionLiveSourceRuntimeError::Supervisor(source),
                                        ),
                                        Err(cleanup) => Err(
                                            ProductionLiveSourceRuntimeError::KrakenConstructionCleanup {
                                                source: Box::new(source),
                                                cleanup: Box::new(cleanup),
                                            },
                                        ),
                                    },
                                }
                            }
                        }
                    }
                }
            }
        };
        let owner = match owner {
            Ok(owner) => owner,
            Err(startup) => {
                return match live.shutdown().await {
                    Ok(_shutdown) => Err(startup),
                    Err(rollback) => Err(ProductionLiveSourceRuntimeError::SourceStartupRollback {
                        startup: Box::new(startup),
                        rollback,
                    }),
                };
            }
        };
        Ok(ProductionLiveSourceRuntime {
            supervisor: owner,
            live,
            source_shutdown,
        })
    }
}

/// Drop-safe owner for either one source supervisor or Kraken's required book/trade pair.
#[derive(Debug)]
enum ProductionSupervisorOwner {
    Single {
        // Declared first so cancellation precedes join-handle detachment on drop.
        cancellation: SupervisorDropCancellation,
        task: tokio::task::JoinHandle<Result<(), ProductionSupervisorError>>,
    },
    Kraken(KrakenPublicSupervisorSet),
}

impl ProductionSupervisorOwner {
    async fn start_single(
        supervisor: ProductionSourceSupervisor,
        cancellation: CancellationToken,
    ) -> Result<Self, ProductionLiveSourceRuntimeError> {
        let (startup_sender, startup_receiver) = oneshot::channel();
        let supervisor_cancellation = cancellation.clone();
        let mut supervisor_task = tokio::spawn(async move {
            supervisor
                .run(supervisor_cancellation, startup_sender)
                .await
        });
        tokio::select! {
            startup = startup_receiver => match startup {
                Ok(()) if !cancellation.is_cancelled() && !supervisor_task.is_finished() => {
                    Ok(Self::Single {
                        cancellation: SupervisorDropCancellation::new(cancellation),
                        task: supervisor_task,
                    })
                }
                Ok(()) | Err(_) => Err(map_single_startup_outcome(supervisor_task.await)),
            },
            outcome = &mut supervisor_task => Err(map_single_startup_outcome(outcome)),
        }
    }

    fn is_healthy(&self) -> bool {
        match self {
            Self::Single { cancellation, task } => {
                !cancellation.token.is_cancelled() && !task.is_finished()
            }
            Self::Kraken(supervisors) => supervisors.is_healthy(),
        }
    }

    async fn shutdown(self, timeout: Duration) -> Result<(), ProductionLiveSourceRuntimeError> {
        match self {
            Self::Single {
                cancellation,
                mut task,
            } => {
                cancellation.cancel();
                match tokio::time::timeout(timeout, &mut task).await {
                    Ok(Ok(Ok(()))) => Ok(()),
                    Ok(Ok(Err(error))) => Err(ProductionLiveSourceRuntimeError::Supervisor(error)),
                    Ok(Err(error)) => Err(ProductionLiveSourceRuntimeError::SupervisorTask(error)),
                    Err(_elapsed) => {
                        task.abort();
                        let _aborted = task.await;
                        Err(ProductionLiveSourceRuntimeError::SupervisorShutdownDeadline)
                    }
                }
            }
            Self::Kraken(supervisors) => {
                let deadline = Instant::now()
                    .checked_add(timeout)
                    .ok_or(ProductionLiveSourceRuntimeError::SupervisorShutdownDeadline)?;
                supervisors
                    .shutdown(deadline)
                    .await
                    .map_err(map_kraken_supervisor_error)
            }
        }
    }
}

fn map_single_startup_outcome(
    outcome: Result<Result<(), ProductionSupervisorError>, tokio::task::JoinError>,
) -> ProductionLiveSourceRuntimeError {
    match outcome {
        Ok(Ok(())) => ProductionLiveSourceRuntimeError::SupervisorExitedBeforeStartup,
        Ok(Err(error)) => ProductionLiveSourceRuntimeError::Supervisor(error),
        Err(error) => ProductionLiveSourceRuntimeError::SupervisorTask(error),
    }
}

fn map_kraken_supervisor_error(
    error: KrakenPublicSupervisorSetError,
) -> ProductionLiveSourceRuntimeError {
    match error {
        KrakenPublicSupervisorSetError::Allocation => {
            ProductionLiveSourceRuntimeError::KrakenSupervisorAllocation
        }
        KrakenPublicSupervisorSetError::Cancelled => {
            ProductionLiveSourceRuntimeError::KrakenSupervisorCancelled
        }
        KrakenPublicSupervisorSetError::InvalidCleanupTimeout => {
            ProductionLiveSourceRuntimeError::KrakenSupervisorInvalidCleanupTimeout
        }
        KrakenPublicSupervisorSetError::DeadlineRange => {
            ProductionLiveSourceRuntimeError::KrakenSupervisorDeadlineRange
        }
        KrakenPublicSupervisorSetError::InvalidCurrentnessTopology => {
            ProductionLiveSourceRuntimeError::KrakenSupervisorInvalidCurrentnessTopology
        }
        KrakenPublicSupervisorSetError::CurrentnessDeadline => {
            ProductionLiveSourceRuntimeError::KrakenSupervisorCurrentnessDeadline
        }
        KrakenPublicSupervisorSetError::ExitedBeforeReadiness { channel } => {
            ProductionLiveSourceRuntimeError::KrakenSupervisorExitedBeforeReadiness {
                channel: kraken_channel_name(channel),
            }
        }
        KrakenPublicSupervisorSetError::Supervisor { channel, source } => {
            ProductionLiveSourceRuntimeError::KrakenChannelSupervisor {
                channel: kraken_channel_name(channel),
                source: Box::new(source),
            }
        }
        KrakenPublicSupervisorSetError::Task { channel, source } => {
            ProductionLiveSourceRuntimeError::KrakenChannelSupervisorTask {
                channel: kraken_channel_name(channel),
                source,
            }
        }
        KrakenPublicSupervisorSetError::ShutdownDeadline => {
            ProductionLiveSourceRuntimeError::KrakenSupervisorShutdownDeadline
        }
    }
}

const fn kraken_channel_name(channel: KrakenPublicChannel) -> &'static str {
    match channel {
        KrakenPublicChannel::Book => "book",
        KrakenPublicChannel::Trades => "trade",
    }
}

/// Owned production live runtime with read-only snapshots and bounded coordinated shutdown.
#[derive(Debug)]
pub struct ProductionLiveSourceRuntime {
    // Declared first so owner drop cancels every source before the live runtime is dropped.
    supervisor: ProductionSupervisorOwner,
    live: LiveRuntimeComposition,
    source_shutdown: Duration,
}

impl ProductionLiveSourceRuntime {
    /// Reports whether the source supervisor still owns the live producer generation.
    #[must_use]
    pub fn is_healthy(&self) -> bool {
        self.supervisor.is_healthy()
    }

    /// Returns authority-free immutable snapshot access.
    pub fn snapshots(&self) -> LiveSnapshotReader {
        self.live.snapshots()
    }

    /// Installs one complete disabled action-hook group without reconnecting the source.
    pub async fn prepare_action_hooks(
        &mut self,
        hooks: Vec<market_squawk_live::RouteActionHook>,
        cancellation: CancellationToken,
    ) -> Result<market_squawk_live::PreparedLiveActionHookGroup, ProductionLiveSourceRuntimeError>
    {
        self.live
            .prepare_action_hooks(hooks, cancellation)
            .await
            .map_err(ProductionLiveSourceRuntimeError::LiveRuntime)
    }

    /// Removes the exact disabled dynamic action-hook group from the running actors.
    pub async fn reap_action_hooks(
        &mut self,
        cancellation: CancellationToken,
    ) -> Result<market_squawk_live::LiveActionHookReapReceipt, ProductionLiveSourceRuntimeError>
    {
        self.live
            .reap_action_hooks(cancellation)
            .await
            .map_err(ProductionLiveSourceRuntimeError::LiveRuntime)
    }

    /// Stops the source supervisor before consuming the live runtime owner.
    ///
    /// # Errors
    ///
    /// Reports supervisor, deadline, task, or runtime shutdown failures after attempting both
    /// lifecycle barriers. A supervisor deadline aborts the task, making durable authority restart
    /// fail closed rather than detaching a producer.
    pub async fn shutdown(self) -> Result<(), ProductionLiveSourceRuntimeError> {
        let Self {
            supervisor,
            live,
            source_shutdown,
        } = self;
        let supervisor_result = supervisor.shutdown(source_shutdown).await.err();
        let live_result = live.shutdown().await;
        match (supervisor_result, live_result) {
            (None, Ok(_shutdown)) => Ok(()),
            (Some(error), Ok(_shutdown)) => Err(error),
            (None, Err(error)) => Err(ProductionLiveSourceRuntimeError::LiveRuntime(error)),
            (Some(supervisor), Err(live)) => {
                Err(ProductionLiveSourceRuntimeError::ShutdownFailures {
                    supervisor: Box::new(supervisor),
                    live,
                })
            }
        }
    }
}

/// Drop guard that never lets the supervisor outlive its sole production owner uncancelled.
#[derive(Debug)]
pub(super) struct SupervisorDropCancellation {
    token: CancellationToken,
}

impl SupervisorDropCancellation {
    pub(super) const fn new(token: CancellationToken) -> Self {
        Self { token }
    }

    pub(super) fn cancel(&self) {
        self.token.cancel();
    }
}

impl Drop for SupervisorDropCancellation {
    fn drop(&mut self) {
        self.cancel();
    }
}

fn validate_coinbase_routes(
    config: &CoinbaseSourceConfig,
    routes: &[LiveRouteConfig],
) -> Result<(), ProductionLiveSourceCompositionError> {
    if routes.len() != config.instruments().len() {
        return Err(ProductionLiveSourceCompositionError::RouteSetMismatch);
    }
    for (index, route) in routes.iter().enumerate() {
        if routes[index.saturating_add(1)..]
            .iter()
            .any(|other| other.route() == route.route())
        {
            return Err(ProductionLiveSourceCompositionError::DuplicateRoute);
        }
    }
    let venue = market_squawk_domain::VenueId::try_from(COINBASE_PROVIDER)?;
    for mapping in config.instruments() {
        let venue_mapping = mapping
            .definition()
            .venue_mappings()
            .iter()
            .find(|candidate| candidate.venue_id() == &venue)
            .ok_or(ProductionLiveSourceCompositionError::RouteSetMismatch)?;
        if venue_mapping.venue_symbol().as_str() != mapping.product() {
            return Err(ProductionLiveSourceCompositionError::RouteDefinitionMismatch);
        }
        let expected = ShardKey::new(venue.clone(), mapping.definition().instrument_id());
        let route = routes
            .iter()
            .find(|route| route.route() == &expected)
            .ok_or(ProductionLiveSourceCompositionError::RouteSetMismatch)?;
        if route.definition() != mapping.definition() {
            return Err(ProductionLiveSourceCompositionError::RouteDefinitionMismatch);
        }
    }
    Ok(())
}

fn validate_kraken_routes(
    config: &market_squawk_platform::KrakenSourceConfig,
    routes: &[LiveRouteConfig],
) -> Result<(), ProductionLiveSourceCompositionError> {
    if routes.len() != 1 {
        return Err(ProductionLiveSourceCompositionError::RouteSetMismatch);
    }
    let venue = market_squawk_domain::VenueId::try_from("kraken")?;
    let venue_mapping = config
        .definition()
        .venue_mappings()
        .iter()
        .find(|candidate| candidate.venue_id() == &venue)
        .ok_or(ProductionLiveSourceCompositionError::RouteSetMismatch)?;
    if venue_mapping.venue_symbol().as_str() != config.symbol() {
        return Err(ProductionLiveSourceCompositionError::RouteDefinitionMismatch);
    }
    let expected = ShardKey::new(venue, config.definition().instrument_id());
    let route = routes
        .first()
        .ok_or(ProductionLiveSourceCompositionError::RouteSetMismatch)?;
    if route.route() != &expected {
        return Err(ProductionLiveSourceCompositionError::RouteSetMismatch);
    }
    if route.definition() != config.definition() {
        return Err(ProductionLiveSourceCompositionError::RouteDefinitionMismatch);
    }
    Ok(())
}

/// Complete immutable Coinbase provider profile derived from validated local configuration.
#[derive(Debug)]
pub(super) struct ProductionCoinbaseProfile {
    adapter_config: CoinbaseExchangeConfig,
    decoder: CoinbaseExchangeDecoder,
}

impl ProductionCoinbaseProfile {
    pub(super) const fn endpoint(&self) -> &'static str {
        self.adapter_config.endpoint()
    }
    pub(super) const fn metadata(&self) -> &SourceMetadata {
        self.adapter_config.metadata()
    }

    pub(super) const fn decoder(&self) -> &CoinbaseExchangeDecoder {
        &self.decoder
    }

    pub(super) fn try_source(
        &self,
        generation: LiveSourceGeneration,
    ) -> Result<CoinbaseExchangeSource, SourceError> {
        CoinbaseExchangeSource::try_new(self.adapter_config.clone(), generation)
    }

    pub(super) fn try_from_at(
        config: &CoinbaseSourceConfig,
        at: Timestamp,
    ) -> Result<Self, ProductionCoinbaseProfileError> {
        let attestation = config.authorization();
        validate_authorization(attestation, at)?;
        let instruments = ProductionInstrumentSet::try_from(config)?;
        let configuration = ProfileInputsEvidence::try_from(config)?;
        let configuration_evidence = exact_evidence(CONFIGURATION_EVIDENCE_DOMAIN, &configuration)?;
        let effective = attestation.effective_interval();
        let authorization = AuthorizationGrant::new(
            AuthorizationMode::PublicInterface,
            attestation.basis().clone(),
            attestation.evidence().clone(),
            effective,
        );
        let budget = ProviderBudgetPolicy::try_new(
            BudgetScope::for_authorization(attestation.provider().clone(), &authorization)?,
            nonzero_u32(REQUESTS_PER_WINDOW)?,
            nonzero_u64(REQUEST_WINDOW_NANOS)?,
            nonzero_u16(MAX_CONCURRENT_REQUESTS)?,
            BackoffPolicy::try_new(
                nonzero_u64(INITIAL_BACKOFF_NANOS)?,
                nonzero_u64(MAXIMUM_BACKOFF_NANOS)?,
                BACKOFF_JITTER_BASIS_POINTS,
            )?,
        )?;
        let freshness_nanos = duration_nanos(config.freshness())?;
        let freshness = FreshnessPolicy::try_new(
            freshness_nanos,
            freshness_nanos,
            freshness_nanos,
            freshness_nanos,
            MAX_CLOCK_SKEW_NANOS,
        )?;
        let transport_limits = CoinbaseTransportLimits::try_new(
            config.max_frame_bytes().get(),
            config.subscription_ack_timeout(),
            config.subscription_ack_timeout(),
        )?;
        let provisional = CoinbaseExchangeConfig::try_new(
            SourceId::try_from(SOURCE_ID)?,
            RevisionBoundPayloadEvidence::new(
                MetadataRevision::new(identifier(PROVISIONAL_METADATA_REVISION)?),
                configuration_evidence.clone(),
            ),
            authorization.clone(),
            configuration_evidence.clone(),
            effective,
            instruments.adapter_mappings().to_vec(),
            production_channels(),
            freshness,
            budget.clone(),
            transport_limits,
        )?;
        let complete_profile = CompleteProfileEvidence {
            implementation_profile_version: IMPLEMENTATION_PROFILE_VERSION,
            metadata_without_revision: metadata_without_revision(provisional.metadata())?,
            configuration,
            transport: TransportEvidence {
                max_frame_bytes: transport_limits.max_frame_bytes(),
                connect_timeout_nanos: duration_nanos(transport_limits.connect_timeout())?,
                io_timeout_nanos: duration_nanos(transport_limits.io_timeout())?,
            },
            channels: ["level2", "market_trades", "heartbeats"],
            pre_acknowledgement_data_message_capacity: PRE_ACKNOWLEDGEMENT_DATA_MESSAGE_CAPACITY,
            pre_acknowledgement_data_byte_capacity: PRE_ACKNOWLEDGEMENT_DATA_BYTE_CAPACITY,
        };
        let (profile_evidence, digest) =
            exact_evidence_with_digest(PROFILE_EVIDENCE_DOMAIN, &complete_profile)?;
        let adapter_config = CoinbaseExchangeConfig::try_new(
            SourceId::try_from(SOURCE_ID)?,
            RevisionBoundPayloadEvidence::new(
                MetadataRevision::new(content_addressed_revision(digest)?),
                profile_evidence,
            ),
            authorization,
            configuration_evidence,
            effective,
            instruments.adapter_mappings().to_vec(),
            production_channels(),
            freshness,
            budget,
            transport_limits,
        )?;
        let decoder = CoinbaseExchangeDecoder::try_new(&adapter_config)?;
        Ok(Self {
            adapter_config,
            decoder,
        })
    }
}

impl TryFrom<&CoinbaseSourceConfig> for ProductionCoinbaseProfile {
    type Error = ProductionCoinbaseProfileError;

    fn try_from(config: &CoinbaseSourceConfig) -> Result<Self, Self::Error> {
        Self::try_from_at(config, system_timestamp()?)
    }
}

fn validate_authorization(
    attestation: &CoinbaseAuthorizationAttestation,
    at: Timestamp,
) -> Result<(), ProductionCoinbaseProfileError> {
    if attestation.provider().as_str() != COINBASE_PROVIDER {
        return Err(ProductionCoinbaseProfileError::AuthorizationMismatch);
    }
    if !attestation.is_effective_at(at) {
        return Err(ProductionCoinbaseProfileError::AuthorizationNotEffective);
    }
    Ok(())
}

#[derive(Serialize)]
struct ProfileInputsEvidence<'a> {
    implementation_profile_version: &'static str,
    endpoint: &'a str,
    event_classes: &'a [market_squawk_domain::LiveEventClass],
    depth: market_squawk_domain::MarketDepth,
    freshness_nanos: u64,
    max_frame_bytes: usize,
    subscription_ack_timeout_nanos: u64,
    control_message_capacity: usize,
    control_byte_capacity: usize,
    subscription_bytes: usize,
    authorization: &'a CoinbaseAuthorizationAttestation,
    instruments: Vec<InstrumentEvidence<'a>>,
}

impl<'a> TryFrom<&'a CoinbaseSourceConfig> for ProfileInputsEvidence<'a> {
    type Error = ProductionCoinbaseProfileError;

    fn try_from(config: &'a CoinbaseSourceConfig) -> Result<Self, Self::Error> {
        let controls = config.control_limits();
        Ok(Self {
            implementation_profile_version: IMPLEMENTATION_PROFILE_VERSION,
            endpoint: config.endpoint(),
            event_classes: config.event_classes(),
            depth: config.depth(),
            freshness_nanos: duration_nanos(config.freshness())?,
            max_frame_bytes: config.max_frame_bytes().get(),
            subscription_ack_timeout_nanos: duration_nanos(config.subscription_ack_timeout())?,
            control_message_capacity: controls.message_capacity().get(),
            control_byte_capacity: controls.byte_capacity().get(),
            subscription_bytes: config.subscription_bytes().get(),
            authorization: config.authorization(),
            instruments: config
                .instruments()
                .iter()
                .map(|mapping| InstrumentEvidence {
                    product: mapping.product(),
                    definition: mapping.definition(),
                })
                .collect(),
        })
    }
}

#[derive(Serialize)]
struct CompleteProfileEvidence<'a> {
    implementation_profile_version: &'static str,
    metadata_without_revision: serde_json::Value,
    configuration: ProfileInputsEvidence<'a>,
    transport: TransportEvidence,
    channels: [&'static str; 3],
    pre_acknowledgement_data_message_capacity: usize,
    pre_acknowledgement_data_byte_capacity: usize,
}

#[derive(Serialize)]
struct TransportEvidence {
    max_frame_bytes: usize,
    connect_timeout_nanos: u64,
    io_timeout_nanos: u64,
}

#[derive(Serialize)]
struct InstrumentEvidence<'a> {
    product: &'a str,
    definition: &'a InstrumentDefinition,
}

fn exact_evidence<T: Serialize>(
    domain: &[u8],
    value: &T,
) -> Result<ExactPayloadEvidence, ProductionCoinbaseProfileError> {
    exact_evidence_with_digest(domain, value).map(|(evidence, _digest)| evidence)
}

fn exact_evidence_with_digest<T: Serialize>(
    domain: &[u8],
    value: &T,
) -> Result<(ExactPayloadEvidence, [u8; 32]), ProductionCoinbaseProfileError> {
    let encoded = serde_json::to_vec(value)
        .map_err(|_error| ProductionCoinbaseProfileError::EvidenceSerialization)?;
    let mut hasher = Sha256::new();
    hasher.update(domain);
    hasher.update(&encoded);
    let digest: [u8; 32] = hasher.finalize().into();
    Ok((
        ExactPayloadEvidence::from_content_digest(EvidenceDigest::new(
            DigestAlgorithm::Sha256,
            digest,
        )),
        digest,
    ))
}

fn metadata_without_revision(
    metadata: &SourceMetadata,
) -> Result<serde_json::Value, ProductionCoinbaseProfileError> {
    let mut value = serde_json::to_value(metadata)
        .map_err(|_error| ProductionCoinbaseProfileError::EvidenceSerialization)?;
    let object = value
        .as_object_mut()
        .ok_or(ProductionCoinbaseProfileError::EvidenceSerialization)?;
    object
        .remove("revision_evidence")
        .ok_or(ProductionCoinbaseProfileError::EvidenceSerialization)?;
    Ok(value)
}

fn content_addressed_revision(
    digest: [u8; 32],
) -> Result<SourceIdentifier, ProductionCoinbaseProfileError> {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut revision = String::with_capacity(77);
    revision.push_str("coinbase-v2-");
    for byte in digest {
        revision.push(char::from(HEX[usize::from(byte >> 4)]));
        revision.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    Ok(SourceIdentifier::try_from(revision)?)
}

fn production_channels() -> Vec<CoinbaseChannel> {
    vec![
        CoinbaseChannel::Level2,
        CoinbaseChannel::MarketTrades,
        CoinbaseChannel::Heartbeats,
    ]
}

fn duration_nanos(value: Duration) -> Result<u64, ProductionCoinbaseProfileError> {
    u64::try_from(value.as_nanos()).map_err(|_error| ProductionCoinbaseProfileError::DurationRange)
}

pub(super) fn system_timestamp() -> Result<Timestamp, ProductionCoinbaseProfileError> {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_error| ProductionCoinbaseProfileError::ClockRange)?;
    let nanos = i64::try_from(elapsed.as_nanos())
        .map_err(|_error| ProductionCoinbaseProfileError::ClockRange)?;
    Ok(Timestamp::from_unix_nanos(nanos))
}

fn identifier(value: &str) -> Result<SourceIdentifier, IdentityError> {
    SourceIdentifier::try_from(value)
}

fn nonzero_u16(value: u16) -> Result<NonZeroU16, ProductionCoinbaseProfileError> {
    NonZeroU16::new(value).ok_or(ProductionCoinbaseProfileError::InvalidStaticPolicy)
}

fn nonzero_u32(value: u32) -> Result<NonZeroU32, ProductionCoinbaseProfileError> {
    NonZeroU32::new(value).ok_or(ProductionCoinbaseProfileError::InvalidStaticPolicy)
}

fn nonzero_u64(value: u64) -> Result<NonZeroU64, ProductionCoinbaseProfileError> {
    NonZeroU64::new(value).ok_or(ProductionCoinbaseProfileError::InvalidStaticPolicy)
}

#[derive(Debug, Error)]
pub enum ProductionCoinbaseProfileError {
    #[error("Coinbase production profile identity is invalid")]
    Identity(#[from] IdentityError),
    #[error("Coinbase production profile instrument mapping is invalid")]
    InstrumentMapping(#[from] super::instruments::ProductionInstrumentError),
    #[error("Coinbase production profile network policy is invalid")]
    NetworkPolicy(#[from] NetworkPolicyError),
    #[error("Coinbase production freshness policy is invalid")]
    Metadata(#[from] SourceMetadataError),
    #[error("Coinbase production adapter configuration is invalid")]
    Adapter(#[from] CoinbaseConfigError),
    #[error("Coinbase production evidence could not be encoded")]
    EvidenceSerialization,
    #[error("Coinbase production duration exceeds the supported nanosecond range")]
    DurationRange,
    #[error("Coinbase authorization attestation names another provider")]
    AuthorizationMismatch,
    #[error("Coinbase authorization attestation is not effective at composition time")]
    AuthorizationNotEffective,
    #[error("system wall clock cannot be represented as a domain timestamp")]
    ClockRange,
    #[error("Coinbase production static policy contains a zero bound")]
    InvalidStaticPolicy,
}

/// Production composition validation failure before any provider connection is opened.
#[derive(Debug, Error)]
pub enum ProductionLiveSourceCompositionError {
    #[error("production Coinbase configuration is required")]
    MissingCoinbaseConfiguration,
    #[error("production Kraken configuration is required")]
    MissingKrakenConfiguration,
    #[error("production source route set does not exactly cover configured instruments")]
    RouteSetMismatch,
    #[error("production source route set contains a duplicate route")]
    DuplicateRoute,
    #[error("production source metadata-set allocation failed")]
    SourceMetadataAllocation,
    #[error("production source route definition differs from validated source configuration")]
    RouteDefinitionMismatch,
    #[error(transparent)]
    Profile(#[from] ProductionCoinbaseProfileError),
    #[error(transparent)]
    KrakenProfile(#[from] ProductionKrakenProfileError),
    #[error(transparent)]
    Provider(#[from] ProductionProviderError),
    #[error("production provider route identity is invalid")]
    RouteIdentity(#[from] IdentityError),
    #[error(transparent)]
    Paths(#[from] PathError),
    #[error(transparent)]
    ProviderRate(#[from] market_squawk_sources::ProviderRateStoreError),
}

/// Production live-source startup or coordinated shutdown failure.
#[derive(Debug, Error)]
pub enum ProductionLiveSourceRuntimeError {
    #[cfg(feature = "release-evidence")]
    #[error("release-performance diagnostic source failed: {0}")]
    ReleaseBenchmark(String),
    #[error("production source supervisor exited before startup completed")]
    SupervisorExitedBeforeStartup,
    #[error("production source supervisor exceeded its shutdown deadline")]
    SupervisorShutdownDeadline,
    #[error(transparent)]
    Paths(#[from] PathError),
    #[error(transparent)]
    CaptureInfrastructure(#[from] DestinationFenceRegistryInitializationError),
    #[error("qualified-market exports do not exactly cover every production source route")]
    QualifiedMarketExportRouteSetMismatch,
    #[error("qualified-market exports contain duplicate ownership for route {route:?}")]
    DuplicateQualifiedMarketExportRoute { route: ShardKey },
    #[error(transparent)]
    LiveRuntime(#[from] LiveRuntimeCompositionError),
    #[error(transparent)]
    CoinbaseDirect(#[from] super::CoinbaseDirectSupervisorError),
    #[error(transparent)]
    Supervisor(#[from] ProductionSupervisorError),
    #[error("public Kraken supervisor-set allocation failed")]
    KrakenSupervisorAllocation,
    #[error("public Kraken supervisor-set startup was cancelled")]
    KrakenSupervisorCancelled,
    #[error("public Kraken supervisor-set cleanup timeout is invalid")]
    KrakenSupervisorInvalidCleanupTimeout,
    #[error("public Kraken supervisor-set deadline cannot be represented")]
    KrakenSupervisorDeadlineRange,
    #[error("public Kraken currentness-observer topology is invalid")]
    KrakenSupervisorInvalidCurrentnessTopology,
    #[error("public Kraken channels did not become atomically current before startup expired")]
    KrakenSupervisorCurrentnessDeadline,
    #[error("public Kraken {channel} supervisor exited before atomic readiness")]
    KrakenSupervisorExitedBeforeReadiness { channel: &'static str },
    #[error("public Kraken {channel} supervisor failed: {source}")]
    KrakenChannelSupervisor {
        channel: &'static str,
        #[source]
        source: Box<ProductionSupervisorError>,
    },
    #[error("public Kraken {channel} supervisor task failed: {source}")]
    KrakenChannelSupervisorTask {
        channel: &'static str,
        #[source]
        source: tokio::task::JoinError,
    },
    #[error("public Kraken supervisors exceeded their shared shutdown deadline")]
    KrakenSupervisorShutdownDeadline,
    #[error("production source supervisor task failed: {0}")]
    SupervisorTask(#[from] tokio::task::JoinError),
    #[error("Kraken trade-supervisor construction and book-supervisor cleanup both failed")]
    KrakenConstructionCleanup {
        #[source]
        source: Box<ProductionSupervisorError>,
        cleanup: Box<ProductionSupervisorError>,
    },
    #[error("source-set startup failed and live-runtime rollback also failed")]
    SourceStartupRollback {
        #[source]
        startup: Box<ProductionLiveSourceRuntimeError>,
        rollback: LiveRuntimeCompositionError,
    },
    #[error("source supervisor and live runtime both failed during shutdown")]
    ShutdownFailures {
        supervisor: Box<ProductionLiveSourceRuntimeError>,
        live: LiveRuntimeCompositionError,
    },
}
