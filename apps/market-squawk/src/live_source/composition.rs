//! Validated platform-to-provider production composition.

use std::{
    num::{NonZeroU16, NonZeroU32, NonZeroU64},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

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
use thiserror::Error;
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;

use super::super::live_runtime::{LiveRuntimeComposition, LiveRuntimeCompositionError};
use super::super::provider_rate::open_provider_rate_authority;
use super::instruments::ProductionInstrumentSet;
use super::kraken::{ProductionKrakenProfile, ProductionKrakenProfileError};
use super::provider::{ProductionProviderError, ProductionSourceProfile, ProductionSourceProvider};
use super::route_actor::RouteBufferLimits;
use super::supervisor::{ProductionSourceSupervisor, ProductionSupervisorError};

const SOURCE_ID: &str = "coinbase-exchange-public";
const PROVISIONAL_METADATA_REVISION: &str = "coinbase-exchange-v1-provisional";
const COINBASE_PROVIDER: &str = "coinbase-exchange";
const IMPLEMENTATION_PROFILE_VERSION: &str = "coinbase-exchange-v1-profile-2026-07-20";
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

/// Validated, connector-sealed production Coinbase composition.
///
/// The caller supplies bounded live-route resources, but cannot replace the provider connector,
/// endpoint, decoder, metadata, authorization evidence, or quality ceiling. Starting the owned
/// runtime is deliberately separate so validation can be inspected before any network access.
#[derive(Debug)]
pub struct ProductionLiveSourceComposition {
    config: AppConfig,
    profile: ProductionSourceProfile,
    routes: Vec<LiveRouteConfig>,
    provider_rate: ProviderRateAuthority,
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
        let profile = match provider {
            ProductionSourceProvider::Coinbase => {
                let source = config
                    .coinbase()
                    .ok_or(ProductionLiveSourceCompositionError::MissingCoinbaseConfiguration)?;
                validate_coinbase_routes(source, &routes)?;
                ProductionSourceProfile::coinbase(
                    ProductionCoinbaseProfile::try_from(source)?,
                    source,
                )?
            }
            ProductionSourceProvider::Kraken => {
                let source = config
                    .kraken()
                    .ok_or(ProductionLiveSourceCompositionError::MissingKrakenConfiguration)?;
                validate_kraken_routes(source, &routes)?;
                ProductionSourceProfile::kraken(ProductionKrakenProfile::try_from(source)?, source)
            }
        };
        Ok(Self {
            config,
            profile,
            routes,
            provider_rate,
        })
    }

    /// Returns the only provider endpoint accepted by the sealed production adapter.
    pub fn endpoint(&self) -> &str {
        self.profile.endpoint()
    }

    /// Returns exact canonical source metadata, including coverage and quality ceiling.
    pub fn metadata(&self) -> &SourceMetadata {
        self.profile.metadata()
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
        self.profile = self.profile.with_local_kraken_endpoint_for_test(endpoint)?;
        Ok(self)
    }

    /// Starts the bounded live runtime and exact-generation Coinbase supervisor.
    ///
    /// The method returns only after durable registry admission, capture-writer startup, and every
    /// dormant route reservation succeed. The sealed Coinbase source is the only connector that
    /// can be opened by the returned owner.
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

    async fn start_on_live_runtime(
        self,
        live: LiveRuntimeComposition,
        route_buffer_limits: RouteBufferLimits,
        paths: LocalPaths,
        capture_process: CaptureProcessInfrastructure,
        cancellation: CancellationToken,
    ) -> Result<ProductionLiveSourceRuntime, ProductionLiveSourceRuntimeError> {
        let routes = self
            .routes
            .iter()
            .map(|route| route.route().clone())
            .collect();
        let supervisor = match ProductionSourceSupervisor::try_new_with_provider_rate(
            &self.config,
            self.profile,
            paths,
            capture_process,
            live.production_ingress(),
            routes,
            route_buffer_limits,
            self.provider_rate,
        ) {
            Ok(supervisor) => supervisor,
            Err(source) => {
                return match live.shutdown().await {
                    Ok(_shutdown) => Err(ProductionLiveSourceRuntimeError::Supervisor(source)),
                    Err(rollback) => Err(
                        ProductionLiveSourceRuntimeError::SupervisorStartupRollback {
                            source: Box::new(source),
                            rollback,
                        },
                    ),
                };
            }
        };
        let source_shutdown = self.config.source_shutdown();
        let (startup_sender, startup_receiver) = oneshot::channel();
        let supervisor_cancellation = cancellation.clone();
        let mut supervisor_task = tokio::spawn(async move {
            supervisor
                .run(supervisor_cancellation, startup_sender)
                .await
        });
        tokio::select! {
            startup = startup_receiver => match startup {
                Ok(()) => Ok(ProductionLiveSourceRuntime {
                    supervisor_cancellation: SupervisorDropCancellation::new(cancellation),
                    live,
                    supervisor: supervisor_task,
                    source_shutdown,
                }),
                Err(_closed) => {
                    let outcome = supervisor_task.await;
                    let startup_error = match outcome {
                        Ok(Ok(())) => ProductionLiveSourceRuntimeError::SupervisorExitedBeforeStartup,
                        Ok(Err(error)) => ProductionLiveSourceRuntimeError::Supervisor(error),
                        Err(error) => ProductionLiveSourceRuntimeError::SupervisorTask(error),
                    };
                    match live.shutdown().await {
                        Ok(_shutdown) => Err(startup_error),
                        Err(rollback) => Err(
                            ProductionLiveSourceRuntimeError::StartupTaskRollback {
                                startup: Box::new(startup_error),
                                rollback,
                            },
                        ),
                    }
                }
            },
            outcome = &mut supervisor_task => {
                let startup_error = match outcome {
                    Ok(Ok(())) => ProductionLiveSourceRuntimeError::SupervisorExitedBeforeStartup,
                    Ok(Err(error)) => ProductionLiveSourceRuntimeError::Supervisor(error),
                    Err(error) => ProductionLiveSourceRuntimeError::SupervisorTask(error),
                };
                match live.shutdown().await {
                    Ok(_shutdown) => Err(startup_error),
                    Err(rollback) => Err(
                        ProductionLiveSourceRuntimeError::StartupTaskRollback {
                            startup: Box::new(startup_error),
                            rollback,
                        },
                    ),
                }
            }
        }
    }
}

/// Owned production live runtime with read-only snapshots and bounded coordinated shutdown.
#[derive(Debug)]
pub struct ProductionLiveSourceRuntime {
    // Declared first so owner drop cancels the supervisor before the live runtime and join handle
    // are dropped. The join handle remains detached only long enough to perform its own cleanup.
    supervisor_cancellation: SupervisorDropCancellation,
    live: LiveRuntimeComposition,
    supervisor: tokio::task::JoinHandle<Result<(), ProductionSupervisorError>>,
    source_shutdown: Duration,
}

impl ProductionLiveSourceRuntime {
    /// Returns authority-free immutable snapshot access.
    pub fn snapshots(&self) -> LiveSnapshotReader {
        self.live.snapshots()
    }

    /// Stops the source supervisor before consuming the live runtime owner.
    ///
    /// # Errors
    ///
    /// Reports supervisor, deadline, task, or runtime shutdown failures after attempting both
    /// lifecycle barriers. A supervisor deadline aborts the task, making durable authority restart
    /// fail closed rather than detaching a producer.
    pub async fn shutdown(mut self) -> Result<(), ProductionLiveSourceRuntimeError> {
        self.supervisor_cancellation.cancel();
        let supervisor_result =
            match tokio::time::timeout(self.source_shutdown, &mut self.supervisor).await {
                Ok(Ok(Ok(()))) => None,
                Ok(Ok(Err(error))) => Some(ProductionLiveSourceRuntimeError::Supervisor(error)),
                Ok(Err(error)) => Some(ProductionLiveSourceRuntimeError::SupervisorTask(error)),
                Err(_elapsed) => {
                    self.supervisor.abort();
                    let _aborted = self.supervisor.await;
                    Some(ProductionLiveSourceRuntimeError::SupervisorShutdownDeadline)
                }
            };
        let live_result = self.live.shutdown().await;
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
    for mapping in config.instruments() {
        let expected = ShardKey::new(
            mapping
                .definition()
                .venue_mappings()
                .first()
                .ok_or(ProductionLiveSourceCompositionError::RouteSetMismatch)?
                .venue_id()
                .clone(),
            mapping.definition().instrument_id(),
        );
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
    let expected = ShardKey::new(
        market_squawk_domain::VenueId::try_from("kraken")?,
        config.definition().instrument_id(),
    );
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
            MAX_CLOCK_SKEW_NANOS,
            freshness_nanos,
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
            channels: ["level2", "matches", "heartbeat"],
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
    revision.push_str("coinbase-v1-");
    for byte in digest {
        revision.push(char::from(HEX[usize::from(byte >> 4)]));
        revision.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    Ok(SourceIdentifier::try_from(revision)?)
}

fn production_channels() -> Vec<CoinbaseChannel> {
    vec![
        CoinbaseChannel::Level2,
        CoinbaseChannel::Matches,
        CoinbaseChannel::Heartbeat,
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
    #[error("production Coinbase route set does not exactly cover configured instruments")]
    RouteSetMismatch,
    #[error("production Coinbase route set contains a duplicate route")]
    DuplicateRoute,
    #[error("production Coinbase route definition differs from validated source configuration")]
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
    #[error("production source supervisor task failed: {0}")]
    SupervisorTask(#[from] tokio::task::JoinError),
    #[error("source supervisor startup failed and live-runtime rollback also failed")]
    SupervisorStartupRollback {
        #[source]
        source: Box<ProductionSupervisorError>,
        rollback: LiveRuntimeCompositionError,
    },
    #[error("source startup task failed and live-runtime rollback also failed")]
    StartupTaskRollback {
        startup: Box<ProductionLiveSourceRuntimeError>,
        rollback: LiveRuntimeCompositionError,
    },
    #[error("source supervisor and live runtime both failed during shutdown")]
    ShutdownFailures {
        supervisor: Box<ProductionLiveSourceRuntimeError>,
        live: LiveRuntimeCompositionError,
    },
}
