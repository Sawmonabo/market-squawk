//! Validated platform-to-provider production composition.

#[path = "coinbase_publication_supervisor.rs"]
mod coinbase_publication_supervisor;
#[path = "kraken_publication_supervisor.rs"]
mod kraken_publication_supervisor;
#[path = "live_runtime.rs"]
mod live_runtime;

use market_squawk_adapter_coinbase::{
    CoinbaseChannel, CoinbaseConfigError, CoinbaseExchangeConfig, CoinbaseExchangeDecoder,
    CoinbaseExchangeSource, CoinbaseProductMapping, CoinbaseTransportLimits,
};
use market_squawk_domain::{
    DigestAlgorithm, EvidenceDigest, ExactPayloadEvidence, IdentityError, InstrumentDefinition,
    MetadataRevision, RevisionBoundPayloadEvidence, SourceId, SourceIdentifier, Timestamp,
};
use market_squawk_live::{
    LiveRouteConfig, LiveRuntimeConfig, LiveSnapshotReader, RouteActionHook,
    RouteCommittedResearchMarketExport, RouteQualifiedMarketExport, ShardKey,
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
    error::Error as StdError,
    num::{NonZeroU16, NonZeroU32, NonZeroU64, NonZeroUsize},
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use thiserror::Error;
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;

use super::super::live_runtime::{LiveRuntimeComposition, LiveRuntimeCompositionError};
use super::super::provider_rate::open_provider_rate_authority;
use super::instruments::{
    ProductionCatalogSelection, ProductionInstrumentError, ProductionInstrumentSet,
};
use super::kraken::{
    KrakenPublicChannel, KrakenPublicCurrentnessObserver, KrakenPublicSupervisorSet,
    KrakenPublicSupervisorSetError, ProductionKrakenProfileError, ProductionKrakenProfileSet,
};
use super::kraken_publication::{
    KrakenCapturedPublicationIngress, KrakenCapturedPublicationReceiver,
};
use super::provider::{ProductionProviderError, ProductionSourceProfile, ProductionSourceProvider};
use super::route_actor::RouteBufferLimits;
use super::sink::{
    CoinbaseCapturedPublicationIngress, CoinbaseCapturedPublicationReceiver,
    ProductionCapturedPublicationIngress,
};
use super::supervisor::{ProductionSourceSupervisor, ProductionSupervisorError};
use crate::provider_activation::CryptoMarketPublicationPackage;
pub(in crate::live_source) use coinbase_publication_supervisor::{
    CoinbasePublicationSupervisor, CoinbasePublicationSupervisorError,
};
use kraken_publication_supervisor::KrakenPublicationSupervisor;
pub(in crate::live_source) use live_runtime::ProductionLiveRuntimeOwner;
use market_squawk_data::{
    InstrumentDefinitionReadCapability, MarketDataInstrumentReadCapability,
    MarketDataInstrumentSynchronizationCapability, MarketDataProviderIdentityQuery,
};
use market_squawk_sources::{ProviderIdentitySelectionEvidence, ProviderNativeIdentityRequest};

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
pub(in crate::live_source) const CRYPTO_PUBLICATION_CHANNEL_CAPACITY: usize = 4;
pub(in crate::live_source) const CRYPTO_PUBLICATION_RETAINED_FRAMES: usize = 8;

struct CryptoPublicationStartup {
    package: CryptoMarketPublicationPackage,
    cancellation: CancellationToken,
    captured: CryptoCapturedPublicationStartup,
    committed_receivers: Vec<market_squawk_live::CommittedResearchMarketObservationReceiver>,
    maximum_inflight: NonZeroUsize,
    limits: crate::application::CryptoPublicationRendezvousLimits,
}

enum CryptoCapturedPublicationStartup {
    Coinbase {
        ingress: CoinbaseCapturedPublicationIngress,
        receiver: CoinbaseCapturedPublicationReceiver,
    },
    Kraken {
        book_ingress: KrakenCapturedPublicationIngress,
        book_receiver: KrakenCapturedPublicationReceiver,
        trade_ingress: KrakenCapturedPublicationIngress,
        trade_receiver: KrakenCapturedPublicationReceiver,
    },
}

enum CryptoPublicationIngresses {
    Coinbase(ProductionCapturedPublicationIngress),
    Kraken {
        book: ProductionCapturedPublicationIngress,
        trades: ProductionCapturedPublicationIngress,
    },
}

#[derive(Debug)]
enum CryptoPublicationSupervisor {
    Coinbase(CoinbasePublicationSupervisor),
    Kraken(KrakenPublicationSupervisor),
}

impl CryptoPublicationSupervisor {
    fn is_healthy(&self) -> bool {
        match self {
            Self::Coinbase(supervisor) => supervisor.is_healthy(),
            Self::Kraken(supervisor) => supervisor.is_healthy(),
        }
    }

    async fn shutdown(self, deadline: Instant) -> Result<(), ProductionLiveSourceRuntimeError> {
        match self {
            Self::Coinbase(supervisor) => supervisor
                .shutdown(deadline)
                .await
                .map_err(crypto_publication_error),
            Self::Kraken(supervisor) => supervisor
                .shutdown(deadline)
                .await
                .map_err(crypto_publication_error),
        }
    }
}

fn crypto_publication_error(
    source: impl StdError + Send + Sync + 'static,
) -> ProductionLiveSourceRuntimeError {
    ProductionLiveSourceRuntimeError::CryptoPublication {
        source: Box::new(source),
    }
}

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
    catalog: Option<ProductionCatalogSelection>,
    completion: Option<Arc<PublicSourceCompletion>>,
}

/// Sealed source topology admitted by one public-provider composition.
///
/// Kraken is intentionally not represented as a generic optional companion. Its book and trade
/// profiles form one required pair with independent metadata, capture authority, and lifecycle.
#[derive(Debug)]
enum ProductionSourceInstallation {
    Single(ProductionSourceProfile),
    KrakenPending {
        local_endpoint: Option<String>,
    },
    Kraken {
        book: ProductionSourceProfile,
        trades: ProductionSourceProfile,
    },
}

impl ProductionSourceInstallation {
    fn primary(&self) -> Option<&ProductionSourceProfile> {
        match self {
            Self::Single(profile) | Self::Kraken { book: profile, .. } => Some(profile),
            Self::KrakenPending { .. } => None,
        }
    }

    const fn is_kraken(&self) -> bool {
        matches!(self, Self::KrakenPending { .. } | Self::Kraken { .. })
    }

    #[cfg(all(test, debug_assertions))]
    fn with_local_kraken_endpoint_for_test(
        self,
        endpoint: &str,
    ) -> Result<Self, ProductionProviderError> {
        if matches!(self, Self::KrakenPending { .. }) {
            return Ok(Self::KrakenPending {
                local_endpoint: Some(endpoint.to_owned()),
            });
        }
        let Self::Kraken { book, trades } = self else {
            return Err(ProductionProviderError::TestConnectorMismatch);
        };
        Ok(Self::Kraken {
            book: book.with_local_kraken_endpoint_for_test(endpoint)?,
            trades: trades.with_local_kraken_endpoint_for_test(endpoint)?,
        })
    }
}

fn validate_crypto_publication_topology(
    installation: &ProductionSourceInstallation,
    package: &CryptoMarketPublicationPackage,
) -> Result<(), ProductionLiveSourceRuntimeError> {
    if matches!(
        (installation, package),
        (
            ProductionSourceInstallation::Single(_),
            CryptoMarketPublicationPackage::Coinbase(_)
        ) | (
            ProductionSourceInstallation::Kraken { .. },
            CryptoMarketPublicationPackage::Kraken(_)
        )
    ) {
        Ok(())
    } else {
        Err(ProductionLiveSourceRuntimeError::CryptoPublicationAuthorityMismatch)
    }
}

pub(in crate::live_source) fn committed_research_exports(
    routes: &[LiveRouteConfig],
    capacity: NonZeroUsize,
    maximum_retained_bytes: NonZeroUsize,
) -> Result<
    (
        Vec<RouteCommittedResearchMarketExport>,
        Vec<market_squawk_live::CommittedResearchMarketObservationReceiver>,
    ),
    ProductionLiveSourceRuntimeError,
> {
    if routes.is_empty() {
        return Err(ProductionLiveSourceRuntimeError::CryptoPublicationBounds);
    }
    let mut exports = Vec::new();
    let mut receivers = Vec::new();
    exports
        .try_reserve_exact(routes.len())
        .map_err(|_| ProductionLiveSourceRuntimeError::CryptoPublicationBounds)?;
    receivers
        .try_reserve_exact(routes.len())
        .map_err(|_| ProductionLiveSourceRuntimeError::CryptoPublicationBounds)?;
    for route in routes {
        let (export, receiver) = RouteCommittedResearchMarketExport::try_new(
            route.route().clone(),
            capacity.get(),
            maximum_retained_bytes.get(),
        )
        .map_err(|_| ProductionLiveSourceRuntimeError::CryptoPublicationBounds)?;
        exports.push(export);
        receivers.push(receiver);
    }
    Ok((exports, receivers))
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
                ProductionSourceInstallation::KrakenPending {
                    local_endpoint: None,
                }
            }
        };
        Ok(Self {
            config,
            installation,
            routes,
            provider_rate,
            catalog: None,
            completion: None,
        })
    }

    /// Binds the selected native routes and checks live actor terms against the installed catalog.
    /// The source registry reselects every request before any session or capture starts.
    pub(crate) fn with_completion_notification(mut self, notify: Arc<tokio::sync::Notify>) -> Self {
        self.completion = Some(Arc::new(PublicSourceCompletion {
            incarnation: uuid::Uuid::new_v4(),
            completed: std::sync::atomic::AtomicBool::new(false),
            notify,
        }));
        self
    }

    pub(crate) fn with_catalog_selection(
        mut self,
        reader: market_squawk_data::MarketDataInstrumentReadCapability,
        execution: &InstrumentDefinitionReadCapability,
        requests: Vec<ProviderNativeIdentityRequest>,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<Self, ProductionInstrumentError> {
        let selected = ProductionCatalogSelection::try_new(reader, requests)?;
        let at = system_timestamp().map_err(|_| ProductionInstrumentError::CatalogUnavailable)?;
        selected.validate_live_routes(execution, &self.routes, at, deadline, cancellation)?;
        self.catalog = Some(selected);
        Ok(self)
    }

    /// Acquires genuine public provider reference evidence, publishes its selected catalog
    /// identity, and binds the exact resulting native requests before source startup.
    pub(crate) async fn with_public_crypto_reference(
        self,
        reader: MarketDataInstrumentReadCapability,
        synchronizer: MarketDataInstrumentSynchronizationCapability,
        capture_store: Arc<market_squawk_platform::SealedResearchJournalStore>,
        execution: &InstrumentDefinitionReadCapability,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<Self, ProductionLiveSourceRuntimeError> {
        let kraken = self.installation.is_kraken();
        let paths = LocalPaths::prepare(self.config.data_dir())
            .map_err(|_| ProductionLiveSourceRuntimeError::CryptoReferenceUnavailable)?;
        let reference_budget = if kraken {
            super::kraken::reference_budget(
                self.config
                    .kraken()
                    .ok_or(ProductionLiveSourceRuntimeError::CryptoReferenceUnavailable)?,
            )
            .map_err(|_| ProductionLiveSourceRuntimeError::CryptoReferenceUnavailable)?
        } else {
            super::crypto_reference::coinbase_reference_budget()
                .map_err(|_| ProductionLiveSourceRuntimeError::CryptoReferenceUnavailable)?
        };
        let selected = super::crypto_reference::synchronize_public_crypto_reference(
            reader,
            synchronizer,
            &paths,
            self.provider_rate.clone(),
            &reference_budget,
            capture_store,
            if kraken { None } else { self.config.coinbase() },
            if kraken { self.config.kraken() } else { None },
            deadline,
            cancellation,
        )
        .await
        .map_err(|error| match error {
            super::crypto_reference::CryptoReferenceError::RateDeferred { not_before } => {
                ProductionLiveSourceRuntimeError::ProviderRateDeferred { not_before }
            }
            error => {
                tracing::warn!(%error, "public crypto reference selection failed");
                ProductionLiveSourceRuntimeError::CryptoReferenceUnavailable
            }
        })?;
        let requests = if kraken {
            selected.kraken_requests
        } else {
            selected.coinbase_requests
        };
        let reader = selected.reader;
        let mut coinbase_evidence = Vec::<ProviderIdentitySelectionEvidence>::new();
        let mut kraken_profiles = None;
        for request in &requests {
            let query = MarketDataProviderIdentityQuery::try_new(
                request.namespace.clone(),
                request.provider_instrument_id.clone(),
                request.knowledge_at,
                request.effective_at,
            )
            .map_err(|_| ProductionLiveSourceRuntimeError::CryptoReferenceUnavailable)?;
            let selection = reader
                .select_provider_identity_as_of(query, deadline, cancellation)
                .map_err(|_| ProductionLiveSourceRuntimeError::CryptoReferenceUnavailable)?
                .ok_or(ProductionLiveSourceRuntimeError::CryptoReferenceUnavailable)?;
            if kraken {
                if kraken_profiles.is_some() {
                    return Err(ProductionLiveSourceRuntimeError::CryptoReferenceUnavailable);
                }
                let record = reader
                    .read_selected_provider_definition(&selection, deadline, cancellation)
                    .map_err(|_| ProductionLiveSourceRuntimeError::CryptoReferenceUnavailable)?;
                kraken_profiles = Some(
                    ProductionKrakenProfileSet::try_from_selection(
                        self.config
                            .kraken()
                            .ok_or(ProductionLiveSourceRuntimeError::CryptoReferenceUnavailable)?,
                        &record,
                        &selection,
                    )
                    .map_err(|_| ProductionLiveSourceRuntimeError::CryptoReferenceUnavailable)?,
                );
            } else {
                coinbase_evidence.push(
                    reader
                        .selected_provider_identity_evidence(
                            &selection,
                            request,
                            deadline,
                            cancellation,
                        )
                        .map_err(|_| {
                            ProductionLiveSourceRuntimeError::CryptoReferenceUnavailable
                        })?,
                );
            }
        }
        #[cfg(all(test, debug_assertions))]
        let local_kraken_endpoint = match &self.installation {
            ProductionSourceInstallation::KrakenPending { local_endpoint } => {
                local_endpoint.clone()
            }
            _ => None,
        };
        let mut composition = self
            .with_catalog_selection(reader, execution, requests, deadline, cancellation)
            .map_err(|error| {
                tracing::warn!(%error, "public crypto route does not match selected catalog terms");
                ProductionLiveSourceRuntimeError::CryptoReferenceUnavailable
            })?;
        composition.installation = if kraken {
            let [book, trades] = kraken_profiles
                .ok_or(ProductionLiveSourceRuntimeError::CryptoReferenceUnavailable)?
                .into_channels();
            let source = composition
                .config
                .kraken()
                .ok_or(ProductionLiveSourceRuntimeError::CryptoReferenceUnavailable)?;
            ProductionSourceInstallation::Kraken {
                book: ProductionSourceProfile::kraken(book, source),
                trades: ProductionSourceProfile::kraken(trades, source),
            }
        } else {
            let source = composition
                .config
                .coinbase()
                .ok_or(ProductionLiveSourceRuntimeError::CryptoReferenceUnavailable)?;
            let profile = ProductionCoinbaseProfile::try_from_selected_at(
                source,
                &coinbase_evidence,
                system_timestamp()
                    .map_err(|_| ProductionLiveSourceRuntimeError::CryptoReferenceUnavailable)?,
            )
            .map_err(|_| ProductionLiveSourceRuntimeError::CryptoReferenceUnavailable)?;
            ProductionSourceInstallation::Single(
                ProductionSourceProfile::coinbase(
                    profile,
                    source,
                    PRE_ACKNOWLEDGEMENT_DATA_MESSAGE_CAPACITY,
                    PRE_ACKNOWLEDGEMENT_DATA_BYTE_CAPACITY,
                )
                .map_err(|_| ProductionLiveSourceRuntimeError::CryptoReferenceUnavailable)?,
            )
        };
        #[cfg(all(test, debug_assertions))]
        if let Some(endpoint) = local_kraken_endpoint {
            composition.installation = composition
                .installation
                .with_local_kraken_endpoint_for_test(&endpoint)
                .map_err(|_| ProductionLiveSourceRuntimeError::CryptoReferenceUnavailable)?;
        }
        Ok(composition)
    }

    /// Returns the only provider endpoint accepted by the sealed production adapter.
    pub fn endpoint(&self) -> Result<&str, ProductionLiveSourceCompositionError> {
        match &self.installation {
            ProductionSourceInstallation::KrakenPending {
                local_endpoint: Some(endpoint),
            } => Ok(endpoint),
            ProductionSourceInstallation::KrakenPending {
                local_endpoint: None,
            } => self
                .config
                .kraken()
                .map(|source| source.endpoint())
                .ok_or(ProductionLiveSourceCompositionError::MissingKrakenConfiguration),
            _ => self
                .installation
                .primary()
                .map(ProductionSourceProfile::endpoint)
                .ok_or(ProductionLiveSourceCompositionError::CatalogSelectionRequired),
        }
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
            ProductionSourceInstallation::KrakenPending { .. } => {
                return Err(ProductionLiveSourceCompositionError::CatalogSelectionRequired);
            }
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
            ProductionSourceInstallation::KrakenPending { .. } => {
                return Err(ProductionLiveSourceCompositionError::CatalogSelectionRequired);
            }
        }
        Ok(metadata.into())
    }

    /// Returns the source whose quote/book observations feed the bounded fair-value export.
    ///
    /// This deliberately does not describe the complete installed topology. Public provenance
    /// consumers must use [`Self::source_metadata`]. Kraken trade observations remain available
    /// through the native market snapshot rather than the quote/book fair-value export.
    pub(crate) fn qualified_market_export_source_id(
        &self,
    ) -> Result<&SourceId, ProductionLiveSourceCompositionError> {
        self.installation
            .primary()
            .map(|profile| profile.metadata().source_id())
            .ok_or(ProductionLiveSourceCompositionError::CatalogSelectionRequired)
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
            ProductionLiveRuntimeOwner::standard(live),
            route_buffer_limits,
            paths,
            capture_process,
            cancellation,
            None,
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
            ProductionLiveRuntimeOwner::standard(live),
            route_buffer_limits,
            paths,
            capture_process,
            cancellation,
            None,
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
            ProductionLiveRuntimeOwner::standard(live),
            route_buffer_limits,
            paths,
            capture_process,
            cancellation,
            None,
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
            ProductionLiveRuntimeOwner::standard(live),
            route_buffer_limits,
            paths,
            capture_process,
            cancellation,
            None,
        )
        .await
    }

    /// Starts a public crypto runtime with exact captured-frame handoffs and one independently
    /// bounded committed-research export for every configured route.
    pub(crate) async fn start_with_qualified_market_exports_and_crypto_publication(
        self,
        runtime_config: LiveRuntimeConfig,
        qualified_market_exports: Vec<RouteQualifiedMarketExport>,
        package: CryptoMarketPublicationPackage,
        publication_cancellation: CancellationToken,
        cancellation: CancellationToken,
    ) -> Result<ProductionLiveSourceRuntime, ProductionLiveSourceRuntimeError> {
        validate_crypto_publication_topology(&self.installation, &package)?;
        self.validate_qualified_market_export_routes(&qualified_market_exports)?;
        let route_buffer_limits = RouteBufferLimits::new(
            runtime_config.mailbox_count_per_shard(),
            runtime_config.maximum_message_bytes(),
        );
        let capacity = NonZeroUsize::new(CRYPTO_PUBLICATION_CHANNEL_CAPACITY)
            .ok_or(ProductionLiveSourceRuntimeError::CryptoPublicationBounds)?;
        let maximum_message_bytes =
            usize::try_from(runtime_config.maximum_message_bytes().get())
                .map_err(|_| ProductionLiveSourceRuntimeError::CryptoPublicationBounds)?;
        let maximum_retained_bytes = maximum_message_bytes
            .checked_mul(CRYPTO_PUBLICATION_RETAINED_FRAMES)
            .and_then(NonZeroUsize::new)
            .ok_or(ProductionLiveSourceRuntimeError::CryptoPublicationBounds)?;
        let (committed_exports, committed_receivers) =
            committed_research_exports(&self.routes, capacity, maximum_retained_bytes)?;
        // One budget spans queued and in-flight frames, including both Kraken channels.
        let publication_frames = Arc::new(tokio::sync::Semaphore::new(capacity.get()));
        let captured = match &package {
            CryptoMarketPublicationPackage::Coinbase(_) => {
                let (ingress, receiver) = CoinbaseCapturedPublicationIngress::try_channel(
                    capacity,
                    Arc::clone(&publication_frames),
                );
                CryptoCapturedPublicationStartup::Coinbase { ingress, receiver }
            }
            CryptoMarketPublicationPackage::Kraken(_) => {
                let (book_ingress, book_receiver) = KrakenCapturedPublicationIngress::try_channel(
                    capacity,
                    Arc::clone(&publication_frames),
                );
                let (trade_ingress, trade_receiver) = KrakenCapturedPublicationIngress::try_channel(
                    capacity,
                    Arc::clone(&publication_frames),
                );
                CryptoCapturedPublicationStartup::Kraken {
                    book_ingress,
                    book_receiver,
                    trade_ingress,
                    trade_receiver,
                }
            }
        };
        let limits = crate::application::CryptoPublicationRendezvousLimits::new(
            capacity,
            maximum_retained_bytes,
            self.config.source_shutdown(),
        );
        let paths = LocalPaths::prepare(self.config.data_dir())?;
        let capture_process =
            initialize_capture_process_infrastructure(CaptureProcessInfrastructureLimits::new(
                self.config
                    .capture_destination_registry_memory_ceiling_bytes(),
            ))?;
        let live = ProductionLiveRuntimeOwner::start_with_research_exports(
            runtime_config,
            self.routes.clone(),
            qualified_market_exports,
            committed_exports,
        )
        .await?;
        self.start_on_live_runtime(
            live,
            route_buffer_limits,
            paths,
            capture_process,
            cancellation,
            Some(CryptoPublicationStartup {
                package,
                cancellation: publication_cancellation,
                captured,
                committed_receivers,
                maximum_inflight: capacity,
                limits,
            }),
        )
        .await
    }

    async fn start_on_live_runtime(
        self,
        live: ProductionLiveRuntimeOwner,
        route_buffer_limits: RouteBufferLimits,
        paths: LocalPaths,
        capture_process: CaptureProcessInfrastructure,
        cancellation: CancellationToken,
        crypto_publication: Option<CryptoPublicationStartup>,
    ) -> Result<ProductionLiveSourceRuntime, ProductionLiveSourceRuntimeError> {
        let Self {
            config,
            installation,
            routes: configured_routes,
            provider_rate,
            catalog,
            completion,
        } = self;
        let routes = configured_routes
            .iter()
            .map(|route| route.route().clone())
            .collect::<Vec<_>>();
        let source_shutdown = config.source_shutdown();
        let ingress = live.production_ingress();
        let (mut publication, publication_ingresses) = match crypto_publication {
            None => (None, None),
            Some(startup) => {
                let CryptoPublicationStartup {
                    package,
                    cancellation,
                    captured,
                    committed_receivers,
                    maximum_inflight,
                    limits,
                } = startup;
                let started = match (package, captured) {
                    (
                        CryptoMarketPublicationPackage::Coinbase(package),
                        CryptoCapturedPublicationStartup::Coinbase { ingress, receiver },
                    ) => CoinbasePublicationSupervisor::start(
                        package,
                        receiver,
                        committed_receivers,
                        maximum_inflight,
                        limits,
                        cancellation,
                    )
                    .map(|supervisor| {
                        (
                            CryptoPublicationSupervisor::Coinbase(supervisor),
                            CryptoPublicationIngresses::Coinbase(
                                ProductionCapturedPublicationIngress::Coinbase(ingress),
                            ),
                        )
                    })
                    .map_err(crypto_publication_error),
                    (
                        CryptoMarketPublicationPackage::Kraken(package),
                        CryptoCapturedPublicationStartup::Kraken {
                            book_ingress,
                            book_receiver,
                            trade_ingress,
                            trade_receiver,
                        },
                    ) => KrakenPublicationSupervisor::start(
                        package,
                        book_receiver,
                        trade_receiver,
                        committed_receivers,
                        maximum_inflight,
                        limits,
                        cancellation,
                    )
                    .map(|supervisor| {
                        (
                            CryptoPublicationSupervisor::Kraken(supervisor),
                            CryptoPublicationIngresses::Kraken {
                                book: ProductionCapturedPublicationIngress::Kraken(book_ingress),
                                trades: ProductionCapturedPublicationIngress::Kraken(trade_ingress),
                            },
                        )
                    })
                    .map_err(crypto_publication_error),
                    _ => Err(ProductionLiveSourceRuntimeError::CryptoPublicationAuthorityMismatch),
                };
                match started {
                    Ok((supervisor, ingresses)) => (Some(supervisor), Some(ingresses)),
                    Err(startup) => {
                        return match live.shutdown().await {
                            Ok(()) => Err(startup),
                            Err(rollback) => {
                                Err(ProductionLiveSourceRuntimeError::SourceStartupRollback {
                                    startup: Box::new(startup),
                                    rollback,
                                })
                            }
                        };
                    }
                }
            }
        };
        let owner = if let Some(catalog) = catalog.as_ref() {
            match installation {
                ProductionSourceInstallation::Single(profile) => {
                    let supervisor =
                        ProductionSourceSupervisor::try_new_with_provider_rate_and_catalog(
                            &config,
                            profile,
                            paths,
                            capture_process,
                            ingress,
                            routes,
                            route_buffer_limits,
                            provider_rate,
                            catalog,
                            Instant::now()
                                .checked_add(source_shutdown)
                                .unwrap_or_else(Instant::now),
                            &cancellation,
                        );
                    let supervisor = match (supervisor, publication_ingresses) {
                        (Ok(supervisor), None) => Ok(supervisor),
                        (Ok(supervisor), Some(CryptoPublicationIngresses::Coinbase(ingress))) => {
                            Ok(supervisor.with_publication(ingress))
                        }
                        (Ok(_), Some(CryptoPublicationIngresses::Kraken { .. })) => Err(
                            ProductionLiveSourceRuntimeError::CryptoPublicationAuthorityMismatch,
                        ),
                        (Err(error), _) => Err(ProductionLiveSourceRuntimeError::Supervisor(error)),
                    };
                    match supervisor {
                        Ok(supervisor) => {
                            ProductionSupervisorOwner::start_single(
                                supervisor.with_completion(completion.clone()),
                                cancellation,
                            )
                            .await
                        }
                        Err(error) => Err(error),
                    }
                }
                ProductionSourceInstallation::Kraken { book, trades } => {
                    match publication_ingresses {
                        None | Some(CryptoPublicationIngresses::Coinbase(_)) => Err(
                            ProductionLiveSourceRuntimeError::CryptoPublicationAuthorityMismatch,
                        ),
                        Some(CryptoPublicationIngresses::Kraken {
                            book: book_publication,
                            trades: trade_publication,
                        }) => {
                            match KrakenPublicCurrentnessObserver::try_new(
                                live.snapshots(),
                                &routes,
                                book.metadata().source_id().clone(),
                                trades.metadata().source_id().clone(),
                            ) {
                                Err(error) => Err(map_kraken_supervisor_error(error)),
                                Ok(currentness) => {
                                    let book_supervisor =
                                ProductionSourceSupervisor::try_new_with_provider_rate_and_catalog(
                                    &config,
                                    book,
                                    paths.clone(),
                                    capture_process,
                                    ingress.clone(),
                                    routes.clone(),
                                    route_buffer_limits,
                                    provider_rate.clone(),
                                    catalog,
                                    Instant::now().checked_add(source_shutdown).unwrap_or_else(Instant::now),
                                    &cancellation,
                                )
                                .map(|supervisor| supervisor.with_publication(book_publication))
                                .map_err(ProductionLiveSourceRuntimeError::Supervisor);
                                    match book_supervisor {
                                        Err(error) => Err(error),
                                        Ok(book_supervisor) => {
                                            let trade_supervisor =
                                        ProductionSourceSupervisor::try_new_with_provider_rate_and_catalog(
                                            &config,
                                            trades,
                                            paths,
                                            capture_process,
                                            ingress,
                                            routes,
                                            route_buffer_limits,
                                            provider_rate,
                                            catalog,
                                            Instant::now().checked_add(source_shutdown).unwrap_or_else(Instant::now),
                                            &cancellation,
                                        )
                                        .map(
                                            |supervisor| {
                                                supervisor.with_publication(trade_publication)
                                            },
                                        );
                                            match trade_supervisor {
                                    Ok(trade_supervisor) => KrakenPublicSupervisorSet::start(
                                        book_supervisor.with_completion(completion.clone()),
                                        trade_supervisor.with_completion(completion.clone()),
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
                    }
                }
                ProductionSourceInstallation::KrakenPending { .. } => {
                    Err(ProductionLiveSourceRuntimeError::Supervisor(
                        ProductionSupervisorError::MissingCatalogSelection,
                    ))
                }
            }
        } else {
            Err(ProductionLiveSourceRuntimeError::Supervisor(
                ProductionSupervisorError::MissingCatalogSelection,
            ))
        };
        let owner = match owner {
            Ok(owner) => owner,
            Err(startup) => {
                let deadline = Instant::now()
                    .checked_add(source_shutdown)
                    .unwrap_or_else(Instant::now);
                let publication_rollback = match publication.take() {
                    Some(publication) => publication.shutdown(deadline).await.err(),
                    None => None,
                };
                let live_rollback = live.shutdown().await.err();
                return match (publication_rollback, live_rollback) {
                    (None, None) => Err(startup),
                    (None, Some(rollback)) => {
                        Err(ProductionLiveSourceRuntimeError::SourceStartupRollback {
                            startup: Box::new(startup),
                            rollback,
                        })
                    }
                    (Some(publication), None) => Err(
                        ProductionLiveSourceRuntimeError::CryptoPublicationStartupRollback {
                            startup: Box::new(startup),
                            publication: Box::new(publication),
                        },
                    ),
                    (Some(publication), Some(live)) => Err(
                        ProductionLiveSourceRuntimeError::SourceStartupRollbackFailures {
                            startup: Box::new(startup),
                            publication: Box::new(publication),
                            live,
                        },
                    ),
                };
            }
        };
        if publication
            .as_ref()
            .is_some_and(|publication| !publication.is_healthy())
        {
            let startup = ProductionLiveSourceRuntimeError::CryptoPublicationExitedBeforeStartup;
            let deadline = Instant::now()
                .checked_add(source_shutdown)
                .unwrap_or_else(Instant::now);
            let supervisor = owner.shutdown(source_shutdown).await.err().map(Box::new);
            let publication = match publication.take() {
                Some(publication) => publication.shutdown(deadline).await.err(),
                None => None,
            };
            let live = live.shutdown().await.err();
            return if supervisor.is_none() && publication.is_none() && live.is_none() {
                Err(startup)
            } else {
                Err(
                    ProductionLiveSourceRuntimeError::StartupRollbackFailureSet {
                        startup: Box::new(startup),
                        supervisor,
                        publication: publication.map(Box::new),
                        live,
                    },
                )
            };
        }
        Ok(ProductionLiveSourceRuntime {
            supervisor: owner,
            completion,
            publication,
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

/// One completion signal per exact public runtime; all Kraken children share the incarnation.
/// The signal grants no recovery authority: the consumer must join and classify the outcome.
#[derive(Debug)]
pub(super) struct PublicSourceCompletion {
    incarnation: uuid::Uuid,
    completed: std::sync::atomic::AtomicBool,
    notify: Arc<tokio::sync::Notify>,
}

impl PublicSourceCompletion {
    pub(super) fn finish(&self) {
        self.completed
            .store(true, std::sync::atomic::Ordering::Release);
        self.notify.notify_one();
    }
}

/// Owned production live runtime with read-only snapshots and bounded coordinated shutdown.
#[derive(Debug)]
pub struct ProductionLiveSourceRuntime {
    // Declared first so owner drop cancels every source before the live runtime is dropped.
    supervisor: ProductionSupervisorOwner,
    completion: Option<Arc<PublicSourceCompletion>>,
    publication: Option<CryptoPublicationSupervisor>,
    live: ProductionLiveRuntimeOwner,
    source_shutdown: Duration,
}

impl ProductionLiveSourceRuntime {
    pub(crate) fn completed_incarnation(&self) -> Option<uuid::Uuid> {
        self.completion
            .as_ref()
            .filter(|signal| signal.completed.load(std::sync::atomic::Ordering::Acquire))
            .map(|signal| signal.incarnation)
    }

    /// Reports whether the source supervisor still owns the live producer generation.
    #[must_use]
    pub fn is_healthy(&self) -> bool {
        self.completed_incarnation().is_none()
            && self.supervisor.is_healthy()
            && self
                .publication
                .as_ref()
                .is_none_or(CryptoPublicationSupervisor::is_healthy)
    }

    /// Returns authority-free immutable snapshot access.
    pub fn snapshots(&self) -> LiveSnapshotReader {
        self.live.snapshots()
    }

    /// Waits for a coalesced hint; callers recheck the complete immutable snapshot.
    pub(crate) async fn next_snapshot_notification(
        &mut self,
    ) -> Option<market_squawk_live::ShardId> {
        self.live.next_snapshot_notification().await
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
            completion: _,
            publication,
            live,
            source_shutdown,
        } = self;
        let supervisor_result = supervisor.shutdown(source_shutdown).await.err();
        let publication_result = match publication {
            Some(publication) => {
                let deadline = Instant::now()
                    .checked_add(source_shutdown)
                    .ok_or(ProductionLiveSourceRuntimeError::SupervisorShutdownDeadline)?;
                publication.shutdown(deadline).await.err()
            }
            None => None,
        };
        let live_result = live.shutdown().await;
        match (supervisor_result, publication_result, live_result) {
            (None, None, Ok(())) => Ok(()),
            (Some(error), None, Ok(())) => Err(error),
            (None, Some(error), Ok(())) => Err(error),
            (None, None, Err(error)) => Err(ProductionLiveSourceRuntimeError::LiveRuntime(error)),
            (supervisor, publication, live) => {
                Err(ProductionLiveSourceRuntimeError::ShutdownFailureSet {
                    supervisor: supervisor.map(Box::new),
                    publication: publication.map(Box::new),
                    live: live.err(),
                })
            }
        }
    }
}

/// Drop guard that never lets the supervisor outlive its sole production owner uncancelled.
#[derive(Debug)]
pub(super) struct SupervisorDropCancellation {
    token: CancellationToken,
    armed: bool,
}

impl SupervisorDropCancellation {
    pub(super) const fn new(token: CancellationToken) -> Self {
        Self { token, armed: true }
    }

    pub(super) fn cancel(&self) {
        if self.armed {
            self.token.cancel();
        }
    }

    pub(super) fn disarm(mut self) {
        self.armed = false;
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
        Self::try_from_instruments_at(config, ProductionInstrumentSet::try_from(config)?, at)
    }

    pub(super) fn try_from_selected_at(
        config: &CoinbaseSourceConfig,
        selected: &[ProviderIdentitySelectionEvidence],
        at: Timestamp,
    ) -> Result<Self, ProductionCoinbaseProfileError> {
        Self::try_from_instruments_at(
            config,
            ProductionInstrumentSet::try_from_selected_public(config, selected)?,
            at,
        )
    }

    fn try_from_instruments_at(
        config: &CoinbaseSourceConfig,
        instruments: ProductionInstrumentSet,
        at: Timestamp,
    ) -> Result<Self, ProductionCoinbaseProfileError> {
        let attestation = config.authorization();
        validate_authorization(attestation, at)?;
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
            selected_public_identities: instruments
                .adapter_mappings()
                .iter()
                .filter_map(CoinbaseProductMapping::selected_public_identity)
                .map(SelectedProfileIdentityEvidence::from)
                .collect(),
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
    selected_public_identities: Vec<SelectedProfileIdentityEvidence<'a>>,
    pre_acknowledgement_data_message_capacity: usize,
    pre_acknowledgement_data_byte_capacity: usize,
}

#[derive(Serialize)]
struct SelectedProfileIdentityEvidence<'a> {
    namespace: &'a SourceId,
    provider_instrument_id: &'a market_squawk_domain::ProviderInstrumentId,
    venue: &'a market_squawk_domain::VenueId,
    venue_symbol: &'a market_squawk_domain::VenueSymbol,
    instrument: market_squawk_domain::InstrumentId,
    definition_digest: EvidenceDigest,
    definition_sequence: u32,
    reference_revision: &'a MetadataRevision,
    reference_payload_digest: EvidenceDigest,
    provider_revision: &'a MetadataRevision,
    provider_payload_digest: EvidenceDigest,
    definition_validity: market_squawk_domain::EffectiveInterval,
    provider_validity: market_squawk_domain::EffectiveInterval,
}

impl<'a> From<&'a ProviderIdentitySelectionEvidence> for SelectedProfileIdentityEvidence<'a> {
    fn from(selected: &'a ProviderIdentitySelectionEvidence) -> Self {
        Self {
            namespace: &selected.native.namespace,
            provider_instrument_id: &selected.native.provider_instrument_id,
            venue: &selected.native.venue,
            venue_symbol: &selected.native.venue_symbol,
            instrument: selected.native.instrument,
            definition_digest: selected.definition_digest,
            definition_sequence: selected.definition_sequence,
            reference_revision: &selected.reference_revision,
            reference_payload_digest: selected.reference_payload_digest,
            provider_revision: &selected.provider_revision,
            provider_payload_digest: selected.provider_payload_digest,
            definition_validity: selected.definition_validity,
            provider_validity: selected.provider_validity,
        }
    }
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
    #[error("public market reference selection is required before source metadata is available")]
    CatalogSelectionRequired,
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
    #[error("public crypto durable publication does not match its exact source topology")]
    CryptoPublicationAuthorityMismatch,
    #[error("public source preparation awaits provider rate admission")]
    ProviderRateDeferred { not_before: Instant },
    #[error("current public crypto reference and economic terms are unavailable")]
    CryptoReferenceUnavailable,
    #[error("public crypto durable-publication bounds are invalid")]
    CryptoPublicationBounds,
    #[error("public crypto publication worker exited before source startup completed")]
    CryptoPublicationExitedBeforeStartup,
    #[error("public crypto publication failed")]
    CryptoPublication {
        #[source]
        source: Box<dyn StdError + Send + Sync>,
    },
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
    #[error("source startup failed and crypto publication rollback also failed")]
    CryptoPublicationStartupRollback {
        #[source]
        startup: Box<ProductionLiveSourceRuntimeError>,
        publication: Box<ProductionLiveSourceRuntimeError>,
    },
    #[error("source startup plus crypto publication and live-runtime rollback all failed")]
    SourceStartupRollbackFailures {
        #[source]
        startup: Box<ProductionLiveSourceRuntimeError>,
        publication: Box<ProductionLiveSourceRuntimeError>,
        live: LiveRuntimeCompositionError,
    },
    #[error("source startup failed and one or more rollback barriers also failed")]
    StartupRollbackFailureSet {
        #[source]
        startup: Box<ProductionLiveSourceRuntimeError>,
        supervisor: Option<Box<ProductionLiveSourceRuntimeError>>,
        publication: Option<Box<ProductionLiveSourceRuntimeError>>,
        live: Option<LiveRuntimeCompositionError>,
    },
    #[error("source supervisor and live runtime both failed during shutdown")]
    ShutdownFailures {
        supervisor: Box<ProductionLiveSourceRuntimeError>,
        live: LiveRuntimeCompositionError,
    },
    #[error("multiple production source shutdown barriers failed")]
    ShutdownFailureSet {
        supervisor: Option<Box<ProductionLiveSourceRuntimeError>>,
        publication: Option<Box<ProductionLiveSourceRuntimeError>>,
        live: Option<LiveRuntimeCompositionError>,
    },
}
