//! Atomic ownership of account-backed market-provider runtime groups.

use std::{
    fmt,
    num::{NonZeroU32, NonZeroUsize},
    sync::Arc,
    time::Instant,
};

use market_squawk_domain::{EvidenceDigest, SourceIdentifier};
use market_squawk_platform::{AppConfig, CaptureProcessInfrastructure};
use market_squawk_services::ServiceError;
use market_squawk_sources::ProviderRateAuthority;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::{
    live_source::{
        KrakenLevel3LiveRuntime,
        display_market::{
            DisplayMarketActorLimits, DisplayMarketDirectory,
            runtime::ProductionDisplaySourceRuntime,
        },
        order_level::{OrderLevelBookKey, OrderLevelDirectory},
        tradier_rest::{TradierRestRuntime, TradierRestRuntimeLimits},
    },
    provider_activation::{
        AlpacaBasicAccountActivation, PreparedAlpacaBasicMarketConfiguration,
        PreparedKrakenL3MarketConfiguration, PreparedMarketProviderConfiguration,
        PreparedTradierMarketConfiguration, ProviderAdapterActivation,
        TradierMarketDataAccountActivation,
    },
};

use super::{
    configuration::{
        AccountMarketSurface, PreparedMarketProviderConfigurationRequest,
        validate_resolved_configuration,
    },
    display::DisplaySourceDescriptor,
    generation::MarketRuntimeGroupGeneration,
    kraken::KrakenSourceDescriptor,
};

/// Runtime evidence for an atomic account-backed group.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct MarketProviderGroupLifecycleEvidence {
    surface_id: SourceIdentifier,
    onboarding_session_id: Uuid,
    public_configuration_digest: EvidenceDigest,
    group_generation: MarketRuntimeGroupGeneration,
}

impl MarketProviderGroupLifecycleEvidence {
    pub(crate) const fn surface_id(&self) -> &SourceIdentifier {
        &self.surface_id
    }

    pub(crate) const fn onboarding_session_id(&self) -> Uuid {
        self.onboarding_session_id
    }

    pub(crate) const fn public_configuration_digest(&self) -> EvidenceDigest {
        self.public_configuration_digest
    }

    pub(crate) const fn group_generation(&self) -> MarketRuntimeGroupGeneration {
        self.group_generation
    }
}

/// Code-owned bounded runtime policy for display actors and on-demand Tradier REST results.
#[derive(Clone, Copy, Debug)]
pub(super) struct AccountMarketRuntimeLimits {
    display_actor: DisplayMarketActorLimits,
    tradier_rest: TradierRestRuntimeLimits,
}

impl AccountMarketRuntimeLimits {
    pub(super) fn try_v1() -> Result<Self, ServiceError> {
        let display_actor = DisplayMarketActorLimits::try_new(
            nonzero_usize(512)?,
            nonzero_u32(4 * 1024 * 1024)?,
            nonzero_u32(512 * 1024)?,
            nonzero_usize(64)?,
            nonzero_u32(4 * 1024 * 1024)?,
            nonzero_u32(64 * 1024)?,
        )
        .map_err(|error| {
            tracing::error!(%error, "account-market display limits are invalid");
            ServiceError::Unavailable
        })?;
        let tradier_rest = TradierRestRuntimeLimits::try_new(
            nonzero_usize(128)?,
            nonzero_usize(64)?,
            nonzero_u32(16 * 1024 * 1024)?,
            nonzero_u32(4 * 1024 * 1024)?,
        )
        .map_err(|error| {
            tracing::error!(%error, "Tradier REST runtime limits are invalid");
            ServiceError::Unavailable
        })?;
        Ok(Self {
            display_actor,
            tradier_rest,
        })
    }
}

/// Fully started group; no child becomes registry-visible until this value is returned.
pub(super) struct AccountMarketRuntimeGroup {
    evidence: MarketProviderGroupLifecycleEvidence,
    descriptors: Box<[Arc<DisplaySourceDescriptor>]>,
    kraken_descriptor: Option<Arc<KrakenSourceDescriptor>>,
    runtime: AccountMarketRuntime,
}

impl AccountMarketRuntimeGroup {
    #[allow(
        clippy::too_many_arguments,
        reason = "every lifecycle, source, rate, capture, and read authority remains explicit"
    )]
    pub(super) async fn start(
        request: PreparedMarketProviderConfigurationRequest,
        prepared: PreparedMarketProviderConfiguration,
        provider_activation: &ProviderAdapterActivation,
        app_config: AppConfig,
        provider_rate: ProviderRateAuthority,
        capture_process: CaptureProcessInfrastructure,
        display_directory: DisplayMarketDirectory,
        order_level_directory: OrderLevelDirectory,
        limits: AccountMarketRuntimeLimits,
        lifecycle: CancellationToken,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<Self, ServiceError> {
        validate_resolved_configuration(request, &prepared)?;
        let generation = MarketRuntimeGroupGeneration::try_from_prepared(request, &prepared)?;
        let evidence = MarketProviderGroupLifecycleEvidence {
            surface_id: SourceIdentifier::try_from(request.surface().surface_id())
                .map_err(|_error| ServiceError::ResourceExhausted)?,
            onboarding_session_id: request.onboarding_session_id(),
            public_configuration_digest: request.expected_public_configuration_digest(),
            group_generation: generation,
        };
        let group_cancellation = lifecycle.child_token();
        let (runtime, descriptors, kraken_descriptor) = match prepared {
            PreparedMarketProviderConfiguration::AlpacaBasic(prepared) => {
                let (runtime, descriptors) = start_alpaca(
                    prepared,
                    provider_activation,
                    app_config,
                    provider_rate,
                    display_directory,
                    limits.display_actor,
                    group_cancellation.clone(),
                    deadline,
                    cancellation,
                )
                .await?;
                (AccountMarketRuntime::Alpaca(runtime), descriptors, None)
            }
            PreparedMarketProviderConfiguration::Tradier(prepared) => {
                let (runtime, descriptors) = start_tradier(
                    prepared,
                    provider_activation,
                    app_config,
                    provider_rate,
                    display_directory,
                    limits,
                    group_cancellation.clone(),
                    deadline,
                    cancellation,
                )
                .await?;
                (AccountMarketRuntime::Tradier(runtime), descriptors, None)
            }
            PreparedMarketProviderConfiguration::KrakenLevel3(prepared) => {
                let descriptor = KrakenSourceDescriptor::try_from_prepared(&prepared)?;
                let runtime = start_kraken(
                    prepared,
                    provider_activation,
                    app_config,
                    provider_rate,
                    capture_process,
                    order_level_directory,
                    group_cancellation.clone(),
                    deadline,
                    cancellation,
                )
                .await?;
                (
                    AccountMarketRuntime::KrakenLevel3(runtime),
                    Box::default(),
                    Some(descriptor),
                )
            }
        };
        if let Err(error) = ensure_before(deadline, cancellation) {
            group_cancellation.cancel();
            if let Err(cleanup_error) = runtime.shutdown().await {
                tracing::error!(%cleanup_error, "account-market post-start cleanup failed");
            }
            return Err(error);
        }
        if group_cancellation.is_cancelled() || !runtime.is_healthy() {
            group_cancellation.cancel();
            runtime.shutdown().await?;
            return Err(ServiceError::Unavailable);
        }
        Ok(Self {
            evidence,
            descriptors,
            kraken_descriptor,
            runtime,
        })
    }

    pub(super) const fn evidence(&self) -> &MarketProviderGroupLifecycleEvidence {
        &self.evidence
    }

    pub(super) fn is_healthy(&self) -> bool {
        self.runtime.is_healthy()
    }

    pub(super) fn display_descriptor_count(&self) -> usize {
        self.descriptors.len()
    }

    pub(super) fn append_display_descriptors(
        &self,
        destination: &mut Vec<Arc<DisplaySourceDescriptor>>,
    ) {
        destination.extend(self.descriptors.iter().map(Arc::clone));
    }

    pub(super) fn owns_display_descriptor(
        &self,
        descriptor: &Arc<DisplaySourceDescriptor>,
    ) -> bool {
        self.descriptors
            .iter()
            .any(|current| Arc::ptr_eq(current, descriptor))
    }

    pub(super) fn display_instrument_count(&self) -> Option<usize> {
        self.descriptors
            .iter()
            .try_fold(0_usize, |count, descriptor| {
                count.checked_add(descriptor.instrument_count())
            })
    }

    pub(super) fn market_instrument_count(&self) -> Option<usize> {
        self.display_instrument_count()?.checked_add(
            self.kraken_descriptor
                .as_ref()
                .map_or(0, |descriptor| descriptor.instrument_count()),
        )
    }

    pub(super) fn append_display_instrument_ids(
        &self,
        destination: &mut Vec<market_squawk_domain::InstrumentId>,
    ) {
        for descriptor in &self.descriptors {
            descriptor.append_instrument_ids(destination);
        }
    }

    pub(super) fn append_market_instrument_ids(
        &self,
        destination: &mut Vec<market_squawk_domain::InstrumentId>,
    ) {
        self.append_display_instrument_ids(destination);
        if let Some(descriptor) = &self.kraken_descriptor {
            descriptor.append_instrument_ids(destination);
        }
    }

    pub(super) fn kraken_read_authority(
        &self,
        instrument_id: market_squawk_domain::InstrumentId,
    ) -> Option<(Arc<KrakenSourceDescriptor>, OrderLevelBookKey)> {
        let descriptor = self
            .kraken_descriptor
            .as_ref()
            .filter(|descriptor| descriptor.supports(instrument_id))?;
        let AccountMarketRuntime::KrakenLevel3(runtime) = &self.runtime else {
            return None;
        };
        let key = runtime.current_key(instrument_id)?;
        Some((Arc::clone(descriptor), key))
    }

    pub(super) fn owns_kraken_descriptor(&self, descriptor: &Arc<KrakenSourceDescriptor>) -> bool {
        self.kraken_descriptor
            .as_ref()
            .is_some_and(|current| Arc::ptr_eq(current, descriptor))
    }

    pub(super) async fn shutdown(self) -> Result<(), ServiceError> {
        self.runtime.shutdown().await
    }
}

impl fmt::Debug for AccountMarketRuntimeGroup {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AccountMarketRuntimeGroup")
            .field("evidence", &self.evidence)
            .field("display_sources", &self.descriptors.len())
            .field("kraken_source", &self.kraken_descriptor.is_some())
            .field("healthy", &self.is_healthy())
            .finish_non_exhaustive()
    }
}

enum AccountMarketRuntime {
    Alpaca(AlpacaRuntimeGroup),
    Tradier(TradierRuntimeGroup),
    KrakenLevel3(KrakenLevel3LiveRuntime),
}

impl AccountMarketRuntime {
    fn is_healthy(&self) -> bool {
        match self {
            Self::Alpaca(runtime) => runtime.is_healthy(),
            Self::Tradier(runtime) => runtime.is_healthy(),
            Self::KrakenLevel3(runtime) => runtime.is_healthy(),
        }
    }

    async fn shutdown(self) -> Result<(), ServiceError> {
        match self {
            Self::Alpaca(runtime) => runtime.shutdown().await,
            Self::Tradier(runtime) => runtime.shutdown().await,
            Self::KrakenLevel3(runtime) => runtime.shutdown().await.map_err(|error| {
                tracing::error!(%error, "authenticated Kraken market runtime shutdown failed");
                ServiceError::Unavailable
            }),
        }
    }
}

struct AlpacaRuntimeGroup {
    _activation: AlpacaBasicAccountActivation,
    iex: ProductionDisplaySourceRuntime,
    options: Option<ProductionDisplaySourceRuntime>,
}

impl AlpacaRuntimeGroup {
    fn is_healthy(&self) -> bool {
        self.iex.is_healthy()
            && self
                .options
                .as_ref()
                .is_none_or(ProductionDisplaySourceRuntime::is_healthy)
    }

    async fn shutdown(self) -> Result<(), ServiceError> {
        let Self {
            _activation,
            iex,
            options,
        } = self;
        let mut failure = None;
        if let Some(options) = options {
            retain_shutdown_error(
                &mut failure,
                options.shutdown().await.map_err(|error| {
                    tracing::error!(%error, "Alpaca options display runtime shutdown failed");
                    ServiceError::Unavailable
                }),
            );
        }
        retain_shutdown_error(
            &mut failure,
            iex.shutdown().await.map_err(|error| {
                tracing::error!(%error, "Alpaca IEX display runtime shutdown failed");
                ServiceError::Unavailable
            }),
        );
        drop(_activation);
        failure.map_or(Ok(()), Err)
    }
}

struct TradierRuntimeGroup {
    _activation: TradierMarketDataAccountActivation,
    stream: ProductionDisplaySourceRuntime,
    rest: TradierRestRuntime,
}

impl TradierRuntimeGroup {
    fn is_healthy(&self) -> bool {
        self.stream.is_healthy() && self.rest.is_healthy()
    }

    async fn shutdown(self) -> Result<(), ServiceError> {
        let Self {
            _activation,
            stream,
            rest,
        } = self;
        let mut failure = None;
        retain_shutdown_error(
            &mut failure,
            rest.shutdown()
                .await
                .map(|receipt| {
                    tracing::debug!(
                        consolidated_generation = receipt.consolidated_generation().get(),
                        derived_index_generation = receipt
                            .derived_index_generation()
                            .map(market_squawk_domain::ConnectionGeneration::get),
                        "Tradier REST runtime shut down cleanly"
                    );
                })
                .map_err(|error| {
                    tracing::error!(%error, "Tradier REST runtime shutdown failed");
                    ServiceError::Unavailable
                }),
        );
        retain_shutdown_error(
            &mut failure,
            stream.shutdown().await.map_err(|error| {
                tracing::error!(%error, "Tradier display runtime shutdown failed");
                ServiceError::Unavailable
            }),
        );
        drop(_activation);
        failure.map_or(Ok(()), Err)
    }
}

#[allow(
    clippy::too_many_arguments,
    reason = "every account, shared-directory, rate, and lifecycle authority remains explicit"
)]
async fn start_alpaca(
    prepared: PreparedAlpacaBasicMarketConfiguration,
    provider_activation: &ProviderAdapterActivation,
    app_config: AppConfig,
    provider_rate: ProviderRateAuthority,
    directory: DisplayMarketDirectory,
    actor_limits: DisplayMarketActorLimits,
    group_cancellation: CancellationToken,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<(AlpacaRuntimeGroup, Box<[Arc<DisplaySourceDescriptor>]>), ServiceError> {
    let (lease, iex_config, iex_bindings, optional) = prepared.into_parts();
    let iex_descriptor = DisplaySourceDescriptor::try_new(
        AccountMarketSurface::AlpacaBasic.surface_id(),
        iex_config.metadata().clone(),
        iex_bindings,
    )?;
    let (options_config, options_descriptor) = match optional {
        Some((config, bindings)) => {
            let descriptor = DisplaySourceDescriptor::try_new(
                AccountMarketSurface::AlpacaBasic.surface_id(),
                config.metadata().clone(),
                bindings,
            )?;
            (Some(config), Some(descriptor))
        }
        None => (None, None),
    };
    let descriptor_count = 1_usize + usize::from(options_descriptor.is_some());
    let mut descriptors = Vec::new();
    descriptors
        .try_reserve_exact(descriptor_count)
        .map_err(|_error| ServiceError::ResourceExhausted)?;
    descriptors.push(iex_descriptor);
    if let Some(descriptor) = options_descriptor {
        descriptors.push(descriptor);
    }
    let descriptors = descriptors.into_boxed_slice();
    let options_expected = descriptor_count == 2;
    let mut activation_guard = StartupCancellation::new(group_cancellation.child_token());
    let mut activation = await_before(
        deadline,
        cancellation,
        provider_activation.activate_alpaca_basic_account(
            lease,
            iex_config,
            options_config,
            activation_guard.token(),
        ),
    )
    .await?;
    activation_guard.disarm();
    let credentials = activation.credentials();
    let iex_config = activation
        .take_iex_config()
        .ok_or(ServiceError::Unavailable)?;
    let mut iex_guard = StartupCancellation::new(group_cancellation.child_token());
    let iex = await_before(
        deadline,
        cancellation,
        ProductionDisplaySourceRuntime::start_alpaca_iex_with_rate_authority(
            app_config.clone(),
            directory.clone(),
            iex_config,
            Arc::clone(&credentials),
            actor_limits,
            provider_rate.clone(),
            iex_guard.token(),
        ),
    )
    .await?;
    iex_guard.disarm();
    let options = match activation.take_options_config() {
        Some(config) if options_expected => {
            let mut options_guard = StartupCancellation::new(group_cancellation.child_token());
            match await_before(
                deadline,
                cancellation,
                ProductionDisplaySourceRuntime::start_alpaca_options_with_rate_authority(
                    app_config,
                    directory,
                    config,
                    credentials,
                    actor_limits,
                    provider_rate,
                    options_guard.token(),
                ),
            )
            .await
            {
                Ok(runtime) => {
                    options_guard.disarm();
                    Some(runtime)
                }
                Err(error) => {
                    group_cancellation.cancel();
                    cleanup_display_runtime(iex, "Alpaca IEX partial-start cleanup").await;
                    return Err(error);
                }
            }
        }
        Some(_unexpected) => {
            group_cancellation.cancel();
            cleanup_display_runtime(iex, "Alpaca IEX invalid-topology cleanup").await;
            return Err(ServiceError::Unavailable);
        }
        None if !options_expected => None,
        None => {
            group_cancellation.cancel();
            cleanup_display_runtime(iex, "Alpaca IEX invalid-topology cleanup").await;
            return Err(ServiceError::Unavailable);
        }
    };
    Ok((
        AlpacaRuntimeGroup {
            _activation: activation,
            iex,
            options,
        },
        descriptors,
    ))
}

#[allow(
    clippy::too_many_arguments,
    reason = "every account, shared-directory, REST, rate, and lifecycle authority remains explicit"
)]
async fn start_tradier(
    prepared: PreparedTradierMarketConfiguration,
    provider_activation: &ProviderAdapterActivation,
    app_config: AppConfig,
    provider_rate: ProviderRateAuthority,
    directory: DisplayMarketDirectory,
    limits: AccountMarketRuntimeLimits,
    group_cancellation: CancellationToken,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<(TradierRuntimeGroup, Box<[Arc<DisplaySourceDescriptor>]>), ServiceError> {
    let (lease, stream_config, rest_config, bindings, derived) = prepared.into_parts();
    let consolidated_descriptor = DisplaySourceDescriptor::try_new(
        AccountMarketSurface::Tradier.surface_id(),
        stream_config.metadata().clone(),
        bindings,
    )?;
    let (derived_activation_config, derived_runtime_config, derived_descriptor) = match derived {
        Some((config, bindings)) => {
            let descriptor = DisplaySourceDescriptor::try_new(
                AccountMarketSurface::Tradier.surface_id(),
                config.metadata().clone(),
                bindings,
            )?;
            (Some(config.clone()), Some(config), Some(descriptor))
        }
        None => (None, None, None),
    };
    let descriptor_count = 1_usize + usize::from(derived_descriptor.is_some());
    let mut descriptors = Vec::new();
    descriptors
        .try_reserve_exact(descriptor_count)
        .map_err(|_error| ServiceError::ResourceExhausted)?;
    descriptors.push(consolidated_descriptor);
    if let Some(descriptor) = derived_descriptor {
        descriptors.push(descriptor);
    }
    let descriptors = descriptors.into_boxed_slice();
    let initial_symbols = tradier_stream_symbols(&stream_config)?;
    let transport_limits = stream_config.transport_limits();
    let rest_runtime_config = rest_config.clone();
    let rest_probe_metadata = rest_runtime_config.metadata().clone();
    let rest_probe_count = rest_runtime_config.mappings().len();
    let derived_probe = derived_runtime_config
        .as_ref()
        .map(|config| (config.metadata().clone(), config.mappings().len()));
    let mut activation_guard = StartupCancellation::new(group_cancellation.child_token());
    let mut activation = await_before(
        deadline,
        cancellation,
        provider_activation.activate_tradier_market_data_account(
            lease,
            stream_config,
            rest_config,
            derived_activation_config,
            transport_limits,
            initial_symbols,
            activation_guard.token(),
        ),
    )
    .await?;
    activation_guard.disarm();

    let mut rest_guard = StartupCancellation::new(group_cancellation.child_token());
    let rest = await_before(
        deadline,
        cancellation,
        TradierRestRuntime::start(
            &mut activation,
            rest_runtime_config,
            derived_runtime_config,
            app_config.clone(),
            provider_rate.clone(),
            limits.tradier_rest,
            rest_guard.token(),
        ),
    )
    .await?;
    rest_guard.disarm();

    let rest_client = rest.client();
    let quote_probe = match await_before(
        deadline,
        cancellation,
        rest_client.fetch_configured_quotes(false, cancellation, deadline),
    )
    .await
    {
        Ok(probe) => probe,
        Err(error) => {
            group_cancellation.cancel();
            cleanup_tradier_rest(rest).await;
            return Err(error);
        }
    };
    if quote_probe.batch().observations().len() != rest_probe_count
        || quote_probe.batch().evidence().source_id() != rest_probe_metadata.source_id()
        || quote_probe.batch().evidence().metadata_revision() != rest_probe_metadata.revision()
        || quote_probe.batch().evidence().connection_generation() != rest.consolidated_generation()
    {
        group_cancellation.cancel();
        cleanup_tradier_rest(rest).await;
        return Err(ServiceError::InvalidResult);
    }
    drop(quote_probe);
    if let Some((metadata, expected_count)) = derived_probe {
        let derived_probe = match await_before(
            deadline,
            cancellation,
            rest_client.fetch_derived_indexes(cancellation, deadline),
        )
        .await
        {
            Ok(probe) => probe,
            Err(error) => {
                group_cancellation.cancel();
                cleanup_tradier_rest(rest).await;
                return Err(error);
            }
        };
        if derived_probe.batch().observations().len() != expected_count
            || derived_probe.batch().evidence().source_id() != metadata.source_id()
            || derived_probe.batch().evidence().metadata_revision() != metadata.revision()
            || Some(derived_probe.batch().evidence().connection_generation())
                != rest.derived_index_generation()
        {
            group_cancellation.cancel();
            cleanup_tradier_rest(rest).await;
            return Err(ServiceError::InvalidResult);
        }
    }

    let streaming = match activation.take_streaming_activation() {
        Ok(streaming) => streaming,
        Err(error) => {
            tracing::error!(%error, "Tradier streaming activation handoff failed");
            group_cancellation.cancel();
            cleanup_tradier_rest(rest).await;
            return Err(ServiceError::Unavailable);
        }
    };
    let mut stream_guard = StartupCancellation::new(group_cancellation.child_token());
    let stream = match await_before(
        deadline,
        cancellation,
        ProductionDisplaySourceRuntime::start_tradier_with_rate_authority(
            app_config,
            directory,
            streaming.config().clone(),
            streaming.account(),
            streaming.subscriptions().clone(),
            limits.display_actor,
            provider_rate,
            stream_guard.token(),
        ),
    )
    .await
    {
        Ok(runtime) => {
            stream_guard.disarm();
            runtime
        }
        Err(error) => {
            group_cancellation.cancel();
            cleanup_tradier_rest(rest).await;
            return Err(error);
        }
    };
    Ok((
        TradierRuntimeGroup {
            _activation: activation,
            stream,
            rest,
        },
        descriptors,
    ))
}

#[allow(
    clippy::too_many_arguments,
    reason = "every account, capture, rate, order-level, and lifecycle authority remains explicit"
)]
async fn start_kraken(
    prepared: PreparedKrakenL3MarketConfiguration,
    provider_activation: &ProviderAdapterActivation,
    app_config: AppConfig,
    provider_rate: ProviderRateAuthority,
    capture_process: CaptureProcessInfrastructure,
    directory: OrderLevelDirectory,
    group_cancellation: CancellationToken,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<KrakenLevel3LiveRuntime, ServiceError> {
    let (lease, config, instruments) = prepared.into_parts();
    let mut activation_guard = StartupCancellation::new(group_cancellation.child_token());
    let activation = await_before(
        deadline,
        cancellation,
        provider_activation.activate_kraken_l3_account(lease, config, activation_guard.token()),
    )
    .await?;
    activation_guard.disarm();
    let mut runtime_guard = StartupCancellation::new(group_cancellation.child_token());
    let runtime = await_before(
        deadline,
        cancellation,
        activation.start_order_level_runtime(
            app_config,
            provider_rate,
            capture_process,
            instruments,
            directory,
            runtime_guard.token(),
        ),
    )
    .await?;
    runtime_guard.disarm();
    Ok(runtime)
}

fn tradier_stream_symbols(
    config: &market_squawk_adapter_tradier::TradierSourceConfig,
) -> Result<Vec<SourceIdentifier>, ServiceError> {
    let mut symbols = Vec::new();
    symbols
        .try_reserve_exact(config.mappings().len())
        .map_err(|_error| ServiceError::ResourceExhausted)?;
    for mapping in config.mappings() {
        symbols.push(
            SourceIdentifier::try_from(mapping.symbol().as_str())
                .map_err(|_error| ServiceError::ResourceExhausted)?,
        );
    }
    Ok(symbols)
}

async fn cleanup_display_runtime(runtime: ProductionDisplaySourceRuntime, context: &'static str) {
    if let Err(error) = runtime.shutdown().await {
        tracing::error!(%error, context, "display child partial-start cleanup failed");
    }
}

async fn cleanup_tradier_rest(runtime: TradierRestRuntime) {
    if let Err(error) = runtime.shutdown().await {
        tracing::error!(%error, "Tradier REST partial-start cleanup failed");
    }
}

fn retain_shutdown_error(failure: &mut Option<ServiceError>, result: Result<(), ServiceError>) {
    if let Err(error) = result
        && failure.is_none()
    {
        *failure = Some(error);
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
            tracing::error!(%error, "account-market runtime operation failed");
            ServiceError::Unavailable
        }),
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

fn nonzero_usize(value: usize) -> Result<NonZeroUsize, ServiceError> {
    NonZeroUsize::new(value).ok_or(ServiceError::Unavailable)
}

fn nonzero_u32(value: u32) -> Result<NonZeroU32, ServiceError> {
    NonZeroU32::new(value).ok_or(ServiceError::Unavailable)
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
