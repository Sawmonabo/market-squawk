//! Atomic ownership of account-backed market-provider runtime groups.

use std::{
    fmt,
    future::Future,
    num::{NonZeroU32, NonZeroUsize},
    pin::Pin,
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use market_squawk_domain::{EvidenceDigest, SourceIdentifier, Timestamp, VenueId, VenueSymbol};
use market_squawk_live::ShardKey;
use market_squawk_platform::{AppConfig, CaptureProcessInfrastructure};
use market_squawk_services::ServiceError;
use market_squawk_sources::{ProviderNativeIdentityRequest, ProviderRateAuthority};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::{
    ProviderActivationLease,
    live_source::{
        KrakenLevel3LiveRuntime,
        ProductionCatalogSelection,
        display_market::{
            DisplayMarketActorLimits, DisplayMarketDirectory, DisplayMarketReadAdmission,
            runtime::ProductionDisplaySourceRuntime,
        },
        order_level::{OrderLevelBookKey, OrderLevelDirectory},
    },
    provider_activation::{
        AlpacaBasicAccountActivation, PreparedAlpacaBasicMarketConfiguration,
        PreparedKrakenL3MarketConfiguration, PreparedMarketProviderConfiguration,
        PreparedSchwabMarketRuntimeStart, ProviderAccountRuntimeCurrentness,
        ProviderAdapterActivation,
    },
};

use super::{
    alpaca_historical::{
        AlpacaHistoricalCapabilityError, AlpacaHistoricalCapabilityOwner,
        AlpacaHistoricalRuntimeCapability,
    },
    configuration::{
        AccountMarketSurface, PreparedMarketProviderConfigurationRequest,
        validate_resolved_configuration, validate_resolved_schwab_configuration,
    },
    display::DisplaySourceDescriptor,
    generation::MarketRuntimeGroupGeneration,
    kraken::KrakenSourceDescriptor,
    schwab_current::{StartedSchwabCurrentRuntime, start_schwab_current_runtime},
    schwab_streamer::SchwabCurrentRuntime,
};

use crate::application::MarketEventDurableRead;

/// Runtime evidence for an atomic account-backed group.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct MarketProviderGroupLifecycleEvidence {
    surface_id: SourceIdentifier,
    onboarding_session_id: Uuid,
    public_configuration_digest: EvidenceDigest,
    runtime_verification_receipt_digest: EvidenceDigest,
    credential_generation: market_squawk_platform::SecretGeneration,
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

    pub(crate) const fn runtime_verification_receipt_digest(&self) -> EvidenceDigest {
        self.runtime_verification_receipt_digest
    }

    pub(crate) const fn credential_generation(&self) -> market_squawk_platform::SecretGeneration {
        self.credential_generation
    }

    pub(crate) const fn group_generation(&self) -> MarketRuntimeGroupGeneration {
        self.group_generation
    }
}

/// Code-owned bounded runtime policy for display actors.
#[derive(Clone, Copy, Debug)]
pub(super) struct AccountMarketRuntimeLimits {
    display_actor: DisplayMarketActorLimits,
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
        Ok(Self { display_actor })
    }
}

/// Source-owned failure of one original constructor, distinct from the result of its cleanup.
#[derive(Clone, Debug)]
pub(super) struct AccountRuntimeStartFailure {
    pub(super) cause: ServiceError,
    pub(super) cleanup: Result<(), ServiceError>,
}

impl AccountRuntimeStartFailure {
    /// Only for branches that have not constructed a child or registry owner.
    pub(super) fn before_owner(cause: ServiceError) -> Self {
        Self {
            cause,
            cleanup: Ok(()),
        }
    }

    pub(super) fn after_cleanup(cause: ServiceError, cleanup: Result<(), ServiceError>) -> Self {
        Self { cause, cleanup }
    }

    fn with_cleanup(mut self, cleanup: Result<(), ServiceError>) -> Self {
        if self.cleanup.is_ok() {
            self.cleanup = cleanup;
        }
        self
    }
}

/// Fully started group; no child becomes registry-visible until this value is returned.
pub(super) struct AccountMarketRuntimeGroup {
    evidence: MarketProviderGroupLifecycleEvidence,
    activation_lease: ProviderActivationLease,
    descriptors: Box<[Arc<DisplaySourceDescriptor>]>,
    kraken_descriptor: Option<Arc<KrakenSourceDescriptor>>,
    read_admission: DisplayMarketReadAdmission,
    currentness: ProviderAccountRuntimeCurrentness,
    currentness_mode: AccountCurrentnessMode,
    lifecycle: CancellationToken,
    currentness_monitor: tokio::task::JoinHandle<()>,
    monitor_result: Option<Result<(), ServiceError>>,
    runtime: AccountMarketRuntime,
    metadata: Arc<[market_squawk_sources::SourceMetadata]>,
    routes: Arc<[ShardKey]>,
    durable_reads: Vec<MarketEventDurableRead>,
}

pub(super) enum PreparedAccountMarketRuntimeStart {
    Standard(PreparedMarketProviderConfiguration),
    Schwab(PreparedSchwabMarketRuntimeStart),
}

#[derive(Clone, Copy)]
enum AccountCurrentnessMode {
    PreparedOrActiveUntilAdmission,
    ActiveOnly,
}

struct AccountRuntimeStartContext {
    evidence: MarketProviderGroupLifecycleEvidence,
    activation_lease: ProviderActivationLease,
    verification_expires_at: market_squawk_domain::Timestamp,
    cleanup_budget: Duration,
    group_cancellation: CancellationToken,
    read_admission: DisplayMarketReadAdmission,
}

struct StartedAccountMarketRuntime {
    runtime: AccountMarketRuntime,
    descriptors: Box<[Arc<DisplaySourceDescriptor>]>,
    kraken_descriptor: Option<Arc<KrakenSourceDescriptor>>,
    currentness: ProviderAccountRuntimeCurrentness,
    currentness_mode: AccountCurrentnessMode,
    metadata: Arc<[market_squawk_sources::SourceMetadata]>,
    routes: Arc<[ShardKey]>,
    durable_reads: Vec<MarketEventDurableRead>,
}

impl AccountMarketRuntimeGroup {
    #[allow(
        clippy::too_many_arguments,
        reason = "every lifecycle, source, rate, capture, and read authority remains explicit"
    )]
    pub(super) async fn start(
        request: PreparedMarketProviderConfigurationRequest,
        prepared: PreparedAccountMarketRuntimeStart,
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
    ) -> Result<Self, AccountRuntimeStartFailure> {
        match &prepared {
            PreparedAccountMarketRuntimeStart::Standard(prepared) => {
                validate_resolved_configuration(request, prepared)
                    .map_err(AccountRuntimeStartFailure::before_owner)?;
            }
            PreparedAccountMarketRuntimeStart::Schwab(prepared) => {
                validate_resolved_schwab_configuration(request, prepared)
                    .map_err(AccountRuntimeStartFailure::before_owner)?;
            }
        }
        let cleanup_budget = app_config.source_shutdown();
        let runtime_incarnation = Uuid::new_v4();
        let generation = match &prepared {
            PreparedAccountMarketRuntimeStart::Standard(prepared) => {
                MarketRuntimeGroupGeneration::try_from_prepared(
                    request,
                    prepared,
                    runtime_incarnation,
                )
                .map_err(AccountRuntimeStartFailure::before_owner)?
            }
            PreparedAccountMarketRuntimeStart::Schwab(prepared) => {
                MarketRuntimeGroupGeneration::try_from_schwab(
                    request,
                    prepared,
                    runtime_incarnation,
                )
                .map_err(AccountRuntimeStartFailure::before_owner)?
            }
        };
        let evidence = MarketProviderGroupLifecycleEvidence {
            surface_id: SourceIdentifier::try_from(request.surface().surface_id())
                .map_err(|_error| ServiceError::ResourceExhausted)
                .map_err(AccountRuntimeStartFailure::before_owner)?,
            onboarding_session_id: request.onboarding_session_id(),
            public_configuration_digest: request.expected_public_configuration_digest(),
            runtime_verification_receipt_digest: request
                .expected_runtime_verification_receipt_digest(),
            credential_generation: request.expected_credential_generation(),
            group_generation: generation,
        };
        let activation_lease = match &prepared {
            PreparedAccountMarketRuntimeStart::Standard(
                PreparedMarketProviderConfiguration::AlpacaBasic(prepared),
            ) => prepared.lease(),
            PreparedAccountMarketRuntimeStart::Standard(
                PreparedMarketProviderConfiguration::KrakenLevel3(prepared),
            ) => prepared.lease(),
            PreparedAccountMarketRuntimeStart::Schwab(prepared) => prepared.activation_lease(),
        }
        .clone();
        let verification_expires_at = activation_lease
            .verification_expires_at()
            .ok_or(ServiceError::Unauthorized)
            .map_err(AccountRuntimeStartFailure::before_owner)?;
        let group_cancellation = lifecycle.child_token();
        let read_admission = DisplayMarketReadAdmission::closed();
        let context = AccountRuntimeStartContext {
            evidence,
            activation_lease,
            verification_expires_at,
            cleanup_budget,
            group_cancellation: group_cancellation.clone(),
            read_admission: read_admission.clone(),
        };
        let provider_start: Pin<
            Box<
                dyn Future<Output = Result<StartedAccountMarketRuntime, AccountRuntimeStartFailure>>
                    + Send
                    + '_,
            >,
        > = match prepared {
            PreparedAccountMarketRuntimeStart::Standard(
                PreparedMarketProviderConfiguration::AlpacaBasic(prepared),
            ) => Box::pin(async move {
                let (runtime, descriptors, currentness, metadata, routes, durable_reads) = start_alpaca(
                    prepared,
                    generation,
                    provider_activation,
                    app_config,
                    provider_rate,
                    display_directory,
                    limits.display_actor,
                    read_admission,
                    group_cancellation,
                    deadline,
                    cancellation,
                )
                .await?;
                Ok(StartedAccountMarketRuntime {
                    runtime: AccountMarketRuntime::Alpaca(runtime),
                    descriptors,
                    kraken_descriptor: None,
                    currentness,
                    currentness_mode: AccountCurrentnessMode::PreparedOrActiveUntilAdmission,
                    metadata, routes, durable_reads,
                })
            }),
            PreparedAccountMarketRuntimeStart::Standard(
                PreparedMarketProviderConfiguration::KrakenLevel3(prepared),
            ) => {
                let descriptor = KrakenSourceDescriptor::try_from_prepared(&prepared)
                    .map_err(AccountRuntimeStartFailure::before_owner)?;
                Box::pin(async move {
                    let (runtime, currentness) = start_kraken(
                        prepared,
                        provider_activation,
                        app_config,
                        provider_rate,
                        capture_process,
                        order_level_directory,
                        group_cancellation,
                        deadline,
                        cancellation,
                    )
                    .await?;
                    Ok(StartedAccountMarketRuntime {
                        runtime: AccountMarketRuntime::KrakenLevel3(runtime),
                        descriptors: Box::default(),
                        kraken_descriptor: Some(descriptor),
                        currentness,
                        currentness_mode: AccountCurrentnessMode::ActiveOnly,
                        metadata: Arc::<[market_squawk_sources::SourceMetadata]>::from([]),
                        routes: Arc::<[ShardKey]>::from([]),
                        durable_reads: Vec::new(),
                    })
                })
            }
            PreparedAccountMarketRuntimeStart::Schwab(prepared) => Box::pin(async move {
                let account_owner = prepared.account_owner();
                let started = start_schwab_current_runtime(
                    prepared,
                    provider_activation.market_data_instruments(),
                    app_config,
                    provider_rate,
                    capture_process,
                    display_directory,
                    limits.display_actor,
                    read_admission,
                    group_cancellation,
                    deadline,
                    cancellation,
                )
                .await?;
                let StartedSchwabCurrentRuntime {
                    runtime,
                    currentness,
                    descriptor,
                    metadata,
                    routes,
                    durable_read,
                    display_monitor,
                } = started;
                Ok(StartedAccountMarketRuntime {
                    runtime: AccountMarketRuntime::Schwab(SchwabRuntimeGroup {
                        current: runtime,
                        _account_owner: account_owner,
                        display_monitor,
                        monitor_result: None,
                    }),
                    descriptors: vec![descriptor].into_boxed_slice(),
                    kraken_descriptor: None,
                    currentness,
                    currentness_mode: AccountCurrentnessMode::ActiveOnly,
                    metadata,
                    routes,
                    durable_reads: vec![durable_read],
                })
            }),
        };
        let started = provider_start.await?;
        let finalization: Pin<
            Box<dyn Future<Output = Result<Self, AccountRuntimeStartFailure>> + Send + '_>,
        > = Box::pin(Self::finish_start(context, started, deadline, cancellation));
        finalization.await
    }

    async fn finish_start(
        context: AccountRuntimeStartContext,
        started: StartedAccountMarketRuntime,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<Self, AccountRuntimeStartFailure> {
        let AccountRuntimeStartContext {
            evidence,
            activation_lease,
            verification_expires_at,
            cleanup_budget,
            group_cancellation,
            read_admission,
        } = context;
        let StartedAccountMarketRuntime {
            runtime,
            descriptors,
            kraken_descriptor,
            currentness,
            currentness_mode,
            metadata,
            routes,
            durable_reads,
        } = started;
        if let Err(error) = ensure_before(deadline, cancellation) {
            let cleanup = cleanup_account_runtime(
                runtime,
                &group_cancellation,
                cleanup_budget,
                "account-market post-start cleanup failed",
            )
            .await;
            return Err(AccountRuntimeStartFailure::after_cleanup(error, cleanup));
        }
        if group_cancellation.is_cancelled() || !runtime.is_healthy() {
            let cleanup = cleanup_account_runtime(
                runtime,
                &group_cancellation,
                cleanup_budget,
                "unhealthy account-market startup cleanup failed",
            )
            .await;
            return Err(AccountRuntimeStartFailure::after_cleanup(
                ServiceError::Unavailable,
                cleanup,
            ));
        }
        let current = match currentness_mode {
            AccountCurrentnessMode::PreparedOrActiveUntilAdmission => {
                await_currentness_before(
                    deadline,
                    cancellation,
                    currentness.is_prepared_or_active(),
                )
                .await
            }
            AccountCurrentnessMode::ActiveOnly => {
                await_currentness_before(deadline, cancellation, currentness.is_active()).await
            }
        };
        match current {
            Ok(true) => {}
            Ok(false) => {
                let cleanup = cleanup_account_runtime(
                    runtime,
                    &group_cancellation,
                    cleanup_budget,
                    "stale account-market startup cleanup failed",
                )
                .await;
                return Err(AccountRuntimeStartFailure::after_cleanup(
                    ServiceError::Unauthorized,
                    cleanup,
                ));
            }
            Err(error) => {
                let cleanup = cleanup_account_runtime(
                    runtime,
                    &group_cancellation,
                    cleanup_budget,
                    "cancelled account-market startup cleanup failed",
                )
                .await;
                return Err(AccountRuntimeStartFailure::after_cleanup(error, cleanup));
            }
        }
        let expiry_delay = match duration_until(verification_expires_at) {
            Ok(delay) => delay,
            Err(error) => {
                let cleanup = cleanup_account_runtime(
                    runtime,
                    &group_cancellation,
                    cleanup_budget,
                    "expired account-market startup cleanup failed",
                )
                .await;
                return Err(AccountRuntimeStartFailure::after_cleanup(error, cleanup));
            }
        };
        let currentness_monitor = spawn_account_currentness_monitor(
            currentness.clone(),
            currentness_mode,
            read_admission.clone(),
            group_cancellation.clone(),
            expiry_delay,
        );
        Ok(Self {
            evidence,
            activation_lease,
            descriptors,
            kraken_descriptor,
            read_admission,
            currentness,
            currentness_mode,
            lifecycle: group_cancellation,
            currentness_monitor,
            monitor_result: None,
            runtime,
            metadata,
            routes,
            durable_reads,
        })
    }

    pub(super) const fn evidence(&self) -> &MarketProviderGroupLifecycleEvidence {
        &self.evidence
    }

    pub(super) fn is_healthy(&self) -> bool {
        let lifecycle_cancelled = self.lifecycle.is_cancelled();
        let currentness_monitor_finished = self.currentness_monitor.is_finished();
        let runtime_healthy = self.runtime.is_healthy();
        !lifecycle_cancelled && !currentness_monitor_finished && runtime_healthy
    }

    pub(super) const fn activation_lease(&self) -> &ProviderActivationLease {
        &self.activation_lease
    }

    /// Opens the one-way read gate after the matching durable lifecycle transition is Active.
    pub(super) fn admit_reads(&self) -> Result<(), ServiceError> {
        if !self.is_healthy() || !self.read_admission.admit() {
            return Err(ServiceError::Unavailable);
        }
        Ok(())
    }

    pub(super) fn reads_are_admitted(&self) -> bool {
        self.read_admission.is_admitted()
    }

    pub(super) fn metadata(&self) -> Arc<[market_squawk_sources::SourceMetadata]> {
        Arc::clone(&self.metadata)
    }

    pub(super) fn routes(&self) -> Arc<[ShardKey]> {
        Arc::clone(&self.routes)
    }

    pub(super) fn durable_reads(&self) -> Vec<MarketEventDurableRead> {
        self.durable_reads.clone()
    }

    pub(super) fn is_published_healthy(&self) -> bool {
        self.reads_are_admitted() && self.is_healthy()
    }

    pub(super) fn display_descriptor_count(&self) -> usize {
        usize::from(self.reads_are_admitted()) * self.descriptors.len()
    }

    pub(super) fn append_display_descriptors(
        &self,
        destination: &mut Vec<Arc<DisplaySourceDescriptor>>,
    ) {
        if self.reads_are_admitted() {
            destination.extend(self.descriptors.iter().map(Arc::clone));
        }
    }

    pub(super) fn owns_display_descriptor(
        &self,
        descriptor: &Arc<DisplaySourceDescriptor>,
    ) -> bool {
        self.reads_are_admitted()
            && self
                .descriptors
                .iter()
                .any(|current| Arc::ptr_eq(current, descriptor))
    }

    pub(super) fn display_instrument_count(&self) -> Option<usize> {
        if !self.reads_are_admitted() {
            return Some(0);
        }
        self.descriptors
            .iter()
            .try_fold(0_usize, |count, descriptor| {
                count.checked_add(descriptor.instrument_count())
            })
    }

    pub(super) fn market_instrument_count(&self) -> Option<usize> {
        if !self.reads_are_admitted() {
            return Some(0);
        }
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
        if !self.reads_are_admitted() {
            return;
        }
        for descriptor in &self.descriptors {
            descriptor.append_instrument_ids(destination);
        }
    }

    pub(super) fn append_market_instrument_ids(
        &self,
        destination: &mut Vec<market_squawk_domain::InstrumentId>,
    ) {
        if !self.reads_are_admitted() {
            return;
        }
        self.append_display_instrument_ids(destination);
        if let Some(descriptor) = &self.kraken_descriptor {
            descriptor.append_instrument_ids(destination);
        }
    }

    pub(super) fn kraken_read_authority(
        &self,
        instrument_id: market_squawk_domain::InstrumentId,
    ) -> Option<(Arc<KrakenSourceDescriptor>, OrderLevelBookKey)> {
        if !self.reads_are_admitted() {
            return None;
        }
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
        self.reads_are_admitted()
            && self
                .kraken_descriptor
                .as_ref()
                .is_some_and(|current| Arc::ptr_eq(current, descriptor))
    }

    /// The retained account can issue demand only while this exact group admits healthy reads.
    pub(super) fn schwab_account_owner(
        &self,
    ) -> Option<Arc<crate::provider_activation::SchwabMarketDataAccountActivation>> {
        if !self.reads_are_admitted() {
            return None;
        }
        match &self.runtime {
            AccountMarketRuntime::Schwab(runtime) if runtime.is_healthy() => {
                Some(Arc::clone(&runtime._account_owner))
            }
            _ => None,
        }
    }

    /// Unhealthy Streamer ownership identifies the original lifecycle allocation only.
    pub(super) fn schwab_recovery_owner(
        &self,
    ) -> Option<Arc<crate::provider_activation::SchwabMarketDataAccountActivation>> {
        match &self.runtime {
            AccountMarketRuntime::Schwab(runtime)
                if matches!(&runtime.current, SchwabCurrentRuntime::Streamer(_)) => {
                Some(Arc::clone(&runtime._account_owner))
            }
            _ => None,
        }
    }

    pub(super) fn option_chain_demand_handle(&self) -> Option<super::alpaca_option_chain::OptionChainDemandHandle> {
        if !self.is_published_healthy() { return None; }
        match &self.runtime {
            AccountMarketRuntime::Alpaca(runtime) => runtime.option_chain.as_ref()
                .filter(|child| child.is_healthy()).map(|child| child.demand_handle()),
            _ => None,
        }
    }

    pub(super) fn alpaca_historical_capability(
        &self,
    ) -> Result<Option<AlpacaHistoricalRuntimeCapability>, AlpacaHistoricalCapabilityError> {
        if !self.reads_are_admitted() {
            return Ok(None);
        }
        match &self.runtime {
            AccountMarketRuntime::Alpaca(runtime) => runtime.historical_capability().map(Some),
            AccountMarketRuntime::KrakenLevel3(_) | AccountMarketRuntime::Schwab(_) => Ok(None),
        }
    }

    pub(super) fn owns_alpaca_historical_capability(
        &self,
        capability: &AlpacaHistoricalRuntimeCapability,
    ) -> bool {
        if !self.reads_are_admitted() {
            return false;
        }
        match &self.runtime {
            AccountMarketRuntime::Alpaca(runtime) => runtime.owns_historical_capability(capability),
            AccountMarketRuntime::KrakenLevel3(_) | AccountMarketRuntime::Schwab(_) => false,
        }
    }

    pub(super) fn begin_shutdown(&self) {
        self.read_admission.revoke();
        self.lifecycle.cancel();
        self.runtime.begin_shutdown();
    }

    /// Published groups retain every child until the original history owner has drained.
    pub(super) async fn finish_published_before(
        &mut self,
        source: &crate::application::AlpacaHistoricalSourceMutationAuthority,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<(), ServiceError> {
        self.begin_shutdown();
        if let AccountMarketRuntime::Alpaca(runtime) = &self.runtime {
            let original = runtime.historical.retirement_capability();
            let parent = source
                .parent_for_runtime(&original)
                .map_err(|_| ServiceError::InvalidResult)?;
            let receipt = source
                .drain_exact(parent, deadline, cancellation)
                .await
                .map_err(|error| {
                    tracing::error!(%error, "retained account history drain failed");
                    if cancellation.is_cancelled() {
                        ServiceError::Cancelled
                    } else if Instant::now() >= deadline {
                        ServiceError::DeadlineExceeded
                    } else {
                        ServiceError::Unavailable
                    }
                })?;
            receipt
                .validate_retired_runtime(&original)
                .map_err(|_| ServiceError::InvalidResult)?;
        }
        let mut failure = join_retained_monitor_before(
            &mut self.currentness_monitor,
            &mut self.monitor_result,
            deadline,
            cancellation,
        )
        .await
        .err();
        retain_shutdown_error(
            &mut failure,
            self.runtime
                .finish_shutdown_before(deadline, cancellation)
                .await,
        );
        failure.map_or(Ok(()), Err)
    }

    /// Unpublished cleanup is driven by the registry-retained startup task, never a request future.
    pub(super) async fn shutdown_before(
        self,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<(), ServiceError> {
        let requested = ensure_before(deadline, cancellation).err();
        self.finish_unpublished_shutdown().await?;
        requested.map_or(Ok(()), Err)
    }

    /// Actual cleanup outcome only; request cancellation is retained separately by the caller.
    pub(super) async fn finish_unpublished_shutdown(mut self) -> Result<(), ServiceError> {
        self.begin_shutdown();
        let mut failure =
            join_retained_monitor(&mut self.currentness_monitor, &mut self.monitor_result)
                .await
                .err();
        retain_shutdown_error(&mut failure, self.runtime.finish_retained_shutdown().await);
        failure.map_or(Ok(()), Err)
    }
}

impl fmt::Debug for AccountMarketRuntimeGroup {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AccountMarketRuntimeGroup")
            .field("evidence", &self.evidence)
            .field("display_sources", &self.descriptors.len())
            .field("kraken_source", &self.kraken_descriptor.is_some())
            .field("reads_admitted", &self.reads_are_admitted())
            .field("healthy", &self.is_healthy())
            .finish_non_exhaustive()
    }
}

enum AccountMarketRuntime {
    Alpaca(AlpacaRuntimeGroup),
    KrakenLevel3(KrakenLevel3LiveRuntime),
    Schwab(SchwabRuntimeGroup),
}

impl AccountMarketRuntime {
    fn is_healthy(&self) -> bool {
        match self {
            Self::Alpaca(runtime) => runtime.is_healthy(),
            Self::KrakenLevel3(runtime) => runtime.is_healthy(),
            Self::Schwab(runtime) => runtime.is_healthy(),
        }
    }

    fn begin_shutdown(&self) {
        match self {
            Self::Alpaca(runtime) => runtime.begin_shutdown(),
            Self::KrakenLevel3(_) | Self::Schwab(_) => {}
        }
    }

    async fn finish_shutdown_before(
        &mut self,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<(), ServiceError> {
        match self {
            Self::Alpaca(runtime) => runtime.finish_shutdown_before(deadline, cancellation).await,
            Self::KrakenLevel3(runtime) => {
                runtime.finish_shutdown_before(deadline, cancellation).await
            }
            Self::Schwab(runtime) => runtime.finish_shutdown_before(deadline, cancellation).await,
        }
    }

    async fn finish_retained_shutdown(&mut self) -> Result<(), ServiceError> {
        self.begin_shutdown();
        match self {
            Self::Alpaca(runtime) => runtime.finish_retained_shutdown().await,
            Self::KrakenLevel3(runtime) => runtime.finish_retained_shutdown().await,
            Self::Schwab(runtime) => runtime.finish_retained_shutdown().await,
        }
    }
}

struct SchwabRuntimeGroup {
    current: SchwabCurrentRuntime,
    _account_owner: Arc<crate::provider_activation::SchwabMarketDataAccountActivation>,
    display_monitor: tokio::task::JoinHandle<()>,
    monitor_result: Option<Result<(), ServiceError>>,
}

impl SchwabRuntimeGroup {
    fn is_healthy(&self) -> bool {
        self.current.is_healthy() && !self.display_monitor.is_finished()
    }

    async fn finish_shutdown_before(
        &mut self,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<(), ServiceError> {
        let mut failure = self
            .current
            .finish_shutdown_before(deadline, cancellation)
            .await
            .err();
        retain_shutdown_error(
            &mut failure,
            join_retained_monitor_before(
                &mut self.display_monitor,
                &mut self.monitor_result,
                deadline,
                cancellation,
            )
            .await,
        );
        failure.map_or(Ok(()), Err)
    }

    async fn finish_retained_shutdown(&mut self) -> Result<(), ServiceError> {
        let mut failure = self.current.finish_retained_shutdown().await.err();
        retain_shutdown_error(
            &mut failure,
            join_retained_monitor(&mut self.display_monitor, &mut self.monitor_result).await,
        );
        failure.map_or(Ok(()), Err)
    }
}

struct AlpacaRuntimeGroup {
    option_chain: Option<super::alpaca_option_chain::AlpacaOptionChainRuntime>,
    historical: AlpacaHistoricalCapabilityOwner,
    options: Option<ProductionDisplaySourceRuntime>,
    iex: ProductionDisplaySourceRuntime,
    _activation: AlpacaBasicAccountActivation,
}

impl AlpacaRuntimeGroup {
    fn is_healthy(&self) -> bool {
        let iex_healthy = self.iex.is_healthy();
        let options_healthy = self
            .options
            .as_ref()
            .is_none_or(ProductionDisplaySourceRuntime::is_healthy);
        iex_healthy && options_healthy && self.option_chain.as_ref().is_none_or(|runtime| runtime.is_healthy())
    }

    fn historical_capability(
        &self,
    ) -> Result<AlpacaHistoricalRuntimeCapability, AlpacaHistoricalCapabilityError> {
        self.historical.issue()
    }

    fn owns_historical_capability(&self, capability: &AlpacaHistoricalRuntimeCapability) -> bool {
        self.historical.owns(capability)
    }

    fn begin_shutdown(&self) {
        if let Some(chain) = &self.option_chain { chain.begin_shutdown(); }
        self.iex.begin_shutdown();
        if let Some(options) = &self.options { options.begin_shutdown(); }
        self.historical.begin_shutdown();
    }

    async fn finish_shutdown_before(
        &mut self,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<(), ServiceError> {
        // The published caller already holds the exact coordinator drain receipt.
        self.begin_shutdown();
        let mut failure = await_before(deadline, cancellation, self.historical.finish_shutdown())
            .await
            .err();
        if let Some(options) = self.options.as_mut() {
            retain_shutdown_error(
                &mut failure,
                options.finish_shutdown_before(deadline, cancellation).await,
            );
        }
        retain_shutdown_error(
            &mut failure,
            self.iex
                .finish_shutdown_before(deadline, cancellation)
                .await,
        );
        if let Some(chain) = &mut self.option_chain {
            retain_shutdown_error(&mut failure, chain.finish_shutdown_before(deadline).await);
        }
        failure.map_or(Ok(()), Err)
    }

    async fn finish_retained_shutdown(&mut self) -> Result<(), ServiceError> {
        self.begin_shutdown();
        let mut failure = self.historical.finish_shutdown().await.err();
        if let Some(options) = self.options.as_mut() {
            retain_shutdown_error(&mut failure, options.finish_retained_shutdown().await);
        }
        retain_shutdown_error(&mut failure, self.iex.finish_retained_shutdown().await);
        if let Some(chain) = &mut self.option_chain {
            retain_shutdown_error(&mut failure, chain.finish_retained_shutdown().await);
        }
        failure.map_or(Ok(()), Err)
    }
}

#[allow(
    clippy::too_many_arguments,
    reason = "every account, shared-directory, rate, and lifecycle authority remains explicit"
)]
async fn start_alpaca(
    prepared: PreparedAlpacaBasicMarketConfiguration,
    group_generation: MarketRuntimeGroupGeneration,
    provider_activation: &ProviderAdapterActivation,
    app_config: AppConfig,
    provider_rate: ProviderRateAuthority,
    directory: DisplayMarketDirectory,
    actor_limits: DisplayMarketActorLimits,
    read_admission: DisplayMarketReadAdmission,
    group_cancellation: CancellationToken,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<
    (
        AlpacaRuntimeGroup,
        Box<[Arc<DisplaySourceDescriptor>]>,
        ProviderAccountRuntimeCurrentness,
        Arc<[market_squawk_sources::SourceMetadata]>,
        Arc<[ShardKey]>,
        Vec<MarketEventDurableRead>,
    ),
    AccountRuntimeStartFailure,
> {
    let option_chain_config = prepared.option_chain_config().cloned();
    let (
        mut activation,
        iex_config,
        iex_bindings,
        historical_metadata,
        historical_request_bounds,
        historical_rights,
        calendar_metadata,
        calendar_rights,
        optional,
    ) = prepared.into_parts();
    let iex_config_for_mappings = iex_config.clone();
    let mut metadata = vec![iex_config.metadata().clone()];
    let mut routes = Vec::new();
    for (source, bindings) in std::iter::once((iex_config.metadata(), iex_bindings.as_ref())).chain(
        optional
            .as_ref()
            .map(|(config, bindings)| (config.metadata(), bindings.as_ref())),
    ) {
        let [venue] = source.coverage().topology().venues() else {
            return Err(AccountRuntimeStartFailure::before_owner(
                ServiceError::InvalidResult,
            ));
        };
        for binding in bindings {
            routes.push(ShardKey::new(venue.clone(), binding.instrument_id()));
        }
    }
    if let Some((config, _)) = &optional {
        metadata.push(config.metadata().clone());
    }
    let options_expected = optional.is_some();
    await_before(
        deadline,
        cancellation,
        activation.require_prepared_or_active(),
    )
    .await
    .map_err(AccountRuntimeStartFailure::before_owner)?;
    let credentials = activation.credentials();
    let iex_generation = provider_activation
        .register_alpaca_publication_generation(&activation, &metadata[0])
        .map_err(|error| {
            tracing::error!(%error, "Alpaca IEX publication generation failed");
            AccountRuntimeStartFailure::before_owner(ServiceError::Unavailable)
        })?;
    let catalog_reader = provider_activation.market_data_instruments();
    if iex_bindings.len() != iex_config_for_mappings.mappings().len() {
        return Err(AccountRuntimeStartFailure::before_owner(
            ServiceError::InvalidResult,
        ));
    }
    let mut iex_native_mappings = Vec::new();
    let mut iex_native_requests = Vec::new();
    for (binding, mapping) in iex_bindings.iter().zip(iex_config_for_mappings.mappings()) {
        let native = binding
            .native_identity()
            .ok_or_else(|| AccountRuntimeStartFailure::before_owner(ServiceError::InvalidResult))?;
        if binding.instrument_id() != mapping.instrument()
            || native.instrument != binding.instrument_id()
            || native.venue_symbol.as_str() != mapping.symbol()
        {
            return Err(AccountRuntimeStartFailure::before_owner(
                ServiceError::InvalidResult,
            ));
        }
        iex_native_mappings.push(
            mapping
                .clone()
                .try_with_native_identity(native.clone())
                .map_err(|_| AccountRuntimeStartFailure::before_owner(ServiceError::Unavailable))?,
        );
        iex_native_requests.push(native.clone());
    }
    let iex_publication_bindings = iex_bindings;
    let iex_catalog =
        ProductionCatalogSelection::try_new(catalog_reader.clone(), iex_native_requests)
            .map_err(|_| AccountRuntimeStartFailure::before_owner(ServiceError::Unavailable))?;
    let mut options_native_mappings = Vec::new();
    let mut options_publication_bindings = None;
    let mut options_catalog = None;
    if let Some((option_config, original_bindings)) = optional.as_ref() {
        if original_bindings.len() != option_config.mappings().len() {
            return Err(AccountRuntimeStartFailure::before_owner(
                ServiceError::InvalidResult,
            ));
        }
        let chain = option_chain_config
            .as_ref()
            .ok_or_else(|| AccountRuntimeStartFailure::before_owner(ServiceError::Unavailable))?;
        let namespace = chain.metadata().source_id();
        let selected_at = wall_timestamp().map_err(AccountRuntimeStartFailure::before_owner)?;
        let mut rebound = Vec::new();
        let mut requests = Vec::new();
        for (binding, mapping) in original_bindings.iter().zip(option_config.mappings()) {
            if binding.instrument_id() != mapping.instrument() {
                return Err(AccountRuntimeStartFailure::before_owner(
                    ServiceError::InvalidResult,
                ));
            }
            let record = catalog_reader
                .latest(binding.instrument_id(), deadline, cancellation)
                .map_err(|_| AccountRuntimeStartFailure::before_owner(ServiceError::Unavailable))?
                .ok_or_else(|| {
                    AccountRuntimeStartFailure::before_owner(ServiceError::Unavailable)
                })?;
            let mut accepted =
                record
                    .definition()
                    .provider_identities()
                    .iter()
                    .filter(|identity| {
                        identity.source_id() == namespace
                            && record.definition().provider_identity_at(
                                identity.source_id(),
                                identity.provider_instrument_id(),
                                selected_at,
                            ) == Some(*identity)
                    });
            let identity = accepted.next().ok_or_else(|| {
                AccountRuntimeStartFailure::before_owner(ServiceError::Unavailable)
            })?;
            if accepted.next().is_some() {
                return Err(AccountRuntimeStartFailure::before_owner(
                    ServiceError::Unavailable,
                ));
            }
            let native = ProviderNativeIdentityRequest {
                namespace: identity.source_id().clone(),
                provider_instrument_id: identity.provider_instrument_id().clone(),
                instrument: binding.instrument_id(),
                venue: VenueId::try_from("alpaca-indicative-options").map_err(|_| {
                    AccountRuntimeStartFailure::before_owner(ServiceError::InvalidResult)
                })?,
                venue_symbol: VenueSymbol::try_from(mapping.symbol()).map_err(|_| {
                    AccountRuntimeStartFailure::before_owner(ServiceError::InvalidResult)
                })?,
                knowledge_at: selected_at,
                effective_at: selected_at,
            };
            rebound.push(
                binding
                    .try_rebind_after_alpaca_reference(
                        &record,
                        &record,
                        &native,
                        &catalog_reader,
                        deadline,
                        cancellation,
                    )
                    .map_err(|_| {
                        AccountRuntimeStartFailure::before_owner(ServiceError::Unavailable)
                    })?,
            );
            options_native_mappings.push(
                mapping
                    .clone()
                    .try_with_native_identity(native.clone())
                    .map_err(|_| {
                        AccountRuntimeStartFailure::before_owner(ServiceError::Unavailable)
                    })?,
            );
            requests.push(native);
        }
        options_catalog = Some(
            ProductionCatalogSelection::try_new(catalog_reader.clone(), requests)
                .map_err(|_| AccountRuntimeStartFailure::before_owner(ServiceError::Unavailable))?,
        );
        options_publication_bindings = Some(rebound.into_boxed_slice());
    }
    // No further catalog writes may occur between this point and source registration.
    let iex_publication = provider_activation
        .bind_alpaca_publication_runtime_after_reference(
            &activation,
            iex_generation,
            &iex_publication_bindings,
            group_cancellation.child_token(),
            deadline,
        )
        .map_err(|error| {
            tracing::error!(%error, "Alpaca IEX publication binding failed");
            AccountRuntimeStartFailure::before_owner(ServiceError::Unavailable)
        })?;
    let mut durable_reads = vec![iex_publication.durable_read()];
    let mut options_publication = match options_publication_bindings.as_ref() {
        Some(bindings) => {
            let generation = provider_activation
                .register_alpaca_publication_generation(&activation, &metadata[1])
                .map_err(|_| AccountRuntimeStartFailure::before_owner(ServiceError::Unavailable))?;
            let publication = provider_activation
                .bind_alpaca_publication_runtime_after_reference(
                    &activation,
                    generation,
                    bindings,
                    group_cancellation.child_token(),
                    deadline,
                )
                .map_err(|_| AccountRuntimeStartFailure::before_owner(ServiceError::Unavailable))?;
            durable_reads.push(publication.durable_read());
            Some(publication)
        }
        None => None,
    };
    let iex_descriptor = DisplaySourceDescriptor::try_new(
        AccountMarketSurface::AlpacaBasic.surface_id(),
        metadata[0].clone(),
        iex_publication_bindings.clone(),
    )
    .map_err(AccountRuntimeStartFailure::before_owner)?;
    let mut descriptors = vec![iex_descriptor];
    if let Some(bindings) = options_publication_bindings {
        descriptors.push(
            DisplaySourceDescriptor::try_new(
                AccountMarketSurface::AlpacaBasic.surface_id(),
                metadata[1].clone(),
                bindings,
            )
            .map_err(AccountRuntimeStartFailure::before_owner)?,
        );
    }
    let descriptors = descriptors.into_boxed_slice();
    let mut historical = AlpacaHistoricalCapabilityOwner::try_new(
        &activation,
        group_generation,
        historical_metadata,
        historical_request_bounds,
        historical_rights,
        calendar_metadata,
        calendar_rights,
        group_cancellation.child_token(),
    )
    .map_err(AccountRuntimeStartFailure::before_owner)?;
    let iex_config = match activation.take_iex_config() {
        Some(config) => match config.try_with_native_mappings(iex_native_mappings) {
            Ok(config) => config,
            Err(error) => {
                tracing::error!(%error, "Alpaca selected IEX mappings were rejected");
                historical.begin_shutdown();
                group_cancellation.cancel();
                let cleanup = historical.finish_shutdown().await;
                return Err(AccountRuntimeStartFailure::after_cleanup(
                    ServiceError::Unavailable,
                    cleanup,
                ));
            }
        },
        None => {
            historical.begin_shutdown();
            group_cancellation.cancel();
            let cleanup = historical.finish_shutdown().await;
            return Err(AccountRuntimeStartFailure::after_cleanup(
                ServiceError::Unavailable,
                cleanup,
            ));
        }
    };
    let mut iex_guard = StartupCancellation::new(group_cancellation.child_token());
    let iex = match await_owned_start(
        deadline,
        cancellation,
        iex_guard.token(),
        ProductionDisplaySourceRuntime::start_alpaca_iex_with_rate_authority(
            app_config.clone(),
            directory.clone(),
            iex_config,
            Arc::clone(&credentials),
            iex_publication,
            actor_limits,
            read_admission.clone(),
            provider_rate.clone(),
            iex_catalog,
            deadline,
            cancellation,
            iex_guard.token(),
        ),
        |failure| {
            tracing::error!(error = %failure.cause, "account child startup failed");
            AccountRuntimeStartFailure::after_cleanup(
                ServiceError::Unavailable,
                failure.cleanup.map_err(|error| {
                    tracing::error!(%error, "account child startup cleanup failed");
                    ServiceError::Unavailable
                }),
            )
        },
    )
    .await
    {
        Ok(runtime) => runtime,
        Err(failure) => {
            group_cancellation.cancel();
            historical.begin_shutdown();
            let cleanup = historical.finish_shutdown().await;
            return Err(failure.with_cleanup(cleanup));
        }
    };
    iex_guard.disarm();
    let options = match activation.take_options_config() {
        Some(config) if options_expected => {
            let config = match config.try_with_native_mappings(options_native_mappings) {
                Ok(config) => config,
                Err(error) => {
                    tracing::error!(%error, "Alpaca selected options mappings were rejected");
                    historical.begin_shutdown();
                    group_cancellation.cancel();
                    let history_cleanup = historical.finish_shutdown().await;
                    let display_cleanup =
                        cleanup_display_runtime(iex, "Alpaca options mapping cleanup").await;
                    return Err(AccountRuntimeStartFailure::after_cleanup(
                        ServiceError::Unavailable,
                        history_cleanup.and(display_cleanup),
                    ));
                }
            };
            let Some(catalog) = options_catalog.take() else {
                historical.begin_shutdown();
                group_cancellation.cancel();
                let history_cleanup = historical.finish_shutdown().await;
                let display_cleanup =
                    cleanup_display_runtime(iex, "Alpaca options catalog cleanup").await;
                return Err(AccountRuntimeStartFailure::after_cleanup(
                    ServiceError::Unavailable,
                    history_cleanup.and(display_cleanup),
                ));
            };
            let Some(publication) = options_publication.take() else {
                historical.begin_shutdown();
                group_cancellation.cancel();
                let history_cleanup = historical.finish_shutdown().await;
                let display_cleanup =
                    cleanup_display_runtime(iex, "Alpaca missing publication cleanup").await;
                return Err(AccountRuntimeStartFailure::after_cleanup(
                    ServiceError::Unavailable,
                    history_cleanup.and(display_cleanup),
                ));
            };
            let mut options_guard = StartupCancellation::new(group_cancellation.child_token());
            match await_owned_start(
                deadline,
                cancellation,
                options_guard.token(),
                ProductionDisplaySourceRuntime::start_alpaca_options_with_rate_authority(
                    app_config,
                    directory,
                    config,
                    credentials,
                    publication,
                    actor_limits,
                    read_admission.clone(),
                    provider_rate,
                    catalog,
                    deadline,
                    cancellation,
                    options_guard.token(),
                ),
                |failure| {
                    tracing::error!(error = %failure.cause, "account child startup failed");
                    AccountRuntimeStartFailure::after_cleanup(
                        ServiceError::Unavailable,
                        failure.cleanup.map_err(|error| {
                            tracing::error!(%error, "account child startup cleanup failed");
                            ServiceError::Unavailable
                        }),
                    )
                },
            )
            .await
            {
                Ok(runtime) => {
                    options_guard.disarm();
                    Some(runtime)
                }
                Err(error) => {
                    historical.begin_shutdown();
                    group_cancellation.cancel();
                    let history_cleanup = historical.finish_shutdown().await;
                    let display_cleanup =
                        cleanup_display_runtime(iex, "Alpaca IEX partial-start cleanup").await;
                    return Err(error.with_cleanup(history_cleanup.and(display_cleanup)));
                }
            }
        }
        Some(_unexpected) => {
            historical.begin_shutdown();
            group_cancellation.cancel();
            let history_cleanup = historical.finish_shutdown().await;
            let display_cleanup =
                cleanup_display_runtime(iex, "Alpaca IEX invalid-topology cleanup").await;
            return Err(AccountRuntimeStartFailure::after_cleanup(
                ServiceError::Unavailable,
                history_cleanup.and(display_cleanup),
            ));
        }
        None if !options_expected => None,
        None => {
            historical.begin_shutdown();
            group_cancellation.cancel();
            let history_cleanup = historical.finish_shutdown().await;
            let display_cleanup =
                cleanup_display_runtime(iex, "Alpaca IEX invalid-topology cleanup").await;
            return Err(AccountRuntimeStartFailure::after_cleanup(
                ServiceError::Unavailable,
                history_cleanup.and(display_cleanup),
            ));
        }
    };
    let option_chain = match option_chain_config {
        Some(config) => match provider_activation.prepare_alpaca_option_chain_child(
            &activation,
            config,
            iex_publication_bindings.into_vec(),
            group_cancellation.child_token(),
        ) {
            Ok(child) => Some(child),
            Err(error) => {
                tracing::error!(%error, "Alpaca option-chain child startup failed");
                historical.begin_shutdown();
                group_cancellation.cancel();
                let mut cleanup = historical.finish_shutdown().await.err();
                if let Some(options) = options {
                    retain_shutdown_error(
                        &mut cleanup,
                        cleanup_display_runtime(
                            options,
                            "Alpaca chain partial-start options cleanup",
                        )
                        .await,
                    );
                }
                retain_shutdown_error(
                    &mut cleanup,
                    cleanup_display_runtime(iex, "Alpaca chain partial-start IEX cleanup").await,
                );
                return Err(AccountRuntimeStartFailure::after_cleanup(
                    ServiceError::Unavailable,
                    cleanup.map_or(Ok(()), Err),
                ));
            }
        },
        None => None,
    };
    let currentness = activation.currentness();
    Ok((
        AlpacaRuntimeGroup {
            option_chain,
            _activation: activation,
            historical,
            iex,
            options,
        },
        descriptors,
        currentness,
        metadata.into(),
        routes.into(),
        durable_reads,
    ))
}

fn spawn_account_currentness_monitor(
    currentness: ProviderAccountRuntimeCurrentness,
    mode: AccountCurrentnessMode,
    read_admission: DisplayMarketReadAdmission,
    lifecycle: CancellationToken,
    expiry_delay: Duration,
) -> tokio::task::JoinHandle<()> {
    spawn_account_currentness_monitor_with_check(
        move |require_active| {
            let currentness = currentness.clone();
            async move {
                if require_active {
                    currentness.is_active().await
                } else {
                    currentness.is_prepared_or_active().await
                }
            }
        },
        mode,
        read_admission,
        lifecycle,
        expiry_delay,
    )
}

fn spawn_account_currentness_monitor_with_check<Check, CheckFuture>(
    mut check_currentness: Check,
    mode: AccountCurrentnessMode,
    read_admission: DisplayMarketReadAdmission,
    lifecycle: CancellationToken,
    expiry_delay: Duration,
) -> tokio::task::JoinHandle<()>
where
    Check: FnMut(bool) -> CheckFuture + Send + 'static,
    CheckFuture: std::future::Future<Output = bool> + Send + 'static,
{
    tokio::spawn(async move {
        let expiry = tokio::time::sleep(expiry_delay);
        tokio::pin!(expiry);
        let mut interval = tokio::time::interval(Duration::from_secs(1));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                biased;
                () = lifecycle.cancelled() => break,
                () = &mut expiry => {
                    read_admission.revoke();
                    lifecycle.cancel();
                    break;
                }
                _ = interval.tick() => {
                    let require_active = read_admission.is_admitted()
                        || matches!(mode, AccountCurrentnessMode::ActiveOnly);
                    let check = check_currentness(require_active);
                    tokio::pin!(check);
                    tokio::select! {
                        biased;
                        () = lifecycle.cancelled() => break,
                        () = &mut expiry => {
                            read_admission.revoke();
                            lifecycle.cancel();
                            break;
                        }
                        current = &mut check => {
                            if !current {
                                read_admission.revoke();
                                lifecycle.cancel();
                                break;
                            }
                        }
                    }
                }
            }
        }
    })
}

/// A timeout drops only this borrow. The group still owns the same unjoined monitor.
async fn join_retained_monitor_before(
    monitor: &mut tokio::task::JoinHandle<()>,
    retained: &mut Option<Result<(), ServiceError>>,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<(), ServiceError> {
    if let Some(result) = *retained {
        return result;
    }
    tokio::select! {
        biased;
        () = cancellation.cancelled() => Err(ServiceError::Cancelled),
        () = tokio::time::sleep_until(deadline.into()) => Err(ServiceError::DeadlineExceeded),
        result = join_retained_monitor(monitor, retained) => result,
    }
}

async fn join_retained_monitor(
    monitor: &mut tokio::task::JoinHandle<()>,
    retained: &mut Option<Result<(), ServiceError>>,
) -> Result<(), ServiceError> {
    if let Some(result) = *retained {
        return result;
    }
    let result = monitor.await.map_err(|error| {
        tracing::error!(%error, "retained account monitor join failed");
        ServiceError::Unavailable
    });
    *retained = Some(result);
    result
}

async fn join_currentness_monitor_before(
    mut monitor: tokio::task::JoinHandle<()>,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<(), ServiceError> {
    tokio::select! {
        biased;
        result = &mut monitor => result.map_err(|error| {
            tracing::error!(%error, "account currentness monitor join failed");
            ServiceError::Unavailable
        }),
        () = cancellation.cancelled() => {
            monitor.abort();
            let _aborted = monitor.await;
            Err(ServiceError::Cancelled)
        }
        () = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => {
            monitor.abort();
            let _aborted = monitor.await;
            Err(ServiceError::DeadlineExceeded)
        }
    }
}

fn duration_until(
    exclusive_expires_at: market_squawk_domain::Timestamp,
) -> Result<Duration, ServiceError> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_error| ServiceError::Unavailable)?
        .as_nanos();
    let expiry = u128::try_from(exclusive_expires_at.unix_nanos())
        .map_err(|_error| ServiceError::Unauthorized)?;
    let remaining = expiry.checked_sub(now).ok_or(ServiceError::Unauthorized)?;
    let remaining = u64::try_from(remaining).map_err(|_error| ServiceError::Unavailable)?;
    Ok(Duration::from_nanos(remaining))
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
) -> Result<(KrakenLevel3LiveRuntime, ProviderAccountRuntimeCurrentness), AccountRuntimeStartFailure>
{
    let (lease, credential_authority, config, instruments) = prepared.into_parts();
    let mut activation_guard = StartupCancellation::new(group_cancellation.child_token());
    let activation = await_before(
        deadline,
        cancellation,
        provider_activation.activate_kraken_l3_account(
            lease,
            credential_authority,
            config,
            activation_guard.token(),
        ),
    )
    .await
    .map_err(AccountRuntimeStartFailure::before_owner)?;
    activation_guard.disarm();
    let currentness = activation.currentness();
    let mut runtime_guard = StartupCancellation::new(group_cancellation.child_token());
    let runtime = await_owned_start(
        deadline,
        cancellation,
        runtime_guard.token(),
        activation.start_order_level_runtime(
            app_config,
            provider_rate,
            capture_process,
            instruments,
            directory,
            runtime_guard.token(),
        ),
        |failure| {
            tracing::error!(error = %failure.cause, "account child startup failed");
            AccountRuntimeStartFailure::after_cleanup(
                ServiceError::Unavailable,
                failure.cleanup.map_err(|error| {
                    tracing::error!(%error, "account child startup cleanup failed");
                    ServiceError::Unavailable
                }),
            )
        },
    )
    .await?;
    runtime_guard.disarm();
    Ok((runtime, currentness))
}

async fn cleanup_display_runtime(
    mut runtime: ProductionDisplaySourceRuntime,
    context: &'static str,
) -> Result<(), ServiceError> {
    let result = runtime.finish_retained_shutdown().await;
    if let Err(error) = &result {
        tracing::error!(%error, context, "display child partial-start cleanup failed");
    }
    result
}

async fn cleanup_account_runtime(
    mut runtime: AccountMarketRuntime,
    lifecycle: &CancellationToken,
    _cleanup_budget: Duration,
    context: &'static str,
) -> Result<(), ServiceError> {
    runtime.begin_shutdown();
    lifecycle.cancel();
    let result = runtime.finish_retained_shutdown().await;
    if let Err(error) = &result {
        tracing::error!(%error, context, "account-market startup cleanup failed");
    }
    result
}

/// Only the retained startup task drives this continuation after an ordinary waiter has left.
async fn await_owned_start<T, E, F, Map>(
    deadline: Instant,
    cancellation: &CancellationToken,
    child_cancellation: CancellationToken,
    future: F,
    map_failure: Map,
) -> Result<T, AccountRuntimeStartFailure>
where
    F: Future<Output = Result<T, E>>,
    Map: FnOnce(E) -> AccountRuntimeStartFailure,
{
    tokio::pin!(future);
    let outcome = tokio::select! {
        biased;
        outcome = &mut future => return outcome.map_err(map_failure),
        () = cancellation.cancelled() => ServiceError::Cancelled,
        () = tokio::time::sleep_until(deadline.into()) => ServiceError::DeadlineExceeded,
    };
    child_cancellation.cancel();
    // Keep the original future and cleanup result; only the original request cause changes.
    future.await.map_err(|error| {
        let mut failure = map_failure(error);
        failure.cause = outcome;
        failure
    })
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

async fn await_currentness_before<F>(
    deadline: Instant,
    cancellation: &CancellationToken,
    future: F,
) -> Result<bool, ServiceError>
where
    F: std::future::Future<Output = bool>,
{
    tokio::select! {
        biased;
        () = cancellation.cancelled() => Err(ServiceError::Cancelled),
        () = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => {
            Err(ServiceError::DeadlineExceeded)
        }
        current = future => Ok(current),
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

fn wall_timestamp() -> Result<Timestamp, ServiceError> {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| ServiceError::Unavailable)?
        .as_nanos();
    let nanos = i64::try_from(nanos).map_err(|_| ServiceError::Unavailable)?;
    Ok(Timestamp::from_unix_nanos(nanos))
}

#[cfg(test)]
mod tests {
    use std::{future, sync::Arc};

    use tokio::sync::{Mutex, oneshot};

    use super::*;

    #[tokio::test]
    async fn pending_currentness_is_cancelled_and_joined_before_shutdown_deadline() {
        let (started_tx, started_rx) = oneshot::channel();
        let started = Arc::new(Mutex::new(Some(started_tx)));
        let read_admission = DisplayMarketReadAdmission::closed();
        let lifecycle = CancellationToken::new();
        let monitor = spawn_account_currentness_monitor_with_check(
            move |_require_active| {
                let started = Arc::clone(&started);
                async move {
                    if let Some(started) = started.lock().await.take() {
                        let _sent = started.send(());
                    }
                    future::pending::<bool>().await
                }
            },
            AccountCurrentnessMode::ActiveOnly,
            read_admission.clone(),
            lifecycle.clone(),
            Duration::from_secs(60),
        );
        tokio::time::timeout(Duration::from_millis(250), started_rx)
            .await
            .expect("first currentness check starts")
            .expect("monitor signals before cancellation");

        read_admission.revoke();
        lifecycle.cancel();
        let deadline = Instant::now()
            .checked_add(Duration::from_millis(250))
            .expect("bounded deadline");
        join_currentness_monitor_before(monitor, deadline, &CancellationToken::new())
            .await
            .expect("pending check is dropped and monitor joins");
        assert!(lifecycle.is_cancelled());
        assert!(!read_admission.is_admitted());
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
