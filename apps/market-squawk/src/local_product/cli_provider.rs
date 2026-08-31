//! Closed, evidence-bound CLI activation for supported research providers.

mod ephemeral;

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::num::{NonZeroU16, NonZeroU32, NonZeroU64};
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::Bytes;
use cap_fs_ext::DirExt as _;
use cap_std::fs::Dir;
use market_squawk_adapter_bea::{bea_api_endpoint_rule, bea_provider_rate_declaration};
use market_squawk_adapter_bls::{BlsAccessTier, BlsRequestPlan, BlsSeriesMetadata};
use market_squawk_adapter_federal_reserve::{
    BOARD_DDP_SOURCE_ID, BOARD_H15_TREASURY_CONSTANT_MATURITIES_ROLLING_DASHBOARD_DATE_COUNT,
    BoardDatasetProfile,
};
use market_squawk_adapter_files::{ExtractionLimits, ExtractionLimitsInput};
use market_squawk_adapter_fred::{
    FredSource, fred_observations_endpoint_rule, fred_release_observations_v2_endpoint_rule,
    fred_series_endpoint_rule, fred_vintage_dates_endpoint_rule,
};
use market_squawk_adapter_schwab::OAuthLoopbackError;
use market_squawk_adapter_sec::{
    RawEvidenceStore, SecParserLimits, SecRepresentationLimits, SecRepresentationRegistry,
};
use market_squawk_adapter_tiingo::tiingo_provider_rate_declaration;
use market_squawk_adapter_treasury::{
    TreasuryDailyRateFamily, TreasuryFiscalQuery, TreasurySourceConfig,
};
use market_squawk_adapter_yahoo::YAHOO_SOURCE_ID;
use market_squawk_data::ImportedUserInputEvidence;
use market_squawk_domain::{
    AssetClass, AuthorizationBasis, CalendarDate, ChecksumCapability, CoverageDelay, DataQuality,
    DeliveryEvidence, DigestAlgorithm, EffectiveInterval, EvidenceDigest, ExactPayloadEvidence,
    InstrumentId, MetadataRevision, ProviderIdentityEvidence, ProviderIdentityLocator,
    ProviderIdentityRecord, ProviderIdentityRecordInput, ProviderIdentityRegistry,
    ProviderInstrumentId, RevisionBoundPayloadEvidence, SchemaVersion, SequenceCapability,
    SourceId, SourceIdentifier, Timestamp,
};
use market_squawk_platform::{
    BoundedInput, LocalPaths, LocalSecretStoreError, UserAuthorizedInputRoot,
};
use market_squawk_sources::{
    ApiEndpointRule, AuthorizationGrant, AuthorizationMode, BackoffPolicy, BudgetScope,
    BudgetWindowSemantics, CoverageDomain, CoverageTopology, EndpointPolicy,
    FRED_ALFRED_API_SURFACE_ID, FreshnessPolicy, HistoricalCapability, HttpRequestBounds,
    InstrumentCoverage, NetworkAccessPolicy, PathScope, ProviderBudgetPolicy, ProviderBudgetWindow,
    ProviderCapabilityRevision, ProviderRateDeclaration, QueryParameterRule, QuerySensitivity,
    SEC_EDGAR_AUTHORITY, SEC_EDGAR_PROFILE_ID, SourceCapabilities, SourceClass, SourceCoverage,
    SourceMetadata, SourceMetadataInput, SourceProtocolProfile, built_in_provider_profiles,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest as _, Sha256};
use thiserror::Error;
use tokio::sync::{Mutex as AsyncMutex, OnceCell, oneshot};
use tokio::task::JoinHandle;
use tokio::time::Instant as TokioInstant;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::application::ResearchProviderRuntimeGeneration;
use crate::provider_activation::{
    BEA_SOURCE_ID, BEA_SURFACE, BeaAdapterActivation, BoardAdapterActivation,
    CommittedProviderAdapterReplacement, ControlledLocalFileAdapterActivation,
    PreparedProviderAdapterReplacement, TiingoAdapterActivation, YahooAdapterActivation,
    fixed_regional_source_config,
};
use crate::provider_onboarding::SecCikInput;
use crate::provider_onboarding::{
    SchwabMarketDoctorRunPreparation, SchwabMarketDoctorRuntimeCoordinator,
    SchwabMarketDoctorRuntimeTerminal, SchwabOAuthBrowserError, SchwabOAuthLifecycleAction,
    SchwabOAuthLifecycleView, SchwabOAuthMarketAuthority, SchwabOAuthRuntime,
    SchwabOAuthRuntimeError,
};
use crate::{
    BlsAdapterActivation, FredAdapterActivation, ProviderActivationLease,
    ProviderActivationOutcome, ProviderAdapterActivation, ProviderAdapterActivationError,
    ProviderAdapterActivationRequest, ProviderOnboardingError, ProviderOnboardingService,
    ProviderPortalActivationAuthority, ProviderPortalActivationError,
    ProviderPortalActivationRequest, ProviderPortalActivationView, SecAdapterActivation,
    StartOnboardingRequest, TreasuryAdapterActivation,
};

use super::LocalProduct;
use super::provider_activation_state::{
    ActivationEvidenceCandidate, DurableActivationQuarantineReason, DurableActivationRecipeState,
    DurableProviderActivationState, RESTORABLE_RESEARCH_SURFACES, SERIALIZED_RESEARCH_SURFACES,
};

const LEGACY_REQUEST_SCHEMA_VERSION: u16 = 2;
const EMBEDDED_PREDECESSOR_REQUEST_SCHEMA_VERSION: u16 = 3;
const PREVIOUS_REQUEST_SCHEMA_VERSION: u16 = 5;
const REQUEST_SCHEMA_VERSION: u16 = 6;
const REQUEST_MAXIMUM_BYTES: u64 = 1024 * 1024;
const SCHWAB_MARKET_DOCTOR_DURATION: Duration = Duration::from_secs(5 * 60);
const BLS_SERIES_METADATA_MAXIMUM_BYTES: u64 = 4 * 1024;
const MAXIMUM_SEC_IDENTITIES: usize = 16;
const MAXIMUM_BLS_SERIES: usize = 1_000;
const SECOND_NANOS: u64 = 1_000_000_000;
const MINUTE_NANOS: u64 = 60 * SECOND_NANOS;
const DAY_NANOS: u64 = 86_400 * SECOND_NANOS;
const SEC_IDENTITY_NAMESPACE_V1: &str =
    "https://market-squawk.local/identity/sec-cik-instrument/v1";
const BLS_PUBLIC_SURFACE: &str = "bls.v1-unregistered";
const BLS_REGISTERED_SURFACE: &str = "bls.v2-registered";
const COINBASE_DIRECT_SURFACE: &str = "coinbase.exchange-direct-market-data";
const COINBASE_PUBLIC_SURFACE: &str = "coinbase.public-market-data";
const KRAKEN_PUBLIC_SURFACE: &str = "kraken.spot-public-market-data";
const TREASURY_XML_SURFACE: &str = "treasury.daily-rates-xml";
const TREASURY_FISCAL_SURFACE: &str = "treasury.fiscal-data";
const FRED_SURFACE: &str = FRED_ALFRED_API_SURFACE_ID;
const FEDERAL_RESERVE_BOARD_SURFACE: &str = "federal-reserve-board.data-download-program";
const YAHOO_SURFACE: &str = "yahoo-finance.experimental-enrichment";
const TIINGO_SURFACE: &str = "tiingo.starter-eod-nav";
const TIINGO_SOURCE_ID: &str = "tiingo-starter";
const LOCAL_FILES_SURFACE: &str = "local.files";
const FRED_UNRATE_DASHBOARD_PROVIDER_DATASET: &str =
    "fred:series-observations:UNRATE:1776-07-04:9999-12-31";
const PORTAL_SOURCE_SURFACES: [&str; 3] = [
    COINBASE_PUBLIC_SURFACE,
    COINBASE_DIRECT_SURFACE,
    KRAKEN_PUBLIC_SURFACE,
];

/// Shared application authority behind local portal adapter activation and durable restart.
#[derive(Clone)]
pub(crate) struct ProviderResearchActivationService {
    paths: LocalPaths,
    onboarding: Arc<ProviderOnboardingService>,
    activation: Arc<ProviderAdapterActivation>,
    state: DurableProviderActivationState,
    tasks: Arc<ProviderActivationTaskAuthority>,
    schwab_doctor_tasks: Arc<SchwabMarketDoctorTaskAuthority>,
    schwab_oauth_lifecycle: Arc<SchwabOAuthServiceLifecycle>,
    schwab_oauth: Arc<OnceCell<Arc<SchwabOAuthRuntime>>>,
    schwab_oauth_factory: Option<SchwabOAuthRuntimeFactory>,
    schwab_doctor: Option<Arc<SchwabMarketDoctorRuntimeCoordinator>>,
}

pub(crate) type SchwabOAuthRuntimeFactory = Arc<
    dyn Fn() -> Result<Arc<SchwabOAuthRuntime>, SchwabOAuthRuntimeInitializationError>
        + Send
        + Sync,
>;

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub(crate) enum SchwabOAuthRuntimeInitializationError {
    #[error("the installation callback identity is unavailable")]
    Installation,
    #[error("the Schwab OAuth runtime configuration is unavailable")]
    Configuration,
    #[error("the Schwab OAuth runtime could not be constructed")]
    Runtime,
}

impl ProviderResearchActivationService {
    pub(super) fn new(
        paths: LocalPaths,
        onboarding: Arc<ProviderOnboardingService>,
        activation: Arc<ProviderAdapterActivation>,
        state: DurableProviderActivationState,
        schwab_oauth_factory: Option<SchwabOAuthRuntimeFactory>,
        schwab_doctor: Option<Arc<SchwabMarketDoctorRuntimeCoordinator>>,
    ) -> Self {
        Self {
            paths,
            onboarding,
            activation,
            state,
            tasks: Arc::new(ProviderActivationTaskAuthority::new()),
            schwab_doctor_tasks: Arc::new(SchwabMarketDoctorTaskAuthority::new()),
            schwab_oauth_lifecycle: Arc::new(SchwabOAuthServiceLifecycle::new()),
            schwab_oauth: Arc::new(OnceCell::new()),
            schwab_oauth_factory,
            schwab_doctor,
        }
    }

    /// Returns only the protected market authority from the portal-owned OAuth runtime.
    /// Tokens and secret material never cross this installed composition boundary.
    pub(crate) async fn schwab_market_authority(
        &self,
        session_id: Uuid,
        cancellation: CancellationToken,
    ) -> Result<SchwabOAuthMarketAuthority, ProviderPortalActivationError> {
        if cancellation.is_cancelled() || session_id.is_nil() {
            return Err(ProviderPortalActivationError::Unavailable);
        }
        let runtime = self
            .schwab_oauth_lifecycle
            .runtime(
                &self.schwab_oauth,
                self.schwab_oauth_factory.as_ref(),
                &cancellation,
            )
            .await?;
        runtime
            .market_authority(session_id, cancellation)
            .await
            .map_err(map_schwab_oauth_error)
    }

    /// Publishes one exact workspace-controlled local-file bundle through the same durable
    /// activation and replacement authority used by provider onboarding.
    pub(crate) async fn activate_controlled_local_files(
        &self,
        configuration: ControlledLocalFileRequest,
        cancellation: CancellationToken,
    ) -> Result<(), CliProviderActivationError> {
        self.tasks.require_admission()?;
        if cancellation.is_cancelled() {
            return Err(CliProviderActivationError::Cancelled);
        }
        let session = self
            .onboarding
            .start(
                StartOnboardingRequest::try_new(LOCAL_FILES_SURFACE, None, None)
                    .map_err(CliProviderActivationError::Onboarding)?,
                cancellation.child_token(),
            )
            .await
            .map_err(CliProviderActivationError::Onboarding)?;
        let session_id = session.session_id();
        let completion = CancellationToken::new();
        let lease = self
            .onboarding
            .prepare_runtime_activation_target(session_id, completion.clone())
            .await
            .map_err(CliProviderActivationError::Onboarding)?;
        require_surface(&lease, ProviderSurface::Exact(LOCAL_FILES_SURFACE))?;
        let request = ActivationRequest {
            schema_version: REQUEST_SCHEMA_VERSION,
            session_id,
            provider: ProviderRequest::ControlledLocalFiles { configuration },
        };
        let request_bytes =
            serde_json::to_vec(&request).map_err(|_| CliProviderActivationError::InvalidRequest)?;
        if request_bytes.is_empty()
            || u64::try_from(request_bytes.len())
                .map_or(true, |length| length > REQUEST_MAXIMUM_BYTES)
        {
            return Err(CliProviderActivationError::InvalidRequest);
        }
        let evidence = LoadedActivationEvidence {
            objects: BTreeMap::new(),
        };
        let activation =
            build_research_activation(&self.paths, &lease, &request_bytes, request, &evidence)?;
        let _activation_guard = self
            .state
            .acquire_activation(LOCAL_FILES_SURFACE)
            .await
            .map_err(|_| CliProviderActivationError::StateUnavailable)?;
        publish_research_activation(
            &self.state,
            &self.activation,
            &self.onboarding,
            &lease,
            &request_bytes,
            &evidence,
            activation,
            completion.clone(),
        )
        .await?;
        self.onboarding
            .reconcile_cleanup(session_id, completion)
            .await
            .map_err(CliProviderActivationError::Onboarding)?;
        Ok(())
    }

    async fn activate_from_portal(
        &self,
        session_id: Uuid,
        request: ProviderPortalActivationRequest,
        cancellation: CancellationToken,
    ) -> Result<ProviderPortalActivationView, CliProviderActivationError> {
        match request {
            ProviderPortalActivationRequest::Source => {
                self.activate_source_from_portal(session_id, cancellation)
                    .await
            }
            request => {
                self.activate_research_from_portal(session_id, request, cancellation)
                    .await
            }
        }
    }

    async fn activate_source_from_portal(
        &self,
        session_id: Uuid,
        cancellation: CancellationToken,
    ) -> Result<ProviderPortalActivationView, CliProviderActivationError> {
        self.tasks.require_admission()?;
        if cancellation.is_cancelled() {
            return Err(CliProviderActivationError::Cancelled);
        }
        let onboarding = Arc::clone(&self.onboarding);
        let response = self
            .tasks
            .spawn(Box::pin(async move {
                let completion = CancellationToken::new();
                let session = onboarding
                    .resume(session_id)
                    .map_err(CliProviderActivationError::Onboarding)?;
                let expected_surface = session.surface_id().to_owned();
                if !PORTAL_SOURCE_SURFACES.contains(&expected_surface.as_str()) {
                    return Err(CliProviderActivationError::SurfaceMismatch);
                }
                let lease = onboarding
                    .prepare_runtime_activation_target(session_id, completion.clone())
                    .await
                    .map_err(CliProviderActivationError::Onboarding)?;
                if lease.surface_id().as_str() != expected_surface {
                    return Err(CliProviderActivationError::SurfaceMismatch);
                }
                onboarding
                    .commit_prepared_activation(&lease)
                    .await
                    .map_err(CliProviderActivationError::Onboarding)?;
                onboarding
                    .reconcile_cleanup(session_id, completion)
                    .await
                    .map_err(CliProviderActivationError::Onboarding)?;
                let active = onboarding
                    .activation_lease(session_id)
                    .map_err(CliProviderActivationError::Onboarding)?;
                Ok(ProviderPortalActivationView::from_lease(
                    active.surface_id().clone(),
                    &active,
                ))
            }))
            .await?;
        response
            .await
            .map_err(|_error| CliProviderActivationError::StateUnavailable)?
    }

    async fn activate_research_from_portal(
        &self,
        session_id: Uuid,
        request: ProviderPortalActivationRequest,
        cancellation: CancellationToken,
    ) -> Result<ProviderPortalActivationView, CliProviderActivationError> {
        self.tasks.require_admission()?;
        if cancellation.is_cancelled() {
            return Err(CliProviderActivationError::Cancelled);
        }
        let paths = self.paths.clone();
        let state = self.state.clone();
        let activation_authority = Arc::clone(&self.activation);
        let onboarding = Arc::clone(&self.onboarding);
        let response = self
            .tasks
            .spawn(Box::pin(async move {
                let completion = CancellationToken::new();
                let lease = onboarding
                    .prepare_runtime_activation_target(session_id, completion.clone())
                    .await
                    .map_err(CliProviderActivationError::Onboarding)?;
                let (provider, evidence) = portal_provider_request(&lease, request)?;
                require_surface(&lease, provider.surface())?;
                let surface_id = lease.surface_id().as_str().to_owned();
                let _activation_guard = state
                    .acquire_activation(&surface_id)
                    .await
                    .map_err(|_error| CliProviderActivationError::StateUnavailable)?;
                let request = ActivationRequest {
                    schema_version: REQUEST_SCHEMA_VERSION,
                    session_id,
                    provider,
                };
                let request_bytes = serde_json::to_vec(&request)
                    .map_err(|_error| CliProviderActivationError::InvalidRequest)?;
                if request_bytes.is_empty()
                    || u64::try_from(request_bytes.len())
                        .map_or(true, |length| length > REQUEST_MAXIMUM_BYTES)
                {
                    return Err(CliProviderActivationError::InvalidRequest);
                }
                let activation =
                    build_research_activation(&paths, &lease, &request_bytes, request, &evidence)?;
                let provider_dataset_identifier = activation.provider_dataset_identifier().cloned();
                publish_research_activation(
                    &state,
                    &activation_authority,
                    &onboarding,
                    &lease,
                    &request_bytes,
                    &evidence,
                    activation,
                    completion.clone(),
                )
                .await?;
                onboarding
                    .reconcile_cleanup(session_id, completion)
                    .await
                    .map_err(CliProviderActivationError::Onboarding)?;
                let active = onboarding
                    .activation_lease(session_id)
                    .map_err(CliProviderActivationError::Onboarding)?;
                Ok(ProviderPortalActivationView::from_research_lease(
                    active.surface_id().clone(),
                    &active,
                    provider_dataset_identifier,
                ))
            }))
            .await?;
        response
            .await
            .map_err(|_error| CliProviderActivationError::StateUnavailable)?
    }

    async fn cancel_from_portal(
        &self,
        session_id: Uuid,
        cancellation: CancellationToken,
    ) -> Result<crate::OnboardingSessionView, CliProviderActivationError> {
        self.tasks.require_admission()?;
        if cancellation.is_cancelled() {
            return Err(CliProviderActivationError::Cancelled);
        }
        let session = self
            .onboarding
            .resume(session_id)
            .map_err(CliProviderActivationError::Onboarding)?;
        let surface_id = session.surface_id().to_owned();
        let _activation_guard = if SERIALIZED_RESEARCH_SURFACES.contains(&surface_id.as_str()) {
            Some(
                self.state
                    .acquire_activation(&surface_id)
                    .await
                    .map_err(|_error| CliProviderActivationError::StateUnavailable)?,
            )
        } else {
            None
        };
        let state = self.state.clone();
        let activation = Arc::clone(&self.activation);
        let onboarding = Arc::clone(&self.onboarding);
        let completion = CancellationToken::new();
        let response = self
            .tasks
            .spawn(Box::pin(async move {
                let _activation_guard = _activation_guard;
                if SERIALIZED_RESEARCH_SURFACES.contains(&surface_id.as_str()) {
                    let profile = SourceIdentifier::try_from(surface_id.as_str())
                        .map_err(|_error| CliProviderActivationError::ProviderConfiguration)?;
                    if let Some(runtime) = activation
                        .research_runtime_generation(&profile)
                        .map_err(CliProviderActivationError::Activation)?
                        .filter(|runtime| runtime.session_id() == session_id)
                    {
                        activation
                            .revoke_research_runtime(&runtime)
                            .await
                            .map_err(CliProviderActivationError::Activation)?;
                    }
                    if RESTORABLE_RESEARCH_SURFACES.contains(&surface_id.as_str()) {
                        match state
                            .load_recipe_for_lifecycle(&surface_id)
                            .map_err(|_error| CliProviderActivationError::StateUnavailable)?
                        {
                            DurableActivationRecipeState::Desired(recipe)
                            | DurableActivationRecipeState::Staged(recipe)
                            | DurableActivationRecipeState::Cutover(recipe)
                                if recipe.session_id == session_id =>
                            {
                                if !state
                                    .quarantine_recipe_if_current(
                                        &surface_id,
                                        recipe.state_digest,
                                        DurableActivationQuarantineReason::Cancelled,
                                    )
                                    .map_err(|_error| {
                                        CliProviderActivationError::StateUnavailable
                                    })?
                                {
                                    return Err(CliProviderActivationError::StateUnavailable);
                                }
                            }
                            DurableActivationRecipeState::Missing
                            | DurableActivationRecipeState::Desired(_)
                            | DurableActivationRecipeState::Staged(_)
                            | DurableActivationRecipeState::Cutover(_)
                            | DurableActivationRecipeState::Quarantined(_) => {}
                        }
                    }
                }
                onboarding
                    .cancel(session_id, completion)
                    .await
                    .map_err(CliProviderActivationError::Onboarding)
            }))
            .await?;
        response
            .await
            .map_err(|_error| CliProviderActivationError::StateUnavailable)?
    }
}

#[derive(Default)]
struct SchwabOAuthServiceLifecycleState {
    drained: bool,
}

/// Sole installed authority serializing lazy OAuth construction against application shutdown.
///
/// The state lock spans construction and final drain. `begin_shutdown` closes admission without
/// waiting for that lock; an initializer that already owns it observes the closed atomic after
/// construction, closes the new runtime immediately, and rejects the request. Final shutdown then
/// waits for that exact initializer before discovering and draining the sole `OnceCell` runtime.
struct SchwabOAuthServiceLifecycle {
    accepting: AtomicBool,
    state: AsyncMutex<SchwabOAuthServiceLifecycleState>,
}

impl SchwabOAuthServiceLifecycle {
    fn new() -> Self {
        Self {
            accepting: AtomicBool::new(true),
            state: AsyncMutex::new(SchwabOAuthServiceLifecycleState::default()),
        }
    }

    async fn runtime(
        &self,
        cell: &OnceCell<Arc<SchwabOAuthRuntime>>,
        factory: Option<&SchwabOAuthRuntimeFactory>,
        cancellation: &CancellationToken,
    ) -> Result<Arc<SchwabOAuthRuntime>, ProviderPortalActivationError> {
        self.require_admission(cancellation)?;
        let state = tokio::select! {
            biased;
            () = cancellation.cancelled() => {
                return Err(ProviderPortalActivationError::Cancelled);
            }
            state = self.state.lock() => state,
        };
        if state.drained {
            return Err(ProviderPortalActivationError::Unavailable);
        }
        self.require_admission(cancellation)?;
        let factory = factory.ok_or(ProviderPortalActivationError::Unavailable)?;
        let runtime = cell
            .get_or_try_init(|| async { factory() })
            .await
            .map_err(|_error| ProviderPortalActivationError::Unavailable)?
            .clone();
        if !self.accepting.load(Ordering::Acquire) {
            runtime.begin_shutdown();
            return Err(ProviderPortalActivationError::Unavailable);
        }
        drop(state);
        Ok(runtime)
    }

    fn require_admission(
        &self,
        cancellation: &CancellationToken,
    ) -> Result<(), ProviderPortalActivationError> {
        if cancellation.is_cancelled() {
            Err(ProviderPortalActivationError::Cancelled)
        } else if self.accepting.load(Ordering::Acquire) {
            Ok(())
        } else {
            Err(ProviderPortalActivationError::Unavailable)
        }
    }

    fn begin_shutdown(&self, runtime: Option<&Arc<SchwabOAuthRuntime>>) {
        self.accepting.store(false, Ordering::Release);
        if let Some(runtime) = runtime {
            runtime.begin_shutdown();
        }
    }

    async fn finish_shutdown(
        &self,
        cell: &OnceCell<Arc<SchwabOAuthRuntime>>,
        deadline: Instant,
    ) -> Result<(), ProviderPortalActivationError> {
        self.begin_shutdown(cell.get());
        let deadline_at = TokioInstant::from_std(deadline);
        let mut state = tokio::time::timeout_at(deadline_at, self.state.lock())
            .await
            .map_err(|_elapsed| ProviderPortalActivationError::StateUnavailable)?;
        if state.drained {
            return Ok(());
        }
        if let Some(runtime) = cell.get() {
            runtime.begin_shutdown();
            runtime
                .finish_shutdown(deadline)
                .await
                .map_err(map_schwab_oauth_error)?;
        }
        state.drained = true;
        Ok(())
    }
}

struct ProviderActivationTaskAuthority {
    accepting: AtomicBool,
    task: AsyncMutex<Option<JoinHandle<()>>>,
}

impl ProviderActivationTaskAuthority {
    fn new() -> Self {
        Self {
            accepting: AtomicBool::new(true),
            task: AsyncMutex::new(None),
        }
    }

    fn require_admission(&self) -> Result<(), CliProviderActivationError> {
        if self.accepting.load(Ordering::Acquire) {
            Ok(())
        } else {
            Err(CliProviderActivationError::StateUnavailable)
        }
    }

    async fn spawn<T>(
        &self,
        work: Pin<Box<dyn Future<Output = T> + Send + 'static>>,
    ) -> Result<oneshot::Receiver<T>, CliProviderActivationError>
    where
        T: Send + 'static,
    {
        self.require_admission()?;
        let mut task = self.task.lock().await;
        if task.as_ref().is_some_and(|task| !task.is_finished()) {
            return Err(CliProviderActivationError::StateUnavailable);
        }
        if let Some(previous) = task.take() {
            previous
                .await
                .map_err(|_error| CliProviderActivationError::StateUnavailable)?;
        }
        if !self.accepting.load(Ordering::Acquire) {
            return Err(CliProviderActivationError::StateUnavailable);
        }
        let (sender, receiver) = oneshot::channel();
        *task = Some(tokio::spawn(async move {
            let _response_waiter = sender.send(work.await);
        }));
        Ok(receiver)
    }

    fn begin_shutdown(&self) {
        self.accepting.store(false, Ordering::Release);
    }

    async fn finish_shutdown(&self, deadline: Instant) -> Result<(), CliProviderActivationError> {
        self.begin_shutdown();
        let deadline = TokioInstant::from_std(deadline);
        let mut slot = tokio::time::timeout_at(deadline, self.task.lock())
            .await
            .map_err(|_error| CliProviderActivationError::StateUnavailable)?;
        let Some(mut task) = slot.take() else {
            return Ok(());
        };
        drop(slot);
        match tokio::time::timeout_at(deadline, &mut task).await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(_error)) => Err(CliProviderActivationError::StateUnavailable),
            Err(_elapsed) => {
                let mut slot = self.task.lock().await;
                if slot.is_some() {
                    return Err(CliProviderActivationError::StateUnavailable);
                }
                *slot = Some(task);
                Err(CliProviderActivationError::StateUnavailable)
            }
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct SchwabMarketDoctorTaskKey {
    session_id: Uuid,
    access_token_generation: u64,
}

struct RetainedSchwabMarketDoctorTask {
    key: SchwabMarketDoctorTaskKey,
    cancellation: CancellationToken,
    task: JoinHandle<()>,
}

/// One application-owned supervisor for immediate and delayed Schwab doctor work.
///
/// The retained key coalesces repeated Connections requests for the same exact OAuth generation.
/// A replacement generation cancels and joins its predecessor before it can start, while unlink
/// and shutdown retain the same drain authority.
struct SchwabMarketDoctorTaskAuthority {
    accepting: AtomicBool,
    shutdown: CancellationToken,
    task: AsyncMutex<Option<RetainedSchwabMarketDoctorTask>>,
}

impl SchwabMarketDoctorTaskAuthority {
    fn new() -> Self {
        Self {
            accepting: AtomicBool::new(true),
            shutdown: CancellationToken::new(),
            task: AsyncMutex::new(None),
        }
    }

    async fn schedule<F, Fut>(
        &self,
        key: SchwabMarketDoctorTaskKey,
        work: F,
    ) -> Result<(), CliProviderActivationError>
    where
        F: FnOnce(CancellationToken) -> Fut,
        Fut: Future<Output = ()> + Send + 'static,
    {
        if !self.accepting.load(Ordering::Acquire) || self.shutdown.is_cancelled() {
            return Err(CliProviderActivationError::StateUnavailable);
        }
        let mut slot = self.task.lock().await;
        if slot
            .as_ref()
            .is_some_and(|retained| retained.key == key && !retained.task.is_finished())
        {
            return Ok(());
        }
        if let Some(predecessor) = slot.take() {
            if !predecessor.task.is_finished() {
                predecessor.cancellation.cancel();
            }
            predecessor
                .task
                .await
                .map_err(|_error| CliProviderActivationError::StateUnavailable)?;
        }
        if !self.accepting.load(Ordering::Acquire) || self.shutdown.is_cancelled() {
            return Err(CliProviderActivationError::StateUnavailable);
        }
        let cancellation = self.shutdown.child_token();
        let task_cancellation = cancellation.clone();
        *slot = Some(RetainedSchwabMarketDoctorTask {
            key,
            cancellation,
            task: tokio::spawn(work(task_cancellation)),
        });
        Ok(())
    }

    async fn cancel_session(
        &self,
        session_id: Uuid,
        cancellation: CancellationToken,
    ) -> Result<(), CliProviderActivationError> {
        let mut slot = tokio::select! {
            biased;
            () = cancellation.cancelled() => return Err(CliProviderActivationError::Cancelled),
            slot = self.task.lock() => slot,
        };
        let Some(retained) = slot.as_mut() else {
            return Ok(());
        };
        if retained.key.session_id != session_id {
            return Ok(());
        }
        retained.cancellation.cancel();
        let joined = tokio::select! {
            biased;
            () = cancellation.cancelled() => return Err(CliProviderActivationError::Cancelled),
            joined = &mut retained.task => joined,
        };
        drop(slot.take());
        joined.map_err(|_error| CliProviderActivationError::StateUnavailable)
    }

    fn begin_shutdown(&self) {
        self.accepting.store(false, Ordering::Release);
        self.shutdown.cancel();
    }

    async fn finish_shutdown(&self, deadline: Instant) -> Result<(), CliProviderActivationError> {
        self.begin_shutdown();
        let deadline = TokioInstant::from_std(deadline);
        let mut slot = tokio::time::timeout_at(deadline, self.task.lock())
            .await
            .map_err(|_error| CliProviderActivationError::StateUnavailable)?;
        let Some(mut retained) = slot.take() else {
            return Ok(());
        };
        match tokio::time::timeout_at(deadline, &mut retained.task).await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(_error)) => Err(CliProviderActivationError::StateUnavailable),
            Err(_elapsed) => {
                *slot = Some(retained);
                Err(CliProviderActivationError::StateUnavailable)
            }
        }
    }
}

#[allow(
    clippy::too_many_arguments,
    reason = "durable recipe, runtime candidate, and cancellation authority remain explicit"
)]
async fn publish_research_activation(
    state: &DurableProviderActivationState,
    activation_authority: &ProviderAdapterActivation,
    onboarding: &ProviderOnboardingService,
    lease: &ProviderActivationLease,
    request_bytes: &[u8],
    evidence: &LoadedActivationEvidence,
    request: ProviderAdapterActivationRequest,
    cancellation: CancellationToken,
) -> Result<(), CliProviderActivationError> {
    let surface_id = lease.surface_id().as_str();
    let evidence_digests = evidence.digests();
    let candidate = activation_authority
        .runtime_generation_for_request(lease, &request)
        .map_err(CliProviderActivationError::Activation)?;
    let candidate_runtime_digest = runtime_generation_digest(&candidate)?;
    let current_runtime = activation_authority
        .research_runtime_generation(lease.surface_id())
        .map_err(CliProviderActivationError::Activation)?;
    let current_state_digest = state
        .current_state_digest(surface_id)
        .map_err(|_error| CliProviderActivationError::StateUnavailable)?;

    if current_runtime.as_ref() == Some(&candidate) {
        return match state.load_recipe(surface_id) {
            Ok(DurableActivationRecipeState::Desired(recipe))
                if recipe.session_id == lease.session_id()
                    && recipe.request_bytes.as_ref() == request_bytes
                    && recipe.evidence_digests == evidence_digests
                    && recipe.runtime_generation_digest == candidate_runtime_digest
                    && recipe.predecessor_runtime_generation_digest
                        != Some(candidate_runtime_digest)
                    && current_state_digest == Some(recipe.state_digest) =>
            {
                Ok(())
            }
            Ok(DurableActivationRecipeState::Missing)
            | Ok(DurableActivationRecipeState::Quarantined(_))
            | Ok(DurableActivationRecipeState::Staged(_))
            | Ok(DurableActivationRecipeState::Cutover(_))
            | Ok(DurableActivationRecipeState::Desired(_))
            | Err(_) => Err(CliProviderActivationError::ProviderConfiguration),
        };
    }

    let predecessor_runtime_digest = current_runtime
        .as_ref()
        .map(runtime_generation_digest)
        .transpose()?;
    let candidate_state_digest = state
        .recipe_digest(
            surface_id,
            lease.session_id(),
            request_bytes,
            &evidence_digests,
            candidate_runtime_digest,
            predecessor_runtime_digest,
        )
        .map_err(|_error| CliProviderActivationError::StateUnavailable)?;

    if let Some(expected) = current_runtime {
        let predecessor_recipe = match state.load_recipe(surface_id) {
            Ok(DurableActivationRecipeState::Desired(recipe))
                if recipe.runtime_generation_digest
                    == predecessor_runtime_digest
                        .ok_or(CliProviderActivationError::ProviderConfiguration)?
                    && current_state_digest == Some(recipe.state_digest) =>
            {
                recipe
            }
            Ok(
                DurableActivationRecipeState::Missing
                | DurableActivationRecipeState::Desired(_)
                | DurableActivationRecipeState::Staged(_)
                | DurableActivationRecipeState::Cutover(_)
                | DurableActivationRecipeState::Quarantined(_),
            )
            | Err(_) => return Err(CliProviderActivationError::ProviderConfiguration),
        };
        let predecessor_state_digest = Some(predecessor_recipe.state_digest);
        let prepared = activation_authority
            .prepare_research_replacement(lease.clone(), request, expected.clone(), cancellation)
            .await
            .map_err(CliProviderActivationError::Activation)?;
        if prepared.candidate() != &candidate {
            return Err(CliProviderActivationError::ProviderConfiguration);
        }
        let published = state
            .publish_staged_replacement(
                surface_id,
                &predecessor_recipe,
                lease.session_id(),
                request_bytes,
                &evidence_digests,
                candidate_runtime_digest,
            )
            .map_err(|_error| CliProviderActivationError::StateUnavailable)?;
        let forward = Box::pin(publish_staged_research_replacement_forward(
            state,
            activation_authority,
            lease,
            evidence,
            &candidate,
            prepared,
            predecessor_state_digest,
            published,
            candidate_state_digest,
        ))
        .await;
        if let Err(failure) = forward {
            let ReplacementPublishFailure {
                transaction,
                predecessor_state_digest,
                published_state_digest,
                candidate_state_digest,
                reason,
                caller_error,
            } = failure;
            reconcile_failed_replacement(
                state,
                activation_authority,
                onboarding,
                transaction,
                predecessor_state_digest,
                published_state_digest,
                candidate_state_digest,
                reason,
            )
            .await?;
            return Err(caller_error);
        }
        state
            .reconcile_evidence_objects()
            .map_err(|_error| CliProviderActivationError::StateUnavailable)?;
        return Ok(());
    }

    let published = state
        .publish_staged_recipe(
            surface_id,
            current_state_digest,
            lease.session_id(),
            request_bytes,
            &evidence_digests,
            candidate_runtime_digest,
            None,
        )
        .map_err(|_error| CliProviderActivationError::StateUnavailable)?;
    if let Err(error) = evidence.persist(state) {
        quarantine_failed_candidate(
            state,
            onboarding,
            surface_id,
            lease.session_id(),
            published,
            DurableActivationQuarantineReason::StateInvalid,
        )?;
        return Err(error);
    }
    let active = match onboarding.commit_prepared_activation(lease).await {
        Ok(active) => active,
        Err(error) => {
            quarantine_failed_candidate(
                state,
                onboarding,
                surface_id,
                lease.session_id(),
                published,
                DurableActivationQuarantineReason::AuthorityInvalidated,
            )?;
            return Err(CliProviderActivationError::Onboarding(error));
        }
    };
    if let Err(error) = require_same_activation_lease(&active, lease) {
        quarantine_failed_candidate(
            state,
            onboarding,
            surface_id,
            lease.session_id(),
            published,
            DurableActivationQuarantineReason::AuthorityInvalidated,
        )?;
        return Err(error);
    }
    let outcome = match activation_authority
        .activate_exact_research_profile(&candidate, request, cancellation)
        .await
    {
        Ok(outcome) => outcome,
        Err(error) => {
            quarantine_failed_candidate(
                state,
                onboarding,
                surface_id,
                lease.session_id(),
                published,
                DurableActivationQuarantineReason::AdapterRejected,
            )?;
            return Err(CliProviderActivationError::Activation(error));
        }
    };
    let Some(activated_generation) = provider_activation_generation(&outcome) else {
        quarantine_failed_candidate(
            state,
            onboarding,
            surface_id,
            lease.session_id(),
            published,
            DurableActivationQuarantineReason::AdapterRejected,
        )?;
        return Err(CliProviderActivationError::ProviderConfiguration);
    };
    if activated_generation != &candidate {
        activation_authority
            .revoke_research_runtime(&candidate)
            .await
            .map_err(CliProviderActivationError::Activation)?;
        quarantine_failed_candidate(
            state,
            onboarding,
            surface_id,
            lease.session_id(),
            published,
            DurableActivationQuarantineReason::AdapterRejected,
        )?;
        return Err(CliProviderActivationError::ProviderConfiguration);
    }
    let desired = match state.promote_staged_recipe(surface_id, published) {
        Ok(desired) => desired,
        Err(_error) => {
            activation_authority
                .revoke_research_runtime(&candidate)
                .await
                .map_err(CliProviderActivationError::Activation)?;
            quarantine_failed_candidate(
                state,
                onboarding,
                surface_id,
                lease.session_id(),
                published,
                DurableActivationQuarantineReason::StateInvalid,
            )?;
            return Err(CliProviderActivationError::StateUnavailable);
        }
    };
    if desired != candidate_state_digest {
        activation_authority
            .revoke_research_runtime(&candidate)
            .await
            .map_err(CliProviderActivationError::Activation)?;
        quarantine_failed_candidate(
            state,
            onboarding,
            surface_id,
            lease.session_id(),
            desired,
            DurableActivationQuarantineReason::StateInvalid,
        )?;
        return Err(CliProviderActivationError::StateUnavailable);
    }
    state
        .reconcile_evidence_objects()
        .map_err(|_error| CliProviderActivationError::StateUnavailable)?;
    Ok(())
}

struct ReplacementPublishFailure {
    transaction: ReplacementRuntimeTransaction,
    predecessor_state_digest: Option<EvidenceDigest>,
    published_state_digest: EvidenceDigest,
    candidate_state_digest: EvidenceDigest,
    reason: DurableActivationQuarantineReason,
    caller_error: CliProviderActivationError,
}

impl ReplacementPublishFailure {
    const fn new(
        transaction: ReplacementRuntimeTransaction,
        predecessor_state_digest: Option<EvidenceDigest>,
        published_state_digest: EvidenceDigest,
        candidate_state_digest: EvidenceDigest,
        reason: DurableActivationQuarantineReason,
        caller_error: CliProviderActivationError,
    ) -> Self {
        Self {
            transaction,
            predecessor_state_digest,
            published_state_digest,
            candidate_state_digest,
            reason,
            caller_error,
        }
    }
}

#[allow(
    clippy::too_many_arguments,
    reason = "exact staged, predecessor, and candidate state remain explicit across compensation"
)]
async fn publish_staged_research_replacement_forward(
    state: &DurableProviderActivationState,
    activation_authority: &ProviderAdapterActivation,
    lease: &ProviderActivationLease,
    evidence: &LoadedActivationEvidence,
    candidate: &ResearchProviderRuntimeGeneration,
    mut prepared: PreparedProviderAdapterReplacement,
    predecessor_state_digest: Option<EvidenceDigest>,
    published_state_digest: EvidenceDigest,
    candidate_state_digest: EvidenceDigest,
) -> Result<(), ReplacementPublishFailure> {
    if let Err(error) = evidence.persist(state) {
        return Err(ReplacementPublishFailure::new(
            ReplacementRuntimeTransaction::Prepared(prepared),
            predecessor_state_digest,
            published_state_digest,
            candidate_state_digest,
            DurableActivationQuarantineReason::StateInvalid,
            error,
        ));
    }
    if let Err(error) = activation_authority
        .revoke_replacement_predecessor(&mut prepared)
        .await
    {
        return Err(ReplacementPublishFailure::new(
            ReplacementRuntimeTransaction::Prepared(prepared),
            predecessor_state_digest,
            published_state_digest,
            candidate_state_digest,
            DurableActivationQuarantineReason::AdapterRejected,
            CliProviderActivationError::Activation(error),
        ));
    }
    let mut committed = match activation_authority
        .commit_research_replacement(&mut prepared)
        .await
    {
        Ok(committed) => committed,
        Err(error) => {
            return Err(ReplacementPublishFailure::new(
                ReplacementRuntimeTransaction::Prepared(prepared),
                predecessor_state_digest,
                published_state_digest,
                candidate_state_digest,
                DurableActivationQuarantineReason::AuthorityInvalidated,
                CliProviderActivationError::Activation(error),
            ));
        }
    };
    let active = match activation_authority
        .commit_replacement_onboarding(&committed)
        .await
    {
        Ok(active) => active,
        Err(error) => {
            return Err(ReplacementPublishFailure::new(
                ReplacementRuntimeTransaction::Committed(committed),
                predecessor_state_digest,
                published_state_digest,
                candidate_state_digest,
                DurableActivationQuarantineReason::AuthorityInvalidated,
                CliProviderActivationError::Activation(error),
            ));
        }
    };
    if let Err(error) = require_same_activation_lease(&active, lease) {
        return Err(ReplacementPublishFailure::new(
            ReplacementRuntimeTransaction::Committed(committed),
            predecessor_state_digest,
            published_state_digest,
            candidate_state_digest,
            DurableActivationQuarantineReason::AuthorityInvalidated,
            error,
        ));
    }
    let cutover =
        match state.commit_staged_cutover(lease.surface_id().as_str(), published_state_digest) {
            Ok(cutover) => cutover,
            Err(_error) => {
                return Err(ReplacementPublishFailure::new(
                    ReplacementRuntimeTransaction::Committed(committed),
                    predecessor_state_digest,
                    published_state_digest,
                    candidate_state_digest,
                    DurableActivationQuarantineReason::StateInvalid,
                    CliProviderActivationError::StateUnavailable,
                ));
            }
        };
    if let Err(error) = activation_authority
        .retire_replacement_predecessor(&committed, cutover)
        .await
    {
        return Err(ReplacementPublishFailure::new(
            ReplacementRuntimeTransaction::Committed(committed),
            predecessor_state_digest,
            cutover,
            candidate_state_digest,
            DurableActivationQuarantineReason::AuthorityInvalidated,
            CliProviderActivationError::Activation(error),
        ));
    }
    let activated = match activation_authority
        .finalize_research_replacement(&mut committed)
        .await
    {
        Ok(activated) => activated,
        Err(error) => {
            return Err(ReplacementPublishFailure::new(
                ReplacementRuntimeTransaction::Committed(committed),
                predecessor_state_digest,
                cutover,
                candidate_state_digest,
                DurableActivationQuarantineReason::AdapterRejected,
                CliProviderActivationError::Activation(error),
            ));
        }
    };
    if activated.generation() != candidate {
        return Err(ReplacementPublishFailure::new(
            ReplacementRuntimeTransaction::Committed(committed),
            predecessor_state_digest,
            cutover,
            candidate_state_digest,
            DurableActivationQuarantineReason::AdapterRejected,
            CliProviderActivationError::ProviderConfiguration,
        ));
    }
    let desired = match state.complete_cutover_recipe(lease.surface_id().as_str(), cutover) {
        Ok(desired) => desired,
        Err(_error) => {
            return Err(ReplacementPublishFailure::new(
                ReplacementRuntimeTransaction::Committed(committed),
                predecessor_state_digest,
                cutover,
                candidate_state_digest,
                DurableActivationQuarantineReason::StateInvalid,
                CliProviderActivationError::StateUnavailable,
            ));
        }
    };
    if desired != candidate_state_digest {
        return Err(ReplacementPublishFailure::new(
            ReplacementRuntimeTransaction::Committed(committed),
            predecessor_state_digest,
            desired,
            candidate_state_digest,
            DurableActivationQuarantineReason::StateInvalid,
            CliProviderActivationError::StateUnavailable,
        ));
    }
    Ok(())
}

fn runtime_generation_digest(
    generation: &ResearchProviderRuntimeGeneration,
) -> Result<EvidenceDigest, CliProviderActivationError> {
    generation
        .generation_digest()
        .map_err(ProviderAdapterActivationError::from)
        .map_err(CliProviderActivationError::Activation)
}

fn provider_activation_generation(
    outcome: &ProviderActivationOutcome,
) -> Option<&ResearchProviderRuntimeGeneration> {
    match outcome {
        ProviderActivationOutcome::Research(activated) => Some(activated.generation()),
        ProviderActivationOutcome::Yahoo(activated) => Some(activated.generation()),
        ProviderActivationOutcome::Tiingo(activated) => Some(activated.generation()),
        ProviderActivationOutcome::Live(_) | ProviderActivationOutcome::CoinbaseDirect(_) => None,
    }
}

enum ReplacementRuntimeTransaction {
    Prepared(PreparedProviderAdapterReplacement),
    Committed(CommittedProviderAdapterReplacement),
}

impl ReplacementRuntimeTransaction {
    fn expected(&self) -> &ResearchProviderRuntimeGeneration {
        match self {
            Self::Prepared(prepared) => prepared.expected(),
            Self::Committed(committed) => committed.expected(),
        }
    }

    fn candidate(&self) -> &ResearchProviderRuntimeGeneration {
        match self {
            Self::Prepared(prepared) => prepared.candidate(),
            Self::Committed(committed) => committed.candidate(),
        }
    }

    async fn into_committed(
        self,
        activation_authority: &ProviderAdapterActivation,
    ) -> Result<CommittedProviderAdapterReplacement, ProviderAdapterActivationError> {
        match self {
            Self::Prepared(mut prepared) => {
                activation_authority
                    .commit_research_replacement(&mut prepared)
                    .await
            }
            Self::Committed(committed) => Ok(committed),
        }
    }
}

#[allow(
    clippy::too_many_arguments,
    reason = "independent runtime, durable-state, and onboarding authority identities stay explicit"
)]
async fn reconcile_failed_replacement(
    state: &DurableProviderActivationState,
    activation_authority: &ProviderAdapterActivation,
    onboarding: &ProviderOnboardingService,
    transaction: ReplacementRuntimeTransaction,
    predecessor_state_digest: Option<EvidenceDigest>,
    staged_state_digest: EvidenceDigest,
    desired_candidate_state_digest: EvidenceDigest,
    reason: DurableActivationQuarantineReason,
) -> Result<(), CliProviderActivationError> {
    let expected = transaction.expected().clone();
    let candidate = transaction.candidate().clone();
    if expected.profile() != candidate.profile() {
        return Err(CliProviderActivationError::ProviderConfiguration);
    }
    let expected_runtime_digest = runtime_generation_digest(&expected)?;
    let candidate_runtime_digest = runtime_generation_digest(&candidate)?;
    let mut transaction = Some(transaction);

    #[derive(Clone, Copy)]
    enum DurableReplacementPhase {
        Staged,
        Cutover,
        Desired,
    }

    let durable_phase = match state.load_recipe_for_lifecycle(expected.profile().as_str()) {
        Ok(DurableActivationRecipeState::Staged(recipe))
            if replacement_recipe_is_exact(
                &recipe,
                expected_runtime_digest,
                candidate_runtime_digest,
                predecessor_state_digest,
            ) && recipe.state_digest == staged_state_digest =>
        {
            Some(DurableReplacementPhase::Staged)
        }
        Ok(DurableActivationRecipeState::Cutover(recipe))
            if replacement_recipe_is_exact(
                &recipe,
                expected_runtime_digest,
                candidate_runtime_digest,
                predecessor_state_digest,
            ) && recipe.state_digest == staged_state_digest =>
        {
            Some(DurableReplacementPhase::Cutover)
        }
        Ok(DurableActivationRecipeState::Desired(recipe))
            if recipe.runtime_generation_digest == candidate_runtime_digest
                && recipe.predecessor_runtime_generation_digest
                    == Some(expected_runtime_digest)
                && recipe.state_digest == desired_candidate_state_digest =>
        {
            Some(DurableReplacementPhase::Desired)
        }
        Ok(
            DurableActivationRecipeState::Missing
            | DurableActivationRecipeState::Desired(_)
            | DurableActivationRecipeState::Staged(_)
            | DurableActivationRecipeState::Cutover(_)
            | DurableActivationRecipeState::Quarantined(_),
        )
        | Err(_) => None,
    };
    let predecessor_current = activation_authority
        .research_runtime_lease_is_current(&expected)
        .await
        .unwrap_or(false);
    if predecessor_current && matches!(durable_phase, Some(DurableReplacementPhase::Staged)) {
        let candidate_discarded = match transaction.as_ref() {
            Some(ReplacementRuntimeTransaction::Prepared(prepared)) => activation_authority
                .discard_prepared_replacement_candidate(prepared, staged_state_digest)
                .await
                .is_ok(),
            Some(ReplacementRuntimeTransaction::Committed(committed)) => activation_authority
                .discard_committed_replacement_candidate(committed, staged_state_digest)
                .await
                .is_ok(),
            None => false,
        };
        if candidate_discarded {
            let restored_runtime = match transaction
                .take()
                .ok_or(CliProviderActivationError::StateUnavailable)?
            {
                ReplacementRuntimeTransaction::Prepared(prepared) => {
                    activation_authority
                        .rollback_prepared_research_replacement(prepared)
                        .await
                }
                ReplacementRuntimeTransaction::Committed(committed) => {
                    activation_authority
                        .rollback_committed_research_replacement(committed)
                        .await
                }
            };
            let restored_state = predecessor_state_digest.and_then(|predecessor_state_digest| {
                state
                    .restore_staged_predecessor(expected.profile().as_str(), staged_state_digest)
                    .ok()
                    .filter(|restored| restored == &predecessor_state_digest)
            });
            if restored_state.is_some()
                && matches!(restored_runtime, Ok(ref restored) if restored == &expected)
                && matches!(
                    activation_authority.research_runtime_generation(expected.profile()),
                    Ok(Some(ref current)) if current == &expected
                )
            {
                let evidence_reconciled = state.reconcile_evidence_objects().is_ok();
                return if evidence_reconciled {
                    Ok(())
                } else {
                    Err(CliProviderActivationError::StateUnavailable)
                };
            }
        }
    }

    let candidate_current = activation_authority
        .research_runtime_lease_is_current(&candidate)
        .await
        .unwrap_or(false);
    if candidate_current
        && matches!(
            durable_phase,
            Some(
                DurableReplacementPhase::Staged
                    | DurableReplacementPhase::Cutover
                    | DurableReplacementPhase::Desired
            )
        )
    {
        let Some(pending) = transaction.take() else {
            return Err(CliProviderActivationError::StateUnavailable);
        };
        if let Ok(mut committed) = pending.into_committed(activation_authority).await {
            let cutover_digest = match durable_phase {
                Some(DurableReplacementPhase::Staged) => state
                    .commit_staged_cutover(candidate.profile().as_str(), staged_state_digest)
                    .ok(),
                Some(DurableReplacementPhase::Cutover) => Some(staged_state_digest),
                Some(DurableReplacementPhase::Desired) => None,
                None => None,
            };
            let predecessor_retired = match cutover_digest {
                Some(cutover_digest) => activation_authority
                    .retire_replacement_predecessor(&committed, cutover_digest)
                    .await
                    .is_ok(),
                None => !predecessor_current,
            };
            let finalized = if committed.runtime_is_finalized()
                || matches!(
                    activation_authority.research_runtime_generation(candidate.profile()),
                    Ok(Some(ref current)) if current == &candidate
                ) {
                true
            } else {
                matches!(
                    activation_authority
                        .finalize_research_replacement(&mut committed)
                        .await,
                    Ok(ref activated) if activated.generation() == &candidate
                )
            };
            let desired_exact = match cutover_digest {
                Some(cutover_digest) if finalized => matches!(
                    state.complete_cutover_recipe(candidate.profile().as_str(), cutover_digest),
                    Ok(desired) if desired == desired_candidate_state_digest
                ),
                None => matches!(
                    state.load_recipe(candidate.profile().as_str()),
                    Ok(DurableActivationRecipeState::Desired(recipe))
                        if recipe.runtime_generation_digest == candidate_runtime_digest
                            && recipe.predecessor_runtime_generation_digest
                                == Some(expected_runtime_digest)
                            && recipe.state_digest == desired_candidate_state_digest
                ),
                Some(_) => false,
            };
            if predecessor_retired && finalized && desired_exact {
                return state
                    .reconcile_evidence_objects()
                    .map_err(|_error| CliProviderActivationError::StateUnavailable);
            }
            drop(committed);
        }
    }

    drop(transaction.take());
    let mut reconciled = true;
    match activation_authority.research_runtime_generation(expected.profile()) {
        Ok(Some(current)) if current == expected || current == candidate => {
            if activation_authority
                .revoke_research_runtime(&current)
                .await
                .is_err()
            {
                reconciled = false;
            }
        }
        Ok(None) => {}
        Ok(Some(_)) | Err(_) => reconciled = false,
    }
    let surface_id = candidate.profile().as_str();
    let quarantine_digest = state.quarantine_recipe(surface_id, reason).ok();
    if quarantine_digest.is_none() {
        reconciled = false;
    }
    let invalidation_evidence = quarantine_digest.unwrap_or(desired_candidate_state_digest);
    for session_id in [expected.session_id(), candidate.session_id()] {
        if onboarding
            .invalidate_activation_recipe(session_id, invalidation_evidence)
            .is_err()
        {
            reconciled = false;
        }
    }
    if state.reconcile_evidence_objects().is_err() {
        reconciled = false;
    }
    if reconciled {
        Ok(())
    } else {
        Err(CliProviderActivationError::StateUnavailable)
    }
}

fn replacement_recipe_is_exact(
    recipe: &super::provider_activation_state::DurableActivationRecipe,
    expected_runtime_digest: EvidenceDigest,
    candidate_runtime_digest: EvidenceDigest,
    predecessor_state_digest: Option<EvidenceDigest>,
) -> bool {
    recipe.runtime_generation_digest == candidate_runtime_digest
        && recipe.predecessor_runtime_generation_digest == Some(expected_runtime_digest)
        && matches!(
            recipe.staged_predecessor.as_deref(),
            Some(predecessor)
                if predecessor.runtime_generation_digest == expected_runtime_digest
                    && Some(predecessor.state_digest) == predecessor_state_digest
        )
}

fn quarantine_failed_candidate(
    state: &DurableProviderActivationState,
    onboarding: &ProviderOnboardingService,
    surface_id: &str,
    session_id: Uuid,
    candidate_state_digest: EvidenceDigest,
    reason: DurableActivationQuarantineReason,
) -> Result<(), CliProviderActivationError> {
    if state
        .quarantine_recipe_if_current(surface_id, candidate_state_digest, reason)
        .map_err(|_error| CliProviderActivationError::StateUnavailable)?
    {
        let quarantine = state
            .load_recipe(surface_id)
            .map_err(|_error| CliProviderActivationError::StateUnavailable)?;
        let DurableActivationRecipeState::Quarantined(quarantine) = quarantine else {
            return Err(CliProviderActivationError::StateUnavailable);
        };
        onboarding
            .invalidate_activation_recipe(session_id, quarantine.state_digest)
            .map_err(CliProviderActivationError::Onboarding)?;
        state
            .reconcile_evidence_objects()
            .map_err(|_error| CliProviderActivationError::StateUnavailable)
    } else {
        Err(CliProviderActivationError::StateUnavailable)
    }
}

fn require_same_activation_lease(
    current: &ProviderActivationLease,
    expected: &ProviderActivationLease,
) -> Result<(), CliProviderActivationError> {
    if current.same_authority_as(expected) {
        Ok(())
    } else {
        Err(CliProviderActivationError::ProviderConfiguration)
    }
}

#[async_trait]
impl ProviderPortalActivationAuthority for ProviderResearchActivationService {
    async fn activate(
        &self,
        session_id: Uuid,
        request: ProviderPortalActivationRequest,
        cancellation: CancellationToken,
    ) -> Result<ProviderPortalActivationView, ProviderPortalActivationError> {
        self.activate_from_portal(session_id, request, cancellation)
            .await
            .map_err(map_portal_activation_error)
    }

    fn provider_dataset_identifier(
        &self,
        profile: &SourceIdentifier,
    ) -> Result<Option<SourceIdentifier>, ProviderPortalActivationError> {
        self.activation
            .registered_discovery_dataset(profile)
            .map_err(CliProviderActivationError::Activation)
            .map_err(map_portal_activation_error)
    }

    async fn cancel(
        &self,
        session_id: Uuid,
        cancellation: CancellationToken,
    ) -> Result<crate::OnboardingSessionView, ProviderPortalActivationError> {
        self.cancel_from_portal(session_id, cancellation)
            .await
            .map_err(map_portal_activation_error)
    }

    async fn schwab_oauth(
        &self,
        session_id: Uuid,
        action: SchwabOAuthLifecycleAction,
        cancellation: CancellationToken,
    ) -> Result<SchwabOAuthLifecycleView, ProviderPortalActivationError> {
        if action == SchwabOAuthLifecycleAction::Unlink {
            self.schwab_doctor_tasks
                .cancel_session(session_id, cancellation.child_token())
                .await
                .map_err(map_portal_activation_error)?;
        }
        let runtime = self
            .schwab_oauth_lifecycle
            .runtime(
                &self.schwab_oauth,
                self.schwab_oauth_factory.as_ref(),
                &cancellation,
            )
            .await?;
        let view = runtime
            .apply(session_id, action, cancellation)
            .await
            .map_err(map_schwab_oauth_error)?;
        if !matches!(
            action,
            SchwabOAuthLifecycleAction::Begin | SchwabOAuthLifecycleAction::Continue
        ) {
            return Ok(view);
        }
        let authority = match runtime
            .market_authority(session_id, CancellationToken::new())
            .await
        {
            Ok(authority) => authority,
            Err(
                SchwabOAuthRuntimeError::ReauthorizationRequired
                | SchwabOAuthRuntimeError::AuthorizationExchangeInFlight,
            ) => return Ok(view),
            Err(error) => return Err(map_schwab_oauth_error(error)),
        };
        let current = authority
            .current_receipt()
            .await
            .map_err(|_error| ProviderPortalActivationError::Unavailable)?;
        let preparation = self
            .onboarding
            .prepare_schwab_market_doctor_run(
                session_id,
                current.generation().get(),
                CancellationToken::new(),
            )
            .await
            .map_err(map_schwab_doctor_onboarding_error)?;
        if matches!(&preparation, SchwabMarketDoctorRunPreparation::Current) {
            return Ok(view);
        }
        let doctor = self
            .schwab_doctor
            .as_ref()
            .cloned()
            .ok_or(ProviderPortalActivationError::Unavailable)?;
        let onboarding = Arc::clone(&self.onboarding);
        let key = SchwabMarketDoctorTaskKey {
            session_id,
            access_token_generation: current.generation().get(),
        };
        self.schwab_doctor_tasks
            .schedule(key, move |completion| async move {
                run_schwab_market_doctor_task(
                    onboarding,
                    doctor,
                    authority,
                    key,
                    preparation,
                    completion,
                )
                .await;
            })
            .await
            .map_err(map_portal_activation_error)?;
        Ok(view)
    }

    fn begin_shutdown(&self) {
        self.tasks.begin_shutdown();
        self.schwab_doctor_tasks.begin_shutdown();
        self.schwab_oauth_lifecycle
            .begin_shutdown(self.schwab_oauth.get());
    }

    async fn finish_shutdown(
        &self,
        deadline: Instant,
    ) -> Result<(), ProviderPortalActivationError> {
        let tasks = self.tasks.finish_shutdown(deadline);
        let schwab_doctor_tasks = self.schwab_doctor_tasks.finish_shutdown(deadline);
        let oauth = self
            .schwab_oauth_lifecycle
            .finish_shutdown(&self.schwab_oauth, deadline);
        let (tasks, schwab_doctor_tasks, oauth) = tokio::join!(tasks, schwab_doctor_tasks, oauth);
        tasks.map_err(map_portal_activation_error)?;
        schwab_doctor_tasks.map_err(map_portal_activation_error)?;
        oauth
    }
}

async fn run_schwab_market_doctor_task(
    onboarding: Arc<ProviderOnboardingService>,
    doctor: Arc<SchwabMarketDoctorRuntimeCoordinator>,
    authority: SchwabOAuthMarketAuthority,
    key: SchwabMarketDoctorTaskKey,
    mut preparation: SchwabMarketDoctorRunPreparation,
    cancellation: CancellationToken,
) {
    let deferred_wait = match &preparation {
        SchwabMarketDoctorRunPreparation::Deferred { wait } => Some(*wait),
        SchwabMarketDoctorRunPreparation::Current | SchwabMarketDoctorRunPreparation::Ready(_) => {
            None
        }
    };
    if let Some(wait) = deferred_wait {
        tokio::select! {
            biased;
            () = cancellation.cancelled() => return,
            () = tokio::time::sleep(wait) => {}
        }
    }
    let current = tokio::select! {
        biased;
        () = cancellation.cancelled() => return,
        current = authority.current_receipt() => match current {
            Ok(current) => current,
            Err(error) => {
                tracing::warn!(%error, "Schwab market-data verification authority is unavailable");
                return;
            }
        }
    };
    if current.generation().get() != key.access_token_generation {
        tracing::warn!(
            "Schwab market-data verification was superseded by a newer authorization generation"
        );
        return;
    }
    if deferred_wait.is_some() {
        preparation = match onboarding
            .prepare_schwab_market_doctor_run(
                key.session_id,
                key.access_token_generation,
                cancellation.child_token(),
            )
            .await
        {
            Ok(preparation) => preparation,
            Err(error) => {
                tracing::warn!(%error, "Schwab market-data verification renewal was not admitted");
                return;
            }
        };
    }
    let lease = match preparation {
        SchwabMarketDoctorRunPreparation::Ready(lease) => lease,
        SchwabMarketDoctorRunPreparation::Current => return,
        SchwabMarketDoctorRunPreparation::Deferred { .. } => {
            tracing::warn!(
                "Schwab market-data verification renewal did not reach its retained boundary"
            );
            return;
        }
    };
    match doctor
        .run(
            lease,
            authority,
            cancellation,
            Instant::now() + SCHWAB_MARKET_DOCTOR_DURATION,
        )
        .await
    {
        Ok(SchwabMarketDoctorRuntimeTerminal::Observed(_)) => {
            tracing::info!("Schwab market-data verification completed");
        }
        Ok(SchwabMarketDoctorRuntimeTerminal::SetupRequired(_)) => {
            tracing::warn!("Schwab market-data verification requires owner setup");
        }
        Err(error) => {
            tracing::warn!(%error, "Schwab market-data verification failed");
        }
    }
}

/// Activates one already-onboarded research provider from a closed, no-follow request.
///
/// The request never carries credential bytes or caller-made rights evidence. Provider-specific
/// inputs are read beneath the request's retained input-root capability. Persistence authority
/// comes only from the active code-owned onboarding lease.
pub(super) async fn activate_research_provider(
    product: &LocalProduct,
    request_path: &Path,
    confirm: bool,
    cancellation: CancellationToken,
) -> Result<Value, CliProviderActivationError> {
    if !confirm {
        return Err(CliProviderActivationError::ConfirmationRequired);
    }
    if cancellation.is_cancelled() {
        return Err(CliProviderActivationError::Cancelled);
    }
    let (root, input, request) = read_request(request_path)?;
    let onboarding = product.provider_onboarding();
    let lease = onboarding
        .prepare_runtime_activation_target(request.session_id, cancellation.clone())
        .await
        .map_err(CliProviderActivationError::Onboarding)?;
    require_surface(&lease, request.provider.surface())?;
    validate_file_fred_request_scope(&lease, &request)?;
    let evidence = LoadedActivationEvidence::from_user(&root, &request)?;
    if cancellation.is_cancelled() {
        return Err(CliProviderActivationError::Cancelled);
    }
    let surface_id = lease.surface_id().as_str().to_owned();
    let _activation_guard = product
        .provider_activation_state()
        .acquire_activation(&surface_id)
        .await
        .map_err(|_| CliProviderActivationError::StateUnavailable)?;
    let session_id = request.session_id;
    let activation = build_research_activation(
        product.paths(),
        &lease,
        input.as_bytes(),
        request,
        &evidence,
    )?;
    let provider_dataset_identifier = activation.provider_dataset_identifier().cloned();
    if cancellation.is_cancelled() {
        return Err(CliProviderActivationError::Cancelled);
    }
    publish_research_activation(
        product.provider_activation_state(),
        product.provider_activation().as_ref(),
        product.provider_onboarding().as_ref(),
        &lease,
        input.as_bytes(),
        &evidence,
        activation,
        cancellation.clone(),
    )
    .await?;
    product
        .provider_onboarding()
        .reconcile_cleanup(session_id, cancellation)
        .await
        .map_err(CliProviderActivationError::Onboarding)?;
    Ok(activation_result(
        lease.surface_id(),
        &lease,
        provider_dataset_identifier.as_ref(),
    ))
}

pub(super) fn restore_research_providers(
    paths: &LocalPaths,
    onboarding: &crate::ProviderOnboardingService,
    activation_authority: &crate::ProviderAdapterActivation,
    state: &DurableProviderActivationState,
) {
    if state.reconcile_evidence_objects().is_err() {
        tracing::error!(
            "provider activation evidence reconciliation failed; affected providers remain disabled"
        );
    }
    for surface_id in RESTORABLE_RESEARCH_SURFACES {
        let recipe = match state.load_recipe_for_startup_recovery(surface_id) {
            Ok(DurableActivationRecipeState::Missing) => continue,
            Ok(DurableActivationRecipeState::Quarantined(quarantine)) => {
                enforce_recovery_quarantine(
                    onboarding,
                    surface_id,
                    quarantine.session_id,
                    quarantine.reason,
                    quarantine.state_digest,
                );
                continue;
            }
            Ok(DurableActivationRecipeState::Staged(recipe)) => {
                recover_research_replacement(
                    paths,
                    onboarding,
                    activation_authority,
                    state,
                    surface_id,
                    recipe,
                    DurableReplacementRecoveryPhase::Staged,
                );
                continue;
            }
            Ok(DurableActivationRecipeState::Cutover(recipe)) => {
                recover_research_replacement(
                    paths,
                    onboarding,
                    activation_authority,
                    state,
                    surface_id,
                    recipe,
                    DurableReplacementRecoveryPhase::Cutover,
                );
                continue;
            }
            Ok(DurableActivationRecipeState::Desired(recipe)) => recipe,
            Err(_error) => {
                quarantine_failed_recovery(
                    onboarding,
                    state,
                    surface_id,
                    None,
                    DurableActivationQuarantineReason::StateInvalid,
                );
                continue;
            }
        };
        let session_id = recipe.session_id;
        let restored = restore_research_provider(
            paths,
            onboarding,
            activation_authority,
            state,
            surface_id,
            recipe,
        );
        match restored {
            Ok(ResearchProviderRecovery::Restored) => {}
            Ok(ResearchProviderRecovery::ResumeRequired) => {
                tracing::warn!(
                    surface_id,
                    "provider activation remains disabled until an explicit user resume"
                );
            }
            Err(error) => {
                let reason = recovery_quarantine_reason(&error);
                quarantine_failed_recovery(onboarding, state, surface_id, Some(session_id), reason);
            }
        }
    }
}

/// Reconstructs one exact retained research recipe for explicit source-lifecycle resume.
///
/// Unlike startup recovery, this path may unlock the already admitted credential generation. It
/// never accepts provider configuration from the lifecycle caller: the existing durable recipe,
/// digest-addressed evidence, active onboarding lease, and provider-specific builder remain the
/// sole construction authority.
pub(super) async fn resume_exact_research_provider(
    paths: &LocalPaths,
    onboarding: &crate::ProviderOnboardingService,
    activation_authority: &crate::ProviderAdapterActivation,
    state: &DurableProviderActivationState,
    surface_id: &str,
    expected_session_id: Uuid,
    cancellation: CancellationToken,
) -> Result<ResearchProviderRuntimeGeneration, CliProviderActivationError> {
    if cancellation.is_cancelled() {
        return Err(CliProviderActivationError::Cancelled);
    }
    let recipe = match state
        .load_recipe_for_lifecycle(surface_id)
        .map_err(|_| CliProviderActivationError::StateUnavailable)?
    {
        DurableActivationRecipeState::Desired(recipe)
            if recipe.session_id == expected_session_id =>
        {
            recipe
        }
        DurableActivationRecipeState::Missing
        | DurableActivationRecipeState::Desired(_)
        | DurableActivationRecipeState::Staged(_)
        | DurableActivationRecipeState::Cutover(_)
        | DurableActivationRecipeState::Quarantined(_) => {
            return Err(CliProviderActivationError::StateUnavailable);
        }
    };
    let prepared = prepare_research_provider_recovery(
        paths,
        onboarding,
        activation_authority,
        state,
        surface_id,
        &recipe,
    )?;
    if cancellation.is_cancelled() {
        return Err(CliProviderActivationError::Cancelled);
    }
    let expected = prepared.generation.clone();
    let outcome = activation_authority
        .activate_exact_research_profile(&expected, prepared.request, cancellation)
        .await
        .map_err(CliProviderActivationError::Activation)?;
    let Some(activated_generation) = provider_activation_generation(&outcome) else {
        return Err(CliProviderActivationError::ProviderConfiguration);
    };
    if activated_generation != &expected {
        return Err(CliProviderActivationError::ProviderConfiguration);
    }
    Ok(expected)
}

#[derive(Clone, Copy)]
enum DurableReplacementRecoveryPhase {
    Staged,
    Cutover,
}

enum ExactCandidateRecovery {
    Active(PreparedResearchProviderRecovery),
    Prepared(PreparedResearchProviderRecovery),
}

fn recover_research_replacement(
    paths: &LocalPaths,
    onboarding: &crate::ProviderOnboardingService,
    activation_authority: &crate::ProviderAdapterActivation,
    state: &DurableProviderActivationState,
    surface_id: &str,
    candidate_recipe: super::provider_activation_state::DurableActivationRecipe,
    phase: DurableReplacementRecoveryPhase,
) {
    let candidate_session = candidate_recipe.session_id;
    let replacement_digest = candidate_recipe.state_digest;
    let desired_candidate_digest = state.recipe_digest(
        surface_id,
        candidate_recipe.session_id,
        &candidate_recipe.request_bytes,
        &candidate_recipe.evidence_digests,
        candidate_recipe.runtime_generation_digest,
        candidate_recipe.predecessor_runtime_generation_digest,
    );
    let Some(predecessor_recipe) = candidate_recipe.staged_predecessor.as_deref() else {
        quarantine_failed_recovery(
            onboarding,
            state,
            surface_id,
            Some(candidate_session),
            DurableActivationQuarantineReason::StateInvalid,
        );
        return;
    };
    let predecessor_session = predecessor_recipe.session_id;
    let predecessor_state_digest = predecessor_recipe.state_digest;
    let prepared_predecessor = prepare_research_provider_recovery(
        paths,
        onboarding,
        activation_authority,
        state,
        surface_id,
        predecessor_recipe,
    );
    let predecessor_is_current = prepared_predecessor.is_ok();
    let candidate_authority = prepare_exact_candidate_recovery(
        paths,
        onboarding,
        activation_authority,
        state,
        surface_id,
        &candidate_recipe,
    );

    if let DurableReplacementRecoveryPhase::Staged = phase {
        match prepared_predecessor {
            Ok(prepared_predecessor) => {
                let candidate_discarded = match candidate_authority {
                    Ok(ExactCandidateRecovery::Active(candidate)) => onboarding
                        .invalidate_activation_recipe(candidate.session_id, replacement_digest)
                        .is_ok(),
                    Ok(ExactCandidateRecovery::Prepared(candidate)) => onboarding
                        .discard_prepared_activation_at_startup(
                            &candidate.lease,
                            replacement_digest,
                        )
                        .is_ok(),
                    Err(_error) => true,
                };
                let restored = candidate_discarded
                    && matches!(
                        state.restore_staged_predecessor(surface_id, replacement_digest),
                        Ok(restored_digest) if restored_digest == predecessor_state_digest
                    );
                if restored {
                    match restore_prepared_research_provider(
                        activation_authority,
                        prepared_predecessor,
                    ) {
                        Ok(ResearchProviderRecovery::Restored) => {
                            if state.reconcile_evidence_objects().is_err() {
                                tracing::warn!(
                                    surface_id,
                                    "restored predecessor evidence reconciliation remains pending"
                                );
                            }
                            return;
                        }
                        Ok(ResearchProviderRecovery::ResumeRequired) => {
                            tracing::warn!(
                                surface_id,
                                "restored predecessor remains disabled until an explicit user resume"
                            );
                            return;
                        }
                        Err(error) => {
                            quarantine_failed_replacement_recovery(
                                onboarding,
                                state,
                                surface_id,
                                predecessor_session,
                                candidate_session,
                                replacement_digest,
                                recovery_quarantine_reason(&error),
                            );
                            return;
                        }
                    }
                }
                quarantine_failed_replacement_recovery(
                    onboarding,
                    state,
                    surface_id,
                    predecessor_session,
                    candidate_session,
                    replacement_digest,
                    DurableActivationQuarantineReason::StateInvalid,
                );
                return;
            }
            Err(_error) => {}
        }
    }

    let Ok(desired_candidate_digest) = desired_candidate_digest else {
        quarantine_failed_replacement_recovery(
            onboarding,
            state,
            surface_id,
            predecessor_session,
            candidate_session,
            replacement_digest,
            DurableActivationQuarantineReason::StateInvalid,
        );
        return;
    };
    let Ok(ExactCandidateRecovery::Active(prepared_candidate)) = candidate_authority else {
        quarantine_failed_replacement_recovery(
            onboarding,
            state,
            surface_id,
            predecessor_session,
            candidate_session,
            replacement_digest,
            DurableActivationQuarantineReason::AuthorityInvalidated,
        );
        return;
    };
    let cutover_digest = match phase {
        DurableReplacementRecoveryPhase::Staged => {
            match state.commit_staged_cutover(surface_id, replacement_digest) {
                Ok(cutover) => cutover,
                Err(_error) => {
                    quarantine_failed_replacement_recovery(
                        onboarding,
                        state,
                        surface_id,
                        predecessor_session,
                        candidate_session,
                        replacement_digest,
                        DurableActivationQuarantineReason::StateInvalid,
                    );
                    return;
                }
            }
        }
        DurableReplacementRecoveryPhase::Cutover => replacement_digest,
    };
    if predecessor_is_current
        && onboarding
            .invalidate_activation_recipe(predecessor_session, cutover_digest)
            .is_err()
    {
        quarantine_failed_replacement_recovery(
            onboarding,
            state,
            surface_id,
            predecessor_session,
            candidate_session,
            cutover_digest,
            DurableActivationQuarantineReason::AuthorityInvalidated,
        );
        return;
    }
    let recovery =
        match restore_prepared_research_provider(activation_authority, prepared_candidate) {
            Ok(recovery) => recovery,
            Err(error) => {
                quarantine_failed_replacement_recovery(
                    onboarding,
                    state,
                    surface_id,
                    predecessor_session,
                    candidate_session,
                    cutover_digest,
                    recovery_quarantine_reason(&error),
                );
                return;
            }
        };
    if !matches!(
        state.complete_cutover_recipe(surface_id, cutover_digest),
        Ok(desired) if desired == desired_candidate_digest
    ) {
        quarantine_failed_replacement_recovery(
            onboarding,
            state,
            surface_id,
            predecessor_session,
            candidate_session,
            cutover_digest,
            DurableActivationQuarantineReason::StateInvalid,
        );
        return;
    }
    if matches!(recovery, ResearchProviderRecovery::ResumeRequired) {
        tracing::warn!(
            surface_id,
            "restored candidate remains disabled until an explicit user resume"
        );
    }
    if state.reconcile_evidence_objects().is_err() {
        tracing::warn!(
            surface_id,
            "restored candidate evidence reconciliation remains pending"
        );
    }
}

enum ResearchProviderRecovery {
    Restored,
    ResumeRequired,
}

struct PreparedResearchProviderRecovery {
    session_id: Uuid,
    lease: ProviderActivationLease,
    request: ProviderAdapterActivationRequest,
    generation: ResearchProviderRuntimeGeneration,
}

fn restore_research_provider(
    paths: &LocalPaths,
    onboarding: &crate::ProviderOnboardingService,
    activation_authority: &crate::ProviderAdapterActivation,
    state: &DurableProviderActivationState,
    surface_id: &str,
    recipe: super::provider_activation_state::DurableActivationRecipe,
) -> Result<ResearchProviderRecovery, CliProviderActivationError> {
    let prepared = prepare_research_provider_recovery(
        paths,
        onboarding,
        activation_authority,
        state,
        surface_id,
        &recipe,
    )?;
    restore_prepared_research_provider(activation_authority, prepared)
}

fn prepare_research_provider_recovery(
    paths: &LocalPaths,
    onboarding: &crate::ProviderOnboardingService,
    activation_authority: &crate::ProviderAdapterActivation,
    state: &DurableProviderActivationState,
    surface_id: &str,
    recipe: &super::provider_activation_state::DurableActivationRecipe,
) -> Result<PreparedResearchProviderRecovery, CliProviderActivationError> {
    let lease = onboarding
        .activation_lease(recipe.session_id)
        .map_err(CliProviderActivationError::Onboarding)?;
    prepare_research_provider_recovery_with_lease(
        paths,
        activation_authority,
        state,
        surface_id,
        recipe,
        lease,
    )
}

fn prepare_exact_candidate_recovery(
    paths: &LocalPaths,
    onboarding: &crate::ProviderOnboardingService,
    activation_authority: &crate::ProviderAdapterActivation,
    state: &DurableProviderActivationState,
    surface_id: &str,
    recipe: &super::provider_activation_state::DurableActivationRecipe,
) -> Result<ExactCandidateRecovery, CliProviderActivationError> {
    if let Ok(active) = prepare_research_provider_recovery(
        paths,
        onboarding,
        activation_authority,
        state,
        surface_id,
        recipe,
    ) {
        return Ok(ExactCandidateRecovery::Active(active));
    }
    let lease = onboarding
        .prepared_activation_lease(recipe.session_id)
        .map_err(CliProviderActivationError::Onboarding)?;
    prepare_research_provider_recovery_with_lease(
        paths,
        activation_authority,
        state,
        surface_id,
        recipe,
        lease,
    )
    .map(ExactCandidateRecovery::Prepared)
}

fn prepare_research_provider_recovery_with_lease(
    paths: &LocalPaths,
    activation_authority: &crate::ProviderAdapterActivation,
    state: &DurableProviderActivationState,
    surface_id: &str,
    recipe: &super::provider_activation_state::DurableActivationRecipe,
    lease: ProviderActivationLease,
) -> Result<PreparedResearchProviderRecovery, CliProviderActivationError> {
    if recipe.request_bytes.len()
        > usize::try_from(REQUEST_MAXIMUM_BYTES)
            .map_err(|_| CliProviderActivationError::InvalidRequest)?
    {
        return Err(CliProviderActivationError::InvalidRequest);
    }
    let request = decode_request(&recipe.request_bytes)?;
    if request.session_id != recipe.session_id {
        return Err(CliProviderActivationError::InvalidRequest);
    }
    if lease.surface_id().as_str() != surface_id {
        return Err(CliProviderActivationError::SurfaceMismatch);
    }
    require_surface(&lease, request.provider.surface())?;
    let evidence = LoadedActivationEvidence::from_durable(state, &request)?;
    if evidence.digests() != recipe.evidence_digests {
        return Err(CliProviderActivationError::InvalidRequest);
    }
    let adapter =
        build_research_activation(paths, &lease, &recipe.request_bytes, request, &evidence)?;
    let candidate = activation_authority
        .runtime_generation_for_request(&lease, &adapter)
        .map_err(CliProviderActivationError::Activation)?;
    if runtime_generation_digest(&candidate)? != recipe.runtime_generation_digest {
        return Err(CliProviderActivationError::ProviderConfiguration);
    }
    Ok(PreparedResearchProviderRecovery {
        session_id: recipe.session_id,
        lease,
        request: adapter,
        generation: candidate,
    })
}

fn restore_prepared_research_provider(
    activation_authority: &crate::ProviderAdapterActivation,
    prepared: PreparedResearchProviderRecovery,
) -> Result<ResearchProviderRecovery, CliProviderActivationError> {
    let outcome =
        match activation_authority.restore_active_profile(prepared.session_id, prepared.request) {
            Ok(outcome) => outcome,
            Err(error) if recovery_requires_explicit_resume(&error) => {
                return Ok(ResearchProviderRecovery::ResumeRequired);
            }
            Err(error) => return Err(CliProviderActivationError::Activation(error)),
        };
    let Some(activated_generation) = provider_activation_generation(&outcome) else {
        return Err(CliProviderActivationError::ProviderConfiguration);
    };
    if activated_generation != &prepared.generation {
        return Err(CliProviderActivationError::ProviderConfiguration);
    }
    Ok(ResearchProviderRecovery::Restored)
}

fn recovery_requires_explicit_resume(error: &ProviderAdapterActivationError) -> bool {
    matches!(
        error,
        ProviderAdapterActivationError::ExplicitResumeRequired
            | ProviderAdapterActivationError::Onboarding(
                ProviderOnboardingError::SecretOperationUnavailable
                    | ProviderOnboardingError::OperationCancelled
            )
    ) || matches!(
        error,
        ProviderAdapterActivationError::Onboarding(ProviderOnboardingError::SecretStore(
            LocalSecretStoreError::ProviderUnavailable
                | LocalSecretStoreError::SessionUnavailable
                | LocalSecretStoreError::Locked
                | LocalSecretStoreError::InteractionRequired
                | LocalSecretStoreError::UserCancelled
                | LocalSecretStoreError::OperationCancelled
                | LocalSecretStoreError::DeadlineExceeded
        ))
    )
}

fn quarantine_failed_replacement_recovery(
    onboarding: &crate::ProviderOnboardingService,
    state: &DurableProviderActivationState,
    surface_id: &str,
    predecessor_session: Uuid,
    candidate_session: Uuid,
    replacement_state_digest: EvidenceDigest,
    reason: DurableActivationQuarantineReason,
) {
    let state_digest = match state.quarantine_recipe(surface_id, reason) {
        Ok(state_digest) => state_digest,
        Err(_error) => {
            tracing::error!(
                surface_id,
                reason = ?reason,
                "provider replacement recovery failed closed; durable quarantine could not be recorded"
            );
            replacement_state_digest
        }
    };
    for session_id in [predecessor_session, candidate_session] {
        if onboarding
            .invalidate_activation_recipe(session_id, state_digest)
            .is_err()
        {
            tracing::warn!(
                surface_id,
                session_id = %session_id,
                "provider replacement recovery retained onboarding cleanup debt"
            );
        }
    }
    if state.reconcile_evidence_objects().is_err() {
        tracing::warn!(
            surface_id,
            "provider replacement quarantine retained unreconciled evidence debt"
        );
    }
}

fn quarantine_failed_recovery(
    onboarding: &crate::ProviderOnboardingService,
    state: &DurableProviderActivationState,
    surface_id: &str,
    session_id: Option<Uuid>,
    reason: DurableActivationQuarantineReason,
) {
    match state.quarantine_recipe(surface_id, reason) {
        Ok(state_digest) => {
            enforce_recovery_quarantine(onboarding, surface_id, session_id, reason, state_digest);
            if state.reconcile_evidence_objects().is_err() {
                tracing::warn!(
                    surface_id,
                    "provider activation quarantine retained unreconciled evidence debt"
                );
            }
        }
        Err(_error) => {
            tracing::error!(
                surface_id,
                reason = ?reason,
                "provider activation recovery failed closed; durable quarantine could not be recorded"
            );
        }
    }
}

fn enforce_recovery_quarantine(
    onboarding: &crate::ProviderOnboardingService,
    surface_id: &str,
    session_id: Option<Uuid>,
    reason: DurableActivationQuarantineReason,
    state_digest: EvidenceDigest,
) {
    if let Some(session_id) = session_id
        && onboarding
            .invalidate_activation_recipe(session_id, state_digest)
            .is_err()
    {
        tracing::warn!(
            surface_id,
            reason = ?reason,
            "provider activation recipe is quarantined but its onboarding session could not be blocked"
        );
        return;
    }
    tracing::warn!(
        surface_id,
        reason = ?reason,
        "provider activation recipe is quarantined; re-onboarding is required"
    );
}

fn recovery_quarantine_reason(
    error: &CliProviderActivationError,
) -> DurableActivationQuarantineReason {
    match error {
        CliProviderActivationError::Onboarding(_) => {
            DurableActivationQuarantineReason::AuthorityInvalidated
        }
        CliProviderActivationError::Activation(_) => {
            DurableActivationQuarantineReason::AdapterRejected
        }
        CliProviderActivationError::StateUnavailable
        | CliProviderActivationError::InputUnavailable => {
            DurableActivationQuarantineReason::StateInvalid
        }
        CliProviderActivationError::ConfirmationRequired
        | CliProviderActivationError::InvalidRequest
        | CliProviderActivationError::SurfaceMismatch
        | CliProviderActivationError::InvalidRights
        | CliProviderActivationError::InvalidMetadata
        | CliProviderActivationError::ProviderConfiguration
        | CliProviderActivationError::Cancelled => {
            DurableActivationQuarantineReason::RequestSuperseded
        }
    }
}

fn build_research_activation(
    paths: &LocalPaths,
    lease: &ProviderActivationLease,
    request_bytes: &[u8],
    request: ActivationRequest,
    evidence: &LoadedActivationEvidence,
) -> Result<ProviderAdapterActivationRequest, CliProviderActivationError> {
    let activation_evidence = activation_evidence(request_bytes, lease);
    let metadata_effective = EffectiveInterval::new(
        lease.authority_effective_at(),
        lease.verification_expires_at(),
    )
    .map_err(|_| CliProviderActivationError::InvalidRights)?;
    let activation = match request.provider {
        ProviderRequest::Sec { identities } => {
            let metadata = metadata_with_source_id(
                lease,
                activation_evidence,
                SEC_EDGAR_AUTHORITY
                    .activation_revision_evidence(activation_evidence)
                    .map_err(|_| CliProviderActivationError::InvalidMetadata)?,
                SEC_EDGAR_AUTHORITY
                    .canonical_source_id()
                    .map_err(|_| CliProviderActivationError::InvalidMetadata)?,
                SEC_EDGAR_AUTHORITY.rate_scope(),
                SourceClass::RegulatoryFiling,
                CoverageDomain::RegulatoryFilings,
                AuthorizationMode::PublicInterface,
                HistoricalCapability::RevisionPreserving,
                metadata_effective,
                SEC_EDGAR_AUTHORITY
                    .endpoint_policy()
                    .map_err(|_| CliProviderActivationError::InvalidMetadata)?,
                SEC_EDGAR_AUTHORITY
                    .budget_policy()
                    .map_err(|_| CliProviderActivationError::InvalidMetadata)?,
            )?;
            let identities = sec_identity_registry(
                &metadata,
                identities,
                activation_evidence,
                metadata_effective,
                lease.issued_at(),
            )?;
            let (raw_store, representations) = sec_state(paths, activation_evidence)?;
            ProviderAdapterActivationRequest::Sec(SecAdapterActivation::new(
                metadata,
                raw_store,
                representations,
                identities,
                SecParserLimits::production_defaults(),
            ))
        }
        ProviderRequest::Bls {
            series_metadata,
            start_year,
            end_year,
        } => {
            let tier = bls_tier(lease)?;
            let series = bls_series(evidence, &series_metadata, activation_evidence)?;
            let series_ids = series
                .iter()
                .map(|metadata| metadata.series_id().to_owned())
                .collect();
            let plan = BlsRequestPlan::try_new(tier, series_ids, start_year, end_year)
                .map_err(|_| CliProviderActivationError::ProviderConfiguration)?;
            let endpoint = match tier {
                BlsAccessTier::PublicV1 => "https://api.bls.gov/publicAPI/v1/timeseries/data/",
                BlsAccessTier::RegisteredV2 => "https://api.bls.gov/publicAPI/v2/timeseries/data/",
            };
            let authorization_mode = match tier {
                BlsAccessTier::PublicV1 => AuthorizationMode::PublicInterface,
                BlsAccessTier::RegisteredV2 => AuthorizationMode::UserAuthorized,
            };
            let metadata = metadata(
                lease,
                activation_evidence,
                "bls",
                "us-bls",
                SourceClass::OfficialAgency,
                CoverageDomain::Macroeconomic,
                authorization_mode,
                HistoricalCapability::Historical,
                metadata_effective,
                exact_endpoint_policy(endpoint, 16 * 1024 * 1024)?,
                bls_budget(lease, authorization_mode, plan.limits().daily_queries())?,
            )?;
            ProviderAdapterActivationRequest::Bls(
                BlsAdapterActivation::try_new(metadata, tier, series, start_year, end_year)
                    .map_err(|_error| CliProviderActivationError::ProviderConfiguration)?,
            )
        }
        ProviderRequest::BeaRegional => {
            let config = fixed_regional_source_config()
                .map_err(|_| CliProviderActivationError::ProviderConfiguration)?;
            let provider_dataset = config
                .contracts()
                .first()
                .filter(|_| config.contracts().len() == 1)
                .map(|contract| contract.dataset_id().clone())
                .ok_or(CliProviderActivationError::ProviderConfiguration)?;
            let metadata = bea_metadata(lease, activation_evidence, metadata_effective, &config)?;
            ProviderAdapterActivationRequest::Bea(BeaAdapterActivation::new(
                metadata,
                provider_dataset,
            ))
        }
        ProviderRequest::TreasuryFiscal {
            first_record_date,
            last_record_date,
            page_size,
        } => {
            let page_size = NonZeroU16::new(page_size)
                .ok_or(CliProviderActivationError::ProviderConfiguration)?;
            let query = TreasuryFiscalQuery::average_interest_rates_v2(
                first_record_date,
                last_record_date,
                page_size,
            )
            .map_err(|_| CliProviderActivationError::ProviderConfiguration)?;
            let config = TreasurySourceConfig::average_interest_rates(query);
            let metadata =
                treasury_metadata(lease, activation_evidence, metadata_effective, &config)?;
            ProviderAdapterActivationRequest::Treasury(TreasuryAdapterActivation::new(
                metadata, config,
            ))
        }
        ProviderRequest::TreasuryDailyRates => {
            let config = TreasurySourceConfig::daily_rates_all_history()
                .map_err(|_| CliProviderActivationError::ProviderConfiguration)?;
            let metadata =
                treasury_metadata(lease, activation_evidence, metadata_effective, &config)?;
            ProviderAdapterActivationRequest::Treasury(TreasuryAdapterActivation::new(
                metadata, config,
            ))
        }
        ProviderRequest::FredAlfred { configuration } => {
            require_current_fred_revision(lease)?;
            let FredProviderRequest { provider_dataset } = *configuration;
            let metadata = metadata(
                lease,
                activation_evidence,
                "fred",
                "fred",
                SourceClass::OfficialAgency,
                CoverageDomain::Macroeconomic,
                AuthorizationMode::UserAuthorized,
                HistoricalCapability::RevisionPreserving,
                metadata_effective,
                fred_network_policy()?,
                fred_budget(lease)?,
            )?;
            ProviderAdapterActivationRequest::Fred(
                FredAdapterActivation::try_new(metadata, provider_dataset)
                    .map_err(CliProviderActivationError::Activation)?,
            )
        }
        ProviderRequest::FederalReserveBoardH15 => {
            let profile = BoardDatasetProfile::h15_treasury_constant_maturities_rolling_dashboard()
                .map_err(|_| CliProviderActivationError::ProviderConfiguration)?;
            let metadata = federal_reserve_board_metadata(
                lease,
                activation_evidence,
                metadata_effective,
                &profile,
            )?;
            ProviderAdapterActivationRequest::Board(BoardAdapterActivation::new(metadata, profile))
        }
        ProviderRequest::YahooEnrichment => {
            let metadata = yahoo_metadata(lease, activation_evidence, metadata_effective)?;
            ProviderAdapterActivationRequest::Yahoo(
                YahooAdapterActivation::try_new(metadata)
                    .map_err(CliProviderActivationError::Activation)?,
            )
        }
        ProviderRequest::TiingoStarterEodNav => {
            let metadata = tiingo_metadata(lease, activation_evidence, metadata_effective)?;
            ProviderAdapterActivationRequest::Tiingo(
                TiingoAdapterActivation::try_new(metadata)
                    .map_err(CliProviderActivationError::Activation)?,
            )
        }
        ProviderRequest::ControlledLocalFiles { configuration } => {
            let manifest_digest = sha256_evidence(&configuration.manifest_sha256)?;
            let admitted_input_set = sha256_evidence(&configuration.admitted_input_set_sha256)?;
            let local_admission = sha256_evidence(&configuration.local_admission_evidence_sha256)?;
            let workspace_receipt =
                sha256_evidence(&configuration.workspace_receipt_evidence_sha256)?;
            let import_receipt = sha256_evidence(&configuration.import_receipt_evidence_sha256)?;
            let evidence = ImportedUserInputEvidence::try_new(
                admitted_input_set,
                manifest_digest,
                local_admission,
                workspace_receipt,
                import_receipt,
            )
            .map_err(|_| CliProviderActivationError::InvalidRights)?;
            let limits = ExtractionLimits::try_new(ExtractionLimitsInput::standard())
                .map_err(|_| CliProviderActivationError::ProviderConfiguration)?;
            let root = paths
                .artifacts()
                .map_err(|_| CliProviderActivationError::InputUnavailable)?
                .open_controlled_import_root(&configuration.root_reference)
                .map_err(|_| CliProviderActivationError::InputUnavailable)?;
            let manifest = root
                .resolve(&configuration.manifest_reference)
                .and_then(|input| {
                    input.open_bounded(ExtractionLimitsInput::standard().max_manifest_bytes)
                })
                .and_then(|input| input.read_bounded())
                .map_err(|_| CliProviderActivationError::InputUnavailable)?;
            if manifest.digest() != manifest_digest {
                return Err(CliProviderActivationError::InvalidRequest);
            }
            let metadata = controlled_local_file_metadata(
                lease,
                activation_evidence,
                manifest_digest,
                Timestamp::from_unix_nanos(configuration.admitted_at_unix_nanos),
            )?;
            let digest_hex = lower_hex(&manifest_digest.bytes());
            let representation_state_root = paths
                .control_root()
                .map_err(|_| CliProviderActivationError::StateUnavailable)?
                .root()
                .join("sources/controlled-file-representations")
                .join(digest_hex);
            ProviderAdapterActivationRequest::ControlledLocalFiles(
                ControlledLocalFileAdapterActivation::new(
                    metadata,
                    root,
                    representation_state_root,
                    manifest,
                    limits,
                    evidence,
                ),
            )
        }
    };
    Ok(activation)
}

fn activation_result(
    profile: &SourceIdentifier,
    lease: &ProviderActivationLease,
    provider_dataset_identifier: Option<&SourceIdentifier>,
) -> Value {
    json!({
        "profile": profile.as_str(),
        "providerDatasetIdentifier": provider_dataset_identifier
            .map(SourceIdentifier::as_str),
        "sessionId": lease.session_id().to_string(),
        "capabilityRevision": lease.capability_revision().get(),
        "capabilityEvidence": lease.capability_digest(),
        "rightsDecisionEvidence": lease.rights_decision_digest(),
        "persistenceRightsEvidence": lease.persistence_evidence(),
        "publicConfigurationEvidence": lease.public_configuration_digest(),
        "credentialGeneration": lease.generation().map(|generation| generation.get()),
        "verificationExpiresAtUnixNanos": lease
            .verification_expires_at()
            .map(Timestamp::unix_nanos),
        "authorityEffectiveAtUnixNanos": lease.authority_effective_at().unix_nanos(),
        "issuedAtUnixNanos": lease.issued_at().unix_nanos(),
    })
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ActivationRequest {
    schema_version: u16,
    session_id: Uuid,
    provider: ProviderRequest,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, tag = "kind", rename_all = "snake_case")]
enum ProviderRequest {
    Sec {
        #[serde(default)]
        identities: Vec<SecIdentityMappingRequest>,
    },
    Bls {
        series_metadata: Vec<ExactInputReference>,
        start_year: u16,
        end_year: u16,
    },
    BeaRegional,
    TreasuryFiscal {
        first_record_date: CalendarDate,
        last_record_date: CalendarDate,
        page_size: u16,
    },
    TreasuryDailyRates,
    FredAlfred {
        configuration: Box<FredProviderRequest>,
    },
    FederalReserveBoardH15,
    YahooEnrichment,
    TiingoStarterEodNav,
    ControlledLocalFiles {
        configuration: ControlledLocalFileRequest,
    },
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ControlledLocalFileRequest {
    pub(crate) root_reference: PathBuf,
    pub(crate) manifest_reference: PathBuf,
    pub(crate) manifest_sha256: String,
    pub(crate) admitted_input_set_sha256: String,
    pub(crate) local_admission_evidence_sha256: String,
    pub(crate) workspace_receipt_evidence_sha256: String,
    pub(crate) import_receipt_evidence_sha256: String,
    pub(crate) admitted_at_unix_nanos: i64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct SecIdentityMappingRequest {
    cik: SecCikInput,
    instrument_id: InstrumentId,
}

impl ProviderRequest {
    const fn surface(&self) -> ProviderSurface {
        match self {
            Self::Sec { .. } => ProviderSurface::Exact(SEC_EDGAR_PROFILE_ID),
            Self::Bls { .. } => ProviderSurface::Either(BLS_PUBLIC_SURFACE, BLS_REGISTERED_SURFACE),
            Self::BeaRegional => ProviderSurface::Exact(BEA_SURFACE),
            Self::TreasuryFiscal { .. } => ProviderSurface::Exact(TREASURY_FISCAL_SURFACE),
            Self::TreasuryDailyRates => ProviderSurface::Exact(TREASURY_XML_SURFACE),
            Self::FredAlfred { .. } => ProviderSurface::Exact(FRED_SURFACE),
            Self::FederalReserveBoardH15 => ProviderSurface::Exact(FEDERAL_RESERVE_BOARD_SURFACE),
            Self::YahooEnrichment => ProviderSurface::Exact(YAHOO_SURFACE),
            Self::TiingoStarterEodNav => ProviderSurface::Exact(TIINGO_SURFACE),
            Self::ControlledLocalFiles { .. } => ProviderSurface::Exact(LOCAL_FILES_SURFACE),
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct LegacyActivationRequest {
    schema_version: u16,
    session_id: Uuid,
    provider: LegacyProviderRequest,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, tag = "kind", rename_all = "snake_case")]
enum LegacyProviderRequest {
    Sec,
    Bls {
        series_metadata: Vec<ExactInputReference>,
        start_year: u16,
        end_year: u16,
    },
    TreasuryFiscal {
        first_record_date: CalendarDate,
        last_record_date: CalendarDate,
        page_size: u16,
    },
}

impl From<LegacyProviderRequest> for ProviderRequest {
    fn from(request: LegacyProviderRequest) -> Self {
        match request {
            LegacyProviderRequest::Sec => Self::Sec {
                identities: Vec::new(),
            },
            LegacyProviderRequest::Bls {
                series_metadata,
                start_year,
                end_year,
            } => Self::Bls {
                series_metadata,
                start_year,
                end_year,
            },
            LegacyProviderRequest::TreasuryFiscal {
                first_record_date,
                last_record_date,
                page_size,
            } => Self::TreasuryFiscal {
                first_record_date,
                last_record_date,
                page_size,
            },
        }
    }
}

fn portal_provider_request(
    lease: &ProviderActivationLease,
    request: ProviderPortalActivationRequest,
) -> Result<(ProviderRequest, LoadedActivationEvidence), CliProviderActivationError> {
    match request {
        ProviderPortalActivationRequest::Source => Err(CliProviderActivationError::SurfaceMismatch),
        ProviderPortalActivationRequest::FederalReserveBoardH15 => {
            require_surface(lease, ProviderSurface::Exact(FEDERAL_RESERVE_BOARD_SURFACE))?;
            Ok((
                ProviderRequest::FederalReserveBoardH15,
                LoadedActivationEvidence {
                    objects: BTreeMap::new(),
                },
            ))
        }
        ProviderPortalActivationRequest::YahooEnrichment => {
            require_surface(lease, ProviderSurface::Exact(YAHOO_SURFACE))?;
            Ok((
                ProviderRequest::YahooEnrichment,
                LoadedActivationEvidence {
                    objects: BTreeMap::new(),
                },
            ))
        }
        ProviderPortalActivationRequest::TiingoStarterEodNav => {
            require_surface(lease, ProviderSurface::Exact(TIINGO_SURFACE))?;
            Ok((
                ProviderRequest::TiingoStarterEodNav,
                LoadedActivationEvidence {
                    objects: BTreeMap::new(),
                },
            ))
        }
        ProviderPortalActivationRequest::Sec { cik } => {
            require_surface(lease, ProviderSurface::Exact(SEC_EDGAR_PROFILE_ID))?;
            Ok((
                ProviderRequest::Sec {
                    identities: vec![SecIdentityMappingRequest {
                        instrument_id: sec_instrument_id(&cik)?,
                        cik,
                    }],
                },
                LoadedActivationEvidence {
                    objects: BTreeMap::new(),
                },
            ))
        }
        ProviderPortalActivationRequest::TreasuryFiscal {
            first_record_date,
            last_record_date,
            page_size,
        } => {
            require_surface(lease, ProviderSurface::Exact(TREASURY_FISCAL_SURFACE))?;
            Ok((
                ProviderRequest::TreasuryFiscal {
                    first_record_date,
                    last_record_date,
                    page_size,
                },
                LoadedActivationEvidence {
                    objects: BTreeMap::new(),
                },
            ))
        }
        ProviderPortalActivationRequest::TreasuryDailyRates => {
            require_surface(lease, ProviderSurface::Exact(TREASURY_XML_SURFACE))?;
            Ok((
                ProviderRequest::TreasuryDailyRates,
                LoadedActivationEvidence {
                    objects: BTreeMap::new(),
                },
            ))
        }
        ProviderPortalActivationRequest::Bls {
            series,
            start_year,
            end_year,
        } => {
            require_surface(
                lease,
                ProviderSurface::Either(BLS_PUBLIC_SURFACE, BLS_REGISTERED_SURFACE),
            )?;
            if series.is_empty() || series.len() > MAXIMUM_BLS_SERIES {
                return Err(CliProviderActivationError::ProviderConfiguration);
            }
            let authorization =
                SourceIdentifier::try_from(format!("portal-session-{}", lease.session_id()))
                    .map_err(|_error| CliProviderActivationError::ProviderConfiguration)?;
            let mut objects = BTreeMap::new();
            let mut references = Vec::new();
            for (index, input) in series.into_iter().enumerate() {
                let metadata = BlsSeriesMetadata::from_verified_input(input, authorization.clone())
                    .map_err(|_error| CliProviderActivationError::ProviderConfiguration)?;
                let digest = metadata.evidence().content_digest();
                let sha256 = lower_hex(&digest.bytes());
                let reference = ExactInputReference {
                    path: PathBuf::from(format!("portal-series-{index}.json")),
                    sha256,
                };
                insert_evidence(
                    &mut objects,
                    &reference,
                    ExactActivationInput {
                        bytes: Arc::from(metadata.exact_payload()),
                        digest,
                    },
                )?;
                references.push(reference);
            }
            Ok((
                ProviderRequest::Bls {
                    series_metadata: references,
                    start_year,
                    end_year,
                },
                LoadedActivationEvidence { objects },
            ))
        }
        ProviderPortalActivationRequest::FredAlfred { provider_dataset } => {
            fred_portal_request(lease, provider_dataset)
        }
    }
}

fn sec_instrument_id(cik: &SecCikInput) -> Result<InstrumentId, CliProviderActivationError> {
    let namespace = Uuid::new_v5(&Uuid::NAMESPACE_URL, SEC_IDENTITY_NAMESPACE_V1.as_bytes());
    InstrumentId::try_from(Uuid::new_v5(&namespace, cik.as_str().as_bytes()))
        .map_err(|_| CliProviderActivationError::ProviderConfiguration)
}

fn sec_identity_registry(
    metadata: &SourceMetadata,
    mappings: Vec<SecIdentityMappingRequest>,
    activation_evidence: EvidenceDigest,
    validity: EffectiveInterval,
    observed_at: Timestamp,
) -> Result<ProviderIdentityRegistry, CliProviderActivationError> {
    if mappings.is_empty() || mappings.len() > MAXIMUM_SEC_IDENTITIES {
        return Err(CliProviderActivationError::ProviderConfiguration);
    }
    let digest = lower_hex(&activation_evidence.bytes());
    let short = digest
        .get(..24)
        .ok_or(CliProviderActivationError::InvalidMetadata)?;
    let revision = MetadataRevision::new(
        SourceIdentifier::try_from(format!("sec-cik-mapping-v1-{short}"))
            .map_err(|_| CliProviderActivationError::InvalidMetadata)?,
    );
    let mut ciks = BTreeSet::new();
    let mut instruments = BTreeSet::new();
    let mut records = Vec::new();
    records
        .try_reserve_exact(mappings.len())
        .map_err(|_| CliProviderActivationError::ProviderConfiguration)?;
    for mapping in mappings {
        let expected = sec_instrument_id(&mapping.cik)?;
        if expected != mapping.instrument_id
            || !ciks.insert(mapping.cik.clone())
            || !instruments.insert(mapping.instrument_id)
        {
            return Err(CliProviderActivationError::ProviderConfiguration);
        }
        let provider_instrument_id =
            ProviderInstrumentId::try_from(mapping.cik.as_str().to_owned())
                .map_err(|_| CliProviderActivationError::ProviderConfiguration)?;
        let locator = ProviderIdentityLocator::new(
            SourceIdentifier::try_from(format!(
                "market-squawk:onboarding:sec-cik:{}",
                mapping.cik.as_str()
            ))
            .map_err(|_| CliProviderActivationError::InvalidMetadata)?,
            revision.as_source_identifier().clone(),
        );
        records.push(ProviderIdentityRecord::new(ProviderIdentityRecordInput {
            instrument_id: mapping.instrument_id,
            source_id: metadata.source_id().clone(),
            provider_instrument_id,
            evidence: ProviderIdentityEvidence::with_version_pinned_locator(
                activation_evidence,
                locator,
            ),
            source_timestamp: None,
            observed_at,
            metadata_revision: revision.clone(),
            validity,
            supersedes: None,
        }));
    }
    ProviderIdentityRegistry::try_from_records(records)
        .map_err(|_| CliProviderActivationError::ProviderConfiguration)
}

fn fred_portal_request(
    lease: &ProviderActivationLease,
    provider_dataset: SourceIdentifier,
) -> Result<(ProviderRequest, LoadedActivationEvidence), CliProviderActivationError> {
    require_surface(lease, ProviderSurface::Exact(FRED_SURFACE))?;
    require_current_fred_revision(lease)?;
    FredSource::series_identifier(&provider_dataset)
        .map_err(|_| CliProviderActivationError::ProviderConfiguration)?;
    Ok((
        ProviderRequest::FredAlfred {
            configuration: Box::new(FredProviderRequest { provider_dataset }),
        },
        LoadedActivationEvidence {
            objects: BTreeMap::new(),
        },
    ))
}

fn default_unrate_dashboard_dataset() -> Result<SourceIdentifier, CliProviderActivationError> {
    let provider_dataset = SourceIdentifier::try_from(FRED_UNRATE_DASHBOARD_PROVIDER_DATASET)
        .map_err(|_| CliProviderActivationError::ProviderConfiguration)?;
    FredSource::series_identifier(&provider_dataset)
        .map_err(|_| CliProviderActivationError::ProviderConfiguration)?;
    Ok(provider_dataset)
}

fn validate_file_fred_request_scope(
    lease: &ProviderActivationLease,
    request: &ActivationRequest,
) -> Result<(), CliProviderActivationError> {
    let ProviderRequest::FredAlfred { configuration } = &request.provider else {
        return Ok(());
    };
    if request.schema_version != REQUEST_SCHEMA_VERSION {
        return Err(CliProviderActivationError::InvalidRights);
    }
    require_current_fred_revision(lease)?;
    FredSource::series_identifier(&configuration.provider_dataset)
        .map(|_series| ())
        .map_err(|_| CliProviderActivationError::ProviderConfiguration)
}

fn current_fred_revision() -> Result<ProviderCapabilityRevision, CliProviderActivationError> {
    let profiles =
        built_in_provider_profiles().map_err(|_| CliProviderActivationError::InvalidMetadata)?;
    profiles
        .get(FRED_SURFACE)
        .map(|profile| profile.capability().revision())
        .ok_or(CliProviderActivationError::InvalidMetadata)
}

fn require_current_fred_revision(
    lease: &ProviderActivationLease,
) -> Result<(), CliProviderActivationError> {
    if lease.capability_revision() != current_fred_revision()? {
        return Err(CliProviderActivationError::InvalidRights);
    }
    Ok(())
}

pub(super) fn treasury_daily_rate_all_history_datasets(
    state: &DurableProviderActivationState,
) -> Result<(Vec<SourceIdentifier>, EvidenceDigest), CliProviderActivationError> {
    let recipe = state
        .load_recipe(TREASURY_XML_SURFACE)
        .map_err(|_| CliProviderActivationError::StateUnavailable)?;
    let DurableActivationRecipeState::Desired(recipe) = recipe else {
        return Err(CliProviderActivationError::StateUnavailable);
    };
    let request = decode_request(&recipe.request_bytes)?;
    if request.session_id != recipe.session_id {
        return Err(CliProviderActivationError::StateUnavailable);
    }
    if !matches!(request.provider, ProviderRequest::TreasuryDailyRates) {
        return Err(CliProviderActivationError::ProviderConfiguration);
    }
    let config = TreasurySourceConfig::daily_rates_all_history()
        .map_err(|_| CliProviderActivationError::ProviderConfiguration)?;
    let catalog = config
        .dataset_catalog()
        .map_err(|_| CliProviderActivationError::ProviderConfiguration)?;
    if !catalog.complete_selected_family_coverage()
        || catalog.datasets().len() != TreasuryDailyRateFamily::ALL.len()
    {
        return Err(CliProviderActivationError::ProviderConfiguration);
    }
    let datasets = catalog
        .datasets()
        .iter()
        .map(|descriptor| descriptor.provider_dataset().clone())
        .collect::<Vec<_>>();
    if datasets
        .iter()
        .any(|dataset| !dataset.as_str().ends_with(":all"))
        || datasets
            .iter()
            .enumerate()
            .any(|(ordinal, dataset)| datasets[..ordinal].contains(dataset))
    {
        return Err(CliProviderActivationError::ProviderConfiguration);
    }
    Ok((datasets, recipe.runtime_generation_digest))
}

pub(super) fn treasury_fiscal_release_query(
    state: &DurableProviderActivationState,
) -> Result<(TreasuryFiscalQuery, EvidenceDigest), CliProviderActivationError> {
    let recipe = state
        .load_recipe(TREASURY_FISCAL_SURFACE)
        .map_err(|_| CliProviderActivationError::StateUnavailable)?;
    let DurableActivationRecipeState::Desired(recipe) = recipe else {
        return Err(CliProviderActivationError::StateUnavailable);
    };
    let request = decode_request(&recipe.request_bytes)?;
    if request.session_id != recipe.session_id {
        return Err(CliProviderActivationError::StateUnavailable);
    }
    let ProviderRequest::TreasuryFiscal {
        first_record_date,
        last_record_date,
        page_size,
    } = request.provider
    else {
        return Err(CliProviderActivationError::ProviderConfiguration);
    };
    let page_size =
        NonZeroU16::new(page_size).ok_or(CliProviderActivationError::ProviderConfiguration)?;
    let query = TreasuryFiscalQuery::average_interest_rates_v2(
        first_record_date,
        last_record_date,
        page_size,
    )
    .map_err(|_| CliProviderActivationError::ProviderConfiguration)?;
    Ok((query, recipe.runtime_generation_digest))
}

/// Returns the exact application-owned FRED dashboard dataset from callable durable state.
///
/// The durable-state owner verifies the recipe envelope before this boundary. Desktop input never
/// selects or overrides the exact configured series and vintage interval at query time.
pub(super) fn fred_dashboard_provider_dataset(
    state: &DurableProviderActivationState,
) -> Result<SourceIdentifier, CliProviderActivationError> {
    let recipe = state
        .load_recipe(FRED_SURFACE)
        .map_err(|_| CliProviderActivationError::StateUnavailable)?;
    let DurableActivationRecipeState::Desired(recipe) = recipe else {
        return Err(CliProviderActivationError::StateUnavailable);
    };
    let request = decode_request(&recipe.request_bytes)?;
    if request.schema_version != REQUEST_SCHEMA_VERSION
        || request.session_id != recipe.session_id
        || !recipe.evidence_digests.is_empty()
    {
        return Err(CliProviderActivationError::StateUnavailable);
    }
    let ProviderRequest::FredAlfred { configuration } = request.provider else {
        return Err(CliProviderActivationError::ProviderConfiguration);
    };
    FredSource::series_identifier(&configuration.provider_dataset)
        .map_err(|_| CliProviderActivationError::ProviderConfiguration)?;
    Ok(configuration.provider_dataset)
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct FredProviderRequest {
    provider_dataset: SourceIdentifier,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ExactInputReference {
    path: PathBuf,
    sha256: String,
}

#[derive(Clone)]
struct ExactActivationInput {
    bytes: Arc<[u8]>,
    digest: EvidenceDigest,
}

impl ExactActivationInput {
    fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }

    const fn digest(&self) -> EvidenceDigest {
        self.digest
    }
}

struct LoadedActivationEvidence {
    objects: BTreeMap<String, ExactActivationInput>,
}

impl LoadedActivationEvidence {
    fn from_user(
        root: &UserAuthorizedInputRoot,
        request: &ActivationRequest,
    ) -> Result<Self, CliProviderActivationError> {
        let mut objects = BTreeMap::new();
        for bounded in evidence_references(request)? {
            let input = read_exact_input(root, bounded.reference, bounded.maximum_bytes)?;
            let exact = ExactActivationInput {
                bytes: Arc::from(input.as_bytes()),
                digest: input.digest(),
            };
            insert_evidence(&mut objects, bounded.reference, exact)?;
        }
        Ok(Self { objects })
    }

    fn from_durable(
        state: &DurableProviderActivationState,
        request: &ActivationRequest,
    ) -> Result<Self, CliProviderActivationError> {
        let mut objects = BTreeMap::new();
        for bounded in evidence_references(request)? {
            let input = state
                .load_evidence(&bounded.reference.sha256, bounded.maximum_bytes)
                .map_err(|_| CliProviderActivationError::StateUnavailable)?;
            let exact = ExactActivationInput {
                bytes: Arc::from(input.as_bytes()),
                digest: input.digest(),
            };
            insert_evidence(&mut objects, bounded.reference, exact)?;
        }
        Ok(Self { objects })
    }

    fn read(
        &self,
        reference: &ExactInputReference,
        maximum_bytes: u64,
    ) -> Result<ExactActivationInput, CliProviderActivationError> {
        let expected = validate_reference_digest(reference)?;
        let input = self
            .objects
            .get(&reference.sha256)
            .ok_or(CliProviderActivationError::InputUnavailable)?;
        let length = u64::try_from(input.as_bytes().len())
            .map_err(|_| CliProviderActivationError::InputUnavailable)?;
        if length > maximum_bytes
            || input.digest().algorithm() != DigestAlgorithm::Sha256
            || input.digest().bytes() != expected
        {
            return Err(CliProviderActivationError::InputUnavailable);
        }
        Ok(input.clone())
    }

    fn persist(
        &self,
        state: &DurableProviderActivationState,
    ) -> Result<(), CliProviderActivationError> {
        let candidates = self
            .objects
            .iter()
            .map(|(digest, input)| ActivationEvidenceCandidate {
                sha256: digest,
                bytes: input.as_bytes(),
            })
            .collect::<Vec<_>>();
        state
            .persist_evidence_bundle(&candidates)
            .map_err(|_| CliProviderActivationError::StateUnavailable)
    }

    fn digests(&self) -> Vec<String> {
        self.objects.keys().cloned().collect()
    }
}

#[derive(Clone, Copy)]
struct BoundedExactReference<'a> {
    reference: &'a ExactInputReference,
    maximum_bytes: u64,
}

enum ProviderSurface {
    Exact(&'static str),
    Either(&'static str, &'static str),
}

fn evidence_references(
    request: &ActivationRequest,
) -> Result<Vec<BoundedExactReference<'_>>, CliProviderActivationError> {
    let mut references = Vec::new();
    match &request.provider {
        ProviderRequest::Sec { identities } => {
            if identities.is_empty() || identities.len() > MAXIMUM_SEC_IDENTITIES {
                return Err(CliProviderActivationError::ProviderConfiguration);
            }
        }
        ProviderRequest::TreasuryFiscal { .. }
        | ProviderRequest::TreasuryDailyRates
        | ProviderRequest::BeaRegional
        | ProviderRequest::FederalReserveBoardH15
        | ProviderRequest::YahooEnrichment
        | ProviderRequest::TiingoStarterEodNav
        | ProviderRequest::ControlledLocalFiles { .. } => {}
        ProviderRequest::Bls {
            series_metadata, ..
        } => {
            if series_metadata.is_empty() || series_metadata.len() > MAXIMUM_BLS_SERIES {
                return Err(CliProviderActivationError::ProviderConfiguration);
            }
            references.extend(
                series_metadata
                    .iter()
                    .map(|reference| BoundedExactReference {
                        reference,
                        maximum_bytes: BLS_SERIES_METADATA_MAXIMUM_BYTES,
                    }),
            );
        }
        ProviderRequest::FredAlfred { configuration } => {
            FredSource::series_identifier(&configuration.provider_dataset)
                .map_err(|_| CliProviderActivationError::ProviderConfiguration)?;
        }
    }
    Ok(references)
}

fn insert_evidence(
    objects: &mut BTreeMap<String, ExactActivationInput>,
    reference: &ExactInputReference,
    input: ExactActivationInput,
) -> Result<(), CliProviderActivationError> {
    let expected = validate_reference_digest(reference)?;
    if input.digest().algorithm() != DigestAlgorithm::Sha256 || input.digest().bytes() != expected {
        return Err(CliProviderActivationError::InvalidRequest);
    }
    if let Some(existing) = objects.get(&reference.sha256) {
        if existing.as_bytes() != input.as_bytes() || existing.digest() != input.digest() {
            return Err(CliProviderActivationError::InvalidRequest);
        }
        return Ok(());
    }
    objects.insert(reference.sha256.clone(), input);
    Ok(())
}

fn validate_reference_digest(
    reference: &ExactInputReference,
) -> Result<[u8; 32], CliProviderActivationError> {
    decode_lower_hex_sha256(&reference.sha256)
}

fn read_request(
    path: &Path,
) -> Result<(UserAuthorizedInputRoot, BoundedInput, ActivationRequest), CliProviderActivationError>
{
    let absolute =
        std::path::absolute(path).map_err(|_| CliProviderActivationError::InputUnavailable)?;
    if absolute
        .components()
        .any(|component| matches!(component, std::path::Component::ParentDir))
    {
        return Err(CliProviderActivationError::InputUnavailable);
    }
    let parent = absolute
        .parent()
        .ok_or(CliProviderActivationError::InputUnavailable)?;
    let name = absolute
        .file_name()
        .ok_or(CliProviderActivationError::InputUnavailable)?;
    let root = UserAuthorizedInputRoot::open(parent)
        .map_err(|_| CliProviderActivationError::InputUnavailable)?;
    let input = read_input(&root, Path::new(name), REQUEST_MAXIMUM_BYTES)?;
    let request = decode_request(input.as_bytes())?;
    Ok((root, input, request))
}

fn decode_request(bytes: &[u8]) -> Result<ActivationRequest, CliProviderActivationError> {
    #[derive(Deserialize)]
    struct SchemaProbe {
        schema_version: u16,
    }

    let schema: SchemaProbe =
        serde_json::from_slice(bytes).map_err(|_| CliProviderActivationError::InvalidRequest)?;
    match schema.schema_version {
        REQUEST_SCHEMA_VERSION => {
            let request: ActivationRequest = serde_json::from_slice(bytes)
                .map_err(|_| CliProviderActivationError::InvalidRequest)?;
            if request.schema_version != REQUEST_SCHEMA_VERSION {
                return Err(CliProviderActivationError::InvalidRequest);
            }
            Ok(request)
        }
        PREVIOUS_REQUEST_SCHEMA_VERSION => {
            let mut request: ActivationRequest = serde_json::from_slice(bytes)
                .map_err(|_| CliProviderActivationError::InvalidRequest)?;
            if request.schema_version != PREVIOUS_REQUEST_SCHEMA_VERSION
                || matches!(
                    &request.provider,
                    ProviderRequest::FredAlfred { .. }
                        | ProviderRequest::BeaRegional
                        | ProviderRequest::YahooEnrichment
                        | ProviderRequest::TiingoStarterEodNav
                )
            {
                return Err(CliProviderActivationError::InvalidRequest);
            }
            request.schema_version = REQUEST_SCHEMA_VERSION;
            Ok(request)
        }
        EMBEDDED_PREDECESSOR_REQUEST_SCHEMA_VERSION => {
            let mut request: ActivationRequest = serde_json::from_slice(bytes)
                .map_err(|_| CliProviderActivationError::InvalidRequest)?;
            if request.schema_version != EMBEDDED_PREDECESSOR_REQUEST_SCHEMA_VERSION
                || matches!(
                    &request.provider,
                    ProviderRequest::FredAlfred { .. }
                        | ProviderRequest::BeaRegional
                        | ProviderRequest::FederalReserveBoardH15
                        | ProviderRequest::YahooEnrichment
                        | ProviderRequest::TiingoStarterEodNav
                        | ProviderRequest::ControlledLocalFiles { .. }
                )
            {
                return Err(CliProviderActivationError::InvalidRequest);
            }
            request.schema_version = REQUEST_SCHEMA_VERSION;
            Ok(request)
        }
        LEGACY_REQUEST_SCHEMA_VERSION => {
            let request: LegacyActivationRequest = serde_json::from_slice(bytes)
                .map_err(|_| CliProviderActivationError::InvalidRequest)?;
            if request.schema_version != LEGACY_REQUEST_SCHEMA_VERSION {
                return Err(CliProviderActivationError::InvalidRequest);
            }
            Ok(ActivationRequest {
                schema_version: REQUEST_SCHEMA_VERSION,
                session_id: request.session_id,
                provider: request.provider.into(),
            })
        }
        _ => Err(CliProviderActivationError::InvalidRequest),
    }
}

fn read_input(
    root: &UserAuthorizedInputRoot,
    reference: &Path,
    maximum_bytes: u64,
) -> Result<BoundedInput, CliProviderActivationError> {
    root.resolve(reference)
        .and_then(|input| input.open_bounded(maximum_bytes))
        .and_then(|input| input.read_bounded())
        .map_err(|_| CliProviderActivationError::InputUnavailable)
}

fn read_exact_input(
    root: &UserAuthorizedInputRoot,
    reference: &ExactInputReference,
    maximum_bytes: u64,
) -> Result<BoundedInput, CliProviderActivationError> {
    let input = read_input(root, &reference.path, maximum_bytes)?;
    let expected = validate_reference_digest(reference)?;
    if input.digest().algorithm() != DigestAlgorithm::Sha256 || input.digest().bytes() != expected {
        return Err(CliProviderActivationError::InvalidRequest);
    }
    Ok(input)
}

fn require_surface(
    lease: &ProviderActivationLease,
    expected: ProviderSurface,
) -> Result<(), CliProviderActivationError> {
    let actual = lease.surface_id().as_str();
    let matches = match expected {
        ProviderSurface::Exact(expected) => actual == expected,
        ProviderSurface::Either(first, second) => actual == first || actual == second,
    };
    if matches {
        Ok(())
    } else {
        Err(CliProviderActivationError::SurfaceMismatch)
    }
}

fn activation_evidence(input: &[u8], lease: &ProviderActivationLease) -> EvidenceDigest {
    let mut hasher = Sha256::new();
    hasher.update(b"market-squawk:cli-provider-activation:v1");
    hasher.update([0]);
    hasher.update(input);
    hasher.update([1]);
    update_digest(&mut hasher, lease.capability_digest());
    hasher.update([2]);
    update_digest(&mut hasher, lease.public_configuration_digest());
    hasher.update([3]);
    update_digest(&mut hasher, lease.rights_decision_digest());
    hasher.update([4]);
    hasher.update(lease.session_id().as_bytes());
    hasher.update([5]);
    hasher.update(
        lease
            .generation()
            .map_or(0_u64, |generation| generation.get())
            .to_be_bytes(),
    );
    hasher.update([6]);
    hasher.update(
        lease
            .verification_expires_at()
            .map_or(i64::MIN, Timestamp::unix_nanos)
            .to_be_bytes(),
    );
    EvidenceDigest::new(DigestAlgorithm::Sha256, hasher.finalize().into())
}

fn update_digest(hasher: &mut Sha256, digest: EvidenceDigest) {
    hasher.update(match digest.algorithm() {
        DigestAlgorithm::Sha256 => [1],
        DigestAlgorithm::Blake3 => [2],
    });
    hasher.update(digest.bytes());
}

fn source_id(
    source_tag: &str,
    surface: &SourceIdentifier,
) -> Result<SourceId, CliProviderActivationError> {
    SourceId::try_from(format!("{source_tag}-{}", surface.as_str()))
        .map_err(|_| CliProviderActivationError::InvalidRequest)
}

fn sha256_evidence(value: &str) -> Result<EvidenceDigest, CliProviderActivationError> {
    Ok(EvidenceDigest::new(
        DigestAlgorithm::Sha256,
        decode_lower_hex_sha256(value)?,
    ))
}

fn controlled_local_file_metadata(
    lease: &ProviderActivationLease,
    activation_evidence: EvidenceDigest,
    manifest_digest: EvidenceDigest,
    admitted_at: Timestamp,
) -> Result<SourceMetadata, CliProviderActivationError> {
    let source_id = source_id("controlled-files", lease.surface_id())?;
    let digest = lower_hex(&manifest_digest.bytes());
    let short = digest
        .get(..24)
        .ok_or(CliProviderActivationError::InvalidMetadata)?;
    let revision = MetadataRevision::new(
        SourceIdentifier::try_from(format!("manifest-{short}"))
            .map_err(|_| CliProviderActivationError::InvalidMetadata)?,
    );
    let provider = SourceIdentifier::try_from("user-imported-local-files")
        .map_err(|_| CliProviderActivationError::InvalidMetadata)?;
    let basis = SourceIdentifier::try_from("controlled-user-import")
        .map_err(|_| CliProviderActivationError::InvalidMetadata)?;
    let manifest_evidence = ExactPayloadEvidence::from_content_digest(manifest_digest);
    let authorization_evidence = ExactPayloadEvidence::from_content_digest(activation_evidence);
    let effective = EffectiveInterval::new(admitted_at, None)
        .map_err(|_| CliProviderActivationError::InvalidMetadata)?;
    SourceMetadata::try_new(SourceMetadataInput::new(
        SchemaVersion::CURRENT,
        source_id,
        RevisionBoundPayloadEvidence::new(revision, manifest_evidence.clone()),
        SourceClass::LocalFile,
        provider,
        AuthorizationGrant::new(
            AuthorizationMode::UserOwnedLocal,
            AuthorizationBasis::new(basis),
            authorization_evidence,
            effective,
        ),
        SourceCoverage::try_non_instrument(
            manifest_evidence,
            effective,
            CoverageDomain::AlternativeData,
            CoverageDelay::Delayed(1),
            DeliveryEvidence::Unknown,
        )
        .map_err(|_| CliProviderActivationError::InvalidMetadata)?,
        DataQuality::DirectUnverified,
        NetworkAccessPolicy::Denied,
        FreshnessPolicy::try_new(1, 1, 1, 1, 0)
            .map_err(|_| CliProviderActivationError::InvalidMetadata)?,
        None,
        SourceCapabilities::new(
            false,
            true,
            SequenceCapability::Unsupported,
            ChecksumCapability::Unsupported,
            HistoricalCapability::RevisionPreserving,
            false,
        ),
        SourceProtocolProfile::NotLive,
    ))
    .map_err(|_| CliProviderActivationError::InvalidMetadata)
}

#[allow(
    clippy::too_many_arguments,
    reason = "every parameter is an independent source-metadata authority dimension"
)]
fn metadata(
    lease: &ProviderActivationLease,
    evidence: EvidenceDigest,
    source_tag: &str,
    provider: &str,
    source_class: SourceClass,
    coverage_domain: CoverageDomain,
    authorization_mode: AuthorizationMode,
    historical: HistoricalCapability,
    effective: EffectiveInterval,
    network: EndpointPolicy,
    budget: ProviderBudgetPolicy,
) -> Result<SourceMetadata, CliProviderActivationError> {
    let source_id = source_id(source_tag, lease.surface_id())?;
    let revision_evidence = activation_revision_evidence(evidence)?;
    metadata_with_source_id(
        lease,
        evidence,
        revision_evidence,
        source_id,
        provider,
        source_class,
        coverage_domain,
        authorization_mode,
        historical,
        effective,
        network,
        budget,
    )
}

fn activation_revision_evidence(
    evidence: EvidenceDigest,
) -> Result<RevisionBoundPayloadEvidence, CliProviderActivationError> {
    let digest = lower_hex(&evidence.bytes());
    let short = digest
        .get(..24)
        .ok_or(CliProviderActivationError::InvalidMetadata)?;
    let revision = MetadataRevision::new(
        SourceIdentifier::try_from(format!("activation-{short}"))
            .map_err(|_| CliProviderActivationError::InvalidMetadata)?,
    );
    Ok(RevisionBoundPayloadEvidence::new(
        revision,
        ExactPayloadEvidence::from_content_digest(evidence),
    ))
}

#[allow(
    clippy::too_many_arguments,
    reason = "every parameter is an independent source-metadata authority dimension"
)]
fn metadata_with_source_id(
    lease: &ProviderActivationLease,
    evidence: EvidenceDigest,
    revision_evidence: RevisionBoundPayloadEvidence,
    source_id: SourceId,
    provider: &str,
    source_class: SourceClass,
    coverage_domain: CoverageDomain,
    authorization_mode: AuthorizationMode,
    historical: HistoricalCapability,
    effective: EffectiveInterval,
    network: EndpointPolicy,
    budget: ProviderBudgetPolicy,
) -> Result<SourceMetadata, CliProviderActivationError> {
    let provider = SourceIdentifier::try_from(provider)
        .map_err(|_| CliProviderActivationError::InvalidMetadata)?;
    let basis = match authorization_mode {
        AuthorizationMode::PublicInterface => {
            SourceIdentifier::try_from("official-public-interface")
                .map_err(|_| CliProviderActivationError::InvalidMetadata)?
        }
        AuthorizationMode::UserAuthorized => authorization_subject(lease)?,
        AuthorizationMode::Licensed | AuthorizationMode::UserOwnedLocal => {
            return Err(CliProviderActivationError::InvalidMetadata);
        }
    };
    let exact = ExactPayloadEvidence::from_content_digest(evidence);
    SourceMetadata::try_new(SourceMetadataInput::new(
        SchemaVersion::CURRENT,
        source_id,
        revision_evidence,
        source_class,
        provider,
        AuthorizationGrant::new(
            authorization_mode,
            AuthorizationBasis::new(basis),
            exact.clone(),
            effective,
        ),
        SourceCoverage::try_non_instrument(
            exact,
            effective,
            coverage_domain,
            CoverageDelay::Delayed(1),
            DeliveryEvidence::Unknown,
        )
        .map_err(|_| CliProviderActivationError::InvalidMetadata)?,
        DataQuality::OfficialDelayed,
        NetworkAccessPolicy::Allowlisted(network),
        FreshnessPolicy::try_new(
            MINUTE_NANOS,
            MINUTE_NANOS,
            DAY_NANOS,
            DAY_NANOS,
            SECOND_NANOS,
        )
        .map_err(|_| CliProviderActivationError::InvalidMetadata)?,
        Some(budget),
        SourceCapabilities::new(
            false,
            true,
            SequenceCapability::Unsupported,
            ChecksumCapability::Unsupported,
            historical,
            false,
        ),
        SourceProtocolProfile::NotLive,
    ))
    .map_err(|_| CliProviderActivationError::InvalidMetadata)
}

fn yahoo_metadata(
    lease: &ProviderActivationLease,
    evidence: EvidenceDigest,
    effective: EffectiveInterval,
) -> Result<SourceMetadata, CliProviderActivationError> {
    provider_market_metadata(
        lease,
        evidence,
        SourceId::try_from(YAHOO_SOURCE_ID)
            .map_err(|_| CliProviderActivationError::InvalidMetadata)?,
        "yahoo-finance-experimental",
        AuthorizationMode::PublicInterface,
        effective,
        yahoo_network_policy()?,
        None,
        HistoricalCapability::Historical,
        vec![AssetClass::Equity, AssetClass::Fund, AssetClass::Index],
    )
}

fn tiingo_metadata(
    lease: &ProviderActivationLease,
    evidence: EvidenceDigest,
    effective: EffectiveInterval,
) -> Result<SourceMetadata, CliProviderActivationError> {
    let declaration = tiingo_provider_rate_declaration()
        .map_err(|_| CliProviderActivationError::InvalidMetadata)?;
    provider_market_metadata(
        lease,
        evidence,
        SourceId::try_from(TIINGO_SOURCE_ID)
            .map_err(|_| CliProviderActivationError::InvalidMetadata)?,
        TIINGO_SOURCE_ID,
        AuthorizationMode::UserAuthorized,
        effective,
        tiingo_network_policy()?,
        Some(declaration.policy().clone()),
        HistoricalCapability::Historical,
        vec![AssetClass::Equity, AssetClass::Fund],
    )
}

#[allow(
    clippy::too_many_arguments,
    reason = "provider market coverage, authorization, network, and budget stay explicit"
)]
fn provider_market_metadata(
    lease: &ProviderActivationLease,
    evidence: EvidenceDigest,
    source_id: SourceId,
    provider: &str,
    authorization_mode: AuthorizationMode,
    effective: EffectiveInterval,
    network: EndpointPolicy,
    budget: Option<ProviderBudgetPolicy>,
    historical: HistoricalCapability,
    asset_classes: Vec<AssetClass>,
) -> Result<SourceMetadata, CliProviderActivationError> {
    let digest = lower_hex(&evidence.bytes());
    let short = digest
        .get(..24)
        .ok_or(CliProviderActivationError::InvalidMetadata)?;
    let revision = MetadataRevision::new(
        SourceIdentifier::try_from(format!("activation-{short}"))
            .map_err(|_| CliProviderActivationError::InvalidMetadata)?,
    );
    let provider = SourceIdentifier::try_from(provider)
        .map_err(|_| CliProviderActivationError::InvalidMetadata)?;
    let basis = match authorization_mode {
        AuthorizationMode::PublicInterface => {
            SourceIdentifier::try_from("official-public-interface")
                .map_err(|_| CliProviderActivationError::InvalidMetadata)?
        }
        AuthorizationMode::UserAuthorized => authorization_subject(lease)?,
        AuthorizationMode::Licensed | AuthorizationMode::UserOwnedLocal => {
            return Err(CliProviderActivationError::InvalidMetadata);
        }
    };
    let exact = ExactPayloadEvidence::from_content_digest(evidence);
    SourceMetadata::try_new(SourceMetadataInput::new(
        SchemaVersion::CURRENT,
        source_id,
        RevisionBoundPayloadEvidence::new(revision, exact.clone()),
        SourceClass::LicensedDataset,
        provider,
        AuthorizationGrant::new(
            authorization_mode,
            AuthorizationBasis::new(basis),
            exact.clone(),
            effective,
        ),
        SourceCoverage::try_instrument(
            exact,
            effective,
            asset_classes,
            CoverageTopology::not_applicable(),
            InstrumentCoverage::partial(),
            None,
            CoverageDelay::Delayed(1),
            DeliveryEvidence::Indirect,
        )
        .map_err(|_| CliProviderActivationError::InvalidMetadata)?,
        DataQuality::Aggregated,
        NetworkAccessPolicy::Allowlisted(network),
        FreshnessPolicy::try_new(
            MINUTE_NANOS,
            MINUTE_NANOS,
            DAY_NANOS,
            DAY_NANOS,
            SECOND_NANOS,
        )
        .map_err(|_| CliProviderActivationError::InvalidMetadata)?,
        budget,
        SourceCapabilities::new(
            false,
            true,
            SequenceCapability::Unsupported,
            ChecksumCapability::Unsupported,
            historical,
            false,
        ),
        SourceProtocolProfile::NotLive,
    ))
    .map_err(|_| CliProviderActivationError::InvalidMetadata)
}

fn yahoo_network_policy() -> Result<EndpointPolicy, CliProviderActivationError> {
    let query_keys = [
        "corsDomain",
        "count",
        "date",
        "enableCb",
        "enableCulturalAssets",
        "enableFuzzyQuery",
        "enableNavLinks",
        "enableResearchReports",
        "events",
        "fetchPricingData",
        "formatted",
        "includeAdjustedClose",
        "interval",
        "lang",
        "listsCount",
        "modules",
        "newsCount",
        "period1",
        "period2",
        "q",
        "query",
        "quotesCount",
        "range",
        "recommendedCount",
        "region",
        "start",
        "symbol",
        "symbols",
        "type",
    ];
    let query_rules = || {
        query_keys
            .iter()
            .copied()
            .map(|key| {
                QueryParameterRule::try_new(
                    SourceIdentifier::try_from(key)
                        .map_err(|_| CliProviderActivationError::InvalidMetadata)?,
                    4_096,
                    false,
                    QuerySensitivity::Public,
                )
                .map_err(|_| CliProviderActivationError::InvalidMetadata)
            })
            .collect::<Result<Vec<_>, _>>()
    };
    let rules = [
        "https://fc.yahoo.com",
        "https://query1.finance.yahoo.com",
        "https://query2.finance.yahoo.com",
        "https://guce.yahoo.com",
        "https://consent.yahoo.com",
        "https://finance.yahoo.com",
    ]
    .into_iter()
    .map(|endpoint| {
        ApiEndpointRule::try_new(endpoint, PathScope::Descendants, query_rules()?, 32, 16_384)
            .map_err(|_| CliProviderActivationError::InvalidMetadata)
    })
    .collect::<Result<Vec<_>, _>>()?;
    EndpointPolicy::try_from_api_rules(rules, request_bounds(4 * 1024 * 1024)?)
        .map_err(|_| CliProviderActivationError::InvalidMetadata)
}

fn tiingo_network_policy() -> Result<EndpointPolicy, CliProviderActivationError> {
    let rule = ApiEndpointRule::try_new(
        "https://api.tiingo.com/tiingo/daily",
        PathScope::Descendants,
        query_rules(&[("startDate", 10), ("endDate", 10)])?,
        2,
        64,
    )
    .map_err(|_| CliProviderActivationError::InvalidMetadata)?;
    EndpointPolicy::try_from_api_rules(vec![rule], request_bounds(2 * 1024 * 1024)?)
        .map_err(|_| CliProviderActivationError::InvalidMetadata)
}

fn authorization_subject(
    lease: &ProviderActivationLease,
) -> Result<SourceIdentifier, CliProviderActivationError> {
    let provider = match lease.surface_id().as_str() {
        BLS_REGISTERED_SURFACE => "us-bls",
        BEA_SURFACE => "bea",
        FRED_SURFACE => "fred",
        TIINGO_SURFACE => TIINGO_SOURCE_ID,
        _ => return Err(CliProviderActivationError::InvalidMetadata),
    };
    let provider = SourceIdentifier::try_from(provider)
        .map_err(|_| CliProviderActivationError::InvalidMetadata)?;
    ProviderRateDeclaration::governed_provider_subject(&provider)
        .map_err(|_| CliProviderActivationError::InvalidMetadata)
}

fn exact_endpoint_policy(
    endpoint: &str,
    response_bytes: u64,
) -> Result<EndpointPolicy, CliProviderActivationError> {
    let rule = ApiEndpointRule::try_new(endpoint, PathScope::Exact, Vec::new(), 1, 1)
        .map_err(|_| CliProviderActivationError::InvalidMetadata)?;
    EndpointPolicy::try_from_api_rules(vec![rule], request_bounds(response_bytes)?)
        .map_err(|_| CliProviderActivationError::InvalidMetadata)
}

fn request_bounds(response_bytes: u64) -> Result<HttpRequestBounds, CliProviderActivationError> {
    HttpRequestBounds::try_new(
        nonzero_u64(5 * SECOND_NANOS)?,
        nonzero_u64(30 * SECOND_NANOS)?,
        nonzero_u64(45 * SECOND_NANOS)?,
        0,
        nonzero_u64(response_bytes)?,
    )
    .map_err(|_| CliProviderActivationError::InvalidMetadata)
}

fn simple_budget(
    provider: &str,
    requests: u32,
    window_nanos: u64,
    concurrency: u16,
    authorization_account: Option<SourceIdentifier>,
) -> Result<ProviderBudgetPolicy, CliProviderActivationError> {
    let provider = SourceIdentifier::try_from(provider)
        .map_err(|_| CliProviderActivationError::InvalidMetadata)?;
    let scope = match authorization_account {
        Some(account) => BudgetScope::with_authorization_account(provider, account),
        None => BudgetScope::new(provider),
    };
    ProviderBudgetPolicy::try_new(
        scope,
        NonZeroU32::new(requests).ok_or(CliProviderActivationError::InvalidMetadata)?,
        nonzero_u64(window_nanos)?,
        NonZeroU16::new(concurrency).ok_or(CliProviderActivationError::InvalidMetadata)?,
        backoff()?,
    )
    .map_err(|_| CliProviderActivationError::InvalidMetadata)
}

fn backoff() -> Result<BackoffPolicy, CliProviderActivationError> {
    BackoffPolicy::try_new(nonzero_u64(SECOND_NANOS)?, nonzero_u64(MINUTE_NANOS)?, 0)
        .map_err(|_| CliProviderActivationError::InvalidMetadata)
}

fn nonzero_u64(value: u64) -> Result<NonZeroU64, CliProviderActivationError> {
    NonZeroU64::new(value).ok_or(CliProviderActivationError::InvalidMetadata)
}

fn fred_budget(
    lease: &ProviderActivationLease,
) -> Result<ProviderBudgetPolicy, CliProviderActivationError> {
    // One account-scoped lane governs every v1 and v2 endpoint on this source metadata.
    let windows = [ProviderBudgetWindow::try_new(
        NonZeroU32::new(1).ok_or(CliProviderActivationError::InvalidMetadata)?,
        nonzero_u64(SECOND_NANOS)?,
        BudgetWindowSemantics::Sliding,
    )
    .map_err(|_| CliProviderActivationError::InvalidMetadata)?];
    ProviderBudgetPolicy::try_new_conjunctive(
        BudgetScope::with_authorization_account(
            SourceIdentifier::try_from("fred")
                .map_err(|_| CliProviderActivationError::InvalidMetadata)?,
            authorization_subject(lease)?,
        ),
        &windows,
        NonZeroU16::new(1).ok_or(CliProviderActivationError::InvalidMetadata)?,
        backoff()?,
    )
    .map_err(|_| CliProviderActivationError::InvalidMetadata)
}

fn bls_tier(lease: &ProviderActivationLease) -> Result<BlsAccessTier, CliProviderActivationError> {
    match lease.surface_id().as_str() {
        BLS_PUBLIC_SURFACE => Ok(BlsAccessTier::PublicV1),
        BLS_REGISTERED_SURFACE => Ok(BlsAccessTier::RegisteredV2),
        _ => Err(CliProviderActivationError::SurfaceMismatch),
    }
}

fn bls_series(
    inputs: &LoadedActivationEvidence,
    references: &[ExactInputReference],
    evidence: EvidenceDigest,
) -> Result<Vec<BlsSeriesMetadata>, CliProviderActivationError> {
    if references.is_empty() || references.len() > MAXIMUM_BLS_SERIES {
        return Err(CliProviderActivationError::ProviderConfiguration);
    }
    let digest = lower_hex(&evidence.bytes());
    let short = digest
        .get(..24)
        .ok_or(CliProviderActivationError::ProviderConfiguration)?;
    let authorization = SourceIdentifier::try_from(format!("bls-series-review-{short}"))
        .map_err(|_| CliProviderActivationError::ProviderConfiguration)?;
    references
        .iter()
        .map(|reference| {
            let input = inputs.read(reference, BLS_SERIES_METADATA_MAXIMUM_BYTES)?;
            BlsSeriesMetadata::parse_exact(
                Bytes::copy_from_slice(input.as_bytes()),
                ExactPayloadEvidence::from_content_digest(input.digest()),
                authorization.clone(),
            )
            .map_err(|_| CliProviderActivationError::ProviderConfiguration)
        })
        .collect()
}

fn bls_budget(
    lease: &ProviderActivationLease,
    mode: AuthorizationMode,
    daily_queries: u16,
) -> Result<ProviderBudgetPolicy, CliProviderActivationError> {
    let provider = SourceIdentifier::try_from("us-bls")
        .map_err(|_| CliProviderActivationError::InvalidMetadata)?;
    let account = match mode {
        AuthorizationMode::PublicInterface => None,
        AuthorizationMode::UserAuthorized => Some(authorization_subject(lease)?),
        AuthorizationMode::Licensed | AuthorizationMode::UserOwnedLocal => {
            return Err(CliProviderActivationError::InvalidMetadata);
        }
    };
    let scope = match account {
        Some(account) => BudgetScope::with_authorization_account(provider, account),
        None => BudgetScope::new(provider),
    };
    let windows = [
        ProviderBudgetWindow::try_new(
            NonZeroU32::new(50).ok_or(CliProviderActivationError::InvalidMetadata)?,
            nonzero_u64(10 * SECOND_NANOS)?,
            BudgetWindowSemantics::Sliding,
        )
        .map_err(|_| CliProviderActivationError::InvalidMetadata)?,
        ProviderBudgetWindow::try_new(
            NonZeroU32::new(u32::from(daily_queries))
                .ok_or(CliProviderActivationError::InvalidMetadata)?,
            nonzero_u64(DAY_NANOS)?,
            BudgetWindowSemantics::Sliding,
        )
        .map_err(|_| CliProviderActivationError::InvalidMetadata)?,
    ];
    ProviderBudgetPolicy::try_new_conjunctive(
        scope,
        &windows,
        NonZeroU16::new(2).ok_or(CliProviderActivationError::InvalidMetadata)?,
        backoff()?,
    )
    .map_err(|_| CliProviderActivationError::InvalidMetadata)
}

fn sec_state(
    paths: &LocalPaths,
    evidence: EvidenceDigest,
) -> Result<(RawEvidenceStore, SecRepresentationRegistry), CliProviderActivationError> {
    let control = paths
        .control_root()
        .map_err(|_| CliProviderActivationError::StateUnavailable)?
        .try_clone_directory()
        .map_err(|_| CliProviderActivationError::StateUnavailable)?;
    let digest = lower_hex(&evidence.bytes());
    let base = open_or_create(
        control,
        &["sources", "provider-adapters", "sec", digest.as_str()],
    )?;
    let raw = open_or_create(
        base.try_clone()
            .map_err(|_| CliProviderActivationError::StateUnavailable)?,
        &["raw"],
    )?;
    let representations = open_or_create(base, &["representations"])?;
    let registry = SecRepresentationRegistry::open(
        representations,
        SecRepresentationLimits::production_defaults(),
    )
    .map_err(|_| CliProviderActivationError::StateUnavailable)?;
    Ok((RawEvidenceStore::new(raw), registry))
}

fn open_or_create(
    mut directory: Dir,
    components: &[&str],
) -> Result<Dir, CliProviderActivationError> {
    for component in components {
        directory = match directory.open_dir_nofollow(*component) {
            Ok(child) => child,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                match directory.create_dir(*component) {
                    Ok(()) => {}
                    Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
                    Err(_) => return Err(CliProviderActivationError::StateUnavailable),
                }
                directory
                    .open_dir_nofollow(*component)
                    .map_err(|_| CliProviderActivationError::StateUnavailable)?
            }
            Err(_) => return Err(CliProviderActivationError::StateUnavailable),
        };
    }
    Ok(directory)
}

fn treasury_metadata(
    lease: &ProviderActivationLease,
    evidence: EvidenceDigest,
    effective: EffectiveInterval,
    config: &TreasurySourceConfig,
) -> Result<SourceMetadata, CliProviderActivationError> {
    let rule = match config {
        TreasurySourceConfig::AverageInterestRates(query) => {
            let page = query
                .page(1)
                .map_err(|_| CliProviderActivationError::ProviderConfiguration)?;
            ApiEndpointRule::try_new(
                without_query(page.url())?,
                PathScope::Exact,
                query_rules(&[
                    ("fields", 1_024),
                    ("filter", 512),
                    ("sort", 128),
                    ("page[number]", 20),
                    ("page[size]", 5),
                ])?,
                5,
                4_096,
            )
        }
        TreasurySourceConfig::DailyRates(config) => {
            let query = config
                .queries()
                .first()
                .ok_or(CliProviderActivationError::ProviderConfiguration)?;
            let page = query
                .page(0)
                .map_err(|_| CliProviderActivationError::ProviderConfiguration)?;
            ApiEndpointRule::try_new(
                without_query(page.url())?,
                PathScope::Exact,
                query_rules(&[
                    ("data", 64),
                    ("field_tdr_date_value", 4),
                    ("field_tdr_date_value_month", 6),
                    ("page", 20),
                ])?,
                3,
                512,
            )
        }
    }
    .map_err(|_| CliProviderActivationError::InvalidMetadata)?;
    let network = EndpointPolicy::try_from_api_rules(vec![rule], request_bounds(64 * 1024 * 1024)?)
        .map_err(|_| CliProviderActivationError::InvalidMetadata)?;
    metadata(
        lease,
        evidence,
        "treasury",
        "us-treasury",
        SourceClass::OfficialAgency,
        CoverageDomain::Macroeconomic,
        AuthorizationMode::PublicInterface,
        HistoricalCapability::Historical,
        effective,
        network,
        lease
            .provider_budget_policy()
            .cloned()
            .ok_or(CliProviderActivationError::InvalidMetadata)?,
    )
}

fn bea_metadata(
    lease: &ProviderActivationLease,
    evidence: EvidenceDigest,
    effective: EffectiveInterval,
    config: &market_squawk_adapter_bea::BeaSourceConfig,
) -> Result<SourceMetadata, CliProviderActivationError> {
    let rule =
        bea_api_endpoint_rule(config).map_err(|_| CliProviderActivationError::InvalidMetadata)?;
    let network = EndpointPolicy::try_from_api_rules(vec![rule], request_bounds(64 * 1024 * 1024)?)
        .map_err(|_| CliProviderActivationError::InvalidMetadata)?;
    let budget = bea_provider_rate_declaration()
        .map_err(|_| CliProviderActivationError::InvalidMetadata)?
        .policy()
        .clone();
    metadata_with_source_id(
        lease,
        evidence,
        activation_revision_evidence(evidence)?,
        SourceId::try_from(BEA_SOURCE_ID)
            .map_err(|_| CliProviderActivationError::InvalidMetadata)?,
        "bea",
        SourceClass::OfficialAgency,
        CoverageDomain::Macroeconomic,
        AuthorizationMode::UserAuthorized,
        HistoricalCapability::Historical,
        effective,
        network,
        budget,
    )
}

fn federal_reserve_board_metadata(
    lease: &ProviderActivationLease,
    evidence: EvidenceDigest,
    effective: EffectiveInterval,
    profile: &BoardDatasetProfile,
) -> Result<SourceMetadata, CliProviderActivationError> {
    let contract = profile.contract();
    let rolling_date_count =
        BOARD_H15_TREASURY_CONSTANT_MATURITIES_ROLLING_DASHBOARD_DATE_COUNT.to_string();
    let exact_query =
        |key: &str, value: &str| -> Result<QueryParameterRule, CliProviderActivationError> {
            QueryParameterRule::try_new_exact_public(
                SourceIdentifier::try_from(key)
                    .map_err(|_| CliProviderActivationError::InvalidMetadata)?,
                SourceIdentifier::try_from(value)
                    .map_err(|_| CliProviderActivationError::InvalidMetadata)?,
            )
            .map_err(|_| CliProviderActivationError::InvalidMetadata)
        };
    let rule = ApiEndpointRule::try_new(
        without_query(contract.url())?,
        PathScope::Exact,
        vec![
            exact_query("filetype", "csv")?,
            exact_query("label", "include")?,
            exact_query("lastobs", &rolling_date_count)?,
            exact_query("layout", "seriescolumn")?,
            exact_query("rel", "H15")?,
            exact_query("series", "bf17364827e38702b42a58cf8eaa3f78")?,
            exact_query("type", "package")?,
        ],
        7,
        256,
    )
    .map_err(|_| CliProviderActivationError::InvalidMetadata)?;
    // Match the closed one-megabyte parser ceiling owned by the rolling dashboard profile.
    let network = EndpointPolicy::try_from_api_rules(vec![rule], request_bounds(1024 * 1024)?)
        .map_err(|_| CliProviderActivationError::InvalidMetadata)?;
    let source_id = SourceId::try_from(BOARD_DDP_SOURCE_ID)
        .map_err(|_| CliProviderActivationError::InvalidMetadata)?;
    metadata_with_source_id(
        lease,
        evidence,
        activation_revision_evidence(evidence)?,
        source_id,
        "federal-reserve-board",
        SourceClass::OfficialAgency,
        CoverageDomain::Macroeconomic,
        AuthorizationMode::PublicInterface,
        HistoricalCapability::Historical,
        effective,
        network,
        simple_budget("federal-reserve-board", 1, MINUTE_NANOS, 1, None)?,
    )
}

fn query_rules(
    rules: &[(&str, u16)],
) -> Result<Vec<QueryParameterRule>, CliProviderActivationError> {
    rules
        .iter()
        .map(|(key, maximum)| {
            QueryParameterRule::try_new(
                SourceIdentifier::try_from(*key)
                    .map_err(|_| CliProviderActivationError::InvalidMetadata)?,
                *maximum,
                false,
                QuerySensitivity::Public,
            )
            .map_err(|_| CliProviderActivationError::InvalidMetadata)
        })
        .collect()
}

fn without_query(url: &str) -> Result<&str, CliProviderActivationError> {
    url.split('?')
        .next()
        .filter(|value| !value.is_empty())
        .ok_or(CliProviderActivationError::InvalidMetadata)
}

fn fred_network_policy() -> Result<EndpointPolicy, CliProviderActivationError> {
    EndpointPolicy::try_from_api_rules(
        vec![
            fred_observations_endpoint_rule(),
            fred_series_endpoint_rule(),
            fred_vintage_dates_endpoint_rule(),
            fred_release_observations_v2_endpoint_rule(),
        ]
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| CliProviderActivationError::InvalidMetadata)?,
        request_bounds(64 * 1024 * 1024)?,
    )
    .map_err(|_| CliProviderActivationError::InvalidMetadata)
}

fn lower_hex(bytes: &[u8; 32]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut value = String::with_capacity(64);
    for byte in bytes {
        value.push(char::from(HEX[usize::from(byte >> 4)]));
        value.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    value
}

fn decode_lower_hex_sha256(value: &str) -> Result<[u8; 32], CliProviderActivationError> {
    let encoded = value.as_bytes();
    if encoded.len() != 64 {
        return Err(CliProviderActivationError::InvalidRequest);
    }
    let mut decoded = [0_u8; 32];
    for (index, pair) in encoded.chunks_exact(2).enumerate() {
        let high = lower_hex_nibble(pair[0]).ok_or(CliProviderActivationError::InvalidRequest)?;
        let low = lower_hex_nibble(pair[1]).ok_or(CliProviderActivationError::InvalidRequest)?;
        decoded[index] = high * 16 + low;
    }
    Ok(decoded)
}

const fn lower_hex_nibble(value: u8) -> Option<u8> {
    match value {
        b'0'..=b'9' => Some(value - b'0'),
        b'a'..=b'f' => Some(value - b'a' + 10),
        _ => None,
    }
}

fn map_portal_activation_error(error: CliProviderActivationError) -> ProviderPortalActivationError {
    match error {
        CliProviderActivationError::Cancelled => ProviderPortalActivationError::Cancelled,
        CliProviderActivationError::StateUnavailable
        | CliProviderActivationError::InputUnavailable => {
            ProviderPortalActivationError::StateUnavailable
        }
        CliProviderActivationError::InvalidRequest
        | CliProviderActivationError::InvalidRights
        | CliProviderActivationError::InvalidMetadata
        | CliProviderActivationError::ProviderConfiguration
        | CliProviderActivationError::SurfaceMismatch
        | CliProviderActivationError::ConfirmationRequired => {
            ProviderPortalActivationError::InvalidRequest
        }
        CliProviderActivationError::Onboarding(_) | CliProviderActivationError::Activation(_) => {
            ProviderPortalActivationError::Unavailable
        }
    }
}

fn map_schwab_oauth_error(error: SchwabOAuthRuntimeError) -> ProviderPortalActivationError {
    match error {
        SchwabOAuthRuntimeError::InvalidSession
        | SchwabOAuthRuntimeError::DifferentSessionOwned => {
            ProviderPortalActivationError::InvalidRequest
        }
        SchwabOAuthRuntimeError::Cancelled
        | SchwabOAuthRuntimeError::Browser(SchwabOAuthBrowserError::Cancelled)
        | SchwabOAuthRuntimeError::Callback(OAuthLoopbackError::Cancelled)
        | SchwabOAuthRuntimeError::Onboarding(ProviderOnboardingError::OperationCancelled) => {
            ProviderPortalActivationError::Cancelled
        }
        SchwabOAuthRuntimeError::InvalidState
        | SchwabOAuthRuntimeError::RuntimeRootUnavailable
        | SchwabOAuthRuntimeError::RuntimeRootAlreadyOwned
        | SchwabOAuthRuntimeError::MarketEpochUnavailable
        | SchwabOAuthRuntimeError::MarketAuthorityRetained
        | SchwabOAuthRuntimeError::CallbackTask
        | SchwabOAuthRuntimeError::ExchangeTask
        | SchwabOAuthRuntimeError::ShutdownDeadline
        | SchwabOAuthRuntimeError::ShutdownStillDraining
        | SchwabOAuthRuntimeError::Clock
        | SchwabOAuthRuntimeError::Authority(_)
        | SchwabOAuthRuntimeError::Onboarding(_) => ProviderPortalActivationError::StateUnavailable,
        SchwabOAuthRuntimeError::InvalidConfiguration
        | SchwabOAuthRuntimeError::ShuttingDown
        | SchwabOAuthRuntimeError::OperationCapacity
        | SchwabOAuthRuntimeError::CallbackAlreadyOwned
        | SchwabOAuthRuntimeError::ReauthorizationRequired
        | SchwabOAuthRuntimeError::AuthorizationExchangeInFlight
        | SchwabOAuthRuntimeError::MarketAuthorityRevoked
        | SchwabOAuthRuntimeError::RandomUnavailable
        | SchwabOAuthRuntimeError::Browser(_)
        | SchwabOAuthRuntimeError::MarketDrain(_)
        | SchwabOAuthRuntimeError::Callback(_) => ProviderPortalActivationError::Unavailable,
    }
}

fn map_schwab_doctor_onboarding_error(
    error: ProviderOnboardingError,
) -> ProviderPortalActivationError {
    match error {
        ProviderOnboardingError::OperationCancelled => ProviderPortalActivationError::Cancelled,
        _ => ProviderPortalActivationError::Unavailable,
    }
}

/// Closed provider-activation failure without path, secret, or response-body disclosure.
#[derive(Debug, Error)]
pub enum CliProviderActivationError {
    #[error("provider activation requires explicit confirmation")]
    ConfirmationRequired,
    #[error("provider activation input is unavailable")]
    InputUnavailable,
    #[error("provider activation request is invalid")]
    InvalidRequest,
    #[error("provider activation request does not match the active profile")]
    SurfaceMismatch,
    #[error("provider activation rights evidence is invalid")]
    InvalidRights,
    #[error("provider activation metadata is invalid")]
    InvalidMetadata,
    #[error("provider activation configuration is invalid")]
    ProviderConfiguration,
    #[error("provider activation state is unavailable")]
    StateUnavailable,
    #[error("provider activation was cancelled")]
    Cancelled,
    #[error(transparent)]
    Onboarding(crate::ProviderOnboardingError),
    #[error(transparent)]
    Activation(crate::ProviderAdapterActivationError),
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::ffi::OsString;
    use std::time::{Duration, Instant};

    use market_squawk_platform::{AppConfig, ConfigOverrides, ConfigSources};
    use market_squawk_sources::{OnboardingState, ProviderPublicConfiguration};

    use super::*;
    use crate::local_product::provider_activation_state::DurableSourceLifecyclePhase;

    type TestResult = Result<(), Box<dyn std::error::Error>>;

    #[tokio::test]
    async fn schwab_doctor_scheduler_coalesces_replacement_and_unlink_drain() -> TestResult {
        let tasks = SchwabMarketDoctorTaskAuthority::new();
        let session_id = Uuid::new_v4();
        let first_key = SchwabMarketDoctorTaskKey {
            session_id,
            access_token_generation: 1,
        };
        let starts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let first_starts = Arc::clone(&starts);
        let (first_started_tx, first_started_rx) = oneshot::channel();
        tasks
            .schedule(first_key, move |cancellation| async move {
                first_starts.fetch_add(1, Ordering::SeqCst);
                let _sent = first_started_tx.send(());
                cancellation.cancelled().await;
            })
            .await?;
        tokio::time::timeout(Duration::from_secs(1), first_started_rx).await??;

        let duplicate_starts = Arc::clone(&starts);
        tasks
            .schedule(first_key, move |_cancellation| async move {
                duplicate_starts.fetch_add(1, Ordering::SeqCst);
            })
            .await?;
        assert_eq!(starts.load(Ordering::SeqCst), 1);

        let replacement_key = SchwabMarketDoctorTaskKey {
            session_id,
            access_token_generation: 2,
        };
        let replacement_starts = Arc::clone(&starts);
        let (replacement_started_tx, replacement_started_rx) = oneshot::channel();
        tasks
            .schedule(replacement_key, move |cancellation| async move {
                replacement_starts.fetch_add(1, Ordering::SeqCst);
                let _sent = replacement_started_tx.send(());
                cancellation.cancelled().await;
            })
            .await?;
        tokio::time::timeout(Duration::from_secs(1), replacement_started_rx).await??;
        assert_eq!(starts.load(Ordering::SeqCst), 2);

        tasks
            .cancel_session(session_id, CancellationToken::new())
            .await?;
        assert!(tasks.task.lock().await.is_none());
        Ok(())
    }

    #[test]
    fn fred_request_requires_current_v1_authority_and_retains_exact_dataset() -> TestResult {
        assert_eq!(current_fred_revision()?.get(), 1);
        let session_id = Uuid::new_v4();
        let mut request = json!({
            "schema_version": REQUEST_SCHEMA_VERSION,
            "session_id": session_id,
            "provider": {
                "kind": "fred_alfred",
                "configuration": {
                    "provider_dataset": "alfred:series-observations:CPIAUCSL:1947-01-01:9999-12-31"
                }
            }
        });
        let decoded = decode_request(&serde_json::to_vec(&request)?)?;
        let ProviderRequest::FredAlfred { configuration } = decoded.provider else {
            return Err("decoded request was not FRED/ALFRED".into());
        };
        assert_eq!(
            configuration.provider_dataset.as_str(),
            "alfred:series-observations:CPIAUCSL:1947-01-01:9999-12-31"
        );
        request["schema_version"] = json!(PREVIOUS_REQUEST_SCHEMA_VERSION);
        assert!(matches!(
            decode_request(&serde_json::to_vec(&request)?),
            Err(CliProviderActivationError::InvalidRequest)
        ));
        Ok(())
    }

    #[tokio::test]
    async fn post_staging_failure_restores_callable_predecessor_and_quarantines_candidate()
    -> TestResult {
        Box::pin(
            post_staging_failure_restores_callable_predecessor_and_quarantines_candidate_body(),
        )
        .await
    }

    async fn post_staging_failure_restores_callable_predecessor_and_quarantines_candidate_body()
    -> TestResult {
        let temporary = tempfile::tempdir()?;
        let config = AppConfig::load(ConfigSources::new(
            None,
            &BTreeMap::<OsString, OsString>::new(),
            ConfigOverrides {
                data_dir: Some(temporary.path().join("data")),
                ..ConfigOverrides::default()
            },
        ))?;
        let product = crate::LocalProduct::try_new(config.clone())?;
        let predecessor_lease = prepared_treasury_lease(&product, "predecessor").await?;
        let (predecessor_bytes, predecessor_evidence, predecessor_request) =
            treasury_activation(&product, &predecessor_lease)?;
        publish_research_activation(
            product.provider_activation_state(),
            product.provider_activation().as_ref(),
            product.provider_onboarding().as_ref(),
            &predecessor_lease,
            &predecessor_bytes,
            &predecessor_evidence,
            predecessor_request,
            CancellationToken::new(),
        )
        .await
        .map_err(|error| {
            std::io::Error::other(format!("predecessor publication failed: {error:?}"))
        })?;
        let state = product.provider_activation_state();
        let DurableActivationRecipeState::Desired(predecessor_recipe) =
            state.load_recipe(TREASURY_FISCAL_SURFACE)?
        else {
            return Err("predecessor activation was not restart-desired".into());
        };
        let predecessor = product
            .provider_activation()
            .research_runtime_generation(predecessor_lease.surface_id())?
            .ok_or("predecessor runtime was not published")?;
        let fiscal_dataset = treasury_fiscal_release_query(state)?.0.dataset()?;
        assert!(
            product
                .research_ingest()
                .is_profile_registered(predecessor.profile())?
        );
        let predecessor_source_identity = product
            .research_ingest()
            .treasury_all_history_source_identity_for_test(&predecessor, &fiscal_dataset)
            .await?;

        let same_session_candidate_digest = EvidenceDigest::new(
            DigestAlgorithm::Sha256,
            Sha256::digest(b"same-session-candidate-generation").into(),
        );
        let same_session_staged = state.publish_staged_replacement(
            TREASURY_FISCAL_SURFACE,
            &predecessor_recipe,
            predecessor_lease.session_id(),
            &predecessor_bytes,
            &predecessor_evidence.digests(),
            same_session_candidate_digest,
        )?;
        assert!(matches!(
            state.load_recipe_for_lifecycle(TREASURY_FISCAL_SURFACE)?,
            DurableActivationRecipeState::Staged(recipe)
                if recipe.session_id == predecessor_lease.session_id()
                    && recipe.runtime_generation_digest == same_session_candidate_digest
                    && recipe.predecessor_runtime_generation_digest
                        == Some(runtime_generation_digest(&predecessor)?)
                    && matches!(
                        recipe.staged_predecessor.as_deref(),
                        Some(retained)
                            if retained.session_id == predecessor_lease.session_id()
                                && retained.runtime_generation_digest
                                    == runtime_generation_digest(&predecessor)?
                    )
        ));
        assert_eq!(
            state.restore_staged_predecessor(TREASURY_FISCAL_SURFACE, same_session_staged,)?,
            predecessor_recipe.state_digest
        );

        let rollback_lease = prepared_treasury_lease(&product, "rollback-candidate").await?;
        let (rollback_bytes, rollback_evidence, rollback_request) =
            treasury_activation(&product, &rollback_lease)?;
        let rollback_candidate = product
            .provider_activation()
            .runtime_generation_for_request(&rollback_lease, &rollback_request)?;
        let rollback_desired_digest = state.recipe_digest(
            TREASURY_FISCAL_SURFACE,
            rollback_lease.session_id(),
            &rollback_bytes,
            &rollback_evidence.digests(),
            runtime_generation_digest(&rollback_candidate)?,
            Some(runtime_generation_digest(&predecessor)?),
        )?;
        let mut prepared = product
            .provider_activation()
            .prepare_research_replacement(
                rollback_lease.clone(),
                rollback_request,
                predecessor.clone(),
                CancellationToken::new(),
            )
            .await?;
        let staged = state
            .publish_staged_replacement(
                TREASURY_FISCAL_SURFACE,
                &predecessor_recipe,
                rollback_lease.session_id(),
                &rollback_bytes,
                &rollback_evidence.digests(),
                runtime_generation_digest(&rollback_candidate)?,
            )
            .map_err(|error| {
                std::io::Error::other(format!("replacement staging failed: {error:?}"))
            })?;
        rollback_evidence.persist(state)?;
        let predecessor_state_digest = predecessor_recipe.state_digest;
        product
            .provider_activation()
            .revoke_replacement_predecessor(&mut prepared)
            .await?;
        assert_eq!(
            product
                .provider_activation()
                .research_runtime_generation(predecessor.profile())?,
            None
        );
        assert!(
            !product
                .research_ingest()
                .is_profile_registered(predecessor.profile())?
        );
        assert!(
            product
                .research_ingest()
                .treasury_all_history_source_identity_for_test(&predecessor, &fiscal_dataset)
                .await
                .is_err()
        );

        reconcile_failed_replacement(
            state,
            product.provider_activation().as_ref(),
            product.provider_onboarding().as_ref(),
            ReplacementRuntimeTransaction::Prepared(prepared),
            Some(predecessor_state_digest),
            staged,
            rollback_desired_digest,
            DurableActivationQuarantineReason::StateInvalid,
        )
        .await
        .map_err(|error| {
            std::io::Error::other(format!("replacement reconciliation failed: {error:?}"))
        })?;

        assert!(matches!(
            state.load_recipe(TREASURY_FISCAL_SURFACE)?,
            DurableActivationRecipeState::Desired(recipe)
                if recipe.session_id == predecessor_lease.session_id()
                    && recipe.request_bytes == predecessor_bytes
                    && recipe.runtime_generation_digest
                        == runtime_generation_digest(&predecessor)?
                    && recipe.state_digest == predecessor_state_digest
        ));
        assert_eq!(
            product
                .provider_activation()
                .research_runtime_generation(predecessor.profile())?,
            Some(predecessor.clone())
        );
        assert!(
            product
                .research_ingest()
                .is_profile_registered(predecessor.profile())?
        );
        assert_eq!(
            product
                .research_ingest()
                .treasury_all_history_source_identity_for_test(&predecessor, &fiscal_dataset)
                .await?,
            predecessor_source_identity
        );
        require_same_activation_lease(
            &product
                .provider_onboarding()
                .activation_lease(predecessor_lease.session_id())?,
            &predecessor_lease,
        )?;
        assert_eq!(
            product
                .provider_onboarding()
                .resume(rollback_lease.session_id())?
                .state(),
            OnboardingState::Blocked
        );
        assert!(
            product
                .provider_onboarding()
                .activation_lease(rollback_lease.session_id())
                .is_err()
        );

        let DurableActivationRecipeState::Desired(predecessor_recipe) =
            state.load_recipe(TREASURY_FISCAL_SURFACE)?
        else {
            return Err("rolled-back predecessor activation was not restart-desired".into());
        };
        let recovery_lease =
            prepared_treasury_lease(&product, "post-invalidation-recovery").await?;
        let (recovery_bytes, recovery_evidence, recovery_request) =
            treasury_activation(&product, &recovery_lease)?;
        let recovery_candidate = product
            .provider_activation()
            .runtime_generation_for_request(&recovery_lease, &recovery_request)?;
        let recovery_desired_digest = state.recipe_digest(
            TREASURY_FISCAL_SURFACE,
            recovery_lease.session_id(),
            &recovery_bytes,
            &recovery_evidence.digests(),
            runtime_generation_digest(&recovery_candidate)?,
            Some(runtime_generation_digest(&predecessor)?),
        )?;
        let mut prepared = product
            .provider_activation()
            .prepare_research_replacement(
                recovery_lease.clone(),
                recovery_request,
                predecessor.clone(),
                CancellationToken::new(),
            )
            .await?;
        let recovery_staged = state.publish_staged_replacement(
            TREASURY_FISCAL_SURFACE,
            &predecessor_recipe,
            recovery_lease.session_id(),
            &recovery_bytes,
            &recovery_evidence.digests(),
            runtime_generation_digest(&recovery_candidate)?,
        )?;
        recovery_evidence.persist(state)?;
        product
            .provider_activation()
            .revoke_replacement_predecessor(&mut prepared)
            .await?;
        let mut committed = product
            .provider_activation()
            .commit_research_replacement(&mut prepared)
            .await?;
        let active = product
            .provider_activation()
            .commit_replacement_onboarding(&committed)
            .await?;
        require_same_activation_lease(&active, &recovery_lease)?;
        drop(prepared);
        assert_eq!(
            product
                .provider_activation()
                .research_runtime_generation(predecessor.profile())?,
            None
        );
        assert!(
            !product
                .research_ingest()
                .is_profile_registered(predecessor.profile())?
        );
        let recovery_cutover =
            state.commit_staged_cutover(TREASURY_FISCAL_SURFACE, recovery_staged)?;
        assert!(matches!(
            state.load_recipe_for_lifecycle(TREASURY_FISCAL_SURFACE)?,
            DurableActivationRecipeState::Cutover(recipe)
                if recipe.session_id == recovery_lease.session_id()
                    && recipe.state_digest == recovery_cutover
                    && recipe.staged_predecessor.is_some()
        ));
        product
            .provider_activation()
            .retire_replacement_predecessor(&committed, recovery_cutover)
            .await?;
        let activated = product
            .provider_activation()
            .finalize_research_replacement(&mut committed)
            .await?;
        assert_eq!(activated.generation(), &recovery_candidate);
        assert_ne!(
            product
                .research_ingest()
                .treasury_all_history_source_identity_for_test(
                    &recovery_candidate,
                    &fiscal_dataset,
                )
                .await?,
            predecessor_source_identity
        );
        assert!(
            product
                .provider_onboarding()
                .activation_recipe_is_invalidated(predecessor.session_id())?
        );
        require_same_activation_lease(
            &product
                .provider_onboarding()
                .activation_lease(recovery_lease.session_id())?,
            &recovery_lease,
        )?;
        assert!(matches!(
            state.load_recipe_for_lifecycle(TREASURY_FISCAL_SURFACE)?,
            DurableActivationRecipeState::Cutover(recipe)
                if recipe.session_id == recovery_lease.session_id()
                    && recipe.state_digest == recovery_cutover
                    && recipe.staged_predecessor.is_some()
        ));
        drop(committed);
        assert!(
            product
                .application()
                .shutdown(Instant::now() + Duration::from_secs(5))
                .await
                .is_complete()
        );
        drop(product);

        let recovered = crate::LocalProduct::try_new(config.clone())?;
        assert!(matches!(
            recovered
                .provider_activation_state()
                .load_recipe(TREASURY_FISCAL_SURFACE)?,
            DurableActivationRecipeState::Desired(recipe)
                if recipe.session_id == recovery_lease.session_id()
                    && recipe.runtime_generation_digest
                        == runtime_generation_digest(&recovery_candidate)?
                    && recipe.state_digest == recovery_desired_digest
        ));
        assert_eq!(
            recovered
                .provider_activation()
                .research_runtime_generation(recovery_candidate.profile())?,
            Some(recovery_candidate.clone())
        );
        assert!(
            recovered
                .research_ingest()
                .is_profile_registered(recovery_candidate.profile())?
        );
        let _restarted_source_identity = recovered
            .research_ingest()
            .treasury_all_history_source_identity_for_test(&recovery_candidate, &fiscal_dataset)
            .await?;
        assert_eq!(
            recovered
                .provider_onboarding()
                .resume(predecessor.session_id())?
                .state(),
            OnboardingState::Blocked
        );

        let lifecycle = recovered
            .provider_activation_state()
            .source_lifecycle_record(TREASURY_FISCAL_SURFACE)?;
        let stopped_transition = recovered
            .provider_activation_state()
            .begin_source_lifecycle_transition(
                TREASURY_FISCAL_SURFACE,
                lifecycle.revision(),
                SourceIdentifier::try_from("treasury-stopped-restart-proof")?,
                EvidenceDigest::new(
                    DigestAlgorithm::Sha256,
                    Sha256::digest(b"treasury-stopped-restart-proof").into(),
                ),
                false,
                Some(recovery_lease.session_id()),
                Some(recovery_lease.public_configuration_digest()),
            )?;
        recovered
            .provider_activation()
            .revoke_research_runtime(&recovery_candidate)
            .await?;
        assert!(
            recovered
                .research_ingest()
                .treasury_all_history_source_identity_for_test(
                    &recovery_candidate,
                    &fiscal_dataset,
                )
                .await
                .is_err()
        );
        recovered
            .provider_activation_state()
            .complete_source_lifecycle_transition(
                TREASURY_FISCAL_SURFACE,
                stopped_transition.transition_digest(),
                DurableSourceLifecyclePhase::Stopped,
                Some(recovery_lease.session_id()),
                Some(recovery_lease.public_configuration_digest()),
                None,
                None,
            )?;
        assert!(
            recovered
                .application()
                .shutdown(Instant::now() + Duration::from_secs(5))
                .await
                .is_complete()
        );
        drop(recovered);

        let recovered = crate::LocalProduct::try_new(config)?;
        assert!(matches!(
            recovered
                .provider_activation_state()
                .load_recipe_for_lifecycle(TREASURY_FISCAL_SURFACE)?,
            DurableActivationRecipeState::Desired(recipe)
                if recipe.session_id == recovery_lease.session_id()
        ));
        assert!(matches!(
            recovered
                .provider_activation_state()
                .load_recipe(TREASURY_FISCAL_SURFACE)?,
            DurableActivationRecipeState::Missing
        ));
        assert_eq!(
            recovered
                .provider_activation()
                .research_runtime_generation(recovery_candidate.profile())?,
            None
        );
        assert!(
            !recovered
                .research_ingest()
                .is_profile_registered(recovery_candidate.profile())?
        );
        assert!(
            recovered
                .research_ingest()
                .treasury_all_history_source_identity_for_test(
                    &recovery_candidate,
                    &fiscal_dataset,
                )
                .await
                .is_err()
        );
        require_same_activation_lease(
            &recovered
                .provider_onboarding()
                .activation_lease(recovery_lease.session_id())?,
            &recovery_lease,
        )?;

        let cancellation = ProviderResearchActivationService::new(
            recovered.paths().clone(),
            recovered.provider_onboarding(),
            recovered.provider_activation(),
            recovered.provider_activation_state().clone(),
            None,
            None,
        );
        let _cancelled = cancellation
            .cancel_from_portal(recovery_lease.session_id(), CancellationToken::new())
            .await?;
        assert!(
            recovered
                .provider_onboarding()
                .activation_lease(recovery_lease.session_id())
                .is_err()
        );
        assert_eq!(
            recovered
                .provider_activation()
                .research_runtime_generation(recovery_candidate.profile())?,
            None
        );
        assert!(
            !recovered
                .research_ingest()
                .is_profile_registered(recovery_candidate.profile())?
        );
        assert!(matches!(
            recovered
                .provider_activation_state()
                .load_recipe_for_lifecycle(TREASURY_FISCAL_SURFACE)?,
            DurableActivationRecipeState::Quarantined(quarantine)
                if quarantine.session_id == Some(recovery_lease.session_id())
                    && quarantine.reason == DurableActivationQuarantineReason::Cancelled
        ));
        assert!(
            recovered
                .application()
                .shutdown(Instant::now() + Duration::from_secs(5))
                .await
                .is_complete()
        );

        let tasks = ProviderActivationTaskAuthority::new();
        let held_activation = recovered
            .provider_activation_state()
            .acquire_activation(TREASURY_FISCAL_SURFACE)
            .await?;
        let retained_state = recovered.provider_activation_state().clone();
        let (started, retained_started) = tokio::sync::oneshot::channel();
        let (completed, retained_completed) = tokio::sync::oneshot::channel();
        let response_waiter = tasks
            .spawn(Box::pin(async move {
                let _started = started.send(());
                let acquired = retained_state
                    .acquire_activation(TREASURY_FISCAL_SURFACE)
                    .await
                    .is_ok();
                let _completed = completed.send(acquired);
            }))
            .await?;
        retained_started.await?;
        assert!(matches!(
            tasks.spawn(Box::pin(async {})).await,
            Err(CliProviderActivationError::StateUnavailable)
        ));
        drop(response_waiter);
        drop(held_activation);
        tasks.begin_shutdown();
        tasks
            .finish_shutdown(Instant::now() + Duration::from_secs(5))
            .await?;
        assert!(
            retained_completed.await?,
            "retained activation did not survive waiter drop and gate contention"
        );
        assert!(matches!(
            tasks.spawn(Box::pin(async {})).await,
            Err(CliProviderActivationError::StateUnavailable)
        ));
        Ok(())
    }

    #[tokio::test]
    async fn sec_identity_recipe_is_stable_evidence_bound_and_legacy_fail_closed() -> TestResult {
        let temporary = tempfile::tempdir()?;
        let config = AppConfig::load(ConfigSources::new(
            None,
            &BTreeMap::<OsString, OsString>::new(),
            ConfigOverrides {
                data_dir: Some(temporary.path().join("data")),
                ..ConfigOverrides::default()
            },
        ))?;
        let product = crate::LocalProduct::try_new(config)?;
        let lease = prepared_sec_lease(&product, "sec-identity-recipe").await?;
        let cik = SecCikInput::try_new("0000320193".to_owned())?;
        let expected_instrument = sec_instrument_id(&cik)?;
        let (provider, evidence) =
            portal_provider_request(&lease, ProviderPortalActivationRequest::Sec { cik })?;
        let request = ActivationRequest {
            schema_version: REQUEST_SCHEMA_VERSION,
            session_id: lease.session_id(),
            provider,
        };
        let request_bytes = serde_json::to_vec(&request)?;
        let recovered = decode_request(&request_bytes)?;
        assert!(matches!(
            &recovered.provider,
            ProviderRequest::Sec { identities }
                if identities.len() == 1
                    && identities[0].cik.as_str() == "0000320193"
                    && identities[0].instrument_id == expected_instrument
        ));
        let activation = build_research_activation(
            product.paths(),
            &lease,
            &request_bytes,
            recovered,
            &evidence,
        )?;
        assert!(matches!(
            activation,
            ProviderAdapterActivationRequest::Sec(_)
        ));

        let legacy = serde_json::to_vec(&json!({
            "schema_version": LEGACY_REQUEST_SCHEMA_VERSION,
            "session_id": lease.session_id(),
            "provider": {"kind": "sec"}
        }))?;
        let legacy = decode_request(&legacy)?;
        assert!(matches!(
            &legacy.provider,
            ProviderRequest::Sec { identities } if identities.is_empty()
        ));
        assert!(matches!(
            evidence_references(&legacy),
            Err(CliProviderActivationError::ProviderConfiguration)
        ));
        assert!(
            product
                .application()
                .shutdown(Instant::now() + Duration::from_secs(5))
                .await
                .is_complete()
        );
        Ok(())
    }

    #[tokio::test]
    async fn portal_source_activation_commits_only_nonresearch_provider_sessions() -> TestResult {
        let temporary = tempfile::tempdir()?;
        let config = AppConfig::load(ConfigSources::new(
            None,
            &BTreeMap::<OsString, OsString>::new(),
            ConfigOverrides {
                data_dir: Some(temporary.path().join("data")),
                ..ConfigOverrides::default()
            },
        ))?;
        let product = crate::LocalProduct::try_new(config)?;
        let source_lease = prepared_anonymous_lease(
            &product,
            "kraken.spot-public-market-data",
            "portal-source-activation",
        )
        .await?;
        let activation = ProviderResearchActivationService::new(
            product.paths().clone(),
            product.provider_onboarding(),
            product.provider_activation(),
            product.provider_activation_state().clone(),
            None,
            None,
        );

        activation
            .activate_from_portal(
                source_lease.session_id(),
                ProviderPortalActivationRequest::Source,
                CancellationToken::new(),
            )
            .await?;
        assert_eq!(
            product
                .provider_onboarding()
                .resume(source_lease.session_id())?
                .state(),
            OnboardingState::ActiveScoped
        );

        let research_lease =
            prepared_treasury_lease(&product, "portal-source-surface-mismatch").await?;
        assert!(matches!(
            activation
                .activate_from_portal(
                    research_lease.session_id(),
                    ProviderPortalActivationRequest::Source,
                    CancellationToken::new(),
                )
                .await,
            Err(CliProviderActivationError::SurfaceMismatch)
        ));
        assert!(
            product
                .application()
                .shutdown(Instant::now() + Duration::from_secs(5))
                .await
                .is_complete()
        );
        Ok(())
    }

    #[tokio::test]
    async fn public_research_activations_return_exact_datasets_and_restore() -> TestResult {
        let temporary = tempfile::tempdir()?;
        let config = AppConfig::load(ConfigSources::new(
            None,
            &BTreeMap::<OsString, OsString>::new(),
            ConfigOverrides {
                data_dir: Some(temporary.path().join("data")),
                ..ConfigOverrides::default()
            },
        ))?;
        let product = crate::LocalProduct::try_new(config.clone())?;
        let lease = prepared_lease(
            &product,
            BLS_PUBLIC_SURFACE,
            "public-bls-dataset",
            ProviderPublicConfiguration::try_new(BTreeMap::from([(
                "registration_mode".to_owned(),
                "unregistered_v1".to_owned(),
            )]))?,
        )
        .await?;
        let request = serde_json::from_value(json!({
            "kind": "bls",
            "series": [{
                "series_id": "LNS14000000",
                "title": "Unemployment Rate",
                "unit": "percent",
                "frequency": "monthly",
                "seasonal_adjustment": "seasonally-adjusted",
                "measure": "unemployment-rate"
            }],
            "start_year": 2025,
            "end_year": 2025
        }))?;
        let activation = ProviderResearchActivationService::new(
            product.paths().clone(),
            product.provider_onboarding(),
            product.provider_activation(),
            product.provider_activation_state().clone(),
            None,
            None,
        );

        let activated = activation
            .activate_from_portal(lease.session_id(), request, CancellationToken::new())
            .await?;
        let value = serde_json::to_value(activated)?;
        let dataset = value
            .get("provider_dataset_identifier")
            .and_then(Value::as_str)
            .ok_or("BLS activation did not return its provider dataset identifier")?;
        let dataset = SourceIdentifier::try_from(dataset)?;
        assert!(dataset.as_str().starts_with("bls:timeseries:public-v1:"));
        assert!(
            market_squawk_adapter_bls::BlsSource::analytical_dataset_identifier(&dataset).is_ok()
        );
        assert!(
            product
                .research_ingest()
                .is_profile_registered(lease.surface_id())?
        );
        let board_lease = prepared_anonymous_lease(
            &product,
            FEDERAL_RESERVE_BOARD_SURFACE,
            "federal-reserve-board-h15-dataset",
        )
        .await?;
        assert!(board_lease.generation().is_none());
        assert!(board_lease.secret_reference().is_none());
        let board_activated = activation
            .activate_from_portal(
                board_lease.session_id(),
                ProviderPortalActivationRequest::FederalReserveBoardH15,
                CancellationToken::new(),
            )
            .await?;
        let board_value = serde_json::to_value(board_activated)?;
        let board_dataset = board_value
            .get("provider_dataset_identifier")
            .and_then(Value::as_str)
            .ok_or("Board activation did not return its provider dataset identifier")?;
        let board_dataset = SourceIdentifier::try_from(board_dataset)?;
        let board_profile =
            BoardDatasetProfile::h15_treasury_constant_maturities_rolling_dashboard()?;
        assert_eq!(&board_dataset, board_profile.dataset());
        assert!(
            board_profile
                .contract()
                .is_h15_treasury_constant_maturities_rolling_dashboard()
        );
        assert_eq!(
            board_profile
                .analytical_dataset()
                .as_str()
                .replace('.', ":"),
            board_profile.dataset().as_str()
        );
        assert!(
            product
                .research_ingest()
                .is_profile_registered(board_lease.surface_id())?
        );
        drop(activation);
        assert!(
            product
                .application()
                .shutdown(Instant::now() + Duration::from_secs(5))
                .await
                .is_complete()
        );
        drop(product);

        let recovered = crate::LocalProduct::try_new(config)?;
        let recovered_activation = ProviderResearchActivationService::new(
            recovered.paths().clone(),
            recovered.provider_onboarding(),
            recovered.provider_activation(),
            recovered.provider_activation_state().clone(),
            None,
            None,
        );
        assert_eq!(
            recovered_activation.provider_dataset_identifier(lease.surface_id())?,
            Some(dataset.clone())
        );
        assert_eq!(
            recovered_activation.provider_dataset_identifier(board_lease.surface_id())?,
            Some(board_dataset.clone())
        );
        let status = crate::local_product::execute_cli_command(
            &recovered,
            crate::cli::Command::Source {
                command: crate::cli::SourceCommand::Status {
                    provider: Some(BLS_PUBLIC_SURFACE.to_owned()),
                },
            },
        )
        .await?;
        let rows = status
            .value()
            .get("data")
            .and_then(Value::as_array)
            .ok_or("source status did not return rows")?;
        assert_eq!(
            rows.first()
                .and_then(|row| row.get("providerDatasetIdentifier"))
                .and_then(Value::as_str),
            Some(dataset.as_str())
        );
        drop(recovered_activation);
        assert!(
            recovered
                .application()
                .shutdown(Instant::now() + Duration::from_secs(5))
                .await
                .is_complete()
        );
        Ok(())
    }

    async fn prepared_treasury_lease(
        product: &crate::LocalProduct,
        operation: &str,
    ) -> Result<ProviderActivationLease, Box<dyn std::error::Error>> {
        prepared_anonymous_lease(product, TREASURY_FISCAL_SURFACE, operation).await
    }

    async fn prepared_sec_lease(
        product: &crate::LocalProduct,
        operation: &str,
    ) -> Result<ProviderActivationLease, Box<dyn std::error::Error>> {
        let configuration = ProviderPublicConfiguration::try_new(BTreeMap::from([
            (
                "administrative_email".to_owned(),
                "operations@example.test".to_owned(),
            ),
            ("organization".to_owned(), "Market Squawk".to_owned()),
        ]))?;
        prepared_lease(product, SEC_EDGAR_PROFILE_ID, operation, configuration).await
    }

    async fn prepared_anonymous_lease(
        product: &crate::LocalProduct,
        surface_id: &str,
        operation: &str,
    ) -> Result<ProviderActivationLease, Box<dyn std::error::Error>> {
        prepared_lease(
            product,
            surface_id,
            operation,
            ProviderPublicConfiguration::default(),
        )
        .await
    }

    async fn prepared_lease(
        product: &crate::LocalProduct,
        surface_id: &str,
        operation: &str,
        public_configuration: ProviderPublicConfiguration,
    ) -> Result<ProviderActivationLease, Box<dyn std::error::Error>> {
        Ok(product
            .provider_onboarding()
            .prepare_noncredential_test_activation(surface_id, public_configuration, operation)
            .await?)
    }

    type TreasuryActivation = (
        Box<[u8]>,
        LoadedActivationEvidence,
        ProviderAdapterActivationRequest,
    );

    fn treasury_activation(
        product: &crate::LocalProduct,
        lease: &ProviderActivationLease,
    ) -> Result<TreasuryActivation, Box<dyn std::error::Error>> {
        let (provider, evidence) = portal_provider_request(
            lease,
            ProviderPortalActivationRequest::TreasuryFiscal {
                first_record_date: CalendarDate::new(2025, 1, 1)?,
                last_record_date: CalendarDate::new(2025, 1, 31)?,
                page_size: 100,
            },
        )?;
        let request = ActivationRequest {
            schema_version: REQUEST_SCHEMA_VERSION,
            session_id: lease.session_id(),
            provider,
        };
        let request_bytes = serde_json::to_vec(&request)?.into_boxed_slice();
        let activation =
            build_research_activation(product.paths(), lease, &request_bytes, request, &evidence)?;
        Ok((request_bytes, evidence, activation))
    }
}
