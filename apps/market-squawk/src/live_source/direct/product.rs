//! One-product Direct registry, capture, synchronization, and reconnect owner.

use std::num::{NonZeroU32, NonZeroUsize};
use std::sync::Arc;
use std::time::{Duration, Instant};

use market_squawk_adapter_coinbase::{
    CoinbaseConfigError, CoinbaseDirectConfig, CoinbaseDirectHmacSigner,
    CoinbaseDirectProductError, CoinbaseDirectProductPreflightFreshness,
    CoinbaseDirectProductReferenceEvidence, CoinbaseDirectSession, CoinbaseDirectSessionError,
};
use market_squawk_data::{
    AcceptedNativeReferenceCapture, MarketDataInstrumentCatalogError,
    MarketDataInstrumentReadCapability, MarketDataInstrumentSynchronizationCapability,
    MarketDataProviderIdentityQuery,
};
use market_squawk_domain::{
    AssetClass, DigestAlgorithm, EvidenceDigest, IdentityError, InstrumentError, SourceIdentifier,
};
use market_squawk_live::{
    BookError, DepthLimit, LiveIngressBindError, LiveRouteConfig, LiveRuntimeIngress,
    OrderLevelLimitError, OrderLevelLimits, OrderLevelRoute,
};
use market_squawk_platform::{
    AppConfig, CaptureChannelError, CaptureChannelLimits, CaptureGenerationError,
    CaptureProcessInfrastructure, CaptureShutdownStatus, CaptureWorkerReapError,
    CaptureWriterPolicy, CaptureWriterPolicyError, CaptureWriterSpawnError,
    LocalAuthorityStateStore, LocalAuthorityStateStoreError, LocalPaths,
    MemoryCaptureSinkConstructionError, RawCaptureControl, RawCaptureRecord, RawCaptureRecordError,
    ResearchObjectControl, ResearchObjectControlError, ResearchObjectControlPoint,
    RollingMemoryCaptureSink, SealedResearchJournalStoreError, SealedResearchRawClaim,
    raw_capture_channel, spawn_capture_writer,
};
use market_squawk_sources::{
    AuthoritativeSourceRegistry, AuthorizationSubjectResolver, BudgetUnavailableReason,
    CaptureGenerationCapabilities, ExtractionAuthority, ExtractionAuthorityError, ProviderBackoffAuthority,
    ProviderBackoffDecision, ProviderBackoffError, ProviderCaptureError, ProviderCaptureMaterial,
    ProviderCapturePageReceipt, ProviderCaptureSetReceipt, ProviderCaptureTerminalDisposition,
    ProviderNativeIdentityRequest, ProviderRateAuthority, RegisteredSource, RegistryError,
    SessionId, SourceError, SourceMetadata, TlsProviderError, install_ring_tls_provider,
};
use sha2::{Digest as _, Sha256};
use thiserror::Error;
use tokio::sync::{Semaphore, mpsc, watch};
use tokio_util::sync::CancellationToken;

use crate::{
    ResearchService, ResearchServiceError, provider_activation::CoinbaseDirectRuntimeAdmission,
};

use super::super::composition::ProductionCoinbaseProfileError;
use super::super::composition::system_timestamp;
use super::super::order_level::{
    MAX_ORDER_LEVEL_INGRESS_COMMANDS, OrderLevelActorLimits, OrderLevelActorShutdown,
    OrderLevelBookKey, OrderLevelDirectory, OrderLevelMonitorError, OrderLevelRegistration,
};
use super::super::route_actor::{RouteActorWorker, RouteBufferLimits, spawn_route_activation};
use super::super::sink::{
    CoinbaseCapturedPublicationIngress, ProductionPredecodedMarketSinkInput,
    ProductionRawMarketSink, ProductionSinkConstructionError, ProductionSinkFailure,
};
use super::super::subscription_state::{
    GenerationIdentity, SubscriptionConstructionError, SubscriptionLimits, SubscriptionStateMachine,
};
use super::coordinator::{
    DirectAccountCoordinator, DirectAccountCoordinatorError, DirectAccountEpoch,
};
use super::output::{CoinbaseDirectOutputFailure, CoinbaseDirectProductOutput};
use super::reference::DirectProductCatalogAssertion;
use crate::live_source::crypto_reference::synchronize_accepted_catalog_references;

const CAPTURE_FLUSH_RECORDS: usize = 256;
const CONTROL_AUDIT_RECORDS: usize = 64;
const CONTROL_AUDIT_BYTES: usize = 64 * 1024;
const BACKOFF_JITTER_SAMPLE_BASIS_POINTS: u16 = 1_000;
const SOURCE_AUTHORITY_ROOT: &str = "coinbase-direct-account-authority";
const SOURCE_AUTHORITY_CHILD: &str = "sources";
const LOCAL_CONCURRENCY_RETRY: Duration = Duration::from_millis(25);
const ORDER_LEVEL_OUTSTANDING_READS: usize = 64;

/// One preflight-complete product notification retained by the account startup barrier.
#[derive(Clone, Copy, Debug)]
pub(super) struct ProductReady {
    pub(super) slot: usize,
    pub(super) epoch: u64,
}

/// Immutable one-product runtime configuration prepared before live-runtime startup.
#[derive(Clone, Debug)]
pub(super) struct ProductRuntimeSpec {
    slot: usize,
    config: CoinbaseDirectConfig,
    route: LiveRouteConfig,
}

impl ProductRuntimeSpec {
    pub(super) const fn new(
        slot: usize,
        config: CoinbaseDirectConfig,
        route: LiveRouteConfig,
    ) -> Self {
        Self {
            slot,
            config,
            route,
        }
    }

    pub(super) const fn slot(&self) -> usize {
        self.slot
    }

    pub(super) const fn route(&self) -> &LiveRouteConfig {
        &self.route
    }

    pub(super) const fn metadata(&self) -> &SourceMetadata {
        self.config.metadata()
    }
}

/// Runs one product until account cancellation or a terminal product defect.
#[allow(
    clippy::too_many_arguments,
    reason = "every product authority and bounded runtime capability remains explicit"
)]
pub(super) async fn run_product(
    spec: ProductRuntimeSpec,
    app_config: AppConfig,
    provider_rate: ProviderRateAuthority,
    catalog_reader: MarketDataInstrumentReadCapability,
    catalog_synchronizer: MarketDataInstrumentSynchronizationCapability,
    research_service: Arc<ResearchService>,
    account_subject: SourceIdentifier,
    admission: CoinbaseDirectRuntimeAdmission,
    capture_process: CaptureProcessInfrastructure,
    live_ingress: LiveRuntimeIngress,
    publication: CoinbaseCapturedPublicationIngress,
    order_level: Option<OrderLevelDirectory>,
    route_buffer_limits: RouteBufferLimits,
    signer: Arc<CoinbaseDirectHmacSigner>,
    ready: mpsc::Sender<ProductReady>,
    mut start: watch::Receiver<bool>,
    bootstrap_slots: Arc<Semaphore>,
    coordinator: DirectAccountCoordinator,
    cancellation: CancellationToken,
) -> Result<(), CoinbaseDirectProductRuntimeError> {
    let paths = LocalPaths::prepare(app_config.data_dir())?;
    let authority_store = LocalAuthorityStateStore::try_open(
        paths
            .control_root()?
            .root()
            .join(SOURCE_AUTHORITY_ROOT)
            .join(account_subject.as_str())
            .join(SOURCE_AUTHORITY_CHILD)
            .join(spec.config.metadata().source_id().as_str()),
    )?;
    let resolver: Arc<dyn AuthorizationSubjectResolver> = Arc::new(provider_rate.clone());
    let registry =
        AuthoritativeSourceRegistry::try_new_durable_with_authorization_subject_resolver_and_provider_rate(
            authority_store,
            resolver,
            provider_rate,
        )?;
    let mut registry =
        registry.with_provider_identity_authority(Arc::new(catalog_reader.clone()))?;
    let registered =
        registry.register_or_resume_exact(spec.config.metadata().clone(), system_timestamp()?)?;
    let reference_profile = spec.config.product_reference_profile();
    let registered_reference = registry
        .register_or_resume_exact(reference_profile.metadata().clone(), system_timestamp()?)?;
    let backoff = registry.provider_backoff_authority(&registered)?;
    let run = run_product_loop(
        &spec,
        &app_config,
        admission,
        capture_process,
        live_ingress,
        &publication,
        order_level.as_ref(),
        route_buffer_limits,
        signer.as_ref(),
        &ready,
        &mut start,
        &bootstrap_slots,
        &mut registry,
        &registered,
        &registered_reference,
        &backoff,
        &catalog_reader,
        &catalog_synchronizer,
        &research_service,
        &coordinator,
        &cancellation,
    )
    .await;
    drop(backoff);
    drop(registered_reference);
    drop(registered);
    let shutdown = registry.shutdown();
    match (run, shutdown) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(source), Ok(())) => Err(source),
        (Ok(()), Err(shutdown)) => Err(shutdown.into()),
        (Err(source), Err(shutdown)) => Err(CoinbaseDirectProductRuntimeError::RunShutdown {
            source: Box::new(source),
            shutdown,
        }),
    }
}

#[allow(
    clippy::too_many_arguments,
    reason = "the sole product owner receives independent lifecycle authorities explicitly"
)]
async fn run_product_loop(
    spec: &ProductRuntimeSpec,
    app_config: &AppConfig,
    admission: CoinbaseDirectRuntimeAdmission,
    capture_process: CaptureProcessInfrastructure,
    live_ingress: LiveRuntimeIngress,
    publication: &CoinbaseCapturedPublicationIngress,
    order_level: Option<&OrderLevelDirectory>,
    route_buffer_limits: RouteBufferLimits,
    signer: &CoinbaseDirectHmacSigner,
    ready: &mpsc::Sender<ProductReady>,
    start: &mut watch::Receiver<bool>,
    bootstrap_slots: &Arc<Semaphore>,
    registry: &mut AuthoritativeSourceRegistry,
    registered: &RegisteredSource,
    registered_reference: &RegisteredSource,
    backoff: &ProviderBackoffAuthority,
    catalog_reader: &MarketDataInstrumentReadCapability,
    catalog_synchronizer: &MarketDataInstrumentSynchronizationCapability,
    research_service: &Arc<ResearchService>,
    coordinator: &DirectAccountCoordinator,
    cancellation: &CancellationToken,
) -> Result<(), CoinbaseDirectProductRuntimeError> {
    let mut ready_sent = false;
    let mut previous_epoch = None;
    loop {
        if cancellation.is_cancelled() {
            return Ok(());
        }
        let mut epoch = match coordinator
            .join_next_epoch(spec.slot(), previous_epoch)
            .await
        {
            Ok(epoch) => epoch,
            Err(DirectAccountCoordinatorError::Cancelled) if cancellation.is_cancelled() => {
                return Ok(());
            }
            Err(error) => return Err(error.into()),
        };
        previous_epoch = Some(epoch.id());
        let generation_cancellation = epoch.cancellation();
        let awaiting_start = !*start.borrow();
        let startup = awaiting_start.then_some((ready, &mut *start));
        let outcome = run_generation(
            spec,
            app_config,
            admission,
            capture_process,
            live_ingress.clone(),
            publication.clone(),
            order_level,
            route_buffer_limits,
            signer,
            registry,
            registered,
            registered_reference,
            catalog_reader,
            catalog_synchronizer,
            research_service,
            startup,
            bootstrap_slots,
            &mut epoch,
            generation_cancellation,
        )
        .await;
        epoch.request_restart();
        drop(epoch);
        if !ready_sent {
            ready_sent = outcome.ready_sent;
        }
        match outcome.result {
            Ok(()) if cancellation.is_cancelled() => return Ok(()),
            Ok(()) => return Err(CoinbaseDirectProductRuntimeError::SourceExited),
            Err(error) if error.coordinated_cancellation() && cancellation.is_cancelled() => {
                return Ok(());
            }
            Err(error) if error.coordinated_cancellation() => continue,
            Err(error) if !ready_sent || !error.recoverable() => return Err(error),
            Err(error) => {
                wait_after_failure(
                    error,
                    backoff,
                    spec.config.limits().product_refresh_interval(),
                    cancellation,
                )
                .await?;
            }
        }
    }
}

struct GenerationOutcome {
    ready_sent: bool,
    result: Result<(), CoinbaseDirectProductRuntimeError>,
}

struct PreparedDirectProductReference {
    selected: ProviderNativeIdentityRequest,
    evidence: CoinbaseDirectProductReferenceEvidence,
    freshness: CoinbaseDirectProductPreflightFreshness,
}

struct SynchronizedDirectProductReference {
    selected: ProviderNativeIdentityRequest,
    evidence: CoinbaseDirectProductReferenceEvidence,
    freshness: CoinbaseDirectProductPreflightFreshness,
    deadline: Instant,
    authority: ExtractionAuthority,
}

struct DirectNativeReferenceReadControl {
    deadline: Instant,
    cancellation: CancellationToken,
}

impl ResearchObjectControl for DirectNativeReferenceReadControl {
    fn checkpoint(
        &self,
        _point: ResearchObjectControlPoint,
    ) -> Result<(), ResearchObjectControlError> {
        if self.cancellation.is_cancelled() {
            Err(ResearchObjectControlError::Cancelled)
        } else if Instant::now() >= self.deadline {
            Err(ResearchObjectControlError::DeadlineExceeded)
        } else {
            Ok(())
        }
    }
}

async fn prepare_direct_product_reference(
    spec: &ProductRuntimeSpec,
    registry: &mut AuthoritativeSourceRegistry,
    registered_reference: &RegisteredSource,
    catalog_reader: &MarketDataInstrumentReadCapability,
    catalog_synchronizer: &MarketDataInstrumentSynchronizationCapability,
    research_service: &ResearchService,
    epoch: &DirectAccountEpoch,
    cancellation: &CancellationToken,
) -> Result<SynchronizedDirectProductReference, CoinbaseDirectProductRuntimeError> {
    let profile = spec.config.product_reference_profile();
    let authority = registry.extraction_authority(registered_reference, profile)?;
    let preflight = CoinbaseDirectSession::preflight_original_product(
        &spec.config,
        &authority,
        install_ring_tls_provider()?,
        cancellation,
    )
    .await?;
    let (body, received_at, completion, request_identity) = preflight.into_original();
    let body_digest = EvidenceDigest::new(DigestAlgorithm::Sha256, Sha256::digest(&body).into());
    let source_id = profile.metadata().source_id().clone();
    let page = ProviderCapturePageReceipt::try_new(
        0,
        request_identity,
        None,
        None,
        200,
        u64::try_from(body.len())
            .map_err(|_| CoinbaseDirectProductRuntimeError::ActivationBinding)?,
        body_digest,
        received_at,
    )?;
    let capture = ProviderCaptureSetReceipt::try_new(
        source_id.clone(),
        profile.metadata().revision().clone(),
        SourceIdentifier::try_from(spec.config.product_url())?,
        request_identity,
        ProviderCaptureTerminalDisposition::StandaloneResponse,
        vec![page],
    )?;
    let record = RawCaptureRecord::try_new_live(
        uuid::Uuid::new_v4(),
        Arc::<str>::from(source_id.as_str()),
        uuid::Uuid::new_v4(),
        Some(0),
        None,
        chrono::DateTime::<chrono::Utc>::from_timestamp_nanos(received_at.unix_nanos()),
        body.clone(),
    )?;
    let material = ProviderCaptureMaterial::try_new(capture, vec![record])?;
    let (expectation, seal_request) = material.into_whole_seal_parts();
    let deadline = Instant::now()
        .checked_add(spec.config.limits().websocket().io_timeout())
        .ok_or(CoinbaseDirectProductRuntimeError::ActivationBinding)?;
    let sealed = research_service
        .seal_provider_capture(seal_request, cancellation, deadline)
        .await?;
    let token = expectation.try_rejoin(sealed)?.try_into_whole()?;
    let evidence = spec
        .config
        .decode_product_reference_evidence(&body, token)?;
    let (evidence, freshness) = completion.finish_validated(evidence)?;
    let assertion =
        DirectProductCatalogAssertion::try_new(&spec.config, &evidence, spec.route.definition())
            .map_err(|error| CoinbaseDirectProductRuntimeError::DirectReference(Box::new(error)))?;
    authority.validate_current()?;
    let _catalog_publication = epoch.catalog_publication().await?;
    if let Some(selected) =
        replay_accepted_direct_reference(catalog_reader, &assertion, deadline, cancellation)?
    {
        authority.validate_current()?;
        return Ok(SynchronizedDirectProductReference {
            selected,
            evidence,
            freshness,
            deadline,
            authority,
        });
    }
    let raw = AcceptedNativeReferenceCapture::from_extraction_http(
        assertion.instrument(),
        assertion.provider_identity().source_id().clone(),
        assertion
            .provider_identity()
            .provider_instrument_id()
            .clone(),
        evidence.capture_token(),
    )?;
    let synchronized = synchronize_accepted_catalog_references(
        catalog_reader.clone(),
        catalog_synchronizer.clone(),
        vec![assertion.accepted_catalog_reference()],
        vec![raw],
        deadline,
        cancellation,
    )
    .await;
    if cancellation.is_cancelled() {
        return Err(DirectAccountCoordinatorError::Cancelled.into());
    }
    let mut selected = synchronized
        .map_err(|error| CoinbaseDirectProductRuntimeError::ReferenceSync(Box::new(error)))?;
    if selected.len() != 1 {
        return Err(CoinbaseDirectProductRuntimeError::ActivationBinding);
    }
    authority.validate_current()?;
    let mut selected = selected.remove(0);
    // The catalog lookup uses publication knowledge time, while the Direct session must bind
    // the effective instant of this freshly sealed product response.
    selected.effective_at = evidence.observed_at();
    Ok(SynchronizedDirectProductReference {
        selected,
        evidence,
        freshness,
        deadline,
        authority,
    })
}

/// Reuses only the exact already-published native assertion after a fresh physical seal and
/// validated response prove the same product bytes. Changed bytes need real supersession.
fn replay_accepted_direct_reference(
    catalog_reader: &MarketDataInstrumentReadCapability,
    assertion: &DirectProductCatalogAssertion,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<Option<ProviderNativeIdentityRequest>, CoinbaseDirectProductRuntimeError> {
    let Some(current) = catalog_reader.latest(assertion.instrument(), deadline, cancellation)?
    else {
        return Ok(None);
    };
    let definition = current.definition();
    if definition.asset_class() != AssetClass::Crypto
        || definition.quote_currency() != assertion.quote_currency()
        || !definition.venue_mappings().iter().any(|venue| {
            venue.venue_id() == assertion.venue()
                && venue.venue_symbol() == assertion.venue_symbol()
        })
    {
        return Err(CoinbaseDirectProductRuntimeError::ActivationBinding);
    }
    let mut identities = definition
        .provider_identities()
        .iter()
        .filter(|identity| identity.source_id() == assertion.provider_identity().source_id());
    let Some(identity) = identities.next() else {
        return Ok(None);
    };
    if identities.next().is_some()
        || identity.instrument_id() != assertion.instrument()
        || identity.provider_instrument_id()
            != assertion.provider_identity().provider_instrument_id()
        || identity.evidence().content_digest() != assertion.body_digest()
        || identity.metadata_revision() != assertion.provider_identity().metadata_revision()
    {
        return Err(CoinbaseDirectProductRuntimeError::ReferenceSync(Box::new(
            crate::live_source::crypto_reference::CryptoReferenceError::CanonicalIdentityUnapproved,
        )));
    }
    let mut request = assertion
        .request_at(system_timestamp()?)
        .map_err(|error| CoinbaseDirectProductRuntimeError::DirectReference(Box::new(error)))?;
    request.effective_at = assertion.provider_identity().observed_at();
    Ok(Some(request))
}

async fn select_direct_product_reference(
    synchronized: SynchronizedDirectProductReference,
    registry: &mut AuthoritativeSourceRegistry,
    registered: &RegisteredSource,
    catalog_reader: &MarketDataInstrumentReadCapability,
    research_service: &ResearchService,
    cancellation: &CancellationToken,
) -> Result<PreparedDirectProductReference, CoinbaseDirectProductRuntimeError> {
    let SynchronizedDirectProductReference {
        selected,
        evidence,
        freshness,
        deadline,
        authority,
    } = synchronized;
    let query = MarketDataProviderIdentityQuery::try_new(
        selected.namespace.clone(),
        selected.provider_instrument_id.clone(),
        selected.knowledge_at,
        selected.effective_at,
    )?;
    let catalog_selection = catalog_reader
        .select_provider_identity_as_of(query, deadline, cancellation)?
        .ok_or(CoinbaseDirectProductRuntimeError::ActivationBinding)?;
    if catalog_selection.exact_receipt()?.instrument_id() != selected.instrument {
        return Err(CoinbaseDirectProductRuntimeError::ActivationBinding);
    }
    let retained = catalog_reader
        .native_reference(&catalog_selection, deadline, cancellation)?
        .ok_or(CoinbaseDirectProductRuntimeError::ActivationBinding)?;
    let SealedResearchRawClaim::JournalSegment(claim) = retained.raw_claim() else {
        return Err(CoinbaseDirectProductRuntimeError::ActivationBinding);
    };
    let claim = claim.clone();
    let store = research_service.provider_capture_store();
    research_service
        .run_owned_research_io(deadline, cancellation, move |worker_cancellation| {
            let control = DirectNativeReferenceReadControl {
                deadline,
                cancellation: worker_cancellation,
            };
            store
                .open_verified_claim_with_control(&claim, &control)
                .map(|_| ())
        })
        .await??;
    authority.validate_current()?;
    registry.record_provider_identities(
        registered,
        std::slice::from_ref(&selected),
        deadline,
        cancellation,
    )?;
    authority.validate_current()?;
    Ok(PreparedDirectProductReference {
        selected,
        evidence,
        freshness,
    })
}

async fn register_order_level_generation(
    directory: Option<&OrderLevelDirectory>,
    spec: &ProductRuntimeSpec,
    generation: market_squawk_domain::ConnectionGeneration,
    cancellation: &CancellationToken,
) -> Result<Option<OrderLevelRegistration>, CoinbaseDirectProductRuntimeError> {
    let Some(directory) = directory else {
        return Ok(None);
    };
    let book = spec.config.limits().book();
    let retained_bytes = u32::try_from(spec.config.checked_maximum_retained_bytes()?)
        .ok()
        .and_then(NonZeroU32::new)
        .ok_or(CoinbaseDirectProductRuntimeError::OrderLevelAccounting)?;
    let order_units = book
        .max_orders()
        .checked_add(book.max_queue_events())
        .and_then(|value| u32::try_from(value).ok())
        .and_then(NonZeroU32::new)
        .ok_or(CoinbaseDirectProductRuntimeError::OrderLevelAccounting)?;
    let read_order_units = u32::try_from(book.max_orders())
        .ok()
        .and_then(NonZeroU32::new)
        .ok_or(CoinbaseDirectProductRuntimeError::OrderLevelAccounting)?;
    let actor_limits = OrderLevelActorLimits::try_new(
        NonZeroUsize::new(
            book.max_queue_events()
                .min(MAX_ORDER_LEVEL_INGRESS_COMMANDS),
        )
        .ok_or(CoinbaseDirectProductRuntimeError::OrderLevelAccounting)?,
        retained_bytes,
        order_units,
        NonZeroUsize::new(ORDER_LEVEL_OUTSTANDING_READS)
            .ok_or(CoinbaseDirectProductRuntimeError::OrderLevelAccounting)?,
        retained_bytes,
        read_order_units,
    )
    .map_err(|error| {
        tracing::error!(%error, "Coinbase Direct order-level actor configuration failed");
        CoinbaseDirectProductRuntimeError::OrderLevelConfiguration
    })?;
    let route = OrderLevelRoute::new(
        spec.config.metadata().source_id().clone(),
        spec.config.venue().clone(),
        spec.config.instrument(),
        spec.config.product().as_source_identifier().clone(),
        generation,
    );
    let limits =
        OrderLevelLimits::new(book.max_orders(), DepthLimit::new(book.published_depth())?)?;
    let deadline = Instant::now()
        .checked_add(spec.config.limits().websocket().connect_timeout())
        .ok_or(CoinbaseDirectProductRuntimeError::OrderLevelAccounting)?;
    directory
        .register(route, limits, actor_limits, cancellation, deadline)
        .await
        .map(Some)
        .map_err(|error| {
            tracing::error!(%error, "Coinbase Direct order-level generation registration failed");
            CoinbaseDirectProductRuntimeError::OrderLevelDirectory
        })
}

async fn unregister_order_level_generation(
    directory: &OrderLevelDirectory,
    key: &OrderLevelBookKey,
    app_config: &AppConfig,
) -> Result<(), CoinbaseDirectProductRuntimeError> {
    let deadline = Instant::now()
        .checked_add(app_config.source_shutdown())
        .ok_or(CoinbaseDirectProductRuntimeError::OrderLevelAccounting)?;
    let cleanup = CancellationToken::new();
    let result = directory
        .unregister(key, &cleanup, deadline)
        .await
        .map_err(|error| {
            tracing::error!(%error, "Coinbase Direct order-level generation cleanup failed");
            CoinbaseDirectProductRuntimeError::OrderLevelDirectory
        })?;
    if result == OrderLevelActorShutdown::Graceful {
        Ok(())
    } else {
        Err(CoinbaseDirectProductRuntimeError::OrderLevelShutdownIncomplete)
    }
}

#[allow(
    clippy::too_many_arguments,
    reason = "generation construction keeps every authority and cleanup owner explicit"
)]
async fn run_generation(
    spec: &ProductRuntimeSpec,
    app_config: &AppConfig,
    admission: CoinbaseDirectRuntimeAdmission,
    capture_process: CaptureProcessInfrastructure,
    live_ingress: LiveRuntimeIngress,
    publication: CoinbaseCapturedPublicationIngress,
    order_level: Option<&OrderLevelDirectory>,
    route_buffer_limits: RouteBufferLimits,
    signer: &CoinbaseDirectHmacSigner,
    registry: &mut AuthoritativeSourceRegistry,
    registered: &RegisteredSource,
    registered_reference: &RegisteredSource,
    catalog_reader: &MarketDataInstrumentReadCapability,
    catalog_synchronizer: &MarketDataInstrumentSynchronizationCapability,
    research_service: &Arc<ResearchService>,
    startup: Option<(&mpsc::Sender<ProductReady>, &mut watch::Receiver<bool>)>,
    bootstrap_slots: &Arc<Semaphore>,
    epoch: &mut DirectAccountEpoch,
    cancellation: CancellationToken,
) -> GenerationOutcome {
    let synchronized = match prepare_direct_product_reference(
        spec,
        registry,
        registered_reference,
        catalog_reader,
        catalog_synchronizer,
        research_service,
        epoch,
        &cancellation,
    )
    .await
    {
        Ok(prepared) => prepared,
        Err(error) => {
            return GenerationOutcome {
                ready_sent: false,
                result: Err(error),
            };
        }
    };
    if let Err(error) = epoch.catalog_synchronized().await {
        return GenerationOutcome {
            ready_sent: false,
            result: Err(error.into()),
        };
    }
    let prepared = match select_direct_product_reference(
        synchronized,
        registry,
        registered,
        catalog_reader,
        research_service,
        &cancellation,
    )
    .await
    {
        Ok(prepared) => prepared,
        Err(error) => {
            return GenerationOutcome {
                ready_sent: false,
                result: Err(error),
            };
        }
    };
    if let Err(error) = epoch.selected().await {
        return GenerationOutcome {
            ready_sent: false,
            result: Err(error.into()),
        };
    }
    let epoch_id = epoch.id();
    let started_at = match system_timestamp() {
        Ok(value) => value,
        Err(error) => {
            return GenerationOutcome {
                ready_sent: false,
                result: Err(error.into()),
            };
        }
    };
    let session_id = match SourceIdentifier::try_from(format!(
        "{}-{}",
        spec.config.metadata().source_id(),
        uuid::Uuid::new_v4()
    )) {
        Ok(value) => SessionId::new(value),
        Err(error) => {
            return GenerationOutcome {
                ready_sent: false,
                result: Err(error.into()),
            };
        }
    };
    let session = match registry.begin_next_session(registered, session_id, started_at) {
        Ok(value) => value,
        Err(error) => {
            return GenerationOutcome {
                ready_sent: false,
                result: Err(error.into()),
            };
        }
    };
    let route_cancellation = cancellation.child_token();
    let mut capture_control: Option<RawCaptureControl<CaptureGenerationCapabilities>> = None;
    let mut capture_writer = None;
    let mut route_worker: Option<RouteActorWorker> = None;
    let mut order_level_key: Option<OrderLevelBookKey> = None;
    let mut ready_sent = false;

    let run = async {
        let capabilities = registry.take_capture_generation_capabilities(&session)?;
        let health_reporter = registry.take_current_health_reporter(&session)?;
        let (publisher, control, writer) = raw_capture_channel(
            &capture_process,
            CaptureChannelLimits::new(
                admission.capture_queue_records_per_product(),
                admission.capture_queue_bytes_per_product(),
            ),
            capabilities,
        )?;
        let sink = RollingMemoryCaptureSink::try_new(
            admission.capture_queue_records_per_product(),
            admission.capture_queue_bytes_per_product(),
        )?;
        let flush_records = NonZeroUsize::new(
            admission
                .capture_queue_records_per_product()
                .get()
                .min(CAPTURE_FLUSH_RECORDS),
        )
        .ok_or(CoinbaseDirectProductRuntimeError::InvalidStaticPolicy)?;
        let policy =
            CaptureWriterPolicy::try_new(flush_records, app_config.capture_flush_interval())?;
        let writer = spawn_capture_writer(writer, sink, policy)?;
        capture_control = Some(control);
        capture_writer = Some(writer);
        capture_control
            .as_mut()
            .ok_or(CoinbaseDirectProductRuntimeError::CaptureOwnerMissing)?
            .activate_initial()?;

        let source_generation = registry.take_live_source_generation(&session)?;
        let order_level_registration = register_order_level_generation(
            order_level,
            spec,
            session.generation(),
            &cancellation,
        )
        .await?;
        let (order_level_ingress, mut order_level_monitor) = match order_level_registration {
            Some(registration) => {
                order_level_key = Some(registration.key().clone());
                let (ingress, monitor) = registration.into_parts();
                (Some(ingress), Some(monitor))
            }
            None => (None, None),
        };
        let dormant = live_ingress.reserve_route(spec.route.route().clone())?;
        let (route, worker) =
            spawn_route_activation(dormant, route_buffer_limits, route_cancellation.clone());
        route_worker = Some(worker);
        let subscription = SubscriptionStateMachine::try_new(
            GenerationIdentity::from_session(&session),
            [spec.config.product().as_source_identifier().as_str()],
            spec.config.limits().websocket().io_timeout(),
            Instant::now(),
            SubscriptionLimits::try_new(CONTROL_AUDIT_RECORDS, CONTROL_AUDIT_BYTES, 0, 0)?,
        )?;
        let mut source = CoinbaseDirectSession::try_new(
            spec.config.clone(),
            source_generation,
            install_ring_tls_provider()?,
            prepared.selected,
            prepared.evidence,
            prepared.freshness,
        )?;
        let mut sink =
            ProductionRawMarketSink::try_new_predecoded(ProductionPredecodedMarketSinkInput {
                capture: publisher,
                registry,
                session: &session,
                health_reporter,
                metadata: spec.config.metadata().clone(),
                subscription,
                live_ingress,
                routes: vec![route],
            })?;
        if let Some((ready, start)) = startup {
            tokio::select! {
                biased;
                () = cancellation.cancelled() => {
                    return Err(DirectAccountCoordinatorError::Cancelled.into());
                }
                sent = ready.send(ProductReady { slot: spec.slot, epoch: epoch_id }) => {
                    sent.map_err(|_error| CoinbaseDirectProductRuntimeError::SupervisorQueue)?;
                }
            }
            ready_sent = true;
            wait_for_account_start(start, &cancellation).await?;
        }

        let bootstrap_permit = tokio::select! {
            biased;
            () = cancellation.cancelled() => {
                return Err(CoinbaseDirectSessionError::Source(SourceError::Cancelled).into());
            }
            permit = Arc::clone(bootstrap_slots).acquire_owned() => {
                permit.map_err(
                    |_error| CoinbaseDirectProductRuntimeError::BootstrapGateClosed,
                )?
            }
        };
        let order_level_publish_timeout = order_level_ingress
            .as_ref()
            .map(|_| spec.config.limits().websocket().io_timeout());
        let mut output = CoinbaseDirectProductOutput::new(
            &mut sink,
            spec.config.product().clone(),
            bootstrap_permit,
            order_level_ingress,
            order_level_publish_timeout,
            publication,
            spec.config
                .metadata()
                .coverage()
                .live()
                .ok_or(CoinbaseDirectProductRuntimeError::ActivationBinding)?
                .provider_product()
                .as_source_identifier()
                .clone(),
            spec.config
                .metadata()
                .coverage()
                .live()
                .ok_or(CoinbaseDirectProductRuntimeError::ActivationBinding)?
                .provider_channel()
                .as_source_identifier()
                .clone(),
        );
        let session_result = match order_level_monitor.as_mut() {
            Some(monitor) => tokio::select! {
                biased;
                terminal = monitor.wait_until_terminal(&cancellation) => match terminal {
                    Ok(failure) => {
                        tracing::error!(%failure, "Coinbase Direct order-level actor failed terminally");
                        Err(CoinbaseDirectProductRuntimeError::OrderLevelTerminal)
                    }
                    Err(OrderLevelMonitorError::Cancelled) if cancellation.is_cancelled() => {
                        Err(CoinbaseDirectSessionError::Source(SourceError::Cancelled).into())
                    }
                    Err(error) => {
                        tracing::error!(%error, "Coinbase Direct order-level monitor failed");
                        Err(CoinbaseDirectProductRuntimeError::OrderLevelMonitor)
                    }
                },
                result = source.run(signer, &mut output, cancellation.clone()) => {
                    result.map_err(Into::into)
                }
            },
            None => source
                .run(signer, &mut output, cancellation.clone())
                .await
                .map_err(Into::into),
        };
        let output_failure = output.terminal_failure();
        drop(output);
        let sink_failure = sink.terminal_failure();
        drop(sink);
        if let Some(failure) = output_failure {
            return Err(failure.into());
        }
        if let Some(failure) = sink_failure {
            return Err(failure.into());
        }
        session_result
    }
    .await;

    route_cancellation.cancel();
    let mut cleanup = None;
    if let (Some(directory), Some(key)) = (order_level, order_level_key.as_ref()) {
        let result = unregister_order_level_generation(directory, key, app_config).await;
        retain_first_error(&mut cleanup, result);
    }
    if let Some(worker) = route_worker {
        let result = cleanup_route_worker(worker).await;
        retain_first_error(&mut cleanup, result);
    }
    let ended_at = system_timestamp().unwrap_or(started_at);
    retain_first_error(
        &mut cleanup,
        registry.end_session(&session, ended_at).map_err(Into::into),
    );
    if let Some(mut control) = capture_control {
        control.invalidate_current();
        drop(control);
    }
    if let Some(writer) = capture_writer {
        retain_first_error(
            &mut cleanup,
            shutdown_capture_writer(writer, app_config.capture_shutdown()).await,
        );
    }
    let result = match (run, cleanup) {
        (Ok(()), None) => Ok(()),
        (Err(source), None) => Err(source),
        (Ok(()), Some(cleanup)) => Err(cleanup),
        (Err(source), Some(cleanup)) => Err(CoinbaseDirectProductRuntimeError::RunCleanup {
            source: Box::new(source),
            cleanup: Box::new(cleanup),
        }),
    };
    GenerationOutcome { ready_sent, result }
}

async fn wait_for_account_start(
    start: &mut watch::Receiver<bool>,
    cancellation: &CancellationToken,
) -> Result<(), CoinbaseDirectProductRuntimeError> {
    while !*start.borrow() {
        tokio::select! {
            biased;
            () = cancellation.cancelled() => {
                return Err(CoinbaseDirectSessionError::Source(SourceError::Cancelled).into());
            }
            changed = start.changed() => {
                changed.map_err(|_error| CoinbaseDirectProductRuntimeError::StartupBarrier)?;
            }
        }
    }
    Ok(())
}

async fn cleanup_route_worker(
    worker: RouteActorWorker,
) -> Result<(), CoinbaseDirectProductRuntimeError> {
    match worker.await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(failure)) => Err(ProductionSinkFailure::RouteActivation(failure).into()),
        Err(error) => Err(CoinbaseDirectProductRuntimeError::RouteTask(error)),
    }
}

async fn shutdown_capture_writer(
    writer: market_squawk_platform::CaptureWriterHandle<CaptureGenerationCapabilities>,
    deadline: Duration,
) -> Result<(), CoinbaseDirectProductRuntimeError> {
    let mut pending = writer.shutdown(deadline);
    let status = pending.wait_until_deadline().await;
    if status == CaptureShutdownStatus::DeadlineElapsed {
        pending.wait_until_terminated().await;
    }
    let termination = pending
        .try_reap()?
        .ok_or(CoinbaseDirectProductRuntimeError::CaptureOwnerMissing)?;
    if status == CaptureShutdownStatus::DeadlineElapsed
        || termination.shutdown_deadline_elapsed()
        || termination.outcome().is_incomplete()
    {
        return Err(CoinbaseDirectProductRuntimeError::CaptureShutdownIncomplete);
    }
    Ok(())
}

fn retain_first_error(
    retained: &mut Option<CoinbaseDirectProductRuntimeError>,
    candidate: Result<(), CoinbaseDirectProductRuntimeError>,
) {
    if retained.is_none() {
        *retained = candidate.err();
    }
}

async fn wait_after_failure(
    error: CoinbaseDirectProductRuntimeError,
    backoff: &ProviderBackoffAuthority,
    product_status_retry: Duration,
    cancellation: &CancellationToken,
) -> Result<(), CoinbaseDirectProductRuntimeError> {
    if matches!(
        &error,
        CoinbaseDirectProductRuntimeError::Session(CoinbaseDirectSessionError::Source(
            SourceError::BudgetUnavailable {
                reason: BudgetUnavailableReason::ConcurrencyExhausted,
            },
        ))
    ) {
        return wait_for_local_retry(LOCAL_CONCURRENCY_RETRY, cancellation).await;
    }
    if matches!(
        &error,
        CoinbaseDirectProductRuntimeError::Output(CoinbaseDirectOutputFailure::ProductUnavailable,)
    ) {
        return wait_for_local_retry(product_status_retry, cancellation).await;
    }
    let deadline = match &error {
        CoinbaseDirectProductRuntimeError::Session(CoinbaseDirectSessionError::Source(
            SourceError::BudgetWaitUntil { deadline },
        )) => *deadline,
        _ => match backoff.apply_refusal(BACKOFF_JITTER_SAMPLE_BASIS_POINTS)? {
            ProviderBackoffDecision::WaitUntil(deadline) => deadline,
            ProviderBackoffDecision::Unavailable(reason) => {
                return Err(CoinbaseDirectProductRuntimeError::BudgetUnavailable(reason));
            }
        },
    };
    let wait = backoff.remaining_wait(deadline)?;
    tokio::select! {
        biased;
        () = cancellation.cancelled() => Ok(()),
        () = tokio::time::sleep(wait) => Ok(()),
    }
}

async fn wait_for_local_retry(
    duration: Duration,
    cancellation: &CancellationToken,
) -> Result<(), CoinbaseDirectProductRuntimeError> {
    tokio::select! {
        biased;
        () = cancellation.cancelled() => Ok(()),
        () = tokio::time::sleep(duration) => Ok(()),
    }
}

/// Product construction, generation, capture, reconnect, or cleanup failure.
#[derive(Debug, Error)]
pub enum CoinbaseDirectProductRuntimeError {
    /// Account-wide generation ordering or cancellation failed closed.
    #[error(transparent)]
    Coordinator(#[from] DirectAccountCoordinatorError),
    /// Activation evidence is incomplete or inconsistent with Direct runtime construction.
    #[error("Coinbase Direct activation evidence is incomplete")]
    ActivationBinding,
    /// Canonical metadata evidence could not be represented.
    #[error("Coinbase Direct metadata evidence encoding failed")]
    EvidenceEncoding,
    /// Checked order-level resource accounting could not be represented.
    #[error("Coinbase Direct order-level accounting is invalid")]
    OrderLevelAccounting,
    /// The exact generation-owned order-level actor did not shut down cleanly.
    #[error("Coinbase Direct order-level actor shutdown was incomplete")]
    OrderLevelShutdownIncomplete,
    /// The exact generation-owned order-level actor entered a terminal fail-closed state.
    #[error("Coinbase Direct order-level actor failed terminally")]
    OrderLevelTerminal,
    /// A static bounded policy unexpectedly produced zero.
    #[error("Coinbase Direct static runtime policy is invalid")]
    InvalidStaticPolicy,
    /// A generation lost an explicitly retained capture owner.
    #[error("Coinbase Direct capture ownership is incomplete")]
    CaptureOwnerMissing,
    /// The transient capture worker did not drain and terminate cleanly.
    #[error("Coinbase Direct capture shutdown was incomplete")]
    CaptureShutdownIncomplete,
    /// A product completed before account cancellation.
    #[error("Coinbase Direct product source exited unexpectedly")]
    SourceExited,
    /// The bounded account startup channel rejected a ready notification.
    #[error("Coinbase Direct account startup queue is unavailable")]
    SupervisorQueue,
    /// The account startup barrier closed before network release.
    #[error("Coinbase Direct account startup barrier closed")]
    StartupBarrier,
    /// The account-wide bootstrap admission owner closed unexpectedly.
    #[error("Coinbase Direct account bootstrap admission closed")]
    BootstrapGateClosed,
    /// Shared provider budget admission is terminally unavailable.
    #[error("Coinbase Direct provider budget is unavailable: {0:?}")]
    BudgetUnavailable(BudgetUnavailableReason),
    /// Product runtime and registry shutdown both failed.
    #[error("Coinbase Direct product runtime and registry shutdown both failed")]
    RunShutdown {
        /// Primary product failure.
        source: Box<Self>,
        /// Registry shutdown failure.
        shutdown: RegistryError,
    },
    /// Product generation and its bounded cleanup both failed.
    #[error("Coinbase Direct generation and bounded cleanup both failed")]
    RunCleanup {
        /// Primary generation failure.
        source: Box<Self>,
        /// Cleanup failure.
        cleanup: Box<Self>,
    },
    /// The Direct adapter rejected configuration or exact runtime bounds.
    #[error(transparent)]
    Configuration(#[from] CoinbaseConfigError),
    /// Trusted wall-clock conversion failed.
    #[error(transparent)]
    Clock(#[from] ProductionCoinbaseProfileError),
    /// Stable financial identity construction failed.
    #[error(transparent)]
    Identity(#[from] IdentityError),
    /// Price-level projection depth could not be represented.
    #[error(transparent)]
    OrderLevelBook(#[from] BookError),
    /// Canonical order-level retained-state limits were invalid.
    #[error(transparent)]
    OrderLevelLimit(#[from] OrderLevelLimitError),
    /// Application actor limits were invalid.
    #[error("Coinbase Direct order-level actor configuration failed")]
    OrderLevelConfiguration,
    /// The process-wide order-level directory rejected this generation.
    #[error("Coinbase Direct order-level directory operation failed")]
    OrderLevelDirectory,
    /// The order-level supervisor monitor failed before the source exited.
    #[error("Coinbase Direct order-level supervisor monitor failed")]
    OrderLevelMonitor,
    /// Authorization or coverage interval construction failed.
    #[error(transparent)]
    Interval(#[from] InstrumentError),
    /// Local path preparation failed.
    #[error(transparent)]
    Paths(#[from] market_squawk_platform::PathError),
    /// Durable authority-store ownership failed.
    #[error(transparent)]
    AuthorityStore(#[from] LocalAuthorityStateStoreError),
    /// Product reference extraction authority is no longer current.
    #[error(transparent)]
    ExtractionAuthority(#[from] ExtractionAuthorityError),
    /// Source registry authority failed.
    #[error(transparent)]
    Registry(#[from] RegistryError),
    /// Original product bytes or receipt could not be sealed into exact physical custody.
    #[error(transparent)]
    ReferenceCapture(#[from] ProviderCaptureError),
    /// Original product raw record was invalid.
    #[error(transparent)]
    RawCapture(#[from] RawCaptureRecordError),
    /// Research journal could not retain the original reference.
    #[error(transparent)]
    Research(#[from] ResearchServiceError),
    /// Sealed original product reference did not match the configured Direct route.
    #[error("Coinbase Direct sealed product reference did not bind to its route: {0}")]
    DirectReference(#[source] Box<dyn std::error::Error + Send + Sync>),
    /// Coinbase rejected the physically sealed product response.
    #[error(transparent)]
    ProductReference(#[from] CoinbaseDirectProductError),
    /// Accepted native capture edge or catalog selection failed.
    #[error(transparent)]
    Catalog(#[from] MarketDataInstrumentCatalogError),
    /// The accepted original could not be physically reopened from its retained journal claim.
    #[error(transparent)]
    Journal(#[from] SealedResearchJournalStoreError),
    /// Shared native catalog synchronization failed.
    #[error("Coinbase Direct native reference synchronization failed: {0}")]
    ReferenceSync(#[source] Box<dyn std::error::Error + Send + Sync>),
    /// Capture channel construction failed.
    #[error(transparent)]
    CaptureChannel(#[from] CaptureChannelError),
    /// Capture generation activation failed.
    #[error(transparent)]
    CaptureGeneration(#[from] CaptureGenerationError),
    /// Transient capture storage construction failed.
    #[error(transparent)]
    CaptureStorage(#[from] MemoryCaptureSinkConstructionError),
    /// Capture writer policy construction failed.
    #[error(transparent)]
    CapturePolicy(#[from] CaptureWriterPolicyError),
    /// Capture worker startup failed.
    #[error(transparent)]
    CaptureWriter(#[from] CaptureWriterSpawnError),
    /// Capture worker reap failed.
    #[error(transparent)]
    CaptureReap(#[from] CaptureWorkerReapError),
    /// Live-route reservation failed.
    #[error(transparent)]
    RouteBind(#[from] LiveIngressBindError),
    /// Route actor task failed.
    #[error("Coinbase Direct route actor task failed")]
    RouteTask(tokio::task::JoinError),
    /// Subscription state construction failed.
    #[error(transparent)]
    Subscription(#[from] SubscriptionConstructionError),
    /// Predecoded production sink construction failed.
    #[error(transparent)]
    SinkConstruction(#[from] ProductionSinkConstructionError),
    /// Production sink authority failed closed.
    #[error("Coinbase Direct production sink failed closed: {0}")]
    Sink(#[from] ProductionSinkFailure),
    /// Direct-specific capture or current-product qualification failed.
    #[error(transparent)]
    Output(#[from] CoinbaseDirectOutputFailure),
    /// Direct transport, synchronization, or signing failed.
    #[error(transparent)]
    Session(#[from] CoinbaseDirectSessionError),
    /// TLS provider installation failed.
    #[error(transparent)]
    Tls(#[from] TlsProviderError),
    /// Shared provider refusal backoff failed.
    #[error(transparent)]
    Backoff(#[from] ProviderBackoffError),
}

impl CoinbaseDirectProductRuntimeError {
    fn coordinated_cancellation(&self) -> bool {
        matches!(
            self,
            Self::Coordinator(DirectAccountCoordinatorError::Cancelled)
                | Self::Session(CoinbaseDirectSessionError::Source(SourceError::Cancelled))
                | Self::Catalog(MarketDataInstrumentCatalogError::Cancelled)
                | Self::Registry(RegistryError::ProviderIdentitySelectionCancelled)
                | Self::Research(ResearchServiceError::Ingest(
                    market_squawk_data::IngestError::Cancelled
                ))
                | Self::Journal(SealedResearchJournalStoreError::ObjectControl(
                    ResearchObjectControlError::Cancelled,
                ))
        ) || matches!(
            self,
            Self::ReferenceSync(error)
                if error.downcast_ref::<crate::live_source::crypto_reference::CryptoReferenceError>()
                    .is_some_and(|error| matches!(error, crate::live_source::crypto_reference::CryptoReferenceError::Cancelled))
        )
    }

    fn recoverable(&self) -> bool {
        match self {
            Self::Sink(failure) => failure.requires_generation_resynchronization(),
            Self::Output(CoinbaseDirectOutputFailure::ProductUnavailable) => true,
            Self::Output(CoinbaseDirectOutputFailure::OrderLevelPublication)
            | Self::OrderLevelTerminal
            | Self::OrderLevelMonitor => true,
            Self::Session(CoinbaseDirectSessionError::Source(source)) => matches!(
                source,
                SourceError::Network
                    | SourceError::ConnectionIdle
                    | SourceError::FrameTooLarge { .. }
                    | SourceError::GenerationResynchronizationRequired
                    | SourceError::ProviderUnavailable
                    | SourceError::BudgetWaitUntil { .. }
                    | SourceError::BudgetUnavailable {
                        reason: BudgetUnavailableReason::ConcurrencyExhausted,
                    }
            ),
            Self::Session(
                CoinbaseDirectSessionError::Decode(_)
                | CoinbaseDirectSessionError::Product(_)
                | CoinbaseDirectSessionError::Snapshot(_)
                | CoinbaseDirectSessionError::Book(_)
                | CoinbaseDirectSessionError::Capture(_)
                | CoinbaseDirectSessionError::Subscription
                | CoinbaseDirectSessionError::WebSocketProtocol
                | CoinbaseDirectSessionError::HttpResponse
                | CoinbaseDirectSessionError::HttpDeadline
                | CoinbaseDirectSessionError::HttpBodyTooLarge
                | CoinbaseDirectSessionError::HttpSegmentLimit,
            ) => true,
            _ => false,
        }
    }
}
