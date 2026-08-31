//! Concrete bounded Schwab market-data doctor probes.
//!
//! This leaf owns only provider-native request execution. Rate admission remains in the doctor
//! orchestrator, OAuth remains in [`SchwabOAuthMarketAuthority`], and exact raw bytes cross only
//! the injected application sealer. The closed plans below cannot represent account, position,
//! transaction, order, money-movement, or Streamer account-activity operations.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::future::Future;
use std::num::{NonZeroU16, NonZeroU64, NonZeroUsize};
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use market_squawk_adapter_schwab::{
    AccessTokenAdmission, ChainRequest, ConnectionGeneration, ExpirationChainRequest,
    InstrumentProjection, MarketDataService, MarketId, MoverFrequency, MoverSort, NativeField,
    ParseBounds, PriceHistoryFrequency, PriceHistoryFrequencyType, PriceHistoryPeriodType,
    PriceHistoryRequest, ProductionSchwabStreamerConnector, ProviderIdentifier, QuoteField,
    QuoteRequest, RawRestResponseReceipt, ReadOnlyRequest, ReadOnlyRoute, RequestAdmission,
    RestExecutionOutcome, RestItemAccounting, RestTransportBounds, SchwabAccessTokenSource,
    SchwabAdapterError, SchwabCaptureCoordinates, SchwabCredentialAuthorityBinding,
    SchwabRestExecutor, SchwabRestFamily, SchwabRestFamilyDoctorInput, SchwabRestPayload,
    SchwabSealedRawRestCapture, SchwabSealedRestResponse, SchwabSealedStreamerCapture,
    SchwabStreamerConnectionControl, SchwabStreamerConnectionControlSource,
    SchwabStreamerConnector, SchwabStreamerExecutor, SchwabStreamerFamilyDoctorAccumulator,
    SchwabStreamerFamilyDoctorHandoff, SchwabTransportError, SchwabTransportTelemetry,
    SchwabUserPreferenceEvidence, SchwabVerticalError, StreamerAdmission, StreamerCaptureSink,
    StreamerCaptureSinkError, StreamerMicrobatch, StreamerSubscription, StreamerTransportBounds,
    TokenAuthorityError, build_instrument_search_request, build_market_hours_request,
    build_movers_request,
};
use market_squawk_domain::{
    CoverageDelay, DigestAlgorithm, EvidenceDigest, MetadataRevision, SourceId, SourceIdentifier,
    Timestamp,
};
use market_squawk_platform::LocalAuthorityStateStore;
use market_squawk_sources::{RuntimeCapabilityDisposition, SchwabMarketDataFamily};
use sha2::{Digest as _, Sha256};
use tokio::sync::{Mutex, mpsc};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::schwab_market_doctor::{
    DoctorFuture, SchwabMarketDataDoctorError, SchwabMarketDoctorCaptureSealer,
    SchwabMarketDoctorFamilyProbeEvidence, SchwabMarketDoctorFamilyProbeInput,
    SchwabMarketDoctorProbeExecutor, SchwabMarketDoctorProbeScope, SchwabMarketDoctorProbeStatus,
    SchwabMarketDoctorRateObservation, SchwabMarketDoctorSetupRequiredEvidence,
    SchwabMarketDoctorUserPreferenceAvailable, SchwabMarketDoctorUserPreferenceOutcome,
    adapter_rest_family, streamer_service, user_preference_endpoint_contract_sha256,
};
use super::schwab_oauth_runtime::SchwabOAuthMarketAuthority;

const PROBE_CONTRACT_DOMAIN: &[u8] = b"market-squawk/schwab-provider-native-doctor-probes/v1";
const REST_OBSERVATION_DOMAIN: &[u8] = b"market-squawk/schwab-rest-doctor-observation/v1";
const REST_LIMITATION_DOMAIN: &[u8] = b"market-squawk/schwab-rest-doctor-limitation/v1";
const USER_PREFERENCE_REASON_DOMAIN: &[u8] =
    b"market-squawk/schwab-user-preference-doctor-reason/v1";
const USER_PREFERENCE_OFFER_DOMAIN: &[u8] =
    b"market-squawk/schwab-user-preference-market-data-offer/v1";
const PRODUCTION_CONTROL_STATE_DOMAIN: &[u8] =
    b"market-squawk/schwab-market-doctor-streamer-control/v1\0";
const PRODUCTION_OPTION_STREAMER_SELECTION_DOMAIN: &[u8] =
    b"market-squawk/schwab-market-doctor/returned-unexpired-option-contract/v1\0";
const PRODUCTION_CONTROL_DIRECTORY: &str = "sources/schwab-market-doctor-streamer-v1";
const PRODUCTION_SOURCE_ID: &str = "schwab-market-doctor";
const PRODUCTION_METADATA_REVISION: &str = "schwab-market-doctor-native-v1";

const REST_FAMILIES: [SchwabMarketDataFamily; 7] = [
    SchwabMarketDataFamily::Quotes,
    SchwabMarketDataFamily::PriceHistory,
    SchwabMarketDataFamily::OptionChains,
    SchwabMarketDataFamily::ExpirationChains,
    SchwabMarketDataFamily::Movers,
    SchwabMarketDataFamily::MarketHours,
    SchwabMarketDataFamily::Instruments,
];

const STREAMER_FAMILIES: [SchwabMarketDataFamily; 12] = [
    SchwabMarketDataFamily::LevelOneEquities,
    SchwabMarketDataFamily::LevelOneOptions,
    SchwabMarketDataFamily::LevelOneFutures,
    SchwabMarketDataFamily::LevelOneFuturesOptions,
    SchwabMarketDataFamily::LevelOneForex,
    SchwabMarketDataFamily::NyseBook,
    SchwabMarketDataFamily::NasdaqBook,
    SchwabMarketDataFamily::OptionsBook,
    SchwabMarketDataFamily::ChartEquity,
    SchwabMarketDataFamily::ChartFutures,
    SchwabMarketDataFamily::ScreenerEquity,
    SchwabMarketDataFamily::ScreenerOption,
];

/// One exact allowlisted REST request plus application-issued raw-capture coordinates.
#[derive(Clone, Debug)]
pub(crate) struct SchwabMarketDoctorRestProbe {
    family: SchwabMarketDataFamily,
    request: ReadOnlyRequest,
    coordinates: SchwabCaptureCoordinates,
}

impl SchwabMarketDoctorRestProbe {
    pub(crate) fn try_new(
        family: SchwabMarketDataFamily,
        request: ReadOnlyRequest,
        coordinates: SchwabCaptureCoordinates,
    ) -> Result<Self, SchwabMarketDataDoctorError> {
        if !rest_route_matches(family, request.route()) {
            return Err(SchwabMarketDataDoctorError::InvalidProbeContract);
        }
        Ok(Self {
            family,
            request,
            coordinates,
        })
    }
}

/// One exact admitted subscription or provider-returned contract selection for one service.
#[derive(Clone, Debug)]
pub(crate) struct SchwabMarketDoctorStreamerProbe {
    family: SchwabMarketDataFamily,
    target: SchwabMarketDoctorStreamerTarget,
}

#[derive(Clone, Debug)]
enum SchwabMarketDoctorStreamerTarget {
    Exact(StreamerSubscription),
    ReturnedOptionContract {
        service: MarketDataService,
        field_ids: BTreeSet<u16>,
    },
}

impl SchwabMarketDoctorStreamerProbe {
    pub(crate) fn try_new(
        family: SchwabMarketDataFamily,
        subscription: StreamerSubscription,
    ) -> Result<Self, SchwabMarketDataDoctorError> {
        if streamer_service(family) != Some(subscription.service()) {
            return Err(SchwabMarketDataDoctorError::InvalidProbeContract);
        }
        Ok(Self {
            family,
            target: SchwabMarketDoctorStreamerTarget::Exact(subscription),
        })
    }

    fn try_returned_option_contract(
        family: SchwabMarketDataFamily,
        field_ids: Vec<u16>,
        admission: StreamerAdmission,
    ) -> Result<Self, SchwabMarketDataDoctorError> {
        let service = streamer_service(family)
            .filter(|service| {
                matches!(
                    service,
                    MarketDataService::LevelOneOptions | MarketDataService::OptionsBook
                )
            })
            .ok_or(SchwabMarketDataDoctorError::InvalidProbeContract)?;
        let field_ids = field_ids.into_iter().collect::<BTreeSet<_>>();
        if field_ids.is_empty() || field_ids.len() > admission.max_fields_per_service() {
            return Err(SchwabMarketDataDoctorError::InvalidProbeContract);
        }
        Ok(Self {
            family,
            target: SchwabMarketDoctorStreamerTarget::ReturnedOptionContract { service, field_ids },
        })
    }

    const fn service(&self) -> MarketDataService {
        match &self.target {
            SchwabMarketDoctorStreamerTarget::Exact(subscription) => subscription.service(),
            SchwabMarketDoctorStreamerTarget::ReturnedOptionContract { service, .. } => *service,
        }
    }

    fn field_ids(&self) -> &BTreeSet<u16> {
        match &self.target {
            SchwabMarketDoctorStreamerTarget::Exact(subscription) => subscription.field_ids(),
            SchwabMarketDoctorStreamerTarget::ReturnedOptionContract { field_ids, .. } => field_ids,
        }
    }
}

struct RetainedUserPreference {
    token_generation: u64,
    provider: SchwabUserPreferenceEvidence,
}

impl fmt::Debug for RetainedUserPreference {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RetainedUserPreference")
            .field("token_generation", &self.token_generation)
            .field("provider", &self.provider)
            .finish()
    }
}

struct ProductionStreamerControlState {
    store: LocalAuthorityStateStore,
    last_generation: u64,
}

/// Restart-safe, application-owned allocator for the doctor's one-use Streamer controls.
struct ProductionSchwabMarketDoctorStreamerControlSource {
    state: Arc<std::sync::Mutex<ProductionStreamerControlState>>,
    source_id: SourceId,
    metadata_revision: MetadataRevision,
    dataset: SourceIdentifier,
}

impl ProductionSchwabMarketDoctorStreamerControlSource {
    fn try_open(control_root: &Path) -> Result<Self, SchwabMarketDataDoctorError> {
        let store =
            LocalAuthorityStateStore::try_open(control_root.join(PRODUCTION_CONTROL_DIRECTORY))
                .map_err(|_| SchwabMarketDataDoctorError::InvalidProbeContract)?;
        let last_generation = match store
            .load()
            .map_err(|_| SchwabMarketDataDoctorError::InvalidProbeContract)?
        {
            Some(payload) => decode_production_control_state(&payload)?,
            None => 0,
        };
        Ok(Self {
            state: Arc::new(std::sync::Mutex::new(ProductionStreamerControlState {
                store,
                last_generation,
            })),
            source_id: source_id(PRODUCTION_SOURCE_ID)?,
            metadata_revision: metadata_revision(PRODUCTION_METADATA_REVISION)?,
            dataset: identifier("schwab-market-doctor-streamer")?,
        })
    }
}

impl SchwabStreamerConnectionControlSource for ProductionSchwabMarketDoctorStreamerControlSource {
    fn mint(
        &self,
    ) -> std::pin::Pin<
        Box<
            dyn Future<Output = Result<SchwabStreamerConnectionControl, SchwabTransportError>>
                + Send
                + '_,
        >,
    > {
        let state = Arc::clone(&self.state);
        let source_id = self.source_id.clone();
        let metadata_revision = self.metadata_revision.clone();
        let dataset = self.dataset.clone();
        Box::pin(async move {
            let generation = tokio::task::spawn_blocking(move || {
                let mut state = state
                    .lock()
                    .map_err(|_| SchwabTransportError::CaptureMaterial)?;
                let generation = state
                    .last_generation
                    .checked_add(1)
                    .ok_or(SchwabTransportError::Overflow)?;
                state
                    .store
                    .store(&encode_production_control_state(generation))
                    .map_err(|_| SchwabTransportError::CaptureMaterial)?;
                state.last_generation = generation;
                Ok::<u64, SchwabTransportError>(generation)
            })
            .await
            .map_err(|_| SchwabTransportError::CaptureMaterial)??;
            let generation = NonZeroU64::new(generation)
                .map(ConnectionGeneration::new)
                .ok_or(SchwabTransportError::Overflow)?;
            let session_identifier = source_id.as_source_identifier().clone();
            let coordinates = SchwabCaptureCoordinates::try_new(
                source_id,
                metadata_revision,
                dataset,
                Uuid::new_v4(),
            )?;
            let stream_identity = SourceIdentifier::try_from(format!(
                "schwab-market-doctor-stream-{}",
                generation.get()
            ))
            .map_err(|_| SchwabTransportError::CaptureMaterial)?;
            Ok(SchwabStreamerConnectionControl::new(
                generation,
                session_identifier,
                coordinates,
                stream_identity,
            ))
        })
    }
}

impl fmt::Debug for ProductionSchwabMarketDoctorStreamerControlSource {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ProductionSchwabMarketDoctorStreamerControlSource")
            .field("state", &"[DURABLE APPLICATION AUTHORITY]")
            .field("source_id", &self.source_id)
            .field("metadata_revision", &self.metadata_revision)
            .field("dataset", &self.dataset)
            .finish()
    }
}

/// Concrete provider-native executor for the closed seven-REST/twelve-Streamer doctor contract.
pub(crate) struct ProviderNativeSchwabMarketDoctorProbeExecutor {
    rest_executor: Arc<SchwabRestExecutor>,
    user_preference_request: ReadOnlyRequest,
    rest: BTreeMap<SchwabMarketDataFamily, SchwabMarketDoctorRestProbe>,
    streamer: BTreeMap<SchwabMarketDataFamily, SchwabMarketDoctorStreamerProbe>,
    streamer_connector: Arc<dyn SchwabStreamerConnector>,
    streamer_control_source: Arc<dyn SchwabStreamerConnectionControlSource>,
    streamer_admission: StreamerAdmission,
    streamer_bounds: StreamerTransportBounds,
    parse_bounds: ParseBounds,
    token_admission: AccessTokenAdmission,
    telemetry: SchwabTransportTelemetry,
    channel_capacity: NonZeroUsize,
    probe_contract_digest: EvidenceDigest,
    user_preference: Mutex<Option<RetainedUserPreference>>,
    streamer_option_contract: Mutex<Option<ProviderIdentifier>>,
}

impl ProviderNativeSchwabMarketDoctorProbeExecutor {
    /// Constructs the complete code-owned read-only production doctor plan.
    ///
    /// The sample identifiers keep each request bounded to one instrument or market. They are
    /// provider-native probe inputs only; no provider identity escapes into product reads.
    pub(crate) fn try_production_installed(
        control_root: &Path,
    ) -> Result<Self, SchwabMarketDataDoctorError> {
        let request_admission = RequestAdmission::new(nonzero(16 * 1024)?, NonZeroUsize::MIN);
        let parse_bounds = ParseBounds::new(
            nonzero(4 * 1024 * 1024)?,
            nonzero(8 * 1024)?,
            nonzero(256 * 1024)?,
            nonzero(64)?,
            512,
            512 * 1024,
        );
        let token_admission =
            AccessTokenAdmission::new(nonzero(16 * 1024)?, Duration::from_secs(60));
        let telemetry = SchwabTransportTelemetry::default();
        let rest_bounds = RestTransportBounds::try_new(
            Duration::from_secs(5),
            Duration::from_secs(15),
            Duration::from_secs(20),
            nonzero(4 * 1024 * 1024)?,
            nonzero(64)?,
            nonzero(64 * 1024)?,
        )
        .map_err(|_| SchwabMarketDataDoctorError::InvalidProbeContract)?;
        let rest_executor = Arc::new(
            SchwabRestExecutor::try_production(
                rest_bounds,
                parse_bounds,
                token_admission,
                telemetry.clone(),
            )
            .map_err(|_| SchwabMarketDataDoctorError::InvalidProbeContract)?,
        );
        let streamer_admission = StreamerAdmission::new(
            request_admission,
            nonzero(STREAMER_FAMILIES.len())?,
            nonzero(8)?,
        );
        let streamer_bounds = StreamerTransportBounds::try_new(
            Duration::from_secs(5),
            Duration::from_secs(10),
            Duration::ZERO,
            0,
            nonzero(4 * 1024 * 1024)?,
            nonzero(64)?,
            nonzero(8 * 1024 * 1024)?,
            Duration::from_millis(250),
        )
        .map_err(|_| SchwabMarketDataDoctorError::InvalidProbeContract)?;
        let streamer_control_source = Arc::new(
            ProductionSchwabMarketDoctorStreamerControlSource::try_open(control_root)?,
        );
        Self::try_production(
            rest_executor,
            ReadOnlyRequest::user_preference(request_admission)
                .map_err(|_| SchwabMarketDataDoctorError::InvalidProbeContract)?,
            production_rest_probes(request_admission)?,
            production_streamer_probes(streamer_admission)?,
            streamer_control_source,
            streamer_admission,
            streamer_bounds,
            parse_bounds,
            token_admission,
            telemetry,
            nonzero(8)?,
        )
    }

    /// Builds the production WSS probe boundary. REST production/injected transport is supplied
    /// explicitly so it shares the application's configured bounds and telemetry.
    #[allow(
        clippy::too_many_arguments,
        reason = "transport, capture, authority, and closed probe plans remain explicit"
    )]
    pub(crate) fn try_production(
        rest_executor: Arc<SchwabRestExecutor>,
        user_preference_request: ReadOnlyRequest,
        rest: Vec<SchwabMarketDoctorRestProbe>,
        streamer: Vec<SchwabMarketDoctorStreamerProbe>,
        streamer_control_source: Arc<dyn SchwabStreamerConnectionControlSource>,
        streamer_admission: StreamerAdmission,
        streamer_bounds: StreamerTransportBounds,
        parse_bounds: ParseBounds,
        token_admission: AccessTokenAdmission,
        telemetry: SchwabTransportTelemetry,
        channel_capacity: NonZeroUsize,
    ) -> Result<Self, SchwabMarketDataDoctorError> {
        Self::try_new(
            rest_executor,
            user_preference_request,
            rest,
            streamer,
            Arc::new(ProductionSchwabStreamerConnector),
            streamer_control_source,
            streamer_admission,
            streamer_bounds,
            parse_bounds,
            token_admission,
            telemetry,
            channel_capacity,
        )
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "the injectable connector preserves the same production lifecycle"
    )]
    pub(crate) fn try_new(
        rest_executor: Arc<SchwabRestExecutor>,
        user_preference_request: ReadOnlyRequest,
        rest: Vec<SchwabMarketDoctorRestProbe>,
        streamer: Vec<SchwabMarketDoctorStreamerProbe>,
        streamer_connector: Arc<dyn SchwabStreamerConnector>,
        streamer_control_source: Arc<dyn SchwabStreamerConnectionControlSource>,
        streamer_admission: StreamerAdmission,
        streamer_bounds: StreamerTransportBounds,
        parse_bounds: ParseBounds,
        token_admission: AccessTokenAdmission,
        telemetry: SchwabTransportTelemetry,
        channel_capacity: NonZeroUsize,
    ) -> Result<Self, SchwabMarketDataDoctorError> {
        if user_preference_request.route() != ReadOnlyRoute::UserPreference
            || parse_bounds.max_response_bytes() > streamer_bounds.max_frame_bytes()
        {
            return Err(SchwabMarketDataDoctorError::InvalidProbeContract);
        }
        let rest = exact_rest_plan(rest)?;
        let streamer = exact_streamer_plan(streamer)?;
        let probe_contract_digest = probe_contract_digest(
            &user_preference_request,
            &rest,
            &streamer,
            streamer_admission,
        )?;
        Ok(Self {
            rest_executor,
            user_preference_request,
            rest,
            streamer,
            streamer_connector,
            streamer_control_source,
            streamer_admission,
            streamer_bounds,
            parse_bounds,
            token_admission,
            telemetry,
            channel_capacity,
            probe_contract_digest,
            user_preference: Mutex::new(None),
            streamer_option_contract: Mutex::new(None),
        })
    }

    async fn execute_user_preference(
        &self,
        authority: &SchwabOAuthMarketAuthority,
        cancellation: CancellationToken,
        deadline: Instant,
    ) -> Result<SchwabMarketDoctorUserPreferenceOutcome, SchwabMarketDataDoctorError> {
        let token = bounded(authority.acquire(), &cancellation, deadline)
            .await?
            .map_err(map_token_error)?;
        let token_generation = token.generation().get();
        let credential_authority = token.credential_authority();
        let outcome = bounded(
            self.rest_executor.execute(
                &self.user_preference_request,
                &token,
                cancellation.child_token(),
            ),
            &cancellation,
            deadline,
        )
        .await?
        .map_err(map_transport_error)?;
        drop(token);
        match outcome {
            RestExecutionOutcome::AcceptedUserPreference(provider) => {
                let receipt = provider.receipt();
                if receipt.credential_authority() != credential_authority {
                    return Err(SchwabMarketDataDoctorError::AuthorityChanged);
                }
                let observed_at = timestamp_from_millis(receipt.received_at_unix_millis())?;
                let rate_observation = SchwabMarketDoctorRateObservation::try_new(
                    SchwabMarketDoctorProbeScope::UserPreference,
                    SchwabMarketDoctorProbeStatus::Http(receipt.status()),
                    retry_after(receipt)?,
                    observed_at,
                )?;
                let bootstrap = provider.bootstrap();
                let evidence = market_squawk_sources::SchwabUserPreferenceDoctorEvidence {
                    endpoint_contract_sha256: user_preference_endpoint_contract_sha256(),
                    request_sha256: digest(receipt.request_sha256()),
                    response_sha256: digest(receipt.body_sha256()),
                    status_code: receipt.status(),
                    response_bytes: receipt.body_bytes(),
                    received_at: observed_at,
                    latency_nanos: millis_to_nanos(receipt.latency_ms())?,
                    market_data_principal_sha256: digest(
                        bootstrap.value().market_data_principal_sha256(),
                    ),
                    // ParsedNative retains the exact provider body digest while secret-bearing
                    // bootstrap coordinates remain unobservable outside the adapter.
                    streamer_bootstrap_sha256: digest(bootstrap.raw_sha256()),
                    market_data_offer_sha256: market_data_offer_digest(
                        bootstrap.value().market_data_permission(),
                        bootstrap.value().level_two_permission(),
                    )?,
                };
                let available = SchwabMarketDoctorUserPreferenceAvailable::try_new(
                    token_generation,
                    evidence,
                    rate_observation,
                )?;
                *self.user_preference.lock().await = Some(RetainedUserPreference {
                    token_generation,
                    provider,
                });
                Ok(SchwabMarketDoctorUserPreferenceOutcome::Available(
                    available,
                ))
            }
            RestExecutionOutcome::UserPreferenceRejected(receipt) => {
                *self.user_preference.lock().await = None;
                setup_required(
                    token_generation,
                    credential_authority,
                    receipt,
                    b"provider-rejected",
                )
            }
            RestExecutionOutcome::InvalidUserPreference { receipt, error } => {
                *self.user_preference.lock().await = None;
                setup_required(
                    token_generation,
                    credential_authority,
                    receipt,
                    adapter_error_tag(&error),
                )
            }
            RestExecutionOutcome::Accepted(_)
            | RestExecutionOutcome::ProviderRejected(_)
            | RestExecutionOutcome::InvalidPayload { .. } => {
                Err(SchwabMarketDataDoctorError::InvalidProbeEvidence)
            }
        }
    }

    async fn execute_rest(
        &self,
        family: SchwabMarketDataFamily,
        authority: &SchwabOAuthMarketAuthority,
        sealer: &dyn SchwabMarketDoctorCaptureSealer,
        cancellation: CancellationToken,
        deadline: Instant,
    ) -> Result<SchwabMarketDoctorFamilyProbeEvidence, SchwabMarketDataDoctorError> {
        if family == SchwabMarketDataFamily::OptionChains {
            *self.streamer_option_contract.lock().await = None;
        }
        let probe = self
            .rest
            .get(&family)
            .ok_or(SchwabMarketDataDoctorError::InvalidProbeContract)?;
        let token = bounded(authority.acquire(), &cancellation, deadline)
            .await?
            .map_err(map_token_error)?;
        let token_generation = token.generation().get();
        let credential_authority = token.credential_authority();
        let outcome = bounded(
            self.rest_executor
                .execute(&probe.request, &token, cancellation.child_token()),
            &cancellation,
            deadline,
        )
        .await?
        .map_err(map_transport_error)?;
        drop(token);
        match outcome {
            RestExecutionOutcome::Accepted(response) => {
                let receipt = response.capture().receipt().clone();
                let accounting = response.accounting();
                if !rest_route_matches(family, receipt.route()) {
                    return Err(SchwabMarketDataDoctorError::InvalidProbeEvidence);
                }
                let full = accounting.returned > 0
                    && accounting.missing == 0
                    && accounting.unexpected == 0
                    && accounting.provider_records > 0;
                if full {
                    SchwabRestFamilyDoctorInput::try_new(
                        adapter_rest_family(family)
                            .ok_or(SchwabMarketDataDoctorError::InvalidProbeContract)?,
                        &response,
                    )
                    .map_err(|_| SchwabMarketDataDoctorError::InvalidProbeEvidence)?;
                }
                let quote_delay = quote_delay_qualification(family, &response)?;
                let pending = response
                    .into_pending_capture(probe.coordinates.clone(), Uuid::new_v4())
                    .map_err(map_transport_error)?;
                let (rejoin, request) = pending.into_sealing_parts();
                let sealed = sealer
                    .seal(request, cancellation.child_token(), deadline)
                    .await?;
                let sealed = rejoin.try_rejoin(sealed).map_err(map_transport_error)?;
                if family == SchwabMarketDataFamily::OptionChains {
                    let selected = sealed
                        .select_unexpired_option_contract()
                        .map_err(|_| SchwabMarketDataDoctorError::InvalidProbeEvidence)?
                        .ok_or(SchwabMarketDataDoctorError::InvalidProbeEvidence)?;
                    *self.streamer_option_contract.lock().await = Some(selected);
                }
                accepted_rest_evidence(
                    family,
                    token_generation,
                    credential_authority,
                    receipt,
                    accounting,
                    sealed,
                    quote_delay,
                )
            }
            RestExecutionOutcome::ProviderRejected(capture) => {
                rejected_rest_evidence(
                    family,
                    token_generation,
                    credential_authority,
                    probe.coordinates.clone(),
                    capture,
                    b"provider-rejected",
                    sealer,
                    cancellation,
                    deadline,
                )
                .await
            }
            RestExecutionOutcome::InvalidPayload { capture, error } => {
                rejected_rest_evidence(
                    family,
                    token_generation,
                    credential_authority,
                    probe.coordinates.clone(),
                    capture,
                    adapter_error_tag(&error),
                    sealer,
                    cancellation,
                    deadline,
                )
                .await
            }
            RestExecutionOutcome::AcceptedUserPreference(_)
            | RestExecutionOutcome::UserPreferenceRejected(_)
            | RestExecutionOutcome::InvalidUserPreference { .. } => {
                Err(SchwabMarketDataDoctorError::InvalidProbeEvidence)
            }
        }
    }

    async fn execute_streamer(
        &self,
        family: SchwabMarketDataFamily,
        authority: &SchwabOAuthMarketAuthority,
        sealer: &dyn SchwabMarketDoctorCaptureSealer,
        cancellation: CancellationToken,
        deadline: Instant,
    ) -> Result<SchwabMarketDoctorFamilyProbeEvidence, SchwabMarketDataDoctorError> {
        let probe = self
            .streamer
            .get(&family)
            .ok_or(SchwabMarketDataDoctorError::InvalidProbeContract)?;
        let service =
            streamer_service(family).ok_or(SchwabMarketDataDoctorError::InvalidProbeContract)?;
        let retained = self.user_preference.lock().await;
        let retained = retained
            .as_ref()
            .ok_or(SchwabMarketDataDoctorError::InvalidProbeEvidence)?;
        let current = bounded(authority.current_receipt(), &cancellation, deadline)
            .await?
            .map_err(|_| SchwabMarketDataDoctorError::AuthorityUnavailable)?;
        if retained.token_generation != current.generation().get()
            || retained.provider.receipt().credential_authority() != current.credential_authority()
        {
            return Err(SchwabMarketDataDoctorError::AuthorityChanged);
        }
        let mut executor = SchwabStreamerExecutor::try_new(
            Arc::clone(&self.streamer_connector),
            Arc::new(authority.clone()),
            Arc::clone(&self.streamer_control_source),
            self.streamer_admission,
            self.streamer_bounds,
            self.parse_bounds,
            self.token_admission,
            self.telemetry.clone(),
        )
        .map_err(map_transport_error)?;
        let subscription = match &probe.target {
            SchwabMarketDoctorStreamerTarget::Exact(subscription) => subscription.clone(),
            SchwabMarketDoctorStreamerTarget::ReturnedOptionContract { service, field_ids } => {
                let selected = self
                    .streamer_option_contract
                    .lock()
                    .await
                    .clone()
                    .ok_or(SchwabMarketDataDoctorError::InvalidProbeEvidence)?;
                StreamerSubscription::try_new(
                    *service,
                    vec![selected],
                    field_ids.iter().copied().collect(),
                    self.streamer_admission,
                )
                .map_err(|_| SchwabMarketDataDoctorError::InvalidProbeContract)?
            }
        };
        executor
            .replace_desired(subscription)
            .map_err(|_| SchwabMarketDataDoctorError::InvalidProbeContract)?;

        let run_cancellation = cancellation.child_token();
        let (sender, mut receiver) = mpsc::channel(self.channel_capacity.get());
        let mut sink = DoctorStreamerSink { sender };
        let run = executor.run(
            retained.provider.bootstrap().value(),
            &mut sink,
            run_cancellation.clone(),
        );
        tokio::pin!(run);
        let mut accumulator = None;
        loop {
            tokio::select! {
                biased;
                () = cancellation.cancelled() => {
                    run_cancellation.cancel();
                    return Err(SchwabMarketDataDoctorError::Cancelled);
                }
                () = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => {
                    run_cancellation.cancel();
                    return Err(SchwabMarketDataDoctorError::Deadline);
                }
                maybe_batch = receiver.recv() => {
                    let batch = maybe_batch.ok_or(SchwabMarketDataDoctorError::InvalidProbeEvidence)?;
                    let capture = self.seal_streamer_batch(
                        batch,
                        sealer,
                        cancellation.child_token(),
                        deadline,
                    ).await?;
                    if let Some(terminal) = observe_streamer_capture(
                        family,
                        service,
                        capture,
                        &mut accumulator,
                    )? {
                        run_cancellation.cancel();
                        let run_result = bounded(&mut run, &cancellation, deadline).await?;
                        match run_result {
                            Ok(_) => return Ok(terminal.evidence),
                            Err(SchwabTransportError::Adapter)
                                if terminal.provider_rejected => return Ok(terminal.evidence),
                            Err(error) => return Err(map_transport_error(error)),
                        }
                    }
                }
                result = &mut run => {
                    while let Ok(batch) = receiver.try_recv() {
                        let capture = self.seal_streamer_batch(
                            batch,
                            sealer,
                            cancellation.child_token(),
                            deadline,
                        ).await?;
                        if let Some(terminal) = observe_streamer_capture(
                            family,
                            service,
                            capture,
                            &mut accumulator,
                        )? {
                            return match result {
                                Ok(_) => Ok(terminal.evidence),
                                Err(SchwabTransportError::Adapter)
                                    if terminal.provider_rejected => Ok(terminal.evidence),
                                Err(error) => Err(map_transport_error(error)),
                            };
                        }
                    }
                    return Err(match result {
                        Ok(_) => SchwabMarketDataDoctorError::InvalidProbeEvidence,
                        Err(error) => map_transport_error(error),
                    });
                }
            }
        }
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "selected service, exact raw batch, sealer, and bounds remain explicit"
    )]
    async fn seal_streamer_batch(
        &self,
        batch: StreamerMicrobatch,
        sealer: &dyn SchwabMarketDoctorCaptureSealer,
        cancellation: CancellationToken,
        deadline: Instant,
    ) -> Result<SchwabSealedStreamerCapture, SchwabMarketDataDoctorError> {
        let event_ids = (0..batch.frames().len())
            .map(|_| Uuid::new_v4())
            .collect::<Vec<_>>();
        let (pending, request) = batch
            .into_pending_capture(event_ids, self.parse_bounds)
            .map_err(map_transport_error)?;
        let sealed = sealer.seal(request, cancellation, deadline).await?;
        let capture = pending.try_rejoin(sealed).map_err(map_transport_error)?;
        if capture.streamer_receipt().frame_count() == 0 {
            return Err(SchwabMarketDataDoctorError::InvalidProbeEvidence);
        }
        Ok(capture)
    }
}

impl SchwabMarketDoctorProbeExecutor for ProviderNativeSchwabMarketDoctorProbeExecutor {
    fn probe_contract_digest(&self) -> EvidenceDigest {
        self.probe_contract_digest
    }

    fn user_preference<'a>(
        &'a self,
        authority: &'a SchwabOAuthMarketAuthority,
        cancellation: CancellationToken,
        deadline: Instant,
    ) -> DoctorFuture<'a, SchwabMarketDoctorUserPreferenceOutcome> {
        Box::pin(self.execute_user_preference(authority, cancellation, deadline))
    }

    fn rest<'a>(
        &'a self,
        family: SchwabMarketDataFamily,
        authority: &'a SchwabOAuthMarketAuthority,
        sealer: &'a dyn SchwabMarketDoctorCaptureSealer,
        cancellation: CancellationToken,
        deadline: Instant,
    ) -> DoctorFuture<'a, SchwabMarketDoctorFamilyProbeEvidence> {
        Box::pin(self.execute_rest(family, authority, sealer, cancellation, deadline))
    }

    fn streamer<'a>(
        &'a self,
        family: SchwabMarketDataFamily,
        authority: &'a SchwabOAuthMarketAuthority,
        sealer: &'a dyn SchwabMarketDoctorCaptureSealer,
        cancellation: CancellationToken,
        deadline: Instant,
    ) -> DoctorFuture<'a, SchwabMarketDoctorFamilyProbeEvidence> {
        Box::pin(self.execute_streamer(family, authority, sealer, cancellation, deadline))
    }
}

impl fmt::Debug for ProviderNativeSchwabMarketDoctorProbeExecutor {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ProviderNativeSchwabMarketDoctorProbeExecutor")
            .field("rest_executor", &self.rest_executor)
            .field("user_preference_request", &self.user_preference_request)
            .field("rest_families", &self.rest.keys())
            .field("streamer_families", &self.streamer.keys())
            .field("streamer_connector", &self.streamer_connector)
            .field("streamer_control_source", &self.streamer_control_source)
            .field("streamer_admission", &self.streamer_admission)
            .field("streamer_bounds", &self.streamer_bounds)
            .field("parse_bounds", &self.parse_bounds)
            .field("token_admission", &self.token_admission)
            .field("channel_capacity", &self.channel_capacity)
            .field("probe_contract_digest", &self.probe_contract_digest)
            .field(
                "user_preference",
                &"[PROVIDER BOOTSTRAP RETAINED IN MEMORY]",
            )
            .field(
                "streamer_option_contract",
                &"[SELECTED FROM SEALED OPTION CHAIN]",
            )
            .finish()
    }
}

fn production_rest_probes(
    admission: RequestAdmission,
) -> Result<Vec<SchwabMarketDoctorRestProbe>, SchwabMarketDataDoctorError> {
    let equity = provider_identifier("SPY")?;
    let quotes = QuoteRequest::try_new(
        vec![equity.clone()],
        vec![QuoteField::Quote, QuoteField::Reference],
        None,
        admission,
    )
    .map_err(|_| SchwabMarketDataDoctorError::InvalidProbeContract)?;
    let history = PriceHistoryRequest::new(equity.clone())
        .period(PriceHistoryPeriodType::Month, NonZeroU16::MIN)
        .frequency(
            PriceHistoryFrequencyType::Daily,
            PriceHistoryFrequency::new(NonZeroU16::MIN),
        )
        .extended_hours(false)
        .previous_close(true)
        .build(admission)
        .map_err(|_| SchwabMarketDataDoctorError::InvalidProbeContract)?;
    let chains = ChainRequest::new(equity.clone())
        .strike_count(NonZeroU16::new(2).ok_or(SchwabMarketDataDoctorError::InvalidProbeContract)?)
        .include_underlying_quote(true)
        .build(admission)
        .map_err(|_| SchwabMarketDataDoctorError::InvalidProbeContract)?;
    let expirations = ExpirationChainRequest::new(equity.clone())
        .build(admission)
        .map_err(|_| SchwabMarketDataDoctorError::InvalidProbeContract)?;
    let movers = build_movers_request(
        provider_identifier("$SPX")?,
        Some(MoverSort::PercentChangeUp),
        Some(MoverFrequency::Five),
        admission,
    )
    .map_err(|_| SchwabMarketDataDoctorError::InvalidProbeContract)?;
    let hours = build_market_hours_request(vec![MarketId::Equity], None, admission)
        .map_err(|_| SchwabMarketDataDoctorError::InvalidProbeContract)?;
    let instruments =
        build_instrument_search_request(equity, InstrumentProjection::SymbolSearch, admission)
            .map_err(|_| SchwabMarketDataDoctorError::InvalidProbeContract)?;
    [
        (SchwabMarketDataFamily::Quotes, quotes.request().clone()),
        (SchwabMarketDataFamily::PriceHistory, history),
        (SchwabMarketDataFamily::OptionChains, chains),
        (SchwabMarketDataFamily::ExpirationChains, expirations),
        (SchwabMarketDataFamily::Movers, movers),
        (SchwabMarketDataFamily::MarketHours, hours),
        (SchwabMarketDataFamily::Instruments, instruments),
    ]
    .into_iter()
    .map(|(family, request)| {
        SchwabMarketDoctorRestProbe::try_new(family, request, production_rest_coordinates(family)?)
    })
    .collect()
}

fn production_streamer_probes(
    admission: StreamerAdmission,
) -> Result<Vec<SchwabMarketDoctorStreamerProbe>, SchwabMarketDataDoctorError> {
    STREAMER_FAMILIES
        .into_iter()
        .map(|family| {
            let service = streamer_service(family)
                .ok_or(SchwabMarketDataDoctorError::InvalidProbeContract)?;
            let fields = vec![0, 1, 2, 3, 4];
            if matches!(
                service,
                MarketDataService::LevelOneOptions | MarketDataService::OptionsBook
            ) {
                return SchwabMarketDoctorStreamerProbe::try_returned_option_contract(
                    family, fields, admission,
                );
            }
            let subscription = StreamerSubscription::try_new(
                service,
                vec![provider_identifier(
                    production_streamer_key(service)
                        .ok_or(SchwabMarketDataDoctorError::InvalidProbeContract)?,
                )?],
                fields,
                admission,
            )
            .map_err(|_| SchwabMarketDataDoctorError::InvalidProbeContract)?;
            SchwabMarketDoctorStreamerProbe::try_new(family, subscription)
        })
        .collect()
}

const fn production_streamer_key(service: MarketDataService) -> Option<&'static str> {
    Some(match service {
        MarketDataService::LevelOneEquities
        | MarketDataService::NyseBook
        | MarketDataService::NasdaqBook
        | MarketDataService::ChartEquity => "SPY",
        MarketDataService::LevelOneFutures | MarketDataService::ChartFutures => "/ES",
        MarketDataService::LevelOneFuturesOptions => "/ES",
        MarketDataService::LevelOneForex => "EUR/USD",
        MarketDataService::ScreenerEquity => "$SPX",
        MarketDataService::ScreenerOption => "SPY",
        MarketDataService::LevelOneOptions | MarketDataService::OptionsBook => {
            return None;
        }
    })
}

fn production_rest_coordinates(
    family: SchwabMarketDataFamily,
) -> Result<SchwabCaptureCoordinates, SchwabMarketDataDoctorError> {
    SchwabCaptureCoordinates::try_new(
        source_id(PRODUCTION_SOURCE_ID)?,
        metadata_revision(PRODUCTION_METADATA_REVISION)?,
        identifier(&format!("schwab-market-doctor-rest-{}", family_tag(family)))?,
        Uuid::new_v4(),
    )
    .map_err(|_| SchwabMarketDataDoctorError::InvalidProbeContract)
}

fn source_id(value: &str) -> Result<SourceId, SchwabMarketDataDoctorError> {
    SourceId::try_from(value).map_err(|_| SchwabMarketDataDoctorError::InvalidProbeContract)
}

fn identifier(value: &str) -> Result<SourceIdentifier, SchwabMarketDataDoctorError> {
    SourceIdentifier::try_from(value).map_err(|_| SchwabMarketDataDoctorError::InvalidProbeContract)
}

fn metadata_revision(value: &str) -> Result<MetadataRevision, SchwabMarketDataDoctorError> {
    Ok(MetadataRevision::new(identifier(value)?))
}

fn provider_identifier(value: &str) -> Result<ProviderIdentifier, SchwabMarketDataDoctorError> {
    ProviderIdentifier::try_new(value)
        .map_err(|_| SchwabMarketDataDoctorError::InvalidProbeContract)
}

fn nonzero(value: usize) -> Result<NonZeroUsize, SchwabMarketDataDoctorError> {
    NonZeroUsize::new(value).ok_or(SchwabMarketDataDoctorError::InvalidProbeContract)
}

fn encode_production_control_state(generation: u64) -> Vec<u8> {
    let mut payload = Vec::with_capacity(PRODUCTION_CONTROL_STATE_DOMAIN.len() + 8);
    payload.extend_from_slice(PRODUCTION_CONTROL_STATE_DOMAIN);
    payload.extend_from_slice(&generation.to_be_bytes());
    payload
}

fn decode_production_control_state(payload: &[u8]) -> Result<u64, SchwabMarketDataDoctorError> {
    let generation = payload
        .strip_prefix(PRODUCTION_CONTROL_STATE_DOMAIN)
        .and_then(|generation| <[u8; 8]>::try_from(generation).ok())
        .map(u64::from_be_bytes)
        .filter(|generation| *generation > 0)
        .ok_or(SchwabMarketDataDoctorError::InvalidProbeContract)?;
    Ok(generation)
}

struct DoctorStreamerSink {
    sender: mpsc::Sender<StreamerMicrobatch>,
}

struct TerminalStreamerProbe {
    evidence: SchwabMarketDoctorFamilyProbeEvidence,
    provider_rejected: bool,
}

impl StreamerCaptureSink for DoctorStreamerSink {
    fn try_publish(
        &mut self,
        microbatch: StreamerMicrobatch,
    ) -> Result<(), StreamerCaptureSinkError> {
        self.sender
            .try_send(microbatch)
            .map_err(|error| match error {
                mpsc::error::TrySendError::Full(_) => StreamerCaptureSinkError::Saturated,
                mpsc::error::TrySendError::Closed(_) => StreamerCaptureSinkError::Closed,
            })
    }
}

fn exact_rest_plan(
    probes: Vec<SchwabMarketDoctorRestProbe>,
) -> Result<
    BTreeMap<SchwabMarketDataFamily, SchwabMarketDoctorRestProbe>,
    SchwabMarketDataDoctorError,
> {
    let mut plan = BTreeMap::new();
    for probe in probes {
        let family = probe.family;
        if !REST_FAMILIES.contains(&family) || plan.insert(family, probe).is_some() {
            return Err(SchwabMarketDataDoctorError::InvalidProbeContract);
        }
    }
    if plan.len() != REST_FAMILIES.len()
        || REST_FAMILIES
            .iter()
            .any(|family| !plan.contains_key(family))
    {
        return Err(SchwabMarketDataDoctorError::InvalidProbeContract);
    }
    Ok(plan)
}

fn exact_streamer_plan(
    probes: Vec<SchwabMarketDoctorStreamerProbe>,
) -> Result<
    BTreeMap<SchwabMarketDataFamily, SchwabMarketDoctorStreamerProbe>,
    SchwabMarketDataDoctorError,
> {
    let mut plan = BTreeMap::new();
    for probe in probes {
        let family = probe.family;
        if !STREAMER_FAMILIES.contains(&family) || plan.insert(family, probe).is_some() {
            return Err(SchwabMarketDataDoctorError::InvalidProbeContract);
        }
    }
    if plan.len() != STREAMER_FAMILIES.len()
        || STREAMER_FAMILIES
            .iter()
            .any(|family| !plan.contains_key(family))
    {
        return Err(SchwabMarketDataDoctorError::InvalidProbeContract);
    }
    Ok(plan)
}

fn probe_contract_digest(
    preference: &ReadOnlyRequest,
    rest: &BTreeMap<SchwabMarketDataFamily, SchwabMarketDoctorRestProbe>,
    streamer: &BTreeMap<SchwabMarketDataFamily, SchwabMarketDoctorStreamerProbe>,
    admission: StreamerAdmission,
) -> Result<EvidenceDigest, SchwabMarketDataDoctorError> {
    let mut hasher = Sha256::new();
    hasher.update(PROBE_CONTRACT_DOMAIN);
    hash_text(&mut hasher, preference.url())?;
    hasher.update(
        u64::try_from(preference.requested_items())
            .map_err(|_| SchwabMarketDataDoctorError::ResourceLimit)?
            .to_be_bytes(),
    );
    for family in REST_FAMILIES {
        let probe = rest
            .get(&family)
            .ok_or(SchwabMarketDataDoctorError::InvalidProbeContract)?;
        hasher.update([family_tag(family)]);
        hash_text(&mut hasher, probe.request.url())?;
        hasher.update(
            u64::try_from(probe.request.requested_items())
                .map_err(|_| SchwabMarketDataDoctorError::ResourceLimit)?
                .to_be_bytes(),
        );
    }
    hasher.update(
        u64::try_from(admission.max_services())
            .map_err(|_| SchwabMarketDataDoctorError::ResourceLimit)?
            .to_be_bytes(),
    );
    hasher.update(
        u64::try_from(admission.max_fields_per_service())
            .map_err(|_| SchwabMarketDataDoctorError::ResourceLimit)?
            .to_be_bytes(),
    );
    for family in STREAMER_FAMILIES {
        let probe = streamer
            .get(&family)
            .ok_or(SchwabMarketDataDoctorError::InvalidProbeContract)?;
        hasher.update([family_tag(family)]);
        hash_text(&mut hasher, probe.service().as_str())?;
        match &probe.target {
            SchwabMarketDoctorStreamerTarget::Exact(subscription) => {
                hasher.update([0]);
                for key in subscription.keys() {
                    hash_text(&mut hasher, key.as_str())?;
                }
            }
            SchwabMarketDoctorStreamerTarget::ReturnedOptionContract { .. } => {
                hasher.update([1]);
                hasher.update(PRODUCTION_OPTION_STREAMER_SELECTION_DOMAIN);
            }
        }
        for field in probe.field_ids() {
            hasher.update(field.to_be_bytes());
        }
    }
    Ok(digest(hasher.finalize().into()))
}

fn setup_required(
    token_generation: u64,
    credential_authority: SchwabCredentialAuthorityBinding,
    receipt: RawRestResponseReceipt,
    reason: &[u8],
) -> Result<SchwabMarketDoctorUserPreferenceOutcome, SchwabMarketDataDoctorError> {
    if receipt.credential_authority() != credential_authority {
        return Err(SchwabMarketDataDoctorError::AuthorityChanged);
    }
    let observed_at = timestamp_from_millis(receipt.received_at_unix_millis())?;
    let rate_observation = SchwabMarketDoctorRateObservation::try_new(
        SchwabMarketDoctorProbeScope::UserPreference,
        SchwabMarketDoctorProbeStatus::Http(receipt.status()),
        retry_after(&receipt)?,
        observed_at,
    )?;
    let evidence = SchwabMarketDoctorSetupRequiredEvidence::try_new(
        token_generation,
        digest(receipt.request_sha256()),
        digest(receipt.body_sha256()),
        receipt.status(),
        receipt.body_bytes(),
        observed_at,
        domain_digest(USER_PREFERENCE_REASON_DOMAIN, reason)?,
        rate_observation,
    )?;
    Ok(SchwabMarketDoctorUserPreferenceOutcome::SetupRequired(
        evidence,
    ))
}

fn quote_delay_qualification(
    family: SchwabMarketDataFamily,
    response: &market_squawk_adapter_schwab::ExecutedRestResponse,
) -> Result<Option<CoverageDelay>, SchwabMarketDataDoctorError> {
    if family != SchwabMarketDataFamily::Quotes {
        return Ok(None);
    }
    let SchwabRestPayload::Quotes(parsed) = response.payload() else {
        return Err(SchwabMarketDataDoctorError::InvalidProbeEvidence);
    };
    let [quote] = parsed.value().quotes() else {
        return Ok(None);
    };
    Ok(match quote.realtime() {
        NativeField::Value(true) => Some(CoverageDelay::RealTime),
        NativeField::Value(false) | NativeField::Absent | NativeField::Null => None,
    })
}

fn accepted_rest_evidence(
    family: SchwabMarketDataFamily,
    token_generation: u64,
    credential_authority: SchwabCredentialAuthorityBinding,
    receipt: RawRestResponseReceipt,
    accounting: RestItemAccounting,
    sealed: SchwabSealedRestResponse,
    quote_delay: Option<CoverageDelay>,
) -> Result<SchwabMarketDoctorFamilyProbeEvidence, SchwabMarketDataDoctorError> {
    if !sealed_rest_family_matches(family, sealed.family())
        || sealed.receipt() != &receipt
        || sealed.accounting() != accounting
    {
        return Err(SchwabMarketDataDoctorError::InvalidProbeEvidence);
    }
    let disposition = if accounting.returned == 0 || accounting.provider_records == 0 {
        RuntimeCapabilityDisposition::Unavailable
    } else if accounting.missing > 0 || accounting.unexpected > 0 {
        RuntimeCapabilityDisposition::Degraded
    } else {
        RuntimeCapabilityDisposition::Available
    };
    let limitation = matches!(disposition, RuntimeCapabilityDisposition::Degraded)
        .then(|| rest_limitation_digest(accounting))
        .transpose()?;
    rest_probe_evidence(
        family,
        disposition,
        token_generation,
        credential_authority,
        &receipt,
        accounting,
        sealed.persisted_receipt().receipt_digest(),
        quote_delay,
        limitation,
        b"typed-accepted",
    )
}

#[allow(
    clippy::too_many_arguments,
    reason = "the raw rejection, capture coordinates, sealer, and bounds stay explicit"
)]
async fn rejected_rest_evidence(
    family: SchwabMarketDataFamily,
    token_generation: u64,
    credential_authority: SchwabCredentialAuthorityBinding,
    coordinates: SchwabCaptureCoordinates,
    capture: market_squawk_adapter_schwab::CapturedRestResponse,
    reason: &[u8],
    sealer: &dyn SchwabMarketDoctorCaptureSealer,
    cancellation: CancellationToken,
    deadline: Instant,
) -> Result<SchwabMarketDoctorFamilyProbeEvidence, SchwabMarketDataDoctorError> {
    let receipt = capture.receipt().clone();
    let accounting = capture.accounting();
    if !rest_route_matches(family, receipt.route()) {
        return Err(SchwabMarketDataDoctorError::InvalidProbeEvidence);
    }
    let pending = capture
        .into_pending_capture(coordinates, Uuid::new_v4())
        .map_err(map_transport_error)?;
    let (rejoin, request) = pending.into_sealing_parts();
    let sealed = sealer.seal(request, cancellation, deadline).await?;
    let sealed = rejoin.try_rejoin(sealed).map_err(map_transport_error)?;
    validate_raw_rest_rejoin(&sealed, &receipt, accounting)?;
    rest_probe_evidence(
        family,
        RuntimeCapabilityDisposition::Unavailable,
        token_generation,
        credential_authority,
        &receipt,
        accounting,
        sealed.persisted_receipt().receipt_digest(),
        None,
        None,
        reason,
    )
}

fn validate_raw_rest_rejoin(
    sealed: &SchwabSealedRawRestCapture,
    receipt: &RawRestResponseReceipt,
    accounting: RestItemAccounting,
) -> Result<(), SchwabMarketDataDoctorError> {
    if sealed.receipt() != receipt || sealed.accounting() != accounting {
        return Err(SchwabMarketDataDoctorError::InvalidProbeEvidence);
    }
    Ok(())
}

#[allow(
    clippy::too_many_arguments,
    reason = "every exact REST observation field is bound before construction"
)]
fn rest_probe_evidence(
    family: SchwabMarketDataFamily,
    disposition: RuntimeCapabilityDisposition,
    token_generation: u64,
    credential_authority: SchwabCredentialAuthorityBinding,
    receipt: &RawRestResponseReceipt,
    accounting: RestItemAccounting,
    sealed_capture_receipt_sha256: EvidenceDigest,
    quote_delay: Option<CoverageDelay>,
    declared_limitation_sha256: Option<EvidenceDigest>,
    outcome: &[u8],
) -> Result<SchwabMarketDoctorFamilyProbeEvidence, SchwabMarketDataDoctorError> {
    if token_generation != receipt.token_generation().get()
        || credential_authority != receipt.credential_authority()
    {
        return Err(SchwabMarketDataDoctorError::AuthorityChanged);
    }
    let observed_at = timestamp_from_millis(receipt.received_at_unix_millis())?;
    let status = SchwabMarketDoctorProbeStatus::Http(receipt.status());
    let rate_observation = SchwabMarketDoctorRateObservation::try_new(
        SchwabMarketDoctorProbeScope::Rest(family),
        status,
        retry_after(receipt)?,
        observed_at,
    )?;
    let service_observation_sha256 = rest_observation_digest(
        family,
        receipt,
        accounting,
        sealed_capture_receipt_sha256,
        outcome,
    )?;
    SchwabMarketDoctorFamilyProbeEvidence::try_new(SchwabMarketDoctorFamilyProbeInput {
        family,
        disposition,
        token_generation,
        status,
        request_sha256: digest(receipt.request_sha256()),
        response_sha256: digest(receipt.body_sha256()),
        raw_payload_sha256: digest(receipt.body_sha256()),
        sealed_capture_receipt_sha256,
        service_observation_sha256,
        requested_items: accounting.requested,
        returned_items: accounting.returned,
        missing_items: accounting.missing,
        unexpected_items: accounting.unexpected,
        provider_records: accounting.provider_records,
        response_bytes: receipt.body_bytes(),
        latency_nanos: millis_to_nanos(receipt.latency_ms())?,
        observed_at,
        service: None,
        quote_delay,
        declared_limitation_sha256,
        rate_observation,
    })
}

fn observe_streamer_capture(
    family: SchwabMarketDataFamily,
    service: MarketDataService,
    capture: SchwabSealedStreamerCapture,
    accumulator: &mut Option<SchwabStreamerFamilyDoctorAccumulator>,
) -> Result<Option<TerminalStreamerProbe>, SchwabMarketDataDoctorError> {
    let responses = capture
        .service_responses()
        .iter()
        .filter(|response| response.service() == service)
        .cloned()
        .collect::<Vec<_>>();
    match responses.as_slice() {
        [] => {
            let mut retained = accumulator
                .take()
                .ok_or(SchwabMarketDataDoctorError::InvalidProbeEvidence)?;
            retained
                .try_push_data_capture(capture)
                .map_err(|rejection| map_vertical_error(rejection.error()))?;
            let handoff = retained.try_finish().map_err(map_vertical_error)?;
            successful_streamer_handoff_evidence(family, service, &handoff).map(Some)
        }
        [response] if response.status_code() == 0 => {
            if accumulator.is_some() {
                return Err(SchwabMarketDataDoctorError::InvalidProbeEvidence);
            }
            let retained =
                SchwabStreamerFamilyDoctorAccumulator::try_from_ack_capture(service, capture)
                    .map_err(|rejection| map_vertical_error(rejection.error()))?;
            *accumulator = Some(retained);
            Ok(None)
        }
        [_response] => {
            if accumulator.is_some() {
                return Err(SchwabMarketDataDoctorError::InvalidProbeEvidence);
            }
            rejected_streamer_capture_evidence(family, service, &capture).map(Some)
        }
        _ => Err(SchwabMarketDataDoctorError::InvalidProbeEvidence),
    }
}

fn successful_streamer_handoff_evidence(
    family: SchwabMarketDataFamily,
    service: MarketDataService,
    handoff: &SchwabStreamerFamilyDoctorHandoff,
) -> Result<TerminalStreamerProbe, SchwabMarketDataDoctorError> {
    let input = handoff.family_input();
    let response = input.service_response();
    if handoff.service() != service
        || response.service() != service
        || response.status_code() != 0
        || response.round_trip_latency_ms().is_none()
        || handoff.capture_count() < 2
        || input.provider_records() == 0
    {
        return Err(SchwabMarketDataDoctorError::InvalidProbeEvidence);
    }
    let last_capture = handoff
        .capture_receipt(handoff.capture_count() - 1)
        .ok_or(SchwabMarketDataDoctorError::InvalidProbeEvidence)?;
    let observed_at = last_capture
        .capture()
        .frames()
        .last()
        .map(|frame| frame.received_at())
        .ok_or(SchwabMarketDataDoctorError::InvalidProbeEvidence)?;
    let status = SchwabMarketDoctorProbeStatus::Streamer(0);
    let rate_observation = SchwabMarketDoctorRateObservation::try_new(
        SchwabMarketDoctorProbeScope::Streamer(family),
        status,
        None,
        observed_at,
    )?;
    let capture_set_sha256 = handoff.capture_set_sha256();
    let evidence =
        SchwabMarketDoctorFamilyProbeEvidence::try_new(SchwabMarketDoctorFamilyProbeInput {
            family,
            disposition: RuntimeCapabilityDisposition::Available,
            token_generation: handoff.token_generation().get(),
            status,
            request_sha256: handoff.request_payload_sha256(),
            response_sha256: response.payload_digest(),
            raw_payload_sha256: capture_set_sha256,
            sealed_capture_receipt_sha256: capture_set_sha256,
            service_observation_sha256: capture_set_sha256,
            requested_items: 1,
            returned_items: 1,
            missing_items: 0,
            unexpected_items: 0,
            provider_records: input.provider_records(),
            response_bytes: handoff.total_payload_bytes(),
            latency_nanos: millis_to_nanos(
                response
                    .round_trip_latency_ms()
                    .ok_or(SchwabMarketDataDoctorError::InvalidProbeEvidence)?,
            )?,
            observed_at,
            service: Some(service.as_str().into()),
            quote_delay: None,
            declared_limitation_sha256: None,
            rate_observation,
        })?;
    Ok(TerminalStreamerProbe {
        evidence,
        provider_rejected: false,
    })
}

fn rejected_streamer_capture_evidence(
    family: SchwabMarketDataFamily,
    service: MarketDataService,
    capture: &SchwabSealedStreamerCapture,
) -> Result<TerminalStreamerProbe, SchwabMarketDataDoctorError> {
    let [response] = capture.service_responses() else {
        return Err(SchwabMarketDataDoctorError::InvalidProbeEvidence);
    };
    let receipt = capture.streamer_receipt();
    if response.service() != service
        || response.status_code() == 0
        || response.request_payload_sha256().is_none()
        || response.sealed_capture_receipt_sha256() != capture.persisted_receipt().receipt_digest()
        || response.round_trip_latency_ms().is_none()
    {
        return Err(SchwabMarketDataDoctorError::InvalidProbeEvidence);
    }
    let observed_at = timestamp_from_millis(response.received_at_unix_millis())?;
    let status = SchwabMarketDoctorProbeStatus::Streamer(response.status_code());
    let rate_observation = SchwabMarketDoctorRateObservation::try_new(
        SchwabMarketDoctorProbeScope::Streamer(family),
        status,
        None,
        observed_at,
    )?;
    let evidence =
        SchwabMarketDoctorFamilyProbeEvidence::try_new(SchwabMarketDoctorFamilyProbeInput {
            family,
            disposition: RuntimeCapabilityDisposition::Unavailable,
            token_generation: receipt.token_generation().get(),
            status,
            request_sha256: response
                .request_payload_sha256()
                .ok_or(SchwabMarketDataDoctorError::InvalidProbeEvidence)?,
            response_sha256: response.payload_digest(),
            raw_payload_sha256: digest(receipt.content_sha256()),
            sealed_capture_receipt_sha256: response.sealed_capture_receipt_sha256(),
            service_observation_sha256: response.observation_sha256(),
            requested_items: 1,
            returned_items: 0,
            missing_items: 1,
            unexpected_items: 0,
            provider_records: 0,
            response_bytes: receipt.payload_bytes(),
            latency_nanos: millis_to_nanos(
                response
                    .round_trip_latency_ms()
                    .ok_or(SchwabMarketDataDoctorError::InvalidProbeEvidence)?,
            )?,
            observed_at,
            service: Some(service.as_str().into()),
            quote_delay: None,
            declared_limitation_sha256: None,
            rate_observation,
        })?;
    Ok(TerminalStreamerProbe {
        evidence,
        provider_rejected: true,
    })
}

fn map_vertical_error(error: SchwabVerticalError) -> SchwabMarketDataDoctorError {
    match error {
        SchwabVerticalError::ResourceLimit | SchwabVerticalError::Overflow => {
            SchwabMarketDataDoctorError::ResourceLimit
        }
        SchwabVerticalError::InvalidCapabilityEvidence => {
            SchwabMarketDataDoctorError::InvalidProbeEvidence
        }
    }
}

fn rest_observation_digest(
    family: SchwabMarketDataFamily,
    receipt: &RawRestResponseReceipt,
    accounting: RestItemAccounting,
    sealed_capture_receipt_sha256: EvidenceDigest,
    outcome: &[u8],
) -> Result<EvidenceDigest, SchwabMarketDataDoctorError> {
    let mut hasher = Sha256::new();
    hasher.update(REST_OBSERVATION_DOMAIN);
    hasher.update([family_tag(family)]);
    hasher.update(receipt.token_generation().get().to_be_bytes());
    hasher.update(
        receipt
            .credential_authority()
            .application_credential_generation()
            .get()
            .to_be_bytes(),
    );
    hasher.update(
        receipt
            .credential_authority()
            .application_credential_reference_sha256()
            .bytes(),
    );
    hasher.update(receipt.request_sha256());
    hasher.update(receipt.status().to_be_bytes());
    hasher.update(receipt.received_at_unix_millis().to_be_bytes());
    hasher.update(receipt.body_bytes().to_be_bytes());
    hasher.update(receipt.body_sha256());
    hasher.update(receipt.latency_ms().to_be_bytes());
    for value in [
        accounting.requested,
        accounting.returned,
        accounting.missing,
        accounting.unexpected,
        accounting.provider_records,
    ] {
        hasher.update(value.to_be_bytes());
    }
    hasher.update(sealed_capture_receipt_sha256.bytes());
    hash_bytes(&mut hasher, outcome)?;
    Ok(digest(hasher.finalize().into()))
}

fn rest_limitation_digest(
    accounting: RestItemAccounting,
) -> Result<EvidenceDigest, SchwabMarketDataDoctorError> {
    let mut hasher = Sha256::new();
    hasher.update(REST_LIMITATION_DOMAIN);
    for value in [
        accounting.requested,
        accounting.returned,
        accounting.missing,
        accounting.unexpected,
        accounting.provider_records,
    ] {
        hasher.update(value.to_be_bytes());
    }
    Ok(digest(hasher.finalize().into()))
}

fn market_data_offer_digest(
    permission: Option<&str>,
    level_two: Option<bool>,
) -> Result<Option<EvidenceDigest>, SchwabMarketDataDoctorError> {
    if permission.is_none() && level_two.is_none() {
        return Ok(None);
    }
    let mut hasher = Sha256::new();
    hasher.update(USER_PREFERENCE_OFFER_DOMAIN);
    match permission {
        Some(value) => {
            hasher.update([1]);
            hash_text(&mut hasher, value)?;
        }
        None => hasher.update([0]),
    }
    match level_two {
        Some(value) => hasher.update([1, u8::from(value)]),
        None => hasher.update([0]),
    }
    Ok(Some(digest(hasher.finalize().into())))
}

fn retry_after(
    receipt: &RawRestResponseReceipt,
) -> Result<Option<Vec<u8>>, SchwabMarketDataDoctorError> {
    let mut values = receipt
        .headers()
        .iter()
        .filter(|header| header.name() == "retry-after")
        .map(|header| header.value().to_vec());
    let value = values.next();
    if values.next().is_some() {
        return Err(SchwabMarketDataDoctorError::InvalidProbeEvidence);
    }
    Ok(value)
}

fn rest_route_matches(family: SchwabMarketDataFamily, route: ReadOnlyRoute) -> bool {
    matches!(
        (family, route),
        (
            SchwabMarketDataFamily::Quotes,
            ReadOnlyRoute::Quotes | ReadOnlyRoute::SingleQuote
        ) | (
            SchwabMarketDataFamily::PriceHistory,
            ReadOnlyRoute::PriceHistory
        ) | (SchwabMarketDataFamily::OptionChains, ReadOnlyRoute::Chains)
            | (
                SchwabMarketDataFamily::ExpirationChains,
                ReadOnlyRoute::ExpirationChain
            )
            | (SchwabMarketDataFamily::Movers, ReadOnlyRoute::Movers)
            | (
                SchwabMarketDataFamily::MarketHours,
                ReadOnlyRoute::Markets | ReadOnlyRoute::SingleMarket
            )
            | (
                SchwabMarketDataFamily::Instruments,
                ReadOnlyRoute::Instruments | ReadOnlyRoute::InstrumentByCusip
            )
    )
}

fn sealed_rest_family_matches(family: SchwabMarketDataFamily, sealed: SchwabRestFamily) -> bool {
    matches!(
        (family, sealed),
        (SchwabMarketDataFamily::Quotes, SchwabRestFamily::Quotes)
            | (
                SchwabMarketDataFamily::PriceHistory,
                SchwabRestFamily::DailyPriceHistory
            )
            | (
                SchwabMarketDataFamily::OptionChains,
                SchwabRestFamily::OptionChain
            )
            | (
                SchwabMarketDataFamily::ExpirationChains,
                SchwabRestFamily::ExpirationChain
            )
            | (SchwabMarketDataFamily::Movers, SchwabRestFamily::Movers)
            | (
                SchwabMarketDataFamily::MarketHours,
                SchwabRestFamily::MarketHours
            )
            | (
                SchwabMarketDataFamily::Instruments,
                SchwabRestFamily::Instruments
            )
    )
}

const fn family_tag(family: SchwabMarketDataFamily) -> u8 {
    match family {
        SchwabMarketDataFamily::Quotes => 1,
        SchwabMarketDataFamily::PriceHistory => 2,
        SchwabMarketDataFamily::OptionChains => 3,
        SchwabMarketDataFamily::ExpirationChains => 4,
        SchwabMarketDataFamily::Movers => 5,
        SchwabMarketDataFamily::MarketHours => 6,
        SchwabMarketDataFamily::Instruments => 7,
        SchwabMarketDataFamily::LevelOneEquities => 8,
        SchwabMarketDataFamily::LevelOneOptions => 9,
        SchwabMarketDataFamily::LevelOneFutures => 10,
        SchwabMarketDataFamily::LevelOneFuturesOptions => 11,
        SchwabMarketDataFamily::LevelOneForex => 12,
        SchwabMarketDataFamily::NyseBook => 13,
        SchwabMarketDataFamily::NasdaqBook => 14,
        SchwabMarketDataFamily::OptionsBook => 15,
        SchwabMarketDataFamily::ChartEquity => 16,
        SchwabMarketDataFamily::ChartFutures => 17,
        SchwabMarketDataFamily::ScreenerEquity => 18,
        SchwabMarketDataFamily::ScreenerOption => 19,
    }
}

fn adapter_error_tag(error: &SchwabAdapterError) -> &'static [u8] {
    match error {
        SchwabAdapterError::InvalidInput => b"invalid-input",
        SchwabAdapterError::RouteNotAllowed => b"route-not-allowed",
        SchwabAdapterError::RequestNotAdmitted => b"request-not-admitted",
        SchwabAdapterError::BoundsExceeded => b"bounds-exceeded",
        SchwabAdapterError::SchemaViolation => b"schema-violation",
        SchwabAdapterError::ArithmeticOverflow => b"arithmetic-overflow",
        SchwabAdapterError::InvalidCallback => b"invalid-callback",
        SchwabAdapterError::InvalidTokenLifecycle => b"invalid-token-lifecycle",
        SchwabAdapterError::InvalidStreamerState => b"invalid-streamer-state",
        SchwabAdapterError::StreamerRejected => b"streamer-rejected",
    }
}

async fn bounded<T>(
    operation: impl Future<Output = T>,
    cancellation: &CancellationToken,
    deadline: Instant,
) -> Result<T, SchwabMarketDataDoctorError> {
    if cancellation.is_cancelled() {
        return Err(SchwabMarketDataDoctorError::Cancelled);
    }
    if Instant::now() >= deadline {
        return Err(SchwabMarketDataDoctorError::Deadline);
    }
    tokio::select! {
        biased;
        () = cancellation.cancelled() => Err(SchwabMarketDataDoctorError::Cancelled),
        () = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => {
            Err(SchwabMarketDataDoctorError::Deadline)
        }
        output = operation => Ok(output),
    }
}

fn map_token_error(error: TokenAuthorityError) -> SchwabMarketDataDoctorError {
    match error {
        TokenAuthorityError::Unavailable | TokenAuthorityError::ReauthorizationRequired => {
            SchwabMarketDataDoctorError::AuthorityUnavailable
        }
    }
}

fn map_transport_error(error: SchwabTransportError) -> SchwabMarketDataDoctorError {
    match error {
        SchwabTransportError::Cancelled => SchwabMarketDataDoctorError::Cancelled,
        SchwabTransportError::Deadline => SchwabMarketDataDoctorError::Deadline,
        SchwabTransportError::TokenAuthorityUnavailable
        | SchwabTransportError::TokenRefreshRequired
        | SchwabTransportError::InvalidToken => SchwabMarketDataDoctorError::AuthorityUnavailable,
        SchwabTransportError::InvalidConfiguration
        | SchwabTransportError::Network
        | SchwabTransportError::Protocol
        | SchwabTransportError::PayloadTooLarge
        | SchwabTransportError::HeaderBoundsExceeded
        | SchwabTransportError::CaptureRejected
        | SchwabTransportError::CaptureMaterial
        | SchwabTransportError::ReconnectExhausted
        | SchwabTransportError::ResynchronizationRequired
        | SchwabTransportError::TelemetryUnavailable
        | SchwabTransportError::Overflow
        | SchwabTransportError::Adapter => SchwabMarketDataDoctorError::InvalidProbeEvidence,
    }
}

fn timestamp_from_millis(millis: u64) -> Result<Timestamp, SchwabMarketDataDoctorError> {
    let nanos = millis
        .checked_mul(1_000_000)
        .and_then(|value| i64::try_from(value).ok())
        .ok_or(SchwabMarketDataDoctorError::Clock)?;
    Ok(Timestamp::from_unix_nanos(nanos))
}

fn millis_to_nanos(millis: u64) -> Result<u64, SchwabMarketDataDoctorError> {
    millis
        .checked_mul(1_000_000)
        .ok_or(SchwabMarketDataDoctorError::Clock)
}

fn digest(bytes: [u8; 32]) -> EvidenceDigest {
    EvidenceDigest::new(DigestAlgorithm::Sha256, bytes)
}

fn domain_digest(
    domain: &[u8],
    value: &[u8],
) -> Result<EvidenceDigest, SchwabMarketDataDoctorError> {
    let mut hasher = Sha256::new();
    hasher.update(domain);
    hash_bytes(&mut hasher, value)?;
    Ok(digest(hasher.finalize().into()))
}

fn hash_text(hasher: &mut Sha256, value: &str) -> Result<(), SchwabMarketDataDoctorError> {
    hash_bytes(hasher, value.as_bytes())
}

fn hash_bytes(hasher: &mut Sha256, value: &[u8]) -> Result<(), SchwabMarketDataDoctorError> {
    hasher.update(
        u64::try_from(value.len())
            .map_err(|_| SchwabMarketDataDoctorError::ResourceLimit)?
            .to_be_bytes(),
    );
    hasher.update(value);
    Ok(())
}
