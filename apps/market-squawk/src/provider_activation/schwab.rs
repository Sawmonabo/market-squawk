//! Exact read-only Schwab OAuth and doctor activation for one account market runtime.

use std::{
    collections::BTreeSet,
    future::Future,
    num::NonZeroUsize,
    pin::Pin,
    sync::{Arc, Mutex},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use market_squawk_adapter_schwab::{
    AccessTokenAdmission, ConnectionGeneration, MarketDataService, ParseBounds, ProviderIdentifier,
    RawRestResponseReceipt, ReadOnlyRequest, ReadOnlyRoute, RequestAdmission, RestExecutionOutcome,
    RestItemAccounting, RestTransportBounds, SchwabAccessTokenSource, SchwabAdapterError,
    SchwabMarketDataQualification, SchwabOAuthAuthorityReceipt, SchwabRestFamily,
    SchwabSealedStreamerCapture, SchwabStreamerConnectionControlSource,
    SchwabStreamerDesiredStateSendError, SchwabStreamerDesiredStateSender, SchwabStreamerExecutor,
    SchwabStreamerFamilyDoctorHandoff, SchwabStreamerRequestAcknowledgement,
    SchwabStreamerRequestPermit, SchwabStreamerRuntimeAuthority, SchwabStreamerRuntimeEvent,
    SchwabTransportError, SchwabTransportTelemetry, StreamerAdmission, StreamerBootstrap,
    StreamerCaptureSink, StreamerCommand, StreamerRunExit, StreamerSubscription,
    StreamerTransportBounds, TokenAuthorityError, TransientAccessToken,
};
use market_squawk_data::{
    DatasetId, ListingReferenceGenerationReceipt, ListingReferenceReadCapability,
    ListingReferenceRightsState,
};
use market_squawk_domain::{
    AssignmentVerification, DataQuality, EffectiveInterval, IdentifierEntitlement, LiveEventClass,
    Timestamp, TradingStatus, VenueId,
};
use market_squawk_sources::{
    BudgetDecision, BudgetDispatchDecision, BudgetPermit, BudgetReservation,
    BudgetReservationDecision, BudgetUnavailableReason, ProviderRateAuthority,
    ProviderRateDeclaration, SCHWAB_MARKET_DATA_SURFACE_ID, SchwabMarketDataDoctorReceiptV1,
    SchwabMarketDataFamily, SharedProviderBudget, SourceMetadata, apply_http_retry_after,
};
use tokio_util::sync::CancellationToken;

use crate::application::{
    MarketEventDurableRead, ResearchIngestCompositionError, ResearchProviderRuntimeGeneration,
    SchwabMarketPublicationError, SchwabRestQuoteCurrentRuntimeInput,
    SchwabRestQuotePublicationPackage, SchwabRestQuoteRuntimeBounds, SchwabRestQuoteRuntimeError,
    SchwabRestQuoteSourceEvidence,
};
use crate::live_source::SchwabRestQuoteCurrentSessionInput;
use crate::provider_onboarding::{SchwabOAuthMarketAuthority, SchwabOAuthPublicationEpoch};
use crate::{ProviderActivationLease, ProviderOnboardingError};

use super::account::{
    ProviderAccountActivationError, ProviderAccountBinding, ProviderAccountRuntimeAuthority,
    ProviderAccountRuntimeCurrentness, ProviderMarketAccount,
};
use super::{
    BoundedMarketInstrumentSet, MarketDataInstrumentBinding, MarketInstrumentBinding,
    MarketInstrumentReferenceBinding, MarketReferenceIdentityApprovalV1,
    MarketReferenceIdentityAuthority, ProviderAdapterActivation,
};

const SCHWAB_QUOTE_SOURCE_ID: &str = "schwab-trader-api";
const SCHWAB_QUOTE_PROVIDER: &str = "schwab-trader-api";
const SCHWAB_QUOTE_PRODUCT: &str = "schwab-rest";
const SCHWAB_QUOTE_CHANNEL: &str = "schwab-rest-quotes";
const SCHWAB_QUOTE_VENUE: &str = "schwab";
const SCHWAB_QUOTE_MAXIMUM_SYMBOLS: usize = 50;
const SCHWAB_QUOTE_MAXIMUM_REQUEST_BYTES: usize = 16 * 1024;
const SCHWAB_QUOTE_MAXIMUM_RESPONSE_BYTES: usize = 4 * 1024 * 1024;
const SCHWAB_QUOTE_MAXIMUM_JSON_NODES: usize = 256 * 1024;
const SCHWAB_QUOTE_MAXIMUM_JSON_DEPTH: usize = 64;
const SCHWAB_QUOTE_MAXIMUM_UNKNOWN_FIELDS: usize = 512;
const SCHWAB_QUOTE_MAXIMUM_UNKNOWN_BYTES: usize = 512 * 1024;
const SCHWAB_QUOTE_MAXIMUM_HEADERS: usize = 64;
const SCHWAB_QUOTE_MAXIMUM_HEADER_BYTES: usize = 64 * 1024;
const SCHWAB_QUOTE_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const SCHWAB_QUOTE_READ_TIMEOUT: Duration = Duration::from_secs(15);
const SCHWAB_QUOTE_REQUEST_TIMEOUT: Duration = Duration::from_secs(20);
const SCHWAB_QUOTE_MINIMUM_TOKEN_LIFETIME: Duration = Duration::from_secs(60);
const SCHWAB_QUOTE_FRESHNESS_MARGIN_DIVISOR: u64 = 2;

/// Non-clone owner of one callable Schwab read-only market-data epoch.
///
/// It retains the exact active onboarding lease, durable doctor receipt, protected OAuth market
/// authority, account-lifetime authority, and shared provider-rate authority. It exposes no
/// account, position, transaction, order, or money-movement operation.
pub struct SchwabMarketDataAccountActivation {
    authority: Arc<ProviderAccountRuntimeAuthority>,
    oauth: SchwabOAuthMarketAuthority,
    doctor: SchwabMarketDataDoctorReceiptV1,
    doctor_generation: Arc<Mutex<SchwabDoctorGenerationDisposition>>,
    rate_budget: Arc<SharedProviderBudget>,
    streamer_authority_issued: Mutex<bool>,
}

/// Provider-neutral product meaning of one internally qualified read-only market-data leaf.
///
/// This type is deliberately not serialized. It lets application composition route an internal
/// provider handoff without exposing provider names, endpoints, token state, or adapter terms to
/// ordinary product DTOs.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ReadOnlyMarketDataLeaf {
    CurrentQuotes,
    HistoricalPrices,
    OptionSurface,
    OptionExpirations,
    TradingHours,
    MarketMovers,
    InstrumentReference,
    EquityLive,
    OptionLive,
    FuturesLive,
    FuturesOptionLive,
    ForexLive,
    VenueBook,
    IntradayChart,
    MarketScreener,
}

/// One-use REST attempt minted by the sole account activation.
///
/// The bearer and OAuth epoch remain private and non-cloneable. A caller may borrow the bearer for
/// exactly the already-bound allowlisted request, then must consume this attempt together with the
/// completed adapter outcome. The response cannot be relabeled as another family or another
/// request after transport.
pub(crate) struct SchwabReadOnlyRestAttempt {
    leaf: ReadOnlyMarketDataLeaf,
    family: SchwabMarketDataFamily,
    route: ReadOnlyRoute,
    request_url: Box<str>,
    requested_items: u64,
    doctor: SchwabMarketDataDoctorReceiptV1,
    currentness: ProviderAccountRuntimeCurrentness,
    token: TransientAccessToken,
    oauth_epoch: SchwabOAuthPublicationEpoch,
    budget: Arc<SharedProviderBudget>,
    reservation: BudgetReservation,
}

impl SchwabReadOnlyRestAttempt {
    pub(crate) const fn leaf(&self) -> ReadOnlyMarketDataLeaf {
        self.leaf
    }

    /// Consumes pre-dispatch admission at the exact transport seam. Dropping this value before
    /// this transition releases concurrency without charging a provider request window.
    pub(crate) fn dispatch(
        self,
    ) -> Result<SchwabDispatchedReadOnlyRestAttempt, SchwabMarketDataActivationError> {
        let permit = match self.reservation.commit_dispatch() {
            BudgetDispatchDecision::Ready(permit) => permit,
            BudgetDispatchDecision::WaitUntil(_deadline) => {
                return Err(SchwabMarketDataActivationError::RateDeferred);
            }
            BudgetDispatchDecision::Unavailable(reason) => {
                return Err(SchwabMarketDataActivationError::RateUnavailable(reason));
            }
        };
        Ok(SchwabDispatchedReadOnlyRestAttempt {
            leaf: self.leaf,
            family: self.family,
            route: self.route,
            request_url: self.request_url,
            requested_items: self.requested_items,
            doctor: self.doctor,
            currentness: self.currentness,
            token: self.token,
            oauth_epoch: self.oauth_epoch,
            budget: self.budget,
            permit,
        })
    }
}

/// One exact dispatched REST operation whose rate permit spans response classification.
pub(crate) struct SchwabDispatchedReadOnlyRestAttempt {
    leaf: ReadOnlyMarketDataLeaf,
    family: SchwabMarketDataFamily,
    route: ReadOnlyRoute,
    request_url: Box<str>,
    requested_items: u64,
    doctor: SchwabMarketDataDoctorReceiptV1,
    currentness: ProviderAccountRuntimeCurrentness,
    token: TransientAccessToken,
    oauth_epoch: SchwabOAuthPublicationEpoch,
    budget: Arc<SharedProviderBudget>,
    permit: BudgetPermit,
}

impl SchwabDispatchedReadOnlyRestAttempt {
    /// Borrows the zeroizing bearer owner only for the already-charged adapter execution.
    pub(crate) const fn token(&self) -> &TransientAccessToken {
        &self.token
    }

    /// Consumes the exact attempt and completed provider-native response into the only application
    /// handoff admitted for this request.
    pub(crate) fn complete(
        self,
        outcome: RestExecutionOutcome,
    ) -> Result<SchwabReadOnlyRestHandoff, SchwabMarketDataActivationError> {
        if !self.currentness.is_active_now() {
            return Err(SchwabMarketDataActivationError::AuthorityMismatch);
        }
        let receipt = rest_outcome_receipt(&outcome)?;
        let accounting = rest_outcome_accounting(&outcome)?;
        let expected_payload = rest_payload_family(self.route)
            .ok_or(SchwabMarketDataActivationError::AuthorityMismatch)?;
        if receipt.route() != self.route
            || receipt.request_url() != self.request_url.as_ref()
            || accounting.requested != self.requested_items
            || receipt.token_generation() != self.token.generation()
            || receipt.credential_authority() != self.token.credential_authority()
            || receipt.token_generation() != self.oauth_epoch.receipt().generation()
            || receipt.credential_authority() != self.oauth_epoch.receipt().credential_authority()
            || matches!(
                &outcome,
                RestExecutionOutcome::Accepted(response)
                    if response.payload().family() != expected_payload
            )
        {
            return Err(SchwabMarketDataActivationError::AuthorityMismatch);
        }
        // An exact provider response terminalizes the dispatched account-rate permit even when
        // later OAuth, doctor, or currentness checks reject publication. Mismatched responses
        // leave the permit to its conservative unknown-completion drop path.
        settle_rest_rate(&self.budget, self.permit, receipt)?;
        self.oauth_epoch
            .validate_current(self.oauth_epoch.receipt())?;
        let observed_at = timestamp_from_millis(receipt.received_at_unix_millis())?;
        let qualification = SchwabMarketDataQualification::try_from_doctor_receipt(
            &self.doctor,
            self.family,
            observed_at,
            self.oauth_epoch.receipt(),
        )
        .map_err(|_error| SchwabMarketDataActivationError::AuthorityMismatch)?;
        if !self.currentness.is_active_now() {
            return Err(SchwabMarketDataActivationError::AuthorityMismatch);
        }
        self.oauth_epoch
            .validate_current(self.oauth_epoch.receipt())?;
        Ok(SchwabReadOnlyRestHandoff {
            leaf: self.leaf,
            qualification,
            outcome,
            oauth_epoch: self.oauth_epoch,
        })
    }
}

impl std::fmt::Debug for SchwabDispatchedReadOnlyRestAttempt {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SchwabDispatchedReadOnlyRestAttempt")
            .field("leaf", &self.leaf)
            .field("family", &self.family)
            .field("route", &self.route)
            .field("request_url", &self.request_url)
            .field("requested_items", &self.requested_items)
            .field("token", &"[PROTECTED TOKEN]")
            .finish()
    }
}

impl std::fmt::Debug for SchwabReadOnlyRestAttempt {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SchwabReadOnlyRestAttempt")
            .field("leaf", &self.leaf)
            .field("family", &self.family)
            .field("route", &self.route)
            .field("request_url", &self.request_url)
            .field("requested_items", &self.requested_items)
            .field("token", &"[PROTECTED TOKEN]")
            .finish()
    }
}

/// Non-cloneable provider-native response handoff with a neutral application routing key.
pub(crate) struct SchwabReadOnlyRestHandoff {
    leaf: ReadOnlyMarketDataLeaf,
    qualification: SchwabMarketDataQualification,
    outcome: RestExecutionOutcome,
    oauth_epoch: SchwabOAuthPublicationEpoch,
}

impl SchwabReadOnlyRestHandoff {
    pub(crate) const fn leaf(&self) -> ReadOnlyMarketDataLeaf {
        self.leaf
    }

    pub(crate) fn into_parts(
        self,
    ) -> (
        SchwabMarketDataQualification,
        RestExecutionOutcome,
        SchwabOAuthPublicationEpoch,
    ) {
        (self.qualification, self.outcome, self.oauth_epoch)
    }
}

impl std::fmt::Debug for SchwabReadOnlyRestHandoff {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SchwabReadOnlyRestHandoff")
            .field("leaf", &self.leaf)
            .field("qualification", &self.qualification)
            .field("outcome", &self.outcome)
            .field("oauth_epoch", &"[CURRENTNESS BARRIER]")
            .finish()
    }
}

type SchwabStreamerServiceSet = [(
    MarketDataService,
    ReadOnlyMarketDataLeaf,
    SchwabMarketDataFamily,
)];

/// One-use application lease that can only be consumed into one multiplexed Streamer executor.
pub(crate) struct SchwabReadOnlyStreamerLease {
    services: Arc<SchwabStreamerServiceSet>,
    currentness: ProviderAccountRuntimeCurrentness,
    oauth: SchwabOAuthMarketAuthority,
    doctor: SchwabMarketDataDoctorReceiptV1,
    doctor_generation: Arc<Mutex<SchwabDoctorGenerationDisposition>>,
    rate_budget: Arc<SharedProviderBudget>,
}

impl SchwabReadOnlyStreamerLease {
    #[allow(
        clippy::too_many_arguments,
        reason = "one consumed executor keeps transport, authority, and bounded resources explicit"
    )]
    pub(crate) fn into_executor(
        self,
        control_source: Arc<dyn SchwabStreamerConnectionControlSource>,
        admission: StreamerAdmission,
        transport_bounds: StreamerTransportBounds,
        parse_bounds: ParseBounds,
        token_admission: AccessTokenAdmission,
        telemetry: SchwabTransportTelemetry,
    ) -> Result<SchwabReadOnlyStreamerExecutor, SchwabTransportError> {
        if self.services.len() > admission.max_services() {
            return Err(SchwabTransportError::InvalidConfiguration);
        }
        let admitted_services = self
            .services
            .iter()
            .map(|(service, _leaf, _family)| *service)
            .collect::<BTreeSet<_>>();
        let token_authority = Arc::new(SchwabReadOnlyStreamerTokenAuthority {
            services: Arc::clone(&self.services),
            currentness: self.currentness.clone(),
            oauth: self.oauth,
            doctor: self.doctor.clone(),
            doctor_generation: Arc::clone(&self.doctor_generation),
        });
        let runtime_authority = Arc::new(SchwabStreamerAccountRateAuthority {
            admitted_services: admitted_services.clone(),
            currentness: self.currentness.clone(),
            budget: self.rate_budget,
            state: Mutex::new(SchwabStreamerRateState::default()),
        });
        let executor = SchwabStreamerExecutor::try_production(
            token_authority.clone(),
            control_source,
            runtime_authority,
            admission,
            transport_bounds,
            parse_bounds,
            token_admission,
            telemetry,
        )?;
        Ok(SchwabReadOnlyStreamerExecutor {
            executor,
            services: self.services,
            admitted_services: Arc::new(admitted_services),
            token_authority,
        })
    }
}

/// Private token source retained only inside the consumed executor lifetime.
struct SchwabReadOnlyStreamerTokenAuthority {
    services: Arc<SchwabStreamerServiceSet>,
    currentness: ProviderAccountRuntimeCurrentness,
    oauth: SchwabOAuthMarketAuthority,
    doctor: SchwabMarketDataDoctorReceiptV1,
    doctor_generation: Arc<Mutex<SchwabDoctorGenerationDisposition>>,
}

impl SchwabReadOnlyStreamerTokenAuthority {
    async fn acquire_bound_token(&self) -> Result<TransientAccessToken, TokenAuthorityError> {
        if !self.currentness.is_active().await {
            return Err(TokenAuthorityError::Unavailable);
        }
        let (token, epoch) = self
            .oauth
            .acquire_publication_attempt()
            .await
            .map_err(map_streamer_token_error)?;
        require_doctor_generation(&self.doctor_generation, epoch.receipt().generation().get())
            .map_err(map_streamer_activation_error)?;
        let observed_at = system_timestamp().map_err(|_error| TokenAuthorityError::Unavailable)?;
        for (_service, _leaf, family) in self.services.iter() {
            SchwabMarketDataQualification::try_from_doctor_receipt(
                &self.doctor,
                *family,
                observed_at,
                epoch.receipt(),
            )
            .map_err(|_error| TokenAuthorityError::ReauthorizationRequired)?;
        }
        if token.generation() != epoch.receipt().generation()
            || token.credential_authority() != epoch.receipt().credential_authority()
            || !self.currentness.is_active().await
        {
            return Err(TokenAuthorityError::ReauthorizationRequired);
        }
        epoch
            .validate_current(epoch.receipt())
            .map_err(map_streamer_token_error)?;
        Ok(token)
    }
}

impl SchwabAccessTokenSource for SchwabReadOnlyStreamerTokenAuthority {
    fn acquire(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<TransientAccessToken, TokenAuthorityError>> + Send + '_>>
    {
        Box::pin(self.acquire_bound_token())
    }
}

impl std::fmt::Debug for SchwabReadOnlyStreamerTokenAuthority {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SchwabReadOnlyStreamerTokenAuthority")
            .field("services", &self.services)
            .field("oauth", &"[PROTECTED TOKEN AUTHORITY]")
            .field("doctor_receipt", &self.doctor.receipt_sha256())
            .finish()
    }
}

/// Sole application-owned executor. It exposes only selected-service desired state and bound
/// publication handoffs; the raw token source and raw executor cannot be recovered or cloned.
pub(crate) struct SchwabReadOnlyStreamerExecutor {
    executor: SchwabStreamerExecutor,
    services: Arc<SchwabStreamerServiceSet>,
    admitted_services: Arc<BTreeSet<MarketDataService>>,
    token_authority: Arc<SchwabReadOnlyStreamerTokenAuthority>,
}

impl SchwabReadOnlyStreamerExecutor {
    pub(crate) fn services(
        &self,
    ) -> impl ExactSizeIterator<Item = (MarketDataService, ReadOnlyMarketDataLeaf)> + '_ {
        self.services
            .iter()
            .map(|(service, leaf, _family)| (*service, *leaf))
    }

    pub(crate) fn replace_desired(
        &mut self,
        subscription: StreamerSubscription,
    ) -> Result<(), SchwabAdapterError> {
        self.require_admitted(subscription.service())?;
        self.executor.replace_desired(subscription)
    }

    pub(crate) fn add_desired(
        &mut self,
        subscription: StreamerSubscription,
    ) -> Result<(), SchwabAdapterError> {
        self.require_admitted(subscription.service())?;
        self.executor.add_desired(subscription)
    }

    pub(crate) fn remove_desired(
        &mut self,
        subscription: StreamerSubscription,
    ) -> Result<(), SchwabAdapterError> {
        self.require_admitted(subscription.service())?;
        self.executor.remove_desired(subscription)
    }

    pub(crate) fn take_desired_state_sender(
        &mut self,
    ) -> Option<SchwabReadOnlyStreamerDesiredStateSender> {
        self.executor.take_desired_state_sender().map(|sender| {
            SchwabReadOnlyStreamerDesiredStateSender {
                sender,
                admitted_services: Arc::clone(&self.admitted_services),
            }
        })
    }

    pub(crate) async fn run(
        &mut self,
        bootstrap: &StreamerBootstrap,
        sink: &mut dyn StreamerCaptureSink,
        cancellation: CancellationToken,
    ) -> Result<StreamerRunExit, SchwabTransportError> {
        self.require_current()
            .await
            .map_err(|_error| SchwabTransportError::TokenRefreshRequired)?;
        self.executor.run(bootstrap, sink, cancellation).await
    }

    pub(crate) async fn require_current(&self) -> Result<(), SchwabMarketDataActivationError> {
        require_streamer_current(&self.token_authority).await
    }

    pub(crate) async fn qualify_sealed_coordinate<'a>(
        &self,
        service: MarketDataService,
        doctor_handoff: &'a SchwabStreamerFamilyDoctorHandoff,
        capture: &'a SchwabSealedStreamerCapture,
        frame_ordinal: u16,
        data_batch_ordinal: u16,
        content_ordinal: u16,
    ) -> Result<SchwabReadOnlyStreamerPublicationHandoff<'a>, SchwabMarketDataActivationError> {
        self.require_current().await?;
        let (_selected_service, leaf, family) = self
            .services
            .iter()
            .find(|(selected, _leaf, _family)| *selected == service)
            .copied()
            .ok_or(SchwabMarketDataActivationError::AuthorityMismatch)?;
        let frame = capture
            .frames()
            .get(usize::from(frame_ordinal))
            .ok_or(SchwabMarketDataActivationError::AuthorityMismatch)?;
        let receipt = capture.streamer_receipt();
        let current = self.token_authority.oauth.current_receipt().await?;
        require_doctor_generation(
            &self.token_authority.doctor_generation,
            current.generation().get(),
        )?;
        if doctor_handoff.service() != service
            || doctor_handoff.acknowledgement().status_code() != 0
            || receipt.token_generation() != current.generation()
            || receipt.credential_authority() != current.credential_authority()
            || receipt.session_identifier() != self.token_authority.doctor.session_identifier()
            || receipt.market_data_principal_sha256()
                != self.token_authority.doctor.market_data_principal_sha256()
            || frame.generation() != receipt.generation()
        {
            return Err(SchwabMarketDataActivationError::AuthorityMismatch);
        }
        let observed_at = timestamp_from_millis(frame.received_at_unix_millis())?;
        let qualification = SchwabMarketDataQualification::try_from_doctor_receipt(
            &self.token_authority.doctor,
            family,
            observed_at,
            current,
        )
        .map_err(|_error| SchwabMarketDataActivationError::AuthorityMismatch)?;
        if !qualification.validates_streamer_publication_coordinate(
            service,
            doctor_handoff,
            capture,
            frame_ordinal,
            data_batch_ordinal,
            content_ordinal,
        ) || !self.token_authority.currentness.is_active().await
        {
            return Err(SchwabMarketDataActivationError::AuthorityMismatch);
        }
        Ok(SchwabReadOnlyStreamerPublicationHandoff {
            leaf,
            service,
            qualification,
            doctor_handoff,
            capture,
            frame_ordinal,
            data_batch_ordinal,
            content_ordinal,
        })
    }

    fn require_admitted(&self, service: MarketDataService) -> Result<(), SchwabAdapterError> {
        if self.admitted_services.contains(&service) {
            Ok(())
        } else {
            Err(SchwabAdapterError::InvalidStreamerState)
        }
    }
}

pub(crate) struct SchwabReadOnlyStreamerDesiredStateSender {
    sender: SchwabStreamerDesiredStateSender,
    admitted_services: Arc<BTreeSet<MarketDataService>>,
}

impl SchwabReadOnlyStreamerDesiredStateSender {
    pub(crate) fn try_send(
        &self,
        command: StreamerCommand,
        subscription: StreamerSubscription,
    ) -> Result<(), SchwabReadOnlyStreamerDesiredStateError> {
        if !self.admitted_services.contains(&subscription.service()) {
            return Err(SchwabReadOnlyStreamerDesiredStateError::NotAdmitted);
        }
        self.sender
            .try_send(command, subscription)
            .map_err(SchwabReadOnlyStreamerDesiredStateError::Queue)
    }
}

#[derive(Debug)]
pub(crate) enum SchwabReadOnlyStreamerDesiredStateError {
    NotAdmitted,
    Queue(SchwabStreamerDesiredStateSendError),
}

/// Non-cloneable publication capability retaining the exact successful ACK and sealed coordinate.
pub(crate) struct SchwabReadOnlyStreamerPublicationHandoff<'a> {
    leaf: ReadOnlyMarketDataLeaf,
    service: MarketDataService,
    qualification: SchwabMarketDataQualification,
    doctor_handoff: &'a SchwabStreamerFamilyDoctorHandoff,
    capture: &'a SchwabSealedStreamerCapture,
    frame_ordinal: u16,
    data_batch_ordinal: u16,
    content_ordinal: u16,
}

impl<'a> SchwabReadOnlyStreamerPublicationHandoff<'a> {
    pub(crate) fn into_parts(
        self,
    ) -> (
        ReadOnlyMarketDataLeaf,
        MarketDataService,
        SchwabMarketDataQualification,
        &'a SchwabStreamerFamilyDoctorHandoff,
        &'a SchwabSealedStreamerCapture,
        u16,
        u16,
        u16,
    ) {
        (
            self.leaf,
            self.service,
            self.qualification,
            self.doctor_handoff,
            self.capture,
            self.frame_ordinal,
            self.data_batch_ordinal,
            self.content_ordinal,
        )
    }
}

async fn require_streamer_current(
    authority: &SchwabReadOnlyStreamerTokenAuthority,
) -> Result<(), SchwabMarketDataActivationError> {
    if !authority.currentness.is_active().await {
        return Err(SchwabMarketDataActivationError::AuthorityMismatch);
    }
    let current = authority.oauth.current_receipt().await?;
    require_doctor_generation(&authority.doctor_generation, current.generation().get())?;
    let observed_at =
        system_timestamp().map_err(|_error| SchwabMarketDataActivationError::AuthorityMismatch)?;
    for (_service, _leaf, family) in authority.services.iter() {
        SchwabMarketDataQualification::try_from_doctor_receipt(
            &authority.doctor,
            *family,
            observed_at,
            current,
        )
        .map_err(|_error| SchwabMarketDataActivationError::AuthorityMismatch)?;
    }
    if !authority.currentness.is_active().await {
        return Err(SchwabMarketDataActivationError::AuthorityMismatch);
    }
    Ok(())
}

#[derive(Default)]
struct SchwabStreamerRateState {
    generation: Option<ConnectionGeneration>,
    connect_attempts: u64,
    reconnect_attempts: u64,
    frames: u64,
    frame_bytes: u64,
    queue_pressure: u64,
    disconnects: u64,
}

struct SchwabStreamerAccountRateAuthority {
    admitted_services: BTreeSet<MarketDataService>,
    currentness: ProviderAccountRuntimeCurrentness,
    budget: Arc<SharedProviderBudget>,
    state: Mutex<SchwabStreamerRateState>,
}

impl std::fmt::Debug for SchwabStreamerAccountRateAuthority {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SchwabStreamerAccountRateAuthority")
            .field("admitted_services", &self.admitted_services)
            .field("budget", &"[SHARED ACCOUNT RATE AUTHORITY]")
            .finish()
    }
}

impl SchwabStreamerRuntimeAuthority for SchwabStreamerAccountRateAuthority {
    fn observe(&self, event: SchwabStreamerRuntimeEvent) -> Result<(), SchwabTransportError> {
        if !self.currentness.is_active_now() {
            return Err(SchwabTransportError::TokenRefreshRequired);
        }
        let mut state = self
            .state
            .lock()
            .map_err(|_poisoned| SchwabTransportError::Protocol)?;
        match event {
            SchwabStreamerRuntimeEvent::ConnectAttempt { reconnecting } => {
                state.connect_attempts = state
                    .connect_attempts
                    .checked_add(1)
                    .ok_or(SchwabTransportError::Overflow)?;
                if reconnecting {
                    state.reconnect_attempts = state
                        .reconnect_attempts
                        .checked_add(1)
                        .ok_or(SchwabTransportError::Overflow)?;
                }
            }
            SchwabStreamerRuntimeEvent::Connected { generation } => {
                if state.generation != Some(generation) {
                    return Err(SchwabTransportError::Protocol);
                }
            }
            SchwabStreamerRuntimeEvent::Frame { generation, bytes } => {
                if state.generation != Some(generation) {
                    return Err(SchwabTransportError::Protocol);
                }
                state.frames = state
                    .frames
                    .checked_add(1)
                    .ok_or(SchwabTransportError::Overflow)?;
                state.frame_bytes = state
                    .frame_bytes
                    .checked_add(bytes)
                    .ok_or(SchwabTransportError::Overflow)?;
            }
            SchwabStreamerRuntimeEvent::QueuePressure => {
                state.queue_pressure = state
                    .queue_pressure
                    .checked_add(1)
                    .ok_or(SchwabTransportError::Overflow)?;
            }
            SchwabStreamerRuntimeEvent::Disconnected { generation, .. } => {
                if state.generation != Some(generation) {
                    return Err(SchwabTransportError::Protocol);
                }
                state.disconnects = state
                    .disconnects
                    .checked_add(1)
                    .ok_or(SchwabTransportError::Overflow)?;
                state.generation = None;
            }
        }
        Ok(())
    }

    fn commit_request(
        &self,
        generation: ConnectionGeneration,
        service: Option<MarketDataService>,
        command: &str,
        request_id: &str,
        request_payload_sha256: market_squawk_domain::EvidenceDigest,
        request_payload_bytes: u64,
    ) -> Result<Box<dyn SchwabStreamerRequestPermit>, SchwabTransportError> {
        if !self.currentness.is_active_now()
            || request_id.is_empty()
            || request_payload_bytes == 0
            || request_payload_sha256.bytes() == [0; 32]
            || match service {
                None => command != "LOGIN",
                Some(service) => {
                    !self.admitted_services.contains(&service)
                        || !matches!(command, "SUBS" | "ADD" | "UNSUBS")
                }
            }
        {
            return Err(SchwabTransportError::Protocol);
        }
        let mut state = self
            .state
            .lock()
            .map_err(|_poisoned| SchwabTransportError::Protocol)?;
        match (state.generation, service) {
            (None, None) => state.generation = Some(generation),
            (Some(current), Some(_)) if current == generation => {}
            _ => return Err(SchwabTransportError::Protocol),
        }
        drop(state);
        let reservation = match self.budget.try_reserve_request() {
            BudgetReservationDecision::Ready(reservation) => reservation,
            BudgetReservationDecision::WaitUntil(_deadline) => {
                return Err(SchwabTransportError::Deadline);
            }
            BudgetReservationDecision::Unavailable(reason) => {
                return Err(map_streamer_budget_error(reason));
            }
        };
        let permit = match reservation.commit_dispatch() {
            BudgetDispatchDecision::Ready(permit) => permit,
            BudgetDispatchDecision::WaitUntil(_deadline) => {
                return Err(SchwabTransportError::Deadline);
            }
            BudgetDispatchDecision::Unavailable(reason) => {
                return Err(map_streamer_budget_error(reason));
            }
        };
        Ok(Box::new(SchwabStreamerAccountRatePermit {
            generation,
            service,
            command: command.to_owned().into_boxed_str(),
            request_id: request_id.to_owned().into_boxed_str(),
            request_payload_sha256,
            request_payload_bytes,
            budget: Arc::clone(&self.budget),
            permit,
        }))
    }
}

struct SchwabStreamerAccountRatePermit {
    generation: ConnectionGeneration,
    service: Option<MarketDataService>,
    command: Box<str>,
    request_id: Box<str>,
    request_payload_sha256: market_squawk_domain::EvidenceDigest,
    request_payload_bytes: u64,
    budget: Arc<SharedProviderBudget>,
    permit: BudgetPermit,
}

impl std::fmt::Debug for SchwabStreamerAccountRatePermit {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SchwabStreamerAccountRatePermit")
            .field("generation", &self.generation)
            .field("service", &self.service)
            .field("permit", &"[DISPATCHED ACCOUNT RATE PERMIT]")
            .finish()
    }
}

impl SchwabStreamerRequestPermit for SchwabStreamerAccountRatePermit {
    fn settle(
        self: Box<Self>,
        acknowledgement: SchwabStreamerRequestAcknowledgement,
    ) -> Result<(), SchwabTransportError> {
        if acknowledgement.generation != self.generation
            || acknowledgement.service != self.service
            || acknowledgement.command != self.command
            || acknowledgement.request_id != self.request_id
            || acknowledgement.request_payload_sha256 != self.request_payload_sha256
            || acknowledgement.request_payload_bytes != self.request_payload_bytes
        {
            return Err(SchwabTransportError::Protocol);
        }
        self.permit.release();
        if acknowledgement.status_code == 0 {
            self.budget
                .record_success()
                .map_err(map_streamer_budget_error)
        } else {
            settle_streamer_budget_decision(self.budget.apply_refusal(0))
        }
    }
}

fn settle_streamer_budget_decision(decision: BudgetDecision) -> Result<(), SchwabTransportError> {
    match decision {
        BudgetDecision::Ready(unexpected) => {
            unexpected.release();
            Err(SchwabTransportError::Protocol)
        }
        BudgetDecision::WaitUntil(_deadline) => Ok(()),
        BudgetDecision::Unavailable(reason) => Err(map_streamer_budget_error(reason)),
    }
}

const fn map_streamer_budget_error(reason: BudgetUnavailableReason) -> SchwabTransportError {
    match reason {
        BudgetUnavailableReason::Disabled
        | BudgetUnavailableReason::RetryAfterExceedsPolicy
        | BudgetUnavailableReason::AvailabilityChanged => SchwabTransportError::Deadline,
        _ => SchwabTransportError::Protocol,
    }
}

/// One-use upstream package for the exact registered Schwab current-quote generation.
///
/// Construction consumes a successful account activation and retains the sole durable
/// publication package. It implements neither `Clone` nor serialization, so OAuth, doctor,
/// publication, and current-session authority cannot be fanned out across competing runtimes.
pub(crate) struct PreparedSchwabMarketRuntimeStart {
    activation: SchwabMarketDataAccountActivation,
    provider_rate: ProviderRateAuthority,
    generation: ResearchProviderRuntimeGeneration,
    evidence: SchwabRestQuoteSourceEvidence,
    bindings: Vec<(
        MarketInstrumentBinding,
        Option<MarketReferenceIdentityApprovalV1>,
    )>,
    display_bindings: Box<[MarketDataInstrumentBinding]>,
    reference_identity: Option<MarketReferenceIdentityAuthority>,
    listing_reference: Option<ListingReferenceReadCapability>,
    nasdaq_generation: Option<ListingReferenceGenerationReceipt>,
    bounds: SchwabRestQuoteRuntimeBounds,
    telemetry: SchwabTransportTelemetry,
    publication: SchwabRestQuotePublicationPackage,
    request_timeout: Duration,
    poll_interval: Duration,
}

impl PreparedSchwabMarketRuntimeStart {
    /// Returns the exact registered provider-runtime generation retained by this package.
    pub(crate) const fn generation(&self) -> &ResearchProviderRuntimeGeneration {
        &self.generation
    }

    /// Returns the paired provider-neutral durable read before the package is consumed.
    pub(crate) const fn durable_read(&self) -> &MarketEventDurableRead {
        self.publication.durable_read()
    }

    pub(crate) fn activation_lease(&self) -> &ProviderActivationLease {
        self.activation.lease()
    }

    pub(crate) fn currentness(&self) -> ProviderAccountRuntimeCurrentness {
        self.activation.currentness()
    }

    pub(crate) const fn metadata(&self) -> &SourceMetadata {
        self.evidence.metadata()
    }

    pub(crate) const fn venue_id(&self) -> &VenueId {
        self.evidence.venue_id()
    }

    pub(crate) fn bindings(
        &self,
    ) -> &[(
        MarketInstrumentBinding,
        Option<MarketReferenceIdentityApprovalV1>,
    )] {
        &self.bindings
    }

    pub(crate) fn display_bindings(&self) -> &[MarketDataInstrumentBinding] {
        &self.display_bindings
    }

    /// Joins the registry-minted current session and lifecycle to the already prepared upstream
    /// authorities without reconstructing source evidence, controls, or durable publication.
    pub(crate) fn into_runtime_input(
        self,
        current: SchwabRestQuoteCurrentSessionInput,
        lifecycle: CancellationToken,
    ) -> SchwabRestQuoteCurrentRuntimeInput {
        let (durable, durable_writer) = self.publication.into_runtime_parts();
        SchwabRestQuoteCurrentRuntimeInput::new(
            self.activation,
            self.provider_rate,
            self.evidence,
            self.bindings,
            self.reference_identity,
            self.listing_reference,
            self.nasdaq_generation,
            self.bounds,
            self.telemetry,
            durable,
            durable_writer,
            current,
            self.request_timeout,
            self.poll_interval,
            lifecycle,
        )
    }
}

impl std::fmt::Debug for PreparedSchwabMarketRuntimeStart {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PreparedSchwabMarketRuntimeStart")
            .field("generation", &self.generation)
            .field("instrument_count", &self.bindings.len())
            .field("oauth", &"[PROTECTED TOKEN AUTHORITY]")
            .field("publication", &"[EXACT DURABLE AUTHORITY]")
            .finish()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SchwabDoctorGenerationDisposition {
    Current(u64),
    RenewalRequired {
        doctor_generation: u64,
        observed_generation: u64,
    },
}

impl SchwabMarketDataAccountActivation {
    pub fn lease(&self) -> &ProviderActivationLease {
        self.authority.lease()
    }

    pub fn account_binding(&self) -> &ProviderAccountBinding {
        self.authority.binding()
    }

    /// Returns only the current non-secret receipt; ordinary runtimes cannot clone raw OAuth.
    pub(crate) async fn current_oauth_receipt(
        &self,
    ) -> Result<SchwabOAuthAuthorityReceipt, SchwabMarketDataActivationError> {
        self.authority.require_current().await?;
        let receipt = self.oauth.current_receipt().await?;
        self.require_doctor_generation(receipt.generation().get())?;
        self.authority.require_current().await?;
        Ok(receipt)
    }

    pub(crate) const fn doctor_receipt(&self) -> &SchwabMarketDataDoctorReceiptV1 {
        &self.doctor
    }

    pub(crate) fn currentness(&self) -> ProviderAccountRuntimeCurrentness {
        self.authority.currentness()
    }

    pub async fn require_current(&self) -> Result<(), SchwabMarketDataActivationError> {
        self.authority.require_current().await?;
        let current = self.oauth.current_receipt().await?;
        self.require_doctor_generation(current.generation().get())
    }

    /// Mints one exact non-cloneable REST attempt for the supplied typed read-only request.
    ///
    /// The route remains the adapter's closed market-data allowlist. `userPreference` is excluded
    /// here because it is reserved for the one Streamer bootstrap owner, and no account, order, or
    /// money-movement route can be represented by `ReadOnlyRequest`.
    pub(crate) async fn acquire_read_only_rest_attempt(
        &self,
        request: &ReadOnlyRequest,
    ) -> Result<SchwabReadOnlyRestAttempt, SchwabMarketDataActivationError> {
        let (family, leaf, _payload) =
            rest_leaf(request.route()).ok_or(SchwabMarketDataActivationError::AuthorityMismatch)?;
        if ReadOnlyRoute::classify(request.method(), request.url())
            .ok()
            .is_none_or(|classified| classified != request.route())
        {
            return Err(SchwabMarketDataActivationError::AuthorityMismatch);
        }
        let requested_items = u64::try_from(request.requested_items())
            .map_err(|_error| SchwabMarketDataActivationError::AuthorityMismatch)?;
        let reservation = match self.rate_budget.try_reserve_request() {
            BudgetReservationDecision::Ready(reservation) => reservation,
            BudgetReservationDecision::WaitUntil(_deadline) => {
                return Err(SchwabMarketDataActivationError::RateDeferred);
            }
            BudgetReservationDecision::Unavailable(reason) => {
                return Err(SchwabMarketDataActivationError::RateUnavailable(reason));
            }
        };
        let (token, oauth_epoch) = self.acquire_publication_attempt().await?;
        let observed_at = system_timestamp()
            .map_err(|_error| SchwabMarketDataActivationError::AuthorityMismatch)?;
        SchwabMarketDataQualification::try_from_doctor_receipt(
            &self.doctor,
            family,
            observed_at,
            oauth_epoch.receipt(),
        )
        .map_err(|_error| SchwabMarketDataActivationError::AuthorityMismatch)?;
        Ok(SchwabReadOnlyRestAttempt {
            leaf,
            family,
            route: request.route(),
            request_url: request.url().to_owned().into_boxed_str(),
            requested_items,
            doctor: self.doctor.clone(),
            currentness: self.currentness(),
            token,
            oauth_epoch,
            budget: Arc::clone(&self.rate_budget),
            reservation,
        })
    }

    /// Issues the sole doctor-bound token authority for one multiplexed Streamer owner.
    ///
    /// Every desired service must be one of the twelve market-data-only services and must have an
    /// available/degraded observation in this exact doctor receipt. Issuance is one-shot for this
    /// activation, so application composition cannot manufacture a second socket owner.
    pub(crate) async fn issue_read_only_streamer_lease(
        &self,
        services: Vec<MarketDataService>,
    ) -> Result<SchwabReadOnlyStreamerLease, SchwabMarketDataActivationError> {
        if services.is_empty() {
            return Err(SchwabMarketDataActivationError::AuthorityMismatch);
        }
        let mut distinct = BTreeSet::new();
        let mut qualified = Vec::new();
        qualified
            .try_reserve_exact(services.len())
            .map_err(|_error| SchwabMarketDataActivationError::AuthorityMismatch)?;
        self.require_current().await?;
        let current = self.oauth.current_receipt().await?;
        self.require_doctor_generation(current.generation().get())?;
        let observed_at = system_timestamp()
            .map_err(|_error| SchwabMarketDataActivationError::AuthorityMismatch)?;
        for service in services {
            if !distinct.insert(service) {
                return Err(SchwabMarketDataActivationError::AuthorityMismatch);
            }
            let (family, leaf) = streamer_leaf(service);
            SchwabMarketDataQualification::try_from_doctor_receipt(
                &self.doctor,
                family,
                observed_at,
                current,
            )
            .map_err(|_error| SchwabMarketDataActivationError::AuthorityMismatch)?;
            qualified.push((service, leaf, family));
        }
        let mut issued = self
            .streamer_authority_issued
            .lock()
            .map_err(|_poisoned| SchwabMarketDataActivationError::AuthorityMismatch)?;
        if *issued {
            return Err(SchwabMarketDataActivationError::AuthorityMismatch);
        }
        *issued = true;
        drop(issued);
        Ok(SchwabReadOnlyStreamerLease {
            services: Arc::from(qualified.into_boxed_slice()),
            currentness: self.currentness(),
            oauth: self.oauth.clone(),
            doctor: self.doctor.clone(),
            doctor_generation: Arc::clone(&self.doctor_generation),
            rate_budget: Arc::clone(&self.rate_budget),
        })
    }

    /// Acquires one exact token/publication attempt behind the serialized OAuth barrier.
    ///
    /// A protected refresh may legitimately advance the token generation. That observation is
    /// latched as `DoctorRenewalRequired`; no request using the rotated token can proceed until
    /// onboarding publishes a fresh doctor receipt and constructs a successor activation.
    pub(crate) async fn acquire_publication_attempt(
        &self,
    ) -> Result<(TransientAccessToken, SchwabOAuthPublicationEpoch), SchwabMarketDataActivationError>
    {
        self.authority.require_current().await?;
        let (token, epoch) = self.oauth.acquire_publication_attempt().await?;
        self.require_doctor_generation(epoch.receipt().generation().get())?;
        Ok((token, epoch))
    }

    fn require_doctor_generation(
        &self,
        observed_generation: u64,
    ) -> Result<(), SchwabMarketDataActivationError> {
        require_doctor_generation(&self.doctor_generation, observed_generation)
    }

    /// Revalidates the exact current source, definition, reference, and provider-symbol binding.
    ///
    /// Preparation and runtime start share this boundary so a package cannot outlive a canonical
    /// identity interval or silently substitute a provider symbol after it was prepared.
    pub(crate) fn validate_current_quote_bindings(
        &self,
        metadata: &SourceMetadata,
        bindings: &[(
            MarketInstrumentBinding,
            Option<MarketReferenceIdentityApprovalV1>,
        )],
        nasdaq_generation: Option<&ListingReferenceGenerationReceipt>,
        at: Timestamp,
        maximum: usize,
        require_exact_coverage: bool,
    ) -> Result<(), SchwabMarketDataActivationError> {
        if maximum == 0
            || maximum > SCHWAB_QUOTE_MAXIMUM_SYMBOLS
            || !self.account_binding().validates_metadata(metadata)
            || metadata.source_id().as_str() != SCHWAB_QUOTE_SOURCE_ID
            || metadata.provider().as_str() != SCHWAB_QUOTE_PROVIDER
            || metadata.budget_policy() != self.lease().provider_budget_policy()
            || !metadata.is_effective_at(at)
            || !self.doctor.is_current_at(at)
            || !self.doctor.admits_source_start()
            || self.doctor.receipt_sha256() != self.lease().runtime_evidence_digest()
            || validate_exact_schwab_quote_bindings(
                bindings,
                metadata,
                nasdaq_generation,
                at,
                maximum,
                require_exact_coverage,
            )
            .is_err()
        {
            return Err(SchwabMarketDataActivationError::AuthorityMismatch);
        }
        Ok(())
    }

    #[cfg(test)]
    pub(crate) async fn acquire_test_publication_attempt(
        oauth: &SchwabOAuthMarketAuthority,
        doctor_generation: u64,
    ) -> Result<(TransientAccessToken, SchwabOAuthPublicationEpoch), SchwabMarketDataActivationError>
    {
        let (token, epoch) = oauth.acquire_publication_attempt().await?;
        let disposition = Mutex::new(SchwabDoctorGenerationDisposition::Current(
            doctor_generation,
        ));
        require_doctor_generation(&disposition, epoch.receipt().generation().get())?;
        Ok((token, epoch))
    }

    #[cfg(test)]
    pub(crate) async fn acquire_test_family_publication_attempt(
        oauth: &SchwabOAuthMarketAuthority,
        doctor: &SchwabMarketDataDoctorReceiptV1,
        family: SchwabMarketDataFamily,
    ) -> Result<(TransientAccessToken, SchwabOAuthPublicationEpoch), SchwabMarketDataActivationError>
    {
        let (token, epoch) =
            Self::acquire_test_publication_attempt(oauth, doctor.access_token_generation()).await?;
        let observed_at = system_timestamp()
            .map_err(|_error| SchwabMarketDataActivationError::AuthorityMismatch)?;
        SchwabMarketDataQualification::try_from_doctor_receipt(
            doctor,
            family,
            observed_at,
            epoch.receipt(),
        )
        .map_err(|_error| SchwabMarketDataActivationError::AuthorityMismatch)?;
        Ok((token, epoch))
    }
}

fn require_doctor_generation(
    authority: &Mutex<SchwabDoctorGenerationDisposition>,
    observed_generation: u64,
) -> Result<(), SchwabMarketDataActivationError> {
    let mut disposition = authority
        .lock()
        .map_err(|_poisoned| SchwabMarketDataActivationError::AuthorityMismatch)?;
    match *disposition {
        SchwabDoctorGenerationDisposition::Current(doctor_generation)
            if doctor_generation == observed_generation =>
        {
            Ok(())
        }
        SchwabDoctorGenerationDisposition::Current(doctor_generation) => {
            *disposition = SchwabDoctorGenerationDisposition::RenewalRequired {
                doctor_generation,
                observed_generation,
            };
            Err(SchwabMarketDataActivationError::DoctorRenewalRequired)
        }
        SchwabDoctorGenerationDisposition::RenewalRequired { .. } => {
            Err(SchwabMarketDataActivationError::DoctorRenewalRequired)
        }
    }
}

fn rest_leaf(
    route: ReadOnlyRoute,
) -> Option<(
    SchwabMarketDataFamily,
    ReadOnlyMarketDataLeaf,
    SchwabRestFamily,
)> {
    Some(match route {
        ReadOnlyRoute::Quotes | ReadOnlyRoute::SingleQuote => (
            SchwabMarketDataFamily::Quotes,
            ReadOnlyMarketDataLeaf::CurrentQuotes,
            SchwabRestFamily::Quotes,
        ),
        ReadOnlyRoute::PriceHistory => (
            SchwabMarketDataFamily::PriceHistory,
            ReadOnlyMarketDataLeaf::HistoricalPrices,
            SchwabRestFamily::DailyPriceHistory,
        ),
        ReadOnlyRoute::Chains => (
            SchwabMarketDataFamily::OptionChains,
            ReadOnlyMarketDataLeaf::OptionSurface,
            SchwabRestFamily::OptionChain,
        ),
        ReadOnlyRoute::ExpirationChain => (
            SchwabMarketDataFamily::ExpirationChains,
            ReadOnlyMarketDataLeaf::OptionExpirations,
            SchwabRestFamily::ExpirationChain,
        ),
        ReadOnlyRoute::Markets | ReadOnlyRoute::SingleMarket => (
            SchwabMarketDataFamily::MarketHours,
            ReadOnlyMarketDataLeaf::TradingHours,
            SchwabRestFamily::MarketHours,
        ),
        ReadOnlyRoute::Movers => (
            SchwabMarketDataFamily::Movers,
            ReadOnlyMarketDataLeaf::MarketMovers,
            SchwabRestFamily::Movers,
        ),
        ReadOnlyRoute::Instruments | ReadOnlyRoute::InstrumentByCusip => (
            SchwabMarketDataFamily::Instruments,
            ReadOnlyMarketDataLeaf::InstrumentReference,
            SchwabRestFamily::Instruments,
        ),
        ReadOnlyRoute::UserPreference => return None,
    })
}

fn rest_payload_family(route: ReadOnlyRoute) -> Option<SchwabRestFamily> {
    rest_leaf(route).map(|(_family, _leaf, payload)| payload)
}

const fn streamer_leaf(
    service: MarketDataService,
) -> (SchwabMarketDataFamily, ReadOnlyMarketDataLeaf) {
    match service {
        MarketDataService::LevelOneEquities => (
            SchwabMarketDataFamily::LevelOneEquities,
            ReadOnlyMarketDataLeaf::EquityLive,
        ),
        MarketDataService::LevelOneOptions => (
            SchwabMarketDataFamily::LevelOneOptions,
            ReadOnlyMarketDataLeaf::OptionLive,
        ),
        MarketDataService::LevelOneFutures => (
            SchwabMarketDataFamily::LevelOneFutures,
            ReadOnlyMarketDataLeaf::FuturesLive,
        ),
        MarketDataService::LevelOneFuturesOptions => (
            SchwabMarketDataFamily::LevelOneFuturesOptions,
            ReadOnlyMarketDataLeaf::FuturesOptionLive,
        ),
        MarketDataService::LevelOneForex => (
            SchwabMarketDataFamily::LevelOneForex,
            ReadOnlyMarketDataLeaf::ForexLive,
        ),
        MarketDataService::NyseBook => (
            SchwabMarketDataFamily::NyseBook,
            ReadOnlyMarketDataLeaf::VenueBook,
        ),
        MarketDataService::NasdaqBook => (
            SchwabMarketDataFamily::NasdaqBook,
            ReadOnlyMarketDataLeaf::VenueBook,
        ),
        MarketDataService::OptionsBook => (
            SchwabMarketDataFamily::OptionsBook,
            ReadOnlyMarketDataLeaf::VenueBook,
        ),
        MarketDataService::ChartEquity => (
            SchwabMarketDataFamily::ChartEquity,
            ReadOnlyMarketDataLeaf::IntradayChart,
        ),
        MarketDataService::ChartFutures => (
            SchwabMarketDataFamily::ChartFutures,
            ReadOnlyMarketDataLeaf::IntradayChart,
        ),
        MarketDataService::ScreenerEquity => (
            SchwabMarketDataFamily::ScreenerEquity,
            ReadOnlyMarketDataLeaf::MarketScreener,
        ),
        MarketDataService::ScreenerOption => (
            SchwabMarketDataFamily::ScreenerOption,
            ReadOnlyMarketDataLeaf::MarketScreener,
        ),
    }
}

fn rest_outcome_receipt(
    outcome: &RestExecutionOutcome,
) -> Result<&RawRestResponseReceipt, SchwabMarketDataActivationError> {
    match outcome {
        RestExecutionOutcome::Accepted(response) => Ok(response.capture().receipt()),
        RestExecutionOutcome::ProviderRejected(capture)
        | RestExecutionOutcome::InvalidPayload { capture, .. } => Ok(capture.receipt()),
        RestExecutionOutcome::AcceptedUserPreference(_)
        | RestExecutionOutcome::UserPreferenceRejected(_)
        | RestExecutionOutcome::InvalidUserPreference { .. } => {
            Err(SchwabMarketDataActivationError::AuthorityMismatch)
        }
    }
}

fn rest_outcome_accounting(
    outcome: &RestExecutionOutcome,
) -> Result<RestItemAccounting, SchwabMarketDataActivationError> {
    match outcome {
        RestExecutionOutcome::Accepted(response) => Ok(response.accounting()),
        RestExecutionOutcome::ProviderRejected(capture)
        | RestExecutionOutcome::InvalidPayload { capture, .. } => Ok(capture.accounting()),
        RestExecutionOutcome::AcceptedUserPreference(_)
        | RestExecutionOutcome::UserPreferenceRejected(_)
        | RestExecutionOutcome::InvalidUserPreference { .. } => {
            Err(SchwabMarketDataActivationError::AuthorityMismatch)
        }
    }
}

fn settle_rest_rate(
    budget: &SharedProviderBudget,
    permit: BudgetPermit,
    receipt: &RawRestResponseReceipt,
) -> Result<(), SchwabMarketDataActivationError> {
    if (200..=299).contains(&receipt.status()) {
        permit.release();
        return budget
            .record_success()
            .map_err(SchwabMarketDataActivationError::RateUnavailable);
    }
    let decision = if receipt.status() == 429 {
        let retry_after = receipt
            .headers()
            .iter()
            .find(|header| header.name() == "retry-after")
            .map(|header| header.value());
        apply_http_retry_after(budget, retry_after, 0)
    } else {
        budget.apply_refusal(0)
    };
    permit.release();
    match decision {
        BudgetDecision::Ready(unexpected) => {
            unexpected.release();
            Err(SchwabMarketDataActivationError::RateUnavailable(
                BudgetUnavailableReason::StateCorrupt,
            ))
        }
        BudgetDecision::WaitUntil(_deadline) => Ok(()),
        BudgetDecision::Unavailable(reason) => {
            Err(SchwabMarketDataActivationError::RateUnavailable(reason))
        }
    }
}

fn timestamp_from_millis(millis: u64) -> Result<Timestamp, SchwabMarketDataActivationError> {
    millis
        .checked_mul(1_000_000)
        .and_then(|value| i64::try_from(value).ok())
        .map(Timestamp::from_unix_nanos)
        .ok_or(SchwabMarketDataActivationError::AuthorityMismatch)
}

fn map_streamer_token_error(
    error: crate::provider_onboarding::SchwabOAuthRuntimeError,
) -> TokenAuthorityError {
    use crate::provider_onboarding::SchwabOAuthRuntimeError;
    match error {
        SchwabOAuthRuntimeError::ReauthorizationRequired
        | SchwabOAuthRuntimeError::MarketAuthorityRevoked
        | SchwabOAuthRuntimeError::ShuttingDown => TokenAuthorityError::ReauthorizationRequired,
        _ => TokenAuthorityError::Unavailable,
    }
}

fn map_streamer_activation_error(error: SchwabMarketDataActivationError) -> TokenAuthorityError {
    match error {
        SchwabMarketDataActivationError::DoctorRenewalRequired
        | SchwabMarketDataActivationError::AuthorityMismatch => {
            TokenAuthorityError::ReauthorizationRequired
        }
        SchwabMarketDataActivationError::Cancelled
        | SchwabMarketDataActivationError::RateDeferred
        | SchwabMarketDataActivationError::RateUnavailable(_)
        | SchwabMarketDataActivationError::RatePool(_)
        | SchwabMarketDataActivationError::Account(_)
        | SchwabMarketDataActivationError::Onboarding(_)
        | SchwabMarketDataActivationError::OAuth(_) => TokenAuthorityError::Unavailable,
    }
}

impl std::fmt::Debug for SchwabMarketDataAccountActivation {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SchwabMarketDataAccountActivation")
            .field("authority", &self.authority)
            .field("oauth", &"[PROTECTED TOKEN AUTHORITY]")
            .field("doctor_receipt", &self.doctor.receipt_sha256())
            .finish()
    }
}

impl ProviderAdapterActivation {
    /// Activates the exact OAuth epoch proven by the retained durable Schwab doctor receipt.
    pub(crate) async fn activate_schwab_market_data_account(
        &self,
        lease: ProviderActivationLease,
        oauth: SchwabOAuthMarketAuthority,
        cancellation: CancellationToken,
    ) -> Result<SchwabMarketDataAccountActivation, SchwabMarketDataActivationError> {
        if cancellation.is_cancelled() {
            return Err(SchwabMarketDataActivationError::Cancelled);
        }
        let binding = ProviderAccountBinding::try_from_lease(
            ProviderMarketAccount::SchwabMarketData,
            &lease,
        )?;
        if oauth.session_id() != lease.session_id() {
            return Err(SchwabMarketDataActivationError::AuthorityMismatch);
        }
        let doctor = lease
            .runtime_verification_evidence()
            .schwab_market_data_receipt()
            .cloned()
            .ok_or(SchwabMarketDataActivationError::AuthorityMismatch)?;
        let current = oauth.current_receipt().await?;
        if cancellation.is_cancelled() {
            return Err(SchwabMarketDataActivationError::Cancelled);
        }
        if doctor.receipt_sha256() != binding.verification_evidence()
            || doctor.access_token_generation() != current.generation().get()
            || doctor.market_data_principal_sha256()
                != lease
                    .account_digest()
                    .ok_or(SchwabMarketDataActivationError::AuthorityMismatch)?
        {
            return Err(SchwabMarketDataActivationError::AuthorityMismatch);
        }
        let rate_declaration = ProviderRateDeclaration::try_for_authorization_subject(
            lease
                .provider_budget_policy()
                .cloned()
                .ok_or(SchwabMarketDataActivationError::AuthorityMismatch)?,
            binding.subject(),
        )?;
        let rate_budget = Arc::new(self.provider_rate.register_budget(rate_declaration)?);
        let authority = Arc::new(ProviderAccountRuntimeAuthority::try_acquire(
            ProviderMarketAccount::SchwabMarketData,
            lease,
            Arc::clone(&self.onboarding),
            &self.app_config,
            self.provider_rate.clone(),
        )?);
        let activation = SchwabMarketDataAccountActivation {
            authority,
            oauth,
            doctor,
            doctor_generation: Arc::new(Mutex::new(SchwabDoctorGenerationDisposition::Current(
                current.generation().get(),
            ))),
            rate_budget,
            streamer_authority_issued: Mutex::new(false),
        };
        activation.require_current().await?;
        Ok(activation)
    }

    /// Prepares the sole current-quote start package after successful Schwab account activation.
    ///
    /// The caller supplies only a generation already registered by the application research
    /// authority and a bounded set minted from canonical definition/reference capabilities. This
    /// boundary revalidates both against the exact current doctor and source metadata before it
    /// binds durable publication. It never derives canonical identity from a ticker or provider
    /// response.
    pub(crate) async fn prepare_schwab_market_runtime_start(
        &self,
        activation: SchwabMarketDataAccountActivation,
        generation: ResearchProviderRuntimeGeneration,
        instruments: BoundedMarketInstrumentSet,
        display_bindings: Vec<MarketDataInstrumentBinding>,
        reference_identity: Option<MarketReferenceIdentityAuthority>,
        listing_reference: Option<ListingReferenceReadCapability>,
        identity_approvals: Vec<MarketReferenceIdentityApprovalV1>,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<PreparedSchwabMarketRuntimeStart, SchwabMarketRuntimeStartError> {
        if cancellation.is_cancelled() || Instant::now() >= deadline {
            return Err(SchwabMarketRuntimeStartError::Cancelled);
        }
        tokio::select! {
            biased;
            () = cancellation.cancelled() => {
                return Err(SchwabMarketRuntimeStartError::Cancelled);
            }
            current = activation.require_current() => current?,
        }
        let now = system_timestamp()?;
        let doctor = activation.doctor_receipt().clone();
        let metadata = generation.metadata();
        if generation.profile().as_str() != SCHWAB_MARKET_DATA_SURFACE_ID
            || super::require_runtime_lease(&generation, activation.lease()).is_err()
            || !activation.account_binding().validates_metadata(metadata)
            || metadata.source_id().as_str() != SCHWAB_QUOTE_SOURCE_ID
            || metadata.provider().as_str() != SCHWAB_QUOTE_PROVIDER
            || metadata.quality_ceiling() != DataQuality::DirectUnverified
            || metadata.budget_policy() != activation.lease().provider_budget_policy()
            || !metadata.is_effective_at(now)
            || !doctor.is_current_at(now)
            || !doctor.admits_source_start()
            || doctor.receipt_sha256() != activation.lease().runtime_evidence_digest()
        {
            return Err(SchwabMarketRuntimeStartError::AuthorityMismatch);
        }
        let registered = self
            .research
            .provider_runtime_generation(generation.profile())?
            .ok_or(SchwabMarketRuntimeStartError::GenerationUnavailable)?;
        if registered != generation {
            return Err(SchwabMarketRuntimeStartError::GenerationUnavailable);
        }

        let doctor_delay = doctor
            .quote_delay()
            .ok_or(SchwabMarketRuntimeStartError::QuoteDelayUnknown)?;
        if metadata.coverage().delay() != doctor_delay {
            return Err(SchwabMarketRuntimeStartError::QuoteDelayUnknown);
        }
        let live = metadata
            .coverage()
            .live()
            .ok_or(SchwabMarketRuntimeStartError::SourceEvidence)?;
        let venue = VenueId::try_from(SCHWAB_QUOTE_VENUE)
            .map_err(|_error| SchwabMarketRuntimeStartError::SourceEvidence)?;
        if live.provider_product().as_source_identifier().as_str() != SCHWAB_QUOTE_PRODUCT
            || live.provider_channel().as_source_identifier().as_str() != SCHWAB_QUOTE_CHANNEL
            || live.rule_for(LiveEventClass::Quote, None).is_none()
            || !metadata.coverage().topology().contains_venue(&venue)
        {
            return Err(SchwabMarketRuntimeStartError::SourceEvidence);
        }
        let nasdaq_generation = selected_nasdaq_generation(instruments.bindings())?;
        match (&nasdaq_generation, &reference_identity, &listing_reference) {
            (Some(expected), Some(_identity), Some(reader)) => {
                let current = reader
                    .current(deadline, &cancellation)
                    .map_err(|_error| SchwabMarketRuntimeStartError::IdentityResolutionRequired)?
                    .ok_or(SchwabMarketRuntimeStartError::IdentityResolutionRequired)?;
                if &current != expected {
                    return Err(SchwabMarketRuntimeStartError::IdentityResolutionRequired);
                }
            }
            (None, None, None) if identity_approvals.is_empty() => {}
            _ => return Err(SchwabMarketRuntimeStartError::IdentityResolutionRequired),
        }
        let bindings = exact_schwab_quote_bindings(
            instruments,
            identity_approvals,
            metadata,
            nasdaq_generation.as_ref(),
            now,
        )?;
        validate_schwab_display_bindings(&bindings, &display_bindings, now)?;
        let bounds = schwab_quote_runtime_bounds()?;
        // Provider-activation composition retains this protected authority internally; ordinary
        // runtime code receives only receipt/publication capabilities.
        let oauth = activation.oauth.clone();
        let oauth_receipt = tokio::select! {
            biased;
            () = cancellation.cancelled() => {
                return Err(SchwabMarketRuntimeStartError::Cancelled);
            }
            receipt = oauth.current_receipt() => receipt?,
        };
        if oauth_receipt.generation().get() != doctor.access_token_generation()
            || oauth_receipt
                .credential_authority()
                .application_credential_generation()
                != doctor.application_credential_generation()
            || oauth_receipt
                .credential_authority()
                .application_credential_reference_sha256()
                != doctor.application_credential_reference_sha256()
        {
            return Err(SchwabMarketRuntimeStartError::AuthorityMismatch);
        }
        let evidence =
            SchwabRestQuoteSourceEvidence::try_new(metadata.clone(), venue, doctor.clone())?;
        let analytical_dataset = DatasetId::try_from(super::MARKET_EVENT_ANALYTICAL_DATASET)
            .map_err(|_error| SchwabMarketRuntimeStartError::InvalidControls)?;
        let publication = self
            .research_mutation
            .bind_schwab_rest_quote_publication_package(
                &generation,
                doctor,
                oauth,
                oauth_receipt,
                analytical_dataset,
                SCHWAB_QUOTE_REQUEST_TIMEOUT,
            )?;
        if cancellation.is_cancelled() {
            return Err(SchwabMarketRuntimeStartError::Cancelled);
        }
        let poll_interval = schwab_quote_poll_interval(metadata, SCHWAB_QUOTE_REQUEST_TIMEOUT)?;
        Ok(PreparedSchwabMarketRuntimeStart {
            activation,
            provider_rate: self.provider_rate.clone(),
            generation,
            evidence,
            bindings,
            display_bindings: display_bindings.into_boxed_slice(),
            reference_identity,
            listing_reference,
            nasdaq_generation,
            bounds,
            telemetry: SchwabTransportTelemetry::default(),
            publication,
            request_timeout: SCHWAB_QUOTE_REQUEST_TIMEOUT,
            poll_interval,
        })
    }
}

fn exact_schwab_quote_bindings(
    instruments: BoundedMarketInstrumentSet,
    identity_approvals: Vec<MarketReferenceIdentityApprovalV1>,
    metadata: &SourceMetadata,
    nasdaq_generation: Option<&ListingReferenceGenerationReceipt>,
    at: Timestamp,
) -> Result<
    Vec<(
        MarketInstrumentBinding,
        Option<MarketReferenceIdentityApprovalV1>,
    )>,
    SchwabMarketRuntimeStartError,
> {
    let bindings = instruments.bindings();
    let mut retained = Vec::new();
    retained
        .try_reserve_exact(bindings.len())
        .map_err(|_error| SchwabMarketRuntimeStartError::InvalidControls)?;
    for binding in bindings {
        let approval = match binding.reference() {
            Some(MarketInstrumentReferenceBinding::NasdaqListing(_)) => {
                let mut matches = identity_approvals
                    .iter()
                    .filter(|approval| approval.instrument_id() == binding.instrument_id());
                let approval = matches
                    .next()
                    .cloned()
                    .ok_or(SchwabMarketRuntimeStartError::IdentityResolutionRequired)?;
                if matches.next().is_some() {
                    return Err(SchwabMarketRuntimeStartError::IdentityResolutionRequired);
                }
                Some(approval)
            }
            Some(MarketInstrumentReferenceBinding::AssignedExternalIdentifier(_)) => None,
            None => return Err(SchwabMarketRuntimeStartError::CanonicalIdentity),
        };
        retained.push((binding.clone(), approval));
    }
    let retained_approvals = retained
        .iter()
        .filter(|(_, approval)| approval.is_some())
        .count();
    if retained_approvals != identity_approvals.len() {
        return Err(SchwabMarketRuntimeStartError::IdentityResolutionRequired);
    }
    validate_exact_schwab_quote_bindings(
        &retained,
        metadata,
        nasdaq_generation,
        at,
        SCHWAB_QUOTE_MAXIMUM_SYMBOLS,
        true,
    )?;
    Ok(retained)
}

fn validate_schwab_display_bindings(
    strict: &[(
        MarketInstrumentBinding,
        Option<MarketReferenceIdentityApprovalV1>,
    )],
    display: &[MarketDataInstrumentBinding],
    at: Timestamp,
) -> Result<(), SchwabMarketRuntimeStartError> {
    if strict.len() != display.len()
        || strict.iter().any(|(binding, _approval)| {
            display
                .iter()
                .filter(|candidate| strict_and_display_definition_match(binding, candidate, at))
                .count()
                != 1
        })
        || display.iter().any(|candidate| {
            strict
                .iter()
                .filter(|(binding, _approval)| {
                    strict_and_display_definition_match(binding, candidate, at)
                })
                .count()
                != 1
        })
    {
        return Err(SchwabMarketRuntimeStartError::CanonicalIdentity);
    }
    Ok(())
}

fn strict_and_display_definition_match(
    strict: &MarketInstrumentBinding,
    display: &MarketDataInstrumentBinding,
    at: Timestamp,
) -> bool {
    let Some(record) = strict.canonical_market_data_definition() else {
        return false;
    };
    let definition = record.definition();
    record.published_at() <= at
        && interval_contains(definition.effective_interval(), at)
        && display.instrument_id() == strict.instrument_id()
        && display.instrument_id() == definition.instrument_id()
        && display.provisional_subscription_symbol() == strict.provider_symbol()
        && display.asset_class() == strict.definition().asset_class()
        && display.asset_class() == definition.asset_class()
        && display.priority() == strict.priority()
        && display.definition_reference_evidence() == definition.reference_evidence()
        && display.definition_effective() == definition.effective_interval()
        && display.definition_revision_digest() == record.revision_digest()
}

fn validate_exact_schwab_quote_bindings(
    bindings: &[(
        MarketInstrumentBinding,
        Option<MarketReferenceIdentityApprovalV1>,
    )],
    metadata: &SourceMetadata,
    nasdaq_generation: Option<&ListingReferenceGenerationReceipt>,
    at: Timestamp,
    maximum: usize,
    require_exact_coverage: bool,
) -> Result<(), SchwabMarketRuntimeStartError> {
    if bindings.is_empty() || maximum == 0 || bindings.len() > maximum {
        return Err(SchwabMarketRuntimeStartError::CanonicalIdentity);
    }
    let mut instrument_ids = BTreeSet::new();
    let mut provider_symbols = BTreeSet::new();
    for (binding, approval) in bindings {
        let canonical_definition = binding
            .canonical_market_data_definition()
            .ok_or(SchwabMarketRuntimeStartError::CanonicalIdentity)?;
        let identity = binding
            .provider_identity()
            .ok_or(SchwabMarketRuntimeStartError::CanonicalIdentity)?;
        let reference = binding
            .reference()
            .ok_or(SchwabMarketRuntimeStartError::CanonicalIdentity)?;
        if canonical_definition.published_at() > at
            || !interval_contains(canonical_definition.definition().effective_interval(), at)
            || !reference_is_current(binding, reference, approval.as_ref(), nasdaq_generation, at)
            || identity.source_id() != metadata.source_id()
            || identity.instrument_id() != binding.instrument_id()
            || binding.execution_terms().instrument_id() != binding.instrument_id()
            || binding.definition().provider_identity_at(
                identity.source_id(),
                identity.provider_instrument_id(),
                at,
            ) != Some(identity)
            || matches!(
                binding.definition().trading_status(),
                TradingStatus::Inactive | TradingStatus::Delisted
            )
            || !metadata
                .coverage()
                .asset_classes()
                .contains(&binding.definition().asset_class())
            || ProviderIdentifier::try_new(binding.provider_symbol().to_owned()).is_err()
            || !instrument_ids.insert(binding.instrument_id())
            || !provider_symbols.insert(binding.provider_symbol().to_owned())
        {
            return Err(SchwabMarketRuntimeStartError::CanonicalIdentity);
        }
    }
    let covered = metadata.coverage().instruments().instruments();
    if require_exact_coverage
        && (covered.len() != instrument_ids.len()
            || covered
                .iter()
                .any(|instrument| !instrument_ids.contains(instrument)))
    {
        return Err(SchwabMarketRuntimeStartError::CanonicalIdentity);
    }
    Ok(())
}

fn reference_is_current(
    binding: &MarketInstrumentBinding,
    reference: &MarketInstrumentReferenceBinding,
    approval: Option<&MarketReferenceIdentityApprovalV1>,
    nasdaq_generation: Option<&ListingReferenceGenerationReceipt>,
    at: Timestamp,
) -> bool {
    let Some(canonical_record) = binding.canonical_market_data_definition() else {
        return false;
    };
    let canonical_definition = canonical_record.definition();
    match reference {
        MarketInstrumentReferenceBinding::NasdaqListing(listing) => {
            let Some(approval) = approval else {
                return false;
            };
            listing.generation().rights_state() == ListingReferenceRightsState::AdmittedScoped
                && !listing.is_test_issue()
                && listing.effective_at() <= at
                && listing.generation().published_at() <= at
                && nasdaq_generation == Some(listing.generation())
                && approval.request().provider_instrument_id().as_str() == listing.provider_symbol()
                && approval.request().venue_id() == listing.listing_venue()
                && approval.instrument_id() == binding.instrument_id()
                && approval.asset_class() == canonical_definition.asset_class()
                && approval.quote_currency() == canonical_definition.quote_currency()
                && approval.definition_revision_digest() == canonical_record.revision_digest()
                && approval.definition_reference_evidence()
                    == canonical_definition.reference_evidence()
                && approval.quote_currency_evidence()
                    == canonical_definition.quote_currency_evidence()
                && approval.listing_payload_evidence() == listing.source_file().payload_evidence()
                && approval.listing_source_timestamp() == listing.effective_at()
                && approval.listing_observed_at() == listing.source_file().received_at()
                && approval.evaluated_at() < approval.expires_at()
                && at < approval.expires_at()
        }
        MarketInstrumentReferenceBinding::AssignedExternalIdentifier(record) => {
            approval.is_none()
                && record.assignment_verification() == AssignmentVerification::VerifiedAssigned
                && record.rights_policy().entitlement()
                    != IdentifierEntitlement::UnknownOrRestricted
                && interval_contains(record.validity(), at)
        }
    }
}

fn selected_nasdaq_generation(
    bindings: &[MarketInstrumentBinding],
) -> Result<Option<ListingReferenceGenerationReceipt>, SchwabMarketRuntimeStartError> {
    let mut selected = None;
    for binding in bindings {
        let Some(MarketInstrumentReferenceBinding::NasdaqListing(listing)) = binding.reference()
        else {
            continue;
        };
        match &selected {
            Some(current) if current != listing.generation() => {
                return Err(SchwabMarketRuntimeStartError::IdentityResolutionRequired);
            }
            Some(_) => {}
            None => selected = Some(listing.generation().clone()),
        }
    }
    Ok(selected)
}

fn interval_contains(interval: EffectiveInterval, at: Timestamp) -> bool {
    interval.starts_at() <= at && interval.ends_at().is_none_or(|end| at < end)
}

fn schwab_quote_poll_interval(
    metadata: &SourceMetadata,
    request_timeout: Duration,
) -> Result<Duration, SchwabMarketRuntimeStartError> {
    let freshness = metadata.freshness_policy();
    let freshness_ceiling = freshness
        .max_connection_idle_nanos()
        .min(freshness.max_transport_age_nanos())
        .min(freshness.max_source_age_nanos())
        .min(freshness.max_market_age_nanos());
    let response_margin = freshness_ceiling
        .checked_div(SCHWAB_QUOTE_FRESHNESS_MARGIN_DIVISOR)
        .ok_or(SchwabMarketRuntimeStartError::InvalidControls)?;
    let request_timeout_nanos = u64::try_from(request_timeout.as_nanos())
        .map_err(|_error| SchwabMarketRuntimeStartError::InvalidControls)?;
    let poll_nanos = response_margin
        .checked_sub(request_timeout_nanos)
        .filter(|value| *value > 0)
        .ok_or(SchwabMarketRuntimeStartError::InvalidControls)?;
    Ok(Duration::from_nanos(poll_nanos))
}

fn schwab_quote_runtime_bounds()
-> Result<SchwabRestQuoteRuntimeBounds, SchwabMarketRuntimeStartError> {
    let request_bytes = nonzero(SCHWAB_QUOTE_MAXIMUM_REQUEST_BYTES)?;
    let symbols = nonzero(SCHWAB_QUOTE_MAXIMUM_SYMBOLS)?;
    let response_bytes = nonzero(SCHWAB_QUOTE_MAXIMUM_RESPONSE_BYTES)?;
    let bounds = SchwabRestQuoteRuntimeBounds {
        request_admission: RequestAdmission::new(request_bytes, symbols),
        transport: RestTransportBounds::try_new(
            SCHWAB_QUOTE_CONNECT_TIMEOUT,
            SCHWAB_QUOTE_READ_TIMEOUT,
            SCHWAB_QUOTE_REQUEST_TIMEOUT,
            response_bytes,
            nonzero(SCHWAB_QUOTE_MAXIMUM_HEADERS)?,
            nonzero(SCHWAB_QUOTE_MAXIMUM_HEADER_BYTES)?,
        )
        .map_err(|_error| SchwabMarketRuntimeStartError::InvalidControls)?,
        parse: ParseBounds::new(
            response_bytes,
            symbols,
            nonzero(SCHWAB_QUOTE_MAXIMUM_JSON_NODES)?,
            nonzero(SCHWAB_QUOTE_MAXIMUM_JSON_DEPTH)?,
            SCHWAB_QUOTE_MAXIMUM_UNKNOWN_FIELDS,
            SCHWAB_QUOTE_MAXIMUM_UNKNOWN_BYTES,
        ),
        token: AccessTokenAdmission::new(request_bytes, SCHWAB_QUOTE_MINIMUM_TOKEN_LIFETIME),
    };
    Ok(bounds)
}

fn nonzero(value: usize) -> Result<NonZeroUsize, SchwabMarketRuntimeStartError> {
    NonZeroUsize::new(value).ok_or(SchwabMarketRuntimeStartError::InvalidControls)
}

fn system_timestamp() -> Result<Timestamp, SchwabMarketRuntimeStartError> {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_error| SchwabMarketRuntimeStartError::Clock)?;
    let nanos =
        i64::try_from(elapsed.as_nanos()).map_err(|_error| SchwabMarketRuntimeStartError::Clock)?;
    Ok(Timestamp::from_unix_nanos(nanos))
}

/// Fail-closed construction error for the one-use Schwab current-market start package.
#[derive(Debug, thiserror::Error)]
pub(crate) enum SchwabMarketRuntimeStartError {
    #[error("Schwab market runtime preparation was cancelled")]
    Cancelled,
    #[error("Schwab market runtime authority does not match the active OAuth and doctor epoch")]
    AuthorityMismatch,
    #[error("the exact registered Schwab publication generation is unavailable")]
    GenerationUnavailable,
    #[error("Schwab quote delay is unknown or conflicts with registered source metadata")]
    QuoteDelayUnknown,
    #[error("Schwab quote source evidence is incomplete or inconsistent")]
    SourceEvidence,
    #[error("Schwab quote runtime requires current accepted canonical provider identity")]
    CanonicalIdentity,
    #[error("Schwab quote runtime requires fresh canonical reference identity resolution")]
    IdentityResolutionRequired,
    #[error("Schwab quote code-owned resource controls are invalid")]
    InvalidControls,
    #[error("the trusted local clock is unavailable")]
    Clock,
    #[error(transparent)]
    Activation(#[from] SchwabMarketDataActivationError),
    #[error(transparent)]
    OAuth(#[from] crate::provider_onboarding::SchwabOAuthRuntimeError),
    #[error(transparent)]
    Research(#[from] ResearchIngestCompositionError),
    #[error(transparent)]
    Publication(#[from] SchwabMarketPublicationError),
    #[error(transparent)]
    Runtime(#[from] SchwabRestQuoteRuntimeError),
}

/// Schwab read-only market-data account activation failure.
#[derive(Debug, thiserror::Error)]
pub enum SchwabMarketDataActivationError {
    #[error("Schwab market-data activation was cancelled")]
    Cancelled,
    #[error("Schwab OAuth, doctor, account, or onboarding authority does not match")]
    AuthorityMismatch,
    #[error("Schwab OAuth token rotation requires a serialized doctor renewal")]
    DoctorRenewalRequired,
    #[error("Schwab shared account rate authority requires a later dispatch")]
    RateDeferred,
    #[error("Schwab shared account rate authority is unavailable: {0:?}")]
    RateUnavailable(BudgetUnavailableReason),
    #[error(transparent)]
    RatePool(#[from] market_squawk_sources::BudgetPoolError),
    #[error(transparent)]
    Account(#[from] ProviderAccountActivationError),
    #[error(transparent)]
    Onboarding(#[from] ProviderOnboardingError),
    #[error(transparent)]
    OAuth(#[from] crate::provider_onboarding::SchwabOAuthRuntimeError),
}
