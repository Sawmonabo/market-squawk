//! The single shared account request budget at native Schwab wire dispatch.
//! Exact ACK completion changes enforcement only; adaptive capacity requires sealed capture.
use crate::provider_activation::{
    ProviderAccountRuntimeCurrentness, SchwabMarketDataAccountActivation,
};
use market_squawk_adapter_schwab::{
    AccessTokenAdmission, ConnectionGeneration, MarketDataService, ParseBounds,
    SchwabAccessTokenSource, SchwabOAuthAuthorityReceipt, SchwabStreamerConnectionControlSource,
    SchwabStreamerConnectionPermit, SchwabStreamerDesiredStateSender, SchwabStreamerExecutor,
    SchwabStreamerRequestAcknowledgement, SchwabStreamerRequestPermit,
    SchwabStreamerRuntimeAuthority, SchwabStreamerRuntimeEvent, SchwabTransportError,
    SchwabTransportTelemetry, StreamerAdmission, StreamerBootstrap, StreamerCaptureSink,
    StreamerRunExit, StreamerSubscription, StreamerTransportBounds, TokenAuthorityError,
    TransientAccessToken,
};
use market_squawk_domain::Timestamp;
use market_squawk_sources::{
    BudgetDecision, BudgetDispatchDecision, BudgetPermit, BudgetReservationDecision,
    BudgetUnavailableReason, SharedProviderBudget,
};
use std::{
    collections::BTreeSet,
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio_util::sync::CancellationToken;

#[derive(Default)]
struct SchwabStreamerRateState {
    generation: Option<ConnectionGeneration>,
}

struct SchwabStreamerAccountRateAuthority {
    admitted_services: BTreeSet<MarketDataService>,
    currentness: ProviderAccountRuntimeCurrentness,
    activation: Arc<SchwabMarketDataAccountActivation>,
    oauth_receipt: SchwabOAuthAuthorityReceipt,
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
        if !matches!(event, SchwabStreamerRuntimeEvent::Disconnected { .. }) {
            self.require_current()?;
        }
        let mut state = self
            .state
            .lock()
            .map_err(|_| SchwabTransportError::Protocol)?;
        match event {
            SchwabStreamerRuntimeEvent::Connected { generation }
            | SchwabStreamerRuntimeEvent::Frame { generation, .. } => {
                if state.generation != Some(generation) {
                    return Err(SchwabTransportError::Protocol);
                }
            }
            SchwabStreamerRuntimeEvent::Disconnected { generation, .. } => {
                if state.generation != Some(generation) {
                    return Err(SchwabTransportError::Protocol);
                }
                state.generation = None;
            }
            SchwabStreamerRuntimeEvent::ConnectAttempt { .. }
            | SchwabStreamerRuntimeEvent::QueuePressure => {}
        }
        Ok(())
    }

    fn commit_connection<'a>(
        &'a self,
        generation: ConnectionGeneration,
        cancellation: &'a CancellationToken,
        deadline: Instant,
    ) -> Pin<
        Box<
            dyn Future<
                    Output = Result<Box<dyn SchwabStreamerConnectionPermit>, SchwabTransportError>,
                > + Send
                + 'a,
        >,
    > {
        Box::pin(async move {
            self.require_current()?;
            if !self.currentness.is_current_now()
                || self
                    .state
                    .lock()
                    .map_err(|_| SchwabTransportError::Protocol)?
                    .generation
                    .is_some()
            {
                return Err(SchwabTransportError::TokenRefreshRequired);
            }
            let permit =
                acquire_streamer_rate_permit(&self.budget, self, cancellation, deadline).await?;
            self.state
                .lock()
                .map_err(|_| SchwabTransportError::Protocol)?
                .generation = Some(generation);
            Ok(Box::new(SchwabStreamerAccountConnectionPermit {
                budget: Arc::clone(&self.budget),
                permit,
            }) as Box<dyn SchwabStreamerConnectionPermit>)
        })
    }

    fn commit_request<'a>(
        &'a self,
        generation: ConnectionGeneration,
        service: Option<MarketDataService>,
        command: &'a str,
        request_id: &'a str,
        request_payload_sha256: market_squawk_domain::EvidenceDigest,
        request_payload_bytes: u64,
        cancellation: &'a CancellationToken,
        deadline: Instant,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<Box<dyn SchwabStreamerRequestPermit>, SchwabTransportError>>
                + Send
                + 'a,
        >,
    > {
        Box::pin(async move {
            self.require_current()?;
            if !self.currentness.is_current_now()
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
                || self
                    .state
                    .lock()
                    .map_err(|_| SchwabTransportError::Protocol)?
                    .generation
                    != Some(generation)
            {
                return Err(SchwabTransportError::Protocol);
            }
            let permit =
                acquire_streamer_rate_permit(&self.budget, self, cancellation, deadline).await?;
            Ok(Box::new(SchwabStreamerAccountRatePermit {
                generation,
                service,
                command: command.to_owned().into_boxed_str(),
                request_id: request_id.to_owned().into_boxed_str(),
                request_payload_sha256,
                request_payload_bytes,
                budget: Arc::clone(&self.budget),
                permit,
            }) as Box<dyn SchwabStreamerRequestPermit>)
        })
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
    fn settle<'a>(
        self: Box<Self>,
        acknowledgement: SchwabStreamerRequestAcknowledgement,
        cancellation: &'a CancellationToken,
        deadline: Instant,
    ) -> Pin<Box<dyn Future<Output = Result<(), SchwabTransportError>> + Send + 'a>> {
        Box::pin(async move {
            ensure_streamer_rate_active(cancellation, deadline)?;
            if acknowledgement.generation() != self.generation
                || acknowledgement.service() != self.service
                || acknowledgement.command() != self.command.as_ref()
                || acknowledgement.request_id() != self.request_id.as_ref()
                || acknowledgement.request_payload_sha256() != self.request_payload_sha256
                || acknowledgement.request_payload_bytes() != self.request_payload_bytes
            {
                return Err(SchwabTransportError::Protocol);
            }
            let result = if acknowledgement.succeeded() {
                self.budget
                    .record_success()
                    .map_err(map_streamer_budget_error)
            } else {
                settle_streamer_budget_decision(self.budget.apply_refusal(0))
            };
            self.permit.release();
            result
        })
    }
}

#[derive(Debug)]
struct SchwabStreamerAccountConnectionPermit {
    budget: Arc<SharedProviderBudget>,
    permit: BudgetPermit,
}

impl SchwabStreamerConnectionPermit for SchwabStreamerAccountConnectionPermit {
    fn connected<'a>(
        self: Box<Self>,
        cancellation: &'a CancellationToken,
        deadline: Instant,
    ) -> Pin<Box<dyn Future<Output = Result<(), SchwabTransportError>> + Send + 'a>> {
        Box::pin(async move {
            ensure_streamer_rate_active(cancellation, deadline)?;
            let result = self
                .budget
                .record_success()
                .map_err(map_streamer_budget_error);
            self.permit.release();
            result
        })
    }
}

async fn acquire_streamer_rate_permit(
    budget: &SharedProviderBudget,
    authority: &SchwabStreamerAccountRateAuthority,
    cancellation: &CancellationToken,
    deadline: Instant,
) -> Result<BudgetPermit, SchwabTransportError> {
    loop {
        ensure_streamer_rate_active(cancellation, deadline)?;
        if authority.require_current().is_err() {
            return Err(SchwabTransportError::TokenRefreshRequired);
        }
        let wait = match budget.try_reserve_request() {
            BudgetReservationDecision::Ready(reservation) => {
                ensure_streamer_rate_active(cancellation, deadline)?;
                if authority.require_current().is_err() {
                    return Err(SchwabTransportError::TokenRefreshRequired);
                }
                match reservation.commit_dispatch() {
                    BudgetDispatchDecision::Ready(permit) => return Ok(permit),
                    BudgetDispatchDecision::WaitUntil(until) => budget
                        .remaining_wait(until)
                        .map_err(map_streamer_budget_error)?,
                    BudgetDispatchDecision::Unavailable(
                        BudgetUnavailableReason::ConcurrencyExhausted,
                    ) => Duration::from_millis(25),
                    BudgetDispatchDecision::Unavailable(reason) => {
                        return Err(map_streamer_budget_error(reason));
                    }
                }
            }
            BudgetReservationDecision::WaitUntil(until) => budget
                .remaining_wait(until)
                .map_err(map_streamer_budget_error)?,
            BudgetReservationDecision::Unavailable(
                BudgetUnavailableReason::ConcurrencyExhausted,
            ) => Duration::from_millis(25),
            BudgetReservationDecision::Unavailable(reason) => {
                return Err(map_streamer_budget_error(reason));
            }
        };
        let wake = Instant::now()
            .checked_add(wait)
            .ok_or(SchwabTransportError::Overflow)?;
        if wake >= deadline {
            return Err(SchwabTransportError::Deadline);
        }
        tokio::select! {
            biased;
            () = cancellation.cancelled() => return Err(SchwabTransportError::Cancelled),
            () = tokio::time::sleep_until(tokio::time::Instant::from_std(wake)) => {}
        }
    }
}

fn ensure_streamer_rate_active(
    cancellation: &CancellationToken,
    deadline: Instant,
) -> Result<(), SchwabTransportError> {
    if cancellation.is_cancelled() {
        Err(SchwabTransportError::Cancelled)
    } else if Instant::now() >= deadline {
        Err(SchwabTransportError::Deadline)
    } else {
        Ok(())
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

impl SchwabStreamerAccountRateAuthority {
    fn require_current(&self) -> Result<(), SchwabTransportError> {
        if !self.currentness.is_current_now() {
            return Err(SchwabTransportError::TokenRefreshRequired);
        }
        self.activation
            .oauth_receipt_currentness()
            .validate_current_receipt(self.oauth_receipt)
            .map_err(|_| SchwabTransportError::TokenRefreshRequired)?;
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| SchwabTransportError::Protocol)?
            .as_nanos();
        let now = Timestamp::from_unix_nanos(
            i64::try_from(nanos).map_err(|_| SchwabTransportError::Overflow)?,
        );
        let doctor = self.activation.doctor_receipt();
        if !doctor.is_current_at(now)
            || !self.oauth_receipt.matches_market_data_authorization(doctor)
            || self.admitted_services.is_empty()
            || self.admitted_services.len() > 12
            || self.admitted_services.iter().any(|service| {
                !doctor
                    .observation()
                    .families
                    .iter()
                    .any(|family| streamer_service(family.family) == Some(*service))
            })
        {
            return Err(SchwabTransportError::TokenRefreshRequired);
        }
        // This authorizes a bounded authenticated read-only request, not family availability.
        // Only the returned same-family sealed ACK/data proof can qualify canonical publication.
        Ok(())
    }
}

#[derive(Debug)]
struct BoundTokenSource {
    authority: Arc<SchwabStreamerAccountRateAuthority>,
}
impl SchwabAccessTokenSource for BoundTokenSource {
    fn acquire(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<TransientAccessToken, TokenAuthorityError>> + Send + '_>>
    {
        Box::pin(async move {
            self.authority
                .require_current()
                .map_err(|_| TokenAuthorityError::ReauthorizationRequired)?;
            let (token, epoch) = self
                .authority
                .activation
                .acquire_runtime_publication_attempt()
                .await
                .map_err(|_| TokenAuthorityError::ReauthorizationRequired)?;
            if epoch.receipt() != self.authority.oauth_receipt {
                return Err(TokenAuthorityError::ReauthorizationRequired);
            }
            epoch
                .validate_current(self.authority.oauth_receipt)
                .map_err(|_| TokenAuthorityError::ReauthorizationRequired)?;
            Ok(token)
        })
    }
}

/// Holds the exclusive socket claim on one retained non-clone account activation.
#[derive(Debug)]
pub(crate) struct GovernedSchwabStreamer {
    authority: Arc<SchwabStreamerAccountRateAuthority>,
    executor: SchwabStreamerExecutor,
    _account_streamer_claim: tokio::sync::OwnedMutexGuard<()>,
}
impl GovernedSchwabStreamer {
    #[allow(
        clippy::too_many_arguments,
        reason = "all native and account authorities remain explicit"
    )]
    pub(crate) async fn try_new(
        activation: Arc<SchwabMarketDataAccountActivation>,
        provider_rate: &market_squawk_sources::ProviderRateAuthority,
        services: BTreeSet<MarketDataService>,
        control: Arc<dyn SchwabStreamerConnectionControlSource>,
        admission: StreamerAdmission,
        bounds: StreamerTransportBounds,
        parse: ParseBounds,
        token_admission: AccessTokenAdmission,
        telemetry: SchwabTransportTelemetry,
    ) -> Result<Self, SchwabTransportError> {
        let account_streamer_claim = activation.claim_streamer()?;
        if services.is_empty() || services.len() > admission.max_services() {
            return Err(SchwabTransportError::InvalidConfiguration);
        }
        activation
            .require_runtime_current()
            .await
            .map_err(|_| SchwabTransportError::TokenRefreshRequired)?;
        let policy = activation
            .lease()
            .provider_budget_policy()
            .cloned()
            .ok_or(SchwabTransportError::InvalidConfiguration)?;
        if policy.weighted_window_count() != 0 {
            return Err(SchwabTransportError::InvalidConfiguration);
        }
        let declaration =
            market_squawk_sources::ProviderRateDeclaration::try_for_authorization_subject(
                policy,
                activation.account_binding().subject(),
            )
            .map_err(|_| SchwabTransportError::InvalidConfiguration)?;
        let budget = provider_rate
            .register_budget(declaration)
            .map_err(|_| SchwabTransportError::Protocol)?;
        let oauth_receipt = activation
            .runtime_oauth_receipt()
            .await
            .map_err(|_| SchwabTransportError::TokenRefreshRequired)?;
        let authority = Arc::new(SchwabStreamerAccountRateAuthority {
            admitted_services: services,
            currentness: activation.runtime_currentness(),
            activation,
            oauth_receipt,
            budget: Arc::new(budget),
            state: Mutex::new(SchwabStreamerRateState::default()),
        });
        authority.require_current()?;
        let token = Arc::new(BoundTokenSource {
            authority: Arc::clone(&authority),
        });
        Ok(Self {
            authority: Arc::clone(&authority),
            _account_streamer_claim: account_streamer_claim,
            executor: SchwabStreamerExecutor::try_production(
                token,
                control,
                admission,
                bounds,
                parse,
                token_admission,
                telemetry,
                authority,
            )?,
        })
    }
    pub(crate) fn replace_desired(
        &mut self,
        subscription: StreamerSubscription,
    ) -> Result<(), SchwabTransportError> {
        if !self
            .authority
            .admitted_services
            .contains(&subscription.service())
        {
            return Err(SchwabTransportError::Protocol);
        }
        self.executor
            .replace_desired(subscription)
            .map_err(Into::into)
    }
    pub(crate) fn take_desired_state_sender(&mut self) -> Option<SchwabStreamerDesiredStateSender> {
        self.executor.take_desired_state_sender()
    }
    pub(crate) async fn run(
        &mut self,
        bootstrap: &StreamerBootstrap,
        sink: &mut dyn StreamerCaptureSink,
        cancellation: CancellationToken,
    ) -> Result<StreamerRunExit, SchwabTransportError> {
        self.authority.require_current()?;
        if bootstrap.market_data_principal_sha256()
            != self
                .authority
                .activation
                .doctor_receipt()
                .market_data_principal_sha256()
                .bytes()
        {
            return Err(SchwabTransportError::Protocol);
        }
        self.executor.run(bootstrap, sink, cancellation).await
    }
}

const fn streamer_service(
    family: market_squawk_sources::SchwabMarketDataFamily,
) -> Option<MarketDataService> {
    match family {
        market_squawk_sources::SchwabMarketDataFamily::LevelOneEquities => {
            Some(MarketDataService::LevelOneEquities)
        }
        market_squawk_sources::SchwabMarketDataFamily::LevelOneOptions => {
            Some(MarketDataService::LevelOneOptions)
        }
        market_squawk_sources::SchwabMarketDataFamily::LevelOneFutures => {
            Some(MarketDataService::LevelOneFutures)
        }
        market_squawk_sources::SchwabMarketDataFamily::LevelOneFuturesOptions => {
            Some(MarketDataService::LevelOneFuturesOptions)
        }
        market_squawk_sources::SchwabMarketDataFamily::LevelOneForex => {
            Some(MarketDataService::LevelOneForex)
        }
        market_squawk_sources::SchwabMarketDataFamily::NyseBook => {
            Some(MarketDataService::NyseBook)
        }
        market_squawk_sources::SchwabMarketDataFamily::NasdaqBook => {
            Some(MarketDataService::NasdaqBook)
        }
        market_squawk_sources::SchwabMarketDataFamily::OptionsBook => {
            Some(MarketDataService::OptionsBook)
        }
        market_squawk_sources::SchwabMarketDataFamily::ChartEquity => {
            Some(MarketDataService::ChartEquity)
        }
        market_squawk_sources::SchwabMarketDataFamily::ChartFutures => {
            Some(MarketDataService::ChartFutures)
        }
        market_squawk_sources::SchwabMarketDataFamily::ScreenerEquity => {
            Some(MarketDataService::ScreenerEquity)
        }
        market_squawk_sources::SchwabMarketDataFamily::ScreenerOption => {
            Some(MarketDataService::ScreenerOption)
        }
        _ => None,
    }
}
