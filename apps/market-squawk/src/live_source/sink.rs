//! Capture-first Coinbase sink and bounded live-route activation.

use std::{
    collections::{HashMap, VecDeque},
    mem::size_of,
    time::Instant,
};

use super::{
    provider::ProductionMarketDecoder,
    route_actor::{RouteActivationBinding, RouteActivationPublisher},
    subscription_state::{
        GenerationIdentity, SubscriptionFailure, SubscriptionPhase, SubscriptionStateMachine,
    },
};
use market_squawk_domain::{ExactPayloadEvidence, StreamIntegrityState, Timestamp};
use market_squawk_live::{LiveIngressBindError, LiveIngressError, LiveRuntimeIngress, ShardKey};
use market_squawk_platform::{CapturePublishError, RawCapturePublisher};
use market_squawk_sources::{
    AuthoritativeSourceRegistry, AuthorizationHealth, BudgetHealth, BudgetPermitLease,
    CaptureGenerationCapabilities, ConnectionLiveness, ControlFrameKind, CurrentHealthReporter,
    CurrentSourceSession, DecodeInternalError, DecodeOutcome, FreshnessPolicy, MarketDecoder,
    ProviderTimestampEvidence, QuarantineReason, RawMarketFrame, RawMarketSink, RegistryError,
    ResynchronizationReason, SinkError, SourceHealthError, SourceHealthSnapshot, SourceMetadata,
    SourceMetadataProvider, ValidatedSessionDecodeOutcome,
};
use thiserror::Error;

/// Input capabilities consumed by one exact-generation production sink.
#[derive(Debug)]
pub(super) struct ProductionRawMarketSinkInput<'a> {
    pub(super) capture: RawCapturePublisher<CaptureGenerationCapabilities>,
    pub(super) registry: &'a mut AuthoritativeSourceRegistry,
    pub(super) session: &'a CurrentSourceSession,
    pub(super) health_reporter: CurrentHealthReporter,
    pub(super) decoder: ProductionMarketDecoder,
    pub(super) subscription: SubscriptionStateMachine,
    pub(super) live_ingress: LiveRuntimeIngress,
    pub(super) routes: Vec<RouteActivationPublisher>,
}

/// Input capabilities for an adapter that already produced capture-bound decoder outcomes.
#[derive(Debug)]
pub(super) struct ProductionPredecodedMarketSinkInput<'a> {
    pub(super) capture: RawCapturePublisher<CaptureGenerationCapabilities>,
    pub(super) registry: &'a mut AuthoritativeSourceRegistry,
    pub(super) session: &'a CurrentSourceSession,
    pub(super) health_reporter: CurrentHealthReporter,
    pub(super) metadata: SourceMetadata,
    pub(super) subscription: SubscriptionStateMachine,
    pub(super) live_ingress: LiveRuntimeIngress,
    pub(super) routes: Vec<RouteActivationPublisher>,
}

/// Exact capture/session/health/live-route bridge used directly by the Coinbase reader.
#[derive(Debug)]
pub(super) struct ProductionRawMarketSink<'a> {
    capture: RawCapturePublisher<CaptureGenerationCapabilities>,
    registry: &'a mut AuthoritativeSourceRegistry,
    session: &'a CurrentSourceSession,
    health_reporter: CurrentHealthReporter,
    decoder: Option<ProductionMarketDecoder>,
    metadata: SourceMetadata,
    generation: GenerationIdentity,
    subscription: SubscriptionStateMachine,
    pending_data: PendingDataBuffer,
    live_ingress: LiveRuntimeIngress,
    routes: HashMap<ShardKey, RouteActivationPublisher>,
    last_transport_at: Option<Timestamp>,
    last_market_at: Option<Timestamp>,
    last_source_at: Option<Timestamp>,
    health_rebind_at: Option<Timestamp>,
    health_valid_until: Option<Timestamp>,
    acknowledgement_evidence: Option<ExactPayloadEvidence>,
    active_request_budget: Option<BudgetPermitLease>,
    terminal: Option<ProductionSinkFailure>,
}

impl<'a> ProductionRawMarketSink<'a> {
    pub(super) fn try_new(
        input: ProductionRawMarketSinkInput<'a>,
    ) -> Result<Self, ProductionSinkConstructionError> {
        let metadata = input.decoder.metadata().clone();
        Self::try_new_inner(
            input.capture,
            input.registry,
            input.session,
            input.health_reporter,
            Some(input.decoder),
            metadata,
            input.subscription,
            input.live_ingress,
            input.routes,
        )
    }

    pub(super) fn try_new_predecoded(
        input: ProductionPredecodedMarketSinkInput<'a>,
    ) -> Result<Self, ProductionSinkConstructionError> {
        Self::try_new_inner(
            input.capture,
            input.registry,
            input.session,
            input.health_reporter,
            None,
            input.metadata,
            input.subscription,
            input.live_ingress,
            input.routes,
        )
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "the common captured-outcome authority keeps every capability explicit"
    )]
    fn try_new_inner(
        capture: RawCapturePublisher<CaptureGenerationCapabilities>,
        registry: &'a mut AuthoritativeSourceRegistry,
        session: &'a CurrentSourceSession,
        health_reporter: CurrentHealthReporter,
        decoder: Option<ProductionMarketDecoder>,
        metadata: SourceMetadata,
        subscription: SubscriptionStateMachine,
        live_ingress: LiveRuntimeIngress,
        route_publishers: Vec<RouteActivationPublisher>,
    ) -> Result<Self, ProductionSinkConstructionError> {
        if route_publishers.is_empty() {
            return Err(ProductionSinkConstructionError::MissingRoutes);
        }
        let mut routes = HashMap::new();
        routes
            .try_reserve(route_publishers.len())
            .map_err(|_error| ProductionSinkConstructionError::AllocationFailed)?;
        for route in route_publishers {
            if routes.insert(route.route().clone(), route).is_some() {
                return Err(ProductionSinkConstructionError::DuplicateRoute);
            }
        }
        let pending_data =
            PendingDataBuffer::try_new(subscription.pre_acknowledgement_data_limits())?;
        Ok(Self {
            capture,
            registry,
            session,
            health_reporter,
            decoder,
            metadata,
            generation: GenerationIdentity::from_session(session),
            subscription,
            pending_data,
            live_ingress,
            routes,
            last_transport_at: None,
            last_market_at: None,
            last_source_at: None,
            health_rebind_at: None,
            health_valid_until: None,
            acknowledgement_evidence: None,
            active_request_budget: None,
            terminal: None,
        })
    }

    pub(super) const fn terminal_failure(&self) -> Option<ProductionSinkFailure> {
        self.terminal
    }

    fn process_frame(&mut self, frame: RawMarketFrame) -> Result<(), ProductionSinkFailure> {
        let receipt = self.capture_frame(&frame)?;
        let validated_frame = self
            .session
            .validate_live_frame(&frame)
            .map_err(ProductionSinkFailure::Registry)?;
        let outcome = self
            .decoder
            .as_mut()
            .ok_or(ProductionSinkFailure::MissingDecoder)?
            .decode(&validated_frame)
            .map_err(ProductionSinkFailure::Decode)?;
        self.process_captured_outcome(outcome, receipt)
    }

    /// Captures and validates one adapter-predecoded frame without decoding it a second time.
    pub(super) fn try_capture_predecoded(
        &mut self,
        frame: &RawMarketFrame,
    ) -> Result<market_squawk_sources::CaptureAdmissionReceipt, SinkError> {
        if let Some(failure) = self.terminal {
            return Err(failure.as_sink_error());
        }
        if self.decoder.is_some() {
            return Err(self.fail(ProductionSinkFailure::UnexpectedDecoder));
        }
        let receipt = self
            .capture_frame(frame)
            .map_err(|failure| self.fail(failure))?;
        self.session
            .validate_live_frame(frame)
            .map_err(ProductionSinkFailure::Registry)
            .map_err(|failure| self.fail(failure))?;
        Ok(receipt)
    }

    pub(super) fn try_process_captured_outcome(
        &mut self,
        outcome: DecodeOutcome,
        receipt: market_squawk_sources::CaptureAdmissionReceipt,
    ) -> Result<(), SinkError> {
        if let Some(failure) = self.terminal {
            return Err(failure.as_sink_error());
        }
        self.process_captured_outcome(outcome, receipt)
            .map_err(|failure| self.fail(failure))
    }

    fn capture_frame(
        &mut self,
        frame: &RawMarketFrame,
    ) -> Result<market_squawk_sources::CaptureAdmissionReceipt, ProductionSinkFailure> {
        let receipt = self
            .capture
            .try_publish(frame)
            .map_err(ProductionSinkFailure::Capture)?;
        self.poll_route_failures()?;
        Ok(receipt)
    }

    fn process_captured_outcome(
        &mut self,
        outcome: DecodeOutcome,
        receipt: market_squawk_sources::CaptureAdmissionReceipt,
    ) -> Result<(), ProductionSinkFailure> {
        self.poll_route_failures()?;
        let latest_source_at = latest_source_timestamp(&outcome);
        let decoded_route = decoded_route(&outcome)?;
        let received_at = outcome.evidence().received_at();
        let validated_session = self
            .registry
            .validate_session(self.session, received_at)
            .map_err(ProductionSinkFailure::Registry)?;
        let disposition = validated_session
            .validate_decode_outcome_owned(outcome, receipt)
            .map_err(ProductionSinkFailure::Registry)?;
        self.last_transport_at = Some(received_at);
        match disposition {
            ValidatedSessionDecodeOutcome::Control(control) => self.process_control(
                control.kind(),
                control.evidence().payload_digest(),
                received_at,
            ),
            ValidatedSessionDecodeOutcome::Data(data) => self.process_data(
                data,
                decoded_route.ok_or(ProductionSinkFailure::RouteUnavailableBeforeUpgrade)?,
                latest_source_at,
                received_at,
            ),
            ValidatedSessionDecodeOutcome::Ignored(ignored) => self
                .subscription
                .observe_ignored(&self.generation, Instant::now(), ignored.reason())
                .map(|_phase| ())
                .map_err(ProductionSinkFailure::Subscription),
            ValidatedSessionDecodeOutcome::Resynchronize(recovery) => {
                Err(ProductionSinkFailure::Resynchronize(recovery.reason()))
            }
            ValidatedSessionDecodeOutcome::Quarantine(quarantine) => {
                Err(ProductionSinkFailure::Quarantine(quarantine.reason()))
            }
        }
    }

    fn process_control(
        &mut self,
        kind: ControlFrameKind,
        payload_digest: market_squawk_domain::EvidenceDigest,
        received_at: Timestamp,
    ) -> Result<(), ProductionSinkFailure> {
        let now = Instant::now();
        match kind {
            ControlFrameKind::SubscriptionAcknowledgement => {
                self.subscription
                    .observe_validated_acknowledgement(&self.generation, now)
                    .map_err(ProductionSinkFailure::Subscription)?;
                self.acknowledgement_evidence =
                    Some(ExactPayloadEvidence::from_content_digest(payload_digest));
                self.flush_pending_data(received_at)?;
            }
            ControlFrameKind::Heartbeat => {
                self.subscription
                    .observe_heartbeat(&self.generation, now)
                    .map_err(ProductionSinkFailure::Subscription)?;
            }
            ControlFrameKind::Ping
            | ControlFrameKind::Pong
            | ControlFrameKind::ProviderFlowControl => {
                self.subscription
                    .observe_control(&self.generation, now, kind)
                    .map_err(ProductionSinkFailure::Subscription)?;
            }
        }
        if self
            .health_valid_until
            .is_some_and(|deadline| received_at > deadline)
            && self.last_market_at.is_some()
        {
            self.record_health(received_at)?;
            self.health_rebind_at = None;
            self.health_valid_until = None;
        }
        Ok(())
    }

    fn process_data(
        &mut self,
        data: market_squawk_sources::CapturedDecodedProviderBatch,
        route: ShardKey,
        latest_source_at: Option<Timestamp>,
        received_at: Timestamp,
    ) -> Result<(), ProductionSinkFailure> {
        if self.subscription.phase() == SubscriptionPhase::AwaitingAcknowledgement
            && self.subscription.accepts_first_validated_data()
        {
            self.subscription
                .observe_validated_acknowledgement(&self.generation, Instant::now())
                .map_err(ProductionSinkFailure::Subscription)?;
            self.acknowledgement_evidence = Some(ExactPayloadEvidence::from_content_digest(
                data.evidence().payload_digest(),
            ));
        }
        if self.subscription.phase() == SubscriptionPhase::AwaitingAcknowledgement
            && self.pending_data.is_enabled()
        {
            return self
                .pending_data
                .try_push(data, route, latest_source_at, received_at);
        }
        self.process_active_data(data, route, latest_source_at, received_at, received_at)
    }

    fn flush_pending_data(
        &mut self,
        acknowledgement_received_at: Timestamp,
    ) -> Result<(), ProductionSinkFailure> {
        while let Some(pending) = self.pending_data.try_pop_front()? {
            self.process_active_data(
                pending.data,
                pending.route,
                pending.latest_source_at,
                pending.received_at,
                acknowledgement_received_at.max(pending.received_at),
            )?;
        }
        Ok(())
    }

    fn process_active_data(
        &mut self,
        data: market_squawk_sources::CapturedDecodedProviderBatch,
        route: ShardKey,
        latest_source_at: Option<Timestamp>,
        received_at: Timestamp,
        health_observed_at: Timestamp,
    ) -> Result<(), ProductionSinkFailure> {
        self.subscription
            .observe_data(&self.generation, Instant::now())
            .map_err(ProductionSinkFailure::Subscription)?;
        self.last_market_at = Some(received_at);
        if let Some(source_at) = latest_source_at {
            self.last_source_at = Some(
                self.last_source_at
                    .map_or(source_at, |previous| previous.max(source_at)),
            );
        }
        let requires_rebind = self
            .health_rebind_at
            .is_none_or(|deadline| received_at >= deadline);
        let dormant = if requires_rebind {
            Some(self.prepare_route(&route)?)
        } else {
            None
        };
        if requires_rebind {
            self.record_health(health_observed_at)?;
        }
        let current = self
            .registry
            .validate_current_authority(self.session)
            .map_err(ProductionSinkFailure::Registry)?;
        let batches = current
            .validate_data_outcome_owned(data)
            .map_err(ProductionSinkFailure::Registry)?;
        if batches.len() != 1 {
            return Err(ProductionSinkFailure::UnexpectedRouteCount);
        }
        let batch = batches
            .into_iter()
            .next()
            .ok_or(ProductionSinkFailure::UnexpectedRouteCount)?;
        let valid_until = batch.current_lease().valid_until();
        let current_route = ShardKey::new(batch.key().venue().clone(), batch.key().instrument());
        if current_route != route {
            return Err(ProductionSinkFailure::RouteMismatch);
        }
        let manager = self
            .routes
            .get_mut(&route)
            .ok_or(ProductionSinkFailure::UnknownRoute)?;
        if let Some(dormant) = dormant {
            manager.start_activation(dormant, batch)?;
            self.health_rebind_at = Some(rebind_at(
                health_observed_at,
                self.metadata.freshness_policy(),
            )?);
            self.health_valid_until = Some(valid_until);
        } else {
            manager.try_publish(batch)?;
        }
        Ok(())
    }

    fn prepare_route(
        &mut self,
        route: &ShardKey,
    ) -> Result<RouteActivationBinding, ProductionSinkFailure> {
        let manager = self
            .routes
            .get_mut(route)
            .ok_or(ProductionSinkFailure::UnknownRoute)?;
        manager.prepare(&self.live_ingress)
    }

    fn poll_route_failures(&mut self) -> Result<(), ProductionSinkFailure> {
        for route in self.routes.values_mut() {
            route.check_failure()?;
        }
        Ok(())
    }

    fn record_health(&mut self, observed_at: Timestamp) -> Result<(), ProductionSinkFailure> {
        let metadata = &self.metadata;
        let authorization_deadline = metadata
            .authorization()
            .inclusive_authorization_deadline()
            .ok_or(ProductionSinkFailure::UnboundedAuthorization)?;
        let coverage_deadline = metadata
            .coverage()
            .inclusive_coverage_deadline()
            .ok_or(ProductionSinkFailure::UnboundedAuthorization)?;
        let live = metadata
            .coverage()
            .live()
            .ok_or(ProductionSinkFailure::MissingLiveCoverage)?;
        let coverage = match self.acknowledgement_evidence.as_ref() {
            Some(evidence) => market_squawk_sources::CoverageHealth::Sufficient {
                evidence: evidence.clone(),
                provider_product: live.provider_product().clone(),
                provider_channel: live.provider_channel().clone(),
                valid_until: coverage_deadline,
            },
            None => market_squawk_sources::CoverageHealth::Uninitialized,
        };
        let health = SourceHealthSnapshot::try_new(
            self.session,
            observed_at,
            ConnectionLiveness::Live {
                last_activity_at: self.last_transport_at.unwrap_or(observed_at),
            },
            self.last_transport_at,
            self.last_market_at,
            self.last_source_at,
            metadata.freshness_policy(),
            StreamIntegrityState::Healthy,
            self.capture.integrity(),
            AuthorizationHealth::Valid {
                evidence: metadata.authorization().evidence().clone(),
                valid_until: authorization_deadline,
            },
            coverage,
            BudgetHealth::Available,
            None,
            Vec::new(),
        )
        .map_err(ProductionSinkFailure::Health)?;
        let update = match self.active_request_budget.as_ref() {
            Some(request) => self
                .health_reporter
                .report_with_active_request(health, request),
            None => self.health_reporter.report(health),
        }
        .map_err(ProductionSinkFailure::Registry)?;
        self.registry
            .record_health(self.session, update)
            .map_err(ProductionSinkFailure::Registry)
    }

    fn fail(&mut self, failure: ProductionSinkFailure) -> SinkError {
        if self.terminal.is_none() {
            self.terminal = Some(failure);
        }
        failure.as_sink_error()
    }
}

#[derive(Debug)]
struct PendingDecodedData {
    data: market_squawk_sources::CapturedDecodedProviderBatch,
    route: ShardKey,
    latest_source_at: Option<Timestamp>,
    received_at: Timestamp,
    retained_bytes: usize,
}

#[derive(Debug)]
struct PendingDataBuffer {
    entries: VecDeque<PendingDecodedData>,
    retained_bytes: usize,
    maximum_messages: usize,
    maximum_bytes: usize,
}

impl PendingDataBuffer {
    fn try_new(
        (maximum_messages, maximum_bytes): (usize, usize),
    ) -> Result<Self, ProductionSinkConstructionError> {
        let mut entries = VecDeque::new();
        entries
            .try_reserve_exact(maximum_messages)
            .map_err(|_error| ProductionSinkConstructionError::AllocationFailed)?;
        Ok(Self {
            entries,
            retained_bytes: 0,
            maximum_messages,
            maximum_bytes,
        })
    }

    const fn is_enabled(&self) -> bool {
        self.maximum_messages != 0
    }

    fn try_push(
        &mut self,
        data: market_squawk_sources::CapturedDecodedProviderBatch,
        route: ShardKey,
        latest_source_at: Option<Timestamp>,
        received_at: Timestamp,
    ) -> Result<(), ProductionSinkFailure> {
        if self.entries.len() == self.maximum_messages {
            return Err(ProductionSinkFailure::PreAcknowledgementBufferCountSaturated);
        }
        let retained_bytes = data
            .retained_bytes()
            .map_err(|_error| ProductionSinkFailure::PreAcknowledgementRetainedSizeOverflow)?
            .checked_add(size_of::<PendingDecodedData>())
            .and_then(|bytes| bytes.checked_add(route.venue().retained_bytes()))
            .ok_or(ProductionSinkFailure::PreAcknowledgementRetainedSizeOverflow)?;
        let next_total = self
            .retained_bytes
            .checked_add(retained_bytes)
            .ok_or(ProductionSinkFailure::PreAcknowledgementRetainedSizeOverflow)?;
        if next_total > self.maximum_bytes {
            return Err(ProductionSinkFailure::PreAcknowledgementBufferBytesSaturated);
        }
        self.entries.push_back(PendingDecodedData {
            data,
            route,
            latest_source_at,
            received_at,
            retained_bytes,
        });
        self.retained_bytes = next_total;
        Ok(())
    }

    fn try_pop_front(&mut self) -> Result<Option<PendingDecodedData>, ProductionSinkFailure> {
        let Some(entry) = self.entries.pop_front() else {
            if self.retained_bytes != 0 {
                return Err(ProductionSinkFailure::PreAcknowledgementBufferAccounting);
            }
            return Ok(None);
        };
        self.retained_bytes = self
            .retained_bytes
            .checked_sub(entry.retained_bytes)
            .ok_or(ProductionSinkFailure::PreAcknowledgementBufferAccounting)?;
        Ok(Some(entry))
    }
}

impl RawMarketSink for ProductionRawMarketSink<'_> {
    fn bind_active_request_budget(&mut self, request: BudgetPermitLease) -> Result<(), SinkError> {
        if self.active_request_budget.is_some() {
            return Err(self.fail(ProductionSinkFailure::DuplicateActiveRequestBudget));
        }
        self.active_request_budget = Some(request);
        Ok(())
    }

    fn try_publish(&mut self, frame: RawMarketFrame) -> Result<(), SinkError> {
        if let Some(failure) = self.terminal {
            return Err(failure.as_sink_error());
        }
        self.process_frame(frame)
            .map_err(|failure| self.fail(failure))
    }

    fn next_deadline(&self) -> Option<Instant> {
        self.subscription.next_deadline()
    }

    fn poll_deadline(&mut self, now: Instant) -> Result<(), SinkError> {
        if let Some(failure) = self.terminal {
            return Err(failure.as_sink_error());
        }
        self.subscription
            .poll_deadline(now)
            .map(|_phase| ())
            .map_err(ProductionSinkFailure::Subscription)
            .map_err(|failure| self.fail(failure))
    }
}

fn decoded_route(outcome: &DecodeOutcome) -> Result<Option<ShardKey>, ProductionSinkFailure> {
    let DecodeOutcome::Data(batch) = outcome else {
        return Ok(None);
    };
    let mut observations = batch.observations().iter();
    let first = observations
        .next()
        .ok_or(ProductionSinkFailure::UnexpectedRouteCount)?;
    let route = ShardKey::new(first.venue().clone(), first.instrument());
    if observations.any(|observation| {
        observation.venue() != route.venue() || observation.instrument() != route.instrument()
    }) {
        return Err(ProductionSinkFailure::UnexpectedRouteCount);
    }
    Ok(Some(route))
}

fn latest_source_timestamp(outcome: &DecodeOutcome) -> Option<Timestamp> {
    let DecodeOutcome::Data(batch) = outcome else {
        return None;
    };
    batch
        .observations()
        .iter()
        .filter_map(|observation| match observation.timestamp() {
            ProviderTimestampEvidence::Provided { value, .. } => Some(*value),
            ProviderTimestampEvidence::AuthoritativelyAbsent(_) => None,
        })
        .max()
}

fn rebind_at(
    observed_at: Timestamp,
    freshness: FreshnessPolicy,
) -> Result<Timestamp, ProductionSinkFailure> {
    let half_life = freshness.max_market_age_nanos() / 2;
    let offset = i64::try_from(half_life.max(1))
        .map_err(|_error| ProductionSinkFailure::HealthDeadlineRange)?;
    observed_at
        .checked_add_nanos(offset)
        .map_err(|_error| ProductionSinkFailure::HealthDeadlineRange)
}

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum RouteActivationFailure {
    #[error("live route activation binding failed: {0}")]
    Bind(LiveIngressBindError),
    #[error("first current batch failed live ingress: {0}")]
    Ingress(LiveIngressError),
    #[error("route actor received a command before generation activation")]
    CommandOrder,
}

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum ProductionSinkFailure {
    #[error("raw capture publication failed")]
    Capture(CapturePublishError),
    #[error("source authority validation failed: {0}")]
    Registry(RegistryError),
    #[error("decoder implementation failed")]
    Decode(DecodeInternalError),
    #[error("predecoded sink cannot accept a raw frame")]
    MissingDecoder,
    #[error("decoded sink cannot accept an adapter-predecoded frame")]
    UnexpectedDecoder,
    #[error("subscription state failed")]
    Subscription(SubscriptionFailure),
    #[error("source requested resynchronization: {0:?}")]
    Resynchronize(ResynchronizationReason),
    #[error("source generation was quarantined: {0:?}")]
    Quarantine(QuarantineReason),
    #[error("source health construction failed")]
    Health(SourceHealthError),
    #[error("live ingress publication failed")]
    Ingress(LiveIngressError),
    #[error("live route activation failed: {0}")]
    RouteActivation(RouteActivationFailure),
    #[error("route actor command count capacity is full")]
    ActivationBufferCountSaturated,
    #[error("route actor retained-batch byte capacity is full")]
    ActivationBufferBytesSaturated,
    #[error("pre-acknowledgement data buffer count capacity is full")]
    PreAcknowledgementBufferCountSaturated,
    #[error("pre-acknowledgement data buffer retained-byte capacity is full")]
    PreAcknowledgementBufferBytesSaturated,
    #[error("pre-acknowledgement retained-size accounting overflowed")]
    PreAcknowledgementRetainedSizeOverflow,
    #[error("pre-acknowledgement retained-size accounting is inconsistent")]
    PreAcknowledgementBufferAccounting,
    #[error("route activation worker is closed")]
    ActivationWorkerClosed,
    #[error("decoded data route is not configured")]
    UnknownRoute,
    #[error("decoded data route changed during authority upgrade")]
    RouteMismatch,
    #[error("Coinbase decoder produced an unexpected routed batch count")]
    UnexpectedRouteCount,
    #[error("Coinbase authorization or coverage is not finitely bounded")]
    UnboundedAuthorization,
    #[error("Coinbase metadata does not declare live coverage")]
    MissingLiveCoverage,
    #[error("health rebind deadline cannot be represented")]
    HealthDeadlineRange,
    #[error("decoded route is unavailable before current-data upgrade")]
    RouteUnavailableBeforeUpgrade,
    #[error("active provider request budget was bound more than once")]
    DuplicateActiveRequestBudget,
}

impl ProductionSinkFailure {
    pub(super) const fn requires_generation_resynchronization(self) -> bool {
        match self {
            Self::Resynchronize(_)
            | Self::Quarantine(_)
            | Self::PreAcknowledgementBufferCountSaturated
            | Self::PreAcknowledgementBufferBytesSaturated
            | Self::PreAcknowledgementRetainedSizeOverflow
            | Self::PreAcknowledgementBufferAccounting => true,
            Self::Subscription(
                SubscriptionFailure::AcknowledgementMismatch
                | SubscriptionFailure::DuplicateAcknowledgement
                | SubscriptionFailure::AcknowledgementDeadlineExceeded
                | SubscriptionFailure::DataBeforeAcknowledgement,
            ) => true,
            Self::Subscription(
                SubscriptionFailure::GenerationInvalid
                | SubscriptionFailure::StaleGeneration
                | SubscriptionFailure::RejectedDataCounterOverflow
                | SubscriptionFailure::TransitionSequenceExhausted
                | SubscriptionFailure::AuditAccountingInvariant,
            )
            | Self::Capture(_)
            | Self::Registry(_)
            | Self::Decode(_)
            | Self::MissingDecoder
            | Self::UnexpectedDecoder
            | Self::Health(_)
            | Self::Ingress(_)
            | Self::RouteActivation(_)
            | Self::ActivationBufferCountSaturated
            | Self::ActivationBufferBytesSaturated
            | Self::ActivationWorkerClosed
            | Self::UnknownRoute
            | Self::RouteMismatch
            | Self::UnexpectedRouteCount
            | Self::UnboundedAuthorization
            | Self::MissingLiveCoverage
            | Self::HealthDeadlineRange
            | Self::DuplicateActiveRequestBudget
            | Self::RouteUnavailableBeforeUpgrade => false,
        }
    }

    const fn as_sink_error(self) -> SinkError {
        match self {
            Self::Capture(CapturePublishError::QueueFull | CapturePublishError::AuthorityBusy)
            | Self::Ingress(
                LiveIngressError::CountCapacityFull | LiveIngressError::ByteCapacityFull,
            )
            | Self::ActivationBufferCountSaturated
            | Self::ActivationBufferBytesSaturated
            | Self::PreAcknowledgementBufferCountSaturated
            | Self::PreAcknowledgementBufferBytesSaturated => SinkError::Saturated,
            Self::Capture(
                CapturePublishError::QueueClosed
                | CapturePublishError::WriterUnavailable
                | CapturePublishError::QueuePoisoned
                | CapturePublishError::QueueInvariant,
            )
            | Self::ActivationWorkerClosed => SinkError::Closed,
            Self::Capture(_)
            | Self::Registry(_)
            | Self::Decode(_)
            | Self::MissingDecoder
            | Self::UnexpectedDecoder
            | Self::Subscription(_)
            | Self::Resynchronize(_)
            | Self::Quarantine(_)
            | Self::Health(_)
            | Self::Ingress(_)
            | Self::RouteActivation(_)
            | Self::PreAcknowledgementRetainedSizeOverflow
            | Self::PreAcknowledgementBufferAccounting
            | Self::UnknownRoute
            | Self::RouteMismatch
            | Self::UnexpectedRouteCount
            | Self::UnboundedAuthorization
            | Self::MissingLiveCoverage
            | Self::HealthDeadlineRange
            | Self::DuplicateActiveRequestBudget
            | Self::RouteUnavailableBeforeUpgrade => SinkError::CaptureIncomplete,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum ProductionSinkConstructionError {
    #[error("production sink requires at least one route")]
    MissingRoutes,
    #[error("production sink route is duplicated")]
    DuplicateRoute,
    #[error("production sink route allocation failed")]
    AllocationFailed,
}
