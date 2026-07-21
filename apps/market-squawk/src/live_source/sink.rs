//! Capture-first Coinbase sink and bounded live-route activation.

use std::{collections::HashMap, time::Instant};

use super::{
    provider::ProductionMarketDecoder,
    route_actor::{RouteActivationBinding, RouteActivationPublisher},
    subscription_state::{GenerationIdentity, SubscriptionFailure, SubscriptionStateMachine},
};
use market_squawk_domain::{ExactPayloadEvidence, StreamIntegrityState, Timestamp};
use market_squawk_live::{LiveIngressBindError, LiveIngressError, LiveRuntimeIngress, ShardKey};
use market_squawk_platform::{CapturePublishError, RawCapturePublisher};
use market_squawk_sources::{
    AuthoritativeSourceRegistry, AuthorizationHealth, BudgetHealth, CaptureGenerationCapabilities,
    ConnectionLiveness, ControlFrameKind, CurrentHealthReporter, CurrentSourceSession,
    DecodeInternalError, DecodeOutcome, FreshnessPolicy, MarketDecoder, ProviderTimestampEvidence,
    QuarantineReason, RawMarketFrame, RawMarketSink, RegistryError, ResynchronizationReason,
    SinkError, SourceHealthError, SourceHealthSnapshot, SourceMetadataProvider,
    ValidatedSessionDecodeOutcome,
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

/// Exact capture/session/health/live-route bridge used directly by the Coinbase reader.
#[derive(Debug)]
pub(super) struct ProductionRawMarketSink<'a> {
    capture: RawCapturePublisher<CaptureGenerationCapabilities>,
    registry: &'a mut AuthoritativeSourceRegistry,
    session: &'a CurrentSourceSession,
    health_reporter: CurrentHealthReporter,
    decoder: ProductionMarketDecoder,
    generation: GenerationIdentity,
    subscription: SubscriptionStateMachine,
    live_ingress: LiveRuntimeIngress,
    routes: HashMap<ShardKey, RouteActivationPublisher>,
    last_transport_at: Option<Timestamp>,
    last_market_at: Option<Timestamp>,
    last_source_at: Option<Timestamp>,
    health_rebind_at: Option<Timestamp>,
    health_valid_until: Option<Timestamp>,
    acknowledgement_evidence: Option<ExactPayloadEvidence>,
    terminal: Option<ProductionSinkFailure>,
}

impl<'a> ProductionRawMarketSink<'a> {
    pub(super) fn try_new(
        input: ProductionRawMarketSinkInput<'a>,
    ) -> Result<Self, ProductionSinkConstructionError> {
        if input.routes.is_empty() {
            return Err(ProductionSinkConstructionError::MissingRoutes);
        }
        let mut routes = HashMap::new();
        routes
            .try_reserve(input.routes.len())
            .map_err(|_error| ProductionSinkConstructionError::AllocationFailed)?;
        for route in input.routes {
            if routes.insert(route.route().clone(), route).is_some() {
                return Err(ProductionSinkConstructionError::DuplicateRoute);
            }
        }
        Ok(Self {
            capture: input.capture,
            registry: input.registry,
            session: input.session,
            health_reporter: input.health_reporter,
            decoder: input.decoder,
            generation: GenerationIdentity::from_session(input.session),
            subscription: input.subscription,
            live_ingress: input.live_ingress,
            routes,
            last_transport_at: None,
            last_market_at: None,
            last_source_at: None,
            health_rebind_at: None,
            health_valid_until: None,
            acknowledgement_evidence: None,
            terminal: None,
        })
    }

    pub(super) const fn terminal_failure(&self) -> Option<ProductionSinkFailure> {
        self.terminal
    }

    fn process_frame(&mut self, frame: RawMarketFrame) -> Result<(), ProductionSinkFailure> {
        let receipt = self
            .capture
            .try_publish(&frame)
            .map_err(ProductionSinkFailure::Capture)?;
        self.poll_route_failures()?;
        let received_at = frame.received_at();
        let validated_frame = self
            .session
            .validate_live_frame(&frame)
            .map_err(ProductionSinkFailure::Registry)?;
        let outcome = self
            .decoder
            .decode(&validated_frame)
            .map_err(ProductionSinkFailure::Decode)?;
        let latest_source_at = latest_source_timestamp(&outcome);
        let decoded_route = decoded_route(&outcome)?;
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
            self.record_health(received_at)?;
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
                received_at,
                self.decoder.metadata().freshness_policy(),
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
        let metadata = self.decoder.metadata();
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
        let update = self
            .health_reporter
            .report(health)
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

impl RawMarketSink for ProductionRawMarketSink<'_> {
    fn try_publish(&mut self, frame: RawMarketFrame) -> Result<(), SinkError> {
        if let Some(failure) = self.terminal {
            return Err(failure.as_sink_error());
        }
        self.process_frame(frame)
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
    #[error("live route activation failed")]
    Bind(LiveIngressBindError),
    #[error("first current batch failed live ingress")]
    Ingress(LiveIngressError),
    #[error("route actor received a command before generation activation")]
    CommandOrder,
}

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum ProductionSinkFailure {
    #[error("raw capture publication failed")]
    Capture(CapturePublishError),
    #[error("source authority validation failed")]
    Registry(RegistryError),
    #[error("decoder implementation failed")]
    Decode(DecodeInternalError),
    #[error("subscription state failed")]
    Subscription(SubscriptionFailure),
    #[error("source requested resynchronization")]
    Resynchronize(ResynchronizationReason),
    #[error("source generation was quarantined")]
    Quarantine(QuarantineReason),
    #[error("source health construction failed")]
    Health(SourceHealthError),
    #[error("live ingress publication failed")]
    Ingress(LiveIngressError),
    #[error("live route activation failed")]
    RouteActivation(RouteActivationFailure),
    #[error("route actor command count capacity is full")]
    ActivationBufferCountSaturated,
    #[error("route actor retained-batch byte capacity is full")]
    ActivationBufferBytesSaturated,
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
}

impl ProductionSinkFailure {
    const fn as_sink_error(self) -> SinkError {
        match self {
            Self::Capture(CapturePublishError::QueueFull | CapturePublishError::AuthorityBusy)
            | Self::Ingress(
                LiveIngressError::CountCapacityFull | LiveIngressError::ByteCapacityFull,
            )
            | Self::ActivationBufferCountSaturated
            | Self::ActivationBufferBytesSaturated => SinkError::Saturated,
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
            | Self::Subscription(_)
            | Self::Resynchronize(_)
            | Self::Quarantine(_)
            | Self::Health(_)
            | Self::Ingress(_)
            | Self::RouteActivation(_)
            | Self::UnknownRoute
            | Self::RouteMismatch
            | Self::UnexpectedRouteCount
            | Self::UnboundedAuthorization
            | Self::MissingLiveCoverage
            | Self::HealthDeadlineRange
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
