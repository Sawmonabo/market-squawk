//! Capture-first Coinbase sink and bounded live-route activation.

use std::{
    collections::{HashMap, VecDeque},
    mem::size_of,
    time::{Duration, Instant},
};

use super::{
    display_market::{
        DisplayMarketIngress, DisplayMarketRouteIdentity, DisplayMarketTerminalFailure,
    },
    provider::{ProductionDecodeOutcome, ProductionMarketDecoder, StartupReadinessPolicy},
    route_actor::{RouteActivationBinding, RouteActivationPublisher},
    subscription_state::{
        GenerationIdentity, SubscriptionFailure, SubscriptionPhase, SubscriptionStateMachine,
    },
};
use market_squawk_adapter_coinbase::{
    CoinbaseMarketDecodeOutcome, CoinbaseMarketFeed, CoinbaseMarketRawLineage,
};
use market_squawk_domain::{ExactPayloadEvidence, StreamIntegrityState, Timestamp};
use market_squawk_live::{LiveIngressBindError, LiveIngressError, LiveRuntimeIngress, ShardKey};
use market_squawk_platform::{CapturePublishError, RawCapturePublisher};
use market_squawk_sources::{
    AuthoritativeSourceRegistry, AuthorizationHealth, BudgetHealth, BudgetPermitLease,
    CaptureGenerationCapabilities, ConnectionLiveness, ControlFrameKind,
    CurrentDecodedProviderBatch, CurrentDecodedProviderBatches, CurrentHealthRecording,
    CurrentHealthReporter, CurrentSourceSession, DecodeInternalError, DecodeOutcome,
    FreshnessPolicy, ProviderTimestampEvidence, QuarantineReason, RawMarketFrame, RawMarketSink,
    RegistryError, ResynchronizationReason, SinkError, SourceHealthError, SourceHealthSnapshot,
    SourceMetadata, SourceMetadataProvider, ValidatedSessionDecodeOutcome,
};
use thiserror::Error;
use tokio::sync::oneshot;

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

/// Input capabilities for one exact-generation display-only source sink.
#[derive(Debug)]
pub(super) struct ProductionDisplayMarketSinkInput<'a> {
    pub(super) capture: RawCapturePublisher<CaptureGenerationCapabilities>,
    pub(super) registry: &'a mut AuthoritativeSourceRegistry,
    pub(super) session: &'a CurrentSourceSession,
    pub(super) health_reporter: CurrentHealthReporter,
    pub(super) decoder: ProductionMarketDecoder,
    pub(super) subscription: SubscriptionStateMachine,
    pub(super) display_ingresses: Vec<DisplayMarketIngress>,
    pub(super) ingress_timeout: Duration,
    pub(super) startup_readiness_policy: StartupReadinessPolicy,
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
    output: QualifiedSourceOutput,
    startup_readiness: Option<oneshot::Sender<()>>,
    startup_ready: bool,
    startup_readiness_policy: StartupReadinessPolicy,
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
        let output = QualifiedSourceOutput::Live(QualifiedLiveOutput::try_new(
            input.live_ingress,
            input.routes,
        )?);
        Self::try_new_inner(
            input.capture,
            input.registry,
            input.session,
            input.health_reporter,
            Some(input.decoder),
            metadata,
            input.subscription,
            output,
            StartupReadinessPolicy::FirstQualifiedData,
        )
    }

    pub(super) fn try_new_with_startup_readiness(
        input: ProductionRawMarketSinkInput<'a>,
        startup_readiness: oneshot::Sender<()>,
    ) -> Result<Self, ProductionSinkConstructionError> {
        let mut sink = Self::try_new(input)?;
        sink.startup_readiness = Some(startup_readiness);
        Ok(sink)
    }

    pub(super) fn try_new_predecoded(
        input: ProductionPredecodedMarketSinkInput<'a>,
    ) -> Result<Self, ProductionSinkConstructionError> {
        let output = QualifiedSourceOutput::Live(QualifiedLiveOutput::try_new(
            input.live_ingress,
            input.routes,
        )?);
        Self::try_new_inner(
            input.capture,
            input.registry,
            input.session,
            input.health_reporter,
            None,
            input.metadata,
            input.subscription,
            output,
            StartupReadinessPolicy::FirstQualifiedData,
        )
    }

    pub(super) fn try_new_display(
        input: ProductionDisplayMarketSinkInput<'a>,
    ) -> Result<Self, ProductionSinkConstructionError> {
        let metadata = input.decoder.metadata().clone();
        let startup_readiness_policy = input.startup_readiness_policy;
        validate_display_generation(&metadata, input.session, &input.display_ingresses)?;
        let output = QualifiedSourceOutput::Display(QualifiedDisplayOutput::try_new(
            input.display_ingresses,
            input.ingress_timeout,
        )?);
        Self::try_new_inner(
            input.capture,
            input.registry,
            input.session,
            input.health_reporter,
            Some(input.decoder),
            metadata,
            input.subscription,
            output,
            startup_readiness_policy,
        )
    }

    pub(super) fn try_new_display_with_startup_readiness(
        input: ProductionDisplayMarketSinkInput<'a>,
        startup_readiness: oneshot::Sender<()>,
    ) -> Result<Self, ProductionSinkConstructionError> {
        let mut sink = Self::try_new_display(input)?;
        sink.startup_readiness = Some(startup_readiness);
        Ok(sink)
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
        output: QualifiedSourceOutput,
        startup_readiness_policy: StartupReadinessPolicy,
    ) -> Result<Self, ProductionSinkConstructionError> {
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
            output,
            startup_readiness: None,
            startup_ready: false,
            startup_readiness_policy,
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

    pub(super) const fn startup_ready(&self) -> bool {
        self.startup_ready
    }

    pub(super) fn record_display_terminal_failure(
        &mut self,
        failure: DisplayMarketTerminalFailure,
    ) {
        tracing::error!(%failure, "display-market generation failed terminally");
        let _sink_error = self.fail(ProductionSinkFailure::DisplayTerminal);
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
        match outcome {
            ProductionDecodeOutcome::Standard(outcome)
            | ProductionDecodeOutcome::Coinbase(CoinbaseMarketDecodeOutcome::Other(outcome)) => {
                self.process_captured_outcome(outcome, receipt)
            }
            ProductionDecodeOutcome::Coinbase(CoinbaseMarketDecodeOutcome::Market(handoff)) => {
                self.process_coinbase_handoff(handoff, receipt)
            }
        }
    }

    fn process_coinbase_handoff(
        &mut self,
        handoff: market_squawk_adapter_coinbase::CoinbaseMarketHandoff,
        receipt: market_squawk_sources::CaptureAdmissionReceipt,
    ) -> Result<(), ProductionSinkFailure> {
        if handoff.evidence().feed() != CoinbaseMarketFeed::AdvancedTradePublic {
            return Err(ProductionSinkFailure::Decode(
                DecodeInternalError::InvariantViolation,
            ));
        }
        let (_evidence, raw_lineage, batch) = handoff.into_parts();
        let CoinbaseMarketRawLineage::AdvancedTrade(payload) = raw_lineage else {
            return Err(ProductionSinkFailure::Decode(
                DecodeInternalError::InvariantViolation,
            ));
        };
        if payload.as_bytes().len() != batch.evidence().frame_bytes() {
            return Err(ProductionSinkFailure::Decode(
                DecodeInternalError::InvariantViolation,
            ));
        }
        self.process_captured_outcome(DecodeOutcome::Data(batch), receipt)
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
        self.output.poll_failures()?;
        Ok(receipt)
    }

    fn process_captured_outcome(
        &mut self,
        outcome: DecodeOutcome,
        receipt: market_squawk_sources::CaptureAdmissionReceipt,
    ) -> Result<(), ProductionSinkFailure> {
        self.output.poll_failures()?;
        let latest_source_at = latest_source_timestamp(&outcome);
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
            ValidatedSessionDecodeOutcome::Data(data) => {
                self.process_data(data, latest_source_at, received_at)
            }
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
            let _recording = self.record_health(received_at)?;
            self.health_rebind_at = None;
            self.health_valid_until = None;
        }
        Ok(())
    }

    fn process_data(
        &mut self,
        data: market_squawk_sources::CapturedDecodedProviderBatch,
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
                .try_push(data, latest_source_at, received_at);
        }
        match self.process_active_data(data, latest_source_at, received_at, received_at)? {
            ActiveDataDisposition::Published => self.publish_startup_readiness(),
            ActiveDataDisposition::FreshnessUnqualified => Err(ProductionSinkFailure::Registry(
                RegistryError::HealthNotQualified,
            )),
        }
    }

    fn flush_pending_data(
        &mut self,
        acknowledgement_received_at: Timestamp,
    ) -> Result<(), ProductionSinkFailure> {
        let mut published = false;
        let mut freshness_unqualified = false;
        while let Some(pending) = self.pending_data.try_pop_front()? {
            match self.process_active_data(
                pending.data,
                pending.latest_source_at,
                pending.received_at,
                acknowledgement_received_at.max(pending.received_at),
            )? {
                ActiveDataDisposition::Published => published = true,
                ActiveDataDisposition::FreshnessUnqualified => freshness_unqualified = true,
            }
        }
        if published
            || freshness_unqualified
                && self.startup_readiness_policy
                    == StartupReadinessPolicy::ExactAcknowledgementAfterCapturedBootstrap
        {
            self.publish_startup_readiness()?;
        }
        Ok(())
    }

    fn process_active_data(
        &mut self,
        data: market_squawk_sources::CapturedDecodedProviderBatch,
        latest_source_at: Option<Timestamp>,
        received_at: Timestamp,
        health_observed_at: Timestamp,
    ) -> Result<ActiveDataDisposition, ProductionSinkFailure> {
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
        if requires_rebind {
            match self.record_health(health_observed_at)? {
                CurrentHealthRecording::Qualified => {}
                CurrentHealthRecording::Unqualified(cause) if cause.is_freshness_only() => {
                    return Ok(ActiveDataDisposition::FreshnessUnqualified);
                }
                CurrentHealthRecording::Unqualified(_cause) => {
                    return Err(ProductionSinkFailure::Registry(
                        RegistryError::HealthNotQualified,
                    ));
                }
            }
        }
        let current = self
            .registry
            .validate_current_authority(self.session)
            .map_err(ProductionSinkFailure::Registry)?;
        let batches = current
            .validate_data_outcome_owned(data)
            .map_err(ProductionSinkFailure::Registry)?;
        let valid_until = self.output.try_publish(batches, received_at)?;
        if requires_rebind {
            self.health_rebind_at = Some(rebind_at(
                health_observed_at,
                self.metadata.freshness_policy(),
            )?);
            self.health_valid_until = Some(valid_until);
        }
        Ok(ActiveDataDisposition::Published)
    }

    fn publish_startup_readiness(&mut self) -> Result<(), ProductionSinkFailure> {
        let Some(readiness) = self.startup_readiness.take() else {
            return Ok(());
        };
        readiness
            .send(())
            .map_err(|_value| ProductionSinkFailure::StartupObserverDropped)?;
        self.startup_ready = true;
        Ok(())
    }

    fn record_health(
        &mut self,
        observed_at: Timestamp,
    ) -> Result<CurrentHealthRecording, ProductionSinkFailure> {
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
        let recording = self
            .registry
            .record_health_with_qualification(self.session, update)
            .map_err(ProductionSinkFailure::Registry)?;
        if recording == CurrentHealthRecording::Qualified {
            self.output.advance_health_revision()?;
        }
        Ok(recording)
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
    latest_source_at: Option<Timestamp>,
    received_at: Timestamp,
    retained_bytes: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ActiveDataDisposition {
    Published,
    FreshnessUnqualified,
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

fn validate_display_generation(
    metadata: &SourceMetadata,
    session: &CurrentSourceSession,
    ingresses: &[DisplayMarketIngress],
) -> Result<(), ProductionSinkConstructionError> {
    if ingresses.is_empty() {
        return Err(ProductionSinkConstructionError::MissingRoutes);
    }
    if ingresses.iter().any(|ingress| {
        ingress.key().source_id() != metadata.source_id()
            || ingress.key().generation() != session.generation()
    }) {
        return Err(ProductionSinkConstructionError::DisplayGenerationMismatch);
    }
    Ok(())
}

#[derive(Debug)]
enum QualifiedSourceOutput {
    Live(QualifiedLiveOutput),
    Display(QualifiedDisplayOutput),
}

impl QualifiedSourceOutput {
    fn advance_health_revision(&mut self) -> Result<(), ProductionSinkFailure> {
        match self {
            Self::Live(output) => output.advance_health_revision(),
            Self::Display(_output) => Ok(()),
        }
    }

    fn try_publish(
        &mut self,
        batches: CurrentDecodedProviderBatches,
        validated_at: Timestamp,
    ) -> Result<Timestamp, ProductionSinkFailure> {
        match self {
            Self::Live(output) => output.try_publish(batches),
            Self::Display(output) => output.try_publish(batches, validated_at),
        }
    }

    fn poll_failures(&mut self) -> Result<(), ProductionSinkFailure> {
        match self {
            Self::Live(output) => output.poll_failures(),
            Self::Display(output) => output.poll_failures(),
        }
    }
}

#[derive(Debug)]
struct QualifiedDisplayOutput {
    routes: HashMap<DisplayMarketRouteIdentity, DisplayMarketIngress>,
    ingress_timeout: Duration,
}

impl QualifiedDisplayOutput {
    fn try_new(
        ingresses: Vec<DisplayMarketIngress>,
        ingress_timeout: Duration,
    ) -> Result<Self, ProductionSinkConstructionError> {
        if ingresses.is_empty() {
            return Err(ProductionSinkConstructionError::MissingRoutes);
        }
        if ingress_timeout.is_zero() {
            return Err(ProductionSinkConstructionError::InvalidDisplayIngressTimeout);
        }
        let mut routes = HashMap::new();
        routes
            .try_reserve(ingresses.len())
            .map_err(|_error| ProductionSinkConstructionError::AllocationFailed)?;
        for ingress in ingresses {
            let route = DisplayMarketRouteIdentity::try_new(
                ingress.key().venue_id(),
                ingress.key().instrument_id(),
            )
            .map_err(|_error| ProductionSinkConstructionError::AllocationFailed)?;
            if routes.insert(route, ingress).is_some() {
                return Err(ProductionSinkConstructionError::DuplicateRoute);
            }
        }
        Ok(Self {
            routes,
            ingress_timeout,
        })
    }

    fn try_publish(
        &mut self,
        batches: CurrentDecodedProviderBatches,
        validated_at: Timestamp,
    ) -> Result<Timestamp, ProductionSinkFailure> {
        let batch_count = batches.len();
        if batch_count == 0 {
            return Err(ProductionSinkFailure::UnexpectedRouteCount);
        }
        self.poll_failures()?;
        let deadline = Instant::now()
            .checked_add(self.ingress_timeout)
            .ok_or(ProductionSinkFailure::OutputDeadlineRange)?;
        let mut prepared = Vec::new();
        prepared
            .try_reserve_exact(batch_count)
            .map_err(|_error| ProductionSinkFailure::OutputAllocationFailed)?;
        let mut valid_until: Option<Timestamp> = None;
        for batch in batches {
            let route =
                DisplayMarketRouteIdentity::try_new(batch.key().venue(), batch.key().instrument())
                    .map_err(|_error| ProductionSinkFailure::OutputAllocationFailed)?;
            if prepared
                .iter()
                .any(|candidate: &PreparedDisplayRoute| candidate.route == route)
            {
                return Err(ProductionSinkFailure::UnexpectedRouteCount);
            }
            let ingress = self
                .routes
                .get(&route)
                .ok_or(ProductionSinkFailure::UnknownRoute)?;
            ingress
                .preflight(&batch, validated_at, deadline)
                .map_err(|error| {
                    tracing::error!(%error, "display-market ingress preflight failed");
                    ProductionSinkFailure::DisplayIngress
                })?;
            valid_until = Some(
                valid_until.map_or(batch.current_lease().valid_until(), |current| {
                    current.min(batch.current_lease().valid_until())
                }),
            );
            prepared.push(PreparedDisplayRoute { route, batch });
        }
        for publication in prepared {
            let ingress = self
                .routes
                .get(&publication.route)
                .ok_or(ProductionSinkFailure::UnknownRoute)?;
            ingress
                .try_publish(publication.batch, validated_at, deadline)
                .map_err(|error| {
                    tracing::error!(%error, "display-market ingress publication failed");
                    ProductionSinkFailure::DisplayIngress
                })?;
        }
        valid_until.ok_or(ProductionSinkFailure::UnexpectedRouteCount)
    }

    fn poll_failures(&self) -> Result<(), ProductionSinkFailure> {
        for ingress in self.routes.values() {
            if let Some(failure) = ingress.current_failure() {
                tracing::error!(%failure, "display-market ingress observed terminal actor state");
                return Err(ProductionSinkFailure::DisplayTerminal);
            }
        }
        Ok(())
    }
}

#[derive(Debug)]
struct PreparedDisplayRoute {
    route: DisplayMarketRouteIdentity,
    batch: CurrentDecodedProviderBatch,
}

/// Sealed current-batch output boundary shared by raw and adapter-predecoded source paths.
///
/// Capture, session, subscription, and source-health authority remain in the parent sink. This
/// component owns only registry-qualified live-ingress routing, so another bounded display output
/// can be added without duplicating those authorities.
#[derive(Debug)]
struct QualifiedLiveOutput {
    live_ingress: LiveRuntimeIngress,
    routes: HashMap<ShardKey, QualifiedRoutePublisher>,
    health_revision: u64,
}

impl QualifiedLiveOutput {
    fn try_new(
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
        for publisher in route_publishers {
            let route = publisher.route().clone();
            if routes
                .insert(
                    route,
                    QualifiedRoutePublisher {
                        publisher,
                        bound_health_revision: None,
                    },
                )
                .is_some()
            {
                return Err(ProductionSinkConstructionError::DuplicateRoute);
            }
        }
        Ok(Self {
            live_ingress,
            routes,
            health_revision: 0,
        })
    }

    fn advance_health_revision(&mut self) -> Result<(), ProductionSinkFailure> {
        self.health_revision = self
            .health_revision
            .checked_add(1)
            .ok_or(ProductionSinkFailure::HealthRevisionExhausted)?;
        Ok(())
    }

    /// Preflights the complete frame partition before admitting any member route.
    ///
    /// Once command admission begins, any failure tears down the exact source generation. The
    /// registry has already validated every observation and retained their shared capture evidence.
    fn try_publish(
        &mut self,
        batches: CurrentDecodedProviderBatches,
    ) -> Result<Timestamp, ProductionSinkFailure> {
        let batch_count = batches.len();
        if self.health_revision == 0 || batch_count == 0 {
            return Err(ProductionSinkFailure::UnexpectedRouteCount);
        }
        self.poll_failures()?;
        let mut prepared = Vec::new();
        prepared
            .try_reserve_exact(batch_count)
            .map_err(|_error| ProductionSinkFailure::OutputAllocationFailed)?;
        let mut valid_until: Option<Timestamp> = None;
        for batch in batches {
            let route = ShardKey::new(batch.key().venue().clone(), batch.key().instrument());
            if prepared
                .iter()
                .any(|candidate: &PreparedQualifiedRoute| candidate.route == route)
            {
                return Err(ProductionSinkFailure::UnexpectedRouteCount);
            }
            let publisher = self
                .routes
                .get_mut(&route)
                .ok_or(ProductionSinkFailure::UnknownRoute)?;
            let activation = if publisher.bound_health_revision == Some(self.health_revision) {
                None
            } else {
                Some(publisher.publisher.prepare(&self.live_ingress)?)
            };
            valid_until = Some(
                valid_until.map_or(batch.current_lease().valid_until(), |current| {
                    current.min(batch.current_lease().valid_until())
                }),
            );
            prepared.push(PreparedQualifiedRoute {
                route,
                activation,
                batch,
            });
        }
        for publication in prepared {
            let publisher = self
                .routes
                .get_mut(&publication.route)
                .ok_or(ProductionSinkFailure::UnknownRoute)?;
            match publication.activation {
                Some(activation) => {
                    publisher
                        .publisher
                        .start_activation(activation, publication.batch)?;
                    publisher.bound_health_revision = Some(self.health_revision);
                }
                None => publisher.publisher.try_publish(publication.batch)?,
            }
        }
        valid_until.ok_or(ProductionSinkFailure::UnexpectedRouteCount)
    }

    fn poll_failures(&mut self) -> Result<(), ProductionSinkFailure> {
        for route in self.routes.values_mut() {
            route.publisher.check_failure()?;
        }
        Ok(())
    }
}

#[derive(Debug)]
struct QualifiedRoutePublisher {
    publisher: RouteActivationPublisher,
    bound_health_revision: Option<u64>,
}

#[derive(Debug)]
struct PreparedQualifiedRoute {
    route: ShardKey,
    activation: Option<RouteActivationBinding>,
    batch: CurrentDecodedProviderBatch,
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
    #[error("live route generation revocation failed: {0}")]
    Revoke(market_squawk_live::LiveIngressRevokeError),
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
    #[error("qualified live-output staging allocation failed")]
    OutputAllocationFailed,
    #[error("qualified output deadline cannot be represented")]
    OutputDeadlineRange,
    #[error("display-market ingress failed")]
    DisplayIngress,
    #[error("display-market generation failed terminally")]
    DisplayTerminal,
    #[error("route activation worker is closed")]
    ActivationWorkerClosed,
    #[error("decoded data route is not configured")]
    UnknownRoute,
    #[error("registry-qualified data produced an invalid routed batch set")]
    UnexpectedRouteCount,
    #[error("source authorization or coverage is not finitely bounded")]
    UnboundedAuthorization,
    #[error("source metadata does not declare live coverage")]
    MissingLiveCoverage,
    #[error("source health revision counter exhausted")]
    HealthRevisionExhausted,
    #[error("health rebind deadline cannot be represented")]
    HealthDeadlineRange,
    #[error("production source startup observer was dropped")]
    StartupObserverDropped,
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
            | Self::OutputAllocationFailed
            | Self::OutputDeadlineRange
            | Self::ActivationWorkerClosed
            | Self::UnknownRoute
            | Self::UnexpectedRouteCount
            | Self::UnboundedAuthorization
            | Self::MissingLiveCoverage
            | Self::HealthRevisionExhausted
            | Self::HealthDeadlineRange
            | Self::StartupObserverDropped
            | Self::DuplicateActiveRequestBudget => false,
            Self::DisplayIngress | Self::DisplayTerminal => true,
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
            | Self::OutputAllocationFailed
            | Self::OutputDeadlineRange
            | Self::DisplayIngress
            | Self::DisplayTerminal
            | Self::UnknownRoute
            | Self::UnexpectedRouteCount
            | Self::UnboundedAuthorization
            | Self::MissingLiveCoverage
            | Self::HealthRevisionExhausted
            | Self::HealthDeadlineRange
            | Self::StartupObserverDropped
            | Self::DuplicateActiveRequestBudget => SinkError::CaptureIncomplete,
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
    #[error("display-market ingress generation does not match source authority")]
    DisplayGenerationMismatch,
    #[error("display-market ingress timeout must be non-zero")]
    InvalidDisplayIngressTimeout,
}
