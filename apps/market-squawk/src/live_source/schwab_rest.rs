//! Registered current-session bridge for one completed Schwab REST quote response.
//!
//! This module owns no network transport and cannot create source authority. A composition owner
//! must supply the exact registry/session, once-issued raw-frame factory, active capture publisher,
//! and display ingresses for one current generation. The bridge then carries the same completed
//! response bytes through capture, decode, current-registry qualification, and display ingress
//! without refetching or reconstructing authority from archival evidence.

use std::{
    sync::{Mutex, TryLockError},
    time::{Duration, Instant},
};

use bytes::Bytes;
use market_squawk_adapter_schwab::{
    ExecutedRestResponse, NativeField, NativeScalar, ParsedNative, ProviderIdentifier,
    QuoteComponentField, QuoteResponse, RawRestResponseReceipt, ReadOnlyRoute, RestItemAccounting,
    SchwabQuote, SchwabRestDelayEvidence, SchwabRestPayload,
};
use market_squawk_domain::{
    CaptureIntegrityState, ConnectionGeneration, CoverageDelay, ExactPayloadEvidence,
    InstrumentExecutionTerms, InstrumentId, PriceTicks, QuantityLots, SnapshotApplicability,
    SourceIdentifier, StreamIntegrityState, Timestamp, VenueId,
};
use market_squawk_platform::{
    CaptureShutdownStatus, CaptureWriterHandle, RawCaptureControl, RawCapturePublisher,
};
use market_squawk_sources::{
    AuthoritativeSourceRegistry, AuthorizationHealth, BudgetHealth, CaptureGenerationCapabilities,
    ChecksumValidationProfile, ConnectionLiveness, CoverageHealth, CurrentDecodedProviderBatch,
    CurrentHealthRecording, CurrentHealthReporter, CurrentSourceSession, DecodeOutcome,
    DecodedProviderBatch, DecoderEvidence, ProviderBookLevel, ProviderChecksumEvidence,
    ProviderDecimalLexeme, ProviderNormalizedObservation, ProviderObservationPayload,
    ProviderPrice, ProviderQuantity, ProviderSequenceEvidence, ProviderSnapshotEvidence,
    ProviderTimestampEvidence, RawFrameFactory, SequenceValidationProfile, SourceHealthSnapshot,
    SourceMetadata, SourceProtocolProfile, TransportFrameKind, ValidatedSessionDecodeOutcome,
};
use tokio_util::sync::CancellationToken;

use super::display_market::{
    DisplayMarketActorLimits, DisplayMarketActorShutdown, DisplayMarketDirectory,
    DisplayMarketIngress, DisplayMarketKey, DisplayMarketReadAdmission,
    DisplayMarketSupervisorMonitor,
};

/// Exact canonical/provider identity and execution terms admitted for one requested symbol.
#[derive(Clone, Debug)]
pub(crate) struct SchwabRestQuoteCurrentInstrument {
    provider_symbol: ProviderIdentifier,
    source_identifier: SourceIdentifier,
    instrument_id: InstrumentId,
    execution_terms: InstrumentExecutionTerms,
}

impl SchwabRestQuoteCurrentInstrument {
    pub(crate) fn try_new(
        provider_symbol: ProviderIdentifier,
        source_identifier: SourceIdentifier,
        instrument_id: InstrumentId,
        execution_terms: InstrumentExecutionTerms,
    ) -> Result<Self, SchwabRestQuoteCurrentUnavailable> {
        if execution_terms.instrument_id() != instrument_id {
            return Err(SchwabRestQuoteCurrentUnavailable::AuthorityOrHealth);
        }
        Ok(Self {
            provider_symbol,
            source_identifier,
            instrument_id,
            execution_terms,
        })
    }
}

/// Opaque same-response evidence retained until durable quote publication has committed.
///
/// The original adapter response remains the sole archival-seal input. This non-serializable
/// projection retains only exact bounded bytes and the already-validated quote payload required
/// by the registered current-session path after archival and canonical publication succeed.
#[derive(Debug)]
pub(crate) struct SchwabRestQuoteCurrentEvidence {
    exact_body: Bytes,
    receipt: RawRestResponseReceipt,
    accounting: RestItemAccounting,
    quotes: ParsedNative<QuoteResponse>,
}

impl SchwabRestQuoteCurrentEvidence {
    pub(crate) fn try_from_response(
        response: &ExecutedRestResponse,
    ) -> Result<Self, SchwabRestQuoteCurrentUnavailable> {
        let SchwabRestPayload::Quotes(quotes) = response.payload() else {
            return Err(SchwabRestQuoteCurrentUnavailable::Decode);
        };
        Ok(Self {
            exact_body: copy_exact_body(response.capture().exact_body())?,
            receipt: response.capture().receipt().clone(),
            accounting: response.accounting(),
            quotes: quotes.clone(),
        })
    }
}

/// Borrowed exact response evidence and source contract supplied to current publication.
pub(crate) struct SchwabRestQuoteCurrentRequest<'a> {
    response: &'a SchwabRestQuoteCurrentEvidence,
    metadata: &'a SourceMetadata,
    venue_id: &'a VenueId,
    delay: SchwabRestDelayEvidence,
    instruments: &'a [SchwabRestQuoteCurrentInstrument],
    deadline: Instant,
}

impl<'a> SchwabRestQuoteCurrentRequest<'a> {
    pub(crate) fn new(
        response: &'a SchwabRestQuoteCurrentEvidence,
        metadata: &'a SourceMetadata,
        venue_id: &'a VenueId,
        delay: SchwabRestDelayEvidence,
        instruments: &'a [SchwabRestQuoteCurrentInstrument],
        deadline: Instant,
    ) -> Self {
        Self {
            response,
            metadata,
            venue_id,
            delay,
            instruments,
            deadline,
        }
    }
}

/// Exact current-display outcome retained separately from durable archival publication.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SchwabRestQuoteCurrentPublication {
    /// The response was not eligible for a current attempt, such as a provider rejection.
    NotApplicable,
    /// Current capture and decode completed but every returned quote truthfully abstained.
    NoPublishableQuotes,
    /// Registry-qualified observations entered every exact display route.
    Published {
        observations: u64,
        source_generation: ConnectionGeneration,
    },
    /// Current publication did not occur for this exact typed reason.
    Unavailable(SchwabRestQuoteCurrentUnavailable),
}

impl SchwabRestQuoteCurrentPublication {
    pub(crate) const fn published(self) -> u64 {
        match self {
            Self::Published { observations, .. } => observations,
            Self::NotApplicable | Self::NoPublishableQuotes | Self::Unavailable(_) => 0,
        }
    }
}

/// Closed current-session failure truth; none of these states authorizes display success.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SchwabRestQuoteCurrentUnavailable {
    /// The bounded current operation expired before publication.
    Deadline,
    /// Registry session, metadata, coverage, health, or currentness authority was unavailable.
    AuthorityOrHealth,
    /// Exact raw-frame construction or capture admission failed.
    Capture,
    /// The accepted provider response could not produce a valid current decoded batch.
    Decode,
    /// A required display route was absent, stale, saturated, or terminal.
    Display,
    /// A bounded allocation failed.
    Allocation,
    /// Another response currently owns the sole generation-local capture/decode path.
    Busy,
    /// The sole synchronous current-generation owner was poisoned.
    Poisoned,
}

/// Object-safe exact-response current publication hook used by the Schwab seal-first sink.
pub(crate) trait SchwabRestQuoteCurrentBridge: std::fmt::Debug + Send + Sync {
    fn publish_current(
        &self,
        request: SchwabRestQuoteCurrentRequest<'_>,
    ) -> SchwabRestQuoteCurrentPublication;
}

/// Capabilities moved from the lifecycle/composition owner into one current generation.
///
/// No constructor in this module can mint any field. Capture activation, current health, and
/// display registration happen before this one-use handoff; the resulting bridge becomes the sole
/// owner responsible for exact-session revocation and complete generation drain.
#[derive(Debug)]
pub(crate) struct SchwabRestQuoteCurrentSessionInput {
    registry: AuthoritativeSourceRegistry,
    session: CurrentSourceSession,
    raw_frames: RawFrameFactory,
    capture: RawCapturePublisher<CaptureGenerationCapabilities>,
    capture_control: RawCaptureControl<CaptureGenerationCapabilities>,
    capture_writer: CaptureWriterHandle<CaptureGenerationCapabilities>,
    health_reporter: CurrentHealthReporter,
    display_directory: DisplayMarketDirectory,
    display_ingresses: Vec<DisplayMarketIngress>,
    ingress_timeout: Duration,
    display_shutdown_timeout: Duration,
    capture_shutdown_timeout: Duration,
}

impl SchwabRestQuoteCurrentSessionInput {
    #[allow(
        clippy::too_many_arguments,
        reason = "every non-reconstructible current-generation capability remains explicit"
    )]
    pub(crate) fn new(
        registry: AuthoritativeSourceRegistry,
        session: CurrentSourceSession,
        raw_frames: RawFrameFactory,
        capture: RawCapturePublisher<CaptureGenerationCapabilities>,
        capture_control: RawCaptureControl<CaptureGenerationCapabilities>,
        capture_writer: CaptureWriterHandle<CaptureGenerationCapabilities>,
        health_reporter: CurrentHealthReporter,
        display_directory: DisplayMarketDirectory,
        display_ingresses: Vec<DisplayMarketIngress>,
        ingress_timeout: Duration,
        display_shutdown_timeout: Duration,
        capture_shutdown_timeout: Duration,
    ) -> Self {
        Self {
            registry,
            session,
            raw_frames,
            capture,
            capture_control,
            capture_writer,
            health_reporter,
            display_directory,
            display_ingresses,
            ingress_timeout,
            display_shutdown_timeout,
            capture_shutdown_timeout,
        }
    }

    /// Returns the registry-minted generation for this exact current source session.
    ///
    /// This is deliberately independent from the OAuth access-token generation: a source
    /// connection may restart while the same token epoch remains current.
    pub(crate) fn connection_generation(&self) -> ConnectionGeneration {
        self.session.generation()
    }

    /// Activates the already constructed capture channel before any display route is registered.
    pub(crate) fn activate_capture_initial(
        &mut self,
    ) -> Result<(), SchwabRestQuoteCurrentUnavailable> {
        self.capture_control
            .activate_initial()
            .map_err(|_error| SchwabRestQuoteCurrentUnavailable::Capture)
    }

    /// Registers and retains one exact display route under this session's cleanup owner.
    pub(crate) async fn register_display_route(
        &mut self,
        key: DisplayMarketKey,
        limits: DisplayMarketActorLimits,
        read_admission: DisplayMarketReadAdmission,
        cancellation: &CancellationToken,
        deadline: Instant,
    ) -> Result<DisplayMarketSupervisorMonitor, SchwabRestQuoteCurrentUnavailable> {
        if key.source_id() != self.session.source_id()
            || key.generation() != self.session.generation()
        {
            return Err(SchwabRestQuoteCurrentUnavailable::Display);
        }
        let registration = self
            .display_directory
            .register(key, limits, read_admission, cancellation, deadline)
            .await
            .map_err(|_error| SchwabRestQuoteCurrentUnavailable::Display)?;
        let (ingress, monitor) = registration.into_parts();
        self.display_ingresses.push(ingress);
        Ok(monitor)
    }

    pub(crate) async fn shutdown(mut self) -> Result<(), SchwabRestQuoteCurrentUnavailable> {
        let mut failure = None;
        let deadline = Instant::now()
            .checked_add(self.display_shutdown_timeout)
            .ok_or(SchwabRestQuoteCurrentUnavailable::Deadline)?;
        retain_current_failure(
            &mut failure,
            self.registry
                .end_session(&self.session, self.session.started_at())
                .map_err(|_error| SchwabRestQuoteCurrentUnavailable::AuthorityOrHealth),
        );
        let cleanup_cancellation = CancellationToken::new();
        for ingress in &self.display_ingresses {
            let result = self
                .display_directory
                .unregister(ingress.key(), &cleanup_cancellation, deadline)
                .await
                .map_err(|_error| SchwabRestQuoteCurrentUnavailable::Display)
                .and_then(|shutdown| {
                    if shutdown == DisplayMarketActorShutdown::Graceful {
                        Ok(())
                    } else {
                        Err(SchwabRestQuoteCurrentUnavailable::Display)
                    }
                });
            retain_current_failure(&mut failure, result);
        }
        drop(self.display_ingresses);
        drop(self.health_reporter);
        drop(self.raw_frames);
        drop(self.capture);
        self.capture_control.invalidate_current();
        drop(self.capture_control);
        retain_current_failure(
            &mut failure,
            shutdown_capture_writer(self.capture_writer, self.capture_shutdown_timeout).await,
        );
        retain_current_failure(
            &mut failure,
            self.registry
                .shutdown()
                .map_err(|_error| SchwabRestQuoteCurrentUnavailable::AuthorityOrHealth),
        );
        failure.map_or(Ok(()), Err)
    }
}

/// Sole synchronous owner of one already-registered current Schwab quote generation.
pub(crate) struct SchwabRestQuoteCurrentSessionBridge {
    state: Mutex<SchwabRestQuoteCurrentSessionInput>,
}

impl std::fmt::Debug for SchwabRestQuoteCurrentSessionBridge {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SchwabRestQuoteCurrentSessionBridge")
            .finish_non_exhaustive()
    }
}

impl SchwabRestQuoteCurrentSessionBridge {
    #[allow(
        clippy::too_many_arguments,
        reason = "the bridge binds every exact current and durable generation coordinate"
    )]
    pub(crate) async fn try_new(
        input: SchwabRestQuoteCurrentSessionInput,
        metadata: &SourceMetadata,
        durable_session_id: &SourceIdentifier,
        generation: ConnectionGeneration,
        venue_id: &VenueId,
        instruments: &[SchwabRestQuoteCurrentInstrument],
    ) -> Result<Self, SchwabRestQuoteCurrentUnavailable> {
        if let Err(error) = validate_session_input(
            &input,
            metadata,
            durable_session_id,
            generation,
            venue_id,
            instruments,
        ) {
            return match input.shutdown().await {
                Ok(()) => Err(error),
                Err(cleanup) => Err(cleanup),
            };
        }
        Ok(Self {
            state: Mutex::new(input),
        })
    }

    /// Returns the advanced generation capabilities after the sole bridge owner is reclaimed.
    /// The lifecycle owner can then end the session and shut down the registry exactly once.
    pub(crate) fn into_input(
        self,
    ) -> Result<SchwabRestQuoteCurrentSessionInput, SchwabRestQuoteCurrentUnavailable> {
        self.state
            .into_inner()
            .map_err(|_poisoned| SchwabRestQuoteCurrentUnavailable::Poisoned)
    }

    /// Ends the exact registry session, unregisters its display routes, and drains capture.
    pub(crate) async fn shutdown(self) -> Result<(), SchwabRestQuoteCurrentUnavailable> {
        self.into_input()?.shutdown().await
    }
}

impl SchwabRestQuoteCurrentBridge for SchwabRestQuoteCurrentSessionBridge {
    fn publish_current(
        &self,
        request: SchwabRestQuoteCurrentRequest<'_>,
    ) -> SchwabRestQuoteCurrentPublication {
        let mut state = match self.state.try_lock() {
            Ok(state) => state,
            Err(TryLockError::WouldBlock) => {
                return SchwabRestQuoteCurrentPublication::Unavailable(
                    SchwabRestQuoteCurrentUnavailable::Busy,
                );
            }
            Err(TryLockError::Poisoned(_poisoned)) => {
                return SchwabRestQuoteCurrentPublication::Unavailable(
                    SchwabRestQuoteCurrentUnavailable::Poisoned,
                );
            }
        };
        match state.publish(request) {
            Ok(publication) => publication,
            Err(reason) => SchwabRestQuoteCurrentPublication::Unavailable(reason),
        }
    }
}

impl SchwabRestQuoteCurrentSessionInput {
    fn publish(
        &mut self,
        request: SchwabRestQuoteCurrentRequest<'_>,
    ) -> Result<SchwabRestQuoteCurrentPublication, SchwabRestQuoteCurrentUnavailable> {
        require_deadline(request.deadline)?;
        validate_request(self, &request)?;

        let body = request.response.exact_body.clone();
        require_deadline(request.deadline)?;
        let frame = self
            .raw_frames
            .try_frame(TransportFrameKind::Text, body)
            .map_err(|_error| SchwabRestQuoteCurrentUnavailable::Capture)?;
        let capture_receipt = self
            .capture
            .try_publish(&frame)
            .map_err(|_error| SchwabRestQuoteCurrentUnavailable::Capture)?;
        let validated_frame = self
            .session
            .validate_live_frame(&frame)
            .map_err(|_error| SchwabRestQuoteCurrentUnavailable::AuthorityOrHealth)?;
        let decoded = decode_quotes(&validated_frame, &request)?;
        require_deadline(request.deadline)?;
        let Some(decoded) = decoded else {
            return Ok(SchwabRestQuoteCurrentPublication::NoPublishableQuotes);
        };
        let observation_count = u64::try_from(decoded.batch.observations().len())
            .map_err(|_error| SchwabRestQuoteCurrentUnavailable::Allocation)?;
        let validated_at = decoded.batch.evidence().received_at();
        let validated_session = self
            .registry
            .validate_session(&self.session, validated_at)
            .map_err(|_error| SchwabRestQuoteCurrentUnavailable::AuthorityOrHealth)?;
        let captured = validated_session
            .validate_decode_outcome_owned(DecodeOutcome::Data(decoded.batch), capture_receipt)
            .map_err(|_error| SchwabRestQuoteCurrentUnavailable::AuthorityOrHealth)?;
        let ValidatedSessionDecodeOutcome::Data(captured) = captured else {
            return Err(SchwabRestQuoteCurrentUnavailable::Decode);
        };
        self.record_current_health(
            request.metadata,
            validated_at,
            decoded.latest_source_at,
            request.response.receipt.body_sha256(),
        )?;
        let current = self
            .registry
            .validate_current_authority(&self.session)
            .map_err(|_error| SchwabRestQuoteCurrentUnavailable::AuthorityOrHealth)?;
        let batches = current
            .validate_data_outcome_owned(captured)
            .map_err(|_error| SchwabRestQuoteCurrentUnavailable::AuthorityOrHealth)?;
        require_deadline(request.deadline)?;
        let ingress_deadline = Instant::now()
            .checked_add(self.ingress_timeout)
            .map(|candidate| candidate.min(request.deadline))
            .ok_or(SchwabRestQuoteCurrentUnavailable::Deadline)?;
        let mut prepared = Vec::new();
        prepared
            .try_reserve_exact(batches.len())
            .map_err(|_error| SchwabRestQuoteCurrentUnavailable::Allocation)?;
        for batch in batches {
            let ingress = display_ingress(&self.display_ingresses, &batch)?;
            ingress
                .preflight(&batch, validated_at, ingress_deadline)
                .map_err(|_error| SchwabRestQuoteCurrentUnavailable::Display)?;
            prepared.push(batch);
        }
        if observation_count == 0 {
            return Err(SchwabRestQuoteCurrentUnavailable::Decode);
        }
        for batch in prepared {
            let ingress = display_ingress(&self.display_ingresses, &batch)?;
            ingress
                .try_publish(batch, validated_at, ingress_deadline)
                .map_err(|_error| SchwabRestQuoteCurrentUnavailable::Display)?;
        }
        Ok(SchwabRestQuoteCurrentPublication::Published {
            observations: observation_count,
            source_generation: self.session.generation(),
        })
    }

    fn record_current_health(
        &mut self,
        metadata: &SourceMetadata,
        observed_at: Timestamp,
        latest_source_at: Option<Timestamp>,
        payload_digest: [u8; 32],
    ) -> Result<(), SchwabRestQuoteCurrentUnavailable> {
        let authorization_deadline = metadata
            .authorization()
            .inclusive_authorization_deadline()
            .ok_or(SchwabRestQuoteCurrentUnavailable::AuthorityOrHealth)?;
        let coverage_deadline = metadata
            .coverage()
            .inclusive_coverage_deadline()
            .ok_or(SchwabRestQuoteCurrentUnavailable::AuthorityOrHealth)?;
        let live = metadata
            .coverage()
            .live()
            .ok_or(SchwabRestQuoteCurrentUnavailable::AuthorityOrHealth)?;
        let health = SourceHealthSnapshot::try_new(
            &self.session,
            observed_at,
            ConnectionLiveness::Live {
                last_activity_at: observed_at,
            },
            Some(observed_at),
            Some(observed_at),
            latest_source_at,
            metadata.freshness_policy(),
            StreamIntegrityState::Healthy,
            self.capture.integrity(),
            AuthorizationHealth::Valid {
                evidence: metadata.authorization().evidence().clone(),
                valid_until: authorization_deadline,
            },
            CoverageHealth::Sufficient {
                evidence: ExactPayloadEvidence::from_content_digest(
                    market_squawk_domain::EvidenceDigest::new(
                        market_squawk_domain::DigestAlgorithm::Sha256,
                        payload_digest,
                    ),
                ),
                provider_product: live.provider_product().clone(),
                provider_channel: live.provider_channel().clone(),
                valid_until: coverage_deadline,
            },
            BudgetHealth::Available,
            None,
            Vec::new(),
        )
        .map_err(|_error| SchwabRestQuoteCurrentUnavailable::AuthorityOrHealth)?;
        let update = self
            .health_reporter
            .report(health)
            .map_err(|_error| SchwabRestQuoteCurrentUnavailable::AuthorityOrHealth)?;
        if self
            .registry
            .record_health_with_qualification(&self.session, update)
            .map_err(|_error| SchwabRestQuoteCurrentUnavailable::AuthorityOrHealth)?
            != CurrentHealthRecording::Qualified
        {
            return Err(SchwabRestQuoteCurrentUnavailable::AuthorityOrHealth);
        }
        Ok(())
    }
}

#[allow(
    clippy::too_many_arguments,
    reason = "current construction validates every exact source/session/route coordinate"
)]
fn validate_session_input(
    input: &SchwabRestQuoteCurrentSessionInput,
    metadata: &SourceMetadata,
    durable_session_id: &SourceIdentifier,
    generation: ConnectionGeneration,
    venue_id: &VenueId,
    instruments: &[SchwabRestQuoteCurrentInstrument],
) -> Result<(), SchwabRestQuoteCurrentUnavailable> {
    if input.ingress_timeout.is_zero()
        || input.display_shutdown_timeout.is_zero()
        || input.capture_shutdown_timeout.is_zero()
        || input.display_ingresses.is_empty()
        || instruments.is_empty()
        || input.display_ingresses.len() != instruments.len()
    {
        return Err(SchwabRestQuoteCurrentUnavailable::Display);
    }
    if input.capture.integrity() != CaptureIntegrityState::Healthy
        || input.capture.health_snapshot() != input.capture_control.identity()
    {
        return Err(SchwabRestQuoteCurrentUnavailable::Capture);
    }
    if input.session.source_id() != metadata.source_id()
        || input.session.revision() != metadata.revision()
        || input.session.session_id().as_source_identifier() != durable_session_id
        || input.session.generation() != generation
    {
        return Err(SchwabRestQuoteCurrentUnavailable::AuthorityOrHealth);
    }
    input
        .session
        .validate_current_lease()
        .map_err(|_error| SchwabRestQuoteCurrentUnavailable::AuthorityOrHealth)?;
    input
        .registry
        .validate_session(&input.session, input.session.started_at())
        .map_err(|_error| SchwabRestQuoteCurrentUnavailable::AuthorityOrHealth)?;
    for (index, ingress) in input.display_ingresses.iter().enumerate() {
        let key = ingress.key();
        if key.source_id() != input.session.source_id()
            || key.generation() != input.session.generation()
            || key.venue_id() != venue_id
            || !instruments
                .iter()
                .any(|instrument| instrument.instrument_id == key.instrument_id())
            || input.display_ingresses[index.saturating_add(1)..]
                .iter()
                .any(|candidate| {
                    candidate.key().venue_id() == key.venue_id()
                        && candidate.key().instrument_id() == key.instrument_id()
                })
        {
            return Err(SchwabRestQuoteCurrentUnavailable::Display);
        }
    }
    Ok(())
}

async fn shutdown_capture_writer(
    writer: CaptureWriterHandle<CaptureGenerationCapabilities>,
    timeout: Duration,
) -> Result<(), SchwabRestQuoteCurrentUnavailable> {
    let mut pending = writer.shutdown(timeout);
    let status = pending.wait_until_deadline().await;
    if status == CaptureShutdownStatus::DeadlineElapsed {
        pending.wait_until_terminated().await;
    }
    let termination = pending
        .try_reap()
        .map_err(|_error| SchwabRestQuoteCurrentUnavailable::Capture)?
        .ok_or(SchwabRestQuoteCurrentUnavailable::Capture)?;
    if status == CaptureShutdownStatus::DeadlineElapsed
        || termination.shutdown_deadline_elapsed()
        || termination.outcome().is_incomplete()
    {
        return Err(SchwabRestQuoteCurrentUnavailable::Capture);
    }
    Ok(())
}

fn retain_current_failure(
    retained: &mut Option<SchwabRestQuoteCurrentUnavailable>,
    candidate: Result<(), SchwabRestQuoteCurrentUnavailable>,
) {
    if retained.is_none() {
        *retained = candidate.err();
    }
}

fn validate_request(
    state: &SchwabRestQuoteCurrentSessionInput,
    request: &SchwabRestQuoteCurrentRequest<'_>,
) -> Result<(), SchwabRestQuoteCurrentUnavailable> {
    let response = request.response;
    let receipt = &response.receipt;
    let accounting = response.accounting;
    let parsed = &response.quotes;
    let provider_records = u64::try_from(parsed.value().quotes().len())
        .map_err(|_error| SchwabRestQuoteCurrentUnavailable::Allocation)?;
    let requested = u64::try_from(request.instruments.len())
        .map_err(|_error| SchwabRestQuoteCurrentUnavailable::Allocation)?;
    if receipt.route() != ReadOnlyRoute::Quotes
        || !(200..=299).contains(&receipt.status())
        || receipt.body_sha256() != parsed.raw_sha256()
        || receipt.body_bytes()
            != u64::try_from(response.exact_body.len())
                .map_err(|_error| SchwabRestQuoteCurrentUnavailable::Allocation)?
        || request.metadata.source_id() != state.session.source_id()
        || request.metadata.revision() != state.session.revision()
        || accounting.requested != requested
        || accounting.returned.checked_add(accounting.missing) != Some(accounting.requested)
        || accounting.returned > accounting.provider_records
        || accounting.provider_records != provider_records
        || accounting.returned.checked_add(accounting.unexpected) != Some(provider_records)
        || !request
            .metadata
            .is_effective_at(timestamp_from_millis(receipt.received_at_unix_millis())?)
        || request.instruments.is_empty()
        || request.metadata.coverage().live().is_none()
        || !request
            .metadata
            .coverage()
            .topology()
            .contains_venue(request.venue_id)
        || !delay_matches(request.metadata.coverage().delay(), request.delay)
    {
        return Err(SchwabRestQuoteCurrentUnavailable::AuthorityOrHealth);
    }
    for (index, instrument) in request.instruments.iter().enumerate() {
        if request.instruments[index.saturating_add(1)..]
            .iter()
            .any(|candidate| {
                candidate.provider_symbol == instrument.provider_symbol
                    || candidate.instrument_id == instrument.instrument_id
            })
        {
            return Err(SchwabRestQuoteCurrentUnavailable::AuthorityOrHealth);
        }
    }
    Ok(())
}

struct DecodedQuotes {
    batch: DecodedProviderBatch,
    latest_source_at: Option<Timestamp>,
}

fn decode_quotes(
    frame: &market_squawk_sources::ValidatedRawMarketFrame<'_>,
    request: &SchwabRestQuoteCurrentRequest<'_>,
) -> Result<Option<DecodedQuotes>, SchwabRestQuoteCurrentUnavailable> {
    let SourceProtocolProfile::Live(protocol) = request.metadata.protocol_profile() else {
        return Err(SchwabRestQuoteCurrentUnavailable::AuthorityOrHealth);
    };
    let sequence_rule = match protocol.sequence() {
        SequenceValidationProfile::Unsupported { rule } => rule.clone(),
        SequenceValidationProfile::Provided { .. } => {
            return Err(SchwabRestQuoteCurrentUnavailable::AuthorityOrHealth);
        }
    };
    let checksum_rule = match protocol.checksum() {
        ChecksumValidationProfile::Unsupported { rule } => rule.clone(),
        ChecksumValidationProfile::Provided { .. } => {
            return Err(SchwabRestQuoteCurrentUnavailable::AuthorityOrHealth);
        }
    };
    let snapshot_rule = request
        .metadata
        .coverage()
        .live()
        .and_then(|coverage| coverage.rule_for(market_squawk_domain::LiveEventClass::Quote, None))
        .and_then(|rule| match rule.snapshot_applicability() {
            SnapshotApplicability::NotApplicable { metadata_rule } => Some(metadata_rule.clone()),
            SnapshotApplicability::Required => None,
        })
        .ok_or(SchwabRestQuoteCurrentUnavailable::AuthorityOrHealth)?;
    let decoder_evidence =
        DecoderEvidence::from_validated_frame(frame, protocol.decoder_rule().clone());
    let parsed = &request.response.quotes;
    if decoder_evidence.payload_digest().bytes() != parsed.raw_sha256() {
        return Err(SchwabRestQuoteCurrentUnavailable::Decode);
    }
    let mut observations = Vec::new();
    let mut latest_source_at: Option<Timestamp> = None;
    observations
        .try_reserve_exact(parsed.value().quotes().len())
        .map_err(|_error| SchwabRestQuoteCurrentUnavailable::Allocation)?;
    for quote in parsed.value().quotes() {
        if realtime_delay_conflicts(quote, request.delay)? {
            return Err(SchwabRestQuoteCurrentUnavailable::AuthorityOrHealth);
        }
        let instrument = request
            .instruments
            .iter()
            .find(|candidate| candidate.provider_symbol.as_str() == quote.symbol().as_str())
            .ok_or(SchwabRestQuoteCurrentUnavailable::AuthorityOrHealth)?;
        let Some(payload) = quote_payload(quote, instrument.execution_terms)? else {
            continue;
        };
        let timestamp = quote_timestamp(quote, protocol)?;
        let Some(timestamp) = timestamp else {
            continue;
        };
        if let ProviderTimestampEvidence::Provided { value, .. } = &timestamp {
            latest_source_at = Some(latest_source_at.map_or(*value, |latest| latest.max(*value)));
        }
        observations.push(
            ProviderNormalizedObservation::try_new(
                instrument.source_identifier.clone(),
                request.venue_id.clone(),
                instrument.instrument_id,
                timestamp,
                ProviderSequenceEvidence::Unsupported {
                    rule: sequence_rule.clone(),
                },
                ProviderSnapshotEvidence::NotApplicable(snapshot_rule.clone()),
                ProviderChecksumEvidence::Unsupported {
                    rule: checksum_rule.clone(),
                },
                payload,
            )
            .map_err(|_error| SchwabRestQuoteCurrentUnavailable::Decode)?,
        );
    }
    if observations.is_empty() {
        return Ok(None);
    }
    DecodedProviderBatch::try_new(decoder_evidence, observations)
        .map(|batch| {
            Some(DecodedQuotes {
                batch,
                latest_source_at,
            })
        })
        .map_err(|_error| SchwabRestQuoteCurrentUnavailable::Decode)
}

fn realtime_delay_conflicts(
    quote: &SchwabQuote,
    delay: SchwabRestDelayEvidence,
) -> Result<bool, SchwabRestQuoteCurrentUnavailable> {
    Ok(match quote.realtime() {
        NativeField::Value(true) => delay != SchwabRestDelayEvidence::RealTime,
        NativeField::Value(false) => delay == SchwabRestDelayEvidence::RealTime,
        NativeField::Absent | NativeField::Null => false,
    })
}

fn quote_payload(
    quote: &SchwabQuote,
    terms: InstrumentExecutionTerms,
) -> Result<Option<ProviderObservationPayload>, SchwabRestQuoteCurrentUnavailable> {
    let bid = quote_side(
        quote,
        QuoteComponentField::BidPrice,
        QuoteComponentField::BidSize,
        terms,
    )?;
    let ask = quote_side(
        quote,
        QuoteComponentField::AskPrice,
        QuoteComponentField::AskSize,
        terms,
    )?;
    match (bid, ask) {
        (QuoteSide::Abstain, _) | (_, QuoteSide::Abstain) => Ok(None),
        (QuoteSide::Absent, QuoteSide::Absent) => Ok(None),
        (bid, ask) => ProviderObservationPayload::quote(bid.level(), ask.level())
            .map(Some)
            .map_err(|_error| SchwabRestQuoteCurrentUnavailable::Decode),
    }
}

enum QuoteSide {
    Absent,
    Level(ProviderBookLevel),
    Abstain,
}

impl QuoteSide {
    fn level(self) -> Option<ProviderBookLevel> {
        match self {
            Self::Level(level) => Some(level),
            Self::Absent | Self::Abstain => None,
        }
    }
}

fn quote_side(
    quote: &SchwabQuote,
    price_name: QuoteComponentField,
    quantity_name: QuoteComponentField,
    terms: InstrumentExecutionTerms,
) -> Result<QuoteSide, SchwabRestQuoteCurrentUnavailable> {
    let price = quote_number(quote, price_name)?;
    let quantity = quote_number(quote, quantity_name)?;
    let (Some(price), Some(quantity)) = (price, quantity) else {
        return if price.is_none() && quantity.is_none() {
            Ok(QuoteSide::Absent)
        } else {
            Ok(QuoteSide::Abstain)
        };
    };
    let price = ProviderDecimalLexeme::try_new(price)
        .map_err(|_error| SchwabRestQuoteCurrentUnavailable::Decode)?;
    let quantity = ProviderDecimalLexeme::try_new(quantity)
        .map_err(|_error| SchwabRestQuoteCurrentUnavailable::Decode)?;
    if PriceTicks::try_from_decimal(price.decimal(), terms.price_tick()).is_err()
        || QuantityLots::try_from_decimal(quantity.decimal(), terms.lot_size())
            .map(|lots| lots.get() == 0)
            .unwrap_or(true)
    {
        return Ok(QuoteSide::Abstain);
    }
    Ok(QuoteSide::Level(ProviderBookLevel::new(
        ProviderPrice::new(price),
        ProviderQuantity::new(quantity),
    )))
}

fn quote_number(
    quote: &SchwabQuote,
    name: QuoteComponentField,
) -> Result<Option<&str>, SchwabRestQuoteCurrentUnavailable> {
    match quote
        .quote_fields()
        .iter()
        .find(|field| field.name() == &name)
        .map(|field| field.value())
    {
        None | Some(NativeScalar::Null) => Ok(None),
        Some(NativeScalar::Number(value)) => Ok(Some(value.as_str())),
        Some(NativeScalar::Bool(_) | NativeScalar::Text(_)) => {
            Err(SchwabRestQuoteCurrentUnavailable::Decode)
        }
    }
}

fn quote_timestamp(
    quote: &SchwabQuote,
    protocol: &market_squawk_sources::LiveProtocolProfile,
) -> Result<Option<ProviderTimestampEvidence>, SchwabRestQuoteCurrentUnavailable> {
    let value = quote_number(quote, QuoteComponentField::QuoteTime)?;
    match (value, protocol.source_timestamps()) {
        (Some(value), true) => {
            let timestamp = value
                .parse::<u64>()
                .map_err(|_error| SchwabRestQuoteCurrentUnavailable::Decode)
                .and_then(timestamp_from_millis)?;
            Ok(Some(ProviderTimestampEvidence::Provided {
                value: timestamp,
                rule: protocol.timestamp_rule().clone(),
            }))
        }
        (None, false) => Ok(Some(ProviderTimestampEvidence::AuthoritativelyAbsent(
            protocol.timestamp_rule().clone(),
        ))),
        (None, true) => Ok(None),
        (Some(_), false) => Err(SchwabRestQuoteCurrentUnavailable::AuthorityOrHealth),
    }
}

fn display_ingress<'a>(
    ingresses: &'a [DisplayMarketIngress],
    batch: &CurrentDecodedProviderBatch,
) -> Result<&'a DisplayMarketIngress, SchwabRestQuoteCurrentUnavailable> {
    ingresses
        .iter()
        .find(|ingress| {
            ingress.key().venue_id() == batch.key().venue()
                && ingress.key().instrument_id() == batch.key().instrument()
        })
        .ok_or(SchwabRestQuoteCurrentUnavailable::Display)
}

fn require_deadline(deadline: Instant) -> Result<(), SchwabRestQuoteCurrentUnavailable> {
    if Instant::now() >= deadline {
        Err(SchwabRestQuoteCurrentUnavailable::Deadline)
    } else {
        Ok(())
    }
}

fn timestamp_from_millis(value: u64) -> Result<Timestamp, SchwabRestQuoteCurrentUnavailable> {
    let nanos = value
        .checked_mul(1_000_000)
        .and_then(|value| i64::try_from(value).ok())
        .ok_or(SchwabRestQuoteCurrentUnavailable::Decode)?;
    Ok(Timestamp::from_unix_nanos(nanos))
}

fn copy_exact_body(body: &[u8]) -> Result<Bytes, SchwabRestQuoteCurrentUnavailable> {
    let mut owned = Vec::new();
    owned
        .try_reserve_exact(body.len())
        .map_err(|_error| SchwabRestQuoteCurrentUnavailable::Allocation)?;
    owned.extend_from_slice(body);
    Ok(Bytes::from(owned))
}

fn delay_matches(declared: CoverageDelay, observed: SchwabRestDelayEvidence) -> bool {
    match (declared, observed) {
        (CoverageDelay::RealTime, SchwabRestDelayEvidence::RealTime) => true,
        (CoverageDelay::Delayed(expected), SchwabRestDelayEvidence::Delayed(actual)) => {
            expected == actual.get()
        }
        _ => false,
    }
}
