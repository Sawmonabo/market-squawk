//! Bounded, generation-owned authority for non-executable display-market observations.
//!
//! Alpaca and Tradier observations enter this module only after the source registry has bound an
//! exact captured frame to current source, health, coverage, budget, and generation authority.
//! One task owns each route's mutable state. Readers receive bounded owned snapshots whose permits
//! remain leased for the lifetime of the result. Nothing in this module constructs execution terms,
//! canonical executable events, orders, signals, or risk authority.

#[path = "display_runtime.rs"]
pub(crate) mod runtime;

use std::{
    mem::size_of,
    num::{NonZeroU32, NonZeroUsize},
    sync::{
        Arc,
        atomic::{AtomicU8, Ordering},
    },
    time::Instant,
};

use market_squawk_domain::{
    AggressorSide, CaptureIntegrityState, ConnectionGeneration, CoverageConsolidation,
    CoverageDelay, CoverageStatus, DataQuality, DeliveryEvidence, EvidenceDigest, HaltTransition,
    InstrumentId, LiveEventClass, MarketDepth, MetadataRevision, RuleVersion, SourceId,
    SourceIdentifier, Timestamp, TradingStatus, VenueId,
};
use market_squawk_sources::{
    CoverageHealth, CurrentDecodedProviderBatch, CurrentProviderObservation,
    CurrentSourceAuthorityLease, FrameId, FreshnessPolicy, ProviderBookLevel,
    ProviderDecimalLexeme, ProviderObservationPayload, ProviderTimestampEvidence, RegistryError,
};
use rust_decimal::Decimal;
use thiserror::Error;
use tokio::{
    sync::{Mutex, OwnedSemaphorePermit, Semaphore, mpsc, oneshot, watch},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;

/// Code-owned ceiling for concurrently retained display-market generations.
pub(crate) const MAX_DISPLAY_MARKET_ROUTES: usize = 8_192;
/// Code-owned ceiling for queued ingress commands per route.
pub(crate) const MAX_DISPLAY_MARKET_INGRESS_COMMANDS: usize = 4_096;
/// Code-owned ceiling for queued or outstanding snapshot reads per route.
pub(crate) const MAX_DISPLAY_MARKET_OUTSTANDING_READS: usize = 1_024;

/// Read-admission gate for a staged account-runtime generation.
///
/// Startup admits once. Expiry, revocation, or shutdown may close the generation permanently;
/// the owning runtime must be replaced before reads can resume.
#[derive(Clone, Debug)]
pub(crate) struct DisplayMarketReadAdmission(Arc<AtomicU8>);

impl DisplayMarketReadAdmission {
    const CLOSED: u8 = 0;
    const ADMITTED: u8 = 1;
    const REVOKED: u8 = 2;

    pub(crate) fn closed() -> Self {
        Self(Arc::new(AtomicU8::new(Self::CLOSED)))
    }

    pub(crate) fn open() -> Self {
        Self(Arc::new(AtomicU8::new(Self::ADMITTED)))
    }

    pub(crate) fn admit(&self) -> bool {
        self.0
            .compare_exchange(
                Self::CLOSED,
                Self::ADMITTED,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
            || self.is_admitted()
    }

    pub(crate) fn is_admitted(&self) -> bool {
        self.0.load(Ordering::Acquire) == Self::ADMITTED
    }

    pub(crate) fn revoke(&self) {
        self.0.store(Self::REVOKED, Ordering::Release);
    }
}

/// Exact identity of one display-only source generation.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) struct DisplayMarketKey {
    source_id: SourceId,
    venue_id: VenueId,
    instrument_id: InstrumentId,
    generation: ConnectionGeneration,
}

/// Generation-independent route identity admitted by one display-only source composition.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) struct DisplayMarketRouteIdentity {
    venue_id: VenueId,
    instrument_id: InstrumentId,
}

impl DisplayMarketRouteIdentity {
    /// Fallibly owns the venue and stable internal instrument identity without execution terms.
    pub(crate) fn try_new(
        venue_id: &VenueId,
        instrument_id: InstrumentId,
    ) -> Result<Self, DisplayMarketDirectoryError> {
        Ok(Self {
            venue_id: try_clone_venue_id(venue_id)?,
            instrument_id,
        })
    }

    pub(crate) const fn venue_id(&self) -> &VenueId {
        &self.venue_id
    }

    pub(crate) const fn instrument_id(&self) -> InstrumentId {
        self.instrument_id
    }
}

impl DisplayMarketKey {
    /// Fallibly owns the complete anti-transplant route key.
    pub(crate) fn try_new(
        source_id: &SourceId,
        venue_id: &VenueId,
        instrument_id: InstrumentId,
        generation: ConnectionGeneration,
    ) -> Result<Self, DisplayMarketDirectoryError> {
        Ok(Self {
            source_id: try_clone_source_id(source_id)?,
            venue_id: try_clone_venue_id(venue_id)?,
            instrument_id,
            generation,
        })
    }

    pub(crate) const fn source_id(&self) -> &SourceId {
        &self.source_id
    }

    pub(crate) const fn venue_id(&self) -> &VenueId {
        &self.venue_id
    }

    pub(crate) const fn instrument_id(&self) -> InstrumentId {
        self.instrument_id
    }

    pub(crate) const fn generation(&self) -> ConnectionGeneration {
        self.generation
    }
}

/// Hard count and byte bounds for one route actor.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct DisplayMarketActorLimits {
    ingress_commands: NonZeroUsize,
    ingress_bytes: NonZeroU32,
    retained_state_bytes: NonZeroU32,
    outstanding_reads: NonZeroUsize,
    read_bytes: NonZeroU32,
    maximum_snapshot_bytes: NonZeroU32,
}

impl DisplayMarketActorLimits {
    /// Constructs count-, ingress-byte-, state-byte-, and leased-read bounds.
    pub(crate) fn try_new(
        ingress_commands: NonZeroUsize,
        ingress_bytes: NonZeroU32,
        retained_state_bytes: NonZeroU32,
        outstanding_reads: NonZeroUsize,
        read_bytes: NonZeroU32,
        maximum_snapshot_bytes: NonZeroU32,
    ) -> Result<Self, DisplayMarketConfigurationError> {
        if ingress_commands.get() > MAX_DISPLAY_MARKET_INGRESS_COMMANDS {
            return Err(DisplayMarketConfigurationError::IngressCommands {
                requested: ingress_commands.get(),
                maximum: MAX_DISPLAY_MARKET_INGRESS_COMMANDS,
            });
        }
        if outstanding_reads.get() > MAX_DISPLAY_MARKET_OUTSTANDING_READS {
            return Err(DisplayMarketConfigurationError::OutstandingReads {
                requested: outstanding_reads.get(),
                maximum: MAX_DISPLAY_MARKET_OUTSTANDING_READS,
            });
        }
        if maximum_snapshot_bytes > read_bytes {
            return Err(DisplayMarketConfigurationError::SnapshotBudget {
                snapshot: maximum_snapshot_bytes.get(),
                total: read_bytes.get(),
            });
        }
        for permits in [
            ingress_bytes.get() as usize,
            retained_state_bytes.get() as usize,
            read_bytes.get() as usize,
            maximum_snapshot_bytes.get() as usize,
        ] {
            if permits > Semaphore::MAX_PERMITS {
                return Err(DisplayMarketConfigurationError::SemaphorePermits {
                    requested: permits,
                    maximum: Semaphore::MAX_PERMITS,
                });
            }
        }
        Ok(Self {
            ingress_commands,
            ingress_bytes,
            retained_state_bytes,
            outstanding_reads,
            read_bytes,
            maximum_snapshot_bytes,
        })
    }
}

/// Invalid code-owned display-market bounds.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub(crate) enum DisplayMarketConfigurationError {
    #[error("display-market directory requested {requested} routes; maximum is {maximum}")]
    DirectoryRoutes { requested: usize, maximum: usize },
    #[error("display-market actor requested {requested} ingress commands; maximum is {maximum}")]
    IngressCommands { requested: usize, maximum: usize },
    #[error("display-market actor requested {requested} reads; maximum is {maximum}")]
    OutstandingReads { requested: usize, maximum: usize },
    #[error("display-market snapshot budget {snapshot} exceeds total read budget {total}")]
    SnapshotBudget { snapshot: u32, total: u32 },
    #[error("display-market actor requested {requested} permits; maximum is {maximum}")]
    SemaphorePermits { requested: usize, maximum: usize },
}

/// Stable fail-closed reason for one exact generation.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub(crate) enum DisplayMarketTerminalFailure {
    #[error("display-market ingress deadline elapsed")]
    IngressDeadline,
    #[error("display-market ingress command capacity was exhausted")]
    IngressCountSaturated,
    #[error("display-market ingress byte capacity was exhausted")]
    IngressBytesSaturated,
    #[error("display-market accounting overflowed")]
    AccountingOverflow,
    #[error("display-market owned projection allocation failed")]
    Allocation,
    #[error("display-market current source authority failed: {0}")]
    Registry(#[source] RegistryError),
    #[error("display-market observation violated integrity contract: {0}")]
    Integrity(#[source] DisplayMarketIntegrityFailure),
}

/// Relational failures that prevent a provider observation from entering display state.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub(crate) enum DisplayMarketIntegrityFailure {
    #[error("batch identity does not match its generation-owned route")]
    BatchIdentity,
    #[error("observation identity or current authority differs from its batch")]
    ObservationIdentity,
    #[error("frame, stream, coverage, or metadata identity is inconsistent")]
    ProvenanceMismatch,
    #[error("observation payload is not a supported quote, trade, or status event")]
    UnsupportedPayload,
    #[error("display-only authority rejected executable, stale, or quarantined source quality")]
    InvalidQuality,
    #[error("provider frame identity did not strictly advance")]
    FrameRegression,
    #[error("coverage or freshness deadline could not be represented")]
    TimeOverflow,
}

/// Nonblocking publisher into one exact generation owner.
#[derive(Debug)]
pub(crate) struct DisplayMarketIngress {
    key: Arc<DisplayMarketKey>,
    commands: mpsc::Sender<IngressCommand>,
    command_budget: Arc<Semaphore>,
    byte_budget: Arc<Semaphore>,
    terminal_requests: watch::Sender<Option<DisplayMarketTerminalFailure>>,
    terminal_state: watch::Receiver<Option<DisplayMarketTerminalFailure>>,
    actor_status: watch::Receiver<Option<DisplayMarketTerminalFailure>>,
    actor_cancellation: CancellationToken,
}

impl DisplayMarketIngress {
    pub(crate) fn key(&self) -> &DisplayMarketKey {
        &self.key
    }

    /// Revalidates and enqueues one capture-bound current batch without waiting.
    ///
    /// Any identity, authority, count, byte, or deadline failure terminally quarantines this exact
    /// generation. The actor performs a second authority and relational check before mutation.
    pub(crate) fn try_publish(
        &self,
        batch: CurrentDecodedProviderBatch,
        validated_at: Timestamp,
        deadline: Instant,
    ) -> Result<(), DisplayMarketIngressError> {
        self.require_open()?;
        if Instant::now() >= deadline {
            return Err(self.fail(DisplayMarketTerminalFailure::IngressDeadline));
        }
        self.validate_batch_identity(&batch)
            .map_err(|failure| self.fail(failure))?;
        batch
            .validate_at(validated_at)
            .map_err(|error| self.fail(DisplayMarketTerminalFailure::Registry(error)))?;
        let retained_bytes = batch
            .retained_bytes()
            .checked_add(size_of::<IngressTicket>())
            .and_then(|bytes| u32::try_from(bytes).ok())
            .ok_or_else(|| self.fail(DisplayMarketTerminalFailure::AccountingOverflow))?;
        let command_permit = Arc::clone(&self.command_budget)
            .try_acquire_owned()
            .map_err(|error| match error {
                tokio::sync::TryAcquireError::NoPermits => {
                    self.fail(DisplayMarketTerminalFailure::IngressCountSaturated)
                }
                tokio::sync::TryAcquireError::Closed => DisplayMarketIngressError::WorkerClosed,
            })?;
        let byte_permit = Arc::clone(&self.byte_budget)
            .try_acquire_many_owned(retained_bytes)
            .map_err(|error| match error {
                tokio::sync::TryAcquireError::NoPermits => {
                    self.fail(DisplayMarketTerminalFailure::IngressBytesSaturated)
                }
                tokio::sync::TryAcquireError::Closed => DisplayMarketIngressError::WorkerClosed,
            })?;
        let command = IngressCommand {
            batch,
            validated_at,
            _ticket: IngressTicket {
                _command_permit: command_permit,
                _byte_permit: byte_permit,
            },
        };
        match self.commands.try_send(command) {
            Ok(()) => Ok(()),
            Err(mpsc::error::TrySendError::Full(_command)) => {
                Err(self.fail(DisplayMarketTerminalFailure::IngressCountSaturated))
            }
            Err(mpsc::error::TrySendError::Closed(_command)) => {
                self.require_open()?;
                Err(DisplayMarketIngressError::WorkerClosed)
            }
        }
    }

    /// Preflights one batch without consuming it or reserving ingress capacity.
    ///
    /// The generation supervisor uses this across every partition of one captured frame before
    /// any partition is admitted. The sole publisher can still encounter bounded capacity or an
    /// actor failure during the subsequent nonblocking enqueue; either outcome terminally closes
    /// the complete source generation.
    pub(crate) fn preflight(
        &self,
        batch: &CurrentDecodedProviderBatch,
        validated_at: Timestamp,
        deadline: Instant,
    ) -> Result<(), DisplayMarketIngressError> {
        self.require_open()?;
        if Instant::now() >= deadline {
            return Err(self.fail(DisplayMarketTerminalFailure::IngressDeadline));
        }
        self.validate_batch_identity(batch)
            .map_err(|failure| self.fail(failure))?;
        batch
            .validate_at(validated_at)
            .map_err(|error| self.fail(DisplayMarketTerminalFailure::Registry(error)))
    }

    /// Returns the exact generation's terminal failure, if one has been published.
    pub(crate) fn current_failure(&self) -> Option<DisplayMarketTerminalFailure> {
        (*self.terminal_state.borrow()).or(*self.actor_status.borrow())
    }

    fn validate_batch_identity(
        &self,
        batch: &CurrentDecodedProviderBatch,
    ) -> Result<(), DisplayMarketTerminalFailure> {
        let binding = batch.current_lease().binding();
        if batch.key().venue() != self.key.venue_id()
            || batch.key().instrument() != self.key.instrument_id()
            || binding.source_id() != self.key.source_id()
            || binding.connection_generation() != self.key.generation()
        {
            return Err(DisplayMarketTerminalFailure::Integrity(
                DisplayMarketIntegrityFailure::BatchIdentity,
            ));
        }
        Ok(())
    }

    fn require_open(&self) -> Result<(), DisplayMarketIngressError> {
        if let Some(failure) = *self.terminal_state.borrow() {
            return Err(DisplayMarketIngressError::Terminal(failure));
        }
        if let Some(failure) = *self.actor_status.borrow() {
            return Err(DisplayMarketIngressError::Terminal(failure));
        }
        if self.actor_cancellation.is_cancelled()
            || self.commands.is_closed()
            || self.terminal_requests.is_closed()
        {
            return Err(DisplayMarketIngressError::WorkerClosed);
        }
        Ok(())
    }

    fn fail(&self, failure: DisplayMarketTerminalFailure) -> DisplayMarketIngressError {
        self.terminal_requests.send_if_modified(|current| {
            if current.is_none() {
                *current = Some(failure);
                true
            } else {
                false
            }
        });
        DisplayMarketIngressError::Terminal(failure)
    }
}

/// A producer could not enter one exact display-market generation.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub(crate) enum DisplayMarketIngressError {
    #[error("display-market generation failed terminally: {0}")]
    Terminal(#[source] DisplayMarketTerminalFailure),
    #[error("display-market generation actor is closed")]
    WorkerClosed,
}

#[derive(Debug)]
struct IngressTicket {
    _command_permit: OwnedSemaphorePermit,
    _byte_permit: OwnedSemaphorePermit,
}

#[derive(Debug)]
struct IngressCommand {
    batch: CurrentDecodedProviderBatch,
    validated_at: Timestamp,
    _ticket: IngressTicket,
}

/// Exact provider decimal retained without tick-size or lot-size conversion.
#[derive(Debug, Eq, PartialEq)]
pub(crate) struct DisplayDecimal {
    value: Decimal,
    provider_lexeme: String,
}

impl DisplayDecimal {
    pub(crate) const fn value(&self) -> Decimal {
        self.value
    }

    pub(crate) fn provider_lexeme(&self) -> &str {
        &self.provider_lexeme
    }

    fn try_from_provider(value: &ProviderDecimalLexeme) -> Result<Self, ProjectionError> {
        Ok(Self {
            value: value.decimal(),
            provider_lexeme: try_clone_text(value.as_str())?,
        })
    }

    fn try_clone(&self) -> Result<Self, ProjectionError> {
        Ok(Self {
            value: self.value,
            provider_lexeme: try_clone_text(&self.provider_lexeme)?,
        })
    }

    fn retained_bytes(&self) -> usize {
        self.provider_lexeme.capacity()
    }
}

/// One exact side of a provider top-of-book quote.
#[derive(Debug, Eq, PartialEq)]
pub(crate) struct DisplayQuoteSide {
    price: DisplayDecimal,
    quantity: DisplayDecimal,
}

impl DisplayQuoteSide {
    pub(crate) const fn price(&self) -> &DisplayDecimal {
        &self.price
    }

    pub(crate) const fn quantity(&self) -> &DisplayDecimal {
        &self.quantity
    }

    fn try_from_provider(level: &ProviderBookLevel) -> Result<Self, ProjectionError> {
        Ok(Self {
            price: DisplayDecimal::try_from_provider(level.price().value())?,
            quantity: DisplayDecimal::try_from_provider(level.quantity().value())?,
        })
    }

    fn try_clone(&self) -> Result<Self, ProjectionError> {
        Ok(Self {
            price: self.price.try_clone()?,
            quantity: self.quantity.try_clone()?,
        })
    }

    fn retained_bytes(&self) -> Option<usize> {
        self.price
            .retained_bytes()
            .checked_add(self.quantity.retained_bytes())
    }
}

/// Exact provider trade payload.
#[derive(Debug, Eq, PartialEq)]
pub(crate) struct DisplayTrade {
    trade_id: SourceIdentifier,
    price: DisplayDecimal,
    quantity: DisplayDecimal,
    aggressor: AggressorSide,
    provider_aggressor_code: Option<SourceIdentifier>,
}

impl DisplayTrade {
    pub(crate) const fn price(&self) -> &DisplayDecimal {
        &self.price
    }

    pub(crate) const fn quantity(&self) -> &DisplayDecimal {
        &self.quantity
    }
}

/// Exact provider quote payload.
#[derive(Debug, Eq, PartialEq)]
pub(crate) struct DisplayQuote {
    bid: Option<DisplayQuoteSide>,
    ask: Option<DisplayQuoteSide>,
}

impl DisplayQuote {
    pub(crate) const fn bid(&self) -> Option<&DisplayQuoteSide> {
        self.bid.as_ref()
    }

    pub(crate) const fn ask(&self) -> Option<&DisplayQuoteSide> {
        self.ask.as_ref()
    }
}

/// Exact provider status payload without execution eligibility.
#[derive(Debug, Eq, PartialEq)]
pub(crate) enum DisplayStatus {
    TradingHalt {
        provider_status: SourceIdentifier,
        transition: HaltTransition,
        reason: SourceIdentifier,
    },
    Instrument {
        provider_status: SourceIdentifier,
        trading_status: TradingStatus,
    },
}

/// Display payload families retained independently so one update cannot erase another family.
#[derive(Debug, Eq, PartialEq)]
pub(crate) enum DisplayMarketPayload {
    Trade(DisplayTrade),
    Quote(DisplayQuote),
    Status(DisplayStatus),
}

/// Whether the effective time came from provider evidence or the trusted local receipt.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DisplayEffectiveTimeBasis {
    Provider,
    Received,
}

/// Compact exact source-coverage evidence retained per observation.
#[derive(Debug, Eq, PartialEq)]
pub(crate) struct DisplayCoverage {
    provider_product: SourceIdentifier,
    provider_channel: SourceIdentifier,
    event_class: LiveEventClass,
    declared_depth: Option<MarketDepth>,
    delay: CoverageDelay,
    consolidation: CoverageConsolidation,
    delivery: DeliveryEvidence,
    status: CoverageStatus,
    static_evidence_digest: EvidenceDigest,
    runtime_evidence_digest: Option<EvidenceDigest>,
    effective_from: Timestamp,
    effective_until: Option<Timestamp>,
}

impl DisplayCoverage {
    pub(crate) const fn provider_product(&self) -> &SourceIdentifier {
        &self.provider_product
    }

    pub(crate) const fn provider_channel(&self) -> &SourceIdentifier {
        &self.provider_channel
    }

    pub(crate) const fn event_class(&self) -> LiveEventClass {
        self.event_class
    }

    pub(crate) const fn declared_depth(&self) -> Option<MarketDepth> {
        self.declared_depth
    }

    pub(crate) const fn delay(&self) -> CoverageDelay {
        self.delay
    }

    pub(crate) const fn consolidation(&self) -> CoverageConsolidation {
        self.consolidation
    }

    pub(crate) const fn delivery(&self) -> DeliveryEvidence {
        self.delivery
    }

    pub(crate) const fn status(&self) -> CoverageStatus {
        self.status
    }

    pub(crate) const fn static_evidence_digest(&self) -> EvidenceDigest {
        self.static_evidence_digest
    }

    pub(crate) const fn runtime_evidence_digest(&self) -> Option<EvidenceDigest> {
        self.runtime_evidence_digest
    }

    pub(crate) const fn effective_from(&self) -> Timestamp {
        self.effective_from
    }

    pub(crate) const fn effective_until(&self) -> Option<Timestamp> {
        self.effective_until
    }
}

/// Capture and source provenance for one owned display observation.
#[derive(Debug, Eq, PartialEq)]
pub(crate) struct DisplayMarketProvenance {
    source_identifier: SourceIdentifier,
    source_at: Option<Timestamp>,
    effective_at: Timestamp,
    effective_time_basis: DisplayEffectiveTimeBasis,
    received_at: Timestamp,
    available_at: Timestamp,
    metadata_revision: MetadataRevision,
    quality: DataQuality,
    display_depth: Option<MarketDepth>,
    generation: ConnectionGeneration,
    session_id: SourceIdentifier,
    frame_id: FrameId,
    payload_digest: EvidenceDigest,
    capture_integrity: CaptureIntegrityState,
    decoder_rule: SourceIdentifier,
    decoder_rule_version: RuleVersion,
    timestamp_rule: SourceIdentifier,
    timestamp_rule_version: RuleVersion,
    coverage: DisplayCoverage,
}

impl DisplayMarketProvenance {
    pub(crate) const fn source_identifier(&self) -> &SourceIdentifier {
        &self.source_identifier
    }

    pub(crate) const fn source_at(&self) -> Option<Timestamp> {
        self.source_at
    }

    pub(crate) const fn effective_at(&self) -> Timestamp {
        self.effective_at
    }

    pub(crate) const fn effective_time_basis(&self) -> DisplayEffectiveTimeBasis {
        self.effective_time_basis
    }

    pub(crate) const fn received_at(&self) -> Timestamp {
        self.received_at
    }

    /// Returns when registry qualification made this observation available to local consumers.
    pub(crate) const fn available_at(&self) -> Timestamp {
        self.available_at
    }

    pub(crate) const fn metadata_revision(&self) -> &MetadataRevision {
        &self.metadata_revision
    }

    pub(crate) const fn quality(&self) -> DataQuality {
        self.quality
    }

    pub(crate) const fn display_depth(&self) -> Option<MarketDepth> {
        self.display_depth
    }

    pub(crate) const fn generation(&self) -> ConnectionGeneration {
        self.generation
    }

    pub(crate) const fn session_id(&self) -> &SourceIdentifier {
        &self.session_id
    }

    pub(crate) const fn frame_id(&self) -> FrameId {
        self.frame_id
    }

    pub(crate) const fn payload_digest(&self) -> EvidenceDigest {
        self.payload_digest
    }

    pub(crate) const fn capture_integrity(&self) -> CaptureIntegrityState {
        self.capture_integrity
    }

    pub(crate) const fn decoder_rule(&self) -> &SourceIdentifier {
        &self.decoder_rule
    }

    pub(crate) const fn decoder_rule_version(&self) -> RuleVersion {
        self.decoder_rule_version
    }

    pub(crate) const fn timestamp_rule(&self) -> &SourceIdentifier {
        &self.timestamp_rule
    }

    pub(crate) const fn timestamp_rule_version(&self) -> RuleVersion {
        self.timestamp_rule_version
    }

    pub(crate) const fn coverage(&self) -> &DisplayCoverage {
        &self.coverage
    }
}

/// One exact provider observation with no execution or action capability.
#[derive(Debug, Eq, PartialEq)]
pub(crate) struct DisplayMarketObservation {
    provenance: DisplayMarketProvenance,
    payload: DisplayMarketPayload,
}

impl DisplayMarketObservation {
    pub(crate) const fn provenance(&self) -> &DisplayMarketProvenance {
        &self.provenance
    }

    pub(crate) const fn payload(&self) -> &DisplayMarketPayload {
        &self.payload
    }
}

/// Read-time freshness independent of the immutable source-quality ceiling.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DisplayMarketAvailability {
    Fresh {
        stale_after: Timestamp,
        expires_after: Timestamp,
    },
    Stale {
        stale_after: Timestamp,
        expires_after: Timestamp,
    },
    Expired {
        expired_after: Timestamp,
    },
    Quarantined {
        failure: DisplayMarketTerminalFailure,
    },
}

/// Owned observation plus freshness calculated at the caller's explicit read time.
#[derive(Debug, Eq, PartialEq)]
pub(crate) struct DisplayMarketReadObservation {
    observation: DisplayMarketObservation,
    availability: DisplayMarketAvailability,
}

impl DisplayMarketReadObservation {
    pub(crate) const fn observation(&self) -> &DisplayMarketObservation {
        &self.observation
    }

    pub(crate) const fn availability(&self) -> DisplayMarketAvailability {
        self.availability
    }
}

/// Bounded deterministic snapshot; permits remain owned until this value is dropped.
#[derive(Debug)]
pub(crate) struct DisplayMarketSnapshotLease {
    key: Arc<DisplayMarketKey>,
    revision: u64,
    trade: Option<DisplayMarketReadObservation>,
    quote: Option<DisplayMarketReadObservation>,
    status: Option<DisplayMarketReadObservation>,
    terminal_failure: Option<DisplayMarketTerminalFailure>,
    _ticket: ReadBudgetTicket,
}

impl DisplayMarketSnapshotLease {
    pub(crate) fn key(&self) -> &DisplayMarketKey {
        &self.key
    }

    pub(crate) const fn revision(&self) -> u64 {
        self.revision
    }

    pub(crate) const fn trade(&self) -> Option<&DisplayMarketReadObservation> {
        self.trade.as_ref()
    }

    pub(crate) const fn quote(&self) -> Option<&DisplayMarketReadObservation> {
        self.quote.as_ref()
    }

    pub(crate) const fn status(&self) -> Option<&DisplayMarketReadObservation> {
        self.status.as_ref()
    }

    pub(crate) const fn terminal_failure(&self) -> Option<DisplayMarketTerminalFailure> {
        self.terminal_failure
    }

    /// Selects the exact observation used for source admission and downstream market evidence.
    pub(crate) fn selection_observation(&self) -> Option<&DisplayMarketReadObservation> {
        let mut selected = None;
        for candidate in [self.quote(), self.trade()].into_iter().flatten() {
            if selected.is_none_or(|current| display_observation_is_better(candidate, current)) {
                selected = Some(candidate);
            }
        }
        selected.or(self.status())
    }
}

fn display_observation_is_better(
    candidate: &DisplayMarketReadObservation,
    current: &DisplayMarketReadObservation,
) -> bool {
    let candidate_provenance = candidate.observation().provenance();
    let current_provenance = current.observation().provenance();
    display_availability_rank(candidate.availability())
        .cmp(&display_availability_rank(current.availability()))
        .then_with(|| {
            display_depth_rank(candidate_provenance.display_depth())
                .cmp(&display_depth_rank(current_provenance.display_depth()))
        })
        .then_with(|| {
            display_quality_rank(display_current_quality(candidate))
                .cmp(&display_quality_rank(display_current_quality(current)))
        })
        .then_with(|| {
            candidate_provenance
                .received_at()
                .cmp(&current_provenance.received_at())
        })
        .is_gt()
}

const fn display_current_quality(observation: &DisplayMarketReadObservation) -> DataQuality {
    match observation.availability() {
        DisplayMarketAvailability::Fresh { .. } => observation.observation().provenance().quality(),
        DisplayMarketAvailability::Stale { .. } | DisplayMarketAvailability::Expired { .. } => {
            DataQuality::Stale
        }
        DisplayMarketAvailability::Quarantined { .. } => DataQuality::Quarantined,
    }
}

const fn display_availability_rank(availability: DisplayMarketAvailability) -> u8 {
    match availability {
        DisplayMarketAvailability::Fresh { .. } => 4,
        DisplayMarketAvailability::Stale { .. } => 3,
        DisplayMarketAvailability::Expired { .. } => 2,
        DisplayMarketAvailability::Quarantined { .. } => 1,
    }
}

const fn display_depth_rank(depth: Option<MarketDepth>) -> u8 {
    match depth {
        Some(MarketDepth::OrderLevel) => 3,
        Some(MarketDepth::PriceLevel) => 2,
        Some(MarketDepth::TopOfBook) => 1,
        None => 0,
    }
}

const fn display_quality_rank(quality: DataQuality) -> u8 {
    match quality {
        DataQuality::DirectVerified => 9,
        DataQuality::DirectUnverified => 8,
        DataQuality::OfficialDelayed => 7,
        DataQuality::Aggregated => 6,
        DataQuality::Indicative => 5,
        DataQuality::Modeled => 4,
        DataQuality::Estimated => 3,
        DataQuality::Stale => 2,
        DataQuality::Quarantined => 1,
    }
}

#[derive(Debug)]
struct ReadBudgetTicket {
    charged_bytes: u32,
    _count_permit: OwnedSemaphorePermit,
    _byte_permit: OwnedSemaphorePermit,
}

#[derive(Debug)]
struct ReadCommand {
    at: Timestamp,
    response: oneshot::Sender<Result<DisplayMarketSnapshotLease, DisplayMarketReadError>>,
    ticket: ReadBudgetTicket,
}

#[derive(Clone, Debug)]
struct ReadClient {
    commands: mpsc::Sender<ReadCommand>,
    count_budget: Arc<Semaphore>,
    byte_budget: Arc<Semaphore>,
    maximum_snapshot_bytes: u32,
    actor_cancellation: CancellationToken,
}

impl ReadClient {
    async fn snapshot(
        &self,
        at: Timestamp,
        cancellation: &CancellationToken,
        deadline: Instant,
    ) -> Result<DisplayMarketSnapshotLease, DisplayMarketReadError> {
        require_read_time(cancellation, deadline)?;
        let count_permit = acquire_one(
            Arc::clone(&self.count_budget),
            &self.actor_cancellation,
            cancellation,
            deadline,
        )
        .await?;
        let byte_permit = acquire_many(
            Arc::clone(&self.byte_budget),
            self.maximum_snapshot_bytes,
            &self.actor_cancellation,
            cancellation,
            deadline,
        )
        .await?;
        let ticket = ReadBudgetTicket {
            charged_bytes: self.maximum_snapshot_bytes,
            _count_permit: count_permit,
            _byte_permit: byte_permit,
        };
        let (response, receiver) = oneshot::channel();
        let command = ReadCommand {
            at,
            response,
            ticket,
        };
        tokio::select! {
            biased;
            () = cancellation.cancelled() => return Err(DisplayMarketReadError::Cancelled),
            () = self.actor_cancellation.cancelled() => {
                return Err(DisplayMarketReadError::WorkerClosed);
            }
            () = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => {
                return Err(DisplayMarketReadError::Deadline);
            }
            result = self.commands.send(command) => {
                result.map_err(|_error| DisplayMarketReadError::WorkerClosed)?;
            }
        }
        await_read_response(receiver, &self.actor_cancellation, cancellation, deadline).await
    }
}

/// A bounded snapshot read failed without returning a partial result.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub(crate) enum DisplayMarketReadError {
    #[error("display-market generation has no observation")]
    Unavailable,
    #[error("exact display-market generation is being unregistered")]
    Unregistering,
    #[error("display-market instrument read requested {requested} sources; maximum is {maximum}")]
    SourceLimit { requested: usize, maximum: usize },
    #[error("display-market snapshot exceeded its admitted byte lease")]
    AccountingOverflow,
    #[error("display-market snapshot allocation failed")]
    Allocation,
    #[error("display-market read was cancelled")]
    Cancelled,
    #[error("display-market read deadline elapsed")]
    Deadline,
    #[error("display-market generation actor is closed")]
    WorkerClosed,
}

/// Supervisor-facing terminal monitor for one exact generation.
#[derive(Debug)]
pub(crate) struct DisplayMarketSupervisorMonitor {
    key: Arc<DisplayMarketKey>,
    status: watch::Receiver<Option<DisplayMarketTerminalFailure>>,
}

impl DisplayMarketSupervisorMonitor {
    pub(crate) fn key(&self) -> &DisplayMarketKey {
        &self.key
    }

    pub(crate) async fn wait_until_terminal(
        &mut self,
        cancellation: &CancellationToken,
    ) -> Result<DisplayMarketTerminalFailure, DisplayMarketMonitorError> {
        if let Some(failure) = *self.status.borrow_and_update() {
            return Ok(failure);
        }
        tokio::select! {
            biased;
            () = cancellation.cancelled() => Err(DisplayMarketMonitorError::Cancelled),
            result = self.status.changed() => match result {
                Ok(()) => (*self.status.borrow_and_update())
                    .ok_or(DisplayMarketMonitorError::WorkerClosed),
                Err(_closed) => Err(DisplayMarketMonitorError::WorkerClosed),
            }
        }
    }
}

#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub(crate) enum DisplayMarketMonitorError {
    #[error("display-market supervisor wait was cancelled")]
    Cancelled,
    #[error("display-market actor exited without a terminal signal")]
    WorkerClosed,
}

/// Handles returned by one exact generation registration.
#[derive(Debug)]
pub(crate) struct DisplayMarketRegistration {
    ingress: DisplayMarketIngress,
    monitor: DisplayMarketSupervisorMonitor,
}

impl DisplayMarketRegistration {
    pub(crate) fn into_parts(self) -> (DisplayMarketIngress, DisplayMarketSupervisorMonitor) {
        (self.ingress, self.monitor)
    }
}

/// Cloneable exact-generation directory around independent single-writer route actors.
#[derive(Clone, Debug)]
pub(crate) struct DisplayMarketDirectory {
    inner: Arc<DirectoryInner>,
}

#[derive(Debug)]
struct DirectoryInner {
    maximum_routes: usize,
    lifecycle: Mutex<()>,
    entries: Mutex<Vec<ActorEntry>>,
    cancellation: CancellationToken,
}

impl Drop for DirectoryInner {
    fn drop(&mut self) {
        self.cancellation.cancel();
    }
}

impl DisplayMarketDirectory {
    /// Preallocates the entire bounded directory before it is shared.
    pub(crate) fn try_new(
        maximum_routes: NonZeroUsize,
        cancellation: CancellationToken,
    ) -> Result<Self, DisplayMarketDirectoryError> {
        if maximum_routes.get() > MAX_DISPLAY_MARKET_ROUTES {
            return Err(DisplayMarketDirectoryError::Configuration(
                DisplayMarketConfigurationError::DirectoryRoutes {
                    requested: maximum_routes.get(),
                    maximum: MAX_DISPLAY_MARKET_ROUTES,
                },
            ));
        }
        let mut entries = Vec::new();
        entries
            .try_reserve_exact(maximum_routes.get())
            .map_err(|_error| DisplayMarketDirectoryError::Allocation)?;
        Ok(Self {
            inner: Arc::new(DirectoryInner {
                maximum_routes: maximum_routes.get(),
                lifecycle: Mutex::new(()),
                entries: Mutex::new(entries),
                cancellation,
            }),
        })
    }

    /// Registers exactly one actor for an exact source/venue/instrument/generation tuple.
    pub(crate) async fn register(
        &self,
        key: DisplayMarketKey,
        limits: DisplayMarketActorLimits,
        read_admission: DisplayMarketReadAdmission,
        cancellation: &CancellationToken,
        deadline: Instant,
    ) -> Result<DisplayMarketRegistration, DisplayMarketDirectoryError> {
        let _lifecycle = lock_directory(
            &self.inner.lifecycle,
            &self.inner.cancellation,
            cancellation,
            deadline,
        )
        .await?;
        if self.inner.cancellation.is_cancelled() {
            return Err(DisplayMarketDirectoryError::Closed);
        }
        let mut entries = lock_directory(
            &self.inner.entries,
            &self.inner.cancellation,
            cancellation,
            deadline,
        )
        .await?;
        let key = Arc::new(key);
        let insertion = match entries.binary_search_by(|entry| entry.key.as_ref().cmp(key.as_ref()))
        {
            Ok(_index) => return Err(DisplayMarketDirectoryError::AlreadyRegistered),
            Err(index) => index,
        };
        if entries.len() == self.inner.maximum_routes {
            return Err(DisplayMarketDirectoryError::Capacity);
        }
        require_directory_time(&self.inner.cancellation, cancellation, deadline)?;

        let actor_cancellation = self.inner.cancellation.child_token();
        let command_budget = Arc::new(Semaphore::new(limits.ingress_commands.get()));
        let byte_budget = Arc::new(Semaphore::new(limits.ingress_bytes.get() as usize));
        let read_count_budget = Arc::new(Semaphore::new(limits.outstanding_reads.get()));
        let read_byte_budget = Arc::new(Semaphore::new(limits.read_bytes.get() as usize));
        let (command_sender, command_receiver) = mpsc::channel(limits.ingress_commands.get());
        let (read_sender, read_receiver) = mpsc::channel(limits.outstanding_reads.get());
        let (terminal_requests, terminal_request_receiver) = watch::channel(None);
        let terminal_state = terminal_requests.subscribe();
        let (status_sender, status) = watch::channel(None);
        let worker_cancellation = actor_cancellation.clone();
        let worker_key = Arc::clone(&key);
        let worker = tokio::spawn(run_actor(
            worker_key,
            limits.retained_state_bytes.get(),
            command_receiver,
            read_receiver,
            terminal_request_receiver,
            status_sender,
            worker_cancellation,
        ));
        let read_client = ReadClient {
            commands: read_sender,
            count_budget: read_count_budget,
            byte_budget: read_byte_budget,
            maximum_snapshot_bytes: limits.maximum_snapshot_bytes.get(),
            actor_cancellation: actor_cancellation.clone(),
        };
        entries.insert(
            insertion,
            ActorEntry {
                key: Arc::clone(&key),
                read_client,
                cancellation: actor_cancellation.clone(),
                worker: Some(worker),
                unregistering: false,
                read_admission,
            },
        );
        drop(entries);
        Ok(DisplayMarketRegistration {
            ingress: DisplayMarketIngress {
                key: Arc::clone(&key),
                commands: command_sender,
                command_budget,
                byte_budget,
                terminal_requests,
                terminal_state,
                actor_status: status.clone(),
                actor_cancellation,
            },
            monitor: DisplayMarketSupervisorMonitor { key, status },
        })
    }

    /// Reads every registered source for one instrument in exact-key order.
    pub(crate) async fn snapshots_for_instrument(
        &self,
        instrument_id: InstrumentId,
        maximum_sources: NonZeroUsize,
        at: Timestamp,
        cancellation: &CancellationToken,
        deadline: Instant,
    ) -> Result<Vec<DisplayMarketSnapshotLease>, DisplayMarketReadError> {
        require_read_time(cancellation, deadline)?;
        let entries = tokio::select! {
            biased;
            () = cancellation.cancelled() => return Err(DisplayMarketReadError::Cancelled),
            () = self.inner.cancellation.cancelled() => {
                return Err(DisplayMarketReadError::WorkerClosed);
            }
            () = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => {
                return Err(DisplayMarketReadError::Deadline);
            }
            entries = self.inner.entries.lock() => entries,
        };
        let match_count = entries
            .iter()
            .filter(|entry| {
                entry.read_admission.is_admitted() && entry.key.instrument_id() == instrument_id
            })
            .count();
        if match_count > maximum_sources.get() {
            return Err(DisplayMarketReadError::SourceLimit {
                requested: match_count,
                maximum: maximum_sources.get(),
            });
        }
        let mut clients = Vec::new();
        clients
            .try_reserve_exact(match_count)
            .map_err(|_error| DisplayMarketReadError::Allocation)?;
        for entry in entries.iter().filter(|entry| {
            entry.read_admission.is_admitted() && entry.key.instrument_id() == instrument_id
        }) {
            if entry.unregistering {
                return Err(DisplayMarketReadError::Unregistering);
            }
            clients.push(entry.read_client.clone());
        }
        drop(entries);
        let mut snapshots = Vec::new();
        snapshots
            .try_reserve_exact(match_count)
            .map_err(|_error| DisplayMarketReadError::Allocation)?;
        for client in clients {
            snapshots.push(client.snapshot(at, cancellation, deadline).await?);
        }
        Ok(snapshots)
    }

    /// Removes and stops one exact generation within the caller's single deadline.
    pub(crate) async fn unregister(
        &self,
        key: &DisplayMarketKey,
        cancellation: &CancellationToken,
        deadline: Instant,
    ) -> Result<DisplayMarketActorShutdown, DisplayMarketDirectoryError> {
        let _lifecycle = lock_directory(
            &self.inner.lifecycle,
            &self.inner.cancellation,
            cancellation,
            deadline,
        )
        .await?;
        let mut entries = lock_directory(
            &self.inner.entries,
            &self.inner.cancellation,
            cancellation,
            deadline,
        )
        .await?;
        let index = entries
            .binary_search_by(|entry| entry.key.as_ref().cmp(key))
            .map_err(|_index| DisplayMarketDirectoryError::NotRegistered)?;
        if entries[index].unregistering {
            return Err(DisplayMarketDirectoryError::Unregistering);
        }
        entries[index].unregistering = true;
        let entry = entries.remove(index);
        drop(entries);
        Ok(stop_actor(entry, cancellation, deadline).await)
    }

    /// Permanently closes the directory and bounds shutdown of every registered actor.
    pub(crate) async fn shutdown(
        &self,
        cancellation: &CancellationToken,
        deadline: Instant,
    ) -> Result<DisplayMarketDirectoryShutdown, DisplayMarketDirectoryError> {
        let _lifecycle = lock_directory(
            &self.inner.lifecycle,
            &self.inner.cancellation,
            cancellation,
            deadline,
        )
        .await?;
        let mut entries = lock_directory(
            &self.inner.entries,
            &self.inner.cancellation,
            cancellation,
            deadline,
        )
        .await?;
        let owned_entries = std::mem::take(&mut *entries);
        drop(entries);
        self.inner.cancellation.cancel();
        let mut result = DisplayMarketDirectoryShutdown::default();
        for entry in owned_entries {
            result.record(stop_actor(entry, cancellation, deadline).await);
        }
        Ok(result)
    }
}

/// Exact disposition of one bounded actor stop.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DisplayMarketActorShutdown {
    Graceful,
    AbortedAtDeadline,
    AbortedOnCancellation,
    WorkerFailed,
}

/// Aggregate evidence from a bounded directory shutdown.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct DisplayMarketDirectoryShutdown {
    graceful: usize,
    aborted_at_deadline: usize,
    aborted_on_cancellation: usize,
    failed: usize,
}

impl DisplayMarketDirectoryShutdown {
    pub(crate) const fn is_complete(self) -> bool {
        self.aborted_at_deadline == 0 && self.aborted_on_cancellation == 0 && self.failed == 0
    }

    fn record(&mut self, disposition: DisplayMarketActorShutdown) {
        match disposition {
            DisplayMarketActorShutdown::Graceful => self.graceful += 1,
            DisplayMarketActorShutdown::AbortedAtDeadline => self.aborted_at_deadline += 1,
            DisplayMarketActorShutdown::AbortedOnCancellation => {
                self.aborted_on_cancellation += 1;
            }
            DisplayMarketActorShutdown::WorkerFailed => self.failed += 1,
        }
    }
}

/// Registration or exact-generation lifecycle failure.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub(crate) enum DisplayMarketDirectoryError {
    #[error("invalid display-market configuration: {0}")]
    Configuration(#[from] DisplayMarketConfigurationError),
    #[error("display-market bounded allocation failed")]
    Allocation,
    #[error("display-market directory is closed")]
    Closed,
    #[error("display-market directory operation was cancelled")]
    Cancelled,
    #[error("display-market directory operation deadline elapsed")]
    Deadline,
    #[error("exact display-market generation is already registered")]
    AlreadyRegistered,
    #[error("exact display-market generation is not registered")]
    NotRegistered,
    #[error("exact display-market generation is being unregistered")]
    Unregistering,
    #[error("display-market route capacity was exhausted")]
    Capacity,
}

#[derive(Debug)]
struct ActorEntry {
    key: Arc<DisplayMarketKey>,
    read_client: ReadClient,
    cancellation: CancellationToken,
    worker: Option<JoinHandle<()>>,
    unregistering: bool,
    read_admission: DisplayMarketReadAdmission,
}

impl Drop for ActorEntry {
    fn drop(&mut self) {
        self.cancellation.cancel();
        if let Some(worker) = &self.worker {
            worker.abort();
        }
    }
}

#[derive(Debug, Default)]
struct DisplayMarketState {
    revision: u64,
    last_frame_id: Option<FrameId>,
    trade: Option<RetainedObservation>,
    quote: Option<RetainedObservation>,
    status: Option<RetainedObservation>,
    terminal_failure: Option<DisplayMarketTerminalFailure>,
}

#[derive(Debug)]
struct RetainedObservation {
    observation: DisplayMarketObservation,
    authority: CurrentSourceAuthorityLease,
    stale_after: Timestamp,
    expires_after: Timestamp,
    retained_charge: usize,
}

#[derive(Debug, Default)]
struct ProjectedUpdate {
    frame_id: Option<FrameId>,
    trade: Option<RetainedObservation>,
    quote: Option<RetainedObservation>,
    status: Option<RetainedObservation>,
}

async fn run_actor(
    key: Arc<DisplayMarketKey>,
    retained_state_limit: u32,
    mut ingress: mpsc::Receiver<IngressCommand>,
    mut reads: mpsc::Receiver<ReadCommand>,
    mut terminal_requests: watch::Receiver<Option<DisplayMarketTerminalFailure>>,
    status: watch::Sender<Option<DisplayMarketTerminalFailure>>,
    cancellation: CancellationToken,
) {
    let mut state = DisplayMarketState::default();
    let mut ingress_open = true;
    let mut reads_open = true;
    let mut terminal_requests_open = true;
    loop {
        if !ingress_open && !reads_open {
            break;
        }
        tokio::select! {
            biased;
            () = cancellation.cancelled() => break,
            changed = terminal_requests.changed(),
                if terminal_requests_open && state.terminal_failure.is_none() => {
                match changed {
                    Ok(()) => {
                        if let Some(failure) = *terminal_requests.borrow_and_update() {
                            enter_terminal(
                                &mut state,
                                failure,
                                &status,
                                &mut ingress,
                                &mut ingress_open,
                            );
                        }
                    }
                    Err(_closed) => terminal_requests_open = false,
                }
            }
            command = ingress.recv(),
                if ingress_open && state.terminal_failure.is_none() => match command {
                Some(IngressCommand { batch, validated_at, _ticket }) => {
                    let result = project_batch(batch, validated_at, &key)
                        .and_then(|update| apply_update(
                            &mut state,
                            update,
                            retained_state_limit,
                        ));
                    if let Err(failure) = result {
                        enter_terminal(
                            &mut state,
                            failure,
                            &status,
                            &mut ingress,
                            &mut ingress_open,
                        );
                    }
                    drop(_ticket);
                }
                None => ingress_open = false,
            },
            command = reads.recv(), if reads_open => match command {
                Some(command) => {
                    if let Some(failure) = process_read(&key, &state, command) {
                        enter_terminal(
                            &mut state,
                            failure,
                            &status,
                            &mut ingress,
                            &mut ingress_open,
                        );
                    }
                }
                None => reads_open = false,
            },
        }
    }
}

fn enter_terminal(
    state: &mut DisplayMarketState,
    failure: DisplayMarketTerminalFailure,
    status: &watch::Sender<Option<DisplayMarketTerminalFailure>>,
    ingress: &mut mpsc::Receiver<IngressCommand>,
    ingress_open: &mut bool,
) {
    if state.terminal_failure.is_none() {
        state.terminal_failure = Some(failure);
        status.send_replace(Some(failure));
    }
    ingress.close();
    while let Ok(command) = ingress.try_recv() {
        drop(command);
    }
    *ingress_open = false;
}

fn project_batch(
    batch: CurrentDecodedProviderBatch,
    validated_at: Timestamp,
    key: &DisplayMarketKey,
) -> Result<ProjectedUpdate, DisplayMarketTerminalFailure> {
    let batch_charge = batch.retained_bytes();
    let batch_authority = batch.current_lease().clone();
    batch_authority
        .validate_at(validated_at)
        .map_err(DisplayMarketTerminalFailure::Registry)?;
    let mut update = ProjectedUpdate::default();
    for current in batch.into_observations() {
        validate_observation_identity(&current, &batch_authority, key, validated_at)?;
        let frame_id = current.frame_evidence().frame_id();
        match update.frame_id {
            None => update.frame_id = Some(frame_id),
            Some(expected) if expected == frame_id => {}
            Some(_different) => {
                return Err(DisplayMarketTerminalFailure::Integrity(
                    DisplayMarketIntegrityFailure::ProvenanceMismatch,
                ));
            }
        }
        let projected = project_observation(current, batch_charge, validated_at)?;
        match projected.observation.payload {
            DisplayMarketPayload::Trade(_) => update.trade = Some(projected),
            DisplayMarketPayload::Quote(_) => update.quote = Some(projected),
            DisplayMarketPayload::Status(_) => update.status = Some(projected),
        }
    }
    if update.frame_id.is_none() {
        return Err(DisplayMarketTerminalFailure::Integrity(
            DisplayMarketIntegrityFailure::ObservationIdentity,
        ));
    }
    Ok(update)
}

fn validate_observation_identity(
    current: &CurrentProviderObservation,
    batch_authority: &CurrentSourceAuthorityLease,
    key: &DisplayMarketKey,
    validated_at: Timestamp,
) -> Result<(), DisplayMarketTerminalFailure> {
    current
        .current_lease()
        .validate_at(validated_at)
        .map_err(DisplayMarketTerminalFailure::Registry)?;
    let binding = current.frame_evidence().binding();
    let authority_binding = current.current_lease().binding();
    let policy = current.policy();
    let coverage = policy.coverage();
    let stream = policy.stream_key();
    let observation = current.observation();
    let same_authority = current
        .current_lease()
        .shares_registry_lineage_with(batch_authority)
        && current.current_lease().health_epoch() == batch_authority.health_epoch()
        && current.current_lease().valid_from() == batch_authority.valid_from()
        && current.current_lease().valid_until() == batch_authority.valid_until();
    if !same_authority
        || current.key().venue() != key.venue_id()
        || current.key().instrument() != key.instrument_id()
        || observation.venue() != key.venue_id()
        || observation.instrument() != key.instrument_id()
        || binding.source_id() != key.source_id()
        || binding.connection_generation() != key.generation()
        || authority_binding != binding
    {
        return Err(DisplayMarketTerminalFailure::Integrity(
            DisplayMarketIntegrityFailure::ObservationIdentity,
        ));
    }
    let health = current.current_lease().runtime_health();
    if stream.source_id() != key.source_id()
        || stream.venue() != key.venue_id()
        || stream.instrument() != key.instrument_id()
        || coverage.source_id() != key.source_id()
        || coverage.venue() != key.venue_id()
        || coverage.event_class() != observation.event_class()
        || coverage.depth() != observation.depth()
        || coverage.provider_product() != stream.provider_product()
        || coverage.provider_channel() != stream.provider_channel()
        || coverage.metadata_revision() != binding.metadata_revision()
        || health.source_id() != key.source_id()
        || health.metadata_revision() != binding.metadata_revision()
        || health.session_id() != binding.session_id()
        || health.connection_generation() != key.generation()
    {
        return Err(DisplayMarketTerminalFailure::Integrity(
            DisplayMarketIntegrityFailure::ProvenanceMismatch,
        ));
    }
    match policy.quality_ceiling() {
        DataQuality::DirectVerified | DataQuality::Stale | DataQuality::Quarantined => Err(
            DisplayMarketTerminalFailure::Integrity(DisplayMarketIntegrityFailure::InvalidQuality),
        ),
        DataQuality::DirectUnverified
        | DataQuality::OfficialDelayed
        | DataQuality::Aggregated
        | DataQuality::Indicative
        | DataQuality::Modeled
        | DataQuality::Estimated => Ok(()),
    }
}

fn project_observation(
    current: CurrentProviderObservation,
    batch_charge: usize,
    validated_at: Timestamp,
) -> Result<RetainedObservation, DisplayMarketTerminalFailure> {
    let observation = current.observation();
    let payload = match observation.payload() {
        ProviderObservationPayload::Trade {
            trade_id,
            price,
            quantity,
            aggressor,
        } => DisplayMarketPayload::Trade(DisplayTrade {
            trade_id: try_clone_source_identifier(trade_id)?,
            price: DisplayDecimal::try_from_provider(price.value())?,
            quantity: DisplayDecimal::try_from_provider(quantity.value())?,
            aggressor: aggressor.side(),
            provider_aggressor_code: aggressor
                .provider_code()
                .map(try_clone_source_identifier)
                .transpose()?,
        }),
        ProviderObservationPayload::Quote { bid, ask } => {
            DisplayMarketPayload::Quote(DisplayQuote {
                bid: bid
                    .as_ref()
                    .map(DisplayQuoteSide::try_from_provider)
                    .transpose()?,
                ask: ask
                    .as_ref()
                    .map(DisplayQuoteSide::try_from_provider)
                    .transpose()?,
            })
        }
        ProviderObservationPayload::TradingHalt {
            status,
            transition,
            reason,
        } => DisplayMarketPayload::Status(DisplayStatus::TradingHalt {
            provider_status: try_clone_source_identifier(status.status())?,
            transition: *transition,
            reason: try_clone_source_identifier(reason)?,
        }),
        ProviderObservationPayload::InstrumentStatus {
            status,
            trading_status,
        } => DisplayMarketPayload::Status(DisplayStatus::Instrument {
            provider_status: try_clone_source_identifier(status.status())?,
            trading_status: *trading_status,
        }),
        ProviderObservationPayload::BookSnapshot(_)
        | ProviderObservationPayload::BookDelta(_)
        | ProviderObservationPayload::Auction { .. }
        | ProviderObservationPayload::CorporateAction { .. } => {
            return Err(DisplayMarketTerminalFailure::Integrity(
                DisplayMarketIntegrityFailure::UnsupportedPayload,
            ));
        }
    };
    let timestamp = observation.timestamp();
    let (source_at, effective_time_basis, timestamp_rule, timestamp_rule_version) = match timestamp
    {
        ProviderTimestampEvidence::Provided { value, rule } => (
            Some(*value),
            DisplayEffectiveTimeBasis::Provider,
            try_clone_source_identifier(rule.provider_rule())?,
            rule.version(),
        ),
        ProviderTimestampEvidence::AuthoritativelyAbsent(rule) => (
            None,
            DisplayEffectiveTimeBasis::Received,
            try_clone_source_identifier(rule.provider_rule())?,
            rule.version(),
        ),
    };
    let frame = current.frame_evidence();
    let policy = current.policy();
    let coverage = policy.coverage();
    let received_at = frame.received_at();
    if received_at > validated_at {
        return Err(DisplayMarketTerminalFailure::Integrity(
            DisplayMarketIntegrityFailure::ProvenanceMismatch,
        ));
    }
    let effective_at = source_at.unwrap_or(received_at);
    if effective_at < coverage.effective_from()
        || coverage
            .effective_until()
            .is_some_and(|until| effective_at > until)
    {
        return Err(DisplayMarketTerminalFailure::Integrity(
            DisplayMarketIntegrityFailure::ProvenanceMismatch,
        ));
    }
    let display_depth = match observation.payload() {
        ProviderObservationPayload::Quote { .. } => Some(MarketDepth::TopOfBook),
        _ => observation.depth(),
    };
    let runtime_coverage = policy.runtime_coverage();
    let (runtime_evidence_digest, runtime_deadline, runtime_status) = match runtime_coverage {
        CoverageHealth::Sufficient {
            evidence,
            valid_until,
            ..
        } => (
            Some(evidence.content_digest()),
            Some(*valid_until),
            CoverageStatus::Sufficient,
        ),
        CoverageHealth::Limited => (None, None, CoverageStatus::Insufficient),
        CoverageHealth::Uninitialized => (None, None, CoverageStatus::Unknown),
    };
    let coverage_status = if runtime_status == CoverageStatus::Sufficient
        && coverage.delay() == CoverageDelay::RealTime
        && coverage.consolidation() != CoverageConsolidation::Partial
    {
        CoverageStatus::Sufficient
    } else if runtime_status == CoverageStatus::Unknown {
        CoverageStatus::Unknown
    } else {
        CoverageStatus::Insufficient
    };
    let expires_after = minimum_timestamp(
        policy
            .valid_until()
            .min(current.current_lease().valid_until()),
        minimum_optional_timestamp(coverage.effective_until(), runtime_deadline),
    );
    let stale_after = freshness_deadline(received_at, source_at, policy.freshness())?;
    let coverage = DisplayCoverage {
        provider_product: try_clone_source_identifier(
            coverage.provider_product().as_source_identifier(),
        )?,
        provider_channel: try_clone_source_identifier(
            coverage.provider_channel().as_source_identifier(),
        )?,
        event_class: coverage.event_class(),
        declared_depth: coverage.depth(),
        delay: coverage.delay(),
        consolidation: coverage.consolidation(),
        delivery: coverage.delivery(),
        status: coverage_status,
        static_evidence_digest: coverage.evidence().content_digest(),
        runtime_evidence_digest,
        effective_from: coverage.effective_from(),
        effective_until: coverage.effective_until(),
    };
    let metadata_revision = MetadataRevision::new(try_clone_source_identifier(
        frame.binding().metadata_revision().as_source_identifier(),
    )?);
    let observation = DisplayMarketObservation {
        provenance: DisplayMarketProvenance {
            source_identifier: try_clone_source_identifier(observation.source_identifier())?,
            source_at,
            effective_at,
            effective_time_basis,
            received_at,
            available_at: validated_at,
            metadata_revision,
            quality: policy.quality_ceiling(),
            display_depth,
            generation: frame.binding().connection_generation(),
            session_id: try_clone_source_identifier(
                frame.binding().session_id().as_source_identifier(),
            )?,
            frame_id: frame.frame_id(),
            payload_digest: frame.payload_digest(),
            capture_integrity: current.current_lease().runtime_health().capture_integrity(),
            decoder_rule: try_clone_source_identifier(frame.decoder_rule().provider_rule())?,
            decoder_rule_version: frame.decoder_rule().version(),
            timestamp_rule,
            timestamp_rule_version,
            coverage,
        },
        payload,
    };
    let retained_charge = batch_charge
        .checked_add(
            observation_retained_bytes(&observation)
                .ok_or(DisplayMarketTerminalFailure::AccountingOverflow)?,
        )
        .ok_or(DisplayMarketTerminalFailure::AccountingOverflow)?;
    Ok(RetainedObservation {
        observation,
        authority: current.current_lease().clone(),
        stale_after,
        expires_after,
        retained_charge,
    })
}

fn apply_update(
    state: &mut DisplayMarketState,
    update: ProjectedUpdate,
    retained_state_limit: u32,
) -> Result<(), DisplayMarketTerminalFailure> {
    let frame_id = update
        .frame_id
        .ok_or(DisplayMarketTerminalFailure::Integrity(
            DisplayMarketIntegrityFailure::ObservationIdentity,
        ))?;
    if state
        .last_frame_id
        .is_some_and(|previous| frame_id.get() <= previous.get())
    {
        return Err(DisplayMarketTerminalFailure::Integrity(
            DisplayMarketIntegrityFailure::FrameRegression,
        ));
    }
    let trade_charge = update
        .trade
        .as_ref()
        .or(state.trade.as_ref())
        .map_or(0, |value| value.retained_charge);
    let quote_charge = update
        .quote
        .as_ref()
        .or(state.quote.as_ref())
        .map_or(0, |value| value.retained_charge);
    let status_charge = update
        .status
        .as_ref()
        .or(state.status.as_ref())
        .map_or(0, |value| value.retained_charge);
    let retained = size_of::<DisplayMarketState>()
        .checked_add(trade_charge)
        .and_then(|bytes| bytes.checked_add(quote_charge))
        .and_then(|bytes| bytes.checked_add(status_charge))
        .ok_or(DisplayMarketTerminalFailure::AccountingOverflow)?;
    if retained > retained_state_limit as usize {
        return Err(DisplayMarketTerminalFailure::AccountingOverflow);
    }
    state.revision = state
        .revision
        .checked_add(1)
        .ok_or(DisplayMarketTerminalFailure::AccountingOverflow)?;
    state.last_frame_id = Some(frame_id);
    if update.trade.is_some() {
        state.trade = update.trade;
    }
    if update.quote.is_some() {
        state.quote = update.quote;
    }
    if update.status.is_some() {
        state.status = update.status;
    }
    Ok(())
}

fn process_read(
    key: &Arc<DisplayMarketKey>,
    state: &DisplayMarketState,
    command: ReadCommand,
) -> Option<DisplayMarketTerminalFailure> {
    let ReadCommand {
        at,
        response,
        ticket,
    } = command;
    let result = snapshot_from_state(Arc::clone(key), state, at, ticket);
    let failure = result.as_ref().err().and_then(|error| match error {
        DisplayMarketReadError::AccountingOverflow => {
            Some(DisplayMarketTerminalFailure::AccountingOverflow)
        }
        DisplayMarketReadError::Allocation => Some(DisplayMarketTerminalFailure::Allocation),
        DisplayMarketReadError::Unavailable
        | DisplayMarketReadError::Unregistering
        | DisplayMarketReadError::SourceLimit { .. }
        | DisplayMarketReadError::Cancelled
        | DisplayMarketReadError::Deadline
        | DisplayMarketReadError::WorkerClosed => None,
    });
    let _ignored = response.send(result);
    failure
}

fn snapshot_from_state(
    key: Arc<DisplayMarketKey>,
    state: &DisplayMarketState,
    at: Timestamp,
    ticket: ReadBudgetTicket,
) -> Result<DisplayMarketSnapshotLease, DisplayMarketReadError> {
    if state.trade.is_none()
        && state.quote.is_none()
        && state.status.is_none()
        && state.terminal_failure.is_none()
    {
        return Err(DisplayMarketReadError::Unavailable);
    }
    let trade = state
        .trade
        .as_ref()
        .map(|value| try_read_observation(value, at, state.terminal_failure))
        .transpose()?;
    let quote = state
        .quote
        .as_ref()
        .map(|value| try_read_observation(value, at, state.terminal_failure))
        .transpose()?;
    let status = state
        .status
        .as_ref()
        .map(|value| try_read_observation(value, at, state.terminal_failure))
        .transpose()?;
    let retained = snapshot_retained_bytes(&trade, &quote, &status)
        .ok_or(DisplayMarketReadError::AccountingOverflow)?;
    if retained > ticket.charged_bytes as usize {
        return Err(DisplayMarketReadError::AccountingOverflow);
    }
    Ok(DisplayMarketSnapshotLease {
        key,
        revision: state.revision,
        trade,
        quote,
        status,
        terminal_failure: state.terminal_failure,
        _ticket: ticket,
    })
}

fn try_read_observation(
    retained: &RetainedObservation,
    at: Timestamp,
    terminal: Option<DisplayMarketTerminalFailure>,
) -> Result<DisplayMarketReadObservation, DisplayMarketReadError> {
    let availability = if let Some(failure) = terminal {
        DisplayMarketAvailability::Quarantined { failure }
    } else if at > retained.expires_after || retained.authority.validate_at(at).is_err() {
        DisplayMarketAvailability::Expired {
            expired_after: retained.expires_after,
        }
    } else if at > retained.stale_after {
        DisplayMarketAvailability::Stale {
            stale_after: retained.stale_after,
            expires_after: retained.expires_after,
        }
    } else {
        DisplayMarketAvailability::Fresh {
            stale_after: retained.stale_after,
            expires_after: retained.expires_after,
        }
    };
    Ok(DisplayMarketReadObservation {
        observation: try_clone_observation(&retained.observation)
            .map_err(|_error| DisplayMarketReadError::Allocation)?,
        availability,
    })
}

fn try_clone_observation(
    value: &DisplayMarketObservation,
) -> Result<DisplayMarketObservation, ProjectionError> {
    let provenance = &value.provenance;
    let coverage = &provenance.coverage;
    let coverage = DisplayCoverage {
        provider_product: try_clone_source_identifier(&coverage.provider_product)?,
        provider_channel: try_clone_source_identifier(&coverage.provider_channel)?,
        event_class: coverage.event_class,
        declared_depth: coverage.declared_depth,
        delay: coverage.delay,
        consolidation: coverage.consolidation,
        delivery: coverage.delivery,
        status: coverage.status,
        static_evidence_digest: coverage.static_evidence_digest,
        runtime_evidence_digest: coverage.runtime_evidence_digest,
        effective_from: coverage.effective_from,
        effective_until: coverage.effective_until,
    };
    let provenance = DisplayMarketProvenance {
        source_identifier: try_clone_source_identifier(&provenance.source_identifier)?,
        source_at: provenance.source_at,
        effective_at: provenance.effective_at,
        effective_time_basis: provenance.effective_time_basis,
        received_at: provenance.received_at,
        available_at: provenance.available_at,
        metadata_revision: MetadataRevision::new(try_clone_source_identifier(
            provenance.metadata_revision.as_source_identifier(),
        )?),
        quality: provenance.quality,
        display_depth: provenance.display_depth,
        generation: provenance.generation,
        session_id: try_clone_source_identifier(&provenance.session_id)?,
        frame_id: provenance.frame_id,
        payload_digest: provenance.payload_digest,
        capture_integrity: provenance.capture_integrity,
        decoder_rule: try_clone_source_identifier(&provenance.decoder_rule)?,
        decoder_rule_version: provenance.decoder_rule_version,
        timestamp_rule: try_clone_source_identifier(&provenance.timestamp_rule)?,
        timestamp_rule_version: provenance.timestamp_rule_version,
        coverage,
    };
    let payload = match &value.payload {
        DisplayMarketPayload::Trade(trade) => DisplayMarketPayload::Trade(DisplayTrade {
            trade_id: try_clone_source_identifier(&trade.trade_id)?,
            price: trade.price.try_clone()?,
            quantity: trade.quantity.try_clone()?,
            aggressor: trade.aggressor,
            provider_aggressor_code: trade
                .provider_aggressor_code
                .as_ref()
                .map(try_clone_source_identifier)
                .transpose()?,
        }),
        DisplayMarketPayload::Quote(quote) => DisplayMarketPayload::Quote(DisplayQuote {
            bid: quote
                .bid
                .as_ref()
                .map(DisplayQuoteSide::try_clone)
                .transpose()?,
            ask: quote
                .ask
                .as_ref()
                .map(DisplayQuoteSide::try_clone)
                .transpose()?,
        }),
        DisplayMarketPayload::Status(DisplayStatus::TradingHalt {
            provider_status,
            transition,
            reason,
        }) => DisplayMarketPayload::Status(DisplayStatus::TradingHalt {
            provider_status: try_clone_source_identifier(provider_status)?,
            transition: *transition,
            reason: try_clone_source_identifier(reason)?,
        }),
        DisplayMarketPayload::Status(DisplayStatus::Instrument {
            provider_status,
            trading_status,
        }) => DisplayMarketPayload::Status(DisplayStatus::Instrument {
            provider_status: try_clone_source_identifier(provider_status)?,
            trading_status: *trading_status,
        }),
    };
    Ok(DisplayMarketObservation {
        provenance,
        payload,
    })
}

fn observation_retained_bytes(value: &DisplayMarketObservation) -> Option<usize> {
    let coverage = &value.provenance.coverage;
    let mut dynamic = value
        .provenance
        .source_identifier
        .retained_bytes()
        .checked_add(
            value
                .provenance
                .metadata_revision
                .as_source_identifier()
                .retained_bytes(),
        )?
        .checked_add(value.provenance.session_id.retained_bytes())?
        .checked_add(value.provenance.decoder_rule.retained_bytes())?
        .checked_add(value.provenance.timestamp_rule.retained_bytes())?
        .checked_add(coverage.provider_product.retained_bytes())?
        .checked_add(coverage.provider_channel.retained_bytes())?;
    dynamic = dynamic.checked_add(match &value.payload {
        DisplayMarketPayload::Trade(trade) => trade
            .trade_id
            .retained_bytes()
            .checked_add(trade.price.retained_bytes())?
            .checked_add(trade.quantity.retained_bytes())?
            .checked_add(
                trade
                    .provider_aggressor_code
                    .as_ref()
                    .map_or(0, SourceIdentifier::retained_bytes),
            )?,
        DisplayMarketPayload::Quote(quote) => quote
            .bid
            .as_ref()
            .map_or(Some(0), DisplayQuoteSide::retained_bytes)?
            .checked_add(
                quote
                    .ask
                    .as_ref()
                    .map_or(Some(0), DisplayQuoteSide::retained_bytes)?,
            )?,
        DisplayMarketPayload::Status(DisplayStatus::TradingHalt {
            provider_status,
            reason,
            ..
        }) => provider_status
            .retained_bytes()
            .checked_add(reason.retained_bytes())?,
        DisplayMarketPayload::Status(DisplayStatus::Instrument {
            provider_status, ..
        }) => provider_status.retained_bytes(),
    })?;
    size_of::<DisplayMarketObservation>().checked_add(dynamic)
}

fn snapshot_retained_bytes(
    trade: &Option<DisplayMarketReadObservation>,
    quote: &Option<DisplayMarketReadObservation>,
    status: &Option<DisplayMarketReadObservation>,
) -> Option<usize> {
    size_of::<DisplayMarketSnapshotLease>()
        .checked_add(trade.as_ref().map_or(Some(0), |value| {
            observation_retained_bytes(&value.observation)
        })?)?
        .checked_add(quote.as_ref().map_or(Some(0), |value| {
            observation_retained_bytes(&value.observation)
        })?)?
        .checked_add(status.as_ref().map_or(Some(0), |value| {
            observation_retained_bytes(&value.observation)
        })?)
}

fn freshness_deadline(
    received_at: Timestamp,
    source_at: Option<Timestamp>,
    freshness: FreshnessPolicy,
) -> Result<Timestamp, DisplayMarketTerminalFailure> {
    let transport = checked_time_add(received_at, freshness.max_transport_age_nanos())?;
    let market = checked_time_add(received_at, freshness.max_market_age_nanos())?;
    let mut deadline = transport.min(market);
    if let Some(source_at) = source_at {
        deadline = deadline.min(checked_time_add(
            source_at,
            freshness.max_source_age_nanos(),
        )?);
    }
    Ok(deadline)
}

fn checked_time_add(
    timestamp: Timestamp,
    nanos: u64,
) -> Result<Timestamp, DisplayMarketTerminalFailure> {
    let nanos = i64::try_from(nanos).map_err(|_error| {
        DisplayMarketTerminalFailure::Integrity(DisplayMarketIntegrityFailure::TimeOverflow)
    })?;
    timestamp.checked_add_nanos(nanos).map_err(|_error| {
        DisplayMarketTerminalFailure::Integrity(DisplayMarketIntegrityFailure::TimeOverflow)
    })
}

fn minimum_optional_timestamp(
    left: Option<Timestamp>,
    right: Option<Timestamp>,
) -> Option<Timestamp> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.min(right)),
        (Some(value), None) | (None, Some(value)) => Some(value),
        (None, None) => None,
    }
}

fn minimum_timestamp(required: Timestamp, optional: Option<Timestamp>) -> Timestamp {
    optional.map_or(required, |value| required.min(value))
}

#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
enum ProjectionError {
    #[error("display-market owned projection allocation failed")]
    Allocation,
    #[error("display-market identity reconstruction failed")]
    Identity,
}

impl From<ProjectionError> for DisplayMarketTerminalFailure {
    fn from(error: ProjectionError) -> Self {
        match error {
            ProjectionError::Allocation | ProjectionError::Identity => Self::Allocation,
        }
    }
}

fn try_clone_text(value: &str) -> Result<String, ProjectionError> {
    let mut owned = String::new();
    owned
        .try_reserve_exact(value.len())
        .map_err(|_error| ProjectionError::Allocation)?;
    owned.push_str(value);
    Ok(owned)
}

fn try_clone_source_identifier(
    value: &SourceIdentifier,
) -> Result<SourceIdentifier, ProjectionError> {
    SourceIdentifier::try_from(try_clone_text(value.as_str())?)
        .map_err(|_error| ProjectionError::Identity)
}

fn try_clone_source_id(value: &SourceId) -> Result<SourceId, DisplayMarketDirectoryError> {
    SourceId::try_from(
        try_clone_text(value.as_str()).map_err(|_error| DisplayMarketDirectoryError::Allocation)?,
    )
    .map_err(|_error| DisplayMarketDirectoryError::Allocation)
}

fn try_clone_venue_id(value: &VenueId) -> Result<VenueId, DisplayMarketDirectoryError> {
    VenueId::try_from(
        try_clone_text(value.as_str()).map_err(|_error| DisplayMarketDirectoryError::Allocation)?,
    )
    .map_err(|_error| DisplayMarketDirectoryError::Allocation)
}

async fn acquire_one(
    budget: Arc<Semaphore>,
    actor_cancellation: &CancellationToken,
    cancellation: &CancellationToken,
    deadline: Instant,
) -> Result<OwnedSemaphorePermit, DisplayMarketReadError> {
    require_read_time(cancellation, deadline)?;
    tokio::select! {
        biased;
        () = cancellation.cancelled() => Err(DisplayMarketReadError::Cancelled),
        () = actor_cancellation.cancelled() => Err(DisplayMarketReadError::WorkerClosed),
        () = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => {
            Err(DisplayMarketReadError::Deadline)
        }
        permit = budget.acquire_owned() => {
            permit.map_err(|_closed| DisplayMarketReadError::WorkerClosed)
        }
    }
}

async fn acquire_many(
    budget: Arc<Semaphore>,
    permits: u32,
    actor_cancellation: &CancellationToken,
    cancellation: &CancellationToken,
    deadline: Instant,
) -> Result<OwnedSemaphorePermit, DisplayMarketReadError> {
    require_read_time(cancellation, deadline)?;
    tokio::select! {
        biased;
        () = cancellation.cancelled() => Err(DisplayMarketReadError::Cancelled),
        () = actor_cancellation.cancelled() => Err(DisplayMarketReadError::WorkerClosed),
        () = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => {
            Err(DisplayMarketReadError::Deadline)
        }
        permit = budget.acquire_many_owned(permits) => {
            permit.map_err(|_closed| DisplayMarketReadError::WorkerClosed)
        }
    }
}

async fn await_read_response(
    receiver: oneshot::Receiver<Result<DisplayMarketSnapshotLease, DisplayMarketReadError>>,
    actor_cancellation: &CancellationToken,
    cancellation: &CancellationToken,
    deadline: Instant,
) -> Result<DisplayMarketSnapshotLease, DisplayMarketReadError> {
    require_read_time(cancellation, deadline)?;
    tokio::select! {
        biased;
        () = cancellation.cancelled() => Err(DisplayMarketReadError::Cancelled),
        () = actor_cancellation.cancelled() => Err(DisplayMarketReadError::WorkerClosed),
        () = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => {
            Err(DisplayMarketReadError::Deadline)
        }
        result = receiver => {
            result.map_err(|_closed| DisplayMarketReadError::WorkerClosed)?
        }
    }
}

async fn lock_directory<'a, T>(
    mutex: &'a Mutex<T>,
    directory_cancellation: &CancellationToken,
    cancellation: &CancellationToken,
    deadline: Instant,
) -> Result<tokio::sync::MutexGuard<'a, T>, DisplayMarketDirectoryError> {
    require_directory_time(directory_cancellation, cancellation, deadline)?;
    tokio::select! {
        biased;
        () = cancellation.cancelled() => Err(DisplayMarketDirectoryError::Cancelled),
        () = directory_cancellation.cancelled() => Err(DisplayMarketDirectoryError::Closed),
        () = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => {
            Err(DisplayMarketDirectoryError::Deadline)
        }
        guard = mutex.lock() => Ok(guard),
    }
}

fn require_read_time(
    cancellation: &CancellationToken,
    deadline: Instant,
) -> Result<(), DisplayMarketReadError> {
    if cancellation.is_cancelled() {
        Err(DisplayMarketReadError::Cancelled)
    } else if Instant::now() >= deadline {
        Err(DisplayMarketReadError::Deadline)
    } else {
        Ok(())
    }
}

fn require_directory_time(
    directory_cancellation: &CancellationToken,
    cancellation: &CancellationToken,
    deadline: Instant,
) -> Result<(), DisplayMarketDirectoryError> {
    if directory_cancellation.is_cancelled() {
        Err(DisplayMarketDirectoryError::Closed)
    } else if cancellation.is_cancelled() {
        Err(DisplayMarketDirectoryError::Cancelled)
    } else if Instant::now() >= deadline {
        Err(DisplayMarketDirectoryError::Deadline)
    } else {
        Ok(())
    }
}

async fn stop_actor(
    mut entry: ActorEntry,
    cancellation: &CancellationToken,
    deadline: Instant,
) -> DisplayMarketActorShutdown {
    entry.cancellation.cancel();
    let Some(mut worker) = entry.worker.take() else {
        return DisplayMarketActorShutdown::WorkerFailed;
    };
    let disposition = tokio::select! {
        biased;
        () = cancellation.cancelled() => {
            worker.abort();
            DisplayMarketActorShutdown::AbortedOnCancellation
        }
        () = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => {
            worker.abort();
            DisplayMarketActorShutdown::AbortedAtDeadline
        }
        result = &mut worker => match result {
            Ok(()) => DisplayMarketActorShutdown::Graceful,
            Err(_error) => DisplayMarketActorShutdown::WorkerFailed,
        }
    };
    if !worker.is_finished() {
        let _ignored = worker.await;
    }
    disposition
}
