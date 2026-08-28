//! Bounded Schwab read-only REST quote production for the provider-neutral market plane.
//!
//! This leaf owns transport scheduling and exact provider/account authority checks. Physical raw
//! sealing and qualified display publication remain application-owned through
//! [`SchwabRestQuoteEventSink`]; the producer cannot bypass the registered source/event path or
//! expose a provider-specific UI read.

use std::collections::BTreeSet;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use market_squawk_adapter_schwab::{
    AccessTokenAdmission, CapturedRestResponse, ExecutedRestResponse, ParseBounds,
    ProviderIdentifier, QuoteField, QuoteRequest, ReadOnlyRoute, RestExecutionOutcome,
    RestItemAccounting, RestTransportBounds, SchwabAccessTokenSource, SchwabAdapterError,
    SchwabOAuthAuthorityReceipt, SchwabRestDelayEvidence, SchwabRestExecutor, SchwabRestFamily,
    SchwabTransportError, SchwabTransportTelemetry, TokenAuthorityError,
};
use market_squawk_domain::{
    ConnectionGeneration, DataQuality, EvidenceDigest, InstrumentId, ProviderChannel,
    ProviderProduct, SourceId, SourceIdentifier, Timestamp, VenueId,
};
use market_squawk_sources::{
    BudgetDecision, BudgetDispatchDecision, BudgetReservationDecision, BudgetUnavailableReason,
    ProviderRateAuthority, ProviderRateDeclaration, RuntimeCapabilityDisposition,
    SchwabMarketDataFamily, SharedProviderBudget, SourceMetadata, apply_http_retry_after,
};
use tokio_util::sync::CancellationToken;

use crate::provider_activation::{
    MarketInstrumentBinding, SchwabMarketDataAccountActivation, SchwabMarketDataActivationError,
};
use crate::provider_onboarding::{SchwabOAuthMarketAuthority, SchwabOAuthPublicationEpoch};

const SCHWAB_PROVIDER: &str = "schwab-trader-api";

type SinkFuture<'a> = Pin<
    Box<
        dyn Future<Output = Result<SchwabRestQuotePublicationReceipt, SchwabRestQuoteSinkError>>
            + Send
            + 'a,
    >,
>;

/// Provider-independent application publication boundary for one exact Schwab REST response.
///
/// The implementation must seal the raw response before canonicalization, publish the resulting
/// qualified quote events through the registered current-event/display ingress, and return exact
/// publication accounting. Rejected and invalid bodies are supplied to the same boundary so raw
/// evidence is never silently dropped.
pub(crate) trait SchwabRestQuoteEventSink: std::fmt::Debug + Send + Sync {
    fn publish(&self, batch: SchwabRestQuoteBatch) -> SinkFuture<'_>;
}

/// Explicit source semantics retained across REST capture, canonicalization, and display ingress.
#[derive(Clone, Debug)]
pub(crate) struct SchwabRestQuoteSourceEvidence {
    metadata: SourceMetadata,
    feed: SourceIdentifier,
    venue_id: VenueId,
    delay: SchwabRestDelayEvidence,
    quality: DataQuality,
    provider_product: ProviderProduct,
    provider_channel: ProviderChannel,
    qualification_evidence: EvidenceDigest,
}

impl SchwabRestQuoteSourceEvidence {
    #[allow(
        clippy::too_many_arguments,
        reason = "the complete provider/feed/venue qualification is one atomic publication input"
    )]
    pub(crate) fn try_new(
        metadata: SourceMetadata,
        feed: SourceIdentifier,
        venue_id: VenueId,
        delay: SchwabRestDelayEvidence,
        quality: DataQuality,
        provider_product: ProviderProduct,
        provider_channel: ProviderChannel,
        qualification_evidence: EvidenceDigest,
    ) -> Result<Self, SchwabRestQuoteRuntimeError> {
        if metadata.provider().as_str() != SCHWAB_PROVIDER
            || !metadata.capabilities().live()
            || metadata.budget_policy().is_none()
            || quality != metadata.quality_ceiling()
            || qualification_evidence.bytes() == [0; 32]
            || metadata.coverage().live().is_none()
        {
            return Err(SchwabRestQuoteRuntimeError::SourceEvidence);
        }
        Ok(Self {
            metadata,
            feed,
            venue_id,
            delay,
            quality,
            provider_product,
            provider_channel,
            qualification_evidence,
        })
    }

    pub(crate) const fn metadata(&self) -> &SourceMetadata {
        &self.metadata
    }

    pub(crate) const fn feed(&self) -> &SourceIdentifier {
        &self.feed
    }

    pub(crate) const fn venue_id(&self) -> &VenueId {
        &self.venue_id
    }

    pub(crate) const fn delay(&self) -> SchwabRestDelayEvidence {
        self.delay
    }

    pub(crate) const fn quality(&self) -> DataQuality {
        self.quality
    }

    pub(crate) const fn provider_product(&self) -> &ProviderProduct {
        &self.provider_product
    }

    pub(crate) const fn provider_channel(&self) -> &ProviderChannel {
        &self.provider_channel
    }

    pub(crate) const fn qualification_evidence(&self) -> EvidenceDigest {
        self.qualification_evidence
    }
}

/// Caller-owned finite production controls. They are local resource limits, never Schwab limits.
#[derive(Clone, Copy, Debug)]
pub(crate) struct SchwabRestQuoteRuntimeBounds {
    pub(crate) request_admission: market_squawk_adapter_schwab::RequestAdmission,
    pub(crate) transport: RestTransportBounds,
    pub(crate) parse: ParseBounds,
    pub(crate) token: AccessTokenAdmission,
}

/// Accepted canonical identity and exact revision-bound economics for one requested symbol.
#[derive(Clone, Debug)]
pub(crate) struct SchwabRestQuoteInstrumentBinding(MarketInstrumentBinding);

impl SchwabRestQuoteInstrumentBinding {
    pub(crate) fn try_new(
        binding: MarketInstrumentBinding,
        source_id: &SourceId,
    ) -> Result<Self, SchwabRestQuoteRuntimeError> {
        let provider_identity = binding
            .provider_identity()
            .ok_or(SchwabRestQuoteRuntimeError::CanonicalIdentity)?;
        if binding.provider_symbol_is_provisional()
            || provider_identity.source_id() != source_id
            || provider_identity.instrument_id() != binding.instrument_id()
            || binding.execution_terms().instrument_id() != binding.instrument_id()
            || ProviderIdentifier::try_new(binding.provider_symbol().to_owned()).is_err()
        {
            return Err(SchwabRestQuoteRuntimeError::CanonicalIdentity);
        }
        Ok(Self(binding))
    }

    pub(crate) const fn instrument_id(&self) -> InstrumentId {
        self.0.instrument_id()
    }

    pub(crate) fn provider_symbol(&self) -> &str {
        self.0.provider_symbol()
    }

    pub(crate) const fn binding(&self) -> &MarketInstrumentBinding {
        &self.0
    }
}

/// One fully accounted provider response handed to the sole raw/canonical/display publisher.
#[derive(Debug)]
pub(crate) struct SchwabRestQuoteBatch {
    outcome: SchwabRestQuoteBatchOutcome,
    evidence: SchwabRestQuoteSourceEvidence,
    bindings: Arc<[SchwabRestQuoteInstrumentBinding]>,
    oauth: SchwabOAuthAuthorityReceipt,
    connection_generation: ConnectionGeneration,
    accounting: RestItemAccounting,
}

#[derive(Debug)]
pub(crate) enum SchwabRestQuoteBatchOutcome {
    Accepted(ExecutedRestResponse),
    ProviderRejected(CapturedRestResponse),
    InvalidPayload {
        capture: CapturedRestResponse,
        error: SchwabAdapterError,
    },
}

impl SchwabRestQuoteBatch {
    pub(crate) fn into_parts(
        self,
    ) -> (
        SchwabRestQuoteBatchOutcome,
        SchwabRestQuoteSourceEvidence,
        Arc<[SchwabRestQuoteInstrumentBinding]>,
        SchwabOAuthAuthorityReceipt,
        ConnectionGeneration,
        RestItemAccounting,
    ) {
        (
            self.outcome,
            self.evidence,
            self.bindings,
            self.oauth,
            self.connection_generation,
            self.accounting,
        )
    }

    pub(crate) const fn evidence(&self) -> &SchwabRestQuoteSourceEvidence {
        &self.evidence
    }

    pub(crate) fn bindings(&self) -> &[SchwabRestQuoteInstrumentBinding] {
        &self.bindings
    }

    pub(crate) const fn oauth(&self) -> SchwabOAuthAuthorityReceipt {
        self.oauth
    }

    pub(crate) const fn connection_generation(&self) -> ConnectionGeneration {
        self.connection_generation
    }

    pub(crate) const fn accounting(&self) -> RestItemAccounting {
        self.accounting
    }
}

/// Exact sink accounting checked before a poll is reported as published.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SchwabRestQuotePublicationReceipt {
    source_id: SourceId,
    connection_generation: ConnectionGeneration,
    requested: u64,
    returned: u64,
    published: u64,
    raw_sealed: bool,
}

impl SchwabRestQuotePublicationReceipt {
    pub(crate) fn try_new(
        source_id: SourceId,
        connection_generation: ConnectionGeneration,
        accounting: RestItemAccounting,
        published: u64,
        raw_sealed: bool,
    ) -> Result<Self, SchwabRestQuoteSinkError> {
        if !raw_sealed || published > accounting.returned {
            return Err(SchwabRestQuoteSinkError::InvalidReceipt);
        }
        Ok(Self {
            source_id,
            connection_generation,
            requested: accounting.requested,
            returned: accounting.returned,
            published,
            raw_sealed,
        })
    }

    pub(crate) const fn published(&self) -> u64 {
        self.published
    }
}

/// Bounded result of one scheduled request attempt.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SchwabRestQuotePollOutcome {
    Published {
        requested: u64,
        returned: u64,
        published: u64,
    },
    SealedWithoutPublication {
        requested: u64,
        returned: u64,
    },
    Deferred(market_squawk_sources::MonotonicInstant),
}

/// Sole production owner of one callable Schwab REST quote generation.
pub(crate) struct SchwabRestQuoteProducer {
    activation: SchwabMarketDataAccountActivation,
    oauth: SchwabOAuthMarketAuthority,
    publication_epoch: SchwabOAuthPublicationEpoch,
    evidence: SchwabRestQuoteSourceEvidence,
    bindings: Arc<[SchwabRestQuoteInstrumentBinding]>,
    request: QuoteRequest,
    executor: SchwabRestExecutor,
    budget: SharedProviderBudget,
    sink: Arc<dyn SchwabRestQuoteEventSink>,
}

impl std::fmt::Debug for SchwabRestQuoteProducer {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SchwabRestQuoteProducer")
            .field("source_id", self.evidence.metadata().source_id())
            .field("instruments", &self.bindings.len())
            .field("oauth", &"[PROTECTED TOKEN AUTHORITY]")
            .finish_non_exhaustive()
    }
}

impl SchwabRestQuoteProducer {
    #[allow(
        clippy::too_many_arguments,
        reason = "authority, source evidence, finite bounds, shared rate, and sink are independent"
    )]
    pub(crate) fn try_production(
        activation: SchwabMarketDataAccountActivation,
        provider_rate: &ProviderRateAuthority,
        evidence: SchwabRestQuoteSourceEvidence,
        bindings: Vec<MarketInstrumentBinding>,
        bounds: SchwabRestQuoteRuntimeBounds,
        telemetry: SchwabTransportTelemetry,
        sink: Arc<dyn SchwabRestQuoteEventSink>,
    ) -> Result<Self, SchwabRestQuoteRuntimeError> {
        if activation.lease().provider_budget_policy() != evidence.metadata().budget_policy()
            || activation.lease().runtime_evidence_digest()
                != activation.doctor_receipt().receipt_sha256()
            || !doctor_admits_quotes(&activation)
            || bindings.is_empty()
            || bindings.len() > bounds.request_admission.max_items()
        {
            return Err(SchwabRestQuoteRuntimeError::Authority);
        }

        let mut symbols = BTreeSet::new();
        let mut instruments = BTreeSet::new();
        let mut qualified = Vec::new();
        qualified
            .try_reserve_exact(bindings.len())
            .map_err(|_| SchwabRestQuoteRuntimeError::Allocation)?;
        for binding in bindings {
            let binding = SchwabRestQuoteInstrumentBinding::try_new(
                binding,
                evidence.metadata().source_id(),
            )?;
            if !symbols.insert(binding.provider_symbol().to_owned())
                || !instruments.insert(binding.instrument_id())
            {
                return Err(SchwabRestQuoteRuntimeError::CanonicalIdentity);
            }
            qualified.push(binding);
        }
        let request = QuoteRequest::try_new(
            qualified
                .iter()
                .map(|binding| ProviderIdentifier::try_new(binding.provider_symbol().to_owned()))
                .collect::<Result<Vec<_>, _>>()?,
            vec![QuoteField::Quote],
            None,
            bounds.request_admission,
        )?;
        let declaration = ProviderRateDeclaration::try_for_authorization_subject(
            activation
                .lease()
                .provider_budget_policy()
                .cloned()
                .ok_or(SchwabRestQuoteRuntimeError::Authority)?,
            activation.account_binding().subject(),
        )?;
        let budget = provider_rate.register_budget(declaration)?;
        let executor = SchwabRestExecutor::try_production(
            bounds.transport,
            bounds.parse,
            bounds.token,
            telemetry,
        )?;
        let oauth = activation.oauth_authority();
        let publication_epoch = activation.publication_epoch();
        Ok(Self {
            activation,
            oauth,
            publication_epoch,
            evidence,
            bindings: qualified.into(),
            request,
            executor,
            budget,
            sink,
        })
    }

    /// Executes at most one provider request. Waiting policy remains with the shared scheduler.
    pub(crate) async fn poll_once(
        &self,
        cancellation: &CancellationToken,
        deadline: Instant,
    ) -> Result<SchwabRestQuotePollOutcome, SchwabRestQuoteRuntimeError> {
        require_time(cancellation, deadline)?;
        tokio::select! {
            biased;
            () = cancellation.cancelled() => return Err(SchwabRestQuoteRuntimeError::Cancelled),
            () = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => {
                return Err(SchwabRestQuoteRuntimeError::Deadline);
            }
            current = self.activation.require_current() => current?,
        }
        let now = wall_timestamp()?;
        if !self.activation.doctor_receipt().is_current_at(now)
            || !self.evidence.metadata().is_effective_at(now)
            || !doctor_admits_quotes(&self.activation)
        {
            return Err(SchwabRestQuoteRuntimeError::RefreshRequired);
        }
        let token = tokio::select! {
            biased;
            () = cancellation.cancelled() => return Err(SchwabRestQuoteRuntimeError::Cancelled),
            () = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => {
                return Err(SchwabRestQuoteRuntimeError::Deadline);
            }
            token = self.oauth.acquire() => token?,
        };
        let oauth = tokio::select! {
            biased;
            () = cancellation.cancelled() => return Err(SchwabRestQuoteRuntimeError::Cancelled),
            () = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => {
                return Err(SchwabRestQuoteRuntimeError::Deadline);
            }
            current = self.oauth.current_receipt() => current?,
        };
        self.publication_epoch
            .validate_current(oauth)
            .map_err(|_error| SchwabRestQuoteRuntimeError::RefreshRequired)?;
        if token.generation() != oauth.generation()
            || oauth.generation().get()
                != self.activation.doctor_receipt().access_token_generation()
        {
            return Err(SchwabRestQuoteRuntimeError::RefreshRequired);
        }
        let generation = ConnectionGeneration::new(oauth.generation().get())
            .map_err(|_| SchwabRestQuoteRuntimeError::Authority)?;
        let reservation = match self.budget.try_reserve_request() {
            BudgetReservationDecision::Ready(reservation) => reservation,
            BudgetReservationDecision::WaitUntil(until) => {
                return Ok(SchwabRestQuotePollOutcome::Deferred(until));
            }
            BudgetReservationDecision::Unavailable(reason) => {
                return Err(SchwabRestQuoteRuntimeError::Budget(reason));
            }
        };
        require_time(cancellation, deadline)?;
        let permit = match reservation.commit_dispatch() {
            BudgetDispatchDecision::Ready(permit) => permit,
            BudgetDispatchDecision::WaitUntil(until) => {
                return Ok(SchwabRestQuotePollOutcome::Deferred(until));
            }
            BudgetDispatchDecision::Unavailable(reason) => {
                return Err(SchwabRestQuoteRuntimeError::Budget(reason));
            }
        };
        let operation_cancellation = cancellation.child_token();
        let outcome = tokio::select! {
            biased;
            () = cancellation.cancelled() => {
                operation_cancellation.cancel();
                return Err(SchwabRestQuoteRuntimeError::Cancelled);
            }
            () = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => {
                operation_cancellation.cancel();
                return Err(SchwabRestQuoteRuntimeError::Deadline);
            }
            result = self.executor.execute(
                self.request.request(),
                &token,
                operation_cancellation.clone(),
            ) => result,
        };
        let outcome = outcome?;
        let (outcome, accounting, receipt) = classify_quote_outcome(outcome)?;
        if accounting.requested != self.bindings.len() as u64
            || accounting.returned.checked_add(accounting.missing) != Some(accounting.requested)
        {
            return Err(SchwabRestQuoteRuntimeError::Accounting);
        }
        let budget_failure = if receipt.status() == 429 {
            let retry_after = receipt
                .headers()
                .iter()
                .find(|header| header.name() == "retry-after")
                .map(|header| header.value());
            budget_control_failure(apply_http_retry_after(&self.budget, retry_after, 0))
        } else if (200..=299).contains(&receipt.status()) {
            self.budget.record_success().err()
        } else {
            budget_control_failure(self.budget.apply_refusal(0))
        };
        permit.release();

        let source_id = self.evidence.metadata().source_id().clone();
        let batch = SchwabRestQuoteBatch {
            outcome,
            evidence: self.evidence.clone(),
            bindings: Arc::clone(&self.bindings),
            oauth,
            connection_generation: generation,
            accounting,
        };
        // Once transport has returned, `batch` is the sole owner of the captured provider body.
        // Publication is therefore an uncancellable exactly-once handoff: observing outer
        // cancellation or deadline here would drop raw evidence before the application sealer
        // could retain it. The caller's terminal state is reported only after the sink returns its
        // raw-seal receipt.
        let publication = self.sink.publish(batch).await?;
        if publication.source_id != source_id
            || publication.connection_generation != generation
            || publication.requested != accounting.requested
            || publication.returned != accounting.returned
            || !publication.raw_sealed
        {
            return Err(SchwabRestQuoteRuntimeError::PublicationReceipt);
        }
        require_time(cancellation, deadline)?;
        if let Some(reason) = budget_failure {
            return Err(SchwabRestQuoteRuntimeError::Budget(reason));
        }
        if publication.published() == 0 {
            Ok(SchwabRestQuotePollOutcome::SealedWithoutPublication {
                requested: accounting.requested,
                returned: accounting.returned,
            })
        } else {
            Ok(SchwabRestQuotePollOutcome::Published {
                requested: accounting.requested,
                returned: accounting.returned,
                published: publication.published(),
            })
        }
    }
}

fn budget_control_failure(decision: BudgetDecision) -> Option<BudgetUnavailableReason> {
    match decision {
        BudgetDecision::WaitUntil(_deadline) => None,
        BudgetDecision::Unavailable(reason) => Some(reason),
        BudgetDecision::Ready(permit) => {
            permit.release();
            Some(BudgetUnavailableReason::StateCorrupt)
        }
    }
}

fn doctor_admits_quotes(activation: &SchwabMarketDataAccountActivation) -> bool {
    activation
        .doctor_receipt()
        .observation()
        .families
        .iter()
        .any(|family| {
            family.family == SchwabMarketDataFamily::Quotes
                && matches!(
                    family.disposition,
                    RuntimeCapabilityDisposition::Available
                        | RuntimeCapabilityDisposition::Degraded
                )
        })
}

fn classify_quote_outcome(
    outcome: RestExecutionOutcome,
) -> Result<
    (
        SchwabRestQuoteBatchOutcome,
        RestItemAccounting,
        market_squawk_adapter_schwab::RawRestResponseReceipt,
    ),
    SchwabRestQuoteRuntimeError,
> {
    match outcome {
        RestExecutionOutcome::Accepted(response) => {
            if response.capture().receipt().route() != ReadOnlyRoute::Quotes
                || response.payload().family() != SchwabRestFamily::Quotes
            {
                return Err(SchwabRestQuoteRuntimeError::Protocol);
            }
            let accounting = response.accounting();
            let receipt = response.capture().receipt().clone();
            Ok((
                SchwabRestQuoteBatchOutcome::Accepted(response),
                accounting,
                receipt,
            ))
        }
        RestExecutionOutcome::ProviderRejected(capture) => {
            if capture.receipt().route() != ReadOnlyRoute::Quotes {
                return Err(SchwabRestQuoteRuntimeError::Protocol);
            }
            let accounting = capture.accounting();
            let receipt = capture.receipt().clone();
            Ok((
                SchwabRestQuoteBatchOutcome::ProviderRejected(capture),
                accounting,
                receipt,
            ))
        }
        RestExecutionOutcome::InvalidPayload { capture, error } => {
            if capture.receipt().route() != ReadOnlyRoute::Quotes {
                return Err(SchwabRestQuoteRuntimeError::Protocol);
            }
            let accounting = capture.accounting();
            let receipt = capture.receipt().clone();
            Ok((
                SchwabRestQuoteBatchOutcome::InvalidPayload { capture, error },
                accounting,
                receipt,
            ))
        }
        RestExecutionOutcome::AcceptedUserPreference(_)
        | RestExecutionOutcome::UserPreferenceRejected(_)
        | RestExecutionOutcome::InvalidUserPreference { .. } => {
            Err(SchwabRestQuoteRuntimeError::Protocol)
        }
    }
}

fn require_time(
    cancellation: &CancellationToken,
    deadline: Instant,
) -> Result<(), SchwabRestQuoteRuntimeError> {
    if cancellation.is_cancelled() {
        Err(SchwabRestQuoteRuntimeError::Cancelled)
    } else if Instant::now() >= deadline {
        Err(SchwabRestQuoteRuntimeError::Deadline)
    } else {
        Ok(())
    }
}

fn wall_timestamp() -> Result<Timestamp, SchwabRestQuoteRuntimeError> {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| SchwabRestQuoteRuntimeError::Clock)?
        .as_nanos();
    let nanos = i64::try_from(nanos).map_err(|_| SchwabRestQuoteRuntimeError::Clock)?;
    Ok(Timestamp::from_unix_nanos(nanos))
}

#[derive(Clone, Copy, Debug, thiserror::Error, Eq, PartialEq)]
pub(crate) enum SchwabRestQuoteSinkError {
    #[error("Schwab quote sink returned invalid raw/publication accounting")]
    InvalidReceipt,
    #[error("Schwab quote sink is unavailable")]
    Unavailable,
    #[error("Schwab quote sink operation was cancelled")]
    Cancelled,
    #[error("Schwab quote sink deadline elapsed")]
    Deadline,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum SchwabRestQuoteRuntimeError {
    #[error("Schwab quote runtime activation or OAuth authority is not current")]
    RefreshRequired,
    #[error("Schwab quote runtime authority does not match its source configuration")]
    Authority,
    #[error("Schwab quote source evidence is incomplete or inconsistent")]
    SourceEvidence,
    #[error("Schwab quote runtime requires accepted canonical instrument/provider-symbol identity")]
    CanonicalIdentity,
    #[error("Schwab quote response accounting is inconsistent")]
    Accounting,
    #[error("Schwab quote route or response family was not the closed quotes family")]
    Protocol,
    #[error("Schwab quote publication receipt does not match the dispatched generation")]
    PublicationReceipt,
    #[error("Schwab quote runtime allocation failed")]
    Allocation,
    #[error("Schwab quote runtime clock is unavailable")]
    Clock,
    #[error("Schwab quote operation was cancelled")]
    Cancelled,
    #[error("Schwab quote operation deadline elapsed")]
    Deadline,
    #[error("Schwab quote provider budget is unavailable: {0:?}")]
    Budget(BudgetUnavailableReason),
    #[error(transparent)]
    Activation(#[from] SchwabMarketDataActivationError),
    #[error(transparent)]
    Adapter(#[from] SchwabAdapterError),
    #[error(transparent)]
    Transport(#[from] SchwabTransportError),
    #[error(transparent)]
    Token(#[from] TokenAuthorityError),
    #[error(transparent)]
    OAuth(#[from] crate::provider_onboarding::SchwabOAuthRuntimeError),
    #[error(transparent)]
    BudgetPool(#[from] market_squawk_sources::BudgetPoolError),
    #[error(transparent)]
    Sink(#[from] SchwabRestQuoteSinkError),
}
