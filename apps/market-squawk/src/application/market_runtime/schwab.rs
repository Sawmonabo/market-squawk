//! Bounded Schwab read-only REST quote production for the provider-neutral market plane.
//!
//! This leaf owns transport scheduling and exact provider/account authority checks. Physical raw
//! sealing and qualified display publication remain application-owned through
//! [`SchwabRestQuoteEventSink`]; the producer cannot bypass the registered source/event path or
//! expose a provider-specific UI read.

use std::collections::BTreeSet;
use std::future::Future;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use market_squawk_adapter_schwab::{
    AccessTokenAdmission, AdaptiveAssessment, CapacityCounters, CapacityObservation,
    CapturedRestResponse, ExecutedRestResponse, ParseBounds, ProviderIdentifier, QuoteField,
    QuoteRequest, ReadOnlyRoute, RequestAdmission, RestExecutionOutcome, RestItemAccounting,
    RestTransportBounds, SchwabAdapterError, SchwabRestExecutor, SchwabRestFamily,
    SchwabTransportError, SchwabTransportTelemetry,
};
use market_squawk_data::{ListingReferenceGenerationReceipt, ListingReferenceReadCapability};
use market_squawk_domain::{ConnectionGeneration, InstrumentId, SourceId, Timestamp, VenueId};
use market_squawk_sources::{
    BudgetDecision, BudgetDispatchDecision, BudgetReservationDecision, BudgetUnavailableReason,
    ProviderRateAuthority, ProviderRateDeclaration, RuntimeCapabilityDisposition,
    SchwabMarketDataDoctorReceiptV1, SchwabMarketDataFamily, SharedProviderBudget, SourceMetadata,
    apply_http_retry_after,
};
use tokio_util::sync::CancellationToken;

use crate::provider_activation::{
    MarketInstrumentBinding, MarketReferenceIdentityApprovalV1, MarketReferenceIdentityAuthority,
    MarketReferenceIdentityResolution, SchwabMarketDataAccountActivation,
    SchwabMarketDataActivationError,
};
use crate::provider_onboarding::SchwabOAuthPublicationEpoch;

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

/// Exact durable doctor authority retained until response-time qualification.
#[derive(Clone, Debug)]
pub(crate) struct SchwabRestQuoteSourceEvidence {
    metadata: SourceMetadata,
    venue_id: VenueId,
    doctor_receipt: SchwabMarketDataDoctorReceiptV1,
}

impl SchwabRestQuoteSourceEvidence {
    pub(crate) fn try_new(
        metadata: SourceMetadata,
        venue_id: VenueId,
        doctor_receipt: SchwabMarketDataDoctorReceiptV1,
    ) -> Result<Self, SchwabRestQuoteRuntimeError> {
        if metadata.provider().as_str() != SCHWAB_PROVIDER
            || !metadata.capabilities().live()
            || metadata.budget_policy().is_none()
            || metadata.coverage().live().is_none()
            || !doctor_receipt.admits_source_start()
        {
            return Err(SchwabRestQuoteRuntimeError::SourceEvidence);
        }
        Ok(Self {
            metadata,
            venue_id,
            doctor_receipt,
        })
    }

    pub(crate) const fn metadata(&self) -> &SourceMetadata {
        &self.metadata
    }

    pub(crate) const fn venue_id(&self) -> &VenueId {
        &self.venue_id
    }

    pub(crate) const fn doctor_receipt(&self) -> &SchwabMarketDataDoctorReceiptV1 {
        &self.doctor_receipt
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
pub(crate) struct SchwabRestQuoteInstrumentBinding {
    binding: MarketInstrumentBinding,
    identity_approval: Option<MarketReferenceIdentityApprovalV1>,
}

impl SchwabRestQuoteInstrumentBinding {
    pub(crate) fn try_new(
        binding: MarketInstrumentBinding,
        source_id: &SourceId,
    ) -> Result<Self, SchwabRestQuoteRuntimeError> {
        Self::try_new_with_identity_approval(binding, None, source_id)
    }

    fn try_new_with_identity_approval(
        binding: MarketInstrumentBinding,
        identity_approval: Option<MarketReferenceIdentityApprovalV1>,
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
        Ok(Self {
            binding,
            identity_approval,
        })
    }

    pub(crate) const fn instrument_id(&self) -> InstrumentId {
        self.binding.instrument_id()
    }

    pub(crate) fn provider_symbol(&self) -> &str {
        self.binding.provider_symbol()
    }

    pub(crate) const fn binding(&self) -> &MarketInstrumentBinding {
        &self.binding
    }

    pub(crate) const fn identity_approval(&self) -> Option<&MarketReferenceIdentityApprovalV1> {
        self.identity_approval.as_ref()
    }

    pub(crate) fn try_all(
        bindings: Vec<(
            MarketInstrumentBinding,
            Option<MarketReferenceIdentityApprovalV1>,
        )>,
        source_id: &SourceId,
        maximum: usize,
    ) -> Result<Vec<Self>, SchwabRestQuoteRuntimeError> {
        if bindings.is_empty() || bindings.len() > maximum {
            return Err(SchwabRestQuoteRuntimeError::Authority);
        }
        let mut symbols = BTreeSet::new();
        let mut instruments = BTreeSet::new();
        let mut qualified = Vec::new();
        qualified
            .try_reserve_exact(bindings.len())
            .map_err(|_| SchwabRestQuoteRuntimeError::Allocation)?;
        for (binding, approval) in bindings {
            let binding = Self::try_new_with_identity_approval(binding, approval, source_id)?;
            if !symbols.insert(binding.provider_symbol().to_owned())
                || !instruments.insert(binding.instrument_id())
            {
                return Err(SchwabRestQuoteRuntimeError::CanonicalIdentity);
            }
            qualified.push(binding);
        }
        Ok(qualified)
    }
}

/// One fully accounted provider response handed to the sole raw/canonical/display publisher.
#[derive(Debug)]
pub(crate) struct SchwabRestQuoteBatch {
    outcome: SchwabRestQuoteBatchOutcome,
    evidence: SchwabRestQuoteSourceEvidence,
    bindings: Arc<[SchwabRestQuoteInstrumentBinding]>,
    oauth_epoch: SchwabOAuthPublicationEpoch,
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
        SchwabOAuthPublicationEpoch,
        ConnectionGeneration,
        RestItemAccounting,
    ) {
        (
            self.outcome,
            self.evidence,
            self.bindings,
            self.oauth_epoch,
            self.connection_generation,
            self.accounting,
        )
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
    current: crate::live_source::SchwabRestQuoteCurrentPublication,
}

impl SchwabRestQuotePublicationReceipt {
    pub(crate) fn try_new(
        source_id: SourceId,
        connection_generation: ConnectionGeneration,
        accounting: RestItemAccounting,
        raw_sealed: bool,
        current: crate::live_source::SchwabRestQuoteCurrentPublication,
    ) -> Result<Self, SchwabRestQuoteSinkError> {
        let published = current.published();
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
            current,
        })
    }

    pub(crate) const fn published(&self) -> u64 {
        self.published
    }

    pub(crate) const fn current(&self) -> crate::live_source::SchwabRestQuoteCurrentPublication {
        self.current
    }
}

/// Bounded result of one scheduled request attempt.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SchwabRestQuotePollOutcome {
    Published {
        requested: u64,
        returned: u64,
        published: u64,
        capacity: CapacityObservation,
    },
    SealedWithoutPublication {
        requested: u64,
        returned: u64,
        current: crate::live_source::SchwabRestQuoteCurrentPublication,
        capacity: CapacityObservation,
    },
    Deferred(market_squawk_sources::MonotonicInstant),
}

/// Sole application scheduler for one Schwab quote runtime generation.
///
/// The adapter supplies validated capacity evidence and the shared provider budget owns explicit
/// rate/backoff deadlines. This authority only adjusts local batch admission and the next safe
/// poll cadence; neither value is represented as a Schwab guarantee.
#[derive(Debug)]
pub(crate) struct SchwabRestQuoteAdaptiveSchedule {
    counters: CapacityCounters,
    batch_items: usize,
    maximum_batch_items: usize,
    poll_nanos: u64,
    minimum_poll_nanos: u64,
    maximum_poll_nanos: u64,
    latency_pressure_ms: u64,
    request_bytes_pressure: u64,
    response_bytes_pressure: u64,
}

impl SchwabRestQuoteAdaptiveSchedule {
    pub(crate) fn try_new(
        maximum_batch_items: usize,
        bounds: SchwabRestQuoteRuntimeBounds,
        request_timeout: Duration,
        maximum_poll_interval: Duration,
    ) -> Result<Self, SchwabRestQuoteRuntimeError> {
        let maximum_poll_nanos = u64::try_from(maximum_poll_interval.as_nanos())
            .map_err(|_error| SchwabRestQuoteRuntimeError::Authority)?;
        let minimum_poll_nanos = maximum_poll_nanos
            .checked_div(2)
            .filter(|value| *value > 0)
            .ok_or(SchwabRestQuoteRuntimeError::Authority)?;
        let latency_pressure_ms = u64::try_from(request_timeout.as_millis())
            .map_err(|_error| SchwabRestQuoteRuntimeError::Authority)?
            .checked_div(2)
            .filter(|value| *value > 0)
            .ok_or(SchwabRestQuoteRuntimeError::Authority)?;
        if maximum_batch_items == 0 || maximum_batch_items > bounds.request_admission.max_items() {
            return Err(SchwabRestQuoteRuntimeError::Authority);
        }
        Ok(Self {
            counters: CapacityCounters::default(),
            batch_items: maximum_batch_items,
            maximum_batch_items,
            poll_nanos: minimum_poll_nanos,
            minimum_poll_nanos,
            maximum_poll_nanos,
            latency_pressure_ms,
            request_bytes_pressure: pressure_threshold(
                bounds.request_admission.max_request_bytes(),
            )?,
            response_bytes_pressure: pressure_threshold(bounds.parse.max_response_bytes())?,
        })
    }

    pub(crate) const fn batch_items(&self) -> usize {
        self.batch_items
    }

    pub(crate) fn poll_interval(&self) -> Duration {
        Duration::from_nanos(self.poll_nanos)
    }

    pub(crate) fn observe(
        &mut self,
        observation: CapacityObservation,
        publication_pressure: bool,
    ) -> Result<(), SchwabRestQuoteRuntimeError> {
        self.counters.record(observation)?;
        let measured_pressure = observation.latency_ms() >= self.latency_pressure_ms
            || observation.request_bytes() >= self.request_bytes_pressure
            || observation.response_bytes() >= self.response_bytes_pressure;
        match observation.assessment() {
            AdaptiveAssessment::Complete if !publication_pressure && !measured_pressure => {
                self.batch_items = self
                    .batch_items
                    .checked_add(1)
                    .unwrap_or(self.maximum_batch_items)
                    .min(self.maximum_batch_items);
                self.poll_nanos = self
                    .poll_nanos
                    .checked_div(2)
                    .unwrap_or(self.minimum_poll_nanos)
                    .max(self.minimum_poll_nanos);
            }
            AdaptiveAssessment::Complete
            | AdaptiveAssessment::Partial
            | AdaptiveAssessment::RateLimited
            | AdaptiveAssessment::IntegrityPressure => self.apply_pressure(),
        }
        Ok(())
    }

    pub(crate) fn observe_queue_or_publication_pressure(&mut self) {
        self.apply_pressure();
    }

    fn apply_pressure(&mut self) {
        self.batch_items = self.batch_items.checked_div(2).unwrap_or(1).max(1);
        self.poll_nanos = self
            .poll_nanos
            .checked_mul(2)
            .unwrap_or(self.maximum_poll_nanos)
            .min(self.maximum_poll_nanos);
    }
}

fn pressure_threshold(limit: usize) -> Result<u64, SchwabRestQuoteRuntimeError> {
    let limit = u64::try_from(limit).map_err(|_error| SchwabRestQuoteRuntimeError::Authority)?;
    limit
        .checked_sub(limit / 4)
        .filter(|value| *value > 0)
        .ok_or(SchwabRestQuoteRuntimeError::Authority)
}

/// Sole production owner of one callable Schwab REST quote generation.
pub(crate) struct SchwabRestQuoteProducer {
    activation: SchwabMarketDataAccountActivation,
    connection_generation: ConnectionGeneration,
    evidence: SchwabRestQuoteSourceEvidence,
    bindings: Arc<[SchwabRestQuoteInstrumentBinding]>,
    reference_identity: Option<MarketReferenceIdentityAuthority>,
    listing_reference: Option<ListingReferenceReadCapability>,
    nasdaq_generation: Option<ListingReferenceGenerationReceipt>,
    request_admission: RequestAdmission,
    next_binding: usize,
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
    #[cfg(test)]
    pub(crate) async fn publish_test_completed_response(
        sink: &dyn SchwabRestQuoteEventSink,
        response: ExecutedRestResponse,
        evidence: SchwabRestQuoteSourceEvidence,
        bindings: Vec<SchwabRestQuoteInstrumentBinding>,
        oauth_epoch: SchwabOAuthPublicationEpoch,
        connection_generation: ConnectionGeneration,
    ) -> Result<SchwabRestQuotePublicationReceipt, SchwabRestQuoteSinkError> {
        let accounting = response.accounting();
        sink.publish(SchwabRestQuoteBatch {
            outcome: SchwabRestQuoteBatchOutcome::Accepted(response),
            evidence,
            bindings: bindings.into(),
            oauth_epoch,
            connection_generation,
            accounting,
        })
        .await
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "authority, source evidence, finite bounds, shared rate, and sink are independent"
    )]
    pub(crate) fn try_production(
        activation: SchwabMarketDataAccountActivation,
        provider_rate: &ProviderRateAuthority,
        connection_generation: ConnectionGeneration,
        evidence: SchwabRestQuoteSourceEvidence,
        bindings: Vec<SchwabRestQuoteInstrumentBinding>,
        reference_identity: Option<MarketReferenceIdentityAuthority>,
        listing_reference: Option<ListingReferenceReadCapability>,
        nasdaq_generation: Option<ListingReferenceGenerationReceipt>,
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
            || nasdaq_generation.is_some()
                != (reference_identity.is_some() && listing_reference.is_some())
            || nasdaq_generation.is_none()
                && bindings
                    .iter()
                    .any(|binding| binding.identity_approval().is_some())
        {
            return Err(SchwabRestQuoteRuntimeError::Authority);
        }

        let _validated_full_request = QuoteRequest::try_new(
            bindings
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
        Ok(Self {
            activation,
            connection_generation,
            evidence,
            bindings: bindings.into(),
            reference_identity,
            listing_reference,
            nasdaq_generation,
            request_admission: bounds.request_admission,
            next_binding: 0,
            executor,
            budget,
            sink,
        })
    }

    pub(crate) fn remaining_budget_wait(
        &self,
        deadline: market_squawk_sources::MonotonicInstant,
    ) -> Result<std::time::Duration, SchwabRestQuoteRuntimeError> {
        self.budget
            .remaining_wait(deadline)
            .map_err(SchwabRestQuoteRuntimeError::Budget)
    }

    /// Executes at most one provider request. Waiting policy remains with the shared scheduler.
    pub(crate) async fn poll_once(
        &mut self,
        maximum_items: usize,
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
        let (now, valid_through) = authority_window(cancellation, deadline)?;
        if !self.activation.doctor_receipt().is_current_at(now)
            || !self.evidence.metadata().is_effective_at(now)
            || !doctor_admits_quotes(&self.activation)
        {
            return Err(SchwabRestQuoteRuntimeError::RefreshRequired);
        }
        let generation = self.connection_generation;
        let (request, bindings, next_binding) = self.request_batch(maximum_items)?;
        self.revalidate_identity_authority(&bindings, now, valid_through, cancellation, deadline)
            .await?;
        let attempt = tokio::select! {
            biased;
            () = cancellation.cancelled() => return Err(SchwabRestQuoteRuntimeError::Cancelled),
            () = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => {
                return Err(SchwabRestQuoteRuntimeError::Deadline);
            }
            attempt = self.activation.acquire_publication_attempt() => attempt?,
        };
        let (token, oauth_epoch) = attempt;
        let oauth = oauth_epoch.receipt();
        oauth_epoch
            .validate_current(oauth)
            .map_err(|_error| SchwabRestQuoteRuntimeError::RefreshRequired)?;
        if token.generation() != oauth.generation()
            || oauth.generation().get()
                != self.activation.doctor_receipt().access_token_generation()
        {
            return Err(SchwabRestQuoteRuntimeError::RefreshRequired);
        }
        let (dispatch_at, dispatch_valid_through) = authority_window(cancellation, deadline)?;
        self.revalidate_dispatch_authority(
            dispatch_at,
            dispatch_valid_through,
            cancellation,
            deadline,
        )?;
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
                request.request(),
                &token,
                operation_cancellation.clone(),
            ) => result,
        };
        let outcome = outcome?;
        let capacity = outcome.capacity_observation()?;
        let (outcome, accounting, receipt) = classify_quote_outcome(outcome)?;
        if accounting.requested != bindings.len() as u64
            || accounting.returned.checked_add(accounting.missing) != Some(accounting.requested)
        {
            return Err(SchwabRestQuoteRuntimeError::Accounting);
        }
        self.next_binding = next_binding;
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
            bindings,
            oauth_epoch,
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
        // The sink receipt proves the exactly-once raw/durable continuation completed. A caller
        // deadline or cancellation observed after that commit cannot turn success into a false
        // no-effect result or authorize a duplicate provider request.
        if let Some(reason) = budget_failure {
            return Err(SchwabRestQuoteRuntimeError::Budget(reason));
        }
        if publication.published() == 0 {
            Ok(SchwabRestQuotePollOutcome::SealedWithoutPublication {
                requested: accounting.requested,
                returned: accounting.returned,
                current: publication.current(),
                capacity,
            })
        } else {
            Ok(SchwabRestQuotePollOutcome::Published {
                requested: accounting.requested,
                returned: accounting.returned,
                published: publication.published(),
                capacity,
            })
        }
    }

    fn request_batch(
        &self,
        maximum_items: usize,
    ) -> Result<
        (QuoteRequest, Arc<[SchwabRestQuoteInstrumentBinding]>, usize),
        SchwabRestQuoteRuntimeError,
    > {
        if maximum_items == 0 || self.bindings.is_empty() {
            return Err(SchwabRestQuoteRuntimeError::Authority);
        }
        let count = maximum_items.min(self.bindings.len());
        let mut selected = Vec::new();
        selected
            .try_reserve_exact(count)
            .map_err(|_error| SchwabRestQuoteRuntimeError::Allocation)?;
        for offset in 0..count {
            let index = self
                .next_binding
                .checked_add(offset)
                .ok_or(SchwabRestQuoteRuntimeError::Accounting)?
                % self.bindings.len();
            selected.push(self.bindings[index].clone());
        }
        let admission = RequestAdmission::new(
            NonZeroUsize::new(self.request_admission.max_request_bytes())
                .ok_or(SchwabRestQuoteRuntimeError::Authority)?,
            NonZeroUsize::new(count).ok_or(SchwabRestQuoteRuntimeError::Authority)?,
        );
        let request = QuoteRequest::try_new(
            selected
                .iter()
                .map(|binding| ProviderIdentifier::try_new(binding.provider_symbol().to_owned()))
                .collect::<Result<Vec<_>, _>>()?,
            vec![QuoteField::Quote],
            None,
            admission,
        )?;
        let next_binding = self
            .next_binding
            .checked_add(count)
            .ok_or(SchwabRestQuoteRuntimeError::Accounting)?
            % self.bindings.len();
        Ok((request, selected.into(), next_binding))
    }

    async fn revalidate_identity_authority(
        &self,
        selected: &[SchwabRestQuoteInstrumentBinding],
        at: Timestamp,
        valid_through: Timestamp,
        cancellation: &CancellationToken,
        deadline: Instant,
    ) -> Result<(), SchwabRestQuoteRuntimeError> {
        self.validate_binding_authority_window(at, valid_through)?;
        self.require_current_nasdaq_generation(cancellation, deadline)?;
        let Some(identity) = &self.reference_identity else {
            return Ok(());
        };
        for binding in selected {
            let Some(retained) = binding.identity_approval() else {
                continue;
            };
            require_time(cancellation, deadline)?;
            let resolution = identity
                .resolve(retained.request().clone(), deadline, cancellation)
                .await
                .map_err(|_error| {
                    if cancellation.is_cancelled() {
                        SchwabRestQuoteRuntimeError::Cancelled
                    } else if Instant::now() >= deadline {
                        SchwabRestQuoteRuntimeError::Deadline
                    } else {
                        SchwabRestQuoteRuntimeError::IdentityResolutionRequired
                    }
                })?;
            let MarketReferenceIdentityResolution::Available(current) = resolution else {
                return Err(SchwabRestQuoteRuntimeError::IdentityResolutionRequired);
            };
            if !same_reference_authority(retained, &current, valid_through) {
                return Err(SchwabRestQuoteRuntimeError::IdentityResolutionRequired);
            }
        }
        Ok(())
    }

    /// Rechecks only retained interval and exact-generation authority while the one-use OAuth
    /// epoch remains owned locally. No provider request or reference re-resolution occurs here.
    fn revalidate_dispatch_authority(
        &self,
        at: Timestamp,
        valid_through: Timestamp,
        cancellation: &CancellationToken,
        deadline: Instant,
    ) -> Result<(), SchwabRestQuoteRuntimeError> {
        self.validate_binding_authority_window(at, valid_through)?;
        self.require_current_nasdaq_generation(cancellation, deadline)
    }

    fn validate_binding_authority_window(
        &self,
        at: Timestamp,
        valid_through: Timestamp,
    ) -> Result<(), SchwabRestQuoteRuntimeError> {
        let mut all = Vec::new();
        all.try_reserve_exact(self.bindings.len())
            .map_err(|_error| SchwabRestQuoteRuntimeError::Allocation)?;
        for binding in self.bindings.iter() {
            all.push((
                binding.binding().clone(),
                binding.identity_approval().cloned(),
            ));
        }
        for evaluated_at in [at, valid_through] {
            self.activation
                .validate_current_quote_bindings(
                    self.evidence.metadata(),
                    &all,
                    self.nasdaq_generation.as_ref(),
                    evaluated_at,
                    self.request_admission.max_items(),
                    true,
                )
                .map_err(|_error| SchwabRestQuoteRuntimeError::IdentityResolutionRequired)?;
        }
        Ok(())
    }

    fn require_current_nasdaq_generation(
        &self,
        cancellation: &CancellationToken,
        deadline: Instant,
    ) -> Result<(), SchwabRestQuoteRuntimeError> {
        let Some(expected_generation) = &self.nasdaq_generation else {
            return Ok(());
        };
        require_time(cancellation, deadline)?;
        let reader = self
            .listing_reference
            .as_ref()
            .ok_or(SchwabRestQuoteRuntimeError::IdentityResolutionRequired)?;
        let current_generation = reader
            .current(deadline, cancellation)
            .map_err(|_error| {
                if cancellation.is_cancelled() {
                    SchwabRestQuoteRuntimeError::Cancelled
                } else if Instant::now() >= deadline {
                    SchwabRestQuoteRuntimeError::Deadline
                } else {
                    SchwabRestQuoteRuntimeError::IdentityResolutionRequired
                }
            })?
            .ok_or(SchwabRestQuoteRuntimeError::IdentityResolutionRequired)?;
        if &current_generation != expected_generation {
            return Err(SchwabRestQuoteRuntimeError::IdentityResolutionRequired);
        }
        Ok(())
    }
}

fn same_reference_authority(
    retained: &MarketReferenceIdentityApprovalV1,
    current: &MarketReferenceIdentityApprovalV1,
    valid_through: Timestamp,
) -> bool {
    retained.request() == current.request()
        && retained.instrument_id() == current.instrument_id()
        && retained.asset_class() == current.asset_class()
        && retained.quote_currency() == current.quote_currency()
        && retained.listing_payload_evidence() == current.listing_payload_evidence()
        && retained.listing_source_timestamp() == current.listing_source_timestamp()
        && retained.listing_observed_at() == current.listing_observed_at()
        && retained.definition_revision_digest() == current.definition_revision_digest()
        && retained.definition_reference_evidence() == current.definition_reference_evidence()
        && retained.quote_currency_evidence() == current.quote_currency_evidence()
        && retained.evaluated_at() < retained.expires_at()
        && current.evaluated_at() < current.expires_at()
        && valid_through < retained.expires_at()
        && valid_through < current.expires_at()
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

/// Conservatively projects one monotonic operation deadline onto the trusted wall clock.
///
/// The monotonic clock is sampled first, so time spent reading the wall clock can only extend the
/// required authority horizon rather than shorten it.
fn authority_window(
    cancellation: &CancellationToken,
    deadline: Instant,
) -> Result<(Timestamp, Timestamp), SchwabRestQuoteRuntimeError> {
    if cancellation.is_cancelled() {
        return Err(SchwabRestQuoteRuntimeError::Cancelled);
    }
    let monotonic_now = Instant::now();
    let remaining = deadline
        .checked_duration_since(monotonic_now)
        .filter(|remaining| !remaining.is_zero())
        .ok_or(SchwabRestQuoteRuntimeError::Deadline)?;
    let at = wall_timestamp()?;
    let remaining_nanos =
        i64::try_from(remaining.as_nanos()).map_err(|_error| SchwabRestQuoteRuntimeError::Clock)?;
    let valid_through = at
        .checked_add_nanos(remaining_nanos)
        .map_err(|_error| SchwabRestQuoteRuntimeError::Clock)?;
    Ok((at, valid_through))
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
    #[error("Schwab quote runtime canonical reference identity must be resolved again")]
    IdentityResolutionRequired,
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
    OAuth(#[from] crate::provider_onboarding::SchwabOAuthRuntimeError),
    #[error(transparent)]
    BudgetPool(#[from] market_squawk_sources::BudgetPoolError),
    #[error(transparent)]
    Sink(#[from] SchwabRestQuoteSinkError),
}
