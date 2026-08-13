//! Cloneable least-authority capabilities over the sole analytical catalog writer.

use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Instant;
use std::{collections::BTreeSet, fmt};

use market_squawk_adapter_alpaca::{
    AlpacaDoctorBatchObservation as AdapterAlpacaDoctorBatchObservation,
    AlpacaDoctorCalendarObservation as AdapterAlpacaDoctorCalendarObservation,
    AlpacaDoctorHistoricalObservation as AdapterAlpacaDoctorHistoricalObservation,
    AlpacaDoctorHttpEvidence as AdapterAlpacaDoctorHttpEvidence,
    AlpacaDoctorObservationDisposition, AlpacaDoctorObservationOrigin, AlpacaDoctorObservedField,
    AlpacaDoctorQuoteObservation as AdapterAlpacaDoctorQuoteObservation,
    AlpacaDoctorRateEvidence as AdapterAlpacaDoctorRateEvidence, AlpacaDoctorRetryAfter,
    AlpacaDoctorStreamObservation as AdapterAlpacaDoctorStreamObservation,
    AlpacaPaperIexDoctorObservation,
};
use market_squawk_domain::{
    CompanyIdentityObservation, CompanyIdentitySurface, DataQuality, DigestAlgorithm,
    EvidenceDigest, InstrumentDefinition, InstrumentId, SourceId, SourceIdentifier, Timestamp,
};
use market_squawk_platform::SecretGeneration;
use market_squawk_sources::{
    ALPACA_BASIC_MARKET_DATA_SURFACE_ID, AlpacaDoctorAdditionalCapability,
    AlpacaDoctorBatchObservation, AlpacaDoctorCalendarObservation, AlpacaDoctorCapabilityEvidence,
    AlpacaDoctorCredentialRealm, AlpacaDoctorHistoricalObservation,
    AlpacaDoctorHistoricalPageEvidence, AlpacaDoctorHttpEvidence, AlpacaDoctorProbeEvidence,
    AlpacaDoctorQuoteObservation, AlpacaDoctorRateEvidence, AlpacaDoctorStreamObservation,
    AlpacaPaperIexDoctorReceiptInput, AlpacaPaperIexDoctorReceiptV1, AlpacaRateLimitField,
    AlpacaRetryAfterEvidence, CapabilityRegistrationOutcome, OnboardingEvent, OnboardingState,
    ProviderCapability, RuntimeCapabilityDisposition, RuntimeVerificationEvidence,
};
use sha2::{Digest as _, Sha256};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::{
    CatalogAuthority, CatalogError, CatalogLimit, CompanyIdentitySearchPage,
    FairValueCatalogCommit, FairValueCatalogOperation, FairValueCatalogPosition,
    FairValueCatalogSnapshot, FairValueCatalogSnapshotLimits, InstrumentSearchPage,
    OnboardingAppendOutcome, OnboardingReservation, OnboardingReservationRequest,
    PinnedInstrumentDefinitions, ResumedProviderOnboarding,
};

/// Cloneable bounded company-identity reader without general catalog authority.
///
/// Company identities remain research metadata. This capability cannot publish canonical
/// instruments or grant execution authority.
#[derive(Clone)]
pub struct CompanyIdentityReadCapability {
    authority: Arc<Mutex<CatalogAuthority>>,
}

impl fmt::Debug for CompanyIdentityReadCapability {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CompanyIdentityReadCapability")
            .field("authority", &"[SEALED COMPANY-IDENTITY READ AUTHORITY]")
            .finish()
    }
}

impl CompanyIdentityReadCapability {
    pub(crate) fn new(authority: Arc<Mutex<CatalogAuthority>>) -> Self {
        Self { authority }
    }

    /// Searches current digest-verified company observations under hard bounds.
    pub fn search(
        &self,
        query: &str,
        maximum_companies: usize,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<CompanyIdentitySearchPage, CatalogError> {
        let limit = CatalogLimit::new(maximum_companies)?;
        self.lock()?
            .catalog()
            .search_company_identities(query, limit, deadline, cancellation)
    }

    /// Reads the exact current source-qualified company observation and its canonical parent.
    ///
    /// Names, tickers, and exchanges are absent from the query. The returned observation is
    /// digest-revalidated and includes its source availability and ingestion coordinates; the
    /// final timestamp is the successful parent ingest's durable completion time.
    pub fn exact_current(
        &self,
        source_id: &SourceId,
        provider_company_id: &SourceIdentifier,
        surface: CompanyIdentitySurface,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<
        Option<(CompanyIdentityObservation, EvidenceDigest, Timestamp)>,
        crate::CompanySecurityIdentityCatalogError,
    > {
        self.authority
            .try_lock()
            .map_err(|_| crate::CompanySecurityIdentityCatalogError::AuthorityUnavailable)?
            .exact_current_company_identity(
                source_id,
                provider_company_id,
                surface,
                deadline,
                cancellation,
            )
    }

    /// Narrows this company reader to exact company/security relationship reads.
    ///
    /// The derived capability retains no company publication, market publication, rights, or
    /// execution authority.
    pub fn security_relationships(&self) -> crate::CompanySecurityIdentityReadCapability {
        crate::CompanySecurityIdentityReadCapability::new(Arc::clone(&self.authority))
    }

    fn lock(&self) -> Result<MutexGuard<'_, CatalogAuthority>, CatalogError> {
        self.authority
            .lock()
            .map_err(|_| CatalogError::AuthorityLockPoisoned)
    }
}

/// Cloneable, bounded canonical-instrument publication authority.
///
/// The capability exposes only restart-safe reference-master reconciliation. It cannot access
/// general catalog records, rights state, analytical generations, or SQLite.
#[derive(Clone)]
pub struct InstrumentCatalogCapability {
    authority: Arc<Mutex<CatalogAuthority>>,
}

impl fmt::Debug for InstrumentCatalogCapability {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("InstrumentCatalogCapability")
            .field("authority", &"[SEALED INSTRUMENT CATALOG AUTHORITY]")
            .finish()
    }
}

impl InstrumentCatalogCapability {
    pub(crate) fn new(authority: Arc<Mutex<CatalogAuthority>>) -> Self {
        Self { authority }
    }

    /// Atomically reconciles one bounded configured instrument universe.
    pub fn synchronize(
        &self,
        instruments: &[InstrumentDefinition],
        observed_at: Timestamp,
        limit: CatalogLimit,
    ) -> Result<usize, CatalogError> {
        self.lock()?
            .synchronize_instruments(instruments, observed_at, limit)
    }

    fn lock(&self) -> Result<MutexGuard<'_, CatalogAuthority>, CatalogError> {
        self.authority
            .lock()
            .map_err(|_| CatalogError::AuthorityLockPoisoned)
    }
}

/// Cloneable point-in-time instrument-definition reader without general catalog authority.
#[derive(Clone)]
pub struct InstrumentDefinitionReadCapability {
    authority: Arc<Mutex<CatalogAuthority>>,
}

impl fmt::Debug for InstrumentDefinitionReadCapability {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("InstrumentDefinitionReadCapability")
            .field(
                "authority",
                &"[SEALED INSTRUMENT-DEFINITION READ AUTHORITY]",
            )
            .finish()
    }
}

impl InstrumentDefinitionReadCapability {
    pub(crate) fn new(authority: Arc<Mutex<CatalogAuthority>>) -> Self {
        Self { authority }
    }

    /// Mints one exact bounded receipt from the sole catalog session.
    pub fn pin(
        &self,
        instrument_ids: &[InstrumentId],
        as_of: Timestamp,
        limit: CatalogLimit,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<PinnedInstrumentDefinitions, CatalogError> {
        self.lock()?.pin_instrument_definitions_bounded(
            instrument_ids,
            as_of,
            limit,
            deadline,
            cancellation,
        )
    }

    /// Returns the newest verified definition for each requested stable identity.
    ///
    /// The operation is bounded by `maximum_instruments`, checks cancellation and deadline around
    /// every catalog read, and never substitutes a definition for a missing identity.
    pub fn latest(
        &self,
        instrument_ids: &[InstrumentId],
        maximum_instruments: usize,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<Vec<InstrumentDefinition>, CatalogError> {
        CatalogLimit::new(maximum_instruments)?;
        if instrument_ids.len() > maximum_instruments
            || instrument_ids
                .iter()
                .copied()
                .collect::<BTreeSet<_>>()
                .len()
                != instrument_ids.len()
        {
            return Err(CatalogError::InvalidLimit);
        }
        let authority = self.lock()?;
        let one = CatalogLimit::new(1)?;
        let mut definitions = Vec::new();
        definitions
            .try_reserve_exact(instrument_ids.len())
            .map_err(|_| CatalogError::Allocation)?;
        for instrument_id in instrument_ids {
            check_read(deadline, cancellation)?;
            if let Some(definition) = authority
                .instrument_history(*instrument_id, one)?
                .into_iter()
                .next()
            {
                definitions.push(definition);
            }
        }
        check_read(deadline, cancellation)?;
        Ok(definitions)
    }

    /// Searches the canonical reference master without exposing general catalog authority.
    ///
    /// Results include only digest-verified current definitions and matching symbol history. The
    /// catalog excludes quarantined or rights-restricted identity assertions before returning the
    /// page.
    pub fn search(
        &self,
        query: &str,
        maximum_instruments: usize,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<InstrumentSearchPage, CatalogError> {
        let limit = CatalogLimit::new(maximum_instruments)?;
        self.lock()?
            .search_instruments(query, limit, deadline, cancellation)
    }

    fn lock(&self) -> Result<MutexGuard<'_, CatalogAuthority>, CatalogError> {
        self.authority
            .lock()
            .map_err(|_| CatalogError::AuthorityLockPoisoned)
    }
}

fn check_read(deadline: Instant, cancellation: &CancellationToken) -> Result<(), CatalogError> {
    if cancellation.is_cancelled() {
        Err(CatalogError::InstrumentDefinitionReadCancelled)
    } else if Instant::now() >= deadline {
        Err(CatalogError::InstrumentDefinitionReadDeadlineExceeded)
    } else {
        Ok(())
    }
}

/// Cloneable fair-value persistence authority without general catalog or SQLite access.
#[derive(Clone)]
pub struct FairValueCatalogCapability {
    authority: Arc<Mutex<CatalogAuthority>>,
}

impl fmt::Debug for FairValueCatalogCapability {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("FairValueCatalogCapability")
            .field("authority", &"[SEALED FAIR-VALUE CATALOG AUTHORITY]")
            .finish()
    }
}

impl FairValueCatalogCapability {
    pub(crate) fn new(authority: Arc<Mutex<CatalogAuthority>>) -> Self {
        Self { authority }
    }

    /// Reads and validates one complete bounded fair-value recovery snapshot.
    pub fn fair_value_snapshot(
        &self,
        limits: FairValueCatalogSnapshotLimits,
    ) -> Result<FairValueCatalogSnapshot, CatalogError> {
        self.lock()?.fair_value_snapshot(limits)
    }

    /// Atomically appends one exact fair-value operation at the expected durable position.
    pub fn append_fair_value_operation(
        &self,
        operation: &FairValueCatalogOperation,
        limits: FairValueCatalogSnapshotLimits,
        expected_position: FairValueCatalogPosition,
    ) -> Result<FairValueCatalogCommit, CatalogError> {
        self.lock()?
            .append_fair_value_operation(operation, limits, expected_position)
    }

    fn lock(&self) -> Result<MutexGuard<'_, CatalogAuthority>, CatalogError> {
        self.authority
            .lock()
            .map_err(|_| CatalogError::AuthorityLockPoisoned)
    }
}

/// Provider-onboarding authority without general catalog or SQLite access.
///
/// Generic transitions cannot submit runtime-verification evidence. Digest runtime evidence is
/// limited to non-Alpaca surfaces, while Alpaca admission consumes the adapter's provider-observed
/// doctor output and binds it to the catalog's retained lifecycle coordinates.
pub struct OnboardingCatalogCapability {
    authority: Arc<Mutex<CatalogAuthority>>,
}

impl fmt::Debug for OnboardingCatalogCapability {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OnboardingCatalogCapability")
            .field("authority", &"[SEALED ONBOARDING CATALOG AUTHORITY]")
            .finish()
    }
}

impl OnboardingCatalogCapability {
    pub(crate) fn new(authority: Arc<Mutex<CatalogAuthority>>) -> Self {
        Self { authority }
    }

    /// Registers one immutable contiguous provider-capability revision.
    pub fn register_provider_capability(
        &self,
        capability: &ProviderCapability,
    ) -> Result<CapabilityRegistrationOutcome, CatalogError> {
        self.lock()?.register_provider_capability(capability)
    }

    /// Creates one durable non-secret onboarding reservation.
    pub fn reserve_provider_onboarding(
        &self,
        request: &OnboardingReservationRequest,
    ) -> Result<OnboardingReservation, CatalogError> {
        self.lock()?.reserve_provider_onboarding(request)
    }

    /// Returns catalog health for this restricted writer session.
    pub fn health(&self) -> Result<crate::CatalogHealth, CatalogError> {
        self.lock()?.health()
    }

    /// Appends one exact non-runtime lifecycle transition or confirms its replay.
    ///
    /// Runtime verification must use one of the evidence-specific methods below. In particular,
    /// arbitrary typed receipt bytes are never accepted through this generic transition path.
    pub fn append_provider_onboarding_event(
        &self,
        reservation: &OnboardingReservation,
        sequence: u64,
        event: OnboardingEvent,
    ) -> Result<OnboardingAppendOutcome, CatalogError> {
        if matches!(&event, OnboardingEvent::RuntimeVerified { .. }) {
            return Err(CatalogError::InvalidRecord);
        }
        self.lock()?
            .append_provider_onboarding_event(reservation, sequence, event)
    }

    /// Appends digest-only runtime evidence for a non-Alpaca surface.
    ///
    /// Alpaca's activating surface requires the provider-owned doctor observation and is rejected
    /// here even if a caller supplies a syntactically valid digest.
    pub fn append_digest_runtime_verification(
        &self,
        reservation: &OnboardingReservation,
        sequence: u64,
        generation: Option<SecretGeneration>,
        evidence_digest: EvidenceDigest,
    ) -> Result<OnboardingAppendOutcome, CatalogError> {
        let authority = self.lock()?;
        let resumed = authority.resume_provider_onboarding(reservation.session_id())?;
        if resumed.reservation() != reservation
            || resumed.lifecycle().surface_id().as_str() == ALPACA_BASIC_MARKET_DATA_SURFACE_ID
        {
            return Err(CatalogError::InvalidOnboardingReservationCapability);
        }
        let evidence = RuntimeVerificationEvidence::digest_v1(evidence_digest)
            .map_err(|_| CatalogError::InvalidRecord)?;
        authority.append_provider_onboarding_event(
            reservation,
            sequence,
            OnboardingEvent::RuntimeVerified {
                generation,
                evidence,
            },
        )
    }

    /// Consumes one provider-observed Alpaca Paper/IEX doctor result into an exact durable event.
    ///
    /// Every authority coordinate is recovered from the retained reservation and lifecycle while
    /// the catalog writer mutex is held. The observation's internally bound credential principal
    /// must exactly match the retained non-expiring authority verification. Neither receipt bytes,
    /// a principal, nor a renewal predecessor can be supplied by the caller.
    pub fn append_alpaca_paper_iex_doctor_observation(
        &self,
        reservation: &OnboardingReservation,
        sequence: u64,
        generation: SecretGeneration,
        observation: AlpacaPaperIexDoctorObservation,
    ) -> Result<OnboardingAppendOutcome, CatalogError> {
        let authority = self.lock()?;
        let resumed = authority.resume_provider_onboarding(reservation.session_id())?;
        if resumed.reservation() != reservation
            || resumed.lifecycle().surface_id().as_str() != ALPACA_BASIC_MARKET_DATA_SURFACE_ID
            || observation.origin() != AlpacaDoctorObservationOrigin::ProviderObserved
        {
            return Err(CatalogError::InvalidOnboardingReservationCapability);
        }
        let lifecycle = resumed.lifecycle();
        let verification = lifecycle
            .generation_verification(generation)
            .ok_or(CatalogError::InvalidRecord)?;
        let principal = verification
            .bindings()
            .account_digest()
            .ok_or(CatalogError::InvalidRecord)?;
        if verification.expires_at().is_some()
            || observation.market_data_principal_sha256() != principal
        {
            return Err(CatalogError::InvalidRecord);
        }
        let rights_decision_digest = lifecycle
            .generation_rights_digest(generation)
            .ok_or(CatalogError::InvalidRecord)?;
        let rate_policy_digest = lifecycle
            .generation_rate_policy_digest(generation)
            .ok_or(CatalogError::InvalidRecord)?;
        if rights_decision_digest != verification.restrictions_digest() {
            return Err(CatalogError::InvalidRecord);
        }
        let predecessor_digest = if lifecycle.state() == OnboardingState::RenewalRequired
            && lifecycle.active_generation() == Some(generation)
            && lifecycle.candidate_generation().is_none()
        {
            Some(
                lifecycle
                    .generation_runtime_evidence(generation)
                    .and_then(RuntimeVerificationEvidence::alpaca_paper_iex_receipt)
                    .map(AlpacaPaperIexDoctorReceiptV1::receipt_sha256)
                    .ok_or(CatalogError::InvalidRecord)?,
            )
        } else if lifecycle.candidate_generation() == Some(generation)
            && lifecycle.generation_runtime_evidence(generation).is_none()
        {
            None
        } else {
            return Err(CatalogError::InvalidRecord);
        };
        let context = lifecycle
            .runtime_verification_context()
            .ok_or(CatalogError::InvalidRecord)?;
        let verified_at = observation.completed_at();
        let exclusive_expires_at = verified_at
            .unix_nanos()
            .checked_add(AlpacaPaperIexDoctorReceiptV1::VALIDITY_NANOS)
            .map(Timestamp::from_unix_nanos)
            .ok_or(CatalogError::InvalidRecord)?;
        let receipt = AlpacaPaperIexDoctorReceiptV1::try_new(AlpacaPaperIexDoctorReceiptInput {
            provider_observation_origin: AlpacaPaperIexDoctorReceiptV1::provider_observed_origin()
                .map_err(|_| CatalogError::InvalidRecord)?,
            provider_observation_sha256: observation.observation_digest(),
            surface_id: lifecycle.surface_id().clone(),
            session_identifier: context.session_identifier().clone(),
            generation,
            realm: AlpacaDoctorCredentialRealm::Paper,
            market_data_principal_sha256: principal,
            capability_revision: lifecycle.capability_revision(),
            capability_digest: lifecycle.capability_digest(),
            public_configuration_digest: context.public_configuration_digest(),
            rights_decision_digest,
            rate_policy_digest,
            data_quality: DataQuality::DirectUnverified,
            quote: AlpacaDoctorProbeEvidence {
                disposition: map_alpaca_doctor_disposition(observation.quote().disposition()),
                disposition_evidence_digest: observation.quote().semantic_result_digest(),
                observation: Some(map_alpaca_quote(observation.quote())),
            },
            batch: AlpacaDoctorProbeEvidence {
                disposition: map_alpaca_doctor_disposition(observation.batch().disposition()),
                disposition_evidence_digest: observation.batch().semantic_result_digest(),
                observation: Some(map_alpaca_batch(observation.batch())),
            },
            stream: AlpacaDoctorProbeEvidence {
                disposition: map_alpaca_doctor_disposition(observation.stream().disposition()),
                disposition_evidence_digest: observation.stream().semantic_result_digest(),
                observation: Some(map_alpaca_stream(observation.stream())),
            },
            historical: AlpacaDoctorProbeEvidence {
                disposition: map_alpaca_doctor_disposition(observation.historical().disposition()),
                disposition_evidence_digest: observation.historical().semantic_result_digest(),
                observation: Some(map_alpaca_historical(observation.historical())?),
            },
            calendar: AlpacaDoctorProbeEvidence {
                disposition: map_alpaca_doctor_disposition(observation.calendar().disposition()),
                disposition_evidence_digest: observation.calendar().semantic_result_digest(),
                observation: Some(map_alpaca_calendar(observation.calendar())),
            },
            additional_capabilities: alpaca_additional_capabilities(&observation)
                .into_boxed_slice(),
            verified_at,
            exclusive_expires_at,
            predecessor_digest,
        })
        .map_err(|_| CatalogError::InvalidRecord)?;
        authority.append_provider_onboarding_event(
            reservation,
            sequence,
            OnboardingEvent::RuntimeVerified {
                generation: Some(generation),
                evidence: RuntimeVerificationEvidence::AlpacaPaperIexDoctorReceiptV1(Box::new(
                    receipt,
                )),
            },
        )
    }

    /// Replays and validates one durable onboarding session for continued operation.
    pub fn resume_provider_onboarding(
        &self,
        session_id: Uuid,
    ) -> Result<ResumedProviderOnboarding, CatalogError> {
        self.lock()?.resume_provider_onboarding(session_id)
    }

    /// Returns newest-first durable sessions within one global row and byte bound.
    pub fn provider_onboarding_sessions(
        &self,
        limit: CatalogLimit,
    ) -> Result<Vec<ResumedProviderOnboarding>, CatalogError> {
        self.lock()?.provider_onboarding_sessions(limit)
    }

    /// Returns the latest durable session for each surface in canonical surface order.
    pub fn current_provider_onboarding_sessions(
        &self,
        limit: CatalogLimit,
    ) -> Result<Vec<ResumedProviderOnboarding>, CatalogError> {
        self.lock()?.current_provider_onboarding_sessions(limit)
    }

    /// Returns one deterministic page of session identities for complete startup reconciliation.
    pub fn provider_onboarding_session_ids_after(
        &self,
        after: Option<Uuid>,
        limit: CatalogLimit,
    ) -> Result<Vec<Uuid>, CatalogError> {
        self.lock()?
            .provider_onboarding_session_ids_after(after, limit)
    }

    fn lock(&self) -> Result<MutexGuard<'_, CatalogAuthority>, CatalogError> {
        self.authority
            .lock()
            .map_err(|_| CatalogError::AuthorityLockPoisoned)
    }
}

fn map_alpaca_quote(
    observation: &AdapterAlpacaDoctorQuoteObservation,
) -> AlpacaDoctorQuoteObservation {
    AlpacaDoctorQuoteObservation {
        http: map_alpaca_http(observation.http()),
        semantic_result_digest: observation.semantic_result_digest(),
        quote_timestamp: observation.quote_timestamp(),
        bid_price: observation.bid_price(),
        ask_price: observation.ask_price(),
        bid_size: observation.bid_size(),
        ask_size: observation.ask_size(),
    }
}

fn map_alpaca_batch(
    observation: &AdapterAlpacaDoctorBatchObservation,
) -> AlpacaDoctorBatchObservation {
    AlpacaDoctorBatchObservation {
        http: map_alpaca_http(observation.http()),
        semantic_result_digest: observation.semantic_result_digest(),
        requested_count: observation.requested_count(),
        returned_count: observation.returned_count(),
        missing_count: observation.missing_count(),
        unexpected_count: observation.unexpected_count(),
        duplicate_count: observation.duplicate_count(),
        invalid_count: observation.invalid_count(),
        effective_cardinality: observation.effective_cardinality(),
        requested_set_digest: observation.requested_symbols_digest(),
        returned_set_digest: observation.returned_symbols_digest(),
        missing_set_digest: observation.missing_symbols_digest(),
        unexpected_set_digest: observation.unexpected_symbols_digest(),
    }
}

fn map_alpaca_stream(
    observation: &AdapterAlpacaDoctorStreamObservation,
) -> AlpacaDoctorStreamObservation {
    AlpacaDoctorStreamObservation {
        endpoint_contract_digest: observation.endpoint_contract_digest(),
        request_digest: observation.request_digest(),
        connected_frame_digest: observation.connected_frame_digest(),
        authenticated_frame_digest: observation.authenticated_frame_digest(),
        subscription_frame_digest: observation.subscription_frame_digest(),
        semantic_result_digest: observation.semantic_result_digest(),
        handshake_status: observation.handshake_status(),
        handshake_rate: map_alpaca_rate(observation.handshake_rate()),
        subscribed_trade_count: observation.subscribed_trade_count(),
        subscribed_quote_count: observation.subscribed_quote_count(),
        frames_observed: observation.frames_observed(),
        bytes_observed: observation.bytes_observed(),
        authenticated_at: observation.authenticated_at(),
        subscribed_at: observation.subscribed_at(),
        close_sent: observation.close_sent(),
        clean_close_observed: observation.clean_close_observed(),
        completed_at: observation.completed_at(),
    }
}

fn map_alpaca_historical(
    observation: &AdapterAlpacaDoctorHistoricalObservation,
) -> Result<AlpacaDoctorHistoricalObservation, CatalogError> {
    let pages = observation
        .pages()
        .iter()
        .map(|page| AlpacaDoctorHistoricalPageEvidence {
            http: map_alpaca_http(page.http()),
            request_page_token_digest: page.request_page_token_digest(),
            response_page_token_digest: page.response_page_token_digest(),
        })
        .collect::<Vec<_>>();
    if pages.len()
        != usize::try_from(observation.page_count()).map_err(|_| CatalogError::InvalidRecord)?
    {
        return Err(CatalogError::InvalidRecord);
    }
    Ok(AlpacaDoctorHistoricalObservation {
        endpoint_contract_digest: observation.endpoint_contract_digest(),
        request_digest: observation.request_digest(),
        semantic_result_digest: observation.semantic_result_digest(),
        start_date: observation.start_date(),
        end_date: observation.end_date(),
        page_count: observation.page_count(),
        returned_bar_count: observation.returned_bar_count(),
        distinct_date_count: observation.distinct_date_count(),
        first_bar_timestamp: observation.first_bar_timestamp(),
        last_bar_timestamp: observation.last_bar_timestamp(),
        returned_dates_digest: observation.returned_dates_digest(),
        pagination_graph_digest: observation.pagination_graph_digest(),
        terminal_page_observed: observation.terminal_page_observed(),
        pages: pages.into_boxed_slice(),
    })
}

fn map_alpaca_calendar(
    observation: &AdapterAlpacaDoctorCalendarObservation,
) -> AlpacaDoctorCalendarObservation {
    AlpacaDoctorCalendarObservation {
        http: map_alpaca_http(observation.http()),
        semantic_result_digest: observation.semantic_result_digest(),
        start_date: observation.start_date(),
        end_date: observation.end_date(),
        session_count: observation.session_count(),
        history_date_count: observation.history_date_count(),
        matched_count: observation.matched_count(),
        missing_history_count: observation.missing_history_count(),
        unexpected_history_count: observation.unexpected_history_count(),
        session_dates_digest: observation.session_dates_digest(),
        history_dates_digest: observation.history_dates_digest(),
        exact_date_reconciliation: observation.exact_date_reconciliation(),
    }
}

fn map_alpaca_http(observation: &AdapterAlpacaDoctorHttpEvidence) -> AlpacaDoctorHttpEvidence {
    AlpacaDoctorHttpEvidence {
        endpoint_contract_digest: observation.endpoint_contract_digest(),
        request_digest: observation.request_digest(),
        status_code: observation.status_code(),
        body_digest: observation.body_digest(),
        response_bytes: observation.response_bytes(),
        received_at: observation.received_at(),
        latency_nanos: observation.latency_nanos(),
        rate: map_alpaca_rate(observation.rate()),
    }
}

fn map_alpaca_rate(observation: &AdapterAlpacaDoctorRateEvidence) -> AlpacaDoctorRateEvidence {
    AlpacaDoctorRateEvidence {
        limit: map_alpaca_observed_field(observation.limit()),
        remaining: map_alpaca_observed_field(observation.remaining()),
        reset_unix_seconds: map_alpaca_observed_field(observation.reset_unix_seconds()),
        retry_after: match observation.retry_after() {
            AlpacaDoctorObservedField::Observed(AlpacaDoctorRetryAfter::DelaySeconds(value)) => {
                AlpacaRateLimitField::Observed(AlpacaRetryAfterEvidence::DelaySeconds(*value))
            }
            AlpacaDoctorObservedField::Observed(AlpacaDoctorRetryAfter::AtUnixSeconds(value)) => {
                AlpacaRateLimitField::Observed(AlpacaRetryAfterEvidence::AtUnixSeconds(*value))
            }
            AlpacaDoctorObservedField::Missing => AlpacaRateLimitField::Missing,
        },
    }
}

fn map_alpaca_observed_field<T: Copy>(
    field: &AlpacaDoctorObservedField<T>,
) -> AlpacaRateLimitField<T> {
    match field {
        AlpacaDoctorObservedField::Observed(value) => AlpacaRateLimitField::Observed(*value),
        AlpacaDoctorObservedField::Missing => AlpacaRateLimitField::Missing,
    }
}

fn map_alpaca_doctor_disposition(
    disposition: AlpacaDoctorObservationDisposition,
) -> RuntimeCapabilityDisposition {
    match disposition {
        AlpacaDoctorObservationDisposition::ObservedAvailable => {
            RuntimeCapabilityDisposition::Available
        }
        AlpacaDoctorObservationDisposition::ObservedDegraded => {
            RuntimeCapabilityDisposition::Degraded
        }
        AlpacaDoctorObservationDisposition::ObservedUnavailable
        | AlpacaDoctorObservationDisposition::Unsupported => {
            RuntimeCapabilityDisposition::Unavailable
        }
        AlpacaDoctorObservationDisposition::Unprobed => RuntimeCapabilityDisposition::NotProbed,
    }
}

fn alpaca_additional_capabilities(
    observation: &AlpacaPaperIexDoctorObservation,
) -> Vec<AlpacaDoctorCapabilityEvidence> {
    [
        (
            AlpacaDoctorAdditionalCapability::OptionsRest,
            observation.indicative_options_rest(),
        ),
        (
            AlpacaDoctorAdditionalCapability::OptionsStream,
            observation.indicative_options_stream(),
        ),
        (
            AlpacaDoctorAdditionalCapability::FixedIncome,
            observation.fixed_income(),
        ),
        (
            AlpacaDoctorAdditionalCapability::CorporateActions,
            observation.corporate_actions(),
        ),
        (
            AlpacaDoctorAdditionalCapability::Sip,
            observation.consolidated_sip(),
        ),
        (AlpacaDoctorAdditionalCapability::Nbbo, observation.nbbo()),
        (AlpacaDoctorAdditionalCapability::Opra, observation.opra()),
        (
            AlpacaDoctorAdditionalCapability::PriceLevelDepth,
            observation.price_level_depth(),
        ),
        (
            AlpacaDoctorAdditionalCapability::OrderLevelDepth,
            observation.order_level_depth(),
        ),
        (
            AlpacaDoctorAdditionalCapability::BrokerageAccount,
            observation.brokerage_account(),
        ),
        (
            AlpacaDoctorAdditionalCapability::Positions,
            observation.positions(),
        ),
        (
            AlpacaDoctorAdditionalCapability::Orders,
            observation.orders(),
        ),
        (
            AlpacaDoctorAdditionalCapability::Trading,
            observation.trading(),
        ),
    ]
    .into_iter()
    .map(|(capability, observed)| {
        let disposition = map_alpaca_doctor_disposition(observed);
        AlpacaDoctorCapabilityEvidence {
            capability,
            disposition,
            disposition_evidence_digest: alpaca_additional_capability_digest(
                capability,
                disposition,
            ),
        }
    })
    .collect()
}

fn alpaca_additional_capability_digest(
    capability: AlpacaDoctorAdditionalCapability,
    disposition: RuntimeCapabilityDisposition,
) -> EvidenceDigest {
    let mut hasher = Sha256::new();
    hasher.update(b"market-squawk/alpaca-paper-iex-doctor-additional-capability/v1\0");
    hasher.update([alpaca_additional_capability_tag(capability)]);
    hasher.update([runtime_capability_disposition_tag(disposition)]);
    EvidenceDigest::new(DigestAlgorithm::Sha256, hasher.finalize().into())
}

const fn alpaca_additional_capability_tag(capability: AlpacaDoctorAdditionalCapability) -> u8 {
    match capability {
        AlpacaDoctorAdditionalCapability::OptionsRest => 1,
        AlpacaDoctorAdditionalCapability::OptionsStream => 2,
        AlpacaDoctorAdditionalCapability::FixedIncome => 3,
        AlpacaDoctorAdditionalCapability::CorporateActions => 4,
        AlpacaDoctorAdditionalCapability::Sip => 5,
        AlpacaDoctorAdditionalCapability::Nbbo => 6,
        AlpacaDoctorAdditionalCapability::Opra => 7,
        AlpacaDoctorAdditionalCapability::PriceLevelDepth => 8,
        AlpacaDoctorAdditionalCapability::OrderLevelDepth => 9,
        AlpacaDoctorAdditionalCapability::BrokerageAccount => 10,
        AlpacaDoctorAdditionalCapability::Positions => 11,
        AlpacaDoctorAdditionalCapability::Orders => 12,
        AlpacaDoctorAdditionalCapability::Trading => 13,
    }
}

const fn runtime_capability_disposition_tag(disposition: RuntimeCapabilityDisposition) -> u8 {
    match disposition {
        RuntimeCapabilityDisposition::Available => 1,
        RuntimeCapabilityDisposition::Degraded => 2,
        RuntimeCapabilityDisposition::Unavailable => 3,
        RuntimeCapabilityDisposition::NotProbed => 4,
    }
}
