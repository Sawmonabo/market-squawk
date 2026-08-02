//! Cloneable least-authority capabilities over the sole analytical catalog writer.

use std::fmt;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Instant;

use market_squawk_domain::{InstrumentId, Timestamp};
use market_squawk_sources::{CapabilityRegistrationOutcome, OnboardingEvent, ProviderCapability};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::{
    CatalogAuthority, CatalogError, CatalogLimit, FairValueCatalogCommit,
    FairValueCatalogOperation, FairValueCatalogPosition, FairValueCatalogSnapshot,
    FairValueCatalogSnapshotLimits, OnboardingAppendOutcome, OnboardingReservation,
    OnboardingReservationRequest, PinnedInstrumentDefinitions, ResumedProviderOnboarding,
};

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

    fn lock(&self) -> Result<MutexGuard<'_, CatalogAuthority>, CatalogError> {
        self.authority
            .lock()
            .map_err(|_| CatalogError::AuthorityLockPoisoned)
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

/// Cloneable provider-onboarding authority without general catalog or SQLite access.
#[derive(Clone)]
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

    /// Appends one exact contiguous lifecycle event or confirms its replay.
    pub fn append_provider_onboarding_event(
        &self,
        reservation: &OnboardingReservation,
        sequence: u64,
        event: OnboardingEvent,
    ) -> Result<OnboardingAppendOutcome, CatalogError> {
        self.lock()?
            .append_provider_onboarding_event(reservation, sequence, event)
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
