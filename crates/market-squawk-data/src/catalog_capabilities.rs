//! Cloneable least-authority capabilities over the sole analytical catalog writer.

use std::fmt;
use std::sync::{Arc, Mutex, MutexGuard};

use market_squawk_sources::{CapabilityRegistrationOutcome, OnboardingEvent, ProviderCapability};
use uuid::Uuid;

use crate::{
    CatalogAuthority, CatalogError, FairValueCatalogCommit, FairValueCatalogOperation,
    FairValueCatalogPosition, FairValueCatalogSnapshot, FairValueCatalogSnapshotLimits,
    OnboardingAppendOutcome, OnboardingReservation, OnboardingReservationRequest,
    ResumedProviderOnboarding,
};

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

    fn lock(&self) -> Result<MutexGuard<'_, CatalogAuthority>, CatalogError> {
        self.authority
            .lock()
            .map_err(|_| CatalogError::AuthorityLockPoisoned)
    }
}
